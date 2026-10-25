//! OKX clients for the live node.
//!
//! Provides the Nautilus client factories and configs from `nautilus_okx`:
//! `OKXDataClientFactory`, `OKXExecutionClientFactory` and their configs,
//! in the EEA region (`eea.okx.com`, where an EEA account trades X-Perps),
//! in cross margin, on the environment `OKX_ENVIRONMENT` names: `demo` or
//! `live`, required, read by us since the adapter takes it only as a
//! config field. The adapter reads the credentials from `OKX_API_KEY`,
//! `OKX_API_SECRET` and `OKX_API_PASSPHRASE`; this module only checks they
//! are set.
//!
//! Before the node starts, the account is checked: net position mode,
//! read through the adapter (`get_account_configuration`), and each
//! instrument's leverage equal to the run's. The adapter does not read
//! leverage, so that is our signed `GET /api/v5/account/leverage-info`,
//! signed with the adapter's `Credential`; to be removed if Nautilus gains
//! it. OKX sets the leverage, by hand on the account; this module never
//! changes the account.
//!
//! The strategy trades OKX instruments on bars built from OKX's 1-minute
//! candles (a composite bar type): the data client serves their warmup
//! from OKX's REST candles, up to now, and the live bars from the candle
//! channel. OKX serves no quote history (the data client does not
//! implement `request_quotes`), so bars built from quotes get no warmup.

use std::collections::HashMap;

use anyhow::{Context, ensure};
use nautilus_common::factories::{ClientConfig, DataClientFactory, ExecutionClientFactory};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_live::config::RoutingConfig;
use nautilus_model::identifiers::InstrumentId;
use nautilus_network::http::{HttpClient, HttpRedirectPolicy};
use nautilus_okx::{
    OKXPositionMode,
    common::{
        credential::Credential,
        enums::{OKXEnvironment, OKXMarginMode, OKXRegion},
    },
    config::{OKXDataClientConfig, OKXExecutionClientConfig},
    factories::{OKXDataClientFactory, OKXExecutionClientFactory},
    http::OKXHttpClient,
};
use rust_decimal::Decimal;
use serde::Deserialize;

use super::node::Clients;
use crate::data::OkxContract;

/// The variable naming the OKX environment of the run, `demo` or `live`.
const ENVIRONMENT_VAR: &str = "OKX_ENVIRONMENT";

/// The region of an account opened on my.okx.com, which trades X-Perps.
const REGION: OKXRegion = OKXRegion::Eea;

/// Cross margin: the account's whole balance backs every position.
const MARGIN_MODE: OKXMarginMode = OKXMarginMode::Cross;

/// The timeout of one account request.
const REQUEST_TIMEOUT_SECS: u64 = 60;

/// The OKX data and execution client configs of a node trading OKX swaps
/// or X-Perps.
#[derive(Debug, Clone)]
pub(super) struct OkxClients {
    pub(super) data: OKXDataClientConfig,
    pub(super) exec: OKXExecutionClientConfig,
}

/// Reads the environment (see `environment_of`), builds the client configs
/// of `instrument_ids` on it (see `configs`), checks the credentials are
/// set, then checks the account: net position mode, and the leverage of
/// each instrument equal to `leverage`.
///
/// # Errors
///
/// Returns an error if `OKX_ENVIRONMENT` is not `demo` or `live`, if an id
/// is not an OKX swap or X-Perp, if a credential is not set (all checked
/// before any request), if OKX cannot be reached or
/// refuses the key, if the account is not in net position mode, or if an
/// instrument's leverage is not `leverage` (or OKX does not list it).
pub(super) async fn clients(
    instrument_ids: &[InstrumentId],
    leverage: Decimal,
) -> anyhow::Result<OkxClients> {
    let environment = environment_of(std::env::var(ENVIRONMENT_VAR).ok().as_deref())?;
    let clients = configs(instrument_ids, environment)?;
    let account = Account::connect(&clients.exec)?;
    ensure_net_mode(account.position_mode().await?)?;
    for &id in instrument_ids {
        ensure_leverage(id, account.leverage(id).await?, leverage)?;
    }
    Ok(clients)
}

/// The OKX environment `value` of `OKX_ENVIRONMENT` names: `demo` or
/// `live`, exactly, as Nautilus' serde names them. No default: a missing,
/// empty or other value is an error naming both.
fn environment_of(value: Option<&str>) -> anyhow::Result<OKXEnvironment> {
    let refused = || {
        anyhow::anyhow!(
            "{ENVIRONMENT_VAR} must be `demo` or `live` (got {value:?}): set it \
             in .env to the environment of the OKX key, `demo` for a demo \
             trading key, `live` for a real one (see .env.example)"
        )
    };
    let value = value.ok_or_else(refused)?;
    serde_json::from_value(serde_json::Value::from(value)).map_err(|_| refused())
}

/// The client configs of `instrument_ids`, OKX swaps or X-Perps: their
/// instrument types and families are loaded (see `OkxContract`), on
/// `environment` and `REGION`, orders in `MARGIN_MODE`; no credential, so
/// the adapter reads them from the environment.
fn configs(
    instrument_ids: &[InstrumentId],
    environment: OKXEnvironment,
) -> anyhow::Result<OkxClients> {
    let mut instrument_types = Vec::new();
    let mut families = Vec::new();
    for &id in instrument_ids {
        let contract = OkxContract::of(id)?;
        if !instrument_types.contains(&contract.inst_type) {
            instrument_types.push(contract.inst_type);
        }
        let family = contract.family.to_string();
        if !families.contains(&family) {
            families.push(family);
        }
    }
    Ok(OkxClients {
        data: OKXDataClientConfig {
            instrument_types: instrument_types.clone(),
            instrument_families: Some(families.clone()),
            environment,
            region: REGION,
            ..Default::default()
        },
        exec: OKXExecutionClientConfig {
            instrument_types,
            instrument_families: Some(families),
            environment,
            region: REGION,
            margin_mode: Some(MARGIN_MODE),
            ..Default::default()
        },
    })
}

impl OkxClients {
    /// The factories and configs, for the node. OKX is one venue, so
    /// Nautilus routes it to these clients without a route of ours.
    pub(super) fn into_node_clients(self) -> Clients {
        let data_config: Box<dyn ClientConfig> = Box::new(self.data);
        let exec_config: Box<dyn ClientConfig> = Box::new(self.exec);
        let data_factory: Box<dyn DataClientFactory> = Box::new(OKXDataClientFactory::new());
        let exec_factory: Box<dyn ExecutionClientFactory> =
            Box::new(OKXExecutionClientFactory::new());
        Clients {
            data_factory,
            data_config,
            exec_factory,
            exec_config,
            routing: RoutingConfig::default(),
        }
    }
}

/// Fails unless the account is in net position mode: one position per
/// instrument, as Nautilus' netting and the strategy's target book assume.
fn ensure_net_mode(mode: OKXPositionMode) -> anyhow::Result<()> {
    ensure!(
        mode == OKXPositionMode::NetMode,
        "the OKX account is in {mode:?}, not in net mode: set it to one-way \
         (Settings, Trading preferences, Position mode: One-way) with no open \
         position or order, then start again"
    );
    Ok(())
}

/// Fails unless `account`, the leverage OKX applies to `id`, is `wanted`.
fn ensure_leverage(id: InstrumentId, account: Decimal, wanted: Decimal) -> anyhow::Result<()> {
    ensure!(
        account == wanted,
        "the OKX account trades {id} at leverage {account}, not {wanted}: \
         set {wanted}x for {} in cross margin on OKX, or run with --leverage {account}",
        id.symbol
    );
    Ok(())
}

/// The OKX account of the clients, read before the node starts.
struct Account {
    /// The adapter's client, for what it reads.
    okx: OKXHttpClient,
    /// Ours, for the leverage the adapter does not read.
    http: HttpClient,
    base_url: String,
    credential: Credential,
}

impl Account {
    /// Clients on the endpoints of `exec`, with the credentials from the
    /// environment.
    fn connect(exec: &OKXExecutionClientConfig) -> anyhow::Result<Self> {
        let credential = Credential::resolve(None, None, None).context(
            "OKX credentials missing: set OKX_API_KEY, OKX_API_SECRET and \
             OKX_API_PASSPHRASE (see .env.example)",
        )?;
        let base_url = exec.http_base_url();
        let okx = OKXHttpClient::with_credentials(
            None,
            None,
            None,
            Some(base_url.clone()),
            exec.http_timeout_secs,
            exec.max_retries,
            exec.retry_delay_initial_ms,
            exec.retry_delay_max_ms,
            exec.environment,
            None,
        )
        .context("building the OKX HTTP client")?;
        let mut headers = HashMap::new();
        if exec.environment == OKXEnvironment::Demo {
            headers.insert("x-simulated-trading".to_string(), "1".to_string());
        }
        let http = HttpClient::builder()
            .headers(headers)
            .redirect_policy(HttpRedirectPolicy::Reject)
            .timeout_secs(REQUEST_TIMEOUT_SECS)
            .build()
            .context("building the OKX account client")?;
        Ok(Self {
            okx,
            http,
            base_url,
            credential,
        })
    }

    /// The account's position mode.
    async fn position_mode(&self) -> anyhow::Result<OKXPositionMode> {
        let configs = self
            .okx
            .raw_client()
            .get_account_configuration()
            .await
            .context("reading the OKX account configuration")?;
        let config = configs
            .first()
            .context("OKX returned no account configuration")?;
        Ok(config.position_mode)
    }

    /// The leverage OKX applies to `id` in `MARGIN_MODE`.
    async fn leverage(&self, id: InstrumentId) -> anyhow::Result<Decimal> {
        let mode = serde_json::to_value(MARGIN_MODE)?;
        let mode = mode.as_str().context("a margin mode is a string")?;
        let path = format!(
            "/api/v5/account/leverage-info?instId={}&mgnMode={mode}",
            id.symbol
        );
        // As the adapter signs: an ISO 8601 time to the millisecond.
        let timestamp = format!(
            "{:.3}",
            get_atomic_clock_realtime().get_time_ns().to_datetime_utc()
        );
        let signature = self.credential.sign(&timestamp, "GET", &path, "");
        let headers = HashMap::from([
            (
                "OK-ACCESS-KEY".to_string(),
                self.credential.api_key().to_string(),
            ),
            (
                "OK-ACCESS-PASSPHRASE".to_string(),
                self.credential.api_passphrase().to_string(),
            ),
            ("OK-ACCESS-TIMESTAMP".to_string(), timestamp),
            ("OK-ACCESS-SIGN".to_string(), signature),
        ]);
        let response = self
            .http
            .get(
                format!("{}{path}", self.base_url),
                None,
                Some(headers),
                None,
                None,
            )
            .await
            .with_context(|| format!("asking OKX the leverage of {id}"))?;
        ensure!(
            response.status.is_success(),
            "OKX answered HTTP {} for the leverage of {id}",
            response.status.as_u16()
        );
        leverage_of(id, &response.body)
    }
}

/// OKX's answer to a leverage request.
#[derive(Debug, Deserialize)]
struct LeverageAnswer {
    code: String,
    msg: String,
    data: Vec<LeverageInfo>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LeverageInfo {
    inst_id: String,
    lever: Decimal,
}

/// The leverage of `id` in `body`, OKX's answer to a leverage request: the
/// one of all its rows (one in net mode); an error if OKX refused the
/// request, listed another instrument or none, or rows disagree.
fn leverage_of(id: InstrumentId, body: &[u8]) -> anyhow::Result<Decimal> {
    let answer: LeverageAnswer =
        serde_json::from_slice(body).with_context(|| format!("parsing OKX's leverage of {id}"))?;
    ensure!(
        answer.code == "0",
        "OKX refused the leverage of {id}: code {}, {}",
        answer.code,
        answer.msg
    );
    let symbol = id.symbol.as_str();
    let mut levers = answer.data.iter().map(|info| {
        ensure!(
            info.inst_id == symbol,
            "OKX answered the leverage of {} for {id}",
            info.inst_id
        );
        Ok(info.lever)
    });
    let lever = levers
        .next()
        .with_context(|| format!("OKX listed no leverage for {id}"))??;
    for other in levers {
        let other = other?;
        ensure!(
            other == lever,
            "OKX lists leverages {lever} and {other} for {id}"
        );
    }
    Ok(lever)
}

#[cfg(test)]
mod tests {
    use nautilus_okx::{OKXInstrumentType, common::urls::get_ws_base_url_public};

    use super::*;

    fn xperp() -> InstrumentId {
        InstrumentId::from("BTC-USD_UM_XPERP-310328.OKX")
    }

    fn swap() -> InstrumentId {
        InstrumentId::from("BTC-USDT-SWAP.OKX")
    }

    #[test]
    fn the_environment_is_demo_or_live_by_its_lowercase_name() {
        assert_eq!(environment_of(Some("demo")).unwrap(), OKXEnvironment::Demo);
        assert_eq!(environment_of(Some("live")).unwrap(), OKXEnvironment::Live);
    }

    #[test]
    fn a_missing_empty_or_other_environment_is_refused_naming_the_variable_and_values() {
        for value in [None, Some(""), Some("Demo"), Some("real"), Some(" demo")] {
            let err = environment_of(value).unwrap_err().to_string();
            assert!(err.contains("OKX_ENVIRONMENT"), "{value:?}: {err}");
            assert!(err.contains("`demo` or `live`"), "{value:?}: {err}");
        }
    }

    #[test]
    fn both_clients_trade_on_the_eea_demo_never_the_real_account() {
        let clients = configs(&[xperp()], OKXEnvironment::Demo).unwrap();
        assert_eq!(
            (clients.data.environment, clients.data.region),
            (OKXEnvironment::Demo, OKXRegion::Eea)
        );
        assert_eq!(
            (clients.exec.environment, clients.exec.region),
            (OKXEnvironment::Demo, OKXRegion::Eea)
        );
        assert_eq!(clients.exec.http_base_url(), "https://eea.okx.com");
        assert_eq!(
            clients.data.ws_public_url(),
            get_ws_base_url_public(OKXRegion::Eea, OKXEnvironment::Demo)
        );
        assert_eq!(
            clients.exec.ws_private_url(),
            "wss://wseeapap.okx.com/ws/v5/private"
        );
    }

    #[test]
    fn orders_are_in_cross_margin() {
        let exec = configs(&[xperp()], OKXEnvironment::Demo).unwrap().exec;
        assert_eq!(exec.margin_mode, Some(OKXMarginMode::Cross));
    }

    #[test]
    fn the_credentials_are_left_to_the_adapter_from_the_environment() {
        let clients = configs(&[xperp()], OKXEnvironment::Demo).unwrap();
        let data = &clients.data;
        let exec = &clients.exec;
        assert!(data.api_key.is_none() && data.api_secret.is_none());
        assert!(data.api_passphrase.is_none());
        assert!(exec.api_key.is_none() && exec.api_secret.is_none());
        assert!(exec.api_passphrase.is_none());
    }

    #[test]
    fn the_clients_load_the_types_and_families_of_the_instruments_each_once() {
        let other_xperp = InstrumentId::from("BTC-USD_UM_XPERP-310404.OKX");
        let clients = configs(&[xperp(), swap(), other_xperp], OKXEnvironment::Demo).unwrap();
        let types = [OKXInstrumentType::Futures, OKXInstrumentType::Swap];
        let families = ["BTC-USD_UM_XPERP".to_string(), "BTC-USDT".to_string()];
        assert_eq!(clients.data.instrument_types, types);
        assert_eq!(
            clients.data.instrument_families.as_deref(),
            Some(&families[..])
        );
        assert_eq!(clients.exec.instrument_types, types);
        assert_eq!(
            clients.exec.instrument_families.as_deref(),
            Some(&families[..])
        );
    }

    #[test]
    fn an_instrument_not_an_okx_swap_or_future_is_refused() {
        for id in ["SXR8.IBIS", "BTC-USDT.OKX"] {
            assert!(
                configs(&[InstrumentId::from(id)], OKXEnvironment::Demo).is_err(),
                "{id}"
            );
        }
    }

    #[test]
    fn a_long_short_account_is_refused_naming_net_mode() {
        ensure_net_mode(OKXPositionMode::NetMode).unwrap();
        let err = ensure_net_mode(OKXPositionMode::LongShortMode)
            .unwrap_err()
            .to_string();
        assert!(err.contains("net mode"), "{err}");
    }

    #[test]
    fn a_leverage_other_than_the_runs_is_refused_naming_both() {
        ensure_leverage(xperp(), Decimal::TEN, Decimal::from(10)).unwrap();
        let err = ensure_leverage(xperp(), Decimal::TEN, Decimal::ONE)
            .unwrap_err()
            .to_string();
        assert!(err.contains("leverage 10, not 1"), "{err}");
        assert!(err.contains("BTC-USD_UM_XPERP-310328"), "{err}");
    }

    #[test]
    fn the_leverage_is_read_from_okxs_answer() {
        let body = br#"{"code":"0","msg":"","data":[{"ccy":"","instId":"BTC-USD_UM_XPERP-310328","lever":"3","mgnMode":"cross","posSide":"net"}]}"#;
        assert_eq!(leverage_of(xperp(), body).unwrap(), Decimal::from(3));
    }

    #[test]
    fn a_refused_empty_or_foreign_leverage_answer_is_an_error() {
        let refused = br#"{"code":"51001","msg":"Instrument ID doesn't exist","data":[]}"#;
        let err = leverage_of(xperp(), refused).unwrap_err().to_string();
        assert!(err.contains("51001"), "{err}");
        let empty = br#"{"code":"0","msg":"","data":[]}"#;
        assert!(leverage_of(xperp(), empty).is_err());
        let foreign = br#"{"code":"0","msg":"","data":[{"instId":"BTC-USDT-SWAP","lever":"3"}]}"#;
        assert!(leverage_of(xperp(), foreign).is_err());
        let split = br#"{"code":"0","msg":"","data":[{"instId":"BTC-USD_UM_XPERP-310328","lever":"3"},{"instId":"BTC-USD_UM_XPERP-310328","lever":"5"}]}"#;
        assert!(leverage_of(xperp(), split).is_err());
    }
}
