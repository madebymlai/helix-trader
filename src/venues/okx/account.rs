//! The OKX account, read through its key before anything trades on it or
//! is filled for it: its position mode through the adapter
//! (`get_account_configuration`), and the leverage OKX applies to an
//! instrument, which the adapter does not read, so that is our signed `GET
//! /api/v5/account/leverage-info`, signed with the adapter's `Credential`;
//! to be removed if Nautilus gains it. OKX sets the leverage, by hand on the
//! account; nothing here changes the account. The key is the adapter's,
//! from `OKX_API_KEY`, `OKX_API_SECRET` and `OKX_API_PASSPHRASE`, on the
//! environment `OKX_ENVIRONMENT` names: `demo` or `live`, required, read by
//! us since the adapter takes it only as a config field.

use std::collections::HashMap;

use anyhow::{Context, ensure};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::identifiers::InstrumentId;
use nautilus_network::http::{HttpClient, HttpRedirectPolicy};
use nautilus_okx::{
    OKXPositionMode,
    common::{
        credential::Credential,
        enums::{OKXEnvironment, OKXMarginMode, OKXRegion},
    },
    config::OKXExecutionClientConfig,
    http::OKXHttpClient,
};
use rust_decimal::Decimal;
use serde::Deserialize;

/// The variable naming the OKX environment of the run, `demo` or `live`.
pub(super) const ENVIRONMENT_VAR: &str = "OKX_ENVIRONMENT";

/// The region of an account opened on my.okx.com, which trades X-Perps.
pub(super) const REGION: OKXRegion = OKXRegion::Eea;

/// Cross margin: the account's whole balance backs every position.
pub(super) const MARGIN_MODE: OKXMarginMode = OKXMarginMode::Cross;

/// The OKX environment `value` of `OKX_ENVIRONMENT` names: `demo` or
/// `live`, exactly, as Nautilus' serde names them. No default: a missing,
/// empty or other value is an error naming both.
pub(super) fn environment_of(value: Option<&str>) -> anyhow::Result<OKXEnvironment> {
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

/// The OKX account, read through its key: what the adapter does not read
/// of it, its leverage, and its position mode.
pub(super) struct Account {
    /// The adapter's client, for what it reads.
    okx: OKXHttpClient,
    /// Ours, for the leverage the adapter does not read.
    http: HttpClient,
    base_url: String,
    credential: Credential,
}

impl Account {
    /// The account of the environment `OKX_ENVIRONMENT` names, in
    /// `REGION`, as the live node's clients reach it, with the credentials
    /// from the environment.
    ///
    /// # Errors
    ///
    /// As `environment_of` and `connect`.
    pub(super) fn of_environment() -> anyhow::Result<Self> {
        let environment = environment_of(std::env::var(ENVIRONMENT_VAR).ok().as_deref())?;
        Self::connect(&OKXExecutionClientConfig {
            environment,
            region: REGION,
            margin_mode: Some(MARGIN_MODE),
            ..Default::default()
        })
    }

    /// Clients on the endpoints of `exec`, with the credentials from the
    /// environment.
    pub(super) fn connect(exec: &OKXExecutionClientConfig) -> anyhow::Result<Self> {
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
            .timeout_secs(exec.http_timeout_secs)
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
    pub(super) async fn position_mode(&self) -> anyhow::Result<OKXPositionMode> {
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
    pub(super) async fn leverage(&self, id: InstrumentId) -> anyhow::Result<Decimal> {
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
    use super::*;

    fn xperp() -> InstrumentId {
        InstrumentId::from("BTC-USD_UM_XPERP-310328.OKX")
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
    fn the_leverage_is_read_from_okxs_answer() {
        let body = br#"{"code":"0","msg":"","data":[{"ccy":"","instId":"BTC-USD_UM_XPERP-310328","lever":"3","mgnMode":"cross","posSide":"net"}]}"#;
        assert_eq!(leverage_of(xperp(), body).unwrap(), Decimal::from(3));
    }

    #[test]
    fn a_refused_empty_or_foreign_leverage_answer_is_an_error() {
        let error = |body: &[u8]| leverage_of(xperp(), body).unwrap_err().to_string();
        let refused = br#"{"code":"51001","msg":"Instrument ID doesn't exist","data":[]}"#;
        let err = error(refused);
        assert!(err.contains("51001"), "{err}");
        let empty = br#"{"code":"0","msg":"","data":[]}"#;
        let err = error(empty);
        assert!(err.contains("no leverage"), "{err}");
        let foreign = br#"{"code":"0","msg":"","data":[{"ccy":"","instId":"BTC-USDT-SWAP","lever":"3","mgnMode":"cross","posSide":"net"}]}"#;
        let err = error(foreign);
        assert!(err.contains("BTC-USDT-SWAP"), "{err}");
        let split = br#"{"code":"0","msg":"","data":[{"ccy":"","instId":"BTC-USD_UM_XPERP-310328","lever":"3","mgnMode":"cross","posSide":"net"},{"ccy":"","instId":"BTC-USD_UM_XPERP-310328","lever":"5","mgnMode":"cross","posSide":"net"}]}"#;
        let err = error(split);
        assert!(err.contains("3 and 5"), "{err}");
    }
}
