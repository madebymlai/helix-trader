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
//! Before the node starts, the account is checked (`account::Account`):
//! net position mode, and each instrument's leverage the one its
//! definition in the catalog holds, as its margin, which backtests run at
//! (see `instrument_from`). This module never changes the account.
//!
//! The strategy trades OKX instruments on bars built from OKX's 1-minute
//! candles (a composite bar type): the data client serves their warmup
//! from OKX's REST candles, up to now, and the live bars from the candle
//! channel. OKX serves no quote history (the data client does not
//! implement `request_quotes`), so bars built from quotes get no warmup.

use std::path::Path;

use anyhow::{Context, ensure};
use nautilus_common::factories::{ClientConfig, DataClientFactory, ExecutionClientFactory};
use nautilus_live::config::RoutingConfig;
use nautilus_model::{identifiers::InstrumentId, instruments::Instrument};
use nautilus_okx::{
    OKXPositionMode,
    common::enums::OKXEnvironment,
    config::{OKXDataClientConfig, OKXExecutionClientConfig},
    factories::{OKXDataClientFactory, OKXExecutionClientFactory},
};
use rust_decimal::Decimal;

use super::{
    account::{Account, ENVIRONMENT_VAR, MARGIN_MODE, REGION, environment_of},
    margin_at, selection,
};
use crate::{
    catalog,
    live::node::{Clients, ExecClient},
};

/// The OKX data and execution client configs of a node trading OKX swaps
/// or X-Perps.
#[derive(Debug, Clone)]
pub(crate) struct OkxClients {
    pub(super) data: OKXDataClientConfig,
    pub(super) exec: OKXExecutionClientConfig,
}

/// Reads the definitions of `instrument_ids` in `catalog` and the
/// environment (see `environment_of`), builds the client configs of the
/// instruments on it (see `configs`), checks the credentials are set, then
/// checks the account: net position mode, and each instrument's leverage
/// the one its definition holds as its margin, which backtests run at.
///
/// # Errors
///
/// Returns an error if the catalog cannot be read or lacks an
/// instrument's definition, if `OKX_ENVIRONMENT` is not `demo` or `live`,
/// if an id is not an OKX swap or X-Perp, if a credential is not set (all
/// checked before any request), if OKX cannot be reached or refuses the
/// key, if the account is not in net position mode, or if an instrument's
/// leverage is not its definition's (or OKX does not list it).
pub(crate) async fn clients(
    instrument_ids: &[InstrumentId],
    catalog: &Path,
) -> anyhow::Result<OkxClients> {
    let defined = catalog::traded_instruments(catalog, instrument_ids).context(
        "an OKX instrument's definition in the catalog holds the account's margin, which \
         backtests run at: fill the catalog before trading it live",
    )?;
    let environment = environment_of(std::env::var(ENVIRONMENT_VAR).ok().as_deref())?;
    let clients = configs(instrument_ids, environment)?;
    let account = Account::connect(&clients.exec)?;
    ensure_net_mode(account.position_mode().await?)?;
    for instrument in &defined {
        let id = instrument.id();
        ensure_leverage(id, account.leverage(id).await?, instrument.margin_init())?;
    }
    Ok(clients)
}

/// The client configs of `instrument_ids`, OKX swaps or X-Perps: their
/// instrument types and families are loaded (see `selection`), on
/// `environment` and `REGION`, orders in `MARGIN_MODE`; no credential, so
/// the adapter reads them from the environment.
fn configs(
    instrument_ids: &[InstrumentId],
    environment: OKXEnvironment,
) -> anyhow::Result<OkxClients> {
    let (instrument_types, families) = selection(instrument_ids)?;
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
    pub(crate) fn into_node_clients(self) -> Clients {
        let data_config: Box<dyn ClientConfig> = Box::new(self.data);
        let exec_config: Box<dyn ClientConfig> = Box::new(self.exec);
        let data_factory: Box<dyn DataClientFactory> = Box::new(OKXDataClientFactory::new());
        let exec_factory: Box<dyn ExecutionClientFactory> =
            Box::new(OKXExecutionClientFactory::new());
        Clients {
            data_factory,
            data_config,
            data_routing: RoutingConfig::default(),
            exec: Some(ExecClient {
                factory: exec_factory,
                config: exec_config,
                routing: RoutingConfig::default(),
            }),
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

/// Fails unless `margin`, the margin of `id` its definition in the catalog
/// holds, is the account's at `leverage`, the leverage OKX applies to `id`
/// (`margin_at`): the catalog was filled at another, which backtests ran
/// at.
fn ensure_leverage(id: InstrumentId, leverage: Decimal, margin: Decimal) -> anyhow::Result<()> {
    if margin_at(leverage)? == margin {
        return Ok(());
    }
    let defined = (Decimal::ONE / margin).round_dp(6).normalize();
    anyhow::bail!(
        "the OKX account trades {id} at leverage {leverage}, but the catalog defines it at \
         leverage {defined}, which backtests run at: set {defined}x for {} in cross margin \
         on OKX, or fill a fresh catalog at {leverage}x",
        id.symbol
    )
}

#[cfg(test)]
mod tests {
    use nautilus_okx::{
        OKXInstrumentType,
        common::enums::{OKXMarginMode, OKXRegion},
    };

    use super::*;
    use crate::venues::{Broker, tests::created_oms_type};

    fn xperp() -> InstrumentId {
        InstrumentId::from("BTC-USD_UM_XPERP-310328.OKX")
    }

    fn swap() -> InstrumentId {
        InstrumentId::from("BTC-USDT-SWAP.OKX")
    }

    /// Against the adapter's client: the broker's answer is our copy of it.
    /// Its answer follows the instrument types the config loads, so these
    /// are the configs of a node. The factory builds no client without
    /// credentials: made up here, so that the environment's are never read;
    /// the client connects to nothing.
    #[test]
    fn the_execution_client_keeps_positions_as_the_broker_answers() {
        for ids in [vec![swap()], vec![xperp()], vec![xperp(), swap()]] {
            let mut clients = configs(&ids, OKXEnvironment::Demo).unwrap();
            clients.exec.api_key = Some("key".into());
            clients.exec.api_secret = Some("secret".into());
            clients.exec.api_passphrase = Some("passphrase".into());

            let created = created_oms_type(clients.into_node_clients());

            assert_eq!(created, Broker::Okx.oms_type(), "{ids:?}");
        }
    }

    #[test]
    fn both_clients_trade_in_the_eea_on_the_given_environment() {
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
            clients.exec.ws_private_url(),
            "wss://wseeapap.okx.com/ws/v5/private"
        );
        let live = configs(&[xperp()], OKXEnvironment::Live).unwrap();
        assert_eq!(
            (live.data.environment, live.data.region),
            (OKXEnvironment::Live, OKXRegion::Eea)
        );
        assert_eq!(
            (live.exec.environment, live.exec.region),
            (OKXEnvironment::Live, OKXRegion::Eea)
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

    /// The catalog's definition holds a margin of 0.2, leverage 5, which
    /// backtests ran at: an account at leverage 10 is refused, naming both.
    #[test]
    fn a_leverage_other_than_the_catalogs_is_refused_naming_both() {
        ensure_leverage(xperp(), Decimal::TEN, Decimal::new(1, 1)).unwrap();
        let err = ensure_leverage(xperp(), Decimal::TEN, Decimal::new(2, 1))
            .unwrap_err()
            .to_string();
        assert!(err.contains("at leverage 10"), "{err}");
        assert!(err.contains("at leverage 5,"), "{err}");
        assert!(err.contains("BTC-USD_UM_XPERP-310328"), "{err}");
    }
}
