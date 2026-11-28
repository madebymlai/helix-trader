//! OKX clients for the live node.
//!
//! Builds client factories and configs from `nautilus_okx`. They use the EEA region (`eea.okx.com`,
//! where an EEA account trades X-Perps) and cross margin. They run on the environment
//! `OKX_ENVIRONMENT` names. The adapter reads the credentials. This module only checks they are
//! set.
//!
//! Before the node starts, this module checks the account (`account::Account`). The account must be
//! in net position mode. Each instrument's leverage must match the one its catalog definition
//! holds as margin (see `instrument_from`). This module never changes the account.
//!
//! The data client does not implement `request_quotes`, so bars built from quotes get no warmup
//! (`venues::serves`) and start at the live quotes. A live start refuses a series the client does
//! not stream (`streams`).

use std::{num::NonZeroUsize, path::Path};

use anyhow::Context;
use nautilus_common::factories::{ClientConfig, DataClientFactory, ExecutionClientFactory};
use nautilus_live::config::RoutingConfig;
use nautilus_model::{
    data::NautilusDataType, enums::BookType, identifiers::InstrumentId, instruments::Instrument,
};
use nautilus_okx::{
    OKXPositionMode,
    common::enums::OKXEnvironment,
    config::{OKXDataClientConfig, OKXExecutionClientConfig},
    factories::{OKXDataClientFactory, OKXExecutionClientFactory},
};
use rust_decimal::Decimal;

use super::{
    account::{Account, ENVIRONMENT_VAR, MARGIN_MODE, REGION, environment_of},
    is_candle, margin_at, selection,
};
use crate::{
    catalog,
    live::node::{Clients, ExecClient},
    strategy::requirements::{CatalogSeries, TickSeries},
    venues::BookFormat,
};

/// The book format the OKX data client streams (`venues::book_format`).
///
/// - Deltas are L2_MBP. Any other book type fails (`subscribe_book_deltas`, `data.rs:2136-2142`).
/// - Depth snapshots hold at most five levels a side, on `books5` (`subscribe_book_depth`,
///   `:2159-2172`, `BOOK_SNAPSHOT_DEPTH`, `:112`).
/// - Deltas with no depth use the 400-level `books` channel (`book_channel`, `:354-382`;
///   `resolve_book_depth`, `common/consts.rs:525-531`; `select_book_channel`, `:533-540`).
pub(in crate::venues) const BOOK: BookFormat = BookFormat {
    deltas: BookType::L2_MBP,
    depth: NonZeroUsize::new(5),
};

/// Checks that the OKX data client streams `source`.
///
/// The client streams:
/// - Quotes (`subscribe_quotes`, `data.rs:2214`).
/// - Trades (`subscribe_trades`, `:2244`).
/// - Candles (`subscribe_bars`, `:2314`). It subscribes through `bar_spec_as_okx_channel`
///   (`websocket/client.rs:1709-1715`), which fails other bar types: see `is_candle`.
/// - The book in `BOOK` format.
///
/// # Errors
///
/// Returns an error if the client does not stream `source`.
pub(crate) fn streams(source: &CatalogSeries) -> anyhow::Result<()> {
    use NautilusDataType as D;

    match source {
        CatalogSeries::Bars(bar_type) => anyhow::ensure!(
            is_candle(bar_type),
            "OKX streams its candles, EXTERNAL bars of the LAST price 1, 3, 5, 15 or 30 minutes, \
             1, 2, 4, 6 or 12 hours, 1, 2, 3 or 5 days, a week or 1, 3, 6 or 12 months wide, or \
             1 second, its quotes, its trades and its book, not {bar_type}"
        ),
        CatalogSeries::Ticks(TickSeries {
            data: D::QuoteTick | D::TradeTick | D::OrderBookDelta | D::OrderBookDepth,
            ..
        }) => {}
        CatalogSeries::Ticks(ticks) => {
            anyhow::bail!(
                "OKX streams its quotes, its trades, its candles and its book, not the {ticks}"
            )
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) struct OkxClients {
    pub(super) data: OKXDataClientConfig,
    pub(super) exec: OKXExecutionClientConfig,
}

/// Builds the client configs for `instrument_ids`, checks the credentials are set, then checks the
/// account.
///
/// # Errors
///
/// Returns an error if:
/// - The catalog cannot be read or lacks an instrument's definition.
/// - `OKX_ENVIRONMENT` is not `demo` or `live`.
/// - An id is not an OKX swap or X-Perp.
/// - A credential is not set.
/// - OKX cannot be reached or refuses the key.
/// - The account is not in net position mode.
/// - An instrument's leverage differs from its definition's, or OKX does not list it.
pub(crate) async fn clients(
    instrument_ids: &[InstrumentId],
    catalog: &Path,
) -> anyhow::Result<OkxClients> {
    let defined = catalog::instruments(catalog, instrument_ids).context(
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
    /// Returns the node clients. OKX is one venue, so Nautilus routes to them without our route.
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

/// Checks the account is in net mode, which Nautilus' netting and the target book assume.
fn ensure_net_mode(mode: OKXPositionMode) -> anyhow::Result<()> {
    anyhow::ensure!(
        mode == OKXPositionMode::NetMode,
        "the OKX account is in {mode:?}, not in net mode: set it to one-way \
         (Settings, Trading preferences, Position mode: One-way) with no open \
         position or order, then start again"
    );
    Ok(())
}

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
    use rstest::rstest;

    use super::*;

    fn xperp() -> InstrumentId {
        InstrumentId::from("BTC-USD_UM_XPERP-310328.OKX")
    }

    fn swap() -> InstrumentId {
        InstrumentId::from("BTC-USDT-SWAP.OKX")
    }

    #[rstest]
    #[case::demo(OKXEnvironment::Demo)]
    #[case::live(OKXEnvironment::Live)]
    fn both_clients_trade_in_the_eea_on_the_given_environment(#[case] environment: OKXEnvironment) {
        let clients = configs(&[xperp()], environment).unwrap();
        assert_eq!(
            (clients.data.environment, clients.data.region),
            (environment, OKXRegion::Eea)
        );
        assert_eq!(
            (clients.exec.environment, clients.exec.region),
            (environment, OKXRegion::Eea)
        );
    }

    #[rstest]
    fn orders_are_in_cross_margin() {
        let exec = configs(&[xperp()], OKXEnvironment::Demo).unwrap().exec;
        assert_eq!(exec.margin_mode, Some(OKXMarginMode::Cross));
    }

    #[rstest]
    fn the_credentials_are_left_to_the_adapter_from_the_environment() {
        let clients = configs(&[xperp()], OKXEnvironment::Demo).unwrap();
        let data = &clients.data;
        let exec = &clients.exec;
        assert!(data.api_key.is_none() && data.api_secret.is_none());
        assert!(data.api_passphrase.is_none());
        assert!(exec.api_key.is_none() && exec.api_secret.is_none());
        assert!(exec.api_passphrase.is_none());
    }

    #[rstest]
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

    #[rstest]
    fn a_long_short_account_is_refused_naming_net_mode() {
        ensure_net_mode(OKXPositionMode::NetMode).unwrap();
        let err = ensure_net_mode(OKXPositionMode::LongShortMode)
            .unwrap_err()
            .to_string();
        assert!(err.contains("LongShortMode"), "{err}");
    }

    /// The catalog margin 0.2 means leverage 5, so an account at leverage 10 is refused.
    #[rstest]
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
