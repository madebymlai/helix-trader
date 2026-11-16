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
//! implement `request_quotes`), so bars built from quotes get no warmup,
//! which requests no series a venue's history does not serve
//! (`venues::serves`), and start at the live quotes. A live start refuses
//! a series the data client does not stream (`streams`); it streams an
//! instrument's book in OKX's format (`BOOK`), which a run subscribes to.

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

/// OKX's book as Nautilus' OKX data client streams it
/// (`venues::book_format`): its deltas of an L2_MBP book, OKX's price
/// levels, the client failing a subscription of any other book type
/// (`subscribe_book_deltas`, `data.rs:2136-2142`), and its depth snapshots
/// of five levels a side, OKX's `books5`, the most the client subscribes
/// to, failing more (`subscribe_book_depth`, `:2159-2172`,
/// `BOOK_SNAPSHOT_DEPTH`, `:112`). A deltas subscription of no depth takes
/// OKX's book 400 levels deep, the client keeping no depth as 0
/// (`book_channel`, `:354-382`; `resolve_book_depth`,
/// `common/consts.rs:525-531`), which subscribes OKX's `books` channel
/// (`select_book_channel`, `:533-540`).
pub(in crate::venues) const BOOK: BookFormat = BookFormat {
    deltas: BookType::L2_MBP,
    depth: NonZeroUsize::new(5),
};

/// Checks that Nautilus' OKX data client, the live node's, streams
/// `source`, an OKX instrument's series, as a run subscribes to it: its
/// quotes (`subscribe_quotes`, `data.rs:2214`), its trades
/// (`subscribe_trades`, `:2244`) and its candles (`subscribe_bars`,
/// `:2314`, whose task subscribes the channel of
/// `bar_spec_as_okx_channel`, `websocket/client.rs:1709-1715`, failing any
/// other bar type: see `is_candle`); and its book, which a run subscribes
/// to in OKX's format (`BOOK`, through `venues::book_format`), its deltas
/// of an L2_MBP book and its depth five levels a side.
///
/// # Errors
///
/// Returns an error if the client does not stream `source`, naming it and
/// saying why.
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
    anyhow::ensure!(
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
    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::{
        cache::Cache,
        clients::DataClient,
        clock::VirtualClock,
        live::runner::replace_data_event_sender,
        messages::data::{SubscribeBookDeltas, SubscribeBookDepth},
    };
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_okx::{
        OKXInstrumentType,
        common::enums::{OKXMarginMode, OKXRegion},
    };
    use rstest::rstest;

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
    #[rstest]
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

    /// The node's OKX data client of a swap, created as the node creates
    /// it, unconnected.
    fn data_client() -> Box<dyn DataClient> {
        let (data_tx, _data_events) = tokio::sync::mpsc::unbounded_channel();
        replace_data_event_sender(data_tx);
        let clients = configs(&[swap()], OKXEnvironment::Demo)
            .unwrap()
            .into_node_clients();
        clients
            .data_factory
            .create(
                clients.data_factory.name(),
                clients.data_config.as_ref(),
                Rc::new(RefCell::new(Cache::default())).into(),
                Rc::new(RefCell::new(VirtualClock::new())),
            )
            .unwrap()
    }

    /// Against the adapter's data client: unconnected, it takes a swap's
    /// book subscribed in OKX's format (`BOOK`), its deltas and its depth,
    /// and fails a subscription of deltas of another book type or of depth
    /// a level deeper before anything connects, so `BOOK` is the one
    /// format it streams, its depth the deepest. Each subscription on a
    /// client of its own.
    #[tokio::test]
    async fn the_data_client_takes_a_book_in_okx_s_format_alone() {
        let (venue, ts) = (Some(swap().venue), UnixNanos::default());
        let deltas = |book_type| {
            data_client().subscribe_book_deltas(SubscribeBookDeltas::new(
                swap(),
                book_type,
                None,
                venue,
                UUID4::new(),
                ts,
                None,
                false,
                None,
                None,
            ))
        };
        let depth = |levels| {
            data_client().subscribe_book_depth(SubscribeBookDepth::new(
                swap(),
                BookType::L2_MBP,
                None,
                venue,
                UUID4::new(),
                ts,
                levels,
                false,
                None,
                None,
            ))
        };

        deltas(BOOK.deltas).unwrap();
        depth(BOOK.depth).unwrap();
        assert!(deltas(BookType::L3_MBO).is_err());
        assert!(depth(BOOK.depth.map(|levels| levels.saturating_add(1))).is_err());
    }

    #[rstest]
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
    fn an_instrument_not_an_okx_swap_or_future_is_refused() {
        for id in ["SXR8.IBIS", "BTC-USDT.OKX"] {
            assert!(
                configs(&[InstrumentId::from(id)], OKXEnvironment::Demo).is_err(),
                "{id}"
            );
        }
    }

    #[rstest]
    fn a_long_short_account_is_refused_naming_net_mode() {
        ensure_net_mode(OKXPositionMode::NetMode).unwrap();
        let err = ensure_net_mode(OKXPositionMode::LongShortMode)
            .unwrap_err()
            .to_string();
        assert!(err.contains("net mode"), "{err}");
    }

    /// The catalog's definition holds a margin of 0.2, leverage 5, which
    /// backtests ran at: an account at leverage 10 is refused, naming both.
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
