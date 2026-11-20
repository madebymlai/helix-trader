//! Serves LSE history and live data through the LSE adapter's data client (`lse`).
//!
//! The node fills history (see `catalog::NodeFill`). The data engine writes the client's answers
//! back to the catalog. The venues are:
//! - `LSEDGE`: London Strategic Edge's derived series. Each is an index instrument named after its
//!   LSE market (`GOLD-FUTURES.LSEDGE`: GOLD in `futures`). Trades come from tick slices. Bars come
//!   from candles, each stamped at its close.
//! - `GLBX`: contracts on CME Globex, Levels 1 to 3 under Databento's ids (`GCZ26.GLBX`), as
//!   trades, quotes, book depths and book deltas.
//! - `XNAS`: Nasdaq TotalView stocks, Levels 1 to 3 under Databento's ids (`NVDA.XNAS`), with the
//!   same data.
//! - `OPRA`: US options under Databento's ids, which are their OSI symbols
//!   (`AAPL 261120C00355000.OPRA`), as trades and bars like an `LSEDGE` series.
//!
//! Live, the client streams the following, through one subscription for each underlying's chain:
//! - Quotes for `LSEDGE` instruments.
//! - Levels 1 to 3 for `GLBX` contracts and `XNAS` stocks (quotes, trades, book depths and book
//!   deltas).
//! - Trades for `OPRA` contracts.
//!
//! The live node routes the client to those venues (`live_clients`). IB's execution client takes
//! the orders of `GLBX`, `XNAS` and `OPRA`.
//!
//! A contract's definition comes from the definitions source of its row. For `LSEDGE`, the source
//! is the catalog alone, written by hand, because LSE publishes none. The data engine answers a
//! request for it from the catalog before asking the client. For the other venues, the source is
//! Databento, asked through Nautilus' Databento client. The client converts LSE's prices at the
//! definition's precision.
//!
//! LSE's reference series are custom series (`lse::LseRow`): reference collections, macro
//! releases, yields, and an instrument's splits and dividends. The same client fills them with no
//! definition. Each row is stamped when its values are known. See `README.md`.

use std::num::NonZeroUsize;

use ::lse::{LseDataClientConfig, LseDataClientFactory};
use jiff::SignedDuration;
use nautilus_live::config::RoutingConfig;
use nautilus_model::{data::DEPTH10_LEN, enums::BookType, identifiers::InstrumentId};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

use super::{BookFormat, DefinitionsSource, Row, databento::DatabentoHistory};
use crate::{
    catalog::{FillData, NodeClient, NodeFill, missing, missing_custom},
    live::node::Clients,
    strategy::requirements::{CustomSlice, SeriesSlice},
};

// The client asks LSE for one UTC day per export, whatever the window
const DAY: SignedDuration = SignedDuration::from_hours(24);

/// The book format the adapter's data client streams and serves (`venues::book_format`).
///
/// Deltas come from an L3_MBO book, with orders at Level 3 by id. The book type a subscription
/// names does not matter (`subscribe_book_deltas`, `crates/lse/src/data.rs:601-605`, reads none;
/// `request_book_deltas`, `:781`). Depth snapshots are ten levels a side, Level 2, the only depth
/// the client serves. Any other depth fails, naming it (`subscribe_book_depth`, `:556-566`;
/// `request_book_depth`, `:739-746`).
pub(super) const BOOK: BookFormat = BookFormat {
    deltas: BookType::L3_MBO,
    depth: NonZeroUsize::new(DEPTH10_LEN),
};

/// Adds to `node` the fill for what the catalog lacks of `slices` (see `catalog::missing`) and of
/// `custom`, LSE's row series (see `catalog::missing_custom`).
///
/// Each contract's definition comes first, from the definitions source of its row.
/// - Databento's client is made by `databento`, and only if Databento has a definition to give. It
///   is asked up to the end of its dataset (`DatabentoHistory::within_datasets`).
/// - The data engine answers LSE's definitions from the catalog. If one is missing, it fails,
///   naming the instrument, before LSE is asked.
///
/// The series and rows follow. They are asked of the client that `config` configures, which reads
/// its keys only when the node creates it. The data engine caches each definition, and the client
/// reads the price precision from it. A custom series has no instrument.
///
/// # Errors
///
/// Returns an error if:
/// - A series is not one the node fills (see `missing`).
/// - The catalog cannot be read.
/// - `databento` or `DatabentoHistory::within_datasets` fails.
pub(super) async fn add_fill(
    node: &mut NodeFill,
    catalog: &ParquetDataCatalog,
    slices: &[SeriesSlice],
    custom: &[CustomSlice],
    config: LseDataClientConfig,
    databento: impl FnOnce() -> anyhow::Result<DatabentoHistory>,
) -> anyhow::Result<()> {
    let (definitions, requests): (Vec<_>, Vec<_>) =
        missing(catalog, slices)?.into_iter().partition(|request| {
            matches!(request.data, FillData::Instrument(id) if Row::of(id.venue).definitions == DefinitionsSource::Databento)
        });
    let custom = missing_custom(catalog, custom)?;
    if !definitions.is_empty() {
        let databento = databento()?;
        let definitions = databento.within_datasets(&definitions).await?;
        node.add(databento.node_client(), &definitions, |data| {
            unreachable!("Databento is asked definitions alone, not {data}")
        });
    }
    node.add_with_custom(node_client(config), &requests, &custom, period);
    Ok(())
}

fn period(_data: FillData) -> SignedDuration {
    DAY
}

// The client is on no venue, so only requests that name it ask it (see `NodeFill::add`)
fn node_client(config: LseDataClientConfig) -> NodeClient {
    NodeClient {
        factory: Box::new(LseDataClientFactory::new()),
        config: Box::new(config),
    }
}

/// Returns the live node's clients for `instrument_ids`.
///
/// This is the adapter's data client and no execution client, because LSE trades nothing.
///
/// The client reads its keys when the node creates it:
/// - The LSE key for `LSEDGE` and `OPRA`.
/// - The CME key for `GLBX`.
/// - The Nasdaq key for `XNAS`.
///
/// The client is on no venue, so it takes the routes of the instruments' venues
/// (`RoutingConfig::venues`) and is not the default (see `live::node::Clients`). A run can read
/// only custom LSE rows and have no instrument. Then the client takes no route, and the series'
/// subscription names it by client id (`venues::custom_data_client`).
pub(super) fn live_clients(instrument_ids: &[InstrumentId]) -> Clients {
    let mut venues: Vec<String> = Vec::new();
    for id in instrument_ids {
        let venue = id.venue.to_string();
        if !venues.contains(&venue) {
            venues.push(venue);
        }
    }
    Clients {
        data_factory: Box::new(LseDataClientFactory::new()),
        data_config: Box::new(
            LseDataClientConfig::builder()
                .stream_ids(instrument_ids.to_vec())
                .build(),
        ),
        data_routing: RoutingConfig {
            default: false,
            venues: Some(venues),
        },
        exec: None,
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, collections::HashSet, num::NonZeroU32, path::Path, rc::Rc};

    use ::lse::{
        LseRow,
        testing::{
            AAPL_CALL, AAPL_CALL_MINUTES, AAPL_CALL_TRADES, AAPL_FACTORS, AAPL_TRADES,
            ALLOWANCE_EXHAUSTED, API_KEY, BTCUSDT_TRADES, CME_API_KEY, DATA_UNAVAILABLE,
            DATA_UNAVAILABLE_TODAY, DATE_NOT_TRADING, EUR_USD_MINUTES, GCZ26, GCZ26_BOOK,
            GCZ26_ORDERS, GCZ26_QUOTES, GCZ26_TRADES, GOLD_FUTURES, GOLD_SPOT, GOLD_SPOT_MINUTES,
            GOLD_TRADES, MockLse, NASDAQ_API_KEY, NVDA, NVDA_BOOK, NVDA_ORDERS, NVDA_QUOTES,
            NVDA_TRADES, PAIRS_GCZ6, PLAN_REQUIRED, REFERENCE_CATALOGUE,
            REFERENCE_DATA_UNAVAILABLE, ROWS_EXHAUSTED, RY_TRADES, Reply, SPX_MINUTES, SPX_TICKS,
            SYMBOL_UNKNOWN, US_CORE_CPI_CALENDAR, US_CORE_CPI_RELEASES, US10Y_BOND_YIELDS,
            VOD_TRADES, ZN_COT, gold_futures, gold_spot,
        },
    };
    use jiff::Timestamp;
    use nautilus_common::{
        clock::{Clock, VirtualClock},
        config::ConfigResult,
    };
    use nautilus_core::{UnixNanos, string::secret::SecretString};
    use nautilus_model::{
        data::{Bar, BarType, Data, DataType, NautilusDataType, TradeTick},
        enums::{AggressorSide, OptionKind, RecordFlag},
        identifiers::{InstrumentId, StrategyId, Symbol, TradeId},
        instruments::{IndexInstrument, InstrumentAny},
        types::{Currency, Price, Quantity},
    };
    use nautilus_trading::strategy::StrategyConfig;
    use rstest::rstest;

    use super::*;
    use crate::{
        strategy::{
            Part, Series, StandardConfig,
            requirements::{CustomSeries, DataRequirements, TickSeries},
        },
        venues::databento::testing::MockDatabento,
    };

    // LSE lists no such series
    const NOSUCH: &str = "NOSUCH-FUTURES.LSEDGE";

    fn at(time: &str) -> Timestamp {
        format!("2026-10-21T{time}Z").parse().unwrap()
    }

    fn defined_as(id: &str) -> InstrumentAny {
        let InstrumentAny::IndexInstrument(mut index) = gold_futures() else {
            unreachable!("an index");
        };
        index.id = InstrumentId::from(id);
        InstrumentAny::IndexInstrument(index)
    }

    // Fills the trades of `id` over the captured second, `[10:00:00, 10:00:01)`
    fn fill(
        dir: &Path,
        lse: &MockLse,
        id: &str,
        definitions: Vec<InstrumentAny>,
    ) -> anyhow::Result<()> {
        fill_over(
            dir,
            lse,
            id,
            definitions,
            (at("10:00:00"), at("10:00:00.999999999")),
            settled(),
        )
    }

    fn fill_over(
        dir: &Path,
        lse: &MockLse,
        id: &str,
        definitions: Vec<InstrumentAny>,
        (from, to): (Timestamp, Timestamp),
        now: Timestamp,
    ) -> anyhow::Result<()> {
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![TickSeries {
                data: NautilusDataType::TradeTick,
                instrument_id: InstrumentId::from(id),
            }],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        };
        fill_at(dir, lse, None, &requirements, definitions, (from, to), now)
    }

    fn fill_requirements(
        dir: &Path,
        lse: &MockLse,
        requirements: &DataRequirements,
        definitions: Vec<InstrumentAny>,
        from: Timestamp,
        to: Timestamp,
    ) -> anyhow::Result<()> {
        fill_from(dir, lse, None, requirements, definitions, (from, to))
    }

    // Days after every window the tests fill, so LSE has loaded each
    fn settled() -> Timestamp {
        "2026-11-25T12:00:00Z".parse().unwrap()
    }

    fn clock_at(now: Timestamp) -> impl Fn() -> Rc<RefCell<dyn Clock>> + 'static {
        move || {
            let clock = VirtualClock::new();
            clock.set_time(UnixNanos::from(now));
            Rc::new(RefCell::new(clock))
        }
    }

    fn fill_from(
        dir: &Path,
        lse: &MockLse,
        databento: Option<&MockDatabento>,
        requirements: &DataRequirements,
        definitions: Vec<InstrumentAny>,
        window: (Timestamp, Timestamp),
    ) -> anyhow::Result<()> {
        fill_at(
            dir,
            lse,
            databento,
            requirements,
            definitions,
            window,
            settled(),
        )
    }

    // With no Databento peer, Databento is never asked
    fn fill_at(
        dir: &Path,
        lse: &MockLse,
        databento: Option<&MockDatabento>,
        requirements: &DataRequirements,
        definitions: Vec<InstrumentAny>,
        (from, to): (Timestamp, Timestamp),
        now: Timestamp,
    ) -> anyhow::Result<()> {
        crate::venues::register_custom_data();
        let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
        if !definitions.is_empty() {
            catalog.write_instruments(definitions).unwrap();
        }
        let slices = requirements.slices(from, to);
        let config = LseDataClientConfig::builder()
            .api_key(SecretString::from(API_KEY))
            .cme_api_key(SecretString::from(CME_API_KEY))
            .nasdaq_api_key(SecretString::from(NASDAQ_API_KEY))
            .base_url_http(lse.url().to_string())
            // The mock answers at once, as in the crate's own tests
            .calls_per_minute(NonZeroU32::new(6000).unwrap())
            .cme_calls_per_minute(NonZeroU32::new(6000).unwrap())
            .nasdaq_calls_per_minute(NonZeroU32::new(6000).unwrap())
            .build();
        let databento = || {
            let peer = databento.expect("Databento asked for a definition");
            DatabentoHistory::with_base_url(&format!("db-{}", "0".repeat(29)), peer.url())
        };
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let mut node = NodeFill::new();
                add_fill(
                    &mut node,
                    &catalog,
                    &slices,
                    &requirements.custom_slices(from, to),
                    config,
                    databento,
                )
                .await?;
                node.run(dir, clock_at(now)).await
            })
    }

    fn asked(lse: &MockLse) -> Vec<(String, String)> {
        lse.calls()
            .iter()
            .map(|call| {
                (
                    call.param("from").unwrap().to_string(),
                    call.param("to").unwrap().to_string(),
                )
            })
            .collect()
    }

    fn stored_trades(dir: &Path) -> Vec<TradeTick> {
        ParquetDataCatalog::new(dir, None, None, None, None)
            .trade_ticks(Some(vec![GOLD_FUTURES.to_string()]), None, None)
            .unwrap()
    }

    /// The captured second, as pyarrow reads LSE's file.
    #[rstest]
    fn the_trades_lse_exports_are_written_back_to_the_catalog() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);

        fill(dir.path(), &lse, GOLD_FUTURES, vec![gold_futures()]).unwrap();

        let stored = stored_trades(dir.path());
        let rows: Vec<_> = stored
            .iter()
            .map(|t| (t.ts_event, t.price, t.size, t.aggressor_side))
            .collect();
        let row = |time, price, size| {
            (
                UnixNanos::from(at(time)),
                Price::from(price),
                Quantity::from(size),
                AggressorSide::NoAggressor,
            )
        };
        assert_eq!(
            rows,
            [
                row("10:00:00.619501", "4136.4", 1),
                row("10:00:00.619501", "4136.4", 1),
                row("10:00:00.690600", "4136.3", 1),
                row("10:00:00.690600", "4136.2", 2),
                row("10:00:00.690600", "4136.1", 2),
                row("10:00:00.901218", "4136.2", 1),
            ]
        );
        let ids: HashSet<_> = stored.iter().map(|t| t.trade_id).collect();
        assert_eq!(ids.len(), stored.len());
        let [export] = &lse.calls()[..] else {
            panic!("{:?}", lse.calls())
        };
        assert_eq!(
            (export.param("from"), export.param("to")),
            (Some("2026-10-21T10:00:00Z"), Some("2026-10-21T10:00:01Z"))
        );
    }

    /// Gold spot's candles, stamped at their open 10:00 to 10:04, as pyarrow reads them.
    #[rstest]
    fn lse_s_candles_are_written_back_to_the_catalog_as_bars_stamped_at_their_close() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![Reply::file(GOLD_SPOT_MINUTES)]);
        let bars = format!("{GOLD_SPOT}-1-MINUTE-LAST-EXTERNAL");
        let requirements = DataRequirements {
            bar_types: vec![BarType::from(bars.as_str())],
            ticks: vec![],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        };

        fill_requirements(
            dir.path(),
            &lse,
            &requirements,
            vec![gold_spot()],
            at("10:01:00"),
            at("10:05:00"),
        )
        .unwrap();

        let stored = ParquetDataCatalog::new(dir.path(), None, None, None, None)
            .bars(Some(vec![bars]), None, None)
            .unwrap();
        let rows: Vec<_> = stored
            .iter()
            .map(|b| (b.ts_event, b.open, b.high, b.low, b.close, b.volume))
            .collect();
        let row = |close, [open, high, low, last]: [&str; 4]| {
            (
                UnixNanos::from(at(close)),
                Price::from(open),
                Price::from(high),
                Price::from(low),
                Price::from(last),
                Quantity::from(0),
            )
        };
        assert_eq!(
            rows,
            [
                row("10:01:00", ["4118.01", "4119.21", "4116.91", "4118.01"]),
                row("10:02:00", ["4118.15", "4118.24", "4116.25", "4116.94"]),
                row("10:03:00", ["4116.97", "4117.27", "4115.27", "4116.17"]),
                row("10:04:00", ["4116.02", "4116.93", "4115.62", "4116.12"]),
                row("10:05:00", ["4116.25", "4118.36", "4116.25", "4118.16"]),
            ]
        );
        let [export] = &lse.calls()[..] else {
            panic!("{:?}", lse.calls())
        };
        assert_eq!(
            (
                export.param("timeframe"),
                export.param("from"),
                export.param("to")
            ),
            (
                Some("1m"),
                Some("2026-10-21T10:00:00Z"),
                Some("2026-10-21T10:05:00Z")
            )
        );
    }

    // An index written by hand, with whole lots as sizes
    fn index(id: &str, raw: &str, precision: u8) -> InstrumentAny {
        InstrumentAny::IndexInstrument(
            IndexInstrument::builder()
                .instrument_id(InstrumentId::from(id))
                .raw_symbol(Symbol::from(raw))
                .currency(Currency::USD())
                .price_precision(precision)
                .size_precision(0)
                .price_increment(Price::new(10f64.powi(-i32::from(precision)), precision))
                .size_increment(Quantity::from(1))
                .ts_event(UnixNanos::default())
                .ts_init(UnixNanos::default())
                .build()
                .unwrap(),
        )
    }

    fn on_the_12th(time: &str) -> Timestamp {
        format!("2026-11-12T{time}Z").parse().unwrap()
    }

    /// The 12 November trade slice of each derived market, as pyarrow reads it.
    ///
    /// Each entry holds the count, total size, first and last times, and lowest and highest prices.
    /// The futures' entries are in `the_trades_lse_exports_are_written_back_to_the_catalog`.
    #[rstest]
    #[case::us_stocks(
        ("AAPL-US_STOCKS.LSEDGE", "us_stocks", "AAPL"), 4, AAPL_TRADES,
        ("15:00:00", "15:00:00.999999999"), (54, 1829), ("15:00:00.00557", "15:00:00.968094"),
        ("358.54", "358.91")
    )]
    #[case::uk_stocks(
        ("VOD-UK_STOCKS.LSEDGE", "uk_stocks", "VOD"), 2, VOD_TRADES,
        ("10:00:00", "10:00:59.999999999"), (9, 33765), ("10:00:12.677", "10:00:35.279"),
        ("117.8", "117.85")
    )]
    #[case::ca_stocks(
        ("RY-CA_STOCKS.LSEDGE", "ca_stocks", "RY"), 2, RY_TRADES,
        ("15:00:00", "15:00:59.999999999"), (23, 3135), ("15:00:05.973", "15:00:36.614"),
        ("288.65", "288.86")
    )]
    #[case::crypto(
        ("BTCUSDT-CRYPTO.LSEDGE", "crypto", "BTCUSDT"), 2, BTCUSDT_TRADES,
        ("10:00:00", "10:00:00.999999999"), (36, 831_000), ("10:00:00.124", "10:00:00.124"),
        ("73496.29", "73497.39")
    )]
    fn the_trades_of_a_series_of_each_derived_market_fill_under_its_lsedge_id(
        #[case] (id, market, symbol): (&str, &str, &str),
        #[case] precision: u8,
        #[case] export: &[u8],
        #[case] window: (&str, &str),
        #[case] (count, total_size): (usize, u64),
        #[case] times: (&str, &str),
        #[case] prices: (&str, &str),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![Reply::file(export)]);
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![TickSeries {
                data: NautilusDataType::TradeTick,
                instrument_id: InstrumentId::from(id),
            }],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        };

        fill_requirements(
            dir.path(),
            &lse,
            &requirements,
            vec![index(id, symbol, precision)],
            on_the_12th(window.0),
            on_the_12th(window.1),
        )
        .unwrap();

        let stored = ParquetDataCatalog::new(dir.path(), None, None, None, None)
            .trade_ticks(Some(vec![id.to_string()]), None, None)
            .unwrap();
        let at = |time| UnixNanos::from(on_the_12th(time));
        assert_eq!(stored.len(), count);
        assert_eq!(
            (stored[0].ts_event, stored[stored.len() - 1].ts_event),
            (at(times.0), at(times.1))
        );
        assert_eq!(
            stored.iter().map(|t| t.size.as_f64()).sum::<f64>(),
            total_size as f64
        );
        let price = |price: &str| Price::from(price).as_f64();
        let (low, high) = stored.iter().fold((f64::MAX, f64::MIN), |(low, high), t| {
            (low.min(t.price.as_f64()), high.max(t.price.as_f64()))
        });
        assert_eq!((low, high), (price(prices.0), price(prices.1)));
        let [export] = &lse.calls()[..] else {
            panic!("{:?}", lse.calls())
        };
        assert_eq!(
            (
                export.param("market"),
                export.param("symbol"),
                export.param("timeframe")
            ),
            (Some(market), Some(symbol), Some("tick"))
        );
    }

    // Close, then open, high, low and last
    type BarRow = (&'static str, [&'static str; 4]);

    /// The 1-minute candles of a forex pair and an index on 12 November, as pyarrow reads them.
    ///
    /// Each has five bars, closing over `[10:01, 10:05]` and `[15:01, 15:05]`. Each bar is stamped
    /// a minute after its row opens and has no volume. LSE stores no trades for forex, and an index
    /// has none.
    #[rstest]
    #[case::a_forex_pair(
        "EUR_USD-FOREX.LSEDGE", "forex", "EUR_USD", 5, EUR_USD_MINUTES, ("10:01:00", "10:05:00"),
        [
            ("10:01:00", ["1.10942", "1.10946", "1.10941", "1.10944"]),
            ("10:02:00", ["1.10942", "1.10944", "1.10933", "1.10938"]),
            ("10:03:00", ["1.10938", "1.10941", "1.10933", "1.10934"]),
            ("10:04:00", ["1.10933", "1.10939", "1.10930", "1.10938"]),
            ("10:05:00", ["1.10936", "1.10941", "1.10930", "1.10930"]),
        ]
    )]
    #[case::an_index(
        "SPX-INDICES.LSEDGE", "indices", "SPX", 2, SPX_MINUTES, ("15:01:00", "15:05:00"),
        [
            ("15:01:00", ["7600.07", "7600.07", "7589.80", "7590.37"]),
            ("15:02:00", ["7590.16", "7593.11", "7587.68", "7587.68"]),
            ("15:03:00", ["7587.39", "7588.30", "7584.93", "7586.79"]),
            ("15:04:00", ["7585.46", "7585.46", "7581.91", "7582.77"]),
            ("15:05:00", ["7583.60", "7590.34", "7582.70", "7588.98"]),
        ]
    )]
    fn the_bars_of_a_forex_pair_and_an_index_fill_under_their_lsedge_ids(
        #[case] id: &str,
        #[case] market: &str,
        #[case] symbol: &str,
        #[case] precision: u8,
        #[case] export: &[u8],
        #[case] window: (&str, &str),
        #[case] rows: [BarRow; 5],
    ) {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![Reply::file(export)]);
        let bars = format!("{id}-1-MINUTE-LAST-EXTERNAL");
        let requirements = DataRequirements {
            bar_types: vec![BarType::from(bars.as_str())],
            ticks: vec![],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        };

        fill_requirements(
            dir.path(),
            &lse,
            &requirements,
            vec![index(id, symbol, precision)],
            on_the_12th(window.0),
            on_the_12th(window.1),
        )
        .unwrap();

        let stored = ParquetDataCatalog::new(dir.path(), None, None, None, None)
            .bars(Some(vec![bars]), None, None)
            .unwrap();
        let row = |b: &Bar| (b.ts_event, b.open, b.high, b.low, b.close, b.volume);
        let expected = |(close, [open, high, low, last]): BarRow| {
            (
                UnixNanos::from(on_the_12th(close)),
                Price::from(open),
                Price::from(high),
                Price::from(low),
                Price::from(last),
                Quantity::from(0),
            )
        };
        assert_eq!(
            stored.iter().map(row).collect::<Vec<_>>(),
            rows.map(expected)
        );
        let [export] = &lse.calls()[..] else {
            panic!("{:?}", lse.calls())
        };
        assert_eq!(
            (
                export.param("market"),
                export.param("symbol"),
                export.param("timeframe")
            ),
            (Some(market), Some(symbol), Some("1m"))
        );
    }

    /// The S&P 500's tick slice of 12 November over `[15:00:00, 15:00:05)`, as pyarrow reads it.
    ///
    /// Bid and ask are the value, with no sizes, as data-ws sends an index's ticks.
    #[rstest]
    fn an_index_s_quotes_fill_from_its_tick_slice_under_its_lsedge_id() {
        const SPX: &str = "SPX-INDICES.LSEDGE";
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![Reply::file(SPX_TICKS)]);
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![TickSeries {
                data: NautilusDataType::QuoteTick,
                instrument_id: InstrumentId::from(SPX),
            }],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        };

        fill_requirements(
            dir.path(),
            &lse,
            &requirements,
            vec![index(SPX, "SPX", 2)],
            on_the_12th("15:00:00"),
            on_the_12th("15:00:04.999999999"),
        )
        .unwrap();

        let stored = ParquetDataCatalog::new(dir.path(), None, None, None, None)
            .quote_ticks(Some(vec![SPX.to_string()]), None, None)
            .unwrap();
        let rows: Vec<_> = stored
            .iter()
            .map(|q| (q.ts_event, q.bid_price, q.ask_price, q.bid_size, q.ask_size))
            .collect();
        let row = |time, value| {
            (
                UnixNanos::from(on_the_12th(time)),
                Price::from(value),
                Price::from(value),
                Quantity::from(0),
                Quantity::from(0),
            )
        };
        assert_eq!(
            rows,
            [
                row("15:00:00.034", "7600.07"),
                row("15:00:02.035", "7599.15"),
                row("15:00:03.067", "7599.18"),
            ]
        );
        let [export] = &lse.calls()[..] else {
            panic!("{:?}", lse.calls())
        };
        assert_eq!(
            (
                export.param("market"),
                export.param("symbol"),
                export.param("timeframe")
            ),
            (Some("indices"), Some("SPX"), Some("tick"))
        );
    }

    /// The definition request fails because LSE publishes none, and LSE is never asked.
    #[rstest]
    fn a_fill_of_an_lsedge_instrument_with_no_definition_fails_naming_it() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);

        let err = fill(dir.path(), &lse, GOLD_FUTURES, vec![])
            .unwrap_err()
            .to_string();

        let request = format!("the definition of {GOLD_FUTURES}");
        for part in [request.as_str(), "LSE publishes no instrument definitions"] {
            assert!(err.contains(part), "{part}: {err}");
        }
        assert!(lse.calls().is_empty(), "{:?}", lse.calls());
        assert_eq!(stored_trades(dir.path()), []);
    }

    // Both are in its metadata, under the config's identifier, here the symbol
    fn lse_rows(source: &str, symbol: &str) -> CustomSeries {
        let metadata = nautilus_core::Params::new(
            [
                ("source".to_string(), serde_json::json!(source)),
                ("symbol".to_string(), serde_json::json!(symbol)),
            ]
            .into(),
        );
        CustomSeries::new("LseRow", symbol.to_string(), Some(metadata)).unwrap()
    }

    fn us10y() -> CustomSeries {
        lse_rows("bond_yields", "US10Y")
    }

    // The captured days are the 9th to the 12th of November, known from 00:00 on the 10th to 00:00
    // on the 13th.
    fn fill_bond_yields(dir: &Path, lse: &MockLse) -> anyhow::Result<()> {
        fill_bond_yields_over(
            dir,
            lse,
            (november("10T00:00:00"), november("13T00:00:00")),
            settled(),
        )
    }

    fn fill_bond_yields_over(
        dir: &Path,
        lse: &MockLse,
        (from, to): (Timestamp, Timestamp),
        now: Timestamp,
    ) -> anyhow::Result<()> {
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![],
            custom: vec![us10y()],
            warmup: SignedDuration::ZERO,
        };
        fill_at(dir, lse, None, &requirements, vec![], (from, to), now)
    }

    fn november(time: &str) -> Timestamp {
        format!("2026-11-{time}Z").parse().unwrap()
    }

    /// The captured rows of the 9th, 10th and 12th, as pyarrow reads them.
    ///
    /// Each row is stamped at the end of its date, under the symbol or the identifier that a
    /// config's `[data] series` chose.
    #[rstest]
    #[case::under_its_symbol(us10y(), "US10Y")]
    #[case::under_the_identifier_its_config_chose(declared_treasury(), "TREASURY-10Y")]
    fn the_bond_yields_lse_exports_are_written_back_as_custom_data_of_their_series(
        #[case] series: CustomSeries,
        #[case] identifier: &str,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![
            Reply::json(REFERENCE_CATALOGUE),
            Reply::file(US10Y_BOND_YIELDS),
        ]);

        fill_custom(
            dir.path(),
            &lse,
            &series,
            ("2026-11-10T00:00:00Z", "2026-11-13T00:00:00Z"),
        )
        .unwrap();

        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let stored = catalog
            .query_custom_data_dynamic(
                "LseRow",
                Some(&[identifier.to_string()]),
                None,
                None,
                None,
                None,
                true,
            )
            .unwrap();
        let rows: Vec<_> = stored
            .iter()
            .map(|data| {
                let Data::Custom(custom) = data else {
                    panic!("not custom data: {data:?}");
                };
                let row = custom.data.as_any().downcast_ref::<LseRow>().unwrap();
                (
                    custom.data_type.clone(),
                    row.ts_init,
                    row.fields["close"].clone(),
                )
            })
            .collect();
        let data_type = DataType::new(
            "LseRow",
            Some(nautilus_core::Params::new(
                [
                    ("source".to_string(), serde_json::json!("bond_yields")),
                    ("symbol".to_string(), serde_json::json!("US10Y")),
                ]
                .into(),
            )),
            Some(identifier.to_string()),
        );
        let row = |day: &str, close: f64| {
            (
                data_type.clone(),
                UnixNanos::from(november(&format!("{day}T00:00:00"))),
                serde_json::json!(close),
            )
        };
        assert_eq!(rows, [row("10", 4.884), row("11", 5.0), row("13", 5.133)]);
    }

    #[derive(Debug, Clone, serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Reader {}

    impl Part for Reader {
        const INSTRUMENTS: usize = 1;

        fn default_base() -> StrategyConfig {
            StrategyConfig {
                strategy_id: Some(StrategyId::from("READER-001")),
                order_id_tag: Some("001".to_string()),
                ..Default::default()
            }
        }

        fn reads(config: &StandardConfig<Self>) -> ConfigResult<Vec<Series>> {
            Ok(config.data.series.clone())
        }
    }

    // US10Y's bond yields are declared under the identifier `TREASURY-10Y`, not the symbol, and
    // loaded after the custom data types are registered, as the process does at its start.
    fn declared_treasury() -> CustomSeries {
        crate::venues::register_custom_data();
        let config: StandardConfig<Reader> = StandardConfig::load(
            r#"
            [data]
            series = [
                { data = "quotes", instrument_id = "XAUUSD.ICMARKETS" },
                { data = "Custom:LseRow", identifier = "TREASURY-10Y", metadata = { source = "bond_yields", symbol = "US10Y" } },
            ]
            warmup = "1h"

            [trade]
            instruments = [{ instrument_id = "XAUUSD.ICMARKETS" }]
            risk = "0.5%"
            "#,
        )
        .unwrap();
        let [series] = <[CustomSeries; 1]>::try_from(config.data_requirements().custom).unwrap();
        series
    }

    /// The captured rows are stored, known until 00:00 on the 13th. A window until now follows.
    ///
    /// LSE has no row yet for the last date of that window (`lse`'s data client: the morning
    /// after). The window is not recorded as stored, so the next run asks again.
    #[rstest]
    fn a_window_of_bond_yields_reaching_into_a_day_lse_has_not_loaded_is_asked_again() {
        let dir = tempfile::tempdir().unwrap();
        let stored = MockLse::start(vec![
            Reply::json(REFERENCE_CATALOGUE),
            Reply::file(US10Y_BOND_YIELDS),
        ]);
        fill_bond_yields(dir.path(), &stored).unwrap();
        let now = november("16T12:00:00");
        let not_loaded = || {
            MockLse::start(vec![
                Reply::json(REFERENCE_CATALOGUE),
                Reply::refusal(404, REFERENCE_DATA_UNAVAILABLE),
            ])
        };
        let first = not_loaded();
        fill_bond_yields_over(dir.path(), &first, (november("10T00:00:00"), now), now).unwrap();

        let again = not_loaded();
        fill_bond_yields_over(dir.path(), &again, (november("10T00:00:00"), now), now).unwrap();

        let froms = [&first, &again].map(|lse| {
            lse.calls()
                .last()
                .and_then(|call| call.param("from").map(String::from))
        });
        assert_eq!(
            froms,
            [Some("2026-11-12"), Some("2026-11-12")].map(|from| from.map(String::from))
        );
    }

    /// US10Y's bond yields under GC, filled from the 28th to the 30th.
    ///
    /// GC rolls from GCQ26 into GCZ26 at 00:00 UTC on 29 July 2026 (`rolls/README.md`). LSE is
    /// asked once per contract slice. Each close is known at the end of its date.
    /// - GCQ26's slice is the 28th and holds the 27th's close.
    /// - GCZ26's slice is the 29th to the 30th and holds the 28th's and 29th's closes.
    #[rstest]
    fn a_roots_custom_series_fills_contract_by_contract_over_each_slice() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![
            Reply::json(REFERENCE_CATALOGUE),
            Reply::file(US10Y_BOND_YIELDS),
            Reply::json(REFERENCE_CATALOGUE),
            Reply::file(US10Y_BOND_YIELDS),
        ]);
        let metadata = us10y().data_type().metadata().cloned();
        let series = CustomSeries::new("LseRow", "GC.GLBX".to_string(), metadata).unwrap();

        fill_custom(
            dir.path(),
            &lse,
            &series,
            ("2026-07-28T00:00:00Z", "2026-07-30T00:00:00Z"),
        )
        .unwrap();

        let exports: Vec<(String, String)> = lse
            .calls()
            .iter()
            .filter_map(|call| Some((call.param("from")?.into(), call.param("to")?.into())))
            .collect();
        assert_eq!(
            exports,
            [("2026-07-27", "2026-07-27"), ("2026-07-28", "2026-07-29")]
                .map(|(from, to)| (from.to_string(), to.to_string()))
        );
    }

    fn fill_custom(
        dir: &Path,
        lse: &MockLse,
        series: &CustomSeries,
        (from, to): (&str, &str),
    ) -> anyhow::Result<()> {
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![],
            custom: vec![series.clone()],
            warmup: SignedDuration::ZERO,
        };
        fill_requirements(
            dir,
            lse,
            &requirements,
            vec![],
            from.parse().unwrap(),
            to.parse().unwrap(),
        )
    }

    fn stored_stamps(dir: &Path, series: &CustomSeries) -> Vec<UnixNanos> {
        let mut catalog = ParquetDataCatalog::new(dir, None, None, None, None);
        let identifier = series.identifier().to_string();
        catalog
            .query_custom_data_dynamic("LseRow", Some(&[identifier]), None, None, None, None, true)
            .unwrap()
            .iter()
            .map(|data| {
                let Data::Custom(custom) = data else {
                    panic!("not custom data: {data:?}");
                };
                assert_eq!(&custom.data_type, series.data_type());
                custom
                    .data
                    .as_any()
                    .downcast_ref::<LseRow>()
                    .unwrap()
                    .ts_init
            })
            .collect()
    }

    /// Each kind of LSE's other sources, under an identifier of spaces and dots.
    ///
    /// Each row is at the time it is known:
    /// - US core inflation releases of March to October 2026, from the economic calendar and the
    ///   macro series of releases.
    /// - ZN Commitments of Traders, known in September 2026.
    /// - AAPL dividends from 2026 to October, from its factors.
    /// - US10Y bond yields of 9, 10 and 12 November, each known at the end of its date.
    ///
    /// The window is stored whole, including days without a row, so a second fill asks LSE nothing.
    #[rstest]
    #[case::the_economic_calendar(
        lse_rows("economic_calendar", "US.Core Inflation Rate YoY"),
        vec![Reply::json(REFERENCE_CATALOGUE), Reply::file(US_CORE_CPI_CALENDAR)],
        ("2026-03-01T00:00:00Z", "2026-10-31T00:00:00Z"),
        US_CORE_CPI_RELEASED.to_vec()
    )]
    #[case::the_macro_series_of_releases(
        lse_rows("macro_release", "US.Core Inflation Rate YoY"),
        vec![Reply::file(US_CORE_CPI_RELEASES)],
        ("2026-03-01T00:00:00Z", "2026-10-31T00:00:00Z"),
        US_CORE_CPI_RELEASED.to_vec()
    )]
    #[case::the_commitments_of_traders(
        lse_rows("cot", "ZN"),
        vec![Reply::json(REFERENCE_CATALOGUE), Reply::file(ZN_COT)],
        ("2026-09-01T00:00:00Z", "2026-10-01T00:00:00Z"),
        vec![
            "2026-09-04T20:30:16Z",
            "2026-09-11T20:30:11Z",
            "2026-09-18T20:30:09Z",
            "2026-09-25T20:30:08Z",
        ]
    )]
    #[case::an_instrument_s_factors(
        lse_rows("factors_dividends", "AAPL-US_STOCKS"),
        vec![Reply::json(AAPL_FACTORS)],
        ("2026-01-01T00:00:00Z", "2026-10-31T00:00:00Z"),
        vec![
            "2026-02-10T00:00:00Z",
            "2026-05-12T00:00:00Z",
            "2026-08-11T00:00:00Z",
        ]
    )]
    #[case::bond_yields(
        us10y(),
        vec![Reply::json(REFERENCE_CATALOGUE), Reply::file(US10Y_BOND_YIELDS)],
        ("2026-11-10T00:00:00Z", "2026-11-13T00:00:00Z"),
        vec![
            "2026-11-10T00:00:00Z",
            "2026-11-11T00:00:00Z",
            "2026-11-13T00:00:00Z",
        ]
    )]
    fn each_kind_of_lse_s_other_sources_fills_written_back_at_the_times_its_rows_are_known(
        #[case] data_type: CustomSeries,
        #[case] script: Vec<Reply>,
        #[case] window: (&str, &str),
        #[case] stamps: Vec<&str>,
    ) {
        let utc = |time: &str| UnixNanos::from(time.parse::<Timestamp>().unwrap());
        let dir = tempfile::tempdir().unwrap();
        let calls = script.len();
        let lse = MockLse::start(script);

        fill_custom(dir.path(), &lse, &data_type, window).unwrap();
        fill_custom(dir.path(), &lse, &data_type, window).unwrap();

        assert_eq!(
            stored_stamps(dir.path(), &data_type),
            stamps.iter().map(|time| utc(time)).collect::<Vec<_>>()
        );
        assert_eq!(lse.calls().len(), calls, "{:?}", lse.calls());
    }

    const US_CORE_CPI_RELEASED: [&str; 8] = [
        "2026-03-11T12:30:00Z",
        "2026-04-10T12:30:00Z",
        "2026-05-12T12:30:00Z",
        "2026-06-10T12:30:00Z",
        "2026-07-14T12:30:00Z",
        "2026-08-12T12:30:00Z",
        "2026-09-11T12:30:00Z",
        "2026-10-14T12:30:00Z",
    ];

    /// LSE has not filled the past release of 11 February 2026, so the window is not written back.
    #[rstest]
    fn a_window_of_releases_lse_has_not_filled_is_asked_again() {
        let dir = tempfile::tempdir().unwrap();
        let calendar = lse_rows("economic_calendar", "US.Core Inflation Rate YoY");
        let february = ("2026-02-01T00:00:00Z", "2026-02-28T00:00:00Z");
        let lse = MockLse::start(vec![
            Reply::json(REFERENCE_CATALOGUE),
            Reply::file(US_CORE_CPI_CALENDAR),
            Reply::json(REFERENCE_CATALOGUE),
            Reply::file(US_CORE_CPI_CALENDAR),
        ]);

        fill_custom(dir.path(), &lse, &calendar, february).unwrap();
        fill_custom(dir.path(), &lse, &calendar, february).unwrap();

        assert_eq!(
            stored_stamps(dir.path(), &calendar),
            Vec::<UnixNanos>::new()
        );
        assert_eq!(lse.calls().len(), 4, "{:?}", lse.calls());
    }

    /// The client refuses the request before LSE is asked. The refusal names the releases.
    #[rstest]
    fn a_fill_of_macro_observations_fails_naming_the_releases() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![]);

        let err = fill_custom(
            dir.path(),
            &lse,
            &lse_rows("macro_observation", "US.core_inflation_rate"),
            ("2026-06-01T00:00:00Z", "2026-10-31T00:00:00Z"),
        )
        .unwrap_err();

        assert!(err.to_string().contains("macro_release"), "{err:#}");
        assert!(lse.calls().is_empty(), "{:?}", lse.calls());
    }

    /// A refusal fails the fill. The error carries the refusal's code, message and required plan.
    /// For a series LSE does not list, it carries the code and the request.
    #[rstest]
    #[case::a_plan_required(
        Reply::refusal(403, PLAN_REQUIRED),
        GOLD_FUTURES,
        gold_futures(),
        &["plan-required", "This feed requires a subscription", "CME Group Lite"]
    )]
    #[case::a_symbol_unknown(
        Reply::refusal(404, SYMBOL_UNKNOWN),
        NOSUCH,
        defined_as(NOSUCH),
        &["symbol-unknown", "the trades of NOSUCH-FUTURES.LSEDGE"]
    )]
    fn lse_s_refusal_fails_the_fill_with_its_code_message_and_required_plan(
        #[case] refusal: Reply,
        #[case] id: &str,
        #[case] definition: InstrumentAny,
        #[case] parts: &[&str],
    ) {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![refusal]);

        let err = fill(dir.path(), &lse, id, vec![definition])
            .unwrap_err()
            .to_string();

        for part in parts {
            assert!(err.contains(part), "{part}: {err}");
        }
        assert_eq!(stored_trades(dir.path()), []);
    }

    /// The 21st is filled and the 22nd is refused for a quota. The fill ends with LSE's message.
    /// The next run asks for the 22nd alone, because LSE holds no row for it, as on a date with no
    /// session.
    #[rstest]
    #[case::of_rows(ROWS_EXHAUSTED)]
    #[case::of_the_allowance(ALLOWANCE_EXHAUSTED)]
    fn a_quota_stops_the_fill_with_lse_s_message_and_the_next_run_asks_only_what_is_missing(
        #[case] quota: &str,
    ) {
        let day = |time: &str| -> Timestamp { time.parse().unwrap() };
        let (from, to) = (
            day("2026-10-21T00:00:00Z"),
            day("2026-10-22T23:59:59.999999999Z"),
        );
        let dir = tempfile::tempdir().unwrap();

        // The first window ends at the first instant of the 22nd, which its export takes
        let refusing = MockLse::start(vec![Reply::file(GOLD_TRADES), Reply::refusal(429, quota)]);

        let err = fill_over(
            dir.path(),
            &refusing,
            GOLD_FUTURES,
            vec![gold_futures()],
            (from, to),
            settled(),
        )
        .unwrap_err()
        .to_string();

        let refusal: serde_json::Value = serde_json::from_str(quota).unwrap();
        let message = refusal["message"].as_str().unwrap();
        assert!(err.contains(message), "{message}: {err}");
        assert_eq!(stored_trades(dir.path()).len(), 6);
        let span = |from: &str, to: &str| (from.to_string(), to.to_string());
        assert_eq!(
            asked(&refusing),
            [
                span("2026-10-21T00:00:00Z", "2026-10-22T00:00:00.000000001Z"),
                span("2026-10-22T00:00:00.000000001Z", "2026-10-23T00:00:00Z"),
            ]
        );

        let resuming = MockLse::start(vec![Reply::refusal(404, DATE_NOT_TRADING)]);
        fill_over(
            dir.path(),
            &resuming,
            GOLD_FUTURES,
            vec![],
            (from, to),
            settled(),
        )
        .unwrap();

        assert_eq!(
            asked(&resuming),
            [(
                "2026-10-22T00:00:00.000000001Z".to_string(),
                "2026-10-23T00:00:00Z".to_string()
            )]
        );

        let done = MockLse::start(vec![]);
        fill_over(
            dir.path(),
            &done,
            GOLD_FUTURES,
            vec![],
            (from, to),
            settled(),
        )
        .unwrap();

        assert!(done.calls().is_empty(), "{:?}", done.calls());
    }

    /// Trades are stored until 20 hours ago. A window until now follows, reaching into the current
    /// UTC day, which LSE stores the morning after. The window is not recorded as stored.
    #[rstest]
    fn a_window_lse_has_not_loaded_is_left_missing_for_the_next_run() {
        let dir = tempfile::tempdir().unwrap();
        let now = november("16T12:00:00");
        let from = now - SignedDuration::from_hours(72);
        let stored_to = now - SignedDuration::from_hours(20);
        let trade = TradeTick::new(
            InstrumentId::from(GOLD_FUTURES),
            Price::from("4136.4"),
            Quantity::from(1),
            AggressorSide::NoAggressor,
            TradeId::new("1"),
            UnixNanos::from(from),
            UnixNanos::from(from),
        );
        ParquetDataCatalog::new(dir.path(), None, None, None, None)
            .write_to_parquet(
                &[trade],
                Some(from.into()),
                Some((stored_to - SignedDuration::from_nanos(1)).into()),
                None,
            )
            .unwrap();

        // The previous day's part, if any, and the current day's part
        let not_loaded = || vec![Reply::refusal(404, DATA_UNAVAILABLE_TODAY); 2];
        let first = MockLse::start(not_loaded());

        fill_over(
            dir.path(),
            &first,
            GOLD_FUTURES,
            vec![gold_futures()],
            (from, now),
            now,
        )
        .unwrap();

        let again = MockLse::start(not_loaded());
        fill_over(dir.path(), &again, GOLD_FUTURES, vec![], (from, now), now).unwrap();
        let asked = asked(&again);
        assert_eq!(
            asked.first().map(|(from, _)| from.as_str()),
            Some(stored_to.to_string().as_str()),
            "{asked:?}"
        );
    }

    /// The trades and quotes of GCZ6 of 2026 over the captured second. The definition is
    /// Databento's.
    ///
    /// For the trades and quotes, LSE's client looks the contract up in LSE's catalogue. It asks
    /// for the rows of the listing with the CME key. Neither client claims a venue, so each request
    /// reaches the client it names.
    #[rstest]
    fn a_glbx_fill_takes_its_definition_from_databento_and_its_trades_and_quotes_from_lse() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![
            Reply::json(PAIRS_GCZ6),
            Reply::file(GCZ26_TRADES),
            Reply::file(GCZ26_QUOTES),
        ]);
        let databento = MockDatabento::start();
        let second: (Timestamp, Timestamp) = (
            "2026-11-12T14:30:00Z".parse().unwrap(),
            "2026-11-12T14:30:00.999999999Z".parse().unwrap(),
        );

        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: [NautilusDataType::TradeTick, NautilusDataType::QuoteTick]
                .map(|data| TickSeries {
                    data,
                    instrument_id: InstrumentId::from(GCZ26),
                })
                .to_vec(),
            custom: vec![],
            warmup: SignedDuration::ZERO,
        };

        fill_from(
            dir.path(),
            &lse,
            Some(&databento),
            &requirements,
            vec![],
            second,
        )
        .unwrap();

        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let [InstrumentAny::FuturesContract(definition)] = &catalog
            .instruments(Some(&[GCZ26.to_string()]), None, None)
            .unwrap()[..]
        else {
            panic!("not one futures contract");
        };
        assert_eq!(
            (
                definition.raw_symbol.as_str(),
                definition.price_precision,
                definition.price_increment,
                definition.multiplier,
                Timestamp::from(definition.expiration_ns),
            ),
            (
                "GCZ6",
                2,
                Price::from("0.10"),
                Quantity::from(100),
                "2026-12-29T18:30:00Z".parse().unwrap(),
            )
        );
        let trades = catalog
            .trade_ticks(Some(vec![GCZ26.to_string()]), None, None)
            .unwrap();
        assert_eq!(trades.len(), 38);
        assert_eq!(
            (trades[0].ts_event, trades[0].price),
            (
                UnixNanos::from(1_794_493_800_001_418_191),
                Price::from("3908.10")
            )
        );
        let quotes = catalog
            .quote_ticks(Some(vec![GCZ26.to_string()]), None, None)
            .unwrap();
        assert_eq!(quotes.len(), 636);
        let asked_of_databento: Vec<_> = databento
            .calls()
            .iter()
            .map(|call| {
                (
                    call.path.clone(),
                    call.param("schema").map(ToString::to_string),
                )
            })
            .collect();
        assert_eq!(
            asked_of_databento,
            [
                ("/v0/metadata.get_dataset_range".to_string(), None),
                (
                    "/v0/timeseries.get_range".to_string(),
                    Some("definition".to_string())
                ),
            ]
        );
        let asked_of_lse: Vec<_> = lse
            .calls()
            .iter()
            .map(|call| {
                (
                    call.path.clone(),
                    call.param("pair_id")
                        .or(call.param("symbol"))
                        .map(ToString::to_string),
                    call.param("data").map(ToString::to_string),
                    call.api_key.clone(),
                )
            })
            .collect();
        let call = |path: &str, instrument: &str, data: Option<&str>| {
            (
                path.to_string(),
                Some(instrument.to_string()),
                data.map(ToString::to_string),
                Some(CME_API_KEY.to_string()),
            )
        };
        assert_eq!(
            asked_of_lse,
            [
                call("/v3/pairs", "GCZ6", None),
                call("/v3/export", "4000000002", Some("trades")),
                call("/v3/export", "4000000002", Some("quotes")),
            ]
        );
    }

    /// A window past the end of Databento's `GLBX.MDP3`, captured at 09:31:40.093383 on 13 November
    /// 2026.
    ///
    /// The definition is asked up to that end, from midnight of the last whole UTC day before it.
    /// Databento defines every active instrument on such a day.
    #[rstest]
    fn a_glbx_fill_past_databento_s_dataset_end_asks_its_definition_up_to_that_end() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![
            Reply::json(PAIRS_GCZ6),
            Reply::refusal(404, DATA_UNAVAILABLE),
        ]);
        let databento = MockDatabento::start();
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![TickSeries {
                data: NautilusDataType::TradeTick,
                instrument_id: InstrumentId::from(GCZ26),
            }],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        };

        fill_from(
            dir.path(),
            &lse,
            Some(&databento),
            &requirements,
            vec![],
            (
                "2026-11-13T09:00:00Z".parse().unwrap(),
                "2026-11-13T09:59:59.999999999Z".parse().unwrap(),
            ),
        )
        .unwrap();

        let calls = databento.calls();
        let definition = calls
            .iter()
            .find(|call| call.param("schema") == Some("definition"))
            .unwrap();

        // In nanoseconds, with the end exclusive, as Nautilus' client asks
        let asked = |param: &str| {
            Timestamp::from_nanosecond(definition.param(param).unwrap().parse().unwrap()).unwrap()
        };
        assert_eq!(
            (asked("start"), asked("end")),
            (
                "2026-11-12T00:00:00Z".parse().unwrap(),
                "2026-11-13T09:31:40.093383Z".parse().unwrap()
            )
        );
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        assert_eq!(
            catalog
                .instruments(Some(&[GCZ26.to_string()]), None, None)
                .unwrap()
                .len(),
            1
        );
    }

    /// The book depths and deltas of GCZ6 of 2026 over the captured second, as LSE's docs record
    /// them (the adapter's `test_data/`).
    ///
    /// Each delta event ends `F_LAST`.
    #[rstest]
    fn a_glbx_fill_takes_its_book_depths_and_deltas_from_lse() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![
            Reply::json(PAIRS_GCZ6),
            Reply::file(GCZ26_BOOK),
            Reply::file(GCZ26_ORDERS),
        ]);
        let databento = MockDatabento::start();
        let second: (Timestamp, Timestamp) = (
            "2026-11-12T14:30:00Z".parse().unwrap(),
            "2026-11-12T14:30:00.999999999Z".parse().unwrap(),
        );
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: [
                NautilusDataType::OrderBookDepth,
                NautilusDataType::OrderBookDelta,
            ]
            .map(|data| TickSeries {
                data,
                instrument_id: InstrumentId::from(GCZ26),
            })
            .to_vec(),
            custom: vec![],
            warmup: SignedDuration::ZERO,
        };

        fill_from(
            dir.path(),
            &lse,
            Some(&databento),
            &requirements,
            vec![],
            second,
        )
        .unwrap();

        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let depths = catalog
            .order_book_depths(Some(vec![GCZ26.to_string()]), None, None)
            .unwrap();
        assert_eq!(depths.len(), 4);
        assert_eq!(
            (depths[0].bids[0].price, depths[0].asks[0].price),
            (Price::from("3908.10"), Price::from("3908.30"))
        );
        let deltas = catalog
            .order_book_deltas(Some(vec![GCZ26.to_string()]), None, None)
            .unwrap();
        assert_eq!(deltas.len(), 4);
        assert!(RecordFlag::F_LAST.matches(deltas[3].flags));
        let asked_of_lse: Vec<_> = lse
            .calls()
            .iter()
            .map(|call| call.param("data").map(ToString::to_string))
            .collect();
        assert_eq!(
            asked_of_lse,
            [None, Some("book".to_string()), Some("orders".to_string())]
        );
    }

    /// The AAPL 355 call of 20 November 2026 over the captured minutes.
    ///
    /// The definition is Databento's, from `OPRA.PILLAR`. LSE's trades and 1-minute bars are asked
    /// by the OSI symbol with the LSE key, which opens LSE's US options. The captured minute of
    /// trades stands for the whole window. The candles of 14:30 to 14:33 stand for the bars,
    /// because LSE has none for 14:34.
    #[rstest]
    fn an_opra_fill_takes_its_definition_from_databento_and_its_trades_and_bars_from_lse() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![
            Reply::file(AAPL_CALL_TRADES),
            Reply::file(AAPL_CALL_MINUTES),
        ]);
        let databento = MockDatabento::start();
        let bar_type = format!("{AAPL_CALL}-1-MINUTE-LAST-EXTERNAL");
        let minutes = (november("12T14:30:00"), november("12T14:34:00"));
        let requirements = DataRequirements {
            bar_types: vec![BarType::from(bar_type.as_str())],
            ticks: vec![TickSeries {
                data: NautilusDataType::TradeTick,
                instrument_id: InstrumentId::from(AAPL_CALL),
            }],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        };

        fill_from(
            dir.path(),
            &lse,
            Some(&databento),
            &requirements,
            vec![],
            minutes,
        )
        .unwrap();

        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let [InstrumentAny::OptionContract(definition)] = &catalog
            .instruments(Some(&[AAPL_CALL.to_string()]), None, None)
            .unwrap()[..]
        else {
            panic!("not one option contract");
        };

        // Expiry is 16:00 New York, the close of its day, and Databento's record gives no
        // multiplier, so the multiplier is the standard contract's 100.
        assert_eq!(
            (
                definition.raw_symbol.as_str(),
                definition.underlying.as_str(),
                definition.option_kind,
                definition.strike_price,
                Timestamp::from(definition.expiration_ns),
                definition.price_increment,
                definition.multiplier,
            ),
            (
                "AAPL  261120C00355000",
                "AAPL",
                OptionKind::Call,
                Price::from("355.00"),
                november("20T21:00:00"),
                Price::from("0.01"),
                Quantity::from(100),
            )
        );
        let trades = catalog
            .trade_ticks(Some(vec![AAPL_CALL.to_string()]), None, None)
            .unwrap();
        assert_eq!(trades.len(), 23);
        assert_eq!(
            (trades[0].ts_event, trades[0].price, trades[0].size),
            (
                UnixNanos::from(november("12T14:30:00.816861166")),
                Price::from("6.70"),
                Quantity::from(1)
            )
        );
        let bars = catalog.bars(Some(vec![bar_type]), None, None).unwrap();
        let closes: Vec<_> = bars.iter().map(|bar| (bar.ts_event, bar.close)).collect();
        assert_eq!(
            closes,
            [
                ("14:31:00", "6.22"),
                ("14:32:00", "6.57"),
                ("14:33:00", "6.15"),
                ("14:34:00", "6.40"),
            ]
            .map(|(time, close)| (
                UnixNanos::from(november(&format!("12T{time}"))),
                Price::from(close)
            ))
        );
        let asked_of_databento: Vec<_> = databento
            .calls()
            .iter()
            .map(|call| {
                (
                    call.path.clone(),
                    call.param("dataset").map(ToString::to_string),
                    call.param("symbols").map(ToString::to_string),
                )
            })
            .collect();
        assert_eq!(
            asked_of_databento,
            [
                (
                    "/v0/metadata.get_dataset_range".to_string(),
                    Some("OPRA.PILLAR".to_string()),
                    None
                ),
                (
                    "/v0/timeseries.get_range".to_string(),
                    Some("OPRA.PILLAR".to_string()),
                    Some("AAPL  261120C00355000".to_string())
                ),
            ]
        );
        let mut asked_of_lse: Vec<_> = lse
            .calls()
            .iter()
            .map(|call| {
                [
                    call.path.as_str(),
                    call.param("market").unwrap_or_default(),
                    call.param("symbol").unwrap_or_default(),
                    call.param("timeframe").unwrap_or_default(),
                    call.api_key.as_deref().unwrap_or_default(),
                ]
                .map(ToString::to_string)
            })
            .collect();
        asked_of_lse.sort();
        let export = |timeframe: &str| {
            [
                "/v3/export",
                "us_options",
                "AAPL  261120C00355000",
                timeframe,
                API_KEY,
            ]
            .map(ToString::to_string)
        };
        assert_eq!(asked_of_lse, [export("1m"), export("tick")]);
    }

    /// NVDA over the second that its documented exports hold.
    ///
    /// The definition is Databento's, from `XNAS.ITCH`. LSE's data is asked by the ticker in
    /// `nasdaq` with the Nasdaq key. Each delta event ends `F_LAST`.
    #[rstest]
    fn an_xnas_fill_takes_its_definition_from_databento_and_its_data_from_lse() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![
            Reply::file(NVDA_TRADES),
            Reply::file(NVDA_QUOTES),
            Reply::file(NVDA_BOOK),
            Reply::file(NVDA_ORDERS),
        ]);
        let databento = MockDatabento::start();
        let second = (november("12T14:30:00"), november("12T14:30:00.999999999"));
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: [
                NautilusDataType::TradeTick,
                NautilusDataType::QuoteTick,
                NautilusDataType::OrderBookDepth,
                NautilusDataType::OrderBookDelta,
            ]
            .map(|data| TickSeries {
                data,
                instrument_id: InstrumentId::from(NVDA),
            })
            .to_vec(),
            custom: vec![],
            warmup: SignedDuration::ZERO,
        };

        fill_from(
            dir.path(),
            &lse,
            Some(&databento),
            &requirements,
            vec![],
            second,
        )
        .unwrap();

        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let [InstrumentAny::Equity(definition)] = &catalog
            .instruments(Some(&[NVDA.to_string()]), None, None)
            .unwrap()[..]
        else {
            panic!("not one equity");
        };
        assert_eq!(
            (
                definition.raw_symbol.as_str(),
                definition.price_precision,
                definition.price_increment,
                definition.lot_size,
            ),
            ("NVDA", 2, Price::from("0.01"), Some(Quantity::from(100)))
        );
        let trades = catalog
            .trade_ticks(Some(vec![NVDA.to_string()]), None, None)
            .unwrap();
        assert_eq!(
            trades
                .iter()
                .map(|trade| (trade.price, trade.size))
                .collect::<Vec<_>>(),
            [
                ("211.15", 100),
                ("211.15", 100),
                ("211.14", 200),
                ("211.17", 50)
            ]
            .map(|(price, size)| (Price::from(price), Quantity::from(size)))
        );
        let quotes = catalog
            .quote_ticks(Some(vec![NVDA.to_string()]), None, None)
            .unwrap();
        assert_eq!(quotes.len(), 4);
        let depths = catalog
            .order_book_depths(Some(vec![NVDA.to_string()]), None, None)
            .unwrap();
        assert_eq!(depths.len(), 4);
        assert_eq!(
            (depths[0].bids[0].price, depths[0].asks[0].price),
            (Price::from("211.15"), Price::from("211.17"))
        );
        let deltas = catalog
            .order_book_deltas(Some(vec![NVDA.to_string()]), None, None)
            .unwrap();
        assert_eq!(deltas.len(), 4);
        assert!(RecordFlag::F_LAST.matches(deltas[3].flags));
        let asked_of_databento: Vec<_> = databento
            .calls()
            .iter()
            .map(|call| {
                (
                    call.path.clone(),
                    call.param("dataset").map(ToString::to_string),
                    call.param("symbols").map(ToString::to_string),
                )
            })
            .collect();
        assert_eq!(
            asked_of_databento,
            [
                (
                    "/v0/metadata.get_dataset_range".to_string(),
                    Some("XNAS.ITCH".to_string()),
                    None
                ),
                (
                    "/v0/timeseries.get_range".to_string(),
                    Some("XNAS.ITCH".to_string()),
                    Some("NVDA".to_string())
                ),
            ]
        );
        let mut asked_of_lse: Vec<_> = lse
            .calls()
            .iter()
            .map(|call| {
                [
                    call.path.as_str(),
                    call.param("market").unwrap_or_default(),
                    call.param("symbol").unwrap_or_default(),
                    call.param("data").unwrap_or_default(),
                    call.api_key.as_deref().unwrap_or_default(),
                ]
                .map(ToString::to_string)
            })
            .collect();
        asked_of_lse.sort();
        let export = |data: &str| {
            ["/v3/export", "nasdaq", "NVDA", data, NASDAQ_API_KEY].map(ToString::to_string)
        };
        assert_eq!(
            asked_of_lse,
            [
                export("book"),
                export("orders"),
                export("quotes"),
                export("trades")
            ]
        );
    }
}
