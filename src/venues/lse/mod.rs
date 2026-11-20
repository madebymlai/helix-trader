//! LSE history and live data, from the LSE adapter's data client (`lse`).
//!
//! History is filled through the node (see `catalog::NodeFill`), the data engine writing the
//! client's answers back to the catalog: of `LSEDGE` instruments, London Strategic Edge's own
//! derived series, each an index instrument named after its LSE market (`GOLD-FUTURES.LSEDGE`: GOLD
//! in `futures`), trades, from LSE's tick slices, and bars, from its candles, each stamped at its
//! close; of `GLBX` contracts, CME Globex's Levels 1 to 3 under the ids Databento gives them
//! (`GCZ26.GLBX`), trades, quotes, book depths and book deltas; of `XNAS` stocks, Nasdaq
//! TotalView's Levels 1 to 3 under the ids Databento gives them, their tickers (`NVDA.XNAS`), the
//! same; and of `OPRA` contracts, US options under the ids Databento gives them, their OSI symbols
//! (`AAPL 261120C00355000.OPRA`), trades and bars, as an `LSEDGE` series'.
//!
//! Live, the client streams the quotes of `LSEDGE` instruments, the Levels 1 to 3 of `GLBX`
//! contracts and `XNAS` stocks, quotes and trades, book depths and book deltas, and the trades of
//! `OPRA` contracts, through one subscription of each underlying's chain, on the live node, routed
//! their venues (`live_clients`), IB's execution client taking `GLBX`'s, `XNAS`'s and `OPRA`'s
//! orders.
//!
//! Each contract's definition is its row's definitions source's: `LSEDGE`'s the catalog's alone,
//! written there by hand, LSE publishing none, the data engine answering a definition request from
//! the catalog before asking the client; `GLBX`'s, `XNAS`'s and `OPRA`'s Databento's, asked of
//! Nautilus' Databento client. The client converts LSE's prices at the definition's precision.
//!
//! LSE's reference series, the rows of its reference collections, of its macro series of releases
//! and of yields and of an instrument's splits and dividends, are custom series (`lse::LseRow`),
//! filled by the same client with no definition, each row stamped when its values are known. See
//! `README.md`.

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

/// The window of one request of a series: a day, as Databento's. The client asks LSE a UTC day an
/// export whatever the window.
const DAY: SignedDuration = SignedDuration::from_hours(24);

/// LSE's book as the LSE adapter's data client streams and serves it, of a `GLBX` contract or an
/// `XNAS` stock (`venues::book_format`): its deltas of an L3_MBO book, CME Globex's or Nasdaq
/// TotalView's orders at Level 3, each by its id, whatever book type a subscription names
/// (`subscribe_book_deltas`, `crates/lse/src/data.rs:601-605`, reading none; `request_book_deltas`,
/// `:781`), and its depth snapshots of ten levels a side, Level 2, the one depth the client streams
/// and serves, a subscription or a request of another failing naming it (`subscribe_book_depth`,
/// `:556-566`; `request_book_depth`, `:739-746`).
pub(super) const BOOK: BookFormat = BookFormat {
    deltas: BookType::L3_MBO,
    depth: NonZeroUsize::new(DEPTH10_LEN),
};

/// Adds to `node` the fill of what the catalog lacks of `slices`, series LSE gives the history of
/// (see `catalog::missing`), and of `custom`, custom series of LSE's rows (`lse::LseRow`) over
/// their slices, a root's contract by contract (see `catalog::missing_custom`): first each
/// contract's definition, asked of its row's definitions source, Databento's client, which
/// `databento` makes only if Databento has a definition to give, within its dataset's end
/// (`DatabentoHistory::within_datasets`), for the contracts Databento defines, and LSE's for the
/// others, the data engine answering them from the catalog before the client, which fails a
/// definition the catalog lacks, naming the instrument, before LSE is asked; then the series and
/// the rows, asked of the LSE client `config` configures, its keys read only when the node creates
/// it, so only if something is missing. The data engine puts each definition in the cache, where
/// the client reads the precision of its prices. A custom series brings no instrument and has none.
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

/// The window one request of any series holds at most (see `NodeFill::add`): a day.
fn period(_data: FillData) -> SignedDuration {
    DAY
}

/// The node's data client: the LSE adapter's, on no venue, so asked only by the requests naming it
/// (see `NodeFill::add`).
fn node_client(config: LseDataClientConfig) -> NodeClient {
    NodeClient {
        factory: Box::new(LseDataClientFactory::new()),
        config: Box::new(config),
    }
}

/// The live node's clients of `instrument_ids`, `LSEDGE` series, `GLBX` contracts, `XNAS` stocks
/// and `OPRA` contracts: the LSE adapter's data client streaming them, its keys read when the node
/// creates it, the LSE key for `LSEDGE` and `OPRA`, the CME key for `GLBX` and the Nasdaq key for
/// `XNAS`, and no execution client, LSE trading nothing, `GLBX`'s, `XNAS`'s and `OPRA`'s orders
/// going to IB's. The client is on no venue, so it takes the routes of theirs
/// (`RoutingConfig::venues`); helix makes it not the default, which Nautilus may make it, nothing
/// depending on that, every venue read having a route (see `live::node::Clients`). With no
/// instrument, the node of a run reading a custom series of LSE's rows alone, it streams none and
/// takes no route, the series' subscription naming it by its client id
/// (`venues::custom_data_client`).
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

    /// LSE lists no such series.
    const NOSUCH: &str = "NOSUCH-FUTURES.LSEDGE";

    fn at(time: &str) -> Timestamp {
        format!("2026-10-21T{time}Z").parse().unwrap()
    }

    /// `GOLD_FUTURES`'s definition, as `id`'s.
    fn defined_as(id: &str) -> InstrumentAny {
        let InstrumentAny::IndexInstrument(mut index) = gold_futures() else {
            unreachable!("an index");
        };
        index.id = InstrumentId::from(id);
        InstrumentAny::IndexInstrument(index)
    }

    /// fill-catalog's fill of the trades of `id` over the captured second, `[10:00:00, 10:00:01)`,
    /// into the catalog in `dir`, from `lse`, the catalog holding `definitions`.
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

    /// fill-catalog's fill of the trades of `id` over `[from, to]` at `now`, as `fill`'s.
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

    /// fill-catalog's fill of `requirements` over `[from, to]` into the catalog in `dir`, from
    /// `lse`, the catalog holding `definitions`.
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

    /// When a fill runs unless a test says otherwise: days after every window the tests fill, so
    /// that LSE has loaded each.
    fn settled() -> Timestamp {
        "2026-11-25T12:00:00Z".parse().unwrap()
    }

    /// The clocks of a node at `now`: Nautilus' virtual clock, set there.
    fn clock_at(now: Timestamp) -> impl Fn() -> Rc<RefCell<dyn Clock>> + 'static {
        move || {
            let clock = VirtualClock::new();
            clock.set_time(UnixNanos::from(now));
            Rc::new(RefCell::new(clock))
        }
    }

    /// fill-catalog's fill of `requirements` over `[from, to]`, as `fill_at`'s, at `settled()`.
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

    /// fill-catalog's fill of `requirements` over `[from, to]` into the catalog in `dir`, from
    /// `lse` and `databento`, Databento's mock peer, the catalog holding `definitions`, the custom
    /// data types helix compiles registered first, as the process does at its start, on a node
    /// whose clock reads `now`. With no peer of Databento's, Databento is never asked.
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
            // The mock answers at once: no plan's minute to keep to, as the crate's own tests have
            // it.
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

    /// The spans LSE was asked for, in order.
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

    /// The captured five minutes of gold spot, candles LSE stamps at 10:00 to 10:04, their open, as
    /// pyarrow reads them; the bars closing over `[10:01, 10:05]`.
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

    /// `id`'s definition, as one is written by hand: an index of LSE's symbol `raw`, its prices at
    /// `precision`, its sizes whole lots.
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

    /// A series of each derived market LSE stores trades of, its trade slice of 12 November
    /// captured from LSE, filled over the captured window under its `LSEDGE` id, as pyarrow reads
    /// LSE's file: the count and the total size, the first and last times, and the lowest and
    /// highest prices. LSE is asked the slice of its market and symbol. Futures' are
    /// `the_trades_lse_exports_are_written_back_to_the_catalog`'s.
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

    /// A bar's close, then its open, high, low and last prices.
    type BarRow = (&'static str, [&'static str; 4]);

    /// The forex pair and the index of LSE's 1-minute candles of 12 November captured from LSE,
    /// filled as the bars closing over `[10:01, 10:05]` and `[15:01, 15:05]` under their `LSEDGE`
    /// ids, as pyarrow reads LSE's files: five bars, each stamped at its close, a minute after the
    /// time LSE's row opens at, of no volume. LSE stores no trades of forex, and an index has none.
    /// Commodities' bars are gold spot's
    /// (`lse_s_candles_are_written_back_to_the_catalog_as_bars_stamped_at_their_close`).
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

    /// The S&P 500's tick slice of 12 November captured from LSE, its values of no size, filled as
    /// quotes over `[15:00:00, 15:00:05)` under its `LSEDGE` id, as pyarrow reads LSE's file: each
    /// a quote whose bid and ask are the value, with no sizes, as data-ws sends an index's ticks.
    /// LSE is asked the index's tick slice.
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

    /// The definition request fails, LSE publishing none, and LSE is never asked.
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

    /// The custom series of the rows of `symbol` in LSE's `source`: both in its metadata, under an
    /// identifier of the config's, here the symbol.
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

    /// US10Y's daily bond yields, a custom series.
    fn us10y() -> CustomSeries {
        lse_rows("bond_yields", "US10Y")
    }

    /// fill-catalog's fill of `us10y()` over the rows of the captured days, the 9th to the 12th of
    /// November, known from the 10th's 00:00 to the 13th's, into the catalog in `dir`, from `lse`.
    fn fill_bond_yields(dir: &Path, lse: &MockLse) -> anyhow::Result<()> {
        fill_bond_yields_over(
            dir,
            lse,
            (november("10T00:00:00"), november("13T00:00:00")),
            settled(),
        )
    }

    /// fill-catalog's fill of `us10y()` over `[from, to]` into the catalog in `dir`, from `lse`, at
    /// `now`.
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

    /// The captured rows, as pyarrow reads LSE's file: the 9th's, the 10th's and the 12th's, read
    /// back as Nautilus decodes custom data, each stamped at its date's end, under the series'
    /// identifier and of its data type, unchanged: the symbol's, or the one a config chose,
    /// declared as its `[data] series` declares it.
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

    /// A part reading every series its config lists, trading one instrument.
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

    /// US10Y's daily bond yields as a config declares them under the identifier `TREASURY-10Y`,
    /// which it chooses, not the symbol: the one custom series of its `[data] series`, loaded after
    /// registering the custom data types helix compiles, as the process does at its start.
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

    /// The captured rows stored, known until the 13th's 00:00, then a window until now, whose last
    /// date, the day before's, is LSE's the morning after it (`lse`'s data client), and of which
    /// LSE holds no row yet. The fill ends without recording the window beside the stored rows, as
    /// an empty answer would be, so that the next run asks for it again.
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

    /// US10Y's daily bond yields under GC's id, a root GC rolls from GCQ26 into GCZ26 at 00:00 UTC
    /// on 29 July 2026 (`rolls/README.md`), filled from the 28th to the 30th: LSE is asked once for
    /// each contract's series, over the slice it stands for GC, each date's close known at its end.
    /// GCQ26's slice, the 28th, holds the 27th's close; GCZ26's, from the 29th to the 30th, the
    /// 28th's and the 29th's.
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

    /// fill-catalog's fill of `series` alone over `[from, to]` into the catalog in `dir`, from
    /// `lse`.
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

    /// The times of the rows of `series` the catalog in `dir` holds, each of its data type, read
    /// back as Nautilus decodes custom data.
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

    /// A series of each kind of LSE's other sources, filled through the node and written back under
    /// its identifier, a code of spaces and dots among them, each row at the time it is known: US
    /// core inflation's releases of March to October 2026, in the economic calendar and in the
    /// macro series of releases; ZN's Commitments of Traders known in September 2026; AAPL's
    /// dividends of 2026 to October, from its factors; and US10Y's bond yields of the captured
    /// days, the 9th, 10th and 12th of November, each known at its date's end. The window is stored
    /// whole, the days without a row too: a second fill asks LSE nothing.
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

    /// The times US core inflation's releases of March to October 2026 came out.
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

    /// LSE has not filled the release of 11 February 2026, past: the window is not written back,
    /// and the next run asks for it again.
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

    /// Refused by LSE's client before LSE is asked, naming the releases.
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

    /// LSE's refusal fails the fill with its facts, storing no trade: its code, message and
    /// required plan; or its code and the request, of a series LSE lists none of.
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

    /// The 21st is filled and the 22nd refused for a quota, which ends the fill with LSE's message.
    /// The next run asks for the 22nd alone, of which LSE holds no row as on a date with no
    /// session, and the run after asks for nothing.
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

    /// Trades stored until 20 hours ago, then a window until now, which reaches into the current
    /// UTC day, LSE storing a day the morning after it. The fill ends, but the window is not
    /// recorded as stored beside the stored trades, so that the next run asks for it again.
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

        // The day before's part, if any, and the current day's
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

    /// GCZ6 of 2026's trades and quotes over the captured second: its definition is Databento's,
    /// asked of Databento's client, which the mock Databento peer answers with Databento's records
    /// of GCZ6, and its trades and quotes LSE's, asked of LSE's client, which looks the contract up
    /// in LSE's catalogue and asks for its listing's rows with the CME key. Neither client claims a
    /// venue: each request reaches the client it names. The definition is the one Databento's
    /// client wrote into this repository's catalog before.
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

    /// A window reaching past the end of Databento's `GLBX.MDP3`, captured at 09:31:40.093383 on 13
    /// November 2026, as a fill ending today does: the definition is asked up to that end, from the
    /// midnight of the last whole UTC day before it, a day Databento defines every instrument
    /// active in, and the fill goes on, LSE answering no rows of the day yet.
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

        // In nanoseconds, the end exclusive, as Nautilus' client asks
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

    /// GCZ6 of 2026's book depths and deltas over the captured second, as LSE's docs record its
    /// book and orders (the adapter's `test_data/`): asked of LSE's client by client id, its
    /// definition of Databento's, and written back to the catalog, each delta event ending
    /// `F_LAST`.
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

    /// AAPL's 355 call of 20 November 2026 over the captured minutes: its definition is
    /// Databento's, of `OPRA.PILLAR`, asked of Databento's client, which the mock Databento peer
    /// answers with Databento's record of the contract, and its trades and 1-minute bars LSE's,
    /// asked of LSE's client by the contract's OSI symbol with the LSE key, which opens LSE's US
    /// options. The captured minute of trades stands for the window's, and the four candles of
    /// 14:30 to 14:33 for the bars closing over it, LSE having none of 14:34.
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

        // 16:00 New York, the close of the day it expires; Databento's record gives no multiplier,
        // so the standard contract's 100.
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

    /// NVDA over the second its documented exports hold: its definition is Databento's, of
    /// `XNAS.ITCH`, asked of Databento's client, which the mock Databento peer answers with
    /// Databento's records of NVDA, and its trades, quotes, book depths and book deltas LSE's,
    /// asked of LSE's client by NVDA's ticker in LSE's `nasdaq` with the Nasdaq key, each written
    /// back to the catalog under `NVDA.XNAS`, each delta event ending `F_LAST`.
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
