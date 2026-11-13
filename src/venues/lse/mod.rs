//! LSE history and live data, from the LSE adapter's data client (`lse`).
//! History is filled through the node (see `catalog::NodeFill`), the data
//! engine writing the client's answers back to the catalog: of `LSEDGE`
//! instruments, London Strategic Edge's own derived series, each an index
//! instrument named after its LSE market (`GOLD-FUTURES.LSEDGE`: GOLD in
//! `futures`), trades, from LSE's tick slices, and bars, from its candles,
//! each stamped at its close; of `GLBX` contracts, CME Globex's Levels 1
//! to 3 under the ids Databento gives them (`GCZ26.GLBX`), trades, quotes,
//! book depths and book deltas; and of `OPRA` contracts, US options under
//! the ids Databento gives them, their OSI symbols
//! (`AAPL  261120C00355000.OPRA`), trades and bars, as an `LSEDGE`
//! series'. Live, the client streams the quotes of `LSEDGE` instruments and
//! the trades of `OPRA` contracts, through one subscription of each
//! underlying's chain, on the live node, routed their venues
//! (`live_clients`), IB's execution client taking `OPRA`'s orders. Each
//! contract's definition is its row's definitions source's: `LSEDGE`'s the
//! catalog's alone, written there by hand, LSE publishing none, the data
//! engine answering a definition request from the catalog before asking the
//! client; `GLBX`'s and `OPRA`'s Databento's, asked of Nautilus' Databento
//! client. The client converts LSE's prices at the definition's precision.
//! LSE's reference series, the rows of its reference collections, are
//! custom data of no instrument (`lse::LseRow`), filled by the same client
//! with no definition. See `README.md`.

use ::lse::{LseDataClientConfig, LseDataClientFactory};
use jiff::{SignedDuration, Timestamp};
use nautilus_live::config::RoutingConfig;
use nautilus_model::{data::DataType, identifiers::InstrumentId};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

use super::{DefinitionsSource, Row, databento::DatabentoHistory};
use crate::{
    catalog::{FillData, NodeClient, NodeFill, missing, missing_custom},
    live::node::Clients,
    strategy::SeriesSlice,
};

/// The window of one request of a series: a day, as Databento's. The
/// client asks LSE a UTC day an export whatever the window.
const DAY: SignedDuration = SignedDuration::from_hours(24);

/// Adds to `node` the fill of what the catalog lacks of `slices`, series
/// LSE gives the history of (see `catalog::missing`), and of `custom`,
/// LSE's rows (`lse::LseRow`) over `[start, end]` (see
/// `catalog::missing_custom`): first each contract's definition, asked of
/// its row's definitions source, Databento's client, which `databento`
/// makes only if Databento has a definition to give, checked against its
/// dataset's end, for the contracts Databento defines, and LSE's for the
/// others, the data engine answering them from the catalog before the
/// client, which fails a definition the catalog lacks, naming the
/// instrument, before LSE is asked; then the series and the rows, asked of
/// the LSE client `config` configures, its keys read only when the node
/// creates it, so only if something is missing. The data engine puts each
/// definition in the cache, where the client reads the precision of its
/// prices. LSE's rows are of no instrument and have none.
///
/// # Errors
///
/// Returns an error if a series is not one the node fills (see `missing`),
/// if a custom series has no identifier, if the catalog cannot be read, or
/// as `databento` and `DatabentoHistory::check`.
#[expect(clippy::too_many_arguments, reason = "a fill's parts")]
pub(super) async fn add_fill(
    node: &mut NodeFill,
    catalog: &ParquetDataCatalog,
    slices: &[SeriesSlice],
    custom: &[DataType],
    start: Timestamp,
    end: Timestamp,
    config: LseDataClientConfig,
    databento: impl FnOnce() -> anyhow::Result<DatabentoHistory>,
) -> anyhow::Result<()> {
    let (definitions, requests): (Vec<_>, Vec<_>) =
        missing(catalog, slices)?.into_iter().partition(|request| {
            request.data == FillData::Instrument
                && Row::of(request.instrument_id.venue).definitions == DefinitionsSource::Databento
        });
    let custom = missing_custom(catalog, custom, start, end)?;
    if !definitions.is_empty() {
        let databento = databento()?;
        databento.check(&definitions).await?;
        node.add(databento.node_client(), &definitions, |data| {
            unreachable!("Databento's check passes definitions alone, not {data}")
        });
    }
    node.add_with_custom(node_client(config), &requests, &custom, period);
    Ok(())
}

/// The window one request of any series holds at most (see
/// `NodeFill::add`): a day.
fn period(_data: FillData) -> SignedDuration {
    DAY
}

/// The node's data client: the LSE adapter's, on no venue, so asked only
/// by the requests naming it (see `NodeFill::add`).
fn node_client(config: LseDataClientConfig) -> NodeClient {
    NodeClient {
        factory: Box::new(LseDataClientFactory::new()),
        config: Box::new(config),
    }
}

/// The live node's clients of `instrument_ids`, `LSEDGE` series and `OPRA`
/// contracts: the LSE adapter's data client streaming them, its key read
/// when the node creates it, and no execution client, LSE trading nothing,
/// `OPRA`'s orders going to IB's. The client is on no venue, so it takes
/// the route of theirs (`RoutingConfig::venues`) and not the default, which
/// IB keeps for the venues without a client of their own.
pub(super) fn live_clients(instrument_ids: &[InstrumentId]) -> Clients {
    clients(
        LseDataClientConfig::builder()
            .stream_ids(instrument_ids.to_vec())
            .build(),
    )
}

/// The live node's clients on `config`, routed the venues of the
/// instruments it streams.
fn clients(config: LseDataClientConfig) -> Clients {
    let mut venues: Vec<String> = Vec::new();
    for id in &config.stream_ids {
        let venue = id.venue.to_string();
        if !venues.contains(&venue) {
            venues.push(venue);
        }
    }
    Clients {
        data_factory: Box::new(LseDataClientFactory::new()),
        data_config: Box::new(config),
        exec: None,
        routing: RoutingConfig {
            default: false,
            venues: Some(venues),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, path::Path};

    use ::lse::{
        LseRow,
        testing::{
            AAPL_CALL, AAPL_CALL_MINUTES, AAPL_CALL_TRADES, AAPL_TRADES, ALLOWANCE_EXHAUSTED,
            API_KEY, BTCUSDT_TRADES, CME_API_KEY, DATA_UNAVAILABLE_TODAY, DATE_NOT_TRADING,
            EUR_USD_MINUTES, GCZ26, GCZ26_BOOK, GCZ26_ORDERS, GCZ26_QUOTES, GCZ26_TRADES,
            GOLD_FUTURES, GOLD_SPOT, GOLD_SPOT_MINUTES, GOLD_TRADES, MockLse, PAIRS_GCZ6,
            PLAN_REQUIRED, REFERENCE_CATALOGUE, REFERENCE_DATA_UNAVAILABLE, ROWS_EXHAUSTED,
            RY_TRADES, Reply, SPX_MINUTES, SPX_TICKS, SYMBOL_UNKNOWN, US10Y_BOND_YIELDS,
            VOD_TRADES, gold_futures, gold_spot,
        },
    };
    use jiff::Timestamp;
    use nautilus_core::{UnixNanos, string::secret::SecretString};
    use nautilus_model::{
        data::{Bar, BarType, Data, DataType, NautilusDataType, TradeTick},
        enums::{AggressorSide, OptionKind, RecordFlag},
        identifiers::{InstrumentId, Symbol, TradeId},
        instruments::{IndexInstrument, InstrumentAny},
        types::{Currency, Price, Quantity},
    };

    use super::*;
    use crate::{
        strategy::{DataRequirements, TickSeries},
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

    /// fill-catalog's fill of the trades of `id` over the captured second,
    /// `[10:00:00, 10:00:01)`, into the catalog in `dir`, from `lse`, the
    /// catalog holding `definitions`.
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
            at("10:00:00"),
            at("10:00:00.999999999"),
        )
    }

    /// fill-catalog's fill of the trades of `id` over `[from, to]`, as
    /// `fill`'s.
    fn fill_over(
        dir: &Path,
        lse: &MockLse,
        id: &str,
        definitions: Vec<InstrumentAny>,
        from: Timestamp,
        to: Timestamp,
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
        fill_requirements(dir, lse, &requirements, definitions, from, to)
    }

    /// fill-catalog's fill of `requirements` over `[from, to]` into the
    /// catalog in `dir`, from `lse`, the catalog holding `definitions`.
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

    /// fill-catalog's fill of `requirements` over `[from, to]` into the
    /// catalog in `dir`, from `lse` and `databento`, Databento's mock peer,
    /// the catalog holding `definitions`. With no peer of Databento's,
    /// Databento is never asked.
    fn fill_from(
        dir: &Path,
        lse: &MockLse,
        databento: Option<&MockDatabento>,
        requirements: &DataRequirements,
        definitions: Vec<InstrumentAny>,
        (from, to): (Timestamp, Timestamp),
    ) -> anyhow::Result<()> {
        let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
        if !definitions.is_empty() {
            catalog.write_instruments(definitions).unwrap();
        }
        let slices = requirements.slices(from, to);
        let config = LseDataClientConfig::builder()
            .api_key(SecretString::from(API_KEY))
            .cme_api_key(SecretString::from(CME_API_KEY))
            .base_url_http(lse.url().to_string())
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
                    &requirements.custom,
                    from,
                    to,
                    config,
                    databento,
                )
                .await?;
                node.run(dir).await
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
    #[test]
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

    /// The captured five minutes of gold spot, candles LSE stamps at 10:00
    /// to 10:04, their open, as pyarrow reads them; the bars closing over
    /// `[10:01, 10:05]`.
    #[test]
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
            .bars(Some(vec![bars.clone()]), None, None)
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

    /// `id`'s definition, as one is written by hand: an index of LSE's
    /// symbol `raw`, its prices at `precision`, its sizes whole lots.
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

    /// A series of each derived market LSE stores trades of, its trade
    /// slice of 12 November captured from LSE, filled over the captured
    /// window under its `LSEDGE` id, as pyarrow reads LSE's file: the
    /// count, the first and last times, the total size and the lowest and
    /// highest prices. LSE is asked the slice of its market and symbol.
    /// Futures' are `the_trades_lse_exports_are_written_back_to_the_catalog`'s.
    #[test]
    fn the_trades_of_a_series_of_each_derived_market_fill_under_its_lsedge_id() {
        struct Case {
            id: &'static str,
            market: &'static str,
            symbol: &'static str,
            precision: u8,
            export: &'static [u8],
            window: (&'static str, &'static str),
            count: usize,
            times: (&'static str, &'static str),
            total_size: u64,
            prices: (&'static str, &'static str),
        }
        let cases = [
            Case {
                id: "AAPL-US_STOCKS.LSEDGE",
                market: "us_stocks",
                symbol: "AAPL",
                precision: 4,
                export: AAPL_TRADES,
                window: ("15:00:00", "15:00:00.999999999"),
                count: 54,
                times: ("15:00:00.00557", "15:00:00.968094"),
                total_size: 1829,
                prices: ("358.54", "358.91"),
            },
            Case {
                id: "VOD-UK_STOCKS.LSEDGE",
                market: "uk_stocks",
                symbol: "VOD",
                precision: 2,
                export: VOD_TRADES,
                window: ("10:00:00", "10:00:59.999999999"),
                count: 9,
                times: ("10:00:12.677", "10:00:35.279"),
                total_size: 33765,
                prices: ("117.8", "117.85"),
            },
            Case {
                id: "RY-CA_STOCKS.LSEDGE",
                market: "ca_stocks",
                symbol: "RY",
                precision: 2,
                export: RY_TRADES,
                window: ("15:00:00", "15:00:59.999999999"),
                count: 23,
                times: ("15:00:05.973", "15:00:36.614"),
                total_size: 3135,
                prices: ("288.65", "288.86"),
            },
            Case {
                id: "BTCUSDT-CRYPTO.LSEDGE",
                market: "crypto",
                symbol: "BTCUSDT",
                precision: 2,
                export: BTCUSDT_TRADES,
                window: ("10:00:00", "10:00:00.999999999"),
                count: 36,
                times: ("10:00:00.124", "10:00:00.124"),
                total_size: 831_000,
                prices: ("73496.29", "73497.39"),
            },
        ];
        for case in cases {
            let id = case.id;
            let dir = tempfile::tempdir().unwrap();
            let lse = MockLse::start(vec![Reply::file(case.export)]);
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
                vec![index(id, case.symbol, case.precision)],
                on_the_12th(case.window.0),
                on_the_12th(case.window.1),
            )
            .unwrap();

            let stored = ParquetDataCatalog::new(dir.path(), None, None, None, None)
                .trade_ticks(Some(vec![id.to_string()]), None, None)
                .unwrap();
            let at = |time| UnixNanos::from(on_the_12th(time));
            assert_eq!(stored.len(), case.count, "{id}");
            assert_eq!(
                (stored[0].ts_event, stored[stored.len() - 1].ts_event),
                (at(case.times.0), at(case.times.1)),
                "{id}"
            );
            assert_eq!(
                stored.iter().map(|t| t.size.as_f64()).sum::<f64>(),
                case.total_size as f64,
                "{id}"
            );
            let price = |price: &str| Price::from(price).as_f64();
            let (low, high) = stored.iter().fold((f64::MAX, f64::MIN), |(low, high), t| {
                (low.min(t.price.as_f64()), high.max(t.price.as_f64()))
            });
            assert_eq!(
                (low, high),
                (price(case.prices.0), price(case.prices.1)),
                "{id}"
            );
            let [export] = &lse.calls()[..] else {
                panic!("{id}: {:?}", lse.calls())
            };
            assert_eq!(
                (
                    export.param("market"),
                    export.param("symbol"),
                    export.param("timeframe")
                ),
                (Some(case.market), Some(case.symbol), Some("tick")),
                "{id}"
            );
        }
    }

    /// The forex pair and the index of LSE's 1-minute candles of 12
    /// November captured from LSE, filled as the bars closing over
    /// `[10:01, 10:05]` and `[15:01, 15:05]` under their `LSEDGE` ids, as
    /// pyarrow reads LSE's files: five bars, the first and the last of
    /// them stamped at their close. LSE stores no trades of forex, and an
    /// index has none. Commodities' bars are gold spot's
    /// (`lse_s_candles_are_written_back_to_the_catalog_as_bars_stamped_at_their_close`).
    #[test]
    fn the_bars_of_a_forex_pair_and_an_index_fill_under_their_lsedge_ids() {
        /// A bar's close, then its open, high, low and last prices.
        type Row = (&'static str, [&'static str; 4]);
        struct Case {
            id: &'static str,
            market: &'static str,
            symbol: &'static str,
            precision: u8,
            export: &'static [u8],
            window: (&'static str, &'static str),
            first: Row,
            last: Row,
        }
        let cases = [
            Case {
                id: "EUR_USD-FOREX.LSEDGE",
                market: "forex",
                symbol: "EUR_USD",
                precision: 5,
                export: EUR_USD_MINUTES,
                window: ("10:01:00", "10:05:00"),
                first: ("10:01:00", ["1.10942", "1.10946", "1.10941", "1.10944"]),
                last: ("10:05:00", ["1.10936", "1.10941", "1.10930", "1.10930"]),
            },
            Case {
                id: "SPX-INDICES.LSEDGE",
                market: "indices",
                symbol: "SPX",
                precision: 2,
                export: SPX_MINUTES,
                window: ("15:01:00", "15:05:00"),
                first: ("15:01:00", ["7600.07", "7600.07", "7589.80", "7590.37"]),
                last: ("15:05:00", ["7583.60", "7590.34", "7582.70", "7588.98"]),
            },
        ];
        for Case {
            id,
            market,
            symbol,
            precision,
            export,
            window,
            first,
            last,
        } in cases
        {
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
                .bars(Some(vec![bars.clone()]), None, None)
                .unwrap();
            let row = |b: &Bar| (b.ts_event, b.open, b.high, b.low, b.close, b.volume);
            let expected = |(close, [open, high, low, last]): Row| {
                (
                    UnixNanos::from(on_the_12th(close)),
                    Price::from(open),
                    Price::from(high),
                    Price::from(low),
                    Price::from(last),
                    Quantity::from(0),
                )
            };
            assert_eq!(stored.len(), 5, "{id}");
            assert_eq!(
                (row(&stored[0]), row(&stored[4])),
                (expected(first), expected(last)),
                "{id}"
            );
            let [export] = &lse.calls()[..] else {
                panic!("{id}: {:?}", lse.calls())
            };
            assert_eq!(
                (
                    export.param("market"),
                    export.param("symbol"),
                    export.param("timeframe")
                ),
                (Some(market), Some(symbol), Some("1m")),
                "{id}"
            );
        }
    }

    /// The S&P 500's tick slice of 12 November captured from LSE, its values
    /// of no size, filled as quotes over `[15:00:00, 15:00:05)` under its
    /// `LSEDGE` id, as pyarrow reads LSE's file: each a quote whose bid and
    /// ask are the value, with no sizes, as data-ws sends an index's ticks.
    /// LSE is asked the index's tick slice.
    #[test]
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

    #[test]
    fn lse_s_symbol_unknown_fails_the_fill_naming_the_request() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![Reply::refusal(404, SYMBOL_UNKNOWN)]);

        let err = fill(dir.path(), &lse, NOSUCH, vec![defined_as(NOSUCH)])
            .unwrap_err()
            .to_string();

        assert!(err.contains(&format!("the trades of {NOSUCH}")), "{err}");
        assert!(err.contains("symbol-unknown"), "{err}");
    }

    /// The definition request fails, LSE publishing none, and LSE is never
    /// asked.
    #[test]
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

    /// US10Y's daily bond yields, custom data of no instrument.
    fn us10y() -> DataType {
        LseRow::data_type("bond_yields", "US10Y")
    }

    /// fill-catalog's fill of `us10y()` over the rows of the captured
    /// days, the 9th to the 12th of November, known from the 10th's 00:00
    /// to the 13th's, into the catalog in `dir`, from `lse`.
    fn fill_bond_yields(dir: &Path, lse: &MockLse) -> anyhow::Result<()> {
        fill_bond_yields_over(dir, lse, november("10T00:00:00"), november("13T00:00:00"))
    }

    /// fill-catalog's fill of `us10y()` over `[from, to]` into the catalog
    /// in `dir`, from `lse`.
    fn fill_bond_yields_over(
        dir: &Path,
        lse: &MockLse,
        from: Timestamp,
        to: Timestamp,
    ) -> anyhow::Result<()> {
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![],
            custom: vec![us10y()],
            warmup: SignedDuration::ZERO,
        };
        fill_requirements(dir, lse, &requirements, vec![], from, to)
    }

    fn november(time: &str) -> Timestamp {
        format!("2026-11-{time}Z").parse().unwrap()
    }

    /// The captured rows, as pyarrow reads LSE's file: the 9th's, the
    /// 10th's and the 12th's, read back as Nautilus decodes custom data,
    /// each stamped at its date's end.
    #[test]
    fn the_bond_yields_lse_exports_are_written_back_as_custom_data_of_their_series() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![
            Reply::json(REFERENCE_CATALOGUE),
            Reply::file(US10Y_BOND_YIELDS),
        ]);

        fill_bond_yields(dir.path(), &lse).unwrap();

        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let identifier = us10y().identifier().unwrap().to_string();
        let stored = catalog
            .query_custom_data_dynamic("LseRow", Some(&[identifier]), None, None, None, None, true)
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
        let row = |day: &str, close: f64| {
            (
                us10y(),
                UnixNanos::from(november(&format!("{day}T00:00:00"))),
                serde_json::json!(close),
            )
        };
        assert_eq!(rows, [row("10", 4.884), row("11", 5.0), row("13", 5.133)]);
    }

    /// The window is stored whole, the days without a row too: a second
    /// fill asks LSE nothing.
    #[test]
    fn a_filled_window_of_bond_yields_is_not_asked_again() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![
            Reply::json(REFERENCE_CATALOGUE),
            Reply::file(US10Y_BOND_YIELDS),
        ]);
        fill_bond_yields(dir.path(), &lse).unwrap();

        fill_bond_yields(dir.path(), &lse).unwrap();

        assert_eq!(lse.calls().len(), 2, "{:?}", lse.calls());
    }

    /// The captured rows stored, known until the 13th's 00:00, then a
    /// window until now, whose last date, the day before's, is LSE's the
    /// morning after it (`lse`'s data client), and of which LSE holds no
    /// row yet. The fill ends without recording the window beside the
    /// stored rows, as an empty answer would be, so that the next run asks
    /// for it again.
    #[test]
    fn a_window_of_bond_yields_reaching_into_a_day_lse_has_not_loaded_is_asked_again() {
        let dir = tempfile::tempdir().unwrap();
        let stored = MockLse::start(vec![
            Reply::json(REFERENCE_CATALOGUE),
            Reply::file(US10Y_BOND_YIELDS),
        ]);
        fill_bond_yields(dir.path(), &stored).unwrap();
        let now = Timestamp::now();
        let not_loaded = || {
            MockLse::start(vec![
                Reply::json(REFERENCE_CATALOGUE),
                Reply::refusal(404, REFERENCE_DATA_UNAVAILABLE),
            ])
        };
        let first = not_loaded();
        fill_bond_yields_over(dir.path(), &first, november("10T00:00:00"), now).unwrap();

        let again = not_loaded();
        fill_bond_yields_over(dir.path(), &again, november("10T00:00:00"), now).unwrap();

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

    #[test]
    fn lse_s_refusal_fails_the_fill_with_its_code_message_and_required_plan() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![Reply::refusal(403, PLAN_REQUIRED)]);

        let err = fill(dir.path(), &lse, GOLD_FUTURES, vec![gold_futures()])
            .unwrap_err()
            .to_string();

        for part in [
            "plan-required",
            "This feed requires a subscription",
            "CME Group Lite",
        ] {
            assert!(err.contains(part), "{part}: {err}");
        }
    }
    /// The 21st is filled and the 22nd refused for a quota, which ends the
    /// fill with LSE's message. The next run asks for the 22nd alone, of
    /// which LSE holds no row as on a date with no session, and the run
    /// after asks for nothing.
    #[test]
    fn a_quota_stops_the_fill_with_lse_s_message_and_the_next_run_asks_only_what_is_missing() {
        let day = |time: &str| -> Timestamp { time.parse().unwrap() };
        let (from, to) = (
            day("2026-10-21T00:00:00Z"),
            day("2026-10-22T23:59:59.999999999Z"),
        );
        for quota in [ROWS_EXHAUSTED, ALLOWANCE_EXHAUSTED] {
            let dir = tempfile::tempdir().unwrap();
            // The first window ends at the first instant of the 22nd, which
            // its export takes.
            let refusing =
                MockLse::start(vec![Reply::file(GOLD_TRADES), Reply::refusal(429, quota)]);

            let err = fill_over(
                dir.path(),
                &refusing,
                GOLD_FUTURES,
                vec![gold_futures()],
                from,
                to,
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
            fill_over(dir.path(), &resuming, GOLD_FUTURES, vec![], from, to).unwrap();

            assert_eq!(
                asked(&resuming),
                [(
                    "2026-10-22T00:00:00.000000001Z".to_string(),
                    "2026-10-23T00:00:00Z".to_string()
                )]
            );

            let done = MockLse::start(vec![]);
            fill_over(dir.path(), &done, GOLD_FUTURES, vec![], from, to).unwrap();

            assert!(done.calls().is_empty(), "{:?}", done.calls());
        }
    }

    /// Trades stored until 20 hours ago, then a window until now, which
    /// reaches into the current UTC day, LSE storing a day the morning after
    /// it. The fill ends, but the window is not recorded as stored beside
    /// the stored trades, so that the next run asks for it again.
    #[test]
    fn a_window_lse_has_not_loaded_is_left_missing_for_the_next_run() {
        let dir = tempfile::tempdir().unwrap();
        let now = Timestamp::now();
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
        // The day before's part, if any, and the current day's.
        let not_loaded = || vec![Reply::refusal(404, DATA_UNAVAILABLE_TODAY); 2];
        let first = MockLse::start(not_loaded());

        fill_over(
            dir.path(),
            &first,
            GOLD_FUTURES,
            vec![gold_futures()],
            from,
            now,
        )
        .unwrap();

        let again = MockLse::start(not_loaded());
        fill_over(dir.path(), &again, GOLD_FUTURES, vec![], from, now).unwrap();
        let asked = asked(&again);
        assert_eq!(
            asked.first().map(|(from, _)| from.as_str()),
            Some(stored_to.to_string().as_str()),
            "{asked:?}"
        );
    }

    /// GCZ6 of 2026's trades and quotes over the captured second: its
    /// definition is Databento's, asked of Databento's client, which the
    /// mock Databento peer answers with Databento's records of GCZ6, and its
    /// trades and quotes LSE's, asked of LSE's client, which looks the
    /// contract up in LSE's catalogue and asks for its listing's rows with
    /// the CME key. Neither client claims a venue: each request reaches the
    /// client it names. The definition is the one Databento's client wrote
    /// into this repository's catalog before.
    #[test]
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

    /// GCZ6 of 2026's book depths and deltas over the captured second, as
    /// LSE's docs record its book and orders (the adapter's `test_data/`):
    /// asked of LSE's client by client id, its definition of Databento's,
    /// and written back to the catalog, each delta event ending `F_LAST`.
    #[test]
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

    /// AAPL's 355 call of 20 November 2026 over the captured minutes: its
    /// definition is Databento's, of `OPRA.PILLAR`, asked of Databento's
    /// client, which the mock Databento peer answers with Databento's record
    /// of the contract, and its trades and 1-minute bars LSE's, asked of
    /// LSE's client by the contract's OSI symbol with the LSE key, which
    /// opens LSE's US options. The captured minute of trades stands for the
    /// window's, and the four candles of 14:30 to 14:33 for the bars
    /// closing over it, LSE having none of 14:34.
    #[test]
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
        // 16:00 New York, the close of the day it expires.
        assert_eq!(
            (
                definition.raw_symbol.as_str(),
                definition.underlying.as_str(),
                definition.option_kind,
                definition.strike_price,
                Timestamp::from(definition.expiration_ns),
                definition.price_increment,
            ),
            (
                "AAPL  261120C00355000",
                "AAPL",
                OptionKind::Call,
                Price::from("355.00"),
                november("20T21:00:00"),
                Price::from("0.01"),
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
}
