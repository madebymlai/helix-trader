//! LSE history of `LSEDGE` instruments, London Strategic Edge's own
//! derived series, each an index instrument named after its LSE market
//! (`GOLD-FUTURES.LSEDGE`: GOLD in `futures`), filled through the node (see
//! `catalog::NodeFill`) by the LSE adapter's data client (`lse`), whose
//! answers the data engine writes back to the catalog: trades, from LSE's
//! tick slices, and bars, from its candles, each stamped at its close.
//! Their definitions are the catalog's alone, written there by
//! hand: LSE publishes none, the data engine answers a definition request
//! from the catalog before asking the client, and the client converts
//! LSE's prices at the definition's precision. LSE's reference series, the
//! rows of its reference collections, are custom data of no instrument
//! (`lse::LseRow`), filled by the same client with no definition. See
//! `README.md`.

use ::lse::{LseDataClientConfig, LseDataClientFactory};
use jiff::{SignedDuration, Timestamp};
use nautilus_model::data::DataType;
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

use crate::{
    catalog::{FillData, NodeClient, NodeFill, missing, missing_custom},
    strategy::SeriesSlice,
};

/// The window of one request of a series: a day, as Databento's. The
/// client asks LSE a UTC day an export whatever the window.
const DAY: SignedDuration = SignedDuration::from_hours(24);

/// Adds to `node` the fill of what the catalog lacks of `slices`, series of
/// `LSEDGE` instruments (see `catalog::missing`), and of `custom`, LSE's
/// rows (`lse::LseRow`) over `[start, end]` (see `catalog::missing_custom`),
/// asked of the LSE client `config` configures, its key read only when the
/// node creates it, so only if something is missing. Each contract's
/// definition is asked of the client too: the data engine answers it from
/// the catalog, before the client, and puts it in the cache, where the
/// client reads the precision of its prices; a definition the catalog lacks
/// leaves the series' request to fail naming the instrument. LSE's rows are
/// of no instrument and have none.
///
/// # Errors
///
/// Returns an error if a series is not one the node fills (see `missing`),
/// if a custom series has no identifier, or if the catalog cannot be read.
pub(super) fn add_fill(
    node: &mut NodeFill,
    catalog: &ParquetDataCatalog,
    slices: &[SeriesSlice],
    custom: &[DataType],
    start: Timestamp,
    end: Timestamp,
    config: LseDataClientConfig,
) -> anyhow::Result<()> {
    let requests = missing(catalog, slices)?;
    let custom = missing_custom(catalog, custom, start, end)?;
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

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, path::Path};

    use ::lse::{
        LseRow,
        testing::{
            ALLOWANCE_EXHAUSTED, API_KEY, DATA_UNAVAILABLE_TODAY, DATE_NOT_TRADING, GOLD_FUTURES,
            GOLD_SPOT, GOLD_SPOT_MINUTES, GOLD_TRADES, MockLse, PLAN_REQUIRED, REFERENCE_CATALOGUE,
            REFERENCE_DATA_UNAVAILABLE, ROWS_EXHAUSTED, Reply, SYMBOL_UNKNOWN, US10Y_BOND_YIELDS,
            gold_futures, gold_spot,
        },
    };
    use jiff::Timestamp;
    use nautilus_core::{UnixNanos, string::secret::SecretString};
    use nautilus_model::{
        data::{BarType, Data, DataType, NautilusDataType, TradeTick},
        enums::AggressorSide,
        identifiers::{InstrumentId, TradeId},
        instruments::InstrumentAny,
        types::{Price, Quantity},
    };

    use super::*;
    use crate::strategy::{DataRequirements, TickSeries};

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
        let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
        if !definitions.is_empty() {
            catalog.write_instruments(definitions).unwrap();
        }
        let slices = requirements.slices(from, to);
        let config = LseDataClientConfig::builder()
            .api_key(SecretString::from(API_KEY))
            .base_url_http(lse.url().to_string())
            .build();
        let mut node = NodeFill::new();
        add_fill(
            &mut node,
            &catalog,
            &slices,
            &requirements.custom,
            from,
            to,
            config,
        )?;
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(node.run(dir))
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

    /// LSE is never asked: its prices could not be read.
    #[test]
    fn a_fill_of_an_lsedge_instrument_with_no_definition_fails_naming_it() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);

        let err = fill(dir.path(), &lse, GOLD_FUTURES, vec![])
            .unwrap_err()
            .to_string();

        assert!(
            err.contains(&format!("{GOLD_FUTURES} is not in the cache")),
            "{err}"
        );
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
            let refusing = MockLse::start(vec![
                Reply::file(GOLD_TRADES),
                // The first window ends at the first instant of the 22nd.
                Reply::refusal(404, DATE_NOT_TRADING),
                Reply::refusal(429, quota),
            ]);

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
}
