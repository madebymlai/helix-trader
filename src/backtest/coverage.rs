//! The check that the catalog can serve a backtest, run before the node is
//! built.
//!
//! A warmup window the catalog does not fully cover gets no answer at all
//! (see the README), so a run on missing data would go on silently with
//! cold indicators; without its data, orders find no market and no bar is
//! built. Every instrument replayed must be stored (the contracts of a
//! root, never the root), and each series the run replays (see
//! `Backtest::series`: those the bars are built from, the declared ticks,
//! the stored book depth and the stored funding of the instruments)
//! covered over its slice of `[engine start − warmup, end]` by its stored
//! intervals (inclusive nanoseconds): the whole of it for a plain
//! instrument, the part its contract stands for a root otherwise; the
//! engine starts 1 ns before the run start. The gaps come from
//! `ParquetDataCatalog::get_missing_intervals_for_request`, which goes
//! through the same `missing_intervals` as the data engine's own catalog
//! lookup for the warmup: adjacent (`end + 1 == start`) and overlapping
//! intervals cover as one, a 1 ns gap is a gap.

use std::collections::BTreeSet;

use anyhow::{Context, bail};
use jiff::Timestamp;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::NautilusDataType,
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, catalog::types::data_path_prefix,
};

use super::Backtest;
use crate::strategy::SeriesSlice;

/// Fails unless the catalog of `backtest` stores every instrument replayed
/// and covers each series over its slice of `[engine start − warmup, end]`,
/// the engine starting 1 ns before `start`; the error names the
/// instrument, or the series and its first missing range.
pub(super) fn check(backtest: &Backtest) -> anyhow::Result<()> {
    let catalog = ParquetDataCatalog::new(&backtest.catalog, None, None, None, None);
    let ids = backtest.instruments()?;
    let names: Vec<String> = ids.iter().map(ToString::to_string).collect();
    let stored: BTreeSet<InstrumentId> = catalog
        .instruments(Some(&names), None, None)
        .context("reading the stored instruments")?
        .iter()
        .map(InstrumentAny::id)
        .collect();
    if let Some(id) = ids.difference(&stored).next() {
        bail!(
            "instrument {id} is not stored in the catalog {}",
            backtest.catalog.display()
        );
    }
    for SeriesSlice {
        series: (data_type, identifier),
        slice,
    } in backtest.series()?
    {
        ensure_covered(&catalog, &data_type, &identifier, slice.from, slice.to)?;
    }
    Ok(())
}

/// Fails unless `catalog` covers the series of `data_type` and `identifier`
/// over `[from, to]`; the error names the first missing range.
fn ensure_covered(
    catalog: &ParquetDataCatalog,
    data_type: &NautilusDataType,
    identifier: &str,
    from: Timestamp,
    to: Timestamp,
) -> anyhow::Result<()> {
    let named = format!("the {} of {identifier}", data_path_prefix(data_type));
    let missing = catalog
        .get_missing_intervals_for_request(
            UnixNanos::from(from).as_u64(),
            UnixNanos::from(to).as_u64(),
            &data_type.clone().into(),
            Some(identifier),
        )
        .with_context(|| format!("reading the stored range of {named}"))?;
    if let Some(&(start, end)) = missing.first() {
        bail!(
            "the catalog does not cover {named} over [{from}, {to}] \
             (its slice of the warmup and the run): [{}, {}] is missing",
            timestamp(start),
            timestamp(end),
        );
    }
    Ok(())
}

fn timestamp(ns: u64) -> Timestamp {
    UnixNanos::from(ns).into()
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use jiff::SignedDuration;
    use nautilus_common::logging::logger::LoggerConfig;
    use nautilus_model::{
        data::{Bar, BarType, FundingRateUpdate, QuoteTick, stubs::stub_depth10},
        enums::OmsType,
        instruments::stubs::equity_aapl,
        types::{Money, Price, Quantity},
    };
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        backtest::commission_per_order,
        strategy::{TickData, TickSeries},
    };

    fn ts(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    fn ns(s: &str) -> UnixNanos {
        UnixNanos::from(ts(s))
    }

    /// The quotes of AAPL.XNAS from the warmup start to the end.
    const QUOTED: &[(&str, &str)] = &[("2026-10-01T00:00:00Z", "2026-10-31T00:00:00Z")];

    /// A catalog holding AAPL.XNAS and one quote of it stored over each
    /// window of `quotes`.
    fn catalog(dir: &Path, quotes: &[(&str, &str)]) {
        let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
        let aapl = equity_aapl();
        for &(from, to) in quotes {
            let (p, q) = (Price::from("100.00"), Quantity::from(1));
            let quote = QuoteTick::new(aapl.id, p, p, q, q, ns(from), ns(from));
            catalog
                .write_to_parquet(&[quote], Some(ns(from)), Some(ns(to)), None)
                .unwrap();
        }
        catalog
            .write_instruments(vec![InstrumentAny::Equity(aapl)])
            .unwrap();
    }

    /// A backtest on the hour bars of `instruments` over October 2026 after one day of
    /// warmup, starting 1 ns after midnight, so that the engine starts at
    /// midnight on 2 October and the warmup at midnight on 1 October.
    fn backtest(dir: &Path, instruments: &[&str]) -> Backtest {
        Backtest {
            catalog: dir.to_path_buf(),
            bar_types: instruments
                .iter()
                .map(|&id| BarType::from(format!("{id}-1-HOUR-MID-INTERNAL").as_str()))
                .collect(),
            start: ts("2026-10-02T00:00:00.000000001Z"),
            end: ts("2026-10-31T00:00:00Z"),
            ticks: vec![],
            warmup: SignedDuration::from_hours(24),
            fee_model: commission_per_order(Money::from("3 USD")).unwrap(),
            balance: Money::from("10000 USD"),
            leverage: Decimal::ONE,
            venue_oms: |_| OmsType::Netting,
            logging: LoggerConfig::default(),
        }
    }

    const AAPL: &str = "AAPL.XNAS";

    /// The error of a backtest of AAPL.XNAS on a catalog with `quotes`.
    fn quotes_error(quotes: &[(&str, &str)]) -> String {
        let dir = tempfile::tempdir().unwrap();
        catalog(dir.path(), quotes);
        check(&backtest(dir.path(), &[AAPL]))
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn quotes_from_the_warmup_start_to_the_end_pass() {
        let dir = tempfile::tempdir().unwrap();
        catalog(dir.path(), QUOTED);
        check(&backtest(dir.path(), &[AAPL])).unwrap();
    }

    #[test]
    fn quotes_from_one_nanosecond_after_the_warmup_start_miss_it() {
        let err = quotes_error(&[("2026-10-01T00:00:00.000000001Z", "2026-10-31T00:00:00Z")]);
        assert!(err.contains("quotes of AAPL.XNAS"), "{err}");
        assert!(
            err.contains("[2026-10-01T00:00:00Z, 2026-10-01T00:00:00Z] is missing"),
            "{err}"
        );
    }

    #[test]
    fn quotes_ending_one_nanosecond_before_the_end_miss_it() {
        let err = quotes_error(&[("2026-10-01T00:00:00Z", "2026-10-30T23:59:59.999999999Z")]);
        assert!(
            err.contains("[2026-10-31T00:00:00Z, 2026-10-31T00:00:00Z] is missing"),
            "{err}"
        );
    }

    #[test]
    fn no_quotes_stored_miss_the_whole_window_naming_the_instrument() {
        let err = quotes_error(&[]);
        assert!(err.contains("quotes of AAPL.XNAS"), "{err}");
        assert!(
            err.contains("[2026-10-01T00:00:00Z, 2026-10-31T00:00:00Z] is missing"),
            "{err}"
        );
    }

    #[test]
    fn a_gap_in_the_quotes_is_missing() {
        let err = quotes_error(&[
            ("2026-10-01T00:00:00Z", "2026-10-10T00:00:00Z"),
            ("2026-10-11T00:00:00Z", "2026-10-31T00:00:00Z"),
        ]);
        assert!(
            err.contains(
                "[2026-10-10T00:00:00.000000001Z, 2026-10-10T23:59:59.999999999Z] is missing"
            ),
            "{err}"
        );
    }

    #[test]
    fn an_instrument_not_stored_fails_with_its_id() {
        let dir = tempfile::tempdir().unwrap();
        catalog(dir.path(), QUOTED);
        let err = check(&backtest(dir.path(), &[AAPL, "MSFT.XNAS"])).unwrap_err();
        let err = err.to_string();
        assert!(err.contains("instrument MSFT.XNAS is not stored"), "{err}");
    }

    #[test]
    fn a_negative_warmup_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        catalog(dir.path(), QUOTED);
        let backtest = Backtest {
            warmup: SignedDuration::from_hours(-1),
            ..backtest(dir.path(), &[AAPL])
        };
        let err = check(&backtest).unwrap_err();
        assert!(err.to_string().contains("warmup"), "{err}");
    }

    const MINUTE_BARS: &str = "AAPL.XNAS-1-MINUTE-LAST-EXTERNAL";

    /// A backtest of AAPL.XNAS on hour bars built from its external minute
    /// bars, over the window of `backtest`.
    fn composite_backtest(dir: &Path) -> Backtest {
        Backtest {
            bar_types: vec![BarType::from(
                "AAPL.XNAS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
            )],
            ..backtest(dir, &[AAPL])
        }
    }

    /// A catalog holding AAPL.XNAS and one minute bar stored over each
    /// window of `bars`.
    fn bar_catalog(dir: &Path, bars: &[(&str, &str)]) {
        let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
        for &(from, to) in bars {
            let p = Price::from("100.00");
            let bar = Bar::new(
                BarType::from(MINUTE_BARS),
                p,
                p,
                p,
                p,
                Quantity::from(1),
                ns(from),
                ns(from),
            );
            catalog
                .write_to_parquet(&[bar], Some(ns(from)), Some(ns(to)), None)
                .unwrap();
        }
        catalog
            .write_instruments(vec![InstrumentAny::Equity(equity_aapl())])
            .unwrap();
    }

    #[test]
    fn composite_bars_need_their_source_bars_from_the_warmup_start_to_the_end() {
        let dir = tempfile::tempdir().unwrap();
        bar_catalog(dir.path(), QUOTED);
        check(&composite_backtest(dir.path())).unwrap();
    }

    #[test]
    fn missing_source_bars_are_named_with_their_range_and_quotes_are_not_asked_for() {
        let dir = tempfile::tempdir().unwrap();
        bar_catalog(
            dir.path(),
            &[("2026-10-01T00:00:00Z", "2026-10-30T00:00:00Z")],
        );
        let err = check(&composite_backtest(dir.path()))
            .unwrap_err()
            .to_string();
        assert!(err.contains(&format!("bars of {MINUTE_BARS}")), "{err}");
        assert!(!err.contains("quotes"), "{err}");
        assert!(
            err.contains("[2026-10-30T00:00:00.000000001Z, 2026-10-31T00:00:00Z] is missing"),
            "{err}"
        );
    }

    /// Stores one funding rate of AAPL.XNAS over `[from, to]`.
    fn store_funding(dir: &Path, from: &str, to: &str) {
        let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
        let funding = FundingRateUpdate::new(
            equity_aapl().id,
            Decimal::new(1, 4),
            Some(480),
            None,
            ns(from),
            ns(from),
        );
        catalog
            .write_to_parquet(&[funding], Some(ns(from)), Some(ns(to)), None)
            .unwrap();
    }

    #[test]
    fn stored_funding_covering_the_window_passes() {
        let dir = tempfile::tempdir().unwrap();
        catalog(dir.path(), QUOTED);
        store_funding(dir.path(), "2026-10-01T00:00:00Z", "2026-10-31T00:00:00Z");
        check(&backtest(dir.path(), &[AAPL])).unwrap();
    }

    #[test]
    fn stored_funding_short_of_the_window_is_named_with_its_missing_range() {
        let dir = tempfile::tempdir().unwrap();
        catalog(dir.path(), QUOTED);
        store_funding(
            dir.path(),
            "2026-10-01T00:00:00Z",
            "2026-10-30T15:59:59.999999999Z",
        );
        let err = check(&backtest(dir.path(), &[AAPL]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("funding_rates of AAPL.XNAS"), "{err}");
        assert!(
            err.contains("[2026-10-30T16:00:00Z, 2026-10-31T00:00:00Z] is missing"),
            "{err}"
        );
    }

    /// Stores one book depth snapshot of AAPL.XNAS over `[from, to]`.
    fn store_book(dir: &Path, from: &str, to: &str) {
        let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
        let mut depth = stub_depth10();
        (depth.instrument_id, depth.ts_event, depth.ts_init) =
            (equity_aapl().id, ns(from), ns(from));
        catalog
            .write_to_parquet(&[depth], Some(ns(from)), Some(ns(to)), None)
            .unwrap();
    }

    #[test]
    fn a_stored_book_short_of_the_window_is_named_with_its_missing_range() {
        let dir = tempfile::tempdir().unwrap();
        catalog(dir.path(), QUOTED);
        store_book(
            dir.path(),
            "2026-10-01T00:00:00Z",
            "2026-10-30T23:59:59.999999999Z",
        );
        let err = check(&backtest(dir.path(), &[AAPL]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("order_book_depths of AAPL.XNAS"), "{err}");
        assert!(
            err.contains("[2026-10-31T00:00:00Z, 2026-10-31T00:00:00Z] is missing"),
            "{err}"
        );
    }

    #[test]
    fn a_stored_book_without_quotes_covering_the_window_passes() {
        let dir = tempfile::tempdir().unwrap();
        catalog(dir.path(), &[]);
        store_book(dir.path(), "2026-10-01T00:00:00Z", "2026-10-31T00:00:00Z");
        check(&backtest(dir.path(), &[AAPL])).unwrap();
    }

    /// A backtest of AAPL.XNAS reading its trades and no bars.
    fn trades_backtest(dir: &Path) -> Backtest {
        Backtest {
            bar_types: vec![],
            ticks: vec![TickSeries {
                data: TickData::Trades,
                instrument_id: equity_aapl().id,
            }],
            ..backtest(dir, &[])
        }
    }

    #[test]
    fn declared_ticks_not_stored_are_named_with_their_missing_range() {
        let dir = tempfile::tempdir().unwrap();
        catalog(dir.path(), QUOTED);
        let err = check(&trades_backtest(dir.path())).unwrap_err().to_string();
        assert!(err.contains("trades of AAPL.XNAS"), "{err}");
        assert!(
            err.contains("[2026-10-01T00:00:00Z, 2026-10-31T00:00:00Z] is missing"),
            "{err}"
        );
    }

    #[test]
    fn the_instrument_of_declared_ticks_must_be_stored() {
        let dir = tempfile::tempdir().unwrap();
        let err = check(&trades_backtest(dir.path())).unwrap_err().to_string();
        assert!(err.contains("instrument AAPL.XNAS is not stored"), "{err}");
    }
}
