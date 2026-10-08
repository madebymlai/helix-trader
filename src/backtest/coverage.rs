//! The check that the catalog can serve a backtest, run before the node is
//! built.
//!
//! A warmup window the catalog does not fully cover gets no answer at all
//! (see the README), so a run on missing data would go on silently with
//! cold indicators. Every instrument must be stored, and every bar type
//! covered over `[start − warmup, end]` by its stored intervals
//! (`ParquetDataCatalog::get_intervals`, inclusive nanoseconds). The gaps
//! come from Nautilus's `missing_intervals`, which the data engine's own
//! catalog lookup for the warmup goes through: adjacent (`end + 1 ==
//! start`) and overlapping intervals cover as one, a 1 ns gap is a gap.

use std::collections::BTreeSet;

use anyhow::{Context, bail, ensure};
use jiff::Timestamp;
use nautilus_core::{ClosedInterval, UnixNanos};
use nautilus_model::{
    data::NautilusDataType,
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, common::coverage::missing_intervals,
};

use super::Backtest;
use crate::data::to_unix_nanos;

/// Fails unless the catalog of `backtest` stores every instrument of its
/// bar types and covers every bar type over `[start − warmup, end]`; the
/// error names the instrument, or the bar type and its first missing range.
pub(super) fn check(backtest: &Backtest) -> anyhow::Result<()> {
    ensure!(
        !backtest.warmup.is_negative(),
        "warmup {:#} is negative",
        backtest.warmup
    );
    let warmup_start = backtest
        .start
        .checked_sub(backtest.warmup)
        .with_context(|| {
            format!(
                "no warmup of {:#} before {}",
                backtest.warmup, backtest.start
            )
        })?;
    let from = to_unix_nanos(warmup_start)?.as_u64();
    let to = to_unix_nanos(backtest.end)?.as_u64();
    let catalog = ParquetDataCatalog::new(&backtest.catalog, None, None, None, None);
    let ids: BTreeSet<InstrumentId> = backtest
        .bar_types
        .iter()
        .map(|bt| bt.instrument_id())
        .collect();
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
    for bar_type in &backtest.bar_types {
        let intervals = catalog
            .get_intervals(&NautilusDataType::Bar.into(), Some(&bar_type.to_string()))
            .with_context(|| format!("reading the stored range of {bar_type}"))?;
        if let Some(gap) = first_missing(from, to, &intervals) {
            bail!(
                "the catalog does not cover {bar_type} over [{warmup_start}, {}] \
                 (start - warmup to end): [{}, {}] is missing",
                backtest.end,
                timestamp(gap.start),
                timestamp(gap.end),
            );
        }
    }
    Ok(())
}

/// The first part of `[from, to]` (inclusive nanoseconds) that `stored`
/// does not cover, if any.
fn first_missing(from: u64, to: u64, stored: &[(u64, u64)]) -> Option<ClosedInterval> {
    let stored: Vec<ClosedInterval> = stored
        .iter()
        .filter_map(|&(start, end)| ClosedInterval::new(start, end))
        .collect();
    missing_intervals(from, to, &stored).into_iter().next()
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
        data::{Bar, BarType},
        instruments::stubs::equity_aapl,
        types::{Money, Price, Quantity},
    };

    use super::*;

    fn missing(from: u64, to: u64, stored: &[(u64, u64)]) -> Option<(u64, u64)> {
        first_missing(from, to, stored).map(Into::into)
    }

    #[test]
    fn one_interval_over_the_window_covers_it() {
        assert_eq!(missing(100, 200, &[(50, 250)]), None);
    }

    #[test]
    fn an_interval_from_the_first_to_the_last_nanosecond_covers_the_window() {
        assert_eq!(missing(100, 200, &[(100, 200)]), None);
    }

    #[test]
    fn a_gap_at_the_warmup_start_is_missing() {
        assert_eq!(missing(100, 200, &[(101, 250)]), Some((100, 100)));
    }

    #[test]
    fn a_gap_at_the_end_is_missing() {
        assert_eq!(missing(100, 200, &[(50, 199)]), Some((200, 200)));
    }

    #[test]
    fn a_gap_in_the_middle_is_missing() {
        assert_eq!(
            missing(100, 200, &[(50, 120), (150, 250)]),
            Some((121, 149))
        );
    }

    #[test]
    fn a_gap_of_one_nanosecond_is_missing() {
        assert_eq!(
            missing(100, 200, &[(50, 120), (122, 250)]),
            Some((121, 121))
        );
    }

    #[test]
    fn adjacent_intervals_cover_as_one() {
        assert_eq!(missing(100, 200, &[(50, 120), (121, 250)]), None);
    }

    #[test]
    fn overlapping_intervals_cover_as_one() {
        assert_eq!(missing(100, 200, &[(130, 250), (50, 160)]), None);
    }

    #[test]
    fn of_several_gaps_the_first_is_named() {
        assert_eq!(
            missing(100, 200, &[(110, 120), (130, 140)]),
            Some((100, 109))
        );
    }

    #[test]
    fn no_stored_interval_misses_the_whole_window() {
        assert_eq!(missing(100, 200, &[]), Some((100, 200)));
    }

    const HOUR: &str = "AAPL.XNAS-1-HOUR-LAST-EXTERNAL";

    fn ts(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    fn ns(s: &str) -> UnixNanos {
        to_unix_nanos(ts(s)).unwrap()
    }

    /// A catalog holding AAPL.XNAS and, per bar type, one bar stored over
    /// each given window.
    fn catalog(dir: &Path, stored: &[(&str, &str, &str)]) {
        let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
        catalog
            .write_instruments(vec![InstrumentAny::Equity(equity_aapl())])
            .unwrap();
        for &(bar_type, from, to) in stored {
            let p = Price::from("100.00");
            let bar = Bar::new(
                BarType::from(bar_type),
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
    }

    /// A backtest of `bar_types` over October 2026 after one day of warmup.
    fn backtest(dir: &Path, bar_types: &[&str]) -> Backtest {
        Backtest {
            catalog: dir.to_path_buf(),
            bar_types: bar_types.iter().map(|&bt| BarType::from(bt)).collect(),
            start: ts("2026-10-02T00:00:00Z"),
            end: ts("2026-10-31T00:00:00Z"),
            warmup: SignedDuration::from_hours(24),
            commission: Money::from("3 USD"),
            balance: Money::from("10000 USD"),
            logging: LoggerConfig::default(),
        }
    }

    #[test]
    fn a_catalog_covering_from_the_warmup_start_to_the_end_passes() {
        let dir = tempfile::tempdir().unwrap();
        catalog(
            dir.path(),
            &[(HOUR, "2026-10-01T00:00:00Z", "2026-10-31T00:00:00Z")],
        );
        check(&backtest(dir.path(), &[HOUR])).unwrap();
    }

    #[test]
    fn a_catalog_from_one_nanosecond_after_the_warmup_start_misses_it() {
        let dir = tempfile::tempdir().unwrap();
        catalog(
            dir.path(),
            &[(
                HOUR,
                "2026-10-01T00:00:00.000000001Z",
                "2026-10-31T00:00:00Z",
            )],
        );
        let err = check(&backtest(dir.path(), &[HOUR])).unwrap_err();
        let err = err.to_string();
        assert!(err.contains(HOUR), "{err}");
        assert!(
            err.contains("[2026-10-01T00:00:00Z, 2026-10-01T00:00:00Z] is missing"),
            "{err}"
        );
    }

    #[test]
    fn a_catalog_ending_one_nanosecond_before_the_end_misses_it() {
        let dir = tempfile::tempdir().unwrap();
        catalog(
            dir.path(),
            &[(
                HOUR,
                "2026-10-01T00:00:00Z",
                "2026-10-30T23:59:59.999999999Z",
            )],
        );
        let err = check(&backtest(dir.path(), &[HOUR])).unwrap_err();
        let err = err.to_string();
        assert!(
            err.contains("[2026-10-31T00:00:00Z, 2026-10-31T00:00:00Z] is missing"),
            "{err}"
        );
    }

    #[test]
    fn a_bar_type_not_stored_misses_the_whole_window() {
        let dir = tempfile::tempdir().unwrap();
        let day = "AAPL.XNAS-1-DAY-LAST-EXTERNAL";
        catalog(
            dir.path(),
            &[(day, "2026-10-01T00:00:00Z", "2026-10-31T00:00:00Z")],
        );
        let err = check(&backtest(dir.path(), &[HOUR])).unwrap_err();
        let err = err.to_string();
        assert!(err.contains(HOUR), "{err}");
        assert!(
            err.contains("[2026-10-01T00:00:00Z, 2026-10-31T00:00:00Z] is missing"),
            "{err}"
        );
    }

    #[test]
    fn every_bar_type_is_checked() {
        let dir = tempfile::tempdir().unwrap();
        let day = "AAPL.XNAS-1-DAY-LAST-EXTERNAL";
        catalog(
            dir.path(),
            &[(HOUR, "2026-10-01T00:00:00Z", "2026-10-31T00:00:00Z")],
        );
        let err = check(&backtest(dir.path(), &[HOUR, day])).unwrap_err();
        assert!(err.to_string().contains(day), "{err}");
    }

    #[test]
    fn an_instrument_not_stored_fails_with_its_id() {
        let dir = tempfile::tempdir().unwrap();
        let msft = "MSFT.XNAS-1-HOUR-LAST-EXTERNAL";
        catalog(
            dir.path(),
            &[
                (HOUR, "2026-10-01T00:00:00Z", "2026-10-31T00:00:00Z"),
                (msft, "2026-10-01T00:00:00Z", "2026-10-31T00:00:00Z"),
            ],
        );
        let err = check(&backtest(dir.path(), &[HOUR, msft])).unwrap_err();
        let err = err.to_string();
        assert!(err.contains("MSFT.XNAS"), "{err}");
        assert!(!err.contains(msft), "{err}");
    }

    #[test]
    fn a_negative_warmup_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        catalog(
            dir.path(),
            &[(HOUR, "2026-10-01T00:00:00Z", "2026-10-31T00:00:00Z")],
        );
        let backtest = Backtest {
            warmup: SignedDuration::from_hours(-1),
            ..backtest(dir.path(), &[HOUR])
        };
        let err = check(&backtest).unwrap_err();
        assert!(err.to_string().contains("warmup"), "{err}");
    }
}
