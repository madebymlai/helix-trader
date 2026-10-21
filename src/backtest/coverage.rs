//! The check that the catalog can serve a backtest, run before the node is
//! built.
//!
//! A warmup window the catalog does not fully cover gets no answer at all
//! (see the README), so a run on missing data would go on silently with
//! cold indicators; without quotes, orders find no market and no bar is
//! built. Every instrument must be stored, and its quotes covered over
//! `[engine start − warmup, end]` by their stored intervals (inclusive
//! nanoseconds); the engine starts 1 ns before the run start. The gaps come
//! from `ParquetDataCatalog::get_missing_intervals_for_request`, which goes
//! through the same `missing_intervals` as the data engine's own catalog
//! lookup for the warmup: adjacent (`end + 1 == start`) and overlapping
//! intervals cover as one, a 1 ns gap is a gap.

use std::collections::BTreeSet;

use anyhow::{Context, bail, ensure};
use jiff::Timestamp;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::NautilusDataType,
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

use super::Backtest;

/// Fails unless the catalog of `backtest` stores every instrument and
/// covers its quotes over `[engine start − warmup, end]`, the engine
/// starting 1 ns before `start`; the error names the instrument, and for
/// quotes the first missing range.
pub(super) fn check(backtest: &Backtest) -> anyhow::Result<()> {
    ensure!(
        !backtest.warmup.is_negative(),
        "warmup {:#} is negative",
        backtest.warmup
    );
    let engine_start = backtest.engine_start()?;
    let warmup_start = engine_start
        .checked_sub(backtest.warmup)
        .with_context(|| format!("no warmup of {:#} before {engine_start}", backtest.warmup))?;
    let catalog = ParquetDataCatalog::new(&backtest.catalog, None, None, None, None);
    let ids = backtest.instruments();
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
    for id in &ids {
        ensure_quoted(&catalog, *id, warmup_start, backtest.end)?;
    }
    Ok(())
}

/// Fails unless `catalog` covers the quotes of `instrument_id` over
/// `[from, to]`; the error names the first missing range.
fn ensure_quoted(
    catalog: &ParquetDataCatalog,
    instrument_id: InstrumentId,
    from: Timestamp,
    to: Timestamp,
) -> anyhow::Result<()> {
    let missing = catalog
        .get_missing_intervals_for_request(
            UnixNanos::from(from).as_u64(),
            UnixNanos::from(to).as_u64(),
            &NautilusDataType::QuoteTick.into(),
            Some(&instrument_id.to_string()),
        )
        .with_context(|| format!("reading the stored range of the quotes of {instrument_id}"))?;
    if let Some(&(start, end)) = missing.first() {
        bail!(
            "the catalog does not cover the quotes of {instrument_id} over [{from}, {to}] \
             (the warmup and the run): [{}, {}] is missing",
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
        data::{BarType, QuoteTick},
        instruments::stubs::equity_aapl,
        types::{Money, Price, Quantity},
    };

    use super::*;
    use crate::backtest::commission_per_order;

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
            warmup: SignedDuration::from_hours(24),
            fee_model: commission_per_order(Money::from("3 USD")).unwrap(),
            balance: Money::from("10000 USD"),
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
}
