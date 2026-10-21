//! Fills the catalog with the history of the given instruments: the
//! instruments and their quotes.
//!
//! Source-agnostic, no node: the quotes come from a fetch function the
//! caller passes, backed by an adapter's historical client (`ib.rs`). Per
//! instrument it resumes after its stored range of quotes
//! (`query_first_timestamp`, `query_last_timestamp`), checks the response,
//! sorts it by `ts_init` and writes it with `write_instruments` and
//! `write_to_parquet` on a `ParquetDataCatalog` (`nautilus_persistence`).
//! Nautilus does not persist requested data on its own.

use std::collections::HashSet;

use anyhow::{Context, bail};
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::QuoteTick,
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, catalog::types::HasCatalogDataType,
};

/// Fills `catalog` with `instruments`, then over `[start, end]` with the
/// quotes of each instrument, fetched one instrument at a time through
/// `fetch_quotes`.
///
/// Instruments already in the catalog are not written again. An
/// instrument's quotes are stored over the windows asked for (widened to
/// the oldest and newest quote). Once stored, they resume 1 ns after the
/// end of that range, whatever `start` (no hole between the stored range
/// and `start`): a covered window asks for nothing, and returned quotes at
/// or before that point are dropped. A `start` before the stored range is
/// an error, raised before fetching: extending history backwards is not
/// supported.
///
/// A fetch over `(from, to]` returns the quotes with `ts_init` in it. Any
/// error stops the run; instruments filled before it stay stored. An empty
/// response on an instrument's first fill, or a quote of another
/// instrument, is an error. Once an instrument's quotes are stored, an
/// empty response, or one of only already stored quotes, means nothing
/// new: nothing is written and the next instruments run. Quotes are stored
/// sorted by `ts_init`, duplicates dropped.
pub async fn fill_catalog(
    catalog: &mut ParquetDataCatalog,
    instruments: Vec<InstrumentAny>,
    start: Timestamp,
    end: Timestamp,
    mut fetch_quotes: impl AsyncFnMut(
        InstrumentId,
        Timestamp,
        Timestamp,
    ) -> anyhow::Result<Vec<QuoteTick>>,
) -> anyhow::Result<()> {
    let instrument_ids: Vec<InstrumentId> = instruments.iter().map(Instrument::id).collect();
    let ids: Vec<String> = instrument_ids.iter().map(ToString::to_string).collect();
    let stored: HashSet<InstrumentId> = catalog
        .instruments(Some(&ids), None, None)
        .context("reading stored instruments")?
        .iter()
        .map(Instrument::id)
        .collect();
    let new = instruments
        .into_iter()
        .filter(|i| !stored.contains(&i.id()))
        .collect();
    catalog
        .write_instruments(new)
        .context("writing instruments")?;
    let start_ns = UnixNanos::from(start);
    let end_ns = UnixNanos::from(end);
    let quotes_type = QuoteTick::catalog_data_type().into();
    for id in instrument_ids {
        let identifier = id.to_string();
        let reading = || format!("reading the stored range of the quotes of {id}");
        let first = catalog
            .query_first_timestamp(&quotes_type, Some(&identifier))
            .with_context(reading)?;
        let last = catalog
            .query_last_timestamp(&quotes_type, Some(&identifier))
            .with_context(reading)?;
        if let (Some(first), Some(last)) = (first, last)
            && start_ns < UnixNanos::from(first)
        {
            bail!(
                "the quotes of {id} are stored over [{}, {}] and {start} is before it: \
                 extending history backwards is not supported; use a new catalog",
                UnixNanos::from(first),
                UnixNanos::from(last),
            );
        }
        let resume_point = last.map(UnixNanos::from);
        let from = match resume_point {
            Some(last) => just_after(last),
            None => start,
        };
        if from >= end {
            continue;
        }
        let mut quotes = fetch_quotes(id, from, end)
            .await
            .with_context(|| format!("fetching the quotes of {id}"))?;
        if quotes.is_empty() && resume_point.is_none() {
            bail!("empty response for the quotes of {id}");
        }
        if let Some(foreign) = quotes.iter().find(|quote| quote.instrument_id != id) {
            bail!(
                "response for the quotes of {id} holds quotes of {}",
                foreign.instrument_id
            );
        }
        if let Some(last) = resume_point {
            quotes.retain(|quote| quote.ts_init > last);
        }
        quotes.sort_by_key(|quote| quote.ts_init);
        quotes.dedup_by_key(|quote| quote.ts_init);
        let (Some(oldest), Some(newest)) = (quotes.first(), quotes.last()) else {
            continue;
        };
        let covered_from = UnixNanos::from(from).min(oldest.ts_init);
        let covered_to = end_ns.max(newest.ts_init);
        catalog
            .write_to_parquet(&quotes, Some(covered_from), Some(covered_to), None)
            .with_context(|| format!("writing the quotes of {id}"))?;
    }
    Ok(())
}

/// The first nanosecond after `last`; jiff reaches past any `UnixNanos`,
/// so even `u64::MAX` has one.
fn just_after(last: UnixNanos) -> Timestamp {
    Timestamp::from(last) + SignedDuration::from_nanos(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_model::{
        instruments::stubs::{equity_aapl, futures_contract_es},
        types::{Price, Quantity},
    };

    fn instrument() -> InstrumentAny {
        InstrumentAny::FuturesContract(futures_contract_es(None, None))
    }

    fn es_id() -> InstrumentId {
        instrument().id()
    }

    fn aapl() -> InstrumentAny {
        InstrumentAny::Equity(equity_aapl())
    }

    fn ns(ns: u64) -> Timestamp {
        Timestamp::from(UnixNanos::from(ns))
    }

    fn as_ns(ts: Timestamp) -> u64 {
        ts.as_nanosecond().try_into().unwrap()
    }

    /// A quote of `id` stamped `ts`.
    fn quote_at(id: InstrumentId, ts: u64) -> QuoteTick {
        QuoteTick::new(
            id,
            Price::from("100.00"),
            Price::from("100.25"),
            Quantity::from(1_000_000),
            Quantity::from(1_000_000),
            UnixNanos::from(ts),
            UnixNanos::from(ts),
        )
    }

    /// The instrument and window the source was asked for, in nanoseconds.
    type Asked = (InstrumentId, u64, u64);

    /// Fills `instruments` and their quotes over `[start, end]`
    /// (nanoseconds) and returns the result with the windows the source was
    /// asked for.
    async fn fill(
        catalog: &mut ParquetDataCatalog,
        instruments: Vec<InstrumentAny>,
        start: u64,
        end: u64,
        mut source: impl FnMut(InstrumentId) -> anyhow::Result<Vec<QuoteTick>>,
    ) -> (anyhow::Result<()>, Vec<Asked>) {
        let mut asked = Vec::new();
        let result = fill_catalog(
            catalog,
            instruments,
            ns(start),
            ns(end),
            async |id, s, e| {
                asked.push((id, as_ns(s), as_ns(e)));
                source(id)
            },
        )
        .await;
        (result, asked)
    }

    fn stored_quotes(catalog: &mut ParquetDataCatalog, id: InstrumentId) -> Vec<u64> {
        catalog
            .quote_ticks(Some(vec![id.to_string()]), None, None)
            .unwrap()
            .iter()
            .map(|q| q.ts_init.as_u64())
            .collect()
    }

    fn new_catalog(dir: &tempfile::TempDir) -> ParquetDataCatalog {
        ParquetDataCatalog::new(dir.path(), None, None, None, None)
    }

    #[tokio::test]
    async fn only_instruments_and_quotes_are_stored() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (r, _) = fill(&mut catalog, vec![instrument()], 0, 100, |id| {
            Ok(vec![quote_at(id, 50)])
        })
        .await;
        r.unwrap();
        let ids = vec!["ESZ21.GLBX".to_string()];
        let stored = catalog.instruments(Some(&ids), None, None).unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].id(), es_id());
        assert_eq!(stored_quotes(&mut catalog, es_id()), vec![50]);
        let mut kinds: Vec<String> = std::fs::read_dir(dir.path().join("data"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        kinds.sort();
        assert_eq!(kinds, ["futures_contract", "quotes"]);
    }

    #[tokio::test]
    async fn quotes_are_written_and_read_back_per_instrument_sorted_and_unique() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let aapl_id = aapl().id();
        let (r, asked) = fill(&mut catalog, vec![instrument(), aapl()], 0, 100, |id| {
            Ok(if id == es_id() {
                vec![
                    quote_at(id, 30),
                    quote_at(id, 10),
                    quote_at(id, 30),
                    quote_at(id, 20),
                ]
            } else {
                vec![quote_at(id, 5)]
            })
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![(es_id(), 0, 100), (aapl_id, 0, 100)]);
        assert_eq!(stored_quotes(&mut catalog, es_id()), vec![10, 20, 30]);
        assert_eq!(stored_quotes(&mut catalog, aapl_id), vec![5]);
    }

    #[tokio::test]
    async fn instruments_are_written_once_across_two_runs() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let instrument_at = |ts: u64| {
            let mut es = futures_contract_es(None, None);
            es.ts_init = UnixNanos::from(ts);
            InstrumentAny::FuturesContract(es)
        };
        let (r, _) = fill(&mut catalog, vec![instrument_at(1)], 0, 100, |id| {
            Ok(vec![quote_at(id, 10)])
        })
        .await;
        r.unwrap();
        let (r, _) = fill(&mut catalog, vec![instrument_at(2)], 0, 200, |id| {
            Ok(vec![quote_at(id, 150)])
        })
        .await;
        r.unwrap();
        let ids = vec!["ESZ21.GLBX".to_string()];
        let stored = catalog.instruments(Some(&ids), None, None).unwrap();
        assert_eq!(stored.len(), 1);
    }

    #[tokio::test]
    async fn quotes_are_covered_over_the_window_asked_for() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (r, _) = fill(&mut catalog, vec![instrument()], 100, 200, |id| {
            Ok(vec![quote_at(id, 149)])
        })
        .await;
        r.unwrap();
        let intervals = catalog
            .get_intervals(
                &QuoteTick::catalog_data_type().into(),
                Some(&es_id().to_string()),
            )
            .unwrap();
        assert_eq!(intervals, vec![(100, 200)]);
    }

    #[tokio::test]
    async fn quotes_resume_asking_only_after_their_stored_range_and_append() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let id = es_id();
        let (r, _) = fill(&mut catalog, vec![instrument()], 0, 100, |id| {
            Ok(vec![quote_at(id, 9), quote_at(id, 99)])
        })
        .await;
        r.unwrap();
        let (r, asked) = fill(&mut catalog, vec![instrument()], 0, 200, |id| {
            Ok(vec![quote_at(id, 99), quote_at(id, 100), quote_at(id, 199)])
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![(id, 101, 200)]);
        assert_eq!(stored_quotes(&mut catalog, id), vec![9, 99, 199]);
    }

    #[tokio::test]
    async fn resume_point_is_that_of_the_same_instrument() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let quotes = |id| Ok(vec![quote_at(id, 10)]);
        let (r, _) = fill(&mut catalog, vec![instrument()], 0, 100, quotes).await;
        r.unwrap();
        let (r, _) = fill(&mut catalog, vec![aapl()], 0, 50, quotes).await;
        r.unwrap();
        let (r, asked) = fill(&mut catalog, vec![instrument(), aapl()], 0, 200, |id| {
            Ok(vec![quote_at(id, 150)])
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![(es_id(), 101, 200), (aapl().id(), 51, 200)]);
    }

    #[tokio::test]
    async fn later_start_than_the_stored_end_still_resumes_right_after_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (r, _) = fill(&mut catalog, vec![instrument()], 0, 100, |id| {
            Ok(vec![quote_at(id, 10)])
        })
        .await;
        r.unwrap();
        let (r, asked) = fill(&mut catalog, vec![instrument()], 500, 600, |id| {
            Ok(vec![quote_at(id, 300), quote_at(id, 550)])
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![(es_id(), 101, 600)]);
        assert_eq!(stored_quotes(&mut catalog, es_id()), vec![10, 300, 550]);
    }

    #[tokio::test]
    async fn window_ending_right_after_the_stored_end_asks_for_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (r, _) = fill(&mut catalog, vec![instrument()], 0, 100, |id| {
            Ok(vec![quote_at(id, 10)])
        })
        .await;
        r.unwrap();
        let (r, asked) = fill(&mut catalog, vec![instrument()], 0, 101, |id| {
            Ok(vec![quote_at(id, 101)])
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![]);
        assert_eq!(stored_quotes(&mut catalog, es_id()), vec![10]);
    }

    #[tokio::test]
    async fn response_of_only_stored_quotes_is_not_an_error_and_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (r, _) = fill(&mut catalog, vec![instrument()], 0, 100, |id| {
            Ok(vec![quote_at(id, 10), quote_at(id, 20)])
        })
        .await;
        r.unwrap();
        let (r, _) = fill(&mut catalog, vec![instrument()], 0, 200, |id| {
            Ok(vec![quote_at(id, 20), quote_at(id, 100)])
        })
        .await;
        r.unwrap();
        assert_eq!(stored_quotes(&mut catalog, es_id()), vec![10, 20]);
    }

    #[tokio::test]
    async fn empty_first_quotes_response_is_an_error_naming_the_instrument() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (r, _) = fill(&mut catalog, vec![instrument()], 0, 100, |_| Ok(vec![])).await;
        let err = r.unwrap_err().to_string();
        assert!(err.contains("quotes of ESZ21.GLBX"), "{err}");
    }

    #[tokio::test]
    async fn empty_resumed_quotes_response_records_nothing_and_the_next_instruments_run() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let aapl_id = aapl().id();
        let both = || vec![instrument(), aapl()];
        let (r, _) = fill(&mut catalog, both(), 0, 100, |id| {
            Ok(vec![quote_at(id, 50)])
        })
        .await;
        r.unwrap();
        let (r, asked) = fill(&mut catalog, both(), 0, 200, |id| {
            Ok(if id == es_id() {
                vec![]
            } else {
                vec![quote_at(id, 150)]
            })
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![(es_id(), 101, 200), (aapl_id, 101, 200)]);
        assert_eq!(stored_quotes(&mut catalog, es_id()), vec![50]);
        assert_eq!(stored_quotes(&mut catalog, aapl_id), vec![50, 150]);
        let (r, asked) = fill(&mut catalog, both(), 0, 200, |id| {
            Ok(vec![quote_at(id, 150)])
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![(es_id(), 101, 200)]);
    }

    #[tokio::test]
    async fn failure_on_the_second_instrument_keeps_the_first_and_a_rerun_fetches_only_the_second()
    {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let aapl_id = aapl().id();
        let both = || vec![instrument(), aapl()];
        let (r, _) = fill(&mut catalog, both(), 0, 100, |id| {
            if id == aapl_id {
                bail!("source down")
            }
            Ok(vec![quote_at(id, 10)])
        })
        .await;
        r.unwrap_err();
        assert_eq!(stored_quotes(&mut catalog, es_id()), vec![10]);
        let (r, asked) = fill(&mut catalog, both(), 0, 100, |id| {
            Ok(vec![quote_at(id, 20)])
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![(aapl_id, 0, 100)]);
        assert_eq!(stored_quotes(&mut catalog, aapl_id), vec![20]);
    }

    #[tokio::test]
    async fn a_quote_of_another_instrument_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let aapl_id = aapl().id();
        let (r, _) = fill(&mut catalog, vec![instrument()], 0, 100, |_| {
            Ok(vec![quote_at(aapl_id, 50)])
        })
        .await;
        let err = r.unwrap_err().to_string();
        assert!(err.contains(&aapl_id.to_string()), "{err}");
        assert!(stored_quotes(&mut catalog, es_id()).is_empty());
    }

    #[tokio::test]
    async fn quote_after_the_window_end_moves_the_resume_point_past_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (r, _) = fill(&mut catalog, vec![instrument()], 0, 100, |id| {
            Ok(vec![quote_at(id, 10), quote_at(id, 150)])
        })
        .await;
        r.unwrap();
        let (r, asked) = fill(&mut catalog, vec![instrument()], 0, 200, |id| {
            Ok(vec![quote_at(id, 150), quote_at(id, 180)])
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![(es_id(), 151, 200)]);
        assert_eq!(stored_quotes(&mut catalog, es_id()), vec![10, 150, 180]);
    }

    #[tokio::test]
    async fn earlier_start_than_the_stored_quotes_is_an_error_and_fetches_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (r, _) = fill(&mut catalog, vec![instrument()], 100, 200, |id| {
            Ok(vec![quote_at(id, 150)])
        })
        .await;
        r.unwrap();
        let (r, asked) = fill(&mut catalog, vec![instrument()], 50, 300, |id| {
            Ok(vec![quote_at(id, 60), quote_at(id, 250)])
        })
        .await;
        let err = r.unwrap_err().to_string();
        assert!(err.contains("quotes of ESZ21.GLBX"), "{err}");
        assert!(err.contains("backwards"), "{err}");
        assert_eq!(asked, vec![]);
        assert_eq!(stored_quotes(&mut catalog, es_id()), vec![150]);
    }

    #[tokio::test]
    async fn start_inside_the_stored_range_resumes_after_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        for (start, end, ts) in [(100, 200, 150), (300, 400, 350)] {
            let (r, _) = fill(&mut catalog, vec![instrument()], start, end, |id| {
                Ok(vec![quote_at(id, ts)])
            })
            .await;
            r.unwrap();
        }
        let (r, asked) = fill(&mut catalog, vec![instrument()], 150, 500, |id| {
            Ok(vec![quote_at(id, 450)])
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![(es_id(), 401, 500)]);
    }

    #[tokio::test]
    async fn stored_range_includes_quotes_returned_before_the_window_start() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (r, _) = fill(&mut catalog, vec![instrument()], 100, 200, |id| {
            Ok(vec![quote_at(id, 50), quote_at(id, 150)])
        })
        .await;
        r.unwrap();
        let (r, asked) = fill(&mut catalog, vec![instrument()], 50, 200, |id| {
            Ok(vec![quote_at(id, 150)])
        })
        .await;
        r.unwrap();
        assert_eq!(asked, vec![]);
    }

    const MINUTE_NS: u64 = 60_000_000_000;

    /// A source of one quote a minute, as IB's are made: the quotes of
    /// `id` stamped 1 ns before each minute's close over `(from, to]`.
    fn minute_quotes(id: InstrumentId, from: u64, to: u64) -> Vec<QuoteTick> {
        (from / MINUTE_NS + 1..=to / MINUTE_NS + 1)
            .map(|minute| quote_at(id, minute * MINUTE_NS - 1))
            .filter(|quote| (from + 1..=to).contains(&quote.ts_init.as_u64()))
            .collect()
    }

    #[tokio::test]
    async fn minute_quotes_are_stored_every_minute_and_a_longer_window_fetches_only_the_new_minutes()
     {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let hour = 60 * MINUTE_NS;
        let mut asked = Vec::new();
        for end in [hour, 2 * hour, 2 * hour] {
            fill_catalog(
                &mut catalog,
                vec![instrument()],
                ns(0),
                ns(end),
                async |id, from, to| {
                    asked.push((as_ns(from), as_ns(to)));
                    Ok(minute_quotes(id, as_ns(from), as_ns(to)))
                },
            )
            .await
            .unwrap();
        }
        assert_eq!(asked, vec![(0, hour), (hour + 1, 2 * hour)]);
        let every_minute: Vec<u64> = (1..=120).map(|m| m * MINUTE_NS - 1).collect();
        assert_eq!(stored_quotes(&mut catalog, es_id()), every_minute);
    }
}
