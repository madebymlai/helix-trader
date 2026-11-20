//! Fills the catalog with the history of the given instruments and the series their strategy's
//! bars are built from.
//!
//! The module is source-agnostic and uses no node. The caller passes a fetch function, backed by
//! an adapter's historical client (`venues::ib`). For each series the module:
//! - Resumes after the stored range.
//! - Checks the response.
//! - Sorts it by `ts_init`.
//! - Writes it with `write_data_enum`, which stores each series under its data type and
//!   identifier.
//!
//! Nautilus does not persist requested data on its own.
//!
//! Temporary, together with IB's fill that uses it: IB's data client answers without a
//! `DataResponseEnvelope`, so Nautilus writes none of its answers back. Once
//! nautilus_trader#5472 is fixed, IB fills through the node (see `node_fill`), and this file
//! goes: helix-trader issue #4 (<https://github.com/madebymlai/helix-trader/issues/4>).

use std::collections::HashSet;

use anyhow::Context;
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Data, HasTsInit, NautilusDataType},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, catalog::types::data_path_prefix,
};

use crate::strategy::requirements::{CatalogSeries, SeriesSlice};

/// Fills `catalog` with `instruments`, then with each of `series` over `[start, end]`.
///
/// The fill fetches one series at a time through `fetch`.
///
/// Instruments already in the catalog are not written again. A series is stored over the windows
/// asked for, widened to its oldest and newest item. It resumes 1 ns after the end of that range,
/// whatever `start` is:
/// - A covered window asks for nothing.
/// - Returned items at or before that point are dropped.
/// - A `start` before the stored range is an error, raised before fetching, because extending
///   history backwards is not supported.
///
/// A fetch over `[from, to]` returns the items of the series with `ts_init` in that closed
/// interval. Any error stops the run, and series filled before it stay stored.
/// - An empty response on a series' first fill is an error.
/// - An item of another series is an error.
/// - Once the series is stored, an empty response writes nothing.
///
/// Items are stored sorted by `ts_init`, with one bar per `ts_init`. Ticks are stored as fetched.
/// Ticks that are alike in every field come from the venue, and each is stored.
pub async fn fill_catalog(
    catalog: &mut ParquetDataCatalog,
    instruments: Vec<InstrumentAny>,
    series: &[CatalogSeries],
    start: Timestamp,
    end: Timestamp,
    mut fetch: impl AsyncFnMut(&CatalogSeries, Timestamp, Timestamp) -> anyhow::Result<Vec<Data>>,
) -> anyhow::Result<()> {
    let ids: Vec<String> = instruments.iter().map(|i| i.id().to_string()).collect();
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
    let end_ns = UnixNanos::from(end);

    for source in series {
        let named = || {
            let (kind, key) = source.key();
            format!("the {} of {key}", data_path_prefix(&kind))
        };
        let (Some(from), resume_point) = resumed(catalog, source, start, end)? else {
            continue;
        };
        let mut items = fetch(source, from, end)
            .await
            .with_context(|| format!("fetching {}", named()))?;
        if items.is_empty() && resume_point.is_none() {
            anyhow::bail!("empty response for {} over [{from}, {end}]", named());
        }

        if let Some(foreign) = items.iter().find(|item| !is_of(item, source)) {
            anyhow::bail!(
                "response for {} holds {} of {}",
                named(),
                data_path_prefix(&NautilusDataType::from_data(foreign)),
                catalog_identifier(foreign),
            );
        }
        if let Some(last) = resume_point {
            items.retain(|item| item.ts_init() > last);
        }
        items.sort_by_key(HasTsInit::ts_init);
        // One bar per close, since a walk back's requests overlap at their bounds. Ticks are kept
        // as given, since a venue that stamps to the second (IB) gives ticks alike in every field.
        items.dedup_by(|later, earlier| match (later, earlier) {
            (Data::Bar(later), Data::Bar(earlier)) => later.ts_init == earlier.ts_init,
            _ => false,
        });
        let (Some(oldest), Some(newest)) = (items.first(), items.last()) else {
            continue;
        };
        let covered_from = UnixNanos::from(from).min(oldest.ts_init());
        let covered_to = end_ns.max(newest.ts_init());
        catalog
            .write_data_enum(&items, Some(covered_from), Some(covered_to), None)
            .with_context(|| format!("writing {}", named()))?;
    }
    Ok(())
}

/// Returns where a fill of `series` over `[start, end]` resumes, with the stored end, if any.
///
/// The fill resumes at `start` if `catalog` stores nothing of the series, else 1 ns after the
/// stored end. It returns none if that point is at or after `end`.
///
/// # Errors
///
/// Returns an error if:
/// - The catalog cannot be read.
/// - `start` is before the stored range, because extending history backwards is not supported.
pub(crate) fn resumed(
    catalog: &ParquetDataCatalog,
    series: &CatalogSeries,
    start: Timestamp,
    end: Timestamp,
) -> anyhow::Result<(Option<Timestamp>, Option<UnixNanos>)> {
    let (data_type, identifier) = series.key();
    let named = || format!("the {} of {identifier}", data_path_prefix(&data_type));
    let catalog_type = data_type.clone().into();
    let reading = || format!("reading the stored range of {}", named());
    let first = catalog
        .query_first_timestamp(&catalog_type, Some(&identifier))
        .with_context(reading)?;
    let last = catalog
        .query_last_timestamp(&catalog_type, Some(&identifier))
        .with_context(reading)?;

    if let (Some(first), Some(last)) = (first, last)
        && UnixNanos::from(start) < UnixNanos::from(first)
    {
        anyhow::bail!(
            "{} are stored over [{}, {}] and {start} is before it: \
             extending history backwards is not supported; use a new catalog",
            named(),
            UnixNanos::from(first),
            UnixNanos::from(last),
        );
    }
    let last = last.map(UnixNanos::from);
    let from = last.map_or(start, just_after);
    Ok(((from < end).then_some(from), last))
}

/// Fills `catalog` with `slices` (`DataRequirements::slices`), one `fill_catalog` per slice.
///
/// Temporary: this is IB's fill until nautilus_trader#5472 is fixed. Then IB fills through the
/// node.
///
/// # Errors
///
/// Returns an error if:
/// - No instrument in `instruments` is a slice's contract, when that slice's turn comes.
/// - `fill_catalog` fails.
///
/// The slices filled before the error stay stored.
pub(crate) async fn fill_slices(
    catalog: &mut ParquetDataCatalog,
    instruments: &[InstrumentAny],
    slices: &[SeriesSlice],
    mut fetch: impl AsyncFnMut(&CatalogSeries, Timestamp, Timestamp) -> anyhow::Result<Vec<Data>>,
) -> anyhow::Result<()> {
    for slice in slices {
        let id = slice.series.instrument_id();
        let instrument = instruments
            .iter()
            .find(|instrument| instrument.id() == id)
            .with_context(|| format!("no instrument of {id} loaded for its slice"))?;
        fill_catalog(
            catalog,
            vec![instrument.clone()],
            std::slice::from_ref(&slice.series),
            slice.from,
            slice.to,
            &mut fetch,
        )
        .await?;
    }
    Ok(())
}

fn is_of(item: &Data, series: &CatalogSeries) -> bool {
    match (item, series) {
        (Data::Bar(bar), CatalogSeries::Bars(expected)) => bar.bar_type == *expected,
        (Data::Bar(_), _) | (_, CatalogSeries::Bars(_)) => false,
        (_, CatalogSeries::Ticks(tick)) => {
            NautilusDataType::from_data(item) == tick.data
                && item.instrument_id() == tick.instrument_id
        }
    }
}

fn catalog_identifier(item: &Data) -> String {
    match item {
        Data::Bar(bar) => bar.bar_type.to_string(),
        other => other.instrument_id().to_string(),
    }
}

/// Returns the first nanosecond after `last`. jiff reaches past `u64::MAX` nanoseconds.
fn just_after(last: UnixNanos) -> Timestamp {
    Timestamp::from(last) + SignedDuration::from_nanos(1)
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        data::{Bar, BarType, OrderBookDepth, QuoteTick, TradeTick, stubs::stub_depth10},
        enums::AggressorSide,
        identifiers::TradeId,
        instruments::{
            NautilusInstrumentType,
            stubs::{equity_aapl, futures_contract_es},
        },
        types::{Price, Quantity},
    };
    use nautilus_persistence::catalog::types::{CatalogDataType, HasCatalogDataType};
    use rstest::rstest;

    use super::*;
    use crate::strategy::requirements::{CatalogSeries, DataRequirements, SeriesSlice, TickSeries};

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

    async fn fill_quotes(
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
        let series: Vec<_> = instruments
            .iter()
            .map(|i| {
                CatalogSeries::Ticks(TickSeries {
                    data: NautilusDataType::QuoteTick,
                    instrument_id: i.id(),
                })
            })
            .collect();
        fill_catalog(
            catalog,
            instruments,
            &series,
            start,
            end,
            async |series, from, to| {
                let CatalogSeries::Ticks(tick) = series else {
                    panic!("{series}")
                };
                assert_eq!(tick.data, NautilusDataType::QuoteTick);
                let quotes = fetch_quotes(tick.instrument_id, from, to).await?;
                Ok(quotes.into_iter().map(Data::Quote).collect())
            },
        )
        .await
    }

    type Asked = (InstrumentId, u64, u64);

    async fn fill(
        catalog: &mut ParquetDataCatalog,
        instruments: Vec<InstrumentAny>,
        start: u64,
        end: u64,
        mut source: impl FnMut(InstrumentId) -> anyhow::Result<Vec<QuoteTick>>,
    ) -> (anyhow::Result<()>, Vec<Asked>) {
        let mut asked = Vec::new();
        let result = fill_quotes(
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

    fn stored_kinds(catalog: &ParquetDataCatalog) -> HashSet<CatalogDataType> {
        catalog.list_data_types().unwrap().into_iter().collect()
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
        assert_eq!(
            stored_kinds(&catalog),
            HashSet::from([
                CatalogDataType::Instrument(NautilusInstrumentType::FuturesContract),
                CatalogDataType::Data(NautilusDataType::QuoteTick)
            ])
        );
    }

    fn files(dir: &std::path::Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
        let mut files = vec![];
        let mut dirs = vec![dir.to_path_buf()];
        while let Some(at) = dirs.pop() {
            for entry in std::fs::read_dir(at).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    dirs.push(path);
                } else {
                    let bytes = std::fs::read(&path).unwrap();
                    files.push((path.strip_prefix(dir).unwrap().to_path_buf(), bytes));
                }
            }
        }
        files.sort();
        files
    }

    #[tokio::test]
    async fn quotes_are_stored_in_the_files_writing_them_directly_makes() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let quotes = |id| vec![quote_at(id, 30), quote_at(id, 10), quote_at(id, 20)];
        let (r, _) = fill(&mut catalog, vec![instrument()], 0, 100, |id| {
            Ok(quotes(id))
        })
        .await;
        r.unwrap();
        let direct = tempfile::tempdir().unwrap();
        let catalog = new_catalog(&direct);
        catalog.write_instruments(vec![instrument()]).unwrap();
        let mut sorted = quotes(es_id());
        sorted.sort_by_key(|quote| quote.ts_init);
        catalog
            .write_to_parquet(
                &sorted,
                Some(UnixNanos::from(0)),
                Some(UnixNanos::from(100)),
                None,
            )
            .unwrap();
        let (filled, written) = (files(dir.path()), files(direct.path()));
        let paths = |files: &[(std::path::PathBuf, Vec<u8>)]| {
            files
                .iter()
                .map(|(path, _)| path.clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(paths(&filled), paths(&written));
        assert_eq!(filled.len(), 2);
        assert!(filled == written, "the files' bytes differ");
    }

    #[tokio::test]
    async fn quotes_are_written_and_read_back_per_instrument_sorted() {
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
        assert_eq!(stored_quotes(&mut catalog, es_id()), vec![10, 20, 30, 30]);
        assert_eq!(stored_quotes(&mut catalog, aapl_id), vec![5]);
    }

    /// IB stamps its historical ticks to the second, so quotes from one second are stored as
    /// given.
    #[tokio::test]
    async fn every_quote_of_one_stamp_is_stored_as_given() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let at_10 = |bid: &str| {
            let quote = quote_at(es_id(), 10);
            QuoteTick {
                bid_price: Price::from(bid),
                ..quote
            }
        };
        let (r, _) = fill(&mut catalog, vec![instrument()], 0, 100, |_| {
            Ok(vec![
                at_10("100.00"),
                at_10("99.75"),
                at_10("99.75"),
                at_10("100.25"),
            ])
        })
        .await;
        r.unwrap();

        let bids: Vec<String> = catalog
            .quote_ticks(Some(vec![es_id().to_string()]), None, None)
            .unwrap()
            .iter()
            .map(|quote| quote.bid_price.to_string())
            .collect();
        assert_eq!(bids, ["100.00", "99.75", "99.75", "100.25"]);
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
        let [kept] = &stored[..] else {
            panic!("{stored:?}")
        };
        assert_eq!(Instrument::ts_init(kept), UnixNanos::from(1));
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
    async fn empty_first_response_names_the_window_asked_for() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (r, _) = fill(&mut catalog, vec![instrument()], 0, 100, |_| Ok(vec![])).await;
        let err = r.unwrap_err().to_string();
        assert!(
            err.ends_with("over [1970-01-01T00:00:00Z, 1970-01-01T00:00:00.0000001Z]"),
            "{err}"
        );
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
                anyhow::bail!("source down")
            }
            Ok(vec![quote_at(id, 10)])
        })
        .await;
        let err = format!("{:#}", r.unwrap_err());
        assert!(
            err.contains(&format!("quotes of {aapl_id}")) && err.contains("source down"),
            "{err}"
        );
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
        // Stored over the first fill's window, 100 to 200 ns, then asked from 50 ns
        assert!(
            err.contains("[100, 200]") && err.contains("1970-01-01T00:00:00.00000005Z"),
            "{err}"
        );
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

    #[tokio::test]
    async fn a_quote_at_the_window_start_is_stored_on_first_fill_and_on_resume() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let mut asked = Vec::new();
        for end in [200, 300] {
            fill_quotes(
                &mut catalog,
                vec![instrument()],
                ns(100),
                ns(end),
                async |id, from, to| {
                    let (from, to) = (as_ns(from), as_ns(to));
                    asked.push((from, to));
                    Ok([100, 150, 201, 250]
                        .into_iter()
                        .filter(|ts| (from..=to).contains(ts))
                        .map(|ts| quote_at(id, ts))
                        .collect())
                },
            )
            .await
            .unwrap();
        }
        assert_eq!(asked, vec![(100, 200), (201, 300)]);
        assert_eq!(
            stored_quotes(&mut catalog, es_id()),
            vec![100, 150, 201, 250]
        );
    }

    const MINUTE_NS: u64 = 60_000_000_000;

    fn minute_quotes(id: InstrumentId, from: u64, to: u64) -> Vec<QuoteTick> {
        (from / MINUTE_NS + 1..=to / MINUTE_NS + 1)
            .map(|minute| quote_at(id, minute * MINUTE_NS - 1))
            .filter(|quote| (from..=to).contains(&quote.ts_init.as_u64()))
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
            fill_quotes(
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

    const BARS: &str = "ESZ21.GLBX-1-MINUTE-LAST-EXTERNAL";

    fn bar_at(ts: u64) -> Bar {
        let price = Price::from("100.00");
        let ts = UnixNanos::from(ts);
        Bar::new(
            BarType::from(BARS),
            price,
            price,
            price,
            price,
            Quantity::from(1),
            ts,
            ts,
        )
    }

    async fn fill_bars(
        catalog: &mut ParquetDataCatalog,
        start: u64,
        end: u64,
        source: impl Fn(u64, u64) -> Vec<Data>,
    ) -> (anyhow::Result<()>, Vec<(CatalogSeries, u64, u64)>) {
        let mut asked = Vec::new();
        let result = fill_catalog(
            catalog,
            vec![instrument()],
            &[CatalogSeries::Bars(BarType::from(BARS))],
            ns(start),
            ns(end),
            async |series, from, to| {
                let (from, to) = (as_ns(from), as_ns(to));
                asked.push((series.clone(), from, to));
                Ok(source(from, to))
            },
        )
        .await;
        (result, asked)
    }

    fn stored_bars(catalog: &mut ParquetDataCatalog) -> Vec<u64> {
        catalog
            .bars(Some(vec![BARS.to_string()]), None, None)
            .unwrap()
            .iter()
            .map(|bar| bar.ts_init.as_u64())
            .collect()
    }

    #[tokio::test]
    async fn bars_are_stored_under_their_bar_type_and_resume_after_their_range() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let bars = |from: u64, to: u64| {
            [10, 30, 20, 30, 150]
                .into_iter()
                .filter(|ts| (from..=to).contains(ts))
                .map(|ts| Data::Bar(bar_at(ts)))
                .collect()
        };
        let (r, _) = fill_bars(&mut catalog, 0, 100, bars).await;
        r.unwrap();
        let (r, asked) = fill_bars(&mut catalog, 0, 200, bars).await;
        r.unwrap();
        assert_eq!(
            asked,
            vec![(CatalogSeries::Bars(BarType::from(BARS)), 101, 200)]
        );
        assert_eq!(stored_bars(&mut catalog), vec![10, 20, 30, 150]);
        let intervals = catalog
            .get_intervals(&NautilusDataType::Bar.into(), Some(BARS))
            .unwrap();
        assert_eq!(intervals, vec![(0, 100), (101, 200)]);
        assert_eq!(
            stored_kinds(&catalog),
            HashSet::from([
                CatalogDataType::Instrument(NautilusInstrumentType::FuturesContract),
                CatalogDataType::Data(NautilusDataType::Bar)
            ])
        );
    }

    #[rstest]
    #[case::a_quote(
        vec![Data::Bar(bar_at(10)), Data::Quote(quote_at(es_id(), 20))],
        "quotes of ESZ21.GLBX"
    )]
    #[case::bars_of_another_bar_type(
        vec![Data::Bar(Bar {
            bar_type: BarType::from("ESZ21.GLBX-1-HOUR-LAST-EXTERNAL"),
            ..bar_at(10)
        })],
        "bars of ESZ21.GLBX-1-HOUR-LAST-EXTERNAL"
    )]
    #[tokio::test]
    async fn a_foreign_item_in_a_response_for_bars_is_an_error_naming_both(
        #[case] response: Vec<Data>,
        #[case] foreign: &str,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (r, _) = fill_bars(&mut catalog, 0, 100, |_, _| response.clone()).await;
        let err = r.unwrap_err().to_string();
        assert!(err.contains(&format!("bars of {BARS}")), "{err}");
        assert!(err.contains(foreign), "{err}");
        assert!(stored_bars(&mut catalog).is_empty());
    }

    #[tokio::test]
    async fn an_empty_first_response_for_bars_is_an_error_naming_the_bar_type() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (r, _) = fill_bars(&mut catalog, 0, 100, |_, _| vec![]).await;
        let err = r.unwrap_err().to_string();
        assert!(err.contains(&format!("bars of {BARS}")), "{err}");
    }

    fn trade_at(ts: u64) -> TradeTick {
        let ts = UnixNanos::from(ts);
        TradeTick::new(
            es_id(),
            Price::from("100.00"),
            Quantity::from(1),
            AggressorSide::Buy,
            TradeId::from(ts.to_string().as_str()),
            ts,
            ts,
        )
    }

    fn depth_at(ts: u64) -> OrderBookDepth {
        let mut depth = stub_depth10();
        let ts = UnixNanos::from(ts);
        (depth.instrument_id, depth.ts_event, depth.ts_init) = (es_id(), ts, ts);
        depth
    }

    /// Trades at 10, 60 and 150 and snapshots at 20 and 180.
    async fn fill_ticks(
        catalog: &mut ParquetDataCatalog,
        start: u64,
        end: u64,
    ) -> (
        anyhow::Result<()>,
        Vec<(NautilusDataType, String, u64, u64)>,
    ) {
        let tick = |data| TickSeries {
            data,
            instrument_id: es_id(),
        };
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![
                tick(NautilusDataType::TradeTick),
                tick(NautilusDataType::OrderBookDepth),
            ],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        };
        let mut asked = Vec::new();
        let result = fill_catalog(
            catalog,
            vec![instrument()],
            &requirements.catalog_series(),
            ns(start),
            ns(end),
            async |series, from, to| {
                let CatalogSeries::Ticks(tick) = series else {
                    panic!("{series}")
                };
                let data_type = &tick.data;
                let (from, to) = (as_ns(from), as_ns(to));
                asked.push((data_type.clone(), tick.instrument_id.to_string(), from, to));
                let within = |ts: &u64| (from..=to).contains(ts);
                Ok(match data_type {
                    NautilusDataType::TradeTick => [10, 60, 150]
                        .into_iter()
                        .filter(within)
                        .map(|ts| Data::Trade(trade_at(ts)))
                        .collect(),
                    _ => [20, 180]
                        .into_iter()
                        .filter(within)
                        .map(|ts| Data::from(depth_at(ts)))
                        .collect(),
                })
            },
        )
        .await;
        (result, asked)
    }

    #[tokio::test]
    async fn declared_ticks_are_stored_and_resume_after_their_range() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let (r, _) = fill_ticks(&mut catalog, 0, 100).await;
        r.unwrap();
        let (r, asked) = fill_ticks(&mut catalog, 0, 200).await;
        r.unwrap();
        let es = es_id().to_string();
        assert_eq!(
            asked,
            vec![
                (NautilusDataType::TradeTick, es.clone(), 101, 200),
                (NautilusDataType::OrderBookDepth, es.clone(), 101, 200),
            ]
        );
        let trades: Vec<u64> = catalog
            .trade_ticks(Some(vec![es.clone()]), None, None)
            .unwrap()
            .iter()
            .map(|trade| trade.ts_init.as_u64())
            .collect();
        assert_eq!(trades, [10, 60, 150]);
        let depths: Vec<u64> = catalog
            .order_book_depths(Some(vec![es]), None, None)
            .unwrap()
            .iter()
            .map(|depth| depth.ts_init.as_u64())
            .collect();
        assert_eq!(depths, [20, 180]);
    }

    fn contracts() -> Vec<InstrumentAny> {
        let mut march = futures_contract_es(None, None);
        march.id = InstrumentId::from("ESH22.GLBX");
        vec![instrument(), InstrumentAny::FuturesContract(march)]
    }

    fn quotes_slice(id: &str, from: u64, to: u64) -> SeriesSlice {
        SeriesSlice {
            series: CatalogSeries::Ticks(TickSeries {
                data: NautilusDataType::QuoteTick,
                instrument_id: InstrumentId::from(id),
            }),
            from: ns(from),
            to: ns(to),
        }
    }

    #[tokio::test]
    async fn each_slice_is_fetched_over_its_window_on_its_contract() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let slices = [
            quotes_slice("ESZ21.GLBX", 0, 100),
            quotes_slice("ESH22.GLBX", 101, 200),
        ];
        let mut asked = Vec::new();

        fill_slices(
            &mut catalog,
            &contracts(),
            &slices,
            async |series, from, to| {
                let CatalogSeries::Ticks(tick) = series else {
                    panic!("{series}")
                };
                assert_eq!(tick.data, NautilusDataType::QuoteTick);
                let id = tick.instrument_id;
                asked.push((id.to_string(), as_ns(from), as_ns(to)));
                Ok(vec![Data::Quote(quote_at(id, as_ns(from) + 10))])
            },
        )
        .await
        .unwrap();

        assert_eq!(
            asked,
            [
                ("ESZ21.GLBX".to_string(), 0, 100),
                ("ESH22.GLBX".to_string(), 101, 200)
            ]
        );
        let march = InstrumentId::from("ESH22.GLBX");
        assert_eq!(stored_quotes(&mut catalog, es_id()), [10]);
        assert_eq!(stored_quotes(&mut catalog, march), [111]);
        let ids = vec!["ESZ21.GLBX".to_string(), "ESH22.GLBX".to_string()];
        assert_eq!(
            catalog.instruments(Some(&ids), None, None).unwrap().len(),
            2
        );
    }

    #[tokio::test]
    async fn a_slice_of_a_contract_not_loaded_is_refused_after_the_slices_before_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
        let slices = [
            quotes_slice("ESZ21.GLBX", 0, 100),
            quotes_slice("ESM22.GLBX", 101, 200),
        ];

        let err = fill_slices(
            &mut catalog,
            &contracts(),
            &slices,
            async |series, from, _| {
                let id = series.instrument_id();
                Ok(vec![Data::Quote(quote_at(id, as_ns(from) + 10))])
            },
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("ESM22.GLBX"), "{err}");
        assert_eq!(stored_quotes(&mut catalog, es_id()), [10]);
    }
}
