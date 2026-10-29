//! What the catalog covers of the data a live node streams into it.
//!
//! Nautilus promotes the staged data of a live node into the catalog every
//! promotion interval, each promotion a file per series named by the
//! `ts_init` of its first and last row (see `live::Recording`). The files
//! of a series a run promotes leave a gap between them, from one's last
//! row to the next one's first, which the catalog counts as missing
//! (`ParquetDataCatalog::get_missing_intervals_for_request`, where a 1 ns
//! gap is a gap): the node was receiving the series across it, and nothing
//! came. A single file has the same blind spot inside it, so these gaps
//! are no data lost; a crash or a stop is, and so is the time before a
//! run's first row.
//!
//! So the node records the start of each run in the catalog before it
//! streams anything (`record_run_start`): an empty directory
//! `live-runs/<ns>` beside `data/`, read back with Nautilus' own listing
//! (`list_directory_stems`). A reader about to read a series joins the
//! gaps the runs streamed across first (`join`), recording each as covered
//! with Nautilus' `extend_file_name`, the rename by which the data engine
//! records a request answered with nothing: the file before the gap then
//! ends where the next one starts, for every later reader, the data
//! engine's own catalog lookup among them, which asks the venue for any
//! gap (a backtest's data client never answers). The node itself renames
//! nothing, so a reader reading while it streams never loses a file under
//! it.
//!
//! A gap is joined when the file after it was promoted (its name holds the
//! replay identity only promotion adds), the file before it ends after the
//! latest run start before the file after it, and so no run started inside
//! it: the file before is the run's own, or the warmup it wrote back up to
//! its subscriptions (IC's, `strategy::warmup`). Everything else stays a
//! gap: that of a crash or a stop, which the next run's start falls in;
//! the time before a run's first row; and a gap before a file written
//! straight to the catalog (fill-catalog's).

use std::path::Path;

use anyhow::Context;
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_model::data::NautilusDataType;
use nautilus_persistence::backend::parquet::{
    catalog::ParquetDataCatalog,
    paths::{parse_filename_timestamps, timestamps_to_filename},
};

/// Where the starts of the live runs are recorded, under the catalog.
const RUNS: &str = "live-runs";

/// Records in the catalog at `catalog` that a live run starts now: call it
/// before the node is built, so that every row it streams, and the warmup
/// it writes back, comes after.
///
/// # Errors
///
/// Returns an error if the record cannot be created.
pub fn record_run_start(catalog: &Path) -> anyhow::Result<()> {
    // Nautilus' clock, which stamps what the node receives.
    let start = get_atomic_clock_realtime().get_time_ns();
    let record = catalog.join(RUNS).join(start.as_u64().to_string());
    std::fs::create_dir_all(&record)
        .with_context(|| format!("recording the live run start {}", record.display()))
}

/// Joins the gaps a live run streamed across in the series of `data_type`
/// and `identifier` over `[from, to]` (inclusive nanoseconds), each whole,
/// to the file before it (see the module docs): call it before reading the
/// series' coverage.
///
/// # Errors
///
/// Returns an error if the catalog cannot be read, or a file cannot be
/// renamed (another reader joined it first, say).
pub fn join(
    catalog: &ParquetDataCatalog,
    data_type: &NautilusDataType,
    identifier: &str,
    from: u64,
    to: u64,
) -> anyhow::Result<()> {
    let catalog_type = data_type.clone().into();
    let gaps = catalog
        .get_missing_intervals_for_request(from, to, &catalog_type, Some(identifier))
        .context("reading the stored intervals")?;
    if gaps.is_empty() {
        return Ok(());
    }
    let mut run_starts: Vec<u64> = catalog
        .list_directory_stems(RUNS)
        .context("reading the live run starts")?
        .iter()
        .filter_map(|start| start.parse().ok())
        .collect();
    run_starts.sort_unstable();
    let files: Vec<StoredFile> = catalog
        .list_parquet_files_with_criteria(
            &catalog_type,
            Some(&[identifier.to_string()]),
            None,
            None,
        )
        .context("listing the stored files")?
        .iter()
        .filter_map(|path| StoredFile::of(path))
        .collect();
    for gap in gaps {
        if let Some((start, end)) = streamed_gap(gap, &files, &run_starts) {
            catalog
                .extend_file_name(
                    &catalog_type,
                    Some(identifier),
                    UnixNanos::from(start),
                    UnixNanos::from(end),
                )
                .with_context(|| format!("joining the gap [{start}, {end}]"))?;
        }
    }
    Ok(())
}

/// A file of a series, as its name tells it.
struct StoredFile {
    start: u64,
    end: u64,
    /// Named by a promotion: its interval and the replay identity Nautilus
    /// adds to it (only promotion does).
    promoted: bool,
}

impl StoredFile {
    /// The file at `path`, if it is named by an interval.
    fn of(path: &str) -> Option<Self> {
        let (start, end) = parse_filename_timestamps(path)?;
        let interval = timestamps_to_filename(UnixNanos::from(start), UnixNanos::from(end));
        Some(Self {
            start,
            end,
            promoted: !path.ends_with(&interval),
        })
    }
}

/// The whole gap between the files of a series in `files` that the
/// missing window `(from, to)` lies in, if a live run streamed across it:
/// the file after it was promoted, and the file before it ends after the
/// latest of `run_starts` before the file after it, so that no run
/// started in between.
fn streamed_gap(
    (from, to): (u64, u64),
    files: &[StoredFile],
    run_starts: &[u64],
) -> Option<(u64, u64)> {
    let before = files
        .iter()
        .map(|f| f.end)
        .filter(|&end| end < from)
        .max()?;
    let after = files
        .iter()
        .filter(|f| f.start > to)
        .min_by_key(|f| f.start)?;
    let run_start = run_starts
        .iter()
        .rev()
        .find(|&&start| start <= after.start)?;
    (after.promoted && *run_start <= before).then_some((before + 1, after.start - 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    const AUDUSD: &str = "AUD/USD.SIM";

    /// A promoted file, named as a promotion names it: its interval and
    /// its replay identity.
    const PROMOTED: bool = true;
    /// A file written straight to the catalog (fill-catalog, a warmup
    /// written back), named by its interval alone.
    const WRITTEN: bool = false;

    /// A catalog in `dir` with the runs starting at `runs` and quote files
    /// of AUD/USD.SIM over the intervals of `files`, each promoted or
    /// written. The files are empty: coverage reads their names.
    fn catalog(dir: &Path, runs: &[u64], files: &[(u64, u64, bool)]) -> ParquetDataCatalog {
        for run in runs {
            std::fs::create_dir_all(dir.join(RUNS).join(run.to_string())).unwrap();
        }
        let quotes = dir.join("data/quotes/AUDUSD.SIM");
        std::fs::create_dir_all(&quotes).unwrap();
        for &(start, end, promoted) in files {
            let name = timestamps_to_filename(UnixNanos::from(start), UnixNanos::from(end));
            let name = if promoted {
                name.replace(".parquet", "_0123456789abcdef.parquet")
            } else {
                name
            };
            std::fs::write(quotes.join(name), b"").unwrap();
        }
        ParquetDataCatalog::new(dir, None, None, None, None)
    }

    /// What the catalog lacks of the quotes over `[from, to]` once joined
    /// over that window, by Nautilus' own count, as the data engine reads
    /// it.
    fn quotes_missing(catalog: &ParquetDataCatalog, from: u64, to: u64) -> Vec<(u64, u64)> {
        join(catalog, &NautilusDataType::QuoteTick, AUDUSD, from, to).unwrap();
        catalog
            .get_missing_intervals_for_request(
                from,
                to,
                &NautilusDataType::QuoteTick.into(),
                Some(AUDUSD),
            )
            .unwrap()
    }

    #[test]
    fn the_files_a_run_promotes_cover_from_the_first_row_to_the_last() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = catalog(
            dir.path(),
            &[1_000],
            &[
                (1_000, 1_100, PROMOTED),
                (1_200, 1_300, PROMOTED),
                (1_400, 1_500, PROMOTED),
            ],
        );
        assert_eq!(quotes_missing(&catalog, 1_000, 1_500), []);
    }

    #[test]
    fn a_window_starting_or_ending_inside_a_streamed_gap_is_covered() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = catalog(
            dir.path(),
            &[1_000],
            &[(1_000, 1_100, PROMOTED), (1_200, 1_300, PROMOTED)],
        );
        assert_eq!(quotes_missing(&catalog, 1_150, 1_300), []);
        assert_eq!(quotes_missing(&catalog, 1_000, 1_150), []);
    }

    #[test]
    fn a_gap_is_joined_whole_for_the_readers_after() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = catalog(
            dir.path(),
            &[1_000],
            &[(1_000, 1_100, PROMOTED), (1_200, 1_300, PROMOTED)],
        );
        quotes_missing(&catalog, 1_150, 1_160);
        let missing = catalog
            .get_missing_intervals_for_request(
                1_000,
                1_300,
                &NautilusDataType::QuoteTick.into(),
                Some(AUDUSD),
            )
            .unwrap();
        assert_eq!(missing, []);
    }

    #[test]
    fn the_first_file_a_run_promotes_follows_the_warmup_it_wrote_back() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = catalog(
            dir.path(),
            &[1_000],
            &[(500, 1_050, WRITTEN), (1_200, 1_300, PROMOTED)],
        );
        assert_eq!(quotes_missing(&catalog, 500, 1_300), []);
    }

    #[test]
    fn the_gap_a_run_start_falls_in_stays_missing() {
        // A crashed run's files, then the next run's.
        let dir = tempfile::tempdir().unwrap();
        let catalog = catalog(
            dir.path(),
            &[400, 1_000],
            &[
                (500, 600, PROMOTED),
                (700, 900, PROMOTED),
                (1_200, 1_300, PROMOTED),
            ],
        );
        assert_eq!(quotes_missing(&catalog, 500, 1_300), [(901, 1_199)]);
    }

    #[test]
    fn the_gap_before_the_first_row_of_a_run_stays_missing() {
        // fill-catalog's history, then the run's first file.
        let dir = tempfile::tempdir().unwrap();
        let catalog = catalog(
            dir.path(),
            &[1_000],
            &[(500, 900, WRITTEN), (1_200, 1_300, PROMOTED)],
        );
        assert_eq!(quotes_missing(&catalog, 500, 1_300), [(901, 1_199)]);
    }

    #[test]
    fn a_gap_before_a_file_the_run_did_not_promote_stays_missing() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = catalog(
            dir.path(),
            &[1_000],
            &[(1_000, 1_100, PROMOTED), (1_200, 1_300, WRITTEN)],
        );
        assert_eq!(quotes_missing(&catalog, 1_000, 1_300), [(1_101, 1_199)]);
    }

    #[test]
    fn promoted_files_without_a_recorded_run_leave_their_gaps_missing() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = catalog(
            dir.path(),
            &[],
            &[(1_000, 1_100, PROMOTED), (1_200, 1_300, PROMOTED)],
        );
        assert_eq!(quotes_missing(&catalog, 1_000, 1_300), [(1_101, 1_199)]);
    }

    #[test]
    fn what_no_file_covers_after_the_last_stays_missing() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = catalog(
            dir.path(),
            &[1_000],
            &[(1_000, 1_100, PROMOTED), (1_200, 1_300, PROMOTED)],
        );
        assert_eq!(quotes_missing(&catalog, 1_000, 1_400), [(1_301, 1_400)]);
    }

    #[test]
    fn a_run_recorded_now_covers_the_gaps_between_the_files_it_promotes() {
        let dir = tempfile::tempdir().unwrap();
        record_run_start(dir.path()).unwrap();
        let now = get_atomic_clock_realtime().get_time_ns().as_u64();
        let catalog = catalog(
            dir.path(),
            &[],
            &[(now, now + 100, PROMOTED), (now + 200, now + 300, PROMOTED)],
        );
        assert_eq!(quotes_missing(&catalog, now, now + 300), []);
    }
}
