//! Databento definitions of `GLBX` instruments (CME Globex, `GLBX.MDP3`),
//! `XNAS` ones (Nasdaq TotalView, `XNAS.ITCH`) and `OPRA` ones (US
//! options, `OPRA.PILLAR`), filled through the node
//! (see `catalog::NodeFill`) by Nautilus' Databento data client
//! (`DatabentoDataClient`), whose answers the data engine writes back to
//! the catalog, each contract under its own id; their history is LSE's
//! (see `lse`). What stays here is what Nautilus' client does not do: each
//! definition asked within its dataset's end, which Databento refuses a
//! request past, read before the node starts. See `README.md`.

use std::{collections::BTreeMap, path::PathBuf};

use anyhow::{Context, ensure};
use jiff::{SignedDuration, Timestamp, tz::TimeZone};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_databento::{
    common::Credential,
    data::DatabentoDataClientConfig,
    factories::DatabentoDataClientFactory,
    historical::{DatabentoHistoricalClient, DatasetRange},
    loader::DatabentoDataLoader,
};

use super::{DefinitionsSource, Row};
use crate::catalog::{FillData, FillRequest, NodeClient};

#[cfg(test)]
pub(super) mod testing;

/// Nautilus' map of Databento publishers to venues, copied from
/// `crates/adapters/databento/publishers.json` at the pinned commit: the
/// client reads it from a path.
const PUBLISHERS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/venues/databento/publishers.json"
);

/// Databento's historical API, for the definitions of `GLBX`, `XNAS` and
/// `OPRA` instruments.
#[derive(Debug)]
pub struct DatabentoHistory {
    /// Nautilus' historical client, for each dataset's end.
    metadata: DatabentoHistoricalClient,
    /// Nautilus' map of venues to datasets, the one its data client asks a
    /// venue's instruments of.
    datasets: DatabentoDataLoader,
    /// The config of Nautilus' data client, the node's.
    config: DatabentoDataClientConfig,
}

impl DatabentoHistory {
    /// The clients, with `api_key`.
    ///
    /// # Errors
    ///
    /// Returns an error if Databento's client cannot be built (a malformed
    /// key, or the map of publishers, which it reads, unreadable).
    pub fn new(api_key: &str) -> anyhow::Result<Self> {
        let metadata = DatabentoHistoricalClient::new(
            Credential::new(api_key),
            PathBuf::from(PUBLISHERS),
            get_atomic_clock_realtime(),
            false,
        )
        .context("building Databento's client")?;
        Ok(Self {
            metadata,
            datasets: datasets()?,
            config: config(api_key),
        })
    }

    /// The clients, with `api_key`, on the historical API at `base_url`, a
    /// mock peer's.
    #[cfg(test)]
    pub(super) fn with_base_url(api_key: &str, base_url: &str) -> anyhow::Result<Self> {
        let metadata = DatabentoHistoricalClient::new_with_base_url(
            Credential::new(api_key),
            PathBuf::from(PUBLISHERS),
            get_atomic_clock_realtime(),
            false,
            base_url,
        )?;
        let mut config = config(api_key);
        config.historical_base_url = Some(base_url.to_owned());
        Ok(Self {
            metadata,
            datasets: datasets()?,
            config,
        })
    }

    /// `requests` (see `catalog::missing`), definitions of `GLBX`, `XNAS`
    /// and `OPRA` instruments, each within the data of the dataset of its
    /// venue, the one Nautilus' client asks (`GLBX.MDP3`, `XNAS.ITCH`,
    /// `OPRA.PILLAR`), whose end is read once a dataset before any is asked
    /// for: Databento refuses a request past it, which a fill ending today
    /// makes (see `within`).
    ///
    /// # Errors
    ///
    /// Returns an error if a request is not a definition of an instrument
    /// Databento defines in its row of the routing table (before any
    /// request), or if a metadata request fails.
    pub async fn within_datasets(
        &self,
        requests: &[FillRequest],
    ) -> anyhow::Result<Vec<FillRequest>> {
        let mut datasets = Vec::new();
        for request in requests {
            let venue = request.instrument_id.venue;
            ensure!(
                request.data == FillData::Instrument
                    && Row::of(venue).definitions == DefinitionsSource::Databento,
                "Databento gives the definitions of GLBX, XNAS and OPRA instruments here, not \
                 {request}"
            );
            let dataset = self
                .datasets
                .get_dataset_for_venue(&venue)
                .with_context(|| format!("Databento has no dataset of {venue}"))?;
            datasets.push(dataset.to_string());
        }
        // The end of each dataset, read once.
        let mut ends: BTreeMap<String, Timestamp> = BTreeMap::new();
        let mut within_ends = Vec::new();
        for (request, dataset) in requests.iter().zip(datasets) {
            let end = match ends.get(&dataset) {
                Some(&end) => end,
                None => {
                    let available = self
                        .metadata
                        .get_dataset_range(&dataset)
                        .await
                        .with_context(|| format!("requesting the range of {dataset}"))?;
                    let end = end_of(&dataset, &available)?;
                    ends.insert(dataset, end);
                    end
                }
            };
            within_ends.push(within(*request, end));
        }
        Ok(within_ends)
    }

    /// The node's data client: Nautilus' Databento client, on no venue, so
    /// asked only by the requests naming it (see `NodeFill::add`). It opens
    /// no live feed: its `connect` starts nothing, only a subscription
    /// would.
    #[must_use]
    pub fn node_client(&self) -> NodeClient {
        NodeClient {
            factory: Box::new(DatabentoDataClientFactory::new()),
            config: Box::new(self.config.clone()),
        }
    }
}

/// The config of Nautilus' data client with `api_key`: `GLBX` instruments
/// on `GLBX`, not their exchanges' MICs, as IB maps them.
fn config(api_key: &str) -> DatabentoDataClientConfig {
    DatabentoDataClientConfig::new(api_key, PathBuf::from(PUBLISHERS), false, true)
}

/// Nautilus' map of venues to the datasets its data client asks their
/// instruments of, from the publishers' map: `XNAS` to `XNAS.ITCH`, the
/// first publisher of the venue there, `OPRA` to `OPRA.PILLAR`, the venue
/// of its consolidated publisher, and `GLBX` to `GLBX.MDP3`, among the
/// defaults Nautilus adds.
///
/// # Errors
///
/// Returns an error if the map of publishers is unreadable.
fn datasets() -> anyhow::Result<DatabentoDataLoader> {
    DatabentoDataLoader::new(Some(PathBuf::from(PUBLISHERS)))
        .context("reading Databento's publishers")
}

/// The exclusive end of `range`, `dataset`'s, which Nautilus' client gives
/// as the `Display` of Databento's `time::OffsetDateTime`, such as
/// `2023-07-20 0:00:00.12 +00:00:00`: the hour unpadded, the fraction to
/// its last nonzero digit, at least one, and the offset to the second.
fn end_of(dataset: &str, range: &DatasetRange) -> anyhow::Result<Timestamp> {
    Timestamp::strptime("%Y-%m-%d %H:%M:%S%.f %::z", &range.end)
        .with_context(|| format!("reading the end of {dataset}, {}", range.end))
}

/// `request`, a definition's, within its dataset's data, up to
/// `available_end`, exclusive: unchanged if it ends before; else asked up
/// to it, holding at least the last whole UTC day before it. Databento's
/// historical API gives a snapshot of every definition active at each UTC
/// midnight, Monday to Friday, for its publishers that do not send them
/// daily, so that "any request for definitions for a 24-hour period
/// beginning at UTC midnight is guaranteed to include definitions for all
/// instruments active during that time period" (Databento's docs,
/// "Instrument definitions", "Snapshots"). A definition holds over its
/// contract's life, so one asked before the window's end defines the
/// contract over all of it.
fn within(request: FillRequest, available_end: Timestamp) -> FillRequest {
    if request.to < available_end {
        return request;
    }
    // The window is closed, which Nautilus' client asks up to an instant
    // past its end.
    let to = available_end - SignedDuration::from_nanos(1);
    let last_day = available_end
        .to_zoned(TimeZone::UTC)
        .start_of_day()
        .and_then(|midnight| midnight.yesterday())
        .expect("a dataset's days are within jiff's range")
        .timestamp();
    FillRequest {
        from: request.from.min(last_day),
        to,
        ..request
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        data::{NautilusDataType, TradeTick},
        enums::AggressorSide,
        identifiers::{InstrumentId, TradeId},
        types::{Price, Quantity},
    };
    use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

    use jiff::SignedDuration;

    use super::*;
    use crate::{
        catalog::missing,
        strategy::{DataRequirements, TickSeries},
    };

    fn ts(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    fn new_catalog() -> (tempfile::TempDir, ParquetDataCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        (dir, catalog)
    }

    fn definition(from: &str, to: &str) -> FillRequest {
        FillRequest {
            data: FillData::Instrument,
            instrument_id: InstrumentId::from("GCZ26.GLBX"),
            from: ts(from),
            to: ts(to),
        }
    }

    /// GLBX.MDP3's end as captured on 13 November 2026: a window before it
    /// is asked as it is; one reaching past it is asked up to it, from the
    /// window's start if it holds the last whole UTC day before the end, 12
    /// November, Databento's day of every definition active, else from
    /// that day's midnight; one starting past the end too is asked so.
    #[test]
    fn a_definition_is_asked_within_its_dataset_s_end() {
        let end = ts("2026-11-13T09:31:40.093383Z");
        let up_to_end = "2026-11-13T09:31:40.093382999Z";
        for ((from, to), (within_from, within_to)) in [
            (
                ("2026-11-12T00:00:00Z", "2026-11-12T23:59:59.999999999Z"),
                ("2026-11-12T00:00:00Z", "2026-11-12T23:59:59.999999999Z"),
            ),
            (
                ("2026-11-12T00:00:00Z", "2026-11-13T23:59:59.999999999Z"),
                ("2026-11-12T00:00:00Z", up_to_end),
            ),
            (
                ("2026-11-12T12:00:00Z", "2026-11-13T23:59:59.999999999Z"),
                ("2026-11-12T00:00:00Z", up_to_end),
            ),
            (
                ("2026-11-13T09:00:00Z", "2026-11-13T09:59:59.999999999Z"),
                ("2026-11-12T00:00:00Z", up_to_end),
            ),
            (
                ("2026-11-13T12:00:00Z", "2026-11-13T12:59:59.999999999Z"),
                ("2026-11-12T00:00:00Z", up_to_end),
            ),
        ] {
            assert_eq!(
                within(definition(from, to), end),
                definition(within_from, within_to),
                "{from} to {to}"
            );
        }
    }

    /// The ends Nautilus' client gives, as Databento's `time` crate writes
    /// them (`time::OffsetDateTime`'s `Display` at the locked version, of the
    /// instants on the right), read as the instants they are.
    #[test]
    fn the_dataset_end_nautilus_gives_reads_as_its_instant() {
        for (end, instant) in [
            ("2023-07-20 0:00:00.0 +00:00:00", "2023-07-20T00:00:00Z"),
            ("2023-07-20 0:00:00.12 +00:00:00", "2023-07-20T00:00:00.12Z"),
            (
                "2023-07-20 12:34:56.123456789 +00:00:00",
                "2023-07-20T12:34:56.123456789Z",
            ),
            (
                "2026-07-14 3:33:20.000000001 +00:00:00",
                "2026-07-14T03:33:20.000000001Z",
            ),
        ] {
            let range = DatasetRange {
                start: String::new(),
                end: end.to_string(),
            };
            assert_eq!(end_of("GLBX.MDP3", &range).unwrap(), ts(instant), "{end}");
        }
    }

    /// GLBX's history is LSE's.
    #[tokio::test]
    async fn a_series_of_glbx_is_refused_before_any_request() {
        let history = DatabentoHistory::new(&format!("db-{}", "0".repeat(29))).unwrap();
        let request = FillRequest {
            data: FillData::Trades,
            instrument_id: InstrumentId::from("GCZ26.GLBX"),
            from: ts("2026-10-26T00:00:00Z"),
            to: ts("2026-10-26T12:00:00Z"),
        };

        let err = history.within_datasets(&[request]).await.unwrap_err();

        assert!(
            err.to_string().contains("the trades of GCZ26.GLBX"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn a_request_of_another_venue_is_refused_before_any_request() {
        let history = DatabentoHistory::new(&format!("db-{}", "0".repeat(29))).unwrap();
        let request = FillRequest {
            data: FillData::Trades,
            instrument_id: InstrumentId::from("GCZ26.XCEC"),
            from: ts("2026-10-26T00:00:00Z"),
            to: ts("2026-10-26T12:00:00Z"),
        };

        let err = history.within_datasets(&[request]).await.unwrap_err();

        assert!(err.to_string().contains("GCZ26.XCEC"), "{err}");
    }

    /// IB defines XCEC's instruments, as it gives their history.
    #[tokio::test]
    async fn a_definition_of_another_venue_is_refused_before_any_request() {
        let history = DatabentoHistory::new(&format!("db-{}", "0".repeat(29))).unwrap();
        let request = FillRequest {
            data: FillData::Instrument,
            instrument_id: InstrumentId::from("GCZ26.XCEC"),
            from: ts("2026-10-26T00:00:00Z"),
            to: ts("2026-10-26T12:00:00Z"),
        };

        let err = history.within_datasets(&[request]).await.unwrap_err();

        assert!(err.to_string().contains("GCZ26.XCEC"), "{err}");
    }

    /// Requirements reading the trades and book of each of `instrument_ids`.
    fn ticks_of(instrument_ids: &[&str]) -> DataRequirements {
        let ticks = instrument_ids
            .iter()
            .flat_map(|id| {
                [
                    NautilusDataType::TradeTick,
                    NautilusDataType::OrderBookDepth,
                ]
                .map(|data| TickSeries {
                    data,
                    instrument_id: InstrumentId::from(*id),
                })
            })
            .collect();
        DataRequirements {
            bar_types: vec![],
            ticks,
            custom: vec![],
            warmup: SignedDuration::ZERO,
        }
    }

    /// What the catalog lacks of the series of `instrument_ids` read over
    /// 2026-07-28 00:00 to 2026-07-29 12:00 UTC, across GC's roll from
    /// GCQ26 to GCZ26 at 2026-07-29 00:00 (two weekdays before Friday 31
    /// July, the last weekday before August, Q's delivery month): (data,
    /// contract, from, to).
    fn missing_across_the_roll(
        catalog: &ParquetDataCatalog,
        instrument_ids: &[&str],
    ) -> Vec<(FillData, String, String, String)> {
        let slices =
            ticks_of(instrument_ids).slices(ts("2026-07-28T00:00Z"), ts("2026-07-29T12:00Z"));
        missing(catalog, &slices)
            .unwrap()
            .into_iter()
            .map(|r| {
                (
                    r.data,
                    r.instrument_id.to_string(),
                    r.from.to_string(),
                    r.to.to_string(),
                )
            })
            .collect()
    }

    fn asked(data: FillData, id: &str, from: &str, to: &str) -> (FillData, String, String, String) {
        (data, id.to_string(), from.to_string(), to.to_string())
    }

    const Q6_FROM: &str = "2026-07-28T00:00:00Z";
    const Q6_TO: &str = "2026-07-28T23:59:59.999999999Z";
    const Z6_FROM: &str = "2026-07-29T00:00:00Z";
    const Z6_TO: &str = "2026-07-29T12:00:00Z";

    #[test]
    fn a_rolled_root_is_asked_contract_by_contract_over_each_slice() {
        let (_dir, catalog) = new_catalog();
        assert_eq!(
            missing_across_the_roll(&catalog, &["GC.GLBX"]),
            [
                asked(FillData::Instrument, "GCQ26.GLBX", Q6_FROM, Q6_TO),
                asked(FillData::Trades, "GCQ26.GLBX", Q6_FROM, Q6_TO),
                asked(FillData::Depth, "GCQ26.GLBX", Q6_FROM, Q6_TO),
                asked(FillData::Instrument, "GCZ26.GLBX", Z6_FROM, Z6_TO),
                asked(FillData::Trades, "GCZ26.GLBX", Z6_FROM, Z6_TO),
                asked(FillData::Depth, "GCZ26.GLBX", Z6_FROM, Z6_TO),
            ]
        );
    }

    /// GCQ26's trades stored up to 13:00 on the 28th, its book not at all.
    #[test]
    fn each_contract_of_a_rolled_root_lacks_only_what_it_does_not_store() {
        let (_dir, catalog) = new_catalog();
        let trade = TradeTick::new(
            InstrumentId::from("GCQ26.GLBX"),
            Price::from("4294.2"),
            Quantity::from(1),
            AggressorSide::Sell,
            TradeId::new("1"),
            ts("2026-07-28T12:00Z").into(),
            ts("2026-07-28T12:00Z").into(),
        );
        catalog
            .write_to_parquet(
                &[trade],
                Some(ts(Q6_FROM).into()),
                Some(ts("2026-07-28T13:00Z").into()),
                None,
            )
            .unwrap();
        assert_eq!(
            missing_across_the_roll(&catalog, &["GC.GLBX"])[..3],
            [
                asked(FillData::Instrument, "GCQ26.GLBX", Q6_FROM, Q6_TO),
                asked(
                    FillData::Trades,
                    "GCQ26.GLBX",
                    "2026-07-28T13:00:00.000000001Z",
                    Q6_TO
                ),
                asked(FillData::Depth, "GCQ26.GLBX", Q6_FROM, Q6_TO),
            ]
        );
    }

    /// GCZ26.GLBX declared as itself is one slice over the window; with
    /// GC.GLBX it is also the root's contract from the roll on.
    #[test]
    fn a_contract_declared_as_itself_and_through_its_root_is_asked_once_over_both() {
        let (_dir, catalog) = new_catalog();
        assert_eq!(
            missing_across_the_roll(&catalog, &["GC.GLBX", "GCZ26.GLBX"])[3..],
            [
                asked(FillData::Instrument, "GCZ26.GLBX", Q6_FROM, Z6_TO),
                asked(FillData::Trades, "GCZ26.GLBX", Q6_FROM, Z6_TO),
                asked(FillData::Depth, "GCZ26.GLBX", Q6_FROM, Z6_TO),
            ]
        );
    }
}
