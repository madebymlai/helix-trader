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

use anyhow::Context;
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
            anyhow::ensure!(
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
    use nautilus_model::identifiers::InstrumentId;
    use rstest::rstest;

    use super::*;

    fn ts(s: &str) -> Timestamp {
        s.parse().unwrap()
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
    #[rstest]
    #[case::before_the_end(
        ("2026-11-12T00:00:00Z", "2026-11-12T23:59:59.999999999Z"),
        ("2026-11-12T00:00:00Z", "2026-11-12T23:59:59.999999999Z")
    )]
    #[case::past_the_end_from_the_last_whole_day(
        ("2026-11-12T00:00:00Z", "2026-11-13T23:59:59.999999999Z"),
        ("2026-11-12T00:00:00Z", UP_TO_END)
    )]
    #[case::past_the_end_from_within_the_last_whole_day(
        ("2026-11-12T12:00:00Z", "2026-11-13T23:59:59.999999999Z"),
        ("2026-11-12T00:00:00Z", UP_TO_END)
    )]
    #[case::around_the_end(
        ("2026-11-13T09:00:00Z", "2026-11-13T09:59:59.999999999Z"),
        ("2026-11-12T00:00:00Z", UP_TO_END)
    )]
    #[case::starting_past_the_end(
        ("2026-11-13T12:00:00Z", "2026-11-13T12:59:59.999999999Z"),
        ("2026-11-12T00:00:00Z", UP_TO_END)
    )]
    fn a_definition_is_asked_within_its_dataset_s_end(
        #[case] (from, to): (&str, &str),
        #[case] (within_from, within_to): (&str, &str),
    ) {
        let end = ts("2026-11-13T09:31:40.093383Z");
        assert_eq!(
            within(definition(from, to), end),
            definition(within_from, within_to)
        );
    }

    /// 1 ns before GLBX.MDP3's end as captured on 13 November 2026.
    const UP_TO_END: &str = "2026-11-13T09:31:40.093382999Z";

    /// The ends Nautilus' client gives, as Databento's `time` crate writes
    /// them (`time::OffsetDateTime`'s `Display` at the locked version, of the
    /// instants on the right), read as the instants they are.
    #[rstest]
    #[case::of_a_tenth_of_a_second("2023-07-20 0:00:00.0 +00:00:00", "2023-07-20T00:00:00Z")]
    #[case::of_hundredths("2023-07-20 0:00:00.12 +00:00:00", "2023-07-20T00:00:00.12Z")]
    #[case::of_nanoseconds(
        "2023-07-20 12:34:56.123456789 +00:00:00",
        "2023-07-20T12:34:56.123456789Z"
    )]
    #[case::of_one_nanosecond(
        "2026-07-14 3:33:20.000000001 +00:00:00",
        "2026-07-14T03:33:20.000000001Z"
    )]
    fn the_dataset_end_nautilus_gives_reads_as_its_instant(
        #[case] end: &str,
        #[case] instant: &str,
    ) {
        let range = DatasetRange {
            start: String::new(),
            end: end.to_string(),
        };
        assert_eq!(end_of("GLBX.MDP3", &range).unwrap(), ts(instant));
    }

    /// GLBX's history is LSE's; IB defines XCEC's instruments, as it
    /// gives their history.
    #[rstest]
    #[case::a_series_of_glbx(FillData::Trades, "GCZ26.GLBX", "the trades of GCZ26.GLBX")]
    #[case::a_series_of_another_venue(FillData::Trades, "GCZ26.XCEC", "GCZ26.XCEC")]
    #[case::a_definition_of_another_venue(FillData::Instrument, "GCZ26.XCEC", "GCZ26.XCEC")]
    #[tokio::test]
    async fn a_request_databento_does_not_serve_is_refused_before_any_request(
        #[case] data: FillData,
        #[case] instrument_id: &str,
        #[case] named: &str,
    ) {
        let history = DatabentoHistory::new(&format!("db-{}", "0".repeat(29))).unwrap();
        let request = FillRequest {
            data,
            instrument_id: InstrumentId::from(instrument_id),
            from: ts("2026-10-26T00:00:00Z"),
            to: ts("2026-10-26T12:00:00Z"),
        };

        let err = history.within_datasets(&[request]).await.unwrap_err();

        assert!(err.to_string().contains(named), "{err}");
    }
}
