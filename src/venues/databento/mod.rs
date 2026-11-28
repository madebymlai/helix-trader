//! Fills Databento definitions for three kinds of instruments:
//! - `GLBX`: CME Globex (`GLBX.MDP3`).
//! - `XNAS`: Nasdaq TotalView (`XNAS.ITCH`).
//! - `OPRA`: US options (`OPRA.PILLAR`).
//!
//! Nautilus' `DatabentoDataClient` fills them through the node (see `catalog::NodeFill`). Their
//! history comes from LSE (`lse`).
//!
//! This module does what Nautilus' client does not: it asks each definition only up to the end of
//! its dataset, because Databento refuses a request past that end. See `README.md`.

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

/// The path to Nautilus' `crates/adapters/databento/publishers.json` at the pinned commit.
const PUBLISHERS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/venues/databento/publishers.json"
);

/// Databento's historical API, for the definitions of `GLBX`, `XNAS` and `OPRA` instruments.
#[derive(Debug)]
pub struct DatabentoHistory {
    metadata: DatabentoHistoricalClient,
    datasets: DatabentoDataLoader,
    config: DatabentoDataClientConfig,
}

impl DatabentoHistory {
    /// Builds the clients with `api_key`.
    ///
    /// # Errors
    ///
    /// Returns an error if Databento's client cannot be built, from a malformed key or an
    /// unreadable map of publishers.
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

    /// Returns `requests` (see `catalog::missing`), each limited to its dataset's end (see
    /// `within`).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A request is not a definition of an instrument that Databento defines in its row of the
    ///   routing table (checked before any request).
    /// - A metadata request fails.
    pub async fn within_datasets(
        &self,
        requests: &[FillRequest],
    ) -> anyhow::Result<Vec<FillRequest>> {
        let mut datasets = Vec::new();

        for request in requests {
            let venue = request.data.instrument_id().venue;
            anyhow::ensure!(
                matches!(request.data, FillData::Instrument(_))
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

    /// Returns the node's data client.
    ///
    /// The client is on no venue, so only requests that name it reach it (see `NodeFill::add`). It
    /// opens no live feed. Only a subscription would.
    #[must_use]
    pub fn node_client(&self) -> NodeClient {
        NodeClient {
            factory: Box::new(DatabentoDataClientFactory::new()),
            config: Box::new(self.config.clone()),
        }
    }
}

// `GLBX` instruments sit on `GLBX`, not on their exchanges' MICs, as IB maps them
fn config(api_key: &str) -> DatabentoDataClientConfig {
    DatabentoDataClientConfig::new(api_key, PathBuf::from(PUBLISHERS), false, true)
}

/// Returns Nautilus' map from venues to the datasets its client asks:
/// - `XNAS` to `XNAS.ITCH`, the venue's first publisher.
/// - `OPRA` to `OPRA.PILLAR`, the venue of its consolidated publisher.
/// - `GLBX` to `GLBX.MDP3`, a default Nautilus adds.
///
/// # Errors
///
/// Returns an error if the map of publishers is unreadable.
fn datasets() -> anyhow::Result<DatabentoDataLoader> {
    DatabentoDataLoader::new(Some(PathBuf::from(PUBLISHERS)))
        .context("reading Databento's publishers")
}

/// Returns the exclusive end of `range`.
///
/// Nautilus' client gives the end as the `Display` of Databento's `time::OffsetDateTime`, such as
/// `2023-07-20 0:00:00.12 +00:00:00`, where:
/// - The hour is unpadded.
/// - The fraction runs to its last nonzero digit, with at least one digit.
/// - The offset runs to the second.
fn end_of(dataset: &str, range: &DatasetRange) -> anyhow::Result<Timestamp> {
    Timestamp::strptime("%Y-%m-%d %H:%M:%S%.f %::z", &range.end)
        .with_context(|| format!("reading the end of {dataset}, {}", range.end))
}

/// Limits `request` to end before `available_end`, which is exclusive.
///
/// Returns the request unchanged if it ends before `available_end`, else asks up to it and keeps at
/// least the last whole UTC day before it.
///
/// Databento snapshots every active definition at each UTC midnight, Monday to Friday, for
/// publishers that do not send them daily. Its docs say: "any request for definitions for a
/// 24-hour period beginning at UTC midnight is guaranteed to include definitions for all
/// instruments active during that time period" (Databento's docs, "Instrument definitions",
/// "Snapshots"). A definition holds
/// over its contract's life.
fn within(request: FillRequest, available_end: Timestamp) -> FillRequest {
    if request.to < available_end {
        return request;
    }

    // Nautilus' client asks up to an instant past a closed window's end
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
            data: FillData::Instrument(InstrumentId::from("GCZ26.GLBX")),
            from: ts(from),
            to: ts(to),
        }
    }

    /// GLBX.MDP3's end is as captured on 13 November 2026.
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

    const UP_TO_END: &str = "2026-11-13T09:31:40.093382999Z";

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

    /// GLBX's history comes from LSE, and IB defines XCEC's instruments.
    #[rstest]
    #[case::a_series_of_glbx(
        FillData::Trades(InstrumentId::from("GCZ26.GLBX")),
        "the trades of GCZ26.GLBX"
    )]
    #[case::a_series_of_another_venue(
        FillData::Trades(InstrumentId::from("GCZ26.XCEC")),
        "GCZ26.XCEC"
    )]
    #[case::a_definition_of_another_venue(
        FillData::Instrument(InstrumentId::from("GCZ26.XCEC")),
        "GCZ26.XCEC"
    )]
    #[tokio::test]
    async fn a_request_databento_does_not_serve_is_refused_before_any_request(
        #[case] data: FillData,
        #[case] named: &str,
    ) {
        let history = DatabentoHistory::new(&format!("db-{}", "0".repeat(29))).unwrap();
        let request = FillRequest {
            data,
            from: ts("2026-10-26T00:00:00Z"),
            to: ts("2026-10-26T12:00:00Z"),
        };

        let err = history.within_datasets(&[request]).await.unwrap_err();

        assert!(err.to_string().contains(named), "{err}");
    }
}
