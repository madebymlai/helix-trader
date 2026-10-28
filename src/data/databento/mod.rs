//! Databento history of `GLBX` instruments (CME Globex, `GLBX.MDP3`),
//! filled through the node (see `node_fill`) by Nautilus' Databento data
//! client (`DatabentoDataClient`), whose answers the data engine writes
//! back to the catalog: definitions, MBP-10 as `OrderBookDepth`
//! snapshots, trades as `TradeTick`s, quotes, each contract under its own
//! id. What stays here is what Nautilus' client does not do: Databento's
//! cost quotes and the dataset's end, checked before the node starts
//! (paid requests are quoted first), and the period each series is asked
//! in. See `README.md`.

use std::path::PathBuf;

use anyhow::{Context, ensure};
use databento::{dbn::Schema, historical::metadata::GetCostParams};
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_databento::{
    common::get_date_time_range, data::DatabentoDataClientConfig,
    factories::DatabentoDataClientFactory, symbology::infer_symbology_type,
};
use nautilus_live::config::RoutingConfig;
use nautilus_model::identifiers::{InstrumentId, Venue};

use super::node_fill::{FillData, FillRequest, NodeClient};

/// Nautilus' map of Databento publishers to venues, copied from
/// `crates/adapters/databento/publishers.json` at the pinned commit: the
/// client reads it from a path.
const PUBLISHERS: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/src/data/databento/publishers.json"
);

/// The Databento dataset of `GLBX` instruments.
const DATASET: &str = "GLBX.MDP3";

/// The window of one MBP-10 request: three hours of COMEX gold are about
/// a million snapshots, a day about 7 million (some 7.5 GB decoded, which
/// Nautilus' write copies once more).
const DEPTH_PERIOD: SignedDuration = SignedDuration::from_hours(3);

/// The window of one request of another series: a UTC day.
const DAY: SignedDuration = SignedDuration::from_hours(24);

/// Databento's historical API, for the history of `GLBX` instruments.
#[derive(Debug)]
pub struct DatabentoHistory {
    /// Databento's own client, for the metadata Nautilus' does not give:
    /// cost quotes and the dataset's end.
    metadata: databento::HistoricalClient,
    /// The config of Nautilus' data client, the node's.
    config: DatabentoDataClientConfig,
}

impl DatabentoHistory {
    /// The clients, with `api_key`.
    ///
    /// # Errors
    ///
    /// Returns an error if Databento's client cannot be built (a malformed
    /// key).
    pub fn new(api_key: &str) -> anyhow::Result<Self> {
        let metadata = databento::HistoricalClient::builder()
            .key(api_key)
            .context("building Databento's client")?
            .build()
            .context("building Databento's client")?;
        let config =
            DatabentoDataClientConfig::new(api_key, PathBuf::from(PUBLISHERS), false, false);
        Ok(Self { metadata, config })
    }

    /// Whether `id` is on `GLBX`, the venue Nautilus gives CME Globex
    /// records (`publishers.json`), so its history comes from here.
    #[must_use]
    pub fn serves(id: InstrumentId) -> bool {
        id.venue == Venue::GLBX()
    }

    /// The window one request of `data` holds at most (see
    /// `NodeFill::add`): three hours of MBP-10, a day of anything else.
    #[must_use]
    pub fn period(data: FillData) -> SignedDuration {
        match data {
            FillData::Depth => DEPTH_PERIOD,
            _ => DAY,
        }
    }

    /// Checks `requests`, of `GLBX` instruments (see `node_fill::missing`),
    /// against the dataset's end, then logs Databento's quote for each and
    /// their total, before any data is asked for.
    ///
    /// # Errors
    ///
    /// Returns an error if a request is not of a `GLBX` instrument (before
    /// any request), if one ends at or past the data Databento has (a
    /// request over it would fail), or if a metadata request fails.
    pub async fn quote(&self, requests: &[FillRequest]) -> anyhow::Result<()> {
        for request in requests {
            ensure!(
                Self::serves(request.instrument_id),
                "Databento serves GLBX instruments here, not {}",
                request.instrument_id
            );
        }
        let Some(end) = requests.iter().map(|request| request.to).max() else {
            return Ok(());
        };
        let available = self
            .metadata
            .clone()
            .metadata()
            .get_dataset_range(DATASET)
            .await
            .with_context(|| format!("requesting the range of {DATASET}"))?;
        ensure_available(
            end,
            Timestamp::from_nanosecond(available.end.unix_timestamp_nanos())?,
        )?;
        let mut total = 0.0;
        for request in requests {
            total += self.log_cost(request).await?;
        }
        log::info!("Databento quotes ${total:.2} in all");
        Ok(())
    }

    /// The node's data client: Nautilus' Databento client, routed the
    /// requests of `GLBX` instruments. It opens no live feed: its
    /// `connect` starts nothing, only a subscription would.
    #[must_use]
    pub fn node_client(&self) -> NodeClient {
        NodeClient {
            factory: Box::new(DatabentoDataClientFactory::new()),
            config: Box::new(self.config.clone()),
            routing: RoutingConfig {
                default: false,
                venues: Some(vec![Venue::GLBX().to_string()]),
            },
        }
    }

    /// Logs and returns Databento's quote for `request`.
    async fn log_cost(&self, request: &FillRequest) -> anyhow::Result<f64> {
        let symbol = request.instrument_id.symbol.to_string();
        let schema = schema(request.data);
        let params = GetCostParams::builder()
            .dataset(DATASET)
            .symbols(vec![symbol.as_str()])
            .schema(schema)
            .stype_in(infer_symbology_type(&symbol))
            .date_time_range(range(request)?)
            .build();
        let cost = self
            .metadata
            .clone()
            .metadata()
            .get_cost(&params)
            .await
            .with_context(|| format!("quoting {request}"))?;
        log::info!("Databento quotes ${cost:.2} for {request} ({schema})");
        Ok(cost)
    }
}

/// The Databento schema Nautilus' client asks `data` in.
fn schema(data: FillData) -> Schema {
    match data {
        FillData::Instrument => Schema::Definition,
        FillData::Quotes => Schema::Mbp1,
        FillData::Trades => Schema::Trades,
        FillData::Depth => Schema::Mbp10,
    }
}

/// Refuses an `end` Databento has no data up to yet: a request over it
/// would fail.
fn ensure_available(end: Timestamp, available_end: Timestamp) -> anyhow::Result<()> {
    ensure!(
        end < available_end,
        "{DATASET} has data up to {available_end} only, not up to {end}"
    );
    Ok(())
}

/// The window of `request` as Databento's date-time range, with an
/// exclusive end.
fn range(request: &FillRequest) -> anyhow::Result<databento::historical::DateTimeRange> {
    let end = UnixNanos::from(request.to).as_u64() + 1;
    get_date_time_range(UnixNanos::from(request.from), UnixNanos::from(end))
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        data::{NautilusDataType, TradeTick},
        enums::AggressorSide,
        identifiers::TradeId,
        types::{Price, Quantity},
    };
    use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

    use super::*;
    use crate::{
        data::node_fill::missing,
        strategy::{DataRequirements, TickData, TickSeries},
    };

    fn ts(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    fn new_catalog() -> (tempfile::TempDir, ParquetDataCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        (dir, catalog)
    }

    #[test]
    fn serves_glbx_instruments_only() {
        assert!(DatabentoHistory::serves(InstrumentId::from("GC.v.0.GLBX")));
        assert!(!DatabentoHistory::serves(InstrumentId::from("GCZ6.XCEC")));
        assert!(!DatabentoHistory::serves(InstrumentId::from(
            "BTC-USDT-SWAP.OKX"
        )));
    }

    #[test]
    fn an_end_databento_has_no_data_up_to_is_refused() {
        let monday = ts("2026-10-26T00:00:00Z");
        let before = monday - SignedDuration::from_nanos(1);
        assert!(ensure_available(before, monday).is_ok());
        let err = ensure_available(monday, monday).unwrap_err().to_string();
        assert!(err.contains("2026-10-26"), "{err}");
    }

    #[test]
    fn mbp_10_is_asked_three_hours_a_request_anything_else_a_day() {
        assert_eq!(
            DatabentoHistory::period(FillData::Depth),
            SignedDuration::from_hours(3)
        );
        for data in [FillData::Trades, FillData::Quotes] {
            assert_eq!(
                DatabentoHistory::period(data),
                SignedDuration::from_hours(24)
            );
        }
    }

    #[test]
    fn each_series_is_quoted_in_the_schema_the_client_asks_it_in() {
        assert_eq!(schema(FillData::Depth), Schema::Mbp10);
        assert_eq!(schema(FillData::Trades), Schema::Trades);
        assert_eq!(schema(FillData::Quotes), Schema::Mbp1);
        assert_eq!(schema(FillData::Instrument), Schema::Definition);
    }

    #[test]
    fn a_quote_range_ends_1_ns_after_the_request() {
        let request = FillRequest {
            data: FillData::Depth,
            instrument_id: InstrumentId::from("GCZ6.GLBX"),
            from: ts("2026-10-26T00:00:00Z"),
            to: ts("2026-10-26T02:59:59.999999999Z"),
        };
        let range = range(&request).unwrap();
        assert_eq!(
            range.end.unix_timestamp_nanos(),
            ts("2026-10-26T03:00:00Z").as_nanosecond()
        );
    }

    #[tokio::test]
    async fn a_request_of_another_venue_is_refused_before_any_request() {
        let history = DatabentoHistory::new(&format!("db-{}", "0".repeat(29))).unwrap();
        let request = FillRequest {
            data: FillData::Trades,
            instrument_id: InstrumentId::from("GCZ6.XCEC"),
            from: ts("2026-10-26T00:00:00Z"),
            to: ts("2026-10-26T12:00:00Z"),
        };

        let err = history.quote(&[request]).await.unwrap_err();

        assert!(err.to_string().contains("GCZ6.XCEC"), "{err}");
    }

    /// Requirements reading the trades and book of each of `instrument_ids`.
    fn ticks_of(instrument_ids: &[&str]) -> DataRequirements {
        let ticks = instrument_ids
            .iter()
            .flat_map(|id| {
                [TickData::Trades, TickData::Depth].map(|data| TickSeries {
                    data,
                    instrument_id: InstrumentId::from(*id),
                })
            })
            .collect();
        DataRequirements {
            bar_types: vec![],
            ticks,
            warmup: SignedDuration::ZERO,
        }
    }

    /// What the catalog lacks of the series of `instrument_ids` read over
    /// 2026-07-28 00:00 to 2026-07-29 12:00 UTC, across GC's roll from
    /// GCQ6 to GCZ6 at 2026-07-29 00:00 (two weekdays before Friday 31
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
                asked(FillData::Instrument, "GCQ6.GLBX", Q6_FROM, Q6_TO),
                asked(FillData::Trades, "GCQ6.GLBX", Q6_FROM, Q6_TO),
                asked(FillData::Depth, "GCQ6.GLBX", Q6_FROM, Q6_TO),
                asked(FillData::Instrument, "GCZ6.GLBX", Z6_FROM, Z6_TO),
                asked(FillData::Trades, "GCZ6.GLBX", Z6_FROM, Z6_TO),
                asked(FillData::Depth, "GCZ6.GLBX", Z6_FROM, Z6_TO),
            ]
        );
    }

    /// GCQ6's trades stored up to 13:00 on the 28th, its book not at all.
    #[test]
    fn each_contract_of_a_rolled_root_lacks_only_what_it_does_not_store() {
        let (_dir, catalog) = new_catalog();
        let trade = TradeTick::new(
            InstrumentId::from("GCQ6.GLBX"),
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
                asked(FillData::Instrument, "GCQ6.GLBX", Q6_FROM, Q6_TO),
                asked(
                    FillData::Trades,
                    "GCQ6.GLBX",
                    "2026-07-28T13:00:00.000000001Z",
                    Q6_TO
                ),
                asked(FillData::Depth, "GCQ6.GLBX", Q6_FROM, Q6_TO),
            ]
        );
        assert_eq!(
            catalog
                .get_intervals(&NautilusDataType::TradeTick.into(), Some("GCQ6.GLBX"))
                .unwrap()
                .len(),
            1
        );
    }

    /// GCZ6.GLBX declared as itself is one slice over the window; with
    /// GC.GLBX it is also the root's contract from the roll on.
    #[test]
    fn a_contract_declared_as_itself_and_through_its_root_is_asked_once_over_both() {
        let (_dir, catalog) = new_catalog();
        assert_eq!(
            missing_across_the_roll(&catalog, &["GC.GLBX", "GCZ6.GLBX"])[3..],
            [
                asked(FillData::Instrument, "GCZ6.GLBX", Q6_FROM, Z6_TO),
                asked(FillData::Trades, "GCZ6.GLBX", Q6_FROM, Z6_TO),
                asked(FillData::Depth, "GCZ6.GLBX", Q6_FROM, Z6_TO),
            ]
        );
    }
}
