//! Recording the live data into the catalog.
//!
//! The live node streams the market data it receives, and the instrument
//! definitions its clients load, through Nautilus' streaming writer
//! (`StreamingConfig`): Feather files appended under a staging directory
//! outside the catalog, in `live/<instance id>/`, promoted into the catalog
//! the node registers every promotion interval and on a clean stop, each
//! staging file deleted once promoted. A retrain the node starts then
//! replays up to the last promotion: it waits `Recording::catalog_delay`
//! after its time for the data up to it. Each promotion names its files by
//! their rows, so a run's files of a series leave gaps between them, which
//! the readers join (`data::live_runs`, from the run starts `Live`
//! records).
//!
//! The node keeps one instance id per trader across restarts, in a file in
//! the staging directory: a run after a crash writes to the same
//! `live/<instance id>/`, where Nautilus recovers the files the crashed
//! run left; the start promotes them as soon as the node is built
//! (`promote_staged`), before its warmup reads the catalog and writes back
//! to it.
//!
//! Order and position events, account states and custom data are not
//! streamed: the catalog holds market data, and the strategies' custom data
//! has a store of its own.

use std::{
    io::ErrorKind,
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, anyhow, ensure};
use jiff::SignedDuration;
use nautilus_core::UUID4;
use nautilus_live::node::LiveNode;
use nautilus_model::{data::NautilusDataType, identifiers::TraderId};
use nautilus_persistence::config::{DataCatalogConfig, RotationConfig, StreamingConfig};

use crate::data::catalog_path;

/// Where the live node stages the data it streams, and how often it
/// promotes it into the catalog.
#[derive(Debug, Clone)]
pub struct Recording {
    /// The staging directory, outside the catalog; created if missing. It
    /// also keeps each trader's instance id.
    pub staging: PathBuf,
    /// How often the staged data is promoted into the catalog: the delay
    /// before a retrain sees it.
    pub promotion_interval: Duration,
}

impl Recording {
    /// The streaming config of the node registering `catalog`, an absolute
    /// path: staged under `staging`, promoted into `catalog` every
    /// `promotion_interval` and on a clean stop, staging files deleted once
    /// promoted.
    ///
    /// # Errors
    ///
    /// Returns an error if the staging directory is inside the catalog or
    /// its path is not UTF-8, or if Nautilus refuses the config (a
    /// promotion interval under a millisecond).
    pub fn streaming(&self, catalog: &DataCatalogConfig) -> anyhow::Result<StreamingConfig> {
        let staging = catalog_path(&self.staging)?;
        // Apart, so that readers and maintenance of the catalog never meet
        // unpromoted files.
        ensure!(
            !Path::new(&staging).starts_with(catalog.path()),
            "the staging directory {staging} is inside the catalog {}: give one outside it",
            catalog.path()
        );
        let mut streaming = StreamingConfig::new(
            staging,
            Some(catalog.clone()),
            FLUSH_INTERVAL_MS,
            false,
            RotationConfig::NoRotation,
        );
        streaming.promotion_interval_ms = Some(
            u64::try_from(self.promotion_interval.as_millis())
                .context("the promotion interval overflows u64 milliseconds")?,
        );
        streaming.promote_on_close = true;
        streaming.delete_feather_after_promotion = true;
        streaming.data_types = Some(STREAMED.to_vec());
        streaming
            .validate()
            .context("Nautilus refuses the streaming config")?;
        Ok(streaming)
    }

    /// How long after a time the data the node received up to it is in
    /// the catalog: a promotion interval, the promotions not being on a
    /// grid, and `PROMOTION_MARGIN` past it. A retrain the node starts at a
    /// time waits that long, so that its replay finds the data up to it.
    #[must_use]
    pub fn catalog_delay(&self) -> SignedDuration {
        SignedDuration::try_from(self.promotion_interval)
            .map_or(SignedDuration::MAX, |interval| interval + PROMOTION_MARGIN)
    }

    /// The instance id of `trader_id`'s node: the one its last run kept in
    /// the staging directory, else a new one, kept there.
    ///
    /// # Errors
    ///
    /// Returns an error if the staging directory cannot be created, or the
    /// trader's instance id file cannot be read, holds no instance id, or
    /// cannot be written.
    pub fn instance_id(&self, trader_id: TraderId) -> anyhow::Result<UUID4> {
        let file = self.staging.join(format!("{trader_id}.instance-id"));
        match std::fs::read_to_string(&file) {
            Ok(kept) => kept.trim().parse::<UUID4>().map_err(|e| {
                anyhow!(
                    "the instance id of {trader_id} in {} is not one ({e}): restore it, \
                     or delete it to strand what its runs left unpromoted",
                    file.display()
                )
            }),
            Err(e) if e.kind() == ErrorKind::NotFound => {
                std::fs::create_dir_all(&self.staging).with_context(|| {
                    format!("creating the staging directory {}", self.staging.display())
                })?;
                let instance_id = UUID4::new();
                // Written whole or not at all: a crash leaves no half id.
                let written = self.staging.join(format!("{trader_id}.instance-id.new"));
                std::fs::write(&written, instance_id.to_string())
                    .and_then(|()| std::fs::rename(&written, &file))
                    .with_context(|| {
                        format!(
                            "keeping the instance id of {trader_id} in {}",
                            file.display()
                        )
                    })?;
                Ok(instance_id)
            }
            Err(e) => Err(e).with_context(|| {
                format!(
                    "reading the instance id of {trader_id} from {}",
                    file.display()
                )
            }),
        }
    }
}

/// Promotes into the catalog what the last run of `node`'s instance left
/// staged, a crashed run's rows: call it once the node is built, before it
/// runs. The node's writer recovered those files when it was built, but
/// would promote them only at its first promotion, after the strategy's
/// warmup has read the catalog and written the venue's answer back over
/// the crash, which they would then overlap: Nautilus refuses them, and
/// they are lost. Closing the writer promotes them with Nautilus' own
/// promotion (`StreamingConfig::promote_on_close`); it is reopened in the
/// same run directory, empty, before the node runs.
///
/// # Errors
///
/// Returns an error if the staged files cannot be promoted, the catalog
/// already holding rows over them, or the writer cannot be reopened.
pub fn promote_staged(node: &mut LiveNode) -> anyhow::Result<()> {
    let kernel = node.kernel_mut();
    kernel
        .close_streaming_writer()
        .context("promoting what the last run of this instance left staged")?;
    kernel
        .reopen_streaming_writer()
        .context("reopening the streaming writer")
}

/// How often the writer flushes what it buffered to its staging files: what
/// a crash loses at most.
const FLUSH_INTERVAL_MS: u64 = 1_000;

/// How long past a promotion interval the data received up to a time is
/// surely in the catalog: past its flush (`FLUSH_INTERVAL_MS`), the live
/// node's bar build delay for the bars it builds, and the promotion's own
/// writing.
const PROMOTION_MARGIN: SignedDuration = SignedDuration::from_mins(1);

/// The data the node streams: the market data its strategy can read and
/// backtests replay, and the instrument definitions, which a backtest over
/// the catalog needs.
const STREAMED: [NautilusDataType; 9] = [
    NautilusDataType::Instrument,
    NautilusDataType::QuoteTick,
    NautilusDataType::TradeTick,
    NautilusDataType::OrderBookDelta,
    NautilusDataType::OrderBookDepth,
    NautilusDataType::Bar,
    NautilusDataType::MarkPriceUpdate,
    NautilusDataType::IndexPriceUpdate,
    NautilusDataType::FundingRateUpdate,
];

#[cfg(test)]
mod tests {
    use super::*;

    fn recording(staging: PathBuf) -> Recording {
        Recording {
            staging,
            promotion_interval: Duration::from_secs(5 * 60),
        }
    }

    fn catalog(dir: &std::path::Path) -> DataCatalogConfig {
        DataCatalogConfig::new(dir.to_str().unwrap().to_string(), None, None)
    }

    #[test]
    fn the_staged_data_is_promoted_into_the_catalog_every_interval_and_on_stop() {
        let dir = tempfile::tempdir().unwrap();
        let streaming = recording(dir.path().join("staging"))
            .streaming(&catalog(&dir.path().join("catalog")))
            .unwrap();
        assert_eq!(
            streaming.writer_path,
            dir.path().join("staging").to_str().unwrap()
        );
        assert_eq!(
            streaming.catalog.unwrap().path(),
            dir.path().join("catalog").to_str().unwrap()
        );
        assert_eq!(streaming.promotion_interval_ms, Some(300_000));
        assert!(streaming.promote_on_close);
        assert!(streaming.delete_feather_after_promotion);
    }

    #[test]
    fn data_is_in_the_catalog_a_promotion_interval_and_a_minute_after_it_arrives() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            recording(dir.path().join("staging")).catalog_delay(),
            SignedDuration::from_mins(6)
        );
    }

    #[test]
    fn a_restart_appends_to_what_the_last_run_staged() {
        let dir = tempfile::tempdir().unwrap();
        let streaming = recording(dir.path().join("staging"))
            .streaming(&catalog(&dir.path().join("catalog")))
            .unwrap();
        assert!(!streaming.replace_existing);
    }

    #[test]
    fn market_data_and_instruments_are_streamed_and_nothing_else() {
        let dir = tempfile::tempdir().unwrap();
        let streaming = recording(dir.path().join("staging"))
            .streaming(&catalog(&dir.path().join("catalog")))
            .unwrap();
        let data_types = streaming.data_types.unwrap();
        for streamed in [
            NautilusDataType::Instrument,
            NautilusDataType::QuoteTick,
            NautilusDataType::TradeTick,
            NautilusDataType::OrderBookDelta,
            NautilusDataType::OrderBookDepth,
            NautilusDataType::Bar,
            NautilusDataType::FundingRateUpdate,
        ] {
            assert!(data_types.contains(&streamed), "{streamed} not streamed");
        }
        assert!(
            !data_types
                .iter()
                .any(|t| matches!(t, NautilusDataType::Custom { .. })),
            "{data_types:?}"
        );
        assert_eq!(streaming.record_types, None);
        assert_eq!(streaming.record_filters, None);
    }

    #[test]
    fn a_staging_directory_inside_the_catalog_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let catalog_dir = dir.path().join("catalog");
        std::fs::create_dir(&catalog_dir).unwrap();
        let err = recording(catalog_dir.join("staging"))
            .streaming(&catalog(&catalog_dir))
            .unwrap_err()
            .to_string();
        assert!(err.contains("inside the catalog"), "{err}");
    }

    #[test]
    fn a_trader_keeps_its_instance_id_across_runs() {
        let dir = tempfile::tempdir().unwrap();
        let trader_id = TraderId::from("HELIX-001");
        let first = recording(dir.path().join("staging"))
            .instance_id(trader_id)
            .unwrap();
        let second = recording(dir.path().join("staging"))
            .instance_id(trader_id)
            .unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn each_trader_has_its_own_instance_id() {
        let dir = tempfile::tempdir().unwrap();
        let recording = recording(dir.path().join("staging"));
        let first = recording.instance_id(TraderId::from("HELIX-001")).unwrap();
        let second = recording.instance_id(TraderId::from("HELIX-002")).unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn an_instance_id_file_without_an_instance_id_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let recording = recording(dir.path().join("staging"));
        let trader_id = TraderId::from("HELIX-001");
        recording.instance_id(trader_id).unwrap();
        for entry in std::fs::read_dir(&recording.staging).unwrap() {
            std::fs::write(entry.unwrap().path(), "not an id").unwrap();
        }
        let err = format!("{:#}", recording.instance_id(trader_id).unwrap_err());
        assert!(err.contains("HELIX-001"), "{err}");
    }
}
