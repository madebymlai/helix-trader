//! Records the live data into the catalog.
//!
//! The live node streams the market data it receives and the instrument definitions its clients
//! load, through Nautilus' streaming writer (`StreamingConfig`).
//! - The writer creates Feather files under a staging directory outside the catalog, in
//!   `live/<instance id>/`.
//! - The files are promoted into the catalog daily and on a clean stop, then deleted.
//! - The retrain actor checks catalog coverage, not staged rows (`model::launcher`).
//!
//! Each promotion names its files by their rows. So a series leaves a daily sliver between files,
//! and the catalog counts that sliver as missing. The live warmup and fill-catalog ask the venue
//! for it once and write the answer back.
//!
//! The node keeps one instance id per trader across restarts, in the staging directory. A run
//! after a crash writes to the same `live/<instance id>/`, where Nautilus recovers the crashed
//! run's files. `promote_staged` promotes them before the warmup reads the catalog. Order and
//! position events, account states and custom data are not streamed.

use std::{io::ErrorKind, path::PathBuf};

use anyhow::Context;
use jiff::SignedDuration;
use nautilus_core::UUID4;
use nautilus_live::node::LiveNode;
use nautilus_model::{data::NautilusDataType, identifiers::TraderId};
use nautilus_persistence::{
    common::storage::normalize_storage_location,
    config::{DataCatalogConfig, RotationConfig, StreamingConfig},
};

/// Where the live node stages the data it streams.
#[derive(Debug, Clone)]
pub struct Recording {
    /// The staging directory, outside the catalog and created if missing.
    ///
    /// It also keeps the instance id of each trader.
    pub staging: PathBuf,
}

impl Recording {
    /// Returns the streaming config of the node that registers `catalog`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The staging directory is inside the catalog.
    /// - Either path cannot be resolved.
    /// - The staging path is not UTF-8.
    pub fn streaming(&self, catalog: &DataCatalogConfig) -> anyhow::Result<StreamingConfig> {
        let staging = self
            .staging
            .to_str()
            .with_context(|| format!("staging path {} is not UTF-8", self.staging.display()))?;
        // The directories stay apart so that readers of the catalog never meet unpromoted files,
        // compared by the paths where Nautilus finds them: against the working directory, with
        // `..` and symlinks resolved.
        let staged = normalize_storage_location(staging)
            .with_context(|| format!("resolving the staging directory {staging}"))?;
        let promoted = normalize_storage_location(&catalog.connect_config().uri)
            .with_context(|| format!("resolving the catalog {}", catalog.path()))?;
        anyhow::ensure!(
            staged != promoted && !staged.starts_with(&format!("{promoted}/")),
            "the staging directory {staging} is inside the catalog {}: give one outside it",
            catalog.path()
        );
        let mut streaming = StreamingConfig::new(
            staging.to_owned(),
            Some(catalog.clone()),
            FLUSH_INTERVAL_MS,
            false,
            RotationConfig::NoRotation,
        );
        streaming.promotion_interval_ms = Some(
            u64::try_from(PROMOTION_INTERVAL.as_millis())
                .expect("a day of milliseconds fits in u64"),
        );
        // Closing the writer promotes what it staged, and `promote_staged` relies on that
        streaming.promote_on_close = true;
        streaming.delete_feather_after_promotion = true;
        streaming.data_types = Some(STREAMED.to_vec());
        streaming
            .validate()
            .context("Nautilus refuses the streaming config")?;
        Ok(streaming)
    }

    /// Returns the instance id of the node of `trader_id`.
    ///
    /// That is the id the last run kept, or else a new id, which is kept.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The staging directory cannot be created.
    /// - The trader's instance id file cannot be read.
    /// - The file holds no instance id.
    /// - The file cannot be written.
    pub fn instance_id(&self, trader_id: TraderId) -> anyhow::Result<UUID4> {
        let file = self.staging.join(format!("{trader_id}.instance-id"));
        match std::fs::read_to_string(&file) {
            Ok(kept) => kept.trim().parse::<UUID4>().map_err(|e| {
                anyhow::anyhow!(
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
                // Written whole or not at all, so a crash leaves no half id
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

/// Promotes into the catalog what the last run of `node`'s instance left staged.
///
/// Call it once the node is built, before it runs.
///
/// If the node's writer promoted them, it would do so only after the warmup wrote the venue's
/// answer over the crashed run's rows. Nautilus refuses the overlap, so the rows would be lost.
/// Closing the writer promotes them instead (`StreamingConfig::promote_on_close`), and the writer
/// is reopened empty. This is the kernel's only public way to promote now, because the writer's
/// `promote_now` is internal.
///
/// # Errors
///
/// Returns an error if:
/// - The staged files cannot be promoted, because the catalog already holds rows over them.
/// - The writer cannot be reopened.
pub(super) fn promote_staged(node: &mut LiveNode) -> anyhow::Result<()> {
    let kernel = node.kernel_mut();
    kernel
        .close_streaming_writer()
        .context("promoting what the last run of this instance left staged")?;
    kernel
        .reopen_streaming_writer()
        .context("reopening the streaming writer")
}

const FLUSH_INTERVAL_MS: u64 = 1_000;

/// A series gains one file and one sliver a day, and staging holds at most a day.
const PROMOTION_INTERVAL: SignedDuration = SignedDuration::from_hours(24);

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
    use std::{collections::HashSet, path::Path};

    use rstest::rstest;

    use super::*;

    fn recording(staging: PathBuf) -> Recording {
        Recording { staging }
    }

    fn catalog(dir: &Path) -> DataCatalogConfig {
        DataCatalogConfig::new(dir.to_str().unwrap().to_string(), None, None)
    }

    #[rstest]
    fn the_staged_data_is_promoted_into_the_catalog_daily_and_on_stop() {
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
        assert_eq!(streaming.flush_interval_ms, 1_000);
        assert_eq!(streaming.promotion_interval_ms, Some(24 * 60 * 60 * 1_000));
        assert!(streaming.delete_feather_after_promotion);
    }

    #[rstest]
    fn a_restart_appends_to_what_the_last_run_staged() {
        let dir = tempfile::tempdir().unwrap();
        let streaming = recording(dir.path().join("staging"))
            .streaming(&catalog(&dir.path().join("catalog")))
            .unwrap();
        assert!(!streaming.replace_existing);
    }

    #[rstest]
    fn market_data_and_instruments_are_streamed_and_nothing_else() {
        let dir = tempfile::tempdir().unwrap();
        let streaming = recording(dir.path().join("staging"))
            .streaming(&catalog(&dir.path().join("catalog")))
            .unwrap();
        let streamed: HashSet<NautilusDataType> =
            streaming.data_types.unwrap().into_iter().collect();
        assert_eq!(
            streamed,
            HashSet::from([
                NautilusDataType::Instrument,
                NautilusDataType::QuoteTick,
                NautilusDataType::TradeTick,
                NautilusDataType::OrderBookDelta,
                NautilusDataType::OrderBookDepth,
                NautilusDataType::Bar,
                NautilusDataType::MarkPriceUpdate,
                NautilusDataType::IndexPriceUpdate,
                NautilusDataType::FundingRateUpdate,
            ])
        );
        assert_eq!(streaming.record_types, None);
        assert_eq!(streaming.record_filters, None);
    }

    fn relative(dir: &Path) -> PathBuf {
        let cwd = std::env::current_dir().unwrap();
        let mut relative: PathBuf =
            std::iter::repeat_n("..", cwd.components().count() - 1).collect();
        relative.push(dir.strip_prefix("/").unwrap());
        relative
    }

    fn absolute(dir: &Path) -> PathBuf {
        dir.to_path_buf()
    }

    /// `--catalog` and `--staging` each take an absolute or a relative directory.
    #[rstest]
    #[case::both_absolute(absolute, absolute)]
    #[case::the_catalog_relative(relative, absolute)]
    #[case::the_staging_relative(absolute, relative)]
    #[case::both_relative(relative, relative)]
    fn a_staging_directory_inside_the_catalog_is_refused_however_either_is_given(
        #[case] catalog_given: fn(&Path) -> PathBuf,
        #[case] staging_given: fn(&Path) -> PathBuf,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let catalog_dir = dir.path().join("catalog");
        std::fs::create_dir(&catalog_dir).unwrap();
        let (catalog_dir, staging) = (
            catalog_given(&catalog_dir),
            staging_given(&catalog_dir.join("staging")),
        );
        let err = recording(staging.clone())
            .streaming(&catalog(&catalog_dir))
            .unwrap_err()
            .to_string();
        // The catalog's path starts the staging path, so check the error names the staging
        // directory apart.
        let staging = staging.to_str().unwrap();
        assert!(err.contains(staging), "{err}");
        assert!(
            err.replace(staging, "")
                .contains(catalog_dir.to_str().unwrap()),
            "{err}"
        );
    }

    #[rstest]
    fn a_staging_directory_beside_the_catalog_named_after_it_is_outside_it() {
        let dir = tempfile::tempdir().unwrap();
        let catalog_dir = dir.path().join("catalog");
        std::fs::create_dir(&catalog_dir).unwrap();
        recording(dir.path().join("catalog-staging"))
            .streaming(&catalog(&catalog_dir))
            .unwrap();
    }

    #[rstest]
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

    #[rstest]
    fn each_trader_has_its_own_instance_id() {
        let dir = tempfile::tempdir().unwrap();
        let recording = recording(dir.path().join("staging"));
        let first = recording.instance_id(TraderId::from("HELIX-001")).unwrap();
        let second = recording.instance_id(TraderId::from("HELIX-002")).unwrap();
        assert_ne!(first, second);
    }

    #[rstest]
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
