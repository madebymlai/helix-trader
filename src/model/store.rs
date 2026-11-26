//! Stores the model sets that a strategy's trains publish, each valid from its retrain time.
//!
//! A model set (`StoredModelSet`) holds the models of one train:
//! - Each side traded, with the calibration of their pair.
//! - The meta, if the strategy has one. Every model set then holds one.
//!
//! A model set is self-contained. Its train alone fixes it as of its retrain time, and it derives
//! nothing from another model set. Its meta is learned from an out-of-fold pair that the train does
//! not publish (`train`). A decision uses the latest model set valid at or before it, sides and
//! meta together.
//!
//! A strategy's model sets are in one directory, named by:
//! - The strategy id.
//! - The dataset's fingerprint (`dataset::fingerprint`).
//! - The model set key.
//!
//! The key covers the store `VERSION` and every training setting of the models:
//! - The sides' learner and settings, retrain interval, window, calibration and fewest rows.
//! - The margins of both sides, which decide the meta's rows.
//! - The meta's settings, or none.
//!
//! Backtest and live read the same list.
//!
//! Only a model's blueprint (`blueprint::ModelBlueprint`) builds a `ModelStore`. The blueprint
//! computes the fingerprint once. The store holds each level's feature names. A train publishes its
//! models with them, and each model is checked against them.
//!
//! `Swap` uses the latest valid model set at each decision, loaded and checked as a whole. If a
//! model set fails a check, `Swap` refuses it with an error naming the model set and the check. No
//! decision is made on it or on the one before it. It is read again at the next decision.
//!
//! A model set is a directory `<valid from>/` (RFC 3339). It holds:
//! - Its manifest `model_set.json` (`ModelSetManifest`).
//! - The directory of the publish that wrote it, with a file per model: `long.json`, `short.json`
//!   and `meta.json`.
//!
//! A publish works in this order:
//! 1. It writes and syncs the models in a directory named by the process and the publish.
//! 2. It writes the manifest, naming that directory, to a temporary file beside the manifest path.
//! 3. It syncs that file and renames it onto the manifest path.
//!
//! The manifest is the commit marker. A model set without one is not listed, so:
//! - A crash leaves no listed model set with cut-short files.
//! - Concurrent publishers never write each other's files, and the last manifest names the models
//!   of one publish.
//!
//! A model's file holds its manifest (`ModelManifest`) and the model as its adapter saved it. A
//! side's model predicts R, and its pair calibrates both sides at once (`Pair::expect`). A meta's
//! model predicts the R of the trade of the side picked, calibrated by its level line
//! (`LoadedModel`). Loading refuses a model whose manifest has the other level's calibration. A
//! reloaded model is checked by predicting its probes, never by its bytes. JSON has no NaN, so a
//! probe row and prediction write NaN and the infinities as strings.

use std::{
    fmt::Debug,
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::Context;
use jiff::Timestamp;
use nautilus_model::identifiers::StrategyId;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::{
    blueprint::ModelBlueprint,
    calibration::{Expected, Ksplit, Line},
    config::{Margins, MetaConfig, ModelConfig},
    deciding::SideRow,
    features::{Fingerprint, meta_names, side_names},
    learner::{LearnerConfig, Model, SavedModel},
    schedule::Retrain,
    targets::{PerSide, Side, TargetConfig},
};
use crate::strategy::risk::R;

const MANIFEST: &str = "model_set.json";

/// The name of a meta model's file. A side model's file is named after its side.
const META: &str = "meta";

const EXTENSION: &str = "json";

/// The publishes of this process so far, which tell its directories and temporary files apart.
static PUBLISHES: AtomicU64 = AtomicU64::new(0);

/// The version of the manifests, layout and reading of a published model set.
///
/// It is keyed with the model set key (`model_set_directory`). Bump it by hand when they change,
/// which keys every store anew and trains every model again.
const VERSION: u32 = 12;

/// A strategy's published model sets, keyed by its dataset's fingerprint and its model set key.
#[derive(Debug, Clone)]
pub struct ModelStore {
    directory: PathBuf,
    /// The fingerprint each model is checked against.
    fingerprint: String,
    /// Each side traded, the long first.
    sides: Vec<SideLevel>,
    meta: Option<MetaLevel>,
}

/// A side of a model store, with its margin and the feature names of its model.
#[derive(Debug, Clone)]
pub struct SideLevel {
    side: Side,
    margin: R,
    feature_names: Vec<String>,
}

/// The meta level of a model store, with its config and feature names.
#[derive(Debug, Clone)]
pub struct MetaLevel {
    config: MetaConfig,
    feature_names: Vec<String>,
}

/// One train's models as published and read back.
///
/// It holds each side traded, the long first, the calibration of their pair, and a meta model if
/// and only if the strategy has a meta.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredModelSet {
    pub valid_from: Timestamp,
    /// Each side traded, the long first.
    pub sides: Vec<StoredModel>,
    pub calibration: Ksplit,
    pub meta: Option<StoredModel>,
}

/// A model set's manifest, written last as its commit marker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ModelSetManifest {
    /// The directory of the publish that holds the model files.
    publish: String,
    calibration: Ksplit,
}

/// A model set loaded and checked, as a decision uses it.
#[derive(Debug)]
pub struct ModelSet {
    pair: Pair,
    meta: Option<LoadedModel>,
}

/// The side models of one model set, loaded and checked together, with their calibration.
#[derive(Debug)]
pub struct Pair {
    models: Vec<(Side, SideModel)>,
    calibration: Ksplit,
}

/// A published model and its manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredModel {
    pub manifest: ModelManifest,
    pub model: SavedModel,
}

/// What a published model is, enough to check it before use.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelManifest {
    /// The fingerprint of its dataset.
    pub fingerprint: String,
    /// The feature names, in the order of a row's values.
    pub feature_names: Vec<String>,
    pub target: TargetConfig,
    /// The side learned. None for a meta model, which learns the trade of the side picked.
    pub side: Option<Side>,
    pub learner: LearnerConfig,
    /// A meta model's level line, as a one-sided pair's (`Ksplit`). None for a side model, which
    /// its pair calibrates (`StoredModelSet::calibration`).
    pub calibration: Option<Line>,
    /// The time that the out-of-fold pair was fitted as of. A meta learned from its picks. None for
    /// a side model.
    pub out_of_fold: Option<Timestamp>,
    /// The constant model that its scores are read beside. It is the training mean of R on the rows
    /// the model was fitted and calibrated on.
    pub baseline: f64,
    /// Rows of the model's calibration slice, with the predictions made on them before any
    /// calibration.
    pub probes: Vec<Probe>,
}

/// A row and the prediction a model made on it when fitted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Probe {
    #[serde(with = "values")]
    pub row: Vec<f64>,
    #[serde(with = "values::one")]
    pub prediction: f64,
}

impl StoredModel {
    /// Returns the meta model, loaded with its level line and checked as `checked` does.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The model cannot be loaded or fails a check.
    /// - Its manifest has no level line, as a side's.
    pub fn load(&self, feature_names: &[String], fingerprint: &str) -> anyhow::Result<LoadedModel> {
        let Some(calibration) = self.manifest.calibration else {
            anyhow::bail!("its manifest has no level line, which every meta model has");
        };
        Ok(LoadedModel {
            model: self.checked(feature_names, fingerprint)?,
            calibration,
            baseline: self.manifest.baseline,
        })
    }

    /// Returns the side model, loaded and checked as `checked` does.
    ///
    /// # Errors
    ///
    /// Returns an error if the model cannot be loaded or fails a check (see `checked`), or if its
    /// manifest has a calibration of its own, as a meta model's.
    fn load_side(&self, feature_names: &[String], fingerprint: &str) -> anyhow::Result<SideModel> {
        anyhow::ensure!(
            self.manifest.calibration.is_none(),
            "its manifest has a level line of its own, which no side model has: its pair \
             calibrates it"
        );
        Ok(SideModel::new(
            self.checked(feature_names, fingerprint)?,
            self.manifest.baseline,
        ))
    }

    /// Loads the model and checks it against the strategy's `feature_names` and `fingerprint`.
    ///
    /// The checks are:
    /// - The manifest's names and fingerprint match.
    /// - The model takes as many features as the manifest names.
    /// - The model predicts each probe to the bit, as when fitted.
    ///
    /// # Errors
    ///
    /// Returns an error if the model cannot be loaded or fails a check.
    pub(super) fn checked(
        &self,
        feature_names: &[String],
        fingerprint: &str,
    ) -> anyhow::Result<Model> {
        let manifest = &self.manifest;
        anyhow::ensure!(
            manifest.feature_names == feature_names,
            "its features {:?} are not the strategy's {feature_names:?}",
            manifest.feature_names
        );
        anyhow::ensure!(
            manifest.fingerprint == fingerprint,
            "its fingerprint {} is not the strategy's {fingerprint}",
            manifest.fingerprint
        );
        let model = manifest.learner.load(&self.model)?;
        let width = model.width();
        anyhow::ensure!(
            width == manifest.feature_names.len(),
            "it takes {width} features, its manifest names {}",
            manifest.feature_names.len()
        );
        for probe in &manifest.probes {
            let prediction = model.predict(&probe.row)?;
            anyhow::ensure!(
                prediction.to_bits() == probe.prediction.to_bits(),
                "it predicts {prediction} on the probe {:?}, not {} as when fitted",
                probe.row,
                probe.prediction
            );
        }
        Ok(model)
    }
}

/// A loaded meta model.
///
/// It predicts the R of the trade of the side picked, calibrated by its level line.
#[derive(Debug)]
pub struct LoadedModel {
    model: Model,
    calibration: Line,
    baseline: f64,
}

/// A loaded model with its baseline (`ModelManifest::baseline`).
pub(super) trait Baselined {
    fn baseline(&self) -> f64;
}

impl LoadedModel {
    /// Returns the calibrated expected R of the trade of `row`.
    ///
    /// # Errors
    ///
    /// Returns an error if `Model::predict` fails.
    pub fn predict(&self, row: &[f64]) -> anyhow::Result<f64> {
        Ok(self.calibration.apply(self.model.predict(row)?))
    }
}

/// A loaded side model.
///
/// It predicts the R of its side's trade, which its pair calibrates (`Pair::expect`).
#[derive(Debug)]
pub struct SideModel {
    model: Model,
    baseline: f64,
}

impl SideModel {
    /// Returns a side model of the fitted `model`, with its baseline `baseline`.
    #[must_use]
    pub const fn new(model: Model, baseline: f64) -> Self {
        Self { model, baseline }
    }

    /// Returns the R predicted for `row`, before its pair's calibration.
    ///
    /// # Errors
    ///
    /// Returns an error if `Model::predict` fails.
    pub fn predict(&self, row: &[f64]) -> anyhow::Result<f64> {
        self.model.predict(row)
    }
}

impl Baselined for LoadedModel {
    fn baseline(&self) -> f64 {
        self.baseline
    }
}

impl Baselined for SideModel {
    fn baseline(&self) -> f64 {
        self.baseline
    }
}

impl ModelStore {
    /// Returns the store of `blueprint` under `root`.
    ///
    /// The store is in the directory of `fingerprint` (`ModelBlueprint::fingerprint`) and the model
    /// set key (`model_set_directory`).
    ///
    /// # Errors
    ///
    /// Returns an error if a learner cannot be written as JSON.
    pub(super) fn new(
        root: &Path,
        blueprint: &ModelBlueprint,
        fingerprint: Fingerprint,
    ) -> anyhow::Result<Self> {
        let (features, config) = (blueprint.features(), blueprint.config());
        let directory =
            model_set_directory(root, blueprint.strategy_id(), fingerprint, config, VERSION)?;
        let side_names = side_names(features.names(), &config.target.payoff_names());
        let sides = config
            .margins
            .sides()
            .map(|(side, margin)| SideLevel {
                side,
                margin,
                feature_names: side_names.clone(),
            })
            .collect();
        let meta = config.meta.as_ref().map(|meta| MetaLevel {
            config: meta.clone(),
            feature_names: meta_names(features.names()),
        });
        Ok(Self {
            directory,
            fingerprint: fingerprint.to_string(),
            sides,
            meta,
        })
    }

    /// Returns the directory of its model set key.
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Returns whether a model set is valid by `now` (`Swap::at`).
    ///
    /// Its models may still fail their checks.
    ///
    /// # Errors
    ///
    /// Returns an error if the model sets cannot be listed.
    pub fn trades_by(&self, now: Timestamp) -> anyhow::Result<bool> {
        Ok(self.model_sets()?.iter().any(|&time| time <= now))
    }

    /// Iterates over each side that the margins trade, the long first.
    pub fn sides(&self) -> impl Iterator<Item = &SideLevel> {
        self.sides.iter()
    }

    /// Returns the side `side`, if the margins trade it.
    #[must_use]
    pub fn side(&self, side: Side) -> Option<&SideLevel> {
        self.sides.iter().find(|level| level.side == side)
    }

    /// Returns the meta level, if the config has a meta section.
    #[must_use]
    pub const fn meta(&self) -> Option<&MetaLevel> {
        self.meta.as_ref()
    }

    /// Returns the times its model sets are valid from, in order.
    ///
    /// Only model sets whose manifest is written are listed.
    ///
    /// # Errors
    ///
    /// Returns an error if its directory cannot be read, or a model set with a manifest is named
    /// by no time.
    pub fn model_sets(&self) -> anyhow::Result<Vec<Timestamp>> {
        let entries = match fs::read_dir(&self.directory) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            entries => entries
                .with_context(|| format!("reading the model store {}", self.directory.display()))?,
        };
        let mut times = vec![];

        for entry in entries {
            let path = entry?.path();
            let manifest = path.join(MANIFEST);
            if !manifest
                .try_exists()
                .with_context(|| format!("reading the model set {}", path.display()))?
            {
                continue;
            }
            let time = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.parse().ok())
                .with_context(|| format!("naming the model set {}", path.display()))?;
            times.push(time);
        }
        times.sort();
        Ok(times)
    }

    /// Publishes `model_set` and returns its directory.
    ///
    /// It replaces the model set valid from the same time, if any. The manifest is written last
    /// (see the module docs).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A side's model names no side.
    /// - The model set holds a meta model and the strategy has no meta, or the reverse.
    /// - A file or a directory cannot be written or synced.
    pub fn publish(&self, model_set: &StoredModelSet) -> anyhow::Result<PathBuf> {
        anyhow::ensure!(
            model_set.meta.is_some() == self.meta.is_some(),
            "publishing a model set {} a meta model for a strategy {} a meta",
            if model_set.meta.is_some() {
                "holding"
            } else {
                "without"
            },
            if self.meta.is_some() {
                "with"
            } else {
                "without"
            }
        );
        let directory = self.model_set(model_set.valid_from);
        let publish = format!(
            "{}-{}",
            std::process::id(),
            PUBLISHES.fetch_add(1, Ordering::Relaxed)
        );
        let models = directory.join(&publish);
        fs::create_dir_all(&models)
            .with_context(|| format!("creating the model set {}", directory.display()))?;
        for model in &model_set.sides {
            let side = model
                .manifest
                .side
                .context("publishing a side's model that names no side")?;
            write(&models.join(file(side.name())), model)?;
        }

        if let Some(meta) = &model_set.meta {
            write(&models.join(file(META)), meta)?;
        }

        // The models' files are named on disk before the manifest listing them is written
        sync_directory(&models)
            .and_then(|()| sync_directory(&directory))
            .with_context(|| format!("syncing the model set {}", directory.display()))?;
        let manifest = directory.join(MANIFEST);
        let written = manifest.with_extension(format!("{EXTENSION}.{publish}.tmp"));
        let text = serde_json::to_string_pretty(&ModelSetManifest {
            publish,
            calibration: model_set.calibration,
        })?;
        synced(&written, text.as_bytes())
            .and_then(|()| fs::rename(&written, &manifest))
            .and_then(|()| sync_directory(&directory))
            .with_context(|| format!("writing the model set {}", manifest.display()))?;
        Ok(directory)
    }

    /// Returns the model set valid from `valid_from`, as published.
    ///
    /// # Errors
    ///
    /// Returns an error if there is none, or one of its files cannot be read.
    pub fn read(&self, valid_from: Timestamp) -> anyhow::Result<StoredModelSet> {
        let manifest = self.manifest(valid_from)?;
        let models = self.model_set(valid_from).join(&manifest.publish);
        let sides = self
            .sides
            .iter()
            .map(|level| read(&models.join(file(level.side.name()))))
            .collect::<anyhow::Result<_>>()?;
        let meta = self
            .meta
            .as_ref()
            .map(|_| read(&models.join(file(META))))
            .transpose()?;
        Ok(StoredModelSet {
            valid_from,
            sides,
            calibration: manifest.calibration,
            meta,
        })
    }

    /// Returns the model sets as a strategy swaps them in.
    ///
    /// Each is loaded and checked as `load` does.
    #[must_use]
    pub fn swap(&self) -> Swap<ModelSet> {
        let models = self.clone();
        Swap::new(move |valid_from| {
            models.load(valid_from).with_context(|| {
                format!("the model set {}", models.model_set(valid_from).display())
            })
        })
    }

    /// Loads the model set valid from `valid_from`.
    ///
    /// Each model is checked against its level's feature names and the fingerprint.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The model set cannot be read, including its meta model.
    /// - A model fails its checks, naming its level.
    /// - The calibration is for a different number of sides than the pair has.
    fn load(&self, valid_from: Timestamp) -> anyhow::Result<ModelSet> {
        let stored = self.read(valid_from)?;
        let models = stored
            .sides
            .iter()
            .zip(&self.sides)
            .map(|(model, level)| {
                let loaded = model
                    .load_side(&level.feature_names, &self.fingerprint)
                    .with_context(|| format!("the {} model", level.side.name()))?;
                Ok((level.side, loaded))
            })
            .collect::<anyhow::Result<_>>()?;
        let meta = stored
            .meta
            .as_ref()
            .zip(self.meta.as_ref())
            .map(|(model, level)| {
                model
                    .load(&level.feature_names, &self.fingerprint)
                    .context("the meta model")
            })
            .transpose()?;
        Ok(ModelSet {
            pair: Pair::new(models, stored.calibration)?,
            meta,
        })
    }

    /// Returns the manifest of the model set valid from `valid_from`.
    ///
    /// # Errors
    ///
    /// Returns an error if there is none, or it cannot be read.
    fn manifest(&self, valid_from: Timestamp) -> anyhow::Result<ModelSetManifest> {
        read(&self.model_set(valid_from).join(MANIFEST))
    }

    fn model_set(&self, valid_from: Timestamp) -> PathBuf {
        self.directory.join(valid_from.to_string())
    }

    /// A path that names a model set by no time.
    ///
    /// Listing fails on it while the model sets still read.
    #[cfg(test)]
    fn unlistable(&self) -> PathBuf {
        self.directory.join("no time")
    }

    /// Makes `model_sets` fail until `mend_listing`.
    #[cfg(test)]
    pub(super) fn break_listing(&self) {
        fs::create_dir_all(self.unlistable()).unwrap();
        fs::write(self.unlistable().join(MANIFEST), "").unwrap();
    }

    /// Undoes `break_listing`.
    #[cfg(test)]
    pub(super) fn mend_listing(&self) {
        fs::remove_dir_all(self.unlistable()).unwrap();
    }

    /// Removes a model set's manifest, as a publish that stopped before writing it would leave
    /// it.
    #[cfg(test)]
    pub(super) fn drop_manifest(&self, valid_from: Timestamp) {
        fs::remove_file(self.model_set(valid_from).join(MANIFEST)).unwrap();
    }

    /// Makes `publish` fail while listing still finds no model sets.
    ///
    /// The directory becomes a link to nowhere.
    #[cfg(test)]
    pub(super) fn break_publishing(&self) {
        fs::create_dir_all(self.directory.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(self.directory.with_extension("nowhere"), &self.directory)
            .unwrap();
    }
}

impl SideLevel {
    #[must_use]
    pub const fn side(&self) -> Side {
        self.side
    }

    /// Returns the expected net R from which the side is traded (`ModelConfig::margins`).
    #[must_use]
    pub const fn margin(&self) -> R {
        self.margin
    }

    /// Returns the feature names of the side model, in the order of its rows
    /// (`features::side_names`).
    #[must_use]
    pub fn feature_names(&self) -> &[String] {
        &self.feature_names
    }
}

impl MetaLevel {
    #[must_use]
    pub const fn config(&self) -> &MetaConfig {
        &self.config
    }

    /// Returns the feature names of the meta, in the order of its rows (`features::meta_names`).
    #[must_use]
    pub fn feature_names(&self) -> &[String] {
        &self.feature_names
    }
}

impl ModelSet {
    /// Returns the pair of its sides.
    #[must_use]
    pub const fn pair(&self) -> &Pair {
        &self.pair
    }

    /// Returns its meta model, or none if the strategy has no meta.
    #[must_use]
    pub const fn meta(&self) -> Option<&LoadedModel> {
        self.meta.as_ref()
    }
}

impl Pair {
    /// Returns the pair of `models`, calibrated by `calibration`.
    ///
    /// The models are each side traded, the long first.
    ///
    /// # Errors
    ///
    /// Returns an error if `calibration` is for a different number of sides than `models` has. A
    /// calibration with a side call is for two sides, and one without is for one.
    pub fn new(models: Vec<(Side, SideModel)>, calibration: Ksplit) -> anyhow::Result<Self> {
        anyhow::ensure!(
            calibration.side_call.is_some() == (models.len() == 2),
            "a calibration of {} sides for a pair of {}",
            if calibration.side_call.is_some() {
                2
            } else {
                1
            },
            models.len()
        );
        Ok(Self {
            models,
            calibration,
        })
    }

    /// Returns the calibrated expectation of the decision, from each side's row in `rows`.
    ///
    /// # Errors
    ///
    /// Returns an error if a model fails to predict.
    pub fn expect(&self, rows: &PerSide<SideRow>) -> anyhow::Result<Expected> {
        let predictions = self
            .models
            .iter()
            .map(|(side, model)| model.predict(&rows.of(*side).features))
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(self.calibration.apply(&predictions))
    }

    /// Returns the calibration of the predictions of its sides.
    #[must_use]
    pub const fn calibration(&self) -> Ksplit {
        self.calibration
    }

    /// Returns the model of `side`.
    ///
    /// # Panics
    ///
    /// Panics if the pair's model store does not trade `side`.
    #[must_use]
    pub fn side(&self, side: Side) -> &SideModel {
        self.models
            .iter()
            .find(|(traded, _)| *traded == side)
            .map_or_else(
                || panic!("the pair holds no {} model", side.name()),
                |(_, model)| model,
            )
    }
}

fn file(name: &str) -> String {
    format!("{name}.{EXTENSION}")
}

/// Writes `value` as JSON to `path` (`synced`).
///
/// # Errors
///
/// Returns an error if the file cannot be written.
fn write(path: &Path, value: &impl Serialize) -> anyhow::Result<()> {
    synced(path, serde_json::to_string_pretty(value)?.as_bytes())
        .with_context(|| format!("writing the model {}", path.display()))
}

/// Writes `contents` to `path` and syncs it, so that a crash afterwards leaves it whole.
///
/// # Errors
///
/// Returns an error if the file cannot be written or synced.
fn synced(path: &Path, contents: &[u8]) -> std::io::Result<()> {
    let mut file = fs::File::create(path)?;
    file.write_all(contents)?;
    file.sync_all()
}

/// Syncs the entries of the directory `path`.
///
/// A file created or renamed in it then survives a crash.
///
/// # Errors
///
/// Returns an error if the directory cannot be opened or synced.
#[cfg(unix)]
fn sync_directory(path: &Path) -> std::io::Result<()> {
    fs::File::open(path)?.sync_all()
}

/// Off Unix a directory cannot be opened to sync it.
#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

/// Returns the JSON of the file `path`.
///
/// # Errors
///
/// Returns an error if the file is missing, cannot be read, or holds no `T`. The error names the
/// file.
fn read<T: DeserializeOwned>(path: &Path) -> anyhow::Result<T> {
    let json = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&json).with_context(|| format!("reading {}", path.display()))
}

fn dataset_directory(root: &Path, strategy_id: StrategyId, fingerprint: Fingerprint) -> PathBuf {
    root.join(strategy_id.as_str())
        .join(fingerprint.to_string())
}

/// Returns the directory under `root` for the model sets of `strategy_id`, `fingerprint`, `config`
/// and store `version` (`VERSION`).
///
/// It is the dataset's directory, then the model set key.
///
/// # Errors
///
/// Returns an error if a learner cannot be written as JSON.
fn model_set_directory(
    root: &Path,
    strategy_id: StrategyId,
    fingerprint: Fingerprint,
    config: &ModelConfig,
    version: u32,
) -> anyhow::Result<PathBuf> {
    let key = Fingerprint::default()
        .with(serde_json::to_string(&config.learner)?.as_bytes())
        .with(retrain_setting(config.retrain).as_bytes())
        .with(&config.window.as_nanos().to_le_bytes())
        .with(&config.calibration.as_nanos().to_le_bytes())
        .with(&config.min_rows.to_le_bytes())
        .with(margins_text(config.margins).as_bytes())
        .with(meta_settings(config.meta.as_ref())?.as_bytes())
        .with(format!("store version {version}").as_bytes());
    Ok(dataset_directory(root, strategy_id, fingerprint).join(key.to_string()))
}

/// Returns the retrain as the model set key holds it.
///
/// Another interval, or never, is another store.
fn retrain_setting(retrain: Retrain) -> String {
    match retrain {
        Retrain::Every(every) => format!("retrain every {}", every.as_nanos()),
        Retrain::Never => "retrain never".to_string(),
    }
}

/// Returns the margins as the model set key holds them, each in R, as in `long 0.5R short none`.
///
/// Margins in another unit are another store.
fn margins_text(margins: Margins) -> String {
    let margin = |margin: Option<R>| margin.map_or_else(|| "none".to_string(), |r| r.to_string());
    format!(
        "long {} short {}",
        margin(margins.long),
        margin(margins.short)
    )
}

/// Returns the meta's training settings as the model set key holds them, or `no meta` for none.
///
/// # Errors
///
/// Returns an error if the meta's learner cannot be written as JSON.
fn meta_settings(meta: Option<&MetaConfig>) -> anyhow::Result<String> {
    let Some(meta) = meta else {
        return Ok("no meta".to_string());
    };
    Ok(format!(
        "meta {} window {} calibration {} margin {} min_rows {}",
        serde_json::to_string(&meta.learner)?,
        meta.window.as_nanos(),
        meta.calibration.as_nanos(),
        meta.margin,
        meta.min_rows
    ))
}

/// The model sets of a model store as a strategy swaps them in, decision by decision in time order
/// (`ModelStore::swap`).
///
/// A model set that fails a check is refused, and none is in use until it loads. It is read again
/// at the next decision.
pub struct Swap<M> {
    read_at: Box<dyn Fn(Timestamp) -> anyhow::Result<M>>,
    current: Option<(Timestamp, M)>,
}

impl<M> Swap<M> {
    fn new(read_at: impl Fn(Timestamp) -> anyhow::Result<M> + 'static) -> Self {
        Self {
            read_at: Box::new(read_at),
            current: None,
        }
    }

    /// Returns the model set in use at `now`, and its valid-from.
    ///
    /// It is the latest model set valid then among `valid_froms`, in order. It is swapped in if not
    /// in use yet.
    ///
    /// # Errors
    ///
    /// Returns an error if the latest model set fails its checks. The error names it and the
    /// check.
    pub fn at(
        &mut self,
        now: Timestamp,
        valid_froms: &[Timestamp],
    ) -> anyhow::Result<Option<(Timestamp, &M)>> {
        let Some(latest) = valid_froms.iter().rfind(|&&time| time <= now).copied() else {
            return Ok(None);
        };

        if self
            .current
            .as_ref()
            .is_none_or(|(valid_from, _)| *valid_from != latest)
        {
            self.current = None;

            // Nautilus logs an error a strategy's handler returns by its message alone, so the
            // message carries the whole chain.
            let model_set = (self.read_at)(latest).map_err(|e| {
                anyhow::anyhow!("no new entry until the model set valid from {latest} loads: {e:#}")
            })?;
            log::info!("the model set valid from {latest} in use from {now}");
            self.current = Some((latest, model_set));
        }
        Ok(self
            .current
            .as_ref()
            .map(|(valid_from, model_set)| (*valid_from, model_set)))
    }
}

impl<M: Debug> Debug for Swap<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(Swap))
            .field("current", &self.current)
            .finish_non_exhaustive()
    }
}

/// A row of values as JSON.
///
/// NaN and the infinities are `"NaN"`, `"inf"` and `"-inf"`. `one` is for a single value.
mod values {
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

    #[derive(Serialize, Deserialize)]
    #[serde(untagged)]
    enum Value {
        Number(f64),
        Name(String),
    }

    impl From<f64> for Value {
        fn from(value: f64) -> Self {
            match value {
                v if v.is_finite() => Self::Number(v),
                v if v.is_nan() => Self::Name("NaN".to_string()),
                v if v > 0.0 => Self::Name("inf".to_string()),
                _ => Self::Name("-inf".to_string()),
            }
        }
    }

    impl Value {
        fn read<E: de::Error>(self) -> Result<f64, E> {
            match self {
                Self::Number(v) => Ok(v),
                Self::Name(name) => match name.as_str() {
                    "NaN" => Ok(f64::NAN),
                    "inf" => Ok(f64::INFINITY),
                    "-inf" => Ok(f64::NEG_INFINITY),
                    _ => Err(E::custom(format!("{name} is not a value"))),
                },
            }
        }
    }

    pub(super) fn serialize<S: Serializer>(row: &[f64], serializer: S) -> Result<S::Ok, S::Error> {
        row.iter()
            .map(|&value| Value::from(value))
            .collect::<Vec<_>>()
            .serialize(serializer)
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<f64>, D::Error> {
        Vec::<Value>::deserialize(deserializer)?
            .into_iter()
            .map(Value::read)
            .collect()
    }

    /// One value as JSON, as a row's are.
    pub(super) mod one {
        use serde::{Deserialize, Deserializer, Serialize, Serializer};

        use super::Value;

        pub(crate) fn serialize<S: Serializer>(
            value: &f64,
            serializer: S,
        ) -> Result<S::Ok, S::Error> {
            Value::from(*value).serialize(serializer)
        }

        pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
            deserializer: D,
        ) -> Result<f64, D::Error> {
            Value::deserialize(deserializer)?.read()
        }
    }
}

#[cfg(test)]
mod tests {
    use jiff::SignedDuration;
    use nautilus_model::{data::NautilusDataType, identifiers::InstrumentId, types::Money};
    use rstest::rstest;
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        backtest::Commission,
        model::{
            dataset::DatasetVenue,
            decide::DecisionRule,
            features::{FeatureSet, testing::last_mid},
            learner::{LearnerConfig, TrainingSet},
            targets::Target,
        },
        strategy::requirements::DataRequirements,
    };

    const GOLD: &str = "XAUUSD.ICMARKETS";
    const STRATEGY: &str = "STORE-001";
    const MIDNIGHT: &str = "2026-10-22T00:00:00Z";

    fn features() -> std::rc::Rc<FeatureSet> {
        last_mid(InstrumentId::from(GOLD)).build().unwrap()
    }

    fn config() -> ModelConfig {
        ModelConfig::builder()
            .decision(DecisionRule::Every(SignedDuration::from_hours(1)))
            .target(
                TargetConfig::builder()
                    .instrument_id(InstrumentId::from(GOLD))
                    .risk("0.5%".parse().unwrap())
                    .spec(
                        Target::builder()
                            .limit(SignedDuration::from_hours(1))
                            .atr(crate::model::targets::DEFAULT_ATR)
                            .build(),
                    )
                    .build(),
            )
            .retrain(SignedDuration::from_hours(24))
            .dataset(venue())
            .window(SignedDuration::from_hours(48))
            .calibration(SignedDuration::from_hours(12))
            .margins(Margins {
                long: Some(R::new(0.5)),
                short: Some(R::new(0.5)),
            })
            .min_rows(30)
            .learner(LearnerConfig::Fake {})
            .build()
    }

    fn meta_config() -> MetaConfig {
        MetaConfig::builder()
            .window(SignedDuration::from_hours(48))
            .calibration(SignedDuration::from_hours(12))
            .margin(R::new(0.55))
            .min_rows(20)
            .learner(LearnerConfig::Fake {})
            .build()
    }

    fn with_meta() -> ModelConfig {
        ModelConfig {
            meta: Some(meta_config()),
            ..config()
        }
    }

    fn blueprint(config: &ModelConfig) -> ModelBlueprint {
        ModelBlueprint::new(
            StrategyId::from(STRATEGY),
            features(),
            config.clone(),
            requirements(),
        )
        .unwrap()
    }

    fn models(root: &Path, config: &ModelConfig) -> ModelStore {
        blueprint(config).model_store(root).unwrap()
    }

    fn requirements() -> DataRequirements {
        DataRequirements {
            bar_types: vec![],
            ticks: vec![crate::strategy::requirements::TickSeries {
                data: NautilusDataType::QuoteTick,
                instrument_id: InstrumentId::from(GOLD),
            }],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        }
    }

    fn venue() -> DatasetVenue {
        DatasetVenue {
            commission: Commission::Rate(Decimal::new(3, 5)),
            balance: Money::from("10000000 USD"),
            latency: None,
        }
    }

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    const PAIR: Ksplit = Ksplit {
        level: Line {
            alpha: 0.1,
            beta: 0.5,
        },
        side_call: Some(1.5),
    };

    /// A fake model of `side`, or a meta model if `side` is none, probed on `rows`.
    fn model(side: Option<Side>, rows: &[Vec<f64>]) -> StoredModel {
        let set = TrainingSet::new(1, vec![0.2, 0.7], vec![0.0, 1.0]).unwrap();
        let fitted = LearnerConfig::Fake {}.fit(&set).unwrap();
        StoredModel {
            manifest: ModelManifest {
                fingerprint: "f".repeat(64),
                feature_names: vec!["mid".to_string()],
                target: config().target,
                side,
                learner: LearnerConfig::Fake {},
                calibration: side.is_none().then_some(Line {
                    alpha: 0.1,
                    beta: 0.5,
                }),
                baseline: 0.5,
                out_of_fold: None,
                probes: rows
                    .iter()
                    .map(|row| Probe {
                        row: row.clone(),
                        prediction: fitted.predict(row).unwrap(),
                    })
                    .collect(),
            },
            model: fitted.save().unwrap(),
        }
    }

    fn model_set(valid_from: &str, rows: &[Vec<f64>], meta: bool) -> StoredModelSet {
        StoredModelSet {
            valid_from: at(valid_from),
            sides: Side::BOTH
                .into_iter()
                .map(|side| model(Some(side), rows))
                .collect(),
            calibration: PAIR,
            meta: meta.then(|| model(None, rows)),
        }
    }

    #[rstest]
    fn a_published_model_set_reads_back_whole() {
        let dir = tempfile::tempdir().unwrap();
        let models = models(dir.path(), &with_meta());
        let published = model_set(MIDNIGHT, &[vec![0.3], vec![0.1 + 0.2]], true);

        models.publish(&published).unwrap();

        assert_eq!(models.read(at(MIDNIGHT)).unwrap(), published);
    }

    #[rstest]
    #[case::without_a_meta_for_a_strategy_with_one(with_meta(), false)]
    #[case::with_a_meta_for_a_strategy_without_one(config(), true)]
    fn a_model_set_whose_meta_is_not_the_strategys_is_refused(
        #[case] config: ModelConfig,
        #[case] meta: bool,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let models = models(dir.path(), &config);

        let err = models
            .publish(&model_set(MIDNIGHT, &[vec![0.3]], meta))
            .unwrap_err();

        assert!(err.to_string().contains("meta"), "{err}");
        assert_eq!(models.model_sets().unwrap(), []);
    }

    #[rstest]
    fn missing_and_infinite_probe_values_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let models = models(dir.path(), &config());
        let rows = [
            vec![f64::NAN],
            vec![f64::INFINITY],
            vec![f64::NEG_INFINITY],
            vec![-0.0],
        ];
        models.publish(&model_set(MIDNIGHT, &rows, false)).unwrap();

        let read = models.read(at(MIDNIGHT)).unwrap();

        let bits = |values: &[f64]| -> Vec<u64> { values.iter().map(|v| v.to_bits()).collect() };
        let (rows_read, predictions): (Vec<f64>, Vec<f64>) = read.sides[0]
            .manifest
            .probes
            .iter()
            .map(|probe| (probe.row[0], probe.prediction))
            .unzip();
        assert_eq!(bits(&rows_read), bits(&rows.concat()));

        assert_eq!(
            bits(&predictions),
            bits(&[0.5, f64::INFINITY, f64::NEG_INFINITY, -0.0])
        );
    }

    #[rstest]
    fn a_store_lists_the_times_its_model_sets_are_valid_from_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let models = models(dir.path(), &config());
        assert!(models.model_sets().unwrap().is_empty());

        for time in ["2026-10-23T00:00:00Z", "2026-10-21T12:00:00Z"] {
            models
                .publish(&model_set(time, &[vec![0.5]], false))
                .unwrap();
        }

        assert_eq!(
            models.model_sets().unwrap(),
            [at("2026-10-21T12:00:00Z"), at("2026-10-23T00:00:00Z")]
        );
    }

    #[rstest]
    fn a_model_set_whose_manifest_is_missing_is_not_listed() {
        let dir = tempfile::tempdir().unwrap();
        let models = models(dir.path(), &config());
        models
            .publish(&model_set(MIDNIGHT, &[vec![0.5]], false))
            .unwrap();
        let noon = "2026-10-22T12:00:00Z";
        models
            .publish(&model_set(noon, &[vec![0.5]], false))
            .unwrap();

        models.drop_manifest(at(noon));

        assert_eq!(models.model_sets().unwrap(), [at(MIDNIGHT)]);
        assert!(!models.trades_by(at("2026-10-21T23:00:00Z")).unwrap());
        assert!(models.trades_by(at(noon)).unwrap());
    }

    #[rstest]
    fn a_publish_stopped_before_its_last_model_lists_no_model_set() {
        let dir = tempfile::tempdir().unwrap();
        let models = models(dir.path(), &config());
        let rows = [vec![0.5]];
        let stopped = StoredModelSet {
            sides: vec![model(Some(Side::Long), &rows), model(None, &rows)],
            ..model_set(MIDNIGHT, &rows, false)
        };

        let err = models.publish(&stopped).unwrap_err().to_string();

        assert!(err.contains("names no side"), "{err}");
        assert_eq!(models.model_sets().unwrap(), []);
    }

    #[rstest]
    fn a_model_set_valid_from_a_fraction_of_a_second_lists_and_reads_back() {
        let dir = tempfile::tempdir().unwrap();
        let models = models(dir.path(), &config());
        let published = model_set("2026-10-22T04:00:00.01Z", &[vec![0.5]], false);

        models.publish(&published).unwrap();

        assert_eq!(
            models.model_sets().unwrap(),
            [at("2026-10-22T04:00:00.01Z")]
        );
        assert_eq!(
            models.read(at("2026-10-22T04:00:00.01Z")).unwrap(),
            published
        );
    }

    #[rstest]
    fn publishers_at_the_same_time_at_once_each_publish_a_whole_model_set() {
        let dir = tempfile::tempdir().unwrap();
        let models = models(dir.path(), &with_meta());
        let published = model_set(MIDNIGHT, &[vec![0.5]], true);

        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..20 {
                        models.publish(&published).unwrap();
                    }
                });
            }
        });

        assert_eq!(models.read(at(MIDNIGHT)).unwrap(), published);
    }

    #[rstest]
    fn publishing_at_the_same_time_replaces_the_model_set() {
        let dir = tempfile::tempdir().unwrap();
        let models = models(dir.path(), &with_meta());
        models
            .publish(&model_set(MIDNIGHT, &[vec![0.5]], true))
            .unwrap();
        let second = model_set(MIDNIGHT, &[vec![0.9]], true);

        models.publish(&second).unwrap();

        assert_eq!(models.model_sets().unwrap(), [at(MIDNIGHT)]);
        assert_eq!(models.read(at(MIDNIGHT)).unwrap(), second);
    }

    fn changed(config: ModelConfig, change: fn(&mut ModelConfig)) -> ModelConfig {
        let mut config = config;
        change(&mut config);
        config
    }

    fn meta_changed(change: fn(&mut MetaConfig)) -> ModelConfig {
        let mut config = with_meta();
        change(config.meta.as_mut().unwrap());
        config
    }

    /// The reference stake is the config's risk applied to the dataset balance.
    #[rstest]
    #[case::another_learner(changed(with_meta(), |config| config.learner = LearnerConfig::Baseline {}))]
    #[case::retrained_weekly(changed(with_meta(), |config| {
        config.retrain = Retrain::Every(SignedDuration::from_hours(168));
    }))]
    #[case::never_retrained(changed(with_meta(), |config| config.retrain = Retrain::Never))]
    #[case::another_window(changed(with_meta(), |config| config.window = SignedDuration::from_hours(72)))]
    #[case::another_calibration(changed(with_meta(), |config| {
        config.calibration = SignedDuration::from_hours(6);
    }))]
    #[case::another_target(changed(with_meta(), |config| {
        config.target.spec.limit = SignedDuration::from_mins(30);
    }))]
    #[case::more_rows(changed(with_meta(), |config| config.min_rows = 50))]
    #[case::another_long_margin(changed(with_meta(), |config| config.margins.long = Some(R::new(0.75))))]
    #[case::another_short_margin(changed(with_meta(), |config| config.margins.short = Some(R::new(0.75))))]
    #[case::no_short(changed(with_meta(), |config| config.margins.short = None))]
    #[case::another_risk(changed(with_meta(), |config| config.target.risk = "1%".parse().unwrap()))]
    #[case::another_dataset_balance(changed(with_meta(), |config| {
        config.dataset.balance = Money::from("20000000 USD");
    }))]
    #[case::no_meta(config())]
    #[case::another_meta_learner(meta_changed(|meta| meta.learner = LearnerConfig::Baseline {}))]
    #[case::another_meta_window(meta_changed(|meta| meta.window = SignedDuration::from_hours(72)))]
    #[case::another_meta_calibration(meta_changed(|meta| {
        meta.calibration = SignedDuration::from_hours(6);
    }))]
    #[case::another_meta_margin(meta_changed(|meta| meta.margin = R::new(0.6)))]
    #[case::more_meta_rows(meta_changed(|meta| meta.min_rows = 30))]
    fn any_other_setting_of_the_sides_or_the_meta_lists_none_of_the_model_sets(
        #[case] other: ModelConfig,
    ) {
        let dir = tempfile::tempdir().unwrap();
        models(dir.path(), &with_meta())
            .publish(&model_set(MIDNIGHT, &[vec![0.5]], true))
            .unwrap();

        let listed = |config: &ModelConfig| models(dir.path(), config).model_sets().unwrap();

        assert_eq!(listed(&with_meta()), [at(MIDNIGHT)]);
        assert!(listed(&other).is_empty());
    }

    #[rstest]
    fn bumping_the_store_version_keys_another_store() {
        let config = with_meta();
        let fingerprint = blueprint(&config).fingerprint().unwrap();
        let at = |version| {
            model_set_directory(
                Path::new("models"),
                StrategyId::from(STRATEGY),
                fingerprint,
                &config,
                version,
            )
            .unwrap()
        };

        assert_ne!(at(VERSION), at(VERSION + 1));
    }

    /// The directory as a literal: id, dataset fingerprint, then model set key.
    ///
    /// A change that rekeys every store, leaving the model sets behind, fails here. A deliberate
    /// rekey updates the literals and also the pinned dataset identifier
    /// (`dataset::tests::a_fixed_strategys_dataset_keeps_its_identifier`).
    #[rstest]
    #[case::without_a_meta(
        config(),
        "de9488a0af76c9eb1ca60369056a7ed348d14d366627cbb7fb43af5f9861fd6f"
    )]
    #[case::with_a_meta(
        with_meta(),
        "03bf2de6d834d3508b955378eb2f8187779466d2951e987b9ef69db7102a00d2"
    )]
    fn a_fixed_strategys_model_sets_keep_their_directory(
        #[case] config: ModelConfig,
        #[case] key: &str,
    ) {
        assert_eq!(
            models(Path::new("models"), &config).directory(),
            Path::new(
                "models/STORE-001/\
                 caaea0d5e3455014f85a52e3efb40d9276913c537309e341bc9c8639190b11f2"
            )
            .join(key)
        );
    }

    #[rstest]
    #[case::without_a_meta(config(), false)]
    #[case::with_a_meta(with_meta(), true)]
    fn a_model_store_trades_on_a_model_set_valid_by_then(
        #[case] config: ModelConfig,
        #[case] meta: bool,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let before = "2026-10-21T23:59:59Z";
        let trades = |now: &str| models(dir.path(), &config).trades_by(at(now)).unwrap();

        assert!(!trades(MIDNIGHT));
        models(dir.path(), &config)
            .publish(&model_set(MIDNIGHT, &[vec![0.5]], meta))
            .unwrap();
        assert!(trades(MIDNIGHT));
        assert!(!trades(before));
    }

    #[rstest]
    fn a_loaded_meta_model_predicts_through_its_calibration() {
        let stored = model(None, &[vec![0.75]]);

        let loaded = stored
            .load(&stored.manifest.feature_names, &stored.manifest.fingerprint)
            .unwrap();

        assert_eq!(stored.manifest.probes[0].prediction, 0.75);
        let prediction = loaded.predict(&[0.25]).unwrap();
        assert!((prediction - 0.225).abs() < 1e-12, "{prediction}");
    }

    #[rstest]
    fn a_model_is_refused_at_the_other_levels_load() {
        let side = model(Some(Side::Long), &[]);
        let meta = model(None, &[]);
        let (names, fingerprint) = (&side.manifest.feature_names, &side.manifest.fingerprint);

        let side_as_meta = side.load(names, fingerprint).unwrap_err().to_string();
        let meta_as_side = meta.load_side(names, fingerprint).unwrap_err().to_string();

        assert!(side_as_meta.contains("no level line"), "{side_as_meta}");
        assert!(side_as_meta.contains("meta"), "{side_as_meta}");
        assert!(
            meta_as_side.contains("a level line of its own"),
            "{meta_as_side}"
        );
        assert!(meta_as_side.contains("side"), "{meta_as_side}");
    }

    #[rstest]
    fn a_model_sets_pair_expects_each_sides_r_through_its_calibration() {
        let dir = tempfile::tempdir().unwrap();
        let models = models(dir.path(), &config());
        let fingerprint = blueprint(&config()).fingerprint().unwrap().to_string();
        let names = models.side(Side::Long).unwrap().feature_names().to_vec();
        let mut published = model_set(MIDNIGHT, &[], false);
        for stored in &mut published.sides {
            stored.manifest.fingerprint.clone_from(&fingerprint);
            stored.manifest.feature_names.clone_from(&names);
            let set = TrainingSet::new(2, vec![0.2, 0.0], vec![0.0]).unwrap();
            stored.model = LearnerConfig::Fake {}.fit(&set).unwrap().save().unwrap();
        }
        models.publish(&published).unwrap();
        let mut model_sets = models.swap();
        let midnight = at(MIDNIGHT);

        let (_, model_set) = model_sets.at(midnight, &[midnight]).unwrap().unwrap();
        let expected = model_set
            .pair()
            .expect(&PerSide {
                long: SideRow::new(&[0.4], &[0.02]),
                short: SideRow::new(&[0.1], &[0.02]),
            })
            .unwrap();

        for (actual, by_hand) in [
            (expected.level, 0.225),
            (expected.side_call, 0.45),
            (expected.of(Side::Long), 0.45),
            (expected.of(Side::Short), 0.0),
        ] {
            assert!((actual - by_hand).abs() < 1e-12, "{expected:?}");
        }
    }

    fn pair_calibrated_side(mut current: serde_json::Value) -> serde_json::Value {
        let manifest = current["manifest"].as_object_mut().unwrap();
        manifest.insert(
            "calibration".to_string(),
            serde_json::json!({"pair": {
                "valid_from": MIDNIGHT,
                "sides": ["long", "short"],
                "lines": {"level": {"alpha": 0.0, "beta": 1.0}, "side_call": 1.0}
            }}),
        );
        current
    }

    fn platt_side(mut current: serde_json::Value) -> serde_json::Value {
        let manifest = current["manifest"].as_object_mut().unwrap();
        manifest.insert(
            "calibration".to_string(),
            serde_json::json!({"a": -0.9, "b": 0.0}),
        );
        manifest.insert(
            "payoff".to_string(),
            serde_json::json!({"win": 1.5, "loss": 1.25}),
        );
        current
    }

    #[rstest]
    #[case::of_a_pair_calibrated_side(pair_calibrated_side)]
    #[case::of_a_platt_side(platt_side)]
    fn a_model_of_an_earlier_manifest_is_refused_naming_its_file(
        #[case] earlier: fn(serde_json::Value) -> serde_json::Value,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let models = models(dir.path(), &config());
        models.publish(&model_set(MIDNIGHT, &[], false)).unwrap();
        let manifest = models.manifest(at(MIDNIGHT)).unwrap();
        let path = models
            .model_set(at(MIDNIGHT))
            .join(manifest.publish)
            .join(file(Side::Long.name()));
        let current = serde_json::to_value(model(Some(Side::Long), &[])).unwrap();
        fs::write(&path, earlier(current).to_string()).unwrap();

        let refused = models.read(at(MIDNIGHT)).unwrap_err();

        let message = format!("{refused:#}");
        assert!(message.contains(&path.display().to_string()), "{message}");
    }
}
