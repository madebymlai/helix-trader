//! The model store: the generations a strategy's trains published, each
//! valid from the retrain time it was trained at.
//!
//! A generation is one train's models (`StoredGeneration`): the model of
//! each side the strategy trades, with the calibration of their pair, and
//! the meta's, if the strategy has one, every generation of a strategy with
//! a meta holding one. It is self-contained: its models are fixed as of its
//! retrain time, by that train alone, from the data and the config, none
//! derived from another generation nor changed by a later train; its meta
//! learned from an ancestor pair its train fitted and did not publish
//! (`train`). A decision uses the latest generation
//! valid at or before it, its sides and its meta together, so no meta ever
//! decides beside the sides of another train, and no side beside another
//! train's other side.
//!
//! A strategy's generations are in one directory, of its strategy id, its
//! dataset's fingerprint (the features, the decision rule, the target and
//! the venue settings of its dataset runs, see `dataset::fingerprint`) and
//! its generation key: every training setting of its models, the sides'
//! (the learner and its settings, the retrain interval, the window, the
//! calibration and the fewest rows of a slice) and the margins of both
//! sides, which decide the side picked on each row and so the meta's rows,
//! then the meta's (its learner and settings, window, calibration, margin
//! and fewest rows) or none, and the store `VERSION`. So a generation is
//! only ever listed beside generations of the same features, costs,
//! training, margins and meta, and of the same manifest. Backtest and live
//! read the same list.
//!
//! A strategy's model store (`ModelStore`) is built of its model's blueprint
//! (`blueprint::ModelBlueprint`), and only there, which computes the
//! fingerprint once and keys the directory with it. The model store holds
//! each side the margins trade, with its margin and its feature names (the
//! strategy's, then the target's payoff names, `features::side_names`),
//! and the meta level, if any, with its config and its feature names; it
//! publishes and lists the generations, and hands out their swap, each
//! model checked against its level's feature names and the fingerprint. A
//! train reads a level's feature names from it, so the names a model is
//! published with are those it is checked against.
//!
//! `Swap` is how the generations are used: at each decision, in time
//! order, the latest valid then, loaded and checked before use, all its
//! models together. A generation failing a check is refused with an error
//! naming it and the check, no decision is made on it nor on the one before
//! it, and it is read again at the next decision, so that a generation that
//! loads once mended is used from then.
//!
//! A generation is a directory, `<valid from>/` (an RFC 3339 time), holding
//! its manifest, `generation.json` (`GenerationManifest`), and the
//! directory of the publish that wrote it, which holds a file per model,
//! `long.json`, `short.json` and `meta.json`. A publish writes its models'
//! files in a directory of its own, named by the process and the publish,
//! then the manifest naming that directory, written beside its name and
//! renamed onto it: the manifest is the generation's commit marker, a
//! generation without one is not listed, and publishers at once, in a
//! backtest and a live node say, never write each other's files, the last
//! manifest renamed naming the models of one publish. A model's file is its
//! manifest (`ModelManifest`), the schema (the feature names in order, the
//! fingerprint), the target spec and side, the learner, the meta's
//! calibration, its level line (a side's is its pair's, the generation's,
//! `calibration::Ksplit`), the meta's ancestor's time, its baseline (the training mean of the R it
//! learned on the rows it was fitted and calibrated on) and probe rows, of
//! its calibration slice, with the predictions the model made on them when
//! fitted, before any calibration, and the model as its adapter saved it. A
//! side's model predicts R (`SideModel`), its pair calibrating both sides'
//! predictions at once (`Pair::expect`); a meta's predicts the R of the
//! trade of the side picked, every prediction calibrated (`LoadedModel`);
//! each carries its baseline (`Baselined`). A model whose manifest has the
//! other level's calibration is refused at load. A reloaded model is checked
//! by predicting the probes, never by its bytes. A missing feature, NaN, has
//! no JSON number: a probe row writes NaN and the infinities as strings, and
//! so does a probe's prediction.

use std::{
    fmt::Debug,
    fs,
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

/// The name of a generation's manifest, in its directory.
const MANIFEST: &str = "generation.json";

/// The name of a meta model's file, in the directory of its generation's
/// publish; a side's is its side's name.
const META: &str = "meta";

/// The extension of a model's file.
const EXTENSION: &str = "json";

/// The publishes of this process so far, which tell its directories and
/// temporary files apart.
static PUBLISHES: AtomicU64 = AtomicU64::new(0);

/// The version of what a published generation holds, its manifests
/// (`GenerationManifest`, `ModelManifest`), its layout and how it is read.
/// Keyed with the generation key (`generation_directory`): bumped by hand
/// whenever that changes, which keys every store anew, so that no
/// generation of another layout is read and every model is trained again,
/// on the datasets as they are.
const VERSION: u32 = 11;

/// A strategy's models: the generations its trains published, keyed by the
/// one fingerprint of its dataset and its generation key, each side its
/// margins trade and its meta model, if it has one.
#[derive(Debug, Clone)]
pub struct ModelStore {
    /// The directory of its generation key, its generations in it.
    directory: PathBuf,
    /// The fingerprint of its dataset, which each model is checked against.
    fingerprint: String,
    /// Each side traded, the long first.
    sides: Vec<SideLevel>,
    meta: Option<MetaLevel>,
}

/// A side of a model store: the side, its margin and the names of its
/// model's features.
#[derive(Debug, Clone)]
pub struct SideLevel {
    side: Side,
    margin: R,
    feature_names: Vec<String>,
}

/// The meta level of a model store: the meta model's config and the names of
/// its features.
#[derive(Debug, Clone)]
pub struct MetaLevel {
    config: MetaConfig,
    feature_names: Vec<String>,
}

/// One train's models as they are published and read back: valid from its
/// retrain time, the model of each side the strategy trades, the long
/// first, the calibration of their pair, fitted on both sides' predictions,
/// and the meta's model, if and only if the strategy has a meta.
/// Self-contained: the models of one train, fixed as of its retrain time,
/// none derived from another generation.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredGeneration {
    pub valid_from: Timestamp,
    /// Each side traded, the long first, its manifest naming its side.
    pub sides: Vec<StoredModel>,
    pub calibration: Ksplit,
    pub meta: Option<StoredModel>,
}

/// What a generation's manifest holds, written last as its commit marker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct GenerationManifest {
    /// The directory of the publish its models' files are in, within the
    /// generation's.
    publish: String,
    /// The calibration of its sides' pair.
    calibration: Ksplit,
}

/// A generation loaded and checked, as a decision uses it: its sides'
/// pair, and its meta's model if the strategy has a meta, all of one train,
/// fixed as of its retrain time and none derived from another generation.
#[derive(Debug)]
pub struct Generation {
    pair: Pair,
    meta: Option<LoadedModel>,
}

/// The models of the sides a strategy trades of one generation, one of
/// each, loaded and checked together, or a train holds before publishing
/// them, with the calibration fitted on them.
#[derive(Debug)]
pub struct Pair {
    /// Each side traded, the long first, and its model.
    models: Vec<(Side, SideModel)>,
    calibration: Ksplit,
}

/// A published model and its manifest.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StoredModel {
    pub manifest: ModelManifest,
    /// The model as its adapter saved it, for the manifest's learner to
    /// load.
    pub model: SavedModel,
}

/// What a published model is, enough to check it before use.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ModelManifest {
    /// The fingerprint of the features, the decision rule, the target and
    /// the venue of its dataset runs.
    pub fingerprint: String,
    /// The names of the features, in the order of a row's values.
    pub feature_names: Vec<String>,
    /// The target spec.
    pub target: TargetConfig,
    /// The side of the target the model learned; none for a meta model,
    /// which learns the trade of the side picked.
    pub side: Option<Side>,
    pub learner: LearnerConfig,
    /// A meta model's calibration, the level line of its predictions of R,
    /// as a one-sided pair's (`Ksplit`); none of a side's, which its pair
    /// calibrates (`StoredGeneration::calibration`).
    pub calibration: Option<Line>,
    /// A meta model's ancestor's time: the time the ancestor pair whose
    /// picks it learned from was fitted as of, a meta's window before its
    /// generation's; none of a side's.
    pub ancestor: Option<Timestamp>,
    /// The model's baseline, the constant model its scores are read
    /// beside: the training mean of the R it learned on the rows it was
    /// fitted and calibrated on.
    pub baseline: f64,
    /// Rows of its calibration slice and the model's predictions on them,
    /// before any calibration.
    pub probes: Vec<Probe>,
}

/// A row and the prediction of a model on it when it was fitted, before
/// any calibration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Probe {
    #[serde(with = "values")]
    pub row: Vec<f64>,
    #[serde(with = "values::one")]
    pub prediction: f64,
}

impl StoredModel {
    /// The meta model, loaded by its manifest's learner with its level line
    /// and checked as `checked` does.
    ///
    /// # Errors
    ///
    /// Returns an error if the model cannot be loaded, fails a check, or its
    /// manifest has no level line, as a side's.
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

    /// The side model, loaded and checked as `checked` does, which its pair
    /// calibrates.
    ///
    /// # Errors
    ///
    /// Returns an error if the model cannot be loaded or fails a check (see
    /// `checked`), or if its manifest has a calibration of its own, as a
    /// meta's.
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

    /// The model, loaded by its manifest's learner and checked against a
    /// strategy of `feature_names` and `fingerprint`: the manifest's
    /// feature names in order and fingerprint are the strategy's, the model
    /// takes as many features, and it predicts each probe to the bit as
    /// when fitted.
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

/// A meta model as a strategy and a score use it: loaded, predicting the
/// realized R of the trade of the side picked, every prediction calibrated
/// by its level line, with its baseline.
#[derive(Debug)]
pub struct LoadedModel {
    model: Model,
    calibration: Line,
    /// The training mean of R on the rows it was fitted and calibrated on
    /// (`ModelManifest::baseline`).
    baseline: f64,
}

/// A model as the store loads it, with the baseline of its manifest
/// (`ModelManifest::baseline`), which its scores are read beside: a
/// meta's (`LoadedModel`) and a side's (`SideModel`) alike.
pub(super) trait Baselined {
    /// The training mean of what the model learned on the rows it was
    /// fitted and calibrated on.
    fn baseline(&self) -> f64;
}

impl LoadedModel {
    /// The calibrated expected R of the trade of `row`.
    ///
    /// # Errors
    ///
    /// Returns an error if `Model::predict` fails.
    pub fn predict(&self, row: &[f64]) -> anyhow::Result<f64> {
        Ok(self.calibration.apply(self.model.predict(row)?))
    }
}

/// A side's model as a strategy and a score use it: loaded, predicting the
/// realized R of its side's trade, which its pair calibrates
/// (`Pair::expect`), with its baseline, the training mean of R.
#[derive(Debug)]
pub struct SideModel {
    model: Model,
    baseline: f64,
}

impl SideModel {
    /// The side's model `model` fitted, its baseline `baseline`.
    #[must_use]
    pub const fn new(model: Model, baseline: f64) -> Self {
        Self { model, baseline }
    }

    /// The R of the side's trade of `row` the model predicts, before its
    /// pair's calibration.
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
    /// The models, under `root`, of the model of `blueprint`, its dataset's
    /// fingerprint `fingerprint`, its blueprint's on the venue of its
    /// dataset runs (`ModelBlueprint::fingerprint`), and trained as its
    /// config says: each side its margins trade, and its meta model if its
    /// config has a meta section, in the directory of that fingerprint and
    /// its generation key (`generation_directory`).
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
            generation_directory(root, blueprint.strategy_id(), fingerprint, config, VERSION)?;
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

    /// The directory of its generation key, its generations in it.
    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    /// Whether a strategy trades on its generations at `now`: a generation
    /// is valid by then (`Swap::at`). Its models may still fail their checks
    /// as they load.
    ///
    /// # Errors
    ///
    /// Returns an error if the generations cannot be listed.
    pub fn trades_by(&self, now: Timestamp) -> anyhow::Result<bool> {
        Ok(self.generations()?.iter().any(|&time| time <= now))
    }

    /// Each side the margins trade, the long first.
    pub fn sides(&self) -> impl Iterator<Item = &SideLevel> {
        self.sides.iter()
    }

    /// The side `side`, if the margins trade it.
    #[must_use]
    pub fn side(&self, side: Side) -> Option<&SideLevel> {
        self.sides.iter().find(|level| level.side == side)
    }

    /// The meta level, if the config has a meta section.
    #[must_use]
    pub const fn meta(&self) -> Option<&MetaLevel> {
        self.meta.as_ref()
    }

    /// The times its generations are valid from, in order: those whose
    /// manifest is written; none if nothing was published.
    ///
    /// # Errors
    ///
    /// Returns an error if its directory cannot be read, or a generation
    /// with a manifest is named by no time.
    pub fn generations(&self) -> anyhow::Result<Vec<Timestamp>> {
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
                .with_context(|| format!("reading the generation {}", path.display()))?
            {
                continue;
            }
            let time = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.parse().ok())
                .with_context(|| format!("naming the generation {}", path.display()))?;
            times.push(time);
        }
        times.sort();
        Ok(times)
    }

    /// Publishes `generation`, valid from its time: its models' files
    /// written in a directory of this publish, named by the process and the
    /// publish, then its manifest, naming that directory, written beside its
    /// name and renamed onto it, replacing the generation valid from then, if
    /// any. Returns the generation's directory, named by its time in the
    /// store's.
    ///
    /// # Errors
    ///
    /// Returns an error if a side's model names no side, the generation
    /// holds a meta model and the strategy has no meta or the reverse, or a
    /// file cannot be written.
    pub fn publish(&self, generation: &StoredGeneration) -> anyhow::Result<PathBuf> {
        anyhow::ensure!(
            generation.meta.is_some() == self.meta.is_some(),
            "publishing a generation {} a meta model for a strategy {} a meta",
            if generation.meta.is_some() {
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
        let directory = self.generation(generation.valid_from);
        let publish = format!(
            "{}-{}",
            std::process::id(),
            PUBLISHES.fetch_add(1, Ordering::Relaxed)
        );
        let models = directory.join(&publish);
        fs::create_dir_all(&models)
            .with_context(|| format!("creating the generation {}", directory.display()))?;
        for model in &generation.sides {
            let side = model
                .manifest
                .side
                .context("publishing a side's model that names no side")?;
            write(&models.join(file(side.name())), model)?;
        }

        if let Some(meta) = &generation.meta {
            write(&models.join(file(META)), meta)?;
        }
        let manifest = directory.join(MANIFEST);
        let written = manifest.with_extension(format!("{EXTENSION}.{publish}.tmp"));
        let text = serde_json::to_string_pretty(&GenerationManifest {
            publish,
            calibration: generation.calibration,
        })?;
        fs::write(&written, text)
            .and_then(|()| fs::rename(&written, &manifest))
            .with_context(|| format!("writing the generation {}", manifest.display()))?;
        Ok(directory)
    }

    /// The generation valid from `valid_from`, as published: the model of
    /// each side traded and its meta's, if the strategy has a meta.
    ///
    /// # Errors
    ///
    /// Returns an error if there is none, or a file of it cannot be read.
    pub fn read(&self, valid_from: Timestamp) -> anyhow::Result<StoredGeneration> {
        let manifest = self.manifest(valid_from)?;
        let models = self.generation(valid_from).join(&manifest.publish);
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
        Ok(StoredGeneration {
            valid_from,
            sides,
            calibration: manifest.calibration,
            meta,
        })
    }

    /// The generations as a strategy swaps them in (`Swap`), each loaded
    /// and checked as `load` does; none in use yet.
    #[must_use]
    pub fn swap(&self) -> Swap<Generation> {
        let models = self.clone();
        Swap::new(move |valid_from| {
            models.load(valid_from).with_context(|| {
                format!("the generation {}", models.generation(valid_from).display())
            })
        })
    }

    /// The generation valid from `valid_from`, loaded and checked: each
    /// side's model against the side models' feature names and the
    /// fingerprint (`StoredModel::load_side`), calibrated as a pair by the
    /// generation's calibration, and the meta's, if the strategy has a meta,
    /// against the meta's.
    ///
    /// # Errors
    ///
    /// Returns an error if it cannot be read, its meta model included, a
    /// model fails its checks, naming its level, or the calibration is not
    /// of as many sides as the pair.
    fn load(&self, valid_from: Timestamp) -> anyhow::Result<Generation> {
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
        Ok(Generation {
            pair: Pair::new(models, stored.calibration)?,
            meta,
        })
    }

    /// The manifest of the generation valid from `valid_from`.
    ///
    /// # Errors
    ///
    /// Returns an error if there is none, or it cannot be read.
    fn manifest(&self, valid_from: Timestamp) -> anyhow::Result<GenerationManifest> {
        read(&self.generation(valid_from).join(MANIFEST))
    }

    /// The directory of the generation valid from `valid_from`.
    fn generation(&self, valid_from: Timestamp) -> PathBuf {
        self.directory.join(valid_from.to_string())
    }

    /// A generation of the store named by no time, so that listing it fails
    /// while its generations can still be read.
    #[cfg(test)]
    fn unlistable(&self) -> PathBuf {
        self.directory.join("no time")
    }

    /// Makes listing the generations fail (`generations`), until
    /// `mend_listing`.
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

    /// Removes the manifest of the generation valid from `valid_from`, as a
    /// publish stopped before it leaves the generation.
    #[cfg(test)]
    pub(super) fn drop_manifest(&self, valid_from: Timestamp) {
        fs::remove_file(self.generation(valid_from).join(MANIFEST)).unwrap();
    }

    /// Makes publishing a generation fail (`publish`), listing them still
    /// finding none: the directory of the generation key a link to nowhere.
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

    /// The expected net R from which the side is traded
    /// (`ModelConfig::margins`).
    #[must_use]
    pub const fn margin(&self) -> R {
        self.margin
    }

    /// The names of the side model's features, in the order of its rows
    /// (`features::side_names`): the strategy's, then the target's payoff
    /// names.
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

    /// The names of the meta's features, in the order of its rows
    /// (`features::meta_names`).
    #[must_use]
    pub fn feature_names(&self) -> &[String] {
        &self.feature_names
    }
}

impl Generation {
    /// Its sides' pair.
    #[must_use]
    pub const fn pair(&self) -> &Pair {
        &self.pair
    }

    /// Its meta's model, none if the strategy has no meta.
    #[must_use]
    pub const fn meta(&self) -> Option<&LoadedModel> {
        self.meta.as_ref()
    }
}

impl Pair {
    /// The pair of `models`, each side traded, the long first, and its
    /// model, calibrated by `calibration`.
    ///
    /// # Errors
    ///
    /// Returns an error if `calibration` is not of as many sides: a side
    /// call of two, none of one.
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

    /// The calibrated expectation of the decision of each side's row `rows`:
    /// each side's model's prediction on its row, calibrated together.
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

    /// The calibration of its sides' predictions.
    #[must_use]
    pub const fn calibration(&self) -> Ksplit {
        self.calibration
    }

    /// The model of `side`.
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

/// The name of the file of the model `name`, a side's or the meta's.
fn file(name: &str) -> String {
    format!("{name}.{EXTENSION}")
}

/// Writes `value` as JSON to `path`.
///
/// # Errors
///
/// Returns an error if the file cannot be written.
fn write(path: &Path, value: &impl Serialize) -> anyhow::Result<()> {
    fs::write(path, serde_json::to_string_pretty(value)?)
        .with_context(|| format!("writing the model {}", path.display()))
}

/// The JSON of the file `path`.
///
/// # Errors
///
/// Returns an error if the file is missing, cannot be read, or holds no
/// `T`, naming it.
fn read<T: DeserializeOwned>(path: &Path) -> anyhow::Result<T> {
    let json = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&json).with_context(|| format!("reading {}", path.display()))
}

/// The directory, under `root`, of the stores of `strategy_id` whose
/// dataset has `fingerprint` (`dataset::fingerprint`): its id, then the
/// fingerprint.
fn dataset_directory(root: &Path, strategy_id: StrategyId, fingerprint: Fingerprint) -> PathBuf {
    root.join(strategy_id.as_str())
        .join(fingerprint.to_string())
}

/// The directory, under `root`, of the generations of `strategy_id` whose
/// dataset has `fingerprint`, its models trained as `config` says, of the
/// store `version` (`VERSION`): its dataset's, then its generation key, the
/// side models' training settings (the learner and its settings, the
/// retrain interval, the window, the calibration and the fewest rows of a
/// slice), the margins of both sides, the meta's training settings or none
/// (`meta_settings`), and the version.
///
/// # Errors
///
/// Returns an error if a learner cannot be written as JSON.
fn generation_directory(
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

/// The retrain `retrain` as the generation key holds it, its interval or
/// never: a strategy retrained at another interval, or never, publishes to
/// and reads from another store, so a weekly-retrained strategy never
/// trades on a generation a daily-retrained one published between its
/// retrains, nor a strategy never retrained on a retrained one's.
fn retrain_setting(retrain: Retrain) -> String {
    match retrain {
        Retrain::Every(every) => format!("retrain every {}", every.as_nanos()),
        Retrain::Never => "retrain never".to_string(),
    }
}

/// The margins `margins` as the generation key holds them, each in R,
/// `long 0.5R short none`, so that a store keyed by margins of another
/// unit, money, is another store.
fn margins_text(margins: Margins) -> String {
    let margin = |margin: Option<R>| margin.map_or_else(|| "none".to_string(), |r| r.to_string());
    format!(
        "long {} short {}",
        margin(margins.long),
        margin(margins.short)
    )
}

/// The training settings of the meta `meta` as the generation key holds
/// them: its learner, window, calibration, margin in R and fewest rows of a
/// slice; `no meta` of none.
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

/// The generations of a model store as a strategy swaps them in, decision by
/// decision in time order: at each, the latest valid then, read as an `M`
/// and checked against the strategy before use. One failing a check is
/// refused, with an error naming it and the check, and no generation is in
/// use until it loads: it is read again at the next decision. A model store
/// hands it out (`ModelStore::swap`).
pub struct Swap<M> {
    /// The generation valid from a time, read and checked.
    read_at: Box<dyn Fn(Timestamp) -> anyhow::Result<M>>,
    /// The generation in use, and its valid-from.
    current: Option<(Timestamp, M)>,
}

impl<M> Swap<M> {
    /// Swaps in the generations `read_at` reads and checks, by their
    /// valid-from; none in use yet.
    fn new(read_at: impl Fn(Timestamp) -> anyhow::Result<M> + 'static) -> Self {
        Self {
            read_at: Box::new(read_at),
            current: None,
        }
    }

    /// The generation in use at `now`, and its valid-from, among those
    /// valid from `valid_froms`, the store's list in order: the latest valid
    /// then, swapped in if not in use yet; none if none is valid yet.
    ///
    /// # Errors
    ///
    /// Returns an error if the latest fails its checks, naming it and the
    /// check: it is read again at the next call.
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
            // One message of the whole chain: Nautilus logs an error a
            // strategy's handler returns by its message alone.
            let generation = (self.read_at)(latest).map_err(|e| {
                anyhow::anyhow!(
                    "no new entry until the generation valid from {latest} loads: {e:#}"
                )
            })?;
            log::info!("the generation valid from {latest} in use from {now}");
            self.current = Some((latest, generation));
        }
        Ok(self
            .current
            .as_ref()
            .map(|(valid_from, generation)| (*valid_from, generation)))
    }
}

/// A swap reads as the generation in use.
impl<M: Debug> Debug for Swap<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(Swap))
            .field("current", &self.current)
            .finish_non_exhaustive()
    }
}

/// A row of values as JSON: a number each, NaN and the infinities, which
/// JSON has no number for, as `"NaN"`, `"inf"` and `"-inf"`; and one value
/// alike (`one`).
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

    /// A meta model taking the trades of the side picked from an expected
    /// net R of 0.55.
    fn meta_config() -> MetaConfig {
        MetaConfig::builder()
            .window(SignedDuration::from_hours(48))
            .calibration(SignedDuration::from_hours(12))
            .margin(R::new(0.55))
            .min_rows(20)
            .learner(LearnerConfig::Fake {})
            .build()
    }

    /// `config()`, its trades taken by `meta_config()`.
    fn with_meta() -> ModelConfig {
        ModelConfig {
            meta: Some(meta_config()),
            ..config()
        }
    }

    /// The model of `config`.
    fn blueprint(config: &ModelConfig) -> ModelBlueprint {
        ModelBlueprint::new(
            StrategyId::from(STRATEGY),
            features(),
            config.clone(),
            requirements(),
        )
        .unwrap()
    }

    /// The models under `root` of `config`.
    fn models(root: &Path, config: &ModelConfig) -> ModelStore {
        blueprint(config).model_store(root).unwrap()
    }

    /// What the strategy reads: the quotes of gold.
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

    /// The venue of the dataset runs: a commission rate of 30 per million,
    /// 10 million USD.
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

    /// The calibration of a pair of both sides, its level line `0.1 + 0.5 C`
    /// and its side call's slope 1.5.
    const PAIR: Ksplit = Ksplit {
        level: Line {
            alpha: 0.1,
            beta: 0.5,
        },
        side_call: Some(1.5),
    };

    /// A fake model of `side`, a meta's of none, on a feature `mid`, probed
    /// on `rows`, its baseline the mean of its training labels, 1/2; a
    /// meta's calibrated by the line `0.1 + 0.5 E`.
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
                ancestor: None,
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

    /// A generation of both sides valid from `valid_from`, each a fake
    /// probed on `rows`, calibrated by `PAIR`, and a meta's if `meta`.
    fn generation(valid_from: &str, rows: &[Vec<f64>], meta: bool) -> StoredGeneration {
        StoredGeneration {
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
    fn a_published_generation_reads_back_whole() {
        let dir = tempfile::tempdir().unwrap();
        let models = models(dir.path(), &with_meta());
        let published = generation(MIDNIGHT, &[vec![0.3], vec![0.1 + 0.2]], true);

        models.publish(&published).unwrap();

        assert_eq!(models.read(at(MIDNIGHT)).unwrap(), published);
    }

    /// Every generation of a strategy with a meta holds a meta model, and
    /// none of one without holds one: a publish of either otherwise is
    /// refused, naming the meta, and publishes nothing.
    #[rstest]
    #[case::without_a_meta_for_a_strategy_with_one(with_meta(), false)]
    #[case::with_a_meta_for_a_strategy_without_one(config(), true)]
    fn a_generation_whose_meta_is_not_the_strategys_is_refused(
        #[case] config: ModelConfig,
        #[case] meta: bool,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let models = models(dir.path(), &config);

        let err = models
            .publish(&generation(MIDNIGHT, &[vec![0.3]], meta))
            .unwrap_err();

        assert!(err.to_string().contains("meta"), "{err}");
        assert_eq!(models.generations().unwrap(), []);
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
        models.publish(&generation(MIDNIGHT, &rows, false)).unwrap();

        let read = models.read(at(MIDNIGHT)).unwrap();

        let bits = |values: &[f64]| -> Vec<u64> { values.iter().map(|v| v.to_bits()).collect() };
        let (rows_read, predictions): (Vec<f64>, Vec<f64>) = read.sides[0]
            .manifest
            .probes
            .iter()
            .map(|probe| (probe.row[0], probe.prediction))
            .unzip();
        assert_eq!(bits(&rows_read), bits(&rows.concat()));
        // The fake predicts the first feature, the training mean, 1/2,
        // where it is missing.
        assert_eq!(
            bits(&predictions),
            bits(&[0.5, f64::INFINITY, f64::NEG_INFINITY, -0.0])
        );
    }

    #[rstest]
    fn a_store_lists_the_times_its_generations_are_valid_from_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let models = models(dir.path(), &config());
        assert!(models.generations().unwrap().is_empty());

        for time in ["2026-10-23T00:00:00Z", "2026-10-21T12:00:00Z"] {
            models
                .publish(&generation(time, &[vec![0.5]], false))
                .unwrap();
        }

        assert_eq!(
            models.generations().unwrap(),
            [at("2026-10-21T12:00:00Z"), at("2026-10-23T00:00:00Z")]
        );
    }

    /// A generation's manifest is written last: one whose models are
    /// written and its manifest not, as a crash before it leaves, is not
    /// listed, so no decision uses it, nor does a model store trade on it.
    #[rstest]
    fn a_generation_whose_manifest_is_missing_is_not_listed() {
        let dir = tempfile::tempdir().unwrap();
        let models = models(dir.path(), &config());
        models
            .publish(&generation(MIDNIGHT, &[vec![0.5]], false))
            .unwrap();
        let noon = "2026-10-22T12:00:00Z";
        models
            .publish(&generation(noon, &[vec![0.5]], false))
            .unwrap();

        models.drop_manifest(at(noon));

        assert_eq!(models.generations().unwrap(), [at(MIDNIGHT)]);
        assert!(!models.trades_by(at("2026-10-21T23:00:00Z")).unwrap());
        assert!(models.trades_by(at(noon)).unwrap());
    }

    #[rstest]
    fn a_generation_valid_from_a_fraction_of_a_second_lists_and_reads_back() {
        let dir = tempfile::tempdir().unwrap();
        let models = models(dir.path(), &config());
        let published = generation("2026-10-22T04:00:00.01Z", &[vec![0.5]], false);

        models.publish(&published).unwrap();

        assert_eq!(
            models.generations().unwrap(),
            [at("2026-10-22T04:00:00.01Z")]
        );
        assert_eq!(
            models.read(at("2026-10-22T04:00:00.01Z")).unwrap(),
            published
        );
    }

    /// Two publishers of a generation at the same time, say a backtest and
    /// a live node's retrain, each write their own models and manifest
    /// before the rename: neither fails, and the generation reads back
    /// whole, its models those of one publish.
    #[rstest]
    fn publishers_at_the_same_time_at_once_each_publish_a_whole_generation() {
        let dir = tempfile::tempdir().unwrap();
        let models = models(dir.path(), &with_meta());
        let published = generation(MIDNIGHT, &[vec![0.5]], true);

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

    /// A generation published again at its time replaces the first whole:
    /// its sides and its meta, the second's probed on other rows.
    #[rstest]
    fn publishing_at_the_same_time_replaces_the_generation() {
        let dir = tempfile::tempdir().unwrap();
        let models = models(dir.path(), &with_meta());
        models
            .publish(&generation(MIDNIGHT, &[vec![0.5]], true))
            .unwrap();
        let second = generation(MIDNIGHT, &[vec![0.9]], true);

        models.publish(&second).unwrap();

        assert_eq!(models.generations().unwrap(), [at(MIDNIGHT)]);
        assert_eq!(models.read(at(MIDNIGHT)).unwrap(), second);
    }

    /// `config` changed by `change`.
    fn changed(config: ModelConfig, change: fn(&mut ModelConfig)) -> ModelConfig {
        let mut config = config;
        change(&mut config);
        config
    }

    /// `with_meta()`, its meta changed by `change`.
    fn meta_changed(change: fn(&mut MetaConfig)) -> ModelConfig {
        let mut config = with_meta();
        change(config.meta.as_mut().unwrap());
        config
    }

    /// A generation published under `with_meta()` is listed there alone:
    /// any other setting of the sides or of the meta selects another
    /// generation key. Another learner, retrain interval, window,
    /// calibration, minimum of rows or target is other training; a model of
    /// another margin of either side, as one priced at another reference
    /// stake, the config's risk of the dataset balance, which a dataset's
    /// rows and a model's payoff features are priced at, would decide
    /// otherwise; and a meta's other settings, or none, are another meta.
    /// The strategy trains again rather than reuse them.
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
    fn any_other_setting_of_the_sides_or_the_meta_lists_none_of_the_generations(
        #[case] other: ModelConfig,
    ) {
        let dir = tempfile::tempdir().unwrap();
        models(dir.path(), &with_meta())
            .publish(&generation(MIDNIGHT, &[vec![0.5]], true))
            .unwrap();

        let listed = |config: &ModelConfig| models(dir.path(), config).generations().unwrap();

        assert_eq!(listed(&with_meta()), [at(MIDNIGHT)]);
        assert!(listed(&other).is_empty());
    }

    /// Bumping the store version is another generation key: no generation
    /// of another layout is read, and every model is trained again.
    #[rstest]
    fn bumping_the_store_version_keys_another_store() {
        let config = with_meta();
        let fingerprint = blueprint(&config).fingerprint().unwrap();
        let at = |version| {
            generation_directory(
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

    /// The directory of a fixed strategy's generations under a models root,
    /// as a literal: its id, its dataset's fingerprint, then its generation
    /// key, of its sides' training settings, the margins of both sides in R,
    /// `long 0.5R short 0.5R`, so that no generation published under a
    /// margin of another unit is listed, its meta's settings, its margin in
    /// R, `0.55R`, among them, or none, and the store `VERSION`. A change
    /// that rekeyed every store, leaving the generations published to it
    /// behind, fails here. A deliberate rekey, a bump of `VERSION` or of
    /// `dataset::VERSION`, or a new input to the dataset's fingerprint or to
    /// the generation key, updates the literals here; a rekey of the dataset
    /// updates the pinned dataset identifier too
    /// (`dataset::tests::a_fixed_strategys_dataset_keeps_its_identifier`).
    #[rstest]
    #[case::without_a_meta(
        config(),
        "f3ccb00e55c138b7bfec59ea9311d2d896b94a50199b789d7446779df3690e83"
    )]
    #[case::with_a_meta(
        with_meta(),
        "14e745ebb978ee6d7738e658c9e574780e5e5f42fabdb3b6d965a47d95fce216"
    )]
    fn a_fixed_strategys_generations_keep_their_directory(
        #[case] config: ModelConfig,
        #[case] key: &str,
    ) {
        assert_eq!(
            models(Path::new("models"), &config).directory(),
            Path::new(
                "models/STORE-001/\
                 0614a0ef7ecea9c6cf17bdf7a38ab2ab47a955646cf2257595b30158aa9e35c9"
            )
            .join(key)
        );
    }

    /// A strategy trades on a generation valid by then: a store holding no
    /// generation, or one valid later alone, has none to trade on yet; with
    /// a meta section alike, each generation holding a meta model.
    #[rstest]
    #[case::without_a_meta(config(), false)]
    #[case::with_a_meta(with_meta(), true)]
    fn a_model_store_trades_on_a_generation_valid_by_then(
        #[case] config: ModelConfig,
        #[case] meta: bool,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let before = "2026-10-21T23:59:59Z";
        let trades = |now: &str| models(dir.path(), &config).trades_by(at(now)).unwrap();

        assert!(!trades(MIDNIGHT));
        models(dir.path(), &config)
            .publish(&generation(MIDNIGHT, &[vec![0.5]], meta))
            .unwrap();
        assert!(trades(MIDNIGHT));
        assert!(!trades(before));
    }

    /// A meta model's every prediction of R is calibrated by its level line,
    /// and its probes are its predictions before it: the fake predicts the
    /// first feature, 0.25, calibrated by the line `0.1 + 0.5 E` to an
    /// expected R of 0.225.
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

    /// A side's model has no calibration of its own, its pair's being the
    /// generation's, and a meta's its own level line: each loaded as the
    /// other's level is refused.
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

    /// The fake long and short of a generation valid from midnight, on a
    /// feature `mid`, then the entry cost of the time-only target, each
    /// predicting its row's mid: the long's 0.4 and the short's 0.1 are C =
    /// 0.25 and D = 0.3, calibrated by the generation's line `0.1 + 0.5 C`
    /// and slope 1.5 to C' = 0.225 and D' = 0.45, the long expecting 0.45R
    /// and the short nought.
    #[rstest]
    fn a_generations_pair_expects_each_sides_r_through_its_calibration() {
        let dir = tempfile::tempdir().unwrap();
        let models = models(dir.path(), &config());
        let fingerprint = blueprint(&config()).fingerprint().unwrap().to_string();
        let names = models.side(Side::Long).unwrap().feature_names().to_vec();
        let mut published = generation(MIDNIGHT, &[], false);
        for stored in &mut published.sides {
            stored.manifest.fingerprint.clone_from(&fingerprint);
            stored.manifest.feature_names.clone_from(&names);
            let set = TrainingSet::new(2, vec![0.2, 0.0], vec![0.0]).unwrap();
            stored.model = LearnerConfig::Fake {}.fit(&set).unwrap().save().unwrap();
        }
        models.publish(&published).unwrap();
        let mut generations = models.swap();
        let midnight = at(MIDNIGHT);

        let (_, generation) = generations.at(midnight, &[midnight]).unwrap().unwrap();
        let expected = generation
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

    /// A side's manifest of an earlier store version: its pair's
    /// calibration in its manifest, naming the pair, beside `current`'s
    /// fields.
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

    /// A side's manifest of an earlier store version: Platt's calibration
    /// and a payoff record, beside `current`'s fields.
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

    /// The manifests of earlier store versions are no models of this one:
    /// a generation holding one is refused as it is read, naming its file.
    #[rstest]
    #[case::of_a_pair_calibrated_side(pair_calibrated_side)]
    #[case::of_a_platt_side(platt_side)]
    fn a_model_of_an_earlier_manifest_is_refused_naming_its_file(
        #[case] earlier: fn(serde_json::Value) -> serde_json::Value,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let models = models(dir.path(), &config());
        models.publish(&generation(MIDNIGHT, &[], false)).unwrap();
        let manifest = models.manifest(at(MIDNIGHT)).unwrap();
        let path = models
            .generation(at(MIDNIGHT))
            .join(manifest.publish)
            .join(file(Side::Long.name()));
        let current = serde_json::to_value(model(Some(Side::Long), &[])).unwrap();
        fs::write(&path, earlier(current).to_string()).unwrap();

        let refused = models.read(at(MIDNIGHT)).unwrap_err();

        let message = format!("{refused:#}");
        assert!(message.contains(&path.display().to_string()), "{message}");
    }
}
