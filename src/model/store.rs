//! The model store: the models published for a strategy, each valid from
//! the time it was trained at.
//!
//! A store is a directory per strategy id, fingerprint (that of its
//! dataset: the features, the decision rule, the target and the
//! venue settings of its dataset runs, see `dataset::fingerprint`) and training
//! settings (the learner and its settings, the retrain interval,
//! the window, the holdout and the minimums its holdout must meet to be
//! published, `holdout::Minimums`) with the store `VERSION`, and side, the
//! long or the short, keyed by the margins of both sides, which decide the
//! side picked on each row of the holdout and so what either publishes, so a
//! model is only ever listed beside models of the same features, costs,
//! training, side and margins, and of the same manifest. Backtest and live
//! read the same list.
//!
//! A meta model's store sits in a `meta/` directory beside its sides,
//! under their training settings, keyed by its own (its learner and
//! settings, window, holdout, margin and minimums) and the sides'
//! margins, which make its rows. A meta model is only ever listed beside
//! meta models of the same sides.
//!
//! A strategy's stores are built together, as its model set (`ModelSet`),
//! and only there: the fingerprint is computed once and keys them all. The
//! model set holds each side the margins trade, with its margin, its feature
//! names (the strategy's, then the target's payoff names,
//! `features::side_names`) and its store, and the meta level, if any, with
//! its config, its feature names and its store; it hands out the swap of the
//! sides' pairs and of the meta's models, checked against the level's
//! feature names and the fingerprint, and lists when each model was
//! published. A train reads a level's feature names from it, so the names a
//! model is published with are those it is checked against.
//!
//! The sides' models are used as pairs (`Pair`), the sides one train
//! published, valid from one time: a pair's valid-from is a time every side
//! traded has a model valid from (`Models::pairs`), so a side's model with
//! no other side's beside it, as a train stopped between its sides'
//! publishes leaves, is in no pair and never used. A one-sided strategy's
//! pairs are its one side's models. A pair is calibrated as one
//! (`calibration::Ksplit`), its calibration fitted on that pair and written
//! into each side's manifest, the same values naming the pair it was fitted
//! on, its valid-from and its sides (`PairCalibration`): a pair whose sides'
//! copies differ, or name another pair, is refused.
//!
//! `Swap` is how the models are used: at each decision, in time order,
//! the latest valid then, the sides' pair or the meta's model, checked
//! before use, one failing a check refused, a pair whole, and the current
//! one kept.
//!
//! Each model is one file, `<valid from>.json` (an RFC 3339 time): its
//! manifest, the schema (the feature names in order, the fingerprint), the
//! target spec and side, the learner, the calibration of its predictions (a
//! side's its pair's, a meta's its own level line, `ModelCalibration`), its
//! baseline (the training mean of the R it learned on the rows it was fitted
//! and calibrated on), its holdout (`holdout::Holdout`) and probe rows with
//! the predictions the model made on them when fitted, before any
//! calibration, and the model as its adapter saved it. A side's model
//! predicts R (`SideModel`), its pair calibrating both sides' predictions at
//! once (`Pair::expect`); a meta's predicts the R of the trade of the side
//! picked, every prediction calibrated (`LoadedModel`); each carries its
//! baseline (`Baselined`). A model whose manifest has the other level's
//! calibration is refused at load. A reloaded model is checked by
//! predicting the probes, never by its bytes. The file is written beside
//! its name, then renamed onto it, so a reader sees a whole model or none.
//! A missing feature, NaN, has no JSON number: a probe row writes NaN and
//! the infinities as strings, and so does a probe's prediction.

use std::{
    fmt, fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, ensure};
use jiff::Timestamp;
use nautilus_model::identifiers::StrategyId;
use serde::{Deserialize, Serialize};

use super::{
    calibration::{Expected, Ksplit, Line},
    config::{Margins, MetaConfig, ModelConfig},
    dataset::{self, DatasetVenue},
    deciding::SideRow,
    features::{FeatureSet, Fingerprint, meta_names, side_names},
    holdout::Holdout,
    learner::{LearnerConfig, Model, SavedModel},
    schedule::{Models, Retrain},
    targets::{PerSide, Side, TargetConfig},
};
use crate::strategy::DataRequirements;

/// The extension of a model's file.
const EXTENSION: &str = "json";

/// The publishes of this process so far, which tell its temporary files
/// apart.
static PUBLISHES: AtomicU64 = AtomicU64::new(0);

/// The directory of the meta models within the directory of their sides'
/// models.
const META: &str = "meta";

/// The version of what a published model's file holds, its manifest
/// (`ModelManifest`) and how a model is read from it. Keyed with the
/// training settings every store of a strategy sits under, the meta's
/// too (`training_directory`): bumped by hand whenever that changes,
/// which keys every store anew, so that no model of another manifest is
/// read and every model is trained again, on the datasets as they are.
const VERSION: u32 = 7;

/// The models of one strategy, fingerprint and training settings, as its
/// model set builds them (`ModelSet`).
#[derive(Debug, Clone)]
pub struct ModelStore {
    directory: PathBuf,
}

/// A strategy's models: the store of each side its margins trade and of
/// its meta model, if it has one, all keyed by the one fingerprint of its
/// dataset, and the swaps that use them.
#[derive(Debug, Clone)]
pub struct ModelSet {
    /// Each side traded, the long first.
    sides: Vec<SideLevel>,
    meta: Option<MetaLevel>,
}

/// A side of a model set: the side, its margin and its models.
#[derive(Debug, Clone)]
pub struct SideLevel {
    side: Side,
    margin: f64,
    models: CheckedStore,
}

/// The models of the sides a strategy trades valid from one time, one of
/// each: the pair one train published, loaded and checked together
/// (`ModelSet::pairs`), or a train holds before publishing it, with the
/// calibration fitted on it.
#[derive(Debug)]
pub struct Pair {
    /// Each side traded, the long first, and its model.
    models: Vec<(Side, SideModel)>,
    calibration: Ksplit,
}

/// The meta level of a model set: the meta model's config and its models.
#[derive(Debug, Clone)]
pub struct MetaLevel {
    config: MetaConfig,
    models: CheckedStore,
}

/// A level's store, and what a model of it is checked against when it is
/// swapped in: the names of the level's features, in order, and the
/// fingerprint of the strategy's dataset.
#[derive(Debug, Clone)]
struct CheckedStore {
    store: ModelStore,
    feature_names: Vec<String>,
    fingerprint: String,
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
    pub strategy_id: StrategyId,
    /// The time the model was trained at, from which it is valid.
    pub valid_from: Timestamp,
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
    /// The calibration of the model's predictions.
    pub calibration: ModelCalibration,
    /// The model's baseline, the constant model its scores are read
    /// beside: the training mean of the R it learned on the rows it was
    /// fitted and calibrated on.
    pub baseline: f64,
    /// The model's holdout, as its publication read it.
    pub holdout: Holdout,
    /// Rows of the holdout and the model's predictions on them, before any
    /// calibration.
    pub probes: Vec<Probe>,
}

/// How a published model's predictions are calibrated.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelCalibration {
    /// A side model's: its pair's, one copy in each side's manifest.
    Pair(PairCalibration),
    /// A meta model's: the level line of its predictions of R, as a
    /// one-sided pair's (`Ksplit`).
    Meta(Line),
}

/// A pair's calibration as each side's manifest holds it, naming the pair it
/// was fitted on: its valid-from and its sides.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PairCalibration {
    pub valid_from: Timestamp,
    /// The sides of the pair, the long first.
    pub sides: Vec<Side>,
    pub lines: Ksplit,
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
    /// manifest has a side's calibration.
    pub fn load(&self, feature_names: &[String], fingerprint: &str) -> anyhow::Result<LoadedModel> {
        let ModelCalibration::Meta(calibration) = self.manifest.calibration else {
            anyhow::bail!("its manifest has a pair's calibration, which no meta model has");
        };
        Ok(LoadedModel {
            model: self.checked(feature_names, fingerprint)?,
            calibration,
            baseline: self.manifest.baseline,
        })
    }

    /// The side model, loaded and checked as `checked` does, with the
    /// calibration of its pair its manifest holds, which the pair checks.
    ///
    /// # Errors
    ///
    /// Returns an error as `load` does, or if its manifest has no pair's
    /// calibration.
    fn load_side(
        &self,
        feature_names: &[String],
        fingerprint: &str,
    ) -> anyhow::Result<(SideModel, PairCalibration)> {
        let ModelCalibration::Pair(calibration) = &self.manifest.calibration else {
            anyhow::bail!("its manifest has no pair's calibration, which a side model trades by");
        };
        let model = SideModel::new(
            self.checked(feature_names, fingerprint)?,
            self.manifest.baseline,
        );
        Ok((model, calibration.clone()))
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
    fn checked(&self, feature_names: &[String], fingerprint: &str) -> anyhow::Result<Model> {
        let manifest = &self.manifest;
        ensure!(
            manifest.feature_names == feature_names,
            "its features {:?} are not the strategy's {feature_names:?}",
            manifest.feature_names
        );
        ensure!(
            manifest.fingerprint == fingerprint,
            "its fingerprint {} is not the strategy's {fingerprint}",
            manifest.fingerprint
        );
        let model = manifest.learner.load(&self.model)?;
        let width = model.width();
        ensure!(
            width == manifest.feature_names.len(),
            "it takes {width} features, its manifest names {}",
            manifest.feature_names.len()
        );
        for probe in &manifest.probes {
            let prediction = model.predict(&probe.row)?;
            ensure!(
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
/// realised R of the trade of the side picked, every prediction calibrated
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
    /// As `Model::predict`.
    pub fn predict(&self, row: &[f64]) -> anyhow::Result<f64> {
        Ok(self.calibration.apply(self.model.predict(row)?))
    }
}

/// A side's model as a strategy and a score use it: loaded, predicting the
/// realised R of its side's trade, which its pair calibrates
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
    /// As `Model::predict`.
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

impl ModelSet {
    /// The models, under `root`, of `strategy_id` deciding with `features`
    /// as `config` says, on its target for a strategy of `requirements`, run
    /// on `venue`, and trained as `config` says: the store of each side its
    /// margins trade, and of its meta model if `config` has a meta section,
    /// in the directory of its dataset's fingerprint and its training
    /// settings.
    ///
    /// # Errors
    ///
    /// Returns an error if a learner cannot be written as JSON, or as
    /// `dataset::fingerprint` does.
    pub fn new(
        root: &Path,
        strategy_id: StrategyId,
        features: &FeatureSet,
        config: &ModelConfig,
        requirements: &DataRequirements,
        venue: &DatasetVenue,
    ) -> anyhow::Result<Self> {
        let fingerprint = dataset::fingerprint(
            features,
            config.decision,
            &config.target,
            requirements,
            venue,
        )?;
        let training = training_directory(root, strategy_id, fingerprint, config, VERSION)?;
        let fingerprint = fingerprint.to_string();
        let side_names = side_names(features.names(), &config.target.payoff_names());
        let sides = config
            .margins
            .sides()
            .map(|(side, margin)| SideLevel {
                side,
                margin,
                models: CheckedStore {
                    store: ModelStore::new(&training, side, config.margins),
                    feature_names: side_names.clone(),
                    fingerprint: fingerprint.clone(),
                },
            })
            .collect();
        let meta = config
            .meta
            .as_ref()
            .map(|meta| {
                Ok::<_, anyhow::Error>(MetaLevel {
                    config: meta.clone(),
                    models: CheckedStore {
                        store: ModelStore::meta(&training, meta, config.margins)?,
                        feature_names: meta_names(features.names()),
                        fingerprint: fingerprint.clone(),
                    },
                })
            })
            .transpose()?;
        Ok(Self { sides, meta })
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

    /// The sides' pairs as a strategy swaps them in, valid from the
    /// stores' pairs (`Models::pairs` of `valid_froms`): each side's model
    /// of a pair loaded and checked against the side models' feature names
    /// and the fingerprint (`StoredModel::load_side`), with the calibration
    /// its manifest holds, which must be the same in every side's and name
    /// this pair, its valid-from and its sides; a pair with a side failing
    /// refused whole; none in use yet.
    #[must_use]
    pub fn pairs(&self) -> Swap<Pair> {
        let sides = self.sides.clone();
        Swap::new(move |valid_from| {
            let mut models = vec![];
            let mut calibrations: Vec<PairCalibration> = vec![];
            for level in &sides {
                let (model, calibration) = level
                    .models
                    .read(valid_from, StoredModel::load_side)
                    .with_context(|| format!("the {} model", level.side.name()))?;
                models.push((level.side, model));
                calibrations.push(calibration);
            }
            let calibration = calibrations.swap_remove(0);
            ensure!(
                calibrations.iter().all(|other| *other == calibration),
                "its sides hold different calibrations: {calibration:?} beside {calibrations:?}"
            );
            let traded: Vec<Side> = sides.iter().map(|level| level.side).collect();
            ensure!(
                calibration.valid_from == valid_from && calibration.sides == traded,
                "its calibration was fitted on the pair of {:?} valid from {}, not on its {traded:?} valid from {valid_from}",
                calibration.sides,
                calibration.valid_from
            );
            Pair::new(models, calibration.lines)
        })
    }

    /// The valid-froms of the store of each model: of each side traded,
    /// whose pairs are the times every side has a model valid from
    /// (`Models::pairs`), and of the meta model, if any.
    ///
    /// # Errors
    ///
    /// Returns an error if a store cannot be read, the long's first.
    pub fn valid_froms(&self) -> anyhow::Result<Models<Vec<Timestamp>>> {
        let side = |side| {
            self.side(side)
                .map(|level| level.models.store.valid_froms())
                .transpose()
        };
        Ok(Models {
            long: side(Side::Long)?,
            short: side(Side::Short)?,
            meta: self
                .meta
                .as_ref()
                .map(|meta| meta.models.store.valid_froms())
                .transpose()?,
        })
    }
}

impl SideLevel {
    #[must_use]
    pub const fn side(&self) -> Side {
        self.side
    }

    /// The expected net P&L from which the side is traded
    /// (`ModelConfig::margins`).
    #[must_use]
    pub const fn margin(&self) -> f64 {
        self.margin
    }

    /// The store the side's models are published in.
    #[must_use]
    pub const fn store(&self) -> &ModelStore {
        &self.models.store
    }

    /// The names of the side model's features, in the order of its rows
    /// (`features::side_names`): the strategy's, then the target's payoff
    /// names.
    #[must_use]
    pub fn feature_names(&self) -> &[String] {
        &self.models.feature_names
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
        ensure!(
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

    /// The model of `side`.
    ///
    /// # Panics
    ///
    /// Panics if the pair's model set does not trade `side`.
    #[must_use]
    pub fn side(&self, side: Side) -> &SideModel {
        self.models
            .iter()
            .find(|(traded, _)| *traded == side)
            .map(|(_, model)| model)
            .unwrap_or_else(|| panic!("the pair holds no {} model", side.name()))
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
        &self.models.feature_names
    }

    /// The store the meta's models are published in.
    #[must_use]
    pub const fn store(&self) -> &ModelStore {
        &self.models.store
    }

    /// The meta's models as a strategy swaps them in, checked against the
    /// meta's feature names and the fingerprint; none in use yet.
    #[must_use]
    pub fn swap(&self) -> Swap<LoadedModel> {
        let models = self.models.clone();
        Swap::new(move |valid_from| models.read(valid_from, StoredModel::load))
    }
}

impl CheckedStore {
    /// The store's model valid from `valid_from`, loaded and checked
    /// against its feature names and fingerprint by `load`.
    ///
    /// # Errors
    ///
    /// Returns an error if there is none, its file cannot be read, or as
    /// `load` does.
    fn read<M>(&self, valid_from: Timestamp, load: Load<M>) -> anyhow::Result<M> {
        let stored = self.store.read(valid_from)?;
        load(&stored, &self.feature_names, &self.fingerprint)
    }
}

impl ModelStore {
    /// The store of the `side` models in `training`, the directory of
    /// their training settings: `long/` or `short/`, keyed by the `margins` of
    /// the sides, which decide the side picked on each row of the holdout and
    /// so what either side publishes.
    fn new(training: &Path, side: Side, margins: Margins) -> Self {
        Self {
            directory: training.join(side.name()).join(
                Fingerprint::default()
                    .with(margins_text(margins).as_bytes())
                    .to_string(),
            ),
        }
    }

    /// The store of the meta model `meta` in `training`, the directory of
    /// the training settings of its sides, which trade `margins`: beside
    /// the sides, under `meta/` and the meta's training settings, the
    /// sides' margins among them (they make the meta's rows).
    ///
    /// # Errors
    ///
    /// Returns an error if the meta's learner cannot be written as JSON.
    fn meta(training: &Path, meta: &MetaConfig, margins: Margins) -> anyhow::Result<Self> {
        let settings = meta_settings(meta)?.with(margins_text(margins).as_bytes());
        Ok(Self {
            directory: training.join(META).join(settings.to_string()),
        })
    }

    /// Publishes `model`, valid from its manifest's time, replacing the
    /// model valid from then, if any: written beside its file, then renamed
    /// onto it. The file written is this publish's own, named by the
    /// process and the publish, so publishers at once, in a backtest and a
    /// live node say, never write or rename each other's.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be written.
    pub fn publish(&self, model: &StoredModel) -> anyhow::Result<()> {
        fs::create_dir_all(&self.directory)
            .with_context(|| format!("creating the model store {}", self.directory.display()))?;
        let path = self.path(model.manifest.valid_from);
        let publish = PUBLISHES.fetch_add(1, Ordering::Relaxed);
        let written =
            path.with_extension(format!("{EXTENSION}.{}-{publish}.tmp", std::process::id()));
        fs::write(&written, serde_json::to_string_pretty(model)?)
            .and_then(|()| fs::rename(&written, &path))
            .with_context(|| format!("writing the model {}", path.display()))
    }

    /// The times the published models are valid from, in order; none if
    /// nothing was published.
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot be read.
    pub fn valid_froms(&self) -> anyhow::Result<Vec<Timestamp>> {
        let entries = match fs::read_dir(&self.directory) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(vec![]),
            entries => entries
                .with_context(|| format!("reading the model store {}", self.directory.display()))?,
        };
        let mut times = vec![];
        for entry in entries {
            let path = entry?.path();
            if path
                .extension()
                .is_some_and(|extension| extension == EXTENSION)
                && let Some(time) = path.file_stem().and_then(|stem| stem.to_str())
            {
                times.push(
                    time.parse()
                        .with_context(|| format!("naming the model {}", path.display()))?,
                );
            }
        }
        times.sort();
        Ok(times)
    }

    /// The model valid from `valid_from`.
    ///
    /// # Errors
    ///
    /// Returns an error if there is none, or its file cannot be read.
    pub fn read(&self, valid_from: Timestamp) -> anyhow::Result<StoredModel> {
        let path = self.path(valid_from);
        let json = fs::read_to_string(&path)
            .with_context(|| format!("reading the model {}", path.display()))?;
        serde_json::from_str(&json).with_context(|| format!("reading the model {}", path.display()))
    }

    fn path(&self, valid_from: Timestamp) -> PathBuf {
        self.directory.join(format!("{valid_from}.{EXTENSION}"))
    }

    /// A file of the store named by no time, so that listing it fails while
    /// its models can still be read.
    #[cfg(test)]
    fn unlistable(&self) -> PathBuf {
        self.directory.join(format!("no time.{EXTENSION}"))
    }

    /// Makes listing the store fail (`valid_froms`), until `mend_listing`.
    #[cfg(test)]
    pub(super) fn break_listing(&self) {
        fs::create_dir_all(&self.directory).unwrap();
        fs::write(self.unlistable(), "").unwrap();
    }

    /// Undoes `break_listing`.
    #[cfg(test)]
    pub(super) fn mend_listing(&self) {
        fs::remove_file(self.unlistable()).unwrap();
    }

    /// Makes publishing to the store fail (`publish`), listing it still finding
    /// nothing: its directory a link to nowhere.
    #[cfg(test)]
    pub(super) fn break_publishing(&self) {
        fs::create_dir_all(self.directory.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(self.directory.with_extension("nowhere"), &self.directory)
            .unwrap();
    }
}

/// The directory, under `root`, of the stores of `strategy_id` whose
/// dataset has `fingerprint` (`dataset::fingerprint`): its id, then the
/// fingerprint.
fn dataset_directory(root: &Path, strategy_id: StrategyId, fingerprint: Fingerprint) -> PathBuf {
    root.join(strategy_id.as_str())
        .join(fingerprint.to_string())
}

/// The directory, under `root`, of the models of `strategy_id` whose
/// dataset has `fingerprint`, its side models trained as `config` says,
/// of the store `version` (`VERSION`): its dataset's, then the side
/// models' training settings, the learner and its settings, the retrain
/// interval, the window, the holdout and the minimums of its
/// holdout, and the version.
fn training_directory(
    root: &Path,
    strategy_id: StrategyId,
    fingerprint: Fingerprint,
    config: &ModelConfig,
    version: u32,
) -> anyhow::Result<PathBuf> {
    let settings = Fingerprint::default()
        .with(serde_json::to_string(&config.learner)?.as_bytes())
        .with(retrain_setting(config.retrain).as_bytes())
        .with(&config.window.as_nanos().to_le_bytes())
        .with(&config.holdout.as_nanos().to_le_bytes())
        .with(&config.min_pnl.to_le_bytes())
        .with(&config.min_trades.to_le_bytes())
        .with(format!("store version {version}").as_bytes());
    Ok(dataset_directory(root, strategy_id, fingerprint).join(settings.to_string()))
}

/// The retrain `retrain` as the training settings a store is keyed by
/// hold it, its interval or never: a strategy retrained at another
/// interval, or never, publishes to and reads from another store, so a
/// weekly-retrained strategy never trades on a model a daily-retrained one
/// published between its retrains, nor a strategy never retrained on a
/// retrained one's.
fn retrain_setting(retrain: Retrain) -> String {
    match retrain {
        Retrain::Every(every) => format!("retrain every {}", every.as_nanos()),
        Retrain::Never => "retrain never".to_string(),
    }
}

/// The margins `margins` as the keys of the stores hold them.
fn margins_text(margins: Margins) -> String {
    let Margins { long, short } = margins;
    format!("long {long:?} short {short:?}")
}

/// The training settings of `meta`: its learner, window, holdout, and
/// the margin deciding the trades its holdout is judged on, and the
/// minimums of its holdout.
fn meta_settings(meta: &MetaConfig) -> anyhow::Result<Fingerprint> {
    Ok(Fingerprint::default()
        .with(serde_json::to_string(&meta.learner)?.as_bytes())
        .with(&meta.window.as_nanos().to_le_bytes())
        .with(&meta.holdout.as_nanos().to_le_bytes())
        .with(&meta.margin.to_le_bytes())
        .with(&meta.min_pnl.to_le_bytes())
        .with(&meta.min_trades.to_le_bytes()))
}

/// How a level's model is loaded from its file and checked against a
/// strategy of feature names and a fingerprint, as a model `M`: a side's
/// with its pair's calibration (`StoredModel::load_side`), a meta's with its
/// own (`StoredModel::load`).
type Load<M> = fn(&StoredModel, &[String], &str) -> anyhow::Result<M>;

/// The models of a level as a strategy swaps them in, decision by decision
/// in time order: at each, the latest valid then, read from its stores as an
/// `M` and checked against the strategy before use, the meta's model or the
/// sides' pair. One failing a check is refused, logged as an error, and the
/// current one kept; it is not read again. A model set hands it out
/// (`ModelSet::pairs`, `MetaLevel::swap`).
pub struct Swap<M> {
    /// The model valid from a time, read and checked.
    read_at: Box<dyn Fn(Timestamp) -> anyhow::Result<M>>,
    /// The model in use, and its valid-from.
    current: Option<(Timestamp, M)>,
    /// The valid-from of the last model read, in use or refused.
    read: Option<Timestamp>,
}

impl<M> Swap<M> {
    /// Swaps in the models `read_at` reads and checks, by their valid-from;
    /// none in use yet.
    fn new(read_at: impl Fn(Timestamp) -> anyhow::Result<M> + 'static) -> Self {
        Self {
            read_at: Box::new(read_at),
            current: None,
            read: None,
        }
    }

    /// The model in use at `now`, and its valid-from, among the models
    /// valid from `valid_froms`, the stores' list in order: the latest
    /// valid then, swapped in if not read yet, unless it fails its checks.
    pub fn at(&mut self, now: Timestamp, valid_froms: &[Timestamp]) -> Option<(Timestamp, &M)> {
        let latest = valid_froms.iter().rfind(|&&time| time <= now).copied();
        if let Some(latest) = latest.filter(|&latest| self.read != Some(latest)) {
            self.read = Some(latest);
            match (self.read_at)(latest) {
                Ok(model) => {
                    log::info!("the model valid from {latest} in use from {now}");
                    self.current = Some((latest, model));
                }
                Err(e) => log::error!(
                    "refused the model valid from {latest}: {e:#}; the current model kept"
                ),
            }
        }
        self.current
            .as_ref()
            .map(|(valid_from, model)| (*valid_from, model))
    }
}

/// A swap reads as the model in use and the valid-from last read.
impl<M: fmt::Debug> fmt::Debug for Swap<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Swap")
            .field("current", &self.current)
            .field("read", &self.read)
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

    pub fn serialize<S: Serializer>(row: &[f64], serializer: S) -> Result<S::Ok, S::Error> {
        row.iter()
            .map(|&value| Value::from(value))
            .collect::<Vec<_>>()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<f64>, D::Error> {
        Vec::<Value>::deserialize(deserializer)?
            .into_iter()
            .map(Value::read)
            .collect()
    }

    /// One value as JSON, as a row's are.
    pub mod one {
        use serde::{Deserialize, Deserializer, Serialize, Serializer};

        use super::Value;

        pub fn serialize<S: Serializer>(value: &f64, serializer: S) -> Result<S::Ok, S::Error> {
            Value::from(*value).serialize(serializer)
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
            Value::deserialize(deserializer)?.read()
        }
    }
}

#[cfg(test)]
mod tests {
    use jiff::SignedDuration;
    use nautilus_model::{
        identifiers::InstrumentId,
        types::{Money, Quantity},
    };
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        backtest::Commission,
        model::{
            calibration::Line,
            decide::DecisionRule,
            deciding::testing::HOLDOUT,
            features::testing::last_mid,
            learner::{LearnerConfig, TrainingSet},
            targets::Target,
        },
    };

    const GOLD: &str = "XAUUSD.ICMARKETS";
    const STRATEGY: &str = "STORE-001";

    fn features() -> std::rc::Rc<FeatureSet> {
        last_mid(InstrumentId::from(GOLD)).build().unwrap()
    }

    fn config() -> ModelConfig {
        ModelConfig::builder()
            .decision(DecisionRule::Every(SignedDuration::from_hours(1)))
            .target(
                TargetConfig::builder()
                    .instrument_id(InstrumentId::from(GOLD))
                    .quantity(Quantity::from(1))
                    .spec(
                        Target::builder()
                            .limit(SignedDuration::from_hours(1))
                            .atr(crate::model::targets::DEFAULT_ATR)
                            .build(),
                    )
                    .build(),
            )
            .retrain(SignedDuration::from_hours(24))
            .window(SignedDuration::from_hours(48))
            .holdout(SignedDuration::from_hours(12))
            .margins(Margins {
                long: Some(0.5),
                short: Some(0.5),
            })
            .min_pnl(1.0)
            .min_trades(30)
            .learner(LearnerConfig::Fake {})
            .build()
    }

    fn store(root: &Path, config: &ModelConfig) -> ModelStore {
        store_net_of(root, config, &venue())
    }

    fn store_net_of(root: &Path, config: &ModelConfig, venue: &DatasetVenue) -> ModelStore {
        side_store(root, config, venue, Side::Long)
    }

    /// The models under `root` of `config`, run on `venue`.
    fn models(root: &Path, config: &ModelConfig, venue: &DatasetVenue) -> ModelSet {
        ModelSet::new(
            root,
            StrategyId::from(STRATEGY),
            &features(),
            config,
            &requirements(),
            venue,
        )
        .unwrap()
    }

    /// The store of the `side` models of `config`, run on `venue`.
    fn side_store(
        root: &Path,
        config: &ModelConfig,
        venue: &DatasetVenue,
        side: Side,
    ) -> ModelStore {
        models(root, config, venue)
            .side(side)
            .unwrap()
            .store()
            .clone()
    }

    /// What the strategy reads: the quotes of gold.
    fn requirements() -> DataRequirements {
        DataRequirements {
            bar_types: vec![],
            ticks: vec![crate::strategy::TickSeries {
                data: nautilus_model::data::NautilusDataType::QuoteTick,
                instrument_id: InstrumentId::from(GOLD),
            }],
            warmup: SignedDuration::ZERO,
        }
    }

    /// The venue of the dataset runs: a commission rate of 30 per million,
    /// 10 million USD, no leverage.
    fn venue() -> DatasetVenue {
        DatasetVenue {
            commission: Commission::Rate(Decimal::new(3, 5)),
            balance: Money::from("10000000 USD"),
            leverage: Decimal::ONE,
        }
    }

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    /// The calibration of a pair of both sides valid from `valid_from`, its
    /// level line `0.1 + 0.5 C` and its side call's slope 1.5.
    fn pair_calibration(valid_from: &str) -> ModelCalibration {
        ModelCalibration::Pair(PairCalibration {
            valid_from: at(valid_from),
            sides: vec![Side::Long, Side::Short],
            lines: Ksplit {
                level: Line {
                    alpha: 0.1,
                    beta: 0.5,
                },
                side_call: Some(1.5),
            },
        })
    }

    /// A fake long model valid from `valid_from`, of the pair of both sides
    /// valid then (`pair_calibration`), on a feature `mid`, probed on
    /// `rows`, its baseline the mean of its training labels, 1/2.
    fn model(valid_from: &str, rows: &[Vec<f64>]) -> StoredModel {
        let set = TrainingSet::new(1, vec![0.2, 0.7], vec![0.0, 1.0]).unwrap();
        let fitted = LearnerConfig::Fake {}.fit(&set).unwrap();
        StoredModel {
            manifest: ModelManifest {
                strategy_id: StrategyId::from(STRATEGY),
                valid_from: at(valid_from),
                fingerprint: "f".repeat(64),
                feature_names: vec!["mid".to_string()],
                target: config().target,
                side: Some(Side::Long),
                learner: LearnerConfig::Fake {},
                calibration: pair_calibration(valid_from),
                baseline: 0.5,
                holdout: HOLDOUT,
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

    #[test]
    fn a_published_model_reads_back_whole() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path(), &config());
        let published = model("2026-10-22T00:00:00Z", &[vec![0.3], vec![0.1 + 0.2]]);

        store.publish(&published).unwrap();

        assert_eq!(store.read(at("2026-10-22T00:00:00Z")).unwrap(), published);
    }

    #[test]
    fn missing_and_infinite_probe_values_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path(), &config());
        let rows = [
            vec![f64::NAN],
            vec![f64::INFINITY],
            vec![f64::NEG_INFINITY],
            vec![-0.0],
        ];
        store
            .publish(&model("2026-10-22T00:00:00Z", &rows))
            .unwrap();

        let read = store.read(at("2026-10-22T00:00:00Z")).unwrap();

        let bits = |rows: Vec<Vec<f64>>| -> Vec<u64> {
            rows.into_iter().flatten().map(f64::to_bits).collect()
        };
        assert_eq!(
            bits(read.manifest.probes.into_iter().map(|p| p.row).collect()),
            bits(rows.to_vec())
        );
    }

    #[test]
    fn a_store_lists_the_times_its_models_are_valid_from_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path(), &config());
        assert!(store.valid_froms().unwrap().is_empty());

        for time in ["2026-10-23T00:00:00Z", "2026-10-21T12:00:00Z"] {
            store.publish(&model(time, &[vec![0.5]])).unwrap();
        }

        assert_eq!(
            store.valid_froms().unwrap(),
            [at("2026-10-21T12:00:00Z"), at("2026-10-23T00:00:00Z")]
        );
    }

    #[test]
    fn a_model_valid_from_a_fraction_of_a_second_lists_and_reads_back() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path(), &config());
        let published = model("2026-10-22T04:00:00.01Z", &[vec![0.5]]);

        store.publish(&published).unwrap();

        assert_eq!(
            store.valid_froms().unwrap(),
            [at("2026-10-22T04:00:00.01Z")]
        );
        assert_eq!(
            store.read(at("2026-10-22T04:00:00.01Z")).unwrap(),
            published
        );
    }

    /// Two publishers of a model at the same time, say a backtest and a
    /// live node's retrain, each write their own file before the rename:
    /// neither fails, and the model reads back whole.
    #[test]
    fn publishers_at_the_same_time_at_once_each_publish_a_whole_model() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path(), &config());
        let published = model("2026-10-22T00:00:00Z", &[vec![0.5]]);

        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..50 {
                        store.publish(&published).unwrap();
                    }
                });
            }
        });

        assert_eq!(store.read(at("2026-10-22T00:00:00Z")).unwrap(), published);
    }

    #[test]
    fn publishing_at_the_same_time_replaces_the_model() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path(), &config());
        store
            .publish(&model("2026-10-22T00:00:00Z", &[vec![0.5]]))
            .unwrap();
        let second = model("2026-10-22T00:00:00Z", &[vec![0.9]]);

        store.publish(&second).unwrap();

        assert_eq!(store.valid_froms().unwrap().len(), 1);
        assert_eq!(store.read(at("2026-10-22T00:00:00Z")).unwrap(), second);
    }

    #[test]
    fn other_training_settings_target_minimums_or_margin_list_none_of_the_models() {
        let dir = tempfile::tempdir().unwrap();
        store(dir.path(), &config())
            .publish(&model("2026-10-22T00:00:00Z", &[vec![0.5]]))
            .unwrap();
        let mut target = config().target;
        target.spec.limit = SignedDuration::from_mins(30);
        let others = [
            ModelConfig {
                learner: LearnerConfig::Baseline {},
                ..config()
            },
            ModelConfig {
                window: SignedDuration::from_hours(72),
                ..config()
            },
            ModelConfig {
                holdout: SignedDuration::from_hours(6),
                ..config()
            },
            ModelConfig { target, ..config() },
        ];

        for other in others {
            assert!(
                store(dir.path(), &other).valid_froms().unwrap().is_empty(),
                "{other:?}"
            );
        }
        let held_to_more = [
            ModelConfig {
                min_pnl: 2.0,
                ..config()
            },
            ModelConfig {
                min_trades: 50,
                ..config()
            },
            ModelConfig {
                margins: Margins {
                    long: Some(0.75),
                    short: Some(0.5),
                },
                ..config()
            },
        ];
        for other in held_to_more {
            assert!(
                store(dir.path(), &other).valid_froms().unwrap().is_empty(),
                "{other:?}"
            );
        }
    }

    /// Bumping the store version is another training directory, which
    /// every store of the strategy sits under, its sides' and its meta's:
    /// no model of another manifest is read, and every model is trained
    /// again.
    #[test]
    fn bumping_the_store_version_keys_another_store() {
        let config = config();
        let fingerprint = dataset::fingerprint(
            &features(),
            config.decision,
            &config.target,
            &requirements(),
            &venue(),
        )
        .unwrap();
        let at = |version| {
            training_directory(
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

    /// One side's margin changes which side is picked on a row, so what the
    /// other publishes: a side's models list only beside the same margins of
    /// both sides, a side not traded being another margin.
    #[test]
    fn either_sides_margin_keys_both_sides_stores() {
        let dir = tempfile::tempdir().unwrap();
        for side in Side::BOTH {
            side_store(dir.path(), &config(), &venue(), side)
                .publish(&model("2026-10-22T00:00:00Z", &[vec![0.5]]))
                .unwrap();
        }
        let margins = |long, short| ModelConfig {
            margins: Margins { long, short },
            ..config()
        };

        for (side, other) in [
            (Side::Long, margins(Some(0.5), Some(0.75))),
            (Side::Long, margins(Some(0.5), None)),
            (Side::Short, margins(Some(0.75), Some(0.5))),
            (Side::Short, margins(None, Some(0.5))),
        ] {
            assert!(
                side_store(dir.path(), &other, &venue(), side)
                    .valid_froms()
                    .unwrap()
                    .is_empty(),
                "{side:?}: {:?}",
                other.margins
            );
        }
    }

    /// The store of the meta model `meta` of `config`.
    fn meta_store(root: &Path, config: &ModelConfig, meta: &MetaConfig) -> ModelStore {
        let config = ModelConfig {
            meta: Some(meta.clone()),
            ..config.clone()
        };
        models(root, &config, &venue())
            .meta()
            .unwrap()
            .store()
            .clone()
    }

    /// A meta model published under one margin and minimums may fall
    /// short of others, and the sides' margins make its rows: a meta of
    /// another margin, minimums or sides' margins lists none of them.
    #[test]
    fn a_meta_model_of_another_margin_minimums_or_sides_margins_is_not_listed() {
        let dir = tempfile::tempdir().unwrap();
        let meta = |margin, min_pnl| {
            MetaConfig::builder()
                .window(SignedDuration::from_hours(48))
                .holdout(SignedDuration::from_hours(12))
                .margin(margin)
                .min_pnl(min_pnl)
                .min_trades(20)
                .learner(LearnerConfig::Fake {})
                .build()
        };
        meta_store(dir.path(), &config(), &meta(0.55, 1.0))
            .publish(&model("2026-10-22T00:00:00Z", &[vec![0.5]]))
            .unwrap();
        let long_only = ModelConfig {
            margins: Margins {
                long: Some(0.5),
                short: None,
            },
            ..config()
        };

        for (config, meta) in [
            (config(), meta(0.55, 2.0)),
            (config(), meta(0.6, 1.0)),
            (long_only, meta(0.55, 1.0)),
        ] {
            assert!(
                meta_store(dir.path(), &config, &meta)
                    .valid_froms()
                    .unwrap()
                    .is_empty(),
                "{config:?}"
            );
        }
    }

    /// A meta model taking the trades of the side picked from an expected
    /// net P&L of 0.55 USD.
    fn meta_config() -> MetaConfig {
        MetaConfig::builder()
            .window(SignedDuration::from_hours(48))
            .holdout(SignedDuration::from_hours(12))
            .margin(0.55)
            .min_pnl(1.0)
            .min_trades(20)
            .learner(LearnerConfig::Fake {})
            .build()
    }

    /// `config()`, retrained as `retrain` says.
    fn retrained(retrain: Retrain) -> ModelConfig {
        ModelConfig {
            retrain,
            ..config()
        }
    }

    /// A weekly-retrained strategy trades on the models of its own
    /// retrains, never on one a daily-retrained strategy published between
    /// them: a store of either retrain lists its own models alone, a
    /// side's and a meta's alike.
    #[test]
    fn a_store_lists_only_the_models_of_its_retrain_interval() {
        let daily = Retrain::Every(SignedDuration::from_hours(24));
        let weekly = Retrain::Every(SignedDuration::from_hours(168));

        for (kind, store) in stores() {
            let dir = tempfile::tempdir().unwrap();
            store(dir.path(), daily)
                .publish(&model("2026-10-23T00:00:00Z", &[vec![0.5]]))
                .unwrap();
            store(dir.path(), weekly)
                .publish(&model("2026-10-22T00:00:00Z", &[vec![0.5]]))
                .unwrap();

            assert_eq!(
                store(dir.path(), daily).valid_froms().unwrap(),
                [at("2026-10-23T00:00:00Z")],
                "{kind} daily store"
            );
            assert_eq!(
                store(dir.path(), weekly).valid_froms().unwrap(),
                [at("2026-10-22T00:00:00Z")],
                "{kind} weekly store"
            );
        }
    }

    /// The store under a root of a strategy retrained as a `Retrain` says.
    type StoreOf = fn(&Path, Retrain) -> ModelStore;

    /// Each kind of store, of a strategy retrained as a `Retrain` says: a
    /// side's and a meta's.
    fn stores() -> [(&'static str, StoreOf); 2] {
        [
            ("a side's", |root, retrain| store(root, &retrained(retrain))),
            ("a meta's", |root, retrain| {
                meta_store(root, &retrained(retrain), &meta_config())
            }),
        ]
    }

    /// A strategy never retrained trades on the models trained for it
    /// alone: its store lists none a retrained one published, nor does a
    /// retrained one's list its, of each kind of store.
    #[test]
    fn a_store_never_retrained_and_a_retrained_one_list_none_of_each_others_models() {
        let daily = Retrain::Every(SignedDuration::from_hours(24));

        for (kind, store) in stores() {
            let dir = tempfile::tempdir().unwrap();
            store(dir.path(), daily)
                .publish(&model("2026-10-23T00:00:00Z", &[vec![0.5]]))
                .unwrap();
            store(dir.path(), Retrain::Never)
                .publish(&model("2026-10-22T00:00:00Z", &[vec![0.5]]))
                .unwrap();

            assert_eq!(
                store(dir.path(), Retrain::Never).valid_froms().unwrap(),
                [at("2026-10-22T00:00:00Z")],
                "{kind} store never retrained"
            );
            assert_eq!(
                store(dir.path(), daily).valid_froms().unwrap(),
                [at("2026-10-23T00:00:00Z")],
                "{kind} daily store"
            );
        }
    }

    #[test]
    fn the_long_and_the_short_models_are_listed_apart() {
        let dir = tempfile::tempdir().unwrap();
        side_store(dir.path(), &config(), &venue(), Side::Long)
            .publish(&model("2026-10-22T00:00:00Z", &[vec![0.5]]))
            .unwrap();

        let short = side_store(dir.path(), &config(), &venue(), Side::Short);

        assert!(short.valid_froms().unwrap().is_empty());
    }

    /// A meta model's every prediction of R is calibrated by its level line,
    /// and its probes are its predictions before it: the fake predicts the
    /// first feature, 0.25, calibrated by the line `0.1 + 0.5 E` to an
    /// expected R of 0.225.
    #[test]
    fn a_loaded_meta_model_predicts_through_its_calibration() {
        let mut stored = model("2026-10-22T00:00:00Z", &[vec![0.75]]);
        stored.manifest.side = None;
        stored.manifest.calibration = ModelCalibration::Meta(Line {
            alpha: 0.1,
            beta: 0.5,
        });

        let loaded = stored
            .load(&stored.manifest.feature_names, &stored.manifest.fingerprint)
            .unwrap();

        assert_eq!(stored.manifest.probes[0].prediction, 0.75);
        let prediction = loaded.predict(&[0.25]).unwrap();
        assert!((prediction - 0.225).abs() < 1e-12, "{prediction}");
    }

    /// A side's model has its pair's calibration and a meta's its own level
    /// line: each loaded as the other's level is refused.
    #[test]
    fn a_model_is_refused_at_the_other_levels_load() {
        let side = model("2026-10-22T00:00:00Z", &[]);
        let mut meta = side.clone();
        meta.manifest.calibration = ModelCalibration::Meta(Line {
            alpha: 0.0,
            beta: 1.0,
        });
        let (names, fingerprint) = (&side.manifest.feature_names, &side.manifest.fingerprint);

        assert!(side.load(names, fingerprint).is_err());
        assert!(meta.load_side(names, fingerprint).is_err());
    }

    /// The fake long and short of a pair valid from midnight, on a feature
    /// `mid`, then the entry cost of the time-only target, each predicting
    /// its row's mid: the long's 0.4 and the short's 0.1 are C = 0.25 and D =
    /// 0.3, calibrated by the pair's line `0.1 + 0.5 C` and slope 1.5 to C' =
    /// 0.225 and D' = 0.45, the long expecting 0.45R and the short nought.
    #[test]
    fn a_pair_expects_each_sides_r_through_its_calibration() {
        let dir = tempfile::tempdir().unwrap();
        let models = models(dir.path(), &config(), &venue());
        let fingerprint = dataset::fingerprint(
            &features(),
            config().decision,
            &config().target,
            &requirements(),
            &venue(),
        )
        .unwrap()
        .to_string();
        let names = models.side(Side::Long).unwrap().feature_names().to_vec();
        for side in Side::BOTH {
            let mut stored = model("2026-10-22T00:00:00Z", &[]);
            stored.manifest.side = Some(side);
            stored.manifest.fingerprint.clone_from(&fingerprint);
            stored.manifest.feature_names.clone_from(&names);
            let set = TrainingSet::new(2, vec![0.2, 0.0], vec![0.0]).unwrap();
            stored.model = LearnerConfig::Fake {}.fit(&set).unwrap().save().unwrap();
            models.side(side).unwrap().store().publish(&stored).unwrap();
        }
        let mut pairs = models.pairs();
        let midnight = at("2026-10-22T00:00:00Z");

        let (_, pair) = pairs.at(midnight, &[midnight]).unwrap();
        let expected = pair
            .expect(&PerSide {
                long: SideRow::new(&[0.4], &[0.02], 1.0),
                short: SideRow::new(&[0.1], &[0.02], 1.0),
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

    /// The manifests of earlier store versions are no models of this one: a
    /// side's of Platt's calibration and a payoff record, and a meta's of
    /// Platt's calibration and its probabilities' log loss and AUC. Read,
    /// each is refused, naming its file.
    #[test]
    fn a_model_of_an_earlier_manifest_is_refused_naming_its_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = store(dir.path(), &config());
        let current = serde_json::to_value(model("2026-10-22T00:00:00Z", &[])).unwrap();
        let mut side = current.clone();
        let manifest = side["manifest"].as_object_mut().unwrap();
        manifest.insert(
            "calibration".to_string(),
            serde_json::json!({"a": -0.9, "b": 0.0}),
        );
        manifest.insert(
            "payoff".to_string(),
            serde_json::json!({"win": 1.5, "loss": 1.25}),
        );
        let mut meta = current;
        let manifest = meta["manifest"].as_object_mut().unwrap();
        manifest.insert("side".to_string(), serde_json::Value::Null);
        manifest.insert(
            "calibration".to_string(),
            serde_json::json!({"platt": {"a": -0.9, "b": 0.0}}),
        );
        manifest["holdout"]["score"] = serde_json::json!({"probability": {
            "log_loss": 0.6,
            "baseline_log_loss": 0.69,
            "auc": 0.6
        }});
        std::fs::create_dir_all(&store.directory).unwrap();

        for (earlier, valid_from) in [
            (side, "2026-10-22T00:00:00Z"),
            (meta, "2026-10-23T00:00:00Z"),
        ] {
            let path = store.path(at(valid_from));
            std::fs::write(&path, earlier.to_string()).unwrap();

            let refused = store.read(at(valid_from)).unwrap_err();

            let message = format!("{refused:#}");
            assert!(message.contains(&path.display().to_string()), "{message}");
        }
    }

    /// A model learned targets net of its dataset run's commission: one of
    /// other costs is another model.
    #[test]
    fn another_commission_lists_none_of_the_models() {
        let dir = tempfile::tempdir().unwrap();
        store(dir.path(), &config())
            .publish(&model("2026-10-22T00:00:00Z", &[vec![0.5]]))
            .unwrap();

        for other in [
            Commission::Rate(Decimal::new(4, 5)),
            Commission::PerOrder(Money::from("3 USD")),
            Commission::PerQuantity(Money::from_str_exact("0.00003 USD").unwrap()),
        ] {
            assert!(
                store_net_of(
                    dir.path(),
                    &config(),
                    &DatasetVenue {
                        commission: other,
                        ..venue()
                    }
                )
                .valid_froms()
                .unwrap()
                .is_empty(),
                "{other:?}"
            );
        }
    }
}
