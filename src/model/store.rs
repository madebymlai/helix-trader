//! The model store: the models published for a strategy, each valid from
//! the time it was trained at.
//!
//! A store is a directory per strategy id, fingerprint (that of its
//! dataset: the features, the decision rule, the targets and the
//! venue settings of its dataset runs, see `dataset::fingerprint`) and training
//! settings (the learner and its settings, the label, the window, the
//! holdout and the minimums its holdout must meet to be published,
//! `holdout::Minimums`), and side, the long or the short, keyed by its
//! margin, which decides the trades its holdout is judged on, so a model
//! is only ever listed beside models of the same features, costs,
//! training, side and margin. Backtest and live read the same list.
//!
//! A meta model's store sits in a `meta/` directory beside its primary's
//! sides, under their training settings, keyed by its own (its learner and
//! settings, window, holdout, threshold and minimums) and the primary's
//! margins, which make its rows; a rule primary has no store of its own,
//! its meta's `meta/` sits under the strategy id and fingerprint. A meta model
//! is only ever listed beside meta models of the same primary.
//!
//! `Swap` is how the models are used: at each decision, in time order,
//! the latest valid then, checked before use, a model failing a check
//! refused and the current one kept.
//!
//! Each model is one file, `<valid from>.json` (an RFC 3339 time): its
//! manifest, the schema (the feature names in order, the fingerprint), the
//! target spec, label and side, the learner, the calibration of its
//! predictions, a side's expected payoff, its holdout (`holdout::Holdout`)
//! and probe rows with the calibrated predictions the model made on them
//! when fitted, and the model as its adapter saved it. A loaded model's every prediction is
//! calibrated (`LoadedModel`). A reloaded model is checked by predicting
//! the probes, never by its bytes. The file is written beside its name, then
//! renamed onto it, so a reader sees a whole model or none. A missing
//! feature, NaN, has no JSON number: a probe row writes NaN and the
//! infinities as strings.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, ensure};
use jiff::Timestamp;
use nautilus_model::identifiers::StrategyId;
use serde::{Deserialize, Serialize};

use super::{
    calibration::Calibration,
    config::{Margins, MetaConfig, ModelConfig, RuleConfig},
    dataset::{self, DatasetVenue},
    decide::DecisionRule,
    features::{FeatureSet, Fingerprint},
    holdout::Holdout,
    learner::{LearnerConfig, Model, SavedModel},
    payoff::Payoff,
    targets::{Label, Side, TargetsConfig},
};
use crate::strategy::DataRequirements;

/// The extension of a model's file.
const EXTENSION: &str = "json";

/// The publishes of this process so far, which tell its temporary files
/// apart.
static PUBLISHES: AtomicU64 = AtomicU64::new(0);

/// The directory of the meta models within the directory of their primary's
/// models, or of a rule primary's dataset.
const META: &str = "meta";

/// The models of one strategy, fingerprint and training settings.
#[derive(Debug, Clone)]
pub struct ModelStore {
    directory: PathBuf,
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
    /// The fingerprint of the features, the decision rule, the targets and
    /// the venue of its dataset runs.
    pub fingerprint: String,
    /// The names of the features, in the order of a row's values.
    pub feature_names: Vec<String>,
    /// The target spec.
    pub targets: TargetsConfig,
    /// The target the model learned.
    pub label: Label,
    /// The side of the target a primary's model learned; none for a meta
    /// model, which learns the primary's.
    pub side: Option<Side>,
    pub learner: LearnerConfig,
    /// The calibration of the model's predictions.
    pub calibration: Calibration,
    /// A primary's expected win and loss of a trade of its side; none for
    /// a meta model.
    pub payoff: Option<Payoff>,
    /// The model's holdout, as its publication read it.
    pub holdout: Holdout,
    /// Rows of the holdout and the model's calibrated predictions on them.
    pub probes: Vec<Probe>,
}

/// A row and the prediction of a model on it when it was fitted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Probe {
    #[serde(with = "values")]
    pub row: Vec<f64>,
    pub prediction: f64,
}

impl StoredModel {
    /// The model, loaded by its manifest's learner with its calibration
    /// and checked against a strategy of `feature_names` and
    /// `fingerprint`: the manifest's feature names in order and
    /// fingerprint are the strategy's, the model takes as many features,
    /// and it predicts each probe to the bit as when fitted.
    ///
    /// # Errors
    ///
    /// Returns an error if the model cannot be loaded or fails a check.
    pub fn load(&self, feature_names: &[String], fingerprint: &str) -> anyhow::Result<LoadedModel> {
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
        let model = LoadedModel {
            model: manifest.learner.load(&self.model)?,
            calibration: manifest.calibration,
            payoff: manifest.payoff,
        };
        let width = model.model.width();
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

/// A model as a strategy and a score use it: loaded, every prediction
/// calibrated, and a primary's expected payoff.
#[derive(Debug)]
pub struct LoadedModel {
    model: Model,
    calibration: Calibration,
    payoff: Option<Payoff>,
}

impl LoadedModel {
    /// The model `model` fitted, its predictions calibrated by
    /// `calibration`, a primary's of `payoff`.
    #[must_use]
    pub const fn new(model: Model, calibration: Calibration, payoff: Option<Payoff>) -> Self {
        Self {
            model,
            calibration,
            payoff,
        }
    }

    /// The calibrated probability that the label of `row` is true.
    ///
    /// # Errors
    ///
    /// As `Model::predict`.
    pub fn predict(&self, row: &[f64]) -> anyhow::Result<f64> {
        Ok(self.calibration.apply(self.model.predict(row)?))
    }

    /// A primary's expected win and loss of a trade of its side.
    #[must_use]
    pub const fn payoff(&self) -> Option<Payoff> {
        self.payoff
    }
}

impl ModelStore {
    /// The store, under `root`, of the `side` models of `strategy_id`
    /// deciding with `features` as `config` says, on targets for a strategy
    /// of `requirements`, run on `venue`, and trained as `config`
    /// says: `long/` or `short/` in the directory of its training settings,
    /// keyed by the side's margin, which decides the trades its holdout
    /// is judged on.
    ///
    /// # Errors
    ///
    /// Returns an error if the margins do not trade `side`, the learner
    /// cannot be written as JSON, or as `dataset::fingerprint` does.
    pub fn new(
        root: &Path,
        strategy_id: StrategyId,
        features: &FeatureSet,
        config: &ModelConfig,
        requirements: &DataRequirements,
        venue: &DatasetVenue,
        side: Side,
    ) -> anyhow::Result<Self> {
        let margin = config
            .margins
            .of(side)
            .with_context(|| format!("the margins trade no {}", side.name()))?;
        Ok(Self {
            directory: primary_directory(root, strategy_id, features, config, requirements, venue)?
                .join(side.name())
                .join(
                    Fingerprint::default()
                        .with(&margin.to_le_bytes())
                        .to_string(),
                ),
        })
    }

    /// The store, under `root`, of the meta model `meta` of the model
    /// primary of `strategy_id` deciding with `features` as `config` says,
    /// on targets for a strategy of `requirements`, run on `venue`:
    /// beside the primary's sides, under `meta/` and the meta's training
    /// settings, the primary's margins among them (they make the meta's
    /// rows).
    ///
    /// # Errors
    ///
    /// Returns an error if a learner cannot be written as JSON, or as
    /// `dataset::fingerprint` does.
    pub fn meta(
        root: &Path,
        strategy_id: StrategyId,
        features: &FeatureSet,
        config: &ModelConfig,
        meta: &MetaConfig,
        requirements: &DataRequirements,
        venue: &DatasetVenue,
    ) -> anyhow::Result<Self> {
        let Margins { long, short } = config.margins;
        let settings =
            meta_settings(meta)?.with(format!("long {long:?} short {short:?}").as_bytes());
        Ok(Self {
            directory: primary_directory(root, strategy_id, features, config, requirements, venue)?
                .join(META)
                .join(settings.to_string()),
        })
    }

    /// The store, under `root`, of the meta models of `strategy_id`, whose
    /// primary is its rule, deciding with `features` as `config` says, on
    /// targets for a strategy of `requirements`, run on `venue`.
    ///
    /// # Errors
    ///
    /// Returns an error if the learner cannot be written as JSON, or as
    /// `dataset::fingerprint` does.
    pub fn of_rule(
        root: &Path,
        strategy_id: StrategyId,
        features: &FeatureSet,
        config: &RuleConfig,
        requirements: &DataRequirements,
        venue: &DatasetVenue,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            directory: dataset_directory(
                root,
                strategy_id,
                features,
                config.decision,
                &config.targets,
                requirements,
                venue,
            )?
            .join(META)
            .join(meta_settings(&config.meta)?.to_string()),
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
}

/// The directory, under `root`, of the stores of `strategy_id` deciding
/// with `features` on `rule`, of `targets` for a strategy of `requirements`,
/// run on `venue`: its id and its dataset's fingerprint.
fn dataset_directory(
    root: &Path,
    strategy_id: StrategyId,
    features: &FeatureSet,
    rule: DecisionRule,
    targets: &TargetsConfig,
    requirements: &DataRequirements,
    venue: &DatasetVenue,
) -> anyhow::Result<PathBuf> {
    let fingerprint = dataset::fingerprint(features, rule, targets, requirements, venue)?;
    Ok(root
        .join(strategy_id.as_str())
        .join(fingerprint.to_string()))
}

/// The directory, under `root`, of the models of the model primary of
/// `strategy_id` deciding with `features` as `config` says, on targets for
/// a strategy of `requirements`, run on `venue`: its dataset's, then
/// its training settings, the learner and its settings, the label, the
/// window, the holdout and the minimums of its holdout.
fn primary_directory(
    root: &Path,
    strategy_id: StrategyId,
    features: &FeatureSet,
    config: &ModelConfig,
    requirements: &DataRequirements,
    venue: &DatasetVenue,
) -> anyhow::Result<PathBuf> {
    let settings = Fingerprint::default()
        .with(serde_json::to_string(&config.learner)?.as_bytes())
        .with(format!("{:?}", config.label).as_bytes())
        .with(&config.window.as_nanos().to_le_bytes())
        .with(&config.holdout.as_nanos().to_le_bytes())
        .with(&config.min_pnl.to_le_bytes())
        .with(&config.min_trades.to_le_bytes());
    Ok(dataset_directory(
        root,
        strategy_id,
        features,
        config.decision,
        &config.targets,
        requirements,
        venue,
    )?
    .join(settings.to_string()))
}

/// The training settings of `meta`: its learner, window, holdout, and
/// the threshold deciding the trades its holdout is judged on, and the
/// minimums of its holdout.
fn meta_settings(meta: &MetaConfig) -> anyhow::Result<Fingerprint> {
    Ok(Fingerprint::default()
        .with(serde_json::to_string(&meta.learner)?.as_bytes())
        .with(&meta.window.as_nanos().to_le_bytes())
        .with(&meta.holdout.as_nanos().to_le_bytes())
        .with(&meta.threshold.to_le_bytes())
        .with(&meta.min_pnl.to_le_bytes())
        .with(&meta.min_trades.to_le_bytes()))
}

/// The models of a store as a strategy swaps them in, decision by decision
/// in time order: at each, the latest valid then, checked against the
/// strategy before use (`StoredModel::load`). One failing a check is
/// refused, logged as an error, and the current one kept; it is not read
/// again.
#[derive(Debug)]
pub struct Swap {
    store: ModelStore,
    /// The feature names a model must carry, in order.
    feature_names: Vec<String>,
    /// The fingerprint a model must carry.
    fingerprint: String,
    /// The model in use, and its valid-from.
    current: Option<(Timestamp, LoadedModel)>,
    /// The valid-from of the last model read, in use or refused.
    read: Option<Timestamp>,
}

impl Swap {
    /// Swaps in the models of `store` for a strategy of `feature_names`
    /// and `fingerprint`; none in use yet.
    #[must_use]
    pub const fn new(store: ModelStore, feature_names: Vec<String>, fingerprint: String) -> Self {
        Self {
            store,
            feature_names,
            fingerprint,
            current: None,
            read: None,
        }
    }

    #[must_use]
    pub const fn store(&self) -> &ModelStore {
        &self.store
    }

    /// The model in use at `now`, and its valid-from, among the models
    /// valid from `valid_froms`, the store's list in order: the latest
    /// valid then, swapped in if not read yet, unless it fails its checks.
    pub fn at(
        &mut self,
        now: Timestamp,
        valid_froms: &[Timestamp],
    ) -> Option<(Timestamp, &LoadedModel)> {
        let latest = valid_froms.iter().rfind(|&&time| time <= now).copied();
        if let Some(latest) = latest.filter(|&latest| self.read != Some(latest)) {
            self.read = Some(latest);
            match self
                .store
                .read(latest)
                .and_then(|stored| stored.load(&self.feature_names, &self.fingerprint))
            {
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

/// A row of values as JSON: a number each, NaN and the infinities, which
/// JSON has no number for, as `"NaN"`, `"inf"` and `"-inf"`.
mod values {
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

    #[derive(Serialize, Deserialize)]
    #[serde(untagged)]
    enum Value {
        Number(f64),
        Name(String),
    }

    pub fn serialize<S: Serializer>(row: &[f64], serializer: S) -> Result<S::Ok, S::Error> {
        row.iter()
            .map(|&value| match value {
                v if v.is_finite() => Value::Number(v),
                v if v.is_nan() => Value::Name("NaN".to_string()),
                v if v > 0.0 => Value::Name("inf".to_string()),
                _ => Value::Name("-inf".to_string()),
            })
            .collect::<Vec<_>>()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<f64>, D::Error> {
        Vec::<Value>::deserialize(deserializer)?
            .into_iter()
            .map(|value| match value {
                Value::Number(v) => Ok(v),
                Value::Name(name) => match name.as_str() {
                    "NaN" => Ok(f64::NAN),
                    "inf" => Ok(f64::INFINITY),
                    "-inf" => Ok(f64::NEG_INFINITY),
                    _ => Err(de::Error::custom(format!("{name} is not a value"))),
                },
            })
            .collect()
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
    use crate::model::{
        dataset::Commission,
        decide::DecisionRule,
        features::testing::last_mid,
        holdout::Earnings,
        learner::{LearnerConfig, TrainingSet},
    };

    const GOLD: &str = "XAUUSD.ICMARKETS";
    const STRATEGY: &str = "STORE-001";

    fn features() -> std::rc::Rc<FeatureSet> {
        last_mid(InstrumentId::from(GOLD)).build().unwrap()
    }

    fn config() -> ModelConfig {
        ModelConfig::builder()
            .decision(DecisionRule::Every(SignedDuration::from_hours(1)))
            .targets(
                TargetsConfig::builder()
                    .instrument_id(InstrumentId::from(GOLD))
                    .quantity(Quantity::from(1))
                    .horizon(SignedDuration::from_hours(1))
                    .build(),
            )
            .label(Label::Horizon)
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

    /// The store of the `side` models of `config`, run on `venue`.
    fn side_store(
        root: &Path,
        config: &ModelConfig,
        venue: &DatasetVenue,
        side: Side,
    ) -> ModelStore {
        ModelStore::new(
            root,
            StrategyId::from(STRATEGY),
            &features(),
            config,
            &requirements(),
            venue,
            side,
        )
        .unwrap()
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
            commission: Commission::Rate("0.00003".to_string()),
            balance: Money::from("10000000 USD"),
            leverage: Decimal::ONE,
        }
    }

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    /// A fake model valid from `valid_from`, probed on `rows`.
    fn model(valid_from: &str, rows: &[Vec<f64>]) -> StoredModel {
        let set = TrainingSet::new(1, vec![0.2, 0.7], vec![false, true]).unwrap();
        let fitted = LearnerConfig::Fake {}.fit(&set).unwrap();
        StoredModel {
            manifest: ModelManifest {
                strategy_id: StrategyId::from(STRATEGY),
                valid_from: at(valid_from),
                fingerprint: "f".repeat(64),
                feature_names: vec!["mid".to_string()],
                targets: config().targets,
                label: Label::Horizon,
                side: Some(Side::Long),
                learner: LearnerConfig::Fake {},
                calibration: Calibration { a: -4.0, b: 2.0 },
                payoff: Some(Payoff {
                    win: 1.5,
                    loss: 1.25,
                }),
                holdout: Holdout {
                    earnings: Earnings::default(),
                    log_loss: 0.6,
                    baseline_log_loss: 0.69,
                    auc: Some(0.625),
                },
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
    fn other_training_settings_targets_minimums_or_margin_list_none_of_the_models() {
        let dir = tempfile::tempdir().unwrap();
        store(dir.path(), &config())
            .publish(&model("2026-10-22T00:00:00Z", &[vec![0.5]]))
            .unwrap();
        let mut targets = config().targets;
        targets.horizon = Some(SignedDuration::from_mins(30));
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
            ModelConfig {
                targets,
                ..config()
            },
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

    #[test]
    fn the_short_margin_does_not_key_the_long_models() {
        let dir = tempfile::tempdir().unwrap();
        store(dir.path(), &config())
            .publish(&model("2026-10-22T00:00:00Z", &[vec![0.5]]))
            .unwrap();
        let other_short = ModelConfig {
            margins: Margins {
                long: Some(0.5),
                short: None,
            },
            ..config()
        };

        assert_eq!(
            store(dir.path(), &other_short).valid_froms().unwrap(),
            [at("2026-10-22T00:00:00Z")]
        );
    }

    /// The store of the meta model `meta` of `config`.
    fn meta_store(root: &Path, config: &ModelConfig, meta: &MetaConfig) -> ModelStore {
        ModelStore::meta(
            root,
            StrategyId::from(STRATEGY),
            &features(),
            config,
            meta,
            &requirements(),
            &venue(),
        )
        .unwrap()
    }

    /// A meta model published under one threshold and minimums may fall
    /// short of others, and the primary's margins make its rows: a meta
    /// of another threshold, minimums or margins lists none of them.
    #[test]
    fn a_meta_model_of_another_threshold_minimums_or_margins_is_not_listed() {
        let dir = tempfile::tempdir().unwrap();
        let meta = |threshold, min_pnl| {
            MetaConfig::builder()
                .window(SignedDuration::from_hours(48))
                .holdout(SignedDuration::from_hours(12))
                .threshold(threshold)
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

    #[test]
    fn the_long_and_the_short_models_are_listed_apart() {
        let dir = tempfile::tempdir().unwrap();
        side_store(dir.path(), &config(), &venue(), Side::Long)
            .publish(&model("2026-10-22T00:00:00Z", &[vec![0.5]]))
            .unwrap();

        let short = side_store(dir.path(), &config(), &venue(), Side::Short);

        assert!(short.valid_froms().unwrap().is_empty());
    }

    /// Every prediction of a loaded model is calibrated, its probes too:
    /// the fake predicts the first feature, 0.75, calibrated by a = -4 and
    /// b = 2 to 1 / (1 + e^-1).
    #[test]
    fn a_loaded_model_predicts_through_its_calibration() {
        let mut stored = model("2026-10-22T00:00:00Z", &[]);
        stored.manifest.probes = vec![Probe {
            row: vec![0.75],
            prediction: 1.0 / (1.0 + (-1.0_f64).exp()),
        }];

        let loaded = stored
            .load(&stored.manifest.feature_names, &stored.manifest.fingerprint)
            .unwrap();

        let prediction = loaded.predict(&[0.25]).unwrap();
        assert!(
            (prediction - 1.0 / (1.0 + 1.0_f64.exp())).abs() < 1e-12,
            "{prediction}"
        );
        assert_eq!(
            loaded.payoff(),
            Some(Payoff {
                win: 1.5,
                loss: 1.25
            })
        );
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
            Commission::Rate("0.00004".to_string()),
            Commission::PerOrder("3 USD".to_string()),
        ] {
            assert!(
                store_net_of(
                    dir.path(),
                    &config(),
                    &DatasetVenue {
                        commission: other.clone(),
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

    /// A model trained at a time is the same whatever the interval that
    /// time came from: backtests and live retraining at other intervals
    /// share it.
    #[test]
    fn another_retrain_interval_lists_the_same_models() {
        let dir = tempfile::tempdir().unwrap();
        store(dir.path(), &config())
            .publish(&model("2026-10-22T00:00:00Z", &[vec![0.5]]))
            .unwrap();
        let weekly = ModelConfig {
            retrain: SignedDuration::from_hours(168),
            ..config()
        };

        assert_eq!(
            store(dir.path(), &weekly).valid_froms().unwrap(),
            [at("2026-10-22T00:00:00Z")]
        );
    }
}
