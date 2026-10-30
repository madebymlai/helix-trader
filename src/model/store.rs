//! The model store: the models published for a strategy, each valid from
//! the time it was trained at.
//!
//! A store is a directory per strategy id, fingerprint (that of its
//! dataset: the features, the decision rule, the targets and the
//! commission they are net of, see `dataset::fingerprint`) and training
//! settings (the learner and its settings, the label, the window, the
//! holdout and the minimum score, below which a model is never
//! published), so a model is only ever listed beside models of the same
//! features, costs and training. Backtest and live read the same list.
//!
//! A meta model's store sits in a `meta/` directory under its primary's,
//! keyed by its own training settings (its learner and settings, window,
//! holdout and minimum score) and the primary's thresholds, which make its
//! rows; a rule primary has no store of its own, its meta's `meta/` sits
//! under the strategy id and fingerprint. A meta model is only ever listed beside
//! meta models of the same primary.
//!
//! `Swap` is how the models are used: at each decision, in time order,
//! the latest valid then, checked before use, a model failing a check
//! refused and the current one kept.
//!
//! Each model is one file, `<valid from>.json` (an RFC 3339 time): its
//! manifest, the schema (the feature names in order, the fingerprint), the
//! target spec and label, the learner, the holdout score and probe rows
//! with the predictions the model made on them when fitted, and the model
//! as its adapter saved it. A reloaded model is checked by predicting the
//! probes, never by its bytes. The file is written beside its name, then
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
    config::{MetaConfig, ModelConfig, RuleConfig, Thresholds},
    dataset::{self, Commission},
    decide::DecisionRule,
    features::{FeatureSet, Fingerprint},
    learner::{LearnerConfig, Model, SavedModel},
    targets::{Label, TargetsConfig},
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
    /// the commission.
    pub fingerprint: String,
    /// The names of the features, in the order of a row's values.
    pub feature_names: Vec<String>,
    /// The target spec.
    pub targets: TargetsConfig,
    /// The target the model learned.
    pub label: Label,
    pub learner: LearnerConfig,
    /// The model's AUC on the holdout.
    pub score: f64,
    /// Rows of the holdout and the model's predictions on them.
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
    /// The model, loaded by its manifest's learner and checked against a
    /// strategy of `feature_names` and `fingerprint`: the manifest's feature
    /// names in order and fingerprint are the strategy's, the model takes
    /// as many features, and it predicts each probe to the bit as when
    /// fitted.
    ///
    /// # Errors
    ///
    /// Returns an error if the model cannot be loaded or fails a check.
    pub fn load(&self, feature_names: &[String], fingerprint: &str) -> anyhow::Result<Model> {
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

impl ModelStore {
    /// The store, under `root`, of the models of `strategy_id` deciding with
    /// `features` as `config` says, on targets for a strategy of
    /// `requirements`, net of `commission`, and trained as `config` says.
    ///
    /// # Errors
    ///
    /// Returns an error if the learner cannot be written as JSON, or as
    /// `dataset::fingerprint` does.
    pub fn new(
        root: &Path,
        strategy_id: StrategyId,
        features: &FeatureSet,
        config: &ModelConfig,
        requirements: &DataRequirements,
        commission: &Commission,
    ) -> anyhow::Result<Self> {
        let settings = Fingerprint::default()
            .with(serde_json::to_string(&config.learner)?.as_bytes())
            .with(format!("{:?}", config.label).as_bytes())
            .with(&config.window.as_nanos().to_le_bytes())
            .with(&config.holdout.as_nanos().to_le_bytes())
            .with(&config.min_score.to_le_bytes());
        Ok(Self {
            directory: dataset_directory(
                root,
                strategy_id,
                features,
                config.decision,
                &config.targets,
                requirements,
                commission,
            )?
            .join(settings.to_string()),
        })
    }

    /// The store of the meta model `meta` of this store's models, whose
    /// trades `thresholds` make: within this store's directory, under
    /// `meta/` and the meta's training settings, `thresholds` among them
    /// (they make the meta's rows).
    ///
    /// # Errors
    ///
    /// Returns an error if the learner cannot be written as JSON.
    pub fn meta(&self, meta: &MetaConfig, thresholds: Thresholds) -> anyhow::Result<Self> {
        let settings = meta_settings(meta)?
            .with(&thresholds.long.to_le_bytes())
            .with(&thresholds.short.to_le_bytes());
        Ok(Self {
            directory: self.directory.join(META).join(settings.to_string()),
        })
    }

    /// The store, under `root`, of the meta models of `strategy_id`, whose
    /// primary is its rule, deciding with `features` as `config` says, on
    /// targets for a strategy of `requirements`, net of `commission`.
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
        commission: &Commission,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            directory: dataset_directory(
                root,
                strategy_id,
                features,
                config.decision,
                &config.targets,
                requirements,
                commission,
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
/// net of `commission`: its id and its dataset's fingerprint.
fn dataset_directory(
    root: &Path,
    strategy_id: StrategyId,
    features: &FeatureSet,
    rule: DecisionRule,
    targets: &TargetsConfig,
    requirements: &DataRequirements,
    commission: &Commission,
) -> anyhow::Result<PathBuf> {
    let fingerprint = dataset::fingerprint(features, rule, targets, requirements, commission)?;
    Ok(root
        .join(strategy_id.as_str())
        .join(fingerprint.to_string()))
}

/// The training settings of `meta`: its learner, window, holdout and
/// minimum score.
fn meta_settings(meta: &MetaConfig) -> anyhow::Result<Fingerprint> {
    Ok(Fingerprint::default()
        .with(serde_json::to_string(&meta.learner)?.as_bytes())
        .with(&meta.window.as_nanos().to_le_bytes())
        .with(&meta.holdout.as_nanos().to_le_bytes())
        .with(&meta.min_score.to_le_bytes()))
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
    current: Option<(Timestamp, Model)>,
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
    pub fn at(&mut self, now: Timestamp, valid_froms: &[Timestamp]) -> Option<(Timestamp, &Model)> {
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
    use nautilus_indicators::average::sma::SimpleMovingAverage;
    use nautilus_model::{enums::PriceType, identifiers::InstrumentId, types::Quantity};

    use super::*;
    use crate::model::{
        config::Thresholds,
        decide::DecisionRule,
        learner::{LearnerConfig, TrainingSet},
    };

    const GOLD: &str = "XAUUSD.ICMARKETS";
    const STRATEGY: &str = "STORE-001";

    fn features() -> std::rc::Rc<FeatureSet> {
        FeatureSet::builder()
            .quotes(
                InstrumentId::from(GOLD),
                "mid",
                SimpleMovingAverage::new(1, Some(PriceType::Mid)),
            )
            .build()
            .unwrap()
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
            .thresholds(Thresholds {
                long: 0.6,
                short: 0.4,
            })
            .min_score(0.5)
            .learner(LearnerConfig::Fake {})
            .build()
    }

    fn store(root: &Path, config: &ModelConfig) -> ModelStore {
        store_net_of(root, config, &commission())
    }

    fn store_net_of(root: &Path, config: &ModelConfig, commission: &Commission) -> ModelStore {
        ModelStore::new(
            root,
            StrategyId::from(STRATEGY),
            &features(),
            config,
            &requirements(),
            commission,
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

    /// A commission rate of 30 per million.
    fn commission() -> Commission {
        Commission::Rate("0.00003".to_string())
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
                learner: LearnerConfig::Fake {},
                score: 0.625,
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
    fn other_training_settings_targets_or_minimum_score_list_none_of_the_models() {
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
        let gated = ModelConfig {
            min_score: 0.9,
            ..config()
        };
        assert!(store(dir.path(), &gated).valid_froms().unwrap().is_empty());
    }

    /// A meta model published at one minimum score may score below
    /// another: a meta of another minimum lists none of them.
    #[test]
    fn a_meta_model_of_another_minimum_score_is_not_listed() {
        let dir = tempfile::tempdir().unwrap();
        let meta = |min_score| {
            MetaConfig::builder()
                .window(SignedDuration::from_hours(48))
                .holdout(SignedDuration::from_hours(12))
                .threshold(0.55)
                .min_score(min_score)
                .learner(LearnerConfig::Fake {})
                .build()
        };
        let primary = store(dir.path(), &config());
        primary
            .meta(&meta(0.5), config().thresholds)
            .unwrap()
            .publish(&model("2026-10-22T00:00:00Z", &[vec![0.5]]))
            .unwrap();

        assert!(
            primary
                .meta(&meta(0.9), config().thresholds)
                .unwrap()
                .valid_froms()
                .unwrap()
                .is_empty()
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
                store_net_of(dir.path(), &config(), &other)
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
