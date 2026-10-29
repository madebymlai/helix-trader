//! The model store: the models published for a strategy, each valid from
//! the time it was trained at.
//!
//! A store is a directory per strategy id, fingerprint (that of its
//! dataset: the features, the decision rule and the targets, see
//! `dataset::fingerprint`) and training settings (the learner and its
//! settings, the label, the window and the holdout), so a model is only
//! ever listed beside models of the same features and training. Backtest
//! and live read the same list.
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
};

use anyhow::Context;
use jiff::Timestamp;
use nautilus_model::identifiers::StrategyId;
use serde::{Deserialize, Serialize};

use super::{
    config::ModelConfig,
    dataset,
    features::{FeatureSet, Fingerprint},
    learner::{LearnerConfig, SavedModel},
    targets::{Label, TargetsConfig},
};

/// The extension of a model's file.
const EXTENSION: &str = "json";

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
    /// The fingerprint of the features, the decision rule and the targets.
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

impl ModelStore {
    /// The store, under `root`, of the models of `strategy_id` deciding with
    /// `features` as `config` says and trained as it says.
    ///
    /// # Errors
    ///
    /// Returns an error if the learner cannot be written as JSON.
    pub fn new(
        root: &Path,
        strategy_id: StrategyId,
        features: &FeatureSet,
        config: &ModelConfig,
    ) -> anyhow::Result<Self> {
        let fingerprint = dataset::fingerprint(features, config.decision, &config.targets);
        let settings = Fingerprint::default()
            .with(serde_json::to_string(&config.learner)?.as_bytes())
            .with(format!("{:?}", config.label).as_bytes())
            .with(&config.window.as_nanos().to_le_bytes())
            .with(&config.holdout.as_nanos().to_le_bytes());
        Ok(Self {
            directory: root
                .join(strategy_id.as_str())
                .join(fingerprint.to_string())
                .join(settings.to_string()),
        })
    }

    /// Publishes `model`, valid from its manifest's time, replacing the
    /// model valid from then, if any: written beside its file, then renamed
    /// onto it.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be written.
    pub fn publish(&self, model: &StoredModel) -> anyhow::Result<()> {
        fs::create_dir_all(&self.directory)
            .with_context(|| format!("creating the model store {}", self.directory.display()))?;
        let path = self.path(model.manifest.valid_from);
        let written = path.with_extension(format!("{EXTENSION}.tmp"));
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
        self.directory
            .join(format!("{valid_from}"))
            .with_extension(EXTENSION)
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
        ModelStore::new(root, StrategyId::from(STRATEGY), &features(), config).unwrap()
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
    fn other_training_settings_or_targets_list_none_of_the_models() {
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
        assert_eq!(store(dir.path(), &gated).valid_froms().unwrap().len(), 1);
    }
}
