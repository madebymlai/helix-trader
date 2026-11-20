//! The model port: a learner fits a model on a training set or loads a saved one, and the model
//! predicts and saves.
//!
//! Each model library is an adapter behind the port, chosen by a variant of `LearnerConfig`, the
//! learner section of a strategy's TOML (`kind` names the adapter). Adding a library is one adapter
//! module and one variant. The adapters:
//!
//! - `lightgbm`: gradient-boosted trees under L2 regression, defaults regularized for a weak
//!   signal.
//! - `perpetual`: gradient-boosted trees on squared loss, stopped by a budget; needs a memory
//!   limit. Fits far slower than LightGBM and saves models many times larger.
//! - `baseline`: the training mean, the benchmark every score is read against.
//! - `fake`: the row's first feature, to steer predictions in tests; behind the `test-support`
//!   feature.
//!
//! Labels are real values fitted on squared error: the model predicts a row's expected label from
//! features in the order it was fitted on. NaN means missing in every adapter. The port, not the
//! adapter, checks a row's width before predicting, so a wrong width is an error, never a library
//! panic nor a prediction from shifted columns.
//!
//! A saved model is the adapter's own text, loaded back by the same learner config. Some libraries
//! do not save the same bytes twice, so a reload's fidelity is told by its predictions. Fitting is
//! deterministic: the same set and settings, seed included, give the same predictions to the bit.
//!
//! `Model` is neither `Send` nor `Sync` (LightGBM's booster is neither), so a model stays on the
//! thread that loaded it.

mod baseline;
mod lightgbm;
mod perpetual;

#[cfg(feature = "test-support")]
mod fake;

use std::fmt::Debug;

pub use lightgbm::LightGbmConfig;
pub use perpetual::PerpetualConfig;
use serde::{Deserialize, Serialize};

/// The learner of a strategy's model, loaded from TOML, such as `learner = { kind = "lightgbm",
/// num_iterations = 200 }`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LearnerConfig {
    /// LightGBM, with its settings.
    #[serde(rename = "lightgbm")]
    LightGbm(LightGbmConfig),
    /// PerpetualBooster, with its settings.
    Perpetual(PerpetualConfig),
    /// The mean of the training labels, whatever the row.
    Baseline {},
    /// The row's first feature; the mean of the training labels where it is missing. Test support,
    /// behind the crate's `test-support` feature.
    #[cfg(feature = "test-support")]
    Fake {},
}

impl LearnerConfig {
    /// Checks the learner's settings.
    ///
    /// # Errors
    ///
    /// Returns an error if the settings are ones the library would not honor (see
    /// `LightGbmConfig::validate`).
    pub fn validate(&self) -> anyhow::Result<()> {
        match self {
            Self::LightGbm(config) => config.validate(),
            Self::Perpetual(_) | Self::Baseline {} => Ok(()),
            #[cfg(feature = "test-support")]
            Self::Fake {} => Ok(()),
        }
    }

    /// Fits a model on `set`.
    ///
    /// # Errors
    ///
    /// Returns an error if the library fails to fit, such as on settings it refuses.
    pub fn fit(&self, set: &TrainingSet) -> anyhow::Result<Model> {
        Ok(Model::new(self.kind(), self.learner().fit(set)?))
    }

    /// Loads a model this learner saved.
    ///
    /// # Errors
    ///
    /// Returns an error if `saved` is not a model of this learner's kind.
    pub fn load(&self, saved: &SavedModel) -> anyhow::Result<Model> {
        Ok(Model::new(self.kind(), self.learner().load(saved)?))
    }

    const fn kind(&self) -> &'static str {
        match self {
            Self::LightGbm(_) => "lightgbm",
            Self::Perpetual(_) => "perpetual",
            Self::Baseline {} => "baseline",
            #[cfg(feature = "test-support")]
            Self::Fake {} => "fake",
        }
    }

    fn learner(&self) -> &dyn Learner {
        match self {
            Self::LightGbm(config) => config,
            Self::Perpetual(config) => config,
            Self::Baseline {} => &baseline::Baseline,
            #[cfg(feature = "test-support")]
            Self::Fake {} => &fake::Fake,
        }
    }
}

/// The rows a model is fitted on: features row by row, all of one width, and each row's label.
#[derive(Debug, Clone, PartialEq)]
pub struct TrainingSet {
    width: usize,
    features: Vec<f64>,
    labels: Vec<f64>,
}

impl TrainingSet {
    /// A set of `labels.len()` rows of `width` features each, `features` holding them row after
    /// row; NaN marks a missing feature. The labels are fitted on their squared error.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - There is no row.
    /// - `width` is zero.
    /// - `features` does not hold `width` features for each label.
    /// - A label is not a finite number.
    pub fn new(width: usize, features: Vec<f64>, labels: Vec<f64>) -> anyhow::Result<Self> {
        if let Some(label) = labels.iter().find(|label| !label.is_finite()) {
            anyhow::bail!("a label of {label} is not a finite number");
        }
        anyhow::ensure!(!labels.is_empty(), "a training set needs a row");
        anyhow::ensure!(width > 0, "a training set needs a feature");
        anyhow::ensure!(
            features.len() == width * labels.len(),
            "{} features are not {} rows of {width}",
            features.len(),
            labels.len()
        );
        Ok(Self {
            width,
            features,
            labels,
        })
    }

    /// The number of features of each row.
    #[must_use]
    pub const fn width(&self) -> usize {
        self.width
    }

    fn rows(&self) -> impl Iterator<Item = &[f64]> {
        self.features.chunks_exact(self.width)
    }

    #[must_use]
    fn labels(&self) -> &[f64] {
        &self.labels
    }

    #[must_use]
    fn mean(&self) -> f64 {
        self.labels.iter().sum::<f64>() / self.labels.len() as f64
    }
}

/// A model as its adapter saved it: text, loaded back by the learner config that fitted it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SavedModel(String);

impl SavedModel {
    /// The saved text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for SavedModel {
    fn from(text: String) -> Self {
        Self(text)
    }
}

/// A fitted or loaded model, which predicts on rows of its width.
pub struct Model {
    kind: &'static str,
    predictor: Box<dyn Predictor>,
}

impl Model {
    fn new(kind: &'static str, predictor: Box<dyn Predictor>) -> Self {
        Self { kind, predictor }
    }

    /// The number of features of a row.
    #[must_use]
    pub fn width(&self) -> usize {
        self.predictor.width()
    }

    /// The prediction on `row`, its expected label; NaN marks a missing feature.
    ///
    /// # Errors
    ///
    /// Returns an error if `row` is not of the model's width, or the library fails to predict.
    pub fn predict(&self, row: &[f64]) -> anyhow::Result<f64> {
        anyhow::ensure!(
            row.len() == self.width(),
            "a row of {} features, the {} model takes {}",
            row.len(),
            self.kind,
            self.width()
        );
        self.predictor.predict(row)
    }

    /// The model as text, for the learner config that fitted it to load.
    ///
    /// # Errors
    ///
    /// Returns an error if the library fails to save.
    pub fn save(&self) -> anyhow::Result<SavedModel> {
        self.predictor.save()
    }
}

impl Debug for Model {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(Model))
            .field("kind", &self.kind)
            .field("width", &self.width())
            .finish()
    }
}

trait Learner {
    fn fit(&self, set: &TrainingSet) -> anyhow::Result<Box<dyn Predictor>>;

    fn load(&self, saved: &SavedModel) -> anyhow::Result<Box<dyn Predictor>>;
}

/// The port's model side, one per adapter. `predict` is given rows of `width` features only.
trait Predictor {
    fn width(&self) -> usize;

    fn predict(&self, row: &[f64]) -> anyhow::Result<f64>;

    fn save(&self) -> anyhow::Result<SavedModel>;
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const ROWS: usize = 600;
    const WIDTH: usize = 4;

    fn uniform() -> impl FnMut() -> f64 {
        let mut state = 0x5eed_u64;
        move || {
            // splitmix64: a known, dependency-free generator
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            (z ^ (z >> 31)) as f64 / u64::MAX as f64
        }
    }

    fn with_missing(row: &[f64], missing: bool) -> impl Iterator<Item = f64> + '_ {
        row.iter().enumerate().map(move |(column, &value)| {
            if column == 2 && missing {
                f64::NAN
            } else {
                value
            }
        })
    }

    /// `ROWS` rows of `WIDTH` seeded features, labeled by a noisy line on the first two, from about
    /// -3 to 2.5. The third feature is missing in about a tenth of the rows, raising the label by
    /// 1.5.
    fn synthetic() -> TrainingSet {
        let mut uniform = uniform();
        let mut features = Vec::with_capacity(ROWS * WIDTH);
        let mut labels = Vec::with_capacity(ROWS);
        for _ in 0..ROWS {
            let row: Vec<f64> = (0..WIDTH).map(|_| uniform()).collect();
            let missing = uniform() < 0.1;
            let raised = if missing { 1.5 } else { 0.0 };
            labels.push(2.0 * row[0] - 3.0 * row[1] + 0.5 * uniform() + raised);
            features.extend(with_missing(&row, missing));
        }
        TrainingSet::new(WIDTH, features, labels).unwrap()
    }

    /// Rows to predict on: training rows and some outside them, with missing features.
    fn probes(set: &TrainingSet) -> Vec<Vec<f64>> {
        let mut rows: Vec<Vec<f64>> = set.rows().take(50).map(<[f64]>::to_vec).collect();
        rows.push(vec![f64::NAN; set.width()]);
        rows.push(vec![0.0; set.width()]);
        rows.push(vec![-3.0, 7.0, f64::NAN, 1e-300]);
        rows
    }

    fn predictions(model: &Model, rows: &[Vec<f64>]) -> Vec<u64> {
        rows.iter()
            .map(|row| model.predict(row).unwrap().to_bits())
            .collect()
    }

    fn others(learner: &LearnerConfig) -> Vec<LearnerConfig> {
        [
            LearnerConfig::LightGbm(LightGbmConfig::builder().build()),
            LearnerConfig::Perpetual(PerpetualConfig::builder().memory_limit(1.0).build()),
            LearnerConfig::Baseline {},
            LearnerConfig::Fake {},
        ]
        .into_iter()
        .filter(|other| std::mem::discriminant(other) != std::mem::discriminant(learner))
        .collect()
    }

    /// The contract every adapter meets, `$name` naming its models in the refusal of another's
    /// saved model.
    macro_rules! contract {
        ($adapter:ident, $config:expr, $name:literal) => {
            mod $adapter {
                use rstest::rstest;

                use super::*;

                fn config() -> LearnerConfig {
                    $config
                }

                #[rstest]
                fn a_reloaded_model_predicts_bit_identically() {
                    let set = synthetic();
                    let model = config().fit(&set).unwrap();
                    let reloaded = config().load(&model.save().unwrap()).unwrap();
                    let rows = probes(&set);
                    assert_eq!(predictions(&reloaded, &rows), predictions(&model, &rows));
                }

                #[rstest]
                fn the_same_settings_fit_the_same_predictions() {
                    let set = synthetic();
                    let rows = probes(&set);
                    assert_eq!(
                        predictions(&config().fit(&set).unwrap(), &rows),
                        predictions(&config().fit(&set).unwrap(), &rows)
                    );
                }

                #[rstest]
                #[case::of_no_feature(0)]
                #[case::one_feature_short(WIDTH - 1)]
                #[case::one_feature_over(WIDTH + 1)]
                fn a_row_of_the_wrong_width_is_an_error(#[case] width: usize) {
                    let model = config().fit(&synthetic()).unwrap();

                    let err = model.predict(&vec![0.5; width]).unwrap_err().to_string();

                    assert!(err.contains(&format!("of {width} features")), "{err}");
                    assert!(err.contains(&format!("takes {WIDTH}")), "{err}");
                }

                #[rstest]
                fn a_reloaded_model_keeps_its_width() {
                    let set = synthetic();
                    let model = config().fit(&set).unwrap();
                    let reloaded = config().load(&model.save().unwrap()).unwrap();
                    assert_eq!(reloaded.width(), set.width());
                }

                #[rstest]
                fn a_model_saved_by_another_learner_is_refused() {
                    let set = synthetic();
                    for other in others(&config()) {
                        let saved = other.fit(&set).unwrap().save().unwrap();
                        let err = config().load(&saved).unwrap_err().to_string();
                        assert!(err.contains($name), "{other:?}: {err}");
                    }
                }
            }
        };
    }

    // Drawing features makes the fit depend on the seed, so the contract pins LightGBM's draws too
    contract!(
        lightgbm,
        LearnerConfig::LightGbm(LightGbmConfig::builder().feature_fraction(0.5).build()),
        "LightGBM"
    );
    contract!(
        perpetual,
        LearnerConfig::Perpetual(PerpetualConfig::builder().memory_limit(1.0).build()),
        "PerpetualBooster"
    );
    contract!(baseline, LearnerConfig::Baseline {}, "baseline");
    contract!(fake, LearnerConfig::Fake {}, "fake");

    #[rstest]
    #[case::without_rows(2, vec![], vec![], &["needs a row"])]
    #[case::without_features(0, vec![], vec![1.0], &["needs a feature"])]
    #[case::with_a_short_row(2, vec![1.0, 2.0, 3.0], vec![1.0, -1.0], &["3 features", "2 rows of 2"])]
    #[case::with_a_label_not_a_number(1, vec![1.0, 2.0], vec![0.5, f64::NAN], &["NaN"])]
    #[case::with_an_infinite_label(1, vec![1.0, 2.0], vec![0.5, f64::INFINITY], &["inf"])]
    #[case::with_a_label_infinitely_low(1, vec![1.0, 2.0], vec![0.5, f64::NEG_INFINITY], &["-inf"])]
    fn a_set_is_refused_naming_its_fault(
        #[case] width: usize,
        #[case] features: Vec<f64>,
        #[case] labels: Vec<f64>,
        #[case] facts: &[&str],
    ) {
        let err = TrainingSet::new(width, features, labels)
            .unwrap_err()
            .to_string();

        for fact in facts {
            assert!(err.contains(fact), "{fact}: {err}");
        }
    }

    #[derive(Debug, Deserialize)]
    struct Section {
        learner: LearnerConfig,
    }

    fn learner(toml: &str) -> anyhow::Result<LearnerConfig> {
        Ok(toml::from_str::<Section>(toml)?.learner)
    }

    #[rstest]
    #[case::lightgbm_with_defaults(
        r#"learner = { kind = "lightgbm", feature_fraction = 0.5 }"#,
        LearnerConfig::LightGbm(LightGbmConfig {
            num_iterations: 100,
            learning_rate: 0.05,
            num_leaves: 7,
            min_data_in_leaf: 200,
            feature_fraction: 0.5,
            seed: 1,
        })
    )]
    #[case::lightgbm_setting_each_of_its_settings(
        r#"learner = { kind = "lightgbm", num_iterations = 300, learning_rate = 0.02, num_leaves = 15, min_data_in_leaf = 50 }"#,
        LearnerConfig::LightGbm(LightGbmConfig {
            num_iterations: 300,
            learning_rate: 0.02,
            num_leaves: 15,
            min_data_in_leaf: 50,
            feature_fraction: 1.0,
            seed: 1,
        })
    )]
    #[case::perpetual_with_defaults(
        r#"learner = { kind = "perpetual", memory_limit = 2.5 }"#,
        LearnerConfig::Perpetual(PerpetualConfig {
            memory_limit: 2.5,
            budget: 0.5,
            iteration_limit: None,
            seed: 1,
        })
    )]
    fn a_learner_loads_from_toml_its_unset_settings_at_their_defaults(
        #[case] toml: &str,
        #[case] expected: LearnerConfig,
    ) {
        assert_eq!(learner(toml).unwrap(), expected);
    }

    #[rstest]
    #[case::perpetual_without_a_memory_limit(
        r#"learner = { kind = "perpetual", budget = 1.0 }"#,
        "memory_limit"
    )]
    #[case::an_unknown_kind(r#"learner = { kind = "xgboost" }"#, "xgboost")]
    #[case::a_misspelt_setting(r#"learner = { kind = "lightgbm", num_leafs = 7 }"#, "num_leafs")]
    #[case::a_setting_of_the_baseline(r#"learner = { kind = "baseline", seed = 7 }"#, "seed")]
    fn a_learner_is_rejected_at_load_naming_the_key_at_fault(
        #[case] toml: &str,
        #[case] key: &str,
    ) {
        let err = learner(toml).unwrap_err();

        assert!(err.to_string().contains(key), "{err}");
    }
}
