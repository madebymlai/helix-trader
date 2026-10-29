//! The model port: a learner fits a model on a training set or loads a
//! saved one, and the model predicts and saves.
//!
//! Each model library is an adapter behind the port, chosen by a variant of
//! `LearnerConfig`, the learner section of a strategy's TOML (`kind`
//! names the adapter, its settings sit beside it). Adding a library is one
//! adapter module and one variant. The adapters:
//!
//! - `lightgbm`: LightGBM's gradient-boosted trees, binary objective.
//! - `baseline`: the training mean, the benchmark every score is read
//!   against.
//! - `fake`: the row's first feature, for tests that need to steer the
//!   probability.
//!
//! A model predicts the probability that a row's label is true, from a row
//! of features in the order it was fitted on. NaN means missing in every
//! adapter, with no setup. The port, not the adapter, checks the width of a
//! row before predicting, so a row of the wrong width is an error, never a
//! panic of a library nor a prediction from shifted columns.
//!
//! A saved model is the adapter's own text, loaded back by the same learner
//! config. Some libraries do not save the same bytes twice, so whether a
//! reload is faithful is told by its predictions, never by the saved bytes.
//! Fitting is deterministic: the same set and the same settings, seed
//! included, give the same predictions to the bit.
//!
//! `Model` holds the adapter behind a hand-written `Debug`, which is all a
//! strategy needs to hold it. It is neither `Send` nor `Sync` (LightGBM's
//! booster is neither), so a model stays on the thread that loaded it.

mod baseline;
mod fake;
mod lightgbm;

use std::fmt;

use anyhow::ensure;
use serde::Deserialize;

pub use lightgbm::LightGbmConfig;

/// The learner of a strategy's model, loaded from TOML, such as
/// `learner = { kind = "lightgbm", num_iterations = 200 }`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LearnerConfig {
    /// LightGBM, with its settings.
    #[serde(rename = "lightgbm")]
    LightGbm(LightGbmConfig),
    /// The mean of the training labels, whatever the row.
    Baseline {},
    /// The row's first feature, clamped to [0, 1]; the mean of the training
    /// labels where it is missing. For tests.
    Fake {},
}

impl LearnerConfig {
    /// Fits a model on `set`.
    ///
    /// # Errors
    ///
    /// Returns an error if the library fails to fit, such as on settings
    /// it refuses.
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
            Self::Baseline {} => "baseline",
            Self::Fake {} => "fake",
        }
    }

    fn learner(&self) -> &dyn Learner {
        match self {
            Self::LightGbm(config) => config,
            Self::Baseline {} => &baseline::Baseline,
            Self::Fake {} => &fake::Fake,
        }
    }
}

/// The rows a model is fitted on: features row by row, all of one width,
/// and each row's label.
#[derive(Debug, Clone, PartialEq)]
pub struct TrainingSet {
    width: usize,
    features: Vec<f64>,
    labels: Vec<bool>,
}

impl TrainingSet {
    /// A set of `labels.len()` rows of `width` features each, `features`
    /// holding them row after row; NaN marks a missing feature.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no row, `width` is zero, or `features`
    /// does not hold `width` features for each label.
    pub fn new(width: usize, features: Vec<f64>, labels: Vec<bool>) -> anyhow::Result<Self> {
        ensure!(!labels.is_empty(), "a training set needs a row");
        ensure!(width > 0, "a training set needs a feature");
        ensure!(
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

    /// Each row's features, in order.
    fn rows(&self) -> impl Iterator<Item = &[f64]> {
        self.features.chunks_exact(self.width)
    }

    /// Each row's label, in order.
    #[must_use]
    fn labels(&self) -> &[bool] {
        &self.labels
    }

    /// The share of labels that are true.
    #[must_use]
    fn mean(&self) -> f64 {
        let trues = self.labels.iter().filter(|&&label| label).count();
        trues as f64 / self.labels.len() as f64
    }
}

/// A model as its adapter saved it: text, loaded back by the learner config
/// that fitted it.
#[derive(Debug, Clone, PartialEq, Eq)]
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

    /// The probability that the label of `row` is true, from its features
    /// in the order the model was fitted on; NaN marks a missing feature.
    ///
    /// # Errors
    ///
    /// Returns an error if `row` is not of the model's width, or the
    /// library fails to predict.
    pub fn predict(&self, row: &[f64]) -> anyhow::Result<f64> {
        ensure!(
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

impl fmt::Debug for Model {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Model")
            .field("kind", &self.kind)
            .field("width", &self.width())
            .finish()
    }
}

/// The port's learner side, one implementation per adapter.
trait Learner {
    fn fit(&self, set: &TrainingSet) -> anyhow::Result<Box<dyn Predictor>>;

    fn load(&self, saved: &SavedModel) -> anyhow::Result<Box<dyn Predictor>>;
}

/// The port's model side, one implementation per adapter. `predict` is
/// given rows of `width` features only.
trait Predictor {
    fn width(&self) -> usize;

    fn predict(&self, row: &[f64]) -> anyhow::Result<f64>;

    fn save(&self) -> anyhow::Result<SavedModel>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fixed synthetic set: `ROWS` rows of `WIDTH` features drawn from a
    /// seeded generator, labelled by a noisy rule on the first two
    /// features. The third feature is missing in about a tenth of the rows,
    /// and a missing third feature makes the label true.
    fn synthetic() -> TrainingSet {
        const ROWS: usize = 600;
        const WIDTH: usize = 4;
        let mut state = 0x5eed_u64;
        let mut uniform = move || {
            // splitmix64: a known, dependency-free generator.
            state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            (z ^ (z >> 31)) as f64 / u64::MAX as f64
        };
        let mut features = Vec::with_capacity(ROWS * WIDTH);
        let mut labels = Vec::with_capacity(ROWS);
        for _ in 0..ROWS {
            let row: Vec<f64> = (0..WIDTH).map(|_| uniform()).collect();
            let missing = uniform() < 0.1;
            labels.push(missing || row[0] + 0.5 * row[1] + 0.3 * uniform() > 0.9);
            features.extend(row.iter().enumerate().map(|(column, &value)| {
                if column == 2 && missing {
                    f64::NAN
                } else {
                    value
                }
            }));
        }
        TrainingSet::new(WIDTH, features, labels).unwrap()
    }

    /// Rows to predict on, the training set's own and some outside it,
    /// with missing features among them.
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

    /// The contract every adapter meets, run by `contract!` against one
    /// learner config.
    macro_rules! contract {
        ($adapter:ident, $config:expr) => {
            mod $adapter {
                use super::*;

                fn config() -> LearnerConfig {
                    $config
                }

                #[test]
                fn a_reloaded_model_predicts_bit_identically() {
                    let set = synthetic();
                    let model = config().fit(&set).unwrap();
                    let reloaded = config().load(&model.save().unwrap()).unwrap();
                    let rows = probes(&set);
                    assert_eq!(predictions(&reloaded, &rows), predictions(&model, &rows));
                }

                #[test]
                fn the_same_settings_fit_the_same_predictions() {
                    let set = synthetic();
                    let rows = probes(&set);
                    assert_eq!(
                        predictions(&config().fit(&set).unwrap(), &rows),
                        predictions(&config().fit(&set).unwrap(), &rows)
                    );
                }

                #[test]
                fn predictions_are_probabilities() {
                    let set = synthetic();
                    let model = config().fit(&set).unwrap();
                    for row in probes(&set) {
                        let probability = model.predict(&row).unwrap();
                        assert!((0.0..=1.0).contains(&probability), "{row:?}: {probability}");
                    }
                }

                #[test]
                fn a_row_of_the_wrong_width_is_an_error() {
                    let set = synthetic();
                    let model = config().fit(&set).unwrap();
                    for width in [0, set.width() - 1, set.width() + 1] {
                        assert!(model.predict(&vec![0.5; width]).is_err(), "{width}");
                    }
                }

                #[test]
                fn a_reloaded_model_keeps_its_width() {
                    let set = synthetic();
                    let model = config().fit(&set).unwrap();
                    let reloaded = config().load(&model.save().unwrap()).unwrap();
                    assert_eq!(reloaded.width(), set.width());
                }

                #[test]
                fn a_model_saved_by_another_learner_is_refused() {
                    let foreign = SavedModel::from("not a model".to_string());
                    assert!(config().load(&foreign).is_err());
                }
            }
        };
    }

    // Drawing features makes the fit depend on the seed, so the contract
    // pins LightGBM's draws too.
    contract!(
        lightgbm,
        LearnerConfig::LightGbm(LightGbmConfig::builder().feature_fraction(0.5).build())
    );
    contract!(baseline, LearnerConfig::Baseline {});
    contract!(fake, LearnerConfig::Fake {});

    #[test]
    fn the_mean_is_the_share_of_true_labels() {
        let set = TrainingSet::new(1, vec![0.0; 4], vec![true, false, true, true]).unwrap();
        assert_eq!(set.mean(), 0.75);
    }

    #[test]
    fn a_set_without_rows_is_refused() {
        assert!(TrainingSet::new(2, vec![], vec![]).is_err());
    }

    #[test]
    fn a_set_without_features_is_refused() {
        assert!(TrainingSet::new(0, vec![], vec![true]).is_err());
    }

    #[test]
    fn a_set_with_a_short_row_is_refused() {
        assert!(TrainingSet::new(2, vec![1.0, 2.0, 3.0], vec![true, false]).is_err());
    }

    #[test]
    fn the_debug_of_a_model_names_its_kind_and_width() {
        let set = TrainingSet::new(3, vec![0.0; 6], vec![true, false]).unwrap();
        let model = LearnerConfig::Baseline {}.fit(&set).unwrap();
        assert_eq!(
            format!("{model:?}"),
            r#"Model { kind: "baseline", width: 3 }"#
        );
    }

    #[derive(Debug, Deserialize)]
    struct Section {
        learner: LearnerConfig,
    }

    fn learner(toml: &str) -> anyhow::Result<LearnerConfig> {
        Ok(toml::from_str::<Section>(toml)?.learner)
    }

    #[test]
    fn loads_lightgbm_from_toml_with_defaults() {
        let config = learner(r#"learner = { kind = "lightgbm", num_leaves = 7 }"#).unwrap();
        assert_eq!(
            config,
            LearnerConfig::LightGbm(LightGbmConfig::builder().num_leaves(7).build())
        );
    }

    #[test]
    fn loads_the_baseline_and_the_fake_from_toml() {
        assert_eq!(
            learner(r#"learner = { kind = "baseline" }"#).unwrap(),
            LearnerConfig::Baseline {}
        );
        assert_eq!(
            learner(r#"learner = { kind = "fake" }"#).unwrap(),
            LearnerConfig::Fake {}
        );
    }

    #[test]
    fn rejects_an_unknown_kind_at_load() {
        let err = learner(r#"learner = { kind = "xgboost" }"#).unwrap_err();
        assert!(err.to_string().contains("xgboost"), "{err}");
    }

    #[test]
    fn rejects_a_misspelt_setting_at_load() {
        let err = learner(r#"learner = { kind = "lightgbm", num_leafs = 7 }"#).unwrap_err();
        assert!(err.to_string().contains("num_leafs"), "{err}");
    }

    #[test]
    fn rejects_a_setting_of_the_baseline_at_load() {
        let err = learner(r#"learner = { kind = "baseline", seed = 7 }"#).unwrap_err();
        assert!(err.to_string().contains("seed"), "{err}");
    }
}
