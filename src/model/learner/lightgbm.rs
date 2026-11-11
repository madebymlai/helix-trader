//! LightGBM: gradient-boosted trees under L2 regression.

use anyhow::{Context, ensure};
use lightgbm3::{Booster, Dataset};
use serde::{Deserialize, Serialize};

use super::{Learner, Predictor, SavedModel, TrainingSet};

/// LightGBM's settings, each named and meant as in LightGBM's parameters.
/// The defaults of the rounds, the learning rate, the leaves and the rows a
/// leaf (100, 0.05, 7 and 200) are regularised for a weak signal.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct LightGbmConfig {
    /// The number of boosting rounds, one tree each.
    #[serde(default = "default_num_iterations")]
    #[builder(default = default_num_iterations())]
    pub num_iterations: u32,
    #[serde(default = "default_learning_rate")]
    #[builder(default = default_learning_rate())]
    pub learning_rate: f64,
    /// The most leaves of a tree.
    #[serde(default = "default_num_leaves")]
    #[builder(default = default_num_leaves())]
    pub num_leaves: u32,
    /// The fewest rows in a leaf, 200 by default and 20 at least: lightgbm3
    /// builds LightGBM's dataset without parameters, and the dataset drops
    /// each feature no split leaves 20 rows each side of, LightGBM's
    /// default, so fewer would not be honoured.
    #[serde(default = "default_min_data_in_leaf")]
    #[builder(default = default_min_data_in_leaf())]
    pub min_data_in_leaf: u32,
    /// The share of the features drawn for each tree.
    #[serde(default = "default_feature_fraction")]
    #[builder(default = default_feature_fraction())]
    pub feature_fraction: f64,
    /// The seed of every draw.
    #[serde(default = "default_seed")]
    #[builder(default = default_seed())]
    pub seed: u32,
}

const fn default_num_iterations() -> u32 {
    100
}

const fn default_learning_rate() -> f64 {
    0.05
}

const fn default_num_leaves() -> u32 {
    7
}

/// The fewest rows in a leaf LightGBM's dataset honours: its default, as
/// lightgbm3 builds the dataset without parameters.
const DATASET_MIN_DATA_IN_LEAF: u32 = 20;

const fn default_min_data_in_leaf() -> u32 {
    200
}

const fn default_feature_fraction() -> f64 {
    1.0
}

const fn default_seed() -> u32 {
    1
}

impl LightGbmConfig {
    /// Checks the settings LightGBM would not honour.
    ///
    /// # Errors
    ///
    /// Returns an error if `min_data_in_leaf` is below 20: lightgbm3
    /// builds LightGBM's dataset without parameters, so LightGBM's default
    /// of 20 applies there whatever the setting.
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.min_data_in_leaf >= DATASET_MIN_DATA_IN_LEAF,
            "min_data_in_leaf {} is below {DATASET_MIN_DATA_IN_LEAF}: lightgbm3 builds \
             LightGBM's dataset without parameters, so LightGBM's default of \
             {DATASET_MIN_DATA_IN_LEAF} applies there",
            self.min_data_in_leaf
        );
        Ok(())
    }

    /// The settings as LightGBM's parameters, fitting L2 regression.
    /// LightGBM, built without OpenMP, trains on one thread; `deterministic`
    /// and `force_row_wise` keep its results from depending on the machine.
    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({
            "objective": "regression",
            "num_iterations": self.num_iterations,
            "learning_rate": self.learning_rate,
            "num_leaves": self.num_leaves,
            "min_data_in_leaf": self.min_data_in_leaf,
            "feature_fraction": self.feature_fraction,
            "seed": self.seed,
            "deterministic": true,
            "force_row_wise": true,
            "num_threads": 1,
            "verbosity": -1,
        })
    }
}

impl Learner for LightGbmConfig {
    fn fit(&self, set: &TrainingSet) -> anyhow::Result<Box<dyn Predictor>> {
        let features: Vec<f64> = set.rows().flatten().copied().collect();
        // LightGBM keeps its labels as f32.
        let labels: Vec<f32> = set.labels().iter().map(|&label| label as f32).collect();
        let dataset = Dataset::from_slice(&features, &labels, i32::try_from(set.width())?, true)?;
        Ok(Box::new(LightGbm::new(Booster::train(
            dataset,
            &self.parameters(),
        )?)?))
    }

    fn load(&self, saved: &SavedModel) -> anyhow::Result<Box<dyn Predictor>> {
        let booster = Booster::from_string(saved.as_str()).context("not a saved LightGBM model")?;
        Ok(Box::new(LightGbm::new(booster)?))
    }
}

/// A LightGBM booster and the width of its rows.
struct LightGbm {
    booster: Booster,
    width: i32,
}

impl LightGbm {
    fn new(booster: Booster) -> anyhow::Result<Self> {
        let width = booster.num_features();
        ensure!(width > 0, "a LightGBM model without features");
        Ok(Self { booster, width })
    }
}

impl Predictor for LightGbm {
    fn width(&self) -> usize {
        self.width.unsigned_abs() as usize
    }

    fn predict(&self, row: &[f64]) -> anyhow::Result<f64> {
        Ok(self.booster.predict(row, self.width, true)?[0])
    }

    fn save(&self) -> anyhow::Result<SavedModel> {
        Ok(SavedModel::from(self.booster.save_string()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::learner::LearnerConfig;

    /// 900 rows of two features whose label is 1 exactly where the first
    /// feature is missing, a third of them, enough for leaves of 200 rows,
    /// and 0 elsewhere, where the feature runs from 0 to 1, zero included.
    fn missing_is_one() -> TrainingSet {
        let mut features = Vec::new();
        let mut labels = Vec::new();
        for i in 0..900_u32 {
            let missing = i % 3 == 0;
            features.push(if missing {
                f64::NAN
            } else {
                f64::from(i % 7) / 6.0
            });
            features.push(f64::from(i % 11));
            labels.push(if missing { 1.0 } else { 0.0 });
        }
        TrainingSet::new(2, features, labels).unwrap()
    }

    #[test]
    fn a_missing_feature_is_learned_apart_from_every_value() {
        let config = LearnerConfig::LightGbm(LightGbmConfig::builder().build());
        let model = config.fit(&missing_is_one()).unwrap();
        assert!(model.predict(&[f64::NAN, 4.0]).unwrap() > 0.9);
        for value in [0.0, 0.5, 1.0] {
            assert!(model.predict(&[value, 4.0]).unwrap() < 0.1, "{value}");
        }
    }

    /// 300 rows of one feature, 0.0 to 0.9 by 0.1, its label 1 below 0.5
    /// and 0 from it: the split leaves 150 rows each side.
    fn halves() -> TrainingSet {
        let features: Vec<f64> = (0..300_u32).map(|i| f64::from(i % 10) / 10.0).collect();
        let labels = features
            .iter()
            .map(|&value| if value < 0.5 { 1.0 } else { 0.0 })
            .collect();
        TrainingSet::new(1, features, labels).unwrap()
    }

    /// Below 200 rows a side, the split is not made: the model predicts
    /// the mean label, one half, on either side of it.
    #[test]
    fn a_leaf_holds_200_rows_at_least_by_default() {
        let model = LearnerConfig::LightGbm(LightGbmConfig::builder().build())
            .fit(&halves())
            .unwrap();
        for value in [0.2, 0.7] {
            assert_eq!(model.predict(&[value]).unwrap(), 0.5, "{value}");
        }
    }

    /// The same split is made with leaves of 150 rows allowed.
    #[test]
    fn a_leaf_of_fewer_rows_is_set() {
        let config = LightGbmConfig::builder().min_data_in_leaf(150).build();
        let model = LearnerConfig::LightGbm(config).fit(&halves()).unwrap();
        assert!(model.predict(&[0.2]).unwrap() > 0.9);
        assert!(model.predict(&[0.7]).unwrap() < 0.1);
    }

    /// 600 rows of one feature, 0.0 to 0.9 by 0.1, labelled 2 below 0.5
    /// and -1 from it: 300 rows a side, enough for leaves of 200.
    fn step() -> TrainingSet {
        let features: Vec<f64> = (0..600_u32).map(|i| f64::from(i % 10) / 10.0).collect();
        let labels = features
            .iter()
            .map(|&value| if value < 0.5 { 2.0 } else { -1.0 })
            .collect();
        TrainingSet::new(1, features, labels).unwrap()
    }

    /// Worked by hand: L2 starts every row at the mean label, 0.5, 1.5
    /// short of either step, and each round's tree, split at the step,
    /// takes 0.05 of what is left on each side. After 100 rounds 1.5 *
    /// 0.95^100 = 0.008881 is left: the model predicts 1.991119 below the
    /// step and -0.991119 from it.
    #[test]
    fn a_step_of_real_labels_is_learned_at_the_default_rate_over_the_default_rounds() {
        let model = LearnerConfig::LightGbm(LightGbmConfig::builder().build())
            .fit(&step())
            .unwrap();
        let left = 1.5 * 0.95_f64.powi(100);
        for (value, expected) in [(0.2, 2.0 - left), (0.7, -1.0 + left)] {
            let predicted = model.predict(&[value]).unwrap();
            assert!((predicted - expected).abs() < 1e-6, "{value}: {predicted}");
        }
    }

    /// On 2,000 rows, enough for leaves of 200 rows.
    #[test]
    fn the_seed_reaches_lightgbm() {
        let fit = |seed| {
            let config = LightGbmConfig::builder()
                .feature_fraction(0.5)
                .seed(seed)
                .build();
            let set = TrainingSet::new(
                3,
                (0..6000_u32)
                    .map(|i| f64::from((i * 7919) % 101) / 100.0)
                    .collect(),
                (0..2000_u32)
                    .map(|i| if (i * 31) % 7 < 3 { 1.0 } else { 0.0 })
                    .collect(),
            )
            .unwrap();
            let model = LearnerConfig::LightGbm(config).fit(&set).unwrap();
            model.predict(&[0.2, 0.5, 0.7]).unwrap().to_bits()
        };
        assert_ne!(fit(1), fit(2));
    }
}
