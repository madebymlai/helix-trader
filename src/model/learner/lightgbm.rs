//! LightGBM trains gradient-boosted trees under L2 regression.

use anyhow::Context;
use lightgbm3::{Booster, Dataset};
use serde::{Deserialize, Serialize};

use super::{Learner, Predictor, SavedModel, TrainingSet};

/// LightGBM's settings, named as LightGBM's parameters are.
///
/// The defaults are regularized for a weak signal.
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
    /// The most leaves in a tree.
    #[serde(default = "default_num_leaves")]
    #[builder(default = default_num_leaves())]
    pub num_leaves: u32,
    /// The fewest rows in a leaf, at least 20.
    ///
    /// lightgbm3 builds the dataset without parameters. LightGBM then drops each feature that has
    /// no split leaving 20 rows on each side.
    #[serde(default = "default_min_data_in_leaf")]
    #[builder(default = default_min_data_in_leaf())]
    pub min_data_in_leaf: u32,
    /// The share of the features drawn for each tree.
    #[serde(default = "default_feature_fraction")]
    #[builder(default = default_feature_fraction())]
    pub feature_fraction: f64,
    /// The seed for every draw.
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

/// LightGBM's dataset default, which applies because lightgbm3 builds the dataset without
/// parameters.
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
    /// Checks the settings LightGBM would not honor.
    ///
    /// # Errors
    ///
    /// Returns an error if `min_data_in_leaf` is below 20. That is LightGBM's dataset default, and
    /// lightgbm3 applies it whatever the setting.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.min_data_in_leaf >= DATASET_MIN_DATA_IN_LEAF,
            "min_data_in_leaf {} is below {DATASET_MIN_DATA_IN_LEAF}: lightgbm3 builds \
             LightGBM's dataset without parameters, so LightGBM's default of \
             {DATASET_MIN_DATA_IN_LEAF} applies there",
            self.min_data_in_leaf
        );
        Ok(())
    }

    /// Builds the LightGBM parameters.
    ///
    /// LightGBM is built without OpenMP, so it trains on one thread. `deterministic` and
    /// `force_row_wise` keep its results from depending on the machine.
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

        // LightGBM stores labels as f32
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

struct LightGbm {
    booster: Booster,
    width: i32,
}

impl LightGbm {
    fn new(booster: Booster) -> anyhow::Result<Self> {
        let width = booster.num_features();
        anyhow::ensure!(width > 0, "a LightGBM model without features");
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
    use rstest::rstest;

    use super::*;
    use crate::model::learner::LearnerConfig;

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

    #[rstest]
    fn a_missing_feature_is_learned_apart_from_every_value() {
        let config = LearnerConfig::LightGbm(LightGbmConfig::builder().build());
        let model = config.fit(&missing_is_one()).unwrap();
        assert!(model.predict(&[f64::NAN, 4.0]).unwrap() > 0.9);
        for value in [0.0, 0.5, 1.0] {
            assert!(model.predict(&[value, 4.0]).unwrap() < 0.1, "{value}");
        }
    }

    fn halves() -> TrainingSet {
        let features: Vec<f64> = (0..300_u32).map(|i| f64::from(i % 10) / 10.0).collect();
        let labels = features
            .iter()
            .map(|&value| if value < 0.5 { 1.0 } else { 0.0 })
            .collect();
        TrainingSet::new(1, features, labels).unwrap()
    }

    /// Below 200 rows on a side, no split is made and the model predicts the mean, 0.5.
    #[rstest]
    #[case::by_default(None, 0.5, 0.5)]
    #[case::set_to_fewer_rows(
        Some(150),
        1.0 - 0.5 * 0.95_f64.powi(100),
        0.5 * 0.95_f64.powi(100)
    )]
    fn a_leaf_holds_200_rows_at_least_by_default(
        #[case] min_data_in_leaf: Option<u32>,
        #[case] below: f64,
        #[case] above: f64,
    ) {
        let config = LightGbmConfig::builder()
            .maybe_min_data_in_leaf(min_data_in_leaf)
            .build();
        let model = LearnerConfig::LightGbm(config).fit(&halves()).unwrap();

        for (value, expected) in [(0.2, below), (0.7, above)] {
            let predicted = model.predict(&[value]).unwrap();
            assert!((predicted - expected).abs() < 1e-6, "{value}: {predicted}");
        }
    }

    fn step() -> TrainingSet {
        let features: Vec<f64> = (0..600_u32).map(|i| f64::from(i % 10) / 10.0).collect();
        let labels = features
            .iter()
            .map(|&value| if value < 0.5 { 2.0 } else { -1.0 })
            .collect();
        TrainingSet::new(1, features, labels).unwrap()
    }

    #[rstest]
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

    #[rstest]
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
