//! LightGBM: gradient-boosted trees with the binary objective.

use anyhow::{Context, ensure};
use lightgbm3::{Booster, Dataset};
use serde::Deserialize;

use super::{Learner, Predictor, SavedModel, TrainingSet};

/// LightGBM's settings, each named and meant as in LightGBM's parameters.
#[derive(Debug, Clone, PartialEq, Deserialize, bon::Builder)]
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
    /// The fewest rows in a leaf.
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
    0.1
}

const fn default_num_leaves() -> u32 {
    31
}

const fn default_min_data_in_leaf() -> u32 {
    20
}

const fn default_feature_fraction() -> f64 {
    1.0
}

const fn default_seed() -> u32 {
    1
}

impl LightGbmConfig {
    /// The settings as LightGBM's parameters. LightGBM, built without
    /// OpenMP, trains on one thread; `deterministic` and `force_row_wise`
    /// keep its results from depending on the machine.
    fn parameters(&self) -> serde_json::Value {
        serde_json::json!({
            "objective": "binary",
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
        let labels: Vec<f32> = set
            .labels()
            .iter()
            .map(|&label| f32::from(u8::from(label)))
            .collect();
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

    /// 300 rows of two features whose label is true exactly where the
    /// first feature is missing; elsewhere it runs from 0 to 1, zero
    /// included.
    fn missing_is_true() -> TrainingSet {
        let mut features = Vec::new();
        let mut labels = Vec::new();
        for i in 0..300_u32 {
            let missing = i % 3 == 0;
            features.push(if missing {
                f64::NAN
            } else {
                f64::from(i % 7) / 6.0
            });
            features.push(f64::from(i % 11));
            labels.push(missing);
        }
        TrainingSet::new(2, features, labels).unwrap()
    }

    #[test]
    fn a_missing_feature_is_learned_apart_from_every_value() {
        let config = LearnerConfig::LightGbm(LightGbmConfig::builder().build());
        let model = config.fit(&missing_is_true()).unwrap();
        assert!(model.predict(&[f64::NAN, 4.0]).unwrap() > 0.9);
        for value in [0.0, 0.5, 1.0] {
            assert!(model.predict(&[value, 4.0]).unwrap() < 0.1, "{value}");
        }
    }

    #[test]
    fn the_seed_reaches_lightgbm() {
        let fit = |seed| {
            let config = LightGbmConfig::builder()
                .feature_fraction(0.5)
                .seed(seed)
                .build();
            let set = TrainingSet::new(
                3,
                (0..600_u32)
                    .map(|i| f64::from((i * 7919) % 101) / 100.0)
                    .collect(),
                (0..200_u32).map(|i| (i * 31) % 7 < 3).collect(),
            )
            .unwrap();
            let model = LearnerConfig::LightGbm(config).fit(&set).unwrap();
            model.predict(&[0.2, 0.5, 0.7]).unwrap().to_bits()
        };
        assert_ne!(fit(1), fit(2));
    }
}
