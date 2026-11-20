//! PerpetualBooster trains gradient-boosted trees on squared loss. It stops at a budget, not at
//! tuned settings. The budget stands in for LightGBM's settings, which PerpetualBooster has no
//! equivalent of.

use anyhow::Context;
use perpetual::{Matrix, PerpetualBooster, booster::config::BoosterIO, objective::Objective};
use serde::{Deserialize, Serialize};

use super::{Learner, Predictor, SavedModel, TrainingSet};

/// PerpetualBooster's settings, named and meant as in the library.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct PerpetualConfig {
    /// The memory a fit may take, in GB.
    ///
    /// Required, because unbounded a fit grows past 8 GB on a modest set. A fit refuses zero or
    /// less, which makes the library panic.
    pub memory_limit: f32,
    /// How far boosting goes. A higher budget fits more and finer trees.
    #[serde(default = "default_budget")]
    #[builder(default = default_budget())]
    pub budget: f32,
    /// The most boosting rounds, applied below the budget's own stop. No limit if unset.
    pub iteration_limit: Option<usize>,
    /// The seed for every draw.
    #[serde(default = "default_seed")]
    #[builder(default = default_seed())]
    pub seed: u64,
}

const fn default_budget() -> f32 {
    0.5
}

const fn default_seed() -> u64 {
    1
}

/// The metadata key for the row width of a saved model. PerpetualBooster does not record the width.
const WIDTH: &str = "width";

impl PerpetualConfig {
    /// Builds a booster of squared loss.
    ///
    /// It fits on one thread, as LightGBM does, so results do not depend on the cores. Its splits
    /// give missing values a branch of their own.
    fn booster(&self) -> PerpetualBooster {
        PerpetualBooster::default()
            .set_objective(Objective::SquaredLoss)
            .set_budget(self.budget)
            .set_memory_limit(Some(self.memory_limit))
            .set_iteration_limit(self.iteration_limit)
            .set_seed(self.seed)
            .set_num_threads(Some(1))
            .set_create_missing_branch(true)
    }
}

impl Learner for PerpetualConfig {
    fn fit(&self, set: &TrainingSet) -> anyhow::Result<Box<dyn Predictor>> {
        anyhow::ensure!(
            self.memory_limit > 0.0,
            "a memory limit of {} GB leaves PerpetualBooster nothing to fit in",
            self.memory_limit
        );
        let width = set.width();
        let rows: Vec<&[f64]> = set.rows().collect();

        // PerpetualBooster reads a matrix column by column
        let columns: Vec<f64> = (0..width)
            .flat_map(|column| rows.iter().map(move |row| row[column]))
            .collect();
        let mut booster = self.booster();
        booster.fit(
            &Matrix::new(&columns, rows.len(), width),
            set.labels(),
            None,
            None,
        )?;
        booster.insert_metadata(WIDTH.to_string(), width.to_string());
        Ok(Box::new(Perpetual::new(booster)?))
    }

    fn load(&self, saved: &SavedModel) -> anyhow::Result<Box<dyn Predictor>> {
        let booster = PerpetualBooster::from_json(saved.as_str())
            .context("not a saved PerpetualBooster model")?;
        Ok(Box::new(Perpetual::new(booster)?))
    }
}

struct Perpetual {
    booster: PerpetualBooster,
    width: usize,
}

impl Perpetual {
    fn new(booster: PerpetualBooster) -> anyhow::Result<Self> {
        let width = booster
            .metadata
            .get(WIDTH)
            .ok_or_else(|| anyhow::anyhow!("a PerpetualBooster model without its width"))?
            .parse()
            .context("the width of a PerpetualBooster model")?;
        anyhow::ensure!(width > 0, "a PerpetualBooster model without features");
        anyhow::ensure!(
            matches!(booster.cfg.objective, Objective::SquaredLoss),
            "a PerpetualBooster model of a loss other than squared loss"
        );
        Ok(Self { booster, width })
    }
}

impl Predictor for Perpetual {
    fn width(&self) -> usize {
        self.width
    }

    fn predict(&self, row: &[f64]) -> anyhow::Result<f64> {
        let matrix = Matrix::new(row, 1, self.width);
        Ok(self.booster.predict(&matrix, false)[0])
    }

    fn save(&self) -> anyhow::Result<SavedModel> {
        Ok(SavedModel::from(self.booster.json_dump()?))
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::model::learner::LearnerConfig;

    fn config() -> PerpetualConfig {
        PerpetualConfig::builder().memory_limit(1.0).build()
    }

    /// 300 rows of two features. The label is 1 where the first feature is missing. Otherwise the
    /// label is 0 and the feature runs from 0 to 1.
    fn missing_is_one() -> TrainingSet {
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
            labels.push(if missing { 1.0 } else { 0.0 });
        }
        TrainingSet::new(2, features, labels).unwrap()
    }

    /// A model blind to the missing feature predicts the mean label, a third, for every row.
    ///
    /// A model that learns the missing feature apart predicts above the mean for it and below the
    /// mean for every value.
    #[rstest]
    fn a_missing_feature_is_learned_apart_from_every_value() {
        let model = LearnerConfig::Perpetual(config())
            .fit(&missing_is_one())
            .unwrap();
        let mean = 1.0 / 3.0;
        assert!(model.predict(&[f64::NAN, 4.0]).unwrap() > mean);
        for value in [0.0, 0.5, 1.0] {
            assert!(model.predict(&[value, 4.0]).unwrap() < mean, "{value}");
        }
    }

    /// 600 rows of one feature, from 0.0 to 0.9 by 0.1. The label is 2 below 0.5 and -1 from it.
    fn step() -> TrainingSet {
        let features: Vec<f64> = (0..600_u32).map(|i| f64::from(i % 10) / 10.0).collect();
        let labels = features
            .iter()
            .map(|&value| if value < 0.5 { 2.0 } else { -1.0 })
            .collect();
        TrainingSet::new(1, features, labels).unwrap()
    }

    #[rstest]
    fn a_step_of_real_labels_is_learned_on_its_squared_loss() {
        let model = LearnerConfig::Perpetual(config()).fit(&step()).unwrap();
        for (value, expected) in [(0.2, 2.0), (0.7, -1.0)] {
            let predicted = model.predict(&[value]).unwrap();
            assert!((predicted - expected).abs() < 0.01, "{value}: {predicted}");
        }
    }

    #[rstest]
    #[case::of_nought(0.0, "0 GB")]
    #[case::below_nought(-1.0, "-1 GB")]
    fn a_memory_limit_of_nothing_is_refused_at_fit(#[case] memory_limit: f32, #[case] limit: &str) {
        let config = PerpetualConfig::builder()
            .memory_limit(memory_limit)
            .build();

        let err = LearnerConfig::Perpetual(config)
            .fit(&missing_is_one())
            .unwrap_err()
            .to_string();

        assert!(err.contains(limit), "{err}");
    }

    // PerpetualBooster draws only on a large set, where each tree fits a sample of the rows, and
    // 50,000 rows of 6 features are a large set.
    #[rstest]
    fn the_seed_reaches_perpetual() {
        let set = TrainingSet::new(
            6,
            (0..300_000_u32)
                .map(|i| f64::from((i * 7919) % 101) / 100.0)
                .collect(),
            (0..50_000_u32)
                .map(|i| if (i * 31) % 7 < 3 { 1.0 } else { 0.0 })
                .collect(),
        )
        .unwrap();
        let fit = |seed| {
            let config = PerpetualConfig::builder()
                .memory_limit(1.0)
                .iteration_limit(3)
                .seed(seed)
                .build();
            let model = LearnerConfig::Perpetual(config).fit(&set).unwrap();
            model
                .predict(&[0.2, 0.5, 0.7, 0.1, 0.9, 0.4])
                .unwrap()
                .to_bits()
        };
        assert_ne!(fit(1), fit(2));
    }
}
