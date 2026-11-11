//! The fake, for tests: the row's first feature is the prediction, so a
//! test steers the model through its features.

use anyhow::Context;
use serde::{Deserialize, Serialize};

use super::{Learner, Predictor, SavedModel, TrainingSet};

pub(super) struct Fake;

impl Learner for Fake {
    fn fit(&self, set: &TrainingSet) -> anyhow::Result<Box<dyn Predictor>> {
        Ok(Box::new(Saved::Fake {
            width: set.width(),
            mean: set.mean(),
        }))
    }

    fn load(&self, saved: &SavedModel) -> anyhow::Result<Box<dyn Predictor>> {
        let saved: Saved =
            serde_json::from_str(saved.as_str()).context("not a saved fake model")?;
        Ok(Box::new(saved))
    }
}

/// The model and its saved text, JSON named by the adapter, so that no
/// other adapter loads it. `mean` is that of the training labels, the
/// prediction where the first feature is missing.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Saved {
    Fake { width: usize, mean: f64 },
}

impl Predictor for Saved {
    fn width(&self) -> usize {
        let Self::Fake { width, .. } = self;
        *width
    }

    fn predict(&self, row: &[f64]) -> anyhow::Result<f64> {
        let Self::Fake { mean, .. } = self;
        let first = row[0];
        Ok(if first.is_nan() { *mean } else { first })
    }

    fn save(&self) -> anyhow::Result<SavedModel> {
        Ok(SavedModel::from(serde_json::to_string(self)?))
    }
}

#[cfg(test)]
mod tests {
    use crate::model::learner::{LearnerConfig, TrainingSet};

    /// Fitted on labels whose mean is (2.5 - 1.0 + 3.5 + 2.0) / 4 = 1.75.
    fn model() -> crate::model::learner::Model {
        let set = TrainingSet::new(2, vec![0.9; 8], vec![2.5, -1.0, 3.5, 2.0]).unwrap();
        LearnerConfig::Fake {}.fit(&set).unwrap()
    }

    #[test]
    fn predicts_the_first_feature() {
        for first in [0.3, -2.0, 1.5] {
            assert_eq!(model().predict(&[first, 0.8]).unwrap(), first);
        }
    }

    #[test]
    fn predicts_the_training_mean_where_the_first_feature_is_missing() {
        assert_eq!(model().predict(&[f64::NAN, 0.5]).unwrap(), 1.75);
    }

    #[test]
    fn does_not_load_a_saved_baseline() {
        let set = TrainingSet::new(1, vec![0.0], vec![1.0]).unwrap();
        let baseline = LearnerConfig::Baseline {}.fit(&set).unwrap();
        assert!(
            LearnerConfig::Fake {}
                .load(&baseline.save().unwrap())
                .is_err()
        );
    }
}
