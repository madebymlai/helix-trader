//! The fake, for tests: the row's first feature is the probability, so a
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
        Ok(if first.is_nan() {
            *mean
        } else {
            first.clamp(0.0, 1.0)
        })
    }

    fn save(&self) -> anyhow::Result<SavedModel> {
        Ok(SavedModel::from(serde_json::to_string(self)?))
    }
}

#[cfg(test)]
mod tests {
    use crate::model::learner::{LearnerConfig, TrainingSet};

    fn model() -> crate::model::learner::Model {
        let set = TrainingSet::new(2, vec![0.9; 8], vec![true, true, true, false]).unwrap();
        LearnerConfig::Fake {}.fit(&set).unwrap()
    }

    #[test]
    fn predicts_the_first_feature() {
        assert_eq!(model().predict(&[0.3, 0.8]).unwrap(), 0.3);
    }

    #[test]
    fn clamps_the_first_feature_to_a_probability() {
        assert_eq!(model().predict(&[-2.0, 0.5]).unwrap(), 0.0);
        assert_eq!(model().predict(&[1.5, 0.5]).unwrap(), 1.0);
    }

    #[test]
    fn predicts_the_training_mean_where_the_first_feature_is_missing() {
        assert_eq!(model().predict(&[f64::NAN, 0.5]).unwrap(), 0.75);
    }

    #[test]
    fn does_not_load_a_saved_baseline() {
        let set = TrainingSet::new(1, vec![0.0], vec![true]).unwrap();
        let baseline = LearnerConfig::Baseline {}.fit(&set).unwrap();
        assert!(
            LearnerConfig::Fake {}
                .load(&baseline.save().unwrap())
                .is_err()
        );
    }
}
