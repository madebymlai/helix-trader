//! The baseline: the mean of the training labels, whatever the row.

use anyhow::Context;
use serde::{Deserialize, Serialize};

use super::{Learner, Predictor, SavedModel, TrainingSet};

pub(super) struct Baseline;

impl Learner for Baseline {
    fn fit(&self, set: &TrainingSet) -> anyhow::Result<Box<dyn Predictor>> {
        Ok(Box::new(Saved::Baseline {
            width: set.width(),
            mean: set.mean(),
        }))
    }

    fn load(&self, saved: &SavedModel) -> anyhow::Result<Box<dyn Predictor>> {
        let saved: Saved =
            serde_json::from_str(saved.as_str()).context("not a saved baseline model")?;
        Ok(Box::new(saved))
    }
}

/// The model and its saved text, JSON named by the adapter, so that no
/// other adapter loads it.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum Saved {
    Baseline { width: usize, mean: f64 },
}

impl Predictor for Saved {
    fn width(&self) -> usize {
        let Self::Baseline { width, .. } = self;
        *width
    }

    fn predict(&self, _row: &[f64]) -> anyhow::Result<f64> {
        let Self::Baseline { mean, .. } = self;
        Ok(*mean)
    }

    fn save(&self) -> anyhow::Result<SavedModel> {
        Ok(SavedModel::from(serde_json::to_string(self)?))
    }
}

#[cfg(test)]
mod tests {
    use crate::model::learner::{LearnerConfig, TrainingSet};

    #[test]
    fn predicts_the_share_of_true_labels_whatever_the_row() {
        let set = TrainingSet::new(
            2,
            vec![0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0],
            vec![true, false, false, false],
        )
        .unwrap();
        let model = LearnerConfig::Baseline {}.fit(&set).unwrap();
        for row in [[0.0, 1.0], [100.0, -5.0], [f64::NAN, f64::NAN]] {
            assert_eq!(model.predict(&row).unwrap(), 0.25, "{row:?}");
        }
    }
}
