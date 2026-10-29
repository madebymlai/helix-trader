//! PerpetualBooster: gradient-boosted trees that stop by a budget instead
//! of tuned settings, with the log-loss objective.

use anyhow::{Context, anyhow, ensure};
use perpetual::booster::config::BoosterIO;
use perpetual::objective::Objective;
use perpetual::{Matrix, PerpetualBooster};
use serde::Deserialize;

use super::{Learner, Predictor, SavedModel, TrainingSet};

/// PerpetualBooster's settings, each named and meant as in the library.
#[derive(Debug, Clone, PartialEq, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct PerpetualConfig {
    /// The memory a fit may take, in GB. Required: unbounded, a fit grows
    /// until the machine runs out, past 8 GB on a modest set. A fit refuses
    /// a limit of zero or less, on which the library panics.
    pub memory_limit: f32,
    /// How far boosting goes: a higher budget fits more and finer trees.
    #[serde(default = "default_budget")]
    #[builder(default = default_budget())]
    pub budget: f32,
    /// The most boosting rounds, below the budget's own stop; none if
    /// unset.
    pub iteration_limit: Option<usize>,
    /// The seed of every draw.
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

/// The metadata key under which a saved model keeps the width of its rows,
/// which PerpetualBooster does not record itself.
const WIDTH: &str = "width";

impl PerpetualConfig {
    /// A booster with these settings. It fits on one thread, as LightGBM
    /// does, so its results do not depend on the machine's cores, and its
    /// splits give missing values a branch of their own, so a missing
    /// feature is learned apart from every value, as in LightGBM.
    fn booster(&self) -> PerpetualBooster {
        PerpetualBooster::default()
            .set_objective(Objective::LogLoss)
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
        ensure!(
            self.memory_limit > 0.0,
            "a memory limit of {} GB leaves PerpetualBooster nothing to fit in",
            self.memory_limit
        );
        let width = set.width();
        let rows: Vec<&[f64]> = set.rows().collect();
        // PerpetualBooster reads a matrix column after column.
        let columns: Vec<f64> = (0..width)
            .flat_map(|column| rows.iter().map(move |row| row[column]))
            .collect();
        let labels: Vec<f64> = set
            .labels()
            .iter()
            .map(|&label| f64::from(u8::from(label)))
            .collect();
        let mut booster = self.booster();
        booster.fit(
            &Matrix::new(&columns, rows.len(), width),
            &labels,
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

/// A PerpetualBooster and the width of its rows.
struct Perpetual {
    booster: PerpetualBooster,
    width: usize,
}

impl Perpetual {
    fn new(booster: PerpetualBooster) -> anyhow::Result<Self> {
        let width = booster
            .metadata
            .get(WIDTH)
            .ok_or_else(|| anyhow!("a PerpetualBooster model without its width"))?
            .parse()
            .context("the width of a PerpetualBooster model")?;
        ensure!(width > 0, "a PerpetualBooster model without features");
        Ok(Self { booster, width })
    }
}

impl Predictor for Perpetual {
    fn width(&self) -> usize {
        self.width
    }

    fn predict(&self, row: &[f64]) -> anyhow::Result<f64> {
        let matrix = Matrix::new(row, 1, self.width);
        Ok(self.booster.predict_proba(&matrix, false, false)[0])
    }

    fn save(&self) -> anyhow::Result<SavedModel> {
        Ok(SavedModel::from(self.booster.json_dump()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::learner::LearnerConfig;

    fn config() -> PerpetualConfig {
        PerpetualConfig::builder().memory_limit(1.0).build()
    }

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
        let model = LearnerConfig::Perpetual(config())
            .fit(&missing_is_true())
            .unwrap();
        assert!(model.predict(&[f64::NAN, 4.0]).unwrap() > 0.9);
        for value in [0.0, 0.5, 1.0] {
            assert!(model.predict(&[value, 4.0]).unwrap() < 0.1, "{value}");
        }
    }

    #[test]
    fn a_memory_limit_of_nothing_is_refused_at_fit() {
        for memory_limit in [0.0, -1.0] {
            let config = PerpetualConfig::builder()
                .memory_limit(memory_limit)
                .build();
            assert!(
                LearnerConfig::Perpetual(config)
                    .fit(&missing_is_true())
                    .is_err(),
                "{memory_limit}"
            );
        }
    }

    // PerpetualBooster draws only on a large set, where it fits each tree
    // on a sample of the rows: 50,000 rows of 6 features are one.
    #[test]
    fn the_seed_reaches_perpetual() {
        let set = TrainingSet::new(
            6,
            (0..300_000_u32)
                .map(|i| f64::from((i * 7919) % 101) / 100.0)
                .collect(),
            (0..50_000_u32).map(|i| (i * 31) % 7 < 3).collect(),
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
