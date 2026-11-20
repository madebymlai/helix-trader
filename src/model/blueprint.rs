//! The model blueprint of a strategy (`ModelBlueprint`).
//!
//! The blueprint holds what the strategy's config alone determines about its model:
//! - Nautilus' strategy id.
//! - The shared feature set.
//! - The model config.
//! - The data requirements.
//!
//! The blueprint checks the decision rule and the target against the requirements as it is built,
//! so nothing built from it checks them again.
//!
//! Every part of the strategy's model is built from its blueprint, the only caller of the dataset
//! fingerprint (`dataset::fingerprint`).
//! - The dataset, the model store and the schedule are built together (`Stored`) on one
//!   fingerprint, which keys the dataset and every store.
//! - The training and the walk-forward are built from them.
//! - A handle has no catalog, so its dataset alone and its model store alone are built from the
//!   blueprint too.
//! - The handle (`handle::ModelHandle`) is built from the blueprint and a mode.

use std::{path::Path, rc::Rc};

use nautilus_model::identifiers::StrategyId;

use super::{
    config::ModelConfig,
    dataset::{self, Dataset},
    features::{FeatureSet, Fingerprint},
    schedule::Schedule,
    store::ModelStore,
    train::Training,
    walk::WalkForward,
};
use crate::strategy::requirements::DataRequirements;

/// A strategy's model as its config alone determines it.
#[derive(Debug)]
pub struct ModelBlueprint {
    strategy_id: StrategyId,
    features: Rc<FeatureSet>,
    config: ModelConfig,
    requirements: DataRequirements,
}

#[derive(Debug)]
pub(super) struct Stored<'a> {
    pub(super) blueprint: &'a ModelBlueprint,
    pub(super) dataset: Dataset,
    pub(super) models: ModelStore,
    pub(super) schedule: Schedule,
}

impl ModelBlueprint {
    /// Returns the model of `strategy_id`, which decides on `features` as `config` says.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The decision rule is invalid (`DecisionRule::validate`).
    /// - The target is invalid for the decision rule and `requirements`
    ///   (`TargetConfig::validate`).
    pub fn new(
        strategy_id: StrategyId,
        features: Rc<FeatureSet>,
        config: ModelConfig,
        requirements: DataRequirements,
    ) -> anyhow::Result<Self> {
        config.decision.validate()?;
        config.target.validate(config.decision, &requirements)?;
        Ok(Self {
            strategy_id,
            features,
            config,
            requirements,
        })
    }

    /// Returns the fingerprint of the dataset runs. It keys the dataset and the stores.
    ///
    /// # Errors
    ///
    /// Returns an error if `dataset::fingerprint` fails.
    pub fn fingerprint(&self) -> anyhow::Result<Fingerprint> {
        dataset::fingerprint(
            &self.features,
            self.config.decision,
            &self.config.target,
            &self.requirements,
            &self.config.dataset,
        )
    }

    /// Returns the dataset in the catalog at `catalog`.
    ///
    /// # Errors
    ///
    /// Returns an error if `fingerprint` fails.
    pub fn dataset(&self, catalog: &Path) -> anyhow::Result<Dataset> {
        Ok(Dataset::new(catalog, self, self.fingerprint()?))
    }

    /// Returns the models under `models`.
    ///
    /// # Errors
    ///
    /// Returns an error if `fingerprint` fails, or if a learner cannot be written as JSON
    /// (`ModelStore::new`).
    pub fn model_store(&self, models: &Path) -> anyhow::Result<ModelStore> {
        ModelStore::new(models, self, self.fingerprint()?)
    }

    /// Returns the training of the models.
    ///
    /// # Errors
    ///
    /// Returns an error if `model_store` would.
    pub fn training(&self, catalog: &Path, models: &Path) -> anyhow::Result<Training> {
        Ok(Training::new(self.stored(catalog, models)?))
    }

    /// Returns the backtests of the models: the check of their start and their scores.
    ///
    /// # Errors
    ///
    /// Returns an error if `model_store` would.
    pub fn walk_forward(&self, catalog: &Path, models: &Path) -> anyhow::Result<WalkForward> {
        Ok(WalkForward::new(self.stored(catalog, models)?))
    }

    pub(super) fn stored(&self, catalog: &Path, models: &Path) -> anyhow::Result<Stored<'_>> {
        let fingerprint = self.fingerprint()?;
        Ok(Stored {
            blueprint: self,
            models: ModelStore::new(models, self, fingerprint)?,
            dataset: Dataset::new(catalog, self, fingerprint),
            schedule: Schedule::of_model(&self.config),
        })
    }

    pub(super) const fn strategy_id(&self) -> StrategyId {
        self.strategy_id
    }

    pub(super) const fn features(&self) -> &Rc<FeatureSet> {
        &self.features
    }

    pub(super) const fn config(&self) -> &ModelConfig {
        &self.config
    }

    pub(super) const fn requirements(&self) -> &DataRequirements {
        &self.requirements
    }
}

#[cfg(test)]
mod tests {
    use jiff::SignedDuration;
    use nautilus_model::identifiers::InstrumentId;
    use rstest::rstest;

    use super::*;
    use crate::model::{
        decide::DecisionRule,
        deciding::testing::{STRATEGY, at, baseline, config, features, requirements},
    };

    fn blueprint(config: ModelConfig) -> anyhow::Result<ModelBlueprint> {
        ModelBlueprint::new(
            StrategyId::from(STRATEGY),
            features(),
            config,
            requirements(),
        )
    }

    /// Refused with the decision rule's error, before the ATR check.
    #[rstest]
    fn a_decision_cadence_not_positive_is_refused() {
        let config = ModelConfig {
            decision: DecisionRule::Every(SignedDuration::ZERO),
            ..config()
        };

        let err = format!("{:#}", blueprint(config).unwrap_err());

        assert!(err.contains("0s"), "{err}");
        assert!(!err.contains("model.target.atr"), "{err}");
    }

    #[rstest]
    fn a_target_whose_atr_has_no_bars_to_be_built_from_is_refused() {
        let mut config = config();
        config.target.instrument_id = InstrumentId::from("EUR/USD.SIM");

        let err = format!("{:#}", blueprint(config).unwrap_err());

        assert!(err.contains("model.target.atr"), "{err}");
        assert!(err.contains("EUR/USD.SIM"), "{err}");
    }

    #[rstest]
    fn the_dataset_and_the_model_store_of_a_model_stored_carry_one_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let blueprint = blueprint(config()).unwrap();
        let model = blueprint
            .stored(&dir.path().join("catalog"), &dir.path().join("models"))
            .unwrap();
        let mut published = baseline("2026-10-22T00:00:00Z", 8);
        published.sides[0].manifest.fingerprint = model.dataset.fingerprint().to_string();
        model.models.publish(&published).unwrap();
        let mut model_sets = model.models.swap();

        let in_use = model_sets
            .at(
                at("2026-10-22T12:00:00Z"),
                &model.models.model_sets().unwrap(),
            )
            .unwrap();

        assert_eq!(
            in_use.map(|(valid_from, _)| valid_from),
            Some(at("2026-10-22T00:00:00Z"))
        );
    }
}
