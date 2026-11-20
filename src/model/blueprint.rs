//! A strategy's model blueprint (`ModelBlueprint`): what its config alone determines about its
//! model: Nautilus' strategy id, the shared feature set, its model config and its data
//! requirements. The decision rule and the target are checked against the requirements as the
//! blueprint is built, so nothing built of it checks them again.
//!
//! Everything of a strategy's model is built of its blueprint, the one caller of the dataset
//! fingerprint (`dataset::fingerprint`). The dataset, the model store and the schedule are built
//! together (`Stored`) on the one fingerprint, keying the dataset and every store; the training and
//! the walk-forward are built of them. A dataset alone and a model store alone, for a handle, which
//! has no catalog, are built of it too. The handle (`handle::ModelHandle`) is built of it and a
//! mode.

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
    /// The model of `strategy_id`, deciding on `features` as `config` says.
    ///
    /// # Errors
    ///
    /// Returns an error if the decision rule is invalid (`DecisionRule::validate`), or else the
    /// target for it and `requirements` (`TargetConfig::validate`).
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

    /// The fingerprint of its dataset runs, which keys its dataset and its stores.
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

    /// Its dataset in the catalog at `catalog`.
    ///
    /// # Errors
    ///
    /// Returns an error if `fingerprint` fails.
    pub fn dataset(&self, catalog: &Path) -> anyhow::Result<Dataset> {
        Ok(Dataset::new(catalog, self, self.fingerprint()?))
    }

    /// Its models under `models`.
    ///
    /// # Errors
    ///
    /// Returns an error if `fingerprint` fails, or if a learner cannot be written as JSON
    /// (`ModelStore::new`).
    pub fn model_store(&self, models: &Path) -> anyhow::Result<ModelStore> {
        ModelStore::new(models, self, self.fingerprint()?)
    }

    /// The training of its models.
    ///
    /// # Errors
    ///
    /// Returns an error if `model_store` would.
    pub fn training(&self, catalog: &Path, models: &Path) -> anyhow::Result<Training> {
        Ok(Training::new(self.stored(catalog, models)?))
    }

    /// The backtests of its models: the check of their start and their scores.
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

    /// A cadence not positive is refused with the decision rule's error, before the target's ATR.
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

    /// An ATR of an instrument the strategy does not read is refused, under the ATR's field.
    #[rstest]
    fn a_target_whose_atr_has_no_bars_to_be_built_from_is_refused() {
        let mut config = config();
        config.target.instrument_id = InstrumentId::from("EUR/USD.SIM");

        let err = format!("{:#}", blueprint(config).unwrap_err());

        assert!(err.contains("model.target.atr"), "{err}");
        assert!(err.contains("EUR/USD.SIM"), "{err}");
    }

    /// A model published under the dataset's fingerprint is the one the model store swaps in.
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
