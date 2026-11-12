//! A strategy's model blueprint (`ModelBlueprint`): what its config alone
//! determines about its model, Nautilus' strategy id, the feature set the
//! strategy shares, its model config and its data requirements, each as
//! it is. Its decision rule and its target are checked as it is built,
//! against its requirements, so a blueprint is of a valid model, and
//! nothing built of it checks them again.
//!
//! Everything of a strategy's model is built of its blueprint, which is
//! the one caller of the dataset fingerprint (`dataset::fingerprint`),
//! given the venue of the dataset runs. On one venue its dataset, its
//! model set and its schedule are built together (`OnVenue`), the
//! fingerprint computed once and keying the dataset and every store, and
//! the training and the walk-forward are each built of them; a dataset
//! alone, for a dataset build, and a model set alone, for a handle, which
//! has no catalog, are built of the blueprint as well. The strategy's
//! model handle (`handle::ModelHandle`) is built of it and a mode, and
//! registers the very feature set the dataset and the stores are
//! fingerprinted from. The venue and the paths are not the blueprint's: a
//! handle in a dataset run has a commission and no venue.

use std::{path::Path, rc::Rc};

use nautilus_model::identifiers::StrategyId;

use super::{
    config::ModelConfig,
    dataset::{self, Dataset, DatasetVenue},
    features::{FeatureSet, Fingerprint},
    schedule::Schedule,
    store::ModelSet,
    train::Training,
    walk::WalkForward,
};
use crate::strategy::DataRequirements;

/// A strategy's model as its config alone determines it, of a valid
/// decision rule and target (`new`).
#[derive(Debug)]
pub struct ModelBlueprint {
    strategy_id: StrategyId,
    /// The features its model decides on, shared with the strategy.
    features: Rc<FeatureSet>,
    config: ModelConfig,
    requirements: DataRequirements,
}

/// A strategy's model on the venue of its dataset runs: its dataset in a
/// catalog and its model set under a models root, both of the one
/// fingerprint of its blueprint on that venue, and its schedule.
#[derive(Debug)]
pub(super) struct OnVenue<'a> {
    /// The blueprint it is the model of.
    pub(super) blueprint: &'a ModelBlueprint,
    pub(super) dataset: Dataset,
    pub(super) models: ModelSet,
    pub(super) schedule: Schedule,
}

impl ModelBlueprint {
    /// The model of `strategy_id`, deciding on `features` as `config`
    /// says, for a strategy of `requirements`.
    ///
    /// # Errors
    ///
    /// Returns an error if the decision rule is invalid
    /// (`DecisionRule::validate`), or else the target for it and
    /// `requirements` (`TargetConfig::validate`).
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

    /// The fingerprint of its dataset run on `venue`
    /// (`dataset::fingerprint`), which keys its dataset and its stores.
    ///
    /// # Errors
    ///
    /// Returns an error as `dataset::fingerprint` does.
    pub fn fingerprint(&self, venue: &DatasetVenue) -> anyhow::Result<Fingerprint> {
        dataset::fingerprint(
            &self.features,
            self.config.decision,
            &self.config.target,
            &self.requirements,
            venue,
        )
    }

    /// Its dataset, run on `venue`, in the catalog at `catalog`.
    ///
    /// # Errors
    ///
    /// Returns an error as `fingerprint` does.
    pub fn dataset(&self, catalog: &Path, venue: &DatasetVenue) -> anyhow::Result<Dataset> {
        Ok(Dataset::new(catalog, self, self.fingerprint(venue)?))
    }

    /// Its models under `models`, of its dataset run on `venue`.
    ///
    /// # Errors
    ///
    /// Returns an error as `fingerprint` does, or if a learner cannot be
    /// written as JSON (`ModelSet::new`).
    pub fn model_set(&self, models: &Path, venue: &DatasetVenue) -> anyhow::Result<ModelSet> {
        ModelSet::new(models, self, self.fingerprint(venue)?)
    }

    /// The training of its models, on its dataset run on `venue`, in the
    /// catalog at `catalog`, and its models in the store under `models`.
    ///
    /// # Errors
    ///
    /// Returns an error as `model_set` does.
    pub fn training(
        &self,
        catalog: &Path,
        models: &Path,
        venue: &DatasetVenue,
    ) -> anyhow::Result<Training> {
        Ok(Training::new(self.on_venue(catalog, models, venue)?))
    }

    /// The backtests of its models, on its dataset run on `venue`, in the
    /// catalog at `catalog`, and its models in the store under `models`:
    /// their retrains and their scores.
    ///
    /// # Errors
    ///
    /// Returns an error as `model_set` does.
    pub fn walk_forward(
        &self,
        catalog: &Path,
        models: &Path,
        venue: &DatasetVenue,
    ) -> anyhow::Result<WalkForward> {
        Ok(WalkForward::new(self.on_venue(catalog, models, venue)?))
    }

    /// Its model on `venue`: its dataset in the catalog at `catalog` and
    /// its models under `models`, of the fingerprint computed once, and its
    /// schedule (`Schedule::of_model`).
    ///
    /// # Errors
    ///
    /// Returns an error as `model_set` does.
    pub(super) fn on_venue(
        &self,
        catalog: &Path,
        models: &Path,
        venue: &DatasetVenue,
    ) -> anyhow::Result<OnVenue<'_>> {
        let fingerprint = self.fingerprint(venue)?;
        Ok(OnVenue {
            blueprint: self,
            models: ModelSet::new(models, self, fingerprint)?,
            dataset: Dataset::new(catalog, self, fingerprint),
            schedule: Schedule::of_model(&self.config),
        })
    }

    pub(super) const fn strategy_id(&self) -> StrategyId {
        self.strategy_id
    }

    /// The features its model decides on, shared with the strategy.
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
    use nautilus_model::{identifiers::InstrumentId, types::Quantity};

    use super::*;
    use crate::model::{
        decide::DecisionRule,
        deciding::testing::{STRATEGY, at, baseline, config, features, requirements, venue},
        targets::Side,
    };

    fn blueprint(config: ModelConfig) -> anyhow::Result<ModelBlueprint> {
        ModelBlueprint::new(
            StrategyId::from(STRATEGY),
            features(),
            config,
            requirements(),
        )
    }

    /// A cadence not positive is refused as the blueprint is built, with
    /// the decision rule's error, before the target's ATR, which has no
    /// bars of that cadence.
    #[test]
    fn a_decision_cadence_not_positive_is_refused() {
        let config = ModelConfig {
            decision: DecisionRule::Every(SignedDuration::ZERO),
            ..config()
        };

        let err = blueprint(config).unwrap_err();

        assert_eq!(
            format!("{err:#}"),
            "the decision interval 0s is not positive"
        );
    }

    /// A target of no size is refused as the blueprint is built, under its
    /// field.
    #[test]
    fn a_target_of_no_size_is_refused() {
        let mut config = config();
        config.target.quantity = Quantity::from(0);

        let err = blueprint(config).unwrap_err();

        assert_eq!(
            format!("{err:#}"),
            "invalid model.target: the target quantity must be positive"
        );
    }

    /// The target is checked against what the strategy reads: an ATR of an
    /// instrument whose prices it does not read is refused as the
    /// blueprint is built, under the ATR's field.
    #[test]
    fn a_target_whose_atr_has_no_bars_to_be_built_from_is_refused() {
        let mut config = config();
        config.target.instrument_id = InstrumentId::from("EUR/USD.SIM");

        let err = blueprint(config).unwrap_err();

        assert_eq!(
            format!("{err:#}"),
            "invalid model.target.atr: the target's ATR has no bars of EUR/USD.SIM to be \
             built from: the strategy reads neither its quotes, its trades nor its bars"
        );
    }

    /// The dataset and the model set of a model on a venue carry one
    /// fingerprint: a model published under the dataset's, as a train
    /// publishes it, is the one the model set swaps in.
    #[test]
    fn the_dataset_and_the_model_set_on_a_venue_carry_one_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let blueprint = blueprint(config()).unwrap();
        let model = blueprint
            .on_venue(
                &dir.path().join("catalog"),
                &dir.path().join("models"),
                &venue(),
            )
            .unwrap();
        let mut published = baseline("2026-10-22T00:00:00Z", 8);
        published.manifest.fingerprint = model.dataset.fingerprint().to_string();
        let store = model.models.side(Side::Long).unwrap().store();
        store.publish(&published).unwrap();
        let mut pairs = model.models.pairs();

        let in_use = pairs.at(at("2026-10-22T12:00:00Z"), &store.valid_froms().unwrap());

        assert_eq!(
            in_use.map(|(valid_from, _)| valid_from),
            Some(at("2026-10-22T00:00:00Z"))
        );
    }
}
