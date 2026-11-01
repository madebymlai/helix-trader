//! Meta-labeling: a primary picks the side of a trade, and a meta model
//! decides whether to take it, and optionally its size.
//!
//! The primary is the strategy's own rule (`RuleConfig`), which decides on
//! a side only when it signals (`ModelHandle::decide_on`), or a model
//! (`ModelConfig` with a meta section), whose side is the one its decision
//! rule picks, on the expected net P&L of each side's calibrated
//! probability (`payoff`). Either way the meta model decides only when the
//! primary signals, on the primary's features with its side and the
//! chosen side's probability after them (`PRIMARY_FEATURES`; a rule's
//! probability is missing, NaN), and learns whether the triple-barrier
//! trade on the primary's side wins after its costs
//! (`TargetsConfig::pnl_index`): a dataset row opens a long and a short at
//! every decision, so the row has the trade of either side, and the meta's
//! rows need no replay of their own.
//!
//! A rule primary's rows are its decisions, its features carrying its
//! side. A model primary's rows are its dataset's, each decided by the
//! models of each side in use at it as a strategy would have used them
//! (`store::Swap`): the latest valid then, trained on rows settled by its
//! valid-from, so every decision is out of sample; the worth of each
//! side's stop is the row's own (`payoff::Realised::unit`). A row before
//! the primary's first model has no decision: the meta's rows start
//! there at the earliest, and with no primary model before a train the
//! meta waits (`schedule::Schedule::plan`). Never retrained, with neither
//! in use, one train fits the primary as of the meta's window before it
//! and the meta at it, on that primary's decisions over the window: the
//! meta filters the very primary it learned. A row the primary makes no
//! trade of is no row of the meta; a long-only primary gives it buys
//! alone.
//!
//! Two levels at most: a meta section has no meta of its own (`config`).

use std::path::Path;

use anyhow::{Context, ensure};
use jiff::Timestamp;
use nautilus_model::{enums::OrderSide, identifiers::StrategyId};

use super::{
    config::{MetaConfig, ModelConfig, RuleConfig},
    dataset::{self, DatasetRow, DatasetVenue},
    features::{FeatureSet, PRIMARY_FEATURES, primary_values},
    holdout::Trade,
    payoff::{self, Realised},
    store::{ModelStore, Swap},
    targets::{Label, Side, TargetsConfig},
    train::Example,
};
use crate::strategy::DataRequirements;

/// A strategy's meta model: how its rows are made from the primary's, and
/// its store.
#[derive(Debug)]
pub struct Meta {
    config: MetaConfig,
    primary: Primary,
    /// The names of the meta's features, the primary's side and
    /// probability last.
    feature_names: Vec<String>,
    /// Where the barrier's P&L and commission of a long and of a short are
    /// among a row's target values.
    trades: [(usize, usize); 2],
    store: ModelStore,
}

/// The primary of a meta model.
#[derive(Debug)]
enum Primary {
    /// The strategy's rule: its side is in its rows' features.
    Rule,
    /// A model, whose sides' models are swapped in as the strategy does.
    Model {
        /// Each side traded, its margin and its store.
        sides: Vec<(Side, f64, ModelStore)>,
        feature_names: Vec<String>,
        fingerprint: String,
    },
}

impl Meta {
    /// The meta model of `config`, the model of `strategy_id` deciding
    /// with `features` on targets for a strategy of `requirements`,
    /// run on `venue`, its models in the store under `models`; none if
    /// `config` has no meta section.
    ///
    /// # Errors
    ///
    /// Returns an error if the targets have no barrier, or a store cannot
    /// be keyed.
    pub fn of_model(
        models: &Path,
        strategy_id: StrategyId,
        features: &FeatureSet,
        config: &ModelConfig,
        requirements: &DataRequirements,
        venue: &DatasetVenue,
    ) -> anyhow::Result<Option<Self>> {
        let Some(meta) = &config.meta else {
            return Ok(None);
        };
        let sides = config
            .margins
            .sides()
            .map(|(side, margin)| {
                let store = ModelStore::new(
                    models,
                    strategy_id,
                    features,
                    config,
                    requirements,
                    venue,
                    side,
                )?;
                Ok((side, margin, store))
            })
            .collect::<anyhow::Result<_>>()?;
        Ok(Some(Self {
            config: meta.clone(),
            feature_names: meta_names(features.names()),
            trades: trades(&config.targets)?,
            primary: Primary::Model {
                sides,
                feature_names: features.names().to_vec(),
                fingerprint: dataset::fingerprint(
                    features,
                    config.decision,
                    &config.targets,
                    requirements,
                    venue,
                )?
                .to_string(),
            },
            store: ModelStore::meta(
                models,
                strategy_id,
                features,
                config,
                meta,
                requirements,
                venue,
            )?,
        }))
    }

    /// The meta model of `config`, of `strategy_id`, whose primary is its
    /// rule, deciding with `features` on targets for a strategy of
    /// `requirements`, run on `venue`, its models in the store under
    /// `models`.
    ///
    /// # Errors
    ///
    /// Returns an error if `features` do not declare the primary's side
    /// and probability (`FeatureSetBuilder::primary`), the targets have no
    /// barrier, or the store cannot be keyed.
    pub fn of_rule(
        models: &Path,
        strategy_id: StrategyId,
        features: &FeatureSet,
        config: &RuleConfig,
        requirements: &DataRequirements,
        venue: &DatasetVenue,
    ) -> anyhow::Result<Self> {
        ensure!(
            features.has_primary(),
            "the features of a rule primary declare its side and probability \
             (`FeatureSetBuilder::primary`)"
        );
        Ok(Self {
            config: config.meta.clone(),
            primary: Primary::Rule,
            feature_names: features.names().to_vec(),
            trades: trades(&config.targets)?,
            store: ModelStore::of_rule(models, strategy_id, features, config, requirements, venue)?,
        })
    }

    #[must_use]
    pub const fn config(&self) -> &MetaConfig {
        &self.config
    }

    /// The names of the meta's features, in the order of its rows.
    #[must_use]
    pub fn feature_names(&self) -> &[String] {
        &self.feature_names
    }

    /// The store of the meta's models.
    #[must_use]
    pub const fn store(&self) -> &ModelStore {
        &self.store
    }

    /// The meta's examples of the primary's dataset `rows`, in time order:
    /// each row the primary makes a trade of, its features with the
    /// primary's side and probability, its label whether the barrier's
    /// trade on that side won. A model primary's decision on a row is that
    /// of its sides' models in use then, none before the first, the rows'
    /// trades read by `realised`, a model primary's.
    ///
    /// # Errors
    ///
    /// Returns an error if a primary's store cannot be read, a model fails
    /// to predict, or a row's stop has no worth.
    ///
    /// # Panics
    ///
    /// Panics if the primary is a model and `realised` is none.
    pub(super) fn examples(
        &self,
        rows: &[&DatasetRow],
        realised: Option<&Realised>,
    ) -> anyhow::Result<Vec<Example>> {
        match &self.primary {
            Primary::Rule => Ok(rows
                .iter()
                .map(|row| {
                    let side = rule_side(&row.features);
                    self.example(row, side)
                })
                .collect()),
            Primary::Model {
                sides,
                feature_names,
                fingerprint,
            } => {
                let realised = realised.expect("a model primary's rows are read by its payoffs");
                let mut swaps = sides
                    .iter()
                    .map(|(side, margin, store)| {
                        let valid_froms = store.valid_froms()?;
                        let swap =
                            Swap::new(store.clone(), feature_names.clone(), fingerprint.clone());
                        Ok((*side, *margin, valid_froms, swap))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()?;
                let mut examples = vec![];
                for row in rows {
                    let now = Timestamp::from(row.ts_event);
                    let mut decided = vec![];
                    for (side, margin, valid_froms, swap) in &mut swaps {
                        let Some((_, model)) = swap.at(now, valid_froms) else {
                            continue;
                        };
                        let probability = model.predict(&row.features)?;
                        let payoff = model.payoff().context("a primary's model has its payoff")?;
                        let expected = payoff.expected(probability) * realised.unit(row, *side)?;
                        decided.push(((*side, probability), expected, *margin));
                    }
                    let Some((side, probability)) = payoff::pick(decided) else {
                        continue;
                    };
                    let side = side.order_side();
                    examples.push(Example {
                        features: meta_row(&row.features, side, probability),
                        ..self.example(row, side)
                    });
                }
                Ok(examples)
            }
        }
    }

    /// The example of `row` on the barrier's trade on `side`: whether it
    /// won, and the trade.
    fn example(&self, row: &DatasetRow, side: OrderSide) -> Example {
        let [long, short] = self.trades;
        let (pnl, commission) = if side == OrderSide::Buy { long } else { short };
        let trade = Trade {
            pnl: row.targets[pnl],
            commission: row.targets[commission],
        };
        Example::of(row, trade.pnl > 0.0, trade)
    }
}

/// A meta model's row: the primary's `features`, then its `side` and
/// `probability`.
#[must_use]
pub fn meta_row(features: &[f64], side: OrderSide, probability: f64) -> Vec<f64> {
    features
        .iter()
        .copied()
        .chain(primary_values(side, probability))
        .collect()
}

/// The names of a meta model's features, of a model primary's `names`.
fn meta_names(names: &[String]) -> Vec<String> {
    names
        .iter()
        .cloned()
        .chain(PRIMARY_FEATURES.map(String::from))
        .collect()
}

/// The side of a rule primary's row of `features`, its side the second
/// last.
fn rule_side(features: &[f64]) -> OrderSide {
    if features[features.len() - 2] > 0.0 {
        OrderSide::Buy
    } else {
        OrderSide::Sell
    }
}

/// Where the barrier's P&L and commission of a long and of a short are
/// among the values of a row of `targets`.
fn trades(targets: &TargetsConfig) -> anyhow::Result<[(usize, usize); 2]> {
    Side::BOTH
        .map(|side| {
            let pnl = targets.pnl_index(Label::Barrier, side)?;
            Some((pnl, targets.commission_index(Label::Barrier, side)?))
        })
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .and_then(|trades| trades.try_into().ok())
        .context("the meta model learns whether the primary's barrier trade wins, and the targets have no barrier")
}
