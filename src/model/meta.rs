//! Meta-labeling: a primary picks the side of a trade, and a meta model
//! decides whether to take it, and optionally its size.
//!
//! The primary is the strategy's own rule (`RuleConfig`), which decides on
//! a side only when it signals (`ModelHandle::decide_on`), or a model
//! (`ModelConfig` with a meta section), whose probability the thresholds
//! make a side (`Thresholds::side`). Either way the meta model decides only
//! when the primary signals, on the primary's features with its side and
//! probability after them (`PRIMARY_FEATURES`; a rule's probability is
//! missing, NaN), and learns whether the triple-barrier trade on the
//! primary's side wins after its costs (`TargetsConfig::pnl_index`): a
//! dataset row opens a long and a short at every decision, so the row has
//! the trade of either side, and the meta's rows need no replay of their
//! own.
//!
//! A rule primary's rows are its decisions, its features carrying its
//! side. A model primary's rows are its dataset's, each with the
//! prediction of the primary model in use at it as a strategy would have
//! used it (`store::Swap`): the latest valid then, trained on rows settled
//! by its valid-from, so every prediction is out of sample. A row before
//! the primary's first model has no prediction: the meta's window starts
//! there at the earliest (`window_start`). A row the primary makes no trade
//! of is no row of the meta.
//!
//! Two levels at most: a meta section has no meta of its own (`config`).

use std::path::Path;

use anyhow::{Context, ensure};
use jiff::Timestamp;
use nautilus_model::{enums::OrderSide, identifiers::StrategyId};

use super::{
    config::{MetaConfig, ModelConfig, RuleConfig, Thresholds},
    dataset::{self, Commission, DatasetRow},
    features::{FeatureSet, PRIMARY_FEATURES, primary_values},
    store::{ModelStore, Swap},
    targets::{Label, TargetsConfig},
    train::Example,
};

/// A strategy's meta model: how its rows are made from the primary's, and
/// its store.
#[derive(Debug)]
pub struct Meta {
    config: MetaConfig,
    primary: Primary,
    /// The names of the meta's features, the primary's side and
    /// probability last.
    feature_names: Vec<String>,
    /// Where the barrier's P&L of a long and of a short are among a row's
    /// target values.
    pnl: [usize; 2],
    store: ModelStore,
}

/// The primary of a meta model.
#[derive(Debug)]
enum Primary {
    /// The strategy's rule: its side is in its rows' features.
    Rule,
    /// A model, whose models are swapped in as the strategy does.
    Model {
        store: ModelStore,
        feature_names: Vec<String>,
        fingerprint: String,
        thresholds: Thresholds,
    },
}

impl Meta {
    /// The meta model of `config`, the model of `strategy_id` deciding
    /// with `features` on targets net of `commission`, its models in the
    /// store under `models`; none if `config` has no meta section.
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
        commission: &Commission,
    ) -> anyhow::Result<Option<Self>> {
        let Some(meta) = &config.meta else {
            return Ok(None);
        };
        let primary = ModelStore::new(models, strategy_id, features, config, commission)?;
        let store = primary.meta(meta, config.thresholds)?;
        Ok(Some(Self {
            config: meta.clone(),
            feature_names: meta_names(features.names()),
            pnl: pnl(&config.targets)?,
            primary: Primary::Model {
                store: primary,
                feature_names: features.names().to_vec(),
                fingerprint: dataset::fingerprint(
                    features,
                    config.decision,
                    &config.targets,
                    commission,
                )
                .to_string(),
                thresholds: config.thresholds,
            },
            store,
        }))
    }

    /// The meta model of `config`, of `strategy_id`, whose primary is its
    /// rule, deciding with `features` on targets net of `commission`, its
    /// models in the store under `models`.
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
        commission: &Commission,
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
            pnl: pnl(&config.targets)?,
            store: ModelStore::of_rule(models, strategy_id, features, config, commission)?,
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

    /// The start of the window of a meta trained at `at`: its window back
    /// from `at`, but not before the primary model's first valid-from; `at`
    /// itself, an empty window, if the primary has no model before it.
    ///
    /// # Errors
    ///
    /// Returns an error if the primary's store cannot be read.
    pub fn window_start(&self, at: Timestamp) -> anyhow::Result<Timestamp> {
        let start = at - self.config.window;
        let Primary::Model { store, .. } = &self.primary else {
            return Ok(start);
        };
        Ok(match store.valid_froms()?.first() {
            Some(&first) if first < at => start.max(first),
            _ => at,
        })
    }

    /// The meta's examples of the primary's dataset `rows`, in time order:
    /// each row the primary makes a trade of, its features with the
    /// primary's side and probability, its label whether the barrier's
    /// trade on that side won. A model primary's prediction on a row is
    /// that of its model in use then, none before its first.
    ///
    /// # Errors
    ///
    /// Returns an error if the primary's store cannot be read, or a model
    /// fails to predict.
    pub(super) fn examples(&self, rows: &[&DatasetRow]) -> anyhow::Result<Vec<Example>> {
        match &self.primary {
            Primary::Rule => Ok(rows
                .iter()
                .map(|row| {
                    let side = rule_side(&row.features);
                    Example::of(row, self.wins(row, side))
                })
                .collect()),
            Primary::Model {
                store,
                feature_names,
                fingerprint,
                thresholds,
            } => {
                let valid_froms = store.valid_froms()?;
                let mut swap = Swap::new(store.clone(), feature_names.clone(), fingerprint.clone());
                let mut examples = vec![];
                for row in rows {
                    let Some((_, model)) = swap.at(Timestamp::from(row.ts_event), &valid_froms)
                    else {
                        continue;
                    };
                    let probability = model.predict(&row.features)?;
                    let Some(side) = thresholds.side(probability) else {
                        continue;
                    };
                    examples.push(Example {
                        features: meta_row(&row.features, side, probability),
                        ..Example::of(row, self.wins(row, side))
                    });
                }
                Ok(examples)
            }
        }
    }

    /// Whether the barrier's trade on `side` won in `row`.
    fn wins(&self, row: &DatasetRow, side: OrderSide) -> bool {
        let [long, short] = self.pnl;
        let at = if side == OrderSide::Buy { long } else { short };
        row.targets[at] > 0.0
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

/// Where the barrier's P&L of a long and of a short are among the values of
/// a row of `targets`.
fn pnl(targets: &TargetsConfig) -> anyhow::Result<[usize; 2]> {
    [OrderSide::Buy, OrderSide::Sell]
        .map(|side| targets.pnl_index(Label::Barrier, side))
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .and_then(|pnl| pnl.try_into().ok())
        .context("the meta model learns whether the primary's barrier trade wins, and the targets have no barrier")
}
