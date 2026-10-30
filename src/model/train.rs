//! Training at a time: the one operation that turns a strategy's dataset
//! into a published model, or keeps the current one, for retrains in a
//! backtest and live alike.
//!
//! A train at `t` takes the rows of the window ending at `t`
//! (`ModelConfig::window`), building the days the dataset lacks by one
//! replay to `t` (`Dataset::settled`), and drops the rows not settled at
//! `t`: no row's targets reach past the train, so the purge between the
//! rows and the model's first use is the longest target, by
//! construction. The holdout is the last part of the window
//! (`ModelConfig::holdout`), with the same gap: the model is fitted on
//! the rows before it settled at its start, and scored on it, the AUC of
//! its predictions against the label (`score::auc`). A score at or above
//! the minimum (`ModelConfig::min_score`) publishes the model in the store
//! (`store`), valid from `t`, its manifest probing it on rows of the
//! holdout; else the current model is kept.
//!
//! A model learns whether the long of its label's target gains after its
//! costs (`targets::Label`).
//!
//! With a meta model (`model::meta`), the primary trains first, then the
//! meta, on the same rows, of the one replay: the meta's window is its
//! own (`MetaConfig::window`), from the primary's first model on, and its
//! rows are made from the primary's out-of-sample predictions, the models
//! of the store the primary used at each decision, the one just published
//! valid from `t` used by none. Fitting, scoring and publishing are the
//! primary's, in the meta's store. A strategy whose primary is its rule
//! trains its meta model alone.
//!
//! A train never errors: what goes wrong is an outcome, the current model
//! kept with the reason (`Kept`), for each model trained.

use std::{fmt, path::Path};

use anyhow::Context;
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::StrategyId;

use super::{
    config::{ModelConfig, RuleConfig},
    dataset::{Commission, Dataset, DatasetRow, SchemaMismatch},
    features::FeatureSet,
    learner::{LearnerConfig, TrainingSet},
    meta::Meta,
    score,
    store::{ModelManifest, ModelStore, Probe, StoredModel},
    targets::{Label, TargetRow, TargetsConfig},
};
use crate::strategy::DataRequirements;

/// The most rows of the holdout a published model is probed on.
const PROBES: usize = 16;

/// The training of one strategy's models: its primary model, its meta
/// model, or both.
#[derive(Debug)]
pub struct Training {
    strategy_id: StrategyId,
    targets: TargetsConfig,
    dataset: Dataset,
    /// The primary model; none for a strategy whose primary is its rule.
    primary: Option<Primary>,
    /// The meta model, if any.
    meta: Option<(Level, Meta)>,
}

/// A primary model's training.
#[derive(Debug)]
struct Primary {
    level: Level,
    /// Where the label is among a row's target values.
    label: usize,
}

/// How the models of one level are fitted, scored and published.
#[derive(Debug)]
struct Level {
    /// The names of the features of its rows, in order.
    feature_names: Vec<String>,
    /// The target it learns, as its manifest says.
    label: Label,
    window: SignedDuration,
    holdout: SignedDuration,
    min_score: f64,
    learner: LearnerConfig,
    store: ModelStore,
}

/// A row as a model is fitted and scored on it: its features and its
/// label, the decision's time and when its targets settled.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Example {
    pub features: Vec<f64>,
    pub label: bool,
    pub ts_event: UnixNanos,
    pub settled_at: UnixNanos,
}

/// What a train did: the outcome of each model it trained, the primary's
/// first.
#[derive(Debug, Clone, PartialEq)]
pub struct Trained {
    /// The primary model's; none for a rule primary.
    pub primary: Option<Outcome>,
    /// The meta model's, if the strategy has one.
    pub meta: Option<Outcome>,
}

/// What a train did to one model.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// A model was published, valid from `valid_from`, with its holdout AUC.
    Published { valid_from: Timestamp, score: f64 },
    /// No model was published: the current one, if any, stays.
    Kept(Kept),
}

/// Why a train kept the current model.
#[derive(Debug, Clone, PartialEq)]
pub enum Kept {
    /// The holdout AUC is below the minimum.
    BelowMinScore { score: f64, min_score: f64 },
    /// The replay, the dataset, the fit or the store failed, or the window
    /// had no row to fit or no holdout to score; the error.
    Failed(String),
    /// The dataset holds rows that do not match the strategy's features or
    /// targets; the mismatch.
    SchemaMismatch(String),
}

impl Training {
    /// The training of `strategy_id`'s model, and of its meta model if
    /// `config` has one, deciding with `features`, on its dataset, for a
    /// strategy of `requirements`, net of `commission`, in the catalog at
    /// `catalog` and its models in the store under `models`, as `config`
    /// says.
    ///
    /// # Errors
    ///
    /// Returns an error if the targets are invalid, the label names a
    /// target they lack, the meta model has no barrier to learn, or a
    /// store cannot be keyed.
    pub fn new(
        catalog: &Path,
        models: &Path,
        strategy_id: StrategyId,
        features: &FeatureSet,
        config: ModelConfig,
        requirements: &DataRequirements,
        commission: &Commission,
    ) -> anyhow::Result<Self> {
        let label = config
            .targets
            .label_index(config.label)
            .with_context(|| format!("the label {:?} has no target", config.label))?;
        let primary = Primary {
            level: Level {
                feature_names: features.names().to_vec(),
                label: config.label,
                window: config.window,
                holdout: config.holdout,
                min_score: config.min_score,
                learner: config.learner.clone(),
                store: ModelStore::new(
                    models,
                    strategy_id,
                    features,
                    &config,
                    requirements,
                    commission,
                )?,
            },
            label,
        };
        let meta = Meta::of_model(
            models,
            strategy_id,
            features,
            &config,
            requirements,
            commission,
        )?
        .map(|meta| {
            let level = Level::of_meta(&meta);
            (level, meta)
        });
        Ok(Self {
            strategy_id,
            dataset: Dataset::new(
                catalog,
                strategy_id,
                features,
                config.decision,
                config.targets.clone(),
                requirements,
                commission,
            )?,
            targets: config.targets,
            primary: Some(primary),
            meta,
        })
    }

    /// The training of the meta model of `strategy_id`, whose primary is
    /// its rule, deciding with `features` (which declare the rule's side
    /// and probability), on its dataset, for a strategy of `requirements`,
    /// net of `commission`, in the catalog at `catalog` and its models in
    /// the store under `models`, as `config` says.
    ///
    /// # Errors
    ///
    /// Returns an error if the targets are invalid or have no barrier, the
    /// features do not declare the primary, or the store cannot be keyed.
    pub fn of_rule(
        catalog: &Path,
        models: &Path,
        strategy_id: StrategyId,
        features: &FeatureSet,
        config: RuleConfig,
        requirements: &DataRequirements,
        commission: &Commission,
    ) -> anyhow::Result<Self> {
        let meta = Meta::of_rule(
            models,
            strategy_id,
            features,
            &config,
            requirements,
            commission,
        )?;
        Ok(Self {
            strategy_id,
            dataset: Dataset::new(
                catalog,
                strategy_id,
                features,
                config.decision,
                config.targets.clone(),
                requirements,
                commission,
            )?,
            targets: config.targets,
            primary: None,
            meta: Some((Level::of_meta(&meta), meta)),
        })
    }

    /// The store the primary model is published in, or a rule primary's
    /// meta model.
    #[must_use]
    pub fn store(&self) -> &ModelStore {
        match (&self.primary, &self.meta) {
            (Some(primary), _) => &primary.level.store,
            (None, Some((meta, _))) => &meta.store,
            (None, None) => unreachable!("a training has a model"),
        }
    }

    /// The store the meta model is published in, if there is one.
    #[must_use]
    pub fn meta_store(&self) -> Option<&ModelStore> {
        self.meta.as_ref().map(|(level, _)| &level.store)
    }

    /// Trains at `at`: `replay` runs the strategy over a span of the days
    /// the dataset lacks and returns the rows it settled (see
    /// `Dataset::settled`), once for both models. Publishes the primary
    /// model, or keeps the current one with the reason, then the meta
    /// model.
    pub fn train_at(
        &self,
        at: Timestamp,
        replay: impl FnOnce(Timestamp, Timestamp) -> anyhow::Result<Vec<TargetRow>>,
    ) -> Trained {
        let from = self
            .primary
            .iter()
            .map(|primary| primary.level.window)
            .chain(self.meta.iter().map(|(level, _)| level.window))
            .max()
            .map_or(at, |window| at - window);
        let rows = match self.dataset.settled(from, at, replay) {
            Ok(rows) => rows,
            Err(e) => {
                return Trained {
                    primary: self.primary.as_ref().map(|_| kept(&e)),
                    meta: self.meta.as_ref().map(|_| kept(&e)),
                };
            }
        };
        let primary = self.primary.as_ref().map(|primary| {
            let start = UnixNanos::from(at - primary.level.window);
            let examples = rows
                .iter()
                .filter(|row| row.ts_event >= start)
                .map(|row| Example::of(row, gained(row, primary.label)))
                .collect();
            self.fit(&primary.level, at, examples)
        });
        let meta = self.meta.as_ref().map(|(level, meta)| {
            let examples = meta.window_start(at).and_then(|start| {
                let start = UnixNanos::from(start);
                let rows: Vec<&DatasetRow> =
                    rows.iter().filter(|row| row.ts_event >= start).collect();
                meta.examples(&rows)
            });
            match examples {
                Ok(examples) => self.fit(level, at, examples),
                Err(e) => kept(&e),
            }
        });
        Trained { primary, meta }
    }

    /// Fits `level`'s model at `at` on `examples`, in time order, and
    /// publishes it, or keeps the current one with the reason.
    fn fit(&self, level: &Level, at: Timestamp, examples: Vec<Example>) -> Outcome {
        self.try_fit(level, at, examples)
            .unwrap_or_else(|e| kept(&e))
    }

    fn try_fit(
        &self,
        level: &Level,
        at: Timestamp,
        examples: Vec<Example>,
    ) -> anyhow::Result<Outcome> {
        let (fitted, held_out) = split(examples, at - level.holdout);
        let set = TrainingSet::new(
            level.feature_names.len(),
            fitted.iter().flat_map(|row| row.features.clone()).collect(),
            fitted.iter().map(|row| row.label).collect(),
        )
        .context("fitting on the window before the holdout")?;
        let model = level.learner.fit(&set)?;
        let predictions = held_out
            .iter()
            .map(|row| Ok((model.predict(&row.features)?, row.label)))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let score = score::auc(&predictions)
            .context("the holdout has no row of each label to score the model on")?;
        if score < level.min_score {
            return Ok(Outcome::Kept(Kept::BelowMinScore {
                score,
                min_score: level.min_score,
            }));
        }
        let probes = spread(held_out.len())
            .map(|i| Probe {
                row: held_out[i].features.clone(),
                prediction: predictions[i].0,
            })
            .collect();
        level.store.publish(&StoredModel {
            manifest: ModelManifest {
                strategy_id: self.strategy_id,
                valid_from: at,
                fingerprint: self.dataset.fingerprint().to_string(),
                feature_names: level.feature_names.clone(),
                targets: self.targets.clone(),
                label: level.label,
                learner: level.learner.clone(),
                score,
                probes,
            },
            model: model.save()?,
        })?;
        Ok(Outcome::Published {
            valid_from: at,
            score,
        })
    }
}

impl Level {
    /// The level of `meta`: it learns whether the barrier on the primary's
    /// side wins.
    fn of_meta(meta: &Meta) -> Self {
        let config = meta.config();
        Self {
            feature_names: meta.feature_names().to_vec(),
            label: Label::Barrier,
            window: config.window,
            holdout: config.holdout,
            min_score: config.min_score,
            learner: config.learner.clone(),
            store: meta.store().clone(),
        }
    }
}

impl Example {
    /// The example of `row`, of `label`.
    pub(super) fn of(row: &DatasetRow, label: bool) -> Self {
        Self {
            features: row.features.clone(),
            label,
            ts_event: row.ts_event,
            settled_at: row.settled_at,
        }
    }
}

impl Trained {
    /// Whether every model trained was published.
    #[must_use]
    pub fn published(&self) -> bool {
        [&self.primary, &self.meta]
            .into_iter()
            .flatten()
            .all(|outcome| matches!(outcome, Outcome::Published { .. }))
    }
}

/// A train of a primary model alone.
impl From<Outcome> for Trained {
    fn from(primary: Outcome) -> Self {
        Self {
            primary: Some(primary),
            meta: None,
        }
    }
}

/// The current model kept for the error `e`: a schema mismatch, or a
/// failure.
fn kept(e: &anyhow::Error) -> Outcome {
    Outcome::Kept(match e.downcast_ref::<SchemaMismatch>() {
        Some(mismatch) => Kept::SchemaMismatch(mismatch.to_string()),
        None => Kept::Failed(format!("{e:#}")),
    })
}

/// Whether the long of the target at `label` among `row`'s targets gained.
pub(super) fn gained(row: &DatasetRow, label: usize) -> bool {
    row.targets[label] > 0.0
}

/// `examples` split at the holdout's start, `holdout`: those before it
/// settled by then, fitted on, and those from it, held out.
pub(super) fn split(examples: Vec<Example>, holdout: Timestamp) -> (Vec<Example>, Vec<Example>) {
    let holdout = UnixNanos::from(holdout);
    let (fitted, held_out): (Vec<_>, Vec<_>) =
        examples.into_iter().partition(|row| row.ts_event < holdout);
    let fitted = fitted
        .into_iter()
        .filter(|row| row.settled_at <= holdout)
        .collect();
    (fitted, held_out)
}

/// The indices of at most `PROBES` of `len` rows, spread evenly from the
/// first.
fn spread(len: usize) -> impl Iterator<Item = usize> {
    let count = len.min(PROBES);
    (0..count).map(move |i| i * len / count)
}

/// A train of one model reads as its outcome; of two, each model's.
impl fmt::Display for Trained {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.primary, &self.meta) {
            (Some(primary), None) => write!(f, "{primary}"),
            (Some(primary), Some(meta)) => {
                write!(f, "the primary model: {primary}; the meta model: {meta}")
            }
            (None, Some(meta)) => write!(f, "the meta model: {meta}"),
            (None, None) => Ok(()),
        }
    }
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Published { valid_from, score } => {
                write!(
                    f,
                    "published a model valid from {valid_from}, holdout AUC {score}"
                )
            }
            Self::Kept(kept) => write!(f, "kept the current model: {kept}"),
        }
    }
}

impl fmt::Display for Kept {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BelowMinScore { score, min_score } => {
                write!(f, "holdout AUC {score} below the minimum {min_score}")
            }
            Self::Failed(why) => write!(f, "failed: {why}"),
            Self::SchemaMismatch(why) => write!(f, "schema mismatch: {why}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use jiff::SignedDuration;
    use nautilus_indicators::average::sma::SimpleMovingAverage;
    use nautilus_model::{
        enums::PriceType,
        identifiers::InstrumentId,
        types::{Money, Quantity},
    };
    use rust_decimal::Decimal;

    use super::*;
    use crate::model::{
        config::{MetaConfig, Thresholds},
        decide::{DecisionRule, FeatureRow},
        learner::LearnerConfig,
        targets::{
            Barrier, Distance, Exit, Label, Outcome as TargetOutcome, Outcomes, TargetRow,
            TargetsConfig,
        },
    };

    const GOLD: &str = "XAUUSD.ICMARKETS";
    const STRATEGY: &str = "TRAIN-001";

    fn features(width: usize) -> Rc<FeatureSet> {
        (0..width)
            .fold(FeatureSet::builder(), |builder, i| {
                builder.quotes(
                    InstrumentId::from(GOLD),
                    &format!("mid_{i}"),
                    SimpleMovingAverage::new(i + 1, Some(PriceType::Mid)),
                )
            })
            .build()
            .unwrap()
    }

    /// The fake learner on hourly decisions, learning the barrier (closed
    /// after 2 hours; a horizon of 1 hour besides), on a window of 24
    /// hours, the last 6 held out, published from an AUC of 0.5.
    fn config() -> ModelConfig {
        ModelConfig::builder()
            .decision(DecisionRule::Every(SignedDuration::from_hours(1)))
            .targets(
                TargetsConfig::builder()
                    .instrument_id(InstrumentId::from(GOLD))
                    .quantity(Quantity::from(1))
                    .barrier(Barrier {
                        take_profit: Distance::BasisPoints(Decimal::new(75, 1)),
                        stop: Distance::BasisPoints(Decimal::from(5)),
                        limit: SignedDuration::from_hours(2),
                        atr: None,
                    })
                    .horizon(SignedDuration::from_hours(1))
                    .build(),
            )
            .label(Label::Barrier)
            .retrain(SignedDuration::from_hours(24))
            .window(SignedDuration::from_hours(24))
            .holdout(SignedDuration::from_hours(6))
            .thresholds(Thresholds {
                long: 0.6,
                short: 0.4,
            })
            .min_score(0.5)
            .learner(LearnerConfig::Fake {})
            .build()
    }

    /// What the strategy reads: the quotes of gold.
    fn requirements() -> DataRequirements {
        DataRequirements {
            bar_types: vec![],
            ticks: vec![crate::strategy::TickSeries {
                data: nautilus_model::data::NautilusDataType::QuoteTick,
                instrument_id: InstrumentId::from(GOLD),
            }],
            warmup: SignedDuration::ZERO,
        }
    }

    /// A commission rate of 30 per million.
    fn commission() -> Commission {
        Commission::Rate("0.00003".to_string())
    }

    /// The training in `dir`, its catalog and its store side by side.
    fn training(dir: &Path, config: ModelConfig) -> Training {
        std::fs::create_dir_all(dir.join("catalog")).unwrap();
        Training::new(
            &dir.join("catalog"),
            &dir.join("models"),
            StrategyId::from(STRATEGY),
            &features(1),
            config,
            &requirements(),
            &commission(),
        )
        .unwrap()
    }

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    /// The row of the decision at `time` on `features`, its barrier's long
    /// winning if `wins`, else losing, settled `after` it.
    fn row(time: &str, features: Vec<f64>, wins: bool, after: SignedDuration) -> TargetRow {
        let ns = UnixNanos::from(at(time));
        let outcome = |pnl: &str, exit| TargetOutcome {
            pnl: Money::from(pnl),
            exit,
        };
        let (long, short) = if wins {
            (
                outcome("3 USD", Exit::TakeProfit),
                outcome("-2 USD", Exit::Stop),
            )
        } else {
            (
                outcome("-2 USD", Exit::Stop),
                outcome("3 USD", Exit::TakeProfit),
            )
        };
        TargetRow {
            decision: FeatureRow {
                strategy_id: StrategyId::from(STRATEGY),
                features,
                ts_event: ns,
                ts_init: ns,
            },
            barrier: Some(Outcomes { long, short }),
            horizon: Some(Outcomes {
                long: outcome("0.5 USD", Exit::Time),
                short: outcome("-0.5 USD", Exit::Time),
            }),
            settled_at: UnixNanos::from(at(time) + after),
        }
    }

    fn hour() -> SignedDuration {
        SignedDuration::from_hours(1)
    }

    /// At midnight of the 7th: the window is the 6th, the holdout from
    /// 18:00. Fitted, a win and a loss (mean 0.5); held out, a win
    /// predicted 0.8 and a loss 0.3 (AUC 1). Around them, wins on the
    /// first feature 0.1 that would change both if taken: one before the
    /// window, one before the holdout settling after its start, one in
    /// the holdout settling after the window's end.
    fn rows() -> Vec<TargetRow> {
        vec![
            row("2026-01-05T23:00:00Z", vec![0.1], true, hour()),
            row("2026-01-06T10:00:00Z", vec![0.9], true, hour()),
            row("2026-01-06T16:00:00Z", vec![0.9], false, hour()),
            row(
                "2026-01-06T17:00:00Z",
                vec![0.1],
                true,
                SignedDuration::from_mins(90),
            ),
            row("2026-01-06T19:00:00Z", vec![0.8], true, hour()),
            row("2026-01-06T20:00:00Z", vec![0.3], false, hour()),
            row("2026-01-06T23:30:00Z", vec![0.1], true, hour()),
        ]
    }

    #[test]
    fn a_train_fits_the_window_before_the_holdout_settled_at_its_start_and_scores_the_holdout() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let t = at("2026-01-07T00:00:00Z");

        let outcome = training.train_at(t, |_, _| Ok(rows()));

        assert_eq!(
            outcome,
            Outcome::Published {
                valid_from: t,
                score: 1.0
            }
            .into()
        );
        let stored = training.store().read(t).unwrap();
        let model = LearnerConfig::Fake {}.load(&stored.model).unwrap();
        // The fake predicts the mean of its training labels where the first
        // feature is missing.
        assert_eq!(model.predict(&[f64::NAN]).unwrap(), 0.5);
        assert_eq!(stored.manifest.score, 1.0);
        let probes: Vec<(Vec<f64>, f64)> = stored
            .manifest
            .probes
            .iter()
            .map(|probe| (probe.row.clone(), probe.prediction))
            .collect();
        assert_eq!(probes, [(vec![0.8], 0.8), (vec![0.3], 0.3)]);
    }

    #[test]
    fn a_train_replays_from_the_first_day_of_the_window_to_its_end() {
        let dir = tempfile::tempdir().unwrap();
        let mut replays = vec![];

        training(dir.path(), config()).train_at(at("2026-01-07T00:00:00Z"), |from, to| {
            replays.push((from, to));
            Ok(rows())
        });

        assert_eq!(
            replays,
            [(at("2026-01-06T00:00:00Z"), at("2026-01-07T00:00:00Z"))]
        );
    }

    #[test]
    fn a_published_model_carries_its_schema_targets_and_learner() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let t = at("2026-01-07T00:00:00Z");

        training.train_at(t, |_, _| Ok(rows()));

        let manifest = training.store().read(t).unwrap().manifest;
        assert_eq!(manifest.strategy_id, StrategyId::from(STRATEGY));
        assert_eq!(manifest.valid_from, t);
        assert_eq!(manifest.feature_names, ["mid_0"]);
        assert_eq!(
            manifest.fingerprint,
            crate::model::dataset::fingerprint(
                &features(1),
                config().decision,
                &config().targets,
                &requirements(),
                &commission(),
            )
            .unwrap()
            .to_string()
        );
        assert_eq!(manifest.targets, config().targets);
        assert_eq!(manifest.label, Label::Barrier);
        assert_eq!(manifest.learner, LearnerConfig::Fake {});
    }

    #[test]
    fn a_score_below_the_minimum_keeps_the_current_model() {
        let dir = tempfile::tempdir().unwrap();
        let t = at("2026-01-07T00:00:00Z");
        training(dir.path(), config()).train_at(t, |_, _| Ok(rows()));
        let worse = |mut rows: Vec<TargetRow>| {
            // The holdout's win now predicted below its loss: AUC 0.
            rows[4].decision.features = vec![0.2];
            rows
        };
        let gated = training(dir.path(), config());

        let outcome = gated.train_at(t + hour(), |_, _| Ok(worse(rows())));

        assert_eq!(
            outcome,
            Outcome::Kept(Kept::BelowMinScore {
                score: 0.0,
                min_score: 0.5
            })
            .into()
        );
        assert_eq!(gated.store().valid_froms().unwrap(), [t]);
    }

    #[test]
    fn a_failed_replay_keeps_the_current_model() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());

        let outcome = training
            .train_at(at("2026-01-07T00:00:00Z"), |_, _| {
                anyhow::bail!("the catalog does not cover the window")
            })
            .primary
            .unwrap();

        let Outcome::Kept(Kept::Failed(why)) = outcome else {
            panic!("{outcome:?}");
        };
        assert!(why.contains("does not cover"), "{why}");
        assert!(training.store().valid_froms().unwrap().is_empty());
    }

    #[test]
    fn a_holdout_of_one_label_fails() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let mut rows = rows();
        rows.remove(5);

        let outcome = training
            .train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows))
            .primary
            .unwrap();

        assert!(
            matches!(outcome, Outcome::Kept(Kept::Failed(_))),
            "{outcome:?}"
        );
    }

    #[test]
    fn rows_of_other_features_are_a_schema_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let wide: Vec<TargetRow> = rows()
            .into_iter()
            .map(|mut row| {
                row.decision.features.push(1.0);
                row
            })
            .collect();

        // The 6th settles by midnight of the 7th plus 2 hours, so it is
        // stored, and read back.
        let outcome = training
            .train_at(at("2026-01-07T02:00:00Z"), |_, _| Ok(wide))
            .primary
            .unwrap();

        assert!(
            matches!(outcome, Outcome::Kept(Kept::SchemaMismatch(_))),
            "{outcome:?}"
        );
    }

    #[test]
    fn an_outcome_reads_as_published_or_kept_with_its_reason() {
        let published = Outcome::Published {
            valid_from: at("2026-01-07T00:00:00Z"),
            score: 0.625,
        };
        let below = Outcome::Kept(Kept::BelowMinScore {
            score: 0.5,
            min_score: 0.55,
        });

        assert!(published.to_string().contains("published"), "{published}");
        assert!(below.to_string().contains("kept"), "{below}");
        assert!(below.to_string().contains("below"), "{below}");
    }

    /// The section of `config` with a meta model on the fake learner, on a
    /// window of `window` hours, the last 6 held out, taking a trade from
    /// 0.5, published from an AUC of 0.
    fn with_meta(window: i64) -> ModelConfig {
        ModelConfig {
            meta: Some(meta(window)),
            ..config()
        }
    }

    fn meta(window: i64) -> MetaConfig {
        MetaConfig::builder()
            .window(SignedDuration::from_hours(window))
            .holdout(SignedDuration::from_hours(6))
            .threshold(0.5)
            .min_score(0.0)
            .learner(LearnerConfig::Fake {})
            .build()
    }

    /// Publishes in `store` a primary baseline valid from `valid_from`,
    /// predicting `probability` on every row.
    fn primary(store: &ModelStore, valid_from: &str, probability: f64) {
        let trues = (probability * 10.0).round() as usize;
        let labels = (0..10).map(|i| i < trues).collect();
        let set = TrainingSet::new(1, vec![0.5; 10], labels).unwrap();
        let model = LearnerConfig::Baseline {}.fit(&set).unwrap();
        let config = config();
        store
            .publish(&StoredModel {
                manifest: ModelManifest {
                    strategy_id: StrategyId::from(STRATEGY),
                    valid_from: at(valid_from),
                    fingerprint: crate::model::dataset::fingerprint(
                        &features(1),
                        config.decision,
                        &config.targets,
                        &requirements(),
                        &commission(),
                    )
                    .unwrap()
                    .to_string(),
                    feature_names: vec!["mid_0".to_string()],
                    targets: config.targets,
                    label: config.label,
                    learner: LearnerConfig::Baseline {},
                    score: 0.6,
                    probes: vec![],
                },
                model: model.save().unwrap(),
            })
            .unwrap();
    }

    /// The decisions of the 6th: one before the primary's first model (a
    /// win), two long trades of the model valid from 06:00, predicting 0.8
    /// (a win and a loss of the long: meta labels true, false), two short
    /// trades of the one valid from 17:00, predicting 0.2, held out (a win
    /// and a loss of the long: the short's labels false, true), the meta's
    /// fake predicting their first feature, 0.3 and 0.7.
    fn meta_rows() -> Vec<TargetRow> {
        vec![
            row("2026-01-06T05:00:00Z", vec![0.5], true, hour()),
            row("2026-01-06T08:00:00Z", vec![0.5], true, hour()),
            row("2026-01-06T10:00:00Z", vec![0.5], false, hour()),
            row("2026-01-06T19:00:00Z", vec![0.3], true, hour()),
            row("2026-01-06T20:00:00Z", vec![0.7], false, hour()),
        ]
    }

    /// The primary's models: from 06:00 a long, from 17:00 a short, and
    /// from 21:00, after every row, a long again, which no row may use.
    fn primaries(training: &Training) {
        primary(training.store(), "2026-01-06T06:00:00Z", 0.8);
        primary(training.store(), "2026-01-06T17:00:00Z", 0.2);
        primary(training.store(), "2026-01-06T21:00:00Z", 0.9);
    }

    #[test]
    fn a_meta_model_learns_the_primarys_side_from_its_out_of_sample_predictions() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), with_meta(24));
        primaries(&training);
        let t = at("2026-01-07T00:00:00Z");

        let trained = training.train_at(t, |_, _| Ok(meta_rows()));

        assert_eq!(
            trained.meta,
            Some(Outcome::Published {
                valid_from: t,
                score: 1.0
            })
        );
        let stored = training.meta_store().unwrap().read(t).unwrap();
        assert_eq!(
            stored.manifest.feature_names,
            ["mid_0", "primary_side", "primary_probability"]
        );
        assert_eq!(stored.manifest.label, Label::Barrier);
        let probes: Vec<(Vec<f64>, f64)> = stored
            .manifest
            .probes
            .iter()
            .map(|probe| (probe.row.clone(), probe.prediction))
            .collect();
        assert_eq!(
            probes,
            [(vec![0.3, -1.0, 0.2], 0.3), (vec![0.7, -1.0, 0.2], 0.7)]
        );
        // Fitted on the two long trades, a win and a loss: the decision
        // before the primary's first model is no row.
        let model = LearnerConfig::Fake {}.load(&stored.model).unwrap();
        assert_eq!(model.predict(&[f64::NAN, 1.0, 0.8]).unwrap(), 0.5);
    }

    #[test]
    fn the_primary_trains_before_its_meta_model_on_one_replay_of_the_longer_window() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), with_meta(36));
        primaries(&training);
        let mut replays = vec![];

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |from, to| {
            replays.push((from, to));
            Ok(meta_rows())
        });

        assert_eq!(
            replays,
            [(at("2026-01-05T00:00:00Z"), at("2026-01-07T00:00:00Z"))]
        );
        assert!(
            trained.primary.is_some() && trained.meta.is_some(),
            "{trained:?}"
        );
    }

    #[test]
    fn a_meta_model_without_a_primary_model_before_it_is_kept() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), with_meta(24));

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(meta_rows()));

        assert!(
            matches!(trained.meta, Some(Outcome::Kept(Kept::Failed(_)))),
            "{trained:?}"
        );
        assert!(
            training
                .meta_store()
                .unwrap()
                .valid_froms()
                .unwrap()
                .is_empty()
        );
    }

    /// The rule's decisions of the 6th, its side among their features:
    /// fitted, a long winning, a short losing and a short winning (labels
    /// true, false, true); held out, a long losing at 0.3 and a short
    /// winning at 0.7 (false, true).
    #[test]
    fn a_rule_primarys_meta_model_learns_whether_the_rules_side_wins() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("catalog")).unwrap();
        let features = FeatureSet::builder()
            .quotes(
                InstrumentId::from(GOLD),
                "mid_0",
                SimpleMovingAverage::new(1, Some(PriceType::Mid)),
            )
            .primary()
            .build()
            .unwrap();
        let config = config();
        let training = Training::of_rule(
            &dir.path().join("catalog"),
            &dir.path().join("models"),
            StrategyId::from(STRATEGY),
            &features,
            RuleConfig {
                decision: config.decision,
                targets: config.targets,
                retrain: config.retrain,
                meta: meta(24),
            },
            &requirements(),
            &commission(),
        )
        .unwrap();
        let (buy, sell) = (1.0, -1.0);
        let rows = vec![
            row(
                "2026-01-06T08:00:00Z",
                vec![0.5, buy, f64::NAN],
                true,
                hour(),
            ),
            row(
                "2026-01-06T10:00:00Z",
                vec![0.5, sell, f64::NAN],
                true,
                hour(),
            ),
            row(
                "2026-01-06T12:00:00Z",
                vec![0.5, sell, f64::NAN],
                false,
                hour(),
            ),
            row(
                "2026-01-06T19:00:00Z",
                vec![0.3, buy, f64::NAN],
                false,
                hour(),
            ),
            row(
                "2026-01-06T20:00:00Z",
                vec![0.7, sell, f64::NAN],
                false,
                hour(),
            ),
        ];
        let t = at("2026-01-07T00:00:00Z");

        let trained = training.train_at(t, |_, _| Ok(rows));

        assert_eq!(trained.primary, None);
        assert_eq!(
            trained.meta,
            Some(Outcome::Published {
                valid_from: t,
                score: 1.0
            })
        );
        let stored = training.store().read(t).unwrap();
        let model = LearnerConfig::Fake {}.load(&stored.model).unwrap();
        assert!(
            (model.predict(&[f64::NAN, buy, f64::NAN]).unwrap() - 2.0 / 3.0).abs() < 1e-12,
            "{stored:?}"
        );
    }

    #[test]
    fn a_train_of_two_models_reads_as_each_outcome() {
        let published = Outcome::Published {
            valid_from: at("2026-01-07T00:00:00Z"),
            score: 0.625,
        };
        let trained = Trained {
            primary: Some(published.clone()),
            meta: Some(Outcome::Kept(Kept::Failed("no row".to_string()))),
        };

        let text = trained.to_string();

        assert!(!trained.published());
        assert!(Trained::from(published).published());
        assert!(text.contains("primary") && text.contains("meta"), "{text}");
        assert!(text.contains("no row"), "{text}");
    }
}
