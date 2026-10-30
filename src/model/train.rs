//! Training at a time: the one operation that turns a strategy's dataset
//! into published models, or keeps the current ones, for retrains in a
//! backtest and live alike.
//!
//! A model primary has a model for each side it trades (the sides of its
//! margins, `ModelConfig::margins`): the long's learns whether the long of
//! the label's target gains after its costs, the short's whether its
//! short does (`targets::Label`), both on the same rows, window and
//! holdout. Each is fitted, calibrated, scored and published on its own:
//! a side failing its gate keeps its current model, the other may
//! publish.
//!
//! A train at `t` takes the rows of the window ending at `t`
//! (`ModelConfig::window`), building the days the dataset lacks by one
//! replay to `t` (`Dataset::settled`), and drops the rows not settled at
//! `t`: no row's targets reach past the train, so the purge between the
//! rows and the model's first use is the longest target, by
//! construction. The window ends in the holdout (`ModelConfig::holdout`)
//! and, before it, the calibration, as long, each part's rows taken with
//! the same gap: a model is fitted on the rows before the calibration
//! settled at its start, calibrated by Platt scaling
//! (`calibration::Calibration`) on its predictions on the calibration's
//! rows settled at the holdout's start, and scored on the holdout, the
//! AUC of its calibrated predictions against the label (`score::auc`). A
//! score at or above the minimum (`ModelConfig::min_score`) publishes the
//! model in its side's store (`store`), valid from `t`, with its
//! calibration, its expected payoff (`payoff::Payoff`, of the realised
//! values of the rows before the holdout, `payoff::Realised`), and its
//! manifest probing it on rows of the holdout; else the current model is
//! kept.
//!
//! With a meta model (`model::meta`), the primary trains first, then the
//! meta, on the same rows, of the one replay: the meta's window is its
//! own (`MetaConfig::window`), from the primary's first model on, and its
//! rows are made from the primary's out-of-sample decisions, those of the
//! models of the store the primary used at each decision, the ones just
//! published valid from `t` used by none. Fitting, calibrating, scoring
//! and publishing are the primary's, in the meta's store. A strategy whose
//! primary is its rule trains its meta model alone.
//!
//! A train never errors: what goes wrong is an outcome, the current model
//! kept with the reason (`Kept`), for each model trained.

use std::{fmt, path::Path};

use anyhow::Context;
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::StrategyId;

use super::{
    calibration::Calibration,
    config::{ModelConfig, RuleConfig},
    dataset::{Commission, Dataset, DatasetRow, SchemaMismatch},
    features::FeatureSet,
    learner::{LearnerConfig, TrainingSet},
    meta::Meta,
    payoff::{Payoff, Realised},
    score,
    store::{LoadedModel, ModelManifest, ModelStore, Probe, StoredModel},
    targets::{Label, Side, TargetRow, TargetsConfig},
};
use crate::strategy::DataRequirements;

/// The most rows of the holdout a published model is probed on.
const PROBES: usize = 16;

/// The training of one strategy's models: its primary's sides, its meta
/// model, or both.
#[derive(Debug)]
pub struct Training {
    strategy_id: StrategyId,
    targets: TargetsConfig,
    dataset: Dataset,
    /// How the rows' trades read in the primary's payoffs; none for a
    /// strategy whose primary is its rule.
    realised: Option<Realised>,
    /// The model of each side the primary trades; none for a strategy
    /// whose primary is its rule.
    sides: Vec<Level>,
    /// The meta model, if any.
    meta: Option<(Level, Meta)>,
}

/// How the models of one level, a primary's side or a meta, are fitted,
/// calibrated, scored and published.
#[derive(Debug)]
struct Level {
    /// The names of the features of its rows, in order.
    feature_names: Vec<String>,
    /// The target it learns, as its manifest says.
    label: Label,
    /// The side of the target a primary's model learns; none for a meta.
    side: Option<Side>,
    window: SignedDuration,
    holdout: SignedDuration,
    min_score: f64,
    learner: LearnerConfig,
    store: ModelStore,
}

/// A row as a model is fitted and scored on it: its features and its
/// label, a primary's realised value of its side's trade (in its payoff's
/// unit), the decision's time and when its targets settled.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Example {
    pub features: Vec<f64>,
    pub label: bool,
    /// A primary's realised value; none for a meta's row.
    pub realised: Option<f64>,
    pub ts_event: UnixNanos,
    pub settled_at: UnixNanos,
}

/// What a train did: the outcome of each model it trained, the primary's
/// sides first.
#[derive(Debug, Clone, PartialEq)]
pub struct Trained {
    /// The long model's; none if the primary trades no long, or is a rule.
    pub long: Option<Outcome>,
    /// The short model's; none if the primary trades no short, or is a
    /// rule.
    pub short: Option<Outcome>,
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
    /// had no row to fit, to calibrate on or to expect a payoff from, or
    /// no holdout to score; the error.
    Failed(String),
    /// The dataset holds rows that do not match the strategy's features or
    /// targets; the mismatch.
    SchemaMismatch(String),
}

impl Training {
    /// The training of `strategy_id`'s models, one for each side of its
    /// margins, and of its meta model if `config` has one, deciding with
    /// `features`, on its dataset, for a strategy of `requirements`, net of
    /// `commission`, in the catalog at `catalog` and its models in the
    /// store under `models`, as `config` says.
    ///
    /// # Errors
    ///
    /// Returns an error if the targets are invalid, the label names a
    /// target they lack, a barrier's instrument is not in the catalog, the
    /// meta model has no barrier to learn, or a store cannot be keyed.
    pub fn new(
        catalog: &Path,
        models: &Path,
        strategy_id: StrategyId,
        features: &FeatureSet,
        config: ModelConfig,
        requirements: &DataRequirements,
        commission: &Commission,
    ) -> anyhow::Result<Self> {
        let realised = Realised::new(catalog, &config.targets, config.label)?;
        let sides = config
            .margins
            .sides()
            .map(|(side, _)| {
                Ok(Level {
                    feature_names: features.names().to_vec(),
                    label: config.label,
                    side: Some(side),
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
                        side,
                    )?,
                })
            })
            .collect::<anyhow::Result<_>>()?;
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
            realised: Some(realised),
            sides,
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
            realised: None,
            sides: vec![],
            meta: Some((Level::of_meta(&meta), meta)),
        })
    }

    /// The store the primary's `side` models are published in, if it
    /// trades that side.
    #[must_use]
    pub fn store(&self, side: Side) -> Option<&ModelStore> {
        self.sides
            .iter()
            .find(|level| level.side == Some(side))
            .map(|level| &level.store)
    }

    /// The store the meta model is published in, if there is one.
    #[must_use]
    pub fn meta_store(&self) -> Option<&ModelStore> {
        self.meta.as_ref().map(|(level, _)| &level.store)
    }

    /// Trains at `at`: `replay` runs the strategy over a span of the days
    /// the dataset lacks and returns the rows it settled (see
    /// `Dataset::settled`), once for every model. Publishes each side's
    /// model, or keeps its current one with the reason, then the meta
    /// model.
    pub fn train_at(
        &self,
        at: Timestamp,
        replay: impl FnOnce(Timestamp, Timestamp) -> anyhow::Result<Vec<TargetRow>>,
    ) -> Trained {
        let from = self
            .sides
            .iter()
            .chain(self.meta.iter().map(|(level, _)| level))
            .map(|level| level.window)
            .max()
            .map_or(at, |window| at - window);
        let rows = match self.dataset.settled(from, at, replay) {
            Ok(rows) => rows,
            Err(e) => {
                let side = |side| self.store(side).map(|_| kept(&e));
                return Trained {
                    long: side(Side::Long),
                    short: side(Side::Short),
                    meta: self.meta.as_ref().map(|_| kept(&e)),
                };
            }
        };
        let side = |side: Side| {
            let level = self.sides.iter().find(|level| level.side == Some(side))?;
            let realised = self.realised.as_ref()?;
            let start = UnixNanos::from(at - level.window);
            let examples = rows
                .iter()
                .filter(|row| row.ts_event >= start)
                .map(|row| {
                    Ok(Example {
                        realised: Some(realised.of(row, side)?),
                        ..Example::of(row, realised.gained(row, side))
                    })
                })
                .collect::<anyhow::Result<Vec<_>>>();
            Some(match examples {
                Ok(examples) => self.fit(level, at, examples),
                Err(e) => kept(&e),
            })
        };
        let (long, short) = (side(Side::Long), side(Side::Short));
        let meta = self.meta.as_ref().map(|(level, meta)| {
            let examples = meta.window_start(at).and_then(|start| {
                let start = UnixNanos::from(start);
                let rows: Vec<&DatasetRow> =
                    rows.iter().filter(|row| row.ts_event >= start).collect();
                meta.examples(&rows, self.realised.as_ref())
            });
            match examples {
                Ok(examples) => self.fit(level, at, examples),
                Err(e) => kept(&e),
            }
        });
        Trained { long, short, meta }
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
        let holdout = at - level.holdout;
        let (fitted, calibrating, held_out) = split(examples, holdout - level.holdout, holdout);
        let set = TrainingSet::new(
            level.feature_names.len(),
            fitted.iter().flat_map(|row| row.features.clone()).collect(),
            fitted.iter().map(|row| row.label).collect(),
        )
        .context("fitting on the window before the calibration")?;
        let model = level.learner.fit(&set)?;
        let calibration = Calibration::fit(
            &calibrating
                .iter()
                .map(|row| Ok((model.predict(&row.features)?, row.label)))
                .collect::<anyhow::Result<Vec<_>>>()?,
        )
        .context("calibrating on the window before the holdout")?;
        let payoff = level
            .side
            .map(|_| {
                let realised: Vec<f64> = fitted
                    .iter()
                    .chain(&calibrating)
                    .filter_map(|row| row.realised)
                    .collect();
                Payoff::of(&realised)
            })
            .transpose()?;
        let saved = model.save()?;
        let model = LoadedModel::new(model, calibration, payoff);
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
                side: level.side,
                learner: level.learner.clone(),
                calibration,
                payoff,
                score,
                probes,
            },
            model: saved,
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
            side: None,
            window: config.window,
            holdout: config.holdout,
            min_score: config.min_score,
            learner: config.learner.clone(),
            store: meta.store().clone(),
        }
    }
}

impl Example {
    /// The example of `row`, of `label`, with no realised value.
    pub(super) fn of(row: &DatasetRow, label: bool) -> Self {
        Self {
            features: row.features.clone(),
            label,
            realised: None,
            ts_event: row.ts_event,
            settled_at: row.settled_at,
        }
    }
}

impl Trained {
    /// Whether every model trained was published.
    #[must_use]
    pub fn published(&self) -> bool {
        [&self.long, &self.short, &self.meta]
            .into_iter()
            .flatten()
            .all(|outcome| matches!(outcome, Outcome::Published { .. }))
    }
}

/// A train of a long model alone.
impl From<Outcome> for Trained {
    fn from(long: Outcome) -> Self {
        Self {
            long: Some(long),
            short: None,
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

/// `examples` split at the calibration's start, `calibration`, and the
/// holdout's, `holdout`: those before the calibration settled by its
/// start, fitted on; those from it to the holdout settled by the
/// holdout's start, calibrated on; and those from the holdout, held out.
pub(super) fn split(
    examples: Vec<Example>,
    calibration: Timestamp,
    holdout: Timestamp,
) -> (Vec<Example>, Vec<Example>, Vec<Example>) {
    let (calibration, holdout) = (UnixNanos::from(calibration), UnixNanos::from(holdout));
    let (mut fitted, mut calibrating, mut held_out) = (vec![], vec![], vec![]);
    for row in examples {
        if row.ts_event >= holdout {
            held_out.push(row);
        } else if row.ts_event >= calibration {
            if row.settled_at <= holdout {
                calibrating.push(row);
            }
        } else if row.settled_at <= calibration {
            fitted.push(row);
        }
    }
    (fitted, calibrating, held_out)
}

/// The indices of at most `PROBES` of `len` rows, spread evenly from the
/// first.
fn spread(len: usize) -> impl Iterator<Item = usize> {
    let count = len.min(PROBES);
    (0..count).map(move |i| i * len / count)
}

/// A train of one model reads as its outcome; of more, each model's.
impl fmt::Display for Trained {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let outcomes: Vec<(&str, &Outcome)> = [
            ("the long model", &self.long),
            ("the short model", &self.short),
            ("the meta model", &self.meta),
        ]
        .into_iter()
        .filter_map(|(name, outcome)| outcome.as_ref().map(|outcome| (name, outcome)))
        .collect();
        match &outcomes[..] {
            [(_, outcome)] => write!(f, "{outcome}"),
            outcomes => {
                let named: Vec<String> = outcomes
                    .iter()
                    .map(|(name, outcome)| format!("{name}: {outcome}"))
                    .collect();
                write!(f, "{}", named.join("; "))
            }
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
        instruments::{InstrumentAny, stubs::audusd_sim},
        types::{Money, Quantity},
    };
    use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
    use rust_decimal::Decimal;

    use super::*;
    use crate::model::{
        config::{Margins, MetaConfig},
        decide::{DecisionRule, FeatureRow},
        learner::LearnerConfig,
        targets::{
            Barrier, Distance, Distances, Exit, Label, Outcome as TargetOutcome, Outcomes,
            TargetRow, TargetsConfig,
        },
    };

    const AUD: &str = "AUD/USD.SIM";
    const STRATEGY: &str = "TRAIN-001";

    fn features(width: usize) -> Rc<FeatureSet> {
        (0..width)
            .fold(FeatureSet::builder(), |builder, i| {
                builder.quotes(
                    InstrumentId::from(AUD),
                    &format!("mid_{i}"),
                    SimpleMovingAverage::new(i + 1, Some(PriceType::Mid)),
                )
            })
            .build()
            .unwrap()
    }

    /// The fake learner on hourly decisions of one AUD/USD, learning the
    /// barrier (closed after 2 hours; a horizon of 1 hour besides) on both
    /// sides, on a window of 24 hours, the last 6 held out and the 6
    /// before them calibrated on, published from an AUC of 0.5.
    fn config() -> ModelConfig {
        ModelConfig::builder()
            .decision(DecisionRule::Every(SignedDuration::from_hours(1)))
            .targets(
                TargetsConfig::builder()
                    .instrument_id(InstrumentId::from(AUD))
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
            .margins(Margins {
                long: Some(0.0),
                short: Some(0.0),
            })
            .min_score(0.5)
            .learner(LearnerConfig::Fake {})
            .build()
    }

    /// What the strategy reads: the quotes of AUD/USD.
    fn requirements() -> DataRequirements {
        DataRequirements {
            bar_types: vec![],
            ticks: vec![crate::strategy::TickSeries {
                data: nautilus_model::data::NautilusDataType::QuoteTick,
                instrument_id: InstrumentId::from(AUD),
            }],
            warmup: SignedDuration::ZERO,
        }
    }

    /// A commission rate of 30 per million.
    fn commission() -> Commission {
        Commission::Rate("0.00003".to_string())
    }

    /// The catalog in `dir`, holding AUD/USD, whose stops' worth a train
    /// reads.
    fn catalog(dir: &Path) -> std::path::PathBuf {
        let catalog = dir.join("catalog");
        std::fs::create_dir_all(&catalog).unwrap();
        ParquetDataCatalog::new(&catalog, None, None, None, None)
            .write_instruments(vec![InstrumentAny::CurrencyPair(audusd_sim())])
            .unwrap();
        catalog
    }

    /// The training in `dir`, its catalog and its store side by side.
    fn training(dir: &Path, config: ModelConfig) -> Training {
        Training::new(
            &catalog(dir),
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

    /// The row of the decision at `time` on `features`, settled `after` it:
    /// on the barrier, each side 2 from its stop, a side winning 3 USD at
    /// its take profit, losing 2 at its stop.
    fn row_of(
        time: &str,
        features: Vec<f64>,
        (long_wins, short_wins): (bool, bool),
        after: SignedDuration,
    ) -> TargetRow {
        let ns = UnixNanos::from(at(time));
        let outcome = |pnl: &str, exit, distances| TargetOutcome {
            pnl: Money::from(pnl),
            exit,
            distances,
        };
        let barrier = |wins| {
            let distances = Some(Distances {
                take_profit: Decimal::from(3),
                stop: Decimal::from(2),
            });
            if wins {
                outcome("3 USD", Exit::TakeProfit, distances)
            } else {
                outcome("-2 USD", Exit::Stop, distances)
            }
        };
        TargetRow {
            decision: FeatureRow {
                strategy_id: StrategyId::from(STRATEGY),
                features,
                ts_event: ns,
                ts_init: ns,
            },
            barrier: Some(Outcomes {
                long: barrier(long_wins),
                short: barrier(short_wins),
            }),
            horizon: Some(Outcomes {
                long: outcome("0.5 USD", Exit::Time, None),
                short: outcome("-0.5 USD", Exit::Time, None),
            }),
            settled_at: UnixNanos::from(at(time) + after),
        }
    }

    /// The row of `row_of`, its long winning if `wins` and its short
    /// losing, or the reverse.
    fn row(time: &str, features: Vec<f64>, wins: bool, after: SignedDuration) -> TargetRow {
        row_of(time, features, (wins, !wins), after)
    }

    fn hour() -> SignedDuration {
        SignedDuration::from_hours(1)
    }

    /// At midnight of the 7th: the window is the 6th, the calibration from
    /// 12:00, the holdout from 18:00. Fitted, a win and a loss of the long
    /// (the fake's mean 0.5). Calibrated on, the fake predicting its
    /// feature, a loss at 0.25 and a win at 0.75, the loss's stop 4 away:
    /// Platt's targets 1/3 and 2/3, the curve 1 / (1 + 4^(1 - 2p)). Held
    /// out, a win at 0.8 and a loss at 0.3, calibrated 1 / (1 + 4^-0.6)
    /// and 1 / (1 + 4^0.4) (AUC 1). Around them, wins on the feature 0.1
    /// that would change the fit or the calibration if taken: one before
    /// the window, one before the calibration settling after its start,
    /// one before the holdout settling after its start, one in the holdout
    /// settling after the window's end. The short is the reverse of the
    /// long throughout.
    fn rows() -> Vec<TargetRow> {
        let mut rows = vec![
            row("2026-01-05T23:00:00Z", vec![0.1], true, hour()),
            row("2026-01-06T08:00:00Z", vec![0.9], true, hour()),
            row("2026-01-06T10:00:00Z", vec![0.9], false, hour()),
            row(
                "2026-01-06T11:30:00Z",
                vec![0.1],
                true,
                SignedDuration::from_mins(60),
            ),
            row("2026-01-06T13:00:00Z", vec![0.25], false, hour()),
            row("2026-01-06T15:00:00Z", vec![0.75], true, hour()),
            row(
                "2026-01-06T17:00:00Z",
                vec![0.1],
                true,
                SignedDuration::from_mins(90),
            ),
            row("2026-01-06T19:00:00Z", vec![0.8], true, hour()),
            row("2026-01-06T20:00:00Z", vec![0.3], false, hour()),
            row("2026-01-06T23:30:00Z", vec![0.1], true, hour()),
        ];
        let barrier = rows[4].barrier.as_mut().unwrap();
        for outcome in [&mut barrier.long, &mut barrier.short] {
            outcome.distances.as_mut().unwrap().stop = Decimal::from(4);
        }
        rows
    }

    fn close(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < 1e-4, "{actual} != {expected}");
    }

    /// Worked by hand on `rows`: each side fitted on the rows before the
    /// calibration settled at its start, calibrated on those before the
    /// holdout settled at its start, scored on the holdout. The long's
    /// realised R, its P&L over its stop's worth, 1 AUD/USD 2 or 4 away:
    /// wins of 1.5R, losses of 1R and 0.5R; the short's wins of 1.5R and
    /// 0.75R, losses of 1R.
    #[test]
    fn a_train_fits_calibrates_and_scores_each_side_on_its_part_of_the_window() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let t = at("2026-01-07T00:00:00Z");

        let trained = training.train_at(t, |_, _| Ok(rows()));

        let published = Outcome::Published {
            valid_from: t,
            score: 1.0,
        };
        assert_eq!(
            trained,
            Trained {
                long: Some(published.clone()),
                short: Some(published),
                meta: None,
            }
        );
        let long = training.store(Side::Long).unwrap().read(t).unwrap();
        let fake = LearnerConfig::Fake {}.load(&long.model).unwrap();
        // The fake predicts the mean of its training labels where the first
        // feature is missing.
        assert_eq!(fake.predict(&[f64::NAN]).unwrap(), 0.5);
        let calibration = long.manifest.calibration;
        close(calibration.apply(0.25), 1.0 / 3.0);
        close(calibration.apply(0.75), 2.0 / 3.0);
        let probes: Vec<&Vec<f64>> = long.manifest.probes.iter().map(|p| &p.row).collect();
        assert_eq!(probes, [&vec![0.8], &vec![0.3]]);
        close(
            long.manifest.probes[0].prediction,
            1.0 / (1.0 + 4_f64.powf(-0.6)),
        );
        close(
            long.manifest.probes[1].prediction,
            1.0 / (1.0 + 4_f64.powf(0.4)),
        );
        let payoff = long.manifest.payoff.unwrap();
        close(payoff.win, 1.5);
        close(payoff.loss, 0.75);
        assert_eq!(long.manifest.side, Some(Side::Long));
        let short = training.store(Side::Short).unwrap().read(t).unwrap();
        assert_eq!(short.manifest.side, Some(Side::Short));
        assert!(
            short.manifest.calibration.apply(0.25) > short.manifest.calibration.apply(0.75),
            "the short wins where the long loses: {:?}",
            short.manifest.calibration
        );
        let payoff = short.manifest.payoff.unwrap();
        close(payoff.win, 1.125);
        close(payoff.loss, 1.0);
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

        let manifest = training
            .store(Side::Long)
            .unwrap()
            .read(t)
            .unwrap()
            .manifest;
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

    /// Margins of the long alone: no short model is trained nor stored.
    #[test]
    fn a_long_only_model_trains_no_short() {
        let dir = tempfile::tempdir().unwrap();
        let long_only = ModelConfig {
            margins: Margins {
                long: Some(0.0),
                short: None,
            },
            ..config()
        };
        let training = training(dir.path(), long_only);

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows()));

        assert!(
            matches!(trained.long, Some(Outcome::Published { .. })),
            "{trained:?}"
        );
        assert_eq!((trained.short, trained.meta), (None, None));
        assert!(training.store(Side::Short).is_none());
    }

    /// The holdout's long wins at 0.8 and loses at 0.3, as its short does:
    /// the short, calibrated decreasing, ranks them the wrong way round
    /// (AUC 0) and keeps its current model, while the long publishes.
    #[test]
    fn a_side_below_the_minimum_score_keeps_its_model_and_the_other_publishes() {
        let dir = tempfile::tempdir().unwrap();
        let t = at("2026-01-07T00:00:00Z");
        training(dir.path(), config()).train_at(t, |_, _| Ok(rows()));
        let worse = |mut rows: Vec<TargetRow>| {
            rows[7] = row_of("2026-01-06T19:00:00Z", vec![0.8], (true, true), hour());
            rows[8] = row_of("2026-01-06T20:00:00Z", vec![0.3], (false, false), hour());
            // In the holdout of the train an hour later: dropped.
            rows.pop();
            rows
        };
        let gated = training(dir.path(), config());

        let trained = gated.train_at(t + hour(), |_, _| Ok(worse(rows())));

        assert_eq!(
            trained.short,
            Some(Outcome::Kept(Kept::BelowMinScore {
                score: 0.0,
                min_score: 0.5
            }))
        );
        assert_eq!(
            gated.store(Side::Short).unwrap().valid_froms().unwrap(),
            [t]
        );
        assert_eq!(
            trained.long,
            Some(Outcome::Published {
                valid_from: t + hour(),
                score: 1.0
            })
        );
    }

    #[test]
    fn a_failed_replay_keeps_the_current_models() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| {
            anyhow::bail!("the catalog does not cover the window")
        });

        for outcome in [trained.long.unwrap(), trained.short.unwrap()] {
            let Outcome::Kept(Kept::Failed(why)) = outcome else {
                panic!("{outcome:?}");
            };
            assert!(why.contains("does not cover"), "{why}");
        }
        assert!(
            training
                .store(Side::Long)
                .unwrap()
                .valid_froms()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn a_holdout_of_one_label_fails() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let mut rows = rows();
        rows.remove(8);

        let outcome = training
            .train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows))
            .long
            .unwrap();

        assert!(
            matches!(outcome, Outcome::Kept(Kept::Failed(_))),
            "{outcome:?}"
        );
    }

    /// No row between 12:00 and 18:00: nothing to calibrate the model on.
    #[test]
    fn a_calibration_without_a_row_fails() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let mut rows = rows();
        rows.drain(4..7);

        let outcome = training
            .train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows))
            .long
            .unwrap();

        let Outcome::Kept(Kept::Failed(why)) = outcome else {
            panic!("{outcome:?}");
        };
        assert!(why.contains("calibrat"), "{why}");
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
            .long
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

    /// The calibration of the primary's models of the meta tests.
    const CALIBRATION: Calibration = Calibration { a: -4.0, b: 2.0 };

    /// Publishes in `store` a primary baseline valid from `valid_from`,
    /// predicting `probability` on every row, calibrated by `CALIBRATION`,
    /// expecting a win and a loss of 1R.
    fn primary(store: &ModelStore, side: Side, valid_from: &str, probability: f64) {
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
                    side: Some(side),
                    learner: LearnerConfig::Baseline {},
                    calibration: CALIBRATION,
                    payoff: Some(Payoff {
                        win: 1.0,
                        loss: 1.0,
                    }),
                    score: 0.6,
                    probes: vec![],
                },
                model: model.save().unwrap(),
            })
            .unwrap();
    }

    /// The decisions of the 6th: one before the primary's first models (a
    /// win of the long), four long trades of the models valid from 06:00,
    /// the long's predicting 0.8 and the short's 0.2: fitted on, a win and
    /// a loss of the long (meta labels true, false); calibrated on, the
    /// meta's fake predicting their first feature, a loss at 0.25 and a win
    /// at 0.75. Two short trades of the models valid from 17:00, the long's
    /// 0.2 and the short's 0.8, held out: a win and a loss of the long (the
    /// short's labels false, true), the meta's fake predicting 0.3 and 0.7.
    fn meta_rows() -> Vec<TargetRow> {
        vec![
            row("2026-01-06T05:00:00Z", vec![0.5], true, hour()),
            row("2026-01-06T08:00:00Z", vec![0.5], true, hour()),
            row("2026-01-06T10:00:00Z", vec![0.5], false, hour()),
            row("2026-01-06T13:00:00Z", vec![0.25], false, hour()),
            row("2026-01-06T15:00:00Z", vec![0.75], true, hour()),
            row("2026-01-06T19:00:00Z", vec![0.3], true, hour()),
            row("2026-01-06T20:00:00Z", vec![0.7], false, hour()),
        ]
    }

    /// The primary's models: from 06:00 a long, from 17:00 a short, and
    /// from 21:00, after every row, a long again, which no row may use.
    fn primaries(training: &Training) {
        let (long, short) = (
            training.store(Side::Long).unwrap(),
            training.store(Side::Short).unwrap(),
        );
        for (valid_from, long_probability) in [
            ("2026-01-06T06:00:00Z", 0.8),
            ("2026-01-06T17:00:00Z", 0.2),
            ("2026-01-06T21:00:00Z", 0.9),
        ] {
            primary(long, Side::Long, valid_from, long_probability);
            primary(short, Side::Short, valid_from, 1.0 - long_probability);
        }
    }

    #[test]
    fn a_meta_model_learns_the_primarys_side_from_its_out_of_sample_decisions() {
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
        assert_eq!(stored.manifest.side, None);
        let short = CALIBRATION.apply(0.8);
        let probes: Vec<&Vec<f64>> = stored.manifest.probes.iter().map(|p| &p.row).collect();
        assert_eq!(probes, [&vec![0.3, -1.0, short], &vec![0.7, -1.0, short]]);
        // Fitted on the two long trades, a win and a loss: the decision
        // before the primary's first model is no row.
        let model = LearnerConfig::Fake {}.load(&stored.model).unwrap();
        assert_eq!(model.predict(&[f64::NAN, 1.0, 0.8]).unwrap(), 0.5);
    }

    /// Margins of the long alone: the short's models are never asked, and
    /// the meta's rows are the long's trades.
    #[test]
    fn a_long_only_primary_gives_its_meta_model_buys_alone() {
        let dir = tempfile::tempdir().unwrap();
        let long_only = ModelConfig {
            margins: Margins {
                long: Some(0.0),
                short: None,
            },
            ..with_meta(24)
        };
        let training = training(dir.path(), long_only);
        for (valid_from, probability) in
            [("2026-01-06T06:00:00Z", 0.8), ("2026-01-06T17:00:00Z", 0.6)]
        {
            primary(
                training.store(Side::Long).unwrap(),
                Side::Long,
                valid_from,
                probability,
            );
        }
        let t = at("2026-01-07T00:00:00Z");

        let trained = training.train_at(t, |_, _| Ok(meta_rows()));

        assert!(
            matches!(trained.meta, Some(Outcome::Published { .. })),
            "{trained:?}"
        );
        let stored = training.meta_store().unwrap().read(t).unwrap();
        let long = CALIBRATION.apply(0.6);
        let probes: Vec<&Vec<f64>> = stored.manifest.probes.iter().map(|p| &p.row).collect();
        assert_eq!(probes, [&vec![0.3, 1.0, long], &vec![0.7, 1.0, long]]);
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
            trained.long.is_some() && trained.short.is_some() && trained.meta.is_some(),
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
    /// true, false, true); calibrated on, a short losing at 0.25 and a long
    /// winning at 0.75; held out, a long losing at 0.3 and a short winning
    /// at 0.7 (false, true).
    #[test]
    fn a_rule_primarys_meta_model_learns_whether_the_rules_side_wins() {
        let dir = tempfile::tempdir().unwrap();
        let features = FeatureSet::builder()
            .quotes(
                InstrumentId::from(AUD),
                "mid_0",
                SimpleMovingAverage::new(1, Some(PriceType::Mid)),
            )
            .primary()
            .build()
            .unwrap();
        let config = config();
        let training = Training::of_rule(
            &catalog(dir.path()),
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
                "2026-01-06T06:00:00Z",
                vec![0.5, buy, f64::NAN],
                true,
                hour(),
            ),
            row(
                "2026-01-06T08:00:00Z",
                vec![0.5, sell, f64::NAN],
                true,
                hour(),
            ),
            row(
                "2026-01-06T10:00:00Z",
                vec![0.5, sell, f64::NAN],
                false,
                hour(),
            ),
            row(
                "2026-01-06T13:00:00Z",
                vec![0.25, sell, f64::NAN],
                true,
                hour(),
            ),
            row(
                "2026-01-06T15:00:00Z",
                vec![0.75, buy, f64::NAN],
                true,
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

        assert_eq!((trained.long, trained.short), (None, None));
        assert_eq!(
            trained.meta,
            Some(Outcome::Published {
                valid_from: t,
                score: 1.0
            })
        );
        let stored = training.meta_store().unwrap().read(t).unwrap();
        let model = LearnerConfig::Fake {}.load(&stored.model).unwrap();
        assert!(
            (model.predict(&[f64::NAN, buy, f64::NAN]).unwrap() - 2.0 / 3.0).abs() < 1e-12,
            "{stored:?}"
        );
    }

    #[test]
    fn a_train_of_several_models_reads_as_each_outcome() {
        let published = Outcome::Published {
            valid_from: at("2026-01-07T00:00:00Z"),
            score: 0.625,
        };
        let trained = Trained {
            long: Some(published.clone()),
            short: Some(Outcome::Kept(Kept::Failed("no short row".to_string()))),
            meta: Some(Outcome::Kept(Kept::Failed("no row".to_string()))),
        };

        let text = trained.to_string();

        assert!(!trained.published());
        assert!(Trained::from(published).published());
        assert!(
            text.contains("long") && text.contains("short") && text.contains("meta"),
            "{text}"
        );
        assert!(
            text.contains("no short row") && text.contains("no row"),
            "{text}"
        );
    }
}
