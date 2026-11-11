//! Training at a time: the one operation that turns a strategy's dataset
//! into published models, or keeps the current ones, for retrains in a
//! backtest and live alike.
//!
//! A strategy has a model for each side it trades (the sides of its
//! margins, `ModelConfig::margins`): the long's learns whether the long of
//! the target gains after its costs, the short's whether its short does,
//! both on the same rows, window and holdout, each reading a row's features
//! then its own side's payoff features (`dataset::DatasetRow::side`), under
//! the names its store checks it against (`store::SideLevel::feature_names`).
//! Each is fitted and calibrated on its own, and the sides are judged and
//! published together.
//!
//! What a train at `t` does to each model is the schedule's plan
//! (`schedule::Schedule::plan`), from the stores' valid-froms alone: a
//! model with a current model at `t` keeps it, in use (retrained, the one
//! valid from `t`; never retrained, the latest valid by `t`, so each model
//! is trained once); a meta with no side's model before `t` waits for the
//! sides' decisions; the others are fitted, as of `t`, or, never retrained,
//! the sides with neither them nor the meta in use, as of `t` less the
//! meta's window, the meta at `t` on their decisions. With nothing to fit
//! the train replays nothing.
//!
//! A model fitted as of `t'`, the train's time or before it, takes the
//! rows of the window ending at `t'` (`ModelConfig::window`), the days
//! the dataset lacks built by one replay to the train's time from the
//! first row a fit reads (`Dataset::settled`), and drops the rows not
//! settled at `t'`: no row's target reaches past `t'`, so the purge
//! between the rows and the model's first use is the target's limit, by
//! construction. The window ends in the holdout (`ModelConfig::holdout`)
//! and, before it, the calibration, as long, each part's rows taken with
//! the same gap: a model is fitted on the rows before the calibration
//! settled at its start, calibrated by Platt scaling
//! (`calibration::Calibration`) on its predictions on the calibration's
//! rows settled at the holdout's start, and judged on the holdout
//! (`holdout::Holdout`). A side also has its expected payoff
//! (`payoff::Payoff`), of the realised values of the rows before the holdout.
//!
//! The gate judges the decision the handle returns, on the target's trade;
//! what a strategy then does with a decision stays outside its figures. Every
//! side the plan fits is fitted before any publishes. Each side's calibrated
//! probabilities are judged on their own: a log loss not below the baseline's,
//! the training mean of the labels of the rows it was fitted and calibrated on
//! (`baseline`), drops the side, refused; its AUC is reported. The earnings are
//! judged on the trades picked over the holdout's rows
//! (`deciding::DecidingModels::pick_among`), on each side's row, among the
//! surviving candidates and, for any other side, the model the handle would
//! use at `t'`: the latest valid at or before it, as the store lists it, which
//! the plan cannot tell, a retrained plan marking a side with only an older
//! model for fitting. That model is out of sample on these rows: it was fitted
//! and calibrated on rows before its valid-from less the holdout, which is part
//! of the store's key. A row where both sides clear their margins counts one
//! trade, the side expected the most; a tie, none. The minimums
//! (`ModelConfig::min_pnl`, `ModelConfig::min_trades`, `Holdout::refusal`) read
//! those picks, beside every trade of the sides traded on the rows, those with
//! a model among them: a side with neither a candidate nor an older model does
//! not trade there.
//!
//! The surviving sides publish together, in their sides' stores (`store`),
//! valid from `t'`, each with its calibration, its baseline, which its
//! out-of-sample scores are read beside (`walk::WalkForward::scores`), its
//! payoff, the earnings judged beside its own log loss, the baseline's and its
//! AUC, and its manifest probing it on rows of the holdout; or none does and
//! the current models stay. A profitable side paired with a losing one is
//! refused with it. A side refused on its log loss reports its own log loss and
//! the earnings of the remaining picks, or none if no side survives. A publish
//! stays a rename per side: a crash between them leaves one published, which
//! the next train keeps in use and judges the other beside.
//!
//! With a meta model (`model::meta`), the sides train first, then the
//! meta, on the same rows, of the one replay: the meta's window is its
//! own (`MetaConfig::window`), from the sides' first model on, and its
//! rows are made from the sides' out-of-sample decisions, those of the
//! models of the stores in use at each decision, read once the
//! sides published: the ones just published valid from `t` used by none,
//! those of a train at two times, valid from the window's start, used
//! throughout it; and should the sides of such a train publish none, the
//! meta waits for the sides' decisions (`Plan::meta_after_sides`). Fitting and
//! calibrating are a side's; the meta is judged alone, on its own holdout, by
//! its log loss and the trades of the side picked taken from its threshold
//! (`MetaConfig::threshold`), and published in its own store.
//!
//! A train never errors: what goes wrong is an outcome, the current model
//! kept with the reason (`Kept`), for each model trained. It succeeds when
//! it kept no model (`Trained::succeeded`).

use std::{fmt, path::Path};

use anyhow::Context;
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::StrategyId;

use super::{
    calibration::Calibration,
    config::ModelConfig,
    dataset::{Dataset, DatasetRow, DatasetVenue, SchemaMismatch},
    deciding::{Candidate, DecidingModels},
    features::FeatureSet,
    holdout::{Earnings, Example, Holdout, Minimums, Refusal, Threshold},
    learner::{LearnerConfig, SavedModel, TrainingSet},
    meta,
    payoff::Payoff,
    schedule::{Models, Retrain, Schedule, Step},
    score,
    store::{
        LoadedModel, MetaLevel, ModelManifest, ModelSet, ModelStore, Probe, SideLevel, SideModel,
        StoredModel,
    },
    targets::{Side, TargetConfig, TargetRow},
};
use crate::strategy::DataRequirements;

/// The most rows of the holdout a published model is probed on.
const PROBES: usize = 16;

/// The training of one strategy's models: its sides, its meta model, or
/// both.
#[derive(Debug)]
pub struct Training {
    strategy_id: StrategyId,
    target: TargetConfig,
    dataset: Dataset,
    /// What each train does to each model.
    schedule: Schedule,
    /// The strategy's models: the stores the plan reads, and the side
    /// models the meta's rows are made from.
    models: ModelSet,
    /// Each model: of each side the strategy trades, and the meta model, if
    /// any.
    levels: Models<Level>,
}

/// How the models of one level, a side or a meta, are fitted, calibrated and
/// published.
#[derive(Debug)]
struct Level {
    /// The names of the features of its rows, in order.
    feature_names: Vec<String>,
    holdout: SignedDuration,
    /// The least its holdout is published with.
    minimums: Minimums,
    learner: LearnerConfig,
    store: ModelStore,
}

/// A model of a level fitted and calibrated as of a time, not published: what
/// its manifest takes of the fit, and what it is judged on.
#[derive(Debug)]
struct Fitted {
    saved: SavedModel,
    calibration: Calibration,
    baseline: f64,
    /// The rows it was fitted and calibrated on.
    trained: Vec<Example>,
    held_out: Vec<Example>,
    /// Its calibrated probability on each row held out.
    probabilities: Vec<f64>,
    /// Its holdout, no earnings judged yet.
    holdout: Holdout,
}

/// A side's model fitted as of `at`, not published, its candidate at the gate.
#[derive(Debug)]
struct SideFit<'a> {
    side: Side,
    margin: f64,
    level: &'a Level,
    at: Timestamp,
    model: SideModel,
    payoff: Payoff,
    fitted: Fitted,
    /// The rows of its holdout, of both sides' trades.
    held_out: Vec<&'a DatasetRow>,
}

/// What a step comes to on the replay's rows.
enum Planned<'a> {
    /// A model is fitted as of `at` on `rows`.
    Fit {
        at: Timestamp,
        rows: Vec<&'a DatasetRow>,
    },
    /// None is: the model's outcome.
    Done(Outcome),
}

/// What a train did: the outcome of each model of the strategy, none for
/// a model it has not (a side it does not trade, or the meta of
/// a strategy without one).
pub type Trained = Models<Outcome>;

/// What a train did to one model.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// A model was published, valid from `valid_from`, with its holdout.
    Published {
        valid_from: Timestamp,
        holdout: Holdout,
    },
    /// None was fitted: the current model, valid from `valid_from`, is in
    /// use (`schedule::Step::Keep`), of a strategy retrained as `retrain`
    /// says, which says how it reads (`Retrain::in_use`).
    InUse {
        valid_from: Timestamp,
        retrain: Retrain,
    },
    /// No model was published: the current one, if any, stays.
    Kept(Kept),
}

/// Why a train kept the current model.
#[derive(Debug, Clone, PartialEq)]
pub enum Kept {
    /// The holdout falls short of the minimums; why, and the holdout.
    Refused { why: Refusal, holdout: Holdout },
    /// The replay, the dataset, the fit or the store failed, or the window
    /// had no row to fit, to calibrate on or to expect a payoff from, or
    /// no holdout to judge; the error.
    Failed(String),
    /// The dataset holds rows that do not match the strategy's features; the
    /// mismatch.
    SchemaMismatch(String),
    /// A meta waits for the sides' decisions to learn from: no side's model
    /// is valid before the train, so none was fitted
    /// (`schedule::Step::Wait`).
    Waiting,
}

impl Training {
    /// The training of `strategy_id`'s models, one for each side of its
    /// margins, and of its meta model if `config` has one, deciding with
    /// `features`, on its dataset, for a strategy of `requirements`,
    /// run on `venue`, in the catalog at `catalog` and its models in the
    /// store under `models`, as `config` says.
    ///
    /// # Errors
    ///
    /// Returns an error if the target is invalid, or a store cannot be keyed.
    pub fn new(
        catalog: &Path,
        models: &Path,
        strategy_id: StrategyId,
        features: &FeatureSet,
        config: ModelConfig,
        requirements: &DataRequirements,
        venue: &DatasetVenue,
    ) -> anyhow::Result<Self> {
        let models = ModelSet::new(models, strategy_id, features, &config, requirements, venue)?;
        let side = |side| {
            models.side(side).map(|traded| Level {
                feature_names: traded.feature_names().to_vec(),
                holdout: config.holdout,
                minimums: config.minimums(),
                learner: config.learner.clone(),
                store: traded.store().clone(),
            })
        };
        let levels = Models {
            long: side(Side::Long),
            short: side(Side::Short),
            meta: models.meta().map(Level::of_meta),
        };
        Ok(Self {
            strategy_id,
            dataset: Dataset::new(
                catalog,
                strategy_id,
                features,
                config.decision,
                config.target.clone(),
                requirements,
                venue,
            )?,
            schedule: Schedule::of_model(&config),
            models,
            levels,
            target: config.target,
        })
    }

    /// Trains at `at` as the schedule plans it (`Schedule::plan`), from
    /// the stores' valid-froms: `replay` runs the strategy over a span of
    /// the days the dataset lacks and returns the rows it settled (see
    /// `Dataset::settled`), once, from where the plan's fits start, and not
    /// at all if it fits none. Fits each side's model the plan fits, as of
    /// its time, and judges the sides together (`sides`), then the meta model
    /// as the plan has it once the sides are published
    /// (`Plan::meta_after_sides`), its rows the sides' decisions by the
    /// store as the sides left it: each fitted is published, or its current
    /// model kept with the reason; a model the plan keeps is in use, and a
    /// meta the plan makes wait is kept, waiting.
    pub fn train_at(
        &self,
        at: Timestamp,
        replay: impl FnOnce(Timestamp, Timestamp) -> anyhow::Result<Vec<TargetRow>>,
    ) -> Trained {
        let listed = match self.models.valid_froms() {
            Ok(listed) => listed,
            Err(e) => return self.levels.as_ref().map(|_| kept(&e)),
        };
        let plan = self.schedule.plan(at, &listed);
        let rows = match plan.replay_from() {
            Some(from) => self.dataset.settled(from, at, replay),
            None => Ok(vec![]),
        };
        let sides = self.sides(plan.steps(), &listed, &rows);
        let meta = || {
            let level = self.levels.meta.as_ref()?;
            let threshold = Threshold(self.models.meta()?.config().threshold);
            let step = match self.models.valid_froms() {
                Ok(valid_froms) => plan.meta_after_sides(&valid_froms)?,
                Err(e) => return Some(kept(&e)),
            };
            Some(match self.planned(step, &rows) {
                Planned::Done(outcome) => outcome,
                Planned::Fit { at, rows } => self
                    .fit_meta(level, threshold, at, &rows)
                    .unwrap_or_else(|e| kept(&e)),
            })
        };
        Trained {
            meta: meta(),
            ..sides
        }
    }

    /// What `step` comes to on the replay's `rows`: its current model in use; a
    /// meta waiting; the current model kept if the replay failed; or a fit as
    /// of the step's time on the step's rows, those of `[from, at)` settled by
    /// `at` (`Step::Fit`).
    fn planned<'a>(&self, step: Step, rows: &'a anyhow::Result<Vec<DatasetRow>>) -> Planned<'a> {
        let (from, at) = match step {
            Step::Keep { valid_from } => {
                return Planned::Done(Outcome::InUse {
                    valid_from,
                    retrain: self.schedule.retrain(),
                });
            }
            Step::Wait => return Planned::Done(Outcome::Kept(Kept::Waiting)),
            Step::Fit { from, at } => (from, at),
        };
        let rows = match rows {
            Ok(rows) => rows,
            Err(e) => return Planned::Done(kept(e)),
        };
        let (start, end) = (UnixNanos::from(from), UnixNanos::from(at));
        Planned::Fit {
            at,
            rows: rows
                .iter()
                .filter(|row| start <= row.ts_event && row.ts_event < end && row.settled_at <= end)
                .collect(),
        }
    }

    /// What a train does to the sides at `steps`, on the replay's `rows`, their
    /// stores as `listed` before any fit: a side the plan does not fit keeps
    /// its outcome; each one it does is fitted, unpublished, its failure its
    /// outcome; a fit failing its log loss drops out, refused; and the
    /// surviving fits are judged together, on what their picks earn (`earned`),
    /// and published together (`publish_sides`).
    fn sides(
        &self,
        steps: &Models<Step>,
        listed: &Models<Vec<Timestamp>>,
        rows: &anyhow::Result<Vec<DatasetRow>>,
    ) -> Trained {
        let mut trained = Trained {
            long: None,
            short: None,
            meta: None,
        };
        let (mut surviving, mut dropped) = (vec![], vec![]);
        for traded in self.models.sides() {
            let side = traded.side();
            let (Some(level), Some(&step)) = (self.levels.side(side), steps.side(side)) else {
                continue;
            };
            let fit = match self.planned(step, rows) {
                Planned::Done(outcome) => {
                    trained.set_side(side, outcome);
                    continue;
                }
                Planned::Fit { at, rows } => self.fit_side(traded, level, at, rows),
            };
            match fit {
                Err(e) => trained.set_side(side, kept(&e)),
                Ok(fit) => match fit.fitted.holdout.log_loss_refusal() {
                    Some(why) => dropped.push((side, why, fit.fitted.holdout)),
                    None => surviving.push(fit),
                },
            }
        }
        let earned = self.earned(&surviving, listed);
        for (side, why, holdout) in dropped {
            let holdout = Holdout {
                earnings: earned.as_ref().copied().unwrap_or_default(),
                ..holdout
            };
            trained.set_side(side, Outcome::Kept(Kept::Refused { why, holdout }));
        }
        match earned {
            Ok(earnings) => self.publish_sides(surviving, earnings, &mut trained),
            Err(e) => {
                for fit in surviving {
                    trained.set_side(fit.side, kept(&e));
                }
            }
        }
        trained
    }

    /// Fits `traded`'s model as of `at` on `rows`, in time order, its level
    /// `level`, not published.
    fn fit_side<'a>(
        &self,
        traded: &SideLevel,
        level: &'a Level,
        at: Timestamp,
        mut rows: Vec<&'a DatasetRow>,
    ) -> anyhow::Result<SideFit<'a>> {
        let side = traded.side();
        let examples = rows.iter().map(|row| Example::of_side(row, side)).collect();
        let (model, fitted) = level.fit(at, examples)?;
        let realised: Vec<f64> = fitted.trained.iter().map(Example::realised).collect();
        let payoff = Payoff::of(&realised)?;
        let holdout = UnixNanos::from(at - level.holdout);
        rows.retain(|row| row.ts_event >= holdout);
        Ok(SideFit {
            side,
            margin: traded.margin(),
            level,
            at,
            model: SideModel::new(model, payoff),
            payoff,
            fitted,
            held_out: rows,
        })
    }

    /// What the decision earns on the holdout of the `surviving` fits, the
    /// sides' one holdout: on each row the side picked among them and, for any
    /// other side, the latest model of its store, as `listed`, valid at or
    /// before the fits' time; beside every trade of those sides. None if no
    /// side survives.
    ///
    /// # Errors
    ///
    /// Returns an error if a model fails to predict.
    fn earned(
        &self,
        surviving: &[SideFit<'_>],
        listed: &Models<Vec<Timestamp>>,
    ) -> anyhow::Result<Earnings> {
        let Some(SideFit { at, held_out, .. }) = surviving.first() else {
            return Ok(Earnings::default());
        };
        let mut others: Vec<_> = self
            .models
            .sides()
            .filter(|traded| surviving.iter().all(|fit| fit.side != traded.side()))
            .map(|traded| (traded, traded.swap()))
            .collect();
        let older = others.iter_mut().filter_map(|(traded, swap)| {
            let (_, model) = swap.at(*at, listed.side(traded.side())?)?;
            Some(Candidate {
                side: traded.side(),
                margin: traded.margin(),
                model,
            })
        });
        let deciding: Vec<Candidate<'_>> = surviving
            .iter()
            .map(|fit| Candidate {
                side: fit.side,
                margin: fit.margin,
                model: &fit.model,
            })
            .chain(older)
            .collect();
        let mut trades = vec![];
        for row in held_out {
            let picked = DecidingModels::pick_among(deciding.iter().copied(), &row.sides())?;
            for Candidate { side, .. } in &deciding {
                let taken = picked.is_some_and(|picked| picked.side == *side);
                trades.push((taken, row.trade(*side)));
            }
        }
        Ok(Earnings::of(&trades))
    }

    /// The outcome of each of the `surviving` fits, each with its own holdout
    /// and `earnings`, in `trained`: all refused if their minimums refuse them,
    /// else published, the long first. A publish failing fails those not yet
    /// published, so none of them publishes.
    fn publish_sides(
        &self,
        surviving: Vec<SideFit<'_>>,
        earnings: Earnings,
        trained: &mut Trained,
    ) {
        let refusal = surviving.iter().find_map(|fit| {
            Holdout {
                earnings,
                ..fit.fitted.holdout
            }
            .refusal(fit.level.minimums)
        });
        let mut failed = None;
        for fit in surviving {
            let SideFit {
                side,
                level,
                at,
                payoff,
                fitted,
                ..
            } = fit;
            let holdout = Holdout {
                earnings,
                ..fitted.holdout
            };
            let outcome = match (refusal, &failed) {
                (Some(why), _) => Outcome::Kept(Kept::Refused { why, holdout }),
                (None, Some(e)) => kept(e),
                (None, None) => {
                    match self.publish(level, at, fitted, Some((side, payoff)), holdout) {
                        Ok(()) => Outcome::Published {
                            valid_from: at,
                            holdout,
                        },
                        Err(e) => {
                            let outcome = kept(&e);
                            failed = Some(e);
                            outcome
                        }
                    }
                }
            };
            trained.set_side(side, outcome);
        }
    }

    /// Fits the meta model of `level` as of `at` on the examples of `rows`, in
    /// time order, and publishes it, or keeps the current one with the reason,
    /// the trades of the side picked taken from `threshold`.
    fn fit_meta(
        &self,
        level: &Level,
        threshold: Threshold,
        at: Timestamp,
        rows: &[&DatasetRow],
    ) -> anyhow::Result<Outcome> {
        let examples = meta::examples(&self.models, rows)?;
        let (_, fitted) = level.fit(at, examples)?;
        let trades: Vec<_> = fitted
            .held_out
            .iter()
            .zip(&fitted.probabilities)
            .map(|(row, &probability)| (threshold.takes(probability), row.trade))
            .collect();
        let holdout = Holdout {
            earnings: Earnings::of(&trades),
            ..fitted.holdout
        };
        if let Some(why) = holdout.refusal(level.minimums) {
            return Ok(Outcome::Kept(Kept::Refused { why, holdout }));
        }
        self.publish(level, at, fitted, None, holdout)?;
        Ok(Outcome::Published {
            valid_from: at,
            holdout,
        })
    }

    /// Publishes in `level`'s store the model `fitted`, valid from `at`, with
    /// `holdout` and, a side's, its side and payoff.
    fn publish(
        &self,
        level: &Level,
        at: Timestamp,
        fitted: Fitted,
        traded: Option<(Side, Payoff)>,
        holdout: Holdout,
    ) -> anyhow::Result<()> {
        let probes = spread(fitted.held_out.len())
            .map(|i| Probe {
                row: fitted.held_out[i].features.clone(),
                prediction: fitted.probabilities[i],
            })
            .collect();
        level.store.publish(&StoredModel {
            manifest: ModelManifest {
                strategy_id: self.strategy_id,
                valid_from: at,
                fingerprint: self.dataset.fingerprint().to_string(),
                feature_names: level.feature_names.clone(),
                target: self.target.clone(),
                side: traded.map(|(side, _)| side),
                learner: level.learner.clone(),
                calibration: fitted.calibration,
                baseline: fitted.baseline,
                payoff: traded.map(|(_, payoff)| payoff),
                holdout,
                probes,
            },
            model: fitted.saved,
        })
    }
}

impl Level {
    /// The level of `meta`: it learns whether the target on the side picked
    /// wins.
    fn of_meta(meta: &MetaLevel) -> Self {
        let config = meta.config();
        Self {
            feature_names: meta.feature_names().to_vec(),
            holdout: config.holdout,
            minimums: config.minimums(),
            learner: config.learner.clone(),
            store: meta.store().clone(),
        }
    }

    /// Its model fitted as of `at` on `examples`, in time order, and calibrated,
    /// judged on its holdout by its probabilities alone (`split`).
    fn fit(&self, at: Timestamp, examples: Vec<Example>) -> anyhow::Result<(LoadedModel, Fitted)> {
        let holdout = at - self.holdout;
        let (fitted, calibrating, held_out) = split(examples, holdout - self.holdout, holdout);
        let set = TrainingSet::new(
            self.feature_names.len(),
            fitted.iter().flat_map(|row| row.features.clone()).collect(),
            fitted.iter().map(|row| row.label).collect(),
        )
        .context("fitting on the window before the calibration")?;
        let model = self.learner.fit(&set)?;
        let calibration = Calibration::fit(
            &calibrating
                .iter()
                .map(|row| Ok((model.predict(&row.features)?, row.label)))
                .collect::<anyhow::Result<Vec<_>>>()?,
        )
        .context("calibrating on the window before the holdout")?;
        let trained: Vec<Example> = fitted.into_iter().chain(calibrating).collect();
        let baseline = baseline(self.feature_names.len(), &trained)?;
        let saved = model.save()?;
        let model = LoadedModel::new(model, calibration, baseline);
        let probabilities = held_out
            .iter()
            .map(|row| model.predict(&row.features))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let predictions: Vec<(f64, bool)> = probabilities
            .iter()
            .zip(&held_out)
            .map(|(&probability, row)| (probability, row.label))
            .collect();
        let by_baseline: Vec<(f64, bool)> =
            held_out.iter().map(|row| (baseline, row.label)).collect();
        let holdout = Holdout {
            earnings: Earnings::default(),
            log_loss: score::log_loss(&predictions)
                .context("no row in the holdout to judge the model on")?,
            baseline_log_loss: score::log_loss(&by_baseline)
                .context("no row in the holdout to judge the model on")?,
            auc: score::auc(&predictions),
        };
        Ok((
            model,
            Fitted {
                saved,
                calibration,
                baseline,
                trained,
                held_out,
                probabilities,
                holdout,
            },
        ))
    }
}

impl Trained {
    /// Whether the train succeeded: it kept no model, each published or
    /// its current one in use.
    #[must_use]
    pub fn succeeded(&self) -> bool {
        self.iter()
            .all(|outcome| !matches!(outcome, Outcome::Kept(_)))
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
fn split(
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

/// The baseline of a model of `width` features trained on `trained`, the
/// rows it is fitted and calibrated on: the training mean of their
/// labels, which the baseline learner fitted on them predicts whatever
/// the row.
///
/// # Errors
///
/// Returns an error if there is no row, or the rows are not `width` wide.
fn baseline(width: usize, trained: &[Example]) -> anyhow::Result<f64> {
    let set = TrainingSet::new(
        width,
        trained
            .iter()
            .flat_map(|row| row.features.clone())
            .collect(),
        trained.iter().map(|row| row.label).collect(),
    )?;
    let baseline = LearnerConfig::Baseline {}.fit(&set)?;
    baseline.predict(&trained[0].features)
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
            Self::Published {
                valid_from,
                holdout,
            } => {
                write!(
                    f,
                    "published a model valid from {valid_from}, holdout {holdout}"
                )
            }
            Self::InUse {
                valid_from,
                retrain,
            } => write!(f, "{}", retrain.in_use(*valid_from)),
            Self::Kept(kept) => write!(f, "kept the current model: {kept}"),
        }
    }
}

impl fmt::Display for Kept {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused { why, holdout } => write!(f, "{why} (holdout {holdout})"),
            Self::Failed(why) => write!(f, "failed: {why}"),
            Self::SchemaMismatch(why) => write!(f, "schema mismatch: {why}"),
            Self::Waiting => write!(
                f,
                "waiting for the sides' decisions: no side's model is valid before the train"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use jiff::SignedDuration;
    use nautilus_model::{
        identifiers::InstrumentId,
        types::{Money, Quantity},
    };
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        backtest::Commission,
        model::{
            config::{Margins, MetaConfig},
            decide::{DecisionRule, FeatureRow},
            features::side_names,
            holdout::Trades,
            learner::LearnerConfig,
            targets::{
                BracketSpec, Distance, Exit, Outcome as TargetOutcome, Outcomes, PerSide, Target,
                TargetRow,
            },
        },
        strategy::{Declared, EmaArgs, IndicatorsConfig},
    };

    const AUD: &str = "AUD/USD.SIM";
    const STRATEGY: &str = "TRAIN-001";

    /// The last mid of AUD/USD, Nautilus' exponential average of one,
    /// `[indicators.mid_0]`.
    const MID: Declared<EmaArgs> = Declared::new("mid_0");

    /// The table of `MID`, as `[indicators]` holds it.
    fn tables() -> IndicatorsConfig {
        toml::from_str(r#"mid_0 = { period = 1, price_type = "MID" }"#).unwrap()
    }

    /// The one feature, `mid_0`.
    fn features() -> Rc<FeatureSet> {
        FeatureSet::builder()
            .quotes(InstrumentId::from(AUD), MID, &tables())
            .build()
            .unwrap()
    }

    /// The fake learner on hourly decisions of one AUD/USD, learning a
    /// bracket closed after 2 hours on both sides, trading each from an
    /// expected 0.5 USD, on a window of 24 hours, the last 6 held out and the 6 before them calibrated on,
    /// published from 1 USD per trade over a trade.
    fn config() -> ModelConfig {
        ModelConfig::builder()
            .decision(DecisionRule::Every(SignedDuration::from_hours(1)))
            .target(
                TargetConfig::builder()
                    .instrument_id(InstrumentId::from(AUD))
                    .quantity(Quantity::from(1))
                    .spec(
                        Target::builder()
                            .bracket(BracketSpec {
                                take_profit: Distance::BasisPoints(Decimal::new(75, 1)),
                                stop: Distance::BasisPoints(Decimal::from(5)),
                                atr: None,
                            })
                            .limit(SignedDuration::from_hours(2))
                            .build(),
                    )
                    .build(),
            )
            .retrain(SignedDuration::from_hours(24))
            .window(SignedDuration::from_hours(24))
            .holdout(SignedDuration::from_hours(6))
            .margins(Margins {
                long: Some(0.5),
                short: Some(0.5),
            })
            .min_pnl(1.0)
            .min_trades(1)
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

    /// The venue of the dataset runs: a commission rate of 30 per million,
    /// 10 million USD, no leverage.
    fn venue() -> DatasetVenue {
        DatasetVenue {
            commission: Commission::Rate(Decimal::new(3, 5)),
            balance: Money::from("10000000 USD"),
            leverage: Decimal::ONE,
        }
    }

    /// The training in `dir`, its catalog and its store side by side: the
    /// catalog holds no instrument, none being read.
    fn training(dir: &Path, config: ModelConfig) -> Training {
        let catalog = dir.join("catalog");
        std::fs::create_dir_all(&catalog).unwrap();
        Training::new(
            &catalog,
            &dir.join("models"),
            StrategyId::from(STRATEGY),
            &features(),
            config,
            &requirements(),
            &venue(),
        )
        .unwrap()
    }

    /// The models of a training in `dir` as `config` says, as `training`
    /// keys them.
    fn models(dir: &Path, config: &ModelConfig) -> ModelSet {
        ModelSet::new(
            &dir.join("models"),
            StrategyId::from(STRATEGY),
            &features(),
            config,
            &requirements(),
            &venue(),
        )
        .unwrap()
    }

    /// The store of the `side` models of a training in `dir` as `config`
    /// says.
    fn store(dir: &Path, config: &ModelConfig, side: Side) -> ModelStore {
        models(dir, config).side(side).unwrap().store().clone()
    }

    /// The store of the meta models of a training in `dir` as `config`
    /// says.
    fn meta_store(dir: &Path, config: &ModelConfig) -> ModelStore {
        models(dir, config).meta().unwrap().store().clone()
    }

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    /// The row of the decision at `time` on `features`, settled `after` it:
    /// each side's stop worth 2 USD, a side winning 3 USD at its take profit,
    /// losing 2 at its stop, net of 0.2 USD of commission; each side's payoff
    /// features `bracket_payoff`'s.
    fn row_of(
        time: &str,
        features: Vec<f64>,
        (long_wins, short_wins): (bool, bool),
        after: SignedDuration,
    ) -> TargetRow {
        let ns = UnixNanos::from(at(time));
        let outcome = |wins| {
            let (pnl, exit) = if wins {
                ("3 USD", Exit::TakeProfit)
            } else {
                ("-2 USD", Exit::Stop)
            };
            TargetOutcome {
                pnl: Money::from(pnl),
                commission: Money::from("0.2 USD"),
                exit,
                unit: 2.0,
            }
        };
        TargetRow {
            decision: FeatureRow {
                strategy_id: StrategyId::from(STRATEGY),
                features,
                ts_event: ns,
                ts_init: ns,
            },
            payoff: bracket_payoff(),
            outcomes: Outcomes {
                long: outcome(long_wins),
                short: outcome(short_wins),
            },
            settled_at: UnixNanos::from(at(time) + after),
        }
    }

    /// The payoff features of each side of a bracket, its take profit's R,
    /// its stop's nominal loss and its entry cost, which no model of the
    /// tests reads: the fake reads its first feature, the strategy's.
    fn bracket_payoff() -> PerSide<Vec<f64>> {
        PerSide {
            long: vec![1.4, 1.1, 0.07],
            short: vec![1.3, 1.2, 0.08],
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
    /// feature, a loss at 0.25 and a win at 0.75, the loss's stop worth 4 USD:
    /// Platt's targets 1/3 and 2/3, the odds of 1/3 and 3 becoming 1/2 and 2,
    /// so odds `o` become `o^s`, `s` being ln 2 / ln 3. Held out, a win at 0.8
    /// and a loss at 0.3, of odds 4 and 3/7, calibrated 1 / (1 + 4^-s) and
    /// 1 / (1 + (7/3)^s) (AUC 1). Around them, wins on the feature 0.1
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
        let outcomes = &mut rows[4].outcomes;
        for outcome in [&mut outcomes.long, &mut outcomes.short] {
            outcome.unit = 4.0;
        }
        rows
    }

    fn close(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < 1e-4, "{actual} != {expected}");
    }

    /// Worked by hand on `rows`: each side fitted on the rows before the
    /// calibration settled at its start, calibrated on those before the
    /// holdout settled at its start, judged on the holdout. The long's
    /// realised R, its P&L over its stop's worth, 2 or 4 USD:
    /// wins of 1.5R, losses of 1R and 0.5R; the short's wins of 1.5R and
    /// 0.75R, losses of 1R.
    #[test]
    fn a_train_fits_calibrates_and_judges_each_side_on_its_part_of_the_window() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let t = at("2026-01-07T00:00:00Z");

        let trained = training.train_at(t, |_, _| Ok(rows()));

        for outcome in [&trained.long, &trained.short] {
            assert!(
                matches!(outcome, Some(Outcome::Published { valid_from, .. }) if *valid_from == t),
                "{trained:?}"
            );
        }
        let long = store(dir.path(), &config(), Side::Long).read(t).unwrap();
        let fake = LearnerConfig::Fake {}.load(&long.model).unwrap();
        // The fake predicts the mean of its training labels where the first
        // feature is missing, on the long's row: the decision's feature, then
        // the long's payoff features.
        assert_eq!(fake.predict(&[f64::NAN, 1.4, 1.1, 0.07]).unwrap(), 0.5);
        let calibration = long.manifest.calibration;
        close(calibration.apply(0.25), 1.0 / 3.0);
        close(calibration.apply(0.75), 2.0 / 3.0);
        let probes: Vec<&Vec<f64>> = long.manifest.probes.iter().map(|p| &p.row).collect();
        assert_eq!(
            probes,
            [&vec![0.8, 1.4, 1.1, 0.07], &vec![0.3, 1.4, 1.1, 0.07]]
        );
        let s = 2_f64.ln() / 3_f64.ln();
        close(
            long.manifest.probes[0].prediction,
            1.0 / (1.0 + 4_f64.powf(-s)),
        );
        close(
            long.manifest.probes[1].prediction,
            1.0 / (1.0 + (7.0_f64 / 3.0).powf(s)),
        );
        let payoff = long.manifest.payoff.unwrap();
        close(payoff.win, 1.5);
        close(payoff.loss, 0.75);
        assert_eq!(long.manifest.side, Some(Side::Long));
        let short = store(dir.path(), &config(), Side::Short).read(t).unwrap();
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

    /// The holdout of `outcome`, published or refused.
    fn holdout_of(outcome: Option<Outcome>) -> Holdout {
        match outcome {
            Some(
                Outcome::Published { holdout, .. } | Outcome::Kept(Kept::Refused { holdout, .. }),
            ) => holdout,
            outcome => panic!("{outcome:?}"),
        }
    }

    fn close_to(actual: Option<f64>, expected: f64) {
        close(actual.unwrap(), expected);
    }

    /// Worked by hand on `rows`'s holdout, each side's trades 2 USD of
    /// stop, its decision on the expected P&L of its payoff from 0.5 USD.
    /// With `s` = ln 2 / ln 3, the long at 19:00 is calibrated c = 1 / (1 +
    /// 4^-s), expected (1.5c - 0.75(1 - c)) 2 = 1.68 USD, and wins 3 USD:
    /// picked; at 20:00 c = 1 / (1 + (7/3)^s), expected 0.16 USD: not. The
    /// short, whose odds `o` become `o^-s`, expects at 19:00 a loss; at 20:00,
    /// c = 1 / (1 + (3/7)^s), (1.125c - (1 - c)) 2 = 0.68 USD, and wins 3 USD:
    /// picked. Two trades of 3 USD; every trade the four of the two sides on
    /// the two rows, two wins of 3 and two losses of 2 (0.5 per trade); at 1.5
    /// times the commission of 0.2 each makes 0.1 less. Both sides hold those
    /// earnings. The log loss of each is (ln (1 + 4^-s) + ln (1 + (3/7)^s)) /
    /// 2, the baseline's, the mean of two wins in four, ln 2; the AUC 1.
    #[test]
    fn the_sides_are_judged_on_the_trades_picked_on_the_holdout() {
        let dir = tempfile::tempdir().unwrap();
        let trained =
            training(dir.path(), config()).train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows()));

        let s = 2_f64.ln() / 3_f64.ln();
        let log_loss = (1.0 + 4_f64.powf(-s))
            .ln()
            .midpoint((1.0 + (3.0_f64 / 7.0).powf(s)).ln());
        for holdout in [holdout_of(trained.long), holdout_of(trained.short)] {
            let Earnings {
                taken,
                every,
                taken_stressed,
                every_stressed,
            } = holdout.earnings;
            assert_eq!((taken.count, every.count), (2, 4), "{holdout:?}");
            close_to(taken.expectancy, 3.0);
            close_to(taken.win_rate, 1.0);
            close_to(every.expectancy, 0.5);
            close_to(every.win_rate, 0.5);
            close_to(taken_stressed.expectancy, 2.9);
            close_to(every_stressed.expectancy, 0.4);
            close(holdout.log_loss, log_loss);
            close(holdout.baseline_log_loss, 2_f64.ln());
            assert_eq!(holdout.auc, Some(1.0));
        }
    }

    /// Worked by hand on `rows`'s holdout, as
    /// `the_sides_are_judged_on_the_trades_picked_on_the_holdout` works it,
    /// from an expected 0.1 USD: at 20:00 the long expects 0.16 USD and the
    /// short 0.68, both clearing, and only the short is picked, winning 3
    /// USD; the long's loss of 2 is taken by no decision. With the long at
    /// 19:00, two trades of 3 USD; every trade the four of the two rows, 0.5
    /// per trade.
    #[test]
    fn a_row_where_both_sides_clear_counts_one_trade() {
        let dir = tempfile::tempdir().unwrap();
        let from_a_tenth = ModelConfig {
            margins: Margins {
                long: Some(0.1),
                short: Some(0.1),
            },
            ..config()
        };

        let trained = training(dir.path(), from_a_tenth)
            .train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows()));

        for outcome in [&trained.long, &trained.short] {
            let Some(Outcome::Published { holdout, .. }) = outcome else {
                panic!("{trained:?}");
            };
            let Earnings { taken, every, .. } = holdout.earnings;
            assert_eq!((taken.count, every.count), (2, 4), "{holdout:?}");
            close_to(taken.expectancy, 3.0);
            close_to(every.expectancy, 0.5);
        }
    }

    /// `rows` but for the holdout: at 19:00 at 0.8 both sides win, at 20:00 at
    /// 0.3 both lose. The short, calibrated decreasing, with `s` = ln 2 / ln 3,
    /// gives its win 1 / (1 + 4^s) and its loss 1 / (1 + (3/7)^s), a log loss
    /// of (ln (1 + 4^s) + ln (1 + (7/3)^s)) / 2, above the baseline's ln 2.
    fn short_showing_no_signal() -> Vec<TargetRow> {
        let mut rows = rows();
        rows[7] = row_of("2026-01-06T19:00:00Z", vec![0.8], (true, true), hour());
        rows[8] = row_of("2026-01-06T20:00:00Z", vec![0.3], (false, false), hour());
        rows
    }

    /// The short's candidate fails its log loss (`short_showing_no_signal`),
    /// so what decides beside the long is the short the handle would use at
    /// the train's time: of its models of the 4th, the 5th and the 8th, that of
    /// the 5th, which the retrained plan does not tell, marking the short for
    /// fitting. It expects 2 (2 c(0.8) - 1), 1.1 USD, on every row: below the
    /// long's 1.68 at 19:00, a win of 3, and picked at 20:00, where the long
    /// expects 0.16, a loss of 2. So 0.5 USD per trade, below the minimum of
    /// 1, where the long alone would have earned 3: refused, and nothing
    /// published. Every trade is the four of the two sides.
    #[test]
    fn a_surviving_side_is_judged_beside_the_other_sides_older_model() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let t = at("2026-01-07T00:00:00Z");
        let short = store(dir.path(), &config(), Side::Short);
        let models = [
            ("2026-01-04T00:00:00Z", 0.5),
            ("2026-01-05T00:00:00Z", 0.8),
            ("2026-01-08T00:00:00Z", 0.5),
        ];
        for (valid_from, probability) in models {
            publish_baseline(&short, Side::Short, valid_from, probability);
        }

        let trained = training.train_at(t, |_, _| Ok(short_showing_no_signal()));

        let Some(Outcome::Kept(Kept::Refused { why, holdout })) = trained.long else {
            panic!("{trained:?}");
        };
        let Refusal::MinPnl { pnl, min_pnl } = why else {
            panic!("{why:?}");
        };
        close(pnl, 0.5);
        assert_eq!(min_pnl, 1.0);
        assert_eq!(holdout.earnings.taken.count, 2, "{holdout:?}");
        assert_eq!(holdout.earnings.every.count, 4, "{holdout:?}");
        assert!(
            store(dir.path(), &config(), Side::Long)
                .valid_froms()
                .unwrap()
                .is_empty()
        );
        assert_eq!(short.valid_froms().unwrap().len(), models.len());
    }

    /// `rows` with the short's pick at 20:00 a win of 0.1 USD: alone the short
    /// would earn less than the minimum of 1 and the long 3, together 1.55
    /// USD per trade, above every trade's: the long's 3 and -2, the short's -2
    /// and 0.1, a win rate of 1/2 of a mean win of 1.55 and a loss of 2, -0.225.
    #[test]
    fn the_sides_publish_together_on_what_their_picks_earned() {
        let dir = tempfile::tempdir().unwrap();
        let t = at("2026-01-07T00:00:00Z");
        let mut rows = rows();
        rows[8].outcomes.short.pnl = Money::from("0.1 USD");

        let trained = training(dir.path(), config()).train_at(t, |_, _| Ok(rows));

        for outcome in [&trained.long, &trained.short] {
            assert!(
                matches!(outcome, Some(Outcome::Published { valid_from, .. }) if *valid_from == t),
                "{trained:?}"
            );
        }
        for side in Side::BOTH {
            let Earnings { taken, every, .. } = store(dir.path(), &config(), side)
                .read(t)
                .unwrap()
                .manifest
                .holdout
                .earnings;
            assert_eq!(taken.count, 2, "{side:?}");
            close_to(taken.expectancy, 1.55);
            close_to(every.expectancy, -0.225);
        }
    }

    /// The long's own trade earns: a win of 3 at 19:00, the row it is picked
    /// on. The short's pick at 20:00 is a loss of 2, its own log loss still
    /// below the baseline's: (ln (1 + 4^-s) + ln (1 + (3/7)^-s)) / 2, about
    /// 0.67, against ln 2. The picks earn 0.5 USD per trade, below the minimum
    /// of 1, and the profitable side is refused with the losing one.
    #[test]
    fn a_profitable_side_paired_with_a_losing_one_is_refused_with_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut rows = rows();
        rows[8] = row_of("2026-01-06T20:00:00Z", vec![0.3], (false, false), hour());

        let trained =
            training(dir.path(), config()).train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows));

        for (side, outcome) in [(Side::Long, &trained.long), (Side::Short, &trained.short)] {
            let Some(Outcome::Kept(Kept::Refused { why, .. })) = outcome else {
                panic!("{trained:?}");
            };
            let Refusal::MinPnl { pnl, min_pnl } = why else {
                panic!("{why:?}");
            };
            close(*pnl, 0.5);
            assert_eq!(*min_pnl, 1.0);
            assert!(
                store(dir.path(), &config(), side)
                    .valid_froms()
                    .unwrap()
                    .is_empty(),
                "{side:?}"
            );
        }
    }

    /// `rows` with a row at 21:00 at 0.7, both sides winning: the long, as
    /// `rows`'s holdout, wins at 0.8 and loses at 0.3, and wins at 0.7; the
    /// short loses at 0.8, wins at 0.3 and wins at 0.7, against its curve. With
    /// `s` = ln 2 / ln 3, the long's log loss is (ln (1 + 4^-s) + 2 ln (1 +
    /// (3/7)^s)) / 3, the short's (ln (1 + 4^-s) + ln (1 + (3/7)^s) + ln (1 +
    /// (7/3)^s)) / 3. At 21:00 the long expects 1.34 USD, the short less than
    /// nought: three trades picked, 3 USD each; every trade, the six of the
    /// rows, four wins and two losses, 4/3.
    #[test]
    fn both_manifests_hold_the_earnings_judged_beside_each_sides_own_log_loss() {
        let dir = tempfile::tempdir().unwrap();
        let t = at("2026-01-07T00:00:00Z");
        let mut rows = rows();
        rows.insert(
            9,
            row_of("2026-01-06T21:00:00Z", vec![0.7], (true, true), hour()),
        );

        training(dir.path(), config()).train_at(t, |_, _| Ok(rows));

        let s = 2_f64.ln() / 3_f64.ln();
        let ln_1_plus = |x: f64| (1.0 + x).ln();
        let (at_the_far_end, near_it, against_the_curve) = (
            ln_1_plus(4_f64.powf(-s)),
            ln_1_plus((3.0_f64 / 7.0).powf(s)),
            ln_1_plus((7.0_f64 / 3.0).powf(s)),
        );
        for (side, log_loss) in [
            (Side::Long, (at_the_far_end + 2.0 * near_it) / 3.0),
            (
                Side::Short,
                (at_the_far_end + near_it + against_the_curve) / 3.0,
            ),
        ] {
            let holdout = store(dir.path(), &config(), side)
                .read(t)
                .unwrap()
                .manifest
                .holdout;
            let Earnings { taken, every, .. } = holdout.earnings;
            assert_eq!((taken.count, every.count), (3, 6), "{side:?}");
            close_to(taken.expectancy, 3.0);
            close_to(every.expectancy, 4.0 / 3.0);
            close(holdout.log_loss, log_loss);
            close(holdout.baseline_log_loss, 2_f64.ln());
            assert_eq!(holdout.auc, Some(1.0), "{side:?}");
        }
    }

    /// `rows` with the holdout's labels reversed: each side wins where it
    /// predicted a loss and loses where it predicted a win, and fails its log
    /// loss. No side survives, so no earnings are judged, an older model of
    /// the short notwithstanding; each reports its own log loss, (ln (1 + 4^s)
    /// + ln (1 + (7/3)^s)) / 2.
    #[test]
    fn with_no_side_surviving_each_reports_its_own_log_loss_and_no_earnings() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        publish_baseline(
            &store(dir.path(), &config(), Side::Short),
            Side::Short,
            "2026-01-05T00:00:00Z",
            0.8,
        );
        let mut rows = rows();
        rows[7] = row("2026-01-06T19:00:00Z", vec![0.8], false, hour());
        rows[8] = row("2026-01-06T20:00:00Z", vec![0.3], true, hour());

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows));

        let s = 2_f64.ln() / 3_f64.ln();
        let log_loss = (1.0 + 4_f64.powf(s))
            .ln()
            .midpoint((1.0 + (7.0_f64 / 3.0).powf(s)).ln());
        for outcome in [&trained.long, &trained.short] {
            let Some(Outcome::Kept(Kept::Refused { why, holdout })) = outcome else {
                panic!("{trained:?}");
            };
            assert!(matches!(why, Refusal::LogLoss { .. }), "{why:?}");
            close(holdout.log_loss, log_loss);
            assert_eq!(holdout.earnings, Earnings::default(), "{holdout:?}");
        }
    }

    /// The long published at the train's time by a train interrupted before
    /// the short's publish, expecting 1.1 USD on every row: it is kept in use,
    /// and decides beside the short fitted now, which it outbids at 20:00,
    /// the long's loss of 2 picked rather than the short's win of 3. So 0.5 USD
    /// per trade, below the minimum of 1: the short is refused, judged beside
    /// the model the handle would use.
    #[test]
    fn a_side_fitted_is_judged_beside_the_other_sides_model_in_use() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let t = at("2026-01-07T00:00:00Z");
        publish_baseline(
            &store(dir.path(), &config(), Side::Long),
            Side::Long,
            "2026-01-07T00:00:00Z",
            0.8,
        );

        let trained = training.train_at(t, |_, _| Ok(rows()));

        assert!(
            matches!(trained.long, Some(Outcome::InUse { valid_from, .. }) if valid_from == t),
            "{trained:?}"
        );
        let Some(Outcome::Kept(Kept::Refused { why, .. })) = trained.short else {
            panic!("{trained:?}");
        };
        let Refusal::MinPnl { pnl, .. } = why else {
            panic!("{why:?}");
        };
        close(pnl, 0.5);
    }

    /// The short never wins on the rows before the holdout, so it has no
    /// payoff to expect and its fit fails. The long survives, and with no
    /// short model to decide beside it is judged alone: its win at 19:00
    /// picked, beside every trade of the side traded, its two.
    #[test]
    fn a_side_whose_fit_fails_leaves_the_other_judged_without_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut rows = rows();
        for row in &mut rows[..7] {
            row.outcomes.short.pnl = Money::from("-2 USD");
        }

        let trained =
            training(dir.path(), config()).train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows));

        assert!(
            matches!(trained.short, Some(Outcome::Kept(Kept::Failed(_)))),
            "{trained:?}"
        );
        let holdout = holdout_of(trained.long);
        assert_eq!(holdout.earnings.taken.count, 1, "{holdout:?}");
        assert_eq!(holdout.earnings.every.count, 2, "{holdout:?}");
    }

    /// The long's publish fails, and the short, judged with it, publishes
    /// nothing.
    #[test]
    fn a_publish_failing_publishes_no_other_side() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        store(dir.path(), &config(), Side::Long).break_publishing();

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows()));

        for outcome in [&trained.long, &trained.short] {
            assert!(
                matches!(outcome, Some(Outcome::Kept(Kept::Failed(_)))),
                "{trained:?}"
            );
        }
        assert!(
            store(dir.path(), &config(), Side::Short)
                .valid_froms()
                .unwrap()
                .is_empty()
        );
    }

    /// `rows` but for the holdout, the long's trades at 18:00 to 22:00 on
    /// `holdout`, each a feature and the long's net P&L.
    fn with_holdout(holdout: &[(f64, &str)]) -> Vec<TargetRow> {
        let mut rows: Vec<TargetRow> = rows().into_iter().take(7).collect();
        for (i, &(feature, pnl)) in holdout.iter().enumerate() {
            let time = format!("2026-01-06T{:02}:00:00Z", 18 + i);
            let mut row = row(&time, vec![feature], !pnl.starts_with('-'), hour());
            row.outcomes.long.pnl = Money::from(pnl);
            rows.push(row);
        }
        rows
    }

    /// The long alone, fitted and calibrated as by `rows` (the curve
    /// 1 / (1 + 4^(1 - 2p))), its payoff a win of 1.5R and a loss of 0.75R on stops
    /// worth 2 USD: it takes a trade expected at least 0.5 USD, from a
    /// calibrated 4/9, a feature of about 0.42. The first holdout ranks
    /// every win above every loss (AUC 1), but takes a win of 0.5 USD at
    /// 0.9 and a loss of 2 at 0.8: -0.75 USD per trade, refused. The second
    /// ranks a win at 0.3 below a loss at 0.8 (AUC 4/6), but takes wins of
    /// 3 at 0.9 and 0.7 and a loss of 2 at 0.8, 4/3 USD per trade, above
    /// the minimum of 1 and every trade's 0.5: published. Both log losses,
    /// about 0.57 and 0.66, are below the baseline's ln 2.
    #[test]
    fn a_holdout_ranked_well_that_loses_is_refused_and_one_ranked_worse_that_earns_is_published() {
        let long_only = || ModelConfig {
            margins: Margins {
                long: Some(0.5),
                short: None,
            },
            ..config()
        };
        let t = at("2026-01-07T00:00:00Z");
        let ranked_well = with_holdout(&[
            (0.9, "0.5 USD"),
            (0.8, "-2 USD"),
            (0.3, "-2 USD"),
            (0.2, "-2 USD"),
        ]);
        let earning = with_holdout(&[
            (0.9, "3 USD"),
            (0.8, "-2 USD"),
            (0.7, "3 USD"),
            (0.3, "0.5 USD"),
            (0.2, "-2 USD"),
        ]);
        let (refused, published) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());

        let losing = training(refused.path(), long_only()).train_at(t, |_, _| Ok(ranked_well));
        let earned = training(published.path(), long_only()).train_at(t, |_, _| Ok(earning));

        let Some(Outcome::Kept(Kept::Refused { why, holdout })) = losing.long else {
            panic!("{losing:?}");
        };
        let Refusal::MinPnl { pnl, min_pnl } = why else {
            panic!("{why:?}");
        };
        close(pnl, -0.75);
        assert_eq!(min_pnl, 1.0);
        assert_eq!(holdout.auc, Some(1.0));
        assert!(holdout.log_loss < holdout.baseline_log_loss, "{holdout:?}");
        let Some(Outcome::Published {
            valid_from,
            holdout,
        }) = earned.long
        else {
            panic!("{earned:?}");
        };
        assert_eq!(valid_from, t);
        close_to(holdout.auc, 4.0 / 6.0);
        assert_eq!(holdout.earnings.taken.count, 3);
        close_to(holdout.earnings.taken.expectancy, 4.0 / 3.0);
        close_to(holdout.earnings.every.expectancy, 0.5);
        assert!(
            store(refused.path(), &long_only(), Side::Long)
                .valid_froms()
                .unwrap()
                .is_empty()
        );
    }

    /// A one-sided strategy has no pick between sides, and the test it has
    /// always had: taking every trade of the holdout earns no more than every
    /// trade, so a margin no trade falls short of is refused.
    #[test]
    fn a_one_sided_strategy_taking_every_trade_of_the_holdout_is_refused() {
        let takes_all = ModelConfig {
            margins: Margins {
                long: Some(-10.0),
                short: None,
            },
            min_pnl: -10.0,
            ..config()
        };
        let dir = tempfile::tempdir().unwrap();

        let trained =
            training(dir.path(), takes_all).train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows()));

        let Some(Outcome::Kept(Kept::Refused { why, .. })) = trained.long else {
            panic!("{trained:?}");
        };
        let Refusal::Every { pnl, every } = why else {
            panic!("{why:?}");
        };
        close(pnl, 0.5);
        close(every, 0.5);
    }

    /// Two trades picked on the holdout, one of each side: fewer than three,
    /// though each side's own count is never compared.
    #[test]
    fn fewer_trades_picked_than_the_minimum_refuse_both_sides() {
        let three = ModelConfig {
            min_trades: 3,
            ..config()
        };
        let dir = tempfile::tempdir().unwrap();

        let trained =
            training(dir.path(), three).train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows()));

        for outcome in [&trained.long, &trained.short] {
            assert!(
                matches!(
                    outcome,
                    Some(Outcome::Kept(Kept::Refused {
                        why: Refusal::Trades {
                            trades: 2,
                            min_trades: 3
                        },
                        ..
                    }))
                ),
                "{trained:?}"
            );
        }
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

    /// The long's model valid from the train's time, a baseline published
    /// beforehand, expecting nought and so never picked, is kept in use, not
    /// fitted again; the short, with none, is fitted and published.
    #[test]
    fn a_model_already_published_at_the_trains_time_is_kept_in_use_and_the_other_fitted() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let t = at("2026-01-07T00:00:00Z");
        publish_baseline(
            &store(dir.path(), &config(), Side::Long),
            Side::Long,
            "2026-01-07T00:00:00Z",
            0.5,
        );

        let trained = training.train_at(t, |_, _| Ok(rows()));

        assert_eq!(
            trained.long,
            Some(Outcome::InUse {
                valid_from: t,
                retrain: config().retrain
            })
        );
        assert!(
            matches!(trained.short, Some(Outcome::Published { valid_from, .. }) if valid_from == t),
            "{trained:?}"
        );
        let long = store(dir.path(), &config(), Side::Long).read(t).unwrap();
        assert_eq!(long.manifest.learner, LearnerConfig::Baseline {});
    }

    /// Both sides have a model valid from the train's time: a rerun of the
    /// train fits nothing, and so replays nothing.
    #[test]
    fn a_train_with_every_model_in_use_replays_nothing_and_reports_each() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let t = at("2026-01-07T00:00:00Z");
        for side in Side::BOTH {
            publish_baseline(
                &store(dir.path(), &config(), side),
                side,
                "2026-01-07T00:00:00Z",
                0.5,
            );
        }
        let mut replays = vec![];

        let trained = training.train_at(t, |from, to| {
            replays.push((from, to));
            Ok(rows())
        });

        assert_eq!(replays, []);
        let in_use = Outcome::InUse {
            valid_from: t,
            retrain: config().retrain,
        };
        assert_eq!(
            trained,
            Trained {
                long: Some(in_use.clone()),
                short: Some(in_use),
                meta: None,
            }
        );
    }

    /// `config`, never retrained.
    fn never() -> ModelConfig {
        ModelConfig {
            retrain: Retrain::Never,
            ..config()
        }
    }

    /// Never retrained, the long's model of the 6th, expecting nought and so
    /// never picked, is current at the train of the 7th: kept in use, not
    /// fitted again; the short, with none, is fitted and published.
    #[test]
    fn never_retrained_a_model_valid_before_the_train_is_kept_in_use_and_the_other_fitted() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), never());
        let t = at("2026-01-07T00:00:00Z");
        let sixth = at("2026-01-06T00:00:00Z");
        publish_baseline_of(
            never(),
            &store(dir.path(), &never(), Side::Long),
            Side::Long,
            "2026-01-06T00:00:00Z",
            0.5,
        );

        let trained = training.train_at(t, |_, _| Ok(rows()));

        assert_eq!(
            trained.long,
            Some(Outcome::InUse {
                valid_from: sixth,
                retrain: Retrain::Never
            })
        );
        assert!(
            matches!(trained.short, Some(Outcome::Published { valid_from, .. }) if valid_from == t),
            "{trained:?}"
        );
        assert_eq!(
            store(dir.path(), &never(), Side::Long)
                .valid_froms()
                .unwrap(),
            [sixth]
        );
    }

    #[test]
    fn a_published_model_carries_its_schema_target_and_learner() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let t = at("2026-01-07T00:00:00Z");

        training.train_at(t, |_, _| Ok(rows()));

        let manifest = store(dir.path(), &config(), Side::Long)
            .read(t)
            .unwrap()
            .manifest;
        assert_eq!(manifest.strategy_id, StrategyId::from(STRATEGY));
        assert_eq!(manifest.valid_from, t);
        // The strategy's feature, then the bracket's payoff features.
        assert_eq!(
            manifest.feature_names,
            ["mid_0", "take_profit_r", "stop_loss_r", "entry_cost"]
        );
        assert_eq!(
            manifest.fingerprint,
            crate::model::dataset::fingerprint(
                &features(),
                config().decision,
                &config().target,
                &requirements(),
                &venue(),
            )
            .unwrap()
            .to_string()
        );
        assert_eq!(manifest.target, config().target);
        assert_eq!(manifest.learner, LearnerConfig::Fake {});
    }

    /// `rows` with the long's loss at 10:00 a win and its win at 17:00 a
    /// loss: the long is fitted on its wins at 08:00 and 10:00 and
    /// calibrated on its loss at 13:00 and its win at 15:00, three wins in
    /// four, 3/4; not on the rows outside those (the win at 23:00 of the
    /// 5th, before the window, the win at 11:30 and the loss at 17:00,
    /// settled after their part's end), nor on the holdout's win and loss,
    /// with which the window's rows are five wins in eight, 5/8. Its
    /// holdout as `rows`'s takes the win at 19:00, its log loss, about
    /// 0.41, below the baseline's, (ln 4 + ln 4/3) / 2, about 0.84: it
    /// publishes.
    #[test]
    fn a_published_sides_baseline_is_the_training_mean_of_the_rows_it_was_fitted_and_calibrated_on()
    {
        let dir = tempfile::tempdir().unwrap();
        let t = at("2026-01-07T00:00:00Z");
        let mut rows = rows();
        rows[2] = row("2026-01-06T10:00:00Z", vec![0.9], true, hour());
        rows[6] = row(
            "2026-01-06T17:00:00Z",
            vec![0.1],
            false,
            SignedDuration::from_mins(90),
        );

        let trained = training(dir.path(), config()).train_at(t, |_, _| Ok(rows));

        assert!(
            matches!(trained.long, Some(Outcome::Published { .. })),
            "{trained:?}"
        );
        let long = store(dir.path(), &config(), Side::Long).read(t).unwrap();
        assert_eq!(long.manifest.baseline, 0.75);
    }

    /// Margins of the long alone: no short model is trained nor stored.
    #[test]
    fn a_long_only_model_trains_no_short() {
        let dir = tempfile::tempdir().unwrap();
        let long_only = ModelConfig {
            margins: Margins {
                long: Some(0.5),
                short: None,
            },
            ..config()
        };
        let training = training(dir.path(), long_only.clone());

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows()));

        assert!(
            matches!(trained.long, Some(Outcome::Published { .. })),
            "{trained:?}"
        );
        assert_eq!((trained.short, trained.meta), (None, None));
        assert!(models(dir.path(), &long_only).side(Side::Short).is_none());
    }

    /// The short fails its log loss (`short_showing_no_signal`), with `s` = ln
    /// 2 / ln 3, against the baseline's ln 2: it publishes nothing, and reports
    /// its own log loss beside the earnings of the remaining picks, the
    /// long's. With no short model to decide beside it, the long is judged
    /// alone: its win of 3 USD at 19:00 picked, beside every trade of the side
    /// traded, its win of 3 and its loss of 2. It publishes.
    #[test]
    fn a_side_refused_on_its_log_loss_reports_it_and_the_earnings_of_the_remaining_picks() {
        let dir = tempfile::tempdir().unwrap();
        let t = at("2026-01-07T00:00:00Z");
        let training = training(dir.path(), config());

        let trained = training.train_at(t, |_, _| Ok(short_showing_no_signal()));

        let Some(Outcome::Kept(Kept::Refused { why, holdout })) = trained.short else {
            panic!("{trained:?}");
        };
        let Refusal::LogLoss { log_loss, baseline } = why else {
            panic!("{why:?}");
        };
        let s = 2_f64.ln() / 3_f64.ln();
        close(
            log_loss,
            (1.0 + 4_f64.powf(s))
                .ln()
                .midpoint((1.0 + (7.0_f64 / 3.0).powf(s)).ln()),
        );
        close(baseline, 2_f64.ln());
        close(holdout.log_loss, log_loss);
        let Earnings { taken, every, .. } = holdout.earnings;
        assert_eq!((taken.count, every.count), (1, 2), "{holdout:?}");
        close_to(taken.expectancy, 3.0);
        close_to(every.expectancy, 0.5);
        assert!(
            store(dir.path(), &config(), Side::Short)
                .valid_froms()
                .unwrap()
                .is_empty()
        );
        let Some(Outcome::Published {
            valid_from,
            holdout: long,
        }) = trained.long
        else {
            panic!("{trained:?}");
        };
        assert_eq!(valid_from, t);
        assert_eq!(long.earnings, holdout.earnings);
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
            store(dir.path(), &config(), Side::Long)
                .valid_froms()
                .unwrap()
                .is_empty()
        );
    }

    /// The loss at 0.3 removed, the long's holdout, of a long-only strategy,
    /// is a win at 0.8, taken: no AUC, and no more earned than taking every
    /// trade.
    #[test]
    fn a_holdout_of_one_label_has_no_auc_and_is_judged_on_its_trades() {
        let dir = tempfile::tempdir().unwrap();
        let long_only = ModelConfig {
            margins: Margins {
                long: Some(0.5),
                short: None,
            },
            ..config()
        };
        let training = training(dir.path(), long_only);
        let mut rows = rows();
        rows.remove(8);

        let outcome = training
            .train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows))
            .long;

        let Some(Outcome::Kept(Kept::Refused { why, holdout })) = outcome else {
            panic!("{outcome:?}");
        };
        assert_eq!(holdout.auc, None);
        assert_eq!(
            why,
            Refusal::Every {
                pnl: 3.0,
                every: 3.0
            }
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

    /// A holdout taking 3 trades of 1.25 USD each, besides 8 of 0.4 USD; at
    /// 1.5 times the commission 1.125 and 0.3; its log loss 0.61, the
    /// baseline's 0.69, its AUC 0.625.
    fn holdout() -> Holdout {
        let trades = |count, expectancy| Trades {
            count,
            expectancy: Some(expectancy),
            win_rate: Some(0.5),
        };
        Holdout {
            earnings: Earnings {
                taken: trades(3, 1.25),
                every: trades(8, 0.4),
                taken_stressed: trades(3, 1.125),
                every_stressed: trades(8, 0.3),
            },
            log_loss: 0.61,
            baseline_log_loss: 0.69,
            auc: Some(0.625),
        }
    }

    /// What a train reports of a model's holdout, published or not: its
    /// trades, net P&L per trade taken and per trade of every trade, at the
    /// commission and at 1.5 times it, its log loss and the baseline's, and
    /// its AUC.
    #[test]
    fn an_outcome_reads_as_published_or_kept_with_its_reason_and_its_holdout() {
        let published = Outcome::Published {
            valid_from: at("2026-01-07T00:00:00Z"),
            holdout: holdout(),
        };
        let below = Outcome::Kept(Kept::Refused {
            why: Refusal::MinPnl {
                pnl: 1.25,
                min_pnl: 1.5,
            },
            holdout: holdout(),
        });

        assert!(published.to_string().contains("published"), "{published}");
        assert!(below.to_string().contains("kept"), "{below}");
        assert!(
            below.to_string().contains("below the minimum 1.5"),
            "{below}"
        );
        for text in [published.to_string(), below.to_string()] {
            let written = figures(&text);
            for figure in [3.0, 1.25, 8.0, 0.4, 1.125, 0.3, 0.61, 0.69, 0.625] {
                assert!(
                    written.iter().any(|&number| (number - figure).abs() < 1e-9),
                    "{figure}: {text}"
                );
            }
        }
    }

    /// The numbers `text` writes, whatever their places.
    fn figures(text: &str) -> Vec<f64> {
        text.split(|c: char| !(c.is_ascii_digit() || c == '.'))
            .filter_map(|word| word.trim_end_matches('.').parse().ok())
            .collect()
    }

    /// The section of `config` with a meta model on the fake learner, on a
    /// window of `window` hours, the last 6 held out, taking a trade from
    /// 0.5, published from 0 USD per trade over a trade.
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
            .min_pnl(0.0)
            .min_trades(1)
            .learner(LearnerConfig::Fake {})
            .build()
    }

    /// The calibration of the sides' models of the meta tests.
    const CALIBRATION: Calibration = Calibration { a: -0.9, b: 0.0 };

    /// Publishes in `store` a baseline of `side`, of `config()`, valid from
    /// `valid_from`, predicting `probability` on every row, as
    /// `publish_baseline_of`.
    fn publish_baseline(store: &ModelStore, side: Side, valid_from: &str, probability: f64) {
        publish_baseline_of(config(), store, side, valid_from, probability);
    }

    /// Publishes in `store` a baseline of `side`, of `config`, valid from
    /// `valid_from`, predicting `probability` on every row, calibrated by
    /// `CALIBRATION`, expecting a win and a loss of 1R, its baseline the
    /// mean of its training labels.
    fn publish_baseline_of(
        config: ModelConfig,
        store: &ModelStore,
        side: Side,
        valid_from: &str,
        probability: f64,
    ) {
        let trues = (probability * 10.0).round() as usize;
        let labels = (0..10).map(|i| i < trues).collect();
        let names = side_names(features().names(), &config.target.payoff_names());
        let set = TrainingSet::new(names.len(), vec![0.5; 10 * names.len()], labels).unwrap();
        let model = LearnerConfig::Baseline {}.fit(&set).unwrap();
        store
            .publish(&StoredModel {
                manifest: ModelManifest {
                    strategy_id: StrategyId::from(STRATEGY),
                    valid_from: at(valid_from),
                    fingerprint: crate::model::dataset::fingerprint(
                        &features(),
                        config.decision,
                        &config.target,
                        &requirements(),
                        &venue(),
                    )
                    .unwrap()
                    .to_string(),
                    feature_names: names,
                    target: config.target,
                    side: Some(side),
                    learner: LearnerConfig::Baseline {},
                    calibration: CALIBRATION,
                    baseline: trues as f64 / 10.0,
                    payoff: Some(Payoff {
                        win: 1.0,
                        loss: 1.0,
                    }),
                    holdout: holdout(),
                    probes: vec![],
                },
                model: model.save().unwrap(),
            })
            .unwrap();
    }

    /// The decisions of the 6th: one before the sides' first models (a
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

    /// The sides' models of a training in `dir` as `config` says: from
    /// 06:00 a long, from 17:00 a short, and from 21:00, after every row, a
    /// long again, which no row may use.
    fn publish_baselines(dir: &Path, config: &ModelConfig) {
        let (long, short) = (
            store(dir, config, Side::Long),
            store(dir, config, Side::Short),
        );
        for (valid_from, long_probability) in [
            ("2026-01-06T06:00:00Z", 0.8),
            ("2026-01-06T17:00:00Z", 0.2),
            ("2026-01-06T21:00:00Z", 0.9),
        ] {
            publish_baseline_of(
                config.clone(),
                &long,
                Side::Long,
                valid_from,
                long_probability,
            );
            publish_baseline_of(
                config.clone(),
                &short,
                Side::Short,
                valid_from,
                1.0 - long_probability,
            );
        }
    }

    #[test]
    fn a_meta_model_learns_the_side_picked_from_its_out_of_sample_decisions() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), with_meta(24));
        publish_baselines(dir.path(), &with_meta(24));
        let t = at("2026-01-07T00:00:00Z");

        let trained = training.train_at(t, |_, _| Ok(meta_rows()));

        let Some(Outcome::Published {
            valid_from,
            holdout,
        }) = trained.meta
        else {
            panic!("{trained:?}");
        };
        assert_eq!((valid_from, holdout.auc), (t, Some(1.0)));
        let stored = meta_store(dir.path(), &with_meta(24)).read(t).unwrap();
        assert_eq!(
            stored.manifest.feature_names,
            ["mid_0", "picked_side", "picked_probability"]
        );
        assert_eq!(stored.manifest.side, None);
        let short = CALIBRATION.apply(0.8);
        let probes: Vec<&Vec<f64>> = stored.manifest.probes.iter().map(|p| &p.row).collect();
        assert_eq!(probes, [&vec![0.3, -1.0, short], &vec![0.7, -1.0, short]]);
        // Fitted on the two long trades, a win and a loss: the decision
        // before the sides' first model is no row.
        let model = LearnerConfig::Fake {}.load(&stored.model).unwrap();
        assert_eq!(model.predict(&[f64::NAN, 1.0, 0.8]).unwrap(), 0.5);
    }

    /// `meta_rows` with the long's loss at 10:00 a win: the meta, on the
    /// longs of the sides' models valid from 06:00, is fitted on the
    /// wins at 08:00 and 10:00 and calibrated on the loss at 13:00 and the
    /// win at 15:00, three wins in four, 3/4; not on the decision at 05:00,
    /// before the sides' first model, a win, nor on the holdout's shorts,
    /// a loss and a win.
    #[test]
    fn a_published_metas_baseline_is_the_training_mean_of_the_rows_it_was_fitted_and_calibrated_on()
    {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), with_meta(24));
        publish_baselines(dir.path(), &with_meta(24));
        let t = at("2026-01-07T00:00:00Z");
        let mut rows = meta_rows();
        rows[2] = row("2026-01-06T10:00:00Z", vec![0.5], true, hour());

        let trained = training.train_at(t, |_, _| Ok(rows));

        assert!(
            matches!(trained.meta, Some(Outcome::Published { .. })),
            "{trained:?}"
        );
        let stored = meta_store(dir.path(), &with_meta(24)).read(t).unwrap();
        assert_eq!(stored.manifest.baseline, 0.75);
    }

    /// `rows` as a time-only target has them: closed at their time, of a
    /// unit of 1, each side's payoff features its entry cost alone.
    fn at_their_time(rows: Vec<TargetRow>) -> Vec<TargetRow> {
        let at_its_time = |outcome| TargetOutcome {
            exit: Exit::Time,
            unit: 1.0,
            ..outcome
        };
        rows.into_iter()
            .map(|row| TargetRow {
                payoff: PerSide {
                    long: vec![0.07],
                    short: vec![0.08],
                },
                outcomes: Outcomes {
                    long: at_its_time(row.outcomes.long),
                    short: at_its_time(row.outcomes.short),
                },
                ..row
            })
            .collect()
    }

    /// A meta model learns the trade of the side picked, on a target with
    /// no exits too. `meta_rows` with the long's loss at 10:00 a win, each
    /// closed at its time: the long picked at each of the four morning
    /// rows expects `2 c(0.8) - 1`, 0.554 USD, a unit of 1 in money and no
    /// stop to weigh, above its margin of 0.5; so the meta is fitted on the
    /// wins at 08:00 and 10:00 and calibrated on the loss at 13:00 and the
    /// win at 15:00, three wins in four, 3/4.
    #[test]
    fn a_meta_model_on_a_time_only_target_learns_from_its_trades() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = with_meta(24);
        config.target.spec = Target::builder()
            .limit(SignedDuration::from_hours(1))
            .build();
        let training = training(dir.path(), config.clone());
        publish_baselines(dir.path(), &config);
        let t = at("2026-01-07T00:00:00Z");
        let mut rows = meta_rows();
        rows[2] = row("2026-01-06T10:00:00Z", vec![0.5], true, hour());

        let trained = training.train_at(t, |_, _| Ok(at_their_time(rows)));

        assert!(
            matches!(trained.meta, Some(Outcome::Published { .. })),
            "{trained:?}"
        );
        let manifest = meta_store(dir.path(), &config).read(t).unwrap().manifest;
        assert_eq!(manifest.baseline, 0.75);
        assert_eq!(manifest.target.spec.bracket, None);
    }

    /// A meta example carries the unit of its trade, the worth of the stop
    /// of the side picked: on `meta_rows` with each short's
    /// stop worth 4 USD, the long's 2, the
    /// four buys of the models valid from 06:00 carry 2 and the two sells
    /// of those valid from 17:00 carry 4, the short still expecting less
    /// than its margin in the morning and more in the evening.
    #[test]
    fn the_metas_examples_carry_the_unit_of_their_sides_trade() {
        let dir = tempfile::tempdir().unwrap();
        let config = with_meta(24);
        publish_baselines(dir.path(), &config);
        let rows: Vec<DatasetRow> = meta_rows()
            .into_iter()
            .map(|mut row| {
                row.outcomes.short.unit = 4.0;
                DatasetRow::of(&row)
            })
            .collect();

        let examples = meta::examples(
            &models(dir.path(), &config),
            &rows.iter().collect::<Vec<_>>(),
        )
        .unwrap();

        let sides_and_units: Vec<(f64, f64)> = examples
            .iter()
            .map(|example| (example.features[1], example.unit))
            .collect();
        assert_eq!(
            sides_and_units,
            [
                (1.0, 2.0),
                (1.0, 2.0),
                (1.0, 2.0),
                (1.0, 2.0),
                (-1.0, 4.0),
                (-1.0, 4.0)
            ]
        );
    }

    /// The examples list the stores as they are when made: one that cannot be
    /// listed fails them, rows or none, so a train never learns silently from
    /// rows skipped.
    #[test]
    fn a_store_that_cannot_be_listed_fails_the_metas_examples() {
        let dir = tempfile::tempdir().unwrap();
        let config = with_meta(24);
        publish_baselines(dir.path(), &config);
        store(dir.path(), &config, Side::Short).break_listing();

        let examples = meta::examples(&models(dir.path(), &config), &[]);

        assert!(examples.is_err());
    }

    /// Margins of the long alone: the short's models are never asked, and
    /// the meta's rows are the long's trades, the
    /// holdout's a loss at 0.3 and a win at 0.7, which the meta, taking the
    /// win, publishes on.
    #[test]
    fn a_long_only_strategy_gives_its_meta_model_buys_alone() {
        let dir = tempfile::tempdir().unwrap();
        let long_only = ModelConfig {
            margins: Margins {
                long: Some(0.0),
                short: None,
            },
            ..with_meta(24)
        };
        let training = training(dir.path(), long_only.clone());
        for (valid_from, probability) in
            [("2026-01-06T06:00:00Z", 0.8), ("2026-01-06T17:00:00Z", 0.6)]
        {
            publish_baseline(
                &store(dir.path(), &long_only, Side::Long),
                Side::Long,
                valid_from,
                probability,
            );
        }
        let mut buys = meta_rows();
        buys[5] = row("2026-01-06T19:00:00Z", vec![0.3], false, hour());
        buys[6] = row("2026-01-06T20:00:00Z", vec![0.7], true, hour());
        let t = at("2026-01-07T00:00:00Z");

        let trained = training.train_at(t, |_, _| Ok(buys));

        assert!(
            matches!(trained.meta, Some(Outcome::Published { .. })),
            "{trained:?}"
        );
        let stored = meta_store(dir.path(), &long_only).read(t).unwrap();
        let long = CALIBRATION.apply(0.6);
        let probes: Vec<&Vec<f64>> = stored.manifest.probes.iter().map(|p| &p.row).collect();
        assert_eq!(probes, [&vec![0.3, 1.0, long], &vec![0.7, 1.0, long]]);
    }

    /// The meta's window of 36 hours is longer than the sides' 24, and
    /// starts after the sides' first models, of the 5th.
    #[test]
    fn a_train_with_a_meta_model_replays_once_from_the_longer_windows_first_day() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), with_meta(36));
        publish_baselines(dir.path(), &with_meta(36));
        for side in Side::BOTH {
            publish_baseline(
                &store(dir.path(), &with_meta(36), side),
                side,
                "2026-01-05T00:00:00Z",
                0.5,
            );
        }
        let mut replays = vec![];

        training.train_at(at("2026-01-07T00:00:00Z"), |from, to| {
            replays.push((from, to));
            Ok(meta_rows())
        });

        assert_eq!(
            replays,
            [(at("2026-01-05T00:00:00Z"), at("2026-01-07T00:00:00Z"))]
        );
    }

    /// The meta's window of 36 hours starts on the 5th, before the
    /// sides' first models, of 06:00 of the 6th, where its rows start:
    /// the one replay starts with the sides' window, on the 6th.
    #[test]
    fn a_train_replays_no_day_before_the_sides_first_model() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), with_meta(36));
        publish_baselines(dir.path(), &with_meta(36));
        let mut replays = vec![];

        training.train_at(at("2026-01-07T00:00:00Z"), |from, to| {
            replays.push((from, to));
            Ok(meta_rows())
        });

        assert_eq!(
            replays,
            [(at("2026-01-06T00:00:00Z"), at("2026-01-07T00:00:00Z"))]
        );
    }

    /// No side's model is valid before the train, so no row of the meta
    /// has a decision of the sides: the meta waits, and none is fitted, the
    /// sides fitted and published as ever.
    #[test]
    fn a_meta_model_without_a_sides_model_before_it_waits() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), with_meta(24));

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(meta_rows()));

        assert_eq!(trained.meta, Some(Outcome::Kept(Kept::Waiting)));
        assert!(
            meta_store(dir.path(), &with_meta(24))
                .valid_froms()
                .unwrap()
                .is_empty()
        );
    }

    /// Never retrained, the sides' models of the 6th, the last valid
    /// from 21:00, are in use at the train of the 7th: kept, their
    /// decisions give the meta its rows, and the meta is fitted alone,
    /// learning from the very models in use.
    #[test]
    fn never_retrained_a_meta_whose_sides_are_in_use_is_fitted_alone() {
        let dir = tempfile::tempdir().unwrap();
        let config = ModelConfig {
            retrain: Retrain::Never,
            ..with_meta(24)
        };
        let training = training(dir.path(), config.clone());
        publish_baselines(dir.path(), &config);
        let t = at("2026-01-07T00:00:00Z");
        let last = at("2026-01-06T21:00:00Z");

        let trained = training.train_at(t, |_, _| Ok(meta_rows()));

        let in_use = Outcome::InUse {
            valid_from: last,
            retrain: Retrain::Never,
        };
        assert_eq!(
            (trained.long.as_ref(), trained.short.as_ref()),
            (Some(&in_use), Some(&in_use))
        );
        assert!(
            matches!(trained.meta, Some(Outcome::Published { valid_from, .. }) if valid_from == t),
            "{trained:?}"
        );
        for side in Side::BOTH {
            let valid_froms = store(dir.path(), &config, side).valid_froms().unwrap();
            assert_eq!(valid_froms.last(), Some(&last), "{side:?}");
        }
    }

    /// `with_meta` on a meta window of 24 hours, never retrained.
    fn never_with_meta() -> ModelConfig {
        ModelConfig {
            retrain: Retrain::Never,
            ..with_meta(24)
        }
    }

    /// The decisions of the 7th, of the sides as `rows` of the 6th fit
    /// them (`a_train_fits_calibrates_and_judges_each_side_on_its_part_of_the_window`):
    /// on the feature p the long calibrated 1 / (1 + 4^(1 - 2p)), a win of
    /// 1.5R and a loss of 0.75R, the short 1 / (1 + 4^(2p - 1)), 1.125R and
    /// 1R, each trading from an expected 0.5 USD on stops worth 2 USD.
    /// Fitted on, two longs at 0.5 (the long expecting 0.75 USD, the short
    /// 0.125), a win and a loss (meta labels true, false); calibrated on, a
    /// short at 0.25 losing and a long at 0.75 winning, the meta's fake
    /// predicting their first feature; held out, a short at 0.3 losing and
    /// a long at 0.7 winning, the side picked calibrated 1 / (1 + 4^-0.4)
    /// either way.
    fn decisions_of_the_seventh() -> Vec<TargetRow> {
        vec![
            row("2026-01-07T08:00:00Z", vec![0.5], true, hour()),
            row("2026-01-07T10:00:00Z", vec![0.5], false, hour()),
            row("2026-01-07T13:00:00Z", vec![0.25], true, hour()),
            row("2026-01-07T15:00:00Z", vec![0.75], true, hour()),
            row("2026-01-07T19:00:00Z", vec![0.3], true, hour()),
            row("2026-01-07T20:00:00Z", vec![0.7], true, hour()),
        ]
    }

    /// The train at the 8th of `config` in `dir`, on an empty store,
    /// replaying `rows` and the decisions of the 7th: the training's
    /// models, what it did, and the spans it replayed.
    fn two_time(
        dir: &Path,
        config: ModelConfig,
    ) -> (ModelSet, Trained, Vec<(Timestamp, Timestamp)>) {
        let models = models(dir, &config);
        let training = training(dir, config);
        let mut replays = vec![];
        let trained = training.train_at(at("2026-01-08T00:00:00Z"), |from, to| {
            replays.push((from, to));
            Ok(rows()
                .into_iter()
                .chain(decisions_of_the_seventh())
                .collect())
        });
        (models, trained, replays)
    }

    /// Never retrained, on an empty store, the train of the 8th fits the
    /// sides as of the 7th, the meta's window of 24 hours before it, and
    /// publishes them valid from then, and the meta at the 8th.
    #[test]
    fn never_retrained_on_an_empty_store_a_train_fits_the_sides_as_of_the_metas_window_before_it_and_the_meta_at_it()
     {
        let dir = tempfile::tempdir().unwrap();
        let (seventh, eighth) = (at("2026-01-07T00:00:00Z"), at("2026-01-08T00:00:00Z"));

        let (models, trained, _) = two_time(dir.path(), never_with_meta());

        for (outcome, valid_from) in [
            (&trained.long, seventh),
            (&trained.short, seventh),
            (&trained.meta, eighth),
        ] {
            let Some(Outcome::Published {
                valid_from: published,
                ..
            }) = outcome
            else {
                panic!("{trained:?}");
            };
            assert_eq!(*published, valid_from, "{trained:?}");
        }
        for side in Side::BOTH {
            assert_eq!(
                models.side(side).unwrap().store().valid_froms().unwrap(),
                [seventh]
            );
        }
        assert_eq!(
            models.meta().unwrap().store().valid_froms().unwrap(),
            [eighth]
        );
    }

    /// The sides fitted as of the 7th are fitted, calibrated and judged
    /// as a train at the 7th would: on `rows`, the holdout's win at 23:30
    /// of the 6th, settled after the 7th, not among them, nor the decisions of
    /// the 7th: two trades picked, of every trade's four.
    #[test]
    fn a_two_time_trains_sides_are_judged_on_rows_settled_by_their_time() {
        let dir = tempfile::tempdir().unwrap();
        let (models, _, _) = two_time(dir.path(), never_with_meta());

        let long = models
            .side(Side::Long)
            .unwrap()
            .store()
            .read(at("2026-01-07T00:00:00Z"))
            .unwrap();

        close(long.manifest.calibration.apply(0.25), 1.0 / 3.0);
        let probes: Vec<&Vec<f64>> = long.manifest.probes.iter().map(|p| &p.row).collect();
        assert_eq!(
            probes,
            [&vec![0.8, 1.4, 1.1, 0.07], &vec![0.3, 1.4, 1.1, 0.07]]
        );
        let Earnings { taken, every, .. } = long.manifest.holdout.earnings;
        assert_eq!((taken.count, every.count), (2, 4));
    }

    /// The meta at the 8th learns from the decisions of the sides just
    /// published over its window, the 7th: held out, the short's at 0.3 and
    /// the long's at 0.7, each side's probability 1 / (1 + (3/7)^s), `s` being
    /// ln 2 / ln 3.
    #[test]
    fn a_two_time_trains_meta_learns_from_the_sides_decisions_over_its_window() {
        let dir = tempfile::tempdir().unwrap();
        let (models, _, _) = two_time(dir.path(), never_with_meta());

        let meta = models
            .meta()
            .unwrap()
            .store()
            .read(at("2026-01-08T00:00:00Z"))
            .unwrap();

        let probability = 1.0 / (1.0 + (3.0_f64 / 7.0).powf(2_f64.ln() / 3_f64.ln()));
        let probes: Vec<&Vec<f64>> = meta.manifest.probes.iter().map(|p| &p.row).collect();
        let [short, long] = probes[..] else {
            panic!("{probes:?}");
        };
        assert_eq!((short[0], short[1]), (0.3, -1.0));
        close(short[2], probability);
        assert_eq!((long[0], long[1]), (0.7, 1.0));
        close(long[2], probability);
    }

    /// One replay serves both: from the sides' window back from the 7th,
    /// the 6th, to the train, the 8th.
    #[test]
    fn a_two_time_train_replays_once_from_the_sides_window_to_its_time() {
        let dir = tempfile::tempdir().unwrap();

        let (_, _, replays) = two_time(dir.path(), never_with_meta());

        assert_eq!(
            replays,
            [(at("2026-01-06T00:00:00Z"), at("2026-01-08T00:00:00Z"))]
        );
    }

    /// Two trades picked on the sides' holdout, below a minimum of three,
    /// neither publishes as of the 7th: the meta has no decision of the
    /// sides to learn from, and waits, with no fit attempted, as the train
    /// says.
    #[test]
    fn the_sides_refused_in_a_two_time_train_leave_the_meta_waiting() {
        let dir = tempfile::tempdir().unwrap();
        let three = ModelConfig {
            min_trades: 3,
            ..never_with_meta()
        };

        let (models, trained, _) = two_time(dir.path(), three);

        for outcome in [&trained.long, &trained.short] {
            assert!(
                matches!(outcome, Some(Outcome::Kept(Kept::Refused { .. }))),
                "{trained:?}"
            );
        }
        assert_eq!(trained.meta, Some(Outcome::Kept(Kept::Waiting)));
        assert!(
            trained
                .to_string()
                .contains("waiting for the sides' decisions"),
            "{trained}"
        );
        assert!(
            models
                .meta()
                .unwrap()
                .store()
                .valid_froms()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn a_train_succeeds_unless_it_kept_a_model() {
        let t = at("2026-01-07T00:00:00Z");
        let published = Outcome::Published {
            valid_from: t,
            holdout: holdout(),
        };
        let in_use = Outcome::InUse {
            valid_from: t,
            retrain: Retrain::Every(SignedDuration::from_hours(24)),
        };
        let fitted_or_in_use = Trained {
            long: Some(published.clone()),
            short: Some(in_use.clone()),
            meta: Some(in_use.clone()),
        };
        let short_kept = Trained {
            short: Some(Outcome::Kept(Kept::Failed("no short row".to_string()))),
            ..fitted_or_in_use.clone()
        };
        let meta_waiting = Trained {
            meta: Some(Outcome::Kept(Kept::Waiting)),
            ..fitted_or_in_use.clone()
        };

        assert!(fitted_or_in_use.succeeded());
        assert!(Trained::from(in_use).succeeded());
        assert!(!short_kept.succeeded());
        assert!(!meta_waiting.succeeded());
    }

    /// A model kept in use reads with its valid-from; a waiting meta as
    /// waiting for the sides' decisions.
    #[test]
    fn a_model_in_use_reads_with_its_valid_from_and_a_waiting_meta_as_waiting() {
        let in_use = Outcome::InUse {
            valid_from: at("2026-01-07T00:00:00Z"),
            retrain: Retrain::Every(SignedDuration::from_hours(24)),
        }
        .to_string();
        let waiting = Outcome::Kept(Kept::Waiting).to_string();

        assert!(
            in_use.contains("in use") && in_use.contains("2026-01-07T00:00:00Z"),
            "{in_use}"
        );
        assert!(
            waiting.contains("waiting for the sides' decisions"),
            "{waiting}"
        );
    }

    /// Never retrained, a model in use reads as in use since its
    /// valid-from, and never retrained.
    #[test]
    fn a_model_in_use_never_retrained_reads_as_in_use_since_its_valid_from() {
        let in_use = Outcome::InUse {
            valid_from: at("2026-01-06T00:00:00Z"),
            retrain: Retrain::Never,
        }
        .to_string();

        assert!(
            in_use.contains("in use since 2026-01-06T00:00:00Z")
                && in_use.contains("never retrained"),
            "{in_use}"
        );
    }

    #[test]
    fn a_train_of_several_models_reads_as_each_outcome() {
        let trained = Trained {
            long: Some(Outcome::Published {
                valid_from: at("2026-01-07T00:00:00Z"),
                holdout: holdout(),
            }),
            short: Some(Outcome::Kept(Kept::Failed("no short row".to_string()))),
            meta: Some(Outcome::Kept(Kept::Failed("no row".to_string()))),
        };

        let text = trained.to_string();

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
