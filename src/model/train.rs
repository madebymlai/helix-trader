//! Training at a time: the one operation that turns a strategy's dataset
//! into published models, or keeps the current ones, for retrains in a
//! backtest and live alike.
//!
//! A strategy has a model for each side it trades (the sides of its
//! margins, `ModelConfig::margins`): each a regression of its side's
//! realised net R, the trade's net P&L over its unit
//! (`holdout::Example::realised`), the long's of the long of the target,
//! the short's of its short, both on the same rows, window and holdout,
//! each reading a row's features then its own side's payoff features
//! (`dataset::DatasetRow::side`), under the names its store checks it
//! against (`store::SideLevel::feature_names`). The sides are calibrated,
//! judged and published as a pair, or not at all: a pair is the sides one
//! train published, valid from one time (`Models::pairs`), and a one-sided
//! strategy's is its one side. A train never judges nor publishes a side
//! beside another side's older model.
//!
//! What a train at `t` does to each model is the schedule's plan
//! (`schedule::Schedule::plan`), from the stores' valid-froms alone: a
//! model with a current model at `t`, the sides their pair, keeps it, in use
//! (retrained, the one valid from `t`; never retrained, the latest valid by
//! `t`, so each model is trained once); a meta with no pair of the sides
//! before `t` waits for the sides' decisions; the others are fitted, the
//! sides every one of them, as of `t`, or, never retrained, the sides with
//! neither them nor the meta in use, as of `t` less the meta's window, the
//! meta at `t` on their decisions. With nothing to fit the train replays
//! nothing.
//!
//! A model fitted as of `t'`, the train's time or before it, takes the
//! rows of the window ending at `t'` (`ModelConfig::window`), the days
//! the dataset lacks built by one replay to the train's time from the
//! first row a fit reads (`Dataset::settled`), and drops the rows not
//! settled at `t'`: no row's target reaches past `t'`, so the purge
//! between the rows and the model's first use is the target's limit, by
//! construction. The window ends in the holdout (`ModelConfig::holdout`)
//! and, before it, the calibration, as long, each part's rows taken with
//! the same gap: each side's model is fitted on the rows before the
//! calibration settled at its start; the pair is calibrated together
//! (`calibration::Ksplit`) on both sides' predictions and R on the
//! calibration's rows settled at the holdout's start, which no model was
//! fitted on and the holdout is not among; and judged on the holdout
//! (`holdout::Holdout`).
//!
//! The gate judges the decision the handle returns, on the target's trade;
//! what a strategy then does with a decision stays outside its figures. Every
//! side of the pair is fitted before any publishes; a fit failing keeps every
//! side, failed, with its error. Each side's calibrated expected R, `E'`, is
//! judged on its own against its trades' R on the holdout
//! (`score::ExpectedScore`): a skill not above nought, its squared error no
//! less than that of the training mean of R on the rows it was fitted and
//! calibrated on (`baseline`), refuses the pair; its mean `R - E'`, its
//! calibration slope and its top-decile gap are reported. The earnings are
//! judged on the trades picked over the holdout's rows
//! (`deciding::DecidingModels::pick_among`), each side's `E'` held to its
//! margin in R, on each side's row, among the pair's own sides alone. A row
//! where both sides clear their margins counts one trade, the side expected
//! the most; a tie, none. The minimums (`ModelConfig::min_pnl`, `ModelConfig::min_trades`,
//! `Holdout::refusal`) read those picks, beside every trade of the pair's
//! sides on the rows, each trade's net P&L in R, over its unit, as the
//! sides learn it (`holdout::Earnings`).
//!
//! The pair is refused for the first reason found, a side's skill, the
//! long's first, then the picks' minimums: every side reports it beside its
//! own holdout, its own score, and the earnings of the pair's picks, and the
//! current pair stays. A profitable side paired with a losing one is refused
//! with it. Otherwise the sides publish together, as judged, with no refit,
//! in their sides' stores (`store`), valid from `t'`, each with the pair's
//! calibration naming the pair, its baseline, which its out-of-sample scores
//! are read beside (`walk::WalkForward::scores`), that holdout, its mean
//! `R - E'` among it, and its manifest probing it on rows of the holdout. A
//! publish is a rename per side, the long first: one failing keeps every
//! side, failed, and the sides published before it, as those a crash
//! between the renames leaves, have no other side beside them at their
//! valid-from, so are in no pair: no decision uses them, and the next train
//! fits the pair again, its publishes replacing them.
//!
//! With a meta model (`model::meta`), the sides train first, then the
//! meta, on the same rows, of the one replay: the meta's window is its
//! own (`MetaConfig::window`), from the sides' first pair on, and its
//! rows are made from the sides' out-of-sample decisions, those of the
//! pair of the stores in use at each decision, read once the
//! sides published: the one just published valid from `t` used by none,
//! that of a train at two times, valid from the window's start, used
//! throughout it; and should the sides of such a train publish none, the
//! meta waits for the sides' decisions (`Plan::meta_after_sides`). The meta
//! is a regression of the realised net R of the trade of the side picked,
//! calibrated by its level line alone (`calibration::Ksplit` of one side) on
//! its own calibration's rows, and is judged alone, on its own holdout, by
//! its skill, as a side is, and by the trades of the side picked its margin
//! takes (`MetaConfig::margin`, `holdout::MetaMargin`), in R, and published
//! in its own store.
//!
//! A train never errors: what goes wrong is an outcome, the current model
//! kept with the reason (`Kept`), for each model trained. It succeeds when
//! it kept no model (`Trained::succeeded`).

use std::fmt;

use anyhow::Context;
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::StrategyId;

use super::{
    blueprint::OnVenue,
    calibration::Ksplit,
    dataset::{Dataset, DatasetRow, SchemaMismatch},
    deciding::DecidingModels,
    holdout::{Earnings, Example, Holdout, MetaMargin, Minimums, Refusal},
    learner::{LearnerConfig, SavedModel, TrainingSet},
    meta,
    schedule::{Models, Retrain, Schedule, Step},
    score::ExpectedScore,
    store::{
        Baselined, MetaLevel, ModelCalibration, ModelManifest, ModelSet, ModelStore, Pair,
        PairCalibration, Probe, SideLevel, SideModel, StoredModel,
    },
    targets::{Side, TargetConfig, TargetRow},
};
use crate::strategy::risk::R;

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

/// A model fitted and calibrated as of a time, not published: what its
/// manifest takes of the fit.
#[derive(Debug)]
struct Fitted {
    saved: SavedModel,
    /// The side of a side's model; none of a meta's.
    side: Option<Side>,
    calibration: ModelCalibration,
    baseline: f64,
    /// Rows of its holdout and its predictions on them, before calibration.
    probes: Vec<Probe>,
}

/// A side's model fitted as of a time on the rows before the calibration,
/// not yet calibrated with the other sides of its pair.
#[derive(Debug)]
struct SideFit<'a> {
    side: Side,
    level: &'a Level,
    /// The model, its baseline the training mean of R on the rows it was
    /// fitted and calibrated on.
    model: SideModel,
    saved: SavedModel,
}

/// A meta model fitted and calibrated as of a time, and how it did on its
/// holdout, not judged yet.
#[derive(Debug)]
struct MetaFit {
    fitted: Fitted,
    held_out: Vec<Example>,
    /// Its calibrated expected R on each row held out.
    expected: Vec<f64>,
    score: ExpectedScore,
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
        holdout: Box<Holdout>,
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
    /// The holdout falls short of the minimums; why, and the holdout. A
    /// side's why is its pair's, its own or another side's.
    Refused { why: Refusal, holdout: Box<Holdout> },
    /// The replay, the dataset, the fit or the store failed, or the window
    /// had no row to fit or to calibrate on, or no holdout to judge; the
    /// error. A side's is its pair's, its own or another side's.
    Failed(String),
    /// The dataset holds rows that do not match the strategy's features; the
    /// mismatch.
    SchemaMismatch(String),
    /// A meta waits for the sides' decisions to learn from: no pair of the
    /// sides is valid before the train, so none was fitted
    /// (`schedule::Step::Wait`).
    Waiting,
}

impl Training {
    /// The training of the models of `model`, a strategy's model on the
    /// venue of its dataset runs, one for each side of its margins, and of
    /// its meta model if its config has one, on its dataset and in its
    /// model set, as its config says.
    pub(super) fn new(model: OnVenue<'_>) -> Self {
        let OnVenue {
            blueprint,
            dataset,
            models,
            schedule,
        } = model;
        let config = blueprint.config();
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
        Self {
            strategy_id: blueprint.strategy_id(),
            dataset,
            schedule,
            models,
            levels,
            target: config.target.clone(),
        }
    }

    /// Trains at `at` as the schedule plans it (`Schedule::plan`), from
    /// the stores' valid-froms: `replay` runs the strategy over a span of
    /// the days the dataset lacks and returns the rows it settled (see
    /// `Dataset::settled`), once, from where the plan's fits start, and not
    /// at all if it fits none. Fits the sides' pair if the plan fits it, as
    /// of its time, and judges the sides together (`sides`), then the meta model
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
        let sides = self.sides(plan.steps(), &rows);
        let meta = || {
            let level = self.levels.meta.as_ref()?;
            let margin = MetaMargin(self.models.meta()?.config().margin);
            let step = match self.models.valid_froms() {
                Ok(valid_froms) => plan.meta_after_sides(&valid_froms)?,
                Err(e) => return Some(kept(&e)),
            };
            Some(match self.planned(step, &rows) {
                Planned::Done(outcome) => outcome,
                Planned::Fit { at, rows } => self
                    .fit_meta(level, margin, at, &rows)
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

    /// What a train does to the sides at `steps`, on the replay's `rows`: the
    /// plan keeps the pair, each side its outcome, or fits it, the sides'
    /// step being their pair's (`Schedule::plan`), the pair judged and
    /// published or refused together (`pair`).
    fn sides(&self, steps: &Models<Step>, rows: &anyhow::Result<Vec<DatasetRow>>) -> Trained {
        let mut trained = Trained {
            long: None,
            short: None,
            meta: None,
        };
        let traded: Vec<Side> = self.models.sides().map(SideLevel::side).collect();
        let Some(&step) = traded.first().and_then(|&side| steps.side(side)) else {
            return trained;
        };
        let outcomes = match self.planned(step, rows) {
            Planned::Done(outcome) => traded.iter().map(|&side| (side, outcome.clone())).collect(),
            Planned::Fit { at, rows } => self
                .pair(at, rows)
                .unwrap_or_else(|e| traded.iter().map(|&side| (side, kept(&e))).collect()),
        };
        for (side, outcome) in outcomes {
            trained.set_side(side, outcome);
        }
        trained
    }

    /// The outcome of each side of the pair fitted as of `at` on `rows`, in
    /// time order, split into the rows fitted on, calibrated on and held out
    /// (`split`): each side's model fitted on its realised R (`fit_side`),
    /// the pair calibrated together on the calibration's rows
    /// (`Ksplit::fit`), and judged on the holdout, each side on its own
    /// calibrated expected R against its trades' R (`ExpectedScore`), the
    /// pair on the earnings of its picks (`earned`); refused, every side,
    /// for the first reason found, a side's skill, the long's first, then the
    /// minimums of the picks; else published together (`publish_pair`).
    ///
    /// # Errors
    ///
    /// Returns an error if a side's fit fails, naming the side, or there is
    /// no row to calibrate on or to judge on, or a model fails to predict.
    fn pair(&self, at: Timestamp, rows: Vec<&DatasetRow>) -> anyhow::Result<Vec<(Side, Outcome)>> {
        let margins: Vec<(Side, R)> = self
            .models
            .sides()
            .map(|traded| (traded.side(), traded.margin()))
            .collect();
        let levels: Vec<(Side, &Level)> = margins
            .iter()
            .filter_map(|&(side, _)| self.levels.side(side).map(|level| (side, level)))
            .collect();
        let holdout = at - levels[0].1.holdout;
        let (fitted, calibrating, held_out) =
            split(rows, (holdout - levels[0].1.holdout, holdout), |row| {
                (row.ts_event, row.settled_at)
            });
        let fits = levels
            .iter()
            .map(|&(side, level)| {
                fit_side(side, level, &fitted, &calibrating)
                    .with_context(|| format!("fitting the {} model", side.name()))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let calibration = Ksplit::fit(
            &calibrating
                .iter()
                .map(|row| {
                    fits.iter()
                        .map(|fit| {
                            let example = Example::of_side(row, fit.side);
                            Ok((fit.model.predict(&example.features)?, example.realised()))
                        })
                        .collect()
                })
                .collect::<anyhow::Result<Vec<_>>>()?,
        )
        .context("calibrating the pair on the window before the holdout")?;
        anyhow::ensure!(
            !held_out.is_empty(),
            "no row in the holdout to judge the models on"
        );
        let (mut models, mut trained) = (vec![], vec![]);
        for SideFit {
            side,
            level,
            model,
            saved,
        } in fits
        {
            trained.push((side, level, saved, model.baseline()));
            models.push((side, model));
        }
        let pair = Pair::new(models, calibration)?;
        let earnings = earned(&pair, &margins, &held_out)?;
        let mut expected: Vec<Vec<(f64, f64, f64)>> = vec![vec![]; trained.len()];
        for row in &held_out {
            let calibrated = pair.expect(&row.sides())?;
            for (&(side, _, _, baseline), rows) in trained.iter().zip(&mut expected) {
                let realised = Example::of_side(row, side).realised();
                rows.push((calibrated.of(side), realised, baseline));
            }
        }
        let sides: Vec<Side> = trained.iter().map(|&(side, ..)| side).collect();
        let holdouts: Vec<Holdout> = expected
            .iter()
            .map(|rows| Holdout {
                earnings,
                score: ExpectedScore::of(rows),
            })
            .collect();
        let refusal = holdouts
            .iter()
            .find_map(Holdout::score_refusal)
            .or_else(|| {
                trained
                    .iter()
                    .zip(&holdouts)
                    .find_map(|((_, level, ..), holdout)| holdout.refusal(level.minimums))
            });
        if let Some(why) = refusal {
            return Ok(sides
                .into_iter()
                .zip(holdouts)
                .map(|(side, holdout)| {
                    let holdout = Box::new(holdout);
                    (side, Outcome::Kept(Kept::Refused { why, holdout }))
                })
                .collect());
        }
        let calibration = ModelCalibration::Pair(PairCalibration {
            valid_from: at,
            sides: sides.clone(),
            lines: calibration,
        });
        let published = trained
            .into_iter()
            .zip(holdouts)
            .map(|((side, level, saved, baseline), holdout)| {
                let probes = spread(held_out.len())
                    .map(|i| {
                        let row = held_out[i].side(side).features;
                        Ok(Probe {
                            prediction: pair.side(side).predict(&row)?,
                            row,
                        })
                    })
                    .collect::<anyhow::Result<_>>()?;
                let fitted = Fitted {
                    saved,
                    side: Some(side),
                    calibration: calibration.clone(),
                    baseline,
                    probes,
                };
                Ok((level, fitted, holdout))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(self.publish_pair(at, published))
    }

    /// Publishes each side of the pair `fits`, the long first, valid from
    /// `at`, each fitted with its holdout: each published, or, should a
    /// publish fail, each kept with its error, the sides published before it
    /// no pair without the others.
    fn publish_pair(
        &self,
        at: Timestamp,
        fits: Vec<(&Level, Fitted, Holdout)>,
    ) -> Vec<(Side, Outcome)> {
        let sides: Vec<Side> = fits
            .iter()
            .filter_map(|(_, fitted, _)| fitted.side)
            .collect();
        let mut published = vec![];
        for (level, fitted, holdout) in fits {
            let side = fitted.side;
            if let Err(e) = self.publish(level, at, fitted, holdout) {
                return sides.into_iter().map(|side| (side, kept(&e))).collect();
            }
            published.extend(side.map(|side| {
                (
                    side,
                    Outcome::Published {
                        valid_from: at,
                        holdout: Box::new(holdout),
                    },
                )
            }));
        }
        published
    }

    /// Fits the meta model of `level` as of `at` on the examples of `rows`, in
    /// time order, and publishes it, or keeps the current one with the reason,
    /// the trades of the side picked taken by `margin` on the net R the meta
    /// expects of each, its calibrated expected R.
    fn fit_meta(
        &self,
        level: &Level,
        margin: MetaMargin,
        at: Timestamp,
        rows: &[&DatasetRow],
    ) -> anyhow::Result<Outcome> {
        let examples = meta::examples(&self.models, rows)?;
        let MetaFit {
            fitted,
            held_out,
            expected,
            score,
        } = level.fit(at, examples)?;
        let trades: Vec<_> = held_out
            .iter()
            .zip(&expected)
            .map(|(row, &expected)| {
                let taken = R::finite(expected).is_some_and(|expected| margin.takes(expected));
                (taken, row.trade)
            })
            .collect();
        let holdout = Holdout {
            earnings: Earnings::of(&trades),
            score,
        };
        if let Some(why) = holdout.refusal(level.minimums) {
            let holdout = Box::new(holdout);
            return Ok(Outcome::Kept(Kept::Refused { why, holdout }));
        }
        self.publish(level, at, fitted, holdout)?;
        Ok(Outcome::Published {
            valid_from: at,
            holdout: Box::new(holdout),
        })
    }

    /// Publishes in `level`'s store the model `fitted`, valid from `at`, with
    /// `holdout`.
    fn publish(
        &self,
        level: &Level,
        at: Timestamp,
        fitted: Fitted,
        holdout: Holdout,
    ) -> anyhow::Result<()> {
        level.store.publish(&StoredModel {
            manifest: ModelManifest {
                strategy_id: self.strategy_id,
                valid_from: at,
                fingerprint: self.dataset.fingerprint().to_string(),
                feature_names: level.feature_names.clone(),
                target: self.target.clone(),
                side: fitted.side,
                learner: level.learner.clone(),
                calibration: fitted.calibration,
                baseline: fitted.baseline,
                holdout,
                probes: fitted.probes,
            },
            model: fitted.saved,
        })
    }
}

impl Level {
    /// The level of `meta`: it learns the realised R of the target on the
    /// side picked.
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

    /// The meta model fitted as of `at` on `examples`, in time order, a
    /// regression of each trade's realised R (`split`), calibrated by its
    /// level line alone (`Ksplit` of one side), and scored on its holdout,
    /// its calibrated expected R against the trades' R beside its baseline,
    /// the training mean of R on the rows it was fitted and calibrated on.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no row to fit on, to calibrate on or to
    /// judge on, a label is no finite number, or the learner fails.
    fn fit(&self, at: Timestamp, examples: Vec<Example>) -> anyhow::Result<MetaFit> {
        let holdout = at - self.holdout;
        let (fitted, calibrating, held_out) =
            split(examples, (holdout - self.holdout, holdout), |row| {
                (row.ts_event, row.settled_at)
            });
        let set = |rows: &[&Example]| {
            TrainingSet::new(
                self.feature_names.len(),
                rows.iter().flat_map(|row| row.features.clone()).collect(),
                rows.iter().map(|row| row.realised()).collect(),
            )
        };
        let model = self.learner.fit(
            &set(&fitted.iter().collect::<Vec<_>>())
                .context("fitting on the window before the calibration")?,
        )?;
        let line = Ksplit::fit(
            &calibrating
                .iter()
                .map(|row| Ok(vec![(model.predict(&row.features)?, row.realised())]))
                .collect::<anyhow::Result<Vec<_>>>()?,
        )
        .context("calibrating on the window before the holdout")?
        .level;
        anyhow::ensure!(
            !held_out.is_empty(),
            "no row in the holdout to judge the model on"
        );
        let trained: Vec<&Example> = fitted.iter().chain(&calibrating).collect();
        let baseline = baseline(&set(&trained)?, &trained[0].features)?;
        let saved = model.save()?;
        let predictions = held_out
            .iter()
            .map(|row| model.predict(&row.features))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let expected: Vec<f64> = predictions.iter().map(|&p| line.apply(p)).collect();
        let score = ExpectedScore::of(
            &expected
                .iter()
                .zip(&held_out)
                .map(|(&expected, row)| (expected, row.realised(), baseline))
                .collect::<Vec<_>>(),
        );
        let probes = spread(held_out.len())
            .map(|i| Probe {
                row: held_out[i].features.clone(),
                prediction: predictions[i],
            })
            .collect();
        Ok(MetaFit {
            fitted: Fitted {
                saved,
                side: None,
                calibration: ModelCalibration::Meta(line),
                baseline,
                probes,
            },
            held_out,
            expected,
            score,
        })
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

/// The model of `side` of `level`, fitted on the realised R of the side's
/// trades of the rows `fitted`, its baseline the training mean of R on
/// those and the rows `calibrating`, not calibrated yet.
///
/// # Errors
///
/// Returns an error if there is no row to fit on, a label is no finite
/// number, or the learner fails.
fn fit_side<'a>(
    side: Side,
    level: &'a Level,
    fitted: &[&DatasetRow],
    calibrating: &[&DatasetRow],
) -> anyhow::Result<SideFit<'a>> {
    let width = level.feature_names.len();
    let set = |rows: &[Example]| {
        TrainingSet::new(
            width,
            rows.iter().flat_map(|row| row.features.clone()).collect(),
            rows.iter().map(Example::realised).collect(),
        )
    };
    let fitting: Vec<Example> = fitted
        .iter()
        .map(|row| Example::of_side(row, side))
        .collect();
    let model = level
        .learner
        .fit(&set(&fitting).context("fitting on the window before the calibration")?)?;
    let trained: Vec<Example> = fitting
        .into_iter()
        .chain(calibrating.iter().map(|row| Example::of_side(row, side)))
        .collect();
    let baseline = baseline(&set(&trained)?, &trained[0].features)?;
    Ok(SideFit {
        side,
        level,
        saved: model.save()?,
        model: SideModel::new(model, baseline),
    })
}

/// What the decision earns on the holdout rows `held_out` of `pair`, in R: on
/// each row the side picked among its sides alone, each held to its margin of
/// `margins`, on each side's row, beside every trade of its sides.
///
/// # Errors
///
/// Returns an error if a model fails to predict.
fn earned(
    pair: &Pair,
    margins: &[(Side, R)],
    held_out: &[&DatasetRow],
) -> anyhow::Result<Earnings> {
    let mut trades = vec![];
    for row in held_out {
        let picked = DecidingModels::pick_among(pair, margins.iter().copied(), &row.sides())?;
        for &(side, _) in margins {
            let taken = picked.is_some_and(|picked| picked.side == side);
            trades.push((taken, row.trade(side)));
        }
    }
    Ok(Earnings::of(&trades))
}

/// `items` split at the calibration's start and the holdout's, `times`, each
/// item's decision time and settlement read by `of`: those before the
/// calibration settled by its start, fitted on; those from it to the holdout
/// settled by the holdout's start, calibrated on; and those from the
/// holdout, held out.
fn split<T>(
    items: Vec<T>,
    (calibration, holdout): (Timestamp, Timestamp),
    of: impl Fn(&T) -> (UnixNanos, UnixNanos),
) -> (Vec<T>, Vec<T>, Vec<T>) {
    let (calibration, holdout) = (UnixNanos::from(calibration), UnixNanos::from(holdout));
    let (mut fitted, mut calibrating, mut held_out) = (vec![], vec![], vec![]);
    for item in items {
        let (ts_event, settled_at) = of(&item);
        if ts_event >= holdout {
            held_out.push(item);
        } else if ts_event >= calibration {
            if settled_at <= holdout {
                calibrating.push(item);
            }
        } else if settled_at <= calibration {
            fitted.push(item);
        }
    }
    (fitted, calibrating, held_out)
}

/// The baseline of a model trained on `set`, the rows it is fitted and
/// calibrated on: the training mean of their labels, which the baseline
/// learner fitted on them predicts whatever the row, `row` say.
///
/// # Errors
///
/// Returns an error if the learner fails.
fn baseline(set: &TrainingSet, row: &[f64]) -> anyhow::Result<f64> {
    LearnerConfig::Baseline {}.fit(set)?.predict(row)
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
                "waiting for the sides' decisions: no pair of the sides is valid before the train"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, rc::Rc};

    use jiff::SignedDuration;
    use nautilus_model::{identifiers::InstrumentId, types::Money};
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        backtest::Commission,
        model::{
            blueprint::ModelBlueprint,
            calibration::Line,
            config::{Margins, MetaConfig, ModelConfig},
            dataset::DatasetVenue,
            decide::{DecisionRule, FeatureRow},
            deciding::testing::unchanged,
            features::{FeatureSet, side_names, side_row},
            holdout::Trades,
            learner::LearnerConfig,
            targets::{
                BracketSpec, Distance, Exit, Outcome as TargetOutcome, Outcomes, PerSide, Target,
                TargetRow,
            },
        },
        strategy::{DataRequirements, Declared, EmaArgs, IndicatorsConfig},
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
    /// bracket closed after 2 hours on both sides, trading the long from an
    /// expected 1R and the short from 0.5R, on a window of 24 hours, the last
    /// 6 held out and the 6 before them calibrated on, published from 0.25R
    /// per trade over a trade. The fake expects alike of both sides, which
    /// tie wherever both clear their margins: one side clears alone between
    /// 0.5R and 1R.
    fn config() -> ModelConfig {
        ModelConfig::builder()
            .decision(DecisionRule::Every(SignedDuration::from_hours(1)))
            .target(
                TargetConfig::builder()
                    .instrument_id(InstrumentId::from(AUD))
                    .risk("0.5%".parse().unwrap())
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
                long: Some(R::new(1.0)),
                short: Some(R::new(0.5)),
            })
            .min_pnl(R::new(0.25))
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

    /// The model of `config`.
    fn blueprint(config: ModelConfig) -> ModelBlueprint {
        ModelBlueprint::new(
            StrategyId::from(STRATEGY),
            features(),
            config,
            requirements(),
        )
        .unwrap()
    }

    /// The training in `dir`, its catalog and its store side by side: the
    /// catalog holds no instrument, none being read.
    fn training(dir: &Path, config: ModelConfig) -> Training {
        let catalog = dir.join("catalog");
        std::fs::create_dir_all(&catalog).unwrap();
        blueprint(config)
            .training(&catalog, &dir.join("models"), &venue())
            .unwrap()
    }

    /// The models of a training in `dir` as `config` says, as `training`
    /// keys them.
    fn models(dir: &Path, config: &ModelConfig) -> ModelSet {
        blueprint(config.clone())
            .model_set(&dir.join("models"), &venue())
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
                unit: Money::from("2 USD"),
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

    /// The decisions of the 6th, each side winning 3 USD at its take profit
    /// or losing 2 at its stop, its stop worth 2 USD unless said, both sides
    /// winning or losing together: of a train at midnight of the 7th, the
    /// window the 6th, the calibration from 12:00, the holdout from 18:00.
    /// Fitted on, at 0.9 a win (1.5R) and a loss (-1R); calibrated on, at
    /// 0.25 a loss on stops worth 4 USD (-0.5R) and at 0.75 a win (1.5R); held
    /// out, at 0.8 a win, the short's stop worth 4 USD (the long's 1.5R, the
    /// short's 0.75R), and at 0.3 a loss (-1R). Around them, wins on the
    /// feature 0.1 that would change the fit or the calibration if taken: one
    /// before the window, one before the calibration settling after its
    /// start, one before the holdout settling after its start, one in the
    /// holdout settling after the window's end.
    fn rows() -> Vec<TargetRow> {
        let both = |time, p, wins, after| row_of(time, vec![p], (wins, wins), after);
        let mut rows = vec![
            both("2026-01-05T23:00:00Z", 0.1, true, hour()),
            both("2026-01-06T08:00:00Z", 0.9, true, hour()),
            both("2026-01-06T10:00:00Z", 0.9, false, hour()),
            both(
                "2026-01-06T11:30:00Z",
                0.1,
                true,
                SignedDuration::from_mins(60),
            ),
            both("2026-01-06T13:00:00Z", 0.25, false, hour()),
            both("2026-01-06T15:00:00Z", 0.75, true, hour()),
            both(
                "2026-01-06T17:00:00Z",
                0.1,
                true,
                SignedDuration::from_mins(90),
            ),
            both("2026-01-06T19:00:00Z", 0.8, true, hour()),
            both("2026-01-06T20:00:00Z", 0.3, false, hour()),
            both("2026-01-06T23:30:00Z", 0.1, true, hour()),
        ];
        let outcomes = &mut rows[4].outcomes;
        for outcome in [&mut outcomes.long, &mut outcomes.short] {
            outcome.unit = Money::from("4 USD");
        }
        rows[7].outcomes.short.unit = Money::from("4 USD");
        rows
    }

    fn close(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < 1e-4, "{actual} != {expected}");
    }

    /// The calibration of `rows`'s pair, worked by hand: each side's fake
    /// predicts the row's feature, so the side call `D` is nought throughout
    /// and has no slope; the level `C`, 0.25 and 0.75, against the sides'
    /// mean R, -0.5 and 1.5, has a slope of 0.5 / 0.125 = 4, clipped to 1.5,
    /// and `alpha_C` 0.5 - 1.5 x 0.5 = -0.25 after the clip. Fitted on the
    /// holdout's rows too, or the rows settled late, `alpha_C` would not be
    /// -0.25. Each side's `E'` is `-0.25 + 1.5p`: 0.95 at 0.8, 0.2 at 0.3.
    const LINES: Ksplit = Ksplit {
        level: Line {
            alpha: -0.25,
            beta: 1.5,
        },
        side_call: Some(0.0),
    };

    /// Worked by hand on `rows`: each side fitted on the rows before the
    /// calibration settled at its start, a regression of their realised R,
    /// 1.5 and -1, of a mean of 0.25, which the fake predicts where its
    /// feature is missing; the pair calibrated together on those before the
    /// holdout settled at its start (`LINES`), each manifest holding the one
    /// calibration and naming the pair it was fitted on; judged on the
    /// holdout. Each side's probes are its rows of the holdout and its
    /// predictions on them, before the calibration: the fake's, their
    /// feature.
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
        let calibration = ModelCalibration::Pair(PairCalibration {
            valid_from: t,
            sides: vec![Side::Long, Side::Short],
            lines: LINES,
        });
        for side in Side::BOTH {
            let stored = store(dir.path(), &config(), side).read(t).unwrap();
            assert_eq!(stored.manifest.side, Some(side));
            assert_eq!(stored.manifest.calibration, calibration, "{side:?}");
            let fake = LearnerConfig::Fake {}.load(&stored.model).unwrap();
            // The side's row: the decision's feature, then its payoff
            // features.
            let payoff = bracket_payoff().of(side).clone();
            let row = |feature| side_row(&[feature], &payoff);
            assert_eq!(fake.predict(&row(f64::NAN)).unwrap(), 0.25, "{side:?}");
            let probes: Vec<(Vec<f64>, f64)> = stored
                .manifest
                .probes
                .iter()
                .map(|probe| (probe.row.clone(), probe.prediction))
                .collect();
            assert_eq!(probes, [(row(0.8), 0.8), (row(0.3), 0.3)], "{side:?}");
        }
    }

    /// The holdout of `outcome`, published or refused.
    fn holdout_of(outcome: Option<Outcome>) -> Holdout {
        match outcome {
            Some(
                Outcome::Published { holdout, .. } | Outcome::Kept(Kept::Refused { holdout, .. }),
            ) => *holdout,
            outcome => panic!("{outcome:?}"),
        }
    }

    fn close_to(actual: Option<f64>, expected: f64) {
        close(actual.unwrap(), expected);
    }

    /// Worked by hand on `rows`'s holdout, each side expecting 0.95R at 0.8
    /// and 0.2R at 0.3 (`LINES`), the long trading from 1R and the short from
    /// 0.5R. At 19:00 the short alone clears, and is picked, a win of 3 USD
    /// on its stop worth 4, 0.75R; at 20:00 neither clears. One trade of
    /// 0.75R; every trade the four of the two sides on the two rows, the
    /// long's win of 1.5R, the short's 0.75R and two losses of 1R, 0.0625R
    /// per trade. At 1.5 times the commission of 0.2 each makes 0.1 USD less,
    /// 0.025R on a stop worth 4 and 0.05R on one worth 2: 0.725R taken, and
    /// 0.0625R less 0.175R / 4 every trade.
    /// Both sides hold those earnings beside their own score,
    /// the baseline each side's mean R on the rows fitted and calibrated on,
    /// 0.375. The long's R, 1.5 and -1: errors 0.55 and -1.2, squared 1.7425,
    /// the baseline's 1.265625 + 1.890625 = 3.15625, a skill of 1 - 1.7425 /
    /// 3.15625; a mean R - E' of -0.325; a calibration slope of 2.5 / 0.75;
    /// the top decile, the row expected 0.95, a gap of 0.55. The short's,
    /// 0.75 and -1: errors -0.2 and -1.2, 1.48 against 0.140625 + 1.890625 =
    /// 2.03125; a mean of -0.7; a slope of 1.75 / 0.75; a gap of -0.2.
    #[test]
    fn the_sides_are_judged_on_the_trades_picked_on_the_holdout() {
        let dir = tempfile::tempdir().unwrap();
        let trained =
            training(dir.path(), config()).train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows()));

        for (holdout, (skill, mean_error, slope, gap)) in [
            (
                holdout_of(trained.long),
                (1.0 - 1.7425 / 3.15625, -0.325, 2.5 / 0.75, 0.55),
            ),
            (
                holdout_of(trained.short),
                (1.0 - 1.48 / 2.03125, -0.7, 1.75 / 0.75, -0.2),
            ),
        ] {
            let Earnings {
                taken,
                every,
                taken_stressed,
                every_stressed,
            } = holdout.earnings;
            assert_eq!((taken.count, every.count), (1, 4), "{holdout:?}");
            close_to(taken.expectancy, 0.75);
            close_to(taken.win_rate, 1.0);
            close_to(every.expectancy, 0.0625);
            close_to(every.win_rate, 0.5);
            close_to(taken_stressed.expectancy, 0.725);
            close_to(every_stressed.expectancy, 0.0625 - 0.175 / 4.0);
            let score = holdout.score;
            assert_eq!(score.rows, 2);
            close_to(score.skill, skill);
            close_to(score.mean_error, mean_error);
            close_to(score.slope, slope);
            close_to(score.top_decile_gap, gap);
        }
    }

    /// A side model of `config()`, in no store, predicting an R of `r` on
    /// every row.
    fn in_hand(r: f64) -> SideModel {
        let width = side_names(features().names(), &config().target.payoff_names()).len();
        let set = TrainingSet::new(width, vec![0.5; width], vec![r]).unwrap();
        SideModel::new(LearnerConfig::Baseline {}.fit(&set).unwrap(), r)
    }

    /// Worked by hand, by a pair in no store whose long expects 0.2R and
    /// short 0.3R on every row, its calibration leaving them as they are: on
    /// two rows where the long loses 2 USD and the short wins 3, both sides
    /// clear margins of 0.1R, and the short alone is picked on each, its
    /// win; the long's losses are taken by no decision. Each stop worth 2 USD,
    /// two trades of 1.5R; every trade the four of the two rows, 0.25R per
    /// trade.
    #[test]
    fn a_row_where_both_sides_clear_counts_one_trade() {
        let pair = Pair::new(
            vec![(Side::Long, in_hand(0.2)), (Side::Short, in_hand(0.3))],
            Ksplit {
                level: Line {
                    alpha: 0.0,
                    beta: 1.0,
                },
                side_call: Some(1.0),
            },
        )
        .unwrap();
        let rows = [
            row("2026-01-06T19:00:00Z", vec![0.8], false, hour()),
            row("2026-01-06T20:00:00Z", vec![0.3], false, hour()),
        ]
        .map(|row| DatasetRow::of(&row));
        let margins = [(Side::Long, R::new(0.1)), (Side::Short, R::new(0.1))];

        let Earnings { taken, every, .. } =
            earned(&pair, &margins, &rows.iter().collect::<Vec<_>>()).unwrap();

        assert_eq!((taken.count, every.count), (2, 4));
        close_to(taken.expectancy, 1.5);
        close_to(every.expectancy, 0.25);
    }

    /// Worked by hand: two baselines, each side's training mean whatever the
    /// row, fitted on the 6th's rows before 12:00, the long winning 1.5R then
    /// losing 1R, a mean of 0.25, the short losing 1R twice, -1, so that `C`
    /// is -0.375 and `D` 1.25 on every row. Calibrated on the long winning
    /// beside the short losing, 2.5R apart, then both winning: a side call
    /// that never varies has no slope, each side's `E'` the level, the sides'
    /// mean R, 0.875, clearing either's margin of 0.5R, and the sides tie on
    /// every row of the holdout, which picks no trade. Each side's R there,
    /// 1.5 and -1 in either order, has a mean of 0.25: its mean `R - E'` is
    /// -0.625. A slope fitted on the calibration's means alone, 3.125 /
    /// 3.125, would expect 1.5R of the long and 0.25R of the short, and pick
    /// the long on every row.
    #[test]
    fn two_sides_whose_predictions_never_vary_pick_no_trade() {
        let dir = tempfile::tempdir().unwrap();
        let baselines = ModelConfig {
            learner: LearnerConfig::Baseline {},
            margins: Margins {
                long: Some(R::new(0.5)),
                short: Some(R::new(0.5)),
            },
            ..config()
        };
        let rows = [
            ("2026-01-06T08:00:00Z", (true, false)),
            ("2026-01-06T10:00:00Z", (false, false)),
            ("2026-01-06T13:00:00Z", (true, false)),
            ("2026-01-06T15:00:00Z", (true, true)),
            ("2026-01-06T19:00:00Z", (true, false)),
            ("2026-01-06T20:00:00Z", (false, true)),
        ]
        .map(|(time, wins)| row_of(time, vec![0.5], wins, hour()));

        let trained = training(dir.path(), baselines)
            .train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows.to_vec()));

        for outcome in [&trained.long, &trained.short] {
            let Some(Outcome::Kept(Kept::Refused { holdout, .. })) = outcome else {
                panic!("{trained:?}");
            };
            assert_eq!(holdout.earnings.taken.count, 0, "{holdout:?}");
            close_to(holdout.score.mean_error, -0.625);
        }
    }

    /// `rows` but for the holdout: at 19:00 at 0.8 the long wins and the
    /// short, on its stop worth 4 USD, loses, -0.5R; at 20:00 at 0.3 the long
    /// loses and the short wins, 1.5R. The short, expecting 0.95R and 0.2R
    /// beside its baseline 0.375, errs by -1.45 and 1.3, squared 3.7925,
    /// against the baseline's 0.765625 + 1.265625 = 2.03125: a skill of 1 -
    /// 3.7925 / 2.03125, below nought.
    fn short_showing_no_skill() -> Vec<TargetRow> {
        let mut rows = rows();
        rows[7] = row_of("2026-01-06T19:00:00Z", vec![0.8], (true, false), hour());
        rows[7].outcomes.short.unit = Money::from("4 USD");
        rows[8] = row_of("2026-01-06T20:00:00Z", vec![0.3], (false, true), hour());
        rows
    }

    /// The short fails its skill (`short_showing_no_skill`), beside the pair
    /// of the 5th, whose short, of nought, is never picked. The holdout's
    /// picks are the train's own sides': at 19:00 the fitted short, expecting
    /// 0.95R, and its loss of 2 on its stop worth 4; so -0.5R per trade, of
    /// every trade's four, where the long beside the 5th's short would have
    /// taken its win alone, 1.5R. Neither side publishes, refused for the
    /// short's skill, and the pair of the 5th stays the current one.
    #[test]
    fn a_side_refused_on_its_skill_refuses_the_pair_judged_on_the_trains_own_picks() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let fifth = "2026-01-05T00:00:00Z";
        for side in Side::BOTH {
            publish_baseline(&store(dir.path(), &config(), side), side, fifth, 0.0);
        }

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| {
            Ok(short_showing_no_skill())
        });

        for outcome in [&trained.long, &trained.short] {
            let Some(Outcome::Kept(Kept::Refused { why, holdout })) = outcome else {
                panic!("{trained:?}");
            };
            assert!(matches!(why, Refusal::Skill { .. }), "{why:?}");
            let Earnings { taken, every, .. } = holdout.earnings;
            assert_eq!((taken.count, every.count), (1, 4), "{holdout:?}");
            close_to(taken.expectancy, -0.5);
        }
        for side in Side::BOTH {
            assert_eq!(
                store(dir.path(), &config(), side).valid_froms().unwrap(),
                [at(fifth)],
                "{side:?}"
            );
        }
    }

    /// `config` trading the long from 1R and the short from 0.1R: of `rows`'s
    /// holdout, expecting 0.95R and 0.2R of either side, the short alone
    /// clears, and is picked on both rows.
    fn short_from_a_tenth() -> ModelConfig {
        ModelConfig {
            margins: Margins {
                long: Some(R::new(1.0)),
                short: Some(R::new(0.1)),
            },
            ..config()
        }
    }

    /// `rows` with the short's trade at 20:00 a win of 0.1 USD, the short
    /// picked on both rows of the holdout (`short_from_a_tenth`). Its pick at
    /// 20:00 alone would earn 0.1 USD on its stop worth 2, 0.05R, below the
    /// minimum of 0.25R, the one at 19:00 3 on its stop worth 4, 0.75R;
    /// together 0.4R per trade, above every trade's, the long's 1.5R and -1R,
    /// the short's 0.75R and 0.05R, a win rate of 3/4 of a mean win of 2.3 / 3
    /// and a loss of 1, 0.325R. Each side's skill is above nought: the
    /// short's R, 0.75 and 0.05, errs by -0.2 and -0.15 against the
    /// baseline's 0.375 and -0.325.
    #[test]
    fn the_sides_publish_together_on_what_their_picks_earned() {
        let dir = tempfile::tempdir().unwrap();
        let t = at("2026-01-07T00:00:00Z");
        let mut rows = rows();
        rows[8].outcomes.short.pnl = Money::from("0.1 USD");
        let config = short_from_a_tenth();

        let trained = training(dir.path(), config.clone()).train_at(t, |_, _| Ok(rows));

        for outcome in [&trained.long, &trained.short] {
            assert!(
                matches!(outcome, Some(Outcome::Published { valid_from, .. }) if *valid_from == t),
                "{trained:?}"
            );
        }
        for side in Side::BOTH {
            let Earnings { taken, every, .. } = store(dir.path(), &config, side)
                .read(t)
                .unwrap()
                .manifest
                .holdout
                .earnings;
            assert_eq!(taken.count, 2, "{side:?}");
            close_to(taken.expectancy, 0.4);
            close_to(every.expectancy, 0.325);
        }
    }

    /// `rows` with the long's trade at 20:00 a win, the short picked on both
    /// rows of the holdout (`short_from_a_tenth`), a win of 3 USD at 19:00 on
    /// its stop worth 4 USD, 0.75R, and a loss of 2 at 20:00 on its stop
    /// worth 2, -1R: the picks earn -0.125R per trade, below the minimum of
    /// 0.25R. The long, never picked, wins 3 USD on both. Each side's
    /// skill is above nought (the long's R, 1.5 twice, errs by 0.55 and 1.3
    /// against the baseline's 1.125 twice), and the profitable side is
    /// refused with the losing one.
    #[test]
    fn a_profitable_side_paired_with_a_losing_one_is_refused_with_it() {
        let dir = tempfile::tempdir().unwrap();
        let mut rows = rows();
        rows[8] = row_of("2026-01-06T20:00:00Z", vec![0.3], (true, false), hour());
        let config = short_from_a_tenth();

        let trained = training(dir.path(), config.clone())
            .train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows));

        for (side, outcome) in [(Side::Long, &trained.long), (Side::Short, &trained.short)] {
            let Some(Outcome::Kept(Kept::Refused { why, .. })) = outcome else {
                panic!("{trained:?}");
            };
            let Refusal::MinPnl { pnl, min_pnl } = why else {
                panic!("{why:?}");
            };
            close(*pnl, -0.125);
            assert_eq!(*min_pnl, R::new(0.25));
            assert!(
                store(dir.path(), &config, side)
                    .valid_froms()
                    .unwrap()
                    .is_empty(),
                "{side:?}"
            );
        }
    }

    /// Each published manifest holds the earnings of the pair's picks beside
    /// its side's own score, worked as in
    /// `the_sides_are_judged_on_the_trades_picked_on_the_holdout`, and the
    /// holdout's mean R - E', which its publish reports.
    #[test]
    fn both_manifests_hold_the_earnings_judged_beside_each_sides_own_score() {
        let dir = tempfile::tempdir().unwrap();
        let t = at("2026-01-07T00:00:00Z");

        let trained = training(dir.path(), config()).train_at(t, |_, _| Ok(rows()));

        for (side, outcome, skill, mean_error) in [
            (Side::Long, &trained.long, 1.0 - 1.7425 / 3.15625, -0.325),
            (Side::Short, &trained.short, 1.0 - 1.48 / 2.03125, -0.7),
        ] {
            let holdout = store(dir.path(), &config(), side)
                .read(t)
                .unwrap()
                .manifest
                .holdout;
            let Earnings { taken, every, .. } = holdout.earnings;
            assert_eq!((taken.count, every.count), (1, 4), "{side:?}");
            let score = holdout.score;
            close_to(score.skill, skill);
            close_to(score.mean_error, mean_error);
            let reported = outcome.as_ref().unwrap().to_string();
            assert!(
                reported.contains(&format!("mean R - E' {mean_error:.6}")),
                "{reported}"
            );
        }
    }

    /// `rows` with the holdout's outcomes reversed: at 19:00 at 0.8 both sides
    /// lose, the short on its stop worth 4 USD, and at 20:00 at 0.3 both win.
    /// Each fails its skill: the long's R, -1 and 1.5, errs by -1.95 and 1.3,
    /// 5.4925 against 3.15625; the short's, -0.5 and 1.5, 3.7925 against
    /// 2.03125. The pair is refused for the long's, the first, and each side
    /// reports its own skill. The pair's picks are still judged: the short at
    /// 19:00, expecting 0.95R, its loss of 2 on its stop worth 4, -0.5R,
    /// beside every trade, two wins of 1.5R and the long's loss of 1R and the
    /// short's, 0.375R per trade.
    #[test]
    fn with_both_sides_failing_their_skill_each_reports_its_own_and_the_pairs_earnings() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let mut rows = rows();
        rows[7] = row_of("2026-01-06T19:00:00Z", vec![0.8], (false, false), hour());
        rows[7].outcomes.short.unit = Money::from("4 USD");
        rows[8] = row_of("2026-01-06T20:00:00Z", vec![0.3], (true, true), hour());

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows));

        let long_skill = 1.0 - 5.4925 / 3.15625;
        for (outcome, own) in [
            (&trained.long, long_skill),
            (&trained.short, 1.0 - 3.7925 / 2.03125),
        ] {
            let Some(Outcome::Kept(Kept::Refused { why, holdout })) = outcome else {
                panic!("{trained:?}");
            };
            let Refusal::Skill { skill } = *why else {
                panic!("{why:?}");
            };
            close_to(skill, long_skill);
            close_to(holdout.score.skill, own);
            let Earnings { taken, every, .. } = holdout.earnings;
            assert_eq!((taken.count, every.count), (1, 4), "{holdout:?}");
            close_to(taken.expectancy, -0.5);
            close_to(every.expectancy, 0.375);
        }
    }

    /// The short's stop on a row fitted on is worth nought, so its R there is
    /// no finite number and its fit fails. The long, fitted, publishes
    /// neither: both are kept, the long for the short's failure, which it
    /// names, and the pair of the 6th stays the current one.
    #[test]
    fn a_side_whose_fit_fails_publishes_neither_side() {
        let dir = tempfile::tempdir().unwrap();
        let sixth = "2026-01-06T00:00:00Z";
        for side in Side::BOTH {
            publish_baseline(&store(dir.path(), &config(), side), side, sixth, 0.0);
        }
        let mut rows = rows();
        rows[1].outcomes.short.unit = Money::from("0 USD");

        let trained =
            training(dir.path(), config()).train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows));

        for outcome in [&trained.long, &trained.short] {
            let Some(Outcome::Kept(Kept::Failed(why))) = outcome else {
                panic!("{trained:?}");
            };
            assert!(why.contains("short") && why.contains("finite"), "{why}");
        }
        for side in Side::BOTH {
            assert_eq!(
                store(dir.path(), &config(), side).valid_froms().unwrap(),
                [at(sixth)],
                "{side:?}"
            );
        }
    }

    /// The short's publish fails after the long's: the long's model is in
    /// its store, valid from the train's time, but has no short beside it,
    /// so the sides list no pair from then and both are kept, failed.
    #[test]
    fn a_publish_failing_after_the_others_leaves_no_pair_published() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let t = at("2026-01-07T00:00:00Z");
        store(dir.path(), &config(), Side::Short).break_publishing();

        let trained = training.train_at(t, |_, _| Ok(rows()));

        for outcome in [&trained.long, &trained.short] {
            assert!(
                matches!(outcome, Some(Outcome::Kept(Kept::Failed(_)))),
                "{trained:?}"
            );
        }
        assert_eq!(
            store(dir.path(), &config(), Side::Long)
                .valid_froms()
                .unwrap(),
            [t]
        );
        assert_eq!(
            models(dir.path(), &config()).valid_froms().unwrap().pairs(),
            []
        );
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

    /// A one-sided strategy has no pick between sides, and the test it has
    /// always had: taking every trade of the holdout, the long's win of 3 USD
    /// and loss of 2 on stops worth 2, 1.5R and -1R, 0.25R a trade, earns no
    /// more than every trade, so a margin no trade falls short of is refused.
    #[test]
    fn a_one_sided_strategy_taking_every_trade_of_the_holdout_is_refused() {
        let takes_all = ModelConfig {
            margins: Margins {
                long: Some(R::new(-10.0)),
                short: None,
            },
            min_pnl: R::new(-10.0),
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
        close(pnl, 0.25);
        close(every, 0.25);
    }

    /// A one-sided strategy's calibration is the level line alone, on its
    /// side's prediction and R: `rows` with the short's win at 15:00 a loss,
    /// which the long's line never reads. The long's R on the calibration's
    /// rows, -0.5 and 1.5, makes the line of `LINES`, and no side call;
    /// the sides' mean R, -0.5 and 0.25, would make `alpha_C` -0.875.
    #[test]
    fn a_one_sided_strategy_calibrates_its_level_line_alone_on_its_sides_r() {
        let long_only = ModelConfig {
            margins: Margins {
                long: Some(R::new(0.5)),
                short: None,
            },
            ..config()
        };
        let dir = tempfile::tempdir().unwrap();
        let t = at("2026-01-07T00:00:00Z");
        let mut rows = rows();
        rows[5].outcomes.short.pnl = Money::from("-2 USD");

        let trained = training(dir.path(), long_only.clone()).train_at(t, |_, _| Ok(rows));

        assert!(
            matches!(trained.long, Some(Outcome::Published { .. })),
            "{trained:?}"
        );
        let calibration = store(dir.path(), &long_only, Side::Long)
            .read(t)
            .unwrap()
            .manifest
            .calibration;
        assert_eq!(
            calibration,
            ModelCalibration::Pair(PairCalibration {
                valid_from: t,
                sides: vec![Side::Long],
                lines: Ksplit {
                    side_call: None,
                    ..LINES
                },
            })
        );
    }

    /// A strategy that picks almost nothing publishes nothing: one trade
    /// picked on the holdout, fewer than two, refuses both sides, though
    /// each side's own count is never compared, and neither store holds a
    /// model.
    #[test]
    fn fewer_trades_picked_than_the_minimum_refuse_both_sides_and_publish_nothing() {
        let two = ModelConfig {
            min_trades: 2,
            ..config()
        };
        let dir = tempfile::tempdir().unwrap();

        let trained = training(dir.path(), two.clone())
            .train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows()));

        for outcome in [&trained.long, &trained.short] {
            assert!(
                matches!(
                    outcome,
                    Some(Outcome::Kept(Kept::Refused {
                        why: Refusal::Trades {
                            trades: 1,
                            min_trades: 2
                        },
                        ..
                    }))
                ),
                "{trained:?}"
            );
        }
        for side in Side::BOTH {
            assert!(
                store(dir.path(), &two, side)
                    .valid_froms()
                    .unwrap()
                    .is_empty(),
                "{side:?}"
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

    /// The long's model valid from the train's time, a baseline published by
    /// a train stopped before its short's publish, has no short beside it: no
    /// pair is valid from then, so both sides are fitted again, judged
    /// together, and published, the long's fitted model in place of the
    /// baseline.
    #[test]
    fn a_side_published_at_the_trains_time_without_the_other_is_fitted_again_with_it() {
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

        for outcome in [&trained.long, &trained.short] {
            assert!(
                matches!(outcome, Some(Outcome::Published { valid_from, .. }) if *valid_from == t),
                "{trained:?}"
            );
        }
        let long = store(dir.path(), &config(), Side::Long).read(t).unwrap();
        assert_eq!(long.manifest.learner, LearnerConfig::Fake {});
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

    /// Never retrained, the pair of the 6th, its long expecting nought and
    /// so never picked, is current at the train of the 7th: kept in use, not
    /// fitted again, and nothing replayed.
    #[test]
    fn never_retrained_a_pair_valid_before_the_train_is_kept_in_use() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), never());
        let sixth = at("2026-01-06T00:00:00Z");
        for side in Side::BOTH {
            publish_baseline_of(
                never(),
                &store(dir.path(), &never(), side),
                side,
                "2026-01-06T00:00:00Z",
                0.5,
            );
        }
        let mut replays = vec![];

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |from, to| {
            replays.push((from, to));
            Ok(rows())
        });

        let in_use = Outcome::InUse {
            valid_from: sixth,
            retrain: Retrain::Never,
        };
        assert_eq!(
            (trained.long.as_ref(), trained.short.as_ref()),
            (Some(&in_use), Some(&in_use))
        );
        assert_eq!(replays, []);
    }

    /// Never retrained, the long's model of the 6th has no short beside it,
    /// so no pair is current at the train of the 7th: both sides are fitted
    /// and published as of the 7th, and the long of the 6th stays in its
    /// store, in no pair.
    #[test]
    fn never_retrained_a_side_valid_before_the_train_without_the_other_is_fitted_again_with_it() {
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

        for outcome in [&trained.long, &trained.short] {
            assert!(
                matches!(outcome, Some(Outcome::Published { valid_from, .. }) if *valid_from == t),
                "{trained:?}"
            );
        }
        assert_eq!(
            store(dir.path(), &never(), Side::Long)
                .valid_froms()
                .unwrap(),
            [sixth, t]
        );
        assert_eq!(
            models(dir.path(), &never()).valid_froms().unwrap().pairs(),
            [t]
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
            blueprint(config())
                .fingerprint(&venue())
                .unwrap()
                .to_string()
        );
        assert_eq!(manifest.target, config().target);
        assert_eq!(manifest.learner, LearnerConfig::Fake {});
    }

    /// Each side's baseline is the mean of its R on the rows it was fitted
    /// and calibrated on, 1.5, -1, -0.5 and 1.5 of `rows`, 0.375; not on the
    /// rows outside those (the win at 23:00 of the 5th, before the window, the
    /// wins at 11:30 and 17:00, settled after their part's end), nor on the
    /// holdout's, with which the long's would be 1/3.
    #[test]
    fn a_published_sides_baseline_is_the_training_mean_of_the_rows_it_was_fitted_and_calibrated_on()
    {
        let dir = tempfile::tempdir().unwrap();
        let t = at("2026-01-07T00:00:00Z");

        let trained = training(dir.path(), config()).train_at(t, |_, _| Ok(rows()));

        assert!(
            matches!(trained.long, Some(Outcome::Published { .. })),
            "{trained:?}"
        );
        for side in Side::BOTH {
            let stored = store(dir.path(), &config(), side).read(t).unwrap();
            assert_eq!(stored.manifest.baseline, 0.375, "{side:?}");
        }
    }

    /// Margins of the long alone: no short model is trained nor stored.
    #[test]
    fn a_long_only_model_trains_no_short() {
        let dir = tempfile::tempdir().unwrap();
        let long_only = ModelConfig {
            margins: Margins {
                long: Some(R::new(0.5)),
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

    /// The short fails its skill (`short_showing_no_skill`) and the long's,
    /// as `the_sides_are_judged_on_the_trades_picked_on_the_holdout` works
    /// it, is above nought. Each side reports the short's skill as the pair's
    /// refusal, beside its own holdout: its own skill, and the earnings of the
    /// pair's picks, the short's loss of 2 USD at 19:00 on its stop worth 4,
    /// -0.5R, beside every trade of both sides, the long's 1.5R and -1R and
    /// the short's -0.5R and 1.5R, 0.375R per trade. Neither publishes.
    #[test]
    fn a_side_refused_on_its_skill_refuses_the_other_reporting_it_beside_its_own_holdout() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| {
            Ok(short_showing_no_skill())
        });

        let short_skill = 1.0 - 3.7925 / 2.03125;
        for (side, outcome, own) in [
            (Side::Long, &trained.long, 1.0 - 1.7425 / 3.15625),
            (Side::Short, &trained.short, short_skill),
        ] {
            let Some(Outcome::Kept(Kept::Refused { why, holdout })) = outcome else {
                panic!("{trained:?}");
            };
            let Refusal::Skill { skill } = *why else {
                panic!("{why:?}");
            };
            close_to(skill, short_skill);
            close_to(holdout.score.skill, own);
            let Earnings { taken, every, .. } = holdout.earnings;
            assert_eq!((taken.count, every.count), (1, 4), "{holdout:?}");
            close_to(taken.expectancy, -0.5);
            close_to(every.expectancy, 0.375);
            assert!(
                store(dir.path(), &config(), side)
                    .valid_froms()
                    .unwrap()
                    .is_empty(),
                "{side:?}"
            );
        }
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
    /// is a win at 0.8, 3 USD on its stop worth 2, 1.5R, taken: no
    /// calibration slope on one row, and no more earned than taking every
    /// trade.
    #[test]
    fn a_holdout_of_one_row_has_no_slope_and_is_judged_on_its_trades() {
        let dir = tempfile::tempdir().unwrap();
        let long_only = ModelConfig {
            margins: Margins {
                long: Some(R::new(0.5)),
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
        assert_eq!(holdout.score.slope, None);
        assert_eq!(
            why,
            Refusal::Every {
                pnl: 1.5,
                every: 1.5
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

    /// A side's holdout taking 3 trades of 1.25R each, besides 8 of 0.4R; at
    /// 1.5 times the commission 1.125R and 0.3R; its skill 0.61, its mean
    /// R - E' -0.69, its calibration slope 0.9 and its top-decile gap 0.625.
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
            score: ExpectedScore {
                rows: 8,
                skill: Some(0.61),
                mean_error: Some(-0.69),
                slope: Some(0.9),
                top_decile_gap: Some(0.625),
            },
        }
    }

    /// What a train reports of a side's holdout, published or not: its
    /// trades, net P&L per trade taken and per trade of every trade, in R, at the
    /// commission and at 1.5 times it, its skill, its mean R - E', its
    /// calibration slope and its top-decile gap.
    #[test]
    fn an_outcome_reads_as_published_or_kept_with_its_reason_and_its_holdout() {
        let published = Outcome::Published {
            valid_from: at("2026-01-07T00:00:00Z"),
            holdout: Box::new(holdout()),
        };
        let below = Outcome::Kept(Kept::Refused {
            why: Refusal::MinPnl {
                pnl: 1.25,
                min_pnl: R::new(1.5),
            },
            holdout: Box::new(holdout()),
        });

        assert!(published.to_string().contains("published"), "{published}");
        assert!(below.to_string().contains("kept"), "{below}");
        assert!(
            below.to_string().contains("below the minimum 1.5R"),
            "{below}"
        );
        for text in [published.to_string(), below.to_string()] {
            let written = figures(&text);
            for figure in [3.0, 1.25, 8.0, 0.4, 1.125, 0.3, 0.61, 0.69, 0.9, 0.625] {
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
    /// window of `window` hours, the last 6 held out, taking a trade from an
    /// expected 0.5R, published from 0R per trade over a trade; each side
    /// traded from 0.5R, which the sides of `publish_baselines` clear, each
    /// expecting 0.6R, the other -0.6R.
    fn with_meta(window: i64) -> ModelConfig {
        ModelConfig {
            margins: Margins {
                long: Some(R::new(0.5)),
                short: Some(R::new(0.5)),
            },
            meta: Some(meta(window)),
            ..config()
        }
    }

    fn meta(window: i64) -> MetaConfig {
        MetaConfig::builder()
            .window(SignedDuration::from_hours(window))
            .holdout(SignedDuration::from_hours(6))
            .margin(R::new(0.5))
            .min_pnl(R::new(0.0))
            .min_trades(1)
            .learner(LearnerConfig::Fake {})
            .build()
    }

    /// Publishes in `store` a baseline of `side`, of `config()`, valid from
    /// `valid_from`, predicting an R of `r` on every row, as
    /// `publish_baseline_of`.
    fn publish_baseline(store: &ModelStore, side: Side, valid_from: &str, r: f64) {
        publish_baseline_of(config(), store, side, valid_from, r);
    }

    /// Publishes in `store` a baseline of `side`, of `config`, valid from
    /// `valid_from`, predicting an R of `r` on every row, its pair's
    /// calibration leaving it as it is (`unchanged`), its baseline that R.
    fn publish_baseline_of(
        config: ModelConfig,
        store: &ModelStore,
        side: Side,
        valid_from: &str,
        r: f64,
    ) {
        let names = side_names(features().names(), &config.target.payoff_names());
        let set = TrainingSet::new(names.len(), vec![0.5; names.len()], vec![r]).unwrap();
        let model = LearnerConfig::Baseline {}.fit(&set).unwrap();
        store
            .publish(&StoredModel {
                manifest: ModelManifest {
                    strategy_id: StrategyId::from(STRATEGY),
                    valid_from: at(valid_from),
                    fingerprint: blueprint(config.clone())
                        .fingerprint(&venue())
                        .unwrap()
                        .to_string(),
                    feature_names: names,
                    calibration: unchanged(&config, at(valid_from)),
                    target: config.target,
                    side: Some(side),
                    learner: LearnerConfig::Baseline {},
                    baseline: r,
                    holdout: holdout(),
                    probes: vec![],
                },
                model: model.save().unwrap(),
            })
            .unwrap();
    }

    /// The decisions of the 6th, the short's trade the reverse of the long's,
    /// each side's stop worth 2 USD, winning 3 USD (1.5R) or losing 2 (-1R):
    /// one before the sides' first models (a win of the long), four long
    /// trades of the models valid from 06:00, the long's predicting 0.6R and
    /// the short's -0.6R: fitted on, a win and a loss of the long; calibrated
    /// on, the meta's fake predicting their first feature, a loss at 0.25 and
    /// a win at 0.75. Two short trades of the models valid from 17:00, the
    /// long's -0.6R and the short's 0.6R, held out: a win and a loss of the
    /// long (the short's -1R and 1.5R), the meta's fake predicting 0.3 and
    /// 0.9. Each decision's level is nought and its side call 1.2 or -1.2.
    fn meta_rows() -> Vec<TargetRow> {
        vec![
            row("2026-01-06T05:00:00Z", vec![0.5], true, hour()),
            row("2026-01-06T08:00:00Z", vec![0.5], true, hour()),
            row("2026-01-06T10:00:00Z", vec![0.5], false, hour()),
            row("2026-01-06T13:00:00Z", vec![0.25], false, hour()),
            row("2026-01-06T15:00:00Z", vec![0.75], true, hour()),
            row("2026-01-06T19:00:00Z", vec![0.3], true, hour()),
            row("2026-01-06T20:00:00Z", vec![0.9], false, hour()),
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
        for (valid_from, long_r) in [
            ("2026-01-06T06:00:00Z", 0.6),
            ("2026-01-06T17:00:00Z", -0.6),
            ("2026-01-06T21:00:00Z", 0.8),
        ] {
            publish_baseline_of(config.clone(), &long, Side::Long, valid_from, long_r);
            publish_baseline_of(config.clone(), &short, Side::Short, valid_from, -long_r);
        }
    }

    /// The meta's calibration on `meta_rows`, worked by hand: its fake
    /// predicting 0.25 and 0.75 on the calibration's rows, against R of -1
    /// and 1.5, means 0.5 and 0.25, a slope of 0.625 / 0.125 = 5, clipped to
    /// 1.5, and `alpha` 0.25 - 1.5 x 0.5 = -0.5: the meta expects `-0.5 +
    /// 1.5p` on the feature `p`, -0.05R at 0.3 and 0.85R at 0.9. Fitted on
    /// the holdout's rows, `alpha` would be 0.25 - 1.5 x 0.6 = -0.65.
    const META_LINE: Line = Line {
        alpha: -0.5,
        beta: 1.5,
    };

    /// Worked by hand on `meta_rows`: the meta fitted on the R of the side
    /// picked of the rows before its calibration, the long's 1.5 and -1, of
    /// a mean of 0.25, which the fake predicts where its feature is missing,
    /// the decision before the sides' first model no row; calibrated by its
    /// level line on the calibration's rows (`META_LINE`); its probes the
    /// holdout's shorts and its predictions on them, before the calibration.
    #[test]
    fn a_meta_model_learns_the_r_of_the_side_picked_from_its_out_of_sample_decisions() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), with_meta(24));
        publish_baselines(dir.path(), &with_meta(24));
        let t = at("2026-01-07T00:00:00Z");

        let trained = training.train_at(t, |_, _| Ok(meta_rows()));

        assert!(
            matches!(trained.meta, Some(Outcome::Published { valid_from, .. }) if valid_from == t),
            "{trained:?}"
        );
        let stored = meta_store(dir.path(), &with_meta(24)).read(t).unwrap();
        assert_eq!(
            stored.manifest.feature_names,
            ["mid_0", "picked_side", "level", "side_call"]
        );
        assert_eq!(stored.manifest.side, None);
        assert_eq!(
            stored.manifest.calibration,
            ModelCalibration::Meta(META_LINE)
        );
        let probes: Vec<(&Vec<f64>, f64)> = stored
            .manifest
            .probes
            .iter()
            .map(|p| (&p.row, p.prediction))
            .collect();
        assert_eq!(
            probes,
            [
                (&vec![0.3, -1.0, 0.0, -1.2], 0.3),
                (&vec![0.9, -1.0, 0.0, -1.2], 0.9)
            ]
        );
        let model = LearnerConfig::Fake {}.load(&stored.model).unwrap();
        assert_eq!(model.predict(&[f64::NAN, 1.0, 0.0, 1.2]).unwrap(), 0.25);
    }

    /// Worked by hand on `meta_rows`'s holdout, the meta expecting -0.05R and
    /// 0.85R (`META_LINE`) of the shorts at 19:00 and 20:00, each on a stop
    /// worth 2 USD, their R -1 and 1.5, its baseline the mean R of the rows
    /// fitted and calibrated on, 0.25. Errors -0.95 and 0.65, squared 1.325,
    /// the baseline's 1.5625 twice, 3.125: a skill of 1 - 1.325 / 3.125; a
    /// mean R - E' of -0.15; deviations from the means 0.4 and 0.25 of -0.45
    /// and 0.45 against -1.25 and 1.25, a slope of 1.125 / 0.405; the top
    /// decile, the row expected 0.85R, a gap of 0.65. Its margin of 0.5R
    /// takes the trade at 20:00, expected 0.85R, a win of 3 USD, 1.5R, and
    /// leaves the one at 19:00, expected -0.05R, a loss of 1R: one trade of
    /// 1.5R beside every trade's 0.25R, and at 1.5 times the commission of
    /// 0.2 each makes 0.1 USD less, 0.05R.
    #[test]
    fn a_meta_model_is_judged_by_its_skill_and_the_trades_its_margin_takes() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), with_meta(24));
        publish_baselines(dir.path(), &with_meta(24));

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(meta_rows()));

        let holdout = holdout_of(trained.meta);
        let Earnings {
            taken,
            every,
            taken_stressed,
            every_stressed,
        } = holdout.earnings;
        assert_eq!((taken.count, every.count), (1, 2), "{holdout:?}");
        close_to(taken.expectancy, 1.5);
        close_to(every.expectancy, 0.25);
        close_to(taken_stressed.expectancy, 1.45);
        close_to(every_stressed.expectancy, 0.2);
        let score = holdout.score;
        assert_eq!(score.rows, 2);
        close_to(score.skill, 1.0 - 1.325 / 3.125);
        close_to(score.mean_error, -0.15);
        close_to(score.slope, 1.125 / 0.405);
        close_to(score.top_decile_gap, 0.65);
    }

    /// The meta's margin is in R: one of 0.9R leaves the trade at 20:00 of
    /// `meta_rows`'s holdout, expected 0.85R, though on its stop worth 2 USD
    /// it expects 1.7 USD, and the one at 19:00, so no trade is taken.
    #[test]
    fn a_meta_models_margin_in_r_leaves_a_trade_expected_less_whatever_its_unit() {
        let dir = tempfile::tempdir().unwrap();
        let config = ModelConfig {
            meta: Some(MetaConfig {
                margin: R::new(0.9),
                ..meta(24)
            }),
            ..with_meta(24)
        };
        let training = training(dir.path(), config.clone());
        publish_baselines(dir.path(), &config);

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(meta_rows()));

        assert_eq!(holdout_of(trained.meta).earnings.taken.count, 0);
    }

    /// `meta_rows` with the holdout's features swapped: the meta expects
    /// 0.85R of the short at 19:00, which loses 1R, and -0.05R of the one at
    /// 20:00, which gains 1.5R, errors of -1.85 and 1.55, squared 5.825
    /// against the baseline's 3.125: a skill of 1 - 5.825 / 3.125, refused
    /// whatever its trades earned, and no meta model published.
    #[test]
    fn a_meta_model_whose_skill_is_not_above_nought_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), with_meta(24));
        publish_baselines(dir.path(), &with_meta(24));
        let t = at("2026-01-07T00:00:00Z");
        let mut rows = meta_rows();
        rows[5].decision.features = vec![0.9];
        rows[6].decision.features = vec![0.3];

        let trained = training.train_at(t, |_, _| Ok(rows));

        let Some(Outcome::Kept(Kept::Refused {
            why: Refusal::Skill { skill },
            ..
        })) = trained.meta
        else {
            panic!("{trained:?}");
        };
        close_to(skill, 1.0 - 5.825 / 3.125);
        assert!(
            meta_store(dir.path(), &with_meta(24))
                .valid_froms()
                .unwrap()
                .is_empty()
        );
    }

    /// A meta of a skill above nought whose margin takes one trade of
    /// `meta_rows`'s holdout (`a_meta_model_is_judged_by_its_skill_and_the_trades_its_margin_takes`)
    /// is refused under a minimum of two trades.
    #[test]
    fn a_meta_model_whose_trades_taken_miss_the_minimums_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let two = ModelConfig {
            meta: Some(MetaConfig {
                min_trades: 2,
                ..meta(24)
            }),
            ..with_meta(24)
        };
        let training = training(dir.path(), two.clone());
        publish_baselines(dir.path(), &two);

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(meta_rows()));

        assert!(
            matches!(
                trained.meta,
                Some(Outcome::Kept(Kept::Refused {
                    why: Refusal::Trades {
                        trades: 1,
                        min_trades: 2
                    },
                    ..
                }))
            ),
            "{trained:?}"
        );
    }

    /// `meta_rows` with the long's loss at 10:00 a win: the meta, on the
    /// longs of the sides' models valid from 06:00, is fitted on the R of the
    /// wins at 08:00 and 10:00, 1.5 each, and calibrated on the loss at 13:00
    /// and the win at 15:00, -1 and 1.5, a mean of 0.875; not on the decision
    /// at 05:00, before the sides' first model, a win, nor on the holdout's
    /// shorts, a loss and a win.
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
        assert_eq!(stored.manifest.baseline, 0.875);
    }

    /// `rows` as a time-only target has them: closed at their time, each
    /// side's payoff features its entry cost alone, their unit the worth of
    /// its ATR, here 1 USD.
    fn at_their_time(rows: Vec<TargetRow>) -> Vec<TargetRow> {
        let at_its_time = |outcome| TargetOutcome {
            exit: Exit::Time,
            unit: Money::from("1 USD"),
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

    /// A meta model learns the R of the trade of the side picked, on a target
    /// with no exits too. `meta_rows` with the long's loss at 10:00 a win,
    /// each closed at its time, of a unit of 1 USD, a win 3R and a loss -2R:
    /// the long picked at each of the four morning rows expects 0.6R, above
    /// its margin of 0.5R; so the meta is fitted on the wins at 08:00 and
    /// 10:00 and calibrated on the loss at 13:00 and the win at 15:00, a mean
    /// R of 1.75. Its calibration, a slope of 1.25 / 0.125 clipped to 1.5, and
    /// `alpha` 0.5 - 0.75, expects 1.1R of the short at 20:00, which its
    /// margin of 0.5R takes, a win of 3, and 0.2R of the one at 19:00, which
    /// it leaves.
    #[test]
    fn a_meta_model_on_a_time_only_target_learns_from_its_trades() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = with_meta(24);
        config.target.spec = Target::builder()
            .limit(SignedDuration::from_hours(1))
            .atr(crate::model::targets::DEFAULT_ATR)
            .build();
        let training = training(dir.path(), config.clone());
        publish_baselines(dir.path(), &config);
        let t = at("2026-01-07T00:00:00Z");
        let mut rows = meta_rows();
        rows[2] = row("2026-01-06T10:00:00Z", vec![0.5], true, hour());

        let trained = training.train_at(t, |_, _| Ok(at_their_time(rows)));

        let Some(Outcome::Published { holdout, .. }) = trained.meta else {
            panic!("{trained:?}");
        };
        assert_eq!(holdout.earnings.taken.count, 1, "{holdout:?}");
        close_to(holdout.earnings.taken.expectancy, 3.0);
        let manifest = meta_store(dir.path(), &config).read(t).unwrap().manifest;
        assert_eq!(manifest.baseline, 1.75);
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
                row.outcomes.short.unit = Money::from("4 USD");
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
            .map(|example| (example.features[1], example.trade.unit))
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
    /// the meta's rows are the long's trades, the holdout's a loss at 0.3 and
    /// a win at 0.7, which the meta, taking the win, expected 0.55R, publishes
    /// on, each of the level of the long of 17:00, 0.2R, and no side call.
    #[test]
    fn a_long_only_strategy_gives_its_meta_model_buys_alone() {
        let dir = tempfile::tempdir().unwrap();
        let long_only = ModelConfig {
            margins: Margins {
                long: Some(R::new(0.0)),
                short: None,
            },
            ..with_meta(24)
        };
        let training = training(dir.path(), long_only.clone());
        for (valid_from, r) in [("2026-01-06T06:00:00Z", 0.6), ("2026-01-06T17:00:00Z", 0.2)] {
            publish_baseline_of(
                long_only.clone(),
                &store(dir.path(), &long_only, Side::Long),
                Side::Long,
                valid_from,
                r,
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
        let probes: Vec<&Vec<f64>> = stored.manifest.probes.iter().map(|p| &p.row).collect();
        assert_eq!(
            probes,
            [&vec![0.3, 1.0, 0.2, 0.0], &vec![0.7, 1.0, 0.2, 0.0]]
        );
    }

    /// The meta's window of 36 hours is longer than the sides' 24, and
    /// starts after the sides' first models, of the 5th.
    #[test]
    fn a_train_with_a_meta_model_replays_once_from_the_longer_windows_first_day() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), with_meta(36));
        publish_baselines(dir.path(), &with_meta(36));
        for side in Side::BOTH {
            publish_baseline_of(
                with_meta(36),
                &store(dir.path(), &with_meta(36), side),
                side,
                "2026-01-05T00:00:00Z",
                0.0,
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

    /// `with_meta` on a meta window of 24 hours, never retrained, its sides
    /// traded from `config`'s margins, so that the sides it fits on `rows`
    /// pick one trade of their holdout.
    fn never_with_meta() -> ModelConfig {
        ModelConfig {
            retrain: Retrain::Never,
            margins: config().margins,
            ..with_meta(24)
        }
    }

    /// The decisions of the 7th, of the sides as `rows` of the 6th fit them
    /// (`a_train_fits_calibrates_and_judges_each_side_on_its_part_of_the_window`):
    /// each side expecting `-0.25 + 1.5p` R on the feature `p` (`LINES`),
    /// alike, so that the short, from 0.5R, is picked alone below 1R, the
    /// long's margin; each side's stop worth 2 USD, a win 1.5R and a loss
    /// -1R. Fitted on, shorts at 0.6 (0.65R expected), a win and a loss;
    /// calibrated on, a short at 0.55 (0.575R) losing and one at 0.65
    /// (0.725R) winning, the meta's fake predicting their first feature, a
    /// slope of 0.125 / 0.005 clipped to 1.5 and `alpha` 0.25 - 1.5 x 0.6,
    /// its line `-0.65 + 1.5p`; held out, a short at 0.55 (0.575R) losing and
    /// one at 0.8 (0.95R) winning, of those levels and no side call, the meta
    /// expecting 0.175R and 0.55R, its margin of 0.5R taking the win.
    fn decisions_of_the_seventh() -> Vec<TargetRow> {
        let short = |time, p, wins| row_of(time, vec![p], (wins, wins), hour());
        vec![
            short("2026-01-07T08:00:00Z", 0.6, true),
            short("2026-01-07T10:00:00Z", 0.6, false),
            short("2026-01-07T13:00:00Z", 0.55, false),
            short("2026-01-07T15:00:00Z", 0.65, true),
            short("2026-01-07T19:00:00Z", 0.55, false),
            short("2026-01-07T20:00:00Z", 0.8, true),
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
    /// the 7th: their calibration `LINES`, one trade picked, of every trade's
    /// four.
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

        let ModelCalibration::Pair(calibration) = long.manifest.calibration else {
            panic!("{:?}", long.manifest.calibration);
        };
        assert_eq!(calibration.lines, LINES);
        let probes: Vec<&Vec<f64>> = long.manifest.probes.iter().map(|p| &p.row).collect();
        assert_eq!(
            probes,
            [&vec![0.8, 1.4, 1.1, 0.07], &vec![0.3, 1.4, 1.1, 0.07]]
        );
        let Earnings { taken, every, .. } = long.manifest.holdout.earnings;
        assert_eq!((taken.count, every.count), (1, 4));
    }

    /// The meta at the 8th learns from the decisions of the sides just
    /// published over its window, the 7th: held out, the shorts at 0.55 and
    /// 0.8, each decision's level `-0.25 + 1.5p`, 0.575 and 0.95, and no side
    /// call.
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

        let probes: Vec<&Vec<f64>> = meta.manifest.probes.iter().map(|p| &p.row).collect();
        let [first, second] = probes[..] else {
            panic!("{probes:?}");
        };
        assert_eq!((first[0], first[1], first[3]), (0.55, -1.0, 0.0));
        close(first[2], 0.575);
        assert_eq!((second[0], second[1], second[3]), (0.8, -1.0, 0.0));
        close(second[2], 0.95);
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

    /// One trade picked on the sides' holdout, below a minimum of two,
    /// neither publishes as of the 7th: the meta has no decision of the
    /// sides to learn from, and waits, with no fit attempted, as the train
    /// says.
    #[test]
    fn the_sides_refused_in_a_two_time_train_leave_the_meta_waiting() {
        let dir = tempfile::tempdir().unwrap();
        let two = ModelConfig {
            min_trades: 2,
            ..never_with_meta()
        };

        let (models, trained, _) = two_time(dir.path(), two);

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
            holdout: Box::new(holdout()),
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
                holdout: Box::new(holdout()),
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
