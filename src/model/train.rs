//! Training at a time: the one operation that turns a strategy's dataset
//! into published models, or keeps the current ones, for the binary's
//! `train`, at one time or at each retrain time of a span
//! (`Training::retrain_times`), and live retrains alike; a backtest trains
//! nothing (`walk`).
//!
//! A strategy has a model for each side it trades (the sides of its
//! margins, `ModelConfig::margins`): each a regression of its side's
//! realized net R, the trade's net P&L over its unit
//! (`holdout::Example::realized`), the long's of the long of the target,
//! the short's of its short, both on the same rows, window and calibration,
//! each reading a row's features then its own side's payoff features
//! (`dataset::DatasetRow::side`), under the names its store checks it
//! against (`store::SideLevel::feature_names`). The sides are calibrated
//! and published as a pair, or not at all: a pair is the sides one train
//! published, valid from one time (`Models::pairs`), and a one-sided
//! strategy's is its one side. A train never publishes a side beside
//! another side's older model.
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
//! construction. The window ends in the calibration slice
//! (`ModelConfig::calibration`), `C` long, and the fit slice is the rest
//! before it, each slice's rows purged the same way (`split`): each side's
//! model is fitted on the rows of `[t' - W, t' - C)` settled by `t' - C`,
//! and the pair is calibrated together (`calibration::Ksplit`) on both
//! sides' predictions and R on the rows of `[t' - C, t')` settled by `t'`,
//! which no model was fitted on.
//!
//! No figure of a slice judges the models: there is no holdout and no gate.
//! A train fails, publishing none of the pair, when a slice has fewer rows
//! than the level's minimum (`ModelConfig::min_rows`), naming the slice, its
//! count and the minimum; when a model's prediction on a calibration row,
//! or the calibration, is no finite number; or when a model reloaded from
//! what it saved does not predict its probes as fitted, the check a reader
//! makes before use (`store::StoredModel`). Out-of-sample evidence is a
//! backtest's (`walk::WalkForward::scores`). Otherwise the sides publish
//! together, in their sides' stores (`store`), valid from `t'`, each with
//! the pair's calibration naming the pair, its baseline, the training mean
//! of R on the rows it was fitted and calibrated on, which its out-of-sample
//! scores are read beside, and its manifest probing it on rows of the
//! calibration slice. A publish is a rename per side, the long first: one
//! failing keeps every side, failed, and the sides published before it, as
//! those a crash between the renames leaves, have no other side beside them
//! at their valid-from, so are in no pair: no decision uses them, and the
//! next train fits the pair again, its publishes replacing them.
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
//! is a regression of the realized net R of the trade of the side picked,
//! split as the sides are on its own calibration slice
//! (`MetaConfig::calibration`), calibrated by its level line alone
//! (`calibration::Ksplit` of one side), held to its own minimum of rows
//! (`MetaConfig::min_rows`) and checked as a side is, and published in its
//! own store.
//!
//! A train never errors: what goes wrong is an outcome, the current model
//! kept with the reason (`Kept`), for each model trained. It succeeds when
//! it kept no model (`Trained::succeeded`).

use std::fmt::Display;

use anyhow::Context;
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::StrategyId;

use super::{
    blueprint::Stored,
    calibration::Ksplit,
    dataset::{Dataset, DatasetRow, SchemaMismatch},
    holdout::Example,
    learner::{LearnerConfig, SavedModel, TrainingSet},
    meta,
    schedule::{Models, Retrain, Schedule, Step},
    store::{
        Baselined, MetaLevel, ModelCalibration, ModelManifest, ModelSet, ModelStore,
        PairCalibration, Probe, SideLevel, SideModel, StoredModel,
    },
    targets::{Side, TargetConfig, TargetRow},
};

/// The most rows of the calibration slice a published model is probed on.
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
    /// The last part of its window, calibrated on and not fitted on.
    calibration: SignedDuration,
    /// The fewest rows of its fit slice and of its calibration slice.
    min_rows: usize,
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
    /// Rows of its calibration slice and its predictions on them, before
    /// calibration.
    probes: Vec<Probe>,
}

/// A side's model fitted as of a time on the fit slice, not yet calibrated
/// with the other sides of its pair.
#[derive(Debug)]
struct SideFit<'a> {
    side: Side,
    level: &'a Level,
    /// The model, its baseline the training mean of R on the rows it was
    /// fitted and calibrated on.
    model: SideModel,
    saved: SavedModel,
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
    /// A model was published, valid from `valid_from`.
    Published { valid_from: Timestamp },
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
    /// The replay, the dataset, the fit, a check of the models or the store
    /// failed, or a slice had fewer rows than the minimum; the error. A
    /// side's is its pair's, its own or another side's.
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
    pub(super) fn new(model: Stored<'_>) -> Self {
        let Stored {
            blueprint,
            dataset,
            models,
            schedule,
        } = model;
        let config = blueprint.config();
        let side = |side| {
            models.side(side).map(|traded| Level {
                feature_names: traded.feature_names().to_vec(),
                calibration: config.calibration,
                min_rows: config.min_rows,
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

    /// The retrain times of a train over the span `[from, to)`, in time
    /// order (`Schedule::retrain_times`), each trained at as `train_at`
    /// trains, after those before it: each planned on the store they left.
    ///
    /// # Errors
    ///
    /// Returns an error if the span is empty or the strategy is never
    /// retrained, trained at one time alone.
    pub fn retrain_times(&self, from: Timestamp, to: Timestamp) -> anyhow::Result<Vec<Timestamp>> {
        self.schedule.retrain_times(from, to)
    }

    /// Trains at `at` as the schedule plans it (`Schedule::plan`), from
    /// the stores' valid-froms: `replay` runs the strategy over a span of
    /// the days the dataset lacks and returns the rows it settled (see
    /// `Dataset::settled`), once, from where the plan's fits start, and not
    /// at all if it fits none. Fits the sides' pair if the plan fits it, as
    /// of its time (`sides`), then the meta model as the plan has it once
    /// the sides are published (`Plan::meta_after_sides`), its rows the
    /// sides' decisions by the store as the sides left it: each fitted is
    /// published, or its current model kept with the reason; a model the
    /// plan keeps is in use, and a meta the plan makes wait is kept,
    /// waiting.
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
            let step = match self.models.valid_froms() {
                Ok(valid_froms) => plan.meta_after_sides(&valid_froms)?,
                Err(e) => return Some(kept(&e)),
            };
            Some(match self.planned(step, &rows) {
                Planned::Done(outcome) => outcome,
                Planned::Fit { at, rows } => {
                    self.fit_meta(level, at, &rows).unwrap_or_else(|e| kept(&e))
                }
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
    /// step being their pair's (`Schedule::plan`), the pair published or
    /// failed together (`pair`).
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

    /// The outcome of each side of the pair fitted as of `at` on `rows`,
    /// split into the fit slice and the calibration slice (`split`), each
    /// of at least the minimum of rows (`Level::ensure_rows`): each side's
    /// model fitted on its realized R on the fit slice (`fit_side`), the
    /// pair calibrated together on the calibration slice (`Ksplit::fit`),
    /// every prediction there and the calibration finite, and each side
    /// checked as a reader checks it (`Training::stored`) before any
    /// publishes (`publish_pair`).
    ///
    /// # Errors
    ///
    /// Returns an error if a slice has fewer rows than the minimum, a side's
    /// fit fails, naming the side, a model fails to predict or predicts no
    /// finite number, the calibration is no finite line, or a side fails its
    /// check.
    fn pair(&self, at: Timestamp, rows: Vec<&DatasetRow>) -> anyhow::Result<Vec<(Side, Outcome)>> {
        let levels: Vec<(Side, &Level)> = self
            .models
            .sides()
            .filter_map(|traded| {
                let side = traded.side();
                self.levels.side(side).map(|level| (side, level))
            })
            .collect();
        let cut = at - levels[0].1.calibration;
        let (fitted, calibrating) = split(rows, cut, |row| (row.ts_event, row.settled_at));
        levels[0]
            .1
            .ensure_rows((cut, at), fitted.len(), calibrating.len())?;
        let fits = levels
            .iter()
            .map(|&(side, level)| {
                fit_side(side, level, &fitted, &calibrating)
                    .with_context(|| format!("fitting the {} model", side.name()))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let predicted = calibrating
            .iter()
            .map(|row| {
                fits.iter()
                    .map(|fit| {
                        let example = Example::of_side(row, fit.side);
                        let prediction = finite_prediction(
                            fit.model.predict(&example.features)?,
                            fit.side.name(),
                            row.ts_event,
                        )?;
                        Ok((prediction, example.realized()))
                    })
                    .collect()
            })
            .collect::<anyhow::Result<Vec<Vec<(f64, f64)>>>>()?;
        let lines =
            Ksplit::fit(&predicted).context("calibrating the pair on the calibration slice")?;
        anyhow::ensure!(
            [
                lines.level.alpha,
                lines.level.beta,
                lines.side_call.unwrap_or(0.0)
            ]
            .iter()
            .all(|value| value.is_finite()),
            "the pair's calibration {lines:?} is not finite"
        );
        let calibration = ModelCalibration::Pair(PairCalibration {
            valid_from: at,
            sides: fits.iter().map(|fit| fit.side).collect(),
            lines,
        });
        let stored = fits
            .into_iter()
            .enumerate()
            .map(|(i, fit)| {
                let probes = spread(calibrating.len())
                    .map(|row| Probe {
                        row: calibrating[row].side(fit.side).features,
                        prediction: predicted[row][i].0,
                    })
                    .collect();
                let fitted = Fitted {
                    side: Some(fit.side),
                    calibration: calibration.clone(),
                    baseline: fit.model.baseline(),
                    saved: fit.saved,
                    probes,
                };
                Ok((fit.side, fit.level, self.stored(fit.level, at, fitted)?))
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(self.publish_pair(at, &stored))
    }

    /// Publishes each side of the pair `stored`, the long first, valid from
    /// `at`: each published, or, should a publish fail, each kept with its
    /// error, the sides published before it no pair without the others.
    fn publish_pair(
        &self,
        at: Timestamp,
        stored: &[(Side, &Level, StoredModel)],
    ) -> Vec<(Side, Outcome)> {
        let sides: Vec<Side> = stored.iter().map(|&(side, ..)| side).collect();
        for (_, level, model) in stored {
            if let Err(e) = level.store.publish(model) {
                return sides.into_iter().map(|side| (side, kept(&e))).collect();
            }
        }
        sides
            .into_iter()
            .map(|side| (side, Outcome::Published { valid_from: at }))
            .collect()
    }

    /// Fits the meta model of `level` as of `at` on the examples of `rows`,
    /// in time order (`Level::fit`), checks it as a reader checks it
    /// (`Training::stored`), and publishes it.
    ///
    /// # Errors
    ///
    /// Returns an error if the examples cannot be made, the fit or its check
    /// fails, or the store fails.
    fn fit_meta(
        &self,
        level: &Level,
        at: Timestamp,
        rows: &[&DatasetRow],
    ) -> anyhow::Result<Outcome> {
        let examples = meta::examples(&self.models, rows)?;
        let stored = self.stored(level, at, level.fit(at, examples)?)?;
        level.store.publish(&stored)?;
        Ok(Outcome::Published { valid_from: at })
    }

    /// The model `fitted` of `level` as its store holds it, valid from `at`,
    /// checked as a reader checks it before use (`StoredModel::checked`):
    /// reloaded from what it saved, it predicts each probe to the bit as
    /// fitted.
    ///
    /// # Errors
    ///
    /// Returns an error if the model fails its check.
    fn stored(&self, level: &Level, at: Timestamp, fitted: Fitted) -> anyhow::Result<StoredModel> {
        let fingerprint = self.dataset.fingerprint().to_string();
        let stored = StoredModel {
            manifest: ModelManifest {
                strategy_id: self.strategy_id,
                valid_from: at,
                fingerprint: fingerprint.clone(),
                feature_names: level.feature_names.clone(),
                target: self.target.clone(),
                side: fitted.side,
                learner: level.learner.clone(),
                calibration: fitted.calibration,
                baseline: fitted.baseline,
                probes: fitted.probes,
            },
            model: fitted.saved,
        };
        stored
            .checked(&level.feature_names, &fingerprint)
            .context("checking the model fitted")?;
        Ok(stored)
    }
}

impl Level {
    /// The level of `meta`: it learns the realized R of the target on the
    /// side picked.
    fn of_meta(meta: &MetaLevel) -> Self {
        let config = meta.config();
        Self {
            feature_names: meta.feature_names().to_vec(),
            calibration: config.calibration,
            min_rows: config.min_rows,
            learner: config.learner.clone(),
            store: meta.store().clone(),
        }
    }

    /// Refuses a fit slice of `fitted` rows, before the calibration slice of
    /// `(cut, at)`, or a calibration slice of `calibrating` rows, fewer than
    /// the level's minimum: naming the slice, its count and the minimum,
    /// the fit slice first.
    ///
    /// # Errors
    ///
    /// Returns an error if a slice has fewer rows than the minimum.
    fn ensure_rows(
        &self,
        (cut, at): (Timestamp, Timestamp),
        fitted: usize,
        calibrating: usize,
    ) -> anyhow::Result<()> {
        let min_rows = self.min_rows;
        anyhow::ensure!(
            fitted >= min_rows,
            "{fitted} rows in the fit slice before {cut}, fewer than min_rows {min_rows}"
        );
        anyhow::ensure!(
            calibrating >= min_rows,
            "{calibrating} rows in the calibration slice from {cut} to {at}, fewer than \
             min_rows {min_rows}"
        );
        Ok(())
    }

    /// The meta model fitted as of `at` on `examples`, in time order, a
    /// regression of each trade's realized R on its fit slice (`split`), of
    /// at least the minimum of rows as its calibration slice is
    /// (`ensure_rows`), calibrated by its level line alone (`Ksplit` of one
    /// side) on its calibration slice, every prediction there and the line
    /// finite, its baseline the training mean of R on both slices.
    ///
    /// # Errors
    ///
    /// Returns an error if a slice has fewer rows than the minimum, a label
    /// is no finite number, the learner fails, a prediction on the
    /// calibration slice is no finite number, or the line is not finite.
    fn fit(&self, at: Timestamp, examples: Vec<Example>) -> anyhow::Result<Fitted> {
        let cut = at - self.calibration;
        let (fitted, calibrating) = split(examples, cut, |row| (row.ts_event, row.settled_at));
        self.ensure_rows((cut, at), fitted.len(), calibrating.len())?;
        let set = |rows: &[&Example]| {
            TrainingSet::new(
                self.feature_names.len(),
                rows.iter().flat_map(|row| row.features.clone()).collect(),
                rows.iter().map(|row| row.realized()).collect(),
            )
        };
        let model = self
            .learner
            .fit(&set(&fitted.iter().collect::<Vec<_>>()).context("fitting on the fit slice")?)?;
        let predictions = calibrating
            .iter()
            .map(|row| finite_prediction(model.predict(&row.features)?, "meta", row.ts_event))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let line = Ksplit::fit(
            &predictions
                .iter()
                .zip(&calibrating)
                .map(|(&prediction, row)| vec![(prediction, row.realized())])
                .collect::<Vec<_>>(),
        )
        .context("calibrating on the calibration slice")?
        .level;
        anyhow::ensure!(
            line.alpha.is_finite() && line.beta.is_finite(),
            "the meta's calibration {line:?} is not finite"
        );
        let trained: Vec<&Example> = fitted.iter().chain(&calibrating).collect();
        let baseline = baseline(&set(&trained)?, &trained[0].features)?;
        let probes = spread(calibrating.len())
            .map(|i| Probe {
                row: calibrating[i].features.clone(),
                prediction: predictions[i],
            })
            .collect();
        Ok(Fitted {
            saved: model.save()?,
            side: None,
            calibration: ModelCalibration::Meta(line),
            baseline,
            probes,
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

/// `prediction`, the `model`'s on the calibration row of `ts_event`, if a
/// finite number.
///
/// # Errors
///
/// Returns an error if it is not.
fn finite_prediction(prediction: f64, model: &str, ts_event: UnixNanos) -> anyhow::Result<f64> {
    anyhow::ensure!(
        prediction.is_finite(),
        "the {model} model predicts {prediction} on the calibration row of {}",
        ts_event.to_rfc3339()
    );
    Ok(prediction)
}

/// The model of `side` of `level`, fitted on the realized R of the side's
/// trades of the rows `fitted`, its baseline the training mean of R on
/// those and the rows `calibrating`, not calibrated yet.
///
/// # Errors
///
/// Returns an error if a label is no finite number, or the learner fails.
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
            rows.iter().map(Example::realized).collect(),
        )
    };
    let fitting: Vec<Example> = fitted
        .iter()
        .map(|row| Example::of_side(row, side))
        .collect();
    let model = level
        .learner
        .fit(&set(&fitting).context("fitting on the fit slice")?)?;
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

/// `items`, each settled by the train, split at the calibration slice's
/// start `cut`, each item's decision time and settlement read by `of`:
/// those before it settled by it, the fit slice; and those from it, the
/// calibration slice. An item before `cut` settling after it is in
/// neither.
fn split<T>(
    items: Vec<T>,
    cut: Timestamp,
    of: impl Fn(&T) -> (UnixNanos, UnixNanos),
) -> (Vec<T>, Vec<T>) {
    let cut = UnixNanos::from(cut);
    let (mut fitted, mut calibrating) = (vec![], vec![]);
    for item in items {
        let (ts_event, settled_at) = of(&item);
        if ts_event >= cut {
            calibrating.push(item);
        } else if settled_at <= cut {
            fitted.push(item);
        }
    }
    (fitted, calibrating)
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
impl Display for Trained {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

impl Display for Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Published { valid_from } => {
                write!(f, "published a model valid from {valid_from}")
            }
            Self::InUse {
                valid_from,
                retrain,
            } => write!(f, "{}", retrain.in_use(*valid_from)),
            Self::Kept(kept) => write!(f, "kept the current model: {kept}"),
        }
    }
}

impl Display for Kept {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
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
    use nautilus_model::{data::NautilusDataType, identifiers::InstrumentId, types::Money};
    use rstest::rstest;
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
            learner::LearnerConfig,
            targets::{
                BracketSpec, Distance, Exit, Outcome as TargetOutcome, Outcomes, PerSide, Target,
                TargetRow,
            },
        },
        strategy::{Declared, EmaArgs, IndicatorsConfig, requirements::DataRequirements, risk::R},
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
    /// 12 calibrated on, each slice of at least one row. The fake expects
    /// alike of both sides, which tie wherever both clear their margins: one
    /// side clears alone between 0.5R and 1R.
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
            .dataset(venue())
            .window(SignedDuration::from_hours(24))
            .calibration(SignedDuration::from_hours(12))
            .margins(Margins {
                long: Some(R::new(1.0)),
                short: Some(R::new(0.5)),
            })
            .min_rows(1)
            .learner(LearnerConfig::Fake {})
            .build()
    }

    /// What the strategy reads: the quotes of AUD/USD.
    fn requirements() -> DataRequirements {
        DataRequirements {
            bar_types: vec![],
            ticks: vec![crate::strategy::requirements::TickSeries {
                data: NautilusDataType::QuoteTick,
                instrument_id: InstrumentId::from(AUD),
            }],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        }
    }

    /// The venue of the dataset runs: a commission rate of 30 per million,
    /// 10 million USD.
    fn venue() -> DatasetVenue {
        DatasetVenue {
            commission: Commission::Rate(Decimal::new(3, 5)),
            balance: Money::from("10000000 USD"),
            latency: None,
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
            .training(&catalog, &dir.join("models"))
            .unwrap()
    }

    /// The models of a training in `dir` as `config` says, as `training`
    /// keys them.
    fn models(dir: &Path, config: &ModelConfig) -> ModelSet {
        blueprint(config.clone())
            .model_set(&dir.join("models"))
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
    /// window the 6th, the calibration slice from 12:00. The fit slice, at
    /// 0.9 a win (1.5R) and a loss (-1R); the calibration slice, at 0.25 a
    /// loss on stops worth 4 USD (-0.5R) and at 0.75 a win (1.5R). Around
    /// them, wins on the feature 0.1 that would change the fit or the
    /// calibration if taken: one before the window, one before the
    /// calibration slice settling after its start, one in it settling after
    /// the window's end.
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
            both("2026-01-06T23:30:00Z", 0.1, true, hour()),
        ];
        let outcomes = &mut rows[4].outcomes;
        for outcome in [&mut outcomes.long, &mut outcomes.short] {
            outcome.unit = Money::from("4 USD");
        }
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
    /// row settled after the window's end too, at 0.1 a win of 1.5R, `alpha_C`
    /// would be 0.8333 - 1.5 x 0.3667 = 0.2833. Each side's `E'` is
    /// `-0.25 + 1.5p`.
    const LINES: Ksplit = Ksplit {
        level: Line {
            alpha: -0.25,
            beta: 1.5,
        },
        side_call: Some(0.0),
    };

    /// Worked by hand on `rows`: each side fitted on the fit slice, the rows
    /// before 12:00 settled by then, a regression of their realized R, 1.5
    /// and -1, of a mean of 0.25, which the fake predicts where its feature
    /// is missing; with the row of 11:30 settled after 12:00 it would be
    /// 2/3. The pair calibrated together on the calibration slice, the rows
    /// from 12:00 settled by the train (`LINES`), each manifest holding the
    /// one calibration and naming the pair it was fitted on. Each side's
    /// probes are its rows of the calibration slice and its predictions on
    /// them, before the calibration: the fake's, their feature.
    #[rstest]
    fn a_train_fits_on_rows_settled_by_the_calibrations_start_and_calibrates_on_the_rest_settled_by_its_time()
     {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let t = at("2026-01-07T00:00:00Z");

        let trained = training.train_at(t, |_, _| Ok(rows()));

        for outcome in [&trained.long, &trained.short] {
            assert_eq!(outcome, &Some(Outcome::Published { valid_from: t }));
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
            assert_eq!(probes, [(row(0.25), 0.25), (row(0.75), 0.75)], "{side:?}");
        }
    }

    /// A calibration row whose feature is infinite: the fake predicts it,
    /// infinite, and the train fails, naming the model and the row, and
    /// publishes neither side.
    #[rstest]
    fn a_prediction_on_the_calibration_slice_that_is_no_finite_number_fails_the_train() {
        let dir = tempfile::tempdir().unwrap();
        let mut rows = rows();
        rows[5].decision.features = vec![f64::INFINITY];

        let trained =
            training(dir.path(), config()).train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows));

        for outcome in [&trained.long, &trained.short] {
            let Some(Outcome::Kept(Kept::Failed(why))) = outcome else {
                panic!("{trained:?}");
            };
            assert!(
                why.contains("long") && why.contains("inf") && why.contains("2026-01-06T15:00:00"),
                "{why}"
            );
        }
        for side in Side::BOTH {
            assert_eq!(
                store(dir.path(), &config(), side).valid_froms().unwrap(),
                [],
                "{side:?}"
            );
        }
    }

    /// The calibration slice's features the largest finite numbers, less
    /// at 13:00 and more at 15:00: the fake predicts them, finite, but the
    /// mean of each row's two sides' predictions, the level, overflows, so
    /// the pair's calibration is no finite line. The train fails, and publishes neither side.
    #[rstest]
    fn a_calibration_that_is_no_finite_line_fails_the_train() {
        let dir = tempfile::tempdir().unwrap();
        let mut rows = rows();
        rows[4].decision.features = vec![-f64::MAX];
        rows[5].decision.features = vec![f64::MAX];

        let trained =
            training(dir.path(), config()).train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows));

        for outcome in [&trained.long, &trained.short] {
            let Some(Outcome::Kept(Kept::Failed(why))) = outcome else {
                panic!("{trained:?}");
            };
            assert!(
                why.contains("calibration") && why.contains("not finite"),
                "{why}"
            );
        }
        assert_eq!(
            store(dir.path(), &config(), Side::Long)
                .valid_froms()
                .unwrap(),
            []
        );
    }

    /// The short's stop on a row fitted on is worth nought, so its R there is
    /// no finite number and its fit fails. The long, fitted, publishes
    /// neither: both are kept, the long for the short's failure, which it
    /// names, and the pair of the 6th stays the current one.
    #[rstest]
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
    /// so the sides list no pair from then and both are kept, failed, for
    /// the short's store, which each names.
    #[rstest]
    fn a_publish_failing_after_the_others_leaves_no_pair_published() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let t = at("2026-01-07T00:00:00Z");
        store(dir.path(), &config(), Side::Short).break_publishing();
        let short = models(dir.path(), &config()).directory().join("short");

        let trained = training.train_at(t, |_, _| Ok(rows()));

        for outcome in [&trained.long, &trained.short] {
            let Some(Outcome::Kept(Kept::Failed(why))) = outcome else {
                panic!("{trained:?}");
            };
            assert!(why.contains(&short.display().to_string()), "{why}");
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

    /// The long's publish fails, the first: both are kept, failed, for the
    /// long's store, which each names, and neither the long nor the short,
    /// fitted with it, publishes anything.
    #[rstest]
    fn a_publish_failing_publishes_no_other_side() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let long = store(dir.path(), &config(), Side::Long);
        long.break_publishing();
        let long_directory = models(dir.path(), &config()).directory().join("long");

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows()));

        for outcome in [&trained.long, &trained.short] {
            let Some(Outcome::Kept(Kept::Failed(why))) = outcome else {
                panic!("{trained:?}");
            };
            assert!(why.contains(&long_directory.display().to_string()), "{why}");
        }
        assert_eq!(long.valid_froms().unwrap(), []);
        assert_eq!(
            store(dir.path(), &config(), Side::Short)
                .valid_froms()
                .unwrap(),
            []
        );
    }

    /// A one-sided strategy's calibration is the level line alone, on its
    /// side's prediction and R: `rows` with the short's win at 15:00 a loss,
    /// which the long's line never reads. The long's R on the calibration
    /// slice, -0.5 and 1.5, makes the line of `LINES`, and no side call;
    /// the sides' mean R, -0.5 and 0.25, would make `alpha_C` -0.875.
    #[rstest]
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

    #[rstest]
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
    /// pair is valid from then, so both sides are fitted again, calibrated
    /// together, and published, the long's fitted model in place of the
    /// baseline.
    #[rstest]
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
    #[rstest]
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
    #[rstest]
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
    #[rstest]
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

    #[rstest]
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
            blueprint(config()).fingerprint().unwrap().to_string()
        );
        assert_eq!(manifest.target, config().target);
        assert_eq!(manifest.learner, LearnerConfig::Fake {});
    }

    /// Each side's baseline is the mean of its R on the rows it was fitted
    /// and calibrated on, 1.5, -1, -0.5 and 1.5 of `rows`, 0.375; not on the
    /// rows outside those, the wins of 1.5R at 23:00 of the 5th, before the
    /// window, at 11:30, settled after the calibration slice's start, and
    /// at 23:30, settled after the train, with any of which it would be 0.6.
    #[rstest]
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
    #[rstest]
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

    #[rstest]
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

    /// No row of the calibration slice settled by the train, the win at
    /// 23:30 settling after it: of a minimum of one row, the train fails,
    /// naming the calibration slice, its 0 rows and the minimum, and
    /// neither side publishes.
    #[rstest]
    fn a_calibration_slice_of_fewer_rows_than_the_minimum_fails() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let mut rows = rows();
        rows.drain(4..6);

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows));

        for outcome in [&trained.long, &trained.short] {
            let Some(Outcome::Kept(Kept::Failed(why))) = outcome else {
                panic!("{trained:?}");
            };
            for fact in ["calibration slice", "0 rows", "min_rows 1"] {
                assert!(why.contains(fact), "{fact}: {why}");
            }
        }
        for side in Side::BOTH {
            assert_eq!(
                store(dir.path(), &config(), side).valid_froms().unwrap(),
                [],
                "{side:?}"
            );
        }
    }

    /// Rows of a feature more than the strategy's: neither side's model is
    /// fitted on them, and neither publishes.
    #[rstest]
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
        let trained = training.train_at(at("2026-01-07T02:00:00Z"), |_, _| Ok(wide));

        for outcome in [&trained.long, &trained.short] {
            assert!(
                matches!(outcome, Some(Outcome::Kept(Kept::SchemaMismatch(_)))),
                "{trained:?}"
            );
        }

        for side in Side::BOTH {
            assert_eq!(
                store(dir.path(), &config(), side).valid_froms().unwrap(),
                [],
                "{side:?}"
            );
        }
    }

    /// The section of `config` with a meta model on the fake learner, on a
    /// window of `window` hours, the last 12 calibrated on, taking a trade
    /// from an expected 0.5R, each slice of at least one row; each side
    /// traded from 0.5R, which the sides of `publish_baselines` clear, the
    /// long expecting 0.6R, the short -0.6R.
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
            .calibration(SignedDuration::from_hours(12))
            .margin(R::new(0.5))
            .min_rows(1)
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
                    fingerprint: blueprint(config.clone()).fingerprint().unwrap().to_string(),
                    feature_names: names,
                    calibration: unchanged(&config, at(valid_from)),
                    target: config.target,
                    side: Some(side),
                    learner: LearnerConfig::Baseline {},
                    baseline: r,
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
    /// the short's -0.6R: the meta's fit slice, a win and a loss of the
    /// long; its calibration slice, from 12:00, the meta's fake predicting
    /// their first feature, a loss at 0.25 and a win at 0.75. Each
    /// decision's level is nought and its side call 1.2.
    fn meta_rows() -> Vec<TargetRow> {
        vec![
            row("2026-01-06T05:00:00Z", vec![0.5], true, hour()),
            row("2026-01-06T08:00:00Z", vec![0.5], true, hour()),
            row("2026-01-06T10:00:00Z", vec![0.5], false, hour()),
            row("2026-01-06T13:00:00Z", vec![0.25], false, hour()),
            row("2026-01-06T15:00:00Z", vec![0.75], true, hour()),
        ]
    }

    /// The sides' models of a training in `dir` as `config` says: from
    /// 06:00 a long, and from 21:00, after every row, a long again, which no
    /// row may use.
    fn publish_baselines(dir: &Path, config: &ModelConfig) {
        let (long, short) = (
            store(dir, config, Side::Long),
            store(dir, config, Side::Short),
        );
        for (valid_from, long_r) in [("2026-01-06T06:00:00Z", 0.6), ("2026-01-06T21:00:00Z", 0.8)] {
            publish_baseline_of(config.clone(), &long, Side::Long, valid_from, long_r);
            publish_baseline_of(config.clone(), &short, Side::Short, valid_from, -long_r);
        }
    }

    /// The meta's calibration on `meta_rows`, worked by hand: its fake
    /// predicting 0.25 and 0.75 on the calibration slice, against R of -1
    /// and 1.5, means 0.5 and 0.25, a slope of 0.625 / 0.125 = 5, clipped to
    /// 1.5, and `alpha` 0.25 - 1.5 x 0.5 = -0.5: the meta expects `-0.5 +
    /// 1.5p` on the feature `p`.
    const META_LINE: Line = Line {
        alpha: -0.5,
        beta: 1.5,
    };

    /// Worked by hand on `meta_rows`: the meta fitted on the R of the side
    /// picked of its fit slice, the rows before 12:00, the long's 1.5 and
    /// -1, of a mean of 0.25, which the fake predicts where its feature is
    /// missing, the decision before the sides' first model no row;
    /// calibrated by its level line on its calibration slice (`META_LINE`);
    /// its probes the calibration slice's longs and its predictions on them,
    /// before the calibration.
    #[rstest]
    fn a_meta_model_learns_the_r_of_the_side_picked_from_its_out_of_sample_decisions() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), with_meta(24));
        publish_baselines(dir.path(), &with_meta(24));
        let t = at("2026-01-07T00:00:00Z");

        let trained = training.train_at(t, |_, _| Ok(meta_rows()));

        assert_eq!(trained.meta, Some(Outcome::Published { valid_from: t }));
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
                (&vec![0.25, 1.0, 0.0, 1.2], 0.25),
                (&vec![0.75, 1.0, 0.0, 1.2], 0.75)
            ]
        );
        let model = LearnerConfig::Fake {}.load(&stored.model).unwrap();
        assert_eq!(model.predict(&[f64::NAN, 1.0, 0.0, 1.2]).unwrap(), 0.25);
    }

    /// A row of the meta's calibration slice whose feature is infinite: the
    /// meta's fake predicts it, infinite, and the meta fails, naming the row,
    /// and publishes none.
    #[rstest]
    fn a_meta_prediction_on_its_calibration_slice_that_is_no_finite_number_fails_it() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), with_meta(24));
        publish_baselines(dir.path(), &with_meta(24));
        let mut rows = meta_rows();
        rows[3].decision.features = vec![f64::INFINITY];

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows));

        let Some(Outcome::Kept(Kept::Failed(why))) = trained.meta else {
            panic!("{trained:?}");
        };
        assert!(
            why.contains("meta") && why.contains("inf") && why.contains("2026-01-06T13:00:00"),
            "{why}"
        );
        assert_eq!(
            meta_store(dir.path(), &with_meta(24))
                .valid_froms()
                .unwrap(),
            []
        );
    }

    /// `meta_rows` with the long's loss at 10:00 a win: the meta, on the
    /// longs of the sides' models valid from 06:00, is fitted on the R of the
    /// wins at 08:00 and 10:00, 1.5 each, and calibrated on the loss at 13:00
    /// and the win at 15:00, -1 and 1.5, a mean of 0.875; not on the decision
    /// at 05:00, before the sides' first model, a win.
    #[rstest]
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
    /// R of 1.75.
    #[rstest]
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

        assert_eq!(trained.meta, Some(Outcome::Published { valid_from: t }));
        let manifest = meta_store(dir.path(), &config).read(t).unwrap().manifest;
        assert_eq!(manifest.baseline, 1.75);
        assert_eq!(manifest.target.spec.bracket, None);
    }

    /// Margins of the long alone: the short's models are never asked, and
    /// the meta's rows are the long's trades, its calibration slice a loss
    /// at 0.25 and a win at 0.75, each of the level of the long of 06:00,
    /// 0.6R, and no side call.
    #[rstest]
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
        publish_baseline_of(
            long_only.clone(),
            &store(dir.path(), &long_only, Side::Long),
            Side::Long,
            "2026-01-06T06:00:00Z",
            0.6,
        );
        let t = at("2026-01-07T00:00:00Z");

        let trained = training.train_at(t, |_, _| Ok(meta_rows()));

        assert_eq!(trained.meta, Some(Outcome::Published { valid_from: t }));
        let stored = meta_store(dir.path(), &long_only).read(t).unwrap();
        let probes: Vec<&Vec<f64>> = stored.manifest.probes.iter().map(|p| &p.row).collect();
        assert_eq!(
            probes,
            [&vec![0.25, 1.0, 0.6, 0.0], &vec![0.75, 1.0, 0.6, 0.0]]
        );
    }

    /// The meta's window of 36 hours is longer than the sides' 24, and
    /// starts after the sides' first models, of the 5th.
    #[rstest]
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
    #[rstest]
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
    #[rstest]
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
    #[rstest]
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
    /// traded from `config`'s margins.
    fn never_with_meta() -> ModelConfig {
        ModelConfig {
            retrain: Retrain::Never,
            margins: config().margins,
            ..with_meta(24)
        }
    }

    /// The decisions of the 7th, of the sides as `rows` of the 6th fit them
    /// (`a_train_fits_on_rows_settled_by_the_calibrations_start_and_calibrates_on_the_rest_settled_by_its_time`):
    /// each side expecting `-0.25 + 1.5p` R on the feature `p` (`LINES`),
    /// alike, so that the short, from 0.5R, is picked alone below 1R, the
    /// long's margin; each side's stop worth 2 USD, a win 1.5R and a loss
    /// -1R. The meta's fit slice, shorts at 0.6 (0.65R expected), a win and
    /// a loss; its calibration slice, a short at 0.55 (0.575R) losing and
    /// one at 0.65 (0.725R) winning, of those levels and no side call.
    fn decisions_of_the_seventh() -> Vec<TargetRow> {
        let short = |time, p, wins| row_of(time, vec![p], (wins, wins), hour());
        vec![
            short("2026-01-07T08:00:00Z", 0.6, true),
            short("2026-01-07T10:00:00Z", 0.6, false),
            short("2026-01-07T13:00:00Z", 0.55, false),
            short("2026-01-07T15:00:00Z", 0.65, true),
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
    #[rstest]
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

    /// The sides fitted as of the 7th are fitted and calibrated as a train
    /// at the 7th would: on `rows`, the win at 23:30 of the 6th, settled
    /// after the 7th, not among them, nor the decisions of the 7th: their
    /// calibration `LINES`, their probes the calibration slice's rows.
    #[rstest]
    fn a_two_time_trains_sides_are_fitted_and_calibrated_on_rows_settled_by_their_time() {
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
            [&vec![0.25, 1.4, 1.1, 0.07], &vec![0.75, 1.4, 1.1, 0.07]]
        );
    }

    /// The meta at the 8th learns from the decisions of the sides just
    /// published over its window, the 7th: its calibration slice the shorts
    /// at 0.55 and 0.65, each decision's level `-0.25 + 1.5p`, 0.575 and
    /// 0.725, and no side call.
    #[rstest]
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
        assert_eq!((second[0], second[1], second[3]), (0.65, -1.0, 0.0));
        close(second[2], 0.725);
    }

    /// One replay serves both: from the sides' window back from the 7th,
    /// the 6th, to the train, the 8th.
    #[rstest]
    fn a_two_time_train_replays_once_from_the_sides_window_to_its_time() {
        let dir = tempfile::tempdir().unwrap();

        let (_, _, replays) = two_time(dir.path(), never_with_meta());

        assert_eq!(
            replays,
            [(at("2026-01-06T00:00:00Z"), at("2026-01-08T00:00:00Z"))]
        );
    }

    /// The sides' fit slice as of the 7th, two rows, short of a minimum of
    /// three: neither side publishes as of the 7th, the meta has no
    /// decision of the sides to learn from, and waits, with no fit
    /// attempted, as the train says.
    #[rstest]
    fn the_sides_failing_in_a_two_time_train_leave_the_meta_waiting() {
        let dir = tempfile::tempdir().unwrap();
        let three = ModelConfig {
            min_rows: 3,
            ..never_with_meta()
        };

        let (models, trained, _) = two_time(dir.path(), three);

        for outcome in [&trained.long, &trained.short] {
            let Some(Outcome::Kept(Kept::Failed(why))) = outcome else {
                panic!("{trained:?}");
            };
            assert!(why.contains("fit slice") && why.contains("2 rows"), "{why}");
        }
        assert_eq!(trained.meta, Some(Outcome::Kept(Kept::Waiting)));
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

    #[rstest]
    fn a_train_succeeds_unless_it_kept_a_model() {
        let t = at("2026-01-07T00:00:00Z");
        let published = Outcome::Published { valid_from: t };
        let in_use = Outcome::InUse {
            valid_from: t,
            retrain: Retrain::Every(SignedDuration::from_hours(24)),
        };
        let fitted_or_in_use = Trained {
            long: Some(published),
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

    /// A model published reads with its valid-from; one kept in use as
    /// already published, with its valid-from, and never retrained as in
    /// use since it, and never retrained; a waiting meta as waiting for the
    /// sides' decisions.
    #[rstest]
    #[case::published(
        Outcome::Published { valid_from: at("2026-01-07T00:00:00Z") },
        &["published", "2026-01-07T00:00:00Z"]
    )]
    #[case::in_use(
        Outcome::InUse {
            valid_from: at("2026-01-07T00:00:00Z"),
            retrain: Retrain::Every(SignedDuration::from_hours(24)),
        },
        &["already published, valid from 2026-01-07T00:00:00Z"]
    )]
    #[case::in_use_never_retrained(
        Outcome::InUse {
            valid_from: at("2026-01-06T00:00:00Z"),
            retrain: Retrain::Never,
        },
        &["already published", "in use since 2026-01-06T00:00:00Z", "never retrained"]
    )]
    #[case::waiting(Outcome::Kept(Kept::Waiting), &["waiting for the sides' decisions"])]
    fn a_model_published_or_in_use_reads_with_its_valid_from_and_a_waiting_meta_as_waiting(
        #[case] outcome: Outcome,
        #[case] facts: &[&str],
    ) {
        let text = outcome.to_string();

        for fact in facts {
            assert!(text.contains(fact), "{fact}: {text}");
        }
    }

    #[rstest]
    fn a_train_of_several_models_reads_as_each_outcome() {
        let trained = Trained {
            long: Some(Outcome::Published {
                valid_from: at("2026-01-07T00:00:00Z"),
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
