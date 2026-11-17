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
//! (`dataset::DatasetRow::side`), under the names its model store checks it
//! against (`store::SideLevel::feature_names`); and its meta model, if it
//! has one (`model::meta`). A train publishes them as one generation
//! (`store::StoredGeneration`), valid from its time, or nothing: a decision
//! uses the sides and the meta of one train together.
//!
//! What a train at `t` does is the schedule's plan (`schedule::Schedule::plan`),
//! from the store's generations alone: a generation current at `t`
//! (retrained, the one valid from `t`; never retrained, the latest valid by
//! `t`, so one generation is trained) is kept, in use; else a generation is
//! trained at `t`, its sides fitted as of `t`, or, never retrained with a
//! meta, as of `t` less the meta's window, and its meta, if any, at `t`,
//! unless it waits for the sides' decisions, no generation being valid
//! before `t`. A kept generation replays nothing.
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
//! A train fails, publishing nothing, when a slice of a level has fewer rows
//! than the level's minimum (`ModelConfig::min_rows`), naming the slice, its
//! count and the minimum; when a model's prediction on a calibration row,
//! or a calibration, is no finite number; or when a model reloaded from
//! what it saved does not predict its probes as fitted, the check a reader
//! makes before use (`store::StoredModel`). Out-of-sample evidence is a
//! backtest's (`walk::WalkForward::scores`). Otherwise the generation
//! publishes whole (`store::ModelStore::publish`), valid from `t`, its sides
//! with the pair's calibration, each model with its baseline, the training
//! mean of R on the rows it was fitted and calibrated on, which its
//! out-of-sample scores are read beside, and its manifest probing it on rows
//! of its calibration slice; its manifest, written last, makes it visible,
//! so a train failing or stopped before it leaves nothing any decision uses.
//!
//! With a meta model, the sides train first, then the meta, on the same
//! rows, of the one replay: the meta's window is its own
//! (`MetaConfig::window`), and its rows are made from the sides'
//! out-of-sample decisions (`meta::examples`): retrained, those of the
//! generations the store holds before the train, read as the train starts,
//! from the first on; never retrained, those of the sides the train fits as
//! of the meta's window before it, over that window. The meta is a
//! regression of the realized net R of the trade of the side picked, split
//! as the sides are on its own calibration slice (`MetaConfig::calibration`),
//! calibrated by its level line alone (`calibration::Ksplit` of one side),
//! held to its own minimum of rows (`MetaConfig::min_rows`) and checked as a
//! side is. A meta that waits publishes the generation without it, which
//! trades nothing (`deciding`), and the next train's meta learns from its
//! sides.
//!
//! A train never errors: what goes wrong is an outcome, the current
//! generation kept with the reason (`Kept`), for each model of the strategy.
//! It succeeds when it kept no model (`Trained::succeeded`).

use std::fmt::Display;

use anyhow::Context;
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;

use super::{
    blueprint::Stored,
    calibration::{self, Ksplit},
    dataset::{Dataset, DatasetRow, SchemaMismatch},
    deciding::{self, DecidingModels},
    holdout::Example,
    learner::{LearnerConfig, SavedModel, TrainingSet},
    meta,
    schedule::{Fit, Fits, MetaStep, Models, Plan, Retrain, Schedule},
    store::{
        Baselined, MetaLevel, ModelManifest, ModelStore, Pair, Probe, SideModel, StoredGeneration,
        StoredModel,
    },
    targets::{Side, TargetConfig, TargetRow},
};

/// The most rows of the calibration slice a published model is probed on.
const PROBES: usize = 16;

/// The training of one strategy's models: its sides, its meta model, or
/// both.
#[derive(Debug)]
pub struct Training {
    target: TargetConfig,
    dataset: Dataset,
    /// What each train does to each model.
    schedule: Schedule,
    /// The strategy's models: the generations the plan reads, which a
    /// retrained meta's rows are made from, and where the train publishes.
    models: ModelStore,
    /// Each model: of each side the strategy trades, and the meta model, if
    /// any.
    levels: Models<Level>,
}

/// How the models of one level, a side or a meta, are fitted and
/// calibrated.
#[derive(Debug)]
struct Level {
    /// The names of the features of its rows, in order.
    feature_names: Vec<String>,
    /// The last part of its window, calibrated on and not fitted on.
    calibration: SignedDuration,
    /// The fewest rows of its fit slice and of its calibration slice.
    min_rows: usize,
    learner: LearnerConfig,
}

/// A model fitted and calibrated as of a time, not published: what its
/// manifest takes of the fit.
#[derive(Debug)]
struct Fitted {
    saved: SavedModel,
    /// The side of a side's model; none of a meta's.
    side: Option<Side>,
    /// A meta's level line; none of a side's, which its pair calibrates.
    calibration: Option<calibration::Line>,
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

/// The sides' pair a train fitted, in hand to decide the meta's rows, and
/// as the generation publishes it: each side's model, and the pair's
/// calibration.
struct PairFit {
    pair: Pair,
    sides: Vec<StoredModel>,
    calibration: Ksplit,
}

/// What a train did: the outcome of each model of the strategy, none for
/// a model it has not (a side it does not trade, or the meta of
/// a strategy without one).
pub type Trained = Models<Outcome>;

/// What a train did to one model.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// A model was published, in the generation valid from `valid_from`.
    Published { valid_from: Timestamp },
    /// None was fitted: the current generation, valid from `valid_from`, is
    /// in use (`schedule::Plan::Keep`), of a strategy retrained as `retrain`
    /// says, which says how it reads (`Retrain::in_use`).
    InUse {
        valid_from: Timestamp,
        retrain: Retrain,
    },
    /// No model was published: the current generation, if any, stays.
    Kept(Kept),
}

/// Why a train kept the current generation, or published one without a
/// meta.
#[derive(Debug, Clone, PartialEq)]
pub enum Kept {
    /// The replay, the dataset, a fit, a check of the models or the store
    /// failed, or a slice had fewer rows than the minimum; the error, the
    /// same for every model of the generation, whichever it was of.
    Failed(String),
    /// The dataset holds rows that do not match the strategy's features; the
    /// mismatch.
    SchemaMismatch(String),
    /// A meta waits for the sides' decisions to learn from: no generation
    /// is valid before the train, so none was fitted, and the generation
    /// published holds none (`schedule::MetaStep::Wait`).
    Waiting,
}

impl Training {
    /// The training of the models of `model`, a strategy's model on the
    /// venue of its dataset runs, one for each side of its margins, and of
    /// its meta model if its config has one, on its dataset and in its
    /// model store, as its config says.
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
            })
        };
        let levels = Models {
            long: side(Side::Long),
            short: side(Side::Short),
            meta: models.meta().map(Level::of_meta),
        };
        Self {
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
    /// the store's generations: each model of the generation current at
    /// `at` in use, if any; else a generation trained and published
    /// (`generation`), or, should anything of it fail, each model kept with
    /// the error. `replay` runs the strategy over a span of the days the
    /// dataset lacks and returns the rows it settled (see
    /// `Dataset::settled`), once, from where the plan's fits start, and not
    /// at all if the plan keeps a generation.
    pub fn train_at(
        &self,
        at: Timestamp,
        replay: impl FnOnce(Timestamp, Timestamp) -> anyhow::Result<Vec<TargetRow>>,
    ) -> Trained {
        let each = |outcome: Outcome| self.levels.as_ref().map(|_| outcome.clone());
        let generations = match self.models.generations() {
            Ok(generations) => generations,
            Err(e) => return each(kept(&e)),
        };

        match self.schedule.plan(at, &generations) {
            Plan::Keep { valid_from } => each(Outcome::InUse {
                valid_from,
                retrain: self.schedule.retrain(),
            }),
            Plan::Train(fits) => self
                .generation(&fits, replay)
                .unwrap_or_else(|e| each(kept(&e))),
        }
    }

    /// Trains the generation of `fits` and publishes it, valid from the
    /// train's time, its sides and its meta, if the meta did not wait: on
    /// the rows `replay` settles from where the fits start, the sides'
    /// pair fitted as of their time (`pair`), then the meta as of the
    /// train's (`Level::fit`), on the decisions the plan says of
    /// (`meta_examples`). Each model is published, and a waiting meta kept,
    /// waiting.
    ///
    /// # Errors
    ///
    /// Returns an error if the replay, a fit, a check or the store fails, or
    /// a slice has fewer rows than its level's minimum, the meta's naming it.
    fn generation(
        &self,
        fits: &Fits,
        replay: impl FnOnce(Timestamp, Timestamp) -> anyhow::Result<Vec<TargetRow>>,
    ) -> anyhow::Result<Trained> {
        let at = fits.at;
        let rows = self.dataset.settled(fits.replay_from(), at, replay)?;
        let pair = self.pair(fits.sides, &rows)?;
        let (meta, meta_outcome) = match (fits.meta, &self.levels.meta) {
            (Some(MetaStep::Wait), _) => (None, Some(Outcome::Kept(Kept::Waiting))),
            (Some(step), Some(level)) => {
                let examples = self
                    .meta_examples(step, fits, &pair.pair, &rows)
                    .context("making the meta model's rows")?;
                let fitted = level.fit(at, examples).context("fitting the meta model")?;
                let stored = self
                    .stored(level, fitted)
                    .context("checking the meta model")?;
                (Some(stored), Some(Outcome::Published { valid_from: at }))
            }
            (None, _) | (Some(_), None) => (None, None),
        };
        self.models.publish(&StoredGeneration {
            valid_from: at,
            sides: pair.sides,
            calibration: pair.calibration,
            meta,
        })?;
        let mut trained = self
            .levels
            .as_ref()
            .map(|_| Outcome::Published { valid_from: at });
        trained.meta = meta_outcome;
        Ok(trained)
    }

    /// The meta's examples of a train of `fits` (`meta::examples`), on the
    /// replay's `rows` over its window, the step `step` of its meta: the
    /// decisions over `[from, the train)` of the generations the store
    /// holds, listed now; or those of `pair`, the sides the train fitted, over
    /// `[their time, the train)`.
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot be listed, a generation fails its
    /// checks, or a model fails to predict.
    fn meta_examples(
        &self,
        step: MetaStep,
        fits: &Fits,
        pair: &Pair,
        rows: &[DatasetRow],
    ) -> anyhow::Result<Vec<Example>> {
        let from = match step {
            MetaStep::OnStored { from } => from,
            MetaStep::OnSides | MetaStep::Wait => fits.sides.at,
        };
        let rows = in_fit(rows, Fit { from, at: fits.at });

        match step {
            MetaStep::OnStored { .. } => {
                let mut generations = DecidingModels::new(&self.models)?;
                meta::examples(&rows, |now, sides| generations.pick(now, sides))
            }
            MetaStep::OnSides | MetaStep::Wait => meta::examples(&rows, |_, sides| {
                DecidingModels::pick_among(pair, deciding::margins(&self.models), sides)
            }),
        }
    }

    /// The pair of the sides fitted as `fit` says on `rows`, those of its
    /// span settled by its time (`in_fit`), split into the fit slice and the
    /// calibration slice (`split`), each of at least the minimum of rows
    /// (`Level::ensure_rows`): each side's model fitted on its realized R on
    /// the fit slice (`fit_side`), the pair calibrated together on the
    /// calibration slice (`Ksplit::fit`), every prediction there and the
    /// calibration finite, and each side checked as a reader checks it
    /// (`Training::stored`).
    ///
    /// # Errors
    ///
    /// Returns an error if a slice has fewer rows than the minimum, a side's
    /// fit fails, naming the side, a model fails to predict or predicts no
    /// finite number, the calibration is no finite line, or a side fails its
    /// check.
    fn pair(&self, fit: Fit, rows: &[DatasetRow]) -> anyhow::Result<PairFit> {
        let at = fit.at;
        let levels: Vec<(Side, &Level)> = self
            .models
            .sides()
            .filter_map(|traded| {
                let side = traded.side();
                self.levels.side(side).map(|level| (side, level))
            })
            .collect();
        let cut = at - levels[0].1.calibration;
        let (fitted, calibrating) =
            split(in_fit(rows, fit), cut, |row| (row.ts_event, row.settled_at));
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
        let calibration =
            Ksplit::fit(&predicted).context("calibrating the pair on the calibration slice")?;
        anyhow::ensure!(
            [
                calibration.level.alpha,
                calibration.level.beta,
                calibration.side_call.unwrap_or(0.0)
            ]
            .iter()
            .all(|value| value.is_finite()),
            "the pair's calibration {calibration:?} is not finite"
        );
        let mut models = vec![];
        let mut sides = vec![];

        for (i, fit) in fits.into_iter().enumerate() {
            let probes = spread(calibrating.len())
                .map(|row| Probe {
                    row: calibrating[row].side(fit.side).features,
                    prediction: predicted[row][i].0,
                })
                .collect();
            let fitted = Fitted {
                side: Some(fit.side),
                calibration: None,
                baseline: fit.model.baseline(),
                saved: fit.saved,
                probes,
            };
            sides.push(self.stored(fit.level, fitted)?);
            models.push((fit.side, fit.model));
        }
        Ok(PairFit {
            pair: Pair::new(models, calibration)?,
            sides,
            calibration,
        })
    }

    /// The model `fitted` of `level` as its generation holds it, checked as
    /// a reader checks it before use (`StoredModel::checked`): reloaded from
    /// what it saved, it predicts each probe to the bit as fitted.
    ///
    /// # Errors
    ///
    /// Returns an error if the model fails its check.
    fn stored(&self, level: &Level, fitted: Fitted) -> anyhow::Result<StoredModel> {
        let fingerprint = self.dataset.fingerprint().to_string();
        let stored = StoredModel {
            manifest: ModelManifest {
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
            calibration: Some(line),
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

/// The rows of `rows` a fit `fit` reads: those of `[from, at)` settled by
/// `at`.
fn in_fit(rows: &[DatasetRow], Fit { from, at }: Fit) -> Vec<&DatasetRow> {
    let (start, end) = (UnixNanos::from(from), UnixNanos::from(at));
    rows.iter()
        .filter(|row| start <= row.ts_event && row.ts_event < end && row.settled_at <= end)
        .collect()
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
                "waiting for the sides' decisions: no generation is valid before the train"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, rc::Rc};

    use jiff::SignedDuration;
    use nautilus_model::{
        data::NautilusDataType,
        identifiers::{InstrumentId, StrategyId},
        types::Money,
    };
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
            store::ModelStore,
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
    fn models(dir: &Path, config: &ModelConfig) -> ModelStore {
        blueprint(config.clone())
            .model_store(&dir.join("models"))
            .unwrap()
    }

    /// The generations of a training in `dir` as `config` says.
    fn generations(dir: &Path, config: &ModelConfig) -> Vec<Timestamp> {
        models(dir, config).generations().unwrap()
    }

    /// The generation valid from `valid_from` of a training in `dir` as
    /// `config` says.
    fn generation(dir: &Path, config: &ModelConfig, valid_from: Timestamp) -> StoredGeneration {
        models(dir, config).read(valid_from).unwrap()
    }

    /// The model of `side` of `generation`.
    fn side_of(generation: &StoredGeneration, side: Side) -> &StoredModel {
        generation
            .sides
            .iter()
            .find(|model| model.manifest.side == Some(side))
            .unwrap()
    }

    /// The meta model of the generation valid from `valid_from` of a
    /// training in `dir` as `config` says.
    fn meta_of(dir: &Path, config: &ModelConfig, valid_from: Timestamp) -> StoredModel {
        generation(dir, config, valid_from).meta.unwrap()
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
    /// from 12:00 settled by the train (`LINES`), the generation holding it
    /// and no side's manifest a calibration of its own. Each side's
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
        let generation = generation(dir.path(), &config(), t);
        assert_eq!(generation.calibration, LINES);
        for side in Side::BOTH {
            let stored = side_of(&generation, side);
            assert_eq!(stored.manifest.calibration, None, "{side:?}");
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
    /// publishes no generation.
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
        assert_eq!(generations(dir.path(), &config()), []);
    }

    /// The calibration slice's features the largest finite numbers, less
    /// at 13:00 and more at 15:00: the fake predicts them, finite, but the
    /// mean of each row's two sides' predictions, the level, overflows, so
    /// the pair's calibration is no finite line. The train fails, and
    /// publishes no generation.
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
        assert_eq!(generations(dir.path(), &config()), []);
    }

    /// The short's stop on a row fitted on is worth nought, so its R there is
    /// no finite number and its fit fails. The long, fitted, publishes
    /// nothing: both are kept, the long for the short's failure, which it
    /// names, and the generation of the 6th stays the current one.
    #[rstest]
    fn a_side_whose_fit_fails_publishes_no_generation() {
        let dir = tempfile::tempdir().unwrap();
        let sixth = "2026-01-06T00:00:00Z";
        publish_generation(dir.path(), &config(), sixth, (0.0, 0.0));
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
        assert_eq!(generations(dir.path(), &config()), [at(sixth)]);
    }

    /// The generation's publish fails: every model is kept, failed, for the
    /// store, which each names, and no generation is listed.
    #[rstest]
    fn a_publish_failing_publishes_no_generation() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let models = models(dir.path(), &config());
        models.break_publishing();

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows()));

        for outcome in [&trained.long, &trained.short] {
            let Some(Outcome::Kept(Kept::Failed(why))) = outcome else {
                panic!("{trained:?}");
            };
            assert!(
                why.contains(&models.directory().display().to_string()),
                "{why}"
            );
        }
        assert_eq!(models.generations().unwrap(), []);
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
        assert_eq!(
            generation(dir.path(), &long_only, t).calibration,
            Ksplit {
                side_call: None,
                ..LINES
            }
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

    /// The generation at the train's time, a baseline published by a train
    /// stopped before its manifest, is not listed: the train fits and
    /// publishes the generation again, the fitted models in place of the
    /// baselines.
    #[rstest]
    fn a_generation_at_the_trains_time_without_its_manifest_is_trained_again() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let t = at("2026-01-07T00:00:00Z");
        publish_generation(dir.path(), &config(), "2026-01-07T00:00:00Z", (0.8, 0.8));
        models(dir.path(), &config()).drop_manifest(t);

        let trained = training.train_at(t, |_, _| Ok(rows()));

        for outcome in [&trained.long, &trained.short] {
            assert!(
                matches!(outcome, Some(Outcome::Published { valid_from, .. }) if *valid_from == t),
                "{trained:?}"
            );
        }
        let generation = generation(dir.path(), &config(), t);
        assert_eq!(
            side_of(&generation, Side::Long).manifest.learner,
            LearnerConfig::Fake {}
        );
    }

    /// A generation is valid from the train's time: a rerun of the train
    /// fits nothing, and so replays nothing.
    #[rstest]
    fn a_train_with_every_model_in_use_replays_nothing_and_reports_each() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let t = at("2026-01-07T00:00:00Z");
        publish_generation(dir.path(), &config(), "2026-01-07T00:00:00Z", (0.5, 0.5));
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

    /// Never retrained, the generation of the 6th, its long expecting
    /// nought and so never picked, is current at the train of the 7th: kept
    /// in use, not fitted again, and nothing replayed.
    #[rstest]
    fn never_retrained_a_generation_valid_before_the_train_is_kept_in_use() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), never());
        let sixth = at("2026-01-06T00:00:00Z");
        publish_generation(dir.path(), &never(), "2026-01-06T00:00:00Z", (0.5, 0.5));
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

    #[rstest]
    fn a_published_model_carries_its_schema_target_and_learner() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let t = at("2026-01-07T00:00:00Z");

        training.train_at(t, |_, _| Ok(rows()));

        assert_eq!(generations(dir.path(), &config()), [t]);
        let manifest = side_of(&generation(dir.path(), &config(), t), Side::Long)
            .manifest
            .clone();
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
        let generation = generation(dir.path(), &config(), t);

        for side in Side::BOTH {
            assert_eq!(
                side_of(&generation, side).manifest.baseline,
                0.375,
                "{side:?}"
            );
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
        assert_eq!(generations(dir.path(), &config()), []);
    }

    /// No row of the calibration slice settled by the train, the win at
    /// 23:30 settling after it: of a minimum of one row, the train fails,
    /// naming the calibration slice, its 0 rows and the minimum, and no
    /// generation is published.
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
        assert_eq!(generations(dir.path(), &config()), []);
    }

    /// Rows of a feature more than the strategy's: neither side's model is
    /// fitted on them, and no generation is published.
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
        assert_eq!(generations(dir.path(), &config()), []);
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

    /// Publishes under `dir` a generation of `config` valid from
    /// `valid_from`: each side traded a baseline predicting an R of its own
    /// of `(long, short)` on every row, its baseline that R, their pair's
    /// calibration leaving them as they are (`unchanged`), and no meta.
    fn publish_generation(
        dir: &Path,
        config: &ModelConfig,
        valid_from: &str,
        (long, short): (f64, f64),
    ) {
        let names = side_names(features().names(), &config.target.payoff_names());
        let side = |side, r| {
            let set = TrainingSet::new(names.len(), vec![0.5; names.len()], vec![r]).unwrap();
            let model = LearnerConfig::Baseline {}.fit(&set).unwrap();
            StoredModel {
                manifest: ModelManifest {
                    fingerprint: blueprint(config.clone()).fingerprint().unwrap().to_string(),
                    feature_names: names.clone(),
                    calibration: None,
                    target: config.target.clone(),
                    side: Some(side),
                    learner: LearnerConfig::Baseline {},
                    baseline: r,
                    probes: vec![],
                },
                model: model.save().unwrap(),
            }
        };
        let sides = config
            .margins
            .sides()
            .map(|(traded, _)| {
                side(
                    traded,
                    match traded {
                        Side::Long => long,
                        Side::Short => short,
                    },
                )
            })
            .collect();
        models(dir, config)
            .publish(&StoredGeneration {
                valid_from: at(valid_from),
                sides,
                calibration: unchanged(config),
                meta: None,
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

    /// The generations of a training in `dir` as `config` says, their
    /// meta waited: from 06:00 a long, and from 21:00, after every row, a
    /// long again, which no row may use.
    fn publish_baselines(dir: &Path, config: &ModelConfig) {
        for (valid_from, long_r) in [("2026-01-06T06:00:00Z", 0.6), ("2026-01-06T21:00:00Z", 0.8)] {
            publish_generation(dir, config, valid_from, (long_r, -long_r));
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
        let stored = meta_of(dir.path(), &with_meta(24), t);
        assert_eq!(
            stored.manifest.feature_names,
            ["mid_0", "picked_side", "level", "side_call"]
        );
        assert_eq!(stored.manifest.side, None);
        assert_eq!(stored.manifest.calibration, Some(META_LINE));
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

    /// A row of the meta's calibration slice whose feature is infinite, the
    /// meta calibrated on 15 hours, from 09:00, and the sides on 12, from
    /// 12:00: the decision at 10:00 is in the sides' fit slice, which the
    /// fake fits on its R alone, and in the meta's calibration slice, where
    /// the meta's fake predicts it, infinite. The meta fails, naming the row,
    /// after the sides fitted: they publish nothing either, every model kept
    /// for the meta's failure, and no generation is published at the train.
    #[rstest]
    fn a_meta_prediction_on_its_calibration_slice_that_is_no_finite_number_fails_the_generation() {
        let dir = tempfile::tempdir().unwrap();
        let config = ModelConfig {
            meta: Some(MetaConfig {
                calibration: SignedDuration::from_hours(15),
                ..meta(24)
            }),
            ..with_meta(24)
        };
        let training = training(dir.path(), config.clone());
        publish_baselines(dir.path(), &config);
        let mut rows = meta_rows();
        rows[2].decision.features = vec![f64::INFINITY];

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows));

        for outcome in trained.iter() {
            let Outcome::Kept(Kept::Failed(why)) = outcome else {
                panic!("{trained:?}");
            };
            assert!(
                why.contains("meta") && why.contains("inf") && why.contains("2026-01-06T10:00:00"),
                "{why}"
            );
        }
        assert_eq!(
            generations(dir.path(), &config),
            [at("2026-01-06T06:00:00Z"), at("2026-01-06T21:00:00Z")]
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
        assert_eq!(
            meta_of(dir.path(), &with_meta(24), t).manifest.baseline,
            0.875
        );
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
        let manifest = meta_of(dir.path(), &config, t).manifest;
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
        publish_generation(dir.path(), &long_only, "2026-01-06T06:00:00Z", (0.6, 0.0));
        let t = at("2026-01-07T00:00:00Z");

        let trained = training.train_at(t, |_, _| Ok(meta_rows()));

        assert_eq!(trained.meta, Some(Outcome::Published { valid_from: t }));
        let stored = meta_of(dir.path(), &long_only, t);
        let probes: Vec<&Vec<f64>> = stored.manifest.probes.iter().map(|p| &p.row).collect();
        assert_eq!(
            probes,
            [&vec![0.25, 1.0, 0.6, 0.0], &vec![0.75, 1.0, 0.6, 0.0]]
        );
    }

    /// The meta's window of 36 hours is longer than the sides' 24, and
    /// starts after the first generation, of the 5th.
    #[rstest]
    fn a_train_with_a_meta_model_replays_once_from_the_longer_windows_first_day() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), with_meta(36));
        publish_baselines(dir.path(), &with_meta(36));
        publish_generation(
            dir.path(),
            &with_meta(36),
            "2026-01-05T00:00:00Z",
            (0.0, 0.0),
        );
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

    /// The meta's window of 36 hours starts on the 5th, before the first
    /// generation, of 06:00 of the 6th, where its rows start: the one replay
    /// starts with the sides' window, on the 6th.
    #[rstest]
    fn a_train_replays_no_day_before_the_first_generation() {
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

    /// No generation is valid before the train, so no row of the meta has
    /// a decision of the sides: the meta waits, and none is fitted, the
    /// sides fitted and published as ever, in a generation holding no meta.
    #[rstest]
    fn a_meta_model_without_a_generation_before_it_waits() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), with_meta(24));
        let t = at("2026-01-07T00:00:00Z");

        let trained = training.train_at(t, |_, _| Ok(meta_rows()));

        assert_eq!(trained.meta, Some(Outcome::Kept(Kept::Waiting)));
        for outcome in [&trained.long, &trained.short] {
            assert_eq!(outcome, &Some(Outcome::Published { valid_from: t }));
        }
        assert_eq!(generation(dir.path(), &with_meta(24), t).meta, None);
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
    ) -> (ModelStore, Trained, Vec<(Timestamp, Timestamp)>) {
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
    /// sides as of the 7th, the meta's window of 24 hours before it, and the
    /// meta at the 8th, and publishes them as one generation, valid from the
    /// 8th.
    #[rstest]
    fn never_retrained_on_an_empty_store_a_train_fits_the_sides_as_of_the_metas_window_before_it_and_the_meta_at_it()
     {
        let dir = tempfile::tempdir().unwrap();
        let eighth = at("2026-01-08T00:00:00Z");

        let (models, trained, _) = two_time(dir.path(), never_with_meta());

        for outcome in trained.iter() {
            assert_eq!(
                outcome,
                &Outcome::Published { valid_from: eighth },
                "{trained:?}"
            );
        }
        assert_eq!(models.generations().unwrap(), [eighth]);
        assert!(models.read(eighth).unwrap().meta.is_some());
    }

    /// The sides fitted as of the 7th are fitted and calibrated as a train
    /// at the 7th would: on `rows`, the win at 23:30 of the 6th, settled
    /// after the 7th, not among them, nor the decisions of the 7th: their
    /// calibration `LINES`, their probes the calibration slice's rows.
    #[rstest]
    fn a_two_time_trains_sides_are_fitted_and_calibrated_on_rows_settled_by_their_time() {
        let dir = tempfile::tempdir().unwrap();
        let (models, _, _) = two_time(dir.path(), never_with_meta());

        let generation = models.read(at("2026-01-08T00:00:00Z")).unwrap();

        assert_eq!(generation.calibration, LINES);
        let long = side_of(&generation, Side::Long);
        let probes: Vec<&Vec<f64>> = long.manifest.probes.iter().map(|p| &p.row).collect();
        assert_eq!(
            probes,
            [&vec![0.25, 1.4, 1.1, 0.07], &vec![0.75, 1.4, 1.1, 0.07]]
        );
    }

    /// The meta at the 8th learns from the decisions of the sides fitted
    /// beside it over its window, the 7th: its calibration slice the shorts
    /// at 0.55 and 0.65, each decision's level `-0.25 + 1.5p`, 0.575 and
    /// 0.725, and no side call.
    #[rstest]
    fn a_two_time_trains_meta_learns_from_the_sides_decisions_over_its_window() {
        let dir = tempfile::tempdir().unwrap();
        let (models, _, _) = two_time(dir.path(), never_with_meta());

        let meta = models
            .read(at("2026-01-08T00:00:00Z"))
            .unwrap()
            .meta
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
    /// three: the train fails, the meta with no decision of the sides to
    /// learn from, and publishes no generation.
    #[rstest]
    fn the_sides_failing_in_a_two_time_train_publish_no_generation() {
        let dir = tempfile::tempdir().unwrap();
        let three = ModelConfig {
            min_rows: 3,
            ..never_with_meta()
        };

        let (models, trained, _) = two_time(dir.path(), three);

        for outcome in trained.iter() {
            let Outcome::Kept(Kept::Failed(why)) = outcome else {
                panic!("{trained:?}");
            };
            assert!(why.contains("fit slice") && why.contains("2 rows"), "{why}");
        }
        assert_eq!(models.generations().unwrap(), []);
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
