//! Training at a time: the one operation that turns a strategy's dataset
//! into published models, or keeps the current ones, for the binary's
//! `train`, at one time or at each retrain time of a span
//! (`Training::retrain_times`), and live retrains alike; a backtest trains
//! nothing (`walk`).
//!
//! A strategy has a model for each side it trades (the sides of its
//! margins, `ModelConfig::margins`): each a regression of its side's
//! realized net R, the trade's net P&L over its unit
//! (`earnings::Example::realized`), the long's of the long of the target,
//! the short's of its short, both on the same rows, window and calibration,
//! each reading a row's features then its own side's payoff features
//! (`dataset::DatasetRow::side`), under the names its model store checks it
//! against (`store::SideLevel::feature_names`); and its meta model, if it
//! has one (`model::meta`). A train publishes them as one generation
//! (`store::StoredGeneration`), valid from its time, or nothing: a decision
//! uses the sides and the meta of one train together.
//!
//! What a train at `t` does is the schedule's plan (`schedule::Schedule::plan`),
//! retrained or never retrained alike: a generation the store holds valid
//! exactly from `t` is kept, already published, and replays nothing; else a
//! generation is
//! trained at `t`, its sides fitted as of `t` and, with a meta, its meta at
//! `t` on the picks of an ancestor pair fitted as of `t` less the meta's
//! window. Nothing else of the store is read: the generation a train
//! publishes depends on the data, the config and `t` alone.
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
//! With a meta model (`model::meta`), its rows come from an ancestor pair
//! the same train fits, on the same rows, of the one replay: the sides
//! fitted as of `t - Wm`, the meta's window before `t`
//! (`MetaConfig::window`), as a train at `t - Wm` fits them, on its window
//! back from there and split alike, held to the sides' minimum, a slice
//! short of it failing the train naming the ancestor. The ancestor decides
//! every row of `[t - Wm, t)` settled by `t` through the decision path, its
//! picks before any meta (`deciding::DecidingModels::pick_among`): those
//! rows are out of sample for it, each after its time, and the picks are
//! the meta's rows (`meta::examples`), whatever the store holds. The meta is
//! a regression of the realized net R of the trade of the side picked,
//! split as the sides are on its own calibration slice
//! (`MetaConfig::calibration`), calibrated by its level line alone
//! (`calibration::Ksplit` of one side), held to its own minimum of rows
//! (`MetaConfig::min_rows`) and checked as a side is, its manifest recording
//! its ancestor's time (`store::ModelManifest::ancestor`). The ancestor is
//! not published: the generation ships the sides fitted as of `t` beside
//! the meta, so its sides are as fresh as a strategy's without a meta, and
//! every generation of a strategy with a meta holds one.
//!
//! A train never errors: what goes wrong is an outcome, its failure with
//! the reason (`Failure`), nothing published and the current generation
//! deciding on. It succeeds when it did not fail (`Trained::succeeded`). Beside its outcome, a train holds the figures it
//! reached, failing or not (`Reached`): the days of the dataset it read,
//! built or cached, and the rows of each slice it counted, of its sides, its
//! ancestor and its meta, beside their minimum (`model::report`).

use std::{fmt::Display, path::PathBuf};

use anyhow::Context;
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;

use super::{
    blueprint::Stored,
    calibration::{self, Ksplit},
    dataset::{Dataset, DatasetRow, DaysRead, SchemaMismatch},
    deciding::{self, DecidingModels},
    earnings::Example,
    learner::{LearnerConfig, SavedModel, TrainingSet},
    meta,
    schedule::{Fit, Fits, Models, Plan, Schedule},
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
    /// The strategy's models: the generations the plan reads for the one it
    /// keeps, and where the train publishes.
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
    /// The time a meta's ancestor pair was fitted as of; none of a side's.
    ancestor: Option<Timestamp>,
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

/// The sides' pair a train fitted, in hand to decide on, the ancestor's,
/// and what of it a generation publishes: each side's level and model as
/// fitted, and the pair's calibration (`Pair::calibration`).
struct PairFit<'a> {
    pair: Pair,
    sides: Vec<(&'a Level, Fitted)>,
}

/// What a train did, and the figures it reached doing it.
#[derive(Debug, Clone, PartialEq)]
pub struct Trained {
    pub outcome: Outcome,
    pub reached: Reached,
}

/// What a train did to the strategy's generations.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// A generation was published, valid from the train's time, in the
    /// directory `generation`.
    Published { generation: PathBuf },
    /// None was fitted: the generation valid from the train's time is
    /// already published (`schedule::Plan::Keep`).
    AlreadyPublished,
    /// The train failed, publishing no generation: the current one, if
    /// any, stays.
    Failed(Failure),
}

/// The figures a train reached, as far as it went: none of a train whose
/// generation is already published, which reads no dataset and fits
/// nothing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Reached {
    /// The days of the dataset it read, once read.
    pub days: Option<DaysRead>,
    /// The slices of the sides fitted as of its time, once counted.
    pub sides: Option<Slices>,
    /// The slices of the ancestor pair, with a meta, once counted.
    pub ancestor: Option<Slices>,
    /// The slices of the meta, once counted.
    pub meta: Option<Slices>,
}

/// The rows of a fit's two slices, and the fewest each must hold, its
/// level's `min_rows`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Slices {
    pub fit: usize,
    pub calibration: usize,
    pub min_rows: usize,
}

/// Why a train failed.
#[derive(Debug, Clone, PartialEq)]
pub enum Failure {
    /// The replay, the dataset, a fit, a check of the models or the store
    /// failed, or a slice had fewer rows than the minimum; the error, the
    /// same for every model of the generation, whichever it was of.
    Error(String),
    /// The dataset holds rows that do not match the strategy's features; the
    /// mismatch.
    SchemaMismatch(String),
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
    /// the store's generations: the generation valid from `at` already
    /// published, if any; else a generation trained and published
    /// (`generation`), or,
    /// should anything of it fail, the train's failure with the error,
    /// beside the figures the train reached. `replay` runs the strategy
    /// over a span of the days the dataset lacks and returns the rows it
    /// settled (see `Dataset::settled`), once, from where the plan's fits
    /// start, and not at all if the plan keeps a generation.
    pub fn train_at(
        &self,
        at: Timestamp,
        replay: impl FnOnce(Timestamp, Timestamp) -> anyhow::Result<Vec<TargetRow>>,
    ) -> Trained {
        let mut reached = Reached::default();
        let outcome = match self.models.generations() {
            Err(e) => failed(&e),
            Ok(generations) => match self.schedule.plan(at, &generations) {
                Plan::Keep => Outcome::AlreadyPublished,
                Plan::Train(fits) => self.generation(&fits, replay, &mut reached).map_or_else(
                    |e| failed(&e),
                    |generation| Outcome::Published { generation },
                ),
            },
        };
        Trained { outcome, reached }
    }

    /// Trains the generation of `fits` and publishes it, valid from the
    /// train's time: on the rows `replay` settles from where the fits start,
    /// its meta, if the strategy has one, learned from the ancestor's picks
    /// (`meta`), then its sides' pair fitted as of the train's time
    /// (`pair`), each side checked as a reader checks it
    /// (`Training::stored`). Each model is published; the generation's
    /// directory is returned. What it reached goes in `reached` as it goes.
    ///
    /// # Errors
    ///
    /// Returns an error if the replay, a fit, a check or the store fails, or
    /// a slice has fewer rows than its level's minimum, the ancestor's and
    /// the meta's naming it.
    fn generation(
        &self,
        fits: &Fits,
        replay: impl FnOnce(Timestamp, Timestamp) -> anyhow::Result<Vec<TargetRow>>,
        reached: &mut Reached,
    ) -> anyhow::Result<PathBuf> {
        let at = fits.at;
        let (rows, days) = self.dataset.settled(fits.replay_from(), at, replay)?;
        reached.days = Some(days);
        let meta = fits
            .ancestor
            .zip(self.levels.meta.as_ref())
            .map(|(ancestor, level)| self.meta(level, ancestor, at, &rows, reached))
            .transpose()?;
        let pair = self.pair(fits.sides, &rows, &mut reached.sides)?;
        let calibration = pair.pair.calibration();
        let sides = pair
            .sides
            .into_iter()
            .map(|(level, fitted)| self.stored(level, fitted))
            .collect::<anyhow::Result<_>>()?;
        self.models.publish(&StoredGeneration {
            valid_from: at,
            sides,
            calibration,
            meta,
        })
    }

    /// The meta model of `level` of a train at `at`, learned from the
    /// ancestor pair fitted as `ancestor` says on `rows` (`pair`): its
    /// examples the ancestor's picks, before any meta, on the rows of
    /// `[its time, at)` settled by `at` (`meta::examples`), fitted as of `at`
    /// (`Level::fit`) and checked as a reader checks it (`Training::stored`),
    /// the ancestor's slices and the meta's counted in `reached`.
    ///
    /// # Errors
    ///
    /// Returns an error if the ancestor's fit fails, naming it, a model
    /// fails to predict, or the meta's fit or check fails, naming the meta.
    fn meta(
        &self,
        level: &Level,
        ancestor: Fit,
        at: Timestamp,
        rows: &[DatasetRow],
        reached: &mut Reached,
    ) -> anyhow::Result<StoredModel> {
        let pair = self
            .pair(ancestor, rows, &mut reached.ancestor)
            .with_context(|| format!("fitting the ancestor pair as of {}", ancestor.at))?
            .pair;
        let window = in_fit(
            rows,
            Fit {
                from: ancestor.at,
                at,
            },
        );
        let examples = meta::examples(&window, |sides| {
            DecidingModels::pick_among(&pair, deciding::margins(&self.models), sides)
        })
        .context("making the meta model's rows")?;
        let fitted = level
            .fit(at, ancestor.at, examples, &mut reached.meta)
            .context("fitting the meta model")?;
        self.stored(level, fitted)
            .context("checking the meta model")
    }

    /// The pair of the sides fitted as `fit` says on `rows`, those of its
    /// span settled by its time (`in_fit`), split into the fit slice and the
    /// calibration slice (`split`), counted in `slices`, each of at least the
    /// minimum of rows (`Level::ensure_rows`): each side's model fitted on its realized R on
    /// the fit slice (`fit_side`), the pair calibrated together on the
    /// calibration slice (`Ksplit::fit`), every prediction there and the
    /// calibration finite; each side's model as fitted, probed on rows of
    /// the calibration slice.
    ///
    /// # Errors
    ///
    /// Returns an error if a slice has fewer rows than the minimum, a side's
    /// fit fails, naming the side, a model fails to predict or predicts no
    /// finite number, or the calibration is no finite line.
    fn pair(
        &self,
        fit: Fit,
        rows: &[DatasetRow],
        slices: &mut Option<Slices>,
    ) -> anyhow::Result<PairFit<'_>> {
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
            .ensure_rows((cut, at), fitted.len(), calibrating.len(), slices)?;
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
                ancestor: None,
                baseline: fit.model.baseline(),
                saved: fit.saved,
                probes,
            };
            sides.push((fit.level, fitted));
            models.push((fit.side, fit.model));
        }
        Ok(PairFit {
            pair: Pair::new(models, calibration)?,
            sides,
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
                ancestor: fitted.ancestor,
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
    /// the fit slice first. Both counts and the minimum go in `slices`
    /// first, refused or not.
    ///
    /// # Errors
    ///
    /// Returns an error if a slice has fewer rows than the minimum.
    fn ensure_rows(
        &self,
        (cut, at): (Timestamp, Timestamp),
        fitted: usize,
        calibrating: usize,
        slices: &mut Option<Slices>,
    ) -> anyhow::Result<()> {
        let min_rows = self.min_rows;
        *slices = Some(Slices {
            fit: fitted,
            calibration: calibrating,
            min_rows,
        });
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

    /// The meta model fitted as of `at` on `examples`, in time order, the
    /// picks of the ancestor pair fitted as of `ancestor`: a regression of
    /// each trade's realized R on its fit slice (`split`), of
    /// at least the minimum of rows as its calibration slice is
    /// (`ensure_rows`, counted in `slices`), calibrated by its level line alone (`Ksplit` of one
    /// side) on its calibration slice, every prediction there and the line
    /// finite, its baseline the training mean of R on both slices.
    ///
    /// # Errors
    ///
    /// Returns an error if a slice has fewer rows than the minimum, a label
    /// is no finite number, the learner fails, a prediction on the
    /// calibration slice is no finite number, or the line is not finite.
    fn fit(
        &self,
        at: Timestamp,
        ancestor: Timestamp,
        examples: Vec<Example>,
        slices: &mut Option<Slices>,
    ) -> anyhow::Result<Fitted> {
        let cut = at - self.calibration;
        let (fitted, calibrating) = split(examples, cut, |row| (row.ts_event, row.settled_at));
        self.ensure_rows((cut, at), fitted.len(), calibrating.len(), slices)?;
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
            ancestor: Some(ancestor),
            baseline,
            probes,
        })
    }
}

impl Trained {
    /// Whether the train succeeded: it did not fail, its generation
    /// published or already published.
    #[must_use]
    pub const fn succeeded(&self) -> bool {
        !matches!(self.outcome, Outcome::Failed(_))
    }
}

/// A train that reached no figure.
impl From<Outcome> for Trained {
    fn from(outcome: Outcome) -> Self {
        Self {
            outcome,
            reached: Reached::default(),
        }
    }
}

/// The failure of the error `e`: a schema mismatch, or any other error.
fn failed(e: &anyhow::Error) -> Outcome {
    Outcome::Failed(match e.downcast_ref::<SchemaMismatch>() {
        Some(mismatch) => Failure::SchemaMismatch(mismatch.to_string()),
        None => Failure::Error(format!("{e:#}")),
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

impl Display for Outcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Published { generation } => {
                write!(f, "published the generation {}", generation.display())
            }
            Self::AlreadyPublished => write!(f, "already published"),
            Self::Failed(failure) => write!(f, "{failure}"),
        }
    }
}

impl Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Error(why) => write!(f, "failed: {why}"),
            Self::SchemaMismatch(why) => write!(f, "schema mismatch: {why}"),
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
            features::{FeatureSet, meta_names, side_names, side_row},
            learner::LearnerConfig,
            schedule::Retrain,
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

    /// The outcome of a train publishing the generation of a training in
    /// `dir` as `config` says valid from `valid_from`, named by its time in
    /// the store's directory.
    fn published(dir: &Path, config: &ModelConfig, valid_from: Timestamp) -> Outcome {
        Outcome::Published {
            generation: models(dir, config).directory().join(valid_from.to_string()),
        }
    }

    /// The slices of `fit` and `calibration` rows, of a minimum of
    /// `min_rows`.
    fn slices(fit: usize, calibration: usize, min_rows: usize) -> Slices {
        Slices {
            fit,
            calibration,
            min_rows,
        }
    }

    /// The days read of `built` built and `cached` cached.
    fn days(built: usize, cached: usize) -> DaysRead {
        DaysRead { built, cached }
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
    /// them, before the calibration: the fake's, their feature. The train
    /// read the 6th, its window, built, and counted its fit slice's 2 rows,
    /// at 08:00 and 10:00, and its calibration slice's 2, at 13:00 and
    /// 15:00.
    #[rstest]
    fn a_train_fits_on_rows_settled_by_the_calibrations_start_and_calibrates_on_the_rest_settled_by_its_time()
     {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let t = at("2026-01-07T00:00:00Z");

        let trained = training.train_at(t, |_, _| Ok(rows()));

        assert_eq!(
            trained,
            Trained {
                outcome: published(dir.path(), &config(), t),
                reached: Reached {
                    days: Some(days(1, 0)),
                    sides: Some(slices(2, 2, 1)),
                    ..Reached::default()
                },
            }
        );
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

        let Outcome::Failed(Failure::Error(why)) = &trained.outcome else {
            panic!("{trained:?}");
        };
        assert!(
            why.contains("long") && why.contains("inf") && why.contains("2026-01-06T15:00:00"),
            "{why}"
        );
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

        let Outcome::Failed(Failure::Error(why)) = &trained.outcome else {
            panic!("{trained:?}");
        };
        assert!(
            why.contains("calibration") && why.contains("not finite"),
            "{why}"
        );
        assert_eq!(generations(dir.path(), &config()), []);
    }

    /// The short's stop on a row fitted on is worth nought, so its R there is
    /// no finite number and its fit fails. The long, fitted, publishes
    /// nothing: the train fails for the short's failure, which it names,
    /// and the generation of the 6th stays the current one.
    #[rstest]
    fn a_side_whose_fit_fails_publishes_no_generation() {
        let dir = tempfile::tempdir().unwrap();
        let sixth = "2026-01-06T00:00:00Z";
        publish_generation(dir.path(), &config(), sixth, (0.0, 0.0));
        let mut rows = rows();
        rows[1].outcomes.short.unit = Money::from("0 USD");

        let trained =
            training(dir.path(), config()).train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows));

        let Outcome::Failed(Failure::Error(why)) = &trained.outcome else {
            panic!("{trained:?}");
        };
        assert!(why.contains("short") && why.contains("finite"), "{why}");
        assert_eq!(generations(dir.path(), &config()), [at(sixth)]);
    }

    /// The generation's publish fails: the train fails for the store,
    /// which it names, and no generation is listed.
    #[rstest]
    fn a_publish_failing_publishes_no_generation() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let models = models(dir.path(), &config());
        models.break_publishing();

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows()));

        let Outcome::Failed(Failure::Error(why)) = &trained.outcome else {
            panic!("{trained:?}");
        };
        assert!(
            why.contains(&models.directory().display().to_string()),
            "{why}"
        );
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
            matches!(trained.outcome, Outcome::Published { .. }),
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

        assert!(
            trained.outcome == published(dir.path(), &config(), t),
            "{trained:?}"
        );
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
        assert_eq!(trained, Trained::from(Outcome::AlreadyPublished));
    }

    /// `config`, never retrained.
    fn never() -> ModelConfig {
        ModelConfig {
            retrain: Retrain::Never,
            ..config()
        }
    }

    /// Never retrained as retrained, a generation valid before the train,
    /// of the 6th, is not the train's: the train of the 7th replays its
    /// window and publishes a generation valid from the 7th beside it.
    #[rstest]
    fn never_retrained_a_generation_valid_before_the_train_does_not_stop_it() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), never());
        let (sixth, seventh) = (at("2026-01-06T00:00:00Z"), at("2026-01-07T00:00:00Z"));
        publish_generation(dir.path(), &never(), "2026-01-06T00:00:00Z", (0.5, 0.5));
        let mut replays = vec![];

        let trained = training.train_at(seventh, |from, to| {
            replays.push((from, to));
            Ok(rows())
        });

        assert_eq!(trained.outcome, published(dir.path(), &never(), seventh));
        assert_eq!(replays, [(sixth, seventh)]);
        assert_eq!(generations(dir.path(), &never()), [sixth, seventh]);
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
            matches!(trained.outcome, Outcome::Published { .. }),
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
            matches!(trained.outcome, Outcome::Published { .. }),
            "{trained:?}"
        );
        assert!(models(dir.path(), &long_only).side(Side::Short).is_none());
    }

    #[rstest]
    fn a_failed_replay_keeps_the_current_models() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| {
            anyhow::bail!("the catalog does not cover the window")
        });

        let Outcome::Failed(Failure::Error(why)) = &trained.outcome else {
            panic!("{trained:?}");
        };
        assert!(why.contains("does not cover"), "{why}");
        assert_eq!(generations(dir.path(), &config()), []);
    }

    /// No row of the calibration slice settled by the train, the win at
    /// 23:30 settling after it: of a minimum of one row, the train fails,
    /// naming the calibration slice, its 0 rows and the minimum, and no
    /// generation is published. It reached the 6th, built, and the counts of
    /// both slices, the fit slice's 2 rows at 08:00 and 10:00.
    #[rstest]
    fn a_calibration_slice_of_fewer_rows_than_the_minimum_fails() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let mut rows = rows();
        rows.drain(4..6);

        let trained = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows));

        let Outcome::Failed(Failure::Error(why)) = &trained.outcome else {
            panic!("{trained:?}");
        };

        for fact in ["calibration slice", "0 rows", "min_rows 1"] {
            assert!(why.contains(fact), "{fact}: {why}");
        }
        assert_eq!(
            trained.reached,
            Reached {
                days: Some(days(1, 0)),
                sides: Some(slices(2, 0, 1)),
                ..Reached::default()
            }
        );
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

        assert!(
            matches!(trained.outcome, Outcome::Failed(Failure::SchemaMismatch(_))),
            "{trained:?}"
        );
        assert_eq!(generations(dir.path(), &config()), []);
    }

    /// The section of `config` with a meta model on the fake learner, on a
    /// window of `window` hours, the last 12 calibrated on, taking a trade
    /// from an expected 0.5R, each slice of at least one row.
    fn with_meta(window: i64) -> ModelConfig {
        ModelConfig {
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
    /// calibration leaving them as they are (`unchanged`), and, with a meta,
    /// a meta baseline predicting nought, calibrated as it is.
    fn publish_generation(
        dir: &Path,
        config: &ModelConfig,
        valid_from: &str,
        (long, short): (f64, f64),
    ) {
        let fingerprint = blueprint(config.clone()).fingerprint().unwrap().to_string();
        let baseline = |names: Vec<String>, side: Option<Side>, r| {
            let set = TrainingSet::new(names.len(), vec![0.5; names.len()], vec![r]).unwrap();
            let model = LearnerConfig::Baseline {}.fit(&set).unwrap();
            StoredModel {
                manifest: ModelManifest {
                    fingerprint: fingerprint.clone(),
                    feature_names: names,
                    calibration: side.is_none().then_some(Line {
                        alpha: 0.0,
                        beta: 1.0,
                    }),
                    target: config.target.clone(),
                    side,
                    learner: LearnerConfig::Baseline {},
                    baseline: r,
                    ancestor: None,
                    probes: vec![],
                },
                model: model.save().unwrap(),
            }
        };
        let names = side_names(features().names(), &config.target.payoff_names());
        let sides = config
            .margins
            .sides()
            .map(|(traded, _)| {
                let r = match traded {
                    Side::Long => long,
                    Side::Short => short,
                };
                baseline(names.clone(), Some(traded), r)
            })
            .collect();
        let meta = config
            .meta
            .as_ref()
            .map(|_| baseline(meta_names(features().names()), None, 0.0));
        models(dir, config)
            .publish(&StoredGeneration {
                valid_from: at(valid_from),
                sides,
                calibration: unchanged(config),
                meta,
            })
            .unwrap();
    }

    /// `with_meta` on a meta window of 24 hours, retrained every day, its
    /// sides traded from `config`'s margins, the long from 1R, the short
    /// from 0.5R.
    fn ancestral() -> ModelConfig {
        with_meta(24)
    }

    /// `ancestral`, never retrained.
    fn never_ancestral() -> ModelConfig {
        ModelConfig {
            retrain: Retrain::Never,
            ..ancestral()
        }
    }

    /// The decisions of the 7th, of the sides as `rows` of the 6th fit them
    /// (`a_train_fits_on_rows_settled_by_the_calibrations_start_and_calibrates_on_the_rest_settled_by_its_time`):
    /// each side expecting `-0.25 + 1.5p` R on the feature `p` (`LINES`),
    /// alike, so that the short, from 0.5R, is picked alone below 1R, the
    /// long's margin; each side's stop worth 2 USD, both sides winning 1.5R
    /// or losing -1R together. The fit slice of a train at the 8th, shorts
    /// at 0.6 (0.65R expected), a win and a loss; its calibration slice, a
    /// short at 0.55 (0.575R) losing and one at 0.65 (0.725R) winning, of
    /// those levels and no side call.
    fn decisions_of_the_seventh() -> Vec<TargetRow> {
        let short = |time, p, wins| row_of(time, vec![p], (wins, wins), hour());
        vec![
            short("2026-01-07T08:00:00Z", 0.6, true),
            short("2026-01-07T10:00:00Z", 0.6, false),
            short("2026-01-07T13:00:00Z", 0.55, false),
            short("2026-01-07T15:00:00Z", 0.65, true),
        ]
    }

    /// The rows of the 6th and the 7th, `rows` and `decisions_of_the_seventh`.
    fn two_days() -> Vec<TargetRow> {
        rows()
            .into_iter()
            .chain(decisions_of_the_seventh())
            .collect()
    }

    /// The train at the 8th of `config` in `dir`, on the store as it is,
    /// replaying `rows`: the training's models, what it did, and the spans
    /// it replayed.
    fn at_the_eighth(
        dir: &Path,
        config: ModelConfig,
        rows: Vec<TargetRow>,
    ) -> (ModelStore, Trained, Vec<(Timestamp, Timestamp)>) {
        let models = models(dir, &config);
        let training = training(dir, config);
        let mut replays = vec![];
        let trained = training.train_at(at("2026-01-08T00:00:00Z"), |from, to| {
            replays.push((from, to));
            Ok(rows)
        });
        (models, trained, replays)
    }

    /// The meta model of the generation of the 8th of `models`.
    fn meta_of_the_eighth(models: &ModelStore) -> StoredModel {
        models
            .read(at("2026-01-08T00:00:00Z"))
            .unwrap()
            .meta
            .unwrap()
    }

    /// The meta's calibration on the decisions of the 7th, worked by hand:
    /// its fake predicting 0.55 and 0.65 on its calibration slice, against R
    /// of -1 and 1.5, means 0.6 and 0.25, a slope of 0.125 / 0.005 = 25,
    /// clipped to 1.5, and `alpha` 0.25 - 1.5 x 0.6 = -0.65.
    const META_LINE: Line = Line {
        alpha: -0.65,
        beta: 1.5,
    };

    /// On an empty store, retrained or never retrained alike, the train of
    /// the 8th with a meta of 24 hours fits the ancestor pair as of the 7th
    /// on its window back from there, the 6th, as a train at the 7th would
    /// (`LINES`), and the meta on the ancestor's picks over the 7th: each a
    /// short, its level `-0.25 + 1.5p`, 0.575 and 0.725 on the calibration
    /// slice, and no side call. Fitted on the win at 23:30 of the 6th,
    /// settled after the 7th, the ancestor's `alpha_C` would be 0.2833, both
    /// sides clearing their margins alike, a tie picking none; fitted on the
    /// 7th, it would be the sides of the 8th. The meta, a regression of the
    /// R of the side picked on its fit slice, the 7th to noon, 1.5 and -1, of
    /// a mean of 0.25, which the fake predicts where its feature is missing,
    /// calibrated on the rest (`META_LINE`), its probes the calibration
    /// slice's shorts and its predictions on them, before the calibration. A
    /// first train publishes a whole generation, its meta included, having
    /// built the 6th and the 7th and counted 2 rows in each slice of the
    /// ancestor, the meta and the sides of the 8th, the 7th's before and
    /// from noon.
    #[rstest]
    #[case::retrained(ancestral())]
    #[case::never_retrained(never_ancestral())]
    fn a_train_with_a_meta_fits_its_meta_on_the_picks_of_an_ancestor_fitted_a_meta_window_before_it(
        #[case] config: ModelConfig,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let eighth = at("2026-01-08T00:00:00Z");

        let (models, trained, _) = at_the_eighth(dir.path(), config, two_days());

        assert_eq!(
            trained.outcome,
            Outcome::Published {
                generation: models.directory().join(eighth.to_string())
            },
            "{trained:?}"
        );
        assert_eq!(
            trained.reached,
            Reached {
                days: Some(days(2, 0)),
                sides: Some(slices(2, 2, 1)),
                ancestor: Some(slices(2, 2, 1)),
                meta: Some(slices(2, 2, 1)),
            }
        );
        assert_eq!(models.generations().unwrap(), [eighth]);
        let meta = meta_of_the_eighth(&models);
        assert_eq!(
            meta.manifest.feature_names,
            ["mid_0", "picked_side", "level", "side_call"]
        );
        assert_eq!(meta.manifest.side, None);
        let line = meta.manifest.calibration.unwrap();
        close(line.alpha, META_LINE.alpha);
        close(line.beta, META_LINE.beta);
        let probes: Vec<(&Vec<f64>, f64)> = meta
            .manifest
            .probes
            .iter()
            .map(|p| (&p.row, p.prediction))
            .collect();
        let [(first, at_first), (second, at_second)] = probes[..] else {
            panic!("{probes:?}");
        };
        assert_eq!(
            (first[0], first[1], first[3], at_first),
            (0.55, -1.0, 0.0, 0.55)
        );
        close(first[2], 0.575);
        assert_eq!(
            (second[0], second[1], second[3], at_second),
            (0.65, -1.0, 0.0, 0.65)
        );
        close(second[2], 0.725);
        let model = LearnerConfig::Fake {}.load(&meta.model).unwrap();
        assert_eq!(model.predict(&[f64::NAN, -1.0, 0.6, 0.0]).unwrap(), 0.25);
    }

    /// The meta's rows are the ancestor's picks over the meta's window
    /// alone: its baseline, the training mean of R on the rows it was fitted
    /// and calibrated on, is that of the 7th's four shorts, 1.5, -1, -1 and
    /// 1.5, 0.25; with the ancestor's pick at 15:00 of the 6th, a short of
    /// 0.875R winning 1.5R, on which the ancestor was calibrated, it would
    /// be 0.5.
    #[rstest]
    fn a_metas_rows_are_its_ancestors_picks_over_its_window_alone() {
        let dir = tempfile::tempdir().unwrap();

        let (models, _, _) = at_the_eighth(dir.path(), ancestral(), two_days());

        assert_eq!(meta_of_the_eighth(&models).manifest.baseline, 0.25);
    }

    /// The sides published are fitted as of the train, the 8th, on their
    /// window back from it, the 7th, not as of the ancestor's time: their
    /// probes the 7th's calibration slice, at 0.55 and 0.65, and their
    /// calibration, worked as `META_LINE` is on the same rows and R, `alpha_C`
    /// -0.65 and a slope of 1.5, no side call, where the ancestor's is
    /// `LINES`. The meta's manifest records its ancestor's time, the 7th, and
    /// no side's has one.
    #[rstest]
    fn a_generation_with_a_meta_ships_sides_fitted_at_its_time_and_its_meta_records_its_ancestors()
    {
        let dir = tempfile::tempdir().unwrap();
        let (models, _, _) = at_the_eighth(dir.path(), ancestral(), two_days());

        let generation = models.read(at("2026-01-08T00:00:00Z")).unwrap();

        let calibration = generation.calibration;
        close(calibration.level.alpha, -0.65);
        close(calibration.level.beta, 1.5);
        assert_eq!(calibration.side_call, Some(0.0));

        for side in Side::BOTH {
            let stored = side_of(&generation, side);
            let probes: Vec<f64> = stored.manifest.probes.iter().map(|p| p.row[0]).collect();
            assert_eq!(probes, [0.55, 0.65], "{side:?}");
            assert_eq!(stored.manifest.ancestor, None, "{side:?}");
        }
        assert_eq!(
            generation.meta.unwrap().manifest.ancestor,
            Some(at("2026-01-07T00:00:00Z"))
        );
    }

    /// A train reads nothing of the store but the generation it would keep:
    /// on a store holding the generations of the 6th, picking the long of
    /// 1.2R, and of the 7th, the short of 0.9R, each with its meta, the
    /// train of the 8th publishes the very generation it publishes on an
    /// empty store, its meta learning from its ancestor's shorts alone.
    #[rstest]
    fn a_trains_generation_does_not_depend_on_the_generations_stored_before_it() {
        let (empty, stored) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        for (valid_from, rs) in [
            ("2026-01-06T00:00:00Z", (1.2, 0.0)),
            ("2026-01-07T00:00:00Z", (0.0, 0.9)),
        ] {
            publish_generation(stored.path(), &ancestral(), valid_from, rs);
        }
        let eighth = at("2026-01-08T00:00:00Z");

        let (on_empty, _, _) = at_the_eighth(empty.path(), ancestral(), two_days());
        let (on_stored, _, _) = at_the_eighth(stored.path(), ancestral(), two_days());

        assert_eq!(
            on_stored.read(eighth).unwrap(),
            on_empty.read(eighth).unwrap()
        );
    }

    /// One replay serves the ancestor, the meta and the sides: from the
    /// ancestor's window back from the 7th, the 6th, to the train, the 8th.
    #[rstest]
    fn a_train_with_a_meta_replays_once_from_its_ancestors_window_to_its_time() {
        let dir = tempfile::tempdir().unwrap();

        let (_, _, replays) = at_the_eighth(dir.path(), ancestral(), two_days());

        assert_eq!(
            replays,
            [(at("2026-01-06T00:00:00Z"), at("2026-01-08T00:00:00Z"))]
        );
    }

    /// The ancestor's fit slice, the 6th's wins and losses before noon, two
    /// rows, short of a minimum of three, while the sides of the 8th have
    /// three in each slice, the 7th's shorts at 09:00 and 14:00 added: the
    /// train fails, naming the ancestor, its slice, its count and the
    /// minimum, and publishes no generation. It reached the 6th and the
    /// 7th, built, and the ancestor's slices, 2 rows each, no slice of the
    /// meta's or the sides', which it never fitted.
    #[rstest]
    fn an_ancestor_slice_of_fewer_rows_than_the_sides_minimum_fails_the_train_naming_it() {
        let dir = tempfile::tempdir().unwrap();
        let three = ModelConfig {
            min_rows: 3,
            ..ancestral()
        };
        let short = |time, wins| row_of(time, vec![0.6], (wins, wins), hour());
        let mut rows = two_days();
        rows.extend([
            short("2026-01-07T09:00:00Z", true),
            short("2026-01-07T14:00:00Z", false),
        ]);

        let (models, trained, _) = at_the_eighth(dir.path(), three, rows);

        let Outcome::Failed(Failure::Error(why)) = &trained.outcome else {
            panic!("{trained:?}");
        };

        for fact in [
            "ancestor",
            "2026-01-07T00:00:00Z",
            "fit slice",
            "2 rows",
            "min_rows 3",
        ] {
            assert!(why.contains(fact), "{fact}: {why}");
        }
        assert_eq!(
            trained.reached,
            Reached {
                days: Some(days(2, 0)),
                ancestor: Some(slices(2, 2, 3)),
                ..Reached::default()
            }
        );
        assert_eq!(models.generations().unwrap(), []);
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
    /// with no exits too: the rows of the 6th and the 7th, each closed at
    /// its time, of a unit of 1 USD, a win 3R and a loss -2R. The ancestor's
    /// calibration slice, a loss at 0.25 and a win at 0.75, means 0.5 and
    /// 0.5, a slope of 1.25 / 0.125 = 10, clipped to 1.5, and `alpha_C` 0.5 -
    /// 1.5 x 0.5 = -0.25, `LINES` again, so it picks the 7th's four shorts:
    /// the meta's baseline their mean R, 3, -2, -2 and 3, 0.5.
    #[rstest]
    fn a_meta_model_on_a_time_only_target_learns_from_its_ancestors_trades() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = ancestral();
        config.target.spec = Target::builder()
            .limit(SignedDuration::from_hours(1))
            .atr(crate::model::targets::DEFAULT_ATR)
            .build();

        let (models, trained, _) = at_the_eighth(dir.path(), config, at_their_time(two_days()));

        assert!(
            matches!(trained.outcome, Outcome::Published { .. }),
            "{trained:?}"
        );
        let manifest = meta_of_the_eighth(&models).manifest;
        assert_eq!(manifest.baseline, 0.5);
        assert_eq!(manifest.target.spec.bracket, None);
    }

    /// Margins of the long alone, from nought: the short's models are never
    /// asked, and the meta's rows are the ancestor's longs, each of the 7th
    /// expecting `-0.25 + 1.5p` of its level line alone, above nought; its
    /// calibration slice a loss at 0.55 and a win at 0.65, buys, of levels
    /// 0.575 and 0.725 and no side call.
    #[rstest]
    fn a_long_only_strategy_gives_its_meta_model_buys_alone() {
        let dir = tempfile::tempdir().unwrap();
        let long_only = ModelConfig {
            margins: Margins {
                long: Some(R::new(0.0)),
                short: None,
            },
            ..ancestral()
        };

        let (models, _, _) = at_the_eighth(dir.path(), long_only, two_days());

        let meta = meta_of_the_eighth(&models);
        let probes: Vec<&Vec<f64>> = meta.manifest.probes.iter().map(|p| &p.row).collect();
        let [first, second] = probes[..] else {
            panic!("{probes:?}");
        };
        assert_eq!((first[0], first[1], first[3]), (0.55, 1.0, 0.0));
        close(first[2], 0.575);
        assert_eq!((second[0], second[1], second[3]), (0.65, 1.0, 0.0));
        close(second[2], 0.725);
    }

    /// The meta's example at 15:00 of the 7th, of its calibration slice from
    /// noon, has an infinite feature, which the fake predicts: the meta's
    /// fit fails, naming the row.
    #[rstest]
    fn a_meta_prediction_on_its_calibration_slice_that_is_no_finite_number_fails_its_fit() {
        let level = Level {
            feature_names: meta_names(features().names()),
            calibration: SignedDuration::from_hours(12),
            min_rows: 1,
            learner: LearnerConfig::Fake {},
        };
        let example = |time, p| Example {
            features: vec![p, -1.0, 0.65, 0.0],
            ..Example::of_side(
                &DatasetRow::of(&row_of(time, vec![p], (true, true), hour())),
                Side::Short,
            )
        };
        let examples = vec![
            example("2026-01-07T08:00:00Z", 0.6),
            example("2026-01-07T13:00:00Z", 0.55),
            example("2026-01-07T15:00:00Z", f64::INFINITY),
        ];

        let err = level
            .fit(
                at("2026-01-08T00:00:00Z"),
                at("2026-01-07T00:00:00Z"),
                examples,
                &mut None,
            )
            .unwrap_err();

        let why = format!("{err:#}");
        assert!(
            why.contains("meta") && why.contains("inf") && why.contains("2026-01-07T15:00:00"),
            "{why}"
        );
    }

    #[rstest]
    fn a_train_succeeds_unless_it_failed() {
        let t = at("2026-01-07T00:00:00Z");
        let published = Outcome::Published {
            generation: PathBuf::from("models").join(t.to_string()),
        };
        let already = Outcome::AlreadyPublished;
        let failed = Outcome::Failed(Failure::Error("no short row".to_string()));

        assert!(Trained::from(published).succeeded());
        assert!(Trained::from(already).succeeded());
        assert!(!Trained::from(failed).succeeded());
    }

    /// A generation published reads with its directory; one already
    /// published as such; a failure as its reason.
    #[rstest]
    #[case::published(
        Outcome::Published { generation: PathBuf::from("models/2026-01-07T00:00:00Z") },
        &["published the generation models/2026-01-07T00:00:00Z"]
    )]
    #[case::failed(
        Outcome::Failed(Failure::Error("no short row".to_string())),
        &["failed: no short row"]
    )]
    #[case::already_published(Outcome::AlreadyPublished, &["already published"])]
    fn an_outcome_reads_as_the_generation_published_already_published_or_the_failure(
        #[case] outcome: Outcome,
        #[case] facts: &[&str],
    ) {
        let text = outcome.to_string();

        for fact in facts {
            assert!(text.contains(fact), "{fact}: {text}");
        }
    }
}
