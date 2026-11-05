//! When a strategy's models are trained, and what each train does: as
//! its `Retrain` says (`ModelConfig::retrain`), every interval, on the
//! fixed UTC grid of the interval's multiples since the Unix epoch,
//! whatever the run start, in a backtest as live; or never, each model
//! trained once, when a train finds none of it in use, and kept. At each
//! train, what is done to each model (`Plan`).
//!
//! Retrained and never retrained differ in the arms of `Retrain` in this
//! module alone, so no caller tells them apart: the section's validation
//! (`Retrain::validate`), a run's train times (`Schedule::times`), the
//! current model at a train's time (`Retrain::current`), how a model kept
//! in use reads (`Retrain::in_use`), and the refusal of a backtest that
//! would trade nothing (`Schedule::ensure_trades`). The model store reads
//! it once more, as a key: models of another retrain, or of none, are
//! another store's (`store`); and a live node once, building the retrain
//! actor of a strategy only if it is retrained
//! (`launcher::RetrainActor::new`).
//!
//! The grid is kept by a Nautilus timer, which `start` sets on a clock
//! from an epoch-aligned time (`last_grid_time`): Nautilus fires it at
//! that time plus whole intervals. Live, the retrain actor of a strategy,
//! one per strategy retrained, sets it on the actor's own clock at its
//! start, which fires it as it goes (`launcher`); a backtest, which trains
//! before its run, takes the times the same timer fires over its span on a
//! virtual clock of its own (`Schedule::times`). Never retrained, a live
//! node has no retrain actor, so no timer is set, and a backtest trains at
//! its start alone, then, should its primary or its meta have no model in
//! use there, is refused: it would trade nothing, and no train comes
//! after.
//!
//! A train at `t` is planned (`Schedule::plan`) from the valid-froms of
//! the stores alone, model by model (the long, the short and the meta,
//! `Models`): a model whose current model at `t` exists keeps it, in use
//! (retrained, the one valid exactly from `t`; never retrained, the
//! latest valid at or before `t`); the others are fitted as of `t`, on
//! the rows of their window back from `t` settled by then; but a model
//! primary's meta, whose rows are the primary's decisions, waits while no
//! primary model is valid before `t`, and its rows start at the primary's
//! first model at the earliest. Never retrained, a meta whose primary is
//! in use is so fitted alone, on the decisions of the very models it will
//! filter; and with neither the meta nor a side of a model primary in
//! use, one train trains both, at two times: the sides as of `t` less the
//! meta's window, on their window back from there, published valid from
//! then, and the meta at `t`, on their out-of-sample decisions over its
//! window, so that it filters the very models it learned from. The rule
//! is the current model's alone: it applies where a side fitted as of
//! the earlier time would be current at `t`, never so retrained, whose
//! models stay on the grid. Should those sides publish none, the meta
//! waits (`Plan::meta_after_sides`). The plan answers whether the train
//! fits anything and where its one replay starts (`Plan::replay_from`), at
//! the first row a fit reads: a kept model reads none, its baseline its
//! own, published with it (`store::ModelManifest::baseline`).

use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, DataActorNative},
    clock::{Clock, VirtualClock},
    config::{ConfigError, ConfigErrorCollector},
    timer::{TimeEvent, TimeEventCallback},
};
use nautilus_core::{DurationNanos, UnixNanos};
use serde::Deserialize;

use super::{config::ModelConfig, targets::Side};

/// The name of the timer `times` fires on its own clock.
const GRID: &str = "RETRAIN";

/// How a strategy's models are retrained: every interval, or never,
/// each model trained once and kept. A `[model]` section gives the
/// interval, such as `retrain = "168h"`; without one, never.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(from = "SignedDuration")]
pub enum Retrain {
    /// At every multiple of the interval since the Unix epoch.
    Every(SignedDuration),
    /// Never: each model is trained once, when a train finds none of it
    /// in use, and kept.
    #[default]
    Never,
}

/// When a strategy trains, and what a train does to each of its models:
/// its retrain, and the windows of its models' rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Schedule {
    retrain: Retrain,
    /// The window of the primary's sides.
    primary: SignedDuration,
    /// The meta model's window, if there is one.
    meta: Option<SignedDuration>,
}

/// A value for each model of a strategy: each side of a model primary,
/// and its meta model; none for a model it has not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Models<T> {
    pub long: Option<T>,
    pub short: Option<T>,
    pub meta: Option<T>,
}

/// What a train at a time does to one model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Keeps its current model, valid from `valid_from`.
    Keep { valid_from: Timestamp },
    /// Fits a model as of `at`, on the rows of `[from, at)` settled by
    /// `at`, and publishes it valid from `at`: the train's time, or the
    /// meta's window before it for a side of a train at two times.
    Fit { from: Timestamp, at: Timestamp },
    /// Fits none: a model primary's meta, whose rows are the primary's
    /// decisions, with no primary model valid before the train to make
    /// them.
    Wait,
}

/// What a train at a time does to each model (`Schedule::plan`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    at: Timestamp,
    steps: Models<Step>,
}

impl Schedule {
    /// The schedule of a model primary's section, and of its meta model if
    /// it has one.
    #[must_use]
    pub fn of_model(config: &ModelConfig) -> Self {
        Self {
            retrain: config.retrain,
            primary: config.window,
            meta: config.meta.as_ref().map(|meta| meta.window),
        }
    }

    /// How the strategy is retrained.
    #[must_use]
    pub const fn retrain(&self) -> Retrain {
        self.retrain
    }

    /// The train times of a run over `[from, to)`, in order: retrained, the
    /// last grid time not after `from`, whose models are those in use at
    /// the run's start, then each grid time before `to` (`times`); never
    /// retrained, `from` alone.
    ///
    /// # Errors
    ///
    /// Returns an error if `from` is not before `to`, or, retrained, as
    /// `times`.
    pub fn times(&self, from: Timestamp, to: Timestamp) -> anyhow::Result<Vec<Timestamp>> {
        match self.retrain {
            Retrain::Every(every) => times(every, from, to),
            Retrain::Never => {
                anyhow::ensure!(from < to, "the span {from} to {to} is empty");
                Ok(vec![from])
            }
        }
    }

    /// The plan of a train at `at`, the stores of the models holding
    /// models valid from `valid_froms`: a model keeps its current model at
    /// `at`, if any (`Retrain::current`), else is fitted as of `at` on the
    /// rows of its window back from `at`; but a model primary's meta waits
    /// while no primary model is valid before `at`, and its rows start at
    /// the primary's first model at the earliest, since a row before it
    /// has no decision of the primary. With no current model of the meta nor of a
    /// side of a model primary, the sides are fitted as of the meta's
    /// window before `at` instead, on their window back from there, if a
    /// model fitted then would be current at `at` (never so retrained):
    /// their decisions over the meta's window, out of sample, are the
    /// meta's rows, and the meta, fitted at `at`, filters the very models
    /// it learned from.
    #[must_use]
    pub fn plan(&self, at: Timestamp, valid_froms: &Models<Vec<Timestamp>>) -> Plan {
        let current = |valid_froms: &[Timestamp]| self.retrain.current(valid_froms, at);
        // The step of a model whose store holds models valid from
        // `valid_froms`: its current model at `at` kept, else one fitted as
        // of `at` on the rows of `window` back from it, from `first` at the
        // earliest.
        let step = |valid_froms: &[Timestamp], window: SignedDuration, first: Timestamp| {
            let from = (at - window).max(first);
            match current(valid_froms) {
                Some(valid_from) => Step::Keep { valid_from },
                None => Step::Fit { from, at },
            }
        };
        // The time the sides are fitted as of, if before `at`.
        let early = self.meta.map(|window| at - window).filter(|&early| {
            [&valid_froms.long, &valid_froms.short]
                .into_iter()
                .flatten()
                .all(|side| current(side).is_none())
                && valid_froms
                    .meta
                    .as_deref()
                    .is_some_and(|meta| current(meta).is_none())
                && current(&[early]).is_some()
        });
        let side = |valid_froms: &Option<Vec<Timestamp>>| {
            let valid_froms = valid_froms.as_deref()?;
            Some(match early {
                Some(early) => Step::Fit {
                    from: early - self.primary,
                    at: early,
                },
                None => step(valid_froms, self.primary, Timestamp::MIN),
            })
        };
        // The primary's first model before `at`, of either side, its fits
        // before `at` included: the meta waits without one.
        let first = first_before(at, primary(valid_froms).chain(early));
        let meta = || {
            let (window, meta) = (self.meta?, valid_froms.meta.as_deref()?);
            Some(match first {
                Some(first) => step(meta, window, first),
                None => Step::Wait,
            })
        };
        Plan {
            at,
            steps: Models {
                long: side(&valid_froms.long),
                short: side(&valid_froms.short),
                meta: meta(),
            },
        }
    }

    /// Refuses a run from `start` that could trade nothing, the stores of
    /// the models holding models valid from `valid_froms`: never
    /// retrained, no train comes after the start, so a model primary with
    /// no side's model in use there, or a meta with none, would trade
    /// nothing; retrained, a later retrain may publish one.
    ///
    /// # Errors
    ///
    /// Returns an error naming the primary, the meta, or both, with no
    /// model in use at `start`, never retrained.
    pub fn ensure_trades(
        &self,
        start: Timestamp,
        valid_froms: &Models<Vec<Timestamp>>,
    ) -> anyhow::Result<()> {
        if let Retrain::Every(_) = self.retrain {
            return Ok(());
        }
        let in_use =
            |valid_froms: &Vec<Timestamp>| self.retrain.current(valid_froms, start).is_some();
        let mut idle = vec![];
        if ![&valid_froms.long, &valid_froms.short]
            .into_iter()
            .flatten()
            .any(in_use)
        {
            idle.push("no side of the primary has a model in use");
        }
        if valid_froms.meta.as_ref().is_some_and(|meta| !in_use(meta)) {
            idle.push("the meta has no model in use");
        }
        anyhow::ensure!(
            idle.is_empty(),
            "never retrained, the strategy would trade nothing from {start}, where {}: no \
             train comes after it",
            idle.join(" and ")
        );
        Ok(())
    }
}

impl Plan {
    /// What the train does to each model.
    #[must_use]
    pub const fn steps(&self) -> &Models<Step> {
        &self.steps
    }

    /// The meta's step once the train fitted the sides, the stores of the
    /// models then holding models valid from `valid_froms`: as planned,
    /// but the meta waits while no primary model is valid before the
    /// train, as when the sides fitted before it published none.
    #[must_use]
    pub fn meta_after_sides(&self, valid_froms: &Models<Vec<Timestamp>>) -> Option<Step> {
        Some(match self.steps.meta? {
            Step::Fit { .. } if first_before(self.at, primary(valid_froms)).is_none() => Step::Wait,
            step => step,
        })
    }

    /// Whether the train fits a model.
    #[must_use]
    pub fn fits(&self) -> bool {
        self.replay_from().is_some()
    }

    /// Where the train's one replay starts: at the first row a fit reads;
    /// none if it fits no model.
    #[must_use]
    pub fn replay_from(&self) -> Option<Timestamp> {
        self.steps
            .iter()
            .filter_map(|step| match step {
                Step::Fit { from, .. } => Some(*from),
                Step::Keep { .. } | Step::Wait => None,
            })
            .min()
    }
}

impl Retrain {
    /// Pushes onto `errors` the violation of an interval not positive,
    /// under `model.retrain`.
    pub fn validate(self, errors: &mut ConfigErrorCollector) {
        if let Self::Every(every) = self {
            errors.check(
                every.is_positive(),
                ConfigError::invalid_value(
                    "model.retrain",
                    format!("the retrain interval {every:#} is not positive"),
                ),
            );
        }
    }

    /// How a model a train kept in use reads, its current model valid from
    /// `valid_from`: retrained, valid from then; never retrained, in use
    /// since then.
    #[must_use]
    pub fn in_use(self, valid_from: Timestamp) -> String {
        match self {
            Self::Every(_) => format!("in use, valid from {valid_from}"),
            Self::Never => format!("in use since {valid_from}, never retrained"),
        }
    }

    /// The current model at `at` of a store holding models valid from
    /// `valid_froms`: retrained, the one valid exactly from `at`; never
    /// retrained, the latest valid at or before `at`.
    fn current(self, valid_froms: &[Timestamp], at: Timestamp) -> Option<Timestamp> {
        match self {
            Self::Every(_) => valid_froms.contains(&at).then_some(at),
            Self::Never => valid_froms
                .iter()
                .copied()
                .filter(|&valid_from| valid_from <= at)
                .max(),
        }
    }
}

/// A retrain every `every`.
impl From<SignedDuration> for Retrain {
    fn from(every: SignedDuration) -> Self {
        Self::Every(every)
    }
}

impl<T> Models<T> {
    /// The value of the model of `side`.
    #[must_use]
    pub const fn side(&self, side: Side) -> Option<&T> {
        match side {
            Side::Long => self.long.as_ref(),
            Side::Short => self.short.as_ref(),
        }
    }

    /// Sets the value of the model of `side`.
    pub fn set_side(&mut self, side: Side, value: T) {
        match side {
            Side::Long => self.long = Some(value),
            Side::Short => self.short = Some(value),
        }
    }

    /// The value of each model, the long's, the short's, then the meta's.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        [&self.long, &self.short, &self.meta].into_iter().flatten()
    }

    /// The values by reference.
    #[must_use]
    pub const fn as_ref(&self) -> Models<&T> {
        Models {
            long: self.long.as_ref(),
            short: self.short.as_ref(),
            meta: self.meta.as_ref(),
        }
    }

    /// The value of `f` of each model's.
    pub fn map<U>(self, mut f: impl FnMut(T) -> U) -> Models<U> {
        Models {
            long: self.long.map(&mut f),
            short: self.short.map(&mut f),
            meta: self.meta.map(&mut f),
        }
    }
}

/// The valid-froms of the stores of the primary's sides, of
/// `valid_froms`.
fn primary(valid_froms: &Models<Vec<Timestamp>>) -> impl Iterator<Item = Timestamp> + '_ {
    valid_froms
        .long
        .iter()
        .chain(&valid_froms.short)
        .flatten()
        .copied()
}

/// The first of `valid_froms` before `at`.
fn first_before(at: Timestamp, valid_froms: impl Iterator<Item = Timestamp>) -> Option<Timestamp> {
    valid_froms.filter(|&valid_from| valid_from < at).min()
}

/// Starts the retrain timer `timer` on `actor`'s clock, firing every
/// `every` from the last grid time not after now, so it first fires on the
/// next one.
///
/// # Errors
///
/// Returns an error if the interval is not positive or Nautilus refuses
/// the timer.
pub fn start<A: DataActor + DataActorNative>(
    actor: &A,
    timer: &str,
    every: SignedDuration,
) -> anyhow::Result<()> {
    let clock = actor.clock();
    let start = last_grid_time(clock.timestamp_ns(), every)?;
    clock.set_timer_ns(
        timer,
        DurationNanos::try_from(every)?,
        Some(start),
        None,
        None,
        Some(true),
        Some(false),
    )
}

/// The retrain times of `every` a run over `[from, to)` uses, in order:
/// the last grid time not after `from`, whose model is valid at the run's
/// start, then each grid time before `to`, as the timer `start` sets
/// fires them.
///
/// # Errors
///
/// Returns an error if `every` is not positive, `from` is not before
/// `to`, or the times are before the epoch.
fn times(every: SignedDuration, from: Timestamp, to: Timestamp) -> anyhow::Result<Vec<Timestamp>> {
    anyhow::ensure!(from < to, "the span {from} to {to} is empty");
    let first = Timestamp::from(last_grid_time(UnixNanos::from(from), every)?);
    let mut clock = VirtualClock::new();
    clock.register_default_handler(TimeEventCallback::from(|_: TimeEvent| {}));
    // Started, as `start` starts it, a grid time before the first, the
    // timer fires on it.
    let before = UnixNanos::from(first - SignedDuration::from_nanos(1));
    clock.advance_time(before, true);
    clock.set_timer_ns(
        GRID,
        DurationNanos::try_from(every)?,
        Some(last_grid_time(before, every)?),
        None,
        None,
        Some(true),
        Some(false),
    )?;
    let events = clock.advance_time(UnixNanos::from(to - SignedDuration::from_nanos(1)), true);
    Ok(events
        .into_iter()
        .filter(|event| event.name == GRID)
        .map(|event| Timestamp::from(event.ts_event))
        .collect())
}

/// The last multiple of `every` since the epoch not after `now`.
///
/// # Errors
///
/// Returns an error if `every` is not positive, or longer than Nautilus'
/// nanosecond durations.
pub fn last_grid_time(now: UnixNanos, every: SignedDuration) -> anyhow::Result<UnixNanos> {
    anyhow::ensure!(
        every.is_positive(),
        "the retrain interval {every:#} is not positive"
    );
    Ok(now.floor(DurationNanos::try_from(every)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    fn day() -> SignedDuration {
        SignedDuration::from_hours(24)
    }

    #[test]
    fn retrains_sit_on_midnights_from_the_last_before_the_start_to_the_end() {
        let times = times(
            day(),
            at("2026-10-21T15:20:00Z"),
            at("2026-10-24T00:00:01Z"),
        )
        .unwrap();

        assert_eq!(
            times,
            [
                at("2026-10-21T00:00:00Z"),
                at("2026-10-22T00:00:00Z"),
                at("2026-10-23T00:00:00Z"),
                at("2026-10-24T00:00:00Z"),
            ]
        );
    }

    #[test]
    fn a_start_on_the_grid_retrains_at_the_start_and_an_end_on_it_does_not() {
        let times = times(
            day(),
            at("2026-10-21T00:00:00Z"),
            at("2026-10-23T00:00:00Z"),
        )
        .unwrap();

        assert_eq!(
            times,
            [at("2026-10-21T00:00:00Z"), at("2026-10-22T00:00:00Z")]
        );
    }

    #[test]
    fn the_grid_does_not_move_with_the_start() {
        let end = at("2026-10-27T00:00:00Z");
        let a = times(day() * 3, at("2026-10-20T01:00:00Z"), end).unwrap();
        let b = times(day() * 3, at("2026-10-22T13:00:00Z"), end).unwrap();

        // Every third day since the epoch: 1970-01-01 is a multiple, so is
        // 2026-10-19 (20,745 days later).
        assert_eq!(
            a,
            [
                at("2026-10-19T00:00:00Z"),
                at("2026-10-22T00:00:00Z"),
                at("2026-10-25T00:00:00Z"),
            ]
        );
        assert_eq!(b, a[1..]);
    }

    #[test]
    fn the_grid_time_is_the_last_multiple_since_the_epoch() {
        let last = |now: &str| {
            Timestamp::from(
                last_grid_time(UnixNanos::from(at(now)), SignedDuration::from_hours(24)).unwrap(),
            )
        };

        assert_eq!(last("2026-10-22T13:00:00Z"), at("2026-10-22T00:00:00Z"));
        assert_eq!(last("2026-10-22T00:00:00Z"), at("2026-10-22T00:00:00Z"));
        assert_eq!(last("2026-10-21T23:59:59Z"), at("2026-10-21T00:00:00Z"));
    }

    #[test]
    fn a_non_positive_interval_is_refused() {
        let err = times(
            SignedDuration::ZERO,
            at("2026-10-21T00:00:00Z"),
            at("2026-10-22T00:00:00Z"),
        )
        .unwrap_err();

        assert!(err.to_string().contains("retrain interval"), "{err}");
    }

    fn hours(hours: i64) -> SignedDuration {
        SignedDuration::from_hours(hours)
    }

    /// A model primary trained on a window of 24 hours, its meta on one of
    /// 48, retrained every day.
    fn daily() -> Schedule {
        Schedule {
            retrain: Retrain::Every(day()),
            primary: hours(24),
            meta: Some(hours(48)),
        }
    }

    /// `daily`'s models, never retrained.
    fn never() -> Schedule {
        Schedule {
            retrain: Retrain::Never,
            ..daily()
        }
    }

    /// The valid-froms of the stores of a model primary's long, its short
    /// and its meta.
    fn stores(long: &[&str], short: &[&str], meta: &[&str]) -> Models<Vec<Timestamp>> {
        let times = |times: &[&str]| Some(times.iter().map(|time| at(time)).collect());
        Models {
            long: times(long),
            short: times(short),
            meta: times(meta),
        }
    }

    fn fit(from: &str, to: &str) -> Step {
        Step::Fit {
            from: at(from),
            at: at(to),
        }
    }

    fn keep(valid_from: &str) -> Step {
        Step::Keep {
            valid_from: at(valid_from),
        }
    }

    const FOURTH: &str = "2026-01-04T00:00:00Z";
    const FIFTH: &str = "2026-01-05T00:00:00Z";
    const SIXTH: &str = "2026-01-06T00:00:00Z";
    const SEVENTH: &str = "2026-01-07T00:00:00Z";
    const EIGHTH: &str = "2026-01-08T00:00:00Z";

    #[test]
    fn on_an_empty_store_the_sides_fit_on_their_window_and_the_meta_waits() {
        let plan = daily().plan(at(SEVENTH), &stores(&[], &[], &[]));

        assert_eq!(
            plan.steps,
            Models {
                long: Some(fit(SIXTH, SEVENTH)),
                short: Some(fit(SIXTH, SEVENTH)),
                meta: Some(Step::Wait),
            }
        );
    }

    /// Retrained, the current model at the train is the one valid exactly
    /// from it: the long's of the 6th is not, its short's of the 7th is.
    #[test]
    fn a_model_valid_from_the_train_is_kept_and_one_valid_before_it_is_not() {
        let plan = daily().plan(
            at(SEVENTH),
            &stores(&[SIXTH], &[SIXTH, SEVENTH], &[SEVENTH]),
        );

        assert_eq!(plan.steps.long, Some(fit(SIXTH, SEVENTH)));
        assert_eq!(plan.steps.short, Some(keep(SEVENTH)));
        assert_eq!(plan.steps.meta, Some(keep(SEVENTH)));
    }

    /// The meta's window of 48 hours starts on the 5th: with the primary's
    /// first model of either side valid from 06:00 of the 6th, its rows
    /// start there; with one valid from the 4th, at the window's start.
    #[test]
    fn the_metas_rows_start_at_the_primarys_first_model_at_the_earliest() {
        for (long, short, from) in [
            (
                "2026-01-06T12:00:00Z",
                "2026-01-06T06:00:00Z",
                "2026-01-06T06:00:00Z",
            ),
            (
                "2026-01-04T00:00:00Z",
                "2026-01-06T06:00:00Z",
                "2026-01-05T00:00:00Z",
            ),
        ] {
            let plan = daily().plan(at(SEVENTH), &stores(&[long], &[short], &[]));

            assert_eq!(plan.steps.meta, Some(fit(from, SEVENTH)), "{long} {short}");
        }
    }

    /// A model primary's models valid from the train's time alone are no
    /// decisions before it: the meta waits.
    #[test]
    fn a_meta_waits_for_a_primary_model_before_the_train() {
        let plan = daily().plan(at(SEVENTH), &stores(&[SEVENTH], &[SEVENTH], &[]));

        assert_eq!(plan.steps.meta, Some(Step::Wait));
    }

    /// The sides fit on the 6th; the meta, kept, reads no row, though its
    /// window starts on the 5th, after the primary's first model, the 4th.
    #[test]
    fn the_replay_starts_at_the_first_row_fitted_a_kept_model_reading_none() {
        let first = "2026-01-04T00:00:00Z";

        let plan = daily().plan(at(SEVENTH), &stores(&[first], &[first], &[SEVENTH]));

        assert!(plan.fits());
        assert_eq!(plan.replay_from(), Some(at(SIXTH)));
    }

    #[test]
    fn a_plan_keeping_every_model_fits_none_and_replays_nothing() {
        let plan = daily().plan(
            at(SEVENTH),
            &stores(&[SIXTH, SEVENTH], &[SIXTH, SEVENTH], &[SEVENTH]),
        );

        assert!(!plan.fits());
        assert_eq!(plan.replay_from(), None);
    }

    /// A waiting meta reads no row: the replay starts at the sides' window.
    #[test]
    fn a_waiting_meta_does_not_replay_its_window() {
        let plan = daily().plan(at(SEVENTH), &stores(&[], &[], &[]));

        assert_eq!(plan.replay_from(), Some(at(SIXTH)));
    }

    #[test]
    fn a_run_never_retrained_trains_at_its_start_alone() {
        let times = never()
            .times(at("2026-10-21T15:20:00Z"), at("2026-10-24T00:00:01Z"))
            .unwrap();

        assert_eq!(times, [at("2026-10-21T15:20:00Z")]);
    }

    /// The valid-from of the model `step` keeps in use, if it keeps one.
    fn in_use(step: Option<Step>) -> Option<Timestamp> {
        match step? {
            Step::Keep { valid_from } => Some(valid_from),
            Step::Fit { .. } | Step::Wait => None,
        }
    }

    /// Never retrained, the current model at the train is the latest valid
    /// at or before it: the long's of the 6th, not its later one of the
    /// 8th; the short's of the 7th itself. The meta's only model, of the
    /// 8th, is after the train: it has none, and fits, from the long's
    /// first model, the 5th.
    #[test]
    fn never_retrained_the_latest_model_valid_by_the_train_is_kept_and_the_others_fitted() {
        let plan = never().plan(
            at(SEVENTH),
            &stores(&[FIFTH, SIXTH, EIGHTH], &[SEVENTH], &[EIGHTH]),
        );

        assert_eq!(in_use(plan.steps.long), Some(at(SIXTH)));
        assert_eq!(in_use(plan.steps.short), Some(at(SEVENTH)));
        assert_eq!(plan.steps.meta, Some(fit(FIFTH, SEVENTH)));
    }

    /// Never retrained, the primary's models of the 6th are in use at the
    /// train of the 7th: kept, they give the meta its rows from the 6th,
    /// and the meta fits alone.
    #[test]
    fn never_retrained_a_meta_whose_primary_is_in_use_fits_alone() {
        let plan = never().plan(at(SEVENTH), &stores(&[SIXTH], &[SIXTH], &[]));

        assert_eq!(in_use(plan.steps.long), Some(at(SIXTH)));
        assert_eq!(in_use(plan.steps.short), Some(at(SIXTH)));
        assert_eq!(plan.steps.meta, Some(fit(SIXTH, SEVENTH)));
    }

    /// Never retrained, with no model of the primary nor of the meta by
    /// the train of the 7th, on an empty store or one holding models of
    /// the 8th alone (published by a train from later): the sides fit as
    /// of the 5th, the meta's window of 48 hours before the train, on
    /// their window of 24 hours back from there, and the meta at the 7th,
    /// on its window, the decisions of the sides from the 5th.
    #[test]
    fn never_retrained_with_no_model_in_use_the_sides_fit_as_of_the_metas_window_before_the_train_and_the_meta_at_it()
     {
        for stores in [
            stores(&[], &[], &[]),
            stores(&[EIGHTH], &[EIGHTH], &[EIGHTH]),
        ] {
            let plan = never().plan(at(SEVENTH), &stores);

            assert_eq!(
                plan.steps,
                Models {
                    long: Some(fit(FOURTH, FIFTH)),
                    short: Some(fit(FOURTH, FIFTH)),
                    meta: Some(fit(FIFTH, SEVENTH)),
                },
                "{stores:?}"
            );
        }
    }

    /// The sides fitted as of the 5th read their window back from there:
    /// the one replay starts on the 4th.
    #[test]
    fn a_two_time_train_replays_from_the_sides_window_back_from_their_time() {
        let plan = never().plan(at(SEVENTH), &stores(&[], &[], &[]));

        assert_eq!(plan.replay_from(), Some(at(FOURTH)));
    }

    /// Retrained, a model fitted before the train would not be current at
    /// it, so whatever the stores hold every model fits as of the train,
    /// if at all: on an empty store or one of models after the train, the
    /// meta waiting; with the primary's models of the 5th, the meta too.
    #[test]
    fn retrained_every_model_fits_as_of_the_train() {
        for (stores, meta) in [
            (stores(&[], &[], &[]), Step::Wait),
            (stores(&[EIGHTH], &[EIGHTH], &[EIGHTH]), Step::Wait),
            (stores(&[FIFTH], &[FIFTH], &[]), fit(FIFTH, SEVENTH)),
        ] {
            let plan = daily().plan(at(SEVENTH), &stores);

            assert_eq!(
                plan.steps,
                Models {
                    long: Some(fit(SIXTH, SEVENTH)),
                    short: Some(fit(SIXTH, SEVENTH)),
                    meta: Some(meta),
                },
                "{stores:?}"
            );
        }
    }

    /// Never retrained, the long's model of the 6th in use: the short and
    /// the meta fit at the train, the meta on the long's decisions from the
    /// 6th.
    #[test]
    fn never_retrained_with_one_side_in_use_the_other_and_the_meta_fit_at_the_train() {
        let plan = never().plan(at(SEVENTH), &stores(&[SIXTH], &[], &[]));

        assert_eq!(
            plan.steps,
            Models {
                long: Some(keep(SIXTH)),
                short: Some(fit(SIXTH, SEVENTH)),
                meta: Some(fit(SIXTH, SEVENTH)),
            }
        );
    }

    /// Never retrained, the meta's model of the 6th in use: the sides fit
    /// at the train.
    #[test]
    fn never_retrained_with_the_meta_in_use_the_sides_fit_at_the_train() {
        let plan = never().plan(at(SEVENTH), &stores(&[], &[], &[SIXTH]));

        assert_eq!(plan.steps.long, Some(fit(SIXTH, SEVENTH)));
        assert_eq!(plan.steps.short, Some(fit(SIXTH, SEVENTH)));
    }

    /// The sides of a two-time train fitted as of the 5th: the meta fits
    /// as planned once either published, and waits, with no primary
    /// decision to learn from, if neither did.
    #[test]
    fn a_two_time_trains_meta_fits_once_a_side_published_and_waits_if_none_did() {
        let plan = never().plan(at(SEVENTH), &stores(&[], &[], &[]));

        assert_eq!(
            plan.meta_after_sides(&stores(&[], &[FIFTH], &[])),
            Some(fit(FIFTH, SEVENTH))
        );
        assert_eq!(
            plan.meta_after_sides(&stores(&[], &[], &[])),
            Some(Step::Wait)
        );
    }
}
