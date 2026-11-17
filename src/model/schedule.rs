//! When a strategy's models are trained, and what each train does: as
//! its `Retrain` says (`ModelConfig::retrain`), every interval, on the
//! fixed UTC grid of the interval's multiples since the Unix epoch,
//! whatever the run start, in a backtest as live; or never, a generation
//! trained once, when a train finds none in use, and kept. At each train,
//! what is done (`Plan`).
//!
//! Retrained and never retrained differ in the arms of `Retrain` in this
//! module alone, so no caller tells them apart: the section's validation
//! (`Retrain::validate`), a run's train times (`Schedule::times`), the
//! current generation at a train's time (`Retrain::current`), how a model
//! kept in use reads (`Retrain::in_use`), and the retrain times of a train
//! over a span, none never retrained (`Schedule::retrain_times`). The model
//! store reads it once more, as a key: generations of another retrain, or
//! of none, are another store's (`store`); and a live node once, building the retrain
//! actor of a strategy only if it is retrained
//! (`launcher::RetrainActor::new`).
//!
//! The grid is kept by a Nautilus timer, which `start` sets on a clock
//! from an epoch-aligned time (`last_grid_time`): Nautilus fires it at
//! that time plus whole intervals. Live, the retrain actor of a strategy,
//! one per strategy retrained, sets it on the actor's own clock at its
//! start, which fires it as it goes (`launcher`); a train over a span,
//! and a backtest's periods, take the times the same timer fires over the
//! span on a virtual clock of its own (`Schedule::times`). Never retrained,
//! a live node has no retrain actor, so no timer is set, and the strategy
//! is trained at one time alone (`train --at`).
//!
//! A train at `t` is planned (`Schedule::plan`): a generation current at
//! `t` in the store (retrained, the one valid exactly from `t`; never
//! retrained, the latest valid at or before `t`) is kept, in use, whole;
//! else the train trains a generation at `t`, the same in both modes and
//! whatever else the store holds: its sides fitted as of `t`, on the rows
//! of their window back from `t` settled by then, and, with a meta, an
//! ancestor pair of the sides fitted alike as of `t` less the meta's
//! window, whose picks over the meta's window are the meta's rows, out of
//! sample (`train`). The plan answers where the train's one replay starts
//! (`Fits::replay_from`), at the first row a fit reads, the ancestor's with
//! a meta: a kept generation reads none, its baselines its own, published
//! with it (`store::ModelManifest::baseline`).

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

/// How a strategy's models are retrained: every interval, or never, one
/// generation trained and kept. A `[model]` section gives the
/// interval, such as `retrain = "168h"`; without one, never.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(from = "SignedDuration")]
pub enum Retrain {
    /// At every multiple of the interval since the Unix epoch.
    Every(SignedDuration),
    /// Never: one generation is trained, when a train finds none in use,
    /// and kept.
    #[default]
    Never,
}

/// When a strategy trains, and what a train does to each of its models:
/// its retrain, and the windows of its models' rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Schedule {
    retrain: Retrain,
    /// The sides' window.
    sides: SignedDuration,
    /// The meta model's window, if there is one.
    meta: Option<SignedDuration>,
}

/// A value for each model of a strategy: each side, and its meta model;
/// none for a model it has not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Models<T> {
    pub long: Option<T>,
    pub short: Option<T>,
    pub meta: Option<T>,
}

/// What a train at a time does (`Schedule::plan`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Plan {
    /// Keeps the generation valid from `valid_from`, current at the train,
    /// whole.
    Keep { valid_from: Timestamp },
    /// Trains a generation valid from the train's time.
    Train(Fits),
}

/// What a train fits of the generation it trains at its time `at`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fits {
    /// The train's time, which the generation is valid from.
    pub at: Timestamp,
    /// The sides' fit, as of `at`.
    pub sides: Fit,
    /// With a meta, the fit of the ancestor pair, the sides fitted as of
    /// the meta's window before `at`, whose picks over `[its time, at)` are
    /// the meta's rows; not published.
    pub ancestor: Option<Fit>,
}

/// A fit as of `at`, on the rows of `[from, at)` settled by `at`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fit {
    pub from: Timestamp,
    pub at: Timestamp,
}

impl Schedule {
    /// The schedule of a `[model]` section, and of its meta model if it
    /// has one.
    #[must_use]
    pub fn of_model(config: &ModelConfig) -> Self {
        Self {
            retrain: config.retrain,
            sides: config.window,
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

    /// The retrain times of a span `[from, to)` a train over it trains at:
    /// those of a run over it (`times`), a retrain time at `to` not among
    /// them, its models deciding no row of the span.
    ///
    /// # Errors
    ///
    /// Returns an error if the strategy is never retrained, so has no
    /// retrain time and trains at one time alone, or as `times`.
    pub fn retrain_times(&self, from: Timestamp, to: Timestamp) -> anyhow::Result<Vec<Timestamp>> {
        match self.retrain {
            Retrain::Every(every) => times(every, from, to),
            Retrain::Never => anyhow::bail!(
                "the strategy is never retrained ([model] retrain): it has no retrain times over \
                 {from} to {to}, and trains at one time alone"
            ),
        }
    }

    /// The plan of a train at `at`, the store holding generations valid
    /// from `generations`: the generation current at `at` (`Retrain::current`)
    /// kept, if any; else a generation trained at `at`, its sides fitted as
    /// of `at` on the rows of their window back from it, and, with a meta,
    /// its ancestor pair fitted alike as of the meta's window before `at`.
    /// The store is read for the generation kept alone: what a train fits
    /// is the same whatever else the store holds.
    #[must_use]
    pub fn plan(&self, at: Timestamp, generations: &[Timestamp]) -> Plan {
        if let Some(valid_from) = self.retrain.current(generations, at) {
            return Plan::Keep { valid_from };
        }
        let fit = |at: Timestamp| Fit {
            from: at - self.sides,
            at,
        };
        Plan::Train(Fits {
            at,
            sides: fit(at),
            ancestor: self.meta.map(|window| fit(at - window)),
        })
    }
}

impl Fits {
    /// Where the train's one replay starts: at the first row a fit reads,
    /// the ancestor's, a meta's window before the sides', if any.
    #[must_use]
    pub fn replay_from(&self) -> Timestamp {
        self.ancestor.unwrap_or(self.sides).from
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

    /// How a model a train kept in use reads, of its current generation
    /// valid from `valid_from`, already published, so not trained again:
    /// retrained, valid from then; never retrained, in use since then.
    #[must_use]
    pub fn in_use(self, valid_from: Timestamp) -> String {
        match self {
            Self::Every(_) => format!("already published, valid from {valid_from}"),
            Self::Never => {
                format!("already published, in use since {valid_from}, never retrained")
            }
        }
    }

    /// The current generation at `at` of a store holding generations valid
    /// from `valid_froms`: retrained, the one valid exactly from `at`; never
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

    /// The value of the model of `side`, mutably.
    pub const fn side_mut(&mut self, side: Side) -> Option<&mut T> {
        match side {
            Side::Long => self.long.as_mut(),
            Side::Short => self.short.as_mut(),
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
/// They are read off that timer rather than worked out: it is set as
/// `start` sets it (`Clock::set_timer_ns`, from the last grid time not
/// after the clock's now, with its `allow_past` and `fire_immediately`) on
/// a `VirtualClock`, which registers it as the live clock does
/// (`validate_and_prepare_timer`) and fires it without waiting, so a
/// train over a span trains at the times a live node retrains by Nautilus' own
/// timer semantics, which this module does not restate. A `VirtualTimer`
/// alone would skip that registration.
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
    use rstest::rstest;

    use super::*;

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    fn day() -> SignedDuration {
        SignedDuration::from_hours(24)
    }

    /// A run retrains at the midnight before its start and every midnight
    /// before its end: a start on the grid retrains at the start, and an
    /// end on it does not.
    #[rstest]
    #[case::off_the_grid(
        "2026-10-21T15:20:00Z",
        "2026-10-24T00:00:01Z",
        &[
            "2026-10-21T00:00:00Z",
            "2026-10-22T00:00:00Z",
            "2026-10-23T00:00:00Z",
            "2026-10-24T00:00:00Z",
        ]
    )]
    #[case::on_the_grid(
        "2026-10-21T00:00:00Z",
        "2026-10-23T00:00:00Z",
        &["2026-10-21T00:00:00Z", "2026-10-22T00:00:00Z"]
    )]
    fn retrains_sit_on_midnights_from_the_last_before_the_start_to_the_end(
        #[case] from: &str,
        #[case] to: &str,
        #[case] expected: &[&str],
    ) {
        let times = times(day(), at(from), at(to)).unwrap();

        assert_eq!(
            times,
            expected.iter().map(|time| at(time)).collect::<Vec<_>>()
        );
    }

    #[rstest]
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

    #[rstest]
    #[case::after_a_multiple("2026-10-22T13:00:00Z", "2026-10-22T00:00:00Z")]
    #[case::on_a_multiple("2026-10-22T00:00:00Z", "2026-10-22T00:00:00Z")]
    #[case::just_before_a_multiple("2026-10-21T23:59:59Z", "2026-10-21T00:00:00Z")]
    fn the_grid_time_is_the_last_multiple_since_the_epoch(#[case] now: &str, #[case] last: &str) {
        let grid_time = last_grid_time(UnixNanos::from(at(now)), SignedDuration::from_hours(24));

        assert_eq!(Timestamp::from(grid_time.unwrap()), at(last));
    }

    #[rstest]
    fn a_non_positive_interval_is_refused() {
        let err = times(
            SignedDuration::ZERO,
            at("2026-10-21T00:00:00Z"),
            at("2026-10-22T00:00:00Z"),
        )
        .unwrap_err();

        assert!(err.to_string().contains("0s"), "{err}");
    }

    fn hours(hours: i64) -> SignedDuration {
        SignedDuration::from_hours(hours)
    }

    /// Sides trained on a window of 24 hours, their meta on one of 48,
    /// retrained every day.
    fn daily() -> Schedule {
        Schedule {
            retrain: Retrain::Every(day()),
            sides: hours(24),
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

    /// `schedule` without a meta.
    fn without_meta(schedule: Schedule) -> Schedule {
        Schedule {
            meta: None,
            ..schedule
        }
    }

    /// The valid-froms of a store's generations.
    fn generations(times: &[&str]) -> Vec<Timestamp> {
        times.iter().map(|time| at(time)).collect()
    }

    fn fit(from: &str, to: &str) -> Fit {
        Fit {
            from: at(from),
            at: at(to),
        }
    }

    /// The train at `at` fitting the sides as `sides` and the ancestor pair
    /// as `ancestor`.
    fn train(at: &str, sides: Fit, ancestor: Option<Fit>) -> Plan {
        Plan::Train(Fits {
            at: self::at(at),
            sides,
            ancestor,
        })
    }

    fn keep(valid_from: &str) -> Plan {
        Plan::Keep {
            valid_from: at(valid_from),
        }
    }

    const FOURTH: &str = "2026-01-04T00:00:00Z";
    const FIFTH: &str = "2026-01-05T00:00:00Z";
    const SIXTH: &str = "2026-01-06T00:00:00Z";
    const SEVENTH: &str = "2026-01-07T00:00:00Z";
    const EIGHTH: &str = "2026-01-08T00:00:00Z";

    /// Retrained, the generation current at the train is the one valid
    /// exactly from it, kept whole; one valid before it is not current, and
    /// a generation is trained: its sides on their window of 24 hours back
    /// from the train, and its ancestor pair as of the 5th, the meta's window
    /// of 48 hours before the train, on its window of 24 hours back from
    /// there.
    #[rstest]
    #[case::of_the_train(&[SIXTH, SEVENTH], keep(SEVENTH))]
    #[case::of_the_day_before(&[SIXTH], train(SEVENTH, fit(SIXTH, SEVENTH), Some(fit(FOURTH, FIFTH))))]
    fn retrained_the_generation_valid_from_the_train_is_kept_and_one_before_it_is_not(
        #[case] valid_froms: &[&str],
        #[case] plan: Plan,
    ) {
        assert_eq!(daily().plan(at(SEVENTH), &generations(valid_froms)), plan);
    }

    /// A train with a meta fits the same ancestor pair whatever the store
    /// holds but the generation it keeps: on an empty store, one of a
    /// generation after the train, or one of the generations before it, the
    /// ancestor as of the 5th on its window back from there, the sides as of
    /// the 7th; and never retrained alike, with no generation by the train.
    #[rstest]
    #[case::retrained_on_an_empty_store(daily(), &[])]
    #[case::retrained_on_a_store_of_a_generation_after_the_train(daily(), &[EIGHTH])]
    #[case::retrained_on_a_store_of_generations_before_the_train(daily(), &[FOURTH, SIXTH])]
    #[case::never_retrained_on_an_empty_store(never(), &[])]
    #[case::never_retrained_on_a_store_of_a_generation_after_the_train(never(), &[EIGHTH])]
    fn a_train_with_a_meta_fits_its_ancestor_as_of_the_metas_window_before_it_whatever_the_store_holds(
        #[case] schedule: Schedule,
        #[case] valid_froms: &[&str],
    ) {
        assert_eq!(
            schedule.plan(at(SEVENTH), &generations(valid_froms)),
            train(SEVENTH, fit(SIXTH, SEVENTH), Some(fit(FOURTH, FIFTH)))
        );
    }

    /// Without a meta, a train fits the sides alone.
    #[rstest]
    #[case::retrained(daily())]
    #[case::never_retrained(never())]
    fn without_a_meta_a_train_fits_the_sides_alone(#[case] schedule: Schedule) {
        assert_eq!(
            without_meta(schedule).plan(at(SEVENTH), &[]),
            train(SEVENTH, fit(SIXTH, SEVENTH), None)
        );
    }

    /// The replay starts at the first row a fit reads: the ancestor's
    /// window from the 4th, or, without a meta, the sides' from the 6th.
    #[rstest]
    #[case::with_a_meta(daily(), FOURTH)]
    #[case::without_a_meta(without_meta(daily()), SIXTH)]
    fn the_replay_starts_at_the_first_row_fitted(#[case] schedule: Schedule, #[case] from: &str) {
        let Plan::Train(fits) = schedule.plan(at(SEVENTH), &[]) else {
            panic!("no train");
        };

        assert_eq!(fits.replay_from(), at(from));
    }

    #[rstest]
    fn a_run_never_retrained_trains_at_its_start_alone() {
        let times = never()
            .times(at("2026-10-21T15:20:00Z"), at("2026-10-24T00:00:01Z"))
            .unwrap();

        assert_eq!(times, [at("2026-10-21T15:20:00Z")]);
    }

    /// Never retrained, the generation current at the train is the latest
    /// valid at or before it, the 6th's, not the later one of the 8th.
    #[rstest]
    fn never_retrained_the_latest_generation_valid_by_the_train_is_kept() {
        let plan = never().plan(at(SEVENTH), &generations(&[FIFTH, SIXTH, EIGHTH]));

        assert_eq!(plan, keep(SIXTH));
    }

    /// Retrained, a span's retrain times are a run's over it; never
    /// retrained, a strategy has none, trained at one time alone.
    #[rstest]
    fn a_spans_retrain_times_are_a_runs_and_never_retrained_it_has_none() {
        let (from, to) = (at("2026-01-06T12:00:00Z"), at(EIGHTH));

        let retrained = daily().retrain_times(from, to).unwrap();
        let err = never().retrain_times(from, to).unwrap_err().to_string();

        assert_eq!(retrained, [at(SIXTH), at(SEVENTH)]);
        assert!(err.contains("never retrained"), "{err}");
    }
}
