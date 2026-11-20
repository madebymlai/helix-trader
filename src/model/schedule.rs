//! Decides when a strategy's models are trained and what each train does.
//!
//! A strategy retrains in one of two ways:
//! - Every interval, on the UTC grid of multiples since the Unix epoch, whatever the run start.
//! - Never. Only `train --at` trains a model set.
//!
//! Only the arms of `Retrain` tell them apart, so callers do not.
//!
//! The grid is a Nautilus timer set from an epoch-aligned time (`last_grid_time`). Live, the
//! retrain actor sets the timer on its own clock. A train over a span and a backtest's periods
//! read the times it fires on a virtual clock (`Schedule::times`).
//!
//! `Schedule::plan` keeps a model set valid exactly from the train's time, since a train is
//! deterministic. Otherwise it fits a new one, whatever else the store holds. A kept model set
//! reads no replay, because its baselines are published with it (`store::ModelManifest::baseline`).

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

/// The name of the timer `times` fires.
const GRID: &str = "RETRAIN";

/// How a strategy's models are retrained, such as `retrain = "168h"`.
///
/// The default is never.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(from = "SignedDuration")]
pub enum Retrain {
    /// At every multiple of the interval since the Unix epoch.
    Every(SignedDuration),
    /// Only `train --at` trains a model set.
    #[default]
    Never,
}

/// A strategy's retrain and the row windows of its models.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Schedule {
    retrain: Retrain,
    sides: SignedDuration,
    meta: Option<SignedDuration>,
}

/// A value for each model of a strategy.
///
/// A model the strategy lacks has `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Models<T> {
    pub long: Option<T>,
    pub short: Option<T>,
    pub meta: Option<T>,
}

/// What a train does (`Schedule::plan`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Plan {
    /// Keeps the published model set valid from the train's time.
    Keep,
    /// Trains a model set valid from the train's time.
    Train(Fits),
}

/// What a train fits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fits {
    /// The train's time. The model set is valid from it.
    pub at: Timestamp,
    /// The fit of the sides.
    pub sides: Fit,
    /// Set with a meta. Holds the sides fitted as of the meta's window before `at`. Their picks
    /// over `[its time, at)` are the meta's rows. Not published.
    pub out_of_fold: Option<Fit>,
}

/// A fit as of `at`.
///
/// It uses the rows in `[from, at)` that are settled by `at`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fit {
    pub from: Timestamp,
    pub at: Timestamp,
}

impl Schedule {
    /// Returns the schedule of a `[model]` section.
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

    /// Returns the train times of a run over `[from, to)`.
    ///
    /// These are the last grid time not after `from`, then each grid time before `to`. If the
    /// strategy is never retrained, the only time is `from`.
    ///
    /// # Errors
    ///
    /// Returns an error if `from` is not before `to`, or, retrained, as `times`.
    pub fn times(&self, from: Timestamp, to: Timestamp) -> anyhow::Result<Vec<Timestamp>> {
        match self.retrain {
            Retrain::Every(every) => times(every, from, to),
            Retrain::Never => {
                anyhow::ensure!(from < to, "the span {from} to {to} is empty");
                Ok(vec![from])
            }
        }
    }

    /// Returns the retrain times of a span `[from, to)`, as `times`.
    ///
    /// # Errors
    ///
    /// Returns an error if the strategy is never retrained, or as `times`.
    pub fn retrain_times(&self, from: Timestamp, to: Timestamp) -> anyhow::Result<Vec<Timestamp>> {
        match self.retrain {
            Retrain::Every(every) => times(every, from, to),
            Retrain::Never => anyhow::bail!(
                "the strategy is never retrained ([model] retrain): it has no retrain times over \
                 {from} to {to}, and trains at one time alone"
            ),
        }
    }

    /// Returns the plan of a train at `at`.
    ///
    /// `model_sets` holds the valid-froms of the model sets in the store.
    #[must_use]
    pub fn plan(&self, at: Timestamp, model_sets: &[Timestamp]) -> Plan {
        if model_sets.contains(&at) {
            return Plan::Keep;
        }
        let fit = |at: Timestamp| Fit {
            from: at - self.sides,
            at,
        };
        Plan::Train(Fits {
            at,
            sides: fit(at),
            out_of_fold: self.meta.map(|window| fit(at - window)),
        })
    }
}

impl Fits {
    /// Returns where the train's one replay starts, which is the first row a fit reads.
    #[must_use]
    pub fn replay_from(&self) -> Timestamp {
        self.out_of_fold.unwrap_or(self.sides).from
    }
}

impl Retrain {
    /// Pushes onto `errors` a violation of `model.retrain` if the interval is not positive.
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
}

impl From<SignedDuration> for Retrain {
    fn from(every: SignedDuration) -> Self {
        Self::Every(every)
    }
}

impl<T> Models<T> {
    /// Returns the value for the model of `side`.
    #[must_use]
    pub const fn side(&self, side: Side) -> Option<&T> {
        match side {
            Side::Long => self.long.as_ref(),
            Side::Short => self.short.as_ref(),
        }
    }

    /// Returns the value for the model of `side`, mutably.
    pub const fn side_mut(&mut self, side: Side) -> Option<&mut T> {
        match side {
            Side::Long => self.long.as_mut(),
            Side::Short => self.short.as_mut(),
        }
    }

    /// Sets the value for the model of `side`.
    pub fn set_side(&mut self, side: Side, value: T) {
        match side {
            Side::Long => self.long = Some(value),
            Side::Short => self.short = Some(value),
        }
    }
}

/// Starts the retrain timer `timer` on `actor`'s clock, first firing at the next grid time.
///
/// # Errors
///
/// Returns an error if the interval is not positive or Nautilus refuses the timer.
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

/// Returns the times the timer set by `start` fires over `[from, to)`, read off a `VirtualClock`.
///
/// The clock registers the timer as the live clock does (`validate_and_prepare_timer`). A bare
/// `VirtualTimer` would skip that.
///
/// # Errors
///
/// Returns an error if `every` is not positive, `from` is not before `to`, or a time is before the
/// epoch.
fn times(every: SignedDuration, from: Timestamp, to: Timestamp) -> anyhow::Result<Vec<Timestamp>> {
    anyhow::ensure!(from < to, "the span {from} to {to} is empty");
    let first = Timestamp::from(last_grid_time(UnixNanos::from(from), every)?);
    let mut clock = VirtualClock::new();
    clock.register_default_handler(TimeEventCallback::from(|_: TimeEvent| {}));

    // Started as `start` starts it, a grid time before the first, so the timer fires on it
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

/// Returns the last multiple of `every` since the epoch that is not after `now`.
///
/// # Errors
///
/// Returns an error if `every` is not positive, or longer than Nautilus' nanosecond durations.
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

        // Every third day since the epoch: 2026-10-19 is 20,745 days after 1970-01-01
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

    fn daily() -> Schedule {
        Schedule {
            retrain: Retrain::Every(day()),
            sides: hours(24),
            meta: Some(hours(48)),
        }
    }

    fn never() -> Schedule {
        Schedule {
            retrain: Retrain::Never,
            ..daily()
        }
    }

    fn without_meta(schedule: Schedule) -> Schedule {
        Schedule {
            meta: None,
            ..schedule
        }
    }

    fn model_sets(times: &[&str]) -> Vec<Timestamp> {
        times.iter().map(|time| at(time)).collect()
    }

    fn fit(from: &str, to: &str) -> Fit {
        Fit {
            from: at(from),
            at: at(to),
        }
    }

    fn train(at: &str, sides: Fit, out_of_fold: Option<Fit>) -> Plan {
        Plan::Train(Fits {
            at: self::at(at),
            sides,
            out_of_fold,
        })
    }

    const FOURTH: &str = "2026-01-04T00:00:00Z";
    const FIFTH: &str = "2026-01-05T00:00:00Z";
    const SIXTH: &str = "2026-01-06T00:00:00Z";
    const SEVENTH: &str = "2026-01-07T00:00:00Z";
    const EIGHTH: &str = "2026-01-08T00:00:00Z";

    #[rstest]
    #[case::retrained_of_the_train(daily(), &[SIXTH, SEVENTH], Plan::Keep)]
    #[case::retrained_of_the_day_before(daily(), &[SIXTH], train(SEVENTH, fit(SIXTH, SEVENTH), Some(fit(FOURTH, FIFTH))))]
    #[case::never_retrained_of_the_train(never(), &[SIXTH, SEVENTH], Plan::Keep)]
    #[case::never_retrained_of_the_day_before(never(), &[FIFTH, SIXTH, EIGHTH], train(SEVENTH, fit(SIXTH, SEVENTH), Some(fit(FOURTH, FIFTH))))]
    fn the_model_set_valid_from_the_train_is_kept_and_one_before_it_is_not(
        #[case] schedule: Schedule,
        #[case] valid_froms: &[&str],
        #[case] plan: Plan,
    ) {
        assert_eq!(schedule.plan(at(SEVENTH), &model_sets(valid_froms)), plan);
    }

    #[rstest]
    #[case::retrained_on_an_empty_store(daily(), &[])]
    #[case::retrained_on_a_store_of_a_model_set_after_the_train(daily(), &[EIGHTH])]
    #[case::retrained_on_a_store_of_model_sets_before_the_train(daily(), &[FOURTH, SIXTH])]
    #[case::never_retrained_on_an_empty_store(never(), &[])]
    #[case::never_retrained_on_a_store_of_a_model_set_after_the_train(never(), &[EIGHTH])]
    fn a_train_with_a_meta_fits_its_out_of_fold_pair_as_of_the_metas_window_before_it_whatever_the_store_holds(
        #[case] schedule: Schedule,
        #[case] valid_froms: &[&str],
    ) {
        assert_eq!(
            schedule.plan(at(SEVENTH), &model_sets(valid_froms)),
            train(SEVENTH, fit(SIXTH, SEVENTH), Some(fit(FOURTH, FIFTH)))
        );
    }

    #[rstest]
    #[case::retrained(daily())]
    #[case::never_retrained(never())]
    fn without_a_meta_a_train_fits_the_sides_alone(#[case] schedule: Schedule) {
        assert_eq!(
            without_meta(schedule).plan(at(SEVENTH), &[]),
            train(SEVENTH, fit(SIXTH, SEVENTH), None)
        );
    }

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

    #[rstest]
    fn a_spans_retrain_times_are_a_runs_and_never_retrained_it_has_none() {
        let (from, to) = (at("2026-01-06T12:00:00Z"), at(EIGHTH));

        let retrained = daily().retrain_times(from, to).unwrap();
        let err = never().retrain_times(from, to).unwrap_err().to_string();

        assert_eq!(retrained, [at(SIXTH), at(SEVENTH)]);
        assert!(err.contains("never retrained"), "{err}");
    }
}
