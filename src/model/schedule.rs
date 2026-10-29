//! When a strategy's model is retrained: every `ModelConfig::retrain`, on
//! the fixed UTC grid of the interval's multiples since the Unix epoch,
//! whatever the run start, in a backtest as live.
//!
//! The grid is kept by a Nautilus timer, `RETRAIN_TIMER`, which `start`
//! sets on a clock from an epoch-aligned time: Nautilus fires it at that
//! time plus whole intervals. A live node's clock fires it as it goes; a
//! backtest, which trains before its run, takes the times the same timer
//! fires over its span on a virtual clock of its own (`times`). Each time
//! is trained at once: `missing` gives those the store has no model valid
//! from.

use anyhow::Context;
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    clock::{Clock, VirtualClock},
    timer::{TimeEvent, TimeEventCallback},
};
use nautilus_core::{DurationNanos, UnixNanos};

/// The name of the timer of the retrains.
pub const RETRAIN_TIMER: &str = "RETRAIN";

/// Starts the retrain timer of `every` on `clock`, from the last grid time
/// not after now, so it first fires on the next one.
///
/// # Errors
///
/// Returns an error if `every` is not positive or Nautilus refuses the
/// timer.
pub fn start(clock: &mut dyn Clock, every: SignedDuration) -> anyhow::Result<()> {
    let now = Timestamp::from(clock.timestamp_ns());
    clock.set_timer_ns(
        RETRAIN_TIMER,
        DurationNanos::try_from(every)?,
        Some(UnixNanos::from(last(every, now)?)),
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
pub fn times(
    every: SignedDuration,
    from: Timestamp,
    to: Timestamp,
) -> anyhow::Result<Vec<Timestamp>> {
    anyhow::ensure!(from < to, "the span {from} to {to} is empty");
    let first = last(every, from)?;
    let mut clock = VirtualClock::new();
    clock.register_default_handler(TimeEventCallback::from(|_: TimeEvent| {}));
    // Started a grid time before the first, the timer fires on it.
    let before = first - SignedDuration::from_nanos(1);
    clock.advance_time(UnixNanos::from(before), true);
    start(&mut clock, every)?;
    let events = clock.advance_time(UnixNanos::from(to - SignedDuration::from_nanos(1)), true);
    Ok(events
        .into_iter()
        .filter(|event| event.name == RETRAIN_TIMER)
        .map(|event| Timestamp::from(event.ts_event))
        .collect())
}

/// The times of `times` no model of `published` (the valid-froms of the
/// store) is valid from, in the order of `times`.
#[must_use]
pub fn missing(times: &[Timestamp], published: &[Timestamp]) -> Vec<Timestamp> {
    times
        .iter()
        .copied()
        .filter(|time| !published.contains(time))
        .collect()
}

/// The last grid time of `every` not after `now`.
fn last(every: SignedDuration, now: Timestamp) -> anyhow::Result<Timestamp> {
    anyhow::ensure!(
        every.is_positive(),
        "the retrain interval {every:#} is not positive"
    );
    let (every, now) = (every.as_nanos(), now.as_nanosecond());
    let last = now.div_euclid(every) * every;
    Timestamp::from_nanosecond(last).with_context(|| format!("no grid time at {last} ns"))
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
    fn the_timer_started_live_fires_on_the_next_grid_time() {
        let mut clock = VirtualClock::new();
        clock.register_default_handler(TimeEventCallback::from(|_: TimeEvent| {}));
        clock.advance_time(UnixNanos::from(at("2026-10-21T15:20:00Z")), true);

        start(&mut clock, SignedDuration::from_hours(6)).unwrap();

        assert_eq!(
            clock.next_time_ns(RETRAIN_TIMER).map(Timestamp::from),
            Some(at("2026-10-21T18:00:00Z"))
        );
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

    #[test]
    fn the_times_missing_are_those_the_store_has_no_model_valid_from() {
        let times = [
            at("2026-10-21T00:00:00Z"),
            at("2026-10-22T00:00:00Z"),
            at("2026-10-23T00:00:00Z"),
        ];
        let published = [at("2026-10-20T00:00:00Z"), at("2026-10-22T00:00:00Z")];

        assert_eq!(
            missing(&times, &published),
            [at("2026-10-21T00:00:00Z"), at("2026-10-23T00:00:00Z")]
        );
    }
}
