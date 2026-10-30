//! When a strategy's model is retrained: every `ModelConfig::retrain`, on
//! the fixed UTC grid of the interval's multiples since the Unix epoch,
//! whatever the run start, in a backtest as live.
//!
//! The grid is kept by a Nautilus timer, named after the strategy
//! (`timer`, so several strategies retrain in one process), which `start`
//! sets on a clock from an epoch-aligned time (`last_grid_time`): Nautilus
//! fires it at that time plus whole intervals. A live node's clock fires
//! it as it goes (`launcher`); a backtest, which trains before its run,
//! takes the times the same timer fires over its span on a virtual clock
//! of its own (`times`). Each time is trained at once: `missing` gives
//! those the store has no model valid from.

use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, DataActorNative},
    clock::{Clock, VirtualClock},
    timer::{TimeEvent, TimeEventCallback},
};
use nautilus_core::{DurationNanos, UnixNanos};
use nautilus_model::identifiers::StrategyId;

/// The name of the timer `times` fires on its own clock.
const GRID: &str = "RETRAIN";

/// The name of the retrain timer of `strategy_id`.
#[must_use]
pub fn timer(strategy_id: StrategyId) -> String {
    format!("RETRAIN-{strategy_id}")
}

/// Starts the retrain timer `timer` of `every` on `actor`'s clock, from
/// the last grid time not after now, so it first fires on the next one.
///
/// # Errors
///
/// Returns an error if `every` is not positive or Nautilus refuses the
/// timer.
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
pub fn times(
    every: SignedDuration,
    from: Timestamp,
    to: Timestamp,
) -> anyhow::Result<Vec<Timestamp>> {
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

/// The last multiple of `every` since the epoch not after `now`.
///
/// # Errors
///
/// Returns an error if `every` is not positive, or the time is before the
/// epoch.
pub fn last_grid_time(now: UnixNanos, every: SignedDuration) -> anyhow::Result<UnixNanos> {
    anyhow::ensure!(
        every.is_positive(),
        "the retrain interval {every:#} is not positive"
    );
    let every = every.as_nanos();
    let start = i128::from(now.as_u64()).div_euclid(every) * every;
    Ok(UnixNanos::from(u64::try_from(start)?))
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
    fn each_strategy_has_a_retrain_timer_of_its_own() {
        assert_eq!(timer(StrategyId::from("LIVE-001")), "RETRAIN-LIVE-001");
        assert_ne!(
            timer(StrategyId::from("LIVE-001")),
            timer(StrategyId::from("LIVE-002"))
        );
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
