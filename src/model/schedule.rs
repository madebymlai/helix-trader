//! When a strategy's models are trained, and what each train does: every
//! `ModelConfig::retrain`, on the fixed UTC grid of the interval's
//! multiples since the Unix epoch, whatever the run start, in a backtest
//! as live; at each train, what is done to each model (`Plan`).
//!
//! The grid is kept by a Nautilus timer, named after the strategy
//! (`timer`, so several strategies retrain in one process), which `start`
//! sets on a clock from an epoch-aligned time (`last_grid_time`): Nautilus
//! fires it at that time plus whole intervals. A live node's clock fires
//! it as it goes (`launcher`); a backtest, which trains before its run,
//! takes the times the same timer fires over its span on a virtual clock
//! of its own (`Schedule::times`).
//!
//! A train at `t` is planned (`Schedule::plan`) from the valid-froms of
//! the stores alone, model by model (the long, the short and the meta,
//! `Models`): a model whose current model at `t` exists keeps it, the one
//! valid exactly from `t`, retrained as the strategy is; the others are
//! fitted as of `t`, on the rows of their window back from `t` settled by
//! then; but a model primary's meta, whose rows are the primary's
//! decisions, waits while no primary model is valid before `t`, and its
//! rows start at the primary's first model at the earliest. The plan
//! answers whether the train fits anything, where its one replay starts
//! (`Plan::replay_from`), and how far back the rows of its models reach
//! (`Plan::reach`), those a kept model was fitted on included, which its
//! baseline is fitted on (`Step::rows`).

use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, DataActorNative},
    clock::{Clock, VirtualClock},
    timer::{TimeEvent, TimeEventCallback},
};
use nautilus_core::{DurationNanos, UnixNanos};
use nautilus_model::identifiers::StrategyId;

use super::{
    config::{ModelConfig, RuleConfig},
    targets::Side,
};

/// The name of the timer `times` fires on its own clock.
const GRID: &str = "RETRAIN";

/// When a strategy trains, and what a train does to each of its models:
/// its retrain interval, and the windows of its models' rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Schedule {
    every: SignedDuration,
    /// The window of a model primary's sides; none for a rule primary.
    primary: Option<SignedDuration>,
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
    /// Keeps its current model, valid from `valid_from`, which was fitted
    /// on the rows from `from`.
    Keep {
        from: Timestamp,
        valid_from: Timestamp,
    },
    /// Fits a model as of `at`, on the rows of `[from, at)` settled by
    /// `at`, and publishes it valid from `at`.
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
            every: config.retrain,
            primary: Some(config.window),
            meta: config.meta.as_ref().map(|meta| meta.window),
        }
    }

    /// The schedule of the meta model of a section whose primary is its
    /// rule.
    #[must_use]
    pub const fn of_rule(config: &RuleConfig) -> Self {
        Self {
            every: config.retrain,
            primary: None,
            meta: Some(config.meta.window),
        }
    }

    /// The train times of a run over `[from, to)`, in order: the last grid
    /// time not after `from`, whose models are those in use at the run's
    /// start, then each grid time before `to` (`times`).
    ///
    /// # Errors
    ///
    /// As `times`.
    pub fn times(&self, from: Timestamp, to: Timestamp) -> anyhow::Result<Vec<Timestamp>> {
        times(self.every, from, to)
    }

    /// The plan of a train at `at`, the stores of the models holding
    /// models valid from `valid_froms`: a model keeps its current model at
    /// `at`, if any (`current`), else is fitted as of `at` on the rows of
    /// its window back from `at`; but a model primary's meta waits while no
    /// primary model is valid before `at`, and its rows start at the
    /// primary's first model at the earliest, since a row before it has no
    /// decision of the primary.
    #[must_use]
    pub fn plan(&self, at: Timestamp, valid_froms: &Models<Vec<Timestamp>>) -> Plan {
        let step = |valid_froms: &[Timestamp], from| match current(valid_froms, at) {
            Some(valid_from) => Step::Keep { from, valid_from },
            None => Step::Fit { from, at },
        };
        let side = |valid_froms: &Option<Vec<Timestamp>>| {
            Some(step(valid_froms.as_deref()?, at - self.primary?))
        };
        // The primary's first model before `at`, of either side.
        let first = [&valid_froms.long, &valid_froms.short]
            .into_iter()
            .flatten()
            .flatten()
            .copied()
            .filter(|&valid_from| valid_from < at)
            .min();
        let meta = || {
            let from = at - self.meta?;
            let meta = valid_froms.meta.as_deref()?;
            Some(match (self.primary, first) {
                (None, _) => step(meta, from),
                (Some(_), Some(first)) => step(meta, from.max(first)),
                (Some(_), None) => Step::Wait,
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
}

impl Plan {
    /// What the train does to each model.
    #[must_use]
    pub const fn steps(&self) -> &Models<Step> {
        &self.steps
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

    /// How far back the rows of the train's models reach: those a model is
    /// fitted on, or a kept model was, which its baseline is fitted on;
    /// the train's time if no model has a row.
    #[must_use]
    pub fn reach(&self) -> Timestamp {
        self.steps
            .iter()
            .filter_map(|step| step.rows())
            .map(|(from, _)| from)
            .min()
            .unwrap_or(self.at)
    }
}

/// The current model at `at` of a store holding models valid from
/// `valid_froms`, retrained: the one valid exactly from `at`.
fn current(valid_froms: &[Timestamp], at: Timestamp) -> Option<Timestamp> {
    valid_froms.contains(&at).then_some(at)
}

impl Step {
    /// The rows the model of this step is fitted on, those of `[from,
    /// to)` settled by `to`: the kept model's, or the fitted one's; none
    /// for a wait.
    #[must_use]
    pub const fn rows(self) -> Option<(Timestamp, Timestamp)> {
        match self {
            Self::Keep { from, valid_from } => Some((from, valid_from)),
            Self::Fit { from, at } => Some((from, at)),
            Self::Wait => None,
        }
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

    /// The value of `f` of each model's, or the first error.
    ///
    /// # Errors
    ///
    /// Returns the first error of `f`, the long's first.
    pub fn try_map<U, E>(self, mut f: impl FnMut(T) -> Result<U, E>) -> Result<Models<U>, E> {
        Ok(Models {
            long: self.long.map(&mut f).transpose()?,
            short: self.short.map(&mut f).transpose()?,
            meta: self.meta.map(&mut f).transpose()?,
        })
    }
}

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
            every: day(),
            primary: Some(hours(24)),
            meta: Some(hours(48)),
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

    fn keep(from: &str, valid_from: &str) -> Step {
        Step::Keep {
            from: at(from),
            valid_from: at(valid_from),
        }
    }

    const SIXTH: &str = "2026-01-06T00:00:00Z";
    const SEVENTH: &str = "2026-01-07T00:00:00Z";

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
        assert_eq!(plan.steps.short, Some(keep(SIXTH, SEVENTH)));
        assert_eq!(plan.steps.meta, Some(keep(SIXTH, SEVENTH)));
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

    /// A rule primary's rows are its own decisions, made without a model.
    #[test]
    fn a_rule_primarys_meta_fits_on_its_window_on_an_empty_store() {
        let rule = Schedule {
            primary: None,
            ..daily()
        };
        let empty = Models {
            long: None,
            short: None,
            meta: Some(vec![]),
        };

        let plan = rule.plan(at(SEVENTH), &empty);

        assert_eq!(
            plan.steps,
            Models {
                long: None,
                short: None,
                meta: Some(fit("2026-01-05T00:00:00Z", SEVENTH)),
            }
        );
    }

    /// The sides fit on the 6th; the meta, kept, was fitted from the 5th,
    /// the primary's first model on the 4th.
    #[test]
    fn the_replay_starts_at_the_first_row_fitted_and_the_reach_at_the_first_of_any_model() {
        let first = "2026-01-04T00:00:00Z";

        let plan = daily().plan(at(SEVENTH), &stores(&[first], &[first], &[SEVENTH]));

        assert!(plan.fits());
        assert_eq!(plan.replay_from(), Some(at(SIXTH)));
        assert_eq!(plan.reach(), at("2026-01-05T00:00:00Z"));
    }

    #[test]
    fn a_plan_keeping_every_model_fits_none_and_replays_nothing() {
        let plan = daily().plan(
            at(SEVENTH),
            &stores(&[SIXTH, SEVENTH], &[SIXTH, SEVENTH], &[SEVENTH]),
        );

        assert!(!plan.fits());
        assert_eq!(plan.replay_from(), None);
        assert_eq!(plan.reach(), at(SIXTH));
    }

    /// A waiting meta reads no row: the reach is the sides'.
    #[test]
    fn a_waiting_meta_does_not_reach_back_its_window() {
        let plan = daily().plan(at(SEVENTH), &stores(&[], &[], &[]));

        assert_eq!(plan.reach(), at(SIXTH));
    }

    #[test]
    fn a_kept_or_fitted_model_has_the_rows_it_is_fitted_on_and_a_wait_none() {
        assert_eq!(keep(SIXTH, SEVENTH).rows(), Some((at(SIXTH), at(SEVENTH))));
        assert_eq!(fit(SIXTH, SEVENTH).rows(), Some((at(SIXTH), at(SEVENTH))));
        assert_eq!(Step::Wait.rows(), None);
    }
}
