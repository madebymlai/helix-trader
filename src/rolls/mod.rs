//! Futures rolls: which dated contract stands for a root at each moment.
//!
//! Nautilus has no continuous instruments: its continuous futures take a
//! caller's transition table, and its venues and data sources take dated
//! contracts only. This module builds that table for any instrument id over
//! a span, from the roll calendar (`calendar.toml`): an id listed there as a
//! product root (`GC.GLBX`) rolls through its contracts (`GCQ6.GLBX`,
//! `GCZ6.GLBX`, ...); any other id is a plain instrument, with no
//! transitions and one slice on itself. Callers use the slices and never
//! branch on an id. A `Roller` keeps a strategy's tick subscriptions on
//! the contract standing for each root, switching at each roll, and
//! `ContinuousBars` makes a strategy's bars on a root Nautilus' continuous
//! bars, which roll themselves.

mod calendar;
mod continuous;
mod roller;

pub use continuous::ContinuousBars;
pub use roller::Roller;

use jiff::{SignedDuration, Timestamp};
use nautilus_model::identifiers::InstrumentId;

/// The transitions of an instrument over a span, in Nautilus' continuous
/// futures shape: a transition hands the root from its pre to its post
/// contract at its time, segment 0 runs on the first pre contract before
/// the first transition, and each later segment runs on a transition's
/// post contract from its time, that time included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionTable {
    /// The contract before the first transition, the instrument itself if
    /// it does not roll.
    first: InstrumentId,
    /// The transitions inside the span, in time order.
    transitions: Vec<Transition>,
    /// The span, both ends included.
    start: Timestamp,
    end: Timestamp,
}

/// A roll: from `time` on, `post` stands for the root instead of `pre`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Transition {
    pub time: Timestamp,
    pub pre: InstrumentId,
    pub post: InstrumentId,
}

impl Transition {
    /// The first transition of `instrument_id` after `time`, `time`
    /// excluded: the next roll of its product from the contract in front
    /// at `time` (so its `pre` is that contract), if the id is a root in
    /// the roll calendar; none otherwise. Unbounded: it holds for any
    /// future time, as live needs.
    #[must_use]
    pub fn after(instrument_id: InstrumentId, time: Timestamp) -> Option<Self> {
        let product = calendar::product(instrument_id)?;
        let front = product.front_at(time);
        Some(Self {
            time: product.roll_out_of(front),
            pre: product.instrument_id(front),
            post: product.instrument_id(product.next(front)),
        })
    }
}

/// The contracts `instrument_id` needs from `time` up to its next roll:
/// the one standing for it at `time`, then the one it rolls into (see
/// `Transition::after`); the instrument alone if it does not roll. What
/// live loads for it at its start.
#[must_use]
pub fn contracts_at(instrument_id: InstrumentId, time: Timestamp) -> Vec<InstrumentId> {
    Transition::after(instrument_id, time)
        .map_or_else(|| vec![instrument_id], |next| vec![next.pre, next.post])
}

/// The part of a span one contract stands for its root, both ends
/// included, as the catalog's ranges are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Slice {
    pub instrument_id: InstrumentId,
    pub from: Timestamp,
    pub to: Timestamp,
}

impl TransitionTable {
    /// The table of `instrument_id` over `[start, end]`: the rolls of its
    /// product from the contract in front at `start`, at times after
    /// `start` up to `end` included, if the id is a root in the roll
    /// calendar; none otherwise.
    #[must_use]
    pub fn of(instrument_id: InstrumentId, start: Timestamp, end: Timestamp) -> Self {
        let first = Transition::after(instrument_id, start).map_or(instrument_id, |next| next.pre);
        let mut transitions = Vec::new();
        let mut time = start;
        while let Some(next) = Transition::after(instrument_id, time)
            && next.time <= end
        {
            transitions.push(next);
            time = next.time;
        }
        Self {
            first,
            transitions,
            start,
            end,
        }
    }

    /// The transitions, in time order.
    #[must_use]
    pub fn transitions(&self) -> &[Transition] {
        &self.transitions
    }

    /// The contract standing for the root at `time`, by Nautilus' segment
    /// rule: the first pre contract before the first transition, then each
    /// transition's post contract from its time on.
    #[must_use]
    pub fn contract_at(&self, time: Timestamp) -> InstrumentId {
        self.transitions
            .iter()
            .rev()
            .find(|transition| transition.time <= time)
            .map_or(self.first, |transition| transition.post)
    }

    /// The slices covering the span, one per contract, in time order: each
    /// ends 1 ns before the next transition, the last at the span's end.
    #[must_use]
    pub fn slices(&self) -> Vec<Slice> {
        let froms = std::iter::once(self.start)
            .chain(self.transitions.iter().map(|transition| transition.time));
        let tos = self
            .transitions
            .iter()
            .map(|transition| transition.time - SignedDuration::from_nanos(1))
            .chain(std::iter::once(self.end));
        froms
            .zip(tos)
            .map(|(from, to)| Slice {
                instrument_id: self.contract_at(from),
                from,
                to,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use jiff::civil;

    use super::*;

    fn gc() -> InstrumentId {
        InstrumentId::from("GC.GLBX")
    }

    fn id(s: &str) -> InstrumentId {
        InstrumentId::from(s)
    }

    fn at(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    /// One nanosecond before `time`.
    fn before(time: &str) -> Timestamp {
        at(time) - SignedDuration::from_nanos(1)
    }

    fn transition(time: &str, pre: &str, post: &str) -> Transition {
        Transition {
            time: at(time),
            pre: id(pre),
            post: id(post),
        }
    }

    fn slice(instrument_id: &str, from: Timestamp, to: Timestamp) -> Slice {
        Slice {
            instrument_id: id(instrument_id),
            from,
            to,
        }
    }

    /// GC's rolls in 2026: 00:00 UTC two weekdays before the last weekday
    /// of the month before each liquid delivery month (G, J, M, Q, Z). The
    /// last weekdays are Fri Jan 30 (Jan 31 is a Saturday), Tue Mar 31,
    /// Fri May 29 (May 31 is a Sunday), Fri Jul 31 and Mon Nov 30; two
    /// weekdays before, Wed Jan 28, Fri Mar 27 (across the weekend), Wed
    /// May 27, Wed Jul 29 and Thu Nov 26.
    #[test]
    fn gc_rolls_in_2026_two_weekdays_before_the_last_weekday_before_each_delivery_month() {
        let table = TransitionTable::of(gc(), at("2026-01-01T00:00Z"), at("2026-12-31T00:00Z"));
        assert_eq!(
            table.transitions(),
            [
                transition("2026-01-28T00:00Z", "GCG6.GLBX", "GCJ6.GLBX"),
                transition("2026-03-27T00:00Z", "GCJ6.GLBX", "GCM6.GLBX"),
                transition("2026-05-27T00:00Z", "GCM6.GLBX", "GCQ6.GLBX"),
                transition("2026-07-29T00:00Z", "GCQ6.GLBX", "GCZ6.GLBX"),
                transition("2026-11-26T00:00Z", "GCZ6.GLBX", "GCG7.GLBX"),
            ]
        );
    }

    #[test]
    fn a_span_starting_after_the_last_roll_of_a_year_starts_on_the_next_years_contract() {
        let table = TransitionTable::of(gc(), at("2026-12-15T00:00Z"), at("2027-02-15T00:00Z"));
        assert_eq!(
            table.transitions(),
            [transition("2027-01-27T00:00Z", "GCG7.GLBX", "GCJ7.GLBX")]
        );
    }

    #[test]
    fn a_roll_at_the_start_of_the_span_is_already_done() {
        let table = TransitionTable::of(gc(), at("2026-07-29T00:00Z"), at("2026-08-31T00:00Z"));
        assert_eq!(table.transitions(), []);
        assert_eq!(
            table.slices(),
            [slice(
                "GCZ6.GLBX",
                at("2026-07-29T00:00Z"),
                at("2026-08-31T00:00Z")
            )]
        );
    }

    #[test]
    fn the_contract_before_a_transition_is_its_pre_contract() {
        let table = TransitionTable::of(gc(), at("2026-07-01T00:00Z"), at("2026-08-31T00:00Z"));
        assert_eq!(
            table.contract_at(before("2026-07-29T00:00Z")),
            id("GCQ6.GLBX")
        );
    }

    #[test]
    fn the_contract_at_a_transition_is_its_post_contract() {
        let table = TransitionTable::of(gc(), at("2026-07-01T00:00Z"), at("2026-08-31T00:00Z"));
        assert_eq!(table.contract_at(at("2026-07-29T00:00Z")), id("GCZ6.GLBX"));
    }

    #[test]
    fn the_contract_after_a_transition_is_its_post_contract() {
        let table = TransitionTable::of(gc(), at("2026-07-01T00:00Z"), at("2026-08-31T00:00Z"));
        assert_eq!(table.contract_at(at("2026-08-10T14:30Z")), id("GCZ6.GLBX"));
    }

    #[test]
    fn the_contract_before_the_first_transition_holds_before_the_span() {
        let table = TransitionTable::of(gc(), at("2026-07-01T00:00Z"), at("2026-08-31T00:00Z"));
        assert_eq!(table.contract_at(at("2026-01-02T00:00Z")), id("GCQ6.GLBX"));
    }

    #[test]
    fn the_slices_of_a_span_with_rolls_end_1_ns_before_each_transition() {
        let table = TransitionTable::of(gc(), at("2026-05-01T00:00Z"), at("2026-08-15T12:00Z"));
        assert_eq!(
            table.slices(),
            [
                slice(
                    "GCM6.GLBX",
                    at("2026-05-01T00:00Z"),
                    before("2026-05-27T00:00Z")
                ),
                slice(
                    "GCQ6.GLBX",
                    at("2026-05-27T00:00Z"),
                    before("2026-07-29T00:00Z")
                ),
                slice(
                    "GCZ6.GLBX",
                    at("2026-07-29T00:00Z"),
                    at("2026-08-15T12:00Z")
                ),
            ]
        );
    }

    #[test]
    fn the_slice_of_a_span_without_rolls_is_the_span_on_the_front_contract() {
        let table = TransitionTable::of(gc(), at("2026-08-31T00:00Z"), at("2026-10-23T00:00Z"));
        assert_eq!(
            table.slices(),
            [slice(
                "GCZ6.GLBX",
                at("2026-08-31T00:00Z"),
                at("2026-10-23T00:00Z")
            )]
        );
    }

    #[test]
    fn a_roll_at_the_end_of_the_span_gives_its_post_contract_the_last_instant() {
        let table = TransitionTable::of(gc(), at("2026-07-01T00:00Z"), at("2026-07-29T00:00Z"));
        assert_eq!(
            table.slices(),
            [
                slice(
                    "GCQ6.GLBX",
                    at("2026-07-01T00:00Z"),
                    before("2026-07-29T00:00Z")
                ),
                slice(
                    "GCZ6.GLBX",
                    at("2026-07-29T00:00Z"),
                    at("2026-07-29T00:00Z")
                ),
            ]
        );
    }

    #[test]
    fn an_id_without_a_calendar_entry_is_one_slice_on_itself() {
        for plain in ["XAUUSD.ICMARKETS", "GCZ6.GLBX", "GC.v.0.GLBX"] {
            let table =
                TransitionTable::of(id(plain), at("2026-01-01T00:00Z"), at("2026-12-31T00:00Z"));
            assert_eq!(table.transitions(), [], "{plain}");
            assert_eq!(
                table.slices(),
                [slice(
                    plain,
                    at("2026-01-01T00:00Z"),
                    at("2026-12-31T00:00Z")
                )],
                "{plain}"
            );
            assert_eq!(table.contract_at(at("2026-07-29T00:00Z")), id(plain));
        }
    }

    #[test]
    fn the_transition_after_a_time_is_the_next_roll_of_its_root() {
        assert_eq!(
            Transition::after(gc(), at("2026-07-28T12:00Z")),
            Some(transition("2026-07-29T00:00Z", "GCQ6.GLBX", "GCZ6.GLBX"))
        );
    }

    #[test]
    fn the_transition_after_a_transition_is_the_one_after_it() {
        assert_eq!(
            Transition::after(gc(), at("2026-07-29T00:00Z")),
            Some(transition("2026-11-26T00:00Z", "GCZ6.GLBX", "GCG7.GLBX"))
        );
    }

    #[test]
    fn the_transition_after_the_last_nanosecond_before_a_roll_is_that_roll() {
        assert_eq!(
            Transition::after(gc(), before("2026-07-29T00:00Z")),
            Some(transition("2026-07-29T00:00Z", "GCQ6.GLBX", "GCZ6.GLBX"))
        );
    }

    #[test]
    fn an_id_without_a_calendar_entry_has_no_transition_after_any_time() {
        assert_eq!(
            Transition::after(id("GCZ6.GLBX"), at("2026-07-28T12:00Z")),
            None
        );
    }

    #[test]
    fn a_root_needs_the_contract_in_front_and_the_one_it_rolls_into() {
        assert_eq!(
            contracts_at(gc(), at("2026-08-10T14:30Z")),
            [id("GCZ6.GLBX"), id("GCG7.GLBX")]
        );
    }

    #[test]
    fn a_root_needs_at_its_roll_the_post_contract_and_the_next() {
        assert_eq!(
            contracts_at(gc(), at("2026-07-29T00:00Z")),
            [id("GCZ6.GLBX"), id("GCG7.GLBX")]
        );
        assert_eq!(
            contracts_at(gc(), before("2026-07-29T00:00Z")),
            [id("GCQ6.GLBX"), id("GCZ6.GLBX")]
        );
    }

    #[test]
    fn an_id_without_a_calendar_entry_needs_itself_alone() {
        assert_eq!(
            contracts_at(id("GCZ6.GLBX"), at("2026-08-10T14:30Z")),
            [id("GCZ6.GLBX")]
        );
    }

    /// Databento's GC.v.0 switches over the stored span, 2026-01-26 to
    /// 2026-10-23, from the stored daily definitions: the day the
    /// definitions' expiration moves to the next contract, all at 00:00
    /// UTC. GC.v.0 rolls by the previous day's volume.
    const DATABENTO_SWITCHES: [(&str, &str, &str); 4] = [
        ("2026-01-30", "GCG6.GLBX", "GCJ6.GLBX"),
        ("2026-03-30", "GCJ6.GLBX", "GCM6.GLBX"),
        ("2026-05-29", "GCM6.GLBX", "GCQ6.GLBX"),
        ("2026-07-31", "GCQ6.GLBX", "GCZ6.GLBX"),
    ];

    /// The weekdays after `from` up to `to` included.
    fn weekdays_after(from: civil::Date, to: civil::Date) -> usize {
        from.series(jiff::Span::new().days(1))
            .skip(1)
            .take_while(|&day| day <= to)
            .filter(|day| {
                !matches!(
                    day.weekday(),
                    civil::Weekday::Saturday | civil::Weekday::Sunday
                )
            })
            .count()
    }

    /// GC rolls `days_before = 2` business days before the rule day, so
    /// each Databento switch falls after the roll, within 2 + 1 business
    /// days of it.
    #[test]
    fn gc_rolls_before_each_databento_switch_on_the_stored_span_within_three_business_days() {
        let table = TransitionTable::of(gc(), at("2026-01-26T00:00Z"), at("2026-10-23T00:00Z"));
        assert_eq!(table.transitions().len(), DATABENTO_SWITCHES.len());
        for (roll, (switch, pre, post)) in table.transitions().iter().zip(DATABENTO_SWITCHES) {
            assert_eq!((roll.pre, roll.post), (id(pre), id(post)));
            let switch: civil::Date = switch.parse().unwrap();
            let roll_day = roll.time.to_zoned(jiff::tz::TimeZone::UTC).date();
            assert!(
                (1..=3).contains(&weekdays_after(roll_day, switch)),
                "Databento switched {pre} to {post} on {switch}, we roll on {roll_day}"
            );
        }
    }
}
