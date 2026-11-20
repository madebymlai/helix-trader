//! Futures rolls: which dated contract stands for a root at each moment.
//!
//! Nautilus' continuous futures take a transition table from the caller. Its venues and data
//! sources take dated contracts only. This module builds that table for any instrument id over a
//! span, from the roll calendar (`calendar.toml`).
//!
//! An id listed there as a product root (`GC.GLBX`) rolls through its contracts (`GCQ26.GLBX`,
//! ...). Any other id is a plain instrument, with no transitions and one slice on itself.
//!
//! Callers use the slices and never branch on an id, with two exceptions:
//! - A custom series' identifier (`CustomSeries::root`) may name no instrument.
//! - Its catalog folder may be a contract's, where a root's series is stored (`root_of`,
//!   `CustomSeries::contract_folder_shared_with`).
//!
//! A `Roller` tells a strategy about each roll. At each roll, its subscriptions
//! (`strategy::subscriptions`) move the root's tick and custom series to the post contract.
//! `ContinuousBars` makes a root's bars Nautilus' continuous bars, which roll themselves.

mod calendar;
mod continuous;
mod roller;

pub use continuous::ContinuousBars;
use jiff::{SignedDuration, Timestamp};
use nautilus_model::identifiers::InstrumentId;
pub use roller::{Roller, front};

/// The transitions of an instrument over a span, in Nautilus' continuous futures shape.
///
/// Segment 0 runs on the first pre contract. Each later segment runs on a transition's post
/// contract, from that transition's time on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransitionTable {
    /// The first pre contract, or the instrument itself if it does not roll.
    first: InstrumentId,
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
    /// Returns the first transition of `instrument_id` after `time`, excluding `time`.
    ///
    /// Returns `None` if the id is not a root in the roll calendar. The transition's `pre` is the
    /// contract in front at `time`. It is unbounded, as live needs.
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

    /// Returns the last transition of `instrument_id` at or before `time`, including `time`.
    ///
    /// Returns `None` if the id is not a root in the roll calendar. The transition's `post` is the
    /// contract in front at `time`, as `contract_at` has it, so `after` starts where this ends.
    /// It is unbounded.
    #[must_use]
    pub fn before(instrument_id: InstrumentId, time: Timestamp) -> Option<Self> {
        let product = calendar::product(instrument_id)?;
        let front = product.front_at(time);
        let previous = product.previous(front);
        Some(Self {
            time: product.roll_out_of(previous),
            pre: product.instrument_id(previous),
            post: product.instrument_id(front),
        })
    }
}

/// Returns the root in the roll calendar that `contract` stands for at some time.
///
/// The contract can be from any year, in one of its product's liquid months, on the root's venue.
/// `GCZ26.GLBX` stands for `GC.GLBX`.
#[must_use]
pub fn root_of(contract: InstrumentId) -> Option<InstrumentId> {
    calendar::root_of(contract)
}

/// The part of a span where one contract stands for its root, both ends included.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Slice {
    pub instrument_id: InstrumentId,
    pub from: Timestamp,
    pub to: Timestamp,
}

impl TransitionTable {
    /// Returns the table of `instrument_id` over `[start, end]`.
    ///
    /// It holds the rolls of the product after `start`, up to `end`. An id that is not a root in
    /// the roll calendar has none.
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

    /// Returns the contract standing for the root at `time`, by Nautilus' segment rule.
    #[must_use]
    pub fn contract_at(&self, time: Timestamp) -> InstrumentId {
        self.transitions
            .iter()
            .rev()
            .find(|transition| transition.time <= time)
            .map_or(self.first, |transition| transition.post)
    }

    /// Returns the slices covering the span, one per contract.
    ///
    /// Each slice ends 1 ns before the next transition. The last slice ends at the end of the span.
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
    use rstest::rstest;

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

    /// The last weekday before each delivery month is Fri Jan 30 (Jan 31 is a Saturday), Tue Mar
    /// 31, Fri May 29 (May 31 is a Sunday), Fri Jul 31 and Mon Nov 30. Two weekdays before are Wed
    /// Jan 28, Fri Mar 27, Wed May 27, Wed Jul 29 and Thu Nov 26.
    #[rstest]
    fn gc_rolls_in_2026_two_weekdays_before_the_last_weekday_before_each_delivery_month() {
        let table = TransitionTable::of(gc(), at("2026-01-01T00:00Z"), at("2026-12-31T00:00Z"));
        assert_eq!(
            table.transitions(),
            [
                transition("2026-01-28T00:00Z", "GCG26.GLBX", "GCJ26.GLBX"),
                transition("2026-03-27T00:00Z", "GCJ26.GLBX", "GCM26.GLBX"),
                transition("2026-05-27T00:00Z", "GCM26.GLBX", "GCQ26.GLBX"),
                transition("2026-07-29T00:00Z", "GCQ26.GLBX", "GCZ26.GLBX"),
                transition("2026-11-26T00:00Z", "GCZ26.GLBX", "GCG27.GLBX"),
            ]
        );
    }

    #[rstest]
    fn a_span_starting_after_the_last_roll_of_a_year_starts_on_the_next_years_contract() {
        let table = TransitionTable::of(gc(), at("2026-12-15T00:00Z"), at("2027-02-15T00:00Z"));
        assert_eq!(
            table.transitions(),
            [transition("2027-01-27T00:00Z", "GCG27.GLBX", "GCJ27.GLBX")]
        );
    }

    #[rstest]
    fn a_roll_at_the_start_of_the_span_is_already_done() {
        let table = TransitionTable::of(gc(), at("2026-07-29T00:00Z"), at("2026-08-31T00:00Z"));
        assert_eq!(table.transitions(), []);
        assert_eq!(
            table.slices(),
            [slice(
                "GCZ26.GLBX",
                at("2026-07-29T00:00Z"),
                at("2026-08-31T00:00Z")
            )]
        );
    }

    /// Rolls GCQ26 to GCZ26 at 2026-07-29T00:00Z.
    #[rstest]
    #[case::before_a_transition_its_pre_contract(before("2026-07-29T00:00Z"), "GCQ26.GLBX")]
    #[case::at_a_transition_its_post_contract(at("2026-07-29T00:00Z"), "GCZ26.GLBX")]
    #[case::after_a_transition_its_post_contract(at("2026-08-10T14:30Z"), "GCZ26.GLBX")]
    #[case::before_the_span_the_first_transition_s_pre_contract(
        at("2026-01-02T00:00Z"),
        "GCQ26.GLBX"
    )]
    fn the_contract_at_a_time_is_the_one_the_transitions_leave_in_front(
        #[case] time: Timestamp,
        #[case] contract: &str,
    ) {
        let table = TransitionTable::of(gc(), at("2026-07-01T00:00Z"), at("2026-08-31T00:00Z"));
        assert_eq!(table.contract_at(time), id(contract));
    }

    #[rstest]
    #[case::with_rolls(
        "2026-05-01T00:00Z",
        "2026-08-15T12:00Z",
        vec![
            slice("GCM26.GLBX", at("2026-05-01T00:00Z"), before("2026-05-27T00:00Z")),
            slice("GCQ26.GLBX", at("2026-05-27T00:00Z"), before("2026-07-29T00:00Z")),
            slice("GCZ26.GLBX", at("2026-07-29T00:00Z"), at("2026-08-15T12:00Z")),
        ],
    )]
    #[case::without_rolls(
        "2026-08-31T00:00Z",
        "2026-10-23T00:00Z",
        vec![slice("GCZ26.GLBX", at("2026-08-31T00:00Z"), at("2026-10-23T00:00Z"))],
    )]
    #[case::with_a_roll_at_its_end(
        "2026-07-01T00:00Z",
        "2026-07-29T00:00Z",
        vec![
            slice("GCQ26.GLBX", at("2026-07-01T00:00Z"), before("2026-07-29T00:00Z")),
            slice("GCZ26.GLBX", at("2026-07-29T00:00Z"), at("2026-07-29T00:00Z")),
        ],
    )]
    fn the_slices_of_a_span_end_1_ns_before_each_transition(
        #[case] from: &str,
        #[case] to: &str,
        #[case] slices: Vec<Slice>,
    ) {
        let table = TransitionTable::of(gc(), at(from), at(to));
        assert_eq!(table.slices(), slices);
    }

    #[rstest]
    #[case::a_cfd("XAUUSD.ICMARKETS")]
    #[case::a_contract("GCZ26.GLBX")]
    #[case::a_continuous_series("GC.v.0.GLBX")]
    fn an_id_without_a_calendar_entry_is_one_slice_on_itself(#[case] plain: &str) {
        let table =
            TransitionTable::of(id(plain), at("2026-01-01T00:00Z"), at("2026-12-31T00:00Z"));
        assert_eq!(table.transitions(), []);
        assert_eq!(
            table.slices(),
            [slice(
                plain,
                at("2026-01-01T00:00Z"),
                at("2026-12-31T00:00Z")
            )]
        );
        assert_eq!(table.contract_at(at("2026-07-29T00:00Z")), id(plain));
    }

    /// GC contracts: `GC{month}{yy}` on GLBX, months G, J, M, Q, Z.
    #[rstest]
    #[case::december_26("GCZ26.GLBX", Some("GC.GLBX"))]
    #[case::february_27("GCG27.GLBX", Some("GC.GLBX"))]
    #[case::august_99("GCQ99.GLBX", Some("GC.GLBX"))]
    #[case::an_illiquid_month("GCV26.GLBX", None)]
    #[case::the_root("GC.GLBX", None)]
    #[case::a_one_digit_year("GCZ6.GLBX", None)]
    #[case::a_three_digit_year("GCZ026.GLBX", None)]
    #[case::another_venue("GCZ26.XCME", None)]
    #[case::a_lowercase_month("GCz26.GLBX", None)]
    #[case::another_product("SIZ26.GLBX", None)]
    #[case::a_continuous_series("GC.v.0.GLBX", None)]
    #[case::a_cfd("XAUUSD.ICMARKETS", None)]
    fn only_a_contract_of_a_liquid_month_of_any_year_stands_for_its_root(
        #[case] contract: &str,
        #[case] root: Option<&str>,
    ) {
        assert_eq!(root_of(id(contract)), root.map(id));
    }

    #[rstest]
    #[case::a_time_before_it(
        gc(),
        at("2026-07-28T12:00Z"),
        Some(transition("2026-07-29T00:00Z", "GCQ26.GLBX", "GCZ26.GLBX"))
    )]
    #[case::a_transition_the_one_after_it(
        gc(),
        at("2026-07-29T00:00Z"),
        Some(transition("2026-11-26T00:00Z", "GCZ26.GLBX", "GCG27.GLBX"))
    )]
    #[case::the_last_nanosecond_before_a_roll_that_roll(
        gc(),
        before("2026-07-29T00:00Z"),
        Some(transition("2026-07-29T00:00Z", "GCQ26.GLBX", "GCZ26.GLBX"))
    )]
    #[case::an_id_without_a_calendar_entry_none(id("GCZ26.GLBX"), at("2026-07-28T12:00Z"), None)]
    fn the_transition_after_a_time_is_the_next_roll_of_its_root(
        #[case] id: InstrumentId,
        #[case] time: Timestamp,
        #[case] next: Option<Transition>,
    ) {
        assert_eq!(Transition::after(id, time), next);
    }

    fn just_after(time: &str) -> Timestamp {
        at(time) + SignedDuration::from_nanos(1)
    }

    /// Nov 30, 2025 is a Sunday: last weekday Fri Nov 28, roll Wed Nov 26.
    #[rstest]
    #[case::a_time_between_rolls_the_earlier_roll(
        gc(),
        at("2026-08-10T14:30Z"),
        Some(transition("2026-07-29T00:00Z", "GCQ26.GLBX", "GCZ26.GLBX"))
    )]
    #[case::a_roll_exactly_at_its_time_that_roll(
        gc(),
        at("2026-07-29T00:00Z"),
        Some(transition("2026-07-29T00:00Z", "GCQ26.GLBX", "GCZ26.GLBX"))
    )]
    #[case::the_nanosecond_after_a_roll_that_roll(
        gc(),
        just_after("2026-07-29T00:00Z"),
        Some(transition("2026-07-29T00:00Z", "GCQ26.GLBX", "GCZ26.GLBX"))
    )]
    #[case::the_last_nanosecond_before_a_roll_the_roll_before(
        gc(),
        before("2026-07-29T00:00Z"),
        Some(transition("2026-05-27T00:00Z", "GCM26.GLBX", "GCQ26.GLBX"))
    )]
    #[case::the_first_roll_of_a_year_the_last_roll_of_the_year_before(
        gc(),
        at("2026-01-27T12:00Z"),
        Some(transition("2025-11-26T00:00Z", "GCZ25.GLBX", "GCG26.GLBX"))
    )]
    #[case::an_id_without_a_calendar_entry_none(id("GCZ26.GLBX"), at("2026-08-10T14:30Z"), None)]
    fn the_transition_before_a_time_is_the_last_roll_of_its_root_up_to_it(
        #[case] id: InstrumentId,
        #[case] time: Timestamp,
        #[case] previous: Option<Transition>,
    ) {
        assert_eq!(Transition::before(id, time), previous);
    }

    /// `before`'s post is the contract standing at the time, and `after` comes after the time.
    #[rstest]
    #[case::the_last_nanosecond_before_the_roll(before("2026-07-29T00:00Z"))]
    #[case::the_roll(at("2026-07-29T00:00Z"))]
    #[case::the_nanosecond_after_the_roll(just_after("2026-07-29T00:00Z"))]
    fn the_transition_before_a_time_ends_on_the_contract_standing_then_and_the_next_comes_after_it(
        #[case] time: Timestamp,
    ) {
        let table = TransitionTable::of(gc(), at("2026-07-01T00:00Z"), at("2026-08-31T00:00Z"));
        let previous = Transition::before(gc(), time).unwrap();
        let next = Transition::after(gc(), time).unwrap();
        assert_eq!(previous.post, table.contract_at(time));
        assert!(previous.time <= time && time < next.time);
    }

    /// Databento's GC.v.0 switches over the stored span, 2026-01-26 to 2026-10-23. Each switch is
    /// on the day the expiration in the stored daily definitions moves to the next contract, at
    /// 00:00 UTC. It rolls by the previous day's volume.
    const DATABENTO_SWITCHES: [(&str, &str, &str); 4] = [
        ("2026-01-30", "GCG26.GLBX", "GCJ26.GLBX"),
        ("2026-03-30", "GCJ26.GLBX", "GCM26.GLBX"),
        ("2026-05-29", "GCM26.GLBX", "GCQ26.GLBX"),
        ("2026-07-31", "GCQ26.GLBX", "GCZ26.GLBX"),
    ];

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

    /// 2 business days before the rule day puts each Databento switch within 2 + 1 business days
    /// after the roll.
    #[rstest]
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
