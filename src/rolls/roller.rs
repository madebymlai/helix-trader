//! The roller: keeps a strategy subscribed to the tick series it reads on
//! the contract standing for each of their instruments, switching at each
//! roll, with the plain Nautilus clock and subscription API, as Nautilus'
//! own (crate-private) roller of continuous bars does.

use std::{fmt::Debug, num::NonZeroUsize};

use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, DataActorNative},
    timer::TimeEvent,
};
use nautilus_model::{
    data::{DataType, NautilusDataType},
    enums::BookType,
    identifiers::InstrumentId,
};

use super::Transition;
use crate::strategy::TickSeries;

/// The prefix of an instrument's roll alert, followed by its id.
const ROLL_ALERT: &str = "ROLL:";

/// The levels a side of an `OrderBookDepth10` snapshot.
const DEPTH: NonZeroUsize = NonZeroUsize::new(10).unwrap();

/// Subscribes a strategy to its tick series, each on the contract standing
/// for its instrument (see `Transition::after`), and rolls them: owned by
/// the strategy, started in its `on_start` and handed its time events in
/// `on_time_event`, which return the roll they made, if any. An instrument
/// that does not roll is subscribed once, with no alert.
///
/// It subscribes through the strategy that owns it, so the strategy calls
/// it on a clone (`self.roller.clone().start(self)`): it holds the series
/// only, and works out the contracts from the clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Roller {
    ticks: Vec<TickSeries>,
}

impl Roller {
    /// A roller of `ticks`, their instruments roots or plain instruments.
    #[must_use]
    pub fn new(ticks: Vec<TickSeries>) -> Self {
        Self { ticks }
    }

    /// Subscribes `actor` to each series on the contract standing for its
    /// instrument now, and sets the alert of each instrument's next roll,
    /// if it rolls (see `on_time_event`).
    ///
    /// # Errors
    ///
    /// Returns an error if the clock refuses an alert.
    pub fn start<A>(&self, actor: &mut A) -> anyhow::Result<()>
    where
        A: DataActor + DataActorNative + Debug + 'static,
    {
        let now = actor.clock().utc_now();
        for instrument_id in self.instrument_ids() {
            self.subscribe(actor, instrument_id, front(instrument_id, now));
            if let Some(next) = Transition::after(instrument_id, now) {
                set_alert(actor, instrument_id, next)?;
            }
        }
        Ok(())
    }

    /// If `event` is an instrument's roll alert, rolls its series: from
    /// the pre contract to the post contract of its transition, and sets
    /// the alert of the next one; returns that transition. `None` for any
    /// other event, which is the strategy's own. A series of the pre
    /// contract that another of the instruments still stands on after the
    /// roll (the contract itself, read beside its root) stays subscribed.
    ///
    /// The alert is at the last nanosecond of the pre contract's slice,
    /// not at the transition: the backtest engine delivers the data of a
    /// time before the alerts of that time, so an alert at the transition
    /// would miss the post contract's tick stamped there. So the strategy
    /// is told after the pre contract's last tick and before the post
    /// contract's first.
    ///
    /// # Errors
    ///
    /// Returns an error if the clock refuses the next alert.
    pub fn on_time_event<A>(
        &self,
        actor: &mut A,
        event: &TimeEvent,
    ) -> anyhow::Result<Option<Transition>>
    where
        A: DataActor + DataActorNative + Debug + 'static,
    {
        let Some(instrument_id) = self
            .instrument_ids()
            .into_iter()
            .find(|&instrument_id| event.name.as_str() == alert_name(instrument_id))
        else {
            return Ok(None);
        };
        let roll = Transition::after(instrument_id, event.ts_event.into())
            .expect("an instrument with a roll alert rolls");
        self.unsubscribe(actor, instrument_id, roll.pre, roll.time);
        self.subscribe(actor, instrument_id, roll.post);
        if let Some(next) = Transition::after(instrument_id, roll.time) {
            set_alert(actor, instrument_id, next)?;
        }
        Ok(Some(roll))
    }

    /// The instruments of the series, each once, in order.
    fn instrument_ids(&self) -> Vec<InstrumentId> {
        let mut ids: Vec<InstrumentId> = Vec::new();
        for tick in &self.ticks {
            if !ids.contains(&tick.instrument_id) {
                ids.push(tick.instrument_id);
            }
        }
        ids
    }

    /// The kinds of the series of `instrument_id`.
    fn data_of(&self, instrument_id: InstrumentId) -> impl Iterator<Item = &NautilusDataType> + '_ {
        self.ticks
            .iter()
            .filter(move |tick| tick.instrument_id == instrument_id)
            .map(|tick| &tick.data)
    }

    /// Subscribes `actor` to the series of `instrument_id` on `contract`.
    fn subscribe<A>(&self, actor: &mut A, instrument_id: InstrumentId, contract: InstrumentId)
    where
        A: DataActor + DataActorNative + Debug + 'static,
    {
        for data in self.data_of(instrument_id) {
            match data {
                NautilusDataType::QuoteTick => actor.subscribe_quotes(contract, None, None),
                NautilusDataType::TradeTick => actor.subscribe_trades(contract, None, None),
                NautilusDataType::OrderBookDepth => actor.subscribe_book_depth(
                    contract,
                    BookType::L2_MBP,
                    Some(DEPTH),
                    None,
                    false,
                    None,
                ),
                NautilusDataType::OrderBookDelta => {
                    actor.subscribe_book_deltas(contract, BookType::L3_MBO, None, None, false, None)
                }
                NautilusDataType::Custom { type_name } => {
                    actor.subscribe_data(custom(type_name, contract), None, None);
                }
                other => unreachable!("{other} is not a tick series (see `TickSeries`)"),
            }
        }
    }

    /// Unsubscribes `actor` from the series of `instrument_id` on
    /// `contract`, but for the kinds an instrument of the roller still
    /// reads on `contract` at `time`, the contract standing for it then.
    fn unsubscribe<A>(
        &self,
        actor: &mut A,
        instrument_id: InstrumentId,
        contract: InstrumentId,
        time: Timestamp,
    ) where
        A: DataActor + DataActorNative + Debug + 'static,
    {
        for data in self.data_of(instrument_id) {
            let still_read = self
                .ticks
                .iter()
                .any(|tick| tick.data == *data && front(tick.instrument_id, time) == contract);
            if still_read {
                continue;
            }
            match data {
                NautilusDataType::QuoteTick => actor.unsubscribe_quotes(contract, None, None),
                NautilusDataType::TradeTick => actor.unsubscribe_trades(contract, None, None),
                NautilusDataType::OrderBookDepth => {
                    actor.unsubscribe_book_depth(contract, None, None);
                }
                NautilusDataType::OrderBookDelta => {
                    actor.unsubscribe_book_deltas(contract, None, None);
                }
                NautilusDataType::Custom { type_name } => {
                    actor.unsubscribe_data(custom(type_name, contract), None, None);
                }
                other => unreachable!("{other} is not a tick series (see `TickSeries`)"),
            }
        }
    }
}

/// The contract standing for `instrument_id` at `time`: the pre contract
/// of its next roll, or itself if it does not roll.
fn front(instrument_id: InstrumentId, time: Timestamp) -> InstrumentId {
    Transition::after(instrument_id, time).map_or(instrument_id, |next| next.pre)
}

/// The data type of the custom data `type_name` of `contract`, keyed by
/// its id (see `TickSeries`).
fn custom(type_name: &str, contract: InstrumentId) -> DataType {
    DataType::new(type_name, None, Some(contract.to_string()))
}

/// The name of the roll alert of `instrument_id`.
fn alert_name(instrument_id: InstrumentId) -> String {
    format!("{ROLL_ALERT}{instrument_id}")
}

/// Sets the roll alert of `instrument_id` for `next`, at the last
/// nanosecond before it (see `Roller::on_time_event`), handled by the
/// actor's `on_time_event`.
fn set_alert<A>(actor: &mut A, instrument_id: InstrumentId, next: Transition) -> anyhow::Result<()>
where
    A: DataActor + DataActorNative + Debug + 'static,
{
    let at: Timestamp = next.time - SignedDuration::from_nanos(1);
    actor
        .clock()
        .set_time_alert(&alert_name(instrument_id), at, None, None)
}
