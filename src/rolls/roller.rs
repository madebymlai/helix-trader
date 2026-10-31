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
use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    data::{DataType, NautilusDataType},
    enums::BookType,
    identifiers::InstrumentId,
};

use super::Transition;
use crate::strategy::{TickSeries, warmup::subscription_params};

/// The prefix of an instrument's roll alert, followed by its id.
const ROLL_ALERT: &str = "ROLL:";

/// The request param that sends an instrument request past the node's
/// catalogs to the venue's data client (`dispatch_instrument_request` in
/// Nautilus' data engine), which loads the instrument as it answers.
const FORCE_INSTRUMENT_UPDATE: &str = "force_instrument_update";

/// The levels a side of an `OrderBookDepth10` snapshot.
const DEPTH: NonZeroUsize = NonZeroUsize::new(10).unwrap();

/// The book type of the book deltas a roller subscribes to, and a warmup
/// requests.
pub const DELTAS_BOOK_TYPE: BookType = BookType::L3_MBO;

/// Subscribes a strategy to its tick series, each on the contract standing
/// for its instrument (see `Transition::after`), and rolls them: owned by
/// the strategy, started in its `on_start` and handed its time events in
/// `on_time_event`, which return the roll they made, if any. An instrument
/// that does not roll is subscribed once, with no alert.
///
/// A live roller loads each post contract when it sets its roll alert, so
/// a node rolls any number of times: it requests the instrument from the
/// venue's data client (see `Loading::Requested`). A replayed one loads
/// nothing: a backtest or dataset run registers every contract of its
/// slices up front.
///
/// It subscribes through the strategy that owns it, so the strategy calls
/// it on a clone (`self.roller.clone().start(self)`): it holds the series
/// only, and works out the contracts from the clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Roller {
    ticks: Vec<TickSeries>,
    loading: Loading,
}

/// How the node comes to hold the contracts a roller subscribes to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Loading {
    /// The run registered them all before it started.
    Registered,
    /// A live node loads at its start the contract standing for each
    /// instrument; the roller requests each post contract when it sets the
    /// alert of its roll, past the catalogs to the venue's data client,
    /// which loads it into its instrument provider as it answers (IB
    /// subscribes only to what its provider holds), and the data engine
    /// adds it to the cache.
    Requested,
}

impl Roller {
    /// A live roller of `ticks`, their instruments roots or plain
    /// instruments: it requests each post contract before its roll.
    #[must_use]
    pub fn live(ticks: Vec<TickSeries>) -> Self {
        Self {
            ticks,
            loading: Loading::Requested,
        }
    }

    /// A roller of `ticks` in a backtest or a dataset run, which register
    /// every contract up front: it requests nothing.
    #[must_use]
    pub fn replayed(ticks: Vec<TickSeries>) -> Self {
        Self {
            ticks,
            loading: Loading::Registered,
        }
    }

    /// Subscribes `actor` to each series on the contract standing for its
    /// instrument now, and sets the alert of each instrument's next roll,
    /// if it rolls (see `on_time_event`), live requesting its post
    /// contract.
    ///
    /// # Errors
    ///
    /// Returns an error if the clock refuses an alert or Nautilus a
    /// request.
    pub fn start<A>(&self, actor: &mut A) -> anyhow::Result<()>
    where
        A: DataActor + DataActorNative + Debug + 'static,
    {
        let now = actor.clock().utc_now();
        for instrument_id in self.instrument_ids() {
            self.subscribe(actor, instrument_id, front(instrument_id, now));
            if let Some(next) = Transition::after(instrument_id, now) {
                self.arm(actor, instrument_id, next)?;
            }
        }
        Ok(())
    }

    /// If `event` is an instrument's roll alert, rolls its series: from
    /// the pre contract to the post contract of its transition, and sets
    /// the alert of the next one, live requesting its post contract;
    /// returns the transition made. `None` for any
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
    /// Returns an error if the clock refuses the next alert or Nautilus
    /// the request.
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
            self.arm(actor, instrument_id, next)?;
        }
        Ok(Some(roll))
    }

    /// Sets the roll alert of `instrument_id` for `next` and, live,
    /// requests its post contract (see `Loading::Requested`).
    fn arm<A>(
        &self,
        actor: &mut A,
        instrument_id: InstrumentId,
        next: Transition,
    ) -> anyhow::Result<()>
    where
        A: DataActor + DataActorNative + Debug + 'static,
    {
        set_alert(actor, instrument_id, next)?;
        if self.loading == Loading::Requested {
            let params = Params::new([(FORCE_INSTRUMENT_UPDATE.to_string(), true.into())].into());
            actor.request_instrument(next.post, None, None, None, Some(params))?;
        }
        Ok(())
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

    /// Subscribes `actor` to the series of `instrument_id` on `contract`,
    /// streaming from now: live, where the warmup, requested up to now,
    /// ends (`strategy::warmup`).
    fn subscribe<A>(&self, actor: &mut A, instrument_id: InstrumentId, contract: InstrumentId)
    where
        A: DataActor + DataActorNative + Debug + 'static,
    {
        let now = UnixNanos::from(actor.clock().utc_now());
        for data in self.data_of(instrument_id) {
            let params = Some(subscription_params(now, []));
            match data {
                NautilusDataType::QuoteTick => actor.subscribe_quotes(contract, None, params),
                NautilusDataType::TradeTick => actor.subscribe_trades(contract, None, params),
                NautilusDataType::OrderBookDepth => actor.subscribe_book_depth(
                    contract,
                    BookType::L2_MBP,
                    Some(DEPTH),
                    None,
                    false,
                    params,
                ),
                NautilusDataType::OrderBookDelta => actor.subscribe_book_deltas(
                    contract,
                    DELTAS_BOOK_TYPE,
                    None,
                    None,
                    false,
                    params,
                ),
                NautilusDataType::Custom { type_name } => {
                    let data_type = DataType::new(type_name, None, Some(contract.to_string()));
                    actor.subscribe_data(data_type, None, params);
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
                    let data_type = DataType::new(type_name, None, Some(contract.to_string()));
                    actor.unsubscribe_data(data_type, None, None);
                }
                other => unreachable!("{other} is not a tick series (see `TickSeries`)"),
            }
        }
    }
}

/// The contract standing for `instrument_id` at `time`: the pre contract
/// of its next roll, or itself if it does not roll. What a roller
/// subscribes to at its start.
#[must_use]
pub fn front(instrument_id: InstrumentId, time: Timestamp) -> InstrumentId {
    Transition::after(instrument_id, time).map_or(instrument_id, |next| next.pre)
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

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::{
        cache::Cache,
        clock::{Clock, VirtualClock},
        messages::data::{DataCommand, RequestCommand},
        msgbus::{
            self, MessagingSwitchboard, stubs::TypedIntoMessageSavingHandler,
            stubs::get_typed_into_message_saving_handler,
        },
        timer::TimeEventCallback,
    };
    use nautilus_core::UnixNanos;
    use nautilus_model::identifiers::{StrategyId, TraderId};
    use nautilus_portfolio::portfolio::Portfolio;
    use nautilus_trading::{
        nautilus_strategy,
        strategy::{StrategyConfig, StrategyCore},
    };

    use super::*;

    #[derive(Debug)]
    struct Probe {
        core: StrategyCore,
    }

    nautilus_strategy!(Probe);

    impl DataActor for Probe {}

    /// What the probe sent the data engine about an instrument.
    #[derive(Debug, PartialEq, Eq)]
    enum Sent {
        Subscribe(InstrumentId),
        Unsubscribe(InstrumentId),
        /// A request for the instrument, and whether it skips the catalog
        /// to reach the venue's client.
        Request(InstrumentId, bool),
    }

    fn id(id: &str) -> InstrumentId {
        InstrumentId::from(id)
    }

    fn at(s: &str) -> UnixNanos {
        s.parse::<Timestamp>().unwrap().into()
    }

    /// A strategy registered at `now` without a node, its clock, and what
    /// it sends the data engine from then on.
    fn registered(
        now: &str,
    ) -> (
        Probe,
        Rc<RefCell<VirtualClock>>,
        TypedIntoMessageSavingHandler<DataCommand>,
    ) {
        let (handler, sent) = get_typed_into_message_saving_handler::<DataCommand>(None);
        msgbus::register_data_command_endpoint(
            MessagingSwitchboard::data_engine_queue_execute(),
            handler,
        );
        let mut probe = Probe {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from("PROBE-001")),
                order_id_tag: Some("001".to_string()),
                ..Default::default()
            }),
        };
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        clock.borrow_mut().advance_time(at(now), true);
        clock
            .borrow_mut()
            .register_default_handler(TimeEventCallback::from(|_: TimeEvent| {}));
        let cache = Rc::new(RefCell::new(Cache::default()));
        let portfolio = Rc::new(RefCell::new(Portfolio::new(
            clock.clone(),
            cache.clone(),
            None,
        )));
        probe
            .core
            .register(TraderId::from("HELIX-001"), clock.clone(), cache, portfolio)
            .unwrap();
        (probe, clock, sent)
    }

    fn sent(commands: &TypedIntoMessageSavingHandler<DataCommand>) -> Vec<Sent> {
        let only = |ids: Option<&[InstrumentId]>| {
            let [id] = ids.unwrap() else {
                panic!("one instrument")
            };
            *id
        };
        commands
            .get_messages()
            .into_iter()
            .map(|command| match command {
                DataCommand::Subscribe(command) => Sent::Subscribe(only(command.instrument_ids())),
                DataCommand::Unsubscribe(command) => {
                    Sent::Unsubscribe(only(command.instrument_ids()))
                }
                DataCommand::Request(RequestCommand::Instrument(request)) => {
                    let forced = request
                        .params
                        .as_ref()
                        .and_then(|params| params.get_bool("force_instrument_update"))
                        == Some(true);
                    Sent::Request(only(Some(&request.instrument_ids)), forced)
                }
                other => panic!("unexpected {other:?}"),
            })
            .collect()
    }

    /// Runs `roller` on the probe from `start` to `end`, a day at a time,
    /// handing it the clock's time events; returns what the probe sent
    /// and the rolls the roller made.
    fn run(roller: &Roller, start: &str, end: &str) -> (Vec<Sent>, Vec<Transition>) {
        let (mut probe, clock, commands) = registered(start);
        roller.start(&mut probe).unwrap();
        let end: Timestamp = end.parse().unwrap();
        let mut now: Timestamp = start.parse().unwrap();
        let mut rolls = Vec::new();
        while now < end {
            now = (now + SignedDuration::from_hours(24)).min(end);
            let events = clock.borrow_mut().advance_time(now.into(), true);
            for event in events {
                rolls.extend(roller.on_time_event(&mut probe, &event).unwrap());
            }
        }
        (sent(&commands), rolls)
    }

    fn gc_trades() -> Vec<TickSeries> {
        vec![TickSeries {
            data: NautilusDataType::TradeTick,
            instrument_id: id("GC.GLBX"),
        }]
    }

    /// GC rolls into GCG7 at 00:00 UTC on 2026-11-26 and into GCJ7 on
    /// 2027-01-27, two business days before the last business day of the
    /// month before delivery (`calendar.toml`); GCJ7 rolls into GCM7.
    #[test]
    fn a_live_roller_rolls_twice_requesting_each_post_contract_before_its_roll() {
        let (sent, rolls) = run(
            &Roller::live(gc_trades()),
            "2026-08-10T14:30:00Z",
            "2027-02-01T00:00:00Z",
        );
        assert_eq!(
            rolls
                .iter()
                .map(|roll| (roll.pre, roll.post))
                .collect::<Vec<_>>(),
            [
                (id("GCZ6.GLBX"), id("GCG7.GLBX")),
                (id("GCG7.GLBX"), id("GCJ7.GLBX"))
            ]
        );
        assert_eq!(
            sent,
            [
                Sent::Subscribe(id("GCZ6.GLBX")),
                Sent::Request(id("GCG7.GLBX"), true),
                Sent::Unsubscribe(id("GCZ6.GLBX")),
                Sent::Subscribe(id("GCG7.GLBX")),
                Sent::Request(id("GCJ7.GLBX"), true),
                Sent::Unsubscribe(id("GCG7.GLBX")),
                Sent::Subscribe(id("GCJ7.GLBX")),
                Sent::Request(id("GCM7.GLBX"), true),
            ]
        );
    }

    #[test]
    fn a_replayed_roller_rolls_twice_requesting_nothing() {
        let (sent, rolls) = run(
            &Roller::replayed(gc_trades()),
            "2026-08-10T14:30:00Z",
            "2027-02-01T00:00:00Z",
        );
        assert_eq!(rolls.len(), 2);
        assert_eq!(
            sent,
            [
                Sent::Subscribe(id("GCZ6.GLBX")),
                Sent::Unsubscribe(id("GCZ6.GLBX")),
                Sent::Subscribe(id("GCG7.GLBX")),
                Sent::Unsubscribe(id("GCG7.GLBX")),
                Sent::Subscribe(id("GCJ7.GLBX")),
            ]
        );
    }

    #[test]
    fn a_live_roller_requests_nothing_for_a_plain_instrument() {
        let ticks = vec![TickSeries {
            data: NautilusDataType::TradeTick,
            instrument_id: id("GCZ6.GLBX"),
        }];
        let (sent, rolls) = run(
            &Roller::live(ticks),
            "2026-08-10T14:30:00Z",
            "2027-02-01T00:00:00Z",
        );
        assert!(rolls.is_empty());
        assert_eq!(sent, [Sent::Subscribe(id("GCZ6.GLBX"))]);
    }
}
