//! The roller: tells a strategy when the contract standing for each instrument it reads changes,
//! with the plain Nautilus clock, as Nautilus' own (crate-private) roller of continuous bars does:
//! an alert before each roll, at which it returns the roll, and, live, the post contract loaded
//! before it, but a custom series' root's alone (`Roller::custom_roots`). It subscribes to nothing:
//! the strategy hands each roll to its subscriptions (`strategy::subscriptions`), which move the
//! rolled instrument's series, its custom series among them, to the post contract, and to whatever
//! else reads the contract, a model strategy's features.

use std::fmt::Debug;

use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, DataActorNative},
    timer::TimeEvent,
};
use nautilus_core::Params;
use nautilus_model::identifiers::InstrumentId;

use super::Transition;
use crate::venues;

/// The prefix of an instrument's roll alert, followed by its id, `@` and the time of the roll the
/// alert is armed for (see `alert_name`).
const ROLL_ALERT: &str = "ROLL:";

/// The request param that sends an instrument request past the node's catalogs to the venue's data
/// client (`dispatch_instrument_request` in Nautilus' data engine), which loads the instrument as
/// it answers.
const FORCE_INSTRUMENT_UPDATE: &str = "force_instrument_update";

/// Rolls a strategy's instruments, each from the contract standing for it (see `Transition::after`)
/// to the next. The strategy owns it, starts it in its `on_start` and hands it its time events in
/// `on_time_event`, which returns the roll it made, if any, for the strategy to hand its
/// subscriptions (`Subscriptions::roll`). An instrument that does not roll gets no alert.
///
/// A live roller loads each post contract when it sets its roll alert, so a node rolls any number
/// of times: it requests the instrument from the venue's data client (see `Loading::Requested`). A
/// replayed one loads nothing: a backtest or dataset run registers every contract of its slices up
/// front.
///
/// It sets its alerts and requests through the strategy that owns it, so the strategy calls it on a
/// clone (`self.roller.clone().start(self, now)`): it holds the instruments only, and works out the
/// contracts from the time it starts at and the roll time each alert's name holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Roller {
    /// The instruments, roots or plain, each once.
    instrument_ids: Vec<InstrumentId>,
    /// The roots of custom series, each once, none of `instrument_ids`: rolled as those are,
    /// loading nothing (see `custom_roots`).
    custom_roots: Vec<InstrumentId>,
    loading: Loading,
}

/// How the node comes to hold the contracts a roller rolls into.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Loading {
    /// The run registered them all before it started.
    Registered,
    /// A live node loads at its start the contract standing for each instrument. The roller
    /// requests each post contract when it sets the alert of its roll, past the catalogs to the
    /// data client the venue's definitions are asked of live (`venues::live_definitions_client`:
    /// its broker's, IB's for GLBX, whose live data is LSE's, which publishes no definitions). That
    /// client loads it into its instrument provider as it answers (IB subscribes only to what its
    /// provider holds), and the data engine adds it to the cache.
    Requested,
}

impl Roller {
    /// A live roller of `instrument_ids`, roots or plain instruments: it requests each post
    /// contract before its roll.
    #[must_use]
    pub fn live(instrument_ids: Vec<InstrumentId>) -> Self {
        Self::of(instrument_ids, Loading::Requested)
    }

    /// A roller of `instrument_ids` in a backtest or a dataset run, which register every contract
    /// up front: it requests nothing.
    #[must_use]
    pub fn replayed(instrument_ids: Vec<InstrumentId>) -> Self {
        Self::of(instrument_ids, Loading::Registered)
    }

    fn of(instrument_ids: Vec<InstrumentId>, loading: Loading) -> Self {
        let mut ids: Vec<InstrumentId> = Vec::new();
        for instrument_id in instrument_ids {
            if !ids.contains(&instrument_id) {
                ids.push(instrument_id);
            }
        }
        Self {
            instrument_ids: ids,
            custom_roots: Vec::new(),
            loading,
        }
    }

    /// The roller rolling `roots` too, the roots of custom series (`CustomSeries::root`), as it
    /// rolls its instruments, but loading none of their contracts, live too: a custom series brings
    /// no instrument, its contract standing in its data type's identifier alone. A root among its
    /// instruments loads its contracts still.
    #[must_use]
    pub fn custom_roots(mut self, roots: impl IntoIterator<Item = InstrumentId>) -> Self {
        for root in roots {
            if !self.instrument_ids.contains(&root) && !self.custom_roots.contains(&root) {
                self.custom_roots.push(root);
            }
        }
        self
    }

    /// The instruments and the custom series' roots it rolls, each once.
    fn rolled(&self) -> impl Iterator<Item = InstrumentId> + '_ {
        self.instrument_ids
            .iter()
            .chain(&self.custom_roots)
            .copied()
    }

    /// Sets the alert of each instrument's next roll after `now`, if it rolls (see
    /// `on_time_event`), live requesting its post contract. `now` is the time the strategy's
    /// subscriptions start from (`Subscriptions::start`), so the alert is of the roll out of the
    /// contract they subscribe to (`front`).
    ///
    /// # Errors
    ///
    /// Returns an error if the clock refuses an alert or Nautilus a request.
    pub fn start<A>(&self, actor: &mut A, now: Timestamp) -> anyhow::Result<()>
    where
        A: DataActor + DataActorNative + Debug + 'static,
    {
        for instrument_id in self.rolled() {
            if let Some(next) = Transition::after(instrument_id, now) {
                self.arm(actor, instrument_id, next)?;
            }
        }
        Ok(())
    }

    /// If `event` is an instrument's roll alert, sets the alert of its next roll, live requesting
    /// its post contract, and returns the roll the alert was armed for, from its pre to its post
    /// contract, which the strategy hands its subscriptions (`Subscriptions::roll`). `None` for any
    /// other event, which is the strategy's own. The roll is the one the alert's name holds, not
    /// one worked out from the event's time: Nautilus fires an alert set in the past at once, at
    /// the clock's time (`validate_and_prepare_time_alert`), so an alert armed after its roll, at a
    /// live start just before it, comes at or after the roll's time, when the next roll is the one
    /// after it.
    ///
    /// The alert is at the last nanosecond of the pre contract's slice, not at the transition: the
    /// backtest engine delivers the data of a time before the alerts of that time, so an alert at
    /// the transition would miss the post contract's tick stamped there. So the strategy is told
    /// after the pre contract's last tick and before the post contract's first.
    ///
    /// # Errors
    ///
    /// Returns an error if the clock refuses the next alert or Nautilus the request.
    pub fn on_time_event<A>(
        &self,
        actor: &mut A,
        event: &TimeEvent,
    ) -> anyhow::Result<Option<Transition>>
    where
        A: DataActor + DataActorNative + Debug + 'static,
    {
        let Some((instrument_id, roll)) = self
            .rolled()
            .find_map(|instrument_id| Some((instrument_id, armed(event, instrument_id)?)))
        else {
            return Ok(None);
        };
        if let Some(next) = Transition::after(instrument_id, roll.time) {
            self.arm(actor, instrument_id, next)?;
        }
        Ok(Some(roll))
    }

    /// Sets the roll alert of `instrument_id` for `next` and, live, requests its post contract (see
    /// `Loading::Requested`) if it is an instrument read, not a custom series' root alone.
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
        if self.loading == Loading::Requested && self.instrument_ids.contains(&instrument_id) {
            let params = Params::new([(FORCE_INSTRUMENT_UPDATE.to_string(), true.into())].into());
            let client = venues::live_definitions_client(next.post.venue);
            actor.request_instrument(next.post, None, None, Some(client), Some(params))?;
        }
        Ok(())
    }
}

/// The contract standing for `instrument_id` at `time`: the pre contract of its next roll, or
/// itself if it does not roll. What a strategy subscribes to at its start (`Subscriptions::start`).
#[must_use]
pub fn front(instrument_id: InstrumentId, time: Timestamp) -> InstrumentId {
    Transition::after(instrument_id, time).map_or(instrument_id, |next| next.pre)
}

/// The name of the roll alert of `instrument_id` for `roll`: its prefix, the instrument and the
/// roll's time, from which `armed` reads the roll back.
fn alert_name(instrument_id: InstrumentId, roll: Transition) -> String {
    format!("{ROLL_ALERT}{instrument_id}@{}", roll.time)
}

/// The roll of `instrument_id` that `event` is the alert of, if it is one: the roll at the time its
/// name holds (see `alert_name`).
fn armed(event: &TimeEvent, instrument_id: InstrumentId) -> Option<Transition> {
    let time: Timestamp = event
        .name
        .as_str()
        .strip_prefix(&format!("{ROLL_ALERT}{instrument_id}@"))?
        .parse()
        .ok()?;
    let roll = Transition::after(instrument_id, time - SignedDuration::from_nanos(1))
        .expect("an instrument with a roll alert rolls");
    Some(roll)
}

/// Sets the roll alert of `instrument_id` for `next`, at the last nanosecond before it (see
/// `Roller::on_time_event`), handled by the actor's `on_time_event`.
fn set_alert<A>(actor: &A, instrument_id: InstrumentId, next: Transition) -> anyhow::Result<()>
where
    A: DataActor + DataActorNative + Debug + 'static,
{
    let at: Timestamp = next.time - SignedDuration::from_nanos(1);
    actor
        .clock()
        .set_time_alert(&alert_name(instrument_id, next), at, None, None)
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::{
        cache::Cache,
        clock::{Clock, VirtualClock},
        messages::data::{DataCommand, RequestCommand},
        msgbus::{
            self, MessagingSwitchboard,
            stubs::{TypedIntoMessageSavingHandler, get_typed_into_message_saving_handler},
        },
        timer::TimeEventCallback,
    };
    use nautilus_core::UnixNanos;
    use nautilus_model::identifiers::{ClientId, StrategyId, TraderId};
    use nautilus_portfolio::portfolio::Portfolio;
    use nautilus_trading::{
        nautilus_strategy,
        strategy::{StrategyConfig, StrategyCore},
    };
    use rstest::rstest;

    use super::*;

    #[derive(Debug)]
    struct Probe {
        core: StrategyCore,
    }

    nautilus_strategy!(Probe);

    impl DataActor for Probe {}

    /// What the probe sent the data engine about an instrument: a roller sends requests alone, so a
    /// subscription shows as a regression.
    #[derive(Debug, PartialEq, Eq)]
    enum Sent {
        Subscribe(InstrumentId),
        Unsubscribe(InstrumentId),
        /// A request for the instrument, whether it skips the catalog, and the client it names.
        Request(InstrumentId, bool, Option<ClientId>),
    }

    fn id(id: &str) -> InstrumentId {
        InstrumentId::from(id)
    }

    fn at(s: &str) -> UnixNanos {
        s.parse::<Timestamp>().unwrap().into()
    }

    /// A strategy registered at `now` without a node, its clock, and what it sends the data engine
    /// from then on.
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
                    Sent::Request(
                        only(Some(&request.instrument_ids)),
                        forced,
                        request.client_id,
                    )
                }
                other => panic!("unexpected {other:?}"),
            })
            .collect()
    }

    /// Runs `roller` on the probe from `start` to `end`, a day at a time, handing it the clock's
    /// time events; returns what the probe sent and the rolls the roller made.
    fn run(roller: &Roller, start: &str, end: &str) -> (Vec<Sent>, Vec<Transition>) {
        let (mut probe, clock, commands) = registered(start);
        roller.start(&mut probe, start.parse().unwrap()).unwrap();
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

    /// GC rolls into GCG27 at 00:00 UTC on 2026-11-26 and into GCJ27 on 2027-01-27, two business
    /// days before the last business day of the month before delivery (`calendar.toml`); GCJ27
    /// rolls into GCM27. Each post contract is asked of GLBX's broker's client, IB's, which
    /// resolves GLBX contracts, not of LSE's, which streams GLBX's data and publishes no
    /// definitions.
    #[rstest]
    #[case::live_requesting_each_post_contract_before_its_roll(
        Roller::live(vec![id("GC.GLBX")]),
        gc_rolls(),
        gc_post_contracts()
    )]
    #[case::replayed_requesting_nothing(Roller::replayed(vec![id("GC.GLBX")]), gc_rolls(), vec![])]
    #[case::live_on_a_custom_series_root_requesting_no_contract(
        Roller::live(vec![]).custom_roots([id("GC.GLBX")]),
        gc_rolls(),
        vec![]
    )]
    #[case::live_on_a_root_read_for_its_ticks_and_its_custom_series_once(
        Roller::live(vec![id("GC.GLBX")]).custom_roots([id("GC.GLBX")]),
        gc_rolls(),
        gc_post_contracts()
    )]
    #[case::live_on_a_plain_instrument_never(Roller::live(vec![id("GCZ26.GLBX")]), vec![], vec![])]
    fn a_roller_rolls_a_root_and_requests_the_post_contracts_its_ticks_need(
        #[case] roller: Roller,
        #[case] expected_rolls: Vec<(InstrumentId, InstrumentId)>,
        #[case] expected_sent: Vec<Sent>,
    ) {
        let (sent, rolls) = run(&roller, "2026-08-10T14:30:00Z", "2027-02-01T00:00:00Z");
        assert_eq!(
            rolls
                .iter()
                .map(|roll| (roll.pre, roll.post))
                .collect::<Vec<_>>(),
            expected_rolls
        );
        assert_eq!(sent, expected_sent);
    }

    /// GC's two rolls between 2026-08-10 and 2027-02-01.
    fn gc_rolls() -> Vec<(InstrumentId, InstrumentId)> {
        vec![
            (id("GCZ26.GLBX"), id("GCG27.GLBX")),
            (id("GCG27.GLBX"), id("GCJ27.GLBX")),
        ]
    }

    /// The requests, of IB's client and past the catalog, of the post contracts of GC's two rolls
    /// and of the roll after them.
    fn gc_post_contracts() -> Vec<Sent> {
        vec![
            Sent::Request(id("GCG27.GLBX"), true, Some(ClientId::from("IB"))),
            Sent::Request(id("GCJ27.GLBX"), true, Some(ClientId::from("IB"))),
            Sent::Request(id("GCM27.GLBX"), true, Some(ClientId::from("IB"))),
        ]
    }

    /// A live start a minute before GC rolls from GCZ26 into GCG27, at 00:00 UTC on 2026-11-26: the
    /// strategy subscribes on GCZ26 from the warmup's `now`, 23:59, and the clock has passed the
    /// roll by the time the roller arms it, so Nautilus fires its alert at once, at the clock's
    /// time (`validate_and_prepare_time_alert`). The roll the roller returns is the one it armed,
    /// out of GCZ26, the contract subscribed, not the next one after the alert's time.
    #[rstest]
    fn a_roll_armed_after_its_time_is_the_one_out_of_the_contract_standing_at_the_start() {
        let roller = Roller::live(vec![id("GC.GLBX")]);
        let (mut probe, clock, _) = registered("2026-11-25T23:59:00Z");
        clock
            .borrow_mut()
            .advance_time(at("2026-11-26T00:00:01Z"), true);

        roller
            .start(&mut probe, "2026-11-25T23:59:00Z".parse().unwrap())
            .unwrap();
        let events = clock
            .borrow_mut()
            .advance_time(at("2026-11-26T00:00:02Z"), true);
        let rolls: Vec<(InstrumentId, InstrumentId)> = events
            .iter()
            .filter_map(|event| roller.on_time_event(&mut probe, event).unwrap())
            .map(|roll| (roll.pre, roll.post))
            .collect();

        assert_eq!(rolls, [(id("GCZ26.GLBX"), id("GCG27.GLBX"))]);
    }
}
