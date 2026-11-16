//! A strategy's live subscriptions, bars, ticks and custom series: what
//! the data engine streams to it, live as in a backtest or a dataset run,
//! through the plain Nautilus subscription API. A strategy holds a
//! `Subscriptions` of the bar types, tick series and custom series it
//! reads, a model strategy's `ModelHandle` building
//! and owning it, starts it from `on_start` after its warmup's start
//! (`strategy::warmup::Warmup::start`) and hands it each roll its roller
//! returns (`rolls::Roller::on_time_event`). The roller tells when the
//! contract standing for an instrument changes; the warmup requests the
//! history; neither subscribes.
//!
//! Every subscription carries the param `start_ns` (`subscription_params`):
//! at the start, the time the warmup's start returns, its `now`, where a
//! live warmup's requests end, so the live stream starts where the warmup
//! ends; at a roll, the roll's, the clock's then. Without it, the data
//! engine of a node with a catalog starts a subscription to quotes, trades,
//! external bars or custom data at the catalog's end, and IB sends every
//! bar since as live. It filters nothing from the live stream.
//!
//! The bars are the strategy's, a model strategy's target's ATR's among
//! them (`TargetSetter::atr_bar_type`), which no strategy declares, its
//! target registering the ATR for them alone: Nautilus keeps one
//! subscription per bar type and actor (`DataActorCore::subscribe_bars`),
//! so a bar type given twice is subscribed once. They are subscribed one
//! subscription per venue: Nautilus routes a list of bar types to the
//! client of their one venue. Bars on a futures
//! root (`GC.GLBX`), in a backtest, are Nautilus' continuous bars
//! (`rolls::ContinuousBars`), which roll themselves: one subscription per
//! bar type, carrying the root's `continuous_future_*` params, which
//! Nautilus takes per bar type.
//!
//! Each tick series is subscribed on the contract standing for its
//! instrument (`rolls::front`), a plain instrument standing for itself, at
//! the start the one standing for it then. At a roll of a root its series
//! move from the pre to the post contract, but for those another series
//! still reads on the pre contract (the contract itself, read beside its
//! root), which stay subscribed. A book is subscribed in the format
//! its venue's live data streams (`venues::book_format`), live as in a
//! backtest or a dataset run, whose replay of the catalog's book reads
//! neither the book type nor the levels of a subscription that manages no
//! book: its deltas at the format's book type, asking no depth, so each
//! client streams the whole book it gives, and its depth at the format's
//! levels, snapshots of price levels (L2_MBP). A venue whose live data
//! streams no book, a cTrader broker's, has its deltas subscribed as
//! L2_MBP, and one streaming no depth, IB, its depth with no levels: a live
//! start refuses such a book before anything connects (`venues::streams`),
//! so only a replay subscribes to it.
//!
//! Custom data is subscribed by its data type: each custom series
//! (`DataRequirements::custom`, a custom series of a config's `[data]
//! series`) as it is read at the start (`CustomSeries::at`). In a backtest
//! or a dataset run its subscription names no client, its engine having
//! none of the series' source: Nautilus subscribes the message bus alone
//! (`DataActorCore::subscribe_data`), where the replay's rows arrive. On a
//! live node (`Subscriptions::live`) it names the series' data client on
//! the node, its type's row's live data source's
//! (`venues::custom_data_client`), LSE's for LSE's rows, which the node
//! has whether or not an instrument of the run routes to it
//! (`venues::live_clients`): Nautilus subscribes the message bus and sends
//! that client the subscription, `SubscribeCustomData` of no venue, which
//! its data engine routes by the client id alone, checking no route of it
//! (`SubscribeCommand::validate_routing` passes a command of no instrument
//! and no bar type; `DataEngine::get_client` answers the client named,
//! with no fallback), and caches and publishes each point the client
//! emits. A series of a type with no row, or a row with no live data
//! source, names none live either, nothing streaming it; a live start
//! refuses a custom series its type's live data does not stream before
//! anything connects (`venues::streams_custom`), every one today. A custom
//! series on a root, its identifier the root's (`GC.GLBX`), is read on the
//! contract standing for it, keyed by the contract's id, and moves at each
//! roll as the root's tick series do, its end and its new subscription
//! naming its client as its start did, but for a data type another series
//! still reads at the roll, which stays subscribed; any other custom
//! series stands for itself, a contract's (`GCZ26.GLBX`) among them, and
//! no roll moves it. A strategy's `on_data` gets its rows: the strategy
//! subscribes to none itself.

use std::{collections::BTreeMap, fmt::Debug};

use jiff::{SignedDuration, Timestamp};
use nautilus_common::actor::{DataActor, DataActorNative};
use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    data::{BarType, DataType, NautilusDataType},
    enums::BookType,
    identifiers::{ClientId, InstrumentId, Venue},
};
use serde_json::{Value, json};

use crate::{
    rolls::{ContinuousBars, Transition, front},
    strategy::requirements::{CustomSeries, TickSeries},
    venues,
};

/// A strategy's subscriptions to the bar types, the tick series and the
/// custom series it reads (see the module): `start` from
/// its `on_start`, after its warmup's start, from the time that returns,
/// and `roll` at each roll its roller returns. It holds the series alone
/// and works the contracts out from the time and the roll, so a strategy
/// holding it calls it on a clone
/// (`self.subscriptions.clone().start(self, now)`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscriptions {
    /// The bar types, of plain instruments or, with continuous bars in
    /// `continuous`, of roots.
    bar_types: Vec<BarType>,
    continuous: ContinuousBars,
    /// The tick series, of roots or plain instruments.
    ticks: Vec<TickSeries>,
    /// The custom series, on roots or standing for themselves, each with
    /// the data client its subscription names: none, but on a live node
    /// (`live`).
    custom: Vec<(CustomSeries, Option<ClientId>)>,
}

impl Subscriptions {
    /// The subscriptions to `bar_types`, of plain instruments, to `ticks`,
    /// of roots or plain instruments, and to `custom`, custom series
    /// (`DataRequirements::custom`), on roots or standing for themselves,
    /// as a backtest or a dataset run makes them, each custom series' naming
    /// no client: a bar type given twice is subscribed once (see the
    /// module).
    #[must_use]
    pub fn new(bar_types: Vec<BarType>, ticks: Vec<TickSeries>, custom: Vec<CustomSeries>) -> Self {
        Self {
            bar_types,
            continuous: ContinuousBars::default(),
            ticks,
            custom: custom.into_iter().map(|series| (series, None)).collect(),
        }
    }

    /// The subscriptions with the bar types on a root with continuous bars
    /// in `continuous` subscribed as those, each with its root's params: in
    /// a backtest, which reads their rolls up front
    /// (`backtest::Backtest::continuous_bars`); live refuses bars on a root.
    #[must_use]
    pub fn continuous(self, continuous: ContinuousBars) -> Self {
        Self { continuous, ..self }
    }

    /// The subscriptions as a live node makes them: each custom series'
    /// naming its data client on the node, its type's row's live data
    /// source's (`venues::custom_data_client`), LSE's for LSE's rows, so
    /// that Nautilus sends that client the subscription; none if its type
    /// has no row, or one with no live data source (see the module). A
    /// strategy's are made so where it is built for a live node, as its
    /// warmup is (`Warmup::live`).
    #[must_use]
    pub fn live(self) -> Self {
        let custom = self
            .custom
            .into_iter()
            .map(|(series, _)| {
                let client = venues::custom_data_client(&series);
                (series, client)
            })
            .collect();
        Self { custom, ..self }
    }

    /// Subscribes `actor` to the bars, then to each tick series on the
    /// contract standing for its instrument at `now` (`rolls::front`), then
    /// to the custom data read at `now` by its data type (`custom_at`),
    /// naming its client, if it has one, all from `now`: the time the
    /// strategy's warmup's start returns (`Warmup::start`), where a live
    /// warmup's requests end.
    pub fn start<A>(&self, actor: &mut A, now: Timestamp)
    where
        A: DataActor + DataActorNative + Debug + 'static,
    {
        let from = UnixNanos::from(now);
        self.subscribe_bars(actor, from);
        for instrument_id in self.instrument_ids() {
            self.subscribe(actor, instrument_id, front(instrument_id, now), from);
        }
        for (data_type, client) in self.custom_at(now, None) {
            actor.subscribe_data(data_type, client, Some(subscription_params(from, [])));
        }
    }

    /// Moves the series of each root `roll` rolls, of which it is the roll
    /// at its time, from its pre to its post contract: unsubscribes `actor`
    /// from them on the pre contract, but for those a series still reads
    /// there at the roll, the contract standing for it then, and subscribes
    /// it to them on the post contract, from now: its tick series, then its
    /// custom data, by the data type it is read by on each side of the roll
    /// (`custom_at`), each naming its series' client, if it has one. Handed
    /// the roll the strategy's roller returns
    /// (`rolls::Roller::on_time_event`), at the last nanosecond before it.
    pub fn roll<A>(&self, actor: &mut A, roll: &Transition)
    where
        A: DataActor + DataActorNative + Debug + 'static,
    {
        let now = UnixNanos::from(actor.clock().utc_now());
        for instrument_id in self.instrument_ids() {
            if !rolled_by(instrument_id, roll) {
                continue;
            }
            self.unsubscribe(actor, instrument_id, roll);
            self.subscribe(actor, instrument_id, roll.post, now);
        }
        let read = self.custom_at(roll.time, None);
        let pre = roll.time - SignedDuration::from_nanos(1);
        for (data_type, client) in self.custom_at(pre, Some(roll)) {
            if !read.iter().any(|(of, _)| *of == data_type) {
                actor.unsubscribe_data(data_type, client, None);
            }
        }
        for (data_type, client) in self.custom_at(roll.time, Some(roll)) {
            actor.subscribe_data(data_type, client, Some(subscription_params(now, [])));
        }
    }

    /// The data types of the custom series read at `time`, each once with
    /// its series' client, of the roots `roll` rolls alone if given: each
    /// as `CustomSeries::at` reads it, a root's on its contract. Custom data
    /// is subscribed by its data type, so one two series read, a contract's
    /// own and its root's then, is subscribed once and kept while either
    /// still reads it; both name one client, their type's.
    fn custom_at(
        &self,
        time: Timestamp,
        roll: Option<&Transition>,
    ) -> Vec<(DataType, Option<ClientId>)> {
        let mut read: Vec<(DataType, Option<ClientId>)> = Vec::new();
        for (series, client) in &self.custom {
            let kept = roll.is_none_or(|roll| series.root().is_some_and(|id| rolled_by(id, roll)));
            let data_type = series.at(time).data_type().clone();
            if kept && !read.iter().any(|(of, _)| *of == data_type) {
                read.push((data_type, *client));
            }
        }
        read
    }

    /// Subscribes `actor` to the bars from `from`: those on a root with
    /// continuous bars per bar type with their `continuous_future_*`
    /// params, the others per venue.
    fn subscribe_bars<A>(&self, actor: &mut A, from: UnixNanos)
    where
        A: DataActor + DataActorNative + Debug + 'static,
    {
        let (continuous_bars, others): (Vec<BarType>, Vec<BarType>) = self
            .bar_types
            .iter()
            .partition(|bar_type| self.continuous.params(bar_type.instrument_id()).is_some());
        // One subscription per continuous bar type: Nautilus takes the params
        // of a continuous future per bar type.
        for bar_type in continuous_bars {
            let params = self.continuous.params(bar_type.instrument_id());
            actor.subscribe_bars(
                bar_type,
                None,
                Some(subscription_params(from, params.into_iter().flatten())),
            );
        }
        // One subscription per venue for the others: Nautilus routes a list of
        // bar types to the client of their one venue.
        let mut by_venue: BTreeMap<Venue, Vec<BarType>> = BTreeMap::new();
        for bar_type in others {
            by_venue
                .entry(bar_type.instrument_id().venue)
                .or_default()
                .push(bar_type);
        }
        for venue_bar_types in by_venue.into_values() {
            actor.subscribe_bars(venue_bar_types, None, Some(subscription_params(from, [])));
        }
    }

    /// The instruments of the tick series, each once, in order.
    fn instrument_ids(&self) -> Vec<InstrumentId> {
        let mut ids: Vec<InstrumentId> = Vec::new();
        for tick in &self.ticks {
            if !ids.contains(&tick.instrument_id) {
                ids.push(tick.instrument_id);
            }
        }
        ids
    }

    /// The kinds of the tick series of `instrument_id`.
    fn data_of(&self, instrument_id: InstrumentId) -> impl Iterator<Item = &NautilusDataType> + '_ {
        self.ticks
            .iter()
            .filter(move |tick| tick.instrument_id == instrument_id)
            .map(|tick| &tick.data)
    }

    /// Subscribes `actor` to the tick series of `instrument_id` on
    /// `contract` from `from`, its book in the format of its venue's live
    /// data (see the module).
    fn subscribe<A>(
        &self,
        actor: &mut A,
        instrument_id: InstrumentId,
        contract: InstrumentId,
        from: UnixNanos,
    ) where
        A: DataActor + DataActorNative + Debug + 'static,
    {
        let book = venues::book_format(contract.venue);
        for data in self.data_of(instrument_id) {
            let params = Some(subscription_params(from, []));
            match data {
                NautilusDataType::QuoteTick => actor.subscribe_quotes(contract, None, params),
                NautilusDataType::TradeTick => actor.subscribe_trades(contract, None, params),
                NautilusDataType::OrderBookDepth => actor.subscribe_book_depth(
                    contract,
                    BookType::L2_MBP,
                    book.and_then(|book| book.depth),
                    None,
                    false,
                    params,
                ),
                NautilusDataType::OrderBookDelta => actor.subscribe_book_deltas(
                    contract,
                    book.map_or(BookType::L2_MBP, |book| book.deltas),
                    None,
                    None,
                    false,
                    params,
                ),
                other => unreachable!("{other} is not a tick series (see `TickSeries`)"),
            }
        }
    }

    /// Unsubscribes `actor` from the tick series of `instrument_id` on the
    /// pre contract of `roll`, but for the kinds an instrument still reads
    /// on it at the roll, the contract standing for it then.
    fn unsubscribe<A>(&self, actor: &mut A, instrument_id: InstrumentId, roll: &Transition)
    where
        A: DataActor + DataActorNative + Debug + 'static,
    {
        let contract = roll.pre;
        for data in self.data_of(instrument_id) {
            let still_read = self
                .ticks
                .iter()
                .any(|tick| tick.data == *data && front(tick.instrument_id, roll.time) == contract);
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
                other => unreachable!("{other} is not a tick series (see `TickSeries`)"),
            }
        }
    }
}

/// Whether `roll` rolls `instrument_id`: it is a root, and `roll` its roll
/// at its time.
fn rolled_by(instrument_id: InstrumentId, roll: &Transition) -> bool {
    Transition::before(instrument_id, roll.time) == Some(*roll)
}

/// The params of a subscription streaming from `from`, bars, ticks or
/// custom data (`start_ns`, see the module), with `others`: the
/// `continuous` params of their root, if bars on one.
fn subscription_params(
    from: UnixNanos,
    others: impl IntoIterator<Item = (String, Value)>,
) -> Params {
    Params::new(
        [("start_ns".to_string(), json!(from.as_u64()))]
            .into_iter()
            .chain(others)
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, num::NonZeroUsize, rc::Rc};

    use nautilus_common::{
        cache::Cache,
        clock::{Clock, VirtualClock},
        messages::data::{DataCommand, SubscribeCommand, UnsubscribeCommand},
        msgbus::{
            self, MessagingSwitchboard,
            stubs::{TypedIntoMessageSavingHandler, get_typed_into_message_saving_handler},
            switchboard::get_custom_topic,
        },
        timer::{TimeEvent, TimeEventCallback},
    };
    use nautilus_core::UnixNanos;
    use nautilus_model::{
        data::NautilusDataType,
        enums::BookType,
        identifiers::{InstrumentId, StrategyId, TraderId},
    };
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

    /// A tick subscription the probe sent the data engine, or its end: its
    /// kind and its contract.
    #[derive(Debug, PartialEq, Eq)]
    enum Sent {
        Subscribe(NautilusDataType, InstrumentId),
        Unsubscribe(NautilusDataType, InstrumentId),
    }

    fn id(id: &str) -> InstrumentId {
        InstrumentId::from(id)
    }

    fn at(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    /// A strategy registered at `now` without a node, its clock, and what
    /// it sends the data engine from then on.
    fn registered(
        now: Timestamp,
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
        clock.borrow_mut().advance_time(now.into(), true);
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

    /// The tick subscriptions of `commands`, and their ends, in order.
    fn sent(commands: &TypedIntoMessageSavingHandler<DataCommand>) -> Vec<Sent> {
        use NautilusDataType as D;
        commands
            .get_messages()
            .into_iter()
            .filter_map(|command| match command {
                DataCommand::Subscribe(command) => {
                    let (data, instrument_id) = match command {
                        SubscribeCommand::Quotes(cmd) => (D::QuoteTick, cmd.instrument_ids[0]),
                        SubscribeCommand::Trades(cmd) => (D::TradeTick, cmd.instrument_ids[0]),
                        SubscribeCommand::BookDepth(cmd) => {
                            (D::OrderBookDepth, cmd.instrument_ids[0])
                        }
                        SubscribeCommand::BookDeltas(cmd) => {
                            (D::OrderBookDelta, cmd.instrument_ids[0])
                        }
                        SubscribeCommand::Bars(_) => return None,
                        other => panic!("unexpected {other:?}"),
                    };
                    Some(Sent::Subscribe(data, instrument_id))
                }
                DataCommand::Unsubscribe(command) => {
                    let (data, instrument_id) = match command {
                        UnsubscribeCommand::Quotes(cmd) => (D::QuoteTick, cmd.instrument_ids[0]),
                        UnsubscribeCommand::Trades(cmd) => (D::TradeTick, cmd.instrument_ids[0]),
                        UnsubscribeCommand::BookDepth(cmd) => {
                            (D::OrderBookDepth, cmd.instrument_ids[0])
                        }
                        UnsubscribeCommand::BookDeltas(cmd) => {
                            (D::OrderBookDelta, cmd.instrument_ids[0])
                        }
                        other => panic!("unexpected {other:?}"),
                    };
                    Some(Sent::Unsubscribe(data, instrument_id))
                }
                other => panic!("unexpected {other:?}"),
            })
            .collect()
    }

    fn ticks(series: &[(NautilusDataType, &str)]) -> Vec<TickSeries> {
        series
            .iter()
            .map(|(data, instrument_id)| TickSeries {
                data: data.clone(),
                instrument_id: id(instrument_id),
            })
            .collect()
    }

    /// The tick subscriptions of a strategy of `ticks` started at `start`
    /// and handed the rolls of GC up to `end`, each at the last nanosecond
    /// before it, as its roller hands it over (`rolls::Roller`).
    fn rolled(ticks: Vec<TickSeries>, start: &str, end: &str) -> Vec<Sent> {
        let subscriptions = Subscriptions::new(vec![], ticks, vec![]);
        let (mut probe, clock, commands) = registered(at(start));
        subscriptions.start(&mut probe, at(start));
        let mut time = at(start);
        while let Some(roll) = Transition::after(id("GC.GLBX"), time)
            && roll.time <= at(end)
        {
            let alert = roll.time - SignedDuration::from_nanos(1);
            clock.borrow_mut().advance_time(alert.into(), true);
            subscriptions.roll(&mut probe, &roll);
            time = roll.time;
        }
        sent(&commands)
    }

    /// GC rolls into GCG27 at 00:00 UTC on 2026-11-26 and into GCJ27 on
    /// 2027-01-27, two business days before the last business day of the
    /// month before delivery (`rolls/calendar.toml`): the root's trades are
    /// subscribed on GCZ26, standing for it on 2026-08-10, then moved at
    /// each roll.
    #[rstest]
    fn a_roots_series_are_subscribed_on_its_front_contract_and_moved_at_each_roll() {
        use NautilusDataType as D;

        let sent = rolled(
            ticks(&[(D::TradeTick, "GC.GLBX")]),
            "2026-08-10T14:30:00Z",
            "2027-02-01T00:00:00Z",
        );

        assert_eq!(
            sent,
            [
                Sent::Subscribe(D::TradeTick, id("GCZ26.GLBX")),
                Sent::Unsubscribe(D::TradeTick, id("GCZ26.GLBX")),
                Sent::Subscribe(D::TradeTick, id("GCG27.GLBX")),
                Sent::Unsubscribe(D::TradeTick, id("GCG27.GLBX")),
                Sent::Subscribe(D::TradeTick, id("GCJ27.GLBX")),
            ]
        );
    }

    /// A plain instrument stands for itself: subscribed once, and no roll
    /// of GC moves it.
    #[rstest]
    fn a_plain_instruments_series_are_subscribed_once_and_never_moved() {
        use NautilusDataType as D;

        let sent = rolled(
            ticks(&[
                (D::TradeTick, "GCZ26.GLBX"),
                (D::QuoteTick, "XAUUSD.ICMARKETS"),
            ]),
            "2026-08-10T14:30:00Z",
            "2027-02-01T00:00:00Z",
        );

        assert_eq!(
            sent,
            [
                Sent::Subscribe(D::TradeTick, id("GCZ26.GLBX")),
                Sent::Subscribe(D::QuoteTick, id("XAUUSD.ICMARKETS")),
            ]
        );
    }

    /// GCZ26 read as itself beside its root GC, which rolls off it into
    /// GCG27 on 2026-11-26: the root's trades and depth move to GCG27, and
    /// GCZ26's trades, which GCZ26 still reads, stay subscribed, its depth,
    /// which nothing reads there any more, not. Nautilus keeps one
    /// subscription per topic and actor (`DataActorCore::add_subscription_any`),
    /// so GCZ26's own trades, subscribed after the root's, send none.
    #[rstest]
    fn a_roll_keeps_the_series_another_instrument_still_reads_on_the_pre_contract() {
        use NautilusDataType as D;

        let sent = rolled(
            ticks(&[
                (D::TradeTick, "GC.GLBX"),
                (D::OrderBookDepth, "GC.GLBX"),
                (D::TradeTick, "GCZ26.GLBX"),
            ]),
            "2026-08-10T14:30:00Z",
            "2026-12-01T00:00:00Z",
        );

        assert_eq!(
            sent,
            [
                Sent::Subscribe(D::TradeTick, id("GCZ26.GLBX")),
                Sent::Subscribe(D::OrderBookDepth, id("GCZ26.GLBX")),
                Sent::Unsubscribe(D::OrderBookDepth, id("GCZ26.GLBX")),
                Sent::Subscribe(D::TradeTick, id("GCG27.GLBX")),
                Sent::Subscribe(D::OrderBookDepth, id("GCG27.GLBX")),
            ]
        );
    }

    /// The handlers subscribed to `data_type` on the message bus: a
    /// subscription to custom data naming no data client subscribes the
    /// message bus alone (`DataActorCore::subscribe_data`).
    fn subscribers(data_type: &DataType) -> usize {
        msgbus::subscriptions_count_any(get_custom_topic(data_type)).unwrap()
    }

    /// The handlers subscribed to the imbalances under `identifier` on the
    /// message bus (see `subscribers`).
    fn imbalance_subscribers(identifier: &str) -> usize {
        subscribers(&DataType::new(
            "Imbalance",
            None,
            Some(identifier.to_string()),
        ))
    }

    /// The metadata of LSE's daily 10-year US yields among its rows.
    fn us10y_metadata() -> Params {
        Params::new(
            [
                ("source".to_string(), json!("bond_yields")),
                ("symbol".to_string(), json!("US10Y")),
            ]
            .into(),
        )
    }

    /// LSE's daily 10-year US yields under `identifier`, a custom series of
    /// LSE's rows, whose type's row's live data source is LSE.
    fn lse_yields(identifier: &str) -> CustomSeries {
        CustomSeries::new("LseRow", identifier.to_string(), Some(us10y_metadata())).unwrap()
    }

    /// The data type of LSE's daily 10-year US yields under `identifier`,
    /// as Nautilus builds it.
    fn lse_yields_type(identifier: &str) -> DataType {
        DataType::new(
            "LseRow",
            Some(us10y_metadata()),
            Some(identifier.to_string()),
        )
    }

    /// Custom series, each a data type keyed by its identifier, here
    /// LSE's 10-year US yields, whose type's row has a live data client,
    /// and the tests' 2-year US yields, of a type with no row, are
    /// subscribed at the start of a replay by their data type, naming no
    /// data client, whatever their type: on the message bus alone, where a
    /// replay's rows arrive, sending the data engine nothing.
    #[rstest]
    fn a_replay_subscribes_custom_series_at_the_start_naming_no_client() {
        let us02y = CustomSeries::new("Yield", "US02Y".to_string(), None).unwrap();
        let subscriptions = Subscriptions::new(vec![], vec![], vec![lse_yields("US10Y"), us02y]);
        let (mut probe, _, commands) = registered(at("2026-08-10T14:30:00Z"));

        subscriptions.start(&mut probe, at("2026-08-10T14:30:00Z"));

        let us02y = DataType::new("Yield", None, Some("US02Y".to_string()));
        assert_eq!(
            [subscribers(&lse_yields_type("US10Y")), subscribers(&us02y)],
            [1, 1]
        );
        assert!(commands.get_messages().is_empty());
    }

    /// A custom data subscription a probe sent the data engine, or its
    /// end: its data type and the data client it names.
    #[derive(Debug, PartialEq, Eq)]
    enum SentCustom {
        Subscribe(DataType, Option<ClientId>),
        Unsubscribe(DataType, Option<ClientId>),
    }

    /// The custom data subscriptions of `commands`, and their ends, in
    /// order.
    fn custom_sent(commands: &TypedIntoMessageSavingHandler<DataCommand>) -> Vec<SentCustom> {
        commands
            .get_messages()
            .into_iter()
            .filter_map(|command| match command {
                DataCommand::Subscribe(SubscribeCommand::Data(cmd)) => {
                    Some(SentCustom::Subscribe(cmd.data_type, cmd.client_id))
                }
                DataCommand::Unsubscribe(UnsubscribeCommand::Data(cmd)) => {
                    Some(SentCustom::Unsubscribe(cmd.data_type, cmd.client_id))
                }
                _ => None,
            })
            .collect()
    }

    /// On a live node, LSE's yields are subscribed naming LSE's client, the
    /// data client of their type's row's live data source, so Nautilus
    /// sends it the subscription; the tests' imbalances, of a type with no
    /// row, name none, subscribed on the message bus alone.
    #[rstest]
    fn a_live_custom_subscription_names_the_live_data_client_of_its_type_s_row() {
        let subscriptions = Subscriptions::new(
            vec![],
            vec![],
            vec![lse_yields("US10Y"), imbalances("GCZ26.GLBX")],
        )
        .live();
        let (mut probe, _, commands) = registered(at("2026-08-10T14:30:00Z"));

        subscriptions.start(&mut probe, at("2026-08-10T14:30:00Z"));

        assert_eq!(
            custom_sent(&commands),
            [SentCustom::Subscribe(
                lse_yields_type("US10Y"),
                Some(ClientId::from("LSE"))
            )]
        );
        assert_eq!(imbalance_subscribers("GCZ26.GLBX"), 1);
    }

    /// On a live node, LSE's yields under the root GC are subscribed on
    /// GCZ26's data type, the contract standing for GC on 2026-08-10, and
    /// moved to GCG27's at GC's roll on 2026-11-26, each subscription and
    /// its end naming LSE's client.
    #[rstest]
    fn a_live_roll_moves_a_roots_custom_series_naming_the_same_client() {
        let subscriptions = Subscriptions::new(vec![], vec![], vec![lse_yields("GC.GLBX")]).live();
        let start = at("2026-08-10T14:30:00Z");
        let (mut probe, clock, commands) = registered(start);
        let roll = Transition::after(id("GC.GLBX"), start).unwrap();

        subscriptions.start(&mut probe, start);
        let alert = roll.time - SignedDuration::from_nanos(1);
        clock.borrow_mut().advance_time(alert.into(), true);
        subscriptions.roll(&mut probe, &roll);

        let lse = Some(ClientId::from("LSE"));
        assert_eq!(
            custom_sent(&commands),
            [
                SentCustom::Subscribe(lse_yields_type("GCZ26.GLBX"), lse),
                SentCustom::Unsubscribe(lse_yields_type("GCZ26.GLBX"), lse),
                SentCustom::Subscribe(lse_yields_type("GCG27.GLBX"), lse),
            ]
        );
    }

    /// The imbalances under `identifier`, a custom series.
    fn imbalances(identifier: &str) -> CustomSeries {
        CustomSeries::new("Imbalance", identifier.to_string(), None).unwrap()
    }

    /// A strategy reading `custom`, started on 2026-08-10 at 14:30, when
    /// GCZ26 stands for GC, then handed GC's roll into GCG27 at 00:00 UTC
    /// on 2026-11-26, at the last nanosecond before it, as its roller hands
    /// it over: the handlers subscribed to the imbalances under each of
    /// `identifiers` on the message bus at the start, then after the roll,
    /// and whether it sent the data engine anything.
    fn imbalances_across_the_roll(
        custom: Vec<CustomSeries>,
        identifiers: &[&str],
    ) -> (Vec<usize>, Vec<usize>, bool) {
        let subscriptions = Subscriptions::new(vec![], vec![], custom);
        let start = at("2026-08-10T14:30:00Z");
        let (mut probe, clock, commands) = registered(start);
        let roll = Transition::after(id("GC.GLBX"), start).unwrap();
        let counts = || {
            identifiers
                .iter()
                .map(|&identifier| imbalance_subscribers(identifier))
                .collect::<Vec<_>>()
        };

        subscriptions.start(&mut probe, start);
        let started = counts();
        let alert = roll.time - SignedDuration::from_nanos(1);
        clock.borrow_mut().advance_time(alert.into(), true);
        subscriptions.roll(&mut probe, &roll);

        (started, counts(), !commands.get_messages().is_empty())
    }

    /// A custom series on the root GC is subscribed on GCZ26's data type,
    /// the contract standing for GC on 2026-08-10, not the root's, and
    /// moved to GCG27's at the roll, naming no client either time.
    #[rstest]
    fn a_roots_custom_series_is_subscribed_on_its_front_contract_and_moved_at_its_roll() {
        let (started, rolled, sent) = imbalances_across_the_roll(
            vec![imbalances("GC.GLBX")],
            &["GC.GLBX", "GCZ26.GLBX", "GCG27.GLBX"],
        );

        assert_eq!(
            (started, rolled, sent),
            (vec![0, 1, 0], vec![0, 0, 1], false)
        );
    }

    /// GCZ26's own imbalances, listed beside the root's, are still read
    /// on GCZ26 at the roll: they stay subscribed, the root's moving on.
    #[rstest]
    fn a_roll_keeps_a_custom_series_another_still_reads_on_the_pre_contract() {
        let (started, rolled, _) = imbalances_across_the_roll(
            vec![imbalances("GC.GLBX"), imbalances("GCZ26.GLBX")],
            &["GCZ26.GLBX", "GCG27.GLBX"],
        );

        assert_eq!((started, rolled), (vec![1, 0], vec![1, 1]));
    }

    /// A contract's custom series and one of no instrument stand for
    /// themselves: subscribed at the start, and no roll of GC moves them.
    #[rstest]
    fn custom_series_of_no_root_are_never_moved() {
        let (started, rolled, _) = imbalances_across_the_roll(
            vec![imbalances("GCZ26.GLBX"), imbalances("US10Y")],
            &["GCZ26.GLBX", "US10Y", "GCG27.GLBX"],
        );

        assert_eq!((started, rolled), (vec![1, 1, 0], vec![1, 1, 0]));
    }

    /// The `start_ns` param of each subscription of `commands`, bars and
    /// ticks.
    fn starts(commands: &TypedIntoMessageSavingHandler<DataCommand>) -> Vec<Option<u64>> {
        commands
            .get_messages()
            .into_iter()
            .filter_map(|command| match command {
                DataCommand::Subscribe(command) => {
                    let params = match command {
                        SubscribeCommand::Bars(cmd) => cmd.params,
                        SubscribeCommand::Quotes(cmd) => cmd.params,
                        SubscribeCommand::Trades(cmd) => cmd.params,
                        other => panic!("unexpected {other:?}"),
                    };
                    Some(params.and_then(|params| params.get_u64("start_ns")))
                }
                _ => None,
            })
            .collect()
    }

    /// A start subscribes from the time it is given, where a live warmup's
    /// requests end, not from the clock's time when it subscribes, a few
    /// moments later live: bars and ticks alike.
    #[rstest]
    fn a_start_subscribes_from_the_time_it_is_given() {
        use NautilusDataType as D;
        let subscriptions = Subscriptions::new(
            vec![BarType::from("XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL")],
            ticks(&[
                (D::QuoteTick, "EURUSD.ICMARKETS"),
                (D::TradeTick, "GC.GLBX"),
            ]),
            vec![],
        );
        let warmed_to = at("2026-08-10T14:30:00Z");
        let (mut probe, _, commands) = registered(at("2026-08-10T14:30:05Z"));

        subscriptions.start(&mut probe, warmed_to);

        let from = Some(UnixNanos::from(warmed_to).as_u64());
        assert_eq!(starts(&commands), [from, from, from]);
    }

    /// At a roll the post contract is subscribed from the clock's time
    /// then, the roll's.
    #[rstest]
    fn a_roll_subscribes_the_post_contract_from_the_time_of_the_roll() {
        let subscriptions = Subscriptions::new(
            vec![],
            ticks(&[(NautilusDataType::TradeTick, "GC.GLBX")]),
            vec![],
        );
        let (mut probe, clock, commands) = registered(at("2026-08-10T14:30:00Z"));
        subscriptions.start(&mut probe, at("2026-08-10T14:30:00Z"));
        let roll = Transition::after(id("GC.GLBX"), at("2026-08-10T14:30:00Z")).unwrap();
        let alert = roll.time - SignedDuration::from_nanos(1);
        clock.borrow_mut().advance_time(alert.into(), true);

        subscriptions.roll(&mut probe, &roll);

        let started = Some(UnixNanos::from(at("2026-08-10T14:30:00Z")).as_u64());
        let rolled = Some(UnixNanos::from(alert).as_u64());
        assert_eq!(starts(&commands), [started, rolled]);
    }

    /// The bar subscriptions of `commands`: each its bar types.
    fn bars_subscribed(commands: &TypedIntoMessageSavingHandler<DataCommand>) -> Vec<Vec<BarType>> {
        commands
            .get_messages()
            .into_iter()
            .filter_map(|command| match command {
                DataCommand::Subscribe(SubscribeCommand::Bars(cmd)) => Some(cmd.bar_types),
                _ => None,
            })
            .collect()
    }

    /// Bars on plain instruments are subscribed one subscription per
    /// venue: Nautilus routes a list of bar types to the client of their
    /// one venue.
    #[rstest]
    fn bars_are_subscribed_one_subscription_per_venue() {
        let gold = BarType::from("XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL");
        let euro = BarType::from("EURUSD.ICMARKETS-1-HOUR-MID-INTERNAL");
        let bid = BarType::from("EUR/USD.IDEALPRO-1-MINUTE-BID-EXTERNAL");
        let ask = BarType::from("EUR/USD.IDEALPRO-1-MINUTE-ASK-EXTERNAL");
        let subscriptions = Subscriptions::new(vec![gold, bid, euro, ask], vec![], vec![]);
        let (mut probe, _, commands) = registered(at("2026-08-10T14:30:00Z"));

        subscriptions.start(&mut probe, at("2026-08-10T14:30:00Z"));

        assert_eq!(
            bars_subscribed(&commands),
            [vec![gold, euro], vec![bid, ask]]
        );
    }

    /// The book subscriptions of `commands`: each its instrument, its
    /// kind, its book type and its depth.
    fn books_subscribed(
        commands: &TypedIntoMessageSavingHandler<DataCommand>,
    ) -> Vec<(
        InstrumentId,
        NautilusDataType,
        BookType,
        Option<NonZeroUsize>,
    )> {
        commands
            .get_messages()
            .into_iter()
            .filter_map(|command| match command {
                DataCommand::Subscribe(SubscribeCommand::BookDeltas(cmd)) => Some((
                    cmd.instrument_ids[0],
                    NautilusDataType::OrderBookDelta,
                    cmd.book_type,
                    cmd.depth,
                )),
                DataCommand::Subscribe(SubscribeCommand::BookDepth(cmd)) => Some((
                    cmd.instrument_ids[0],
                    NautilusDataType::OrderBookDepth,
                    cmd.book_type,
                    cmd.depth,
                )),
                _ => None,
            })
            .collect()
    }

    /// Each book in the format its venue's live data streams: a GLBX
    /// contract's, LSE's, its deltas as orders (L3_MBO) and its depth ten
    /// levels a side; a CME contract's, IB's, its deltas as rows of IB's
    /// market depth (L2_MBP); an OKX swap's its deltas as price levels
    /// (L2_MBP) and its depth five levels a side. Deltas of the whole book
    /// each client streams, no depth asked; depth snapshots of price
    /// levels.
    #[rstest]
    fn each_book_is_subscribed_in_its_venue_s_format() {
        use NautilusDataType as D;
        let swap = "BTC-USDT-SWAP.OKX";
        let subscriptions = Subscriptions::new(
            vec![],
            ticks(&[
                (D::OrderBookDelta, "GCZ26.GLBX"),
                (D::OrderBookDepth, "GCZ26.GLBX"),
                (D::OrderBookDelta, "ESZ26.CME"),
                (D::OrderBookDelta, swap),
                (D::OrderBookDepth, swap),
            ]),
            vec![],
        );
        let (mut probe, _, commands) = registered(at("2026-08-10T14:30:00Z"));

        subscriptions.start(&mut probe, at("2026-08-10T14:30:00Z"));

        let levels = NonZeroUsize::new;
        assert_eq!(
            books_subscribed(&commands),
            [
                (id("GCZ26.GLBX"), D::OrderBookDelta, BookType::L3_MBO, None),
                (
                    id("GCZ26.GLBX"),
                    D::OrderBookDepth,
                    BookType::L2_MBP,
                    levels(10)
                ),
                (id("ESZ26.CME"), D::OrderBookDelta, BookType::L2_MBP, None),
                (id(swap), D::OrderBookDelta, BookType::L2_MBP, None),
                (id(swap), D::OrderBookDepth, BookType::L2_MBP, levels(5)),
            ]
        );
    }

    /// A book its venue's live data streams none of, a cTrader broker's
    /// deltas, its venue having no book, and an IB instrument's depth,
    /// IB's having none: a live start refuses it first
    /// (`venues::streams`), so only a replay subscribes to it, which reads
    /// neither the book type nor the levels, and it replays as declared.
    /// Deltas as L2_MBP, depth with no levels.
    #[rstest]
    fn a_book_its_venue_streams_none_of_is_subscribed_as_l2_mbp_with_no_levels() {
        use NautilusDataType as D;
        let subscriptions = Subscriptions::new(
            vec![],
            ticks(&[
                (D::OrderBookDelta, "XAUUSD.ICMARKETS"),
                (D::OrderBookDepth, "SXR8.IBIS"),
            ]),
            vec![],
        );
        let (mut probe, _, commands) = registered(at("2026-08-10T14:30:00Z"));

        subscriptions.start(&mut probe, at("2026-08-10T14:30:00Z"));

        assert_eq!(
            books_subscribed(&commands),
            [
                (
                    id("XAUUSD.ICMARKETS"),
                    D::OrderBookDelta,
                    BookType::L2_MBP,
                    None
                ),
                (id("SXR8.IBIS"), D::OrderBookDepth, BookType::L2_MBP, None),
            ]
        );
    }
}
