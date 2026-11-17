//! The warmup of strategies on bars Nautilus builds (internal time bars) on
//! a live node: a warmup request at start, which hydrates the strategy's
//! indicators before the live stream. A backtest or a dataset run makes no
//! request: its indicators warm on the data replayed, from the run's start.
//! The warmup subscribes to nothing: the strategy subscribes through its
//! subscriptions (`strategy::subscriptions`), from the time its warmup's
//! start returns (`Warmup::start`), where a live warmup's requests end.
//! Either way a strategy decides once its indicators are initialized
//! (`indicators_initialized`, a model strategy's handle reading its own);
//! a model strategy, on a timer, also waits for a live warmup to end
//! (`Warmup::is_warming`).
//!
//! Owns Nautilus' protocol, so that no strategy names it. The request is
//! chosen from the bar type alone: a composite bar type is built from its
//! source bars, so their bars over `[now - warmup, now]` are requested
//! (`request_bars` of `BarType::composite`); an external bar type, which
//! its venue builds (a conversion pair's BID and ASK bars on IB), is
//! requested as it is (`request_bars` of itself), building nothing, as
//! Nautilus aggregates a request into internal bar types alone; any other
//! bar type is built from the ticks its price type names, as Nautilus
//! builds it: LAST bars from trades, so the trades of its instrument are
//! requested (`request_trades`), BID, ASK and MID bars from quotes, so its
//! quotes are (`request_quotes`), over the same window.
//!
//! The window ends at `now`, given, not left open: the data engine trims a
//! response to its request's bounds (`DataResponse::trim_to_bounds`), so
//! the warmup ends on the last bar closed by `now`, and the live stream,
//! which starts at the subscription, continues with the bar in progress.
//! Left open, IB's answer holds that bar too, partial (stamped at its
//! close, after `now`), and the live stream sends it again, complete.
//!
//! Long requests come a window at a time: each is one of Nautilus' time-range
//! requests in windows of 23 hours (`WINDOW`, `time_range`), the data engine
//! asking a client for one window, handing its answer on, writing it back on
//! its own and asking for the next, a catalog answering what it holds of each.
//! One aggregator builds a request's bars across its windows, its params
//! being the request's, not the windows'. Nautilus ignores the windows of a
//! continuous future's request and takes none on custom data. A window of
//! deltas whose day's start a catalog holds, earlier windows' write-back
//! included, is read from there and opens with a snapshot of the book at its
//! own start, so a book indicator reads one per window beside the day's. The
//! node runs on between windows, so features can be
//! initialized on the older windows alone; a decision waits for the end.
//!
//! Each request carries the params
//! `bar_types` (the data engine aggregates the response into those bars,
//! none for a request building none, as an external bar type's),
//! `update_subscriptions` (the aggregator goes on with the live data, so
//! the bar spanning the warmup end comes out once and complete; without it
//! that bar comes out partial, or as a one-update bar when the warmup ends
//! on a bar close), `skip_first_non_full_bar` (the oldest bar, missing
//! the data before `now - warmup`, is dropped) and `update_catalog` (with
//! a catalog registered, the engine answers from it first and asks the
//! venue only for what it lacks, then writes back the answers a data
//! client certifies complete, as IC's does). The bars a request builds
//! reach only the cache, Nautilus delivering no built bars, under their
//! standard type, a composite's without its source: the warmup hands them to
//! its indicators, each once, when the request completes
//! (`Warmup::on_data_response`). An external bar type's request hands its
//! indicators nothing: its bars come in its responses, which the data
//! engine caches (where `get_xrate` reads a conversion pair's), and no
//! indicator reads external bars, which the strategies' bar types are not.
//! A response's data is that window's alone, and the request's completion
//! carries none.
//!
//! First, a live start asks the definition of each instrument it warms up
//! that the cache lacks (`request_instrument`), naming the client the
//! venue's row names (`venues::live_definitions_client`): its broker's,
//! else its live data source's. The data engine answers it from a catalog
//! before asking that client (`dispatch_instrument_request` in
//! `data/src/engine/streaming.rs`) and caches it as it takes the request;
//! it takes an actor's commands in the order sent, through one queue
//! (`DataEngine.queue_execute`), so the definition is in the cache before
//! it takes the warmup's requests and the strategy's subscriptions, sent
//! after it (`strategy::subscriptions`): LSE's client, which publishes no
//! definitions and reads LSE's prices at the definition's precision, finds
//! it there. Definitions the broker clients cached when they connected are
//! not asked, so a catalog's older copy does not replace them. A custom
//! series asks none: it brings no instrument (see below).
//!
//! The requests form one request group (`DataActor::request_group`):
//! Nautilus calls `on_data_response` with the group once every request is
//! answered, and with the first failure if one failed (a page of the venue
//! lost, say), its data received up to then kept.
//!
//! The warmup's start returns its `now`, from which the strategy then
//! subscribes to the bars and the tick series (`strategy::subscriptions`),
//! so the live stream starts where the warmup ends; it subscribes to every
//! bar type the warmup builds, the harness's among them (the target's
//! ATR's, `model::targets`), so their aggregators go on from the warmup's
//! (`update_subscriptions`).
//!
//! The cache keeps at most `bar_capacity` bars per bar type and evicts the
//! oldest beyond it, so the live node running the strategy takes its cache
//! config from `Warmup::cache_config`, which holds the whole warmup of
//! every bar type the warmup builds or, if external, requests.
//!
//! An hour with no tick or source bar at all means the market is closed
//! (a night, a weekend, a holiday): the data engine builds no bar for it,
//! in the warmup as after it. Nautilus' default builds one, flat at the
//! last close, which a session on the time of day alone would trade on. So
//! the node running the strategy takes its data engine config from
//! `data_engine_config` (backtest) or `live_data_engine_config` (live),
//! which turn `time_bars_build_with_no_updates` off. Both build time bars
//! `bar_build_delay` after their close, the delay of the strategy's bar
//! types.
//!
//! Bars on a futures root (`GC.GLBX`) are Nautilus' continuous bars, built
//! from the trades of the contract standing for the root and adjusted
//! across its rolls (see `rolls::ContinuousBars`): their request is
//! `request_bars` of the bar type itself, one per bar type, carrying the
//! root's `continuous_future_*` params, as their subscription does
//! (`strategy::subscriptions`). Nautilus builds such a request's bars into
//! the cache only, with neither `update_subscriptions` nor
//! `skip_first_non_full_bar`, and answers it with no bars; `continuous_bars`
//! reads them once the group ended.
//!
//! The tick series of the strategy's data requirements are requested over
//! the same window in the same group, whatever reads them: those its
//! features read, the quotes of the pairs its equity converts through
//! (`venues::rate_series`), which Nautilus caches as it does every
//! response's quotes, so that a trade sized at the start has their rate,
//! and a traded instrument's quotes no series prices. Each is requested
//! catalog first with `update_catalog`, a root's on the contract standing
//! for it at the start (`rolls::front`) alone: `request_quotes`, `request_trades`, `request_book_depth` and
//! `request_book_deltas` (with the book type of its venue's book, which the
//! strategy subscribes to, `venues::book_format`, onto which Nautilus replays
//! the deltas from the day's start into the snapshot a window opens with).
//! A series some bars are built from is requested once, building them.
//!
//! A series, bars' source or tick series, is requested only if its venue's
//! history serves it (`venues::serves`), the rule fill-catalog fills by:
//! one it does not serve, an OKX instrument's quotes, is requested no
//! history, which the warmup logs as a warning naming why; the series is
//! subscribed from the start all the same, and the bars built from it
//! start there. A series its venue's live data does not stream is refused
//! before a live node starts (`venues::streams`). The definitions of the
//! instruments of every series are asked, served or not, as their
//! clients need them.
//!
//! The last window ends at `now`, trimmed to it, and the strategy's
//! subscriptions start from that `now` (`start_ns`, see
//! `strategy::subscriptions`). A venue can resend its latest tick on a
//! subscription, as IC and IB do on quotes, and Nautilus forwards it, so
//! the warmup's last tick can come once more as the first live one, made as
//! it was.
//!
//! The windows of a response arrive one at a time with the node running
//! between them, and the live stream starts at once, so live ticks come
//! while older windows are pending. Nautilus holds live ticks for the bar
//! aggregators it builds a request's bars with, not for an actor's
//! indicators. So the warmup hands its tick indicators (`feeding_ticks`, a
//! model strategy's features) the quotes and trades of its responses, from
//! the actor's `on_historical_quotes` and `on_historical_trades`, and holds
//! the live ones the actor hands it from `on_quote` and `on_trade` until
//! the group ends, then hands them over in the order they came, dropping
//! those the history holds (`hold`, which says the rule and what upstream
//! change replaces it); after the warmup, and in a replay, live ticks go
//! straight through. Nautilus routes depth and deltas to no indicator, only
//! to the actor's handlers, so the warmup hands a response's to its book
//! indicators (`BookIndicator`, `feeding_books`), from the actor's
//! `on_historical_book_depth` and `on_historical_book_deltas`, the deltas in
//! the batches the live stream carries (`batches`), and the live ones the
//! actor hands it from `on_book_depth` and `on_book_deltas` the same way,
//! held as live ticks are.
//!
//! Each series' history comes in the windows of its own request, in the
//! order the venues answer them, not in time order across series, so a
//! feature reading several series would read their history out of order.
//! The warmup therefore merges its history across series (`merge`, which
//! says the rule): Temporary until Nautilus hands a warmup's history in
//! time order across data types, helix-trader#46. It queues each requested
//! series' quotes, trades, depth, deltas and custom data and hands them to
//! the hold in `ts_event` order up to the earliest, over the series whose
//! requests are still pending, of the latest `ts_event` each brought; a
//! series whose request completes or fails holds the others back no
//! longer, ties go quotes, trades, depth, deltas, then custom data, and
//! the group's end hands on all that is held. Past `merge::HELD` ticks
//! held, the warmup ends incomplete, naming the series held back, and the
//! rest of its history is left out. The hold's seams move as the merge
//! hands ticks on. It is separate from the hold, which goes with
//! helix-trader#16 while the merge stays until its own issue closes. The
//! bars a request builds are not merged: they reach the indicators when it
//! completes, as above. A replay merges nothing, Nautilus replaying in time
//! order already.
//!
//! Each custom series of the requirements (`DataRequirements::custom`, an
//! LSE reference series, an instrument's custom data under its id or the
//! catalog's own) is requested in the same group over the same window
//! with `request_data` of its data type as read at the start
//! (`CustomSeries::at`), a root's the front contract's, each data type
//! once, by one rule (`custom_request`): if its type's row's history
//! serves it, naming the data client of the row's live data source on the
//! node (`venues::custom_data_client`), LSE's for LSE's rows, with
//! `update_catalog`, the catalog answering first and the client the rest,
//! written back, as with every series served; if its type has no row, or
//! one with no history source, naming `CATALOG`, which no node registers,
//! with no params, and Nautilus answers it from the catalog alone
//! (`dispatch_date_range_request`, `crates/data/src/engine/streaming.rs`):
//! it is the catalog's, written into it from outside helix
//! (`venues::serves_custom`), and a client would fail the request, ending
//! the warmup incomplete. No definition is requested for it: it brings no
//! instrument. Raw features read custom data from Nautilus' cache, where
//! the data engine puts each point it processes, live or replayed, but
//! not a request's response (`docs/concepts/cache.md`, "Custom data"): the
//! warmup adds the response's points to the cache itself, oldest first,
//! as Nautilus' docs ask (`Cache::add_custom_data`), but a point no later
//! than the latest the cache holds of its data type
//! (`Warmup::on_historical_data`). Nautilus routes custom data to no
//! indicator, only to the actor's `on_data` and `on_historical_data`, so
//! the warmup hands a response's points to its custom indicators too
//! (`CustomIndicator`, `feeding_custom`), and the live ones the actor hands
//! it from `on_data` the same way, held as live ticks are. No source's live client streams a
//! custom series yet, LSE's streaming none and nothing the catalog's, so a
//! live start refuses a run reading one (`venues::streams_custom`) and no
//! run reaches this request today: it is the warmup half of such a run,
//! whose subscription names the same client (`strategy::subscriptions`),
//! kept for when a source streams it.
//!
//! A strategy holds a `Warmup`, which runs these calls, and delegates its
//! hooks to it; a model strategy's `ModelHandle` builds and owns it, and
//! `model_data_hooks!` writes the model strategy's hooks. The functions are
//! its parts: `start` from `on_start`, `Warmup::on_historical_quotes`,
//! `on_historical_trades`, `on_historical_book_depth`,
//! `on_historical_book_deltas` and `on_historical_data` from the hooks of
//! the same names, `Warmup::on_quote`, `on_trade`, `on_book_depth`,
//! `on_book_deltas` and `on_data` from those, and
//! `Warmup::on_data_response`, or `finished` alone, from `on_data_response`,
//! then, on bars on a root, `continuous_bars`; in a backtest or a dataset run
//! nothing, the strategy subscribing alone.

use std::{
    any::Any,
    cell::{Cell, RefCell},
    collections::HashMap,
    fmt::Debug,
    rc::Rc,
};

use anyhow::Context;
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, DataActorNative, indicators::SharedActorIndicator},
    cache::CacheConfig,
    messages::data::DataResponseEnvelope,
};
use nautilus_core::{Params, UUID4};
use nautilus_data::engine::config::DataEngineConfig;
use nautilus_live::config::LiveDataEngineConfig;
use nautilus_model::{
    data::{
        Bar, BarType, CustomData, NautilusDataType, OrderBookDelta, OrderBookDeltas,
        OrderBookDepth, QuoteTick, TradeTick,
    },
    enums::RecordFlag,
    identifiers::{ClientId, InstrumentId},
};
use serde_json::{Value, json};

use crate::{
    rolls::{ContinuousBars, front},
    strategy::requirements::{
        CatalogSeries, CustomSeries, DataRequirements, TickSeries, bar_sources,
    },
    time_range, venues,
};

mod hold;
mod merge;
mod tick;

use hold::Hold;
use merge::Merge;
use tick::{Series, Tick};

/// The window a warmup is asked in (see `time_range`): under a day by more
/// than a bar. IB's client asks a window as one request between its bounds,
/// from a bar before its start, keeping the bars within it
/// (nautilus_trader#5621), and ibapi sends a span of up to a day in
/// seconds, a longer one in whole days plus one, which IB counts in
/// sessions: 23 hours of 1-minute bars go as `82860 S` at most, a day's
/// window as `3 D`, three sessions fetched to keep one day's bars. Venues
/// page within a window.
const WINDOW: SignedDuration = SignedDuration::from_hours(23);

/// The data client a warmup's request of a custom series names when no
/// source's history serves it (`venues::serves_custom`, see
/// `custom_request`), one served naming its source's live data
/// client: such data is written into the catalog from outside helix, and
/// the catalog alone holds it. Nautilus' data engine answers a request
/// naming no registered client from its catalogs alone, planning no client
/// leg for what they lack (`dispatch_date_range_request` in
/// `data/src/engine/streaming.rs`: `if !has_client {
/// missing_intervals.clear(); }`), and an empty response where they hold
/// nothing. A live node registers its catalog (`live::node`); with none,
/// the engine would send the request to the client and fail it.
const CATALOG: &str = "CATALOG";

/// What a warmup hands the book depth and deltas of its responses to,
/// which Nautilus routes to no indicator (`ActorIndicator` reads quotes,
/// trades and bars alone): a model strategy's features. Temporary until
/// Nautilus routes book depth and deltas to registered indicators:
/// helix-trader#47.
pub trait BookIndicator {
    /// Reads `depth`, a snapshot of the warmup, oldest first.
    fn handle_depth(&self, depth: &OrderBookDepth);

    /// Reads `deltas`, a batch of the warmup as the live stream carries
    /// it, oldest first.
    fn handle_deltas(&self, deltas: &OrderBookDeltas);
}

/// What a warmup hands custom data to, of its responses and live, which
/// Nautilus hands an actor's `on_historical_data` and `on_data` alone, its
/// `Indicator` reading none: a model strategy's features.
pub trait CustomIndicator {
    /// Reads `data`, a point of the warmup, oldest first, or a live one.
    fn handle_custom(&self, data: &CustomData);
}

/// A strategy's warmup, built from its data requirements for a live node
/// (`live`) or a replay (`replayed`, a backtest or a dataset run): the one
/// owner of the protocol above, so that a strategy holds one and its hooks
/// delegate to it. Nautilus delivers historical data only to the
/// strategy's own hooks, so a strategy calls `start` from `on_start`,
/// `on_data_response` from `on_data_response`, `on_historical_quotes`,
/// `on_historical_trades`, `on_historical_book_depth`,
/// `on_historical_book_deltas` and `on_historical_data` from theirs, and
/// `on_quote`, `on_trade`, `on_book_depth`, `on_book_deltas` and `on_data`
/// from its live hooks.
///
/// Live, `start` requests the warmup of the bar types the strategy reads,
/// of those the strategy's harness needs warm (`building`, the target's
/// ATR's) and of the tick series of its requirements its venue's history
/// serves (`venues::serves`), and of its custom series, and returns the
/// warmup's end, from
/// which the strategy subscribes (`strategy::subscriptions`) to those bars
/// and ticks; the bars each request built
/// go to the indicators the warmup feeds (`feeding`, `building`), oldest
/// first, when it completes, each response's quotes and trades to its tick
/// indicators (`feeding_ticks`), holding the live ones until the group ends,
/// its depth and deltas to its book indicators (`feeding_books`), holding
/// the live ones as ticks, its custom data to the cache
/// (`on_historical_data`) and to its custom indicators (`feeding_custom`),
/// holding the live points as ticks; the quotes, trades, depth, deltas and
/// custom data of the responses in time order across the series (`merge`).
/// On a replay `start` requests nothing and returns the clock's time; the
/// live ticks, depth, deltas and custom data go straight to their
/// indicators.
/// The strategies read no bars on a root, so a warmup has no continuous
/// bars. A live node sizes its cache from `cache_config`, which holds the
/// warmup of every bar type the warmup builds or, if external, requests.
pub struct Warmup {
    start: Start,
    /// How far back the live request starts: the requirements' warmup.
    length: SignedDuration,
    /// The bar types the strategy reads.
    bar_types: Vec<BarType>,
    /// The bar types the harness needs warm beyond those.
    harness: Vec<BarType>,
    /// The tick series of the requirements, of roots or plain instruments,
    /// warmed as they are if served.
    ticks: Vec<TickSeries>,
    /// The custom series of the requirements, warmed by their data type.
    custom: Vec<CustomSeries>,
    /// What the warmup's bars are handed to.
    indicators: Vec<SharedActorIndicator>,
    /// What hands the history of a live start's series on in time order
    /// across them, to the hold (see `merge`).
    merge: Merge,
    /// What hands the quotes, trades, depth, deltas and custom data, of the
    /// warmup and live, to the indicators reading them, holding the live
    /// ones while a live start's group is pending.
    hold: Hold,
    /// The request group of the last live start.
    group: Cell<UUID4>,
    /// Whether that group has not ended yet.
    warming: Cell<bool>,
    /// The bar types each request of that start not yet complete builds.
    building: RefCell<HashMap<UUID4, Vec<BarType>>>,
}

/// What a warmup's `start` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Start {
    /// Requests the warmup: a live node.
    Request,
    /// Requests nothing: a replay, whose indicators warm on its data.
    Replay,
}

impl Warmup {
    /// The warmup of a strategy of `requirements` on a live node: its
    /// start requests `requirements.warmup` of history.
    #[must_use]
    pub fn live(requirements: &DataRequirements) -> Self {
        Self::of(requirements, Start::Request)
    }

    /// The warmup of a strategy of `requirements` in a backtest or a
    /// dataset run: its start requests nothing.
    #[must_use]
    pub fn replayed(requirements: &DataRequirements) -> Self {
        Self::of(requirements, Start::Replay)
    }

    fn of(requirements: &DataRequirements, start: Start) -> Self {
        Self {
            start,
            length: requirements.warmup,
            bar_types: requirements.bar_types.clone(),
            harness: Vec::new(),
            ticks: requirements.ticks.clone(),
            custom: requirements.custom.clone(),
            indicators: Vec::new(),
            merge: Merge::new(merge::HELD),
            hold: Hold::default(),
            group: Cell::new(UUID4::new()),
            warming: Cell::new(false),
            building: RefCell::default(),
        }
    }

    /// The warmup handing its bars to `indicator` too, which reads the
    /// bars of its own bar types alone.
    #[must_use]
    pub fn feeding(mut self, indicator: SharedActorIndicator) -> Self {
        self.indicators.push(indicator);
        self
    }

    /// The warmup building the bars of `bar_type` too, if the strategy
    /// does not read it, for `indicator` of the harness, which it feeds:
    /// live, the data engine builds them from the warmup's history and
    /// goes on with them once the strategy subscribes to them
    /// (`strategy::subscriptions`), the bar spanning the warmup's end once.
    #[must_use]
    pub fn building(mut self, bar_type: BarType, indicator: SharedActorIndicator) -> Self {
        if !self.bar_types.contains(&bar_type) && !self.harness.contains(&bar_type) {
            self.harness.push(bar_type);
        }
        self.feeding(indicator)
    }

    /// The warmup handing the book depth and deltas of its responses to
    /// `books` too, and the live ones the strategy hands it
    /// (`on_book_depth`, `on_book_deltas`): live, those held until its group
    /// ends, and those the history holds dropped (see `hold`).
    #[must_use]
    pub fn feeding_books(mut self, books: Rc<dyn BookIndicator>) -> Self {
        self.hold.feeding_books(books);
        self
    }

    /// The warmup handing the custom data of its responses to `custom` too,
    /// and the live and replayed points the strategy hands it (`on_data`):
    /// live, those held until its group ends, and those the history holds
    /// dropped (see `hold`).
    #[must_use]
    pub fn feeding_custom(mut self, custom: Rc<dyn CustomIndicator>) -> Self {
        self.hold.feeding_custom(custom);
        self
    }

    /// The warmup handing the quotes and trades of its responses to
    /// `indicator` too, and the live ones the strategy hands it (`on_quote`,
    /// `on_trade`): live, those held until its group ends, and those the
    /// history holds dropped (see `hold`).
    #[must_use]
    pub fn feeding_ticks(mut self, indicator: SharedActorIndicator) -> Self {
        self.hold.feeding(indicator);
        self
    }

    /// Starts the warmup of `actor`, from its `on_start`: live, requests it
    /// up to now as a new request group (see `start`); on a replay,
    /// requests nothing. Returns that `now`, the clock's time, where the
    /// warmup ends: the strategy's subscriptions start there
    /// (`Subscriptions::start`), so the live stream goes on from the
    /// warmup.
    ///
    /// # Errors
    ///
    /// Returns an error if a live request fails (see `start`).
    pub fn start<A>(&self, actor: &mut A) -> anyhow::Result<Timestamp>
    where
        A: DataActor + DataActorNative + Debug + 'static,
    {
        let now = actor.clock().utc_now();
        if self.start == Start::Request {
            self.group.set(UUID4::new());
            self.warming.set(true);
            self.hold.hold();
            // The strategy's bar types and the harness's.
            let requirements = DataRequirements {
                bar_types: self.built(),
                ticks: self.ticks.clone(),
                custom: self.custom.clone(),
                warmup: self.length,
            };
            let requested = start(
                actor,
                now,
                &requirements,
                &ContinuousBars::default(),
                self.group.get(),
            )?;
            self.merge.start(
                requested
                    .iter()
                    .filter_map(|request| Some((request.id, request.series.clone()?))),
            );

            if requested.is_empty() {
                self.warming.set(false);
                self.end()?;
            }
            *self.building.borrow_mut() = requested
                .into_iter()
                .map(|request| (request.id, request.built))
                .collect();
        }
        Ok(now)
    }

    /// Reads `response`, from the actor's `on_data_response`: hands the bars
    /// a request built to the indicators the warmup feeds, oldest first, once
    /// the request completes, failed or not, and logs the end of the warmup,
    /// if `response` ends it.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator fails to read a bar, or naming the
    /// reason if the warmup ended incomplete (see `finished`), which Nautilus
    /// logs.
    pub fn on_data_response<A>(
        &self,
        actor: &A,
        response: &DataResponseEnvelope,
    ) -> anyhow::Result<()>
    where
        A: DataActor + DataActorNative,
    {
        let ends = *response.correlation_id() == self.group.get();
        let cut = ends && self.merge.is_cut();
        if ends {
            self.warming.set(false);
        }

        if response.completed() {
            self.merge
                .complete(response.correlation_id(), |tick| self.hold.history(tick))?;
            let built = self.building.borrow_mut().remove(response.correlation_id());
            for bar in built_bars(actor, &built.unwrap_or_default()) {
                for indicator in &self.indicators {
                    indicator.handle_bar(&bar)?;
                }
            }
        }

        if ends {
            self.end()?;
        }

        if finished(self.group.get(), response)? && !cut {
            log::info!("Warmup complete");
        }
        Ok(())
    }

    /// Ends the merge and the hold of a live start: hands on all the
    /// history held, in time order, then the live ticks held.
    fn end(&self) -> anyhow::Result<()> {
        self.merge.end(|tick| self.hold.history(tick))?;
        self.hold.release()
    }

    /// Hands `ticks`, a response's, to the merge, which hands them on in
    /// time order across the series (see `merge`); past its bound, ends
    /// the warmup incomplete.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator reading a tick fails, or naming
    /// the series held back if the merge goes past its bound.
    fn history(&self, ticks: impl IntoIterator<Item = Tick>) -> anyhow::Result<()> {
        let Some(overflow) = self.merge.history(ticks, |tick| self.hold.history(tick))? else {
            return Ok(());
        };
        self.warming.set(false);
        self.hold.release()?;
        anyhow::bail!("the warmup is incomplete: {overflow}")
    }

    /// Reads the completion of every request of the last start, then of its
    /// group, as a node answering them all brings them.
    #[cfg(test)]
    pub(crate) fn complete<A>(&self, actor: &A) -> anyhow::Result<()>
    where
        A: DataActor + DataActorNative,
    {
        let requests: Vec<UUID4> = self.building.borrow().keys().copied().collect();
        for id in requests.into_iter().chain([self.group.get()]) {
            self.on_data_response(actor, &DataResponseEnvelope::terminal(id, None))?;
        }
        Ok(())
    }

    /// Reads the end of the last start's group, failed with `reason`, as a
    /// node whose request failed brings it: the warmup's error naming it.
    #[cfg(test)]
    pub(crate) fn fail<A>(&self, actor: &A, reason: &str) -> anyhow::Error
    where
        A: DataActor + DataActorNative,
    {
        let failed = DataResponseEnvelope::terminal(self.group.get(), Some(reason.to_string()));
        self.on_data_response(actor, &failed)
            .expect_err("a failed group is an error")
    }

    /// Whether a live start's requests are not all answered yet: its windows
    /// reach the indicators one at a time, with the node running between
    /// them, so what they hold until then is no state to decide on.
    #[must_use]
    pub fn is_warming(&self) -> bool {
        self.warming.get()
    }

    /// Hands the book depth of `response`, a response to the warmup's
    /// request, to the book indicators it feeds, oldest first and, live, in
    /// time order across the series (see `merge`), from the actor's
    /// `on_historical_book_depth`.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator reading a tick the merge hands on
    /// fails, or naming the series held back if the merge goes past its
    /// bound, which ends the warmup incomplete.
    pub fn on_historical_book_depth(&self, response: &[OrderBookDepth]) -> anyhow::Result<()> {
        self.history(
            response
                .iter()
                .map(|depth| Tick::Depth(Box::new(depth.clone()))),
        )
    }

    /// Hands the book deltas of `response`, a response to the warmup's
    /// request, to the book indicators it feeds, in the batches the live
    /// stream carries (see `batches`), oldest first, from the actor's
    /// `on_historical_book_deltas`.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator reading a tick fails, or if the
    /// merge goes past its bound, as `on_historical_book_depth`.
    pub fn on_historical_book_deltas(&self, response: &[OrderBookDelta]) -> anyhow::Result<()> {
        self.history(batches(response).into_iter().map(Tick::Deltas))
    }

    /// Takes `depth`, a live snapshot, from the actor's `on_book_depth`: the
    /// book indicators read it once a live start's group has ended, held
    /// until then, and never if the history holds it (see `hold`). It
    /// returns no error: a book indicator reads depth without failing, and
    /// the result is returned as the tick hand-overs' are.
    pub fn on_book_depth(&self, depth: &OrderBookDepth) -> anyhow::Result<()> {
        self.hold.live(Tick::Depth(Box::new(depth.clone())))
    }

    /// Takes `deltas`, a live batch, from the actor's `on_book_deltas`, as
    /// `on_book_depth` does a snapshot. It returns no error, as
    /// `on_book_depth`.
    pub fn on_book_deltas(&self, deltas: &OrderBookDeltas) -> anyhow::Result<()> {
        self.hold.live(Tick::Deltas(deltas.clone()))
    }

    /// Adds the custom data of `response`, a response to the warmup's
    /// request of a custom series, to `actor`'s cache, oldest first, from
    /// the actor's
    /// `on_historical_data`: the data engine caches the custom data it
    /// processes, not a response (see the module). The cache keeps a data
    /// type's points in the order added, the latest added first, so a
    /// point no later (`ts_event`) than the latest the cache holds of its
    /// data type is left out: a live point the node took while the request
    /// was pending stays the latest, and a point the history and the live
    /// stream both carry is held once. Then hands each point to the custom
    /// indicators it feeds (`feeding_custom`), oldest first, as
    /// `on_historical_quotes` does quotes. Nautilus answers `request_data`
    /// with a batch (`Vec<CustomData>`); any other payload is no response
    /// of the warmup's.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator reading a tick fails, or if the
    /// merge goes past its bound, as `on_historical_book_depth`.
    pub fn on_historical_data<A: DataActorNative>(
        &self,
        actor: &A,
        response: &dyn Any,
    ) -> anyhow::Result<()> {
        let Some(data) = response.downcast_ref::<Vec<CustomData>>() else {
            return Ok(());
        };
        {
            let cache = actor.cache_rc();
            let mut cache = cache.borrow_mut();
            for point in data {
                let later = cache
                    .custom_data(&point.data_type)
                    .is_none_or(|latest| point.data.ts_event() > latest.data.ts_event());
                if later {
                    cache.add_custom_data(point.clone());
                }
            }
        }
        self.history(data.iter().cloned().map(Tick::Custom))
    }

    /// Takes `data`, a live or replayed point of custom data, from the
    /// actor's `on_data`, as `on_quote` does a quote: the custom
    /// indicators read it once a live start's group has ended, held until
    /// then, and never if the history holds it (see `hold`). It returns no
    /// error: a custom indicator reads custom data without failing.
    pub fn on_data(&self, data: &CustomData) -> anyhow::Result<()> {
        self.hold.live(Tick::Custom(data.clone()))
    }

    /// Hands the quotes of `response`, a response to the warmup's request,
    /// to the indicators it feeds ticks (`feeding_ticks`), oldest first
    /// and, live, in time order across the series (see `merge`), from the
    /// actor's `on_historical_quotes`.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator reading a tick the merge hands on
    /// fails, the first of which ends the hand-over, or naming the series
    /// held back if the merge goes past its bound, which ends the warmup
    /// incomplete.
    pub fn on_historical_quotes(&self, response: &[QuoteTick]) -> anyhow::Result<()> {
        self.history(response.iter().copied().map(Tick::Quote))
    }

    /// Hands the trades of `response` over as `on_historical_quotes` does
    /// quotes, from the actor's `on_historical_trades`.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator reading a tick fails, or if the
    /// merge goes past its bound, as `on_historical_quotes`.
    pub fn on_historical_trades(&self, response: &[TradeTick]) -> anyhow::Result<()> {
        self.history(response.iter().copied().map(Tick::Trade))
    }

    /// Takes `quote`, a live one, from the actor's `on_quote`: the
    /// indicators it feeds ticks read it once a live start's group has
    /// ended, held until then, and never if the history holds it (see
    /// `hold`).
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator reading it fails.
    pub fn on_quote(&self, quote: &QuoteTick) -> anyhow::Result<()> {
        self.hold.live(Tick::Quote(*quote))
    }

    /// Takes `trade`, a live one, from the actor's `on_trade`, as
    /// `on_quote` does a quote.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator reading it fails.
    pub fn on_trade(&self, trade: &TradeTick) -> anyhow::Result<()> {
        self.hold.live(Tick::Trade(*trade))
    }

    /// The warmup's merge holding at most `limit` ticks, not `merge::HELD`.
    #[cfg(test)]
    pub(crate) fn holding(mut self, limit: usize) -> Self {
        self.merge = Merge::new(limit);
        self
    }

    /// The cache config of a live node running the warmup: sized for the
    /// warmup of every bar type it builds (see `cache_config`).
    ///
    /// # Errors
    ///
    /// Returns an error if `cache_config` fails.
    pub fn cache_config(&self) -> anyhow::Result<CacheConfig> {
        cache_config(&self.built(), self.length)
    }

    /// The bar types a live warmup brings into the cache, built or, if
    /// external, requested: the strategy's, then the harness's.
    fn built(&self) -> Vec<BarType> {
        self.bar_types
            .iter()
            .chain(&self.harness)
            .copied()
            .collect()
    }
}

impl Debug for Warmup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(Warmup))
            .field("start", &self.start)
            .field("length", &self.length)
            .field("bar_types", &self.bar_types)
            .field("harness", &self.harness)
            .field("ticks", &self.ticks)
            .field("indicators", &self.indicators.len())
            .field("merge", &self.merge)
            .field("hold", &self.hold)
            .field("group", &self.group)
            .field("warming", &self.warming)
            .field("building", &self.building)
            .finish()
    }
}

/// Requests the warmup of `requirements`, its bar types, its ticks and its
/// custom series, over `[now - warmup, now]`, its warmup, as the request
/// group `group`; the strategy subscribes to them from `now`
/// (`strategy::subscriptions`). The bars on a
/// root with continuous bars in `continuous` are
/// its continuous future, requested per bar type with
/// their `continuous_future_*` params; the others are requested per series
/// they are built from (see `requests`): the source bars of a composite bar
/// type, the trades of the instrument of internal LAST bars, the quotes of
/// the instrument of internal BID, ASK and MID bars; an external bar type
/// is requested as it is, building nothing; each tick series is requested on
/// the contract standing for its instrument now, once with the bars built
/// from it if any; each of its custom series is requested by its data
/// type as read now (see `custom_request`), of its type's row's live
/// data client, catalog first and written back, if its type's history
/// serves it, of the catalog alone if it is the catalog's
/// (`venues::serves_custom`). A series its venue's history does not
/// serve (`venues::serves`) is requested no history, logged as a warning,
/// its instrument's definition asked all the same. The
/// warmup of each request arrives a window at a time in
/// `on_historical_bars`, `on_historical_trades`, `on_historical_quotes`,
/// `on_historical_book_depth` or `on_historical_book_deltas`, at once in
/// `on_historical_data`; the end of each request and of the group in
/// `on_data_response`, where `finished` tells the group's. `group` is the
/// caller's, stored before the call: a group whose requests are all answered
/// at once ends before this returns. No bar types, no ticks and no custom
/// data make no request and no group, which `DataActor::request_group`
/// refuses empty.
/// Returns each request: its id, the bar types it builds, none for a
/// continuous future's, read once the group ended (`continuous_bars`), and
/// the series of the ticks or custom data it answers, which a live
/// `Warmup` merges (see `merge`).
///
/// # Errors
///
/// Returns an error if a request fails, or if a custom series' type's
/// history source does not serve it, or its row names no live data source
/// to ask (see `custom_request`), which the load and the live start
/// refuse. A composite bar
/// type's source bars are external, as the config's load requires (see
/// `strategy::DataConfig`): they are requested, not built.
pub fn start<A>(
    actor: &mut A,
    now: Timestamp,
    requirements: &DataRequirements,
    continuous: &ContinuousBars,
    group: UUID4,
) -> anyhow::Result<Vec<Requested>>
where
    A: DataActor + DataActorNative + Debug + 'static,
{
    let start = now.checked_sub(requirements.warmup)?;
    let windows = time_range::params(start, now, WINDOW);
    let (continuous_bars, others): (Vec<BarType>, Vec<BarType>) = requirements
        .bar_types
        .iter()
        .partition(|bar_type| continuous.params(bar_type.instrument_id()).is_some());
    let sources = requests(&others, &requirements.ticks, now);
    let mut requested = Vec::new();
    // First, so that the data engine, which takes an actor's commands in
    // the order sent, caches each definition the catalog answers before it
    // handles any request or subscription of the instrument.
    for instrument_id in undefined(actor, &sources) {
        let client = venues::live_definitions_client(instrument_id.venue);
        let id = actor.request_instrument(instrument_id, None, None, Some(client), None)?;
        requested.push(Requested::of(id));
    }
    for bar_type in &continuous_bars {
        let params = continuous.params(bar_type.instrument_id());
        let id = actor.request_bars(
            *bar_type,
            Some(start),
            Some(now),
            None,
            None,
            Some(request_params(
                [bar_type].into_iter(),
                params.into_iter().flatten(),
            )),
        )?;
        // Read once the group ended (see `continuous_bars`).
        requested.push(Requested::of(id));
    }
    for (source, built) in sources {
        if let Err(e) = venues::serves(&source) {
            let bars: Vec<String> = built.iter().map(ToString::to_string).collect();
            let bars = if bars.is_empty() {
                String::new()
            } else {
                format!(", and so are the bars built from it, {}", bars.join(", "))
            };
            log::warn!("No warmup of {source}: {e:#}; it is subscribed from now{bars}");
            continue;
        }
        let params = Some(request_params(built.iter(), windows.clone()));
        let series = match &source {
            CatalogSeries::Bars(_) => None,
            CatalogSeries::Ticks(tick) => Some(Series::of(tick)),
        };
        let id = match source {
            CatalogSeries::Bars(source) => {
                actor.request_bars(source, Some(start), Some(now), None, None, params)?
            }
            CatalogSeries::Ticks(tick) => match tick.data {
                NautilusDataType::TradeTick => actor.request_trades(
                    tick.instrument_id,
                    Some(start),
                    Some(now),
                    None,
                    None,
                    params,
                )?,
                NautilusDataType::QuoteTick => actor.request_quotes(
                    tick.instrument_id,
                    Some(start),
                    Some(now),
                    None,
                    None,
                    params,
                )?,
                NautilusDataType::OrderBookDepth => actor.request_book_depth(
                    tick.instrument_id,
                    Some(start),
                    Some(now),
                    None,
                    None,
                    None,
                    params,
                )?,
                // The book type of its venue's book, which the strategy
                // subscribes to: Nautilus replays the deltas before the
                // start onto a book of it, into the snapshot the response
                // starts with.
                NautilusDataType::OrderBookDelta => {
                    let id = tick.instrument_id;
                    let book = venues::book_format(id.venue).with_context(|| {
                        format!(
                            "the live data of {} streams no book, so no deltas of {id} are \
                             requested",
                            id.venue
                        )
                    })?;
                    actor.request_book_deltas(
                        id,
                        Some(start),
                        Some(now),
                        None,
                        None,
                        Some(request_params(
                            built.iter(),
                            windows
                                .iter()
                                .cloned()
                                .chain([("book_type".to_string(), json!(book.deltas.to_string()))]),
                        )),
                    )?
                }
                data => unreachable!("{data} is not a tick series (see `TickSeries`)"),
            },
        };
        requested.push(Requested { id, built, series });
    }
    // As read now, a root's on its front contract, each once; of its type's
    // row's live data client if its history serves it, of the catalog alone
    // if not, building nothing: Nautilus takes no windows on custom data.
    let mut warmed: Vec<CustomSeries> = Vec::new();
    for series in &requirements.custom {
        let read = series.at(now);
        if warmed.contains(&read) {
            continue;
        }
        let (client, params) = custom_request(series)?;
        let id = actor.request_data(
            read.data_type().clone(),
            client,
            Some(start),
            Some(now),
            None,
            params,
        )?;
        requested.push(Requested {
            series: Some(Series::Custom(read.data_type().clone())),
            ..Requested::of(id)
        });
        warmed.push(read);
    }

    if !requested.is_empty() {
        let ids = requested.iter().map(|request| request.id).collect();
        actor.request_group(ids, Some(group))?;
    }
    Ok(requested)
}

/// A request of a warmup's start: its id, the bar types it builds, and the
/// series of the history it hands on in time order across series (see
/// `merge`), none for bars or a definition.
#[derive(Debug)]
pub struct Requested {
    id: UUID4,
    built: Vec<BarType>,
    series: Option<Series>,
}

impl Requested {
    /// The request `id`, building no bars and merging no history.
    const fn of(id: UUID4) -> Self {
        Self {
            id,
            built: Vec::new(),
            series: None,
        }
    }
}

/// The client a warmup asks `series`, a custom series, of, and the
/// request's params: if its type's history serves it
/// (`venues::serves_custom`), its data client on the node
/// (`venues::custom_data_client`) with `update_catalog`, the catalog
/// answering first, the client the rest, its answers written back, as with
/// every series served; if not, `CATALOG` with no params, the catalog alone
/// answering: data written into it from outside helix, which no client
/// answers anything of to write back.
///
/// # Errors
///
/// Returns an error if `venues::serves_custom` fails, or if its type's
/// history serves it and its row names no live data source, whose client
/// the node would ask, naming its type.
fn custom_request(series: &CustomSeries) -> anyhow::Result<(ClientId, Option<Params>)> {
    if venues::serves_custom(series)?.is_none() {
        return Ok((ClientId::new(CATALOG), None));
    }
    let client = venues::custom_data_client(series).with_context(|| {
        format!(
            "no live data source asks the history of custom data of the type {}: its row \
             names none",
            series.type_name()
        )
    })?;
    let params = Params::new([("update_catalog".to_string(), json!(true))].into());
    Ok((client, Some(params)))
}

/// The instruments of `sources` whose definitions the cache of `actor`
/// lacks, each once, in order: those the series of a data client are of.
fn undefined<A>(actor: &A, sources: &[(CatalogSeries, Vec<BarType>)]) -> Vec<InstrumentId>
where
    A: DataActor + DataActorNative,
{
    let cache = actor.cache();
    let mut undefined: Vec<InstrumentId> = Vec::new();
    for (source, _) in sources {
        let instrument_id = source.instrument_id();
        if !undefined.contains(&instrument_id) && cache.instrument(&instrument_id).is_none() {
            undefined.push(instrument_id);
        }
    }
    undefined
}

/// The warmup requests of `bar_types`, bar types on no root, and of
/// `ticks` at `now`: one per series the bars are built from, as Nautilus'
/// data engine builds them (see `strategy::requirements::catalog_keys`), with the bar
/// types built from it, in the order the series first come, an external
/// bar type being its own series, which builds none of it; then
/// one per tick series on the contract standing for its instrument at
/// `now` (`rolls::front`) that no bar is built from, building none.
fn requests(
    bar_types: &[BarType],
    ticks: &[TickSeries],
    now: Timestamp,
) -> Vec<(CatalogSeries, Vec<BarType>)> {
    let mut requests: Vec<(CatalogSeries, Vec<BarType>)> = Vec::new();
    for (source, bar_type) in bar_sources(bar_types).zip(bar_types) {
        // An external bar type is its own source: requested as it is, it
        // builds nothing, Nautilus aggregating into internal bar types alone.
        let built = (source != CatalogSeries::Bars(*bar_type)).then_some(*bar_type);
        match requests.iter_mut().find(|(of, _)| *of == source) {
            Some((_, of_source)) => of_source.extend(built),
            None => requests.push((source, built.into_iter().collect())),
        }
    }
    for tick in ticks {
        let source = CatalogSeries::Ticks(TickSeries {
            data: tick.data.clone(),
            instrument_id: front(tick.instrument_id, now),
        });
        if !requests.iter().any(|(of, _)| *of == source) {
            requests.push((source, vec![]));
        }
    }
    requests
}

/// `deltas`, a response's, in the batches the live stream carries: each
/// closed by its `F_LAST` delta, the trailing deltas without one a batch
/// of their own, as Nautilus' data engine publishes a response's deltas.
fn batches(deltas: &[OrderBookDelta]) -> Vec<OrderBookDeltas> {
    let mut batches = Vec::new();
    let mut batch: Vec<OrderBookDelta> = Vec::new();
    for delta in deltas {
        batch.push(*delta);
        if RecordFlag::F_LAST.matches(delta.flags) {
            batches.push(OrderBookDeltas::new(
                delta.instrument_id,
                std::mem::take(&mut batch),
            ));
        }
    }
    if let Some(last) = batch.last() {
        batches.push(OrderBookDeltas::new(last.instrument_id, batch));
    }
    batches
}

/// The params of a warmup request building `bar_types`, with `others`:
/// the `continuous` params of their root, if on one, or a book type.
fn request_params<'a>(
    bar_types: impl Iterator<Item = &'a BarType>,
    others: impl IntoIterator<Item = (String, Value)>,
) -> Params {
    let bar_types: Vec<String> = bar_types.map(ToString::to_string).collect();
    Params::new(
        [
            ("bar_types".to_string(), json!(bar_types)),
            ("update_subscriptions".to_string(), json!(true)),
            ("skip_first_non_full_bar".to_string(), json!(true)),
            ("update_catalog".to_string(), json!(true)),
        ]
        .into_iter()
        .chain(others)
        .collect(),
    )
}

/// Whether the warmup `group` ended with `response`, a response to the
/// actor's requests: `Ok(true)` if it ended complete, `Ok(false)` if
/// `response` ends something else.
///
/// # Errors
///
/// Returns an error if `response` ends the group and a request of it
/// failed, naming the reason: the warmup holds what the requests got
/// before.
pub fn finished(group: UUID4, response: &DataResponseEnvelope) -> anyhow::Result<bool> {
    if *response.correlation_id() != group {
        return Ok(false);
    }

    match response.failure_reason() {
        Some(reason) => anyhow::bail!("the warmup is incomplete: {reason}"),
        None => Ok(true),
    }
}

/// The bars of `bar_types` in `actor`'s cache, oldest first. A node caches a
/// bar type's bars under its standard type, a composite's without its source
/// (`BarType::standard`, `TimeBarAggregator::new`).
fn built_bars<A>(actor: &A, bar_types: &[BarType]) -> Vec<Bar>
where
    A: DataActor + DataActorNative,
{
    let cache = actor.cache();
    let mut bars: Vec<Bar> = bar_types
        .iter()
        .flat_map(|bar_type| cache.bars(&bar_type.standard()).unwrap_or_default())
        .collect();
    // The cache holds the newest first.
    bars.sort_by_key(|bar| bar.ts_event);
    bars
}

/// The warmup bars of those of `bar_types` on a root with continuous bars
/// in `continuous`, oldest first. Nautilus builds a continuous future's
/// warmup into the cache only and answers its request with no bars: a
/// strategy reads them here once `finished` tells the group ended. A backtest's group ends in `start`, before any
/// bar of the run reaches the cache.
pub fn continuous_bars<A>(actor: &A, bar_types: &[BarType], continuous: &ContinuousBars) -> Vec<Bar>
where
    A: DataActor + DataActorNative,
{
    let cache = actor.cache();
    let mut bars: Vec<Bar> = bar_types
        .iter()
        .filter(|bar_type| continuous.params(bar_type.instrument_id()).is_some())
        .flat_map(|bar_type| cache.bars(bar_type).unwrap_or_default())
        .collect();
    // The cache holds the newest first.
    bars.sort_by_key(|bar| bar.ts_event);
    bars
}

/// The cache config of a live node whose strategy warms up on `bar_types`
/// over `warmup`: Nautilus' default, its bar capacity raised by the most
/// bars the warmup builds of one bar type. The default capacity stays on
/// top as headroom for the bars built while the warmup is pending.
///
/// # Errors
///
/// Returns an error if the warmup is negative, if a bar type is not
/// aggregated on time, or if the capacity a warmup needs is beyond what the
/// cache allows.
pub fn cache_config(bar_types: &[BarType], warmup: SignedDuration) -> anyhow::Result<CacheConfig> {
    let headroom = CacheConfig::default().bar_capacity;
    let mut longest: Option<(BarType, usize)> = None;
    for bar_type in bar_types {
        let needed = warmup_bars(bar_type, warmup)?;
        if longest.is_none_or(|(_, most)| needed > most) {
            longest = Some((*bar_type, needed));
        }
    }
    let Some((bar_type, needed)) = longest else {
        return Ok(CacheConfig::default());
    };
    let capacity = headroom.saturating_add(needed);
    CacheConfig::builder()
        .bar_capacity(capacity)
        .build()
        .with_context(|| {
            format!(
                "the warmup of {warmup:#} needs up to {needed} bars of {bar_type}: \
                 the cache cannot hold {capacity} bars ({headroom} of headroom included)"
            )
        })
}

/// Whether the data engine builds a bar for a period without any quote.
const BUILD_BARS_WITHOUT_QUOTES: bool = false;

/// The data engine config of a backtest or a dataset run of a strategy
/// on `bar_types`: Nautilus' default, without bars for periods without
/// quotes, and building time bars `bar_build_delay` after their close
/// (`time_bars_build_delay`), as a live node does.
#[must_use]
pub fn data_engine_config(bar_types: &[BarType]) -> DataEngineConfig {
    DataEngineConfig {
        time_bars_build_with_no_updates: BUILD_BARS_WITHOUT_QUOTES,
        time_bars_build_delay: build_delay_micros(bar_types),
        ..DataEngineConfig::default()
    }
}

/// How long, in microseconds, a node waits after a bar's close before
/// building a bar of source bars: a venue sends a source bar once it has
/// closed, so its last source bar arrives after the bar's close (OKX's
/// confirmed 1-minute candles: 0.3 to 1.5 s, measured 2026-10-22; IB's
/// keepUpToDate 1-minute bars: 0.4 to 1.3 s, measured 2026-10-26).
const SOURCE_BAR_DELAY_MICROS: i64 = 5_000_000;

/// The data engine config of a live node running a strategy on
/// `bar_types`: Nautilus' default, without bars for periods without
/// quotes, and building time bars `bar_build_delay` after their close
/// (`time_bars_build_delay`).
#[must_use]
pub fn live_data_engine_config(bar_types: &[BarType]) -> LiveDataEngineConfig {
    LiveDataEngineConfig {
        time_bars_build_with_no_updates: BUILD_BARS_WITHOUT_QUOTES,
        time_bars_build_delay: build_delay_micros(bar_types),
        ..LiveDataEngineConfig::default()
    }
}

/// How long after a bar's close a node running a strategy on `bar_types`
/// builds its time bars, a backtest and a dataset run as a live node:
/// `SOURCE_BAR_DELAY_MICROS` if any bar type is composite, so that the
/// last source bar is in, else none. A backtest's source bars are in the
/// catalog by the close already; it builds its bars as late all the same,
/// so that its bars, stamped when built, and its decisions fall where a
/// live node's do.
#[must_use]
pub fn bar_build_delay(bar_types: &[BarType]) -> SignedDuration {
    if bar_types.iter().any(BarType::is_composite) {
        SignedDuration::from_micros(SOURCE_BAR_DELAY_MICROS)
    } else {
        SignedDuration::ZERO
    }
}

/// `bar_build_delay` of `bar_types` in microseconds, as Nautilus' data
/// engine configs take it (`time_bars_build_delay`).
fn build_delay_micros(bar_types: &[BarType]) -> u64 {
    let delay = bar_build_delay(bar_types).unsigned_abs().as_micros();
    u64::try_from(delay).expect("a delay of seconds")
}

/// The most bars a warmup of `warmup` builds of `bar_type`: one per bar
/// length, and one for the bar spanning the warmup start.
fn warmup_bars(bar_type: &BarType, warmup: SignedDuration) -> anyhow::Result<usize> {
    let spec = bar_type.spec();
    anyhow::ensure!(
        spec.is_time_aggregated(),
        "bar type {bar_type} is not aggregated on time: no bar count for its warmup"
    );
    let warmup_ns =
        u128::try_from(warmup.as_nanos()).with_context(|| format!("negative warmup {warmup:#}"))?;
    let length_ns = spec.timedelta().as_nanos().unsigned_abs();
    let bars = warmup_ns.div_ceil(length_ns) + 1;
    usize::try_from(bars).with_context(|| format!("no bar count for a warmup of {warmup:#}"))
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc, sync::Arc};

    use nautilus_common::{
        actor::indicators::ActorIndicator,
        cache::Cache,
        clock::VirtualClock,
        messages::data::{DataCommand, DataResponse, RequestCommand, RequestCustomData},
        msgbus::{
            self, MessageBus, MessagingSwitchboard, TypedHandler,
            stubs::{TypedIntoMessageSavingHandler, get_typed_into_message_saving_handler},
        },
    };
    use nautilus_core::UnixNanos;
    use nautilus_data::engine::DataEngine;
    use nautilus_model::{
        data::{BookOrder, DataType, QuoteTick, TradeTick, stubs::stub_depth10},
        enums::{AggressorSide, BookAction, OrderSide, RecordFlag},
        identifiers::{InstrumentId, StrategyId, TradeId, TraderId},
        instruments::{InstrumentAny, stubs::futures_contract_es},
        types::{Price, Quantity},
    };
    use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
    use nautilus_portfolio::portfolio::Portfolio;
    use nautilus_trading::{
        nautilus_strategy,
        strategy::{StrategyConfig, StrategyCore},
    };
    use rstest::rstest;

    use super::*;
    use crate::strategy::requirements::TickSeries;

    const DAY: SignedDuration = SignedDuration::from_hours(24);

    #[derive(Debug)]
    struct Probe {
        core: StrategyCore,
    }

    nautilus_strategy!(Probe);

    impl DataActor for Probe {}

    /// A strategy registered without a node at 2026-08-10T14:30:00Z, and
    /// what it sends the data engine from then on.
    fn registered() -> (Probe, TypedIntoMessageSavingHandler<DataCommand>) {
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
        let now: Timestamp = "2026-08-10T14:30:00Z".parse().unwrap();
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        clock.borrow_mut().advance_time(now.into(), true);
        let cache = Rc::new(RefCell::new(Cache::default()));
        let portfolio = Rc::new(RefCell::new(Portfolio::new(
            clock.clone(),
            cache.clone(),
            None,
        )));
        probe
            .core
            .register(TraderId::from("HELIX-001"), clock, cache, portfolio)
            .unwrap();
        (probe, sent)
    }

    /// The data requests of `commands`, in order, the definitions asked
    /// aside (see `definitions_asked`): each its series, its id and its
    /// params.
    fn sent_requests(
        commands: &TypedIntoMessageSavingHandler<DataCommand>,
    ) -> Vec<(CatalogSeries, UUID4, Option<Params>)> {
        let ticks = |data, instrument_ids: &[InstrumentId]| {
            let [instrument_id] = instrument_ids else {
                panic!("one instrument")
            };
            CatalogSeries::Ticks(TickSeries {
                data,
                instrument_id: *instrument_id,
            })
        };
        commands
            .get_messages()
            .into_iter()
            .filter_map(|command| match command {
                DataCommand::Request(RequestCommand::Instrument(_)) => None,
                DataCommand::Request(request) => Some(request),
                _ => None,
            })
            .map(|request| {
                let id = *request.request_id();
                let (source, params) = match request {
                    RequestCommand::Quotes(request) => (
                        ticks(NautilusDataType::QuoteTick, &request.instrument_ids),
                        request.params,
                    ),
                    RequestCommand::Trades(request) => (
                        ticks(NautilusDataType::TradeTick, &request.instrument_ids),
                        request.params,
                    ),
                    RequestCommand::BookDepth(request) => (
                        ticks(NautilusDataType::OrderBookDepth, &request.instrument_ids),
                        request.params,
                    ),
                    RequestCommand::BookDeltas(request) => (
                        ticks(NautilusDataType::OrderBookDelta, &request.instrument_ids),
                        request.params,
                    ),
                    RequestCommand::Bars(request) => {
                        let [bar_type] = request.bar_types.as_slice() else {
                            panic!("one bar type")
                        };
                        (CatalogSeries::Bars(*bar_type), request.params)
                    }
                    other => panic!("unexpected {other:?}"),
                };
                (source, id, params)
            })
            .collect()
    }

    /// The definitions `commands` ask, each its instrument and the client
    /// it names, and how many data requests come before it.
    fn definitions_asked(
        commands: &TypedIntoMessageSavingHandler<DataCommand>,
    ) -> Vec<(InstrumentId, Option<ClientId>, usize)> {
        let mut data_requests = 0;
        let mut asked = Vec::new();
        for command in commands.get_messages() {
            match command {
                DataCommand::Request(RequestCommand::Instrument(request)) => {
                    let [instrument_id] = request.instrument_ids.as_slice() else {
                        panic!("one instrument")
                    };
                    asked.push((*instrument_id, request.client_id, data_requests));
                }
                DataCommand::Request(_) => data_requests += 1,
                _ => {}
            }
        }
        asked
    }

    /// The requests of `commands`, in order: each its series and the bar
    /// types it builds (its `bar_types` param).
    fn requested(
        commands: &TypedIntoMessageSavingHandler<DataCommand>,
    ) -> Vec<(CatalogSeries, Vec<BarType>)> {
        sent_requests(commands)
            .into_iter()
            .map(|(source, _, params)| {
                let built = params
                    .as_ref()
                    .and_then(|params| params.get("bar_types"))
                    .and_then(Value::as_array)
                    .expect("a bar_types param")
                    .iter()
                    .map(|bar_type| BarType::from(bar_type.as_str().unwrap()))
                    .collect();
                (source, built)
            })
            .collect()
    }

    /// An indicator reading nothing, for a warmup to feed.
    struct Inert;

    impl ActorIndicator for Inert {
        fn key(&self) -> usize {
            std::ptr::from_ref(self).addr()
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn initialized(&self) -> anyhow::Result<bool> {
            Ok(true)
        }

        fn handle_quote(&self, _quote: &QuoteTick) -> anyhow::Result<()> {
            Ok(())
        }

        fn handle_trade(&self, _trade: &TradeTick) -> anyhow::Result<()> {
            Ok(())
        }

        fn handle_bar(&self, _bar: &Bar) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// An indicator recording the bar type of each bar it reads.
    #[derive(Default)]
    struct Recorder {
        bar_types: RefCell<Vec<BarType>>,
    }

    impl ActorIndicator for Recorder {
        fn key(&self) -> usize {
            std::ptr::from_ref(self).addr()
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn initialized(&self) -> anyhow::Result<bool> {
            Ok(true)
        }

        fn handle_quote(&self, _quote: &QuoteTick) -> anyhow::Result<()> {
            Ok(())
        }

        fn handle_trade(&self, _trade: &TradeTick) -> anyhow::Result<()> {
            Ok(())
        }

        fn handle_bar(&self, bar: &Bar) -> anyhow::Result<()> {
            self.bar_types.borrow_mut().push(bar.bar_type);
            Ok(())
        }
    }

    /// The requirements of a strategy reading `bar_types` over a day.
    fn reading(bar_types: &[&str]) -> DataRequirements {
        DataRequirements {
            bar_types: bar_types.iter().map(|&b| BarType::from(b)).collect(),
            ticks: vec![],
            custom: vec![],
            warmup: DAY,
        }
    }

    /// The harness's bars are built after the strategy's, once, and not
    /// again if the strategy reads them: both are MID bars of SXR8, built
    /// by the one request of its quotes.
    #[rstest]
    fn a_warmup_builds_the_harness_bars_once_after_the_strategys() {
        let minutes = BarType::from("SXR8.IBIS-1-MINUTE-MID-INTERNAL");
        let hours = BarType::from("SXR8.IBIS-1-HOUR-MID-INTERNAL");
        let inert = || Rc::new(Inert) as SharedActorIndicator;
        let warmup = Warmup::live(&reading(&["SXR8.IBIS-1-HOUR-MID-INTERNAL"]))
            .building(minutes, inert())
            .building(minutes, inert())
            .building(hours, inert());
        let (mut probe, commands) = registered();

        warmup.start(&mut probe).unwrap();

        assert_eq!(
            requested(&commands),
            [(
                CatalogSeries::Ticks(tick(NautilusDataType::QuoteTick, "SXR8.IBIS")),
                vec![hours, minutes],
            )]
        );
    }

    fn tick(data: NautilusDataType, instrument_id: &str) -> TickSeries {
        TickSeries {
            data,
            instrument_id: InstrumentId::from(instrument_id),
        }
    }

    /// The requirements of a strategy reading `bar_types` and `ticks` over a
    /// day.
    fn reading_ticks(bar_types: &[&str], ticks: Vec<TickSeries>) -> DataRequirements {
        DataRequirements {
            ticks,
            ..reading(bar_types)
        }
    }

    /// A contract's deltas are requested at the book type of its venue's
    /// book, onto which Nautilus replays them into the snapshot a window
    /// opens with: a GLBX contract's, LSE's orders, as L3_MBO, as the
    /// strategy subscribes to them (`venues::book_format`,
    /// `strategy::subscriptions`).
    #[rstest]
    fn a_live_warmup_requests_deltas_at_the_book_type_of_their_venue() {
        let deltas = tick(NautilusDataType::OrderBookDelta, "GCZ26.GLBX");
        let warmup = Warmup::live(&reading_ticks(&[], vec![deltas.clone()]));
        let (mut probe, commands) = registered();

        warmup.start(&mut probe).unwrap();

        let book_types: Vec<(CatalogSeries, Option<String>)> = sent_requests(&commands)
            .into_iter()
            .map(|(source, _, params)| {
                let book_type =
                    params.and_then(|params| params.get_str("book_type").map(str::to_string));
                (source, book_type)
            })
            .collect();
        assert_eq!(
            book_types,
            [(CatalogSeries::Ticks(deltas), Some("L3_MBO".to_string()))]
        );
    }

    /// A live start requests the history of the ticks of the requirements
    /// too, a root's on the contract standing for it then: GC rolls into
    /// GCZ26 on 2026-07-29 and out of it on 2026-11-26 (`rolls/README.md`),
    /// so on 2026-08-10 the root's trades and depth are GCZ26's, requested
    /// once beside GCZ26's own trades. IC's gold quotes, which build the
    /// strategy's bars, are requested once, building them; the ticks no
    /// bar is built from build none.
    #[rstest]
    fn a_live_warmup_requests_a_roots_ticks_on_its_front_contract() {
        let bars = BarType::from("XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL");
        let warmup = Warmup::live(&reading_ticks(
            &["XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL"],
            vec![
                tick(NautilusDataType::TradeTick, "GC.GLBX"),
                tick(NautilusDataType::OrderBookDepth, "GC.GLBX"),
                tick(NautilusDataType::QuoteTick, "XAUUSD.ICMARKETS"),
                tick(NautilusDataType::TradeTick, "GCZ26.GLBX"),
                tick(NautilusDataType::OrderBookDelta, "GC.GLBX"),
            ],
        ));
        let (mut probe, commands) = registered();

        warmup.start(&mut probe).unwrap();

        assert_eq!(
            requested(&commands),
            [
                (
                    CatalogSeries::Ticks(tick(NautilusDataType::QuoteTick, "XAUUSD.ICMARKETS")),
                    vec![bars],
                ),
                (
                    CatalogSeries::Ticks(tick(NautilusDataType::TradeTick, "GCZ26.GLBX")),
                    vec![],
                ),
                (
                    CatalogSeries::Ticks(tick(NautilusDataType::OrderBookDepth, "GCZ26.GLBX")),
                    vec![],
                ),
                (
                    CatalogSeries::Ticks(tick(NautilusDataType::OrderBookDelta, "GCZ26.GLBX")),
                    vec![],
                ),
            ]
        );
    }

    /// A series its venue's history does not serve is requested no history
    /// (`venues::serves`): OKX's client requests no quotes, so neither the
    /// quotes OKX's MID bars are built from nor OKX's quotes as ticks are
    /// asked, nor SXR8's book depth, which IB serves none of; OKX's trades
    /// are. The definitions of their instruments are asked all the same,
    /// before any series, each of the client its venue's row names, so that
    /// the clients subscribing to the series find them.
    #[rstest]
    fn a_live_warmup_requests_no_series_its_venue_does_not_serve_but_asks_its_definition() {
        let swap = "BTC-USDT-SWAP.OKX";
        let warmup = Warmup::live(&reading_ticks(
            &["BTC-USDT-SWAP.OKX-1-HOUR-MID-INTERNAL"],
            vec![
                tick(NautilusDataType::QuoteTick, swap),
                tick(NautilusDataType::TradeTick, swap),
                tick(NautilusDataType::OrderBookDepth, "SXR8.IBIS"),
            ],
        ));
        let (mut probe, commands) = registered();

        warmup.start(&mut probe).unwrap();

        assert_eq!(
            requested(&commands),
            [(
                CatalogSeries::Ticks(tick(NautilusDataType::TradeTick, swap)),
                vec![]
            )]
        );
        assert_eq!(
            definitions_asked(&commands),
            [
                (InstrumentId::from(swap), Some(ClientId::from("OKX")), 0),
                (
                    InstrumentId::from("SXR8.IBIS"),
                    Some(ClientId::from("IB")),
                    0
                ),
            ]
        );
    }

    /// A definition of `id`, as a broker's client puts one in the cache.
    fn defined(id: &str) -> InstrumentAny {
        let mut contract = futures_contract_es(None, None);
        contract.id = InstrumentId::from(id);
        InstrumentAny::FuturesContract(contract)
    }

    /// LSE's gold futures quotes, SXR8's trades and book depth and IC's
    /// gold quotes, which build the strategy's bars; IC's client put its
    /// gold in the cache. Each instrument the cache lacks is asked once, of
    /// the client its venue's row names (`venues::live_definitions_client`),
    /// before any of its series is: LSE's for LSEDGE, which has no broker,
    /// IB's for IBIS. Three series are requested, IB serving no book depth.
    #[rstest]
    fn a_live_warmup_asks_the_definitions_the_cache_lacks_before_any_series() {
        let warmup = Warmup::live(&reading_ticks(
            &["XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL"],
            vec![
                tick(NautilusDataType::QuoteTick, "GOLD-FUTURES.LSEDGE"),
                tick(NautilusDataType::TradeTick, "SXR8.IBIS"),
                tick(NautilusDataType::OrderBookDepth, "SXR8.IBIS"),
            ],
        ));
        let (mut probe, commands) = registered();
        probe
            .cache_rc()
            .borrow_mut()
            .add_instrument(defined("XAUUSD.ICMARKETS"))
            .unwrap();

        warmup.start(&mut probe).unwrap();

        assert_eq!(
            definitions_asked(&commands),
            [
                (
                    InstrumentId::from("GOLD-FUTURES.LSEDGE"),
                    Some(ClientId::from("LSE")),
                    0
                ),
                (
                    InstrumentId::from("SXR8.IBIS"),
                    Some(ClientId::from("IB")),
                    0
                ),
            ]
        );
        assert_eq!(
            requested(&commands),
            [
                (
                    CatalogSeries::Ticks(tick(NautilusDataType::QuoteTick, "XAUUSD.ICMARKETS")),
                    vec![BarType::from("XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL")],
                ),
                (
                    CatalogSeries::Ticks(tick(NautilusDataType::QuoteTick, "GOLD-FUTURES.LSEDGE")),
                    vec![],
                ),
                (
                    CatalogSeries::Ticks(tick(NautilusDataType::TradeTick, "SXR8.IBIS")),
                    vec![],
                ),
            ]
        );
    }

    /// The imbalances under `identifier`, a custom series of a type no row
    /// routes, the catalog's.
    fn imbalances(identifier: &str) -> CustomSeries {
        CustomSeries::new("Imbalance", identifier.to_string(), None).unwrap()
    }

    /// A custom series brings no instrument, under a plain instrument's id
    /// or a root's alike: its warmup needs no definition, and asks none,
    /// not even of the contract standing for the root.
    #[rstest]
    fn a_live_warmup_asks_no_definition_for_a_custom_series_under_an_instruments_id() {
        let warmup = Warmup::live(&DataRequirements {
            custom: vec![imbalances("NVDA.XNAS"), imbalances("GC.GLBX")],
            ..reading(&[])
        });
        let (mut probe, commands) = registered();

        warmup.start(&mut probe).unwrap();

        assert_eq!(definitions_asked(&commands), []);
        assert_eq!(custom_requests(&commands).len(), 2);
    }

    /// Live on 2026-08-10 at 14:30, the imbalances under GC's id, a root
    /// GC rolls into GCZ26 on 2026-07-29 (`rolls/README.md`), the
    /// catalog's, no row routing their type, are requested as GCZ26's,
    /// the contract standing for GC, over the day of warmup ending now,
    /// naming `CATALOG`, which the catalog alone answers, with no params,
    /// no client answering anything to write back.
    #[rstest]
    fn a_live_warmup_requests_a_roots_custom_series_of_the_catalog_on_its_front_contract() {
        let warmup = Warmup::live(&DataRequirements {
            custom: vec![imbalances("GC.GLBX")],
            ..reading(&[])
        });
        let (mut probe, commands) = registered();

        warmup.start(&mut probe).unwrap();

        let requests = custom_requests(&commands);
        let [request] = requests.as_slice() else {
            panic!("{} custom data requests", requests.len())
        };
        let at = |time: &str| -> Timestamp { time.parse().unwrap() };
        assert_eq!(
            (
                &request.data_type,
                request.client_id,
                request.start,
                request.end,
                request.params.is_none(),
            ),
            (
                &DataType::new("Imbalance", None, Some("GCZ26.GLBX".to_string())),
                ClientId::from("CATALOG"),
                Some(at("2026-08-09T14:30:00Z")),
                Some(at("2026-08-10T14:30:00Z")),
                true,
            )
        );
    }

    /// The metadata of LSE's daily 10-year US yields.
    fn us10y_metadata() -> Params {
        Params::new(
            [
                ("source".to_string(), json!("bond_yields")),
                ("symbol".to_string(), json!("US10Y")),
            ]
            .into(),
        )
    }

    /// The data type of LSE's daily 10-year US yields under the identifier
    /// `US10Y`, as Nautilus builds it.
    fn us10y() -> DataType {
        DataType::new("LseRow", Some(us10y_metadata()), Some("US10Y".to_string()))
    }

    /// The requirements of a strategy reading LSE's daily 10-year US
    /// yields alone, a custom series under the identifier `US10Y`, over a
    /// day.
    fn reading_us10y() -> DataRequirements {
        let series =
            CustomSeries::new("LseRow", "US10Y".to_string(), Some(us10y_metadata())).unwrap();
        DataRequirements {
            custom: vec![series],
            ..reading(&[])
        }
    }

    /// The custom data requests of `commands`, in order.
    fn custom_requests(
        commands: &TypedIntoMessageSavingHandler<DataCommand>,
    ) -> Vec<RequestCustomData> {
        commands
            .get_messages()
            .into_iter()
            .filter_map(|command| match command {
                DataCommand::Request(RequestCommand::Data(request)) => Some(request),
                _ => None,
            })
            .collect()
    }

    /// Live on 2026-08-10 at 14:30, a custom series is requested of its
    /// type's row's live data client on the node, LSE's for LSE's rows,
    /// over the day of warmup ending now, by its data type, catalog first
    /// and written back: the data engine answers what the catalog holds
    /// and asks the client for the rest.
    #[rstest]
    fn a_live_warmup_requests_a_custom_series_of_its_type_s_client_over_the_window() {
        let warmup = Warmup::live(&reading_us10y());
        let (mut probe, commands) = registered();

        warmup.start(&mut probe).unwrap();

        let requests = custom_requests(&commands);
        let [request] = requests.as_slice() else {
            panic!("{} custom data requests", requests.len())
        };
        let at = |time: &str| -> Timestamp { time.parse().unwrap() };
        assert_eq!(
            (
                &request.data_type,
                request.client_id,
                request.start,
                request.end
            ),
            (
                &us10y(),
                ClientId::from("LSE"),
                Some(at("2026-08-09T14:30:00Z")),
                Some(at("2026-08-10T14:30:00Z"))
            )
        );
        let update_catalog = request
            .params
            .as_ref()
            .and_then(|params| params.get_bool("update_catalog"));
        assert_eq!(update_catalog, Some(true));
    }

    /// LSE's daily 10-year US yields under `identifier`, a custom series.
    fn us10y_under(identifier: &str) -> CustomSeries {
        CustomSeries::new("LseRow", identifier.to_string(), Some(us10y_metadata())).unwrap()
    }

    /// The data types the custom data requests of a live warmup of
    /// `custom`, started on 2026-08-10 at 14:30, ask for, in order.
    fn requested_custom(custom: Vec<CustomSeries>) -> Vec<DataType> {
        let warmup = Warmup::live(&DataRequirements {
            custom,
            ..reading(&[])
        });
        let (mut probe, commands) = registered();

        warmup.start(&mut probe).unwrap();

        custom_requests(&commands)
            .into_iter()
            .map(|request| request.data_type)
            .collect()
    }

    /// The yields under GC's id, a root GC rolls into GCZ26 on 2026-07-29
    /// (`rolls/README.md`), are requested as GCZ26's, the contract standing
    /// for GC on 2026-08-10, their type and metadata kept; GCZ26's own
    /// yields, listed beside the root's, are the same data type now:
    /// requested once.
    #[rstest]
    #[case::the_root_alone(&["GC.GLBX"])]
    #[case::the_root_and_its_contract(&["GC.GLBX", "GCZ26.GLBX"])]
    fn a_live_warmup_requests_a_roots_custom_series_once_on_the_contract_standing_for_it(
        #[case] identifiers: &[&str],
    ) {
        let custom = identifiers.iter().map(|&id| us10y_under(id)).collect();

        assert_eq!(
            requested_custom(custom),
            [us10y_under("GCZ26.GLBX").data_type().clone()]
        );
    }

    /// A row of `us10y()` made at midnight on 2026-08-`day`, its close
    /// `close`.
    fn us10y_row(day: &str, close: f64) -> CustomData {
        let ts = UnixNanos::from(
            format!("2026-08-{day}T00:00:00Z")
                .parse::<Timestamp>()
                .unwrap(),
        );
        let fields = [("close".to_string(), json!(close))].into_iter().collect();
        let row = ::lse::LseRow::new("bond_yields".into(), "US10Y".into(), fields, ts, ts);
        CustomData::new(Arc::new(row), us10y())
    }

    /// The closes of the rows of `us10y()` `probe`'s cache holds, the
    /// latest added first, as Nautilus' cache keeps them.
    fn cached_us10y_closes(probe: &Probe) -> Vec<Value> {
        probe
            .cache()
            .custom_data_history(&us10y())
            .iter()
            .map(|point| {
                point
                    .data
                    .as_any()
                    .downcast_ref::<::lse::LseRow>()
                    .unwrap()
                    .fields["close"]
                    .clone()
            })
            .collect()
    }

    /// The response's rows reach the cache, where the data engine puts no
    /// response's, oldest first: the cache holds them the latest first.
    #[rstest]
    fn the_rows_of_a_warmup_s_custom_series_land_in_the_cache() {
        let warmup = Warmup::live(&reading_us10y());
        let (mut probe, _commands) = registered();
        warmup.start(&mut probe).unwrap();

        warmup
            .on_historical_data(&probe, &vec![us10y_row("09", 4.25), us10y_row("10", 4.5)])
            .unwrap();

        assert_eq!(cached_us10y_closes(&probe), [json!(4.5), json!(4.25)]);
    }

    /// The node takes a live row of 2026-08-10, closing at 4.40, while the
    /// warmup's request is pending, and caches it. The response's rows, of
    /// 2026-08-08, 2026-08-09 and 2026-08-10, no later than it, are left
    /// out: the live row stays the latest, and the cache holds the row the
    /// history and the live stream both carry once.
    #[rstest]
    fn a_warmup_response_older_than_a_live_row_leaves_it_the_latest() {
        let warmup = Warmup::live(&reading_us10y());
        let (mut probe, _commands) = registered();
        warmup.start(&mut probe).unwrap();
        probe
            .cache_rc()
            .borrow_mut()
            .add_custom_data(us10y_row("10", 4.4));

        warmup
            .on_historical_data(
                &probe,
                &vec![
                    us10y_row("08", 4.1),
                    us10y_row("09", 4.25),
                    us10y_row("10", 4.4),
                ],
            )
            .unwrap();

        assert_eq!(cached_us10y_closes(&probe), [json!(4.4)]);
    }

    /// What a warmup hands custom data to, recording the close of each row.
    #[derive(Default)]
    struct Rows {
        closes: RefCell<Vec<Value>>,
    }

    impl CustomIndicator for Rows {
        fn handle_custom(&self, data: &CustomData) {
            let row = data.data.as_any().downcast_ref::<::lse::LseRow>().unwrap();
            self.closes.borrow_mut().push(row.fields["close"].clone());
        }
    }

    /// Custom data as quotes: started live, its group pending, the history
    /// answers the rows of 2026-08-09 and 2026-08-10; live, the source
    /// sends the row of 2026-08-10 again and that of 2026-08-11, both held
    /// until the group ends. The custom indicator then reads the history's
    /// rows, then the live row of 2026-08-11; the one the history holds is
    /// dropped.
    #[rstest]
    fn live_custom_data_is_held_and_dropped_at_the_seam_as_ticks_are() {
        let rows = Rc::new(Rows::default());
        let warmup = Warmup::live(&reading_us10y()).feeding_custom(Rc::clone(&rows) as _);
        let (mut probe, _) = registered();
        warmup.start(&mut probe).unwrap();

        warmup
            .on_historical_data(&probe, &vec![us10y_row("09", 4.25), us10y_row("10", 4.5)])
            .unwrap();
        for live in [us10y_row("10", 4.5), us10y_row("11", 4.4)] {
            warmup.on_data(&live).unwrap();
        }
        let pending = rows.closes.borrow().clone();
        warmup.complete(&probe).unwrap();

        assert_eq!(pending, [json!(4.25), json!(4.5)]);
        assert_eq!(rows.closes.take(), [json!(4.25), json!(4.5), json!(4.4)]);
    }

    /// A replay holds nothing: each row the strategy gets reaches the
    /// custom indicator as it comes.
    #[rstest]
    fn replayed_custom_data_goes_straight_to_the_custom_indicator() {
        let rows = Rc::new(Rows::default());
        let warmup = Warmup::replayed(&reading_us10y()).feeding_custom(Rc::clone(&rows) as _);
        let (mut probe, _) = registered();
        warmup.start(&mut probe).unwrap();

        warmup.on_data(&us10y_row("09", 4.25)).unwrap();
        warmup.on_data(&us10y_row("10", 4.5)).unwrap();

        assert_eq!(rows.closes.take(), [json!(4.25), json!(4.5)]);
    }

    /// What a warmup hands books to, recording them.
    #[derive(Default)]
    struct Books {
        depths: RefCell<Vec<OrderBookDepth>>,
        deltas: RefCell<Vec<OrderBookDeltas>>,
    }

    impl BookIndicator for Books {
        fn handle_depth(&self, depth: &OrderBookDepth) {
            self.depths.borrow_mut().push(depth.clone());
        }

        fn handle_deltas(&self, deltas: &OrderBookDeltas) {
            self.deltas.borrow_mut().push(deltas.clone());
        }
    }

    /// A delta of GCZ26 with `sequence` and `flags`.
    fn delta(sequence: u64, flags: u8) -> OrderBookDelta {
        OrderBookDelta::new(
            InstrumentId::from("GCZ26.GLBX"),
            BookAction::Add,
            BookOrder::new(
                OrderSide::Buy,
                Price::from("4000.0"),
                Quantity::from(1),
                sequence,
            ),
            flags,
            sequence,
            UnixNanos::from(sequence),
            UnixNanos::from(sequence),
        )
    }

    /// Nautilus answers a deltas request with the deltas alone; the warmup
    /// hands them over in the batches the live stream carries, each closed
    /// by its `F_LAST` delta, the trailing ones without it a batch of
    /// their own, as Nautilus' data engine publishes a response's deltas.
    #[rstest]
    fn the_warmup_hands_book_deltas_over_in_the_batches_the_live_stream_carries() {
        let books = Rc::new(Books::default());
        let warmup = Warmup::live(&reading(&[])).feeding_books(Rc::clone(&books) as _);
        let last = RecordFlag::F_LAST as u8;

        warmup
            .on_historical_book_deltas(&[delta(1, 0), delta(2, last), delta(3, last), delta(4, 0)])
            .unwrap();

        let batches: Vec<Vec<u64>> = books
            .deltas
            .borrow()
            .iter()
            .map(|batch| batch.deltas.iter().map(|d| d.sequence).collect())
            .collect();
        assert_eq!(batches, [vec![1, 2], vec![3], vec![4]]);
    }

    /// A snapshot of a book's depth made, and numbered, at `at`.
    fn depth_at(at: u64) -> OrderBookDepth {
        OrderBookDepth {
            sequence: at,
            ts_event: UnixNanos::from(at),
            ..stub_depth10()
        }
    }

    /// A batch of GCZ26's deltas, one made, and numbered, at `at`.
    fn deltas_at(at: u64) -> OrderBookDeltas {
        OrderBookDeltas::new(
            InstrumentId::from("GCZ26.GLBX"),
            vec![delta(at, RecordFlag::F_LAST as u8)],
        )
    }

    /// Depth and deltas as quotes: started live, its group pending, the
    /// history answers snapshots made at 1 and 2 and a batch of deltas made
    /// at 3; live, the venue sends the snapshot of 2 again, one made at 5,
    /// the batch of 3 again and one made at 6, all held until the group
    /// ends. The book indicators then read the history's, then the live
    /// snapshot of 5 and batch of 6; the two the history holds are dropped.
    #[rstest]
    fn live_book_depth_and_deltas_are_held_and_dropped_at_the_seam_as_ticks_are() {
        let books = Rc::new(Books::default());
        let warmup = Warmup::live(&reading_ticks(
            &[],
            vec![tick(NautilusDataType::QuoteTick, XAU)],
        ))
        .feeding_books(Rc::clone(&books) as _);
        let (mut probe, _) = registered();
        warmup.start(&mut probe).unwrap();

        warmup
            .on_historical_book_depth(&[depth_at(1), depth_at(2)])
            .unwrap();
        warmup
            .on_historical_book_deltas(&[delta(3, RecordFlag::F_LAST as u8)])
            .unwrap();
        for live in [depth_at(2), depth_at(5)] {
            warmup.on_book_depth(&live).unwrap();
        }
        for live in [deltas_at(3), deltas_at(6)] {
            warmup.on_book_deltas(&live).unwrap();
        }
        let pending = (books.depths.borrow().len(), books.deltas.borrow().len());
        warmup.complete(&probe).unwrap();

        let depths: Vec<u64> = books.depths.borrow().iter().map(|d| d.sequence).collect();
        let batches: Vec<u64> = books.deltas.borrow().iter().map(|b| b.sequence).collect();
        assert_eq!(pending, (2, 1));
        assert_eq!(depths, [1, 2, 5]);
        assert_eq!(batches, [3, 6]);
    }

    /// The cache holds the warmup of the harness's bars too: 1,440
    /// 1-minute bars over a day, and one spanning the start.
    #[rstest]
    fn the_cache_holds_the_warmup_of_every_bar_type_the_warmup_builds() {
        let minutes = BarType::from("SXR8.IBIS-1-MINUTE-MID-INTERNAL");

        let warmup = Warmup::live(&reading(&["SXR8.IBIS-1-HOUR-MID-INTERNAL"]))
            .building(minutes, Rc::new(Inert));

        assert_eq!(warmup.cache_config().unwrap().bar_capacity, 10_000 + 1_441);
    }

    /// The warmup requests each bar type's history from what Nautilus
    /// builds it from: LAST bars from the instrument's trades, MID bars
    /// from its quotes, bars of source bars from those, one request per
    /// series building every bar type of it.
    #[rstest]
    fn each_bar_type_warms_up_on_what_its_price_type_builds_it_from() {
        let hour_last = BarType::from("ESM24.CME-1-HOUR-LAST-INTERNAL");
        let minute_last = BarType::from("ESM24.CME-1-MINUTE-LAST-INTERNAL");
        let hour_mid = BarType::from("ESM24.CME-1-HOUR-MID-INTERNAL");
        let composite = BarType::from("ESM24.CME-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL");
        let ticks = |data, instrument_id: &str| {
            CatalogSeries::Ticks(TickSeries {
                data,
                instrument_id: InstrumentId::from(instrument_id),
            })
        };
        let warmup = Warmup::live(&reading(&[
            "ESM24.CME-1-HOUR-LAST-INTERNAL",
            "ESM24.CME-1-HOUR-MID-INTERNAL",
            "ESM24.CME-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
            "ESM24.CME-1-MINUTE-LAST-INTERNAL",
        ]));
        let (mut probe, commands) = registered();

        warmup.start(&mut probe).unwrap();

        assert_eq!(
            requested(&commands),
            vec![
                (
                    ticks(NautilusDataType::TradeTick, "ESM24.CME"),
                    vec![hour_last, minute_last]
                ),
                (
                    ticks(NautilusDataType::QuoteTick, "ESM24.CME"),
                    vec![hour_mid]
                ),
                (
                    CatalogSeries::Bars(BarType::from("ESM24.CME-1-MINUTE-LAST-EXTERNAL")),
                    vec![composite]
                ),
            ]
        );
    }

    /// An external bar type, an IDEALPRO pair's BID and ASK bars, is
    /// requested as it is and builds nothing: Nautilus' data engine refuses
    /// a request naming one in its `bar_types`, aggregating into internal
    /// bar types alone. One that is also a composite's source is requested
    /// once, building the composite.
    #[rstest]
    fn an_external_bar_type_is_requested_as_it_is_building_nothing() {
        let bid = BarType::from("EUR/USD.IDEALPRO-1-MINUTE-BID-EXTERNAL");
        let ask = BarType::from("EUR/USD.IDEALPRO-1-MINUTE-ASK-EXTERNAL");
        let minutes = BarType::from("ESM24.CME-1-MINUTE-LAST-EXTERNAL");
        let composite = BarType::from("ESM24.CME-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL");
        let warmup = Warmup::live(&reading(&[
            "ESM24.CME-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
            "EUR/USD.IDEALPRO-1-MINUTE-BID-EXTERNAL",
            "EUR/USD.IDEALPRO-1-MINUTE-ASK-EXTERNAL",
            "ESM24.CME-1-MINUTE-LAST-EXTERNAL",
        ]));
        let (mut probe, commands) = registered();

        warmup.start(&mut probe).unwrap();

        assert_eq!(
            requested(&commands),
            vec![
                (CatalogSeries::Bars(minutes), vec![composite]),
                (CatalogSeries::Bars(bid), vec![]),
                (CatalogSeries::Bars(ask), vec![]),
            ]
        );
    }

    /// A request's completion hands over the bars of the bar types it built
    /// alone, each once, under the standard type a node caches them by: the
    /// composite's under its type without its source. Another request's
    /// completion hands over its own.
    #[rstest]
    fn a_completed_request_hands_over_the_bars_it_built_alone() {
        let hour_mid = BarType::from("ESM24.CME-1-HOUR-MID-INTERNAL");
        let composite = BarType::from("ESM24.CME-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL");
        let recorder = Rc::new(Recorder::default());
        let warmup = Warmup::live(&reading(&[
            "ESM24.CME-1-HOUR-MID-INTERNAL",
            "ESM24.CME-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
        ]))
        .feeding(Rc::clone(&recorder) as SharedActorIndicator);
        let (mut probe, commands) = registered();
        warmup.start(&mut probe).unwrap();
        for bar_type in [hour_mid, composite.standard()] {
            let bar = Bar {
                bar_type,
                ..Bar::default()
            };
            probe.cache_rc().borrow_mut().add_bar(bar).unwrap();
        }
        let id_of = |wanted: &CatalogSeries| {
            sent_requests(&commands)
                .into_iter()
                .find_map(|(source, id, _)| (source == *wanted).then_some(id))
                .unwrap()
        };
        let quotes = id_of(&CatalogSeries::Ticks(tick(
            NautilusDataType::QuoteTick,
            "ESM24.CME",
        )));
        let minutes = id_of(&CatalogSeries::Bars(BarType::from(
            "ESM24.CME-1-MINUTE-LAST-EXTERNAL",
        )));
        let completed = |id| DataResponseEnvelope::terminal(id, None);

        warmup.on_data_response(&probe, &completed(quotes)).unwrap();
        assert_eq!(recorder.bar_types.take(), [hour_mid]);
        warmup
            .on_data_response(&probe, &completed(minutes))
            .unwrap();
        assert_eq!(recorder.bar_types.take(), [composite.standard()]);
        // Its completion reads once: nothing is handed over again.
        warmup
            .on_data_response(&probe, &completed(minutes))
            .unwrap();
        assert_eq!(recorder.bar_types.take(), []);
    }

    /// A day of every series is asked in windows of 23 hours, the two a day
    /// can hold: under a day by more than a bar, so that IB is asked each in
    /// seconds (see `WINDOW`).
    #[rstest]
    fn every_series_is_asked_in_windows_under_a_day() {
        let warmup = Warmup::live(&reading_ticks(
            &[
                "XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL",
                "ESM24.CME-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
            ],
            vec![
                tick(NautilusDataType::TradeTick, "GCZ26.GLBX"),
                tick(NautilusDataType::OrderBookDepth, "GCZ26.GLBX"),
                tick(NautilusDataType::OrderBookDelta, "GCZ26.GLBX"),
            ],
        ));
        let (mut probe, commands) = registered();

        warmup.start(&mut probe).unwrap();

        let sent = sent_requests(&commands);
        assert_eq!(sent.len(), 5);
        for (source, _, params) in sent {
            let params = params.expect("params");
            assert!(params.contains_key("time_range_generator"), "{source:?}");
            assert_eq!(
                params.get("durations_seconds"),
                Some(&json!([82_800.0, 82_800.0])),
                "{source:?}"
            );
        }
    }

    /// A live start's warmup goes on until its group ends, whatever its
    /// requests answered by then: warming once started, still once its
    /// first series' request is answered, SXR8's quotes or the custom
    /// series of LSE's yields alone, and no longer once the group ends.
    #[rstest]
    #[case::of_bars(reading(&["SXR8.IBIS-1-HOUR-MID-INTERNAL"]))]
    #[case::of_a_custom_series_alone(reading_us10y())]
    fn a_warmup_goes_on_until_its_group_ends(#[case] requirements: DataRequirements) {
        let warmup = Warmup::live(&requirements);
        let (mut probe, commands) = registered();
        warmup.start(&mut probe).unwrap();
        let request = commands
            .get_messages()
            .into_iter()
            .find_map(|command| match command {
                DataCommand::Request(RequestCommand::Instrument(_)) => None,
                DataCommand::Request(request) => Some(*request.request_id()),
                _ => None,
            })
            .expect("a series requested");

        let started = warmup.is_warming();
        warmup
            .on_data_response(&probe, &DataResponseEnvelope::terminal(request, None))
            .unwrap();
        let answered = warmup.is_warming();
        warmup.complete(&probe).unwrap();

        assert_eq!(
            (started, answered, warmup.is_warming()),
            (true, true, false)
        );
    }

    /// A replay's warmup makes no request, and so never goes on.
    #[rstest]
    fn a_replays_warmup_never_goes_on() {
        let replayed = Warmup::replayed(&reading(&["SXR8.IBIS-1-HOUR-MID-INTERNAL"]));
        let (mut probe, _) = registered();

        replayed.start(&mut probe).unwrap();

        assert!(!replayed.is_warming());
    }

    /// What `Ticks` records of a tick: its kind, its instrument and its
    /// `ts_event`.
    type Read = (&'static str, InstrumentId, UnixNanos);

    /// An indicator recording each quote and trade it reads.
    #[derive(Default)]
    struct Ticks {
        read: RefCell<Vec<Read>>,
    }

    impl ActorIndicator for Ticks {
        fn key(&self) -> usize {
            std::ptr::from_ref(self).addr()
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn initialized(&self) -> anyhow::Result<bool> {
            Ok(true)
        }

        fn handle_quote(&self, quote: &QuoteTick) -> anyhow::Result<()> {
            let read = ("quote", quote.instrument_id, quote.ts_event);
            self.read.borrow_mut().push(read);
            Ok(())
        }

        fn handle_trade(&self, trade: &TradeTick) -> anyhow::Result<()> {
            let read = ("trade", trade.instrument_id, trade.ts_event);
            self.read.borrow_mut().push(read);
            Ok(())
        }

        fn handle_bar(&self, _bar: &Bar) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// CME gold, whose history LSE serves, quotes and trades.
    const XAU: &str = "GCZ26.GLBX";
    const EUR: &str = "EURUSD.ICMARKETS";

    /// `time` on 2026-08-10, UTC, the day `registered` starts on.
    fn on_the_day(time: &str) -> UnixNanos {
        let at: Timestamp = format!("2026-08-10T{time}Z").parse().unwrap();
        UnixNanos::from(at)
    }

    /// A quote of `instrument_id` made at `made` (`ts_event`) and stamped
    /// by its client at `received` (`ts_init`).
    fn quote_of(instrument_id: &str, made: &str, received: &str) -> QuoteTick {
        QuoteTick::new(
            InstrumentId::from(instrument_id),
            Price::from("4000.00"),
            Price::from("4000.10"),
            Quantity::from(1),
            Quantity::from(1),
            on_the_day(made),
            on_the_day(received),
        )
    }

    /// A quote of GCZ26 made at `made` and stamped then, as a history's.
    fn quote(made: &str) -> QuoteTick {
        quote_of(XAU, made, made)
    }

    /// A trade of GCZ26 made at `made` and stamped by its client at
    /// `received`.
    fn trade(made: &str, received: &str) -> TradeTick {
        TradeTick::new(
            InstrumentId::from(XAU),
            Price::from("4000.00"),
            Quantity::from(1),
            AggressorSide::Buy,
            TradeId::from(made),
            on_the_day(made),
            on_the_day(received),
        )
    }

    /// What `Ticks` records of a quote of `instrument_id` made at `made`.
    fn read_quote(instrument_id: &str, made: &str) -> Read {
        ("quote", InstrumentId::from(instrument_id), on_the_day(made))
    }

    /// What `Ticks` records of a trade of GCZ26 made at `made`.
    fn read_trade(made: &str) -> Read {
        ("trade", InstrumentId::from(XAU), on_the_day(made))
    }

    /// The live warmup of a day of GCZ26's quotes and trades handing its
    /// ticks to `ticks`, started at 14:30 (`registered`), its group pending,
    /// and the strategy it started on.
    fn warming(ticks: &Rc<Ticks>) -> (Warmup, Probe) {
        let warmup = Warmup::live(&reading_ticks(
            &[],
            vec![
                tick(NautilusDataType::QuoteTick, XAU),
                tick(NautilusDataType::TradeTick, XAU),
            ],
        ))
        .feeding_ticks(Rc::clone(ticks) as SharedActorIndicator);
        let (mut probe, _) = registered();
        warmup.start(&mut probe).unwrap();
        (warmup, probe)
    }

    /// Worked by hand. Started at 14:30, the warmup is answered in two
    /// windows, 23 hours up to 13:30 then the hour since: the first holds
    /// quotes made at 12:00 and 13:29; a live quote made at 14:30:01 comes
    /// before the second, which holds 14:00 and 14:29; another, made at
    /// 14:30:02, before the group ends. The live quotes are held until then:
    /// the features read the four of the history, then the two live ones as
    /// they came, in time order.
    #[rstest]
    fn live_quotes_reach_the_features_after_the_warmups_last_window_in_the_order_they_came() {
        let ticks = Rc::new(Ticks::default());
        let (warmup, probe) = warming(&ticks);

        warmup
            .on_historical_quotes(&[quote("12:00:00"), quote("13:29:00")])
            .unwrap();
        warmup
            .on_quote(&quote_of(XAU, "14:30:01", "14:30:01.050"))
            .unwrap();
        warmup
            .on_historical_quotes(&[quote("14:00:00"), quote("14:29:00")])
            .unwrap();
        warmup
            .on_quote(&quote_of(XAU, "14:30:02", "14:30:02.050"))
            .unwrap();
        warmup.complete(&probe).unwrap();

        assert_eq!(
            *ticks.read.borrow(),
            [
                read_quote(XAU, "12:00:00"),
                read_quote(XAU, "13:29:00"),
                read_quote(XAU, "14:00:00"),
                read_quote(XAU, "14:29:00"),
                read_quote(XAU, "14:30:01"),
                read_quote(XAU, "14:30:02"),
            ]
        );
    }

    /// The hold ends with the group, not with one of its requests. Started
    /// at 14:30, the warmup asks GCZ26's quotes and its trades, a request
    /// each: the quotes' answers a quote made at 14:29:00, the trades' a
    /// trade made at 14:29:30, and the quotes' completes; then a live quote
    /// made at 14:30:01 comes while the trades' request is still pending,
    /// and is held, and the group ends. The features read the history while
    /// the group is pending, and the live quote last.
    #[rstest]
    fn live_ticks_are_held_until_the_group_ends_not_one_of_its_requests() {
        let ticks = Rc::new(Ticks::default());
        let warmup = Warmup::live(&reading_ticks(
            &[],
            vec![
                tick(NautilusDataType::QuoteTick, XAU),
                tick(NautilusDataType::TradeTick, XAU),
            ],
        ))
        .feeding_ticks(Rc::clone(&ticks) as SharedActorIndicator);
        let (mut probe, commands) = registered();
        warmup.start(&mut probe).unwrap();
        let quotes = sent_requests(&commands)
            .into_iter()
            .find_map(|(source, id, _)| {
                (source == CatalogSeries::Ticks(tick(NautilusDataType::QuoteTick, XAU)))
                    .then_some(id)
            })
            .unwrap();

        warmup.on_historical_quotes(&[quote("14:29:00")]).unwrap();
        warmup
            .on_historical_trades(&[trade("14:29:30", "14:29:30")])
            .unwrap();
        warmup
            .on_data_response(&probe, &DataResponseEnvelope::terminal(quotes, None))
            .unwrap();
        warmup
            .on_quote(&quote_of(XAU, "14:30:01", "14:30:01.050"))
            .unwrap();
        let pending = ticks.read.borrow().clone();
        warmup.complete(&probe).unwrap();

        assert_eq!(
            pending,
            [read_quote(XAU, "14:29:00"), read_trade("14:29:30")]
        );
        assert_eq!(
            *ticks.read.borrow(),
            [
                read_quote(XAU, "14:29:00"),
                read_trade("14:29:30"),
                read_quote(XAU, "14:30:01"),
            ]
        );
    }

    /// Worked by hand: the overlap at the seam. The history's last quote
    /// was made at 14:29:59. Held while the warmup is pending: the venue's
    /// resend of it on the subscription, made at 14:29:59 and stamped on
    /// receipt at 14:30:00.200, a `ts_init` after the history's; a quote
    /// made at 14:29:57 and received at 14:30:00.300; and one made at
    /// 14:30:00.400. The first two are at or before the history's last by
    /// `ts_event`, which the history holds, so dropped whatever their
    /// `ts_init`; the third is after it, and read.
    #[rstest]
    fn live_quotes_at_or_before_the_historys_last_are_dropped_whatever_their_ts_init() {
        let ticks = Rc::new(Ticks::default());
        let (warmup, probe) = warming(&ticks);

        warmup
            .on_historical_quotes(&[quote("14:29:58"), quote("14:29:59")])
            .unwrap();
        for live in [
            quote_of(XAU, "14:29:59", "14:30:00.200"),
            quote_of(XAU, "14:29:57", "14:30:00.300"),
            quote_of(XAU, "14:30:00.400", "14:30:00.450"),
        ] {
            warmup.on_quote(&live).unwrap();
        }
        warmup.complete(&probe).unwrap();

        assert_eq!(
            *ticks.read.borrow(),
            [
                read_quote(XAU, "14:29:58"),
                read_quote(XAU, "14:29:59"),
                read_quote(XAU, "14:30:00.400"),
            ]
        );
    }

    /// The overlap is judged within a series, the quotes or the trades of
    /// one instrument: the history of GCZ26's quotes ends at 14:29:59, and
    /// a live trade of GCZ26 made at 14:29:30 and a live quote of EURUSD
    /// made at 14:29:00, series without history, are read.
    #[rstest]
    fn a_series_without_history_drops_none_of_its_live_ticks() {
        let ticks = Rc::new(Ticks::default());
        let (warmup, probe) = warming(&ticks);

        warmup.on_historical_quotes(&[quote("14:29:59")]).unwrap();
        warmup.on_trade(&trade("14:29:30", "14:30:00.100")).unwrap();
        warmup
            .on_quote(&quote_of(EUR, "14:29:00", "14:30:00.200"))
            .unwrap();
        warmup.complete(&probe).unwrap();

        assert_eq!(
            *ticks.read.borrow(),
            [
                read_quote(XAU, "14:29:59"),
                read_trade("14:29:30"),
                read_quote(EUR, "14:29:00"),
            ]
        );
    }

    /// Worked by hand: trades as quotes. The history answers a trade made
    /// at 14:29:58; a live trade made at 14:30:01 comes, held; the history
    /// answers one made at 14:29:59, then the venue resends it live, made at
    /// 14:29:59 and received at 14:30:01.500, dropped. The features read
    /// the history's two, then the live one.
    #[rstest]
    fn live_trades_are_held_and_dropped_at_the_seam_as_quotes_are() {
        let ticks = Rc::new(Ticks::default());
        let (warmup, probe) = warming(&ticks);

        warmup
            .on_historical_trades(&[trade("14:29:58", "14:29:58")])
            .unwrap();
        warmup.on_trade(&trade("14:30:01", "14:30:01.050")).unwrap();
        warmup
            .on_historical_trades(&[trade("14:29:59", "14:29:59")])
            .unwrap();
        warmup.on_trade(&trade("14:29:59", "14:30:01.500")).unwrap();
        warmup.complete(&probe).unwrap();

        assert_eq!(
            *ticks.read.borrow(),
            [
                read_trade("14:29:58"),
                read_trade("14:29:59"),
                read_trade("14:30:01"),
            ]
        );
    }

    /// Once the group has ended, with the history's last quote made at
    /// 14:29:59, a live quote made at 14:30:05 is read as it comes; the
    /// venue's resend of that last quote, made at 14:29:59 and received at
    /// 14:30:00.200, can come after a warmup the catalog answered at once,
    /// and is dropped then too.
    #[rstest]
    #[case::after_the_history("14:30:05", "14:30:05.050", &["14:29:59", "14:30:05"])]
    #[case::held_by_the_history("14:29:59", "14:30:00.200", &["14:29:59"])]
    fn after_the_warmup_live_ticks_reach_the_features_as_they_come_unless_the_history_holds_them(
        #[case] made: &str,
        #[case] received: &str,
        #[case] read: &[&str],
    ) {
        let ticks = Rc::new(Ticks::default());
        let (warmup, probe) = warming(&ticks);
        warmup.on_historical_quotes(&[quote("14:29:59")]).unwrap();
        warmup.complete(&probe).unwrap();

        warmup.on_quote(&quote_of(XAU, made, received)).unwrap();

        let read: Vec<Read> = read.iter().map(|&made| read_quote(XAU, made)).collect();
        assert_eq!(*ticks.read.borrow(), read);
    }

    /// A group ending failed ends the warmup all the same: the live quote
    /// held, made at 14:30:01, is read, and the live ones after it as they
    /// come.
    #[rstest]
    fn a_failed_warmup_hands_over_the_live_ticks_it_held() {
        let ticks = Rc::new(Ticks::default());
        let (warmup, probe) = warming(&ticks);
        warmup
            .on_quote(&quote_of(XAU, "14:30:01", "14:30:01.050"))
            .unwrap();

        let err = warmup.fail(&probe, "venue cut the history").to_string();
        warmup
            .on_quote(&quote_of(XAU, "14:30:02", "14:30:02.050"))
            .unwrap();

        assert!(err.contains("venue cut the history"), "{err}");
        assert_eq!(
            *ticks.read.borrow(),
            [read_quote(XAU, "14:30:01"), read_quote(XAU, "14:30:02")]
        );
    }

    /// A replay requests no warmup and holds nothing: a quote, a snapshot
    /// of depth made at 5 and a batch of deltas made at 6 reach the
    /// features and the book indicators as they come.
    #[rstest]
    fn a_replays_ticks_and_books_reach_the_features_as_they_come() {
        let ticks = Rc::new(Ticks::default());
        let books = Rc::new(Books::default());
        let replayed = Warmup::replayed(&reading_ticks(
            &[],
            vec![tick(NautilusDataType::QuoteTick, XAU)],
        ))
        .feeding_ticks(Rc::clone(&ticks) as SharedActorIndicator)
        .feeding_books(Rc::clone(&books) as _);
        let (mut probe, _) = registered();
        replayed.start(&mut probe).unwrap();

        replayed.on_quote(&quote("14:30:01")).unwrap();
        replayed.on_book_depth(&depth_at(5)).unwrap();
        replayed.on_book_deltas(&deltas_at(6)).unwrap();

        let depths: Vec<u64> = books.depths.borrow().iter().map(|d| d.sequence).collect();
        let batches: Vec<u64> = books.deltas.borrow().iter().map(|b| b.sequence).collect();
        assert_eq!(*ticks.read.borrow(), [read_quote(XAU, "14:30:01")]);
        assert_eq!(depths, [5]);
        assert_eq!(batches, [6]);
    }

    /// What `Log` records of what it reads: its kind and when it was made.
    type Entry = (&'static str, UnixNanos);

    /// A feature reading every kind the warmup hands on, recording each in
    /// one log, so that the order across kinds shows.
    #[derive(Default)]
    struct Log {
        read: RefCell<Vec<Entry>>,
    }

    impl ActorIndicator for Log {
        fn key(&self) -> usize {
            std::ptr::from_ref(self).addr()
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn initialized(&self) -> anyhow::Result<bool> {
            Ok(true)
        }

        fn handle_quote(&self, quote: &QuoteTick) -> anyhow::Result<()> {
            self.read.borrow_mut().push(("quote", quote.ts_event));
            Ok(())
        }

        fn handle_trade(&self, trade: &TradeTick) -> anyhow::Result<()> {
            self.read.borrow_mut().push(("trade", trade.ts_event));
            Ok(())
        }

        fn handle_bar(&self, _bar: &Bar) -> anyhow::Result<()> {
            Ok(())
        }
    }

    impl BookIndicator for Log {
        fn handle_depth(&self, depth: &OrderBookDepth) {
            self.read.borrow_mut().push(("depth", depth.ts_event));
        }

        fn handle_deltas(&self, deltas: &OrderBookDeltas) {
            self.read.borrow_mut().push(("deltas", deltas.ts_event));
        }
    }

    impl CustomIndicator for Log {
        fn handle_custom(&self, data: &CustomData) {
            self.read
                .borrow_mut()
                .push(("custom", data.data.ts_event()));
        }
    }

    /// The live warmup of `requirements` handing everything to `log`,
    /// started at 14:30 (`registered`), its group pending, the strategy it
    /// started on, and what that sent the data engine.
    fn logging(
        requirements: &DataRequirements,
        log: &Rc<Log>,
    ) -> (Warmup, Probe, TypedIntoMessageSavingHandler<DataCommand>) {
        logging_with(Warmup::live(requirements), log)
    }

    fn logging_with(
        warmup: Warmup,
        log: &Rc<Log>,
    ) -> (Warmup, Probe, TypedIntoMessageSavingHandler<DataCommand>) {
        let warmup = warmup
            .feeding_ticks(Rc::clone(log) as SharedActorIndicator)
            .feeding_books(Rc::clone(log) as _)
            .feeding_custom(Rc::clone(log) as _);
        let (mut probe, commands) = registered();
        warmup.start(&mut probe).unwrap();
        (warmup, probe, commands)
    }

    /// The requirements of a day of GCZ26's quotes and trades.
    fn quotes_and_trades() -> DataRequirements {
        reading_ticks(
            &[],
            vec![
                tick(NautilusDataType::QuoteTick, XAU),
                tick(NautilusDataType::TradeTick, XAU),
            ],
        )
    }

    /// The id of the request of GCZ26's `data` among those `commands` sent.
    fn request_of(
        commands: &TypedIntoMessageSavingHandler<DataCommand>,
        data: NautilusDataType,
    ) -> UUID4 {
        let wanted = CatalogSeries::Ticks(tick(data, XAU));
        sent_requests(commands)
            .into_iter()
            .find_map(|(source, id, _)| (source == wanted).then_some(id))
            .expect("a request of the series")
    }

    /// What `Log` records of a `kind` made at `made` on the day.
    fn entry(kind: &'static str, made: &str) -> Entry {
        (kind, on_the_day(made))
    }

    /// Worked by hand: a feature on GCZ26's quotes and trades, each asked
    /// in two windows, the venue answering them in an order of its own.
    /// The quotes' first window holds 12:00 and 13:00, their second 14:00;
    /// the trades' 12:00:30 and 13:00:30, then 14:00:30. Whatever the order
    /// the four answers come in, the feature reads the six ticks in time
    /// order once the group ends.
    #[rstest]
    #[case::the_quotes_first(&[0, 1, 2, 3])]
    #[case::the_trades_first(&[2, 3, 0, 1])]
    #[case::interleaved(&[2, 0, 3, 1])]
    fn two_series_answered_window_by_window_reach_a_feature_in_time_order(#[case] order: &[usize]) {
        let log = Rc::new(Log::default());
        let (warmup, probe, _) = logging(&quotes_and_trades(), &log);
        let quotes = [
            vec![quote("12:00:00"), quote("13:00:00")],
            vec![quote("14:00:00")],
        ];
        let trades = [
            vec![trade("12:00:30", "12:00:30"), trade("13:00:30", "13:00:30")],
            vec![trade("14:00:30", "14:00:30")],
        ];

        for &answer in order {
            match answer {
                0 | 1 => warmup.on_historical_quotes(&quotes[answer]).unwrap(),
                _ => warmup.on_historical_trades(&trades[answer - 2]).unwrap(),
            }
        }
        warmup.complete(&probe).unwrap();

        assert_eq!(
            *log.read.borrow(),
            [
                entry("quote", "12:00:00"),
                entry("trade", "12:00:30"),
                entry("quote", "13:00:00"),
                entry("trade", "13:00:30"),
                entry("quote", "14:00:00"),
                entry("trade", "14:00:30"),
            ]
        );
    }

    /// While both requests are pending, the history is handed on up to
    /// the series lagging behind: the trades answered both windows, up to
    /// 14:00:30, the quotes their first, up to 13:00. The feature has read
    /// what was made by 13:00, and the trades after it wait for the quotes.
    #[rstest]
    fn a_pending_series_holds_the_others_history_made_after_its_latest() {
        let log = Rc::new(Log::default());
        let (warmup, _probe, _) = logging(&quotes_and_trades(), &log);

        warmup
            .on_historical_trades(&[trade("12:00:30", "12:00:30"), trade("13:00:30", "13:00:30")])
            .unwrap();
        warmup
            .on_historical_trades(&[trade("14:00:30", "14:00:30")])
            .unwrap();
        warmup
            .on_historical_quotes(&[quote("12:00:00"), quote("13:00:00")])
            .unwrap();

        assert_eq!(
            *log.read.borrow(),
            [
                entry("quote", "12:00:00"),
                entry("trade", "12:00:30"),
                entry("quote", "13:00:00"),
            ]
        );
    }

    /// GCZ26's quotes answered 12:00 and 14:00, its trades nothing yet: the
    /// trades could still bring older ones, so the quotes wait. The trades'
    /// request then ends, answered with nothing (a closed market) or failed,
    /// and holds the quotes back no longer: the feature reads them while
    /// the group is still pending. A failed request still ends the warmup
    /// incomplete with its group.
    #[rstest]
    #[case::completed(None)]
    #[case::failed(Some("venue cut the history"))]
    fn a_series_whose_request_ended_holds_the_others_back_no_longer(#[case] failure: Option<&str>) {
        let log = Rc::new(Log::default());
        let (warmup, probe, commands) = logging(&quotes_and_trades(), &log);
        let trades = request_of(&commands, NautilusDataType::TradeTick);

        warmup
            .on_historical_quotes(&[quote("12:00:00"), quote("14:00:00")])
            .unwrap();
        let held = log.read.borrow().clone();
        let ended = DataResponseEnvelope::terminal(trades, failure.map(str::to_string));
        warmup.on_data_response(&probe, &ended).unwrap();

        assert_eq!(held, []);
        assert_eq!(
            *log.read.borrow(),
            [entry("quote", "12:00:00"), entry("quote", "14:00:00")]
        );
        assert!(warmup.is_warming());
        if let Some(reason) = failure {
            let err = warmup.fail(&probe, reason).to_string();
            assert!(err.contains(reason), "{err}");
        }
    }

    /// The requirements of a day of GCZ26's quotes, trades, depth and
    /// deltas, and of LSE's 10-year US yields.
    fn every_kind() -> DataRequirements {
        DataRequirements {
            ticks: [
                NautilusDataType::QuoteTick,
                NautilusDataType::TradeTick,
                NautilusDataType::OrderBookDepth,
                NautilusDataType::OrderBookDelta,
            ]
            .into_iter()
            .map(|data| tick(data, XAU))
            .collect(),
            ..reading_us10y()
        }
    }

    /// A snapshot of GCZ26's depth made at `made`.
    fn depth_made(made: UnixNanos) -> OrderBookDepth {
        OrderBookDepth {
            instrument_id: InstrumentId::from(XAU),
            ts_event: made,
            ts_init: made,
            ..stub_depth10()
        }
    }

    /// A delta of GCZ26 made at `made`, closing its batch.
    fn delta_made(made: UnixNanos) -> OrderBookDelta {
        OrderBookDelta {
            ts_event: made,
            ts_init: made,
            ..delta(1, RecordFlag::F_LAST as u8)
        }
    }

    /// A feature reads ties across series in a fixed order of kinds, as a
    /// replay does: quotes, trades, depth, deltas, then custom data. Every
    /// series answers one point made at midnight, in the reverse order.
    #[rstest]
    fn ties_across_series_reach_a_feature_in_the_order_of_their_kinds() {
        let log = Rc::new(Log::default());
        let (warmup, probe, _) = logging(&every_kind(), &log);
        let midnight = on_the_day("00:00:00");

        warmup
            .on_historical_data(&probe, &vec![us10y_row("10", 4.5)])
            .unwrap();
        warmup
            .on_historical_book_deltas(&[delta_made(midnight)])
            .unwrap();
        warmup
            .on_historical_book_depth(&[depth_made(midnight)])
            .unwrap();
        warmup
            .on_historical_trades(&[trade("00:00:00", "00:00:00")])
            .unwrap();
        warmup.on_historical_quotes(&[quote("00:00:00")]).unwrap();
        warmup.complete(&probe).unwrap();

        let kinds: Vec<&str> = log.read.borrow().iter().map(|(kind, _)| *kind).collect();
        assert_eq!(kinds, ["quote", "trade", "depth", "deltas", "custom"]);
    }

    /// Custom data is merged as ticks are: the yields of 2026-08-09 and
    /// 2026-08-10, at midnight, answered at once, wait for GCZ26's quotes,
    /// answered in two windows, 2026-08-09 at 12:00 then 2026-08-10 at
    /// 06:00; the feature reads them in time order.
    #[rstest]
    fn custom_data_reaches_a_feature_in_time_order_among_ticks() {
        let log = Rc::new(Log::default());
        let requirements = DataRequirements {
            ticks: vec![tick(NautilusDataType::QuoteTick, XAU)],
            ..reading_us10y()
        };
        let (warmup, probe, _) = logging(&requirements, &log);
        let at = |time: &str| UnixNanos::from(time.parse::<Timestamp>().unwrap());
        let noon_before = at("2026-08-09T12:00:00Z");
        let noon_before_quote = QuoteTick {
            ts_event: noon_before,
            ts_init: noon_before,
            ..quote("12:00:00")
        };

        warmup
            .on_historical_data(&probe, &vec![us10y_row("09", 4.25), us10y_row("10", 4.5)])
            .unwrap();
        warmup.on_historical_quotes(&[noon_before_quote]).unwrap();
        warmup.on_historical_quotes(&[quote("06:00:00")]).unwrap();
        warmup.complete(&probe).unwrap();

        assert_eq!(
            *log.read.borrow(),
            [
                ("custom", at("2026-08-09T00:00:00Z")),
                ("quote", noon_before),
                ("custom", at("2026-08-10T00:00:00Z")),
                ("quote", at("2026-08-10T06:00:00Z")),
            ]
        );
    }

    /// The history held is bounded: past it, the warmup ends incomplete,
    /// naming the series held back. Bounded at two, GCZ26's quotes answer
    /// three while its trades answer nothing: the warmup is no longer
    /// warming, the feature has read the three quotes, in order, and the
    /// trades the venue answers after are left out, older than what it
    /// read.
    #[rstest]
    fn past_its_bound_the_warmup_ends_incomplete_naming_the_series_held_back() {
        let log = Rc::new(Log::default());
        let (warmup, _probe, _) = logging_with(Warmup::live(&quotes_and_trades()).holding(2), &log);

        let err = warmup
            .on_historical_quotes(&[quote("12:00:00"), quote("13:00:00"), quote("14:00:00")])
            .expect_err("past the bound");
        let after = warmup.on_historical_trades(&[trade("12:30:00", "12:30:00")]);

        let err = format!("{err:#}");
        for part in ["incomplete", "quotes of GCZ26.GLBX"] {
            assert!(err.contains(part), "{err}");
        }
        assert!(!warmup.is_warming());
        assert!(after.is_ok(), "{after:?}");
        assert_eq!(
            *log.read.borrow(),
            [
                entry("quote", "12:00:00"),
                entry("quote", "13:00:00"),
                entry("quote", "14:00:00"),
            ]
        );
    }

    /// The live ticks held still follow the history, deduplicated against
    /// what was handed on: GCZ26's quotes answer 14:29:59, held for the
    /// trades; live, the venue resends that quote, then sends one made at
    /// 14:30:01 and a trade made at 14:30:02; the trades answer 14:29:58.
    /// The feature reads the history in time order, then the live ticks
    /// but the resend.
    #[rstest]
    fn live_ticks_held_follow_the_merged_history_deduplicated() {
        let log = Rc::new(Log::default());
        let (warmup, probe, _) = logging(&quotes_and_trades(), &log);

        warmup.on_historical_quotes(&[quote("14:29:59")]).unwrap();
        warmup
            .on_quote(&quote_of(XAU, "14:29:59", "14:30:00.200"))
            .unwrap();
        warmup
            .on_quote(&quote_of(XAU, "14:30:01", "14:30:01.050"))
            .unwrap();
        warmup.on_trade(&trade("14:30:02", "14:30:02.050")).unwrap();
        warmup
            .on_historical_trades(&[trade("14:29:58", "14:29:58")])
            .unwrap();
        warmup.complete(&probe).unwrap();

        assert_eq!(
            *log.read.borrow(),
            [
                entry("trade", "14:29:58"),
                entry("quote", "14:29:59"),
                entry("quote", "14:30:01"),
                entry("trade", "14:30:02"),
            ]
        );
    }

    /// Custom data of the tests: an imbalance of a book.
    #[nautilus_serialization::arrow_custom_data]
    #[nautilus_model::custom_data]
    pub struct Imbalance {
        pub value: f64,
        pub ts_event: UnixNanos,
        pub ts_init: UnixNanos,
    }

    /// Pins that Nautilus' data engine with a catalog answers a request
    /// naming a client it has not registered from the catalog alone,
    /// asking no client for what the catalog lacks
    /// (`dispatch_date_range_request`, `crates/data/src/engine/
    /// streaming.rs:489-497`): `custom_request` names `CATALOG`, which no
    /// node registers, for a custom series no source serves.
    ///
    /// No data client serves the catalog's custom series, GCZ26's
    /// imbalances: the request of it naming `CATALOG` is answered with the
    /// points in its window, and fails none of it.
    #[rstest]
    fn drift_a_custom_data_request_naming_the_catalog_is_answered_from_the_catalog_alone() {
        MessageBus::new(TraderId::from("TRADER-001"), UUID4::new(), None, None)
            .register_message_bus();
        nautilus_serialization::ensure_custom_data_registered::<Imbalance>();
        let at = |time: &str| -> Timestamp { format!("2026-08-10T{time}Z").parse().unwrap() };
        let data_type = DataType::new("Imbalance", None, Some("GCZ26.GLBX".to_string()));
        let points: Vec<CustomData> = [("13:00:00", 0.5), ("13:45:00", -0.25), ("14:15:00", 0.75)]
            .map(|(time, value)| {
                let ts = UnixNanos::from(at(time));
                CustomData::new(Arc::new(Imbalance::new(value, ts, ts)), data_type.clone())
            })
            .to_vec();
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        catalog
            .write_custom_data_batch(
                points,
                Some(at("12:00:00").into()),
                Some(at("14:30:00").into()),
                None,
            )
            .unwrap();
        let mut clock = VirtualClock::new();
        clock.advance_time(at("14:30:00").into(), true);
        let mut engine = DataEngine::new(
            Rc::new(RefCell::new(clock)),
            Rc::new(RefCell::new(Cache::default())),
            None,
        );
        engine.register_catalog(Box::new(catalog), None);
        let request_id = UUID4::new();
        let answers: Rc<RefCell<Vec<DataResponseEnvelope>>> = Rc::default();
        let saved = Rc::clone(&answers);
        msgbus::register_response_handler(
            &request_id,
            TypedHandler::from(move |envelope: &DataResponseEnvelope| {
                saved.borrow_mut().push(envelope.clone());
            }),
        );

        engine
            .execute_request(RequestCommand::Data(RequestCustomData::new(
                ClientId::new(CATALOG),
                data_type,
                Some(at("13:30:00")),
                Some(at("14:30:00")),
                None,
                request_id,
                at("14:30:00").into(),
                None,
            )))
            .unwrap();

        let answers = answers.borrow();
        let [answer] = answers.as_slice() else {
            panic!("{} answers", answers.len())
        };
        assert_eq!(answer.failure_reason(), None);
        let Some(DataResponse::Data(response)) = answer.response() else {
            panic!("{answer:?}")
        };
        let values: Vec<f64> = response
            .data
            .downcast_ref::<Vec<CustomData>>()
            .unwrap()
            .iter()
            .map(|point| {
                point
                    .data
                    .as_any()
                    .downcast_ref::<Imbalance>()
                    .unwrap()
                    .value
            })
            .collect();
        assert_eq!(values, [-0.25, 0.75]);
    }

    /// The warmup finishes on its group's response alone, another's,
    /// failed or not, ending nothing; its group failed is an error naming
    /// the reason.
    #[rstest]
    #[case::another_groups_failed(false, Some("lost"), Ok(false))]
    #[case::its_group_complete(true, None, Ok(true))]
    #[case::its_group_failed(true, Some("page timed out"), Err("page timed out"))]
    fn the_warmup_finishes_on_its_groups_response_alone(
        #[case] its_group: bool,
        #[case] failure: Option<&str>,
        #[case] finishes: Result<bool, &str>,
    ) {
        let group = UUID4::new();
        let correlation = if its_group { group } else { UUID4::new() };
        let response = DataResponseEnvelope::terminal(correlation, failure.map(str::to_string));

        match (finished(group, &response), finishes) {
            (Ok(finished), Ok(expected)) => assert_eq!(finished, expected),
            (Err(e), Err(reason)) => assert!(e.to_string().contains(reason), "{e}"),
            (finished, expected) => panic!("{finished:?}, not {expected:?}"),
        }
    }

    #[rstest]
    fn no_bar_types_keep_the_default_cache() {
        assert_eq!(cache_config(&[], DAY).unwrap(), CacheConfig::default());
    }

    /// The error names the warmup, -24h, which jiff's friendly format
    /// writes `24h ago`.
    #[rstest]
    fn a_negative_warmup_is_an_error() {
        let bar_types = [BarType::from("SXR8.IBIS-1-HOUR-MID-INTERNAL")];
        let err = cache_config(&bar_types, -DAY).unwrap_err().to_string();
        assert!(err.contains("24h ago"), "{err}");
    }

    #[rstest]
    fn a_bar_type_not_aggregated_on_time_is_an_error() {
        let bar_types = [BarType::from("SXR8.IBIS-100-TICK-MID-INTERNAL")];
        let err = cache_config(&bar_types, DAY).unwrap_err().to_string();
        assert!(err.contains("SXR8.IBIS-100-TICK-MID-INTERNAL"), "{err}");
    }

    #[rstest]
    fn the_cache_holds_the_warmup_of_the_shortest_bars_on_top_of_the_default() {
        let bar_types = [
            BarType::from("SXR8.IBIS-1-MINUTE-MID-INTERNAL"),
            BarType::from("EUNL.IBIS-1-HOUR-MID-INTERNAL"),
        ];
        // 720 hours of 1-minute bars: 43,200, and one spanning the start.
        let cache = cache_config(&bar_types, SignedDuration::from_hours(720)).unwrap();
        assert_eq!(cache.bar_capacity, 10_000 + 43_201);
    }

    #[rstest]
    fn a_warmup_the_cache_cannot_hold_is_an_error_naming_the_bars() {
        // 1-second bars over twelve days: 1,036,801 bars, beyond the
        // 1,000,000 a cache holds at most.
        let bar_types = [BarType::from("SXR8.IBIS-1-SECOND-MID-INTERNAL")];
        let err = cache_config(&bar_types, SignedDuration::from_hours(12 * 24)).unwrap_err();
        let err = format!("{err:#}");
        for part in ["SXR8.IBIS-1-SECOND-MID-INTERNAL", "1036801 bars"] {
            assert!(err.contains(part), "{err}");
        }
    }

    /// Bars built from quotes close on time; bars built from source bars
    /// wait 5 seconds, 5,000,000 microseconds, for the last one, and so do
    /// all bars of a strategy reading one of them. The delay a decision
    /// waits for (`bar_build_delay`) is the one a backtest, a dataset run
    /// and a live node build their bars with, none building a bar for a
    /// period without updates.
    #[rstest]
    #[case::of_quotes(&["SXR8.IBIS-1-HOUR-MID-INTERNAL"], SignedDuration::ZERO, 0)]
    #[case::of_source_bars(
        &["BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"],
        SignedDuration::from_secs(5),
        5_000_000
    )]
    #[case::of_both(
        &[
            "SXR8.IBIS-1-HOUR-MID-INTERNAL",
            "BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
        ],
        SignedDuration::from_secs(5),
        5_000_000
    )]
    fn bars_are_built_as_late_after_their_close_in_a_backtest_as_live(
        #[case] bar_types: &[&str],
        #[case] delay: SignedDuration,
        #[case] delay_micros: u64,
    ) {
        let bar_types: Vec<BarType> = bar_types.iter().map(|&b| BarType::from(b)).collect();

        let backtest = data_engine_config(&bar_types);
        let live = live_data_engine_config(&bar_types);

        assert_eq!(bar_build_delay(&bar_types), delay);
        assert_eq!(
            (backtest.time_bars_build_delay, live.time_bars_build_delay),
            (delay_micros, delay_micros)
        );
        assert!(!backtest.time_bars_build_with_no_updates);
        assert!(!live.time_bars_build_with_no_updates);
    }
}
