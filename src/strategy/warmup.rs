//! The warmup of strategies on bars Nautilus builds (internal time bars) on
//! a live node: a warmup request at start, which hydrates the strategy's
//! indicators before the live stream. A backtest or a dataset run makes no
//! request: its strategy only subscribes (`subscribe`), and its indicators
//! warm on the data replayed, from the run's start. Either way a strategy
//! decides once its indicators are initialized (`indicators_initialized`);
//! a model strategy, on a timer, also waits for a live warmup to end
//! (`Warmup::is_warming`).
//!
//! Owns Nautilus' protocol, so that no strategy names it. The request is
//! chosen from the bar type alone: a composite bar type is built from its
//! source bars, so their bars over `[now - warmup, now]` are requested
//! (`request_bars` of `BarType::composite`); any other bar type is built
//! from the ticks its price type names, as Nautilus builds it: LAST bars
//! from trades, so the trades of its instrument are requested
//! (`request_trades`), BID, ASK and MID bars from quotes, so its quotes
//! are (`request_quotes`), over the same window.
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
//! continuous future's request and takes none on custom data. Deltas are asked
//! at once: each window would replay the deltas from its day's start into a
//! snapshot of its own. The node runs on between windows, so features can be
//! initialized on the older windows alone; a decision waits for the end.
//!
//! Each request carries the params
//! `bar_types` (the data engine aggregates the response into those bars),
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
//! (`Warmup::on_data_response`). A response's data is that window's alone,
//! and the request's completion carries none.
//!
//! The requests form one request group (`DataActor::request_group`):
//! Nautilus calls `on_data_response` with the group once every request is
//! answered, and with the first failure if one failed (a page of the venue
//! lost, say), its data received up to then kept.
//!
//! Then the bars are subscribed to, one subscription per venue, with the
//! param `start_ns` = `now`: the live stream starts where the warmup ends.
//! Without it, the data engine of a node with a catalog starts a
//! subscription to external bars at the catalog's end, and IB sends every
//! bar since as live.
//!
//! The cache keeps at most `bar_capacity` bars per bar type and evicts the
//! oldest beyond it, so the live node running the strategy takes its cache
//! config from `Warmup::cache_config`, which holds the whole warmup of
//! every bar type the warmup builds.
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
//! `request_bars` of the bar type itself, and both it and their
//! subscription, one per bar type, carry the root's `continuous_future_*`
//! params. Nautilus builds such a request's bars into the cache only, with
//! neither `update_subscriptions` nor `skip_first_non_full_bar`, and
//! answers it with no bars; `continuous_bars` reads them once the group
//! ended.
//!
//! Ticks a strategy's features read as they are (`Warmup::reading`, a
//! model strategy's handle naming its features') are requested over the
//! same window in the same group, catalog first with `update_catalog`, a
//! root's on the contract standing for it at the start (`rolls::front`)
//! alone: `request_quotes`, `request_trades`, `request_book_depth` and
//! `request_book_deltas` (with the book type the roller subscribes to,
//! `rolls::DELTAS_BOOK_TYPE`, onto which Nautilus replays the deltas from
//! the day's start into the snapshot the response opens with). A series
//! some bars are built from is requested once, building them. The last
//! window ends at `now`, trimmed to it, and the roller, started right
//! after, subscribes to the series with `start_ns` its own `now`
//! (`subscription_params`). That keeps the data engine from starting the
//! subscription at the catalog's end; it filters nothing from the live
//! stream. A venue can resend its latest tick on a subscription, as IC and
//! IB do on quotes, and Nautilus forwards it, so the warmup's last tick can
//! come once more as the first live one, stamped as it was. Nautilus hands a
//! response's quotes and trades to the indicators registered for their
//! instrument, so a strategy's features registered for the contract
//! before the requests read them with no further call; it routes depth
//! and deltas to no indicator, so the
//! warmup hands them to its book indicators (`BookIndicator`), the deltas
//! in the batches the live stream carries (`batches`). Ticks arriving on
//! the live stream while a venue's part of a response is pending reach the
//! features before that part: Nautilus buffers no ticks for indicators as
//! it aggregates bars across a request's end.
//!
//! Custom data the features read (a raw feature's, a root's on its
//! contract's data type) is requested in the same group over the same
//! window with `request_data`, naming the client `CATALOG`, which no node
//! registers: no data client serves custom data, and Nautilus answers the
//! request from the catalog alone. Nautilus routes custom data to no
//! indicator either, so the warmup hands the response's points to its
//! custom data indicators (`CustomDataIndicator`), oldest first.
//!
//! A strategy holds a `Warmup`, which runs these calls, and delegates its
//! hooks to it; a model strategy's `ModelHandle` builds and owns it. The
//! functions are its parts: `start` from `on_start`,
//! `Warmup::on_historical_book_depth`, `on_historical_book_deltas` and
//! `on_historical_data` from the hooks of the same names, and
//! `Warmup::on_data_response`, or `finished` alone, from `on_data_response`,
//! then, on bars on a root, `continuous_bars`; in a backtest or a dataset run
//! `subscribe` from `on_start` alone.

use std::{
    any::Any,
    cell::{Cell, RefCell},
    collections::{BTreeMap, HashMap},
    fmt::Debug,
    rc::Rc,
};

use anyhow::{Context, bail, ensure};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, DataActorNative, indicators::SharedActorIndicator},
    cache::CacheConfig,
    messages::data::DataResponseEnvelope,
};
use nautilus_core::{Params, UUID4, UnixNanos};
use nautilus_data::engine::config::DataEngineConfig;
use nautilus_live::config::LiveDataEngineConfig;
use nautilus_model::{
    data::{
        Bar, BarType, CustomData, DataType, NautilusDataType, OrderBookDelta, OrderBookDeltas,
        OrderBookDepth,
    },
    enums::RecordFlag,
    identifiers::{ClientId, Venue},
};
use serde_json::{Value, json};

use crate::{
    rolls::{ContinuousBars, DELTAS_BOOK_TYPE, front},
    strategy::{DataRequirements, Source, TickSeries, bar_sources},
    time_range,
};

/// The window a warmup is asked in (see `time_range`): under a day, as IB's
/// client asks a duration of a day or more in days and IB counts those as
/// sessions, the current one included, so that 72 hours started in a session
/// lost the oldest of them (841 of 2,791 bars of SXR8, measured 2026-11-04).
/// Under a day it asks in seconds. Venues page within a window.
const WINDOW: SignedDuration = SignedDuration::from_hours(23);

/// The data client a warmup's request of custom data names: no data client
/// serves custom data, and Nautilus' data engine answers a request naming
/// no registered client from its catalogs alone, planning no client leg
/// for what they lack (`dispatch_date_range_request` in
/// `data/src/engine/streaming.rs`: `if !has_client {
/// missing_intervals.clear(); }`), and an empty response where they hold
/// nothing. A live node registers its catalog (`live::node`); with none,
/// the engine would send the request to the client and fail it.
const CATALOG: &str = "CATALOG";

/// What a warmup hands the book depth and deltas of its responses to,
/// which Nautilus routes to no indicator (`ActorIndicator` reads quotes,
/// trades and bars alone): a model strategy's features.
pub trait BookIndicator {
    /// Reads `depth`, a snapshot of the warmup, oldest first.
    fn handle_depth(&self, depth: &OrderBookDepth);

    /// Reads `deltas`, a batch of the warmup as the live stream carries
    /// it, oldest first.
    fn handle_deltas(&self, deltas: &OrderBookDeltas);
}

/// What a warmup hands the custom data of its responses to, which Nautilus
/// routes to no indicator: a model strategy's features.
pub trait CustomDataIndicator {
    /// Reads `data`, a point of the warmup, oldest first.
    fn handle_data(&self, data: &CustomData);
}

/// A strategy's warmup, built from its data requirements for a live node
/// (`live`) or a replay (`replayed`, a backtest or a dataset run): the one
/// owner of the protocol above, so that a strategy holds one and its hooks
/// delegate to it. Nautilus delivers historical data only to the
/// strategy's own hooks, so a strategy calls `start` from `on_start`,
/// `on_data_response` from `on_data_response`, and
/// `on_historical_book_depth`, `on_historical_book_deltas` and
/// `on_historical_data` from theirs.
///
/// Live, `start` requests the warmup of the bar types the strategy reads,
/// of those the strategy's harness needs warm (`building`, the barrier's
/// ATR's) and of the tick series its features read (`reading`), and
/// subscribes to the bars from the warmup's end; the bars each request built
/// go to the indicators the warmup feeds (`feeding`, `building`), oldest
/// first, when it completes, each response's depth and deltas to its book
/// indicators (`feeding_books`), its custom data to its custom data
/// indicators (`feeding_custom_data`).
/// On a replay `start` subscribes to the strategy's bar types alone: the
/// harness subscribes to its own.
/// The strategies read no bars on a root, so a warmup has no continuous
/// bars. A live node sizes its cache from `cache_config`, which holds the
/// warmup of every bar type the warmup builds.
pub struct Warmup {
    start: Start,
    /// How far back the live request starts: the requirements' warmup.
    length: SignedDuration,
    /// The bar types the strategy reads.
    bar_types: Vec<BarType>,
    /// The bar types the harness needs warm beyond those.
    harness: Vec<BarType>,
    /// The tick series warmed as they are, of roots or plain instruments.
    ticks: Vec<TickSeries>,
    /// What the warmup's bars are handed to.
    indicators: Vec<SharedActorIndicator>,
    /// What the warmup's book depth and deltas are handed to.
    books: Vec<Rc<dyn BookIndicator>>,
    /// What the warmup's custom data is handed to.
    custom_data: Vec<Rc<dyn CustomDataIndicator>>,
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
    /// Requests the warmup, then subscribes from its end: a live node.
    Request,
    /// Subscribes alone: a replay, whose indicators warm on its data.
    Subscribe,
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
        Self::of(requirements, Start::Subscribe)
    }

    fn of(requirements: &DataRequirements, start: Start) -> Self {
        Self {
            start,
            length: requirements.warmup,
            bar_types: requirements.bar_types.clone(),
            harness: Vec::new(),
            ticks: Vec::new(),
            indicators: Vec::new(),
            books: Vec::new(),
            custom_data: Vec::new(),
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
    /// goes on with them, the bar spanning the warmup's end once.
    #[must_use]
    pub fn building(mut self, bar_type: BarType, indicator: SharedActorIndicator) -> Self {
        if !self.bar_types.contains(&bar_type) && !self.harness.contains(&bar_type) {
            self.harness.push(bar_type);
        }
        self.feeding(indicator)
    }

    /// The warmup requesting the history of `ticks` too, quotes, trades,
    /// book depth, deltas or custom data of roots or plain instruments,
    /// each on the contract standing for its instrument at the start
    /// (`rolls::front`): the series a strategy's features read, which its
    /// roller subscribes to from the warmup's end. Quotes and trades reach
    /// the indicators Nautilus has registered for them; depth and deltas
    /// reach those `feeding_books` names, custom data, requested from the
    /// catalog alone (see `CATALOG`), those `feeding_custom_data` names.
    #[must_use]
    pub fn reading(mut self, ticks: Vec<TickSeries>) -> Self {
        self.ticks = ticks;
        self
    }

    /// The warmup handing the book depth and deltas of its responses to
    /// `books` too.
    #[must_use]
    pub fn feeding_books(mut self, books: Rc<dyn BookIndicator>) -> Self {
        self.books.push(books);
        self
    }

    /// The warmup handing the custom data of its responses to `custom_data`
    /// too.
    #[must_use]
    pub fn feeding_custom_data(mut self, custom_data: Rc<dyn CustomDataIndicator>) -> Self {
        self.custom_data.push(custom_data);
        self
    }

    /// Starts the warmup of `actor`, from its `on_start`: live, requests it
    /// as a new request group and subscribes to the bars it builds from
    /// its end (see `start`); on a replay, subscribes to the strategy's
    /// bar types (see `subscribe`).
    ///
    /// # Errors
    ///
    /// Returns an error if a live request fails (see `start`).
    pub fn start<A>(&self, actor: &mut A) -> anyhow::Result<()>
    where
        A: DataActor + DataActorNative + Debug + 'static,
    {
        let continuous = ContinuousBars::default();
        match self.start {
            Start::Request => {
                self.group.set(UUID4::new());
                self.warming.set(true);
                let requested = start(
                    actor,
                    &self.built(),
                    &self.ticks,
                    self.length,
                    &continuous,
                    self.group.get(),
                )?;
                if requested.is_empty() {
                    self.warming.set(false);
                }
                *self.building.borrow_mut() = requested.into_iter().collect();
                Ok(())
            }
            Start::Subscribe => {
                subscribe(actor, &self.bar_types, &continuous);
                Ok(())
            }
        }
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
        if *response.correlation_id() == self.group.get() {
            self.warming.set(false);
        }
        if response.completed() {
            let built = self.building.borrow_mut().remove(response.correlation_id());
            for bar in built_bars(actor, &built.unwrap_or_default()) {
                for indicator in &self.indicators {
                    indicator.handle_bar(&bar)?;
                }
            }
        }
        if finished(self.group.get(), response)? {
            log::info!("Warmup complete");
        }
        Ok(())
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

    /// Whether a live start's requests are not all answered yet: its windows
    /// reach the indicators one at a time, with the node running between
    /// them, so what they hold until then is no state to decide on.
    #[must_use]
    pub fn is_warming(&self) -> bool {
        self.warming.get()
    }

    /// Hands the book depth of `response`, a response to the warmup's
    /// request, to the book indicators it feeds, oldest first, from the
    /// actor's `on_historical_book_depth`.
    pub fn on_historical_book_depth(&self, response: &[OrderBookDepth]) {
        for depth in response {
            for books in &self.books {
                books.handle_depth(depth);
            }
        }
    }

    /// Hands the book deltas of `response`, a response to the warmup's
    /// request, to the book indicators it feeds, in the batches the live
    /// stream carries (see `batches`), oldest first, from the actor's
    /// `on_historical_book_deltas`.
    pub fn on_historical_book_deltas(&self, response: &[OrderBookDelta]) {
        for deltas in batches(response) {
            for books in &self.books {
                books.handle_deltas(&deltas);
            }
        }
    }

    /// Hands the custom data of `response`, a response to the warmup's
    /// request, to the custom data indicators it feeds, oldest first, from
    /// the actor's `on_historical_data`. Nautilus answers `request_data`
    /// with a batch (`Vec<CustomData>`); any other payload is no response
    /// of the warmup's.
    pub fn on_historical_data(&self, response: &dyn Any) {
        let Some(data) = response.downcast_ref::<Vec<CustomData>>() else {
            return;
        };
        for point in data {
            for custom_data in &self.custom_data {
                custom_data.handle_data(point);
            }
        }
    }

    /// The cache config of a live node running the warmup: sized for the
    /// warmup of every bar type it builds (see `cache_config`).
    ///
    /// # Errors
    ///
    /// As `cache_config`.
    pub fn cache_config(&self) -> anyhow::Result<CacheConfig> {
        cache_config(&self.built(), self.length)
    }

    /// The bar types a live warmup builds: the strategy's, then the
    /// harness's.
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
            .field("books", &self.books.len())
            .field("custom_data", &self.custom_data.len())
            .field("group", &self.group)
            .field("warming", &self.warming)
            .field("building", &self.building)
            .finish()
    }
}

/// Requests the warmup of `bar_types`, internal bar types, and of `ticks`,
/// over `[now - warmup, now]` as the request group `group`, then subscribes
/// `actor` to the bars from `now`; the ticks' subscriptions are their
/// roller's. The bars on a root with continuous bars in `continuous` are
/// its continuous future, requested per bar type with
/// their `continuous_future_*` params; the others are requested per series
/// they are built from (see `requests`): the source bars of a composite bar
/// type, the trades of the instrument of LAST bars, the quotes of the
/// instrument of BID, ASK and MID bars; each tick series is requested on
/// the contract standing for its instrument now, once with the bars built
/// from it if any, custom data from the catalog alone (see `CATALOG`). The
/// warmup of each request arrives a window at a time in
/// `on_historical_bars`, `on_historical_trades`, `on_historical_quotes` or
/// `on_historical_book_depth`, at once in `on_historical_book_deltas` or
/// `on_historical_data`; the end of each request and of the group in
/// `on_data_response`, where `finished` tells the group's. `group` is the
/// caller's, stored before the call: a group whose requests are all answered
/// at once ends before this returns. No bar types and no ticks make no
/// request and no group, which `DataActor::request_group` refuses empty.
/// Returns each request's id with the bar types it builds, none for a
/// continuous future's, read once the group ended (`continuous_bars`).
///
/// # Errors
///
/// Returns an error if a request fails. A composite bar type's source bars
/// are external, as the config's load requires (see
/// `strategy::DataConfig`): they are requested, not built.
pub fn start<A>(
    actor: &mut A,
    bar_types: &[BarType],
    ticks: &[TickSeries],
    warmup: SignedDuration,
    continuous: &ContinuousBars,
    group: UUID4,
) -> anyhow::Result<Vec<(UUID4, Vec<BarType>)>>
where
    A: DataActor + DataActorNative + Debug + 'static,
{
    let now = actor.clock().utc_now();
    let start = now.checked_sub(warmup)?;
    let windows = time_range::params(start, now, WINDOW);
    let (continuous_bars, others): (Vec<BarType>, Vec<BarType>) = bar_types
        .iter()
        .partition(|bar_type| continuous.params(bar_type.instrument_id()).is_some());
    let mut requested = Vec::new();
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
        requested.push((id, vec![]));
    }
    for (source, built) in requests(&others, ticks, now) {
        let params = Some(request_params(built.iter(), windows.clone()));
        let id = match source {
            Source::Bars(source) => {
                actor.request_bars(source, Some(start), Some(now), None, None, params)?
            }
            Source::Ticks(tick) => match tick.data {
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
                // The book type the roller subscribes to: Nautilus replays
                // the deltas before the start onto a book of it, into the
                // snapshot the response starts with.
                NautilusDataType::OrderBookDelta => actor.request_book_deltas(
                    tick.instrument_id,
                    Some(start),
                    Some(now),
                    None,
                    None,
                    Some(request_params(
                        built.iter(),
                        [("book_type".to_string(), json!(DELTAS_BOOK_TYPE.to_string()))],
                    )),
                )?,
                // From the catalog alone (see `CATALOG`), with no params:
                // no client answers to write back, no bars built from it.
                NautilusDataType::Custom { type_name } => actor.request_data(
                    DataType::new(&type_name, None, Some(tick.instrument_id.to_string())),
                    ClientId::new(CATALOG),
                    Some(start),
                    Some(now),
                    None,
                    None,
                )?,
                data => unreachable!("{data} is not read by a tick feature"),
            },
        };
        requested.push((id, built));
    }
    if !requested.is_empty() {
        let ids = requested.iter().map(|(id, _)| *id).collect();
        actor.request_group(ids, Some(group))?;
    }
    subscribe_from(actor, bar_types, continuous, UnixNanos::from(now));
    Ok(requested)
}

/// Subscribes `actor` to `bar_types`, internal bar types, from now, with
/// no warmup request: in a backtest or a dataset run, whose strategy warms
/// up on the data replayed, and after `start`'s requests live, the live
/// stream starting where the warmup ends. The bars on a root with
/// continuous bars in `continuous` are subscribed per bar type with their
/// `continuous_future_*` params, the others per venue.
pub fn subscribe<A>(actor: &mut A, bar_types: &[BarType], continuous: &ContinuousBars)
where
    A: DataActor + DataActorNative + Debug + 'static,
{
    let now = UnixNanos::from(actor.clock().utc_now());
    subscribe_from(actor, bar_types, continuous, now);
}

/// Subscribes `actor` to `bar_types` as `subscribe` does, from `now`.
fn subscribe_from<A>(
    actor: &mut A,
    bar_types: &[BarType],
    continuous: &ContinuousBars,
    now: UnixNanos,
) where
    A: DataActor + DataActorNative + Debug + 'static,
{
    let (continuous_bars, others): (Vec<BarType>, Vec<BarType>) = bar_types
        .iter()
        .partition(|bar_type| continuous.params(bar_type.instrument_id()).is_some());
    // One subscription per continuous bar type: Nautilus takes the params
    // of a continuous future per bar type.
    for bar_type in continuous_bars {
        let params = continuous.params(bar_type.instrument_id());
        actor.subscribe_bars(
            bar_type,
            None,
            Some(subscription_params(now, params.into_iter().flatten())),
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
        actor.subscribe_bars(venue_bar_types, None, Some(subscription_params(now, [])));
    }
}

/// The warmup requests of `bar_types`, internal bar types on no root, and
/// of `ticks` at `now`: one per series the bars are built from, as
/// Nautilus' data engine builds them (see `strategy::catalog_series`), with
/// the bar types built from it, in the order the series first come; then
/// one per tick series on the contract standing for its instrument at
/// `now` (`rolls::front`) that no bar is built from, building none.
fn requests(
    bar_types: &[BarType],
    ticks: &[TickSeries],
    now: Timestamp,
) -> Vec<(Source, Vec<BarType>)> {
    let mut requests: Vec<(Source, Vec<BarType>)> = Vec::new();
    for (source, bar_type) in bar_sources(bar_types).zip(bar_types) {
        match requests.iter_mut().find(|(of, _)| *of == source) {
            Some((_, built)) => built.push(*bar_type),
            None => requests.push((source, vec![*bar_type])),
        }
    }
    for tick in ticks {
        let source = Source::Ticks(TickSeries {
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

/// The params of the subscription to the data warmed up to `now`, bars or
/// ticks: the live stream starts there (`start_ns`). Without it, the data
/// engine of a node with a catalog starts a subscription to quotes, trades,
/// external bars or custom data at the catalog's end. With `others`: the
/// `continuous` params of their root, if bars on one.
pub(crate) fn subscription_params(
    now: UnixNanos,
    others: impl IntoIterator<Item = (String, Value)>,
) -> Params {
    Params::new(
        [("start_ns".to_string(), json!(now.as_u64()))]
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
/// Returns an error naming the reason if `response` ends the group and a
/// request of it failed: the warmup holds what the requests got before.
pub fn finished(group: UUID4, response: &DataResponseEnvelope) -> anyhow::Result<bool> {
    if *response.correlation_id() != group {
        return Ok(false);
    }
    match response.failure_reason() {
        Some(reason) => bail!("the warmup is incomplete: {reason}"),
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
    ensure!(
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
    use nautilus_data::engine::DataEngine;
    use nautilus_model::{
        data::{BookOrder, QuoteTick, TradeTick, stubs::stub_depth10},
        enums::{BookAction, OrderSide, RecordFlag},
        identifiers::{InstrumentId, StrategyId, TraderId},
        types::{Price, Quantity},
    };
    use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
    use nautilus_portfolio::portfolio::Portfolio;
    use nautilus_trading::{
        nautilus_strategy,
        strategy::{StrategyConfig, StrategyCore},
    };

    use super::*;
    use crate::strategy::TickSeries;

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

    /// The requests of `commands`, in order: each its series, its id and its
    /// params.
    fn sent_requests(
        commands: &TypedIntoMessageSavingHandler<DataCommand>,
    ) -> Vec<(Source, UUID4, Option<Params>)> {
        let ticks = |data, instrument_ids: &[InstrumentId]| {
            let [instrument_id] = instrument_ids else {
                panic!("one instrument")
            };
            Source::Ticks(TickSeries {
                data,
                instrument_id: *instrument_id,
            })
        };
        commands
            .get_messages()
            .into_iter()
            .filter_map(|command| match command {
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
                        (Source::Bars(*bar_type), request.params)
                    }
                    other => panic!("unexpected {other:?}"),
                };
                (source, id, params)
            })
            .collect()
    }

    /// The requests of `commands`, in order: each its series and the bar
    /// types it builds (its `bar_types` param).
    fn requested(
        commands: &TypedIntoMessageSavingHandler<DataCommand>,
    ) -> Vec<(Source, Vec<BarType>)> {
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
            warmup: DAY,
        }
    }

    /// The harness's bars are built after the strategy's, once, and not
    /// again if the strategy reads them: both are MID bars of SXR8, built
    /// by the one request of its quotes.
    #[test]
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
                Source::Ticks(tick(NautilusDataType::QuoteTick, "SXR8.IBIS")),
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

    /// A live start requests the history of the ticks the strategy reads
    /// too, a root's on the contract standing for it then: GC rolls into
    /// GCZ26 on 2026-07-29 and out of it on 2026-11-26 (`rolls/README.md`),
    /// so on 2026-08-10 the root's trades and depth are GCZ26's, requested
    /// once beside GCZ26's own trades. IC's gold quotes, which build the
    /// strategy's bars, are requested once, building them; the ticks no
    /// bar is built from build none.
    #[test]
    fn a_live_warmup_requests_a_roots_ticks_on_its_front_contract() {
        let bars = BarType::from("XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL");
        let warmup =
            Warmup::live(&reading(&["XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL"])).reading(vec![
                tick(NautilusDataType::TradeTick, "GC.GLBX"),
                tick(NautilusDataType::OrderBookDepth, "GC.GLBX"),
                tick(NautilusDataType::QuoteTick, "XAUUSD.ICMARKETS"),
                tick(NautilusDataType::TradeTick, "GCZ26.GLBX"),
                tick(NautilusDataType::OrderBookDelta, "GC.GLBX"),
            ]);
        let (mut probe, commands) = registered();

        warmup.start(&mut probe).unwrap();

        assert_eq!(
            requested(&commands),
            [
                (
                    Source::Ticks(tick(NautilusDataType::QuoteTick, "XAUUSD.ICMARKETS")),
                    vec![bars],
                ),
                (
                    Source::Ticks(tick(NautilusDataType::TradeTick, "GCZ26.GLBX")),
                    vec![],
                ),
                (
                    Source::Ticks(tick(NautilusDataType::OrderBookDepth, "GCZ26.GLBX")),
                    vec![],
                ),
                (
                    Source::Ticks(tick(NautilusDataType::OrderBookDelta, "GCZ26.GLBX")),
                    vec![],
                ),
            ]
        );
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
    #[test]
    fn the_warmup_hands_book_deltas_over_in_the_batches_the_live_stream_carries() {
        let books = Rc::new(Books::default());
        let warmup = Warmup::live(&reading(&[])).feeding_books(Rc::clone(&books) as _);
        let last = RecordFlag::F_LAST as u8;

        warmup.on_historical_book_deltas(&[
            delta(1, 0),
            delta(2, last),
            delta(3, last),
            delta(4, 0),
        ]);

        let batches: Vec<Vec<u64>> = books
            .deltas
            .borrow()
            .iter()
            .map(|batch| batch.deltas.iter().map(|d| d.sequence).collect())
            .collect();
        assert_eq!(batches, [vec![1, 2], vec![3], vec![4]]);
    }

    /// Nautilus routes no depth to indicators: the warmup hands each
    /// snapshot of a response over, in order.
    #[test]
    fn the_warmup_hands_book_depth_over_in_order() {
        let books = Rc::new(Books::default());
        let warmup = Warmup::live(&reading(&[])).feeding_books(Rc::clone(&books) as _);
        let depth = |sequence| OrderBookDepth {
            sequence,
            ..stub_depth10()
        };

        warmup.on_historical_book_depth(&[depth(7), depth(8)]);

        let sequences: Vec<u64> = books.depths.borrow().iter().map(|d| d.sequence).collect();
        assert_eq!(sequences, [7, 8]);
    }

    /// The cache holds the warmup of the harness's bars too: 1,440
    /// 1-minute bars over a day, and one spanning the start.
    #[test]
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
    #[test]
    fn each_bar_type_warms_up_on_what_its_price_type_builds_it_from() {
        let hour_last = BarType::from("ESM24.CME-1-HOUR-LAST-INTERNAL");
        let minute_last = BarType::from("ESM24.CME-1-MINUTE-LAST-INTERNAL");
        let hour_mid = BarType::from("ESM24.CME-1-HOUR-MID-INTERNAL");
        let composite = BarType::from("ESM24.CME-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL");
        let ticks = |data, instrument_id: &str| {
            Source::Ticks(TickSeries {
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
                    Source::Bars(BarType::from("ESM24.CME-1-MINUTE-LAST-EXTERNAL")),
                    vec![composite]
                ),
            ]
        );
    }

    /// A request's completion hands over the bars of the bar types it built
    /// alone, each once, under the standard type a node caches them by: the
    /// composite's under its type without its source. Another request's
    /// completion hands over its own.
    #[test]
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
        let id_of = |wanted: &Source| {
            sent_requests(&commands)
                .into_iter()
                .find_map(|(source, id, _)| (source == *wanted).then_some(id))
                .unwrap()
        };
        let quotes = id_of(&Source::Ticks(tick(
            NautilusDataType::QuoteTick,
            "ESM24.CME",
        )));
        let minutes = id_of(&Source::Bars(BarType::from(
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

    /// The bars, quotes, trades and depth of a day are asked in windows of
    /// 23 hours, the two a day can hold: under a day, so that IB counts no
    /// sessions. Deltas are asked at once: each window would replay the
    /// deltas from its day's start into a snapshot of its own.
    #[test]
    fn every_series_but_deltas_is_asked_in_windows_under_a_day() {
        let warmup = Warmup::live(&reading(&[
            "XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL",
            "ESM24.CME-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
        ]))
        .reading(vec![
            tick(NautilusDataType::TradeTick, "GCZ26.GLBX"),
            tick(NautilusDataType::OrderBookDepth, "GCZ26.GLBX"),
            tick(NautilusDataType::OrderBookDelta, "GCZ26.GLBX"),
        ]);
        let (mut probe, commands) = registered();

        warmup.start(&mut probe).unwrap();

        for (source, _, params) in sent_requests(&commands) {
            let params = params.expect("params");
            let windows = params.get("durations_seconds").cloned();
            let generator = params.contains_key("time_range_generator");
            if source == Source::Ticks(tick(NautilusDataType::OrderBookDelta, "GCZ26.GLBX")) {
                assert_eq!((windows, generator), (None, false), "{source:?}");
            } else {
                assert_eq!(windows, Some(json!([82_800.0, 82_800.0])), "{source:?}");
                assert!(generator, "{source:?}");
            }
        }
    }

    /// A live start's warmup goes on until its group ends, whatever its
    /// requests answered by then; a replay's makes no request and so none.
    #[test]
    fn a_warmup_goes_on_until_its_group_ends() {
        let warmup = Warmup::live(&reading(&["SXR8.IBIS-1-HOUR-MID-INTERNAL"]));
        let (mut probe, commands) = registered();
        warmup.start(&mut probe).unwrap();
        let (_, request, _) = sent_requests(&commands).remove(0);

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

        let replayed = Warmup::replayed(&reading(&["SXR8.IBIS-1-HOUR-MID-INTERNAL"]));
        let (mut probe, _) = registered();
        replayed.start(&mut probe).unwrap();
        assert!(!replayed.is_warming());
    }

    /// Custom data of the tests: an imbalance of a book.
    #[nautilus_serialization::arrow_custom_data]
    #[nautilus_model::custom_data]
    pub struct Imbalance {
        pub value: f64,
        pub ts_event: UnixNanos,
        pub ts_init: UnixNanos,
    }

    /// No data client serves custom data: Nautilus' data engine, with a
    /// catalog and no data client, answers a request of it naming
    /// `CATALOG` from the catalog alone, with the points in its window,
    /// and fails none of it.
    #[test]
    fn a_custom_data_request_naming_the_catalog_is_answered_from_the_catalog_alone() {
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

    #[test]
    fn the_warmup_finishes_on_its_groups_response_alone() {
        let group = UUID4::new();

        let other = DataResponseEnvelope::terminal(UUID4::new(), Some("lost".to_string()));
        let complete = DataResponseEnvelope::terminal(group, None);

        assert!(!finished(group, &other).unwrap());
        assert!(finished(group, &complete).unwrap());
    }

    #[test]
    fn a_failed_warmup_is_an_error_naming_the_reason() {
        let group = UUID4::new();
        let failed = DataResponseEnvelope::terminal(group, Some("page timed out".to_string()));

        let err = finished(group, &failed).unwrap_err().to_string();

        assert!(err.contains("page timed out"), "{err}");
    }

    #[test]
    fn no_bar_types_keep_the_default_cache() {
        assert_eq!(cache_config(&[], DAY).unwrap(), CacheConfig::default());
    }

    #[test]
    fn a_negative_warmup_is_an_error() {
        let bar_types = [BarType::from("SXR8.IBIS-1-HOUR-MID-INTERNAL")];
        let err = cache_config(&bar_types, -DAY).unwrap_err().to_string();
        assert!(err.contains("negative warmup"), "{err}");
    }

    #[test]
    fn a_bar_type_not_aggregated_on_time_is_an_error() {
        let bar_types = [BarType::from("SXR8.IBIS-100-TICK-MID-INTERNAL")];
        let err = cache_config(&bar_types, DAY).unwrap_err().to_string();
        assert!(err.contains("SXR8.IBIS-100-TICK-MID-INTERNAL"), "{err}");
    }

    #[test]
    fn the_cache_holds_the_warmup_of_the_shortest_bars_on_top_of_the_default() {
        let bar_types = [
            BarType::from("SXR8.IBIS-1-MINUTE-MID-INTERNAL"),
            BarType::from("EUNL.IBIS-1-HOUR-MID-INTERNAL"),
        ];
        // 720 hours of 1-minute bars: 43,200, and one spanning the start.
        let cache = cache_config(&bar_types, SignedDuration::from_hours(720)).unwrap();
        assert_eq!(cache.bar_capacity, 10_000 + 43_201);
    }

    #[test]
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

    #[test]
    fn bars_built_from_quotes_close_on_time_in_a_backtest_as_live() {
        let bar_types = [BarType::from("SXR8.IBIS-1-HOUR-MID-INTERNAL")];
        let backtest = data_engine_config(&bar_types);
        let live = live_data_engine_config(&bar_types);
        assert_eq!(
            (backtest.time_bars_build_delay, live.time_bars_build_delay),
            (0, 0)
        );
        assert!(!backtest.time_bars_build_with_no_updates);
        assert!(!live.time_bars_build_with_no_updates);
    }

    #[test]
    fn bars_built_from_source_bars_wait_for_the_last_one_in_a_backtest_as_live() {
        let bar_types = [BarType::from(
            "BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
        )];
        let backtest = data_engine_config(&bar_types);
        let live = live_data_engine_config(&bar_types);
        assert_eq!(
            (backtest.time_bars_build_delay, live.time_bars_build_delay),
            (5_000_000, 5_000_000)
        );
        assert!(!backtest.time_bars_build_with_no_updates);
        assert!(!live.time_bars_build_with_no_updates);
    }

    /// The delay a decision waits for is the one a backtest, a dataset run
    /// and a live node build its bars with.
    #[test]
    fn the_bar_build_delay_is_every_nodes() {
        for bar_types in [
            vec![
                BarType::from("SXR8.IBIS-1-HOUR-MID-INTERNAL"),
                BarType::from("BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"),
            ],
            vec![BarType::from("SXR8.IBIS-1-HOUR-MID-INTERNAL")],
        ] {
            let delay = u64::try_from(bar_build_delay(&bar_types).as_micros()).unwrap();
            assert_eq!(
                data_engine_config(&bar_types).time_bars_build_delay,
                delay,
                "{bar_types:?}"
            );
            assert_eq!(
                live_data_engine_config(&bar_types).time_bars_build_delay,
                delay,
                "{bar_types:?}"
            );
        }
    }
}
