//! Warms up a strategy's indicators on a live node, before the live stream starts.
//!
//! This covers strategies on bars Nautilus builds (internal time bars). A warmup request at start
//! hydrates the indicators. A backtest or a dataset run makes no request, and its indicators warm
//! on the replayed data. The warmup subscribes to nothing. The strategy subscribes
//! (`strategy::subscriptions`) from the time `Warmup::start` returns. A strategy decides once its
//! indicators are initialized and a live warmup has ended (`Warmup::is_warming`).
//!
//! The module owns Nautilus' protocol, so that no strategy names it. The request depends on the
//! bar type:
//! - Composite: requests its source bars over `[now - warmup, now]` (`request_bars` of
//!   `BarType::composite`).
//! - External, built by its venue (a conversion pair's BID and ASK bars on IB): requests the bar
//!   type itself (`request_bars`) and builds nothing, as Nautilus aggregates into internal bar
//!   types alone.
//! - Any other: requests the ticks its price type names. LAST bars come from trades
//!   (`request_trades`). BID, ASK and MID bars come from quotes (`request_quotes`).
//!
//! The window ends at a given `now` and is not left open. The data engine trims a response to its
//! request's bounds (`DataResponse::trim_to_bounds`), so the warmup ends on the last bar closed by
//! `now`. Left open, IB's answer also holds the bar in progress. That bar is partial (stamped after
//! `now`), and the live stream sends it again, complete.
//!
//! Long requests come in time-range windows of 23 hours (`WINDOW`, `time_range`). A catalog answers
//! what it holds. One aggregator builds a request's bars across its windows. Nautilus ignores the
//! windows of a continuous future's request and takes none on custom data. If a catalog holds the
//! start of a deltas request's day, the request opens with a snapshot of the book at the request's
//! start. Later windows continue that book with none (`time_range_book_deltas_child_params` in
//! `crates/data/src/engine/time_range.rs`). The node runs on between windows, so features can be
//! initialized on older windows alone. A decision waits for the end.
//!
//! Each request carries these params:
//! - `bar_types`: the bars the response is aggregated into. It is empty for an external bar type.
//! - `update_subscriptions`: the aggregator goes on with the live data, so the bar spanning the
//!   warmup end comes out once and complete.
//! - `skip_first_non_full_bar`: drops the oldest bar, which misses the data before
//!   `now - warmup`.
//! - `update_catalog`: with a catalog registered, the engine answers from it first and asks the
//!   venue only for the rest. It writes back the answers a data client certifies complete, as IC's
//!   does.
//!
//! The bars a request builds reach only the cache. They carry their standard type, and a
//! composite's has no source. The data engine builds a window's bars before it hands the response
//! on (`handle_time_range_pipeline_payload` in `crates/data/src/engine/time_range.rs`). It builds
//! the request's last bars before the completion (`cleanup_request_bar_aggregators` in
//! `crates/data/src/engine/bar.rs`). So at each response (`Warmup::on_data_response`) the warmup
//! reads the bars built since the last response, each once, and hands them on with the ticks. A bar
//! closing after `now` (`ts_init`) is one of the live data the request held
//! (`finish_historical_bar_aggregator`). It is live, so it goes among the live ticks the hold
//! holds (`Hold::built_live`). An external bar type's request hands its indicators nothing, as no
//! indicator reads external bars. A response's data is its window's alone, and the completion
//! carries none.
//!
//! A live start first asks for the definition of each instrument the cache lacks
//! (`request_instrument`). It asks the client the venue's row names
//! (`venues::live_definitions_client`): its broker's, else its live data source's. The data engine
//! answers from a catalog before asking that client (`dispatch_instrument_request` in
//! `data/src/engine/streaming.rs`). It also takes an actor's commands in order through one queue
//! (`DataEngine.queue_execute`). So the definition is cached before the requests and
//! subscriptions, and LSE's client, which publishes no definitions, finds it there. Definitions the
//! broker clients cached on connecting are not asked. A custom series asks none.
//!
//! The requests form one request group (`DataActor::request_group`). Nautilus calls
//! `on_data_response` with the group once every request is answered, or with the first failure. It
//! keeps the data received up to then.
//!
//! The strategy subscribes from `now` to every bar type the warmup builds, including the
//! harness's (the target's ATR's, `model::targets`), so their aggregators go on from the warmup's.
//! The cache keeps at most `bar_capacity` bars per bar type and evicts the oldest. So the live node
//! takes its cache config from `Warmup::cache_config`, which holds the whole warmup of every bar
//! type.
//!
//! An hour with no tick or source bar means the market is closed, so the data engine builds no bar
//! for it. Nautilus' default builds one, flat at the last close, which a session on the time of
//! day alone would trade on. So the node takes its data engine config from `data_engine_config`
//! (backtest) or `live_data_engine_config` (live). Both turn `time_bars_build_with_no_updates` off
//! and build time bars `bar_build_delay` after their close.
//!
//! Bars on a futures root (`GC.GLBX`) are Nautilus' continuous bars. They are built from the trades
//! of the contract standing for the root and adjusted across its rolls (see
//! `rolls::ContinuousBars`). The warmup requests them as `request_bars` of the bar type itself,
//! with the root's `continuous_future_*` params. Nautilus builds such a request's bars into the
//! cache only, with neither `update_subscriptions` nor `skip_first_non_full_bar`, and answers with
//! no bars. `continuous_bars` reads them once the group has ended.
//!
//! The warmup requests the tick series of the data requirements over the same window in the same
//! group, whatever reads them. This includes the quotes of the pairs the equity converts through
//! (`venues::rate_series`), which Nautilus caches so that a trade sized at the start has their
//! rate. Each series is requested catalog first with `update_catalog`, and a root's series on the
//! contract standing for it at the start (`rolls::front`). The requests are `request_quotes`,
//! `request_trades`, `request_book_depth` and `request_book_deltas` (with the book type of the
//! venue's book, `venues::book_format`). A series some bars are built from is requested once,
//! building them.
//!
//! A series is requested only if its venue's history serves it (`venues::serves`), the rule
//! fill-catalog fills by. For a series the venue does not serve, such as an OKX instrument's
//! quotes, the warmup requests no history and logs a warning. It still subscribes to the series
//! from the start. A series its venue's live data does not stream is refused before a live node
//! starts (`venues::streams`). Definitions are asked for every series' instruments, served or not.
//!
//! A venue can resend its latest tick on a subscription, as IC and IB do on quotes. So the
//! warmup's last tick can come once more as the first live one.
//!
//! Live data comes while older windows are pending, and Nautilus holds live ticks for the bar
//! aggregators, not for an actor's indicators. So the warmup does two things from the actor's
//! hooks:
//! - Hands the history of its responses to the tick indicators (`feeding_ticks`), book indicators
//!   (`BookIndicator`, `feeding_books`), custom indicators (`CustomIndicator`, `feeding_custom`)
//!   and bar indicators (`feeding`, `building`). Nautilus routes depth, deltas and custom data to
//!   no indicator and registers the strategy's bar indicators for nothing. The deltas go in the
//!   batches the live stream carries (`batches`).
//! - Holds the live data until the group ends, then hands it over in order, dropping what the
//!   history holds (`hold`, which says the rule). After the warmup, and in a replay, live data
//!   goes straight through.
//!
//! Temporary, as the hold is, until Nautilus holds an actor's live data while its request is
//! pending: helix-trader#16, nautilus_trader#5675.
//!
//! Each series' history comes in the windows of its own request. The venues answer them in their
//! own order, not in time order across series. The warmup therefore merges its history across
//! series (`merge`, which says the rule): Temporary until Nautilus hands a warmup's history in time
//! order across data types, helix-trader#46. The merge works as follows:
//! - It queues the history of each series a fed indicator reads. The other series go on as they
//!   come.
//! - It hands the history to the hold in `ts_init` order, as a replay does.
//! - Past `merge::HELD` bytes held (2 GiB), the warmup ends incomplete, naming the series held
//!   back, and the rest of its history is left out.
//! - The same happens if a series brings a tick initialized before the latest it brought, as the
//!   merge trusts no such history.
//! - It keeps a request's bars past its completion while other series are pending.
//!
//! The merge is separate from the hold, which goes with helix-trader#16 while the merge stays until
//! its own issue closes. A replay merges nothing, as Nautilus replays in time order already.
//!
//! The warmup requests each custom series of the requirements (`DataRequirements::custom`) in the
//! same group over the same window. It uses `request_data` of the data type as read at the start
//! (`CustomSeries::at`), once per data type, by one rule (`custom_request`):
//! - If the type's row has a history source that serves it: asks the data client of the row's live
//!   data source (`venues::custom_data_client`) with `update_catalog`, as with every series served.
//! - If the type has no row, or a row with no history source: asks `CATALOG`, which no node
//!   registers, with no params. Nautilus answers from the catalog alone
//!   (`dispatch_date_range_request`, `crates/data/src/engine/streaming.rs`). The series belongs to
//!   the catalog and is written into it from outside helix (`venues::serves_custom`), and a client
//!   would fail the request.
//!
//! Raw features read custom data from Nautilus' cache. The data engine puts each point it
//! processes there, live or replayed, but not a request's response (`docs/concepts/cache.md`,
//! "Custom data"). So the warmup adds the response's points to the cache itself, oldest first, as
//! Nautilus' docs ask (`Cache::add_custom_data`). It skips a point no later than the latest the
//! cache holds of its type (`Warmup::on_historical_data`). No source's live client streams a custom
//! series yet, so a live start refuses a run reading one (`venues::streams_custom`) and no run
//! reaches this request today. This is the warmup half of such a run, kept for when a source
//! streams it.
//!
//! A strategy holds a `Warmup` and delegates its hooks to it: `start` from `on_start`, the
//! `Warmup::on_*` methods from the hooks of the same names, then `continuous_bars` on a root's
//! bars. A model strategy's `ModelHandle` builds and owns it, and `model_data_hooks!` writes the
//! hooks. In a backtest or a dataset run nothing is called.

use std::{any::Any, cell::Cell, fmt::Debug, rc::Rc};

use anyhow::Context;
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

/// The window a warmup is asked in (see `time_range`).
///
/// It is under a day by more than a bar. IB's client asks a window as one request between its
/// bounds, from a bar before its start, and keeps the bars within it (nautilus_trader#5621). ibapi
/// sends a span of up to a day in seconds, and a longer one in whole days plus one, counted in
/// sessions. So 23 hours of 1-minute bars go as `82860 S` at most, while a day's window goes as
/// `3 D`: three sessions fetched to keep one day's bars.
const WINDOW: SignedDuration = SignedDuration::from_hours(23);

/// The data client a warmup names when it requests a custom series that no source's history serves
/// (`venues::serves_custom`).
///
/// The series is written into the catalog from outside helix. Nautilus' data engine answers a
/// request naming no registered client from its catalogs alone. It plans no client leg for what
/// they lack (`dispatch_date_range_request` in `data/src/engine/streaming.rs`:
/// `if !has_client { missing_intervals.clear(); }`). A live node registers its catalog
/// (`live::node`). With none, the engine would fail the request.
const CATALOG: &str = "CATALOG";

/// What a warmup hands the book depth and deltas of its responses to.
///
/// Nautilus routes them to no indicator (`ActorIndicator` reads quotes, trades and bars alone).
/// Temporary until Nautilus routes book depth and deltas to registered indicators:
/// helix-trader#47.
pub trait BookIndicator {
    /// Reads `depth`, a snapshot, oldest first.
    fn handle_depth(&self, depth: &OrderBookDepth);

    /// Reads `deltas`, a batch as the live stream carries it, oldest first.
    fn handle_deltas(&self, deltas: &OrderBookDeltas);
}

/// What a warmup hands custom data to, from its responses and live.
///
/// Nautilus hands custom data to an actor's `on_historical_data` and `on_data` alone. Its
/// `Indicator` reads none.
pub trait CustomIndicator {
    /// Reads `data`, a point, oldest first or live.
    fn handle_custom(&self, data: &CustomData);
}

/// A strategy's warmup for a live node (`live`) or a replay (`replayed`).
///
/// It is the one owner of the protocol above. Nautilus delivers historical data only to the
/// strategy's own hooks, so the strategy delegates them to its warmup, as the module lists. On a
/// replay, `start` requests nothing and live data goes straight to the indicators.
///
/// The strategies read no bars on a root, so a warmup has no continuous bars.
pub struct Warmup {
    start: Start,
    length: SignedDuration,
    bar_types: Vec<BarType>,
    harness: Vec<BarType>,
    ticks: Vec<TickSeries>,
    custom: Vec<CustomSeries>,
    /// The bar types, tick series and custom series the fed indicators read.
    ///
    /// The merge orders their history.
    fed_bars: Vec<BarType>,
    fed_ticks: Vec<TickSeries>,
    fed_custom: Vec<CustomSeries>,
    /// Hands the history of a live start's series to the hold, in time order across the series.
    ///
    /// Temporary until Nautilus hands a warmup's history in time order across data types:
    /// helix-trader#46.
    merge: Merge,
    hold: Hold,
    end: Cell<UnixNanos>,
    group: Cell<UUID4>,
    warming: Cell<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Start {
    Request,
    Replay,
}

impl Warmup {
    /// Returns the warmup for a strategy with `requirements` on a live node.
    ///
    /// It requests `requirements.warmup` of history.
    #[must_use]
    pub fn live(requirements: &DataRequirements) -> Self {
        Self::of(requirements, Start::Request)
    }

    /// Returns the warmup for a strategy with `requirements` in a backtest or a dataset run.
    ///
    /// It requests nothing.
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
            fed_bars: Vec::new(),
            fed_ticks: Vec::new(),
            fed_custom: Vec::new(),
            merge: Merge::new(merge::HELD),

            // Temporary: the hold of live data, helix-trader#16
            hold: Hold::default(),
            end: Cell::new(UnixNanos::default()),
            group: Cell::new(UUID4::new()),
            warming: Cell::new(false),
        }
    }

    /// Returns the warmup that also hands its bars to `indicator`.
    ///
    /// The indicator reads the bars of `bar_types` alone. These are the bars its requests build
    /// and the live and replayed ones from `on_bar`. The hold holds them and deduplicates them
    /// against the history (see `hold`), and the merge orders them. The indicator is registered
    /// with Nautilus for no bar of its own.
    #[must_use]
    pub fn feeding(mut self, bar_types: &[BarType], indicator: SharedActorIndicator) -> Self {
        self.fed_bars.extend(bar_types);

        // Temporary: the hold of live data, helix-trader#16
        self.hold.feeding_bars(indicator);
        self
    }

    /// Returns the warmup that also builds the bars of `bar_type`, if the strategy does not read
    /// it.
    ///
    /// The bars are for the harness's `indicator`, which the warmup feeds. Live, the data engine
    /// goes on with them once the strategy subscribes.
    #[must_use]
    pub fn building(mut self, bar_type: BarType, indicator: SharedActorIndicator) -> Self {
        if !self.bar_types.contains(&bar_type) && !self.harness.contains(&bar_type) {
            self.harness.push(bar_type);
        }
        self.feeding(&[bar_type], indicator)
    }

    /// Returns the warmup that also hands book depth and deltas to `books`.
    ///
    /// The indicator reads the depth and deltas of `ticks`. These are the ones from its responses
    /// and the live ones from `on_book_depth` and `on_book_deltas`. The hold holds and
    /// deduplicates them as `feeding` does, and the merge orders `ticks` as `feeding_ticks` does.
    #[must_use]
    pub fn feeding_books(mut self, ticks: &[TickSeries], books: Rc<dyn BookIndicator>) -> Self {
        self.fed_ticks.extend_from_slice(ticks);

        // Temporary: the hold of live data, helix-trader#16
        self.hold.feeding_books(books);
        self
    }

    /// Returns the warmup that also hands custom data to `custom`.
    ///
    /// The indicator reads the custom data of `series`. This is the data from its responses and the
    /// live and replayed points from `on_data`. The hold holds and deduplicates them as `feeding`
    /// does. The merge orders `series` by their data types as read at the start
    /// (`CustomSeries::at`).
    #[must_use]
    pub fn feeding_custom(
        mut self,
        series: &[CustomSeries],
        custom: Rc<dyn CustomIndicator>,
    ) -> Self {
        self.fed_custom.extend_from_slice(series);

        // Temporary: the hold of live data, helix-trader#16
        self.hold.feeding_custom(custom);
        self
    }

    /// Returns the warmup that also hands quotes and trades to `indicator`.
    ///
    /// The indicator reads the quotes and trades of `ticks`. These are the ones from its responses
    /// and the live ones from `on_quote` and `on_trade`, handled as `feeding` does. A series no fed
    /// indicator reads, such as a conversion pair's quotes, goes on as it comes.
    #[must_use]
    pub fn feeding_ticks(mut self, ticks: &[TickSeries], indicator: SharedActorIndicator) -> Self {
        self.fed_ticks.extend_from_slice(ticks);

        // Temporary: the hold of live data, helix-trader#16
        self.hold.feeding(indicator);
        self
    }

    /// Starts the warmup of `actor`, from its `on_start`.
    ///
    /// Live, it requests the warmup up to now as a new request group (see `start`). On a replay, it
    /// requests nothing. Returns that `now`, where the strategy's subscriptions start.
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
            self.end.set(UnixNanos::from(now));
            self.group.set(UUID4::new());
            self.warming.set(true);

            // Temporary: the hold of live data, helix-trader#16
            self.hold.hold();

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

            // Temporary: the merge of the history across series, until Nautilus hands it in time
            // order, helix-trader#46. The merge ranks ties by the strategy's own series, as a
            // replay reads them (`Backtest::series`), not by the harness's bars.
            let own = DataRequirements {
                bar_types: self.bar_types.clone(),
                ..requirements.clone()
            };
            let fed = DataRequirements {
                bar_types: self.fed_bars.clone(),
                ticks: self.fed_ticks.clone(),
                custom: self.fed_custom.clone(),
                warmup: self.length,
            };
            self.merge.start(&requested, &own, &fed, now);

            if requested.is_empty() {
                self.warming.set(false);
                self.end()?;
            }
        }
        Ok(now)
    }

    /// Reads `response`, from the actor's `on_data_response`.
    ///
    /// It does three things:
    /// - Hands the bars the request built since the last response to the merge, with the latest
    ///   data the window brought.
    /// - At a request's completion, frees the hold its series has on the others.
    /// - Logs the end of the warmup.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - An indicator fails to read a bar or tick the merge hands on.
    /// - The merge goes past its bound, naming the series held back.
    /// - A series' bars came out of time order, naming that series.
    /// - The warmup ended incomplete, naming the reason (see `finished`).
    pub fn on_data_response<A>(
        &self,
        actor: &A,
        response: &DataResponseEnvelope,
    ) -> anyhow::Result<()>
    where
        A: DataActor + DataActorNative,
    {
        let ends = *response.correlation_id() == self.group.get();
        if ends {
            self.warming.set(false);
        }

        // Temporary: the merge reads the bars the request built and its end, helix-trader#46
        self.merge
            .response(actor, response, self.end.get(), &self.hold)?;
        let cut = if ends { self.end()? } else { false };

        if finished(self.group.get(), response)? && !cut {
            log::info!("Warmup complete");
        }
        Ok(())
    }

    /// Ends the merge and the hold of a live start.
    ///
    /// Hands on the history held, then the live ticks. Returns whether the merge was cut, which
    /// means the warmup ended incomplete.
    fn end(&self) -> anyhow::Result<bool> {
        // Temporary: the merge's end, helix-trader#46
        let cut = self.merge.end(&self.hold)?;

        // Temporary: the hold of live data, helix-trader#16
        self.hold.release()?;
        Ok(cut)
    }

    /// Hands `ticks`, from a response, to the merge (see `merge`).
    ///
    /// The warmup ends incomplete if the merge goes past its bound or a series' history is out of
    /// time order.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - An indicator reading a tick fails.
    /// - The merge goes past its bound, naming the series.
    /// - The series is out of time order, naming the series.
    fn history(&self, ticks: impl IntoIterator<Item = Tick>) -> anyhow::Result<()> {
        // Temporary: the merge of the history across series, helix-trader#46
        self.merge.history(ticks, &self.hold)
    }

    /// Reads the completion of every request of the last start, then the completion of its group.
    #[cfg(test)]
    pub(crate) fn complete<A>(&self, actor: &A) -> anyhow::Result<()>
    where
        A: DataActor + DataActorNative,
    {
        let requests = self.merge.pending();
        for id in requests.into_iter().chain([self.group.get()]) {
            self.on_data_response(actor, &DataResponseEnvelope::terminal(id, None))?;
        }
        Ok(())
    }

    /// Reads the end of the last start's group, which failed with `reason`, the warmup's error.
    #[cfg(test)]
    pub(crate) fn fail<A>(&self, actor: &A, reason: &str) -> anyhow::Error
    where
        A: DataActor + DataActorNative,
    {
        let failed = DataResponseEnvelope::terminal(self.group.get(), Some(reason.to_string()));
        self.on_data_response(actor, &failed)
            .expect_err("a failed group is an error")
    }

    /// Returns whether a live start's requests are not all answered yet.
    ///
    /// Until then the indicators hold a partial state to decide on.
    #[must_use]
    pub fn is_warming(&self) -> bool {
        // Temporary: the merge cut ends the warmup incomplete, helix-trader#46
        self.warming.get() && !self.merge.is_cut()
    }

    /// Hands the book depth of `response` to the book indicators it feeds, from the actor's
    /// `on_historical_book_depth`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - An indicator reading a tick the merge hands on fails.
    /// - The merge goes past its bound, which ends the warmup incomplete.
    /// - The series' history comes out of time order, which ends the warmup incomplete.
    pub fn on_historical_book_depth(&self, response: &[OrderBookDepth]) -> anyhow::Result<()> {
        self.history(
            response
                .iter()
                .map(|depth| Tick::Depth(Box::new(depth.clone()))),
        )
    }

    /// Hands the book deltas of `response` to the book indicators it feeds, from the actor's
    /// `on_historical_book_deltas`.
    ///
    /// The deltas go in the batches the live stream carries (see `batches`).
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator reading a tick fails, or if the merge goes past its bound,
    /// as `on_historical_book_depth`.
    pub fn on_historical_book_deltas(&self, response: &[OrderBookDelta]) -> anyhow::Result<()> {
        self.history(batches(response).into_iter().map(Tick::Deltas))
    }

    /// Takes `depth`, a live snapshot, from the actor's `on_book_depth`.
    ///
    /// It treats the snapshot as `on_quote` treats a quote, for the book indicators (see `hold`). A
    /// book indicator reads depth without failing.
    pub fn on_book_depth(&self, depth: &OrderBookDepth) -> anyhow::Result<()> {
        // Temporary: the hold of live data, helix-trader#16
        self.hold.live(Tick::Depth(Box::new(depth.clone())))
    }

    /// Takes `deltas`, a live batch, from the actor's `on_book_deltas`, as `on_book_depth` does it.
    pub fn on_book_deltas(&self, deltas: &OrderBookDeltas) -> anyhow::Result<()> {
        // Temporary: the hold of live data, helix-trader#16
        self.hold.live(Tick::Deltas(deltas.clone()))
    }

    /// Adds the custom data of `response` to `actor`'s cache, then hands each point to the custom
    /// indicators it feeds, from the actor's `on_historical_data`.
    ///
    /// The data engine caches no response's custom data, so the warmup adds the points oldest
    /// first. The cache keeps the latest added first. So a point no later (`ts_event`) than the
    /// latest of its data type is left out. A live point taken while the request was pending stays
    /// the latest, and a point both carry is held once. Any payload but Nautilus'
    /// `Vec<CustomData>` is no response of the warmup's.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator reading a tick fails, or if the merge goes past its bound,
    /// as `on_historical_book_depth`.
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

    /// Takes `data`, a live or replayed point of custom data, from the actor's `on_data`.
    ///
    /// It treats the point as `on_quote` treats a quote, for the custom indicators (see `hold`). A
    /// custom indicator reads custom data without failing.
    pub fn on_data(&self, data: &CustomData) -> anyhow::Result<()> {
        // Temporary: the hold of live data, helix-trader#16
        self.hold.live(Tick::Custom(data.clone()))
    }

    /// Hands the quotes of `response` to the tick indicators, from the actor's
    /// `on_historical_quotes`.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator reading a tick fails, or if the merge goes past its bound
    /// or the series' history comes out of time order, ending the warmup incomplete.
    pub fn on_historical_quotes(&self, response: &[QuoteTick]) -> anyhow::Result<()> {
        self.history(response.iter().copied().map(Tick::Quote))
    }

    /// Hands the trades of `response` over as `on_historical_quotes` hands over quotes.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator reading a tick fails, or if the merge goes past its bound,
    /// as `on_historical_quotes`.
    pub fn on_historical_trades(&self, response: &[TradeTick]) -> anyhow::Result<()> {
        self.history(response.iter().copied().map(Tick::Trade))
    }

    /// Takes `bar`, a live or replayed bar, from the actor's `on_bar`.
    ///
    /// It treats the bar as `on_quote` treats a quote, for the bar indicators (see `hold`).
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator reading it fails.
    pub fn on_bar(&self, bar: &Bar) -> anyhow::Result<()> {
        // Temporary: the hold of live data, helix-trader#16
        self.hold.live(Tick::Bar(*bar))
    }

    /// Takes `quote`, a live quote, from the actor's `on_quote`.
    ///
    /// The warmup holds it until the group has ended and drops it if the history holds it (see
    /// `hold`). Then the tick indicators read it.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator reading it fails.
    pub fn on_quote(&self, quote: &QuoteTick) -> anyhow::Result<()> {
        // Temporary: the hold of live data, helix-trader#16
        self.hold.live(Tick::Quote(*quote))
    }

    /// Takes `trade`, a live trade, from the actor's `on_trade`, as `on_quote` does for a quote.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator reading it fails.
    pub fn on_trade(&self, trade: &TradeTick) -> anyhow::Result<()> {
        // Temporary: the hold of live data, helix-trader#16
        self.hold.live(Tick::Trade(*trade))
    }

    #[cfg(test)]
    pub(crate) fn holding(mut self, limit: usize) -> Self {
        self.merge = Merge::new(limit);
        self
    }

    /// Returns the cache config of a live node running the warmup (see `cache_config`).
    ///
    /// # Errors
    ///
    /// Returns an error if `cache_config` fails.
    pub fn cache_config(&self) -> anyhow::Result<CacheConfig> {
        cache_config(&self.built(), self.length)
    }

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
            .field("merge", &self.merge)
            .field("hold", &self.hold)
            .field("end", &self.end)
            .field("group", &self.group)
            .field("warming", &self.warming)
            .finish()
    }
}

/// Requests the warmup of `requirements` over `[now - warmup, now]` as the request group `group`.
///
/// The caller owns `group` and stores it before the call. A group whose requests are all answered
/// at once ends before this returns. No requests make no group, which `DataActor::request_group`
/// refuses.
///
/// Returns each request with its id, the bar types it builds and the series it answers. A
/// continuous future's request builds none, and `continuous_bars` reads them. A live `Warmup`
/// merges the series.
///
/// # Errors
///
/// Returns an error if:
/// - A request fails.
/// - A custom series' type's history source does not serve it, or its row names no live data
///   source to ask (see `custom_request`).
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

    // Definitions first: the data engine takes an actor's commands in order and caches each
    // definition the catalog answers before any request or subscription of the instrument.
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

                // The book type of the venue's book the strategy subscribes to, onto which Nautilus
                // replays the deltas before the start, into the snapshot the response opens with.
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

    // Each series once, a root's on its front contract, as Nautilus takes no windows on custom data
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

/// A request of a warmup's start.
///
/// It holds the request's id, the bar types it builds, and the series whose history it hands on in
/// time order (see `merge`). A request for bars or a definition has no series.
#[derive(Debug)]
pub struct Requested {
    id: UUID4,
    built: Vec<BarType>,
    series: Option<Series>,
}

impl Requested {
    const fn of(id: UUID4) -> Self {
        Self {
            id,
            built: Vec::new(),
            series: None,
        }
    }
}

/// Returns the client a warmup asks for `series`, and the request's params (see the module).
///
/// # Errors
///
/// Returns an error if `venues::serves_custom` fails, or if the history serves the type and its
/// row names no live data source.
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

/// Returns one request per series the bars of `bar_types` are built from, none on a root. Then it
/// returns one request per tick series at `now` (`rolls::front`) that no bar is built from.
fn requests(
    bar_types: &[BarType],
    ticks: &[TickSeries],
    now: Timestamp,
) -> Vec<(CatalogSeries, Vec<BarType>)> {
    let mut requests: Vec<(CatalogSeries, Vec<BarType>)> = Vec::new();

    for (source, bar_type) in bar_sources(bar_types).zip(bar_types) {
        // An external bar type is its own source, requested as it is, and builds nothing, as
        // Nautilus aggregates into internal bar types alone.
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

/// Splits `deltas`, from a response, into the batches the live stream carries.
///
/// Each batch closes with its `F_LAST` delta. The trailing deltas form a batch of their own, as
/// Nautilus' data engine publishes them.
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

/// Returns whether the warmup `group` ended complete with `response`.
///
/// Returns `Ok(false)` if `response` ends something else.
///
/// # Errors
///
/// Returns an error if `response` ends the group and a request in it failed, naming the reason.
pub fn finished(group: UUID4, response: &DataResponseEnvelope) -> anyhow::Result<bool> {
    if *response.correlation_id() != group {
        return Ok(false);
    }

    match response.failure_reason() {
        Some(reason) => anyhow::bail!("the warmup is incomplete: {reason}"),
        None => Ok(true),
    }
}

/// Returns the warmup bars of the `bar_types` on a root with continuous bars in `continuous`,
/// oldest first.
///
/// Nautilus builds them into the cache only and answers the request with no bars. So a strategy
/// reads them here once `finished` says the group ended.
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

    // The cache holds the newest bar first
    bars.sort_by_key(|bar| bar.ts_event);
    bars
}

/// Returns the cache config of a live node whose strategy warms up on `bar_types` over `warmup`.
///
/// It is Nautilus' default, with the bar capacity raised by the most bars the warmup builds for
/// one bar type.
///
/// # Errors
///
/// Returns an error if the warmup is negative, if a bar type is not aggregated on time, or if the
/// capacity the warmup needs is beyond what the cache allows.
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

const BUILD_BARS_WITHOUT_QUOTES: bool = false;

/// Returns the data engine config of a backtest or a dataset run of a strategy on `bar_types`.
///
/// It is Nautilus' default. It builds no bars for periods without quotes, and builds time bars
/// `bar_build_delay` after their close, as a live node does.
#[must_use]
pub fn data_engine_config(bar_types: &[BarType]) -> DataEngineConfig {
    DataEngineConfig {
        time_bars_build_with_no_updates: BUILD_BARS_WITHOUT_QUOTES,
        time_bars_build_delay: build_delay_micros(bar_types),
        ..DataEngineConfig::default()
    }
}

/// Microseconds a node waits after a bar's close before building a bar of source bars.
///
/// A venue sends a source bar once it is closed (OKX's confirmed 1-minute candles: 0.3 to 1.5 s,
/// measured 2026-10-22; IB's keepUpToDate 1-minute bars: 0.4 to 1.3 s, measured 2026-10-26).
const SOURCE_BAR_DELAY_MICROS: i64 = 5_000_000;

/// Returns the data engine config of a live node running a strategy on `bar_types`.
///
/// It is Nautilus' default. It builds no bars for periods without quotes, and builds time bars
/// `bar_build_delay` after their close.
#[must_use]
pub fn live_data_engine_config(bar_types: &[BarType]) -> LiveDataEngineConfig {
    LiveDataEngineConfig {
        time_bars_build_with_no_updates: BUILD_BARS_WITHOUT_QUOTES,
        time_bars_build_delay: build_delay_micros(bar_types),
        ..LiveDataEngineConfig::default()
    }
}

/// Returns how long a node building time bars on `bar_types` waits after a bar's close.
///
/// The wait is `SOURCE_BAR_DELAY_MICROS` if any bar type is composite, so that the last source bar
/// is in. A backtest waits the same, so that its decisions fall where a live node's do.
#[must_use]
pub fn bar_build_delay(bar_types: &[BarType]) -> SignedDuration {
    if bar_types.iter().any(BarType::is_composite) {
        SignedDuration::from_micros(SOURCE_BAR_DELAY_MICROS)
    } else {
        SignedDuration::ZERO
    }
}

fn build_delay_micros(bar_types: &[BarType]) -> u64 {
    let delay = bar_build_delay(bar_types).unsigned_abs().as_micros();
    u64::try_from(delay).expect("a delay of seconds")
}

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
        messages::data::{
            DataCommand, DataResponse, QuotesResponse, RequestCommand, RequestCustomData,
        },
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
                DataCommand::Request(RequestCommand::Instrument(_) | RequestCommand::Data(_)) => {
                    None
                }
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

    fn reading(bar_types: &[&str]) -> DataRequirements {
        DataRequirements {
            bar_types: bar_types.iter().map(|&b| BarType::from(b)).collect(),
            ticks: vec![],
            custom: vec![],
            warmup: DAY,
        }
    }

    /// The harness's bars are built once, after the strategy's, even if the strategy reads them.
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

    fn reading_ticks(bar_types: &[&str], ticks: Vec<TickSeries>) -> DataRequirements {
        DataRequirements {
            ticks,
            ..reading(bar_types)
        }
    }

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

    /// GC rolls into GCZ26 on 2026-07-29 and out on 2026-11-26 (`rolls/README.md`).
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

    fn defined(id: &str) -> InstrumentAny {
        let mut contract = futures_contract_es(None, None);
        contract.id = InstrumentId::from(id);
        InstrumentAny::FuturesContract(contract)
    }

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

    fn imbalances(identifier: &str) -> CustomSeries {
        CustomSeries::new("Imbalance", identifier.to_string(), None).unwrap()
    }

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

    /// GC rolls into GCZ26 on 2026-07-29 (`rolls/README.md`).
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

    fn us10y_metadata() -> Params {
        Params::new(
            [
                ("source".to_string(), json!("bond_yields")),
                ("symbol".to_string(), json!("US10Y")),
            ]
            .into(),
        )
    }

    fn us10y() -> DataType {
        DataType::new("LseRow", Some(us10y_metadata()), Some("US10Y".to_string()))
    }

    fn reading_us10y() -> DataRequirements {
        let series =
            CustomSeries::new("LseRow", "US10Y".to_string(), Some(us10y_metadata())).unwrap();
        DataRequirements {
            custom: vec![series],
            ..reading(&[])
        }
    }

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

    fn us10y_under(identifier: &str) -> CustomSeries {
        CustomSeries::new("LseRow", identifier.to_string(), Some(us10y_metadata())).unwrap()
    }

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

    /// The response's rows are cached oldest first, so the cache holds them latest first.
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

    /// The history answers 2026-08-09 and 2026-08-10. Live, 2026-08-10 is resent and 2026-08-11 is
    /// held until the group ends.
    #[rstest]
    fn live_custom_data_is_held_and_dropped_at_the_seam_as_ticks_are() {
        let rows = Rc::new(Rows::default());
        let requirements = reading_us10y();
        let warmup =
            Warmup::live(&requirements).feeding_custom(&requirements.custom, Rc::clone(&rows) as _);
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

    #[rstest]
    fn replayed_custom_data_goes_straight_to_the_custom_indicator() {
        let rows = Rc::new(Rows::default());
        let requirements = reading_us10y();
        let warmup = Warmup::replayed(&requirements)
            .feeding_custom(&requirements.custom, Rc::clone(&rows) as _);
        let (mut probe, _) = registered();
        warmup.start(&mut probe).unwrap();

        warmup.on_data(&us10y_row("09", 4.25)).unwrap();
        warmup.on_data(&us10y_row("10", 4.5)).unwrap();

        assert_eq!(rows.closes.take(), [json!(4.25), json!(4.5)]);
    }

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

    /// Nautilus answers a deltas request with the deltas alone, so the warmup batches them by
    /// `F_LAST`.
    #[rstest]
    fn the_warmup_hands_book_deltas_over_in_the_batches_the_live_stream_carries() {
        let books = Rc::new(Books::default());
        let warmup = Warmup::live(&reading(&[])).feeding_books(&[], Rc::clone(&books) as _);
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

    fn depth_at(at: u64) -> OrderBookDepth {
        OrderBookDepth {
            sequence: at,
            ts_event: UnixNanos::from(at),
            ..stub_depth10()
        }
    }

    fn deltas_at(at: u64) -> OrderBookDeltas {
        OrderBookDeltas::new(
            InstrumentId::from("GCZ26.GLBX"),
            vec![delta(at, RecordFlag::F_LAST as u8)],
        )
    }

    #[rstest]
    fn live_book_depth_and_deltas_are_held_and_dropped_at_the_seam_as_ticks_are() {
        let books = Rc::new(Books::default());
        let requirements = reading_ticks(&[], vec![tick(NautilusDataType::QuoteTick, XAU)]);
        let warmup =
            Warmup::live(&requirements).feeding_books(&requirements.ticks, Rc::clone(&books) as _);
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

    /// The cache holds the harness's warmup too: 1,440 1-minute bars over a day, plus one.
    #[rstest]
    fn the_cache_holds_the_warmup_of_every_bar_type_the_warmup_builds() {
        let minutes = BarType::from("SXR8.IBIS-1-MINUTE-MID-INTERNAL");

        let warmup = Warmup::live(&reading(&["SXR8.IBIS-1-HOUR-MID-INTERNAL"]))
            .building(minutes, Rc::new(Inert));

        assert_eq!(warmup.cache_config().unwrap().bar_capacity, 10_000 + 1_441);
    }

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

    /// An external bar type is requested as it is and builds nothing, as Nautilus' data engine
    /// refuses it in `bar_types`.
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

    #[rstest]
    fn each_requests_built_bars_are_handed_over_once_as_the_merge_frees_them() {
        let hour_mid = BarType::from("ESM24.CME-1-HOUR-MID-INTERNAL");
        let composite = BarType::from("ESM24.CME-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL");
        let recorder = Rc::new(Recorder::default());
        let requirements = reading(&[
            "ESM24.CME-1-HOUR-MID-INTERNAL",
            "ESM24.CME-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
        ]);
        let warmup = Warmup::live(&requirements).feeding(
            &requirements.bar_types,
            Rc::clone(&recorder) as SharedActorIndicator,
        );
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
        assert_eq!(recorder.bar_types.take(), []);
        warmup
            .on_data_response(&probe, &completed(minutes))
            .unwrap();
        assert_eq!(recorder.bar_types.take(), [hour_mid, composite.standard()]);

        warmup
            .on_data_response(&probe, &completed(minutes))
            .unwrap();
        assert_eq!(recorder.bar_types.take(), []);
    }

    /// Windows of 23 hours stay under a day by more than a bar, so that IB is asked in seconds (see
    /// `WINDOW`).
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

    #[rstest]
    fn a_replays_warmup_never_goes_on() {
        let replayed = Warmup::replayed(&reading(&["SXR8.IBIS-1-HOUR-MID-INTERNAL"]));
        let (mut probe, _) = registered();

        replayed.start(&mut probe).unwrap();

        assert!(!replayed.is_warming());
    }

    type Read = (&'static str, InstrumentId, UnixNanos);

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

    const XAU: &str = "GCZ26.GLBX";
    const EUR: &str = "EURUSD.ICMARKETS";

    /// `time` on 2026-08-10, UTC, the day `registered` starts on.
    fn on_the_day(time: &str) -> UnixNanos {
        let at: Timestamp = format!("2026-08-10T{time}Z").parse().unwrap();
        UnixNanos::from(at)
    }

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

    fn quote(made: &str) -> QuoteTick {
        quote_of(XAU, made, made)
    }

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

    fn read_quote(instrument_id: &str, made: &str) -> Read {
        ("quote", InstrumentId::from(instrument_id), on_the_day(made))
    }

    fn read_trade(made: &str) -> Read {
        ("trade", InstrumentId::from(XAU), on_the_day(made))
    }

    fn warming(ticks: &Rc<Ticks>) -> (Warmup, Probe) {
        let warmup = Warmup::live(&quotes_and_trades()).feeding_ticks(
            &quotes_and_trades().ticks,
            Rc::clone(ticks) as SharedActorIndicator,
        );
        let (mut probe, _) = registered();
        warmup.start(&mut probe).unwrap();
        (warmup, probe)
    }

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

    #[rstest]
    fn live_ticks_are_held_until_the_group_ends_not_one_of_its_requests() {
        let ticks = Rc::new(Ticks::default());
        let warmup = Warmup::live(&quotes_and_trades()).feeding_ticks(
            &quotes_and_trades().ticks,
            Rc::clone(&ticks) as SharedActorIndicator,
        );
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

    /// A resend at or before the history's last quote by `ts_event` is dropped, whatever its
    /// `ts_init`.
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

    #[rstest]
    fn a_replays_ticks_and_books_reach_the_features_as_they_come() {
        let ticks = Rc::new(Ticks::default());
        let books = Rc::new(Books::default());
        let requirements = reading_ticks(&[], vec![tick(NautilusDataType::QuoteTick, XAU)]);
        let replayed = Warmup::replayed(&requirements)
            .feeding_ticks(
                &requirements.ticks,
                Rc::clone(&ticks) as SharedActorIndicator,
            )
            .feeding_books(&requirements.ticks, Rc::clone(&books) as _);
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

    type Entry = (&'static str, UnixNanos);

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

        fn handle_bar(&self, bar: &Bar) -> anyhow::Result<()> {
            self.read.borrow_mut().push(("bar", bar.ts_event));
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

    fn logging(
        requirements: &DataRequirements,
        log: &Rc<Log>,
    ) -> (Warmup, Probe, TypedIntoMessageSavingHandler<DataCommand>) {
        logging_with(Warmup::live(requirements), requirements, log)
    }

    fn logging_with(
        warmup: Warmup,
        read: &DataRequirements,
        log: &Rc<Log>,
    ) -> (Warmup, Probe, TypedIntoMessageSavingHandler<DataCommand>) {
        let warmup = warmup
            .feeding(&read.bar_types, Rc::clone(log) as SharedActorIndicator)
            .feeding_ticks(&read.ticks, Rc::clone(log) as SharedActorIndicator)
            .feeding_books(&read.ticks, Rc::clone(log) as _)
            .feeding_custom(&read.custom, Rc::clone(log) as _);
        let (mut probe, commands) = registered();
        warmup.start(&mut probe).unwrap();
        (warmup, probe, commands)
    }

    fn quotes_and_trades() -> DataRequirements {
        reading_ticks(
            &[],
            vec![
                tick(NautilusDataType::QuoteTick, XAU),
                tick(NautilusDataType::TradeTick, XAU),
            ],
        )
    }

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

    fn entry(kind: &'static str, made: &str) -> Entry {
        (kind, on_the_day(made))
    }

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

    /// EURUSD's quotes, a conversion pair's, are requested but not read.
    #[rstest]
    fn a_series_no_fed_indicator_reads_holds_no_history_back() {
        let log = Rc::new(Log::default());
        let gold = tick(NautilusDataType::QuoteTick, XAU);
        let requirements = reading_ticks(
            &[],
            vec![gold.clone(), tick(NautilusDataType::QuoteTick, EUR)],
        );
        let read = reading_ticks(&[], vec![gold]);
        let (warmup, _probe, commands) = logging_with(Warmup::live(&requirements), &read, &log);

        warmup
            .on_historical_quotes(&[quote("12:00:00"), quote("13:00:00")])
            .unwrap();

        let pair = CatalogSeries::Ticks(tick(NautilusDataType::QuoteTick, EUR));
        assert!(
            sent_requests(&commands)
                .iter()
                .any(|(source, ..)| *source == pair)
        );
        assert_eq!(
            *log.read.borrow(),
            [entry("quote", "12:00:00"), entry("quote", "13:00:00")]
        );
    }

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

    fn depth_made(made: UnixNanos) -> OrderBookDepth {
        OrderBookDepth {
            instrument_id: InstrumentId::from(XAU),
            ts_event: made,
            ts_init: made,
            ..stub_depth10()
        }
    }

    fn delta_made(made: UnixNanos) -> OrderBookDelta {
        OrderBookDelta {
            ts_event: made,
            ts_init: made,
            ..delta(1, RecordFlag::F_LAST as u8)
        }
    }

    /// Ties across series go in the order a replay hands them: custom data, then the catalog series
    /// in backtest order (`DataRequirements::catalog_series`), then built bars.
    #[rstest]
    fn ties_across_series_reach_a_feature_in_the_order_a_replay_hands_them() {
        let log = Rc::new(Log::default());
        let requirements = DataRequirements {
            bar_types: vec![BarType::from(XAU_HOURS)],
            ticks: [
                NautilusDataType::OrderBookDelta,
                NautilusDataType::TradeTick,
                NautilusDataType::QuoteTick,
                NautilusDataType::OrderBookDepth,
            ]
            .into_iter()
            .map(|data| tick(data, XAU))
            .collect(),
            ..reading_us10y()
        };
        let (warmup, probe, commands) = logging(&requirements, &log);
        let quotes = request_of(&commands, NautilusDataType::QuoteTick);
        let midnight = on_the_day("00:00:00");

        answer_quotes(
            &warmup,
            &probe,
            quotes,
            &[quote("00:00:00")],
            &[hour_bar("00:00:00")],
        );
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
        warmup.complete(&probe).unwrap();

        let kinds: Vec<&str> = log.read.borrow().iter().map(|(kind, _)| *kind).collect();
        assert_eq!(
            kinds,
            ["custom", "quote", "deltas", "trade", "depth", "bar"]
        );
    }

    /// Ties go as a replay of the strategy's own series hands them (`Backtest::series`), not the
    /// harness's bars.
    #[rstest]
    fn ties_go_as_a_replay_of_the_strategys_own_series_hands_them() {
        let log = Rc::new(Log::default());
        let requirements = reading_ticks(
            &[],
            vec![
                tick(NautilusDataType::TradeTick, XAU),
                tick(NautilusDataType::QuoteTick, XAU),
            ],
        );
        let atr = Rc::new(Log::default());
        let warmup = Warmup::live(&requirements).building(
            BarType::from(XAU_HOURS),
            Rc::clone(&atr) as SharedActorIndicator,
        );
        let (warmup, probe, _) = logging_with(warmup, &requirements, &log);

        warmup.on_historical_quotes(&[quote("00:00:00")]).unwrap();
        warmup
            .on_historical_trades(&[trade("00:00:00", "00:00:00")])
            .unwrap();
        warmup.complete(&probe).unwrap();

        assert_eq!(
            *log.read.borrow(),
            [entry("trade", "00:00:00"), entry("quote", "00:00:00")]
        );
    }

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

    #[rstest]
    fn past_its_bound_the_warmup_ends_incomplete_naming_the_series_held_back() {
        let log = Rc::new(Log::default());
        let (warmup, _probe, _) = logging_with(
            Warmup::live(&quotes_and_trades()).holding(2 * size_of::<Tick>()),
            &quotes_and_trades(),
            &log,
        );

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

    #[rstest]
    #[case::within_a_response(vec![vec!["13:00:00", "12:00:00"]])]
    #[case::across_windows(vec![vec!["13:00:00"], vec!["12:00:00"]])]
    fn a_series_history_out_of_time_order_ends_the_warmup_incomplete_naming_it(
        #[case] windows: Vec<Vec<&str>>,
    ) {
        let log = Rc::new(Log::default());
        let (warmup, _probe, _) = logging(&quotes_and_trades(), &log);

        let answered: Vec<anyhow::Result<()>> = windows
            .iter()
            .map(|made| {
                let quotes: Vec<QuoteTick> = made.iter().map(|made| quote(made)).collect();
                warmup.on_historical_quotes(&quotes)
            })
            .collect();
        let after = warmup.on_historical_trades(&[trade("12:30:00", "12:30:00")]);

        let (last, earlier) = answered.split_last().unwrap();
        assert!(earlier.iter().all(Result::is_ok), "{earlier:?}");
        let err = format!("{:#}", last.as_ref().expect_err("out of time order"));
        for part in ["incomplete", "quotes of GCZ26.GLBX"] {
            assert!(err.contains(part), "{err}");
        }
        assert!(!warmup.is_warming());
        assert!(after.is_ok(), "{after:?}");
        assert_eq!(*log.read.borrow(), [entry("quote", "13:00:00")]);
    }

    #[rstest]
    #[case::deltas("deltas of GCZ26.GLBX")]
    #[case::depth("depth of GCZ26.GLBX")]
    #[case::custom("custom data")]
    fn the_merge_counts_what_a_tick_keeps_on_the_heap_against_its_bound(#[case] series: &str) {
        let log = Rc::new(Log::default());
        let midnight = on_the_day("00:00:00");
        let deltas: Vec<OrderBookDelta> = (0..10)
            .map(|at| OrderBookDelta {
                ts_event: midnight,
                ts_init: midnight,
                ..delta(at, if at == 9 { RecordFlag::F_LAST as u8 } else { 0 })
            })
            .collect();
        let row = us10y_row("10", 4.5);
        let heap = match series {
            "deltas of GCZ26.GLBX" => 10 * size_of::<OrderBookDelta>(),
            "depth of GCZ26.GLBX" => size_of::<OrderBookDepth>(),
            _ => size_of_val(&*row.data),
        };
        let bound = size_of::<Tick>() + heap - 1;
        let (warmup, probe, _) = logging_with(
            Warmup::live(&every_kind()).holding(bound),
            &every_kind(),
            &log,
        );

        let err = match series {
            "deltas of GCZ26.GLBX" => warmup.on_historical_book_deltas(&deltas),
            "depth of GCZ26.GLBX" => warmup.on_historical_book_depth(&[depth_made(midnight)]),
            _ => warmup.on_historical_data(&probe, &vec![row]),
        }
        .expect_err("past the bound");

        let err = format!("{err:#}");
        for part in ["incomplete", series] {
            assert!(err.contains(part), "{err}");
        }
        assert!(!warmup.is_warming());
    }

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

    const XAU_HOURS: &str = "GCZ26.GLBX-1-HOUR-MID-INTERNAL";

    fn bars_and_trades() -> DataRequirements {
        reading_ticks(&[XAU_HOURS], vec![tick(NautilusDataType::TradeTick, XAU)])
    }

    fn logging_bars(log: &Rc<Log>) -> (Warmup, Probe, TypedIntoMessageSavingHandler<DataCommand>) {
        let read = reading_ticks(
            &[XAU_HOURS],
            vec![
                tick(NautilusDataType::QuoteTick, XAU),
                tick(NautilusDataType::TradeTick, XAU),
            ],
        );
        logging_with(Warmup::live(&bars_and_trades()), &read, log)
    }

    fn hour_bar(close: &str) -> Bar {
        let price = Price::from("4000.00");
        Bar::new(
            BarType::from(XAU_HOURS),
            price,
            price,
            price,
            price,
            Quantity::from(1),
            on_the_day(close),
            on_the_day(close),
        )
    }

    /// Answers a window of GCZ26's quotes, request `request`, as a node does.
    fn answer_quotes(
        warmup: &Warmup,
        probe: &Probe,
        request: UUID4,
        quotes: &[QuoteTick],
        built: &[Bar],
    ) {
        for bar in built {
            probe.cache_rc().borrow_mut().add_bar(*bar).unwrap();
        }
        warmup.on_historical_quotes(quotes).unwrap();
        let response = DataResponse::Quotes(QuotesResponse::new(
            request,
            ClientId::from("FAKE"),
            InstrumentId::from(XAU),
            quotes.to_vec(),
            None,
            None,
            UnixNanos::default(),
            None,
        ));
        let window = DataResponseEnvelope::new(response, false, None);
        warmup.on_data_response(probe, &window).unwrap();
    }

    #[rstest]
    #[case::the_quotes_first(&[0, 1, 2, 3])]
    #[case::the_trades_first(&[2, 3, 0, 1])]
    #[case::interleaved(&[2, 0, 3, 1])]
    fn built_bars_reach_a_feature_in_time_order_among_ticks_window_by_window(
        #[case] order: &[usize],
    ) {
        let log = Rc::new(Log::default());
        let (warmup, probe, commands) = logging_bars(&log);
        let quotes = request_of(&commands, NautilusDataType::QuoteTick);
        let windows = [
            (
                vec![quote("12:00:00"), quote("13:00:00")],
                vec![hour_bar("12:00:00")],
            ),
            (vec![quote("14:00:00")], vec![hour_bar("13:00:00")]),
        ];
        let trades = [
            vec![trade("12:00:30", "12:00:30"), trade("13:00:30", "13:00:30")],
            vec![trade("14:00:30", "14:00:30")],
        ];

        for &answer in order {
            match answer {
                0 | 1 => {
                    let (ticks, built) = &windows[answer];
                    answer_quotes(&warmup, &probe, quotes, ticks, built);
                }
                _ => warmup.on_historical_trades(&trades[answer - 2]).unwrap(),
            }
        }

        let last = hour_bar("14:00:00");
        probe.cache_rc().borrow_mut().add_bar(last).unwrap();
        warmup.complete(&probe).unwrap();

        assert_eq!(
            *log.read.borrow(),
            [
                entry("quote", "12:00:00"),
                entry("bar", "12:00:00"),
                entry("trade", "12:00:30"),
                entry("quote", "13:00:00"),
                entry("bar", "13:00:00"),
                entry("trade", "13:00:30"),
                entry("quote", "14:00:00"),
                entry("bar", "14:00:00"),
                entry("trade", "14:00:30"),
            ]
        );
    }

    /// The merge orders by `ts_init`, as a replay does: the bar has `ts_event` 12:00 and `ts_init`
    /// 13:00.
    #[rstest]
    fn the_merge_orders_series_by_ts_init_not_ts_event() {
        let log = Rc::new(Log::default());
        let (warmup, probe, commands) = logging_bars(&log);
        let quotes = request_of(&commands, NautilusDataType::QuoteTick);
        let open_stamped = Bar {
            ts_event: on_the_day("12:00:00"),
            ..hour_bar("13:00:00")
        };

        answer_quotes(
            &warmup,
            &probe,
            quotes,
            &[quote("12:00:00"), quote("13:00:00")],
            &[open_stamped],
        );
        warmup
            .on_historical_trades(&[trade("12:30:00", "12:30:00")])
            .unwrap();
        warmup.complete(&probe).unwrap();

        assert_eq!(
            *log.read.borrow(),
            [
                entry("quote", "12:00:00"),
                entry("trade", "12:30:00"),
                entry("quote", "13:00:00"),
                entry("bar", "12:00:00"),
            ]
        );
    }

    #[rstest]
    fn a_pending_requests_bars_stand_at_the_latest_its_data_brought() {
        let log = Rc::new(Log::default());
        let (warmup, probe, commands) = logging_bars(&log);
        let quotes = request_of(&commands, NautilusDataType::QuoteTick);

        answer_quotes(
            &warmup,
            &probe,
            quotes,
            &[quote("12:00:00"), quote("13:00:00")],
            &[hour_bar("12:00:00")],
        );
        warmup
            .on_historical_trades(&[trade("12:30:00", "12:30:00"), trade("12:45:00", "12:45:00")])
            .unwrap();

        assert_eq!(
            *log.read.borrow(),
            [
                entry("quote", "12:00:00"),
                entry("bar", "12:00:00"),
                entry("trade", "12:30:00"),
                entry("trade", "12:45:00"),
            ]
        );
    }

    #[rstest]
    fn a_request_completing_while_another_pends_hands_its_bars_on_as_far_as_the_other_reached() {
        let log = Rc::new(Log::default());
        let (warmup, probe, commands) = logging_bars(&log);
        let quotes = request_of(&commands, NautilusDataType::QuoteTick);
        let trades = request_of(&commands, NautilusDataType::TradeTick);

        warmup
            .on_historical_trades(&[trade("12:00:30", "12:00:30")])
            .unwrap();
        answer_quotes(
            &warmup,
            &probe,
            quotes,
            &[quote("12:00:00"), quote("13:00:00")],
            &[hour_bar("12:00:00")],
        );
        let last = hour_bar("13:00:00");
        probe.cache_rc().borrow_mut().add_bar(last).unwrap();
        let completed = |id| DataResponseEnvelope::terminal(id, None);
        warmup.on_data_response(&probe, &completed(quotes)).unwrap();
        let pending = log.read.borrow().clone();
        warmup.on_data_response(&probe, &completed(trades)).unwrap();

        assert_eq!(
            pending,
            [
                entry("quote", "12:00:00"),
                entry("bar", "12:00:00"),
                entry("trade", "12:00:30"),
            ]
        );
        assert_eq!(
            log.read.borrow()[pending.len()..],
            [entry("quote", "13:00:00"), entry("bar", "13:00:00")]
        );
    }

    #[rstest]
    fn live_bars_are_held_behind_the_history_until_the_group_ends() {
        let log = Rc::new(Log::default());
        let (warmup, probe, commands) = logging_bars(&log);
        let quotes = request_of(&commands, NautilusDataType::QuoteTick);

        answer_quotes(&warmup, &probe, quotes, &[quote("14:00:00")], &[]);
        probe
            .cache_rc()
            .borrow_mut()
            .add_bar(hour_bar("14:00:00"))
            .unwrap();
        let completed = DataResponseEnvelope::terminal(quotes, None);
        warmup.on_data_response(&probe, &completed).unwrap();
        warmup.on_bar(&hour_bar("15:00:00")).unwrap();
        let pending = log.read.borrow().clone();
        warmup
            .on_historical_trades(&[trade("14:00:30", "14:00:30")])
            .unwrap();
        warmup.complete(&probe).unwrap();

        assert_eq!(pending, []);
        assert_eq!(
            *log.read.borrow(),
            [
                entry("quote", "14:00:00"),
                entry("bar", "14:00:00"),
                entry("trade", "14:00:30"),
                entry("bar", "15:00:00"),
            ]
        );
    }

    /// A bar built at completion from held live data (`finish_historical_bar_aggregator`) is live,
    /// as it closes after the warmup's end.
    #[rstest]
    fn a_bar_built_from_the_live_data_a_request_held_is_read_among_the_live_ticks() {
        let log = Rc::new(Log::default());
        let (warmup, probe, commands) = logging_bars(&log);
        let quotes = request_of(&commands, NautilusDataType::QuoteTick);

        answer_quotes(
            &warmup,
            &probe,
            quotes,
            &[quote("14:00:00")],
            &[hour_bar("14:00:00")],
        );
        warmup
            .on_quote(&quote_of(XAU, "14:45:00", "14:45:00.050"))
            .unwrap();
        warmup.on_trade(&trade("14:50:00", "14:50:00.050")).unwrap();
        warmup
            .on_quote(&quote_of(XAU, "15:00:01", "15:00:01.050"))
            .unwrap();
        probe
            .cache_rc()
            .borrow_mut()
            .add_bar(hour_bar("15:00:00"))
            .unwrap();
        let completed = DataResponseEnvelope::terminal(quotes, None);
        warmup.on_data_response(&probe, &completed).unwrap();
        warmup
            .on_historical_trades(&[trade("14:00:30", "14:00:30")])
            .unwrap();
        warmup.complete(&probe).unwrap();

        assert_eq!(
            *log.read.borrow(),
            [
                entry("quote", "14:00:00"),
                entry("bar", "14:00:00"),
                entry("trade", "14:00:30"),
                entry("quote", "14:45:00"),
                entry("trade", "14:50:00"),
                entry("bar", "15:00:00"),
                entry("quote", "15:00:01"),
            ]
        );
    }

    #[rstest]
    #[case::held(true)]
    #[case::after_the_warmup(false)]
    fn a_live_bar_at_or_before_the_historys_last_is_dropped(#[case] held: bool) {
        let log = Rc::new(Log::default());
        let (warmup, probe, commands) = logging_bars(&log);
        let quotes = request_of(&commands, NautilusDataType::QuoteTick);
        answer_quotes(&warmup, &probe, quotes, &[quote("14:00:00")], &[]);
        probe
            .cache_rc()
            .borrow_mut()
            .add_bar(hour_bar("14:00:00"))
            .unwrap();
        warmup
            .on_historical_trades(&[trade("14:00:30", "14:00:30")])
            .unwrap();

        if !held {
            warmup.complete(&probe).unwrap();
        }
        warmup.on_bar(&hour_bar("14:00:00")).unwrap();
        warmup.on_bar(&hour_bar("15:00:00")).unwrap();
        if held {
            warmup.complete(&probe).unwrap();
        }

        let bars: Vec<Entry> = log
            .read
            .borrow()
            .iter()
            .filter(|(kind, _)| *kind == "bar")
            .copied()
            .collect();
        assert_eq!(bars, [entry("bar", "14:00:00"), entry("bar", "15:00:00")]);
    }

    #[rstest]
    fn a_replays_bars_reach_the_features_as_they_come() {
        let log = Rc::new(Log::default());
        let (replayed, _probe, _) = logging_with(
            Warmup::replayed(&bars_and_trades()),
            &bars_and_trades(),
            &log,
        );

        replayed.on_bar(&hour_bar("14:00:00")).unwrap();

        assert_eq!(*log.read.borrow(), [entry("bar", "14:00:00")]);
    }

    #[nautilus_serialization::arrow_custom_data]
    #[nautilus_model::custom_data]
    pub struct Imbalance {
        pub value: f64,
        pub ts_event: UnixNanos,
        pub ts_init: UnixNanos,
    }

    /// Pins that Nautilus' data engine with a catalog answers a request naming an unregistered
    /// client from the catalog alone, and asks no client for what it lacks
    /// (`dispatch_date_range_request`, `crates/data/src/engine/streaming.rs:489-497`).
    /// `custom_request` names `CATALOG` for a custom series no source serves.
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

    /// The error names the warmup, -24h, which jiff writes `24h ago`.
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

        // 720 hours of 1-minute bars: 43,200, plus one spanning the start
        let cache = cache_config(&bar_types, SignedDuration::from_hours(720)).unwrap();
        assert_eq!(cache.bar_capacity, 10_000 + 43_201);
    }

    #[rstest]
    fn a_warmup_the_cache_cannot_hold_is_an_error_naming_the_bars() {
        // 1-second bars over 12 days: 1,036,801 bars, over the 1,000,000 a cache holds
        let bar_types = [BarType::from("SXR8.IBIS-1-SECOND-MID-INTERNAL")];
        let err = cache_config(&bar_types, SignedDuration::from_hours(12 * 24)).unwrap_err();
        let err = format!("{err:#}");
        for part in ["SXR8.IBIS-1-SECOND-MID-INTERNAL", "1036801 bars"] {
            assert!(err.contains(part), "{err}");
        }
    }

    /// Bars built from source bars wait 5 s (5,000,000 us) for the last source bar, as in a
    /// backtest, a dataset run and a live node.
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
