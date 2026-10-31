//! The warmup of strategies on bars Nautilus builds (internal time bars) on
//! a live node: a warmup request at start, which hydrates the strategy's
//! indicators before the live stream. A backtest or a dataset run makes no
//! request: its strategy only subscribes (`subscribe`), and its indicators
//! warm on the data replayed, from the run's start. Either way a strategy
//! decides once its indicators are initialized (`indicators_initialized`),
//! never on the end of a warmup.
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
//! reach only the cache: the response carries what was requested, and the
//! built bars are in the cache when it arrives; Nautilus delivers no built
//! bars.
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
//! which turn `time_bars_build_with_no_updates` off.
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
//! A strategy holds a `Warmup`, which runs these calls, and delegates its
//! hooks to it; a model strategy's `ModelHandle` builds and owns it. The
//! functions are its parts: `start` from `on_start`, `bars` from
//! `on_historical_quotes`, `on_historical_trades` and `on_historical_bars`,
//! and `finished` from `on_data_response`, then, on bars on a root,
//! `continuous_bars`; in a backtest or a dataset run `subscribe` from
//! `on_start` alone.

use std::{cell::Cell, collections::BTreeMap, fmt::Debug};

use anyhow::{Context, bail, ensure};
use jiff::SignedDuration;
use nautilus_common::{
    actor::{DataActor, DataActorNative, indicators::SharedActorIndicator},
    cache::CacheConfig,
    messages::data::DataResponseEnvelope,
};
use nautilus_core::{Params, UUID4, UnixNanos};
use nautilus_data::engine::config::DataEngineConfig;
use nautilus_live::config::LiveDataEngineConfig;
use nautilus_model::{
    data::{Bar, BarType, Data, NautilusDataType},
    enums::AggregationSource,
    identifiers::Venue,
};
use serde_json::{Value, json};

use crate::{
    rolls::ContinuousBars,
    strategy::{DataRequirements, Source, bar_sources},
};

/// A strategy's warmup, built from its data requirements for a live node
/// (`live`) or a replay (`replayed`, a backtest or a dataset run): the one
/// owner of the protocol above, so that a strategy holds one and its hooks
/// delegate to it. Nautilus delivers historical data only to the
/// strategy's own hooks, so a strategy calls `start` from `on_start`,
/// `on_data_response` from `on_data_response`, and `on_historical` from
/// `on_historical_quotes`, `on_historical_trades` and `on_historical_bars`.
///
/// Live, `start` requests the warmup of the bar types the strategy reads
/// and of those the strategy's harness needs warm (`building`, the
/// barrier's ATR's), and subscribes to them from the warmup's end; the
/// bars of each response go to the indicators the warmup feeds
/// (`feeding`, `building`), oldest first. On a replay `start` subscribes
/// to the strategy's bar types alone: the harness subscribes to its own.
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
    /// What the warmup's bars are handed to.
    indicators: Vec<SharedActorIndicator>,
    /// The request group of the last live start.
    group: Cell<UUID4>,
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
            indicators: Vec::new(),
            group: Cell::new(UUID4::new()),
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
                start(
                    actor,
                    &self.built(),
                    self.length,
                    &continuous,
                    self.group.get(),
                )
            }
            Start::Subscribe => {
                subscribe(actor, &self.bar_types, &continuous);
                Ok(())
            }
        }
    }

    /// Logs the end of the warmup, if `response` ends it, from the actor's
    /// `on_data_response`.
    ///
    /// # Errors
    ///
    /// Returns an error naming the reason if the warmup ended incomplete
    /// (see `finished`), which Nautilus logs.
    pub fn on_data_response(&self, response: &DataResponseEnvelope) -> anyhow::Result<()> {
        if finished(self.group.get(), response)? {
            log::info!("Warmup complete");
        }
        Ok(())
    }

    /// Hands the warmup bars of `response`, the quotes, trades or bars of
    /// a response to `actor`'s warmup request, to the indicators the
    /// warmup feeds, oldest first, from the actor's `on_historical_quotes`,
    /// `on_historical_trades` and `on_historical_bars`.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator fails to read a bar.
    pub fn on_historical<A, D>(&self, actor: &A, response: &[D]) -> anyhow::Result<()>
    where
        A: DataActor + DataActorNative,
        D: Copy + Into<Data>,
    {
        for bar in bars(actor, response) {
            for indicator in &self.indicators {
                indicator.handle_bar(&bar)?;
            }
        }
        Ok(())
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

    /// How long after a bar's close a live node builds the strategy's
    /// bars (see `live_bar_build_delay`).
    #[must_use]
    pub fn live_bar_build_delay(&self) -> SignedDuration {
        live_bar_build_delay(&self.bar_types)
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
            .field("indicators", &self.indicators.len())
            .field("group", &self.group)
            .finish()
    }
}

/// Requests the warmup of `bar_types`, internal bar types, over
/// `[now - warmup, now]` as the request group `group`, then subscribes
/// `actor` to them from `now`. The bars on a root with continuous bars in
/// `continuous` are its continuous future, requested per bar type with
/// their `continuous_future_*` params; the others are requested per series
/// they are built from (see `requests`): the source bars of a composite bar
/// type, the trades of the instrument of LAST bars, the quotes of the
/// instrument of BID, ASK and MID bars. The warmup of each request arrives
/// in `on_historical_bars`, `on_historical_trades` or
/// `on_historical_quotes`, where `bars` returns it; the end of the group
/// in `on_data_response`, where `finished` tells it. `group` is the
/// caller's, stored before the call: a group whose requests are all
/// answered at once ends before this returns. No bar types make no
/// request and no group, which `DataActor::request_group` refuses empty: a
/// strategy reading ticks alone has no warmup.
///
/// # Errors
///
/// Returns an error if a composite bar type's source bars are not
/// external (built, not requested), or if a request fails.
pub fn start<A>(
    actor: &mut A,
    bar_types: &[BarType],
    warmup: SignedDuration,
    continuous: &ContinuousBars,
    group: UUID4,
) -> anyhow::Result<()>
where
    A: DataActor + DataActorNative + Debug + 'static,
{
    let now = actor.clock().utc_now();
    let start = now.checked_sub(warmup)?;
    for source in bar_types
        .iter()
        .filter(|bar_type| bar_type.is_composite())
        .map(BarType::composite)
    {
        ensure!(
            source.is_externally_aggregated(),
            "no warmup for bars built from {source}: the source bars of a \
             composite bar type must be external"
        );
    }
    let (continuous_bars, others): (Vec<BarType>, Vec<BarType>) = bar_types
        .iter()
        .partition(|bar_type| continuous.params(bar_type.instrument_id()).is_some());
    let mut requested = Vec::new();
    for bar_type in &continuous_bars {
        let params = continuous.params(bar_type.instrument_id());
        requested.push(actor.request_bars(
            *bar_type,
            Some(start),
            Some(now),
            None,
            None,
            Some(request_params([bar_type].into_iter(), params)),
        )?);
    }
    for (source, built) in requests(&others) {
        let params = Some(request_params(built.iter(), None));
        requested.push(match source {
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
                data => unreachable!("bars are built from quotes or trades, not {data}"),
            },
        });
    }
    if !requested.is_empty() {
        actor.request_group(requested, Some(group))?;
    }
    subscribe_from(actor, bar_types, continuous, UnixNanos::from(now));
    Ok(())
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
        actor.subscribe_bars(bar_type, None, Some(subscription_params(now, params)));
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
        actor.subscribe_bars(venue_bar_types, None, Some(subscription_params(now, None)));
    }
}

/// The warmup requests of `bar_types`, internal bar types on no root: one
/// per series they are built from, as Nautilus' data engine builds them
/// (see `strategy::catalog_series`), with the bar types built from it, in
/// the order the series first come.
fn requests(bar_types: &[BarType]) -> Vec<(Source, Vec<BarType>)> {
    let mut requests: Vec<(Source, Vec<BarType>)> = Vec::new();
    for (source, bar_type) in bar_sources(bar_types).zip(bar_types) {
        match requests.iter_mut().find(|(of, _)| *of == source) {
            Some((_, built)) => built.push(*bar_type),
            None => requests.push((source, vec![*bar_type])),
        }
    }
    requests
}

/// The params of a warmup request building `bar_types`, with the
/// `continuous` params of their root, if on one.
fn request_params<'a>(
    bar_types: impl Iterator<Item = &'a BarType>,
    continuous: Option<[(String, Value); 2]>,
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
        .chain(continuous.into_iter().flatten())
        .collect(),
    )
}

/// The params of the subscription to the bars warmed up to `now`: the live
/// stream starts there. With the `continuous` params of their root, if on
/// one.
fn subscription_params(now: UnixNanos, continuous: Option<[(String, Value); 2]>) -> Params {
    Params::new(
        [("start_ns".to_string(), json!(now.as_u64()))]
            .into_iter()
            .chain(continuous.into_iter().flatten())
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

/// The warmup bars built from `response`, the bars, trades or quotes of a
/// response to `start`, oldest first: those of the bar types of its
/// instrument built from its series (see `built_from`).
pub fn bars<A, D>(actor: &A, response: &[D]) -> Vec<Bar>
where
    A: DataActor + DataActorNative,
    D: Copy + Into<Data>,
{
    let Some(&first) = response.first() else {
        return vec![];
    };
    let first: Data = first.into();
    let instrument_id = first.instrument_id();
    let cache = actor.cache();
    let cached = cache.bar_types(Some(&instrument_id), None, AggregationSource::Internal);
    let mut bars: Vec<Bar> = built_from(&cached, first)
        .iter()
        .flat_map(|bar_type| cache.bars(bar_type).unwrap_or_default())
        .collect();
    // The cache holds the newest first.
    bars.sort_by_key(|bar| bar.ts_event);
    bars
}

/// Those of `bar_types` built from the series of `response`, the first
/// datum of a response to `start`: its catalog series (see
/// `strategy::catalog_series`), the bar type of source bars, the data type
/// and instrument of ticks. Another response of the warmup on the same
/// instrument builds the others.
fn built_from(bar_types: &[BarType], response: Data) -> Vec<BarType> {
    let series = match response {
        Data::Bar(bar) => (NautilusDataType::Bar, bar.bar_type.to_string()),
        data => (
            NautilusDataType::from_data(&data),
            data.instrument_id().to_string(),
        ),
    };
    bar_types
        .iter()
        .zip(bar_sources(bar_types))
        .filter(|(_, source)| source.series() == series)
        .map(|(bar_type, _)| *bar_type)
        .collect()
}

/// The warmup bars of those of `bar_types` on a root with continuous bars
/// in `continuous`, oldest first. Nautilus builds a continuous future's
/// warmup into the cache only and answers its request with no bars, so
/// `bars` gets none of them: a strategy reads them here once `finished`
/// tells the group ended. A backtest's group ends in `start`, before any
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

/// The data engine config of a backtest running a strategy: Nautilus'
/// default, without bars for periods without quotes.
#[must_use]
pub fn data_engine_config() -> DataEngineConfig {
    DataEngineConfig {
        time_bars_build_with_no_updates: BUILD_BARS_WITHOUT_QUOTES,
        ..DataEngineConfig::default()
    }
}

/// How long, in microseconds, a live node waits after a bar's close before
/// building a bar of source bars: a venue sends a source bar once it has
/// closed, so its last source bar arrives after the bar's close (OKX's
/// confirmed 1-minute candles: 0.3 to 1.5 s, measured 2026-10-22; IB's
/// keepUpToDate 1-minute bars: 0.4 to 1.3 s, measured 2026-10-26).
const SOURCE_BAR_DELAY_MICROS: i64 = 5_000_000;

/// The data engine config of a live node running a strategy on
/// `bar_types`: Nautilus' default, without bars for periods without
/// quotes, and building time bars `live_bar_build_delay` after their close
/// (`time_bars_build_delay`).
#[must_use]
pub fn live_data_engine_config(bar_types: &[BarType]) -> LiveDataEngineConfig {
    let delay = live_bar_build_delay(bar_types).unsigned_abs().as_micros();
    LiveDataEngineConfig {
        time_bars_build_with_no_updates: BUILD_BARS_WITHOUT_QUOTES,
        time_bars_build_delay: u64::try_from(delay).expect("a delay of seconds"),
        ..LiveDataEngineConfig::default()
    }
}

/// How long after a bar's close a live node running a strategy on
/// `bar_types` builds its time bars: `SOURCE_BAR_DELAY_MICROS` if any bar
/// type is composite, so that the last source bar is in, else none. A
/// backtest builds every bar at its close, the source bars being in the
/// catalog already.
#[must_use]
pub fn live_bar_build_delay(bar_types: &[BarType]) -> SignedDuration {
    if bar_types.iter().any(BarType::is_composite) {
        SignedDuration::from_micros(SOURCE_BAR_DELAY_MICROS)
    } else {
        SignedDuration::ZERO
    }
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
    use std::rc::Rc;

    use nautilus_common::actor::indicators::ActorIndicator;
    use nautilus_model::{
        data::{QuoteTick, TradeTick},
        identifiers::InstrumentId,
    };

    use super::*;
    use crate::strategy::TickSeries;

    const DAY: SignedDuration = SignedDuration::from_hours(24);

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

    /// The requirements of a strategy reading `bar_types` over a day.
    fn reading(bar_types: &[&str]) -> DataRequirements {
        DataRequirements {
            bar_types: bar_types.iter().map(|&b| BarType::from(b)).collect(),
            ticks: vec![],
            warmup: DAY,
        }
    }

    /// The harness's bars are built after the strategy's, once, and not
    /// again if the strategy reads them.
    #[test]
    fn a_warmup_builds_the_harness_bars_once_after_the_strategys() {
        let minutes = BarType::from("SXR8.IBIS-1-MINUTE-MID-INTERNAL");
        let hours = BarType::from("SXR8.IBIS-1-HOUR-MID-INTERNAL");
        let inert = || Rc::new(Inert) as SharedActorIndicator;

        let warmup = Warmup::live(&reading(&["SXR8.IBIS-1-HOUR-MID-INTERNAL"]))
            .building(minutes, inert())
            .building(minutes, inert())
            .building(hours, inert());

        assert_eq!(warmup.built(), vec![hours, minutes]);
    }

    /// A root's ticks, as any ticks, have no warmup of their own: a live
    /// start requests the history of the bars alone, none of the root nor
    /// of its contracts, the one in front or the next.
    #[test]
    fn a_live_warmup_requests_no_history_of_a_roots_ticks() {
        let requirements = DataRequirements {
            ticks: vec![TickSeries {
                data: NautilusDataType::TradeTick,
                instrument_id: InstrumentId::from("GC.GLBX"),
            }],
            ..reading(&["XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL"])
        };

        let warmup = Warmup::live(&requirements);

        assert_eq!(
            requests(&warmup.built()),
            [(
                Source::Ticks(TickSeries {
                    data: NautilusDataType::QuoteTick,
                    instrument_id: InstrumentId::from("XAUUSD.ICMARKETS"),
                }),
                vec![BarType::from("XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL")],
            )]
        );
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
        let hour_last = BarType::from("ESM4.CME-1-HOUR-LAST-INTERNAL");
        let minute_last = BarType::from("ESM4.CME-1-MINUTE-LAST-INTERNAL");
        let hour_mid = BarType::from("ESM4.CME-1-HOUR-MID-INTERNAL");
        let composite = BarType::from("ESM4.CME-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL");
        let ticks = |data, instrument_id: &str| {
            Source::Ticks(TickSeries {
                data,
                instrument_id: InstrumentId::from(instrument_id),
            })
        };

        let requested = requests(&[hour_last, hour_mid, composite, minute_last]);

        assert_eq!(
            requested,
            vec![
                (
                    ticks(NautilusDataType::TradeTick, "ESM4.CME"),
                    vec![hour_last, minute_last]
                ),
                (
                    ticks(NautilusDataType::QuoteTick, "ESM4.CME"),
                    vec![hour_mid]
                ),
                (
                    Source::Bars(BarType::from("ESM4.CME-1-MINUTE-LAST-EXTERNAL")),
                    vec![composite]
                ),
            ]
        );
    }

    /// A response hands over the bars built from it alone: on an
    /// instrument with LAST and MID bars, a trades response the LAST bars,
    /// a quotes response the MID bars, so neither feeds the other's twice.
    #[test]
    fn a_response_hands_over_the_bars_built_from_its_series_alone() {
        let hour_last = BarType::from("ESM4.CME-1-HOUR-LAST-INTERNAL");
        let hour_mid = BarType::from("ESM4.CME-1-HOUR-MID-INTERNAL");
        let composite = BarType::from("ESM4.CME-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL");
        let cached = [hour_last, hour_mid, composite];
        let instrument_id = InstrumentId::from("ESM4.CME");
        let trade = TradeTick {
            instrument_id,
            ..TradeTick::default()
        };
        let quote = QuoteTick {
            instrument_id,
            ..QuoteTick::default()
        };
        let minute = Bar {
            bar_type: BarType::from("ESM4.CME-1-MINUTE-LAST-EXTERNAL"),
            ..Bar::default()
        };

        assert_eq!(built_from(&cached, trade.into()), [hour_last]);
        assert_eq!(built_from(&cached, quote.into()), [hour_mid]);
        assert_eq!(built_from(&cached, minute.into()), [composite]);
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
        for part in ["SXR8.IBIS-1-SECOND-MID-INTERNAL", "1036801 bars", "1000000"] {
            assert!(err.contains(part), "{err}");
        }
    }

    #[test]
    fn live_bars_built_from_quotes_close_on_time() {
        let bar_types = [BarType::from("SXR8.IBIS-1-HOUR-MID-INTERNAL")];
        let config = live_data_engine_config(&bar_types);
        assert_eq!(config.time_bars_build_delay, 0);
        assert!(!config.time_bars_build_with_no_updates);
    }

    #[test]
    fn live_bars_built_from_source_bars_wait_for_the_last_one() {
        let bar_types = [BarType::from(
            "BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
        )];
        let config = live_data_engine_config(&bar_types);
        assert_eq!(config.time_bars_build_delay, 5_000_000);
        assert!(!config.time_bars_build_with_no_updates);
    }

    /// The delay a live decision waits for is the one the node builds its
    /// bars with.
    #[test]
    fn the_live_bar_build_delay_is_the_live_nodes() {
        let composite = [
            BarType::from("SXR8.IBIS-1-HOUR-MID-INTERNAL"),
            BarType::from("BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"),
        ];
        let quotes = [BarType::from("SXR8.IBIS-1-HOUR-MID-INTERNAL")];

        assert_eq!(
            live_bar_build_delay(&composite),
            SignedDuration::from_secs(5)
        );
        assert_eq!(live_bar_build_delay(&quotes), SignedDuration::ZERO);
    }
}
