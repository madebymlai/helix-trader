//! The warmup of strategies on bars Nautilus builds (internal time bars),
//! the same in a backtest and live.
//!
//! Owns Nautilus' protocol, so that no strategy names it. The request is
//! chosen from the bar type alone: a composite bar type is built from its
//! source bars, so their bars over `[now - warmup, now]` are requested
//! (`request_bars` of `BarType::composite`); any other bar type is built
//! from quotes (BID, ASK or MID), so the quotes of its instrument are
//! requested (`request_quotes`), over the same window.
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
//! client certifies complete, as IC's does; a backtest's catalog answers
//! are never written back). The bars a request builds reach only the
//! cache: the response carries what was requested, and the built bars are
//! in the cache when it arrives; Nautilus delivers no built bars.
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
//! oldest beyond it, so the node running the strategy takes its cache
//! config from `cache_config`, which holds the whole warmup.
//!
//! An hour with no quote or source bar at all means the market is closed
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
//! A strategy calls `start` from `on_start`, `bars` from
//! `on_historical_quotes` and `on_historical_bars`, and `finished` from
//! `on_data_response`, then, on bars on a root, `continuous_bars`.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
};

use anyhow::{Context, bail, ensure};
use jiff::SignedDuration;
use nautilus_common::{
    actor::{DataActor, DataActorNative},
    cache::CacheConfig,
    messages::data::DataResponseEnvelope,
};
use nautilus_core::{Params, UUID4, UnixNanos};
use nautilus_data::engine::config::DataEngineConfig;
use nautilus_live::config::LiveDataEngineConfig;
use nautilus_model::{
    data::{Bar, BarType, Data},
    enums::AggregationSource,
    identifiers::{InstrumentId, Venue},
};
use serde_json::{Value, json};

use crate::rolls::ContinuousBars;

/// Requests the warmup of `bar_types`, internal bar types, over
/// `[now - warmup, now]` as the request group `group`, then subscribes
/// `actor` to them from `now`. The bars of a composite bar type are built
/// from its source bars, requested per source bar type; the bars on a root
/// with continuous bars in `continuous` are its continuous future,
/// requested per bar type with their `continuous_future_*` params; the
/// others are built from quotes, requested per instrument. The warmup of
/// each request arrives in `on_historical_bars` or `on_historical_quotes`,
/// where `bars` returns it; the end of the group in `on_data_response`,
/// where `finished` tells it. `group` is the caller's, stored before the
/// call: a group whose requests are all answered at once ends before this
/// returns.
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
    let (composites, others): (Vec<BarType>, Vec<BarType>) = bar_types
        .iter()
        .partition(|bar_type| bar_type.is_composite());
    let (continuous_bars, of_quotes): (Vec<BarType>, Vec<BarType>) = others
        .into_iter()
        .partition(|bar_type| continuous.params(bar_type.instrument_id()).is_some());
    let sources: BTreeSet<BarType> = composites.iter().map(BarType::composite).collect();
    let mut requests = Vec::new();
    for source in sources {
        ensure!(
            source.is_externally_aggregated(),
            "no warmup for bars built from {source}: the source bars of a \
             composite bar type must be external"
        );
        let built = composites
            .iter()
            .filter(|bar_type| bar_type.composite() == source);
        requests.push(actor.request_bars(
            source,
            Some(start),
            Some(now),
            None,
            None,
            Some(request_params(built, None)),
        )?);
    }
    for bar_type in &continuous_bars {
        let params = continuous.params(bar_type.instrument_id());
        requests.push(actor.request_bars(
            *bar_type,
            Some(start),
            Some(now),
            None,
            None,
            Some(request_params([bar_type].into_iter(), params)),
        )?);
    }
    let instruments: BTreeSet<InstrumentId> =
        of_quotes.iter().map(BarType::instrument_id).collect();
    for instrument_id in instruments {
        let built = of_quotes
            .iter()
            .filter(|bar_type| bar_type.instrument_id() == instrument_id);
        requests.push(actor.request_quotes(
            instrument_id,
            Some(start),
            Some(now),
            None,
            None,
            Some(request_params(built, None)),
        )?);
    }
    actor.request_group(requests, Some(group))?;
    let now_ns = UnixNanos::from(now);
    // One subscription per continuous bar type: Nautilus takes the params
    // of a continuous future per bar type.
    for &bar_type in &continuous_bars {
        let params = continuous.params(bar_type.instrument_id());
        actor.subscribe_bars(bar_type, None, Some(subscription_params(now_ns, params)));
    }
    // One subscription per venue for the others: Nautilus routes a list of
    // bar types to the client of their one venue.
    let mut by_venue: BTreeMap<Venue, Vec<BarType>> = BTreeMap::new();
    let others = bar_types
        .iter()
        .filter(|bar_type| !continuous_bars.contains(bar_type));
    for bar_type in others {
        by_venue
            .entry(bar_type.instrument_id().venue)
            .or_default()
            .push(*bar_type);
    }
    for venue_bar_types in by_venue.into_values() {
        actor.subscribe_bars(
            venue_bar_types,
            None,
            Some(subscription_params(now_ns, None)),
        );
    }
    Ok(())
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

/// The warmup bars of the instrument of `response`, the bars or quotes of
/// a response to `start`, oldest first.
pub fn bars<A, D>(actor: &A, response: &[D]) -> Vec<Bar>
where
    A: DataActor + DataActorNative,
    D: Copy + Into<Data>,
{
    let Some(&first) = response.first() else {
        return vec![];
    };
    let instrument_id = first.into().instrument_id();
    let cache = actor.cache();
    let mut bars: Vec<Bar> = cache
        .bar_types(Some(&instrument_id), None, AggregationSource::Internal)
        .iter()
        .flat_map(|bar_type| cache.bars(bar_type).unwrap_or_default())
        .collect();
    // The cache holds the newest first.
    bars.sort_by_key(|bar| bar.ts_event);
    bars
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

/// The cache config of a node whose strategy warms up on `bar_types` over
/// `warmup`: Nautilus' default, its bar capacity raised by the most bars
/// the warmup builds of one bar type. The default capacity stays on top as
/// headroom for the bars built while the warmup is pending.
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
    use super::*;

    const DAY: SignedDuration = SignedDuration::from_hours(24);

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
