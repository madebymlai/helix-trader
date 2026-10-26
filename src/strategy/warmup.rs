//! The warmup of strategies on bars Nautilus builds (internal time bars),
//! the same in a backtest and live.
//!
//! Owns Nautilus' protocol, so that no strategy names it. The request is
//! chosen from the bar type alone: a composite bar type is built from its
//! source bars, so their bars since `now - warmup` are requested
//! (`request_bars` of `BarType::composite`); any other bar type is built
//! from quotes (BID, ASK or MID), so the quotes of its instrument are
//! requested (`request_quotes`). Each request carries the params
//! `bar_types` (the data engine aggregates the response into those bars),
//! `update_subscriptions` (the aggregator goes on with the live data, so
//! the bar spanning the warmup end comes out once and complete; without it
//! that bar comes out partial, or as a one-update bar when the warmup ends
//! on a bar close) and `skip_first_non_full_bar` (the oldest bar, missing
//! the data before `now - warmup`, is dropped), then the bars are
//! subscribed to. The bars a request builds reach only the cache: the
//! response carries what was requested, and the built bars are in the
//! cache when it arrives.
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
//! A strategy calls `start` from `on_start` and `bars` from
//! `on_historical_quotes` and `on_historical_bars`.

use std::{collections::BTreeSet, fmt::Debug};

use anyhow::{Context, ensure};
use jiff::SignedDuration;
use nautilus_common::{
    actor::{DataActor, DataActorNative},
    cache::CacheConfig,
};
use nautilus_core::Params;
use nautilus_data::engine::config::DataEngineConfig;
use nautilus_live::config::LiveDataEngineConfig;
use nautilus_model::{
    data::{Bar, BarType, Data},
    enums::AggregationSource,
    identifiers::InstrumentId,
};
use serde_json::json;

/// Requests the warmup of `bar_types`, internal bar types, over `warmup`
/// before now, then subscribes `actor` to them. The bars of a composite bar
/// type are built from its source bars, requested per source bar type; the
/// others from quotes, requested per instrument. The warmup of each request
/// arrives in `on_historical_bars` or `on_historical_quotes`, where `bars`
/// returns it.
///
/// # Errors
///
/// Returns an error if a composite bar type's source bars are not
/// external (built, not requested), or if a request fails.
pub fn start<A>(actor: &mut A, bar_types: &[BarType], warmup: SignedDuration) -> anyhow::Result<()>
where
    A: DataActor + DataActorNative + Debug + 'static,
{
    let start = actor.clock().utc_now().checked_sub(warmup)?;
    let (composites, of_quotes): (Vec<BarType>, Vec<BarType>) = bar_types
        .iter()
        .partition(|bar_type| bar_type.is_composite());
    let sources: BTreeSet<BarType> = composites.iter().map(BarType::composite).collect();
    for source in sources {
        ensure!(
            source.is_externally_aggregated(),
            "no warmup for bars built from {source}: the source bars of a \
             composite bar type must be external"
        );
        let built = composites
            .iter()
            .filter(|bar_type| bar_type.composite() == source);
        actor.request_bars(source, Some(start), None, None, None, Some(params(built)))?;
    }
    let instruments: BTreeSet<InstrumentId> =
        of_quotes.iter().map(BarType::instrument_id).collect();
    for instrument_id in instruments {
        let built = of_quotes
            .iter()
            .filter(|bar_type| bar_type.instrument_id() == instrument_id);
        actor.request_quotes(
            instrument_id,
            Some(start),
            None,
            None,
            None,
            Some(params(built)),
        )?;
    }
    for bar_type in bar_types {
        actor.subscribe_bars(*bar_type, None, None);
    }
    Ok(())
}

/// The params of a warmup request building `bar_types`.
fn params<'a>(bar_types: impl Iterator<Item = &'a BarType>) -> Params {
    let bar_types: Vec<String> = bar_types.map(ToString::to_string).collect();
    Params::new(
        [
            ("bar_types".to_string(), json!(bar_types)),
            ("update_subscriptions".to_string(), json!(true)),
            ("skip_first_non_full_bar".to_string(), json!(true)),
        ]
        .into(),
    )
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
const SOURCE_BAR_DELAY_MICROS: u64 = 5_000_000;

/// The data engine config of a live node running a strategy on
/// `bar_types`: Nautilus' default, without bars for periods without
/// quotes, and, if any bar type is composite, building time bars
/// `SOURCE_BAR_DELAY_MICROS` after their close (`time_bars_build_delay`),
/// so that the last source bar is in.
#[must_use]
pub fn live_data_engine_config(bar_types: &[BarType]) -> LiveDataEngineConfig {
    let delay = if bar_types.iter().any(BarType::is_composite) {
        SOURCE_BAR_DELAY_MICROS
    } else {
        0
    };
    LiveDataEngineConfig {
        time_bars_build_with_no_updates: BUILD_BARS_WITHOUT_QUOTES,
        time_bars_build_delay: delay,
        ..LiveDataEngineConfig::default()
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
}
