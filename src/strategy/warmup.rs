//! The warmup of strategies on bars Nautilus builds from quotes (internal
//! BID, ASK or MID time bars), the same in a backtest and live.
//!
//! Owns Nautilus' protocol, so that no strategy names it. Per instrument it
//! requests the historical quotes since `now - warmup` with the request
//! params `bar_types` (the data engine aggregates the quotes into those
//! bars), `update_subscriptions` (the aggregator goes on with the live
//! quotes, so the bar spanning the warmup end comes out once and complete;
//! without it that bar comes out partial, or as a one-quote bar when the
//! warmup ends on a bar close) and `skip_first_non_full_bar` (the oldest
//! bar, missing the quotes before `now - warmup`, is dropped), then
//! subscribes to the bars. The bars a request builds reach only the cache,
//! never `on_historical_bars`, and are there when the quotes response
//! arrives.
//!
//! The cache keeps at most `bar_capacity` bars per bar type and evicts the
//! oldest beyond it, so the node running the strategy takes its cache
//! config from `cache_config`, which holds the whole warmup.
//!
//! An hour with no quote at all means the market is closed (a night, a
//! weekend, a holiday): the data engine builds no bar for it, in the
//! warmup as after it. Nautilus' default builds one, flat at the last
//! close, which a session on the time of day alone would trade on. So the
//! node running the strategy takes its data engine config from
//! `data_engine_config` (backtest) or `live_data_engine_config` (live),
//! which turn `time_bars_build_with_no_updates` off.
//!
//! An instrument whose history the run cannot serve, as on a venue with
//! no quote history, warms up on live bars instead: no request, and the
//! first bar, partial since it opens at the start, is dropped
//! (`skip_first_non_full_bar` on the subscription). The run tells the
//! strategy which instruments those are through `WarmUp`, before it
//! starts.
//!
//! A strategy calls `start` from `on_start` and `bars` from
//! `on_historical_quotes`.

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
    data::{Bar, BarType, QuoteTick},
    enums::AggregationSource,
    identifiers::InstrumentId,
};
use serde_json::json;

/// A strategy warmed up through `start`, which a run tells, before
/// starting it, which of its instruments have no history to warm up on.
pub trait WarmUp {
    /// Warms `instrument_ids` up on live bars only: the strategy passes
    /// them to `start`, which requests no history for them.
    fn warm_up_on_live_bars(&mut self, instrument_ids: &[InstrumentId]);
}

/// Requests the warmup of `bar_types`, internal bar types, over `warmup`
/// before now, then subscribes `actor` to them. The warmup of each
/// instrument arrives in `on_historical_quotes`, where `bars` returns it.
/// The instruments of `live_bars` get no request: their bars come from the
/// live quotes alone, the first, partial one dropped.
///
/// # Errors
///
/// Returns an error if a request fails.
pub fn start<A>(
    actor: &mut A,
    bar_types: &[BarType],
    warmup: SignedDuration,
    live_bars: &[InstrumentId],
) -> anyhow::Result<()>
where
    A: DataActor + DataActorNative + Debug + 'static,
{
    let start = actor.clock().utc_now().checked_sub(warmup)?;
    let instruments: BTreeSet<InstrumentId> = bar_types
        .iter()
        .map(BarType::instrument_id)
        .filter(|id| !live_bars.contains(id))
        .collect();
    for instrument_id in instruments {
        let of_instrument: Vec<String> = bar_types
            .iter()
            .filter(|bar_type| bar_type.instrument_id() == instrument_id)
            .map(ToString::to_string)
            .collect();
        let mut params = Params::new();
        params.insert("bar_types".to_string(), json!(of_instrument));
        params.insert("update_subscriptions".to_string(), json!(true));
        params.insert("skip_first_non_full_bar".to_string(), json!(true));
        actor.request_quotes(instrument_id, Some(start), None, None, None, Some(params))?;
    }
    for bar_type in bar_types {
        let params = live_bars.contains(&bar_type.instrument_id()).then(|| {
            let mut params = Params::new();
            params.insert("skip_first_non_full_bar".to_string(), json!(true));
            params
        });
        actor.subscribe_bars(*bar_type, None, params);
    }
    Ok(())
}

/// The warmup bars of the instrument of `quotes`, a quotes response to
/// `start`, oldest first.
pub fn bars<A>(actor: &A, quotes: &[QuoteTick]) -> Vec<Bar>
where
    A: DataActor + DataActorNative,
{
    let Some(quote) = quotes.first() else {
        return vec![];
    };
    let cache = actor.cache();
    let mut bars: Vec<Bar> = cache
        .bar_types(
            Some(&quote.instrument_id),
            None,
            AggregationSource::Internal,
        )
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

/// The data engine config of a live node running a strategy: Nautilus'
/// default, without bars for periods without quotes.
#[must_use]
pub fn live_data_engine_config() -> LiveDataEngineConfig {
    LiveDataEngineConfig {
        time_bars_build_with_no_updates: BUILD_BARS_WITHOUT_QUOTES,
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
}
