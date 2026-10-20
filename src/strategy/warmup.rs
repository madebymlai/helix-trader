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
use nautilus_model::{
    data::{Bar, BarType, QuoteTick},
    enums::AggregationSource,
    identifiers::InstrumentId,
};
use serde_json::json;

/// Requests the warmup of `bar_types`, internal bar types, over `warmup`
/// before now, then subscribes `actor` to them. The warmup of each
/// instrument arrives in `on_historical_quotes`, where `bars` returns it.
///
/// # Errors
///
/// Returns an error if a request fails.
pub fn start<A>(actor: &mut A, bar_types: &[BarType], warmup: SignedDuration) -> anyhow::Result<()>
where
    A: DataActor + DataActorNative + Debug + 'static,
{
    let start = actor.clock().utc_now().checked_sub(warmup)?;
    let instruments: BTreeSet<InstrumentId> =
        bar_types.iter().map(BarType::instrument_id).collect();
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
        actor.subscribe_bars(*bar_type, None, None);
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
