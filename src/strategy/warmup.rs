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
//! A strategy calls `start` from `on_start` and `bars` from
//! `on_historical_quotes`.

use std::{collections::BTreeSet, fmt::Debug};

use jiff::SignedDuration;
use nautilus_common::actor::{DataActor, DataActorNative};
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
