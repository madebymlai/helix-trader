//! Trades: what a strategy sends, closes at its time limit and arms again
//! at a restart, on Nautilus' own orders, cache and clock, knowing nothing
//! of how the strategy decided.
//!
//! A `Trade` is an instrument, a side, a quantity and its `Exits`.
//! `Trade::send` rounds the quantity to the instrument's sizes and sends,
//! with both exits, a Nautilus bracket (`OrderFactory::bracket`: a market
//! entry, a take-profit limit and a stop-market, one-updates-the-other,
//! the legs reduce-only), without them a market entry. A time limit sets a
//! time alert named after the entry, `TIME_ALERT` and its client order id,
//! the time limit after the send, and tags the entry with it
//! (`TIME_LIMIT_TAG`), where the cache, persisted live, keeps it.
//!
//! The time limit runs from the send, the entry's creation (its
//! `ts_init`), not from its fill: a dataset run labels a decision's target
//! from the decision, and on a market entry the two differ by the
//! venue's latency only (none in a backtest).
//!
//! Nautilus delivers time events to the strategy alone: the strategy hands
//! each to `on_time_event`, which at a trade's alert cancels its orders
//! still open, the entry and its legs, and, if the trade is still open,
//! closes its position (`close_position`, tagged `TIME_CLOSE_TAG`, see
//! `is_time_close`). A trade is open while an open position holds its
//! entry's fill and no leg of the entry (an order Nautilus names it the
//! parent of) is filled whole (`OrderStatus::Filled`): a leg partly filled
//! leaves the rest of the trade open, which its time closes. On a hedging
//! venue the position is the one its entry opened; on a netting venue the
//! venue's one position, which a later entry adds into, so closing a
//! trade there closes the trades it holds with it, a later trade's alert
//! finding it closed. A position closed and opened again is a new one,
//! holding none of the earlier fills. The open orders and positions are
//! never purged from the cache, so a dataset run's purge of closed ones
//! loses nothing of an open trade.
//!
//! Nautilus has no time limit on a position, so the alert and its close
//! are ours; at a restart `arm_open_trades`, from `on_start`, sets the
//! alert of each trade again from the cache whose entry is still an open
//! order (as Nautilus' `reactivate_gtd_timers` walks the open orders) or
//! which is still open, at its entry's creation plus the time limit it is
//! tagged with, at once if that has passed.
//! Nautilus' GTD expiry on the bracket's legs does not fit: an expired leg
//! leaves the position open, IC sends no leg's time in force (its legs
//! become the position's stop loss and take profit), and OKX takes GTC
//! orders alone in an order list.

use std::{collections::BTreeSet, str::FromStr};

use anyhow::{Context, bail};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, DataActorNative},
    cache::CacheApi,
    timer::TimeEvent,
};
use nautilus_model::{
    enums::{OrderSide, OrderStatus},
    identifiers::{ClientOrderId, InstrumentId},
    instruments::Instrument,
    orders::{Order, OrderAny},
    position::Position,
    types::{Price, Quantity},
};
use nautilus_trading::strategy::{Strategy, StrategyNative};
use ustr::Ustr;

/// The prefix of a trade's time alert, followed by its entry's client
/// order id.
const TIME_ALERT: &str = "TRADE-TIME:";

/// The prefix of the tag of a trade's entry with its time limit, followed
/// by the limit, `TIME_LIMIT:PT4H`.
const TIME_LIMIT_TAG: &str = "TIME_LIMIT:";

/// The tag of the order closing a trade at its time limit.
const TIME_CLOSE_TAG: &str = "TIME_CLOSE";

/// The tag of a bracket's entry, Nautilus' default (`OrderFactory::bracket`).
const ENTRY_TAG: &str = "ENTRY";

/// A trade to send: `quantity` of `instrument_id` on `side`, ended by its
/// exits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trade {
    pub instrument_id: InstrumentId,
    pub side: OrderSide,
    pub quantity: Quantity,
    pub exits: Exits,
}

/// How a trade ends: at its take profit or its stop loss, both or none,
/// on the instrument's prices, or at its time limit after the send, if it
/// has one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Exits {
    pub take_profit: Option<Price>,
    pub stop_loss: Option<Price>,
    pub time_limit: Option<SignedDuration>,
}

/// The client order ids a trade's orders take, for a caller naming them:
/// the entry's and its legs'.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderIds {
    pub entry: ClientOrderId,
    pub take_profit: ClientOrderId,
    pub stop_loss: ClientOrderId,
}

impl Trade {
    /// Sends the trade from `strategy`, its orders of the factory's ids:
    /// a bracket with both exits, a market entry without them, and its
    /// time alert if it has a time limit.
    ///
    /// # Errors
    ///
    /// Returns an error if one exit is given without the other, the
    /// instrument is not in the cache, the quantity rounds to no size of
    /// it, or Nautilus refuses an order or the alert.
    pub fn send<S>(&self, strategy: &mut S) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        self.submit(strategy, None)
    }

    /// Sends the trade as `send`, its orders named `ids`.
    ///
    /// # Errors
    ///
    /// As `send`.
    pub fn send_as<S>(&self, strategy: &mut S, ids: OrderIds) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        self.submit(strategy, Some(ids))
    }

    fn submit<S>(&self, strategy: &mut S, ids: Option<OrderIds>) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        let instrument_id = self.instrument_id;
        let instrument = strategy
            .cache()
            .instrument(&instrument_id)
            .with_context(|| format!("instrument {instrument_id} not in cache"))?;
        let size = self.quantity.as_decimal();
        let quantity = instrument
            .try_make_qty_from_decimal(size, None)
            // In the message, not as context: Nautilus logs only the top
            // error of the chain.
            .map_err(|e| anyhow::anyhow!("ordering {size} of {instrument_id}: {e}"))?;
        let limit_tag = self
            .exits
            .time_limit
            .map(|limit| Ustr::from(&format!("{TIME_LIMIT_TAG}{limit}")));
        let entry = match (self.exits.take_profit, self.exits.stop_loss) {
            (Some(take_profit), Some(stop_loss)) => {
                let orders = strategy
                    .order()
                    .bracket()
                    .instrument_id(instrument_id)
                    .order_side(self.side)
                    .quantity(quantity)
                    .entry_tags(
                        [Some(Ustr::from(ENTRY_TAG)), limit_tag]
                            .into_iter()
                            .flatten()
                            .collect(),
                    )
                    .maybe_entry_client_order_id(ids.map(|ids| ids.entry))
                    .maybe_tp_client_order_id(ids.map(|ids| ids.take_profit))
                    .maybe_sl_client_order_id(ids.map(|ids| ids.stop_loss))
                    .tp_price(take_profit)
                    .sl_trigger_price(stop_loss)
                    .call();
                // Nautilus' bracket is the entry, then its legs.
                let entry = orders[0].client_order_id();
                strategy.submit_order_list(orders, None, None, None)?;
                entry
            }
            (None, None) => {
                let order = strategy.order().market(
                    instrument_id,
                    self.side,
                    quantity,
                    None,
                    None,
                    None,
                    None,
                    None,
                    limit_tag.map(|tag| vec![tag]),
                    ids.map(|ids| ids.entry),
                );
                let entry = order.client_order_id();
                strategy.submit_order(order, None, None, None)?;
                entry
            }
            _ => bail!(
                "a trade of {instrument_id} has a take profit or a stop loss alone: a \
                 Nautilus bracket has both"
            ),
        };
        match self.exits.time_limit {
            Some(limit) => {
                let at = Timestamp::from(strategy.clock().timestamp_ns()) + limit;
                strategy
                    .clock()
                    .set_time_alert(&time_alert(entry), at, None, None)
            }
            None => Ok(()),
        }
    }
}

/// At the time alert of a trade of `strategy`, closes it: cancels its
/// orders still open, the entry and its legs, and closes its position if
/// the trade is still open, tagged as a time close. Whether `event` is a
/// trade's time alert: another is left to the strategy.
///
/// # Errors
///
/// Returns an error if Nautilus refuses the cancel or the close.
pub fn on_time_event<S>(strategy: &mut S, event: &TimeEvent) -> anyhow::Result<bool>
where
    S: Strategy + StrategyNative + DataActor + DataActorNative,
{
    let Some(entry) = event
        .name
        .strip_prefix(TIME_ALERT)
        .and_then(|entry| ClientOrderId::new_checked(entry).ok())
    else {
        return Ok(false);
    };
    let strategy_id = strategy.strategy_id();
    let (orders, position) = {
        let cache = strategy.cache();
        let orders: Vec<ClientOrderId> = cache
            .orders_open(None, None, strategy_id.as_ref(), None, None)
            .iter()
            .filter(|order| {
                order.client_order_id() == entry || order.parent_order_id() == Some(entry)
            })
            .map(Order::client_order_id)
            .collect();
        let position = cache
            .positions_open(None, None, strategy_id.as_ref(), None, None)
            .into_iter()
            .find(|position| holds_open(&cache, position, entry));
        (orders, position)
    };
    if !orders.is_empty() {
        strategy.cancel_orders(orders, None, None)?;
    }
    if let Some(position) = position {
        let tags = Some(vec![Ustr::from(TIME_CLOSE_TAG)]);
        strategy.close_position(&position, None, tags, None, None, None, None)?;
    }
    Ok(true)
}

/// Sets again, from `strategy`'s start (a restart), the time alert of each
/// of its trades still open, at its entry's creation plus the time limit
/// it is tagged with, or now if that has passed (the clock's
/// `allow_past`): each entry tagged with a time limit whose trade is open,
/// an open order of the strategy (as Nautilus' `reactivate_gtd_timers`
/// walks them), or its fill held by an open position of the strategy and
/// no leg of it filled whole.
///
/// # Errors
///
/// Returns an error if Nautilus refuses an alert.
pub fn arm_open_trades<S>(strategy: &mut S) -> anyhow::Result<()>
where
    S: Strategy + StrategyNative + DataActor + DataActorNative,
{
    let strategy_id = strategy.strategy_id();
    let open: Vec<(ClientOrderId, Timestamp)> = {
        let cache = strategy.cache();
        let unfilled = cache
            .orders_open(None, None, strategy_id.as_ref(), None, None)
            .into_iter()
            .map(|order| order.client_order_id());
        let filled = cache
            .positions_open(None, None, strategy_id.as_ref(), None, None)
            .into_iter()
            .flat_map(|position| {
                position
                    .client_order_ids()
                    .into_iter()
                    .filter(|&entry| holds_open(&cache, &position, entry))
                    .collect::<Vec<_>>()
            });
        let entries: BTreeSet<ClientOrderId> = unfilled.chain(filled).collect();
        entries
            .into_iter()
            .filter_map(|entry| {
                let order = cache.order(&entry)?;
                let limit = time_limit(&order)?;
                Some((entry, Timestamp::from(order.ts_init()) + limit))
            })
            .collect()
    };
    for (entry, at) in open {
        strategy
            .clock()
            .set_time_alert(&time_alert(entry), at, None, None)?;
    }
    Ok(())
}

/// Whether `order` is the close of a trade at its time limit
/// (`on_time_event`).
#[must_use]
pub fn is_time_close(order: &OrderAny) -> bool {
    order
        .tags()
        .is_some_and(|tags| tags.contains(&Ustr::from(TIME_CLOSE_TAG)))
}

/// Whether `position` holds the trade of `entry` open: its entry's fill
/// and no leg of it filled whole, an order of `cache` Nautilus names the
/// entry the parent of (`parent_order_id`) in `OrderStatus::Filled`. A leg
/// partly filled leaves the rest of the trade open in the position.
///
/// Read from the position's fills and the legs' own link, not from the
/// entry's order or `Cache::position_for_order`: Nautilus purges a closed
/// order with its index (`purge_closed_orders`, a dataset run's a minute
/// after it closes) unless an order linked to it is still open, so a
/// bracket's filled entry stays while a leg is open, but a market entry
/// with a time limit, linked to none, goes while its position stays open.
/// Once a trade has ended its legs, closed too, are purged with its entry:
/// a leg filled whole ended the trade on a hedging venue by closing its
/// position. Live keeps closed orders a week (`live::node`).
fn holds_open(cache: &CacheApi<'_>, position: &Position, entry: ClientOrderId) -> bool {
    let filled = position.client_order_ids();
    filled.contains(&entry)
        && !filled.iter().any(|id| {
            cache.order(id).is_some_and(|leg| {
                leg.parent_order_id() == Some(entry) && leg.status() == OrderStatus::Filled
            })
        })
}

/// The time limit `entry` is tagged with, if any.
fn time_limit(entry: &OrderAny) -> Option<SignedDuration> {
    entry
        .tags()?
        .iter()
        .find_map(|tag| SignedDuration::from_str(tag.as_str().strip_prefix(TIME_LIMIT_TAG)?).ok())
}

/// The name of the time alert of the trade of `entry`.
fn time_alert(entry: ClientOrderId) -> String {
    format!("{TIME_ALERT}{entry}")
}

#[cfg(test)]
mod tests;
