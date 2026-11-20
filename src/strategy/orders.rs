//! Trades: what a strategy sends, closes at its time limit and arms again at a restart, on
//! Nautilus' own orders, cache and clock.
//!
//! A `Trade` is an instrument, a side, its `Exits` and its one R, the distance in price from
//! the exits' reference price to the stop. `Trade::send` sizes it at a `Stake` with Nautilus'
//! fixed-risk sizer over its one R (`Trade::size`; `Trade::send_at` at a fixed equity, a dataset
//! run's balance) and tags the entry with its R in money and the multiple (`entry_risk`). With
//! a `Bracket` it sends a Nautilus bracket, the legs reduce-only and the take profit post-only
//! where the broker takes such orders (`venues::takes_reduce_only_and_post_only`), in a
//! backtest as live; without one, a market entry. A time limit sets a time alert named after
//! the entry (`TIME_ALERT`) and tags the entry with the limit (`TIME_LIMIT_TAG`), which the
//! cache, persisted live, keeps.
//!
//! The limit runs from the send (the entry's `ts_init`), not the fill: a dataset run labels a
//! target from the decision, and on a market entry the two differ by the venue's latency only.
//!
//! Time events reach the strategy alone, which hands each to `on_time_event`. At a trade's
//! alert that cancels the open orders and, if the trade is open, closes the position without
//! waiting for the cancels (`close_position`, reduce-only, tagged `TIME_CLOSE_TAG`, see
//! `is_time_close` and `venues/ib/README.md` for the risk on IB). A trade is open while an open
//! position holds its entry's fill and no leg (an order Nautilus names a child of the entry) is
//! filled whole; a partly filled leg leaves the rest open. On a hedging venue the position is
//! the entry's own. On a netting venue it is the venue's one, so closing a trade closes the
//! trades it holds and a later alert finds it closed. A position closed and reopened is a new
//! one, with none of the earlier fills. Open orders and positions are never purged from the
//! cache.
//!
//! Nautilus has no time limit on a position, so the alert and close are ours, and
//! `arm_open_trades` sets the alerts again at a restart. Nautilus' GTD expiry on the legs does
//! not fit: an expired leg leaves the position open, IC sends no leg's time in force (its legs
//! become the position's stop loss and take profit), and OKX takes GTC orders alone in an order
//! list. OKX denies a bracket whole, its stop loss a stop order in an order list, until
//! nautilus_trader#5668 is fixed.

use std::{cmp::Ordering, collections::BTreeSet, str::FromStr};

use anyhow::Context;
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, DataActorNative},
    cache::CacheApi,
    timer::TimeEvent,
};
use nautilus_model::{
    accounts::Account,
    enums::{OrderSide, OrderStatus},
    identifiers::{AccountId, ClientOrderId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
    orders::{Order, OrderAny},
    position::Position,
    types::{Money, Price, Quantity},
};
use nautilus_risk::sizing::calculate_fixed_risk_position_size;
use nautilus_trading::strategy::{Strategy, StrategyNative};
use rust_decimal::{Decimal, prelude::ToPrimitive};
use ustr::Ustr;

use super::{conversions::Rates, risk::Stake};
use crate::venues::{self, EquitySource};

const TIME_ALERT: &str = "TRADE-TIME:";

/// Tag prefix of a trade's time limit, `TIME_LIMIT:PT4H`.
const TIME_LIMIT_TAG: &str = "TIME_LIMIT:";

const TIME_CLOSE_TAG: &str = "TIME_CLOSE";

/// The tag of a bracket's entry, Nautilus' default (`OrderFactory::bracket`).
const ENTRY_TAG: &str = "ENTRY";

/// Tag prefix of an entry's R in money, `R:500.00 USD` (`entry_risk`).
const R_TAG: &str = "R:";

/// Tag prefix of an entry's stake multiple, `MULTIPLE:2` (`entry_risk`).
const MULTIPLE_TAG: &str = "MULTIPLE:";

/// A trade to send: of `instrument_id` on `side`, one R of it spanning `one_r` in price from
/// `reference`, the price its exits are set from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trade {
    instrument_id: InstrumentId,
    side: OrderSide,
    reference: Price,
    one_r: Decimal,
    exits: Exits,
}

/// What an entry sent at a stake risks, as its tags say (`entry_risk`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryRisk {
    /// Its R in money, in the instrument's P&L currency.
    pub r: Money,
    /// The multiple of the config's risk it was staked at.
    pub multiple: Decimal,
}

/// How a trade ends: at a bracket exit or at its time limit after the send.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Exits {
    pub bracket: Option<Bracket>,
    pub time_limit: Option<SignedDuration>,
}

/// A trade's take profit and stop loss prices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bracket {
    pub take_profit: Price,
    pub stop_loss: Price,
}

/// Rounds `price` to the nearer of `instrument`'s ticks around it. A tie goes the way `away`
/// points from the decision's price (1 up, -1 down), so an exit is not nearer than its
/// distance. Nautilus' `make_price` rounds to the precision alone, which is no tick when the
/// tick is coarser.
///
/// # Errors
///
/// Returns an error if `price` is outside the instrument's prices.
pub fn nearest_tick(
    instrument: &InstrumentAny,
    price: Decimal,
    away: Decimal,
) -> anyhow::Result<Price> {
    let id = instrument.id();
    let value = price
        .to_f64()
        .with_context(|| format!("the exit {price} of {id}"))?;
    let (Some(below), Some(above)) = (
        instrument.next_bid_price(value, 0),
        instrument.next_ask_price(value, 0),
    ) else {
        anyhow::bail!("the exit {price} is outside the prices of {id}");
    };
    let (down, up) = (price - below.as_decimal(), above.as_decimal() - price);
    Ok(match down.cmp(&up) {
        Ordering::Less => below,
        Ordering::Greater => above,
        Ordering::Equal if away.is_sign_positive() => above,
        Ordering::Equal => below,
    })
}

/// The client order ids of a trade's entry and legs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderIds {
    pub entry: ClientOrderId,
    pub take_profit: ClientOrderId,
    pub stop_loss: ClientOrderId,
}

impl Trade {
    /// Creates a trade ended by `exits`, one R of it spanning `one_r` from `reference`: the
    /// distance to its stop, or an ATR's for a trade ended by its time alone.
    #[must_use]
    pub const fn new(
        instrument_id: InstrumentId,
        side: OrderSide,
        reference: Price,
        one_r: Decimal,
        exits: Exits,
    ) -> Self {
        Self {
            instrument_id,
            side,
            reference,
            one_r,
            exits,
        }
    }

    /// Sends the trade at `stake` of the account's equity: a bracket with a `Bracket`, a market
    /// entry without, and its time alert if it has a time limit.
    ///
    /// The equity is of the account the risk engine checks orders against (`routed_account`),
    /// as its broker reports it (`equity`), converted into the P&L currency at `rates`. Live on
    /// IB it is net liquidation, which counts every position of the account, on any venue and
    /// of any process; otherwise positions on the broker's other venues are left out.
    ///
    /// It sends nothing, and logs why, if the account holds no equity, `rates` do not convert
    /// it, or the size rounds down to none. The risk engine denies a quantity below the
    /// instrument's minimum or a margin beyond the free balance.
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument is not in the cache or is inverse (as `refuse_inverse`
    /// refuses it), the equity converted overflows, its stop is outside its prices, Nautilus fails
    /// to size it, or Nautilus refuses an order or the alert.
    pub fn send<S>(&self, strategy: &mut S, stake: Stake, rates: Rates) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        let instrument_id = self.instrument_id;
        let venue = instrument_id.venue;
        let instrument = linear(strategy, instrument_id)?;
        let currency = instrument.cost_currency();
        let account = routed_account(strategy, venue);
        let named = || account.map_or_else(|| format!("of {venue}"), |account| account.to_string());
        let held = equity(strategy, venue, account);
        if held.is_empty() {
            log::error!(
                "not sending the {} of {instrument_id} at a stake of {stake}: the account {} \
                 holds no equity, the cache holding no balance of it",
                self.side,
                named()
            );
            return Ok(());
        }
        let converted = rates.convert(&strategy.cache_ref(), held.iter().copied(), currency);
        let Some(equity) = converted? else {
            let held: Vec<String> = held.iter().map(ToString::to_string).collect();
            log::error!(
                "not sending the {} of {instrument_id} at a stake of {stake}: the account {} \
                 holds [{}], not converted into {currency}, the currency of its P&L, at {rates}",
                self.side,
                named(),
                held.join(", ")
            );
            return Ok(());
        };
        self.sized(strategy, &instrument, stake, equity, None)
    }

    /// Sends the trade at `stake` of a fixed `equity`, its orders named `ids`: a dataset run's
    /// balance, converted at the decision's rate, which a target's row is priced at whatever the
    /// run's P&L (`model::targets`).
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument is not in the cache or is inverse (as `refuse_inverse`
    /// refuses it), its stop is outside its prices, Nautilus fails to size it, or Nautilus refuses
    /// an order or the alert (see `send`).
    pub fn send_at<S>(
        &self,
        strategy: &mut S,
        stake: Stake,
        equity: Money,
        ids: OrderIds,
    ) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        let instrument = linear(strategy, self.instrument_id)?;
        self.sized(strategy, &instrument, stake, equity, Some(ids))
    }

    /// Returns the size at `stake` of `equity` in the P&L currency: Nautilus' fixed-risk size
    /// (`calculate_fixed_risk_position_size`), rounded down to the size increment, at most the
    /// maximum quantity, no commission counted. The stop is one R from the reference, on the
    /// tick grid, the tick away when one R is no whole count of ticks, so the size never risks
    /// more than the stake over one R. Zero if it rounds down to none.
    ///
    /// # Errors
    ///
    /// Returns an error if its stop is outside the instrument's prices or Nautilus fails to size
    /// it.
    pub fn size(
        &self,
        instrument: &InstrumentAny,
        stake: Stake,
        equity: Money,
    ) -> anyhow::Result<Quantity> {
        calculate_fixed_risk_position_size(
            instrument,
            self.reference,
            self.stop(instrument)?,
            equity,
            stake.fraction(),
            Decimal::ZERO,
            Decimal::ONE,
            None,
            instrument.size_increment().as_decimal(),
            1,
        )
        // In the message, not as context: Nautilus logs only the top error of the chain
        .map_err(|e| anyhow::anyhow!("sizing the {} of {}: {e}", self.side, self.instrument_id))
    }

    /// Returns its R in money on `quantity`: what one R is worth in the P&L currency, exact
    /// (`worth`).
    ///
    /// # Errors
    ///
    /// Returns an error if the worth overflows or is more money than Nautilus holds.
    pub fn risk(&self, instrument: &InstrumentAny, quantity: Quantity) -> anyhow::Result<Money> {
        Money::from_decimal_native(
            worth(instrument, quantity, self.one_r)?,
            instrument.cost_currency(),
        )
        .map_err(|e| anyhow::anyhow!("the R of {quantity} of {}: {e}", self.instrument_id))
    }

    fn sized<S>(
        &self,
        strategy: &mut S,
        instrument: &InstrumentAny,
        stake: Stake,
        equity: Money,
        ids: Option<OrderIds>,
    ) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        let quantity = self.size(instrument, stake, equity)?;
        if quantity.is_zero() {
            log::warn!(
                "not sending the {} of {}: at a stake of {stake} of {equity}, its size rounds \
                 down to zero, one R of {} from {} worth more than that on its size increment {}",
                self.side,
                self.instrument_id,
                self.one_r,
                self.reference,
                instrument.size_increment()
            );
            return Ok(());
        }
        let r = self.risk(instrument, quantity)?;
        let tags = [
            format!("{R_TAG}{r}"),
            format!("{MULTIPLE_TAG}{}", stake.multiple().normalize()),
        ]
        .map(|tag| Ustr::from(&tag));
        self.submit(strategy, instrument, quantity, &tags, ids)
    }

    /// # Errors
    ///
    /// Returns an error if it is outside the instrument's prices.
    fn stop(&self, instrument: &InstrumentAny) -> anyhow::Result<Price> {
        let (reference, one_r) = (self.reference.as_decimal(), self.one_r);
        let sell = self.side == OrderSide::Sell;
        let stop = if sell {
            reference + one_r
        } else {
            reference - one_r
        };
        stop.to_f64()
            .and_then(|value| {
                if sell {
                    instrument.next_ask_price(value, 0)
                } else {
                    instrument.next_bid_price(value, 0)
                }
            })
            .with_context(|| {
                format!(
                    "the stop {stop} of the {} of {} is outside its prices",
                    self.side,
                    instrument.id()
                )
            })
    }

    fn submit<S>(
        &self,
        strategy: &mut S,
        instrument: &InstrumentAny,
        quantity: Quantity,
        tags: &[Ustr],
        ids: Option<OrderIds>,
    ) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        let instrument_id = instrument.id();
        let limit_tag = self
            .exits
            .time_limit
            .map(|limit| Ustr::from(&format!("{TIME_LIMIT_TAG}{limit}")));
        let tagged = |first: Option<Ustr>| -> Vec<Ustr> {
            first
                .into_iter()
                .chain(limit_tag)
                .chain(tags.iter().copied())
                .collect()
        };
        let entry = match self.exits.bracket {
            Some(Bracket {
                take_profit,
                stop_loss,
            }) => {
                let takes = venues::takes_reduce_only_and_post_only(instrument_id.venue);
                let orders = strategy
                    .order()
                    .bracket()
                    .instrument_id(instrument_id)
                    .order_side(self.side)
                    .quantity(quantity)
                    .entry_tags(tagged(Some(Ustr::from(ENTRY_TAG))))
                    .maybe_entry_client_order_id(ids.map(|ids| ids.entry))
                    .maybe_tp_client_order_id(ids.map(|ids| ids.take_profit))
                    .maybe_sl_client_order_id(ids.map(|ids| ids.stop_loss))
                    .tp_price(take_profit)
                    .tp_post_only(takes)
                    .tp_reduce_only(takes)
                    .sl_trigger_price(stop_loss)
                    .sl_reduce_only(takes)
                    .call();
                // Nautilus' bracket is the entry, then its legs
                let entry = orders[0].client_order_id();
                // Denied whole on OKX until nautilus_trader#5668 is fixed
                strategy.submit_order_list(orders, None, None, None)?;
                entry
            }
            None => {
                let tags = tagged(None);
                let order = strategy.order().market(
                    instrument_id,
                    self.side,
                    quantity,
                    None,
                    None,
                    None,
                    None,
                    None,
                    (!tags.is_empty()).then_some(tags),
                    ids.map(|ids| ids.entry),
                );
                let entry = order.client_order_id();
                strategy.submit_order(order, None, None, None)?;
                entry
            }
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

/// The account an order on `venue` is checked against, as Nautilus' risk engine selects it
/// (`docs/concepts/execution/index.md`, "Account selection", rules 4 and 5;
/// `RiskEngine::order_account_id`): the one the cache registers for the execution client routed
/// for the venue, else the default client (`Cache::client_id_for_venue`). A live node routes by
/// its clients' `RoutingConfig` (IB the default, its account issued under IB, not under the
/// exchanges its instruments name), a backtest each simulated venue to its own client. With
/// none, the account issued under `venue`. Read from the native cache (`cache_ref`): the
/// strategy's cache facade holds no client's account.
fn routed_account<S>(strategy: &S, venue: Venue) -> Option<AccountId>
where
    S: DataActorNative,
{
    let cache = strategy.cache_ref();
    cache
        .client_id_for_venue(&venue)
        .and_then(|client| cache.account_id_for_client(client))
        .copied()
}

/// The equity of `account` per currency, from the source its broker names
/// (`venues::equity_source`): `Portfolio::equity`, balance totals plus the open P&L of its
/// positions on `venue`; or, for IB, whose balance holds that P&L already, balance totals
/// alone. The latter is read only while the account is as its broker reports it: a backtest's
/// simulated account, whose state Nautilus calculates from its fills
/// (`Account::calculated_account_state`), holds no open P&L and is read from
/// `Portfolio::equity`, as is the account issued under `venue` when none is routed.
fn equity<S>(strategy: &S, venue: Venue, account: Option<AccountId>) -> Vec<Money>
where
    S: Strategy + StrategyNative + DataActorNative,
{
    if venues::equity_source(venue) == EquitySource::BalanceTotals {
        let cache = strategy.cache_ref();
        if let Some(reported) = account
            .and_then(|id| cache.account(&id))
            .filter(|account| !account.calculated_account_state())
        {
            return reported.balances_total().into_values().collect();
        }
    }
    strategy
        .portfolio()
        .equity(&venue, account.as_ref())
        .into_values()
        .collect()
}

/// # Errors
///
/// Returns an error if it is not there.
fn cached<S>(strategy: &S, instrument_id: InstrumentId) -> anyhow::Result<InstrumentAny>
where
    S: DataActor + DataActorNative,
{
    strategy
        .cache()
        .instrument(&instrument_id)
        .with_context(|| format!("instrument {instrument_id} not in cache"))
}

/// The instrument from `strategy`'s cache, linear.
///
/// # Errors
///
/// Returns an error if it is not in the cache or is inverse.
fn linear<S>(strategy: &S, instrument_id: InstrumentId) -> anyhow::Result<InstrumentAny>
where
    S: DataActor + DataActorNative,
{
    let instrument = cached(strategy, instrument_id)?;
    ensure_linear(&instrument)?;
    Ok(instrument)
}

/// Refuses an instrument `strategy` trades, from its start, if it is inverse. One not in the
/// cache yet is refused at its first send.
///
/// # Errors
///
/// Returns an error if it is inverse.
pub fn refuse_inverse<S>(strategy: &S, instrument_id: InstrumentId) -> anyhow::Result<()>
where
    S: DataActor + DataActorNative,
{
    match strategy.cache().instrument(&instrument_id) {
        Some(instrument) => ensure_linear(&instrument),
        None => Ok(()),
    }
}

/// Checks that `instrument` is linear: Nautilus' fixed-risk sizer reads P&L as a multiple of a
/// distance in price, which an inverse instrument's is not, nor are the worths of distances on
/// it (`worth`).
///
/// # Errors
///
/// Returns an error if it is inverse.
pub(crate) fn ensure_linear(instrument: &InstrumentAny) -> anyhow::Result<()> {
    anyhow::ensure!(
        !instrument.is_inverse(),
        "{} is inverse: a trade on it can neither be sized at a stake nor have its R in money, \
         a model target's unit, worked out, Nautilus' fixed-risk sizer reading its P&L as a \
         multiple of a distance in price, which an inverse instrument's is not",
        instrument.id()
    );
    Ok(())
}

/// What a `distance` in price is worth on `quantity` of linear `instrument`, in its P&L
/// currency: quantity times multiplier times distance, unrounded. Nautilus' notional value of
/// the distance is rounded and it has no call returning one unrounded, so this product is ours,
/// written as `Position` makes its P&L.
///
/// # Errors
///
/// Returns an error if the worth overflows.
pub(crate) fn worth(
    instrument: &InstrumentAny,
    quantity: Quantity,
    distance: Decimal,
) -> anyhow::Result<Decimal> {
    quantity
        .as_decimal()
        .checked_mul(instrument.multiplier().as_decimal())
        .and_then(|value| value.checked_mul(distance))
        .with_context(|| {
            format!(
                "the worth of {distance} on {quantity} of {} overflows",
                instrument.id()
            )
        })
}

/// At a trade's time alert, cancels its orders still open and closes its position if the trade
/// is open, tagged as a time close. Returns whether `event` is a trade's time alert.
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
        let reduce_only = venues::takes_reduce_only_and_post_only(position.instrument_id.venue);
        strategy.close_position(&position, None, tags, None, Some(reduce_only), None, None)?;
    }
    Ok(true)
}

/// Sets again, from a restart, the time alert of each open trade, at its entry's creation plus
/// the tagged limit, or now if that has passed. A trade is open if its entry is an open order
/// of the strategy (as Nautilus' `reactivate_gtd_timers` walks them), or its fill is held by
/// an open position of the strategy and no leg of it is filled whole.
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

/// Whether `order` is the close of a trade at its time limit (`on_time_event`).
#[must_use]
pub fn is_time_close(order: &OrderAny) -> bool {
    order
        .tags()
        .is_some_and(|tags| tags.contains(&Ustr::from(TIME_CLOSE_TAG)))
}

/// Whether `position` holds the trade of `entry` open (see the module doc).
///
/// Read from the position's fills and the legs' parent link, not from the entry's order or
/// `Cache::position_for_order`: Nautilus purges a closed order with its index
/// (`purge_closed_orders`, a dataset run's a minute after it closes) unless an order linked to
/// it is still open, so a market entry with a time limit, linked to none, goes while its
/// position stays open. A bracket's filled entry stays while a leg is open. Live keeps closed
/// orders a week (`live::node`).
fn holds_open(cache: &CacheApi<'_>, position: &Position, entry: ClientOrderId) -> bool {
    let filled = position.client_order_ids();
    filled.contains(&entry)
        && !filled.iter().any(|id| {
            cache.order(id).is_some_and(|leg| {
                leg.parent_order_id() == Some(entry) && leg.status() == OrderStatus::Filled
            })
        })
}

/// What `entry` risks, as tagged by a send at a stake (`Trade::send`). None of an order sent
/// otherwise.
#[must_use]
pub fn entry_risk(entry: &OrderAny) -> Option<EntryRisk> {
    let tags = entry.tags()?;
    let tagged = |prefix: &str| {
        tags.iter()
            .find_map(|tag| tag.as_str().strip_prefix(prefix))
    };
    Some(EntryRisk {
        r: Money::from_str_exact(tagged(R_TAG)?).ok()?,
        multiple: Decimal::from_str(tagged(MULTIPLE_TAG)?).ok()?,
    })
}

fn time_limit(entry: &OrderAny) -> Option<SignedDuration> {
    entry
        .tags()?
        .iter()
        .find_map(|tag| SignedDuration::from_str(tag.as_str().strip_prefix(TIME_LIMIT_TAG)?).ok())
}

fn time_alert(entry: ClientOrderId) -> String {
    format!("{TIME_ALERT}{entry}")
}

#[cfg(test)]
mod tests;
