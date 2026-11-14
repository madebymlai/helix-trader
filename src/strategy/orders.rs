//! Trades: what a strategy sends, closes at its time limit and arms again
//! at a restart, on Nautilus' own orders, cache and clock, knowing nothing
//! of how the strategy decided.
//!
//! A `Trade` is an instrument, a side, its `Exits` and its one R, the
//! distance in price from the price its exits are set from to its stop.
//! A strategy sends it at a `Stake`, the config's `Risk` times a multiple
//! it picks: `Trade::send` sizes it with Nautilus' fixed-risk sizer to
//! risk that fraction of the venue's equity over its one R
//! (`Trade::size`; `Trade::send_at` at a fixed equity, a dataset run's
//! balance), tags its entry with its R in money and the multiple
//! (`entry_risk`), and sends,
//! with a `Bracket`, a Nautilus bracket (`OrderFactory::bracket`: a market
//! entry, a take-profit limit and a stop-market, one-updates-the-other,
//! the legs reduce-only and the take profit post-only where the
//! instrument's live broker takes such orders, in a backtest as live:
//! `venues::takes_reduce_only_and_post_only`, the legs tagged besides
//! Nautilus' tags with the venue's tag linking them at its broker, named
//! after the entry: `venues::bracket_exits_tag`, IB's OCA group), without
//! one a market entry.
//! A time limit sets a time alert named after the entry, `TIME_ALERT` and
//! its client order id, the time limit after the send, and tags the entry
//! with it (`TIME_LIMIT_TAG`), where the cache, persisted live, keeps it.
//!
//! The time limit runs from the send, the entry's creation (its
//! `ts_init`), not from its fill: a dataset run labels a decision's target
//! from the decision, and on a market entry the two differ by the
//! venue's latency only (none in a backtest).
//!
//! Nautilus delivers time events to the strategy alone: the strategy hands
//! each to `on_time_event`, which at a trade's alert cancels its orders
//! still open, the entry and its legs, and, if the trade is still open,
//! closes its position (`close_position`, reduce-only as the legs are,
//! tagged `TIME_CLOSE_TAG`, see `is_time_close`), not waiting for the
//! cancels (see `venues/ib/README.md` for what that risks on IB). A trade
//! is open while an open position holds its entry's fill and no leg of the
//! entry (an order Nautilus names it the parent of) is filled whole
//! (`OrderStatus::Filled`): a leg partly filled leaves the rest of the
//! trade open, which its time closes. On a hedging
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

use std::{cmp::Ordering, collections::BTreeSet, str::FromStr};

use anyhow::{Context, bail, ensure};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, DataActorNative},
    cache::CacheApi,
    timer::TimeEvent,
};
use nautilus_model::{
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

use super::risk::Stake;
use crate::venues;

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

/// The tag of a bracket's take profit, Nautilus' default, which the venue's
/// tag of its exits joins (`venues::bracket_exits_tag`).
const TAKE_PROFIT_TAG: &str = "TAKE_PROFIT";

/// The tag of a bracket's stop loss, Nautilus' default, which the venue's
/// tag of its exits joins (`venues::bracket_exits_tag`).
const STOP_LOSS_TAG: &str = "STOP_LOSS";

/// The prefix of the tag of an entry sent at a stake with its R in money,
/// what its quantity is worth over one R in the instrument's P&L
/// currency, followed by that money, `R:500.00 USD` (`entry_risk`).
const R_TAG: &str = "R:";

/// The prefix of the tag of an entry sent at a stake with the stake's
/// multiple, followed by it, `MULTIPLE:2` (`entry_risk`).
const MULTIPLE_TAG: &str = "MULTIPLE:";

/// A trade to send: of `instrument_id` on `side`, ended by its exits, one
/// R of it spanning `one_r` in price from `reference`, the price its exits
/// are set from, and sized as it is sent at a stake (`Trade::send`). Its
/// fields are the module's: what it trades, its exits and its one R are
/// set as it is built.
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
    /// Its R in money: what its quantity is worth over one R, in the
    /// instrument's P&L currency.
    pub r: Money,
    /// The multiple of the config's risk it was staked at.
    pub multiple: Decimal,
}

/// How a trade ends: at an exit of its bracket, if it has one, or at its
/// time limit after the send, if it has one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Exits {
    pub bracket: Option<Bracket>,
    pub time_limit: Option<SignedDuration>,
}

/// A trade's take profit and stop loss, on the instrument's prices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bracket {
    pub take_profit: Price,
    pub stop_loss: Price,
}

/// `price` on the nearer of `instrument`'s ticks around it, Nautilus' tick
/// grid, as its venue takes an exit; at a tie, the one `away` (1 up, -1
/// down) from the decision's price, so that an exit is not nearer than its
/// distance. Nautilus' `make_price` rounds to the instrument's precision
/// alone, which is no tick when the tick is coarser.
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
        bail!("the exit {price} is outside the prices of {id}");
    };
    let (down, up) = (price - below.as_decimal(), above.as_decimal() - price);
    Ok(match down.cmp(&up) {
        Ordering::Less => below,
        Ordering::Greater => above,
        Ordering::Equal if away.is_sign_positive() => above,
        Ordering::Equal => below,
    })
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
    /// The trade of `instrument_id` on `side`, ended by `exits`, one R of
    /// it spanning `one_r` in price from `reference`, the price its exits
    /// are set from: the distance to its stop, or an ATR's for a trade
    /// ended by its time alone.
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

    /// Sends the trade from `strategy` at `stake` of the equity of the
    /// account its orders are checked against, its orders of the factory's
    /// ids: a Nautilus bracket with a `Bracket`, a market entry without one,
    /// and its time alert if it has a time limit. Its quantity is its
    /// size at that stake of that equity (`size`), its entry tagged with
    /// its R in money and the stake's multiple (`entry_risk`).
    ///
    /// The equity is the account's (`Portfolio::equity`) in the
    /// instrument's P&L currency, the account Nautilus' risk engine checks
    /// the orders against (`routed_account`): its balance and the open P&L
    /// of its positions on the instrument's venue. Positions the account
    /// holds on its broker's other venues are left out, as are those of
    /// other processes, which this cache does not hold anyway, one process
    /// running one strategy.
    ///
    /// It sends nothing, and logs why, if the account holds no equity in the
    /// P&L currency (equity is not converted), or if its size rounds down
    /// to none. Nautilus' risk engine judges what it sends: a
    /// quantity below the instrument's minimum, or a margin beyond the
    /// free balance, is denied.
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument is not in the cache or is
    /// inverse (as `refuse_inverse` refuses it), its stop is outside its
    /// prices, Nautilus fails to size it, or Nautilus refuses an order or
    /// the alert.
    pub fn send<S>(&self, strategy: &mut S, stake: Stake) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActor + DataActorNative,
    {
        let instrument_id = self.instrument_id;
        let venue = instrument_id.venue;
        let instrument = linear(strategy, instrument_id)?;
        let currency = instrument.cost_currency();
        let account = routed_account(strategy, venue);
        let held = strategy.portfolio().equity(&venue, account.as_ref());
        let Some(&equity) = held.get(&currency) else {
            let held: Vec<String> = held.values().map(ToString::to_string).collect();
            log::error!(
                "not sending the {} of {instrument_id} at a stake of {stake}: the account {} \
                 holds no equity in {currency}, the currency of its P&L, but [{}], which is not \
                 converted",
                self.side,
                account.map_or_else(|| format!("of {venue}"), |account| account.to_string()),
                held.join(", ")
            );
            return Ok(());
        };
        self.sized(strategy, &instrument, stake, equity, None)
    }

    /// Sends the trade from `strategy` at `stake` of `equity`, as `send`
    /// does but for a fixed equity in place of the account's, its orders
    /// named `ids`: a dataset run's, at its balance, which a target's row
    /// is priced at whatever the run's P&L (`model::targets`).
    ///
    /// # Errors
    ///
    /// As `send`.
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

    /// Its size at `stake` of `equity`, in the P&L currency of
    /// `instrument`, its instrument: Nautilus' fixed-risk size
    /// (`calculate_fixed_risk_position_size`), the stake's fraction of the
    /// equity over what its stop is worth on one unit, rounded down to the
    /// instrument's size increment, at most its maximum quantity, no
    /// commission counted. The stop it is sized at is one R from its
    /// reference price, against it, on the instrument's tick grid, the tick
    /// away from the reference when one R is no whole count of ticks: its
    /// size never risks more than its stake over one R. None if it rounds
    /// down to none.
    ///
    /// # Errors
    ///
    /// Returns an error if its stop is outside the instrument's prices or
    /// Nautilus fails to size it.
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
        // In the message, not as context: Nautilus logs only the top
        // error of the chain.
        .map_err(|e| anyhow::anyhow!("sizing the {} of {}: {e}", self.side, self.instrument_id))
    }

    /// Its R in money on `quantity` of `instrument`, its instrument: what
    /// one R is worth on it in the instrument's P&L currency, exact
    /// (`worth`), as its entry is tagged (`entry_risk`).
    ///
    /// # Errors
    ///
    /// Returns an error if the worth overflows or is more money than
    /// Nautilus holds.
    pub fn risk(&self, instrument: &InstrumentAny, quantity: Quantity) -> anyhow::Result<Money> {
        Money::from_decimal_native(
            worth(instrument, quantity, self.one_r)?,
            instrument.cost_currency(),
        )
        .map_err(|e| anyhow::anyhow!("the R of {quantity} of {}: {e}", self.instrument_id))
    }

    /// Sends the trade from `strategy` on `instrument`, its instrument, at
    /// `stake` of `equity` (`size`), its entry tagged with its R in money
    /// and the stake's multiple, its orders named `ids` or of the
    /// factory's; nothing, logged, if its size rounds down to none.
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

    /// The stop the trade is sized at: one R from its reference, against
    /// it, on the instrument's tick grid (Nautilus' `next_bid_price` and
    /// `next_ask_price`), the tick away from the reference.
    ///
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

    /// Sends `quantity` of the trade from `strategy`, its entry tagged
    /// with `tags` as well, its orders named `ids` or of the factory's.
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
                let venue = instrument_id.venue;
                let takes = venues::takes_reduce_only_and_post_only(venue);
                // The factory's entry id is drawn here, first, as the
                // bracket would draw it, so that the venue's tag of the
                // exits can name it.
                let entry = ids.map_or_else(
                    || strategy.order().generate_client_order_id(),
                    |ids| ids.entry,
                );
                let linked = venues::bracket_exits_tag(venue, entry);
                let exit_tags = |tag: &str| -> Vec<Ustr> {
                    std::iter::once(Ustr::from(tag)).chain(linked).collect()
                };
                let orders = strategy
                    .order()
                    .bracket()
                    .instrument_id(instrument_id)
                    .order_side(self.side)
                    .quantity(quantity)
                    .entry_tags(tagged(Some(Ustr::from(ENTRY_TAG))))
                    .entry_client_order_id(entry)
                    .maybe_tp_client_order_id(ids.map(|ids| ids.take_profit))
                    .maybe_sl_client_order_id(ids.map(|ids| ids.stop_loss))
                    .tp_price(take_profit)
                    .tp_post_only(takes)
                    .tp_reduce_only(takes)
                    .tp_tags(exit_tags(TAKE_PROFIT_TAG))
                    .sl_trigger_price(stop_loss)
                    .sl_reduce_only(takes)
                    .sl_tags(exit_tags(STOP_LOSS_TAG))
                    .call();
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

/// The account an order on `venue` is checked against, as Nautilus' risk
/// engine selects it (`docs/concepts/execution/index.md`, "Account
/// selection", rules 4 and 5; `RiskEngine::order_account_id`): the account
/// the cache registers for the execution client routed for the venue, else
/// for the default client (`Cache::client_id_for_venue`), the routes the
/// node registered, a live node's by its clients' `RoutingConfig` (IB the
/// default, its account issued under IB, not under the exchanges its
/// instruments name), a backtest's each simulated venue to its own client;
/// or, with none, none here, the account issued under `venue`
/// (`Portfolio::equity` without one). Read from the native cache
/// (`cache_ref`): the strategy's cache facade holds no client's account.
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

/// The instrument `instrument_id` from `strategy`'s cache.
///
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

/// The instrument `instrument_id` from `strategy`'s cache, linear.
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

/// Refuses `instrument_id` as an instrument `strategy` trades, from its
/// start, if it is inverse: a trade on it cannot be sized at a stake
/// (`Trade::send`). One not in the cache yet is refused at its first send.
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

/// Checks that `instrument` is linear: Nautilus' fixed-risk sizer
/// (`calculate_fixed_risk_position_size`) reads a trade's P&L as a
/// multiple of a distance in price, which an inverse instrument's is not,
/// nor are the worths of distances on it (`worth`), a model target's unit
/// and payoff among them.
///
/// # Errors
///
/// Returns an error if it is inverse.
pub(crate) fn ensure_linear(instrument: &InstrumentAny) -> anyhow::Result<()> {
    ensure!(
        !instrument.is_inverse(),
        "{} is inverse: a trade on it can neither be sized at a stake nor have its R in money, \
         a model target's unit, worked out, Nautilus' fixed-risk sizer reading its P&L as a \
         multiple of a distance in price, which an inverse instrument's is not",
        instrument.id()
    );
    Ok(())
}

/// What a `distance` in price is worth on `quantity` of linear
/// `instrument`, in its P&L currency: exactly quantity times multiplier
/// times distance, not rounded to the currency's precision, the money made
/// as Nautilus' `Position` makes its P&L. Nautilus' notional value of the
/// distance is rounded to that precision, and it has no call on an
/// instrument returning one unrounded, so this one product is ours,
/// written as it writes it.
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
        let reduce_only = venues::takes_reduce_only_and_post_only(position.instrument_id.venue);
        strategy.close_position(&position, None, tags, None, Some(reduce_only), None, None)?;
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

/// What `entry` risks, as it is tagged by a send at a stake
/// (`Trade::send`): its R in money and the stake's multiple. None of an
/// order sent otherwise, a time close's or another strategy's.
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
