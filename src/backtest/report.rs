//! The results of a run, as plain text.
//!
//! This module prints the order and position counts, statistics and final balances of the Nautilus
//! `BacktestResult`. The statistics include the max drawdown, which `node::run` registers. It also
//! prints the costs, which the result does not hold. The costs come from the fills (`OrderFilled`)
//! in the engine's cache, the quotes they filled on and the commissions its accounts booked. A run
//! on bars or trades alone has no spread to measure.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write,
};

use ahash::AHashMap;
use anyhow::Context;
use nautilus_backtest::result::BacktestResult;
use nautilus_common::cache::{Cache, CacheConfig};
use nautilus_core::UnixNanos;
use nautilus_model::{
    accounts::AccountAny,
    data::QuoteTick,
    enums::{OrderSide, PriceType},
    events::{OrderEventAny, OrderFilled},
    identifiers::InstrumentId,
    instruments::Instrument,
    orders::Order,
    types::{Currency, Money},
};
use rust_decimal::{Decimal, prelude::ToPrimitive};

use super::trades::Trades;

/// A finished run: the Nautilus result, what its fills cost, and its positions.
#[derive(Debug)]
pub struct Outcome {
    pub result: BacktestResult,
    /// Every position of the run, for its trades file (`trades`).
    pub trades: Trades,
    /// Keyed by the code of the currency the accounts book them in, as the result's `stats_pnls`
    /// is.
    costs: BTreeMap<String, Costs>,
}

#[derive(Debug, Default)]
struct Costs {
    /// The commission the run's accounts booked in this currency, which Nautilus nets into its
    /// `PnL (total)`.
    commission: Decimal,
    /// For a buy, a fill's value at its price less its value at the mid of its quote. For a sell,
    /// the reverse. Converted into this currency at the rate Nautilus books the fill at.
    spread: Decimal,
    trades: usize,
    /// Fills of an instrument for which the run replayed no quote. If there are any, the spread is
    /// not measured.
    unquoted: usize,
}

impl Outcome {
    /// Returns `result` with the costs of every fill of the orders in `cache`, and the positions in
    /// it (`Trades::of`).
    ///
    /// A fill's costs are in the currency its account books it in: the account's base currency, or
    /// the fill's own on an account without one (`AccountsManager::update_balances` in Nautilus'
    /// portfolio crate). Its commission is what the accounts booked (`booked`). Its spread is
    /// measured on the quote it filled on, the last quote of its instrument at or before it in
    /// `streams`, the quote series the run replayed, a later stream's quote winning a tie. It is
    /// converted at the rate Nautilus books the fill at: `Cache::get_xrate` on the fill's venue, at
    /// the bid for a sell and the ask for a buy, over the replayed quotes at or before it. A fill of
    /// an instrument with no quote in `streams` has no spread.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A stream fails or is out of time order.
    /// - A fill's instrument or account is not in `cache`, or its instrument is inverse (its value
    ///   is not linear in the price).
    /// - A fill's instrument has quotes and none precedes the fill.
    /// - No replayed quote converts a fill's currency into its account's.
    /// - Nautilus fails to encode the positions.
    pub(super) fn new<S>(
        result: BacktestResult,
        cache: &Cache,
        streams: impl IntoIterator<Item = S>,
    ) -> anyhow::Result<Self>
    where
        S: IntoIterator<Item = anyhow::Result<QuoteTick>>,
    {
        let orders = cache.orders(None, None, None, None, None);
        let mut fills: Vec<&OrderFilled> = orders
            .iter()
            .flat_map(|order| order.events())
            .filter_map(|event| match event {
                OrderEventAny::Filled(fill) => Some(fill),
                _ => None,
            })
            .collect();
        fills.sort_by_key(|fill| fill.ts_event);

        // The replayed quotes as of each fill, for Nautilus' quote and rate lookups
        let mut replayed = Cache::new(
            Some(CacheConfig {
                data_capacity: 1,
                ..CacheConfig::default()
            }),
            None,
        );

        for id in cache.instrument_ids(None) {
            let instrument = cache
                .instrument(id)
                .context("an instrument the cache lists")?;
            replayed.add_instrument(instrument.clone())?;
        }
        let mut costs: BTreeMap<String, Costs> = BTreeMap::new();
        let mut unquoted = Vec::new();
        let mut quoted = BTreeSet::new();
        let mut quotes = in_time_order(streams);
        let mut fills = fills.into_iter().peekable();

        loop {
            let quote = quotes.next().transpose()?;
            while let Some(fill) =
                fills.next_if(|fill| quote.is_none_or(|quote| fill.ts_event < quote.ts_init))
            {
                let id = fill.instrument_id;
                let instrument = cache
                    .instrument(&id)
                    .with_context(|| format!("instrument {id} of a fill not in the cache"))?;
                anyhow::ensure!(!instrument.is_inverse(), "{id} is inverse");
                let account = cache.account(&fill.account_id).with_context(|| {
                    format!("account {} of a fill not in the cache", fill.account_id)
                })?;
                let booked_in = account.base_currency().unwrap_or(fill.currency);
                let cost = costs.entry(booked_in.code.to_string()).or_default();
                cost.trades += 1;

                let Some(quote) = replayed.quote(&id) else {
                    unquoted.push((fill, booked_in));
                    continue;
                };
                let mid = quote.extract_price(PriceType::Mid)?.as_decimal();
                let price = fill.last_px.as_decimal();
                let (paid, rate_side) = if fill.order_side == OrderSide::Buy {
                    (price - mid, PriceType::Ask)
                } else {
                    (mid - price, PriceType::Bid)
                };
                let rate = replayed
                    .get_xrate(id.venue, fill.currency, booked_in, rate_side)
                    .with_context(|| {
                        format!(
                            "no replayed quote on {} converts {} into {booked_in}, the currency of \
                             the fill of {id} at {}",
                            id.venue, fill.currency, fill.ts_event
                        )
                    })?;
                let size = fill.last_qty.as_decimal() * instrument.multiplier().as_decimal();
                cost.spread += paid * size * rate;
            }
            let Some(quote) = quote else { break };
            quoted.insert(quote.instrument_id);
            replayed.add_quote(quote)?;
        }

        for (fill, booked_in) in unquoted {
            let id = fill.instrument_id;
            anyhow::ensure!(
                !quoted.contains(&id),
                "no quote of {id} before its fill at {}",
                fill.ts_event
            );
            costs
                .entry(booked_in.code.to_string())
                .or_default()
                .unquoted += 1;
        }

        for account in cache.accounts_all_owned() {
            for commission in booked(&account).into_values() {
                costs
                    .entry(commission.currency.code.to_string())
                    .or_default()
                    .commission += commission.as_decimal();
            }
        }
        Ok(Self {
            result,
            trades: Trades::of(cache)?,
            costs,
        })
    }
}

/// Returns the commission `account` booked, by currency.
///
/// An account with a base currency books each commission converted into it, at the rate on the
/// fill's venue; one without books it in the currency charged (`AccountsManager::update_balances`
/// in Nautilus' portfolio crate).
fn booked(account: &AccountAny) -> AHashMap<Currency, Money> {
    match account {
        AccountAny::Margin(account) => account.commissions(),
        AccountAny::Cash(account) => account.commissions(),
        AccountAny::Betting(account) => account.commissions(),
        AccountAny::Wallet(account) => account.commissions(),
    }
}

/// Returns the quotes of `streams` in time order, a later stream's quote after an earlier one's
/// of the same time, so that it wins a tie.
///
/// Each stream is read once, a quote at a time, so a run's whole quote history is never held in
/// memory. A stream failing, or holding a quote stamped before the last quote of its instrument
/// ahead of it, is an error item.
fn in_time_order<S>(
    streams: impl IntoIterator<Item = S>,
) -> impl Iterator<Item = anyhow::Result<QuoteTick>>
where
    S: IntoIterator<Item = anyhow::Result<QuoteTick>>,
{
    let mut streams: Vec<_> = streams
        .into_iter()
        .map(|stream| stream.into_iter().peekable())
        .collect();
    let mut last: BTreeMap<(usize, InstrumentId), UnixNanos> = BTreeMap::new();
    std::iter::from_fn(move || {
        // The earliest head, the first stream's of a tie, or the first failed head
        let mut next: Option<(usize, UnixNanos)> = None;

        for (at, stream) in streams.iter_mut().enumerate() {
            match stream.peek() {
                Some(Err(_)) => return stream.next(),
                Some(Ok(quote)) if next.is_none_or(|(_, ts)| quote.ts_init < ts) => {
                    next = Some((at, quote.ts_init));
                }
                _ => {}
            }
        }
        let (at, _) = next?;
        let quote = streams[at].next()?;
        Some(quote.and_then(|quote| {
            let id = quote.instrument_id;
            if let Some(previous) = last.insert((at, id), quote.ts_init) {
                anyhow::ensure!(
                    previous <= quote.ts_init,
                    "quotes of {id} out of time order: {} after {previous}",
                    quote.ts_init
                );
            }
            Ok(quote)
        }))
    })
}

/// Returns `outcome` as text: one `name: value` line per figure.
///
/// Sections come in a fixed order and names are sorted, so that equal results print equally.
///
/// The costs of each currency are the commission, the spread paid, their sum per trade (a fill),
/// and the gross PnL, which is Nautilus' `PnL (total)` plus the commission and the spread. That
/// PnL is realized only. For a position still open at the end, the commission and the gross PnL
/// count the commission paid to open, which no closed position's PnL holds. The gross PnL also
/// holds the spread paid to open. A currency with a fill on no quote prints its spread as not
/// applicable, and its cost per trade and gross PnL hold the commission alone.
///
/// Each figure of a currency is in it: its commission is what the run's accounts booked in it,
/// which its `PnL (total)`, the change of its balances, nets, and its spread and trades are those
/// of the fills its accounts book in it, the spread converted at the rate of each fill (`Outcome::new`).
#[must_use]
pub fn report(outcome: &Outcome) -> String {
    let result = &outcome.result;
    let mut out = format!(
        "Orders: {}\nPositions: {}\n",
        result.total_orders, result.total_positions
    );
    let pnls: BTreeMap<_, _> = result.stats_pnls.iter().collect();
    for (currency, stats) in &pnls {
        section(&mut out, &format!("PnL statistics ({currency})"), *stats);
    }
    for (currency, costs) in &outcome.costs {
        let f64 = |value: Decimal| value.to_f64().unwrap_or(f64::NAN);
        let spread = (costs.unquoted == 0).then_some(costs.spread);
        let total = costs.commission + spread.unwrap_or_default();
        let mut figures = BTreeMap::from([("Commission".to_string(), f64(costs.commission))]);
        if let Some(spread) = spread {
            figures.insert("Spread".to_string(), f64(spread));
        }
        if costs.trades > 0 {
            let per_trade = total / Decimal::from(costs.trades);
            figures.insert("Cost per trade".to_string(), f64(per_trade));
        }
        if let Some(pnl) = pnls
            .get(currency)
            .and_then(|stats| stats.get("PnL (total)"))
        {
            figures.insert("Gross PnL".to_string(), pnl + f64(total));
        }
        section(&mut out, &format!("Costs ({currency})"), &figures);
        if spread.is_none() {
            // The spread line is last among the section's sorted names
            out.push_str("  Spread: n/a (no quotes)\n");
        }
    }
    section(&mut out, "Return statistics", &result.stats_returns);
    section(&mut out, "General statistics", &result.stats_general);
    let balances: BTreeMap<_, _> = result
        .summary
        .iter()
        .filter(|(key, _)| key.starts_with("account.") && key.ends_with(".total"))
        .collect();
    out.push_str("\nFinal balances\n");
    for (key, balance) in balances {
        let _ = writeln!(out, "  {key}: {balance}");
    }
    out
}

fn section<'a>(
    out: &mut String,
    title: &str,
    stats: impl IntoIterator<Item = (&'a String, &'a f64)>,
) {
    let stats: BTreeMap<_, _> = stats.into_iter().collect();
    let _ = writeln!(out, "\n{title}");
    for (name, value) in stats {
        let _ = writeln!(out, "  {name}: {value:.6}");
    }
}

#[cfg(test)]
mod tests {
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::{
        accounts::MarginAccount,
        enums::{AccountType, OrderType},
        events::AccountState,
        identifiers::{AccountId, ClientOrderId, Symbol, VenueOrderId},
        instruments::{InstrumentAny, stubs::equity_aapl},
        orders::{
            OrderAny, OrderTestBuilder,
            stubs::{OrderFilledTestBuilder, TestOrderEventStubs},
        },
        types::{AccountBalance, Price, Quantity},
    };
    use rstest::rstest;

    use super::*;

    fn sxr8() -> InstrumentAny {
        let mut equity = equity_aapl();
        equity.id = InstrumentId::from("SXR8.IBIS");
        equity.currency = Currency::EUR();
        InstrumentAny::Equity(equity)
    }

    fn filled(
        id: &str,
        instrument: &InstrumentAny,
        side: OrderSide,
        qty: u64,
        px: &str,
        commission: &str,
    ) -> OrderAny {
        filled_at(id, instrument, side, qty, px, commission, 2)
    }

    fn filled_at(
        id: &str,
        instrument: &InstrumentAny,
        side: OrderSide,
        qty: u64,
        px: &str,
        commission: &str,
        ts: u64,
    ) -> OrderAny {
        let order = OrderTestBuilder::new(OrderType::Market)
            .client_order_id(ClientOrderId::from(id))
            .instrument_id(instrument.id())
            .side(side)
            .quantity(Quantity::from(qty))
            .build();
        let mut order = order;
        order
            .apply(TestOrderEventStubs::accepted(
                &order,
                AccountId::from("SIM-001"),
                VenueOrderId::from(format!("V-{id}").as_str()),
            ))
            .unwrap();
        let fill = OrderFilledTestBuilder::new(&order, instrument)
            .account_id(AccountId::from(
                format!("{}-001", instrument.id().venue).as_str(),
            ))
            .last_px(Price::from(px))
            .commission(Money::from(commission))
            .ts_event(UnixNanos::from(ts))
            .build();
        order.apply(fill).unwrap();
        order
    }

    fn outcome() -> Outcome {
        let stats = |pairs: &[(&str, f64)]| {
            pairs
                .iter()
                .map(|&(name, value)| (name.to_string(), value))
                .collect()
        };
        let result = BacktestResult {
            trader_id: "BACKTESTER-001".to_string(),
            machine_id: "machine".to_string(),
            instance_id: UUID4::new(),
            run_config_id: None,
            run_id: None,
            run_started: None,
            run_finished: None,
            backtest_start: None,
            backtest_end: None,
            elapsed_time_secs: 0.0,
            iterations: 0,
            total_events: 0,
            total_orders: 5,
            total_positions: 3,
            summary: [
                ("orders.total", "5"),
                ("account.IBIS.balance.EUR.total", "9942.00 EUR"),
                ("account.IBIS.balance.EUR.free", "9000.00 EUR"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
            stats_pnls: [
                (
                    "EUR".to_string(),
                    stats(&[("Win Rate", 0.4), ("PnL (total)", -58.0)]),
                ),
                ("USD".to_string(), stats(&[("PnL (total)", 1.5)])),
                ("CHF".to_string(), stats(&[("PnL (total)", 2.5)])),
                ("GBP".to_string(), stats(&[("PnL (total)", 3.5)])),
            ]
            .into_iter()
            .collect(),
            stats_returns: stats(&[
                ("Sharpe Ratio (252 days)", 1.25),
                ("Max Drawdown", -0.2),
                ("Average (Return)", 0.001),
            ]),
            stats_general: stats(&[("Long Ratio", 0.5)]),
            returns_series: BTreeMap::new(),
        };
        let (sxr8, aapl) = (sxr8(), InstrumentAny::Equity(equity_aapl()));
        let mut cache = Cache::default();
        cache.add_instrument(sxr8.clone()).unwrap();
        cache.add_instrument(aapl.clone()).unwrap();

        for order in [
            filled("O-1", &sxr8, OrderSide::Buy, 10, "100.10", "3 EUR"),
            filled("O-2", &sxr8, OrderSide::Sell, 10, "99.90", "3 EUR"),
            filled("O-3", &sxr8, OrderSide::Buy, 10, "100.10", "1.25 GBP"),
            filled("O-4", &aapl, OrderSide::Buy, 1, "150.00", "0.25 USD"),
            filled("O-5", &aapl, OrderSide::Sell, 1, "150.00", "0.25 USD"),
        ] {
            cache.add_order(order, None, None, false).unwrap();
        }
        cache
            .add_account(account(
                "IBIS-001",
                "9942.00 EUR",
                None,
                &["6 EUR", "1.25 GBP"],
            ))
            .unwrap();
        cache
            .add_account(account("XNAS-001", "100.00 USD", None, &["0.50 USD"]))
            .unwrap();
        let quote = quote(&sxr8, "99.90", "100.10", 1);
        Outcome::new(result, &cache, [[Ok(quote)]]).unwrap()
    }

    /// A margin account holding `balance` that has booked each of `commissions`.
    fn account(
        id: &str,
        balance: &str,
        base: Option<Currency>,
        commissions: &[&str],
    ) -> AccountAny {
        let balance = Money::from(balance);
        let state = AccountState::new(
            AccountId::from(id),
            AccountType::Margin,
            vec![AccountBalance::new(
                balance,
                Money::zero(balance.currency),
                balance,
            )],
            vec![],
            true,
            UUID4::new(),
            UnixNanos::default(),
            UnixNanos::default(),
            base,
        );
        let mut account = MarginAccount::new(state, true);
        for commission in commissions {
            account
                .try_update_commissions(Money::from(*commission))
                .unwrap();
        }
        AccountAny::Margin(account)
    }

    /// A cache holding `instrument` and an account of its venue without a base currency.
    fn cached(instrument: &InstrumentAny) -> Cache {
        let mut cache = Cache::default();
        cache.add_instrument(instrument.clone()).unwrap();
        let (venue, currency) = (instrument.id().venue, instrument.quote_currency());
        let balance = format!("10000 {currency}");
        cache
            .add_account(account(&format!("{venue}-001"), &balance, None, &[]))
            .unwrap();
        cache
    }

    fn quote(instrument: &InstrumentAny, bid: &str, ask: &str, ts: u64) -> QuoteTick {
        QuoteTick::new(
            instrument.id(),
            Price::from(bid),
            Price::from(ask),
            Quantity::from(1),
            Quantity::from(1),
            UnixNanos::from(ts),
            UnixNanos::from(ts),
        )
    }

    fn empty_result() -> BacktestResult {
        BacktestResult {
            trader_id: "BACKTESTER-001".to_string(),
            machine_id: "machine".to_string(),
            instance_id: UUID4::new(),
            run_config_id: None,
            run_id: None,
            run_started: None,
            run_finished: None,
            backtest_start: None,
            backtest_end: None,
            elapsed_time_secs: 0.0,
            iterations: 0,
            total_events: 0,
            total_orders: 0,
            total_positions: 0,
            summary: Default::default(),
            stats_pnls: Default::default(),
            stats_returns: Default::default(),
            stats_general: Default::default(),
            returns_series: BTreeMap::new(),
        }
    }

    #[rstest]
    fn each_fill_costs_on_the_last_quote_at_or_before_it_a_later_stream_winning_a_tie() {
        let sxr8 = sxr8();
        let mut cache = cached(&sxr8);

        for (id, ts) in [("O-1", 2), ("O-2", 4), ("O-3", 9)] {
            let order = filled_at(id, &sxr8, OrderSide::Buy, 10, "101.00", "0 EUR", ts);
            cache.add_order(order, None, None, false).unwrap();
        }

        let first = [
            quote(&sxr8, "99.90", "100.10", 1),
            quote(&sxr8, "99.80", "100.00", 2),
            quote(&sxr8, "100.80", "101.00", 6),
        ];
        let second = [
            quote(&sxr8, "100.40", "100.60", 2),
            quote(&sxr8, "100.70", "100.90", 3),
            quote(&sxr8, "99.00", "99.20", 5),
        ];
        let streams = [first.map(Ok), second.map(Ok)];
        let outcome = Outcome::new(empty_result(), &cache, streams).unwrap();

        assert_eq!(outcome.costs["EUR"].spread, Decimal::from(8));
        assert_eq!(outcome.costs["EUR"].unquoted, 0);
    }

    #[rstest]
    fn refuses_a_stream_out_of_time_order() {
        let sxr8 = sxr8();
        let mut cache = cached(&sxr8);
        let order = filled("O-1", &sxr8, OrderSide::Buy, 10, "100.10", "3 EUR");
        cache.add_order(order, None, None, false).unwrap();
        let stream = [
            quote(&sxr8, "99.90", "100.10", 3),
            quote(&sxr8, "99.90", "100.10", 1),
        ];
        let err = Outcome::new(empty_result(), &cache, [stream.map(Ok)]).unwrap_err();

        let err = err.to_string();
        assert!(
            err.contains("SXR8.IBIS") && err.contains("1 after 3"),
            "{err}"
        );
    }

    #[rstest]
    fn refuses_a_fill_of_a_quoted_instrument_with_no_quote_before_it() {
        let sxr8 = sxr8();
        let mut cache = cached(&sxr8);
        let order = filled("O-1", &sxr8, OrderSide::Buy, 10, "100.10", "3 EUR");
        cache.add_order(order, None, None, false).unwrap();
        let stream = [Ok(quote(&sxr8, "99.90", "100.10", 3))];
        let err = Outcome::new(empty_result(), &cache, [stream]).unwrap_err();
        assert!(
            err.to_string()
                .contains("no quote of SXR8.IBIS before its fill"),
            "{err}"
        );
    }

    #[rstest]
    fn prints_the_counts_the_costs_and_every_statistic_sorted_by_section() {
        let expected = "\
Orders: 5
Positions: 3

PnL statistics (CHF)
  PnL (total): 2.500000

PnL statistics (EUR)
  PnL (total): -58.000000
  Win Rate: 0.400000

PnL statistics (GBP)
  PnL (total): 3.500000

PnL statistics (USD)
  PnL (total): 1.500000

Costs (EUR)
  Commission: 6.000000
  Cost per trade: 3.000000
  Gross PnL: -49.000000
  Spread: 3.000000

Costs (GBP)
  Commission: 1.250000
  Gross PnL: 4.750000
  Spread: 0.000000

Costs (USD)
  Commission: 0.500000
  Cost per trade: 0.250000
  Gross PnL: 2.000000
  Spread: n/a (no quotes)

Return statistics
  Average (Return): 0.001000
  Max Drawdown: -0.200000
  Sharpe Ratio (252 days): 1.250000

General statistics
  Long Ratio: 0.500000

Final balances
  account.IBIS.balance.EUR.total: 9942.00 EUR
";
        assert_eq!(report(&outcome()), expected);
    }

    /// An account in EUR trading AAPL in USD books the commission in EUR, and each fill's spread of
    /// 0.10 USD is converted at the EUR/USD rate on its venue, 1.25: 0.08 EUR. Its costs are in EUR
    /// alone, beside its PnL.
    #[rstest]
    fn every_cost_is_in_the_currency_the_account_books_it_in() {
        let aapl = InstrumentAny::Equity(equity_aapl());
        let eurusd =
            InstrumentAny::CurrencyPair(nautilus_model::instruments::stubs::default_fx_ccy(
                Symbol::from("EUR/USD"),
                Some(aapl.id().venue),
            ));
        let mut cache = Cache::default();
        cache.add_instrument(aapl.clone()).unwrap();
        cache.add_instrument(eurusd.clone()).unwrap();

        for (id, side, px) in [
            ("O-1", OrderSide::Buy, "150.10"),
            ("O-2", OrderSide::Sell, "149.90"),
        ] {
            let order = filled(id, &aapl, side, 1, px, "3 EUR");
            cache.add_order(order, None, None, false).unwrap();
        }
        cache
            .add_account(account(
                "XNAS-001",
                "9995.00 EUR",
                Some(Currency::EUR()),
                &["6 EUR"],
            ))
            .unwrap();
        let mut result = empty_result();
        result.stats_pnls = [(
            "EUR".to_string(),
            [("PnL (total)".to_string(), -5.0)].into_iter().collect(),
        )]
        .into_iter()
        .collect();
        let quotes = [
            quote(&eurusd, "1.25000", "1.25000", 1),
            quote(&aapl, "149.90", "150.10", 1),
        ];

        let outcome = Outcome::new(result, &cache, [quotes.map(Ok)]).unwrap();

        let report = report(&outcome);
        assert!(!report.contains("USD"), "{report}");
        assert!(
            report.contains(
                "Costs (EUR)\n  Commission: 6.000000\n  Cost per trade: 3.080000\n  Gross PnL: 1.160000\n  Spread: 0.160000\n"
            ),
            "{report}"
        );
    }

    #[rstest]
    fn refuses_a_spread_no_replayed_quote_converts_into_its_accounts_currency() {
        let aapl = InstrumentAny::Equity(equity_aapl());
        let mut cache = Cache::default();
        cache.add_instrument(aapl.clone()).unwrap();
        let order = filled("O-1", &aapl, OrderSide::Buy, 1, "150.10", "3 EUR");
        cache.add_order(order, None, None, false).unwrap();
        cache
            .add_account(account("XNAS-001", "10000 EUR", Some(Currency::EUR()), &[]))
            .unwrap();
        let quotes = [Ok(quote(&aapl, "149.90", "150.10", 1))];

        let err = Outcome::new(empty_result(), &cache, [quotes]).unwrap_err();

        let err = err.to_string();
        assert!(
            ["XNAS", "USD", "EUR", "AAPL.XNAS"]
                .iter()
                .all(|named| err.contains(named)),
            "{err}"
        );
    }
}
