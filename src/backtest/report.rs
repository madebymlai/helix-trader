//! The results of a run, as plain text.
//!
//! Prints what the Nautilus `BacktestResult` holds: the order and position
//! counts, its statistics (with the max drawdown, which the run registers
//! with the portfolio: see `node::run`) and the final balances from its
//! summary. Next to the PnL it prints the costs, which the result does not
//! hold: from the fills (`OrderFilled`, with their commission) of the
//! orders in the engine's cache, and the quotes they filled on, if the run
//! replayed quotes of their instrument; a run on bars or trades alone has
//! no spread to measure.

use std::{collections::BTreeMap, fmt::Write};

use anyhow::{Context, ensure};
use nautilus_backtest::result::BacktestResult;
use nautilus_common::cache::Cache;
use nautilus_model::{
    data::QuoteTick,
    enums::{OrderSide, PriceType},
    events::OrderEventAny,
    identifiers::InstrumentId,
    instruments::Instrument,
    orders::Order,
};
use rust_decimal::{Decimal, prelude::ToPrimitive};

/// A finished run: the Nautilus result and what its fills cost.
#[derive(Debug)]
pub struct Outcome {
    pub result: BacktestResult,
    /// Per currency code, as the result's `stats_pnls`.
    costs: BTreeMap<String, Costs>,
}

/// What the fills paid in one currency.
#[derive(Debug, Default)]
struct Costs {
    commission: Decimal,
    /// Each fill's value at its price less its value at the mid of the
    /// quote it filled on, for a buy, the reverse for a sell.
    spread: Decimal,
    /// The fills priced in this currency.
    trades: usize,
    /// Those of an instrument the run replayed no quote of: if any, the
    /// spread of the currency is not measured.
    unquoted: usize,
}

impl Outcome {
    /// `result` and the costs of every fill of the orders in `cache`, each
    /// on the last of `quotes` of its instrument stamped at or before the
    /// fill: the quote the venue filled it on. A fill of an instrument
    /// with no quote in `quotes` (a run on bars or trades) has no spread.
    ///
    /// # Errors
    ///
    /// Returns an error if a fill's instrument is not in `cache` or is
    /// inverse (its value is not linear in the price), or if its instrument
    /// has quotes and none precedes the fill.
    pub(super) fn new(
        result: BacktestResult,
        cache: &Cache,
        quotes: &[QuoteTick],
    ) -> anyhow::Result<Self> {
        let mut quoted: BTreeMap<InstrumentId, Vec<&QuoteTick>> = BTreeMap::new();
        for quote in quotes {
            quoted.entry(quote.instrument_id).or_default().push(quote);
        }
        for quotes in quoted.values_mut() {
            quotes.sort_by_key(|quote| quote.ts_init);
        }
        let mut costs: BTreeMap<String, Costs> = BTreeMap::new();
        for order in cache.orders(None, None, None, None, None) {
            for event in order.events() {
                let OrderEventAny::Filled(fill) = event else {
                    continue;
                };
                let id = fill.instrument_id;
                let instrument = cache
                    .instrument(&id)
                    .with_context(|| format!("instrument {id} of a fill not in the cache"))?;
                ensure!(!instrument.is_inverse(), "{id} is inverse");
                let cost = costs.entry(fill.currency.code.to_string()).or_default();
                cost.trades += 1;
                if let Some(commission) = fill.commission {
                    let code = commission.currency.code.to_string();
                    costs.entry(code).or_default().commission += commission.as_decimal();
                }
                let Some(quotes) = quoted.get(&id) else {
                    costs
                        .entry(fill.currency.code.to_string())
                        .or_default()
                        .unquoted += 1;
                    continue;
                };
                let before = quotes.partition_point(|quote| quote.ts_init <= fill.ts_event);
                let quote = before
                    .checked_sub(1)
                    .map(|at| quotes[at])
                    .with_context(|| {
                        format!("no quote of {id} before its fill at {}", fill.ts_event)
                    })?;
                let mid = quote.extract_price(PriceType::Mid)?.as_decimal();
                let price = fill.last_px.as_decimal();
                let paid = if fill.order_side == OrderSide::Buy {
                    price - mid
                } else {
                    mid - price
                };
                let size = fill.last_qty.as_decimal() * instrument.multiplier().as_decimal();
                costs
                    .entry(fill.currency.code.to_string())
                    .or_default()
                    .spread += paid * size;
            }
        }
        Ok(Self { result, costs })
    }
}

/// `outcome` as text: one `name: value` line per figure, sections in a
/// fixed order and names sorted, so that equal results print equally.
///
/// The costs of each currency: the commission, the spread paid, their sum
/// per trade (a fill), and the gross PnL, the PnL before both: Nautilus'
/// `PnL (total)` plus the two. That PnL is realized only, so the gross
/// also holds the spread a position still open at the end paid to open. A
/// currency with a fill on no quote (a run on bars) prints its spread as
/// not applicable, and its cost per trade and gross PnL hold the commission
/// alone.
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
            // Last of the section's sorted names.
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

/// Appends `title` and the sorted `stats` under it.
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
        enums::OrderType,
        identifiers::ClientOrderId,
        instruments::{InstrumentAny, stubs::equity_aapl},
        orders::{
            OrderAny, OrderTestBuilder,
            stubs::{OrderFilledTestBuilder, TestOrderStubs},
        },
        types::{Currency, Money, Price, Quantity},
    };

    use super::*;

    /// SXR8.IBIS, priced in EUR.
    fn sxr8() -> InstrumentAny {
        let mut equity = equity_aapl();
        equity.id = InstrumentId::from("SXR8.IBIS");
        equity.currency = Currency::EUR();
        InstrumentAny::Equity(equity)
    }

    /// A market order `id` of `instrument`, accepted, then filled on
    /// `side` for `qty` at `px` at 2 ns, charged `commission`.
    fn filled(
        id: &str,
        instrument: &InstrumentAny,
        side: OrderSide,
        qty: u64,
        px: &str,
        commission: &str,
    ) -> OrderAny {
        let order = OrderTestBuilder::new(OrderType::Market)
            .client_order_id(ClientOrderId::from(id))
            .instrument_id(instrument.id())
            .side(side)
            .quantity(Quantity::from(qty))
            .build();
        let mut order = TestOrderStubs::make_accepted_order(&order);
        let fill = OrderFilledTestBuilder::new(&order, instrument)
            .last_px(Price::from(px))
            .commission(Money::from(commission))
            .ts_event(UnixNanos::from(2))
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
        // Three fills of 10 SXR8.IBIS in EUR, each 0.10 off the mid of
        // 100.00, 1.00 EUR of spread: two charged 3 EUR, one 1.25 GBP. Two
        // fills of AAPL.XNAS in USD, of no quote, each charged 0.25 USD.
        for order in [
            filled("O-1", &sxr8, OrderSide::Buy, 10, "100.10", "3 EUR"),
            filled("O-2", &sxr8, OrderSide::Sell, 10, "99.90", "3 EUR"),
            filled("O-3", &sxr8, OrderSide::Buy, 10, "100.10", "1.25 GBP"),
            filled("O-4", &aapl, OrderSide::Buy, 1, "150.00", "0.25 USD"),
            filled("O-5", &aapl, OrderSide::Sell, 1, "150.00", "0.25 USD"),
        ] {
            cache.add_order(order, None, None, false).unwrap();
        }
        let quote = QuoteTick::new(
            sxr8.id(),
            Price::from("99.90"),
            Price::from("100.10"),
            Quantity::from(1),
            Quantity::from(1),
            UnixNanos::from(1),
            UnixNanos::from(1),
        );
        Outcome::new(result, &cache, &[quote]).unwrap()
    }

    #[test]
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
}
