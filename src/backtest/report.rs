//! The results of a run, as plain text.
//!
//! Prints what the Nautilus `BacktestResult` holds: the order and position
//! counts, its statistics, the final balances from its summary, and the max
//! drawdown, which is not in its default statistics: Nautilus's
//! `MaxDrawdown` (`nautilus_analysis`) over the returns series. Next to the
//! PnL it prints the costs, which the result does not hold: from the fills
//! (`OrderFilled`, with their commission) of the orders in the engine's
//! cache, and the quotes they filled on.

use std::{collections::BTreeMap, fmt::Write};

use anyhow::{Context, ensure};
use nautilus_analysis::{statistic::PortfolioStatistic, statistics::max_drawdown::MaxDrawdown};
use nautilus_backtest::result::BacktestResult;
use nautilus_common::cache::Cache;
use nautilus_model::{
    data::QuoteTick, enums::OrderSide, events::OrderEventAny, identifiers::InstrumentId,
    instruments::Instrument, orders::Order,
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
}

impl Outcome {
    /// `result` and the costs of every fill of the orders in `cache`, each
    /// on the last of `quotes` of its instrument stamped at or before the
    /// fill: the quote the venue filled it on.
    ///
    /// # Errors
    ///
    /// Returns an error if a fill's instrument is not in `cache` or is
    /// inverse (its value is not linear in the price), or if no quote of
    /// it precedes the fill.
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
                let quotes = quoted.get(&id).map_or(&[][..], Vec::as_slice);
                let before = quotes.partition_point(|quote| quote.ts_init <= fill.ts_event);
                let quote = before
                    .checked_sub(1)
                    .map(|at| quotes[at])
                    .with_context(|| {
                        format!("no quote of {id} before its fill at {}", fill.ts_event)
                    })?;
                let mid =
                    (quote.bid_price.as_decimal() + quote.ask_price.as_decimal()) / Decimal::TWO;
                let price = fill.last_px.as_decimal();
                let paid = if fill.order_side == OrderSide::Buy {
                    price - mid
                } else {
                    mid - price
                };
                let size = fill.last_qty.as_decimal() * instrument.multiplier().as_decimal();
                let cost = costs.entry(fill.currency.code.to_string()).or_default();
                cost.spread += paid * size;
                cost.trades += 1;
                if let Some(commission) = fill.commission {
                    let code = commission.currency.code.to_string();
                    costs.entry(code).or_default().commission += commission.as_decimal();
                }
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
/// also holds the spread a position still open at the end paid to open.
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
        let total = costs.commission + costs.spread;
        let mut figures = BTreeMap::from([
            ("Commission".to_string(), f64(costs.commission)),
            ("Spread".to_string(), f64(costs.spread)),
        ]);
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
    }
    let mut returns = result.stats_returns.clone();
    if let Some(drawdown) = MaxDrawdown::new().calculate_from_returns(&result.returns_series) {
        returns.insert(MaxDrawdown::new().name(), drawdown);
    }
    section(&mut out, "Return statistics", &returns);
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

    use super::*;

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
            total_orders: 9,
            total_positions: 5,
            summary: [
                ("orders.total", "9"),
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
                ("Average (Return)", 0.001),
            ]),
            stats_general: stats(&[("Long Ratio", 0.5)]),
            // Up 10%, then down 20%: the equity falls from 1.1 to 0.88.
            returns_series: [(UnixNanos::from(1), 0.1), (UnixNanos::from(2), -0.2)]
                .into_iter()
                .collect(),
        };
        // 9 fills in EUR; a commission in GBP, of fills priced in EUR too.
        let costs = BTreeMap::from([
            (
                "EUR".to_string(),
                Costs {
                    commission: Decimal::new(2700, 2),
                    spread: Decimal::new(450, 2),
                    trades: 9,
                },
            ),
            (
                "GBP".to_string(),
                Costs {
                    commission: Decimal::new(125, 2),
                    ..Costs::default()
                },
            ),
        ]);
        Outcome { result, costs }
    }

    #[test]
    fn prints_the_counts_the_costs_and_every_statistic_sorted_by_section() {
        let expected = "\
Orders: 9
Positions: 5

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
  Commission: 27.000000
  Cost per trade: 3.500000
  Gross PnL: -26.500000
  Spread: 4.500000

Costs (GBP)
  Commission: 1.250000
  Gross PnL: 4.750000
  Spread: 0.000000

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
