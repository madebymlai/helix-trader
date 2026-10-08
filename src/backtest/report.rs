//! The results of a run, as plain text.
//!
//! Prints what the Nautilus `BacktestResult` holds: the order and position
//! counts, its statistics, the final balances from its summary, and the max
//! drawdown, which is not in its default statistics: Nautilus's
//! `MaxDrawdown` (`nautilus_analysis`) over the returns series.

use std::{collections::BTreeMap, fmt::Write};

use nautilus_analysis::{statistic::PortfolioStatistic, statistics::max_drawdown::MaxDrawdown};
use nautilus_backtest::result::BacktestResult;

/// `result` as text: one `name: value` line per figure, sections in a fixed
/// order and names sorted, so that equal results print equally.
#[must_use]
pub fn report(result: &BacktestResult) -> String {
    let mut out = format!(
        "Orders: {}\nPositions: {}\n",
        result.total_orders, result.total_positions
    );
    let pnls: BTreeMap<_, _> = result.stats_pnls.iter().collect();
    for (currency, stats) in pnls {
        section(&mut out, &format!("PnL statistics ({currency})"), stats);
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

    fn result() -> BacktestResult {
        let stats = |pairs: &[(&str, f64)]| {
            pairs
                .iter()
                .map(|&(name, value)| (name.to_string(), value))
                .collect()
        };
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
        }
    }

    #[test]
    fn prints_the_counts_and_every_statistic_sorted_by_section() {
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

Return statistics
  Average (Return): 0.001000
  Max Drawdown: -0.200000
  Sharpe Ratio (252 days): 1.250000

General statistics
  Long Ratio: 0.500000

Final balances
  account.IBIS.balance.EUR.total: 9942.00 EUR
";
        assert_eq!(report(&result()), expected);
    }
}
