//! Measures what a model's decisions took and earned on rows, as a backtest's out-of-sample scores
//! read it (`walk`). It also defines the examples a model is fitted on (`Example`).
//!
//! `Earnings` holds what the trades taken earned. That is Nautilus' `Expectancy` and `WinRate` of
//! their net P&Ls in R (`calculate_from_realized_pnls`). Each net P&L is divided by its unit as
//! the model learns it (`Example::realized`), so the figures hold whatever the trades' size or
//! instrument. `Earnings` holds the same figures for every trade of the rows, and both again at
//! 1.5 times the commission (`STRESS`).
//!
//! Nautilus' `Expectancy` is the mean win times the win rate plus the mean loss times the loss
//! rate. The rates are among the trades that gained or lost, so a trade at 0 counts as a trade but
//! weighs in neither. These figures score trades, not predictions (`score::ExpectedScore`). They
//! decide nothing about publishing a model (`train`).

use std::fmt::Display;

use nautilus_analysis::{
    statistic::PortfolioStatistic,
    statistics::{expectancy::Expectancy, win_rate::WinRate},
};
use nautilus_core::UnixNanos;
use serde::{Deserialize, Serialize};

use super::{dataset::DatasetRow, targets::Side};
use crate::strategy::risk::R;

/// The multiple of the commission that the stressed earnings use.
pub const STRESS: f64 = 1.5;

/// A row's trade, all in the target's P&L currency.
///
/// It holds the net P&L, the commission that P&L is net of, and the unit. The unit is the money one
/// R is (`targets::SideTarget::unit`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Trade {
    pub pnl: f64,
    pub commission: f64,
    pub unit: f64,
}

/// A row as a model is fitted and judged on it.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Example {
    pub features: Vec<f64>,
    /// The trade on a side model's side, or on the side picked for a meta.
    pub trade: Trade,
    pub ts_event: UnixNanos,
    pub settled_at: UnixNanos,
}

/// What trades earned.
///
/// It holds their count, and the `Expectancy` and `WinRate` of their net P&Ls in R. Both are
/// `None` without a trade.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct Trades {
    pub count: usize,
    pub expectancy: Option<f64>,
    pub win_rate: Option<f64>,
}

/// What decisions earned on rows.
///
/// It sits beside what every trade of the rows earned, both at the commission and at `STRESS`
/// times it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct Earnings {
    pub taken: Trades,
    pub every: Trades,
    pub taken_stressed: Trades,
    pub every_stressed: Trades,
}

/// A meta model's decision rule, the same in a decision as in a backtest's scores.
///
/// The trade of the side picked is taken when the meta's expected R is at least the margin
/// (`MetaConfig::margin`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MetaMargin(pub R);

impl MetaMargin {
    /// Returns whether a meta expecting `expected_r` takes the trade, which it does at or above the
    /// margin.
    #[must_use]
    pub fn takes(self, expected_r: R) -> bool {
        expected_r >= self.0
    }
}

impl Trade {
    fn r_at(self, factor: f64) -> f64 {
        (1.0 - factor).mul_add(self.commission, self.pnl) / self.unit
    }
}

impl Trades {
    /// Returns the earnings, in R, of trades with net P&Ls `rs`.
    #[must_use]
    pub fn of(rs: &[f64]) -> Self {
        let some = |value: Option<f64>| value.filter(|value| !value.is_nan());
        Self {
            count: rs.len(),
            expectancy: some(Expectancy {}.calculate_from_realized_pnls(rs)),
            win_rate: some(WinRate {}.calculate_from_realized_pnls(rs)),
        }
    }
}

impl Earnings {
    /// Returns the earnings of `rows`. Each row pairs whether the decision took it with its trade.
    #[must_use]
    pub fn of(rows: &[(bool, Trade)]) -> Self {
        let trades = |taken_only: bool, factor: f64| {
            let rs: Vec<f64> = rows
                .iter()
                .filter(|(taken, _)| *taken || !taken_only)
                .map(|(_, trade)| trade.r_at(factor))
                .collect();
            Trades::of(&rs)
        };
        Self {
            taken: trades(true, 1.0),
            every: trades(false, 1.0),
            taken_stressed: trades(true, STRESS),
            every_stressed: trades(false, STRESS),
        }
    }
}

impl Example {
    pub(super) fn of_side(row: &DatasetRow, side: Side) -> Self {
        Self {
            features: row.side(side).features,
            trade: row.trade(side),
            ts_event: row.ts_event,
            settled_at: row.settled_at,
        }
    }

    pub(super) fn realized(&self) -> f64 {
        self.trade.r_at(1.0)
    }
}

fn value(value: Option<f64>) -> String {
    value.map_or_else(|| "n/a".to_string(), |value| format!("{value:.6}"))
}

pub(super) fn in_r(r: Option<f64>) -> String {
    r.map_or_else(|| "n/a".to_string(), |r| format!("{r:.6}R"))
}

impl Display for Trades {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} trades, expectancy {}, win rate {}",
            self.count,
            in_r(self.expectancy),
            value(self.win_rate)
        )
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn close(actual: Option<f64>, expected: f64) {
        let actual = actual.unwrap();
        assert!((actual - expected).abs() < 1e-9, "{actual} != {expected}");
    }

    fn trade(pnl: f64, unit: f64) -> Trade {
        Trade {
            pnl,
            commission: 0.3,
            unit,
        }
    }

    #[rstest]
    #[case::at_the_margin(0.25, 0.25, true)]
    #[case::above_the_margin(0.25, 0.26, true)]
    #[case::below_the_margin(0.25, 0.24, false)]
    #[case::at_a_margin_below_nought(-0.5, -0.5, true)]
    #[case::below_a_margin_below_nought(-0.5, -0.75, false)]
    fn a_meta_margin_takes_an_expected_r_at_or_above_it(
        #[case] margin: f64,
        #[case] expected_r: f64,
        #[case] taken: bool,
    ) {
        assert_eq!(MetaMargin(R::new(margin)).takes(R::new(expected_r)), taken);
    }

    /// The stressed run charges 1.5 times the commission of 0.3, so each trade loses 0.15 more.
    #[rstest]
    fn the_earnings_are_nautilus_expectancy_and_win_rate_of_the_r_of_the_trades_taken_and_of_every_trade()
     {
        let rows = [
            (true, trade(3.0, 2.0)),
            (true, trade(-2.0, 4.0)),
            (true, trade(3.0, 2.0)),
            (false, trade(0.5, 1.0)),
            (false, trade(-2.0, 2.0)),
        ];

        let earnings = Earnings::of(&rows);

        assert_eq!(earnings.taken.count, 3);
        close(earnings.taken.expectancy, 5.0 / 6.0);
        close(earnings.taken.win_rate, 2.0 / 3.0);
        assert_eq!(earnings.every.count, 5);
        close(earnings.every.expectancy, 0.4);
        close(earnings.every.win_rate, 0.6);
        assert_eq!(earnings.taken_stressed.count, 3);
        close(earnings.taken_stressed.expectancy, 5.0 / 6.0 - 0.0625);
        close(earnings.taken_stressed.win_rate, 2.0 / 3.0);
        assert_eq!(earnings.every_stressed.count, 5);
        close(earnings.every_stressed.expectancy, 0.4 - 0.0825);
        close(earnings.every_stressed.win_rate, 0.6);
    }

    /// Pins that Nautilus' `Expectancy` is `avg_winner * win_rate + avg_loser * loss_rate`, with
    /// the rates among the trades that gained or lost
    /// (`crates/analysis/src/statistics/expectancy.rs:75-95`). `Trades::of` takes it.
    #[rstest]
    fn drift_a_trade_at_nought_weighs_in_no_expectancy() {
        let trades = Trades::of(&[2.0, -1.0, 0.0]);

        close(trades.expectancy, 0.5);
    }

    #[rstest]
    fn a_trade_at_nought_is_counted() {
        let trades = Trades::of(&[2.0, -1.0, 0.0]);

        assert_eq!(trades.count, 3);
    }

    #[rstest]
    fn no_trade_earns_nothing_to_report() {
        assert_eq!(
            Trades::of(&[]),
            Trades {
                count: 0,
                expectancy: None,
                win_rate: None
            }
        );
    }
}
