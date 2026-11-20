//! What a model's decisions took and earned on rows, as a backtest's out-of-sample scores read it
//! (`walk`), and the examples a model is fitted on (`Example`).
//!
//! The decision takes some of the trades of its rows: the sides', the side picked on each row
//! (`deciding::DecidingModels`); a meta model's (`MetaMargin`), the trades of the side picked its
//! margin takes. What the trades earned (`Earnings`) is Nautilus' `Expectancy` and `WinRate` of
//! their net P&Ls in R (`calculate_from_realized_pnls`), each trade's net P&L over its unit, the
//! money one R of it is, as its model learns it (`Example::realized`), so that the same figures
//! describe a model whatever its trades' size or instrument. Beside them is the same of every trade
//! of the rows, which taking them all would earn, and both again at 1.5 times the commission
//! (`STRESS`).
//!
//! Nautilus' `Expectancy` is the mean win times the win rate plus the mean loss times the loss
//! rate, the rates among the trades that gained or lost: a trade at nought, counted among the
//! trades, weighs in neither. Nautilus scores trades, not predictions: how well a model predicted
//! the rows is `score::ExpectedScore`. No figure here decides whether a model is published
//! (`train`).

use std::fmt::Display;

use nautilus_analysis::{
    statistic::PortfolioStatistic,
    statistics::{expectancy::Expectancy, win_rate::WinRate},
};
use nautilus_core::UnixNanos;
use serde::{Deserialize, Serialize};

use super::{dataset::DatasetRow, targets::Side};
use crate::strategy::risk::R;

/// The multiple of the commission the stressed earnings are at.
pub const STRESS: f64 = 1.5;

/// A row's trade: its net P&L and the commission it is net of, and its unit, the money one R of it
/// is (`targets::SideTarget::unit`), all in the target's P&L currency.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Trade {
    pub pnl: f64,
    pub commission: f64,
    pub unit: f64,
}

/// A row as a model is fitted and judged on it: its features, the trade it is of, whose realized R
/// the model learns (`realized`), the decision's time and when its target settled.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Example {
    pub features: Vec<f64>,
    /// The trade of the row: of a side model's side, or of the side picked for a meta.
    pub trade: Trade,
    pub ts_event: UnixNanos,
    pub settled_at: UnixNanos,
}

/// What trades earned: their count, and Nautilus' `Expectancy` and `WinRate` of their net P&Ls in
/// R, none without a trade.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct Trades {
    pub count: usize,
    /// The expectancy: the average net P&L per trade, in R.
    pub expectancy: Option<f64>,
    pub win_rate: Option<f64>,
}

/// What decisions earned on rows, beside every trade of the rows, at the commission and at `STRESS`
/// times it; all at nought, the default, of no trade judged.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct Earnings {
    /// The trades taken.
    pub taken: Trades,
    /// Every trade of the rows.
    pub every: Trades,
    /// The trades taken, at `STRESS` times the commission.
    pub taken_stressed: Trades,
    /// Every trade, at `STRESS` times the commission.
    pub every_stressed: Trades,
}

/// A meta model's decision rule: the trade of the side picked taken when the net R the meta expects
/// of it, its calibrated expected R, is at least the margin, in R (`takes`, `MetaConfig::margin`),
/// in a decision as in a backtest's scores.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MetaMargin(pub R);

impl MetaMargin {
    /// Whether a meta model expecting `expected_r` of the trade takes it: at or above the margin.
    #[must_use]
    pub fn takes(self, expected_r: R) -> bool {
        expected_r >= self.0
    }
}

impl Trade {
    /// Its net P&L in R had the commission been `factor` times what it was.
    fn r_at(self, factor: f64) -> f64 {
        (1.0 - factor).mul_add(self.commission, self.pnl) / self.unit
    }
}

impl Trades {
    /// What trades of net P&Ls `rs`, in R, earned.
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
    /// The earnings of `rows`, each whether the decision took it and its trade: of each trade's net
    /// P&L in R.
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
    /// The example of `row` on `side`'s trade: the side's row (`DatasetRow::side`), the trade and
    /// its payoff's unit.
    pub(super) fn of_side(row: &DatasetRow, side: Side) -> Self {
        Self {
            features: row.side(side).features,
            trade: row.trade(side),
            ts_event: row.ts_event,
            settled_at: row.settled_at,
        }
    }

    /// The realized value of its trade: its net P&L over its unit, in R.
    pub(super) fn realized(&self) -> f64 {
        self.trade.r_at(1.0)
    }
}

/// `value` to six places, or n/a.
fn value(value: Option<f64>) -> String {
    value.map_or_else(|| "n/a".to_string(), |value| format!("{value:.6}"))
}

/// `r`, a value in R, to six places with its suffix, or n/a.
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

    /// A trade of `pnl` net of 0.3 of commission, one R of it worth `unit`.
    fn trade(pnl: f64, unit: f64) -> Trade {
        Trade {
            pnl,
            commission: 0.3,
            unit,
        }
    }

    /// A margin of 0.25R, and one below nought, which a trade expected to lose a little clears.
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

    /// Taken, wins of 3 over units of 2, 1.5R twice, and a loss of 2 over a unit of 4, -0.5R: no
    /// trade at nought, so their mean, 5/6R, where their money would make 4/3. Every trade, besides
    /// a win of 0.5 over a unit of 1, 0.5R, and a loss of 2 over a unit of 2, -1R: 0.4R, of a win
    /// rate of 3/5. At 1.5 times the commission of 0.3 each loses 0.15 more, 0.15 of its unit's R:
    /// the trades taken 0.075R, 0.0375R and 0.075R, 0.0625R a trade; every trade 0.15R and 0.075R
    /// besides, 0.0825R a trade. None changes sign, so the win rates stay 2/3 and 3/5 over the same
    /// 3 and 5 trades.
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

    /// Pins that Nautilus' `Expectancy` weighs its wins and losses by their rates among the trades
    /// that gained or lost, a trade at nought in neither, `avg_winner * win_rate + avg_loser *
    /// loss_rate` (`crates/analysis/src/statistics/expectancy.rs:75-95`): `Trades::of` takes its
    /// expectancy, which a backtest's scores report (`walk::WalkForward::scores`), and the module
    /// states that rule.
    ///
    /// A trade at nought is neither a win nor a loss in Nautilus' expectancy, a win of 2 and a loss
    /// of 1 making 0.5, not a third.
    #[rstest]
    fn drift_a_trade_at_nought_weighs_in_no_expectancy() {
        let trades = Trades::of(&[2.0, -1.0, 0.0]);

        close(trades.expectancy, 0.5);
    }

    /// A trade at nought, neither a win nor a loss, is a trade all the same: a win, a loss and one
    /// at nought are three.
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
