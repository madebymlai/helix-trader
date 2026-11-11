//! A retrained model's holdout as its publication reads it: what the decision
//! would have earned there, not how well a model ranks the rows.
//!
//! The decision takes some of the trades of its rows: the sides', the side
//! picked on each row (`deciding::DecidingModels`), which a train's gate and
//! the walk-forward's scores both read (`train`, `walk`); a meta model's
//! (`Threshold`), the trades of the side picked from its threshold.
//! What the trades earned (`Earnings`) is Nautilus' `Expectancy` and `WinRate`
//! of their net P&Ls (`calculate_from_realized_pnls`), as the backtest
//! report reads them, beside the same of every trade of the rows, which
//! taking them all would earn; and both again at 1.5 times the
//! commission (`STRESS`), reported only. Nautilus' `Expectancy` is the
//! mean win times the win rate plus the mean loss times the loss rate,
//! the rates among the trades that gained or lost: a trade at nought,
//! counted among the trades, weighs in neither.
//!
//! A model is published (`Holdout::refusal` none) when, on its holdout,
//! its calibrated probabilities' log loss is below the baseline's, the
//! training mean's (`Holdout::log_loss_refusal`), and the trades taken, at
//! least the minimum count (`Minimums`), earned per trade at least the minimum
//! net P&L and more than every trade of the rows did. The log loss and the AUC
//! are ours (`score`): Nautilus scores trades, not probabilities. The AUC is
//! reported, never required.

use std::fmt;

use nautilus_analysis::{
    statistic::PortfolioStatistic,
    statistics::{expectancy::Expectancy, win_rate::WinRate},
};
use nautilus_core::UnixNanos;
use serde::{Deserialize, Serialize};

use super::{dataset::DatasetRow, deciding::SideRow, targets::Side};

/// The multiple of the commission the stressed earnings are at.
pub const STRESS: f64 = 1.5;

/// A row's trade: its net P&L and the commission it is net of, in the
/// target's P&L currency.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Trade {
    pub pnl: f64,
    pub commission: f64,
}

/// A row as a model is fitted and judged on it: its features and its
/// label, the trade the label is of and the unit of that trade's payoff,
/// the decision's time and when its target settled.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct Example {
    pub features: Vec<f64>,
    pub label: bool,
    /// The trade whose gain the label is: of a side model's side, or of the
    /// side picked for a meta.
    pub trade: Trade,
    /// The unit of the trade's payoff in the row (`DatasetRow::units`).
    pub unit: f64,
    pub ts_event: UnixNanos,
    pub settled_at: UnixNanos,
}

/// What trades earned: their count, and Nautilus' `Expectancy` and
/// `WinRate` of their net P&Ls, none without a trade.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct Trades {
    pub count: usize,
    /// The net P&L per trade.
    pub expectancy: Option<f64>,
    pub win_rate: Option<f64>,
}

/// What decisions earned on rows, beside every trade of the rows, at the
/// commission and at `STRESS` times it; all at nought, the default, of no trade
/// judged.
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

/// A model's holdout as its publication reads it.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Holdout {
    /// What was judged: a meta's own, or the picks between the sides, the same
    /// for each side; none for a side refused on its log loss with no side
    /// surviving.
    pub earnings: Earnings,
    /// The log loss of the model's calibrated probabilities.
    pub log_loss: f64,
    /// The log loss of the baseline, the training mean.
    pub baseline_log_loss: f64,
    /// The AUC of the model's calibrated probabilities; none without a row
    /// of each label.
    pub auc: Option<f64>,
}

/// The least a model's holdout is published with: the net P&L per trade taken,
/// in the target's P&L currency, over at least a count of trades.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Minimums {
    pub min_pnl: f64,
    pub min_trades: usize,
}

/// Why a model was not published.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Refusal {
    /// Its log loss is not below the baseline's.
    LogLoss { log_loss: f64, baseline: f64 },
    /// Fewer trades were taken than the minimum.
    Trades { trades: usize, min_trades: usize },
    /// The net P&L per trade taken is below the minimum.
    MinPnl { pnl: f64, min_pnl: f64 },
    /// The net P&L per trade taken is not above every trade's.
    Every { pnl: f64, every: f64 },
}

/// A meta model's decision rule: the trade of the side picked taken from the
/// threshold (`takes`), in a decision as in a holdout.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Threshold(pub f64);

impl Threshold {
    /// Whether a meta model's `probability` takes the trade: at or above
    /// the threshold, so NaN does not.
    #[must_use]
    pub fn takes(self, probability: f64) -> bool {
        probability >= self.0
    }
}

impl Trade {
    /// Its net P&L had the commission been `factor` times what it was.
    fn pnl_at(self, factor: f64) -> f64 {
        (1.0 - factor).mul_add(self.commission, self.pnl)
    }
}

impl Trades {
    /// What trades of net P&Ls `pnls` earned.
    #[must_use]
    pub fn of(pnls: &[f64]) -> Self {
        let some = |value: Option<f64>| value.filter(|value| !value.is_nan());
        Self {
            count: pnls.len(),
            expectancy: some(Expectancy {}.calculate_from_realized_pnls(pnls)),
            win_rate: some(WinRate {}.calculate_from_realized_pnls(pnls)),
        }
    }
}

impl Earnings {
    /// The earnings of `rows`, each whether the decision took it and its trade.
    #[must_use]
    pub fn of(rows: &[(bool, Trade)]) -> Self {
        let trades = |taken_only: bool, factor: f64| {
            let pnls: Vec<f64> = rows
                .iter()
                .filter(|(taken, _)| *taken || !taken_only)
                .map(|(_, trade)| trade.pnl_at(factor))
                .collect();
            Trades::of(&pnls)
        };
        Self {
            taken: trades(true, 1.0),
            every: trades(false, 1.0),
            taken_stressed: trades(true, STRESS),
            every_stressed: trades(false, STRESS),
        }
    }
}

impl Holdout {
    /// Why a model of this holdout is not published under `minimums`, the
    /// first reason of its log loss, its count of trades, the minimum net
    /// P&L and every trade's; none if it is.
    #[must_use]
    pub fn refusal(&self, minimums: Minimums) -> Option<Refusal> {
        if let Some(refusal) = self.log_loss_refusal() {
            return Some(refusal);
        }
        let Earnings { taken, every, .. } = self.earnings;
        if taken.count < minimums.min_trades {
            return Some(Refusal::Trades {
                trades: taken.count,
                min_trades: minimums.min_trades,
            });
        }
        let pnl = taken.expectancy.unwrap_or(f64::NAN);
        if pnl.is_nan() || pnl < minimums.min_pnl {
            return Some(Refusal::MinPnl {
                pnl,
                min_pnl: minimums.min_pnl,
            });
        }
        // Every trade includes those taken: it has an expectancy too.
        let every = every.expectancy.unwrap_or(f64::NAN);
        (pnl <= every).then_some(Refusal::Every { pnl, every })
    }

    /// Why a model of this holdout is not published whatever it earned: its
    /// log loss not below the baseline's.
    #[must_use]
    pub fn log_loss_refusal(&self) -> Option<Refusal> {
        (self.log_loss >= self.baseline_log_loss).then_some(Refusal::LogLoss {
            log_loss: self.log_loss,
            baseline: self.baseline_log_loss,
        })
    }
}

impl Example {
    /// The example of `row` on `side`'s trade: the side's row
    /// (`DatasetRow::side`), whether the trade gained, the trade, and its
    /// payoff's unit.
    pub(super) fn of_side(row: &DatasetRow, side: Side) -> Self {
        let SideRow { features, unit } = row.side(side);
        Self {
            features,
            label: row.gained(side),
            trade: row.trade(side),
            unit,
            ts_event: row.ts_event,
            settled_at: row.settled_at,
        }
    }

    /// The realised value of its trade: its net P&L over its unit.
    pub(super) fn realised(&self) -> f64 {
        self.trade.pnl / self.unit
    }
}

/// `value` to six places, or n/a.
fn value(value: Option<f64>) -> String {
    value.map_or_else(|| "n/a".to_string(), |value| format!("{value:.6}"))
}

impl fmt::Display for Trades {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} trades, net P&L per trade {}, win rate {}",
            self.count,
            value(self.expectancy),
            value(self.win_rate)
        )
    }
}

impl fmt::Display for Holdout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Earnings {
            taken,
            every,
            taken_stressed,
            every_stressed,
        } = self.earnings;
        write!(
            f,
            "taken {taken}; every trade {every}; at {STRESS}x the commission, taken {}, \
             every trade {}; log loss {:.6}, baseline {:.6}; AUC {}",
            value(taken_stressed.expectancy),
            value(every_stressed.expectancy),
            self.log_loss,
            self.baseline_log_loss,
            value(self.auc)
        )
    }
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LogLoss { log_loss, baseline } => write!(
                f,
                "holdout log loss {log_loss:.6} not below the baseline's {baseline:.6}"
            ),
            Self::Trades { trades, min_trades } => write!(
                f,
                "{trades} holdout trades taken, fewer than the minimum {min_trades}"
            ),
            Self::MinPnl { pnl, min_pnl } => write!(
                f,
                "holdout net P&L per trade taken {pnl:.6} below the minimum {min_pnl}"
            ),
            Self::Every { pnl, every } => write!(
                f,
                "holdout net P&L per trade taken {pnl:.6} not above every trade's {every:.6}"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(actual: Option<f64>, expected: f64) {
        let actual = actual.unwrap();
        assert!((actual - expected).abs() < 1e-9, "{actual} != {expected}");
    }

    fn trade(pnl: f64) -> Trade {
        Trade {
            pnl,
            commission: 0.3,
        }
    }

    #[test]
    fn a_threshold_takes_a_probability_at_or_above_it_and_not_nan() {
        let threshold = Threshold(0.6);

        assert!(threshold.takes(0.6));
        assert!(threshold.takes(0.61));
        assert!(!threshold.takes(0.59));
        assert!(!threshold.takes(f64::NAN));
    }

    /// Taken, wins of 3 and a loss of 2: 3 times 2/3 less 2 times 1/3,
    /// 4/3. Every trade, besides a win of 0.5 and a loss of 2: the mean
    /// win 13/6 three times in five, the loss 2 twice, 0.5. At 1.5 times
    /// the commission of 0.3 each loses 0.15 more: 7/6 and 0.35.
    #[test]
    fn the_earnings_are_nautilus_expectancy_and_win_rate_of_the_trades_taken_and_of_every_trade() {
        let rows = [
            (true, trade(3.0)),
            (true, trade(-2.0)),
            (true, trade(3.0)),
            (false, trade(0.5)),
            (false, trade(-2.0)),
        ];

        let earnings = Earnings::of(&rows);

        assert_eq!(earnings.taken.count, 3);
        close(earnings.taken.expectancy, 4.0 / 3.0);
        close(earnings.taken.win_rate, 2.0 / 3.0);
        assert_eq!(earnings.every.count, 5);
        close(earnings.every.expectancy, 0.5);
        close(earnings.every.win_rate, 0.6);
        close(earnings.taken_stressed.expectancy, 4.0 / 3.0 - 0.15);
        close(earnings.every_stressed.expectancy, 0.35);
    }

    /// A trade at nought is neither a win nor a loss in Nautilus'
    /// expectancy, a win of 2 and a loss of 1 making 0.5, not a third.
    #[test]
    fn a_trade_at_nought_is_counted_and_weighs_in_no_expectancy() {
        let trades = Trades::of(&[2.0, -1.0, 0.0]);

        assert_eq!(trades.count, 3);
        close(trades.expectancy, 0.5);
    }

    #[test]
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

    /// A holdout whose decision took 3 trades of 1.2 each, every trade's 0.4,
    /// its log loss 0.6 against the baseline's 0.69.
    fn holdout() -> Holdout {
        let trades = |count, expectancy| Trades {
            count,
            expectancy: Some(expectancy),
            win_rate: Some(0.5),
        };
        Holdout {
            earnings: Earnings {
                taken: trades(3, 1.2),
                every: trades(8, 0.4),
                ..Earnings::default()
            },
            log_loss: 0.6,
            baseline_log_loss: 0.69,
            auc: None,
        }
    }

    const MINIMUMS: Minimums = Minimums {
        min_pnl: 1.0,
        min_trades: 3,
    };

    #[test]
    fn a_holdout_beating_the_baseline_over_enough_trades_earning_the_minimum_and_more_than_every_trade_is_published()
     {
        assert_eq!(holdout().refusal(MINIMUMS), None);
        let at_the_minimum = Minimums {
            min_pnl: 1.2,
            ..MINIMUMS
        };
        assert_eq!(holdout().refusal(at_the_minimum), None);
    }

    #[test]
    fn a_log_loss_not_below_the_baselines_is_refused() {
        let worse = Holdout {
            log_loss: 0.69,
            ..holdout()
        };

        assert_eq!(
            worse.refusal(MINIMUMS),
            Some(Refusal::LogLoss {
                log_loss: 0.69,
                baseline: 0.69
            })
        );
    }

    #[test]
    fn fewer_trades_than_the_minimum_are_refused() {
        let minimums = Minimums {
            min_trades: 4,
            ..MINIMUMS
        };

        assert_eq!(
            holdout().refusal(minimums),
            Some(Refusal::Trades {
                trades: 3,
                min_trades: 4
            })
        );
    }

    #[test]
    fn a_net_pnl_per_trade_below_the_minimum_is_refused() {
        let minimums = Minimums {
            min_pnl: 1.5,
            ..MINIMUMS
        };

        assert_eq!(
            holdout().refusal(minimums),
            Some(Refusal::MinPnl {
                pnl: 1.2,
                min_pnl: 1.5
            })
        );
    }

    /// No trade taken has no net P&L per trade: below any minimum.
    #[test]
    fn no_trade_taken_is_refused() {
        let mut none = holdout();
        none.earnings.taken = Trades::of(&[]);
        let minimums = Minimums {
            min_trades: 0,
            ..MINIMUMS
        };

        assert!(
            matches!(none.refusal(minimums), Some(Refusal::MinPnl { .. })),
            "{:?}",
            none.refusal(minimums)
        );
    }

    #[test]
    fn a_net_pnl_per_trade_not_above_every_trades_is_refused() {
        let mut no_better = holdout();
        no_better.earnings.every.expectancy = Some(1.2);

        assert_eq!(
            no_better.refusal(MINIMUMS),
            Some(Refusal::Every {
                pnl: 1.2,
                every: 1.2
            })
        );
    }
}
