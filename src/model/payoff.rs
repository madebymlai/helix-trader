//! What a side's trade is expected to make, and the side picked:
//! decisions on expected net P&L, each decision weighing the costs of its
//! own (Elkan 2001; Bahnsen et al. 2015).
//!
//! A side's model is published with its expected win and loss
//! (`Payoff`): with exits, in R, the multiple of the stop's distance a
//! trade realised, its net P&L (fees and slippage included) over its own
//! stop's worth, the unit of its row (`dataset::DatasetRow::units`), so a
//! stop-out with slippage can be below -1R; on a time-only target, which has
//! no stop, in money, its unit 1. They
//! are the means of the training rows' wins and of their losses, Nautilus'
//! `AvgWinner` and `AvgLoser` of their realised values. At a decision a
//! side's expected net P&L is its calibrated probability `p` times the
//! win, less `1 - p` times the loss (`Payoff::expected`), with exits times
//! the worth of the stop the decision sets: the costs, in the
//! realised R, scale with each decision's stop, and a fixed cost weighs
//! more against a tight ATR stop. A side trades if that is at or above its
//! margin (`ModelConfig::margins`); of two, the larger, none at a tie
//! (`pick`).

use anyhow::ensure;
use nautilus_analysis::{
    statistic::PortfolioStatistic,
    statistics::{loser_avg::AvgLoser, winner_avg::AvgWinner},
};
use serde::{Deserialize, Serialize};

/// The expected win and loss of a side's trade, both positive: in R with
/// exits, in the target's P&L currency on a time-only target.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Payoff {
    pub win: f64,
    pub loss: f64,
}

impl Payoff {
    /// The payoff of trades that realised `realised`: the mean of the
    /// gains, Nautilus' `AvgWinner`, and of the losses, less its
    /// `AvgLoser`.
    ///
    /// # Errors
    ///
    /// Returns an error if no trade gained or none lost.
    pub fn of(realised: &[f64]) -> anyhow::Result<Self> {
        let win = AvgWinner {}
            .calculate_from_realized_pnls(realised)
            .unwrap_or(f64::NAN);
        let loss = -AvgLoser {}
            .calculate_from_realized_pnls(realised)
            .unwrap_or(f64::NAN);
        ensure!(
            win.is_finite() && loss.is_finite(),
            "no win or no loss among the {} rows to expect a trade's from",
            realised.len()
        );
        Ok(Self { win, loss })
    }

    /// The expected net P&L of a trade won with `probability`, in the
    /// payoff's unit: `p` times the win, less `1 - p` times the loss.
    #[must_use]
    pub fn expected(&self, probability: f64) -> f64 {
        probability.mul_add(self.win, -(1.0 - probability) * self.loss)
    }
}

/// The side traded of `sides`, each a side (or what the caller has of it,
/// its probability say), its expected net P&L and its margin: of those at
/// or above their margin, the one expected the most; none if no side
/// clears its margin, or two tie.
#[must_use]
pub fn pick<T>(sides: impl IntoIterator<Item = (T, f64, f64)>) -> Option<T> {
    let cleared: Vec<(T, f64)> = sides
        .into_iter()
        .filter(|&(_, expected, margin)| expected >= margin)
        .map(|(side, expected, _)| (side, expected))
        .collect();
    let best = cleared
        .iter()
        .map(|&(_, expected)| expected)
        .fold(f64::NEG_INFINITY, f64::max);
    let mut best = cleared
        .into_iter()
        .filter(|&(_, expected)| expected == best);
    match (best.next(), best.next()) {
        (Some((side, _)), None) => Some(side),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::targets::Side;

    fn close(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < 1e-12, "{actual} != {expected}");
    }

    /// Wins of 2R and 1.5R, losses of 1R and 1.25R (a stop slipped), and a
    /// trade at nought, neither: a win of 1.75R, a loss of 1.125R.
    #[test]
    fn a_payoff_is_the_mean_win_and_the_mean_loss() {
        let payoff = Payoff::of(&[2.0, -1.0, 1.5, -1.25, 0.0]).unwrap();

        close(payoff.win, 1.75);
        close(payoff.loss, 1.125);
    }

    #[test]
    fn trades_without_a_win_or_without_a_loss_have_no_payoff() {
        assert!(Payoff::of(&[1.0, 2.0]).is_err());
        assert!(Payoff::of(&[-1.0]).is_err());
        assert!(Payoff::of(&[]).is_err());
    }

    /// 0.6 of 1.8R less 0.4 of 1.2R: 0.6R.
    #[test]
    fn the_expected_pnl_is_the_probable_win_less_the_probable_loss() {
        let payoff = Payoff {
            win: 1.8,
            loss: 1.2,
        };

        close(payoff.expected(0.6), 0.6);
        close(payoff.expected(0.4), 0.0);
    }

    #[test]
    fn a_side_trades_from_its_margin() {
        assert_eq!(pick([(Side::Short, 1.0, 1.0)]), Some(Side::Short));
        assert_eq!(pick([(Side::Short, 0.99, 1.0)]), None);
        assert_eq!(pick([(Side::Long, f64::NAN, 1.0)]), None);
    }

    #[test]
    fn of_two_sides_clearing_their_margins_the_larger_trades() {
        assert_eq!(
            pick([(Side::Long, 1.5, 1.0), (Side::Short, 2.0, 1.0)]),
            Some(Side::Short)
        );
        assert_eq!(
            pick([(Side::Long, 1.5, 1.0), (Side::Short, 2.0, 2.5)]),
            Some(Side::Long)
        );
    }

    #[test]
    fn two_sides_tied_trade_neither() {
        assert_eq!(
            pick([(Side::Long, 2.0, 1.0), (Side::Short, 2.0, 1.0)]),
            None
        );
    }

    #[test]
    fn no_side_trades_nothing() {
        assert_eq!(pick::<Side>([]), None);
    }
}
