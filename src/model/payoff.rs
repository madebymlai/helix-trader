//! What a model primary's trade on a side is expected to make, and the
//! side it trades: decisions on expected net P&L, each decision weighing
//! the costs of its own (Elkan 2001; Bahnsen et al. 2015).
//!
//! A side's model is published with its expected win and loss
//! (`Payoff`): on a barrier, in R, the multiple of the stop's distance a
//! trade realised, its net P&L (fees and slippage included) over its own
//! stop's worth (`targets::stop_worth`), so a stop-out with slippage can
//! be below -1R; on a horizon, which has no stop, in money. They are the
//! means of the training rows' wins and of their losses, Nautilus'
//! `AvgWinner` and `AvgLoser` of their realised values. At a decision a
//! side's expected net P&L is its calibrated probability `p` times the
//! win, less `1 - p` times the loss (`Payoff::expected`), on a barrier
//! times the worth of the stop the decision sets: the costs, in the
//! realised R, scale with each decision's stop, and a fixed cost weighs
//! more against a tight ATR stop. A side trades if that is at or above its
//! margin (`ModelConfig::margins`); of two, the larger, none at a tie
//! (`pick`).

use std::path::Path;

use anyhow::{Context, ensure};
use nautilus_analysis::{
    statistic::PortfolioStatistic,
    statistics::{loser_avg::AvgLoser, winner_avg::AvgWinner},
};
use nautilus_model::{instruments::InstrumentAny, types::Quantity};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use serde::{Deserialize, Serialize};

use super::{
    dataset::DatasetRow,
    holdout::Trade,
    targets::{self, Label, Side, TargetsConfig},
};

/// The expected win and loss of a side's trade, both positive: in R on a
/// barrier, in the targets' P&L currency on a horizon.
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

/// How the trades of a dataset's rows are read: each side's trade of
/// their label, its net P&L and commission (`holdout::Trade`), and the
/// unit of its payoff, the worth of its stop on a barrier
/// (`targets::stop_worth`, the instrument the catalog holds), one on a
/// horizon, its realised value its net P&L over its unit.
#[derive(Debug)]
pub struct Realised {
    /// Where each side's P&L, its commission and, on a barrier, its
    /// stop's distance are among a row's target values, the long's first.
    sides: [(usize, usize, Option<usize>); 2],
    /// On a barrier, the targets' instrument and quantity, which make a
    /// distance money.
    worth: Option<(InstrumentAny, Quantity)>,
}

impl Realised {
    /// The reading of the rows of `targets` learned as `label`, a barrier's
    /// stops worth what they are on the targets' instrument as the catalog
    /// at `catalog` last holds it.
    ///
    /// # Errors
    ///
    /// Returns an error if the targets have no `label` target, or the
    /// catalog holds no instrument of the barrier's targets.
    pub fn new(catalog: &Path, targets: &TargetsConfig, label: Label) -> anyhow::Result<Self> {
        let sides = Side::BOTH.map(|side| {
            (
                targets.pnl_index(label, side),
                targets.commission_index(label, side),
                targets.stop_index(label, side),
            )
        });
        let [
            (Some(long), Some(long_commission), long_stop),
            (Some(short), Some(short_commission), short_stop),
        ] = sides
        else {
            anyhow::bail!("the label {label:?} has no target");
        };
        let worth = match label {
            Label::Horizon => None,
            Label::Barrier => {
                let id = targets.instrument_id;
                let instrument = ParquetDataCatalog::new(catalog, None, None, None, None)
                    .query_instruments(Some(&[id.to_string()]))
                    .with_context(|| format!("reading the instrument {id}"))?
                    .pop()
                    .with_context(|| format!("the catalog holds no instrument {id}"))?;
                Some((instrument, targets.quantity))
            }
        };
        Ok(Self {
            sides: [
                (long, long_commission, long_stop),
                (short, short_commission, short_stop),
            ],
            worth,
        })
    }

    /// Whether `side`'s trade in `row` gained after its costs: the label of
    /// its model.
    #[must_use]
    pub fn gained(&self, row: &DatasetRow, side: Side) -> bool {
        row.targets[self.side(side).0] > 0.0
    }

    /// `side`'s trade in `row`: its net P&L and its commission.
    #[must_use]
    pub fn trade(&self, row: &DatasetRow, side: Side) -> Trade {
        let (pnl, commission, _) = self.side(side);
        Trade {
            pnl: row.targets[pnl],
            commission: row.targets[commission],
        }
    }

    /// The unit of `side`'s payoff in `row`, in money: the worth of the
    /// stop its decision set on a barrier, one on a horizon.
    ///
    /// # Errors
    ///
    /// Returns an error if the stop cannot be made in the instrument's
    /// prices.
    pub fn unit(&self, row: &DatasetRow, side: Side) -> anyhow::Result<f64> {
        match (&self.worth, self.side(side).2) {
            (Some((instrument, quantity)), Some(stop)) => {
                targets::stop_worth(instrument, *quantity, row.targets[stop])
            }
            _ => Ok(1.0),
        }
    }

    const fn side(&self, side: Side) -> (usize, usize, Option<usize>) {
        match side {
            Side::Long => self.sides[0],
            Side::Short => self.sides[1],
        }
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

    /// Worked by hand. Gold's barrier a take profit of 2 ATRs and a stop
    /// of 1, a trade of one ounce: its long wins 1.8R and loses 1.2R, the
    /// costs in both, a probability of 0.6 expected 0.6R. At a decision
    /// whose ATR is 5.00 the stop is worth 5 USD, the trade expected 3 USD;
    /// at one whose ATR is 1.00, 1 USD, expected 0.60 USD. With a margin of
    /// 2 USD the first trades and the second does not, at the same
    /// probability: a fixed threshold on it trades both or neither.
    #[test]
    fn the_same_probability_trades_against_a_wide_atr_stop_and_not_a_tight_one() {
        let payoff = Payoff {
            win: 1.8,
            loss: 1.2,
        };
        let margin = 2.0;
        let decide = |stop: f64| pick([(Side::Long, payoff.expected(0.6) * stop, margin)]);

        assert_eq!(decide(5.0), Some(Side::Long));
        assert_eq!(decide(1.0), None);
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
