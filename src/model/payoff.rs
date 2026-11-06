//! What a side's trade is expected to make, and the side picked:
//! decisions on expected net P&L, each decision weighing the costs of its
//! own (Elkan 2001; Bahnsen et al. 2015).
//!
//! A side's model is published with its expected win and loss
//! (`Payoff`): with exits, in R, the multiple of the stop's distance a
//! trade realised, its net P&L (fees and slippage included) over its own
//! stop's worth (`targets::stop_worth`), so a stop-out with slippage can
//! be below -1R; on a time-only target, which has no stop, in money. They
//! are the means of the training rows' wins and of their losses, Nautilus'
//! `AvgWinner` and `AvgLoser` of their realised values. At a decision a
//! side's expected net P&L is its calibrated probability `p` times the
//! win, less `1 - p` times the loss (`Payoff::expected`), with exits times
//! the worth of the stop the decision sets: the costs, in the
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
    targets::{self, PerSide, Side, TargetConfig},
};

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

/// How the trades of a dataset's rows are read: each side's trade, its net
/// P&L and commission (`holdout::Trade`), and the unit of its payoff, the
/// worth of its stop with exits (`targets::stop_worth`, the instrument the
/// catalog holds), one without, its realised value its net P&L over its
/// unit.
#[derive(Debug)]
pub struct Realised {
    /// Where each side's P&L, its commission and, with exits, its stop's
    /// distance are among a row's target values, the long's first.
    sides: [(usize, usize, Option<usize>); 2],
    /// With exits, the target's instrument and quantity, which make a
    /// distance money.
    worth: Option<(InstrumentAny, Quantity)>,
}

impl Realised {
    /// The reading of the rows of `target`, its stops, with exits, worth
    /// what they are on its instrument as the catalog at `catalog` last
    /// holds it.
    ///
    /// # Errors
    ///
    /// Returns an error if, with exits, the catalog holds no instrument of
    /// the target or an inverse one (see `targets::ensure_linear`).
    pub fn new(catalog: &Path, target: &TargetConfig) -> anyhow::Result<Self> {
        let sides = Side::BOTH.map(|side| {
            (
                target.pnl_index(side),
                target.commission_index(side),
                target.stop_index(side),
            )
        });
        let worth = match target.spec.bracket() {
            None => None,
            Some(_) => {
                let id = target.instrument_id;
                let instrument = ParquetDataCatalog::new(catalog, None, None, None, None)
                    .query_instruments(Some(&[id.to_string()]))
                    .with_context(|| format!("reading the instrument {id}"))?
                    .pop()
                    .with_context(|| format!("the catalog holds no instrument {id}"))?;
                targets::ensure_linear(&instrument)?;
                Some((instrument, target.quantity))
            }
        };
        Ok(Self { sides, worth })
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
    /// stop its decision set with exits, one without.
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

    /// The unit of each side's payoff in `row` (`unit`).
    ///
    /// # Errors
    ///
    /// As `unit`.
    pub fn units(&self, row: &DatasetRow) -> anyhow::Result<PerSide<f64>> {
        Ok(PerSide {
            long: self.unit(row, Side::Long)?,
            short: self.unit(row, Side::Short)?,
        })
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
    use jiff::SignedDuration;
    use nautilus_model::instruments::{Instrument, stubs::btcusd_bybit};
    use rust_decimal::Decimal;

    use super::*;
    use crate::model::targets::{Distance, Target};

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

    /// Bybit's inverse BTCUSD, settled in BTC, in a catalog, and the target
    /// `spec` on it.
    fn inverse(spec: Target) -> (tempfile::TempDir, TargetConfig) {
        let instrument = InstrumentAny::CryptoPerpetual(btcusd_bybit());
        let dir = tempfile::tempdir().unwrap();
        ParquetDataCatalog::new(dir.path(), None, None, None, None)
            .write_instruments(vec![instrument.clone()])
            .unwrap();
        let target = TargetConfig::builder()
            .instrument_id(instrument.id())
            .quantity(Quantity::from(1))
            .spec(spec)
            .build();
        (dir, target)
    }

    /// Nautilus' notional value of an inverse instrument divides by the
    /// price, so a stop's distance has no worth in R on it.
    #[test]
    fn a_target_with_exits_on_an_inverse_instrument_is_refused() {
        let (catalog, target) = inverse(
            Target::builder()
                .take_profit(Distance::BasisPoints(Decimal::from(20)))
                .stop(Distance::BasisPoints(Decimal::from(10)))
                .limit(SignedDuration::from_mins(30))
                .build(),
        );

        let err = Realised::new(catalog.path(), &target).unwrap_err();

        assert!(format!("{err:#}").contains("inverse"), "{err:#}");
    }

    /// In money, a time-only target has no stop to weigh, and so no need of
    /// `targets::ensure_linear`: it works on an inverse instrument.
    #[test]
    fn a_time_only_target_on_an_inverse_instrument_reads_its_trades_in_money() {
        let (catalog, target) = inverse(
            Target::builder()
                .limit(SignedDuration::from_mins(30))
                .build(),
        );

        let realised = Realised::new(catalog.path(), &target).unwrap();

        let row = DatasetRow::new(
            vec![],
            vec![2.0, 0.1, 0.0, -2.0, 0.1, 0.0],
            0.into(),
            0.into(),
            0.into(),
        );
        assert_eq!(realised.unit(&row, Side::Long).unwrap(), 1.0);
        assert!(realised.gained(&row, Side::Long));
        assert!(!realised.gained(&row, Side::Short));
    }
}
