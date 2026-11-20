//! Picks the side to trade from expected net R, so each decision weighs its own costs (Elkan 2001;
//! Bahnsen et al. 2015).
//!
//! A side's expected net R is its calibrated expected R (`calibration::Expected`). That is the
//! side's net P&L (fees and slippage included) over the unit of its row, which is the trade's R at
//! the reference stake. With exits, the unit is the worth of the trade's own stop. On a time-only
//! target there is no stop, so the unit is the worth of a stop as far away as the ATR at the
//! decision (`targets::SideTarget::unit`). The costs in R therefore scale with each decision's
//! unit, and a fixed cost weighs more on a small unit.
//!
//! An expected R is a finite `R` (`deciding::DecidingModels::pick_among`). The rules for picking
//! (`pick`):
//! - A side trades if its expected R is at or above its margin (`ModelConfig::margins`,
//!   `deciding::DecidingModels::pick_among`).
//! - Of two sides that trade, the one with the larger expected R is picked.
//! - At a tie, no side is picked.

use crate::strategy::risk::R;

/// Returns the side of `sides` that trades.
///
/// Each side is a tag with its expected R and margin. Of the sides at or above their margin, the
/// one with the highest expected R is picked. None is picked if no side clears or two tie.
#[must_use]
pub fn pick<T>(sides: impl IntoIterator<Item = (T, R, R)>) -> Option<T> {
    let cleared: Vec<(T, R)> = sides
        .into_iter()
        .filter(|&(_, expected, margin)| expected >= margin)
        .map(|(side, expected, _)| (side, expected))
        .collect();
    let best = cleared
        .iter()
        .map(|&(_, expected)| expected)
        .reduce(|best, expected| if expected > best { expected } else { best });
    let mut best = cleared
        .into_iter()
        .filter(|&(_, expected)| Some(expected) == best);
    match (best.next(), best.next()) {
        (Some((side, _)), None) => Some(side),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::model::targets::Side;

    #[rstest]
    #[case::a_side_at_its_margin(vec![(Side::Short, R::new(1.0), R::new(1.0))], Some(Side::Short))]
    #[case::a_side_below_its_margin(vec![(Side::Short, R::new(0.99), R::new(1.0))], None)]
    #[case::of_two_clearing_the_larger(
        vec![
            (Side::Long, R::new(1.5), R::new(1.0)),
            (Side::Short, R::new(2.0), R::new(1.0)),
        ],
        Some(Side::Short)
    )]
    #[case::of_two_the_larger_below_its_margin(
        vec![
            (Side::Long, R::new(1.5), R::new(1.0)),
            (Side::Short, R::new(2.0), R::new(2.5)),
        ],
        Some(Side::Long)
    )]
    #[case::two_tied(
        vec![
            (Side::Long, R::new(2.0), R::new(1.0)),
            (Side::Short, R::new(2.0), R::new(1.0)),
        ],
        None
    )]
    #[case::no_side(vec![], None)]
    fn of_the_sides_clearing_their_margins_the_larger_trades(
        #[case] sides: Vec<(Side, R, R)>,
        #[case] traded: Option<Side>,
    ) {
        assert_eq!(pick(sides), traded);
    }
}
