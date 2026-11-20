//! The side picked: decisions on expected net R, each decision weighing the costs of its own (Elkan
//! 2001; Bahnsen et al. 2015).
//!
//! A side's expected net R at a decision is its calibrated expected R (`calibration::Expected`),
//! the realized value its model learned: its net P&L (fees and slippage included) over the unit of
//! its row, its trade's R at the reference stake. The unit is the worth of the trade's own stop
//! with exits, or, on a time-only target, which has no stop, the worth of a stop as far as its ATR
//! at the decision (`targets::SideTarget::unit`). So the costs, in R, scale with each decision's
//! unit, and a fixed cost weighs more on a small one.
//!
//! An expected R is an `R`, finite (`deciding::DecidingModels::pick_among`). A side trades if its
//! expected R is at or above its margin, in R (`ModelConfig::margins`,
//! `deciding::DecidingModels::pick_among`); of two, the larger, none at a tie (`pick`).

use crate::strategy::risk::R;

/// The side traded of `sides`, each a side (or what the caller has of it, its expectation say), its
/// expected R and its margin: of those at or above their margin, the one expected the most; none if
/// no side clears its margin, or two tie.
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

    /// Of the sides at or above their margins, the one expected the most trades; none if no side
    /// clears its margin, or two tie.
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
