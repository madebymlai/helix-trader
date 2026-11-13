//! The side picked: decisions on expected net R, each decision weighing
//! the costs of its own (Elkan 2001; Bahnsen et al. 2015).
//!
//! A side's expected net R at a decision is its calibrated expected R
//! (`calibration::Expected`), the realised value its model learned, its net
//! P&L (fees and slippage included) over the unit of its row: with exits its
//! own stop's worth, on a time-only target, which has no stop, the worth of a
//! stop as far as its ATR at the decision (`targets::SideTarget::unit`); so
//! the costs, in R, scale with each decision's unit, and a fixed cost weighs
//! more against a tight ATR stop. A side trades if its expected R is at or
//! above its margin, in R (`ModelConfig::margins`,
//! `deciding::DecidingModels::pick_among`); of two, the larger, none at a tie
//! (`pick`).

use crate::strategy::r::R;

/// The side traded of `sides`, each a side (or what the caller has of it,
/// its expectation say), its expected R and its margin: of those at
/// or above their margin, the one expected the most; none if no side
/// clears its margin, or two tie.
#[must_use]
pub fn pick<T>(sides: impl IntoIterator<Item = (T, f64, R)>) -> Option<T> {
    let cleared: Vec<(T, f64)> = sides
        .into_iter()
        .filter(|&(_, expected, margin)| margin.reached_by(expected))
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

    #[test]
    fn a_side_trades_from_its_margin() {
        assert_eq!(pick([(Side::Short, 1.0, R::new(1.0))]), Some(Side::Short));
        assert_eq!(pick([(Side::Short, 0.99, R::new(1.0))]), None);
        assert_eq!(pick([(Side::Long, f64::NAN, R::new(1.0))]), None);
    }

    #[test]
    fn of_two_sides_clearing_their_margins_the_larger_trades() {
        assert_eq!(
            pick([
                (Side::Long, 1.5, R::new(1.0)),
                (Side::Short, 2.0, R::new(1.0))
            ]),
            Some(Side::Short)
        );
        assert_eq!(
            pick([
                (Side::Long, 1.5, R::new(1.0)),
                (Side::Short, 2.0, R::new(2.5))
            ]),
            Some(Side::Long)
        );
    }

    #[test]
    fn two_sides_tied_trade_neither() {
        assert_eq!(
            pick([
                (Side::Long, 2.0, R::new(1.0)),
                (Side::Short, 2.0, R::new(1.0))
            ]),
            None
        );
    }

    #[test]
    fn no_side_trades_nothing() {
        assert_eq!(pick::<Side>([]), None);
    }
}
