//! The side picked: decisions on expected net P&L, each decision weighing
//! the costs of its own (Elkan 2001; Bahnsen et al. 2015).
//!
//! A side's expected net P&L at a decision is its calibrated expected R
//! (`calibration::Expected`), the realised value its model learned, net of
//! the trade's costs, over the unit of its row, times the unit the decision
//! sets (`deciding::DecidingModels::pick_among`): with exits the worth of
//! the stop, so the costs, in R, scale with each decision's stop, and a
//! fixed cost weighs more against a tight ATR stop; on a time-only target
//! one, its R in money. A side trades if that is at or above its margin
//! (`ModelConfig::margins`); of two, the larger, none at a tie (`pick`).

/// The side traded of `sides`, each a side (or what the caller has of it,
/// its expectation say), its expected net P&L and its margin: of those at
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
