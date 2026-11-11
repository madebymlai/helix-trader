//! Calibration: a model's predictions turned into what a decision weighs.
//!
//! A strategy's side models predict R, and a pair's predictions are
//! calibrated together (`Ksplit`), on the rows of the calibration period of
//! the split, which no model was fitted on: the level, the mean of the
//! sides' predictions, by a line, and the side call, the long's prediction
//! less the short's, by a slope through nought, each slope clipped to
//! `[0, 1.5]`, so that a part with no signal shrinks toward nothing without
//! the other moving. A decision's expected R of each side is its calibrated
//! level plus or less half its calibrated side call (`Expected`). A
//! one-sided strategy's calibration is the level line alone, on its one
//! side's prediction, and so is a meta model's, on its prediction of the R
//! of the trade of the side picked (`Line`). Least squares on so few
//! parameters has a closed form: a calibration refitted on the same rows is
//! the same to the bit. Nautilus has no calibration.

use anyhow::ensure;
use serde::{Deserialize, Serialize};

use super::targets::Side;

/// The range a slope of `Ksplit` is clipped to: no slope below nought, so
/// a part ranking the rows the wrong way round shows no signal, and none
/// above 1.5.
const SLOPES: (f64, f64) = (0.0, 1.5);

/// A line, `alpha + beta x`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Line {
    pub alpha: f64,
    pub beta: f64,
}

impl Line {
    /// The line at `x`.
    #[must_use]
    pub fn apply(&self, x: f64) -> f64 {
        self.beta.mul_add(x, self.alpha)
    }
}

/// The calibration of the predictions of R of a pair's sides, fitted on
/// both together (Ksplit): the level `C`, the mean of the sides'
/// predictions, and the side call `D`, the long's prediction less the
/// short's, each by its own line, `C' = alpha_C + beta_C C` and `D' =
/// beta_D D`, so that the side call is shrunk without the level moving.
/// Of one side, the level line alone, `C` its own prediction.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Ksplit {
    /// The level's line.
    pub level: Line,
    /// The side call's slope, `beta_D`; none of one side.
    pub side_call: Option<f64>,
}

/// A decision's calibrated expectation: its level `C'` and its side call
/// `D'`, nought of one side, of which each side's `E'`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Expected {
    pub level: f64,
    pub side_call: f64,
}

impl Expected {
    /// The expected R of `side`: `C' + D'/2` for the long, `C' - D'/2` for
    /// the short, the level of one side.
    #[must_use]
    pub fn of(&self, side: Side) -> f64 {
        match side {
            Side::Long => self.side_call.mul_add(0.5, self.level),
            Side::Short => self.side_call.mul_add(-0.5, self.level),
        }
    }
}

impl Ksplit {
    /// The calibration fitted on `rows`, each a row's sides, the long
    /// first, one or two, each its model's prediction and its trade's
    /// realised R. `beta_C` is the least squares slope of the sides' mean
    /// R on `C`, `beta_D` that of the long's R less the short's on `D`,
    /// through nought, each clipped to `[0, 1.5]`, and nought where its
    /// part's predictions never vary, the same to the bit on every row;
    /// `alpha_C` is the mean R less `beta_C` times the mean `C`, after the
    /// clip. A side call that never varies so leaves the sides tied.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no row, or a row has neither one side
    /// nor two.
    pub fn fit(rows: &[Vec<(f64, f64)>]) -> anyhow::Result<Self> {
        ensure!(!rows.is_empty(), "no row to calibrate the models on");
        let pair = match rows[0].len() {
            1 => false,
            2 => true,
            sides => anyhow::bail!("a row of {sides} sides to calibrate"),
        };
        let levels: Vec<(f64, f64)> = rows
            .iter()
            .map(|sides| {
                (
                    mean(sides.iter().map(|side| side.0)),
                    mean(sides.iter().map(|side| side.1)),
                )
            })
            .collect();
        let (c, y) = (
            mean(levels.iter().map(|level| level.0)),
            mean(levels.iter().map(|level| level.1)),
        );
        let (covariance, variance) = levels.iter().fold((0.0, 0.0), |(cov, var), &(x, r)| {
            ((x - c).mul_add(r - y, cov), (x - c).mul_add(x - c, var))
        });
        // Their mean rounded, levels that never vary would have a variance
        // of rounding, and a slope of noise.
        let beta = if varies(levels.iter().map(|level| level.0)) {
            slope(covariance, variance)
        } else {
            0.0
        };
        let side_call = pair.then(|| {
            let calls: Vec<(f64, f64)> = rows
                .iter()
                .map(|sides| (sides[0].0 - sides[1].0, sides[0].1 - sides[1].1))
                .collect();
            // A side call that never varies would fit a slope of the mean
            // R long less R short over it, the calibration's means alone.
            if varies(calls.iter().map(|call| call.0)) {
                let (product, square) =
                    calls.iter().fold((0.0, 0.0), |(product, square), &(d, t)| {
                        (d.mul_add(t, product), d.mul_add(d, square))
                    });
                slope(product, square)
            } else {
                0.0
            }
        });
        Ok(Self {
            level: Line {
                alpha: beta.mul_add(-c, y),
                beta,
            },
            side_call,
        })
    }

    /// The calibrated expectation of `predictions`, each side's of the
    /// pair, the long first, one or two as fitted.
    #[must_use]
    pub fn apply(&self, predictions: &[f64]) -> Expected {
        let level = self.level.apply(mean(predictions.iter().copied()));
        let side_call = match (self.side_call, predictions) {
            (Some(beta), &[long, short]) => beta * (long - short),
            _ => 0.0,
        };
        Expected { level, side_call }
    }
}

/// The mean of `values`.
fn mean(values: impl ExactSizeIterator<Item = f64>) -> f64 {
    let count = values.len() as f64;
    values.sum::<f64>() / count
}

/// Whether `predictions` vary: any is not the first, to the bit.
fn varies(mut predictions: impl Iterator<Item = f64>) -> bool {
    let first = predictions.next();
    predictions.any(|prediction| Some(prediction) != first)
}

/// The slope `numerator / denominator` of predictions that vary, the
/// denominator their spread, clipped to `SLOPES`.
fn slope(numerator: f64, denominator: f64) -> f64 {
    (numerator / denominator).clamp(SLOPES.0, SLOPES.1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn near(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < 1e-12, "{actual} != {expected}");
    }

    /// The rows of a pair whose level `C` and side `D`, predicted, are
    /// `cs` and `ds`, and whose mean R and R long less R short, realised,
    /// are `ys` and `ts`: each row the long's prediction and R, `C + D/2`
    /// and `Y + T/2`, then the short's, `C - D/2` and `Y - T/2`.
    fn pair_rows(cs: [f64; 3], ds: [f64; 3], ys: [f64; 3], ts: [f64; 3]) -> Vec<Vec<(f64, f64)>> {
        (0..3)
            .map(|i| {
                vec![
                    (cs[i] + ds[i] / 2.0, ys[i] + ts[i] / 2.0),
                    (cs[i] - ds[i] / 2.0, ys[i] - ts[i] / 2.0),
                ]
            })
            .collect()
    }

    /// The level, C of 0, 1 and 2, against the sides' mean R of 0.3, 0.6
    /// and 1.5: means 1 and 0.8, a covariance of (-1)(-0.5) + (1)(0.7) =
    /// 1.2 over a variance of 2, so beta_C = 0.6 and alpha_C = 0.8 - 0.6 =
    /// 0.2. The side call, D of 1, -1 and 2, against R long less R short of
    /// 0.5, -0.5 and 1: (0.5 + 0.5 + 2) / (1 + 1 + 4), beta_D = 0.5. At a
    /// long's prediction of 1 and a short's of 0, C = 0.5 and D = 1: C' =
    /// 0.2 + 0.3 = 0.5, D' = 0.5, the long's E' 0.75 and the short's 0.25.
    #[test]
    fn a_pairs_level_and_side_call_are_fitted_by_least_squares() {
        let rows = pair_rows(
            [0.0, 1.0, 2.0],
            [1.0, -1.0, 2.0],
            [0.3, 0.6, 1.5],
            [0.5, -0.5, 1.0],
        );

        let fit = Ksplit::fit(&rows).unwrap();

        near(fit.level.alpha, 0.2);
        near(fit.level.beta, 0.6);
        near(fit.side_call.unwrap(), 0.5);
        let expected = fit.apply(&[1.0, 0.0]);
        near(expected.level, 0.5);
        near(expected.side_call, 0.5);
        near(expected.of(Side::Long), 0.75);
        near(expected.of(Side::Short), 0.25);
    }

    /// The level's slope, 2 on C of 0, 1 and 2 against a mean R of 0, 2 and
    /// 4, is clipped to 1.5, and alpha_C refitted after the clip: 2 - 1.5 x 1
    /// = 0.5, not the unclipped 0. The side call's, -0.5 on D of 1, -1 and 2
    /// against R long less R short of -0.5, 0.5 and -1, is clipped to
    /// nought. At predictions of 1 and 0, C' = 0.5 + 1.5 x 0.5 = 1.25 and D'
    /// = 0: each side's E' is the level.
    #[test]
    fn a_slope_above_one_and_a_half_is_clipped_to_it_and_one_below_nought_to_nought() {
        let rows = pair_rows(
            [0.0, 1.0, 2.0],
            [1.0, -1.0, 2.0],
            [0.0, 2.0, 4.0],
            [-0.5, 0.5, -1.0],
        );

        let fit = Ksplit::fit(&rows).unwrap();

        near(fit.level.beta, 1.5);
        near(fit.level.alpha, 0.5);
        near(fit.side_call.unwrap(), 0.0);
        let expected = fit.apply(&[1.0, 0.0]);
        near(expected.of(Side::Long), 1.25);
        near(expected.of(Side::Short), 1.25);
    }

    /// The level's slope, -0.5 on C of 0, 1 and 2 against a mean R of 1,
    /// 0.5 and 0, is clipped to nought, alpha_C then the mean R, 0.5; the
    /// side call's, 2 on D of 1, -1 and 2 against 2, -2 and 4, to 1.5.
    #[test]
    fn a_level_slope_below_nought_leaves_the_mean_and_a_side_call_above_one_and_a_half_is_clipped()
    {
        let rows = pair_rows(
            [0.0, 1.0, 2.0],
            [1.0, -1.0, 2.0],
            [1.0, 0.5, 0.0],
            [2.0, -2.0, 4.0],
        );

        let fit = Ksplit::fit(&rows).unwrap();

        near(fit.level.beta, 0.0);
        near(fit.level.alpha, 0.5);
        near(fit.side_call.unwrap(), 1.5);
    }

    /// One side: the level line alone, on its own prediction and R, 0, 1
    /// and 2 against 0.3, 0.6 and 1.5: alpha 0.2 and beta 0.6, no side
    /// call. At a prediction of 0.5 its E' is the level, 0.5, whichever the
    /// side.
    #[test]
    fn one_side_fits_the_level_line_alone_on_its_own_r() {
        let rows: Vec<Vec<(f64, f64)>> = [(0.0, 0.3), (1.0, 0.6), (2.0, 1.5)]
            .into_iter()
            .map(|row| vec![row])
            .collect();

        let fit = Ksplit::fit(&rows).unwrap();

        near(fit.level.alpha, 0.2);
        near(fit.level.beta, 0.6);
        assert_eq!(fit.side_call, None);
        let expected = fit.apply(&[0.5]);
        assert_eq!(expected.side_call, 0.0);
        near(expected.of(Side::Long), 0.5);
        near(expected.of(Side::Short), 0.5);
    }

    /// One side's slope is clipped too: 2 on 0, 1 and 2 against 0, 2 and 4
    /// becomes 1.5, its alpha 2 - 1.5 = 0.5.
    #[test]
    fn one_sides_slope_is_clipped_and_its_alpha_refitted() {
        let rows: Vec<Vec<(f64, f64)>> = [(0.0, 0.0), (1.0, 2.0), (2.0, 4.0)]
            .into_iter()
            .map(|row| vec![row])
            .collect();

        let fit = Ksplit::fit(&rows).unwrap();

        near(fit.level.beta, 1.5);
        near(fit.level.alpha, 0.5);
    }

    /// Predictions that never vary show no signal, though the long's R
    /// exceeds the short's by 1 on every row, as the long's prediction
    /// exceeds the short's: no slope, the level the mean R, 0.5, and no
    /// side call, each side's E' the level. Least squares through nought
    /// would give the side call 0.2 / 0.02, clipped to 1.5.
    #[test]
    fn predictions_that_never_vary_fit_no_slope() {
        let rows: Vec<Vec<(f64, f64)>> = [(0.5, -0.5), (1.5, 0.5)]
            .into_iter()
            .map(|(long, short)| vec![(0.2, long), (0.1, short)])
            .collect();

        let fit = Ksplit::fit(&rows).unwrap();

        assert_eq!((fit.level.beta, fit.side_call), (0.0, Some(0.0)));
        near(fit.level.alpha, 0.5);
        let expected = fit.apply(&[0.2, 0.1]);
        near(expected.of(Side::Long), 0.5);
        near(expected.of(Side::Short), 0.5);
    }

    /// Many rows of one level, 0.1 a side, whose mean in floating point is
    /// not 0.1 to the bit: still no slope, the level the mean R.
    #[test]
    fn a_level_that_never_varies_over_many_rows_has_no_slope() {
        for count in [24, 30, 37, 50] {
            let rows: Vec<Vec<(f64, f64)>> = (0..count)
                .map(|i| vec![(0.1, f64::from(i % 3) + 0.1), (0.1, 0.3)])
                .collect();

            let fit = Ksplit::fit(&rows).unwrap();

            assert_eq!(fit.level.beta, 0.0, "{count} rows");
        }
    }

    #[test]
    fn no_row_has_no_ksplit() {
        assert!(Ksplit::fit(&[]).is_err());
    }
}
