//! Calibration of a model's predictions into what a decision weighs.
//!
//! A pair's side models are calibrated together (`Ksplit`) on the calibration period's rows, which
//! no model was fitted on:
//! - The level, the mean of the sides' predictions, by a line.
//! - The side call, the long's prediction less the short's, by a slope through nought.
//!
//! Each slope is clipped to `[0, 1.5]`. A one-sided strategy and a meta model use the level line
//! alone (`Line`). Nautilus has no calibration.

use serde::{Deserialize, Serialize};

use super::targets::Side;

/// No slope below nought: a part ranking the rows the wrong way shows no signal.
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

/// The level `C' = alpha_C + beta_C C` and the side call `D' = beta_D D`, the latter none of one
/// side.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Ksplit {
    /// The level's line.
    pub level: Line,
    /// The side call's slope, `beta_D`; none of one side.
    pub side_call: Option<f64>,
}

/// A decision's calibrated level `C'` and side call `D'`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Expected {
    pub level: f64,
    pub side_call: f64,
}

impl Expected {
    /// The expected R of `side`: `C' + D'/2` for the long, `C' - D'/2` for the short.
    #[must_use]
    pub fn of(&self, side: Side) -> f64 {
        match side {
            Side::Long => self.side_call.mul_add(0.5, self.level),
            Side::Short => self.side_call.mul_add(-0.5, self.level),
        }
    }
}

impl Ksplit {
    /// Fits `rows`, each a row's one or two sides, the long first, as (prediction, realized R).
    /// `beta_C` is the least squares slope of the mean R on `C`, `beta_D` that of the long's R less
    /// the short's on `D`, through nought. A slope is nought where its predictions never vary, to
    /// the bit. `alpha_C` is the mean R less `beta_C` times the mean `C`, after the clip.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no row, or a row has neither one side nor two.
    pub fn fit(rows: &[Vec<(f64, f64)>]) -> anyhow::Result<Self> {
        anyhow::ensure!(!rows.is_empty(), "no row to calibrate the models on");
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

        // The rounded mean of constant levels would give a slope of noise
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

    /// The calibrated expectation of `predictions`, the long's first.
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

fn mean(values: impl ExactSizeIterator<Item = f64>) -> f64 {
    let count = values.len() as f64;
    values.sum::<f64>() / count
}

/// Whether any prediction differs from the first, to the bit.
fn varies(mut predictions: impl Iterator<Item = f64>) -> bool {
    let first = predictions.next();
    predictions.any(|prediction| Some(prediction) != first)
}

/// The slope `numerator / denominator`, clipped to `SLOPES`.
fn slope(numerator: f64, denominator: f64) -> f64 {
    (numerator / denominator).clamp(SLOPES.0, SLOPES.1)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn near(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < 1e-12, "{actual} != {expected}");
    }

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

    /// Means 1 and 0.8, covariance (-1)(-0.5) + (1)(0.7) = 1.2 over variance 2: beta_C = 0.6,
    /// alpha_C = 0.8 - 0.6 = 0.2. beta_D = (0.5 + 0.5 + 2) / (1 + 1 + 4) = 0.5. At predictions 1
    /// and 0, C = 0.5, D = 1: C' = 0.2 + 0.3 = 0.5, D' = 0.5, E' 0.75 and 0.25.
    #[rstest]
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

    /// At predictions 1 and 0, C = 0.5, D = 1:
    ///
    /// - Level slope 2 clips to 1.5, alpha_C 2 - 1.5 x 1 = 0.5. Side call slope -0.5 clips to
    ///   nought. C' = 0.5 + 1.5 x 0.5 = 1.25, D' = 0.
    /// - Level slope -0.5 clips to nought, alpha_C 0.5. Side call (2 + 2 + 8) / 6 = 2 clips to 1.5.
    ///   C' = 0.5, D' = 1.5: E' 0.5 + 0.75 = 1.25 and 0.5 - 0.75 = -0.25.
    #[rstest]
    #[case::a_level_above_and_a_side_call_below(
        [0.0, 2.0, 4.0],
        [-0.5, 0.5, -1.0],
        (0.5, 1.5),
        0.0,
        (1.25, 1.25)
    )]
    #[case::a_level_below_and_a_side_call_above(
        [1.0, 0.5, 0.0],
        [2.0, -2.0, 4.0],
        (0.5, 0.0),
        1.5,
        (1.25, -0.25)
    )]
    fn a_slope_above_one_and_a_half_is_clipped_to_it_and_one_below_nought_to_nought(
        #[case] ys: [f64; 3],
        #[case] ts: [f64; 3],
        #[case] level: (f64, f64),
        #[case] side_call: f64,
        #[case] long_and_short: (f64, f64),
    ) {
        let rows = pair_rows([0.0, 1.0, 2.0], [1.0, -1.0, 2.0], ys, ts);

        let fit = Ksplit::fit(&rows).unwrap();

        near(fit.level.alpha, level.0);
        near(fit.level.beta, level.1);
        near(fit.side_call.unwrap(), side_call);
        let expected = fit.apply(&[1.0, 0.0]);
        near(expected.of(Side::Long), long_and_short.0);
        near(expected.of(Side::Short), long_and_short.1);
    }

    /// At 0.5, E' is the level: 0.2 + 0.6 x 0.5 = 0.5, or, the slope 2 clipped to 1.5, alpha
    /// 2 - 1.5 = 0.5 and E' 0.5 + 0.75 = 1.25.
    #[rstest]
    #[case::fitted([0.3, 0.6, 1.5], (0.2, 0.6), 0.5)]
    #[case::clipped_and_its_alpha_refitted([0.0, 2.0, 4.0], (0.5, 1.5), 1.25)]
    fn one_side_fits_the_level_line_alone_on_its_own_r(
        #[case] rs: [f64; 3],
        #[case] level: (f64, f64),
        #[case] expected_r: f64,
    ) {
        let rows: Vec<Vec<(f64, f64)>> = [0.0, 1.0, 2.0]
            .into_iter()
            .zip(rs)
            .map(|row| vec![row])
            .collect();

        let fit = Ksplit::fit(&rows).unwrap();

        near(fit.level.alpha, level.0);
        near(fit.level.beta, level.1);
        assert_eq!(fit.side_call, None);
        let expected = fit.apply(&[0.5]);
        assert_eq!(expected.side_call, 0.0);
        near(expected.of(Side::Long), expected_r);
        near(expected.of(Side::Short), expected_r);
    }

    /// The mean R is 0.5. Least squares through nought would give the side call 0.2 / 0.02,
    /// clipped to 1.5.
    #[rstest]
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

    /// The mean R is (m + 0.4) / 2, m the mean of `i % 3`: 1 over 24 or 30 rows; (12 + 2 x 12) / 37
    /// over 37; (17 + 2 x 16) / 50 = 0.98 over 50.
    #[rstest]
    #[case::of_24_rows(24, 0.7)]
    #[case::of_30_rows(30, 0.7)]
    #[case::of_37_rows(37, (36.0 / 37.0 + 0.4) / 2.0)]
    #[case::of_50_rows(50, 0.69)]
    fn a_level_that_never_varies_over_many_rows_has_no_slope(
        #[case] count: u8,
        #[case] mean_r: f64,
    ) {
        let rows: Vec<Vec<(f64, f64)>> = (0..count)
            .map(|i| vec![(0.1, f64::from(i % 3) + 0.1), (0.1, 0.3)])
            .collect();

        let fit = Ksplit::fit(&rows).unwrap();

        assert_eq!(fit.level.beta, 0.0);
        near(fit.level.alpha, mean_r);
        assert_eq!(fit.side_call, Some(0.0));
    }

    #[rstest]
    fn no_row_has_no_ksplit() {
        let err = Ksplit::fit(&[]).unwrap_err().to_string();

        assert!(err.contains("no row"), "{err}");
    }
}
