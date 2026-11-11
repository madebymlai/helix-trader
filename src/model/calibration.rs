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
//! side's prediction. Least squares on so few parameters has a closed form:
//! a calibration refitted on the same rows is the same to the bit.
//!
//! A meta model's probabilities are calibrated by Platt scaling on their
//! log-odds (`Platt`): a logistic curve, `1 / (1 + exp(a x + b))`, `x`
//! being `ln(p / (1 - p))`, fitted by maximum likelihood to the labels of
//! rows it was not fitted on. So `a = -1, b = 0` leaves a prediction as it
//! is, a slope between -1 and 0 says the model was too sure of itself, and
//! none says it shows no signal, `b` alone then giving the rate. Each label
//! is taken as Platt's smoothed target, `(N+ + 1) / (N+ + 2)` for a true one
//! and `1 / (N- + 2)` for a false one, so that a few rows of one label do
//! not make a certainty. The fit is Newton's method with a backtracking line
//! search, as Lin, Lin and Weng's note on Platt's probabilistic outputs
//! (2007) gives it: deterministic, so a calibration refitted on the same
//! rows is the same to the bit. Nautilus has no calibration.

use anyhow::ensure;
use serde::{Deserialize, Serialize};

use super::targets::Side;

/// The most Newton steps of a fit.
const MAX_ITERATIONS: usize = 100;

/// The smallest step of the line search before it gives up.
const MIN_STEP: f64 = 1e-10;

/// The added to the Hessian's diagonal, which keeps it invertible.
const SIGMA: f64 = 1e-12;

/// The gradient under which a fit has converged.
const EPSILON: f64 = 1e-5;

/// How near a certainty a prediction's log-odds are read, those of 0 and 1
/// being infinite: a learner can predict one, the baseline on labels of one
/// kind.
const NEAREST_CERTAINTY: f64 = 1e-12;

/// The log-odds of the prediction `p`.
fn log_odds(p: f64) -> f64 {
    let p = p.clamp(NEAREST_CERTAINTY, 1.0 - NEAREST_CERTAINTY);
    (p / (1.0 - p)).ln()
}

/// A Platt scaling on log-odds: the probability of a prediction whose
/// log-odds are `x` is `1 / (1 + exp(a x + b))`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Platt {
    pub a: f64,
    pub b: f64,
}

impl Platt {
    /// The calibration fitted on `rows`, each a model's prediction and its
    /// label.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no row.
    pub fn fit(rows: &[(f64, bool)]) -> anyhow::Result<Self> {
        ensure!(!rows.is_empty(), "no row to calibrate the model on");
        let trues = rows.iter().filter(|row| row.1).count() as f64;
        let falses = rows.len() as f64 - trues;
        let (high, low) = ((trues + 1.0) / (trues + 2.0), 1.0 / (falses + 2.0));
        let targets: Vec<(f64, f64)> = rows
            .iter()
            .map(|&(p, label)| (log_odds(p), if label { high } else { low }))
            .collect();
        let mut fit = Self {
            a: 0.0,
            b: ((falses + 1.0) / (trues + 1.0)).ln(),
        };
        let mut loss = fit.loss(&targets);
        for _ in 0..MAX_ITERATIONS {
            let (mut h11, mut h22, mut h21, mut g1, mut g2) = (SIGMA, SIGMA, 0.0, 0.0, 0.0);
            for &(x, target) in &targets {
                let (probability, complement) = fit.split(x);
                let d2 = probability * complement;
                h11 += x * x * d2;
                h22 += d2;
                h21 += x * d2;
                let d1 = target - probability;
                g1 += x * d1;
                g2 += d1;
            }
            if g1.abs() < EPSILON && g2.abs() < EPSILON {
                break;
            }
            let det = h11 * h22 - h21 * h21;
            let da = -(h22 * g1 - h21 * g2) / det;
            let db = -(-h21 * g1 + h11 * g2) / det;
            let descent = g1 * da + g2 * db;
            let mut step = 1.0;
            while step >= MIN_STEP {
                let next = Self {
                    a: fit.a + step * da,
                    b: fit.b + step * db,
                };
                let next_loss = next.loss(&targets);
                if next_loss < loss + 0.0001 * step * descent {
                    (fit, loss) = (next, next_loss);
                    break;
                }
                step /= 2.0;
            }
            if step < MIN_STEP {
                break;
            }
        }
        Ok(fit)
    }

    /// The calibrated probability of the prediction `p`.
    #[must_use]
    pub fn apply(&self, p: f64) -> f64 {
        self.split(log_odds(p)).0
    }

    /// The probability at the log-odds `x` and its complement, each computed
    /// where it does not overflow.
    fn split(&self, x: f64) -> (f64, f64) {
        let z = self.a.mul_add(x, self.b);
        if z >= 0.0 {
            let e = (-z).exp();
            (e / (1.0 + e), 1.0 / (1.0 + e))
        } else {
            let e = z.exp();
            (1.0 / (1.0 + e), e / (1.0 + e))
        }
    }

    /// The cross-entropy of the calibration against `targets`, each a
    /// prediction's log-odds and its smoothed target.
    fn loss(&self, targets: &[(f64, f64)]) -> f64 {
        targets
            .iter()
            .map(|&(x, target)| {
                let z = self.a.mul_add(x, self.b);
                if z >= 0.0 {
                    target.mul_add(z, (-z).exp().ln_1p())
                } else {
                    (target - 1.0).mul_add(z, z.exp().ln_1p())
                }
            })
            .sum()
    }
}

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
    /// through nought; each is clipped to `[0, 1.5]`, a part whose
    /// predictions never vary having none, and `alpha_C` is the mean R less
    /// `beta_C` times the mean `C`, after the clip.
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
        let varies = levels.iter().any(|level| level.0 != levels[0].0);
        let beta = if varies {
            slope(covariance, variance)
        } else {
            0.0
        };
        let side_call = pair.then(|| {
            let (product, square) = rows.iter().fold((0.0, 0.0), |(product, square), sides| {
                let (d, t) = (sides[0].0 - sides[1].0, sides[0].1 - sides[1].1);
                (d.mul_add(t, product), d.mul_add(d, square))
            });
            slope(product, square)
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
        let level = self
            .level
            .beta
            .mul_add(mean(predictions.iter().copied()), self.level.alpha);
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

/// The slope `numerator / denominator` clipped to `SLOPES`: nought where
/// the predictions never vary, the denominator nought.
fn slope(numerator: f64, denominator: f64) -> f64 {
    if denominator == 0.0 {
        return 0.0;
    }
    (numerator / denominator).clamp(SLOPES.0, SLOPES.1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < 1e-4, "{actual} != {expected}");
    }

    /// One false row at 0.25, one true at 0.75: Platt's targets 1/3 and
    /// 2/3, which two parameters fit exactly, and 0.5 half way.
    #[test]
    fn a_false_and_a_true_row_calibrate_to_platts_targets() {
        let calibration = Platt::fit(&[(0.25, false), (0.75, true)]).unwrap();

        close(calibration.apply(0.25), 1.0 / 3.0);
        close(calibration.apply(0.75), 2.0 / 3.0);
        close(calibration.apply(0.5), 0.5);
    }

    /// Every row predicted 0.9, three true and one false: the targets are
    /// 4/5 and 1/3, whose mean, 41/60, the calibrated prediction is.
    #[test]
    fn a_constant_prediction_calibrates_to_the_mean_of_the_targets() {
        let rows = [(0.9, true), (0.9, true), (0.9, true), (0.9, false)];

        close(Platt::fit(&rows).unwrap().apply(0.9), 41.0 / 60.0);
    }

    #[test]
    fn a_slope_of_minus_one_and_no_intercept_leaves_a_prediction_as_it_is() {
        let unchanged = Platt { a: -1.0, b: 0.0 };

        for p in [0.05, 0.5, 0.6, 0.8, 0.95] {
            close(unchanged.apply(p), p);
        }
    }

    /// Half the slope takes the root of the odds: 4 to 1 become 2 to 1.
    #[test]
    fn half_the_slope_takes_the_root_of_the_odds() {
        close(Platt { a: -0.5, b: 0.0 }.apply(0.8), 2.0 / 3.0);
    }

    /// With no slope, `b` alone is the odds against: 3 to 1 is 1/4.
    #[test]
    fn no_slope_gives_every_prediction_the_same_rate() {
        let same = Platt {
            a: 0.0,
            b: 3f64.ln(),
        };

        for p in [0.1, 0.5, 0.9] {
            close(same.apply(p), 0.25);
        }
    }

    /// `rows` rows predicted `p`, a share `trues` of them true.
    fn at(p: f64, rows: usize, trues: f64) -> impl Iterator<Item = (f64, bool)> {
        (0..rows).map(move |row| (p, (row as f64) < trues * rows as f64))
    }

    /// 80% true where 0.8 is predicted and 20% where 0.2 is: the fit is the
    /// curve leaving them as they are, up to Platt's smoothing.
    #[test]
    fn a_model_already_calibrated_fits_the_curve_leaving_it_as_it_is() {
        let rows: Vec<_> = at(0.8, 10_000, 0.8).chain(at(0.2, 10_000, 0.2)).collect();

        let fit = Platt::fit(&rows).unwrap();

        assert!((fit.a + 1.0).abs() < 1e-3 && fit.b.abs() < 1e-3, "{fit:?}");
    }

    /// Half true at either prediction: the predictions say nothing.
    #[test]
    fn a_model_with_no_signal_fits_no_slope() {
        let rows: Vec<_> = at(0.8, 10_000, 0.5).chain(at(0.2, 10_000, 0.5)).collect();

        let fit = Platt::fit(&rows).unwrap();

        assert!(fit.a.abs() < 1e-3 && fit.b.abs() < 1e-3, "{fit:?}");
    }

    /// A learner can predict a certainty, the baseline on labels of one
    /// kind, whose log-odds are infinite.
    #[test]
    fn a_prediction_of_a_certainty_calibrates_to_a_finite_rate() {
        let unchanged = Platt { a: -1.0, b: 0.0 };
        close(unchanged.apply(0.0), 0.0);
        close(unchanged.apply(1.0), 1.0);

        for rows in [
            vec![(0.0, false), (1.0, true), (0.0, true), (1.0, false)],
            vec![(1.0, true), (1.0, true), (1.0, false)],
            vec![(0.0, false), (0.0, false), (0.0, true)],
        ] {
            let fit = Platt::fit(&rows).unwrap();

            assert!(fit.a.is_finite() && fit.b.is_finite(), "{fit:?}");
            for p in [0.0, 0.5, 1.0] {
                assert!((0.0..=1.0).contains(&fit.apply(p)), "{fit:?}");
            }
        }
    }

    #[test]
    fn a_ranking_the_wrong_way_round_calibrates_decreasing() {
        let rows = [(0.2, true), (0.3, true), (0.7, false), (0.8, false)];

        let calibration = Platt::fit(&rows).unwrap();

        assert!(
            calibration.apply(0.2) > calibration.apply(0.8),
            "{calibration:?}"
        );
    }

    #[test]
    fn a_refit_on_the_same_rows_is_the_same_to_the_bit() {
        let rows = [
            (0.1, false),
            (0.4, true),
            (0.35, false),
            (0.8, true),
            (0.6, true),
        ];

        assert_eq!(Platt::fit(&rows).unwrap(), Platt::fit(&rows).unwrap());
    }

    #[test]
    fn no_row_is_refused() {
        assert!(Platt::fit(&[]).is_err());
    }

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

    /// Predictions that never vary show no signal: no slope, the level the
    /// mean R, 0.5, and no side call.
    #[test]
    fn predictions_that_never_vary_fit_no_slope() {
        let rows: Vec<Vec<(f64, f64)>> = [(0.0, 1.0), (1.0, 0.0)]
            .into_iter()
            .map(|(long, short)| vec![(0.2, long), (0.1, short)])
            .collect();

        let fit = Ksplit::fit(&rows).unwrap();

        assert_eq!((fit.level.beta, fit.side_call), (0.0, Some(0.0)));
        near(fit.level.alpha, 0.5);
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
