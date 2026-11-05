//! Calibration: a model's probabilities turned into win rates, so that an
//! expected P&L can be computed from them.
//!
//! A model is calibrated by Platt scaling on the log-odds of its prediction:
//! a logistic curve, `1 / (1 + exp(a x + b))`, `x` being `ln(p / (1 - p))`,
//! fitted by maximum likelihood to the labels of rows it was not fitted on.
//! So `a = -1, b = 0` leaves a prediction as it is, a slope between -1 and 0
//! says the model was too sure of itself, and none says it shows no signal,
//! `b` alone then giving the rate. Each label is taken as Platt's
//! smoothed target, `(N+ + 1) / (N+ + 2)` for a true one and `1 / (N- + 2)`
//! for a false one, so that a few rows of one label do not make a
//! certainty. The fit is Newton's method with a backtracking line search,
//! as Lin, Lin and Weng's note on Platt's probabilistic outputs (2007)
//! gives it: deterministic, so a calibration refitted on the same rows is
//! the same to the bit. Nautilus has no calibration.

use anyhow::ensure;
use serde::{Deserialize, Serialize};

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
pub struct Calibration {
    pub a: f64,
    pub b: f64,
}

impl Calibration {
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
        let calibration = Calibration::fit(&[(0.25, false), (0.75, true)]).unwrap();

        close(calibration.apply(0.25), 1.0 / 3.0);
        close(calibration.apply(0.75), 2.0 / 3.0);
        close(calibration.apply(0.5), 0.5);
    }

    /// Every row predicted 0.9, three true and one false: the targets are
    /// 4/5 and 1/3, whose mean, 41/60, the calibrated prediction is.
    #[test]
    fn a_constant_prediction_calibrates_to_the_mean_of_the_targets() {
        let rows = [(0.9, true), (0.9, true), (0.9, true), (0.9, false)];

        close(Calibration::fit(&rows).unwrap().apply(0.9), 41.0 / 60.0);
    }

    #[test]
    fn a_slope_of_minus_one_and_no_intercept_leaves_a_prediction_as_it_is() {
        let unchanged = Calibration { a: -1.0, b: 0.0 };

        for p in [0.05, 0.5, 0.6, 0.8, 0.95] {
            close(unchanged.apply(p), p);
        }
    }

    /// Half the slope takes the root of the odds: 4 to 1 become 2 to 1.
    #[test]
    fn half_the_slope_takes_the_root_of_the_odds() {
        close(Calibration { a: -0.5, b: 0.0 }.apply(0.8), 2.0 / 3.0);
    }

    /// With no slope, `b` alone is the odds against: 3 to 1 is 1/4.
    #[test]
    fn no_slope_gives_every_prediction_the_same_rate() {
        let same = Calibration {
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

        let fit = Calibration::fit(&rows).unwrap();

        assert!((fit.a + 1.0).abs() < 1e-3 && fit.b.abs() < 1e-3, "{fit:?}");
    }

    /// Half true at either prediction: the predictions say nothing.
    #[test]
    fn a_model_with_no_signal_fits_no_slope() {
        let rows: Vec<_> = at(0.8, 10_000, 0.5).chain(at(0.2, 10_000, 0.5)).collect();

        let fit = Calibration::fit(&rows).unwrap();

        assert!(fit.a.abs() < 1e-3 && fit.b.abs() < 1e-3, "{fit:?}");
    }

    /// A learner can predict a certainty, the baseline on labels of one
    /// kind, whose log-odds are infinite.
    #[test]
    fn a_prediction_of_a_certainty_calibrates_to_a_finite_rate() {
        let unchanged = Calibration { a: -1.0, b: 0.0 };
        close(unchanged.apply(0.0), 0.0);
        close(unchanged.apply(1.0), 1.0);

        for rows in [
            vec![(0.0, false), (1.0, true), (0.0, true), (1.0, false)],
            vec![(1.0, true), (1.0, true), (1.0, false)],
            vec![(0.0, false), (0.0, false), (0.0, true)],
        ] {
            let fit = Calibration::fit(&rows).unwrap();

            assert!(fit.a.is_finite() && fit.b.is_finite(), "{fit:?}");
            for p in [0.0, 0.5, 1.0] {
                assert!((0.0..=1.0).contains(&fit.apply(p)), "{fit:?}");
            }
        }
    }

    #[test]
    fn a_ranking_the_wrong_way_round_calibrates_decreasing() {
        let rows = [(0.2, true), (0.3, true), (0.7, false), (0.8, false)];

        let calibration = Calibration::fit(&rows).unwrap();

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

        assert_eq!(
            Calibration::fit(&rows).unwrap(),
            Calibration::fit(&rows).unwrap()
        );
    }

    #[test]
    fn no_row_is_refused() {
        assert!(Calibration::fit(&[]).is_err());
    }
}
