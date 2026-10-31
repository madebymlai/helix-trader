//! Calibration: a model's probabilities turned into win rates, so that an
//! expected P&L can be computed from them.
//!
//! A model is calibrated by Platt scaling: a logistic curve of its
//! prediction, `1 / (1 + exp(a p + b))`, fitted by maximum likelihood to the
//! labels of rows it was not fitted on, each label taken as Platt's
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

/// A Platt scaling: the probability of a prediction `p` is
/// `1 / (1 + exp(a p + b))`.
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
            .map(|&(p, label)| (p, if label { high } else { low }))
            .collect();
        let mut fit = Self {
            a: 0.0,
            b: ((falses + 1.0) / (trues + 1.0)).ln(),
        };
        let mut loss = fit.loss(&targets);
        for _ in 0..MAX_ITERATIONS {
            let (mut h11, mut h22, mut h21, mut g1, mut g2) = (SIGMA, SIGMA, 0.0, 0.0, 0.0);
            for &(p, target) in &targets {
                let (probability, complement) = fit.split(p);
                let d2 = probability * complement;
                h11 += p * p * d2;
                h22 += d2;
                h21 += p * d2;
                let d1 = target - probability;
                g1 += p * d1;
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
        self.split(p).0
    }

    /// The probability of `p` and its complement, each computed where it
    /// does not overflow.
    fn split(&self, p: f64) -> (f64, f64) {
        let z = self.a.mul_add(p, self.b);
        if z >= 0.0 {
            let e = (-z).exp();
            (e / (1.0 + e), 1.0 / (1.0 + e))
        } else {
            let e = z.exp();
            (1.0 / (1.0 + e), e / (1.0 + e))
        }
    }

    /// The cross-entropy of the calibration against `targets`, each a
    /// prediction and its smoothed target.
    fn loss(&self, targets: &[(f64, f64)]) -> f64 {
        targets
            .iter()
            .map(|&(p, target)| {
                let z = self.a.mul_add(p, self.b);
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
