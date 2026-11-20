//! How a model's expected values meet the values realized on its rows.

use serde::{Deserialize, Serialize};

/// How a model's expected values meet the realized values, beside a baseline's.
///
/// It is empty for no rows.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct ExpectedScore {
    pub rows: usize,
    /// One less the squared error over the baseline's. None where the baseline's is zero.
    pub skill: Option<f64>,
    /// The mean of realized less expected.
    pub mean_error: Option<f64>,
    /// The least squares slope of realized on expected, 1 if calibrated; none if expected never
    /// varies.
    pub slope: Option<f64>,
    /// The mean of realized less expected over the top tenth of rows by expected, rounded up, the
    /// earlier first on a tie.
    pub top_decile_gap: Option<f64>,
}

impl ExpectedScore {
    /// Returns the score of `rows`, each (expected, realized, baseline's expected).
    #[must_use]
    pub fn of(rows: &[(f64, f64, f64)]) -> Self {
        if rows.is_empty() {
            return Self::default();
        }
        let count = rows.len() as f64;
        let mean = |value: fn(&(f64, f64, f64)) -> f64| rows.iter().map(value).sum::<f64>() / count;
        let squares = |value: fn(&(f64, f64, f64)) -> f64| {
            rows.iter().map(|row| value(row).powi(2)).sum::<f64>()
        };
        let (error, baseline_error) = (squares(|r| r.1 - r.0), squares(|r| r.1 - r.2));
        let (expected, realized) = (mean(|r| r.0), mean(|r| r.1));
        let (covariance, variance) = rows.iter().fold((0.0, 0.0), |(cov, var), row| {
            let x = row.0 - expected;
            (x.mul_add(row.1 - realized, cov), x.mul_add(x, var))
        });
        let mut ranked = rows.to_vec();
        ranked.sort_by(|a, b| b.0.total_cmp(&a.0));
        let top = &ranked[..rows.len().div_ceil(10)];
        Self {
            rows: rows.len(),
            skill: (baseline_error > 0.0).then(|| 1.0 - error / baseline_error),
            mean_error: Some(mean(|r| r.1 - r.0)),

            // Compared to the first row, since a constant's mean rounds and its variance is noise.
            slope: rows
                .iter()
                .any(|row| row.0 != rows[0].0)
                .then(|| covariance / variance),
            top_decile_gap: Some(top.iter().map(|r| r.1 - r.0).sum::<f64>() / top.len() as f64),
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn close(actual: Option<f64>, expected: f64) {
        let actual = actual.unwrap();
        assert!((actual - expected).abs() < 1e-12, "{actual} != {expected}");
    }

    /// Squared errors 0.01 + 0.36 + 0.49 + 0.36 = 1.22, the baseline's 0.0625 + 0.5625 + 0.5625 +
    /// 0.4225 = 1.61: skill 1 - 1.22 / 1.61. Errors -0.1, 0.6, -0.7, 0.6: mean 0.1. Means 0.25 and
    /// 0.35: covariance 0.0525 + 0.0975 + 0.0425 + 0.0275 = 0.22 over variance 0.05, slope 4.4. The
    /// top decile is one row, expected 0.4, realized 1: gap 0.6.
    #[rstest]
    fn expectations_score_their_skill_mean_error_slope_and_top_decile_gap() {
        let rows = [
            (0.1, 0.0, 0.25),
            (0.4, 1.0, 0.25),
            (0.2, -0.5, 0.25),
            (0.3, 0.9, 0.25),
        ];

        let score = ExpectedScore::of(&rows);

        assert_eq!(score.rows, 4);
        close(score.skill, 1.0 - 1.22 / 1.61);
        close(score.mean_error, 0.1);
        close(score.slope, 4.4);
        close(score.top_decile_gap, 0.6);
    }

    /// The top decile of 11 rows is 2, expected 10 and 9, realized 12 and 10: gap (2 + 1) / 2 =
    /// 1.5.
    #[rstest]
    fn the_top_decile_is_the_tenth_of_the_rows_expected_the_most_rounded_up() {
        let rows: Vec<(f64, f64, f64)> = (0..11)
            .map(|i| {
                let realized = match i {
                    10 => 12.0,
                    9 => 10.0,
                    _ => 0.0,
                };
                (f64::from(i), realized, 0.0)
            })
            .collect();

        close(ExpectedScore::of(&rows).top_decile_gap, 1.5);
    }

    /// Mean error and gap are 1 - 0.5 = 0.5, the top decile of 2 rows being 1.
    #[rstest]
    fn no_baseline_error_has_no_skill_and_no_spread_has_no_slope() {
        let score = ExpectedScore::of(&[(0.5, 1.0, 1.0), (0.5, 1.0, 1.0)]);

        assert_eq!(
            score,
            ExpectedScore {
                rows: 2,
                skill: None,
                mean_error: Some(0.5),
                slope: None,
                top_decile_gap: Some(0.5),
            }
        );
    }

    /// The mean of 24 rows of -0.2 is not -0.2 to the bit.
    #[rstest]
    fn one_expectation_over_many_rows_has_no_slope() {
        let rows: Vec<(f64, f64, f64)> = (0..24)
            .map(|i| (-0.2, f64::from(i % 3) - 1.0, 0.0))
            .collect();

        assert_eq!(ExpectedScore::of(&rows).slope, None);
    }

    #[rstest]
    fn no_row_has_no_expected_score() {
        assert_eq!(ExpectedScore::of(&[]), ExpectedScore::default());
    }
}
