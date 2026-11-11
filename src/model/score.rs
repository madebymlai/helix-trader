//! How well a model's predictions do on its rows: how its expected values
//! meet the values realised (`ExpectedScore`), and, of a probability, how
//! it ranks the rows' labels and how close it is to them.

use serde::{Deserialize, Serialize};

/// The least probability `log_loss` takes a label to be given: a model sure
/// of the other label costs a large loss, not an infinite one.
const CLIP: f64 = 1e-15;

/// How a model's expected values meet the values realised on rows, beside
/// a baseline's: all none, of no row.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct ExpectedScore {
    pub rows: usize,
    /// One less the squared error of the expected values over that of the
    /// baseline's; none where the baseline's is nought.
    pub skill: Option<f64>,
    /// The mean of the values realised less those expected.
    pub mean_error: Option<f64>,
    /// The least squares slope of the values realised on those expected, 1
    /// of a calibrated model; none where the expected values never vary.
    pub slope: Option<f64>,
    /// The mean of the values realised less those expected over the tenth
    /// of the rows expected the most, rounded up, the earlier of a tie
    /// first.
    pub top_decile_gap: Option<f64>,
}

impl ExpectedScore {
    /// The score of `rows`, each a value expected, the value realised and
    /// the baseline's expected value.
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
        let (expected, realised) = (mean(|r| r.0), mean(|r| r.1));
        let (covariance, variance) = rows.iter().fold((0.0, 0.0), |(cov, var), row| {
            let x = row.0 - expected;
            (x.mul_add(row.1 - realised, cov), x.mul_add(x, var))
        });
        let mut ranked = rows.to_vec();
        ranked.sort_by(|a, b| b.0.total_cmp(&a.0));
        let top = &ranked[..rows.len().div_ceil(10)];
        Self {
            rows: rows.len(),
            skill: (baseline_error > 0.0).then(|| 1.0 - error / baseline_error),
            mean_error: Some(mean(|r| r.1 - r.0)),
            slope: (variance > 0.0).then(|| covariance / variance),
            top_decile_gap: Some(top.iter().map(|r| r.1 - r.0).sum::<f64>() / top.len() as f64),
        }
    }
}

/// The area under the ROC curve of `rows`, each a prediction and its
/// label: the share of pairs of a true and a false row in which the true
/// row's prediction is the higher, a tie counting half. None without a
/// row of each label.
#[must_use]
pub fn auc(rows: &[(f64, bool)]) -> Option<f64> {
    let mut sorted = rows.to_vec();
    sorted.sort_by(|a, b| a.0.total_cmp(&b.0));
    // Mann-Whitney: the ranks of the true rows, a tie given its run's mean
    // rank, less the least they could sum to.
    let (mut true_ranks, mut trues) = (0.0, 0_usize);
    let mut start = 0;
    while start < sorted.len() {
        let end = start
            + sorted[start..]
                .iter()
                .take_while(|row| row.0.total_cmp(&sorted[start].0).is_eq())
                .count();
        let rank = (start + end + 1) as f64 / 2.0;
        let run_trues = sorted[start..end].iter().filter(|row| row.1).count();
        true_ranks += rank * run_trues as f64;
        trues += run_trues;
        start = end;
    }
    let falses = sorted.len() - trues;
    if trues == 0 || falses == 0 {
        return None;
    }
    let (trues, falses) = (trues as f64, falses as f64);
    Some((true_ranks - trues * (trues + 1.0) / 2.0) / (trues * falses))
}

/// The log loss of `rows`, each a probability and its label: the mean of
/// minus the log of the probability given to the true label, at least
/// `CLIP`. None without a row.
#[must_use]
pub fn log_loss(rows: &[(f64, bool)]) -> Option<f64> {
    if rows.is_empty() {
        return None;
    }
    let total: f64 = rows
        .iter()
        .map(|&(probability, label)| {
            let given = if label {
                probability
            } else {
                1.0 - probability
            };
            -given.max(CLIP).ln()
        })
        .sum();
    Some(total / rows.len() as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_perfect_ranking_scores_one_and_its_reverse_zero() {
        let ranked = [(0.9, true), (0.8, true), (0.2, false), (0.1, false)];
        let reversed = ranked.map(|(prediction, label)| (prediction, !label));

        assert_eq!(auc(&ranked), Some(1.0));
        assert_eq!(auc(&reversed), Some(0.0));
    }

    #[test]
    fn the_auc_is_the_share_of_true_false_pairs_ranked_in_order() {
        // 0.8 > 0.6, 0.8 > 0.2, 0.4 < 0.6, 0.4 > 0.2: three of four.
        let rows = [(0.8, true), (0.4, true), (0.6, false), (0.2, false)];

        assert_eq!(auc(&rows), Some(0.75));
    }

    #[test]
    fn a_tie_counts_half_a_pair() {
        // 0.5 = 0.5 a half, 0.9 > 0.5 a whole: 1.5 of 2.
        let rows = [(0.5, true), (0.9, true), (0.5, false)];

        assert_eq!(auc(&rows), Some(0.75));
        assert_eq!(auc(&[(0.3, true), (0.3, false), (0.3, false)]), Some(0.5));
    }

    #[test]
    fn rows_of_one_label_have_no_auc() {
        assert_eq!(auc(&[(0.3, true), (0.7, true)]), None);
        assert_eq!(auc(&[(0.3, false)]), None);
        assert_eq!(auc(&[]), None);
    }

    #[test]
    fn the_log_loss_is_the_mean_cost_of_the_probability_given_to_each_label() {
        // -(ln 0.8 + ln 0.6) / 2.
        let loss = log_loss(&[(0.8, true), (0.4, false)]).unwrap();

        assert!((loss - 0.366_984_587_6).abs() < 1e-9, "{loss}");
    }

    #[test]
    fn a_sure_wrong_probability_costs_the_clipped_loss() {
        // -ln 1e-15.
        let loss = log_loss(&[(1.0, false)]).unwrap();

        assert!((loss - 34.538_776_394_9).abs() < 1e-6, "{loss}");
        assert_eq!(log_loss(&[(0.0, false)]), Some(0.0));
    }

    #[test]
    fn no_row_has_no_log_loss() {
        assert_eq!(log_loss(&[]), None);
    }

    fn close(actual: Option<f64>, expected: f64) {
        let actual = actual.unwrap();
        assert!((actual - expected).abs() < 1e-12, "{actual} != {expected}");
    }

    /// Expected 0.1, 0.4, 0.2 and 0.3, realised 0, 1, -0.5 and 0.9, the
    /// baseline 0.25 throughout. Squared errors 0.01 + 0.36 + 0.49 + 0.36 =
    /// 1.22, the baseline's 0.0625 + 0.5625 + 0.5625 + 0.4225 = 1.61: a
    /// skill of 1 - 1.22 / 1.61. Errors -0.1, 0.6, -0.7 and 0.6, a mean of
    /// 0.1. The slope: means 0.25 and 0.35, a covariance of 0.0525, 0.0975,
    /// 0.0425 and 0.0275, 0.22, over a variance of 0.05, 4.4. The top
    /// decile, a tenth of four rows rounded up, is the row expected 0.4,
    /// which realised 1: a gap of 0.6.
    #[test]
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

    /// Eleven rows expected 0 to 10: the top decile, a tenth rounded up, is
    /// the two expected 10 and 9, which realised 12 and 10, a gap of 1.5.
    #[test]
    fn the_top_decile_is_the_tenth_of_the_rows_expected_the_most_rounded_up() {
        let rows: Vec<(f64, f64, f64)> = (0..11)
            .map(|i| {
                let realised = match i {
                    10 => 12.0,
                    9 => 10.0,
                    _ => 0.0,
                };
                (f64::from(i), realised, 0.0)
            })
            .collect();

        close(ExpectedScore::of(&rows).top_decile_gap, 1.5);
    }

    /// Realised values all at the baseline leave it no error to beat: no
    /// skill. One expectation for every row has no slope.
    #[test]
    fn no_baseline_error_has_no_skill_and_no_spread_has_no_slope() {
        let score = ExpectedScore::of(&[(0.5, 1.0, 1.0), (0.5, 1.0, 1.0)]);

        assert_eq!((score.skill, score.slope), (None, None));
        close(score.mean_error, 0.5);
    }

    #[test]
    fn no_row_has_no_expected_score() {
        assert_eq!(ExpectedScore::of(&[]), ExpectedScore::default());
    }
}
