//! How well a model's predictions rank its rows' labels, and how close
//! its probabilities are to them.

/// The least probability `log_loss` takes a label to be given: a model sure
/// of the other label costs a large loss, not an infinite one.
const CLIP: f64 = 1e-15;

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
}
