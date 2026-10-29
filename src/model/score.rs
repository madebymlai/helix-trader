//! How well a model's predictions rank its rows' labels.

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
}
