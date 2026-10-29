//! Training at a time: the one operation that turns a strategy's dataset
//! into a published model, or keeps the current one, for retrains in a
//! backtest and live alike.
//!
//! A train at `t` takes the rows of the window ending at `t`
//! (`ModelConfig::window`), building the days the dataset lacks by one
//! replay to `t` (`Dataset::settled`), and drops the rows not settled at
//! `t`: no row's targets reach past the train, so the purge between the
//! rows and the model's first use is the longest target, by
//! construction. The holdout is the last part of the window
//! (`ModelConfig::holdout`), with the same gap: the model is fitted on
//! the rows before it settled at its start, and scored on it, the AUC of
//! its predictions against the label (`score::auc`). A score at or above
//! the minimum (`ModelConfig::min_score`) publishes the model in the store
//! (`store`), valid from `t`, its manifest probing it on rows of the
//! holdout; else the current model is kept.
//!
//! A model learns whether the long of its label's target gains after its
//! costs (`targets::Label`).
//!
//! A train never errors: what goes wrong is an outcome, the current model
//! kept with the reason (`Kept`).

use std::{fmt, path::Path};

use anyhow::Context;
use jiff::Timestamp;
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::StrategyId;

use super::{
    config::ModelConfig,
    dataset::{Dataset, DatasetRow, SchemaMismatch},
    features::FeatureSet,
    learner::TrainingSet,
    score,
    store::{ModelManifest, ModelStore, Probe, StoredModel},
    targets::TargetRow,
};

/// The most rows of the holdout a published model is probed on.
const PROBES: usize = 16;

/// The training of one strategy's model.
#[derive(Debug)]
pub struct Training {
    strategy_id: StrategyId,
    feature_names: Vec<String>,
    config: ModelConfig,
    /// Where the label is among a row's target values.
    label: usize,
    dataset: Dataset,
    store: ModelStore,
}

/// What a train did.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// A model was published, valid from `valid_from`, with its holdout AUC.
    Published { valid_from: Timestamp, score: f64 },
    /// No model was published: the current one, if any, stays.
    Kept(Kept),
}

/// Why a train kept the current model.
#[derive(Debug, Clone, PartialEq)]
pub enum Kept {
    /// The holdout AUC is below the minimum.
    BelowMinScore { score: f64, min_score: f64 },
    /// The replay, the dataset, the fit or the store failed, or the window
    /// had no row to fit or no holdout to score; the error.
    Failed(String),
    /// The dataset holds rows that do not match the strategy's features or
    /// targets; the mismatch.
    SchemaMismatch(String),
}

impl Training {
    /// The training of `strategy_id`'s model, deciding with `features`, on
    /// its dataset in the catalog at `catalog` and its models in the store
    /// under `models`, as `config` says.
    ///
    /// # Errors
    ///
    /// Returns an error if the targets are invalid, the label names a
    /// target they lack, or the store cannot be keyed.
    pub fn new(
        catalog: &Path,
        models: &Path,
        strategy_id: StrategyId,
        features: &FeatureSet,
        config: ModelConfig,
    ) -> anyhow::Result<Self> {
        let label = config
            .targets
            .label_index(config.label)
            .with_context(|| format!("the label {:?} has no target", config.label))?;
        Ok(Self {
            strategy_id,
            feature_names: features.names().to_vec(),
            label,
            dataset: Dataset::new(
                catalog,
                strategy_id,
                features,
                config.decision,
                config.targets.clone(),
            )?,
            store: ModelStore::new(models, strategy_id, features, &config)?,
            config,
        })
    }

    /// The store the models are published in.
    #[must_use]
    pub const fn store(&self) -> &ModelStore {
        &self.store
    }

    /// Trains at `at`: `replay` runs the strategy over a span of the days
    /// the dataset lacks and returns the rows it settled (see
    /// `Dataset::settled`). Publishes the model, or keeps the current one
    /// with the reason.
    pub fn train_at(
        &self,
        at: Timestamp,
        replay: impl FnOnce(Timestamp, Timestamp) -> anyhow::Result<Vec<TargetRow>>,
    ) -> Outcome {
        match self.train(at, replay) {
            Ok(outcome) => outcome,
            Err(e) => Outcome::Kept(match e.downcast_ref::<SchemaMismatch>() {
                Some(mismatch) => Kept::SchemaMismatch(mismatch.to_string()),
                None => Kept::Failed(format!("{e:#}")),
            }),
        }
    }

    fn train(
        &self,
        at: Timestamp,
        replay: impl FnOnce(Timestamp, Timestamp) -> anyhow::Result<Vec<TargetRow>>,
    ) -> anyhow::Result<Outcome> {
        let rows = self.dataset.settled(at - self.config.window, at, replay)?;
        let (fitted, held_out) = split(rows, at - self.config.holdout);
        let set = TrainingSet::new(
            self.feature_names.len(),
            fitted.iter().flat_map(|row| row.features.clone()).collect(),
            fitted.iter().map(|row| self.label_of(row)).collect(),
        )
        .context("fitting on the window before the holdout")?;
        let model = self.config.learner.fit(&set)?;
        let predictions = held_out
            .iter()
            .map(|row| Ok((model.predict(&row.features)?, self.label_of(row))))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let score = score::auc(&predictions)
            .context("the holdout has no row of each label to score the model on")?;
        if score < self.config.min_score {
            return Ok(Outcome::Kept(Kept::BelowMinScore {
                score,
                min_score: self.config.min_score,
            }));
        }
        let probes = spread(held_out.len())
            .map(|i| Probe {
                row: held_out[i].features.clone(),
                prediction: predictions[i].0,
            })
            .collect();
        self.store.publish(&StoredModel {
            manifest: ModelManifest {
                strategy_id: self.strategy_id,
                valid_from: at,
                fingerprint: self.dataset.fingerprint().to_string(),
                feature_names: self.feature_names.clone(),
                targets: self.config.targets.clone(),
                label: self.config.label,
                learner: self.config.learner.clone(),
                score,
                probes,
            },
            model: model.save()?,
        })?;
        Ok(Outcome::Published {
            valid_from: at,
            score,
        })
    }

    /// Whether the long of the label's target gained in `row`.
    fn label_of(&self, row: &DatasetRow) -> bool {
        row.targets[self.label] > 0.0
    }
}

/// `rows` split at the holdout's start, `holdout`: those before it settled
/// by then, fitted on, and those from it, held out.
fn split(rows: Vec<DatasetRow>, holdout: Timestamp) -> (Vec<DatasetRow>, Vec<DatasetRow>) {
    let holdout = UnixNanos::from(holdout);
    let (fitted, held_out): (Vec<_>, Vec<_>) =
        rows.into_iter().partition(|row| row.ts_event < holdout);
    let fitted = fitted
        .into_iter()
        .filter(|row| row.settled_at <= holdout)
        .collect();
    (fitted, held_out)
}

/// The indices of at most `PROBES` of `len` rows, spread evenly from the
/// first.
fn spread(len: usize) -> impl Iterator<Item = usize> {
    let count = len.min(PROBES);
    (0..count).map(move |i| i * len / count)
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Published { valid_from, score } => {
                write!(
                    f,
                    "published a model valid from {valid_from}, holdout AUC {score}"
                )
            }
            Self::Kept(kept) => write!(f, "kept the current model: {kept}"),
        }
    }
}

impl fmt::Display for Kept {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BelowMinScore { score, min_score } => {
                write!(f, "holdout AUC {score} below the minimum {min_score}")
            }
            Self::Failed(why) => write!(f, "failed: {why}"),
            Self::SchemaMismatch(why) => write!(f, "schema mismatch: {why}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use jiff::SignedDuration;
    use nautilus_indicators::average::sma::SimpleMovingAverage;
    use nautilus_model::{
        enums::PriceType,
        identifiers::InstrumentId,
        types::{Money, Quantity},
    };
    use rust_decimal::Decimal;

    use super::*;
    use crate::model::{
        config::Thresholds,
        decide::{DecisionRule, FeatureRow},
        learner::LearnerConfig,
        targets::{
            Barrier, Exit, Label, Outcome as TargetOutcome, Outcomes, TargetRow, TargetsConfig,
        },
    };

    const GOLD: &str = "XAUUSD.ICMARKETS";
    const STRATEGY: &str = "TRAIN-001";

    fn features(width: usize) -> Rc<FeatureSet> {
        (0..width)
            .fold(FeatureSet::builder(), |builder, i| {
                builder.quotes(
                    InstrumentId::from(GOLD),
                    &format!("mid_{i}"),
                    SimpleMovingAverage::new(i + 1, Some(PriceType::Mid)),
                )
            })
            .build()
            .unwrap()
    }

    /// The fake learner on hourly decisions, learning the barrier (closed
    /// after 2 hours; a horizon of 1 hour besides), on a window of 24
    /// hours, the last 6 held out, published from an AUC of 0.5.
    fn config() -> ModelConfig {
        ModelConfig::builder()
            .decision(DecisionRule::Every(SignedDuration::from_hours(1)))
            .targets(
                TargetsConfig::builder()
                    .instrument_id(InstrumentId::from(GOLD))
                    .quantity(Quantity::from(1))
                    .barrier(Barrier {
                        take_profit: Decimal::from(3),
                        stop: Decimal::from(2),
                        limit: SignedDuration::from_hours(2),
                    })
                    .horizon(SignedDuration::from_hours(1))
                    .build(),
            )
            .label(Label::Barrier)
            .window(SignedDuration::from_hours(24))
            .holdout(SignedDuration::from_hours(6))
            .thresholds(Thresholds {
                long: 0.6,
                short: 0.4,
            })
            .min_score(0.5)
            .learner(LearnerConfig::Fake {})
            .build()
    }

    /// The training in `dir`, its catalog and its store side by side.
    fn training(dir: &Path, config: ModelConfig) -> Training {
        std::fs::create_dir_all(dir.join("catalog")).unwrap();
        Training::new(
            &dir.join("catalog"),
            &dir.join("models"),
            StrategyId::from(STRATEGY),
            &features(1),
            config,
        )
        .unwrap()
    }

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    /// The row of the decision at `time` on `features`, its barrier's long
    /// winning if `wins`, else losing, settled `after` it.
    fn row(time: &str, features: Vec<f64>, wins: bool, after: SignedDuration) -> TargetRow {
        let ns = UnixNanos::from(at(time));
        let outcome = |pnl: &str, exit| TargetOutcome {
            pnl: Money::from(pnl),
            exit,
        };
        let (long, short) = if wins {
            (
                outcome("3 USD", Exit::TakeProfit),
                outcome("-2 USD", Exit::Stop),
            )
        } else {
            (
                outcome("-2 USD", Exit::Stop),
                outcome("3 USD", Exit::TakeProfit),
            )
        };
        TargetRow {
            decision: FeatureRow {
                strategy_id: StrategyId::from(STRATEGY),
                features,
                ts_event: ns,
                ts_init: ns,
            },
            barrier: Some(Outcomes { long, short }),
            horizon: Some(Outcomes {
                long: outcome("0.5 USD", Exit::Time),
                short: outcome("-0.5 USD", Exit::Time),
            }),
            settled_at: UnixNanos::from(at(time) + after),
        }
    }

    fn hour() -> SignedDuration {
        SignedDuration::from_hours(1)
    }

    /// At midnight of the 7th: the window is the 6th, the holdout from
    /// 18:00. Fitted, a win and a loss (mean 0.5); held out, a win
    /// predicted 0.8 and a loss 0.3 (AUC 1). Around them, wins on the
    /// first feature 0.1 that would change both if taken: one before the
    /// window, one before the holdout settling after its start, one in
    /// the holdout settling after the window's end.
    fn rows() -> Vec<TargetRow> {
        vec![
            row("2026-01-05T23:00:00Z", vec![0.1], true, hour()),
            row("2026-01-06T10:00:00Z", vec![0.9], true, hour()),
            row("2026-01-06T16:00:00Z", vec![0.9], false, hour()),
            row(
                "2026-01-06T17:00:00Z",
                vec![0.1],
                true,
                SignedDuration::from_mins(90),
            ),
            row("2026-01-06T19:00:00Z", vec![0.8], true, hour()),
            row("2026-01-06T20:00:00Z", vec![0.3], false, hour()),
            row("2026-01-06T23:30:00Z", vec![0.1], true, hour()),
        ]
    }

    #[test]
    fn a_train_fits_the_window_before_the_holdout_settled_at_its_start_and_scores_the_holdout() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let t = at("2026-01-07T00:00:00Z");

        let outcome = training.train_at(t, |_, _| Ok(rows()));

        assert_eq!(
            outcome,
            Outcome::Published {
                valid_from: t,
                score: 1.0
            }
        );
        let stored = training.store().read(t).unwrap();
        let model = LearnerConfig::Fake {}.load(&stored.model).unwrap();
        // The fake predicts the mean of its training labels where the first
        // feature is missing.
        assert_eq!(model.predict(&[f64::NAN]).unwrap(), 0.5);
        assert_eq!(stored.manifest.score, 1.0);
        let probes: Vec<(Vec<f64>, f64)> = stored
            .manifest
            .probes
            .iter()
            .map(|probe| (probe.row.clone(), probe.prediction))
            .collect();
        assert_eq!(probes, [(vec![0.8], 0.8), (vec![0.3], 0.3)]);
    }

    #[test]
    fn a_train_replays_from_the_first_day_of_the_window_to_its_end() {
        let dir = tempfile::tempdir().unwrap();
        let mut replays = vec![];

        training(dir.path(), config()).train_at(at("2026-01-07T00:00:00Z"), |from, to| {
            replays.push((from, to));
            Ok(rows())
        });

        assert_eq!(
            replays,
            [(at("2026-01-06T00:00:00Z"), at("2026-01-07T00:00:00Z"))]
        );
    }

    #[test]
    fn a_published_model_carries_its_schema_targets_and_learner() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let t = at("2026-01-07T00:00:00Z");

        training.train_at(t, |_, _| Ok(rows()));

        let manifest = training.store().read(t).unwrap().manifest;
        assert_eq!(manifest.strategy_id, StrategyId::from(STRATEGY));
        assert_eq!(manifest.valid_from, t);
        assert_eq!(manifest.feature_names, ["mid_0"]);
        assert_eq!(
            manifest.fingerprint,
            crate::model::dataset::fingerprint(&features(1), config().decision, &config().targets)
                .to_string()
        );
        assert_eq!(manifest.targets, config().targets);
        assert_eq!(manifest.label, Label::Barrier);
        assert_eq!(manifest.learner, LearnerConfig::Fake {});
    }

    #[test]
    fn a_score_below_the_minimum_keeps_the_current_model() {
        let dir = tempfile::tempdir().unwrap();
        let t = at("2026-01-07T00:00:00Z");
        training(dir.path(), config()).train_at(t, |_, _| Ok(rows()));
        let worse = |mut rows: Vec<TargetRow>| {
            // The holdout's win now predicted below its loss: AUC 0.
            rows[4].decision.features = vec![0.2];
            rows
        };
        let gated = training(dir.path(), config());

        let outcome = gated.train_at(t + hour(), |_, _| Ok(worse(rows())));

        assert_eq!(
            outcome,
            Outcome::Kept(Kept::BelowMinScore {
                score: 0.0,
                min_score: 0.5
            })
        );
        assert_eq!(gated.store().valid_froms().unwrap(), [t]);
    }

    #[test]
    fn a_failed_replay_keeps_the_current_model() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());

        let outcome = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| {
            anyhow::bail!("the catalog does not cover the window")
        });

        let Outcome::Kept(Kept::Failed(why)) = outcome else {
            panic!("{outcome:?}");
        };
        assert!(why.contains("does not cover"), "{why}");
        assert!(training.store().valid_froms().unwrap().is_empty());
    }

    #[test]
    fn a_holdout_of_one_label_fails() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let mut rows = rows();
        rows.remove(5);

        let outcome = training.train_at(at("2026-01-07T00:00:00Z"), |_, _| Ok(rows));

        assert!(
            matches!(outcome, Outcome::Kept(Kept::Failed(_))),
            "{outcome:?}"
        );
    }

    #[test]
    fn rows_of_other_features_are_a_schema_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let training = training(dir.path(), config());
        let wide: Vec<TargetRow> = rows()
            .into_iter()
            .map(|mut row| {
                row.decision.features.push(1.0);
                row
            })
            .collect();

        // The 6th settles by midnight of the 7th plus 2 hours, so it is
        // stored, and read back.
        let outcome = training.train_at(at("2026-01-07T02:00:00Z"), |_, _| Ok(wide));

        assert!(
            matches!(outcome, Outcome::Kept(Kept::SchemaMismatch(_))),
            "{outcome:?}"
        );
    }

    #[test]
    fn an_outcome_reads_as_published_or_kept_with_its_reason() {
        let published = Outcome::Published {
            valid_from: at("2026-01-07T00:00:00Z"),
            score: 0.625,
        };
        let below = Outcome::Kept(Kept::BelowMinScore {
            score: 0.5,
            min_score: 0.55,
        });

        assert!(published.to_string().contains("published"), "{published}");
        assert!(below.to_string().contains("kept"), "{below}");
        assert!(below.to_string().contains("below"), "{below}");
    }
}
