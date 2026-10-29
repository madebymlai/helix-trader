//! A backtest of a strategy trading on its model, retrained on its
//! schedule as live would be: what the backtest does before its run, and
//! the out-of-sample scores it reports after.
//!
//! Before the run (`WalkForward::prepare`), the dataset is built over the
//! span, from the window of the first retrain to the end of the span's
//! last day (`Dataset::build`), so that the last retrain period has
//! targets to score; then the model is trained at every retrain time of
//! the span (`schedule::times`) the store has no model valid from
//! (`schedule::missing`). Each build and train is a child process in a
//! backtest (one Nautilus node per process), handed in by the caller; a
//! train then finds the days of its window built and replays none. A
//! model in the store is reused, so a second backtest of the same span
//! builds and trains nothing. A train that kept the current model
//! published none, so the next backtest trains at that time again. The
//! run then trades on the store as it is: at each decision, the latest
//! model valid then (`handle`).
//!
//! After the run (`WalkForward::scores`), each decision's row of the
//! dataset in the span is scored against its label with the model the
//! strategy used at it: the latest valid then, a model failing its checks
//! refused and the current one kept, as the handle does. Every model is
//! out of sample on its rows: trained at its valid-from on rows settled
//! by then. Beside it, the baseline: the training mean, fitted at the
//! start of each retrain period on the rows a model trained then is
//! fitted on. The AUC and log loss (`score`) of both are given per
//! retrain period and over the span.

use std::{collections::BTreeSet, fmt, path::Path};

use anyhow::{bail, ensure};
use jiff::Timestamp;
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::StrategyId;

use super::{
    config::ModelConfig,
    dataset::{self, Dataset, DatasetRow},
    features::FeatureSet,
    learner::{LearnerConfig, Model, TrainingSet},
    meta::Meta,
    schedule, score,
    store::{ModelStore, Swap},
    train::{self, Example},
};

/// The retrains and scores of one strategy's backtests.
#[derive(Debug)]
pub struct WalkForward {
    feature_names: Vec<String>,
    config: ModelConfig,
    /// Where the label is among a row's target values.
    label: usize,
    dataset: Dataset,
    store: ModelStore,
    /// The meta model, if any: trained with the primary.
    meta: Option<Meta>,
}

/// The out-of-sample scores of a backtest.
#[derive(Debug, Clone, PartialEq)]
pub struct Scores {
    /// Each retrain period of the span, in order.
    pub periods: Vec<Period>,
    /// The model's over the span, its rows with a model.
    pub model: Score,
    /// The baseline's over the span.
    pub baseline: Score,
    /// The meta model's, on the rows the primary in use made a trade of,
    /// if there is one.
    pub meta: Option<Box<Scores>>,
}

/// The scores of one retrain period.
#[derive(Debug, Clone, PartialEq)]
pub struct Period {
    /// The period's start: its retrain time, or the span's start.
    pub from: Timestamp,
    /// The period's end: the next retrain time, or the span's end.
    pub to: Timestamp,
    /// The valid-froms of the models used in the period: one, unless a
    /// model valid from within it was published off the schedule.
    pub models: Vec<Timestamp>,
    /// The model's, on the rows decided with one.
    pub model: Score,
    /// The baseline's, on every row of the period.
    pub baseline: Score,
}

/// How a model's probabilities do on rows of the dataset.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Score {
    pub rows: usize,
    /// None without a row of each label.
    pub auc: Option<f64>,
    /// None without a row.
    pub log_loss: Option<f64>,
}

impl WalkForward {
    /// The backtests of `strategy_id`'s model, deciding with `features`,
    /// on its dataset in the catalog at `catalog` and its models in the
    /// store under `models`, as `config` says.
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
        let Some(label) = config.targets.label_index(config.label) else {
            bail!("the label {:?} has no target", config.label);
        };
        Ok(Self {
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
            meta: Meta::of_model(models, strategy_id, features, &config)?,
            config,
        })
    }

    /// Prepares a backtest over `[start, end)`: if the dataset lacks a day
    /// from the window of the span's first retrain time to the span's end,
    /// `build` builds it over that span (`Dataset::build` of its days);
    /// then `train` trains at each retrain time of the span the store has
    /// no model valid from, or the meta model's store, if any, in time
    /// order (`Training::train_at`, which trains both).
    ///
    /// # Errors
    ///
    /// Returns an error if the span is empty, the dataset or the store
    /// cannot be read, or as `build` and `train` do.
    pub fn prepare(
        &self,
        start: Timestamp,
        end: Timestamp,
        build: impl FnOnce(Timestamp, Timestamp) -> anyhow::Result<()>,
        mut train: impl FnMut(Timestamp) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let times = schedule::times(self.config.retrain, start, end)?;
        let window = self.meta.as_ref().map_or(self.config.window, |meta| {
            self.config.window.max(meta.config().window)
        });
        let from = times[0] - window;
        let (first, last) = dataset::days(from, end);
        if !self.dataset.missing(first, last)?.is_empty() {
            build(from, end)?;
        }
        let mut missing: BTreeSet<Timestamp> =
            schedule::missing(&times, &self.store.valid_froms()?)
                .into_iter()
                .collect();
        if let Some(meta) = &self.meta {
            missing.extend(schedule::missing(&times, &meta.store().valid_froms()?));
        }
        for at in missing {
            train(at)?;
        }
        Ok(())
    }

    /// The scores of a backtest over `[start, end)`, prepared (`prepare`):
    /// the model's, and the meta model's, if any.
    ///
    /// # Errors
    ///
    /// Returns an error if the span is empty, the dataset lacks a day of
    /// it or of a retrain window, a model fails to predict, or the
    /// dataset or a store cannot be read.
    pub fn scores(&self, start: Timestamp, end: Timestamp) -> anyhow::Result<Scores> {
        let times = schedule::times(self.config.retrain, start, end)?;
        let (first, last) = dataset::days(start, end);
        let missing = self.dataset.missing(first, last)?;
        ensure!(
            missing.is_empty(),
            "the dataset lacks the days {missing:?} of the backtest: build it first"
        );
        let mut rows = self.dataset.read(start, end)?;
        rows.retain(|row| row.ts_event < UnixNanos::from(end));
        let fingerprint = self.dataset.fingerprint().to_string();
        let examples: Vec<Example> = rows
            .iter()
            .map(|row| Example::of(row, train::gained(row, self.label)))
            .collect();
        let models = Swap::new(
            self.store.clone(),
            self.feature_names.clone(),
            fingerprint.clone(),
        );
        let mut scores = score(&times, start, end, &examples, models, |at| {
            self.baseline(at)
        })?;
        if let Some(meta) = &self.meta {
            let examples = meta.examples(&rows.iter().collect::<Vec<_>>())?;
            let models = Swap::new(
                meta.store().clone(),
                meta.feature_names().to_vec(),
                fingerprint,
            );
            let meta_scores = score(&times, start, end, &examples, models, |at| {
                self.meta_baseline(meta, at)
            })?;
            scores.meta = Some(Box::new(meta_scores));
        }
        Ok(scores)
    }

    /// The baseline a retrain at `at` fits: the training mean of the rows
    /// a model trained then is fitted on (`train::split`); none without
    /// such a row.
    fn baseline(&self, at: Timestamp) -> anyhow::Result<Option<Model>> {
        let rows = self.settled(at - self.config.window, at)?;
        let examples = rows
            .iter()
            .map(|row| Example::of(row, train::gained(row, self.label)))
            .collect();
        fit_baseline(self.feature_names.len(), examples, at - self.config.holdout)
    }

    /// The baseline of `meta` a retrain at `at` fits, as `baseline`: on the
    /// rows a meta model trained then is fitted on.
    fn meta_baseline(&self, meta: &Meta, at: Timestamp) -> anyhow::Result<Option<Model>> {
        let rows = self.settled(meta.window_start(at)?, at)?;
        let examples = meta.examples(&rows.iter().collect::<Vec<_>>())?;
        fit_baseline(
            meta.feature_names().len(),
            examples,
            at - meta.config().holdout,
        )
    }

    /// The rows of `[from, at)` settled at `at`, of days the dataset holds.
    fn settled(&self, from: Timestamp, at: Timestamp) -> anyhow::Result<Vec<DatasetRow>> {
        self.dataset.settled(from, at, |_, _| {
            bail!("the dataset lacks days of the window ending {at}: build it first")
        })
    }
}

/// The scores over `[start, end)`, retrained at `times`, of the models
/// `models` swaps in on `examples`, in time order, beside the baseline
/// `baseline` fits at each retrain time.
fn score(
    times: &[Timestamp],
    start: Timestamp,
    end: Timestamp,
    examples: &[Example],
    mut models: Swap,
    baseline: impl Fn(Timestamp) -> anyhow::Result<Option<Model>>,
) -> anyhow::Result<Scores> {
    let valid_froms = models.store().valid_froms()?;
    let (mut model_rows, mut baseline_rows) = (vec![], vec![]);
    let mut periods = vec![];
    for (i, &at) in times.iter().enumerate() {
        let (from, to) = (at.max(start), times.get(i + 1).copied().unwrap_or(end));
        let (from_ns, to_ns) = (UnixNanos::from(from), UnixNanos::from(to));
        let baseline = baseline(at)?;
        let (mut period_models, mut by_model, mut by_baseline) = (BTreeSet::new(), vec![], vec![]);
        for example in examples
            .iter()
            .filter(|example| from_ns <= example.ts_event && example.ts_event < to_ns)
        {
            if let Some((valid_from, model)) =
                models.at(Timestamp::from(example.ts_event), &valid_froms)
            {
                period_models.insert(valid_from);
                by_model.push((model.predict(&example.features)?, example.label));
            }
            if let Some(baseline) = &baseline {
                by_baseline.push((baseline.predict(&example.features)?, example.label));
            }
        }
        periods.push(Period {
            from,
            to,
            models: period_models.into_iter().collect(),
            model: Score::of(&by_model),
            baseline: Score::of(&by_baseline),
        });
        model_rows.extend(by_model);
        baseline_rows.extend(by_baseline);
    }
    Ok(Scores {
        periods,
        model: Score::of(&model_rows),
        baseline: Score::of(&baseline_rows),
        meta: None,
    })
}

/// The training mean of `examples` of `width` features fitted on before
/// the holdout from `holdout` (`train::split`); none without such a row.
fn fit_baseline(
    width: usize,
    examples: Vec<Example>,
    holdout: Timestamp,
) -> anyhow::Result<Option<Model>> {
    let (fitted, _) = train::split(examples, holdout);
    if fitted.is_empty() {
        return Ok(None);
    }
    let set = TrainingSet::new(
        width,
        fitted.iter().flat_map(|row| row.features.clone()).collect(),
        fitted.iter().map(|row| row.label).collect(),
    )?;
    Ok(Some(LearnerConfig::Baseline {}.fit(&set)?))
}

impl Score {
    /// The score of `rows`, each a probability and its label.
    fn of(rows: &[(f64, bool)]) -> Self {
        Self {
            rows: rows.len(),
            auc: score::auc(rows),
            log_loss: score::log_loss(rows),
        }
    }
}

/// The scores as text, in the sections of the backtest's report: one per
/// period, then the span; then the meta model's, if any, the same way.
impl fmt::Display for Scores {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        sections(f, "Model", self)?;
        match &self.meta {
            Some(meta) => sections(f, "Meta model", meta),
            None => Ok(()),
        }
    }
}

/// The sections of `scores`, each headed by `model`, the model's name.
fn sections(f: &mut fmt::Formatter<'_>, model: &str, scores: &Scores) -> fmt::Result {
    for period in &scores.periods {
        writeln!(
            f,
            "\n{model} scores, out of sample, from {} to {}",
            period.from, period.to
        )?;
        let models = if period.models.is_empty() {
            "none".to_string()
        } else {
            let models: Vec<String> = period.models.iter().map(ToString::to_string).collect();
            models.join(", ")
        };
        writeln!(f, "  Model valid from: {models}")?;
        lines(f, false, period.model)?;
        lines(f, true, period.baseline)?;
    }
    writeln!(f, "\n{model} scores, out of sample, over the span")?;
    lines(f, false, scores.model)?;
    lines(f, true, scores.baseline)
}

/// The lines of `score`, the model's or the `baseline`'s.
fn lines(f: &mut fmt::Formatter<'_>, baseline: bool, score: Score) -> fmt::Result {
    let value = |value: Option<f64>| value.map_or_else(|| "n/a".to_string(), |v| format!("{v:.6}"));
    let [auc, log_loss, rows] = if baseline {
        ["Baseline AUC", "Baseline log loss", "Baseline rows"]
    } else {
        ["AUC", "Log loss", "Rows"]
    };
    writeln!(f, "  {auc}: {}", value(score.auc))?;
    writeln!(f, "  {log_loss}: {}", value(score.log_loss))?;
    writeln!(f, "  {rows}: {}", score.rows)
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

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
        config::{MetaConfig, Thresholds},
        decide::{DecisionRule, FeatureRow},
        store::{ModelManifest, StoredModel},
        targets::{Barrier, Distance, Exit, Label, Outcome, Outcomes, TargetRow, TargetsConfig},
    };

    const GOLD: &str = "XAUUSD.ICMARKETS";
    const STRATEGY: &str = "WALK-001";

    fn features() -> Rc<FeatureSet> {
        FeatureSet::builder()
            .quotes(
                InstrumentId::from(GOLD),
                "mid",
                SimpleMovingAverage::new(1, Some(PriceType::Mid)),
            )
            .build()
            .unwrap()
    }

    /// The fake learner on hourly decisions, learning whether the long of
    /// a horizon of 1 hour gains, retrained every day on the day before,
    /// its last 6 hours held out.
    fn config() -> ModelConfig {
        ModelConfig::builder()
            .decision(DecisionRule::Every(SignedDuration::from_hours(1)))
            .targets(
                TargetsConfig::builder()
                    .instrument_id(InstrumentId::from(GOLD))
                    .quantity(Quantity::from(1))
                    .horizon(SignedDuration::from_hours(1))
                    .build(),
            )
            .label(Label::Horizon)
            .retrain(SignedDuration::from_hours(24))
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

    fn walk(dir: &Path) -> WalkForward {
        std::fs::create_dir_all(dir.join("catalog")).unwrap();
        WalkForward::new(
            &dir.join("catalog"),
            &dir.join("models"),
            StrategyId::from(STRATEGY),
            &features(),
            config(),
        )
        .unwrap()
    }

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    /// The row of the decision at `time` on `feature`, its long gaining if
    /// `wins`, settled an hour later.
    fn row(time: &str, feature: f64, wins: bool) -> TargetRow {
        let ns = UnixNanos::from(at(time));
        let outcome = |pnl: &str| Outcome {
            pnl: Money::from(pnl),
            exit: Exit::Time,
        };
        let (long, short) = if wins {
            ("1 USD", "-1 USD")
        } else {
            ("-1 USD", "1 USD")
        };
        TargetRow {
            decision: FeatureRow {
                strategy_id: StrategyId::from(STRATEGY),
                features: vec![feature],
                ts_event: ns,
                ts_init: ns,
            },
            barrier: None,
            horizon: Some(Outcomes {
                long: outcome(long),
                short: outcome(short),
            }),
            settled_at: UnixNanos::from(at(time) + SignedDuration::from_hours(1)),
        }
    }

    /// The 6th to the 8th of January. Fitted at the retrain of the 7th,
    /// before its holdout from 18:00 of the 6th: two wins in three (the
    /// baseline 2/3); held out, a loss. Fitted at the retrain of the 8th:
    /// one win in three (1/3), the decisions of the 7th, scored on the 7th
    /// as well. Scored on the 8th: two wins.
    fn rows() -> Vec<TargetRow> {
        vec![
            row("2026-01-06T10:00:00Z", 0.5, true),
            row("2026-01-06T12:00:00Z", 0.5, false),
            row("2026-01-06T14:00:00Z", 0.5, true),
            row("2026-01-06T20:00:00Z", 0.5, false),
            row("2026-01-07T04:00:00Z", 0.8, true),
            row("2026-01-07T08:00:00Z", 0.3, false),
            row("2026-01-07T12:00:00Z", 0.6, false),
            row("2026-01-08T06:00:00Z", 0.9, true),
            row("2026-01-08T10:00:00Z", 0.2, true),
        ]
    }

    /// Builds the dataset of `walk` from the 6th to the 8th.
    fn build(walk: &WalkForward) {
        walk.dataset
            .write(
                &rows(),
                at("2026-01-06T00:00:00Z"),
                at("2026-01-09T01:00:00Z"),
            )
            .unwrap();
    }

    /// A fake model valid from `valid_from`, of `fingerprint`.
    fn model(walk: &WalkForward, valid_from: Timestamp, fingerprint: String) -> StoredModel {
        let set = TrainingSet::new(1, vec![0.5], vec![true]).unwrap();
        let model = LearnerConfig::Fake {}.fit(&set).unwrap();
        StoredModel {
            manifest: ModelManifest {
                strategy_id: StrategyId::from(STRATEGY),
                valid_from,
                fingerprint,
                feature_names: walk.feature_names.clone(),
                targets: config().targets,
                label: Label::Horizon,
                learner: LearnerConfig::Fake {},
                score: 0.5,
                probes: vec![],
            },
            model: model.save().unwrap(),
        }
    }

    fn publish(walk: &WalkForward, valid_from: Timestamp) {
        let fingerprint = walk.dataset.fingerprint().to_string();
        walk.store
            .publish(&model(walk, valid_from, fingerprint))
            .unwrap();
    }

    /// What `prepare` over the 7th and the 8th asked for: the spans built
    /// and the times trained at. The build writes the dataset, a train
    /// publishes a model.
    fn prepare(walk: &WalkForward) -> (Vec<(Timestamp, Timestamp)>, Vec<Timestamp>) {
        let (built, trained) = (RefCell::new(vec![]), RefCell::new(vec![]));
        walk.prepare(
            at("2026-01-07T00:00:00Z"),
            at("2026-01-09T00:00:00Z"),
            |from, to| {
                built.borrow_mut().push((from, to));
                build(walk);
                Ok(())
            },
            |at| {
                trained.borrow_mut().push(at);
                publish(walk, at);
                Ok(())
            },
        )
        .unwrap();
        (built.take(), trained.take())
    }

    #[test]
    fn a_backtest_builds_from_its_first_window_then_trains_at_each_retrain_time() {
        let dir = tempfile::tempdir().unwrap();

        let (built, trained) = prepare(&walk(dir.path()));

        assert_eq!(
            built,
            [(at("2026-01-06T00:00:00Z"), at("2026-01-09T00:00:00Z"))]
        );
        assert_eq!(
            trained,
            [at("2026-01-07T00:00:00Z"), at("2026-01-08T00:00:00Z")]
        );
    }

    #[test]
    fn a_second_backtest_of_the_span_builds_and_trains_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk(dir.path());
        prepare(&walk);

        assert_eq!(prepare(&walk), (vec![], vec![]));
    }

    #[test]
    fn a_model_in_the_store_is_reused() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk(dir.path());
        publish(&walk, at("2026-01-07T00:00:00Z"));

        let (_, trained) = prepare(&walk);

        assert_eq!(trained, [at("2026-01-08T00:00:00Z")]);
    }

    fn close(actual: Option<f64>, expected: f64) {
        let actual = actual.unwrap();
        assert!((actual - expected).abs() < 1e-6, "{actual} != {expected}");
    }

    /// Worked by hand. The 7th, the fake's probabilities its feature: the
    /// win 0.8 above the losses 0.3 and 0.6 (AUC 1), log loss `-(ln 0.8 +
    /// ln 0.7 + ln 0.4) / 3`; the baseline 2/3 throughout (AUC 0.5),
    /// `-(ln 2/3 + 2 ln 1/3) / 3`. The 8th, two wins (no AUC), `-(ln 0.9 +
    /// ln 0.2) / 2`; the baseline 1/3, `-ln 1/3`. The span: of the six pairs
    /// of a win and a loss, the win above in four; the baseline's wins
    /// 2/3, 1/3 and 1/3 against its losses at 2/3, a tie each for the
    /// first: one of six.
    #[test]
    fn each_period_scores_the_model_in_use_and_the_baseline_fitted_at_its_start() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk(dir.path());
        prepare(&walk);

        let scores = walk
            .scores(at("2026-01-07T00:00:00Z"), at("2026-01-09T00:00:00Z"))
            .unwrap();

        let [first, second] = &scores.periods[..] else {
            panic!("{scores:?}");
        };
        assert_eq!(
            (first.from, first.to, first.models.clone()),
            (
                at("2026-01-07T00:00:00Z"),
                at("2026-01-08T00:00:00Z"),
                vec![at("2026-01-07T00:00:00Z")]
            )
        );
        assert_eq!((first.model.rows, first.baseline.rows), (3, 3));
        close(first.model.auc, 1.0);
        close(first.model.log_loss, 0.498_703);
        close(first.baseline.auc, 0.5);
        close(first.baseline.log_loss, 0.867_563);
        assert_eq!(second.models, [at("2026-01-08T00:00:00Z")]);
        assert_eq!((second.model.rows, second.model.auc), (2, None));
        close(second.model.log_loss, 0.857_399);
        close(second.baseline.log_loss, 1.098_612);
        assert_eq!((scores.model.rows, scores.baseline.rows), (5, 5));
        close(scores.model.auc, 4.0 / 6.0);
        close(scores.model.log_loss, 0.642_181);
        close(scores.baseline.auc, 1.0 / 6.0);
        close(scores.baseline.log_loss, 0.959_983);
    }

    #[test]
    fn a_model_failing_its_checks_is_not_scored_and_the_current_one_is() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk(dir.path());
        build(&walk);
        publish(&walk, at("2026-01-07T00:00:00Z"));
        walk.store
            .publish(&model(&walk, at("2026-01-08T00:00:00Z"), "f".repeat(64)))
            .unwrap();

        let scores = walk
            .scores(at("2026-01-07T00:00:00Z"), at("2026-01-09T00:00:00Z"))
            .unwrap();

        assert_eq!(scores.periods[1].models, [at("2026-01-07T00:00:00Z")]);
        assert_eq!(scores.periods[1].model.rows, 2);
    }

    #[test]
    fn rows_before_any_model_have_no_model_score_but_the_baselines() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk(dir.path());
        build(&walk);
        publish(&walk, at("2026-01-08T00:00:00Z"));

        let scores = walk
            .scores(at("2026-01-07T00:00:00Z"), at("2026-01-09T00:00:00Z"))
            .unwrap();

        let first = &scores.periods[0];
        assert!(first.models.is_empty(), "{first:?}");
        assert_eq!((first.model.rows, first.model.log_loss), (0, None));
        assert_eq!(first.baseline.rows, 3);
        assert_eq!(scores.model.rows, 2);
    }

    #[test]
    fn scores_of_a_span_the_dataset_lacks_are_refused() {
        let dir = tempfile::tempdir().unwrap();

        let err = walk(dir.path())
            .scores(at("2026-01-07T00:00:00Z"), at("2026-01-09T00:00:00Z"))
            .unwrap_err();

        assert!(err.to_string().contains("lacks"), "{err}");
    }

    #[test]
    fn the_scores_read_as_report_sections() {
        let score = Score {
            rows: 3,
            auc: Some(0.75),
            log_loss: None,
        };
        let scores = Scores {
            periods: vec![Period {
                from: at("2026-01-07T00:00:00Z"),
                to: at("2026-01-08T00:00:00Z"),
                models: vec![at("2026-01-07T00:00:00Z")],
                model: score,
                baseline: score,
            }],
            model: score,
            baseline: Score::default(),
            meta: None,
        };

        assert_eq!(
            scores.to_string(),
            "
Model scores, out of sample, from 2026-01-07T00:00:00Z to 2026-01-08T00:00:00Z
  Model valid from: 2026-01-07T00:00:00Z
  AUC: 0.750000
  Log loss: n/a
  Rows: 3
  Baseline AUC: 0.750000
  Baseline log loss: n/a
  Baseline rows: 3

Model scores, out of sample, over the span
  AUC: 0.750000
  Log loss: n/a
  Rows: 3
  Baseline AUC: n/a
  Baseline log loss: n/a
  Baseline rows: 0
"
        );
    }

    /// `config` on a barrier besides its horizon, with a meta model on a
    /// window of 48 hours.
    fn with_meta() -> ModelConfig {
        let mut config = config();
        config.targets.barrier = Some(Barrier {
            take_profit: Distance::BasisPoints(Decimal::from(10)),
            stop: Distance::BasisPoints(Decimal::from(10)),
            limit: SignedDuration::from_hours(2),
            atr: None,
        });
        config.meta = Some(
            MetaConfig::builder()
                .window(SignedDuration::from_hours(48))
                .holdout(SignedDuration::from_hours(6))
                .threshold(0.5)
                .min_score(0.5)
                .learner(LearnerConfig::Fake {})
                .build(),
        );
        config
    }

    /// The primary's models in the store at both retrain times, its meta
    /// model's at none: the backtest builds from the meta's longer window
    /// and trains at both times, which train both models.
    #[test]
    fn a_backtest_with_a_meta_model_builds_its_window_and_trains_where_either_lacks() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("catalog")).unwrap();
        let walk = WalkForward::new(
            &dir.path().join("catalog"),
            &dir.path().join("models"),
            StrategyId::from(STRATEGY),
            &features(),
            with_meta(),
        )
        .unwrap();
        for at in [at("2026-01-07T00:00:00Z"), at("2026-01-08T00:00:00Z")] {
            let fingerprint = walk.dataset.fingerprint().to_string();
            walk.store.publish(&model(&walk, at, fingerprint)).unwrap();
        }
        let (built, trained) = (RefCell::new(vec![]), RefCell::new(vec![]));

        walk.prepare(
            at("2026-01-07T00:00:00Z"),
            at("2026-01-09T00:00:00Z"),
            |from, to| {
                built.borrow_mut().push((from, to));
                Ok(())
            },
            |at| {
                trained.borrow_mut().push(at);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(
            built.take(),
            [(at("2026-01-05T00:00:00Z"), at("2026-01-09T00:00:00Z"))]
        );
        assert_eq!(
            trained.take(),
            [at("2026-01-07T00:00:00Z"), at("2026-01-08T00:00:00Z")]
        );
    }

    #[test]
    fn the_meta_models_scores_follow_the_models_in_the_report() {
        let score = Score {
            rows: 2,
            auc: None,
            log_loss: Some(0.5),
        };
        let period = Period {
            from: at("2026-01-07T00:00:00Z"),
            to: at("2026-01-08T00:00:00Z"),
            models: vec![],
            model: score,
            baseline: score,
        };
        let meta = Scores {
            periods: vec![period.clone()],
            model: score,
            baseline: score,
            meta: None,
        };
        let scores = Scores {
            periods: vec![period],
            model: score,
            baseline: score,
            meta: Some(Box::new(meta)),
        };

        let text = scores.to_string();

        let primary = text
            .find("\nModel scores, out of sample, over the span")
            .unwrap();
        let meta = text
            .find("\nMeta model scores, out of sample, from 2026-01-07T00:00:00Z")
            .unwrap();
        assert!(primary < meta, "{text}");
        assert!(
            text.contains("\nMeta model scores, out of sample, over the span"),
            "{text}"
        );
    }
}
