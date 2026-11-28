//! Backtests a strategy trading on its models as they are in the store.
//!
//! It trains nothing. Its models are those the binary's `train` published
//! (`train::Training::train_at`), so no backtest trains over the range it judges.
//!
//! Before the run (`WalkForward::untrained`), a backtest is refused if the store holds no model
//! set to trade on at its start (`ModelStore::trades_by`, as a live start checks). The refusal
//! names the first train time of the span (`Schedule::times`). The run then trades on the store as
//! it is. At each decision it uses the latest model set valid then (`handle`), so if the store
//! lacks a retrain time, the earlier model set keeps deciding through that period.
//!
//! After the run (`WalkForward::scores`), each model is scored on the row of each decision in the
//! dataset. The score uses the model set the strategy used at that decision, swapped in as the
//! handle swaps it (`store::Swap`). A row whose model set fails its checks is decided by none.
//! - A side's calibrated expected R, `E'`, is scored against the realized R of its trade.
//! - The meta's `E'` is scored on the rows a side is picked on, against the R of the trade of the
//!   side picked.
//!
//! Every model is out of sample on its rows. It is trained by the valid-from of its model set, on
//! rows settled by then.
//!
//! Each model is scored on the rows it decided, beside its own baseline
//! (`store::ModelManifest::baseline`). The baseline is the training mean of R on the rows that
//! model was fitted on. A row decided with no model set has no score. The score gives skill, mean
//! `R - E'`, calibration slope and top-decile gap (`score::ExpectedScore`), per retrain period and
//! over the span. Beside it are the earnings in R (`earnings::Earnings`), with every trade of
//! those rows, at the commission and at 1.5 times it.
//!
//! The trades a side takes are those the decision itself picks for it (`DecidingModels::evaluate`),
//! before any meta.
//! - A row where both sides clear counts one trade, for the side expected the most.
//! - A tie counts none.
//!
//! A meta takes the trades of the side picked that its margin accepts (`earnings::MetaMargin`).
//! This judges the decision the handle returns, on the target's trade, at the events the strategy's
//! event filter marks. What the strategy actually traded differs from it only where it filters the
//! model's answer on account or execution state, such as an open position or a loss limit.

use std::{collections::BTreeSet, fmt::Display};

use jiff::Timestamp;
use nautilus_core::UnixNanos;

use super::{
    blueprint::Stored,
    dataset::{self, Dataset, DatasetRow},
    deciding::{DecidingModels, MetaVerdict},
    earnings::{Earnings, Example, STRESS, in_r},
    schedule::{Models, Schedule},
    score::ExpectedScore,
    store::{Baselined, ModelStore},
};

/// Checks the store before a backtest and scores the backtest after it, for one strategy.
#[derive(Debug)]
pub struct WalkForward {
    dataset: Dataset,
    schedule: Schedule,
    models: ModelStore,
}

/// The out-of-sample scores of a backtest, one per model.
///
/// The meta's scores are on the rows a side is picked on.
pub type Scores = Models<ModelScores>;

/// The out-of-sample scores of one model.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelScores {
    pub periods: Vec<Period>,
    /// The score over the span, on its rows with a model, beside the baselines.
    pub score: ExpectedScore,
    /// What was earned over the span.
    pub earnings: Earnings,
}

/// The scores of one retrain period.
#[derive(Debug, Clone, PartialEq)]
pub struct Period {
    /// The period's start: its retrain time, or the span's start.
    pub from: Timestamp,
    /// The period's end: the next retrain time, or the span's end.
    pub to: Timestamp,
    /// The valid-froms of the model sets used. There is more than one if a model set was
    /// published off the schedule.
    pub models: Vec<Timestamp>,
    /// The model's score on the rows decided with one, beside the baselines of the models that
    /// decided them.
    pub score: ExpectedScore,
    /// What was earned on those rows: the trades taken, beside every trade.
    pub earnings: Earnings,
}

impl WalkForward {
    pub(super) fn new(model: Stored<'_>) -> Self {
        let Stored {
            dataset,
            models,
            schedule,
            ..
        } = model;
        Self {
            dataset,
            schedule,
            models,
        }
    }

    /// Returns the train time that a backtest over `[start, end)` lacks at its start.
    ///
    /// That is the first train time of the span, whose train fills the gap. It applies if the
    /// store holds no model set to trade on there.
    ///
    /// # Errors
    ///
    /// Returns an error if the span is empty or a store cannot be read.
    pub fn untrained(&self, start: Timestamp, end: Timestamp) -> anyhow::Result<Option<Timestamp>> {
        let first = self.schedule.times(start, end)?[0];
        Ok((!self.models.trades_by(start)?).then_some(first))
    }

    /// Returns the scores of a backtest over `[start, end)`, whose dataset is built over it.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The span is empty.
    /// - The dataset lacks a day of it.
    /// - A model fails to predict.
    /// - The dataset or the store cannot be read.
    pub fn scores(&self, start: Timestamp, end: Timestamp) -> anyhow::Result<Scores> {
        let times = self.schedule.times(start, end)?;
        let (first, last) = dataset::days(start, end);
        let missing = self.dataset.missing(first, last)?;
        anyhow::ensure!(
            missing.is_empty(),
            "the dataset lacks the days {missing:?} of the backtest: build it first"
        );
        let mut rows = self.dataset.read(start, end)?;
        rows.retain(|row| row.ts_event < UnixNanos::from(end));
        let judged = self.judged(&rows)?;
        let scores = |judged: &Vec<Judged>| score(&times, (start, end), judged);
        Ok(Scores {
            long: judged.long.as_ref().map(scores),
            short: judged.short.as_ref().map(scores),
            meta: judged.meta.as_ref().map(scores),
        })
    }

    /// Returns what the model set in use at each of `rows` says of each model's example.
    ///
    /// A side's example is on the side's trade. The meta's example is on the trade of the side
    /// picked (`DecidingModels::evaluate`).
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot be listed, or a model fails to predict.
    fn judged(&self, rows: &[DatasetRow]) -> anyhow::Result<Models<Vec<Judged>>> {
        let listed = self.models.model_sets()?;
        let mut model_sets = self.models.swap();
        let mut judged = Models {
            long: None,
            short: None,
            meta: self.models.meta().map(|_| vec![]),
        };

        for level in self.models.sides() {
            judged.set_side(level.side(), vec![]);
        }

        for row in rows {
            let Ok(Some((valid_from, model_set))) =
                model_sets.at(Timestamp::from(row.ts_event), &listed)
            else {
                continue;
            };
            let evaluation =
                DecidingModels::evaluate(&self.models, model_set, &row.features, &row.sides())?;
            for level in self.models.sides() {
                let side = level.side();
                if let Some(judged) = judged.side_mut(side) {
                    judged.push(Judged {
                        valid_from,
                        example: Example::of_side(row, side),
                        expected: evaluation.expected.of(side),
                        baseline: model_set.pair().side(side).baseline(),
                        taken: evaluation
                            .pick
                            .as_ref()
                            .is_some_and(|pick| pick.picked.side == side),
                    });
                }
            }

            if let (Some(pick), Some(meta), Some(judged)) =
                (evaluation.pick, model_set.meta(), judged.meta.as_mut())
                && let Some(verdict) = pick.meta
            {
                let (expected, taken) = match verdict {
                    MetaVerdict::Accepted(r) => (r.as_f64(), true),
                    MetaVerdict::Declined(r) => (r, false),
                };
                judged.push(Judged {
                    valid_from,
                    expected,
                    baseline: meta.baseline(),
                    taken,
                    example: Example::of_side(row, pick.picked.side),
                });
            }
        }
        Ok(judged)
    }
}

struct Judged {
    valid_from: Timestamp,
    example: Example,
    expected: f64,
    baseline: f64,
    taken: bool,
}

fn score(
    times: &[Timestamp],
    (start, end): (Timestamp, Timestamp),
    judged: &[Judged],
) -> ModelScores {
    let (mut model_rows, mut model_trades) = (vec![], vec![]);
    let mut periods = vec![];

    for (i, &at) in times.iter().enumerate() {
        let (from, to) = (at.max(start), times.get(i + 1).copied().unwrap_or(end));
        let (from_ns, to_ns) = (UnixNanos::from(from), UnixNanos::from(to));
        let (mut period_models, mut rows, mut trades) = (BTreeSet::new(), vec![], vec![]);
        for judged in judged
            .iter()
            .filter(|judged| from_ns <= judged.example.ts_event && judged.example.ts_event < to_ns)
        {
            period_models.insert(judged.valid_from);
            rows.push((judged.expected, judged.example.realized(), judged.baseline));
            trades.push((judged.taken, judged.example.trade));
        }
        periods.push(Period {
            from,
            to,
            models: period_models.into_iter().collect(),
            score: ExpectedScore::of(&rows),
            earnings: Earnings::of(&trades),
        });
        model_rows.extend(rows);
        model_trades.extend(trades);
    }
    ModelScores {
        periods,
        score: ExpectedScore::of(&model_rows),
        earnings: Earnings::of(&model_trades),
    }
}

impl Display for Scores {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (model, scores) in [
            ("Long model", &self.long),
            ("Short model", &self.short),
            ("Meta model", &self.meta),
        ] {
            if let Some(scores) = scores {
                sections(f, model, scores)?;
            }
        }
        Ok(())
    }
}

fn sections(
    f: &mut std::fmt::Formatter<'_>,
    model: &str,
    scores: &ModelScores,
) -> std::fmt::Result {
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
        lines(f, period.score)?;
        earned(f, &period.earnings)?;
    }
    writeln!(f, "\n{model} scores, out of sample, over the span")?;
    lines(f, scores.score)?;
    earned(f, &scores.earnings)
}

fn lines(f: &mut std::fmt::Formatter<'_>, score: ExpectedScore) -> std::fmt::Result {
    let value = |value: Option<f64>| value.map_or_else(|| "n/a".to_string(), |v| format!("{v:.6}"));
    writeln!(f, "  Skill: {}", value(score.skill))?;
    writeln!(f, "  Mean R - E': {}", value(score.mean_error))?;
    writeln!(f, "  Calibration slope: {}", value(score.slope))?;
    writeln!(f, "  Top-decile gap: {}", value(score.top_decile_gap))?;
    writeln!(f, "  Rows: {}", score.rows)
}

fn earned(f: &mut std::fmt::Formatter<'_>, earnings: &Earnings) -> std::fmt::Result {
    let value = |value: Option<f64>| value.map_or_else(|| "n/a".to_string(), |v| format!("{v:.6}"));
    let Earnings {
        taken,
        every,
        taken_stressed,
        every_stressed,
    } = earnings;
    writeln!(f, "  Trades taken: {}", taken.count)?;
    writeln!(
        f,
        "  Expectancy of the trades taken: {}",
        in_r(taken.expectancy)
    )?;
    writeln!(f, "  Win rate of trades taken: {}", value(taken.win_rate))?;
    writeln!(f, "  Every trade: {}", every.count)?;
    writeln!(f, "  Expectancy of every trade: {}", in_r(every.expectancy))?;
    writeln!(f, "  Win rate, every trade: {}", value(every.win_rate))?;
    writeln!(
        f,
        "  Expectancy of the trades taken at {STRESS}x the commission: {}",
        in_r(taken_stressed.expectancy)
    )?;
    writeln!(
        f,
        "  Expectancy of every trade at {STRESS}x the commission: {}",
        in_r(every_stressed.expectancy)
    )
}

#[cfg(test)]
mod tests {
    use std::{path::Path, rc::Rc};

    use jiff::SignedDuration;
    use nautilus_model::{
        data::NautilusDataType,
        identifiers::{InstrumentId, StrategyId},
        types::{Currency, Money},
    };
    use rstest::rstest;
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        backtest::Commission,
        model::{
            blueprint::ModelBlueprint,
            calibration::Line,
            config::{Margins, MetaConfig, ModelConfig},
            dataset::DatasetVenue,
            decide::{DecisionRule, FeatureRow},
            deciding::testing::unchanged,
            earnings::Trades,
            features::{FeatureSet, meta_names, side_names, testing::last_mid},
            learner::{LearnerConfig, TrainingSet},
            schedule::Retrain,
            store::{ModelManifest, StoredModel, StoredModelSet},
            targets::{
                BracketSpec, Distance, Exit, Outcome, Outcomes, PerSide, Side, Target,
                TargetConfig, TargetRow,
            },
        },
        strategy::{requirements::DataRequirements, risk::R},
    };

    const GOLD: &str = "XAUUSD.ICMARKETS";
    const STRATEGY: &str = "WALK-001";

    fn features() -> Rc<FeatureSet> {
        last_mid(InstrumentId::from(GOLD)).build().unwrap()
    }

    /// A fake learner on hourly decisions with a time-only target of 1 hour. It is retrained
    /// daily, and its last 6 hours are held out.
    fn config() -> ModelConfig {
        ModelConfig::builder()
            .decision(DecisionRule::Every(SignedDuration::from_hours(1)))
            .target(
                TargetConfig::builder()
                    .instrument_id(InstrumentId::from(GOLD))
                    .risk("0.5%".parse().unwrap())
                    .spec(
                        Target::builder()
                            .limit(SignedDuration::from_hours(1))
                            .atr(crate::model::targets::DEFAULT_ATR)
                            .build(),
                    )
                    .build(),
            )
            .retrain(SignedDuration::from_hours(24))
            .dataset(venue())
            .window(SignedDuration::from_hours(24))
            .calibration(SignedDuration::from_hours(6))
            .margins(Margins {
                long: Some(R::new(0.0)),
                short: Some(R::new(0.0)),
            })
            .min_rows(1)
            .learner(LearnerConfig::Fake {})
            .build()
    }

    fn requirements() -> DataRequirements {
        DataRequirements {
            bar_types: vec![],
            ticks: vec![crate::strategy::requirements::TickSeries {
                data: NautilusDataType::QuoteTick,
                instrument_id: InstrumentId::from(GOLD),
            }],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        }
    }

    fn venue() -> DatasetVenue {
        DatasetVenue {
            commission: Commission::Rate(Decimal::new(3, 5)),
            balance: Money::from("10000000 USD"),
            latency: None,
        }
    }

    fn blueprint(config: &ModelConfig) -> ModelBlueprint {
        ModelBlueprint::new(
            StrategyId::from(STRATEGY),
            features(),
            config.clone(),
            requirements(),
        )
        .unwrap()
    }

    fn walk(dir: &Path) -> WalkForward {
        walk_of(dir, &config())
    }

    fn walk_of(dir: &Path, config: &ModelConfig) -> WalkForward {
        std::fs::create_dir_all(dir.join("catalog")).unwrap();
        blueprint(config)
            .walk_forward(&dir.join("catalog"), &dir.join("models"))
            .unwrap()
    }

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    /// The long gains 1 USD if `wins`, else loses 1, and the short does the reverse. Each is net of
    /// 0.1 USD commission and has a unit of 1 USD.
    fn row(time: &str, feature: f64, wins: bool) -> TargetRow {
        let ns = UnixNanos::from(at(time));
        let outcome = |pnl: &str| Outcome {
            pnl: Money::from(pnl),
            commission: Money::from("0.1 USD"),
            exit: Exit::Time,
            unit: Money::from("1 USD"),
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
            payoff: PerSide {
                long: vec![0.07],
                short: vec![0.08],
            },
            outcomes: Outcomes {
                long: outcome(long),
                short: outcome(short),
            },
            settled_at: UnixNanos::from(at(time) + SignedDuration::from_hours(1)),
        }
    }

    /// The rows run from the 6th to the 8th. On each, the long's R is 1 or -1 and the short's R is
    /// the reverse.
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

    fn dataset(dir: &Path, config: &ModelConfig) -> Dataset {
        blueprint(config).dataset(&dir.join("catalog")).unwrap()
    }

    fn build(dir: &Path) {
        dataset(dir, &config())
            .write(
                &rows(),
                at("2026-01-06T00:00:00Z"),
                at("2026-01-09T01:00:00Z"),
            )
            .unwrap();
    }

    const CALIBRATION: Line = Line {
        alpha: 0.1,
        beta: 0.5,
    };

    /// A fake model of `side`. Its R is the first feature of the row, and its baseline is
    /// `baseline`.
    fn model_of(
        config: &ModelConfig,
        side: Side,
        fingerprint: String,
        baseline: f64,
    ) -> StoredModel {
        let names = side_names(features().names(), &config.target.payoff_names());
        let set = TrainingSet::new(names.len(), vec![0.5; names.len()], vec![1.0]).unwrap();
        let model = LearnerConfig::Fake {}.fit(&set).unwrap();
        StoredModel {
            manifest: ModelManifest {
                fingerprint,
                feature_names: names,
                target: config.target.clone(),
                side: Some(side),
                learner: LearnerConfig::Fake {},
                calibration: None,
                baseline,
                out_of_fold: None,
                probes: vec![],
            },
            model: model.save().unwrap(),
        }
    }

    fn models(dir: &Path, config: &ModelConfig) -> ModelStore {
        blueprint(config).model_store(&dir.join("models")).unwrap()
    }

    fn publish_model_set(
        dir: &Path,
        config: &ModelConfig,
        valid_from: Timestamp,
        sides: Vec<StoredModel>,
        meta: Option<StoredModel>,
    ) {
        models(dir, config)
            .publish(&StoredModelSet {
                valid_from,
                sides,
                calibration: unchanged(config),
                meta,
            })
            .unwrap();
    }

    fn publish(dir: &Path, config: &ModelConfig, valid_from: Timestamp) {
        let fingerprint = dataset(dir, config).fingerprint().to_string();
        let sides = config
            .margins
            .sides()
            .map(|(side, _)| {
                model_of(
                    config,
                    side,
                    fingerprint.clone(),
                    trained_baseline(side, valid_from),
                )
            })
            .collect();
        publish_model_set(dir, config, valid_from, sides, None);
    }

    /// The training mean of the side's R on `rows` at the 7th and the 8th, nought at any other
    /// time.
    fn trained_baseline(side: Side, valid_from: Timestamp) -> f64 {
        let long = if valid_from == at("2026-01-07T00:00:00Z") {
            1.0 / 3.0
        } else if valid_from == at("2026-01-08T00:00:00Z") {
            -1.0 / 3.0
        } else {
            0.0
        };

        match side {
            Side::Long => long,
            Side::Short => -long,
        }
    }

    fn trained(dir: &Path, config: &ModelConfig) {
        build(dir);
        for valid_from in [at("2026-01-07T00:00:00Z"), at("2026-01-08T00:00:00Z")] {
            publish(dir, config, valid_from);
        }
    }

    fn close(actual: Option<f64>, expected: f64) {
        let actual = actual.unwrap();
        assert!((actual - expected).abs() < 1e-6, "{actual} != {expected}");
    }

    #[rstest]
    fn each_period_scores_its_model_beside_that_models_own_baseline_on_the_same_rows() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk(dir.path());
        trained(dir.path(), &config());

        let scores = walk
            .scores(at("2026-01-07T00:00:00Z"), at("2026-01-09T00:00:00Z"))
            .unwrap();

        let long = scores.long.as_ref().unwrap();
        let [first, second] = &long.periods[..] else {
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
        let first = first.score;
        assert_eq!(first.rows, 3);
        close(first.skill, 1.0 - 4.29 / 4.0);
        close(first.mean_error, -0.9);
        close(first.slope, 70.0 / 19.0);
        close(first.top_decile_gap, 0.2);
        assert_eq!(second.models, [at("2026-01-08T00:00:00Z")]);
        let second = second.score;
        assert_eq!(second.rows, 2);
        close(second.skill, 1.0 - 0.65 * 9.0 / 32.0);
        close(second.mean_error, 0.45);
        close(second.slope, 0.0);
        close(second.top_decile_gap, 0.1);
        let span = long.score;
        assert_eq!(span.rows, 5);
        close(span.skill, 1.0 - 4.94 * 9.0 / 68.0);
        close(span.mean_error, -1.8 / 5.0);
        close(span.slope, 0.44 / 0.372);
        let short = scores.short.as_ref().unwrap().periods[0].score;
        close(short.skill, 1.0 - 3.89 / 4.0);
        close(short.slope, -70.0 / 19.0);
    }

    fn one_sided_scores(dir: &Path, side: Side) -> Scores {
        let margin = |traded| (traded == side).then_some(R::new(0.5));
        let config = ModelConfig {
            margins: Margins {
                long: margin(Side::Long),
                short: margin(Side::Short),
            },
            ..config()
        };
        let walk = walk_of(dir, &config);
        let span = ("2026-01-07T00:00:00Z", "2026-01-09T00:00:00Z");
        trained(dir, &config);
        walk.scores(at(span.0), at(span.1)).unwrap()
    }

    #[rstest]
    #[case::the_long(
        Side::Long,
        [
            (2, 0.0),
            (3, -1.0 / 3.0),
            (2, -0.05),
            (3, -1.0 / 3.0 - 0.05),
            (1, 1.0),
            (2, 1.0),
            (3, 1.0 / 3.0),
            (5, 0.2),
        ]
    )]
    #[case::the_short(
        Side::Short,
        [
            (2, 0.0),
            (3, 1.0 / 3.0),
            (2, -0.05),
            (3, 1.0 / 3.0 - 0.05),
            (1, -1.0),
            (2, -1.0),
            (3, -1.0 / 3.0),
            (5, -0.2),
        ]
    )]
    fn each_period_reports_what_the_models_decisions_earned_beside_every_trade(
        #[case] side: Side,
        #[case] expected: [(usize, f64); 8],
    ) {
        let dir = tempfile::tempdir().unwrap();

        let scores = one_sided_scores(dir.path(), side);

        let traded = match side {
            Side::Long => scores.long.as_ref(),
            Side::Short => scores.short.as_ref(),
        }
        .unwrap();
        let [first, second] = &traded.periods[..] else {
            panic!("{scores:?}");
        };
        let earned = [
            first.earnings.taken,
            first.earnings.every,
            first.earnings.taken_stressed,
            first.earnings.every_stressed,
            second.earnings.taken,
            second.earnings.every,
            traded.earnings.taken,
            traded.earnings.every,
        ];

        for (trades, (count, expectancy)) in earned.into_iter().zip(expected) {
            trades_are(trades, count, expectancy);
        }
        close(first.earnings.taken.win_rate, 0.5);
    }

    /// Opposed sides. The long is a fake expecting the feature of its row in R. The short is a
    /// baseline expecting 0.5R whatever the row.
    fn publish_opposed(
        dir: &Path,
        config: &ModelConfig,
        valid_from: Timestamp,
        meta: Option<StoredModel>,
    ) {
        let fingerprint = dataset(dir, config).fingerprint().to_string();
        let sides = Side::BOTH
            .into_iter()
            .map(|side| {
                let mut opposed = model_of(
                    config,
                    side,
                    fingerprint.clone(),
                    trained_baseline(side, valid_from),
                );

                if side == Side::Short {
                    predict_constant(&mut opposed, 0.5);
                }
                opposed
            })
            .collect();
        publish_model_set(dir, config, valid_from, sides, meta);
    }

    fn predict_constant(model: &mut StoredModel, expected: f64) {
        let width = model.manifest.feature_names.len();
        let set = TrainingSet::new(width, vec![0.0; width], vec![expected]).unwrap();
        model.manifest.learner = LearnerConfig::Baseline {};
        model.model = model.manifest.learner.fit(&set).unwrap().save().unwrap();
    }

    /// Long expects 0.30R and realizes -1R, and short expects 0.20R and realizes +1R. A meta
    /// expecting 0.10R against 0.15R declines the long.
    #[rstest]
    #[case::without_meta(false)]
    #[case::declined(true)]
    fn both_sides_score_and_the_long_pick_counts_even_when_declined(#[case] has_meta: bool) {
        let dir = tempfile::tempdir().unwrap();
        let mut config = config();
        if has_meta {
            config.meta = with_meta().meta;
            config.meta.as_mut().unwrap().margin = R::new(0.15);
        }
        let walk = walk_of(dir.path(), &config);
        let start = at("2026-01-07T08:00:00Z");
        let end = start + SignedDuration::from_hours(1);
        build(dir.path());
        let dataset = dataset(dir.path(), &config);
        let sides = Side::BOTH
            .into_iter()
            .map(|side| {
                let mut model = model_of(&config, side, dataset.fingerprint().to_string(), 0.0);
                if side == Side::Short {
                    predict_constant(&mut model, 0.2);
                }
                model
            })
            .collect();
        let meta = config.meta.as_ref().map(|_| {
            let mut meta = meta_model(dataset.fingerprint().to_string());
            predict_constant(&mut meta, 0.0); // Calibrates to 0.1 + 0.5 * 0.
            meta
        });
        publish_model_set(dir.path(), &config, start, sides, meta);
        let mut deciding = DecidingModels::new(&models(dir.path(), &config)).unwrap();
        let rows = dataset.read(start, end).unwrap();
        let row = &rows[0];

        let taken = deciding.decide(start, &row.features, &row.sides()).unwrap();
        let scores = walk.scores(start, end).unwrap();

        if has_meta {
            assert_eq!(taken, None);
            let meta = scores.meta.unwrap();
            assert_eq!(meta.score.rows, 1);
            close(meta.score.mean_error, -1.10);
            assert_eq!(meta.earnings.taken.count, 0);
        } else {
            let taken = taken.unwrap();
            assert_eq!(taken.side, Side::Long);
            close(Some(taken.expected_r.as_f64()), 0.30);
            assert_eq!(taken.meta_expected_r, None);
        }
        let (long, short) = (scores.long.unwrap(), scores.short.unwrap());
        assert_eq!((long.score.rows, short.score.rows), (1, 1));
        close(long.score.mean_error, -1.30);
        close(short.score.mean_error, 0.80);
        trades_are(long.earnings.taken, 1, -1.0);
        assert_eq!(short.earnings.taken.count, 0);
    }

    /// Each side clears its margin of nought on every row, and the side expected more is picked.
    /// The long expects its feature and the short expects 0.5.
    fn opposed_scores(dir: &Path) -> Scores {
        let walk = walk(dir);
        build(dir);
        for valid_from in [at("2026-01-07T00:00:00Z"), at("2026-01-08T00:00:00Z")] {
            publish_opposed(dir, &config(), valid_from, None);
        }
        walk.scores(at("2026-01-07T00:00:00Z"), at("2026-01-09T00:00:00Z"))
            .unwrap()
    }

    fn trades_are(trades: Trades, count: usize, expectancy: f64) {
        assert_eq!(trades.count, count, "{trades:?}");
        close(trades.expectancy, expectancy);
    }

    #[rstest]
    fn each_trade_counts_for_the_side_picked_alone_where_both_sides_clear() {
        let dir = tempfile::tempdir().unwrap();

        let scores = opposed_scores(dir.path());

        let (long, short) = (scores.long.unwrap(), scores.short.unwrap());
        trades_are(long.periods[0].earnings.taken, 2, 0.0);
        close(long.periods[0].earnings.taken.win_rate, 0.5);
        trades_are(short.periods[0].earnings.taken, 1, 1.0);
        close(short.periods[0].earnings.taken.win_rate, 1.0);
        trades_are(long.periods[1].earnings.taken, 1, 1.0);
        trades_are(short.periods[1].earnings.taken, 1, -1.0);
        trades_are(long.earnings.taken, 3, 1.0 / 3.0);
        trades_are(short.earnings.taken, 2, 0.0);
    }

    #[rstest]
    fn a_sides_stressed_earnings_count_only_the_trades_picked_for_it() {
        let dir = tempfile::tempdir().unwrap();

        let scores = opposed_scores(dir.path());

        let (long, short) = (scores.long.unwrap(), scores.short.unwrap());
        trades_are(long.periods[0].earnings.taken_stressed, 2, -0.05);
        trades_are(short.periods[0].earnings.taken_stressed, 1, 0.95);
        trades_are(long.earnings.taken_stressed, 3, 1.0 / 3.0 - 0.05);
        trades_are(short.earnings.taken_stressed, 2, -0.05);
    }

    #[rstest]
    fn each_sides_every_trade_stays_its_own_beside_the_trades_picked() {
        let dir = tempfile::tempdir().unwrap();

        let scores = opposed_scores(dir.path());

        let (long, short) = (scores.long.unwrap(), scores.short.unwrap());
        trades_are(long.periods[0].earnings.every, 3, -1.0 / 3.0);
        trades_are(short.periods[0].earnings.every, 3, 1.0 / 3.0);
        trades_are(long.periods[1].earnings.every, 2, 1.0);
        trades_are(short.periods[1].earnings.every, 2, -1.0);
        trades_are(long.earnings.every, 5, 0.2);
        trades_are(short.earnings.every, 5, -0.2);
        trades_are(long.earnings.every_stressed, 5, 0.15);
        trades_are(short.earnings.every_stressed, 5, -0.25);
    }

    #[rstest]
    fn each_sides_score_stays_on_all_its_rows_with_a_model() {
        let dir = tempfile::tempdir().unwrap();

        let scores = opposed_scores(dir.path());

        let (long, short) = (scores.long.unwrap(), scores.short.unwrap());
        let (first_long, first_short) = (long.periods[0].score, short.periods[0].score);
        assert_eq!((first_long.rows, first_short.rows), (3, 3));
        close(first_long.skill, 1.0 - 4.29 / 4.0);
        close(first_short.skill, 1.0 - 2.75 / 4.0);
        assert_eq!(first_short.slope, None);
        assert_eq!((long.score.rows, short.score.rows), (5, 5));
    }

    #[rstest]
    fn a_row_where_the_sides_tie_counts_no_trade_for_either() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk(dir.path());
        trained(dir.path(), &config());

        let scores = walk
            .scores(at("2026-01-07T00:00:00Z"), at("2026-01-09T00:00:00Z"))
            .unwrap();

        for side in [scores.long.unwrap(), scores.short.unwrap()] {
            assert_eq!(side.earnings.taken.count, 0, "{side:?}");
            assert_eq!(side.earnings.taken_stressed.count, 0, "{side:?}");
            assert_eq!(side.earnings.every.count, 5, "{side:?}");
            assert_eq!(side.score.rows, 5, "{side:?}");
        }
    }

    fn with_exits() -> ModelConfig {
        ModelConfig {
            target: TargetConfig::builder()
                .instrument_id(InstrumentId::from("AUD/USD.SIM"))
                .risk("0.5%".parse().unwrap())
                .spec(
                    Target::builder()
                        .bracket(BracketSpec {
                            take_profit: Distance::BasisPoints(Decimal::from(20)),
                            stop: Distance::BasisPoints(Decimal::from(10)),
                            atr: None,
                        })
                        .limit(SignedDuration::from_hours(1))
                        .build(),
                )
                .build(),
            ..config()
        }
    }

    fn row_of_stops(time: &str, (long, short): (f64, f64)) -> TargetRow {
        let mut row = row(time, 0.8, true);
        row.outcomes.long.unit = Money::new(long, Currency::USD());
        row.outcomes.short.unit = Money::new(short, Currency::USD());
        row.payoff = PerSide {
            long: vec![1.4, 1.1, 0.07],
            short: vec![1.3, 1.2, 0.08],
        };
        row
    }

    #[rstest]
    fn sides_expecting_the_same_r_tie_whatever_their_units() {
        let dir = tempfile::tempdir().unwrap();
        let config = with_exits();
        let walk = walk_of(dir.path(), &config);
        let (seventh, eighth) = (at("2026-01-07T00:00:00Z"), at("2026-01-08T00:00:00Z"));
        dataset(dir.path(), &config)
            .write(
                &[
                    row_of_stops("2026-01-07T04:00:00Z", (2.0, 4.0)),
                    row_of_stops("2026-01-07T08:00:00Z", (4.0, 2.0)),
                ],
                seventh,
                eighth + SignedDuration::from_hours(1),
            )
            .unwrap();
        publish(dir.path(), &config, seventh);

        let scores = walk.scores(seventh, eighth).unwrap();

        assert_eq!(scores.long.unwrap().earnings.taken.count, 0);
        assert_eq!(scores.short.unwrap().earnings.taken.count, 0);
    }

    /// Its R is the first feature of the row, calibrated by `CALIBRATION`. Its baseline is 0.5.
    fn meta_model(fingerprint: String) -> StoredModel {
        let names = meta_names(features().names());
        let set = TrainingSet::new(names.len(), vec![0.5; names.len()], vec![1.0]).unwrap();
        let mut meta = model_of(&config(), Side::Long, fingerprint, 0.5);
        meta.manifest.side = None;
        meta.manifest.calibration = Some(CALIBRATION);
        meta.manifest.feature_names = names;
        meta.model = LearnerConfig::Fake {}.fit(&set).unwrap().save().unwrap();
        meta
    }

    /// Opposed sides (`publish_opposed`) and a meta (`meta_model`), valid from the 7th and the 8th.
    /// The meta takes a trade from an expected 0.3 USD.
    fn opposed_meta_scores(dir: &Path) -> Scores {
        opposed_meta_scores_of(dir, &with_meta(), &rows())
    }

    fn opposed_meta_scores_of(dir: &Path, config: &ModelConfig, rows: &[TargetRow]) -> Scores {
        let walk = walk_of(dir, config);
        dataset(dir, config)
            .write(rows, at("2026-01-06T00:00:00Z"), at("2026-01-09T01:00:00Z"))
            .unwrap();
        let fingerprint = dataset(dir, config).fingerprint().to_string();

        for valid_from in [at("2026-01-07T00:00:00Z"), at("2026-01-08T00:00:00Z")] {
            publish_opposed(
                dir,
                config,
                valid_from,
                Some(meta_model(fingerprint.clone())),
            );
        }
        walk.scores(at("2026-01-07T00:00:00Z"), at("2026-01-09T00:00:00Z"))
            .unwrap()
    }

    #[rstest]
    fn the_metas_margin_is_in_r_whatever_the_unit_of_the_side_picked() {
        let dir = tempfile::tempdir().unwrap();
        let config = with_meta();
        let at_two: Vec<TargetRow> = rows()
            .into_iter()
            .map(|mut row| {
                row.outcomes.long.unit = Money::from("2 USD");
                row.outcomes.short.unit = Money::from("2 USD");
                row
            })
            .collect();

        let meta = opposed_meta_scores_of(dir.path(), &config, &at_two)
            .meta
            .unwrap();

        trades_are(meta.earnings.taken, 3, 1.0 / 6.0);
        trades_are(meta.earnings.every, 5, 0.1);
    }

    #[rstest]
    fn a_metas_earnings_read_the_trades_of_the_side_picked_from_its_margin() {
        let dir = tempfile::tempdir().unwrap();

        let meta = opposed_meta_scores(dir.path()).meta.unwrap();

        trades_are(meta.periods[0].earnings.taken, 2, 0.0);
        trades_are(meta.periods[0].earnings.every, 3, 1.0 / 3.0);
        trades_are(meta.periods[1].earnings.taken, 1, 1.0);
        trades_are(meta.periods[1].earnings.every, 2, 0.0);
        trades_are(meta.earnings.taken, 3, 1.0 / 3.0);
        trades_are(meta.earnings.every, 5, 0.2);
    }

    #[rstest]
    fn the_metas_expected_r_is_scored_on_the_trades_of_the_side_picked() {
        let dir = tempfile::tempdir().unwrap();

        let meta = opposed_meta_scores(dir.path()).meta.unwrap();

        let score = meta.score;
        assert_eq!(score.rows, 5);
        close(score.skill, 1.0 - 4.415 / 5.25);
        close(score.mean_error, -0.9 / 5.0);
        close(score.slope, 0.32 / 0.093);
        close(score.top_decile_gap, 0.45);
    }

    #[rstest]
    fn a_sides_trades_picked_are_counted_before_any_meta() {
        let dir = tempfile::tempdir().unwrap();

        let scores = opposed_meta_scores(dir.path());

        trades_are(scores.long.unwrap().earnings.taken, 3, 1.0 / 3.0);
        trades_are(scores.short.unwrap().earnings.taken, 2, 0.0);
    }

    /// The 8th's long has another fingerprint, which is refused as it loads.
    #[rstest]
    fn rows_of_a_model_set_failing_its_checks_are_not_scored() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk(dir.path());
        build(dir.path());
        let eighth = at("2026-01-08T00:00:00Z");
        publish(dir.path(), &config(), at("2026-01-07T00:00:00Z"));
        let fingerprint = dataset(dir.path(), &config()).fingerprint().to_string();
        publish_model_set(
            dir.path(),
            &config(),
            eighth,
            vec![
                model_of(&config(), Side::Long, "f".repeat(64), 0.5),
                model_of(&config(), Side::Short, fingerprint, 0.5),
            ],
            None,
        );

        let scores = walk
            .scores(at("2026-01-07T00:00:00Z"), at("2026-01-09T00:00:00Z"))
            .unwrap();

        for side in [scores.long.unwrap(), scores.short.unwrap()] {
            let [first, second] = &side.periods[..] else {
                panic!("{side:?}");
            };
            assert_eq!(first.score.rows, 3, "{first:?}");
            assert!(second.models.is_empty(), "{second:?}");
            assert_eq!(second.score.rows, 0, "{second:?}");
        }
    }

    #[rstest]
    fn a_meta_is_scored_beside_its_own_model_sets_sides_alone() {
        let dir = tempfile::tempdir().unwrap();
        let config = with_meta();
        let walk = walk_of(dir.path(), &config);
        dataset(dir.path(), &config)
            .write(
                &rows(),
                at("2026-01-06T00:00:00Z"),
                at("2026-01-09T01:00:00Z"),
            )
            .unwrap();
        let fingerprint = dataset(dir.path(), &config).fingerprint().to_string();
        publish_opposed(
            dir.path(),
            &config,
            at("2026-01-07T00:00:00Z"),
            Some(meta_model(fingerprint.clone())),
        );
        publish_opposed(
            dir.path(),
            &config,
            at("2026-01-08T00:00:00Z"),
            Some(meta_model(fingerprint)),
        );

        let scores = walk
            .scores(at("2026-01-07T00:00:00Z"), at("2026-01-09T00:00:00Z"))
            .unwrap();

        let meta = scores.meta.unwrap();
        assert_eq!(meta.periods[0].score.rows, 3, "{meta:?}");
        assert_eq!(
            meta.periods[0].models,
            [at("2026-01-07T00:00:00Z")],
            "{meta:?}"
        );
        assert_eq!(meta.periods[1].score.rows, 2, "{meta:?}");
        assert_eq!(
            meta.periods[1].models,
            [at("2026-01-08T00:00:00Z")],
            "{meta:?}"
        );
        trades_are(scores.long.unwrap().periods[1].earnings.taken, 1, 1.0);
    }

    #[rstest]
    fn rows_before_any_model_have_no_score_of_a_model_nor_of_a_baseline() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk(dir.path());
        build(dir.path());
        publish(dir.path(), &config(), at("2026-01-08T00:00:00Z"));

        let scores = walk
            .scores(at("2026-01-07T00:00:00Z"), at("2026-01-09T00:00:00Z"))
            .unwrap();

        let long = scores.long.unwrap();
        let first = &long.periods[0];
        assert!(first.models.is_empty(), "{first:?}");
        assert_eq!(first.score, ExpectedScore::default());
        assert_eq!(long.score.rows, 2);
    }

    /// A model set valid from 12:00 of the 7th, its long's baseline 1/4. The 7th's period uses the
    /// 7th's long, baseline 1/3, at 04:00 and 08:00, and this one at 12:00.
    #[rstest]
    fn a_period_scores_each_row_beside_the_baseline_of_the_model_that_decided_it() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk(dir.path());
        build(dir.path());
        for at in [at("2026-01-07T00:00:00Z"), at("2026-01-08T00:00:00Z")] {
            publish(dir.path(), &config(), at);
        }
        let fingerprint = dataset(dir.path(), &config()).fingerprint().to_string();
        let sides = [(Side::Long, 0.25), (Side::Short, 0.75)]
            .map(|(side, baseline)| model_of(&config(), side, fingerprint.clone(), baseline));
        publish_model_set(
            dir.path(),
            &config(),
            at("2026-01-07T12:00:00Z"),
            sides.into(),
            None,
        );

        let scores = walk
            .scores(at("2026-01-07T00:00:00Z"), at("2026-01-09T00:00:00Z"))
            .unwrap();

        let first = &scores.long.unwrap().periods[0];
        assert_eq!(
            first.models,
            [at("2026-01-07T00:00:00Z"), at("2026-01-07T12:00:00Z")]
        );
        let first = first.score;
        assert_eq!(first.rows, 3);
        close(first.skill, 1.0 - 4.29 / (20.0 / 9.0 + 25.0 / 16.0));
    }

    #[rstest]
    fn scores_of_a_span_the_dataset_lacks_are_refused() {
        let dir = tempfile::tempdir().unwrap();

        let err = walk(dir.path())
            .scores(at("2026-01-07T00:00:00Z"), at("2026-01-09T00:00:00Z"))
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("2026-01-07") && err.contains("2026-01-08"),
            "{err}"
        );
    }

    #[rstest]
    fn the_scores_read_as_report_sections() {
        let score = ExpectedScore {
            rows: 3,
            skill: Some(0.75),
            mean_error: Some(-0.25),
            slope: None,
            top_decile_gap: Some(0.5),
        };
        let trades = |count, expectancy| Trades {
            count,
            expectancy,
            win_rate: expectancy.map(|_| 0.5),
        };
        let earnings = Earnings {
            taken: trades(2, Some(0.5)),
            every: trades(3, Some(-0.25)),
            taken_stressed: trades(2, Some(0.45)),
            every_stressed: trades(3, Some(-0.3)),
        };
        let scores = Scores {
            long: None,
            short: Some(ModelScores {
                periods: vec![Period {
                    from: at("2026-01-07T00:00:00Z"),
                    to: at("2026-01-08T00:00:00Z"),
                    models: vec![at("2026-01-07T00:00:00Z")],
                    score,
                    earnings,
                }],
                score: ExpectedScore::default(),
                earnings: Earnings::default(),
            }),
            meta: Some(ModelScores {
                periods: vec![],
                score: ExpectedScore {
                    rows: 2,
                    skill: Some(0.25),
                    mean_error: Some(0.125),
                    slope: Some(1.5),
                    top_decile_gap: None,
                },
                earnings: Earnings::default(),
            }),
        };

        assert_eq!(
            scores.to_string(),
            "
Short model scores, out of sample, from 2026-01-07T00:00:00Z to 2026-01-08T00:00:00Z
  Model valid from: 2026-01-07T00:00:00Z
  Skill: 0.750000
  Mean R - E': -0.250000
  Calibration slope: n/a
  Top-decile gap: 0.500000
  Rows: 3
  Trades taken: 2
  Expectancy of the trades taken: 0.500000R
  Win rate of trades taken: 0.500000
  Every trade: 3
  Expectancy of every trade: -0.250000R
  Win rate, every trade: 0.500000
  Expectancy of the trades taken at 1.5x the commission: 0.450000R
  Expectancy of every trade at 1.5x the commission: -0.300000R

Short model scores, out of sample, over the span
  Skill: n/a
  Mean R - E': n/a
  Calibration slope: n/a
  Top-decile gap: n/a
  Rows: 0
  Trades taken: 0
  Expectancy of the trades taken: n/a
  Win rate of trades taken: n/a
  Every trade: 0
  Expectancy of every trade: n/a
  Win rate, every trade: n/a
  Expectancy of the trades taken at 1.5x the commission: n/a
  Expectancy of every trade at 1.5x the commission: n/a

Meta model scores, out of sample, over the span
  Skill: 0.250000
  Mean R - E': 0.125000
  Calibration slope: 1.500000
  Top-decile gap: n/a
  Rows: 2
  Trades taken: 0
  Expectancy of the trades taken: n/a
  Win rate of trades taken: n/a
  Every trade: 0
  Expectancy of every trade: n/a
  Win rate, every trade: n/a
  Expectancy of the trades taken at 1.5x the commission: n/a
  Expectancy of every trade at 1.5x the commission: n/a
"
        );
    }

    fn with_meta() -> ModelConfig {
        let mut config = config();
        config.meta = Some(
            MetaConfig::builder()
                .window(SignedDuration::from_hours(48))
                .calibration(SignedDuration::from_hours(6))
                .margin(R::new(0.3))
                .min_rows(1)
                .learner(LearnerConfig::Fake {})
                .build(),
        );
        config
    }

    #[rstest]
    fn the_long_the_short_then_the_meta_models_scores_follow_in_the_report() {
        let score = ExpectedScore::default();
        let scores = ModelScores {
            periods: vec![Period {
                from: at("2026-01-07T00:00:00Z"),
                to: at("2026-01-08T00:00:00Z"),
                models: vec![],
                score,
                earnings: Earnings::default(),
            }],
            score,
            earnings: Earnings::default(),
        };
        let scores = Scores {
            long: Some(scores.clone()),
            short: Some(scores.clone()),
            meta: Some(scores),
        };

        let text = scores.to_string();

        let at = |heading: &str| {
            text.find(&format!("\n{heading} scores, out of sample, over the span"))
                .unwrap_or_else(|| panic!("no {heading}: {text}"))
        };
        assert!(at("Long model") < at("Short model"), "{text}");
        assert!(at("Short model") < at("Meta model"), "{text}");
    }

    fn never() -> ModelConfig {
        ModelConfig {
            retrain: Retrain::Never,
            ..config()
        }
    }

    fn never_with_meta() -> ModelConfig {
        ModelConfig {
            retrain: Retrain::Never,
            ..with_meta()
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum Published {
        Sides(&'static str),
        WithMeta(&'static str),
    }

    fn publish_each(dir: &Path, config: &ModelConfig, published: &[Published]) {
        let fingerprint = dataset(dir, config).fingerprint().to_string();

        for &model_set in published {
            match model_set {
                Published::Sides(valid_from) => publish(dir, config, at(valid_from)),
                Published::WithMeta(valid_from) => {
                    publish_opposed(
                        dir,
                        config,
                        at(valid_from),
                        Some(meta_model(fingerprint.clone())),
                    );
                }
            }
        }
    }

    #[rstest]
    #[case::retrained_on_an_empty_store(config(), &[], "2026-01-07T00:00:00Z")]
    #[case::never_retrained_on_an_empty_store(never(), &[], "2026-01-07T12:00:00Z")]
    #[case::with_model_sets_after_its_start(
        config(),
        &[Published::Sides("2026-01-08T00:00:00Z")],
        "2026-01-07T00:00:00Z"
    )]
    #[case::with_a_meta_and_model_sets_after_its_start(
        with_meta(),
        &[Published::WithMeta("2026-01-08T00:00:00Z")],
        "2026-01-07T00:00:00Z"
    )]
    fn a_backtest_with_no_model_to_trade_on_at_its_start_is_untrained_at_its_first_train_time(
        #[case] config: ModelConfig,
        #[case] published: &[Published],
        #[case] first: &str,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk_of(dir.path(), &config);
        publish_each(dir.path(), &config, published);

        let untrained = walk
            .untrained(at("2026-01-07T12:00:00Z"), at("2026-01-09T00:00:00Z"))
            .unwrap();

        assert_eq!(untrained, Some(at(first)));
    }

    /// The 6th's model set was published at no retrain time of the span, and it decides until the
    /// next.
    #[rstest]
    #[case::retrained(config(), &[Published::Sides("2026-01-06T00:00:00Z")])]
    #[case::never_retrained(never(), &[Published::Sides("2026-01-06T00:00:00Z")])]
    #[case::with_a_meta_model(never_with_meta(), &[Published::WithMeta("2026-01-06T00:00:00Z")])]
    #[case::retrained_with_a_meta_model_from_its_first_retrain_time(
        with_meta(),
        &[Published::WithMeta("2026-01-07T00:00:00Z")]
    )]
    fn a_backtest_with_models_to_trade_on_at_its_start_is_not_untrained(
        #[case] config: ModelConfig,
        #[case] published: &[Published],
    ) {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk_of(dir.path(), &config);
        publish_each(dir.path(), &config, published);

        let untrained = walk
            .untrained(at("2026-01-07T12:00:00Z"), at("2026-01-09T00:00:00Z"))
            .unwrap();

        assert_eq!(untrained, None);
    }

    #[rstest]
    fn a_backtest_never_retrained_scores_the_span_as_one_period() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk_of(dir.path(), &never());
        build(dir.path());
        publish(dir.path(), &never(), at("2026-01-07T00:00:00Z"));
        let span = ("2026-01-07T00:00:00Z", "2026-01-09T00:00:00Z");

        let scores = walk.scores(at(span.0), at(span.1)).unwrap();

        let long = scores.long.unwrap();
        let [period] = &long.periods[..] else {
            panic!("{long:?}");
        };
        assert_eq!(
            (period.from, period.to, period.models.clone()),
            (
                at("2026-01-07T00:00:00Z"),
                at("2026-01-09T00:00:00Z"),
                vec![at("2026-01-07T00:00:00Z")]
            )
        );
        assert_eq!(period.score.rows, 5);
    }

    #[rstest]
    fn a_backtest_never_retrained_scores_a_model_beside_its_own_baseline() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk_of(dir.path(), &never());
        build(dir.path());
        publish(dir.path(), &never(), at("2026-01-07T00:00:00Z"));
        let span = ("2026-01-08T00:00:00Z", "2026-01-09T00:00:00Z");

        let scores = walk.scores(at(span.0), at(span.1)).unwrap();

        let long = scores.long.unwrap();
        let [period] = &long.periods[..] else {
            panic!("{long:?}");
        };
        let period = period.score;
        assert_eq!(period.rows, 2);
        close(period.skill, 1.0 - 0.65 * 9.0 / 8.0);
    }

    #[rstest]
    fn a_retrained_backtest_scores_an_earlier_model_in_use_beside_its_own_baseline() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk(dir.path());
        build(dir.path());
        publish(dir.path(), &config(), at("2026-01-07T00:00:00Z"));

        let scores = walk
            .scores(at("2026-01-07T00:00:00Z"), at("2026-01-09T00:00:00Z"))
            .unwrap();

        let long = scores.long.unwrap();
        let second = &long.periods[1];
        assert_eq!(second.models, [at("2026-01-07T00:00:00Z")]);
        let second = second.score;
        assert_eq!(second.rows, 2);
        close(second.skill, 1.0 - 0.65 * 9.0 / 8.0);
    }
}
