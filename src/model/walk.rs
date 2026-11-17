//! A backtest of a strategy trading on its models, as they are in the
//! store: what the backtest checks before its run, and the out-of-sample
//! scores it reports after. A backtest trains nothing: its models are those
//! the binary's `train` published (`train::Training::train_at`), at each
//! retrain time of a span or at one time, so that no backtest trains over
//! the range it judges.
//!
//! Before the run (`WalkForward::untrained`), a backtest whose store holds
//! no generation to trade on at its start (`ModelStore::trades_by`: the
//! latest valid by then, holding a meta model if the strategy has a meta,
//! as a live start checks) is refused: it would trade nothing from its
//! start, and names the retrain time there, the span's first train time
//! (`Schedule::times`: the last retrain time not after the start, or, never
//! retrained, the start). The run then trades on the store as it is: at
//! each decision, the latest generation valid then (`handle`), as live
//! does, so a retrain time the store lacks leaves the generation before it
//! deciding through its period.
//!
//! After the run (`WalkForward::scores`), each model is scored on each
//! decision's row of the dataset in the span with the generation the
//! strategy used at it: the latest valid then, swapped in as the handle
//! swaps it (`store::Swap`), a row whose generation fails its checks
//! decided by none, as the handle decides no trade there. Each side, its
//! calibrated expected R, `E'`, against its trade's realized R; the meta,
//! on the rows a side is picked on, of a generation holding a meta model,
//! its calibrated expected R against the R of the trade of the side picked.
//! Every model is out of sample on its rows: trained by its generation's
//! valid-from on rows settled by then. Beside each, on the very rows it
//! decided, its own baseline, as its manifest has it
//! (`store::ModelManifest::baseline`): the training mean of R on the rows
//! that model was fitted and calibrated on. A period whose retrain time the
//! store lacks, its train failed or never run, so scores the earlier
//! generation in use beside its baselines, a period of generations
//! published off the schedule each row beside the baseline of the model
//! that decided it, and a row decided with no generation, as before the
//! first, has no score. Each model's skill against those baselines, its
//! mean `R - E'`, calibration slope and top-decile gap (`score::ExpectedScore`) are
//! given per retrain period and over the span, on all its rows with a
//! generation, and what was earned there, in R (`holdout::Earnings`), beside
//! every trade of those rows, its own, at the commission and at 1.5 times
//! it. A side's trades taken are those picked for it on each row by the
//! decision itself (`deciding::DecidingModels::pick_among`), before any
//! meta: a row where both sides clear counts one trade, for the side
//! expected the most, a tie none, so with no meta the sides' trades add up
//! to the decisions returned. A meta's are the trades of the side picked its
//! margin takes (`holdout::MetaMargin`), out of sample. All this judges the
//! decision the handle returns, on the target's trade: no section adds the
//! sides up, as the backtest's report gives what the strategy actually
//! traded, which can differ whenever it filters its decisions.

use std::{collections::BTreeSet, fmt::Display};

use jiff::Timestamp;
use nautilus_core::UnixNanos;

use super::{
    blueprint::Stored,
    dataset::{self, Dataset, DatasetRow},
    deciding::{self, DecidingModels},
    features::meta_row,
    holdout::{Earnings, Example, MetaMargin, STRESS, in_r},
    schedule::{Models, Schedule},
    score::ExpectedScore,
    store::{Baselined, ModelStore},
};
use crate::strategy::risk::R;

/// The check before the run and the scores after it of one strategy's
/// backtests.
#[derive(Debug)]
pub struct WalkForward {
    dataset: Dataset,
    /// When the models are trained, and what each train does.
    schedule: Schedule,
    /// The strategy's models: each side traded, with its margin and its
    /// store, and the meta level, if any.
    models: ModelStore,
}

/// The out-of-sample scores of a backtest: of each model, the meta's on
/// the rows a side is picked on.
pub type Scores = Models<ModelScores>;

/// The out-of-sample scores of one model.
#[derive(Debug, Clone, PartialEq)]
pub struct ModelScores {
    /// Each retrain period of the span, in order.
    pub periods: Vec<Period>,
    /// The model's over the span, its rows with a model, beside the
    /// baselines', each row's its model's.
    pub score: ExpectedScore,
    /// What was earned over the span, as in a period.
    pub earnings: Earnings,
}

/// The scores of one retrain period.
#[derive(Debug, Clone, PartialEq)]
pub struct Period {
    /// The period's start: its retrain time, or the span's start.
    pub from: Timestamp,
    /// The period's end: the next retrain time, or the span's end.
    pub to: Timestamp,
    /// The valid-froms of the generations used in the period: one, unless
    /// a generation valid from within it was published off the schedule.
    pub models: Vec<Timestamp>,
    /// The model's, on the rows decided with one, beside the baselines' on
    /// the same rows: each row's that of the model that decided it.
    pub score: ExpectedScore,
    /// What was earned on the rows decided with one: the trades taken, a
    /// side's those picked for it, beside every trade of those rows.
    pub earnings: Earnings,
}

impl WalkForward {
    /// The backtests of the models of `model`, a strategy's model on the
    /// venue of its dataset runs, on its dataset and in its model store, on
    /// its schedule.
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

    /// The train time at the start of a backtest over `[start, end)` if
    /// the store holds no generation to trade on there (`ModelStore::trades_by`):
    /// the span's first train time (`Schedule::times`), whose train fills
    /// it; none if they hold one.
    ///
    /// # Errors
    ///
    /// Returns an error if the span is empty or a store cannot be read.
    pub fn untrained(&self, start: Timestamp, end: Timestamp) -> anyhow::Result<Option<Timestamp>> {
        let first = self.schedule.times(start, end)?[0];
        Ok((!self.models.trades_by(start)?).then_some(first))
    }

    /// The scores of a backtest over `[start, end)`, its dataset built over
    /// it: each side's model's, and the meta model's, if any, of the
    /// generation in use at each row (`judged`).
    ///
    /// # Errors
    ///
    /// Returns an error if the span is empty, the dataset lacks a day of
    /// it, a model fails to predict, or the dataset or the store cannot be
    /// read.
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

    /// What the generation in use at each of `rows`, in time order, says of
    /// each model's example of it, swapped in as the handle swaps it, a row
    /// whose generation fails its checks judged by none: each side's, on the
    /// side's trade; and the meta's, if the generation holds one, on the
    /// trade of the side picked, if any (`DecidingModels::pick_among`).
    ///
    /// # Errors
    ///
    /// Returns an error if the store cannot be listed, or a model fails to
    /// predict.
    fn judged(&self, rows: &[DatasetRow]) -> anyhow::Result<Models<Vec<Judged>>> {
        let listed = self.models.generations()?;
        let mut generations = self.models.swap();
        let margin = self
            .models
            .meta()
            .map(|level| MetaMargin(level.config().margin));
        let mut judged = Models {
            long: None,
            short: None,
            meta: margin.map(|_| vec![]),
        };

        for level in self.models.sides() {
            judged.set_side(level.side(), vec![]);
        }

        for row in rows {
            let Ok(Some((valid_from, generation))) =
                generations.at(Timestamp::from(row.ts_event), &listed)
            else {
                continue;
            };
            let sides = row.sides();
            let pair = generation.pair();
            let expected = pair.expect(&sides)?;
            let picked = DecidingModels::pick_among(pair, deciding::margins(&self.models), &sides)?;
            for level in self.models.sides() {
                let side = level.side();
                if let Some(judged) = judged.side_mut(side) {
                    judged.push(Judged {
                        valid_from,
                        example: Example::of_side(row, side),
                        expected: expected.of(side),
                        baseline: pair.side(side).baseline(),
                        taken: picked.is_some_and(|picked| picked.side == side),
                    });
                }
            }

            if let (Some(margin), Some(meta), Some(picked), Some(judged)) =
                (margin, generation.meta(), picked, judged.meta.as_mut())
            {
                let example = Example {
                    features: meta_row(&row.features, picked.side.order_side(), picked.expected),
                    ..Example::of_side(row, picked.side)
                };
                let expected = meta.predict(&example.features)?;
                judged.push(Judged {
                    valid_from,
                    expected,
                    baseline: meta.baseline(),
                    taken: R::finite(expected).is_some_and(|expected| margin.takes(expected)),
                    example,
                });
            }
        }
        Ok(judged)
    }
}

/// What the generation in use on a model's example says of it.
struct Judged {
    /// The generation's valid-from.
    valid_from: Timestamp,
    example: Example,
    /// Its calibrated expected R of the example's trade.
    expected: f64,
    /// Its baseline's (`Baselined`).
    baseline: f64,
    /// Whether the example's trade was taken.
    taken: bool,
}

/// The scores over `[start, end)`, retrained at `times`, of a model's
/// examples `judged`, in time order, each by the generation in use on it
/// and beside its own baseline: their expected R against the R each
/// example's trade realized.
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

/// The scores as text, in the sections of the backtest's report: of each
/// model, the long's, the short's, then the meta's, one per period, then
/// the span.
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

/// The sections of `scores`, each headed by `model`, the model's name.
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

/// The lines of `score`: its skill, mean `R - E'`, calibration slope and
/// top-decile gap.
fn lines(f: &mut std::fmt::Formatter<'_>, score: ExpectedScore) -> std::fmt::Result {
    let value = |value: Option<f64>| value.map_or_else(|| "n/a".to_string(), |v| format!("{v:.6}"));
    writeln!(f, "  Skill: {}", value(score.skill))?;
    writeln!(f, "  Mean R - E': {}", value(score.mean_error))?;
    writeln!(f, "  Calibration slope: {}", value(score.slope))?;
    writeln!(f, "  Top-decile gap: {}", value(score.top_decile_gap))?;
    writeln!(f, "  Rows: {}", score.rows)
}

/// The lines of `earnings`: the trades taken, a side's those picked for it,
/// every trade of the rows the model decided, and both at `STRESS` times the
/// commission, the expectancy in R.
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
            features::{FeatureSet, meta_names, side_names, testing::last_mid},
            holdout::Trades,
            learner::{LearnerConfig, TrainingSet},
            schedule::Retrain,
            store::{ModelManifest, StoredGeneration, StoredModel},
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

    /// The fake learner on hourly decisions, learning the R of each side of a
    /// time-only target of 1 hour, its unit measured in Wilder's ATR of 14
    /// hourly bars, trading each side from an expected 0, retrained every day
    /// on the day before, its last 6 hours held out.
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

    /// What the strategy reads: the quotes of gold.
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

    /// The venue of the dataset runs: a commission rate of 30 per million,
    /// 10 million USD.
    fn venue() -> DatasetVenue {
        DatasetVenue {
            commission: Commission::Rate(Decimal::new(3, 5)),
            balance: Money::from("10000000 USD"),
            latency: None,
        }
    }

    /// The model of `config`.
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

    /// The walk of `config` in `dir`, its catalog and store side by side.
    fn walk_of(dir: &Path, config: &ModelConfig) -> WalkForward {
        std::fs::create_dir_all(dir.join("catalog")).unwrap();
        blueprint(config)
            .walk_forward(&dir.join("catalog"), &dir.join("models"))
            .unwrap()
    }

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    /// The row of the decision at `time` on `feature`, its long gaining 1
    /// USD if `wins`, else losing 1, the short the reverse, each net of 0.1
    /// USD of commission and of a unit of 1, settled an hour later; each
    /// side's payoff features a time-only target's, its entry cost, which no
    /// model of the tests reads.
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

    /// The 6th to the 8th of January, each the long's R of 1 or -1 and the
    /// short's the reverse. Of the 6th, before 18:00, two wins in three, the
    /// long's mean R 1/3, then a loss. Of the 7th: one win in three, -1/3,
    /// scored on the 7th as well. Scored on the 8th: two wins.
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

    /// The dataset of a walk in `dir` as `config` says, in its catalog.
    fn dataset(dir: &Path, config: &ModelConfig) -> Dataset {
        blueprint(config).dataset(&dir.join("catalog")).unwrap()
    }

    /// Builds the dataset of `walk(dir)` from the 6th to the 8th.
    fn build(dir: &Path) {
        dataset(dir, &config())
            .write(
                &rows(),
                at("2026-01-06T00:00:00Z"),
                at("2026-01-09T01:00:00Z"),
            )
            .unwrap();
    }

    /// The calibration of the meta models of the tests, its level line `0.1
    /// + 0.5 E`.
    const CALIBRATION: Line = Line {
        alpha: 0.1,
        beta: 0.5,
    };

    /// A fake model of `side` of `config`, of `fingerprint`, on the
    /// features then the target's payoff names: its R the row's first
    /// feature, its baseline `baseline`.
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
                probes: vec![],
            },
            model: model.save().unwrap(),
        }
    }

    /// The models of a walk in `dir` as `config` says.
    fn models(dir: &Path, config: &ModelConfig) -> ModelStore {
        blueprint(config).model_store(&dir.join("models")).unwrap()
    }

    /// Publishes for a walk in `dir` as `config` says the generation valid
    /// from `valid_from` of `sides`, each side traded, their pair's
    /// calibration leaving them as they are (`unchanged`), and `meta`, if
    /// any.
    fn publish_generation(
        dir: &Path,
        config: &ModelConfig,
        valid_from: Timestamp,
        sides: Vec<StoredModel>,
        meta: Option<StoredModel>,
    ) {
        models(dir, config)
            .publish(&StoredGeneration {
                valid_from,
                sides,
                calibration: unchanged(config),
                meta,
            })
            .unwrap();
    }

    /// Publishes for a walk in `dir` as `config` says a generation valid
    /// from `valid_from`, a fake for each side, with the baseline of
    /// `trained_baseline`, and no meta.
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
        publish_generation(dir, config, valid_from, sides, None);
    }

    /// The baseline the `side` model published at `valid_from` carries, a
    /// training mean of the side's R on `rows`: at the 7th the long's two
    /// wins and a loss of the 6th before 18:00, 1/3, at the 8th its win and
    /// two losses of the 7th, -1/3, the short's the reverse, winning where
    /// the long loses; nought at any other time, whose models' baselines no
    /// test reads.
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

    /// Builds the dataset of `walk(dir)` and publishes the models of
    /// `config` a train at each retrain time of the 7th and the 8th would.
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

    /// Worked by hand, the long's, each model beside its own baseline, the
    /// training mean of R its train fitted (`trained_baseline`), on the rows
    /// it decided, both sides' fakes expecting the row's feature `p` alike.
    /// The 7th, `p` of 0.8, 0.3 and 0.6 against R of 1, -1 and -1: squared
    /// errors 0.04 + 1.69 + 2.56 = 4.29, the baseline 1/3's 4/9 + 16/9 + 16/9
    /// = 4, a skill of 1 - 4.29 / 4; errors of 0.2, -1.3 and -1.6, a mean of
    /// -0.9; deviations from the means 17/30 and -1/3 of 7, -8 and 1 thirtieths
    /// against 4/3, -2/3 and -2/3, a slope of (42/90) / (114/900) = 70/19; the
    /// top decile the row at 0.8, a gap of 0.2. The 8th, 0.9 and 0.2 against
    /// two wins: 0.01 + 0.64 = 0.65 against the baseline -1/3's 32/9, a skill
    /// of 1 - 0.65 x 9/32; a mean error of 0.45; no spread of R, a slope of
    /// nought; the top decile at 0.9, 0.1. The span, the five rows beside
    /// each one's baseline: 4.94 against 68/9; a mean error of -1.8/5;
    /// deviations from 0.56 and 0.2, a slope of 0.44 / 0.372. The short wins
    /// where the long loses: on the 7th its errors 1.8, 0.7 and 0.4 against
    /// the baseline -1/3's 4, a skill of 1 - 3.89 / 4, its slope -70/19.
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

    /// The scores over the 7th and the 8th of a walk in `dir` trading `side`
    /// alone, from an expected 0.5R, a train publishing its model at each
    /// retrain time.
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

    /// Worked by hand, with no side to pick between: each model expects its
    /// row's feature in R, its unit 1 USD, so a side trades from a feature of
    /// 0.5. The 7th, the long takes the win at 0.8 and the loss at 0.6,
    /// nought per trade, every trade (with the loss at 0.3) -1/3; the 8th, the
    /// win at 0.9, every trade (with the win at 0.2) 1; the span, two wins and
    /// a loss taken, 1/3, of five trades, 1/5. The short alone, the reverse
    /// trades of the same decisions: the 7th, a loss at 0.8 and a win at 0.6,
    /// nought, every trade 1/3; the 8th, the loss at 0.9, every trade (with
    /// the loss at 0.2) -1; the span, a win and two losses taken, -1/3, of
    /// five trades, -1/5. At 1.5 times the commission of 0.1 each trade makes
    /// 0.05 less. Each count and expectancy in order: the 7th's trades taken
    /// and every trade, stressed and not, the 8th's, the span's.
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

    /// Publishes for a walk in `dir` as `config` says the generation valid
    /// from `valid_from` of opposed sides, each with the baseline a train
    /// then fits (`trained_baseline`): the long's a fake, expecting its row's
    /// feature in R, the short's a baseline expecting 0.5R whatever the row,
    /// their pair's calibration leaving each as it is (`unchanged`), up to
    /// rounding; and the meta model `meta`, if any.
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
                    let width = opposed.manifest.feature_names.len();
                    let set = TrainingSet::new(width, vec![0.0; width], vec![0.5]).unwrap();
                    opposed.manifest.learner = LearnerConfig::Baseline {};
                    opposed.model = LearnerConfig::Baseline {}
                        .fit(&set)
                        .unwrap()
                        .save()
                        .unwrap();
                }
                opposed
            })
            .collect();
        publish_generation(dir, config, valid_from, sides, meta);
    }

    /// The scores over the 7th and the 8th of `walk(dir)`, with `rows` and each
    /// side's opposed models (`publish_opposed`) valid from the 7th and the 8th,
    /// each side clearing its margin of nought on every row, and the one
    /// expected the more picked, the long's its feature, the short's 0.5:
    ///
    /// - 04:00 of the 7th, 0.8: the long, a win.
    /// - 08:00 of the 7th, 0.3: the short, a win.
    /// - 12:00 of the 7th, 0.6: the long, a loss.
    /// - 06:00 of the 8th, 0.9: the long, a win.
    /// - 10:00 of the 8th, 0.2: the short, a loss.
    fn opposed_scores(dir: &Path) -> Scores {
        let walk = walk(dir);
        build(dir);
        for valid_from in [at("2026-01-07T00:00:00Z"), at("2026-01-08T00:00:00Z")] {
            publish_opposed(dir, &config(), valid_from, None);
        }
        walk.scores(at("2026-01-07T00:00:00Z"), at("2026-01-09T00:00:00Z"))
            .unwrap()
    }

    /// `trades` are `count` trades of expectancy `expectancy`, in R.
    fn trades_are(trades: Trades, count: usize, expectancy: f64) {
        assert_eq!(trades.count, count, "{trades:?}");
        close(trades.expectancy, expectancy);
    }

    /// On the rows of `opposed_scores`: the 7th, the long takes the win at 0.8
    /// and the loss at 0.6, nought per trade, the short the win at 0.3, 1,
    /// though each side clears on one more; the 8th, the long the win at 0.9,
    /// the short the loss at 0.2. The span, the long's two wins and a loss, 1/3,
    /// the short's win and loss, nought: five trades of five rows.
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

    /// At 1.5 times the commission of 0.1, each trade picked on the rows of
    /// `opposed_scores` makes 0.05 less: the long's on the 7th, a win and a
    /// loss, -0.05, the short's, a win, 0.95.
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

    /// On the rows of `opposed_scores`, the long's own trades: on the 7th a
    /// win and two losses, -1/3; on the 8th two wins, 1; the span, 1/5. The
    /// short's, the reverse: 1/3, -1 and -1/5. At 1.5 times the commission
    /// each makes 0.05 less.
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

    /// Worked as in
    /// `each_period_scores_its_model_beside_that_models_own_baseline_on_the_same_rows`:
    /// on the 7th's rows of `opposed_scores` the long's skill is that of its
    /// feature against its R, 1 - 4.29 / 4, and the short's that of 0.5 against
    /// R of -1, 1 and 1 beside its baseline -1/3, 2.25 + 0.25 + 0.25 against
    /// 4, 1 - 2.75 / 4, its expectation never varying and so of no slope: each
    /// on all three rows, though two are picked for the long and one for the
    /// short.
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

    /// The models of both sides alike: on every row they expect the same, so
    /// tie, whether or not they clear their margins.
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

    /// `config`, on a target with exits on AUD/USD.
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

    /// The row of `row`, a win of the long at 0.8, each side's stop worth
    /// what `(long, short)` say, its payoff features a bracket's.
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

    /// The models of both sides alike, expecting the same R, tie on every
    /// row whatever their stops are worth: at 04:00 the short's is worth
    /// twice the long's, at 08:00 the long's twice the short's, and neither
    /// side is picked on either row.
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

    /// A fake meta model of `fingerprint`, on the meta's features: its R
    /// that of the row's first feature calibrated by `CALIBRATION`, its
    /// baseline 0.5.
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

    /// The scores over the 7th and the 8th of a walk as `with_meta` says,
    /// with `rows`, generations of opposed sides (`publish_opposed`) and a
    /// meta model (`meta_model`) valid from the 7th and the 8th, the meta taking a trade
    /// from an expected 0.3 USD. The sides pick as in `opposed_scores`.
    fn opposed_meta_scores(dir: &Path) -> Scores {
        opposed_meta_scores_of(dir, &with_meta(), &rows())
    }

    /// The scores of `opposed_meta_scores`, of a walk as `config` says on
    /// `rows`.
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

    /// Worked by hand: `rows` with every unit 2 USD, the sides picking as on
    /// units of 1 USD, and the meta expecting the same R of each trade,
    /// `0.1 + 0.5p`: 0.5, 0.25, 0.4, 0.55 and 0.2, twice that in USD. Its
    /// margin of 0.3R takes the trades at 0.8, 0.6 and 0.9, as on units of 1
    /// USD, not those at 0.3 and 0.2, though they expect 0.5 and 0.4 USD. The
    /// trades taken earn 1/3 USD a trade, as on units of 1 USD, beside every
    /// trade's 0.2: over units of 2, 1/6R and 0.1R.
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

    /// Worked by hand. On the five rows the sides pick on, the trade of the
    /// side picked, each of a unit of 1 USD, the meta expecting `0.1 + 0.5p`
    /// R of the feature `p`: 0.5, 0.25 and 0.4 on the 7th, 0.55 and 0.2 on
    /// the 8th, so its margin of 0.3R takes those at 0.8, 0.6 and 0.9,
    /// not at 0.3 and 0.2. The 7th: a win, the short's win and a loss, 1/3,
    /// of which it takes the first and the last, nought. The 8th: a win and
    /// the short's loss, nought, of which it takes the win, 1. The span, 1/3
    /// of three beside 1/5 of five.
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

    /// Worked by hand, the meta's expected R (`0.1 + 0.5p`) of the five rows
    /// against their R, 1, 1, -1, 1 and -1, beside its baseline of 0.5: errors
    /// 0.5, 0.75, -1.4, 0.45 and -1.2, squared 4.415, the baseline's 5.25, a
    /// skill of 1 - 4.415 / 5.25; a mean R - E' of -0.9 / 5; deviations from
    /// the means 0.38 and 0.2 of 0.12, -0.13, 0.02, 0.17 and -0.18 against 0.8,
    /// 0.8, -1.2, 0.8 and -1.2, a slope of 0.32 / 0.093; the top decile, the
    /// row expected 0.55, a gap of 0.45.
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

    /// With a meta filtering the side picked, each side still counts every
    /// trade picked for it, as in `opposed_scores`: the meta's section says
    /// which of them it took.
    #[rstest]
    fn a_sides_trades_picked_are_counted_before_any_meta() {
        let dir = tempfile::tempdir().unwrap();

        let scores = opposed_meta_scores(dir.path());

        trades_are(scores.long.unwrap().earnings.taken, 3, 1.0 / 3.0);
        trades_are(scores.short.unwrap().earnings.taken, 2, 0.0);
    }

    /// The generation of the 8th has a long of another fingerprint, refused
    /// as it loads: as the handle decides no trade on it, nor on the
    /// generation of the 7th, no row of the 8th is scored, of either side;
    /// the 7th's are.
    #[rstest]
    fn rows_of_a_generation_failing_its_checks_are_not_scored() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk(dir.path());
        build(dir.path());
        let eighth = at("2026-01-08T00:00:00Z");
        publish(dir.path(), &config(), at("2026-01-07T00:00:00Z"));
        let fingerprint = dataset(dir.path(), &config()).fingerprint().to_string();
        publish_generation(
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

    /// A generation's meta is scored beside its own sides alone: that of the
    /// 7th holds a meta model, that of the 8th none, its meta having waited,
    /// so the meta scores the 7th's three rows and none of the 8th, never
    /// beside the 8th's sides, which still pick their two.
    #[rstest]
    fn a_meta_is_scored_beside_its_own_generations_sides_alone() {
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
            Some(meta_model(fingerprint)),
        );
        publish_opposed(dir.path(), &config, at("2026-01-08T00:00:00Z"), None);

        let scores = walk
            .scores(at("2026-01-07T00:00:00Z"), at("2026-01-09T00:00:00Z"))
            .unwrap();

        let meta = scores.meta.unwrap();
        assert_eq!(meta.periods[0].score.rows, 3, "{meta:?}");
        assert_eq!(meta.periods[1].score.rows, 0, "{meta:?}");
        assert!(meta.periods[1].models.is_empty(), "{meta:?}");
        trades_are(scores.long.unwrap().periods[1].earnings.taken, 1, 1.0);
    }

    /// A baseline is a model's: the 7th's three rows, decided with none,
    /// have no baseline either, and over the span the baseline has the
    /// model's two rows of the 8th.
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

    /// A generation published off the schedule, valid from 12:00 of the 7th, its
    /// long's baseline 1/4: the period of the 7th uses the 7th's long, its
    /// baseline 1/3, at 04:00 and 08:00, a win and a loss, and the later one
    /// at 12:00, a loss, each row scored beside the baseline of the model
    /// that decided it: the long's squared errors 4.29 against 4/9 + 16/9 +
    /// 25/16, a skill of 1 - 4.29 / (20/9 + 25/16), where the 7th's baseline
    /// throughout would make 1 - 4.29 / 4.
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
        publish_generation(
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

    /// No dataset built: the refusal lists the days of the span it lacks,
    /// the 7th and the 8th.
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

    /// `config`, with a meta model on a window of 48 hours.
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

    /// `config`, never retrained.
    fn never() -> ModelConfig {
        ModelConfig {
            retrain: Retrain::Never,
            ..config()
        }
    }

    /// `with_meta`, never retrained.
    fn never_with_meta() -> ModelConfig {
        ModelConfig {
            retrain: Retrain::Never,
            ..with_meta()
        }
    }

    /// A generation published to the store of a walk in `dir` as `config`
    /// says: of the sides alone, or with a meta model.
    #[derive(Debug, Clone, Copy)]
    enum Published {
        Sides(&'static str),
        WithMeta(&'static str),
    }

    /// Publishes `published` to the store of a walk in `dir` as `config`
    /// says.
    fn publish_each(dir: &Path, config: &ModelConfig, published: &[Published]) {
        let fingerprint = dataset(dir, config).fingerprint().to_string();

        for &generation in published {
            match generation {
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

    /// A backtest from 12:00 of the 7th to the 9th whose store holds no
    /// generation to trade on at its start is untrained there: at the span's
    /// first train time, retrained daily the midnight of the 7th, never
    /// retrained its start. A store holding a generation whose meta waited,
    /// with a meta level, holds nothing to trade on either, even beside an
    /// earlier generation holding one; nor one whose generations come after
    /// the start.
    #[rstest]
    #[case::retrained_on_an_empty_store(config(), &[], "2026-01-07T00:00:00Z")]
    #[case::never_retrained_on_an_empty_store(never(), &[], "2026-01-07T12:00:00Z")]
    #[case::with_generations_after_its_start(
        config(),
        &[Published::Sides("2026-01-08T00:00:00Z")],
        "2026-01-07T00:00:00Z"
    )]
    #[case::with_a_generation_whose_meta_waited(
        with_meta(),
        &[Published::Sides("2026-01-06T00:00:00Z")],
        "2026-01-07T00:00:00Z"
    )]
    #[case::with_its_latest_generations_meta_waited(
        with_meta(),
        &[Published::WithMeta("2026-01-06T00:00:00Z"), Published::Sides("2026-01-07T06:00:00Z")],
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

    /// A backtest from 12:00 of the 7th whose store holds a generation to
    /// trade on by then trains nothing: the generation of the 6th, published
    /// at no retrain time of the span, decides until the next, retrained or
    /// not; with a meta level, holding a meta model.
    #[rstest]
    #[case::retrained(config(), &[Published::Sides("2026-01-06T00:00:00Z")])]
    #[case::never_retrained(never(), &[Published::Sides("2026-01-06T00:00:00Z")])]
    #[case::with_a_meta_model(never_with_meta(), &[Published::WithMeta("2026-01-06T00:00:00Z")])]
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

    /// Never retrained, the models trained at the start, the 7th, are used
    /// over the whole span: one period, the 7th to the 9th, of the five
    /// rows decided in it.
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

    /// Never retrained, the long's model of the 7th in use over a backtest
    /// from the 8th is scored beside its own baseline, the one its manifest
    /// carries, 1/3 (`trained_baseline`): on the 8th's two wins at
    /// 0.9 and 0.2, squared errors 0.65 against 8/9, a skill of 1 - 0.65 x
    /// 9/8, where the 8th's baseline, -1/3, would make 1 - 0.65 x 9/32.
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

    /// Retrained, the models of the 7th are in use over both periods, the
    /// store holding none of the 8th: on the 8th the long's model of the 7th
    /// is scored beside its own baseline, 1/3 (`trained_baseline`), not the
    /// training mean a train of the 8th would have fitted, -1/3: on the 8th's
    /// two wins, a skill of 1 - 0.65 x 9/8.
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
