//! A backtest of a strategy trading on its models, retrained on its
//! schedule as live would be: what the backtest does before its run, and
//! the out-of-sample scores it reports after.
//!
//! Before the run (`WalkForward::prepare`), the dataset is built over the
//! span, from where the replay of the plan of the first train starts
//! (`Plan::replay_from`: the first row a fit reads), or from that train's
//! time if it fits no model, to the end of the span's last day
//! (`Dataset::build`), so that the last retrain period has targets to
//! score: a backtest whose models are all in the store builds from its
//! first train time, its start, or, retrained, the last retrain time not
//! after it. Then the models are trained at every train time of the span
//! (`Schedule::times`: each retrain time, or, never retrained, the start
//! alone) whose plan fits a model (`Schedule::plan`), each planned once
//! the trains before it published. Never retrained, a backtest whose
//! sides or meta then have no model in use at its start is refused: it
//! would trade nothing (`Schedule::ensure_trades`). Each build and train
//! is handed in by the caller, the binary's in its own process, one after
//! another before the run, each dataset run a Nautilus node disposed
//! before the next; a train finds the days it reads from there
//! built, and a later train whose window reaches before them replays the
//! days missing itself (`Dataset::settled`), as any train does. A model in
//! the store is reused, so a second backtest of the same span builds and
//! trains nothing. A train that kept a model's current one published none,
//! so the next backtest trains at that time again.
//! The run then trades on the stores as they are: at each decision, the
//! latest pair of the sides valid then (`handle`).
//!
//! After the run (`WalkForward::scores`), each side is scored on each
//! decision's row of the dataset in the span, its calibrated expected R,
//! `E'`, against its trade's realised R, with the pair the strategy used at
//! it: the latest valid then, a pair with a model failing its checks refused
//! and the current one kept, as the handle does. Every model is out of
//! sample on its rows: trained at its valid-from on rows settled by then.
//! Beside each, on the very rows it decided, its own baseline, as its
//! manifest has it (`store::ModelManifest::baseline`): the training mean of
//! R on the rows that model was fitted and calibrated on, at its
//! valid-from. A period whose train was refused so scores the earlier model
//! in use beside that model's baseline, a period of models published off
//! the schedule each row beside the baseline of the model that decided it,
//! and a row decided with no pair, as before the sides' first, has no score.
//! Each side's skill against those baselines, mean `R - E'`, calibration
//! slope and top-decile gap (`score::ExpectedScore`) are given per retrain
//! period and over the span, on all the side's rows with a pair, and what was
//! earned there, in R (`holdout::Earnings`), beside every trade of those rows,
//! its own, at the commission and at 1.5 times it; and the same of the meta, its
//! calibrated expected R against the R of the trade of the side picked, on
//! the rows a side is picked on with a meta model. A side's trades taken are
//! those picked for it on each row by the decision itself
//! (`deciding::DecidingModels::pick`), the picks a train's gate reads (`train`),
//! before any meta: a row where both sides clear counts one trade, for the side
//! expected the most, a tie none, so with no meta the sides' trades add up to
//! the decisions returned. A meta's are the trades of the side picked its
//! margin takes (`holdout::MetaMargin`), the figures its holdout is judged on,
//! out of sample. All this judges the decision the handle returns, on the target's
//! trade: no section adds the sides up, as the backtest's report gives what the
//! strategy actually traded, which can differ whenever it filters its decisions.

use std::{collections::BTreeSet, fmt};

use anyhow::ensure;
use jiff::Timestamp;
use nautilus_core::UnixNanos;

use super::{
    blueprint::OnVenue,
    dataset::{self, Dataset, DatasetRow},
    deciding::DecidingModels,
    holdout::{Earnings, Example, MetaMargin, STRESS, in_r},
    meta,
    schedule::{Models, Schedule},
    score::ExpectedScore,
    store::{Baselined, ModelSet, Swap},
    targets::Side,
};

/// The retrains and scores of one strategy's backtests.
#[derive(Debug)]
pub struct WalkForward {
    dataset: Dataset,
    /// When the models are trained, and what each train does.
    schedule: Schedule,
    /// The strategy's models: each side traded, with its margin and its
    /// store, and the meta level, if any.
    models: ModelSet,
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
    /// The valid-froms of the models used in the period: one, unless a
    /// model valid from within it was published off the schedule.
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
    /// venue of its dataset runs, on its dataset and in its model set, on
    /// its schedule.
    pub(super) fn new(model: OnVenue<'_>) -> Self {
        let OnVenue {
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

    /// Prepares a backtest over `[start, end)`: if the dataset lacks a day
    /// from where the replay of the plan of the span's first train time
    /// starts (`Plan::replay_from`), or that time if it fits no model, to
    /// the span's end, `build` builds it over that span (`Dataset::build`
    /// of its days); then `train` trains at each train time of the span
    /// (`Schedule::times`) whose plan fits a model (`Schedule::plan`,
    /// `Training::train_at`), in time order, each planned after the
    /// trains before it: a pair of the sides they publish gives the meta its
    /// rows from then on. A backtest that could then trade nothing is
    /// refused (`Schedule::ensure_trades`).
    ///
    /// # Errors
    ///
    /// Returns an error if the span is empty, the dataset or a store
    /// cannot be read, as `build` and `train` do, or, never retrained, if
    /// the sides or the meta have no model in use at `start`.
    pub fn prepare(
        &self,
        start: Timestamp,
        end: Timestamp,
        build: impl FnOnce(Timestamp, Timestamp) -> anyhow::Result<()>,
        mut train: impl FnMut(Timestamp) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let times = self.schedule.times(start, end)?;
        let from = self
            .schedule
            .plan(times[0], &self.models.valid_froms()?)
            .replay_from()
            .unwrap_or(times[0]);
        let (first, last) = dataset::days(from, end);
        if !self.dataset.missing(first, last)?.is_empty() {
            build(from, end)?;
        }
        for at in times {
            if self.schedule.plan(at, &self.models.valid_froms()?).fits() {
                train(at)?;
            }
        }
        self.schedule
            .ensure_trades(start, &self.models.valid_froms()?)
    }

    /// The scores of a backtest over `[start, end)`, prepared (`prepare`):
    /// each side's model's, and the meta model's, if any.
    ///
    /// # Errors
    ///
    /// Returns an error if the span is empty, the dataset lacks a day of
    /// it, a model fails to predict, or the dataset or a store cannot be read.
    pub fn scores(&self, start: Timestamp, end: Timestamp) -> anyhow::Result<Scores> {
        let times = self.schedule.times(start, end)?;
        let (first, last) = dataset::days(start, end);
        let missing = self.dataset.missing(first, last)?;
        ensure!(
            missing.is_empty(),
            "the dataset lacks the days {missing:?} of the backtest: build it first"
        );
        let mut rows = self.dataset.read(start, end)?;
        rows.retain(|row| row.ts_event < UnixNanos::from(end));
        let listed = self.models.valid_froms()?;
        let picked = self.picked(&rows)?;
        let mut scores = Scores {
            long: None,
            short: None,
            meta: None,
        };
        let pairs = listed.pairs();
        for level in self.models.sides() {
            let side = level.side();
            let examples: Vec<Example> =
                rows.iter().map(|row| Example::of_side(row, side)).collect();
            let side_scores = score(
                &times,
                (start, end),
                &examples,
                &pairs,
                self.models.pairs(),
                |pair, row, _| {
                    Ok(Judged {
                        expected: pair.expect(&rows[row].sides())?.of(side),
                        baseline: pair.side(side).baseline(),
                        taken: picked[row] == Some(side),
                    })
                },
            )?;
            match side {
                Side::Long => scores.long = Some(side_scores),
                Side::Short => scores.short = Some(side_scores),
            }
        }
        if let (Some(level), Some(valid_froms)) = (self.models.meta(), &listed.meta) {
            let margin = MetaMargin(level.config().margin);
            let examples = meta::examples(&self.models, &rows.iter().collect::<Vec<_>>())?;
            scores.meta = Some(score(
                &times,
                (start, end),
                &examples,
                valid_froms,
                level.swap(),
                |model, _, example| {
                    let expected = model.predict(&example.features)?;
                    Ok(Judged {
                        expected,
                        baseline: model.baseline(),
                        taken: margin.takes(expected),
                    })
                },
            )?);
        }
        Ok(scores)
    }

    /// The side picked on each of `rows`, in time order, by the decision
    /// itself, before any meta: on each side's row, among the sides of the
    /// pair in use then (`DecidingModels::pick`).
    fn picked(&self, rows: &[DatasetRow]) -> anyhow::Result<Vec<Option<Side>>> {
        let mut models = DecidingModels::new(&self.models)?;
        rows.iter()
            .map(|row| {
                let picked = models.pick(Timestamp::from(row.ts_event), &row.sides())?;
                Ok(picked.map(|picked| picked.side))
            })
            .collect()
    }
}

/// What the model in use on an example says of it.
struct Judged {
    /// Its calibrated expected R of the example's trade.
    expected: f64,
    /// Its baseline's (`Baselined`).
    baseline: f64,
    /// Whether the example's trade was taken.
    taken: bool,
}

/// The scores over `[start, end)`, retrained at `times`, of the models
/// `models` swaps in on `examples`, in time order, among those valid from
/// `valid_froms`, each beside its own baseline on the rows it decided:
/// their expected R against the R each example's trade realised. `judge`
/// gives what the model in use on an example, at its position in
/// `examples`, says of it.
fn score<M>(
    times: &[Timestamp],
    (start, end): (Timestamp, Timestamp),
    examples: &[Example],
    valid_froms: &[Timestamp],
    mut models: Swap<M>,
    judge: impl Fn(&M, usize, &Example) -> anyhow::Result<Judged>,
) -> anyhow::Result<ModelScores> {
    let (mut model_rows, mut model_trades) = (vec![], vec![]);
    let mut periods = vec![];
    for (i, &at) in times.iter().enumerate() {
        let (from, to) = (at.max(start), times.get(i + 1).copied().unwrap_or(end));
        let (from_ns, to_ns) = (UnixNanos::from(from), UnixNanos::from(to));
        let (mut period_models, mut judged, mut trades) = (BTreeSet::new(), vec![], vec![]);
        for (row, example) in examples
            .iter()
            .enumerate()
            .filter(|(_, example)| from_ns <= example.ts_event && example.ts_event < to_ns)
        {
            if let Some((valid_from, model)) =
                models.at(Timestamp::from(example.ts_event), valid_froms)
            {
                period_models.insert(valid_from);
                let Judged {
                    expected,
                    baseline,
                    taken,
                } = judge(model, row, example)?;
                judged.push((expected, example.realised(), baseline));
                trades.push((taken, example.trade));
            }
        }
        periods.push(Period {
            from,
            to,
            models: period_models.into_iter().collect(),
            score: ExpectedScore::of(&judged),
            earnings: Earnings::of(&trades),
        });
        model_rows.extend(judged);
        model_trades.extend(trades);
    }
    Ok(ModelScores {
        periods,
        score: ExpectedScore::of(&model_rows),
        earnings: Earnings::of(&model_trades),
    })
}

/// The scores as text, in the sections of the backtest's report: of each
/// model, the long's, the short's, then the meta's, one per period, then
/// the span.
impl fmt::Display for Scores {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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
fn sections(f: &mut fmt::Formatter<'_>, model: &str, scores: &ModelScores) -> fmt::Result {
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
fn lines(f: &mut fmt::Formatter<'_>, score: ExpectedScore) -> fmt::Result {
    let value = |value: Option<f64>| value.map_or_else(|| "n/a".to_string(), |v| format!("{v:.6}"));
    writeln!(f, "  Skill: {}", value(score.skill))?;
    writeln!(f, "  Mean R - E': {}", value(score.mean_error))?;
    writeln!(f, "  Calibration slope: {}", value(score.slope))?;
    writeln!(f, "  Top-decile gap: {}", value(score.top_decile_gap))?;
    writeln!(f, "  Rows: {}", score.rows)
}

/// The lines of `earnings`: the trades taken, a side's those picked for it,
/// every trade of the rows the model decided, and both at `STRESS` times the
/// commission, the net P&L per trade in R.
fn earned(f: &mut fmt::Formatter<'_>, earnings: &Earnings) -> fmt::Result {
    let value = |value: Option<f64>| value.map_or_else(|| "n/a".to_string(), |v| format!("{v:.6}"));
    let Earnings {
        taken,
        every,
        taken_stressed,
        every_stressed,
    } = earnings;
    writeln!(f, "  Trades taken: {}", taken.count)?;
    writeln!(f, "  Net P&L per trade taken: {}", in_r(taken.expectancy))?;
    writeln!(f, "  Win rate of trades taken: {}", value(taken.win_rate))?;
    writeln!(f, "  Every trade: {}", every.count)?;
    writeln!(
        f,
        "  Net P&L per trade, every trade: {}",
        in_r(every.expectancy)
    )?;
    writeln!(f, "  Win rate, every trade: {}", value(every.win_rate))?;
    writeln!(
        f,
        "  Net P&L per trade taken at {STRESS}x the commission: {}",
        in_r(taken_stressed.expectancy)
    )?;
    writeln!(
        f,
        "  Net P&L per trade, every trade, at {STRESS}x the commission: {}",
        in_r(every_stressed.expectancy)
    )
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, path::Path, rc::Rc};

    use jiff::SignedDuration;
    use nautilus_model::{
        identifiers::{InstrumentId, StrategyId},
        types::{Money, Quantity},
    };
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
            deciding::testing::{HOLDOUT, unchanged},
            features::{FeatureSet, meta_names, side_names, testing::last_mid},
            holdout::Trades,
            learner::{LearnerConfig, TrainingSet},
            schedule::Retrain,
            store::{ModelCalibration, ModelManifest, ModelStore, StoredModel},
            targets::{
                BracketSpec, Distance, Exit, Outcome, Outcomes, PerSide, Target, TargetConfig,
                TargetRow,
            },
        },
        strategy::{DataRequirements, r::R},
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
                    .quantity(Quantity::from(1))
                    .spec(
                        Target::builder()
                            .limit(SignedDuration::from_hours(1))
                            .atr(crate::model::targets::DEFAULT_ATR)
                            .build(),
                    )
                    .build(),
            )
            .retrain(SignedDuration::from_hours(24))
            .window(SignedDuration::from_hours(24))
            .holdout(SignedDuration::from_hours(6))
            .margins(Margins {
                long: Some(R::new(0.0)),
                short: Some(R::new(0.0)),
            })
            .min_pnl(R::new(0.0))
            .min_trades(1)
            .learner(LearnerConfig::Fake {})
            .build()
    }

    /// What the strategy reads: the quotes of gold.
    fn requirements() -> DataRequirements {
        DataRequirements {
            bar_types: vec![],
            ticks: vec![crate::strategy::TickSeries {
                data: nautilus_model::data::NautilusDataType::QuoteTick,
                instrument_id: InstrumentId::from(GOLD),
            }],
            warmup: SignedDuration::ZERO,
        }
    }

    /// The venue of the dataset runs: a commission rate of 30 per million,
    /// 10 million USD, no leverage.
    fn venue() -> DatasetVenue {
        DatasetVenue {
            commission: Commission::Rate(Decimal::new(3, 5)),
            balance: Money::from("10000000 USD"),
            leverage: Decimal::ONE,
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
        walk_of(dir, config())
    }

    /// The walk of `config` in `dir`, its catalog and store side by side.
    fn walk_of(dir: &Path, config: ModelConfig) -> WalkForward {
        std::fs::create_dir_all(dir.join("catalog")).unwrap();
        blueprint(&config)
            .walk_forward(&dir.join("catalog"), &dir.join("models"), &venue())
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
            unit: 1.0,
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
    /// short's the reverse. Fitted at the retrain of the 7th, before its
    /// holdout from 18:00 of the 6th: two wins in three, the long's mean R
    /// 1/3; held out, a loss. Fitted at the retrain of the 8th: one win in
    /// three, -1/3, the decisions of the 7th, scored on the 7th as well.
    /// Scored on the 8th: two wins.
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
        blueprint(config)
            .dataset(&dir.join("catalog"), &venue())
            .unwrap()
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

    /// A fake side model of `config()` valid from `valid_from`, of
    /// `fingerprint`, its baseline `baseline` (`model_of`).
    fn model(valid_from: Timestamp, fingerprint: String, baseline: f64) -> StoredModel {
        model_of(&config(), valid_from, fingerprint, baseline)
    }

    /// A fake side model of `config` valid from `valid_from`, of
    /// `fingerprint`, on the features then the target's payoff names: its R
    /// the row's first feature, its pair's calibration leaving it as it is
    /// (`unchanged`), its baseline `baseline`.
    fn model_of(
        config: &ModelConfig,
        valid_from: Timestamp,
        fingerprint: String,
        baseline: f64,
    ) -> StoredModel {
        let names = side_names(features().names(), &config.target.payoff_names());
        let set = TrainingSet::new(names.len(), vec![0.5; names.len()], vec![1.0]).unwrap();
        let model = LearnerConfig::Fake {}.fit(&set).unwrap();
        StoredModel {
            manifest: ModelManifest {
                strategy_id: StrategyId::from(STRATEGY),
                valid_from,
                fingerprint,
                feature_names: names,
                target: config.target.clone(),
                side: Some(Side::Long),
                learner: LearnerConfig::Fake {},
                calibration: unchanged(config, valid_from),
                baseline,
                holdout: HOLDOUT,
                probes: vec![],
            },
            model: model.save().unwrap(),
        }
    }

    /// The models of a walk in `dir` as `config` says.
    fn models(dir: &Path, config: &ModelConfig) -> ModelSet {
        blueprint(config)
            .model_set(&dir.join("models"), &venue())
            .unwrap()
    }

    /// The store of the `side` models of a walk in `dir` as `config` says.
    fn store(dir: &Path, config: &ModelConfig, side: Side) -> ModelStore {
        models(dir, config).side(side).unwrap().store().clone()
    }

    /// Publishes a fake model valid from `valid_from` for each side of a
    /// walk in `dir` as `config` says, with the baseline a train then fits
    /// (`trained_baseline`).
    fn publish(dir: &Path, config: &ModelConfig, valid_from: Timestamp) {
        let fingerprint = dataset(dir, config).fingerprint().to_string();
        for (side, _) in config.margins.sides() {
            let baseline = trained_baseline(side, valid_from);
            store(dir, config, side)
                .publish(&model_of(config, valid_from, fingerprint.clone(), baseline))
                .unwrap();
        }
    }

    /// The baseline of the `side` model a train at `valid_from` fits on
    /// `rows`, the training mean of the side's R before its holdout: at the
    /// 7th the long's two wins and a loss, 1/3, at the 8th its win and two
    /// losses, -1/3, the short's the reverse, winning where the long loses;
    /// nought at any other time, whose models' baselines no test reads.
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

    /// What `prepare` of `walk(dir)` over the 7th and the 8th asked for:
    /// the spans built and the times trained at. The build writes the
    /// dataset, a train publishes a model.
    fn prepare(dir: &Path, walk: &WalkForward) -> (Vec<(Timestamp, Timestamp)>, Vec<Timestamp>) {
        let (built, trained) = (RefCell::new(vec![]), RefCell::new(vec![]));
        walk.prepare(
            at("2026-01-07T00:00:00Z"),
            at("2026-01-09T00:00:00Z"),
            |from, to| {
                built.borrow_mut().push((from, to));
                build(dir);
                Ok(())
            },
            |at| {
                trained.borrow_mut().push(at);
                publish(dir, &config(), at);
                Ok(())
            },
        )
        .unwrap();
        (built.take(), trained.take())
    }

    #[test]
    fn a_backtest_builds_from_its_first_window_then_trains_at_each_retrain_time() {
        let dir = tempfile::tempdir().unwrap();

        let (built, trained) = prepare(dir.path(), &walk(dir.path()));

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
        prepare(dir.path(), &walk);

        assert_eq!(prepare(dir.path(), &walk), (vec![], vec![]));
    }

    #[test]
    fn a_model_in_the_store_is_reused() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk(dir.path());
        publish(dir.path(), &config(), at("2026-01-07T00:00:00Z"));

        let (_, trained) = prepare(dir.path(), &walk);

        assert_eq!(trained, [at("2026-01-08T00:00:00Z")]);
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
    #[test]
    fn each_period_scores_its_model_beside_that_models_own_baseline_on_the_same_rows() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk(dir.path());
        prepare(dir.path(), &walk);

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
        let walk = walk_of(dir, config.clone());
        let span = ("2026-01-07T00:00:00Z", "2026-01-09T00:00:00Z");
        prepared(dir, &walk, span, Some(&config)).unwrap();
        walk.scores(at(span.0), at(span.1)).unwrap()
    }

    /// Worked by hand, with no side to pick between: each model expects its
    /// row's feature in R, its unit 1 USD, so a side trades from a feature of
    /// 0.5. The 7th, the long takes the win at 0.8 and the loss at 0.6,
    /// nought per trade, every trade (with the loss at 0.3) -1/3; the 8th, the
    /// win at 0.9, every trade (with the win at 0.2) 1; the span, two wins and
    /// a loss taken, 1/3, of five trades, 1/5. At 1.5 times the commission of
    /// 0.1 each trade makes 0.05 less.
    #[test]
    fn each_period_reports_what_the_models_decisions_earned_beside_every_trade() {
        let dir = tempfile::tempdir().unwrap();

        let scores = one_sided_scores(dir.path(), Side::Long);

        let long = scores.long.as_ref().unwrap();
        let [first, second] = &long.periods[..] else {
            panic!("{scores:?}");
        };
        trades_are(first.earnings.taken, 2, 0.0);
        close(first.earnings.taken.win_rate, 0.5);
        trades_are(first.earnings.every, 3, -1.0 / 3.0);
        trades_are(first.earnings.taken_stressed, 2, -0.05);
        trades_are(first.earnings.every_stressed, 3, -1.0 / 3.0 - 0.05);
        trades_are(second.earnings.taken, 1, 1.0);
        trades_are(second.earnings.every, 2, 1.0);
        trades_are(long.earnings.taken, 3, 1.0 / 3.0);
        trades_are(long.earnings.every, 5, 0.2);
    }

    /// The short alone, the reverse trades of the same decisions, takes on the
    /// 7th a loss at 0.8 and a win at 0.6, nought, every trade 1/3.
    #[test]
    fn a_short_only_strategy_reports_what_its_decisions_earned_beside_every_trade() {
        let dir = tempfile::tempdir().unwrap();

        let scores = one_sided_scores(dir.path(), Side::Short);

        let short = &scores.short.as_ref().unwrap().periods[0];
        trades_are(short.earnings.taken, 2, 0.0);
        trades_are(short.earnings.every, 3, 1.0 / 3.0);
    }

    /// Publishes for `side` of a walk in `dir` as `config` says a model valid
    /// from `valid_from`, with the baseline a train then fits
    /// (`trained_baseline`): the long's a fake, expecting its row's feature in
    /// R, the short's a baseline expecting 0.5R whatever the row, their pair's
    /// calibration leaving each as it is (`unchanged`), up to rounding.
    fn publish_opposed(dir: &Path, config: &ModelConfig, side: Side, valid_from: Timestamp) {
        let fingerprint = dataset(dir, config).fingerprint().to_string();
        let mut opposed = model_of(
            config,
            valid_from,
            fingerprint,
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
        store(dir, config, side).publish(&opposed).unwrap();
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
            for side in Side::BOTH {
                publish_opposed(dir, &config(), side, valid_from);
            }
        }
        walk.scores(at("2026-01-07T00:00:00Z"), at("2026-01-09T00:00:00Z"))
            .unwrap()
    }

    /// `trades` are `count` trades of `expectancy` net P&L per trade, in R.
    fn trades_are(trades: Trades, count: usize, expectancy: f64) {
        assert_eq!(trades.count, count, "{trades:?}");
        close(trades.expectancy, expectancy);
    }

    /// On the rows of `opposed_scores`: the 7th, the long takes the win at 0.8
    /// and the loss at 0.6, nought per trade, the short the win at 0.3, 1,
    /// though each side clears on one more; the 8th, the long the win at 0.9,
    /// the short the loss at 0.2. The span, the long's two wins and a loss, 1/3,
    /// the short's win and loss, nought: five trades of five rows.
    #[test]
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
    #[test]
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
    #[test]
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
    #[test]
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
    #[test]
    fn a_row_where_the_sides_tie_counts_no_trade_for_either() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk(dir.path());
        prepare(dir.path(), &walk);

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

    /// The short's first model is the 8th's, the long's of the 7th having none
    /// beside it: on the 7th no pair decides, and neither side has a row
    /// scored or a trade. On the 8th the pair picks as in `opposed_scores`.
    #[test]
    fn a_side_with_no_model_beside_it_decides_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk(dir.path());
        build(dir.path());
        let (seventh, eighth) = (at("2026-01-07T00:00:00Z"), at("2026-01-08T00:00:00Z"));
        publish_opposed(dir.path(), &config(), Side::Long, seventh);
        publish_opposed(dir.path(), &config(), Side::Long, eighth);
        publish_opposed(dir.path(), &config(), Side::Short, eighth);

        let scores = walk.scores(seventh, at("2026-01-09T00:00:00Z")).unwrap();

        let (long, short) = (scores.long.unwrap(), scores.short.unwrap());
        for side in [&long, &short] {
            let first = &side.periods[0];
            assert!(first.models.is_empty(), "{first:?}");
            assert_eq!(first.score.rows, 0, "{first:?}");
            assert_eq!(first.earnings.taken.count, 0, "{first:?}");
            assert_eq!(first.earnings.every.count, 0, "{first:?}");
        }
        trades_are(long.periods[1].earnings.taken, 1, 1.0);
        trades_are(short.periods[1].earnings.taken, 1, -1.0);
    }

    /// `config`, on a target with exits on AUD/USD.
    fn with_exits() -> ModelConfig {
        ModelConfig {
            target: TargetConfig::builder()
                .instrument_id(InstrumentId::from("AUD/USD.SIM"))
                .quantity(Quantity::from(1))
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
        row.outcomes.long.unit = long;
        row.outcomes.short.unit = short;
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
    #[test]
    fn sides_expecting_the_same_r_tie_whatever_their_units() {
        let dir = tempfile::tempdir().unwrap();
        let config = with_exits();
        let walk = walk_of(dir.path(), config.clone());
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

    /// A fake meta model valid from `valid_from`, of `fingerprint`, on the
    /// meta's features: its R that of the row's first feature calibrated by
    /// `CALIBRATION`, its baseline 0.5.
    fn meta_model(valid_from: Timestamp, fingerprint: String) -> StoredModel {
        let names = meta_names(features().names());
        let set = TrainingSet::new(names.len(), vec![0.5; names.len()], vec![1.0]).unwrap();
        let mut meta = model(valid_from, fingerprint, 0.5);
        meta.manifest.side = None;
        meta.manifest.calibration = ModelCalibration::Meta(CALIBRATION);
        meta.manifest.feature_names = names;
        meta.model = LearnerConfig::Fake {}.fit(&set).unwrap().save().unwrap();
        meta
    }

    /// The scores over the 7th and the 8th of `meta_walk(dir)`, with `rows`,
    /// each side's opposed models (`publish_opposed`) and a meta model
    /// (`meta_model`) valid from the 7th and the 8th, the meta taking a trade
    /// from an expected 0.3 USD. The sides pick as in `opposed_scores`.
    fn opposed_meta_scores(dir: &Path) -> Scores {
        opposed_meta_scores_of(dir, &with_meta(), &rows())
    }

    /// The scores of `opposed_meta_scores`, of a walk as `config` says on
    /// `rows`.
    fn opposed_meta_scores_of(dir: &Path, config: &ModelConfig, rows: &[TargetRow]) -> Scores {
        let walk = walk_of(dir, config.clone());
        dataset(dir, config)
            .write(rows, at("2026-01-06T00:00:00Z"), at("2026-01-09T01:00:00Z"))
            .unwrap();
        let fingerprint = dataset(dir, config).fingerprint().to_string();
        for valid_from in [at("2026-01-07T00:00:00Z"), at("2026-01-08T00:00:00Z")] {
            for side in Side::BOTH {
                publish_opposed(dir, config, side, valid_from);
            }
            meta_store_of(dir, config)
                .publish(&meta_model(valid_from, fingerprint.clone()))
                .unwrap();
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
    #[test]
    fn the_metas_margin_is_in_r_whatever_the_unit_of_the_side_picked() {
        let dir = tempfile::tempdir().unwrap();
        let config = with_meta();
        let at_two: Vec<TargetRow> = rows()
            .into_iter()
            .map(|mut row| {
                row.outcomes.long.unit = 2.0;
                row.outcomes.short.unit = 2.0;
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
    #[test]
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
    #[test]
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
    #[test]
    fn a_sides_trades_picked_are_counted_before_any_meta() {
        let dir = tempfile::tempdir().unwrap();

        let scores = opposed_meta_scores(dir.path());

        trades_are(scores.long.unwrap().earnings.taken, 3, 1.0 / 3.0);
        trades_are(scores.short.unwrap().earnings.taken, 2, 0.0);
    }

    /// The pair of the 8th has a long of another fingerprint, refused as it
    /// loads: the pair is, and on the 8th the pair of the 7th is scored, its
    /// short too, never the 8th's short beside the 7th's long.
    #[test]
    fn a_model_failing_its_checks_is_not_scored_and_the_current_one_is() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk(dir.path());
        build(dir.path());
        let eighth = at("2026-01-08T00:00:00Z");
        publish(dir.path(), &config(), at("2026-01-07T00:00:00Z"));
        store(dir.path(), &config(), Side::Long)
            .publish(&model(eighth, "f".repeat(64), 0.5))
            .unwrap();
        let fingerprint = dataset(dir.path(), &config()).fingerprint().to_string();
        store(dir.path(), &config(), Side::Short)
            .publish(&model(eighth, fingerprint, 0.5))
            .unwrap();

        let scores = walk
            .scores(at("2026-01-07T00:00:00Z"), at("2026-01-09T00:00:00Z"))
            .unwrap();

        for side in [scores.long.unwrap(), scores.short.unwrap()] {
            let second = &side.periods[1];
            assert_eq!(second.models, [at("2026-01-07T00:00:00Z")], "{second:?}");
            assert_eq!(second.score.rows, 2, "{second:?}");
        }
    }

    /// A baseline is a model's: the 7th's three rows, decided with none,
    /// have no baseline either, and over the span the baseline has the
    /// model's two rows of the 8th.
    #[test]
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

    /// A pair published off the schedule, valid from 12:00 of the 7th, its
    /// long's baseline 1/4: the period of the 7th uses the 7th's long, its
    /// baseline 1/3, at 04:00 and 08:00, a win and a loss, and the later one
    /// at 12:00, a loss, each row scored beside the baseline of the model
    /// that decided it: the long's squared errors 4.29 against 4/9 + 16/9 +
    /// 25/16, a skill of 1 - 4.29 / (20/9 + 25/16), where the 7th's baseline
    /// throughout would make 1 - 4.29 / 4.
    #[test]
    fn a_period_scores_each_row_beside_the_baseline_of_the_model_that_decided_it() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk(dir.path());
        build(dir.path());
        for at in [at("2026-01-07T00:00:00Z"), at("2026-01-08T00:00:00Z")] {
            publish(dir.path(), &config(), at);
        }
        let fingerprint = dataset(dir.path(), &config()).fingerprint().to_string();
        for (side, baseline) in [(Side::Long, 0.25), (Side::Short, 0.75)] {
            store(dir.path(), &config(), side)
                .publish(&model(
                    at("2026-01-07T12:00:00Z"),
                    fingerprint.clone(),
                    baseline,
                ))
                .unwrap();
        }

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
  Net P&L per trade taken: 0.500000R
  Win rate of trades taken: 0.500000
  Every trade: 3
  Net P&L per trade, every trade: -0.250000R
  Win rate, every trade: 0.500000
  Net P&L per trade taken at 1.5x the commission: 0.450000R
  Net P&L per trade, every trade, at 1.5x the commission: -0.300000R

Short model scores, out of sample, over the span
  Skill: n/a
  Mean R - E': n/a
  Calibration slope: n/a
  Top-decile gap: n/a
  Rows: 0
  Trades taken: 0
  Net P&L per trade taken: n/a
  Win rate of trades taken: n/a
  Every trade: 0
  Net P&L per trade, every trade: n/a
  Win rate, every trade: n/a
  Net P&L per trade taken at 1.5x the commission: n/a
  Net P&L per trade, every trade, at 1.5x the commission: n/a

Meta model scores, out of sample, over the span
  Skill: 0.250000
  Mean R - E': 0.125000
  Calibration slope: 1.500000
  Top-decile gap: n/a
  Rows: 2
  Trades taken: 0
  Net P&L per trade taken: n/a
  Win rate of trades taken: n/a
  Every trade: 0
  Net P&L per trade, every trade: n/a
  Win rate, every trade: n/a
  Net P&L per trade taken at 1.5x the commission: n/a
  Net P&L per trade, every trade, at 1.5x the commission: n/a
"
        );
    }

    /// `config`, with a meta model on a window of 48 hours.
    fn with_meta() -> ModelConfig {
        let mut config = config();
        config.meta = Some(
            MetaConfig::builder()
                .window(SignedDuration::from_hours(48))
                .holdout(SignedDuration::from_hours(6))
                .margin(R::new(0.3))
                .min_pnl(R::new(0.0))
                .min_trades(1)
                .learner(LearnerConfig::Fake {})
                .build(),
        );
        config
    }

    /// The sides' models in the store at both retrain times and the two
    /// days before, their meta model's at none: the backtest builds from the
    /// meta's longer window and trains at both times, where the meta lacks
    /// a model.
    #[test]
    fn a_backtest_with_a_meta_model_builds_its_window_and_trains_where_either_lacks() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("catalog")).unwrap();
        let walk = blueprint(&with_meta())
            .walk_forward(
                &dir.path().join("catalog"),
                &dir.path().join("models"),
                &venue(),
            )
            .unwrap();
        for at in [
            at("2026-01-05T00:00:00Z"),
            at("2026-01-06T00:00:00Z"),
            at("2026-01-07T00:00:00Z"),
            at("2026-01-08T00:00:00Z"),
        ] {
            publish(dir.path(), &with_meta(), at);
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

    /// What `prepare` of `walk` over the 7th and the 8th built, its trains
    /// training nothing.
    fn built(walk: &WalkForward) -> Vec<(Timestamp, Timestamp)> {
        let built = RefCell::new(vec![]);
        walk.prepare(
            at("2026-01-07T00:00:00Z"),
            at("2026-01-09T00:00:00Z"),
            |from, to| {
                built.borrow_mut().push((from, to));
                Ok(())
            },
            |_| Ok(()),
        )
        .unwrap();
        built.take()
    }

    /// On an empty store the meta waits at the first retrain, the 7th, with
    /// no side's model before it, so reads no row there: the backtest
    /// builds from the sides' window, the 6th, not the meta's.
    #[test]
    fn a_backtest_from_an_empty_store_builds_from_the_sides_window_its_meta_waiting() {
        let dir = tempfile::tempdir().unwrap();

        let built = built(&meta_walk(dir.path()));

        assert_eq!(
            built,
            [(at("2026-01-06T00:00:00Z"), at("2026-01-09T00:00:00Z"))]
        );
    }

    /// The sides' first model is the 6th's: the meta's rows, its
    /// decisions, start there, though its window starts on the 5th.
    #[test]
    fn a_backtest_builds_no_day_before_the_sides_first_model() {
        let dir = tempfile::tempdir().unwrap();
        for at in [
            at("2026-01-06T00:00:00Z"),
            at("2026-01-07T00:00:00Z"),
            at("2026-01-08T00:00:00Z"),
        ] {
            publish(dir.path(), &with_meta(), at);
        }

        let built = built(&meta_walk(dir.path()));

        assert_eq!(
            built,
            [(at("2026-01-06T00:00:00Z"), at("2026-01-09T00:00:00Z"))]
        );
    }

    /// The walk of `with_meta` in `dir`, its catalog and store side by side.
    fn meta_walk(dir: &Path) -> WalkForward {
        walk_of(dir, with_meta())
    }

    /// The store of the meta models of `meta_walk(dir)`.
    fn meta_store(dir: &Path) -> ModelStore {
        meta_store_of(dir, &with_meta())
    }

    /// The store of the meta models of a walk in `dir` as `config` says.
    fn meta_store_of(dir: &Path, config: &ModelConfig) -> ModelStore {
        models(dir, config).meta().unwrap().store().clone()
    }

    /// A train of `meta_walk(dir)` at `at`, publishing as
    /// `Training::train_at` may: the sides, and the meta if the sides
    /// had a model before `at`.
    fn train_both(dir: &Path, at: Timestamp) -> anyhow::Result<()> {
        let config = with_meta();
        let earlier = store(dir, &config, Side::Long)
            .valid_froms()?
            .first()
            .is_some_and(|&t| t < at);
        publish(dir, &config, at);
        if earlier {
            let fingerprint = dataset(dir, &config).fingerprint().to_string();
            meta_store(dir).publish(&model(at, fingerprint, 0.5))?;
        }
        Ok(())
    }

    /// A meta model has no row before its sides' first model, so none
    /// is trained at the sides' first retrain time: a second backtest
    /// of the span finds nothing to train.
    #[test]
    fn a_second_backtest_with_a_meta_model_trains_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let walk = meta_walk(dir.path());
        let span = (at("2026-01-07T00:00:00Z"), at("2026-01-09T00:00:00Z"));
        walk.prepare(
            span.0,
            span.1,
            |_, _| Ok(()),
            |at| train_both(dir.path(), at),
        )
        .unwrap();
        let trained = RefCell::new(vec![]);

        walk.prepare(
            span.0,
            span.1,
            |_, _| Ok(()),
            |at| {
                trained.borrow_mut().push(at);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(trained.take(), []);
    }

    /// A backtest from the 8th trains no meta there, the sides' first
    /// model; a backtest from the 7th then trains the sides at the 7th
    /// first, which gives the meta at the 8th rows: it trains there too.
    #[test]
    fn a_backtest_from_earlier_trains_the_meta_where_the_sides_now_precede_it() {
        let dir = tempfile::tempdir().unwrap();
        let walk = meta_walk(dir.path());
        let end = at("2026-01-10T00:00:00Z");
        walk.prepare(
            at("2026-01-08T00:00:00Z"),
            end,
            |_, _| Ok(()),
            |at| train_both(dir.path(), at),
        )
        .unwrap();
        let meta = meta_store(dir.path());
        assert_eq!(meta.valid_froms().unwrap(), [at("2026-01-09T00:00:00Z")]);

        walk.prepare(
            at("2026-01-07T00:00:00Z"),
            end,
            |_, _| Ok(()),
            |at| train_both(dir.path(), at),
        )
        .unwrap();

        assert_eq!(
            meta.valid_froms().unwrap(),
            [at("2026-01-08T00:00:00Z"), at("2026-01-09T00:00:00Z")]
        );
    }

    #[test]
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

    /// What a backtest's preparation asked for: the spans built and the
    /// times trained at.
    type Asked = (Vec<(Timestamp, Timestamp)>, Vec<Timestamp>);

    /// What `prepare` of `walk`, in `dir`, over `[start, end)` asked for,
    /// or its error. The build writes the dataset; a train publishes a
    /// model of each side of `published`, if any, as a train of it would,
    /// and none if its holdouts fell short.
    fn prepared(
        dir: &Path,
        walk: &WalkForward,
        (start, end): (&str, &str),
        published: Option<&ModelConfig>,
    ) -> anyhow::Result<Asked> {
        let (built, trained) = (RefCell::new(vec![]), RefCell::new(vec![]));
        walk.prepare(
            at(start),
            at(end),
            |from, to| {
                built.borrow_mut().push((from, to));
                build(dir);
                Ok(())
            },
            |at| {
                trained.borrow_mut().push(at);
                if let Some(config) = published {
                    publish(dir, config, at);
                }
                Ok(())
            },
        )?;
        Ok((built.take(), trained.take()))
    }

    /// Never retrained, the backtest from 12:00 of the 7th trains at its
    /// start alone, off the daily grid, and builds from its window back
    /// from there.
    #[test]
    fn a_backtest_never_retrained_builds_its_window_and_trains_at_its_start_alone() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk_of(dir.path(), never());

        let (built, trained) = prepared(
            dir.path(),
            &walk,
            ("2026-01-07T12:00:00Z", "2026-01-09T00:00:00Z"),
            Some(&never()),
        )
        .unwrap();

        assert_eq!(
            built,
            [(at("2026-01-06T12:00:00Z"), at("2026-01-09T00:00:00Z"))]
        );
        assert_eq!(trained, [at("2026-01-07T12:00:00Z")]);
    }

    /// The models trained at the first backtest's start, 12:00 of the 7th,
    /// are in use at a second's of the same span, and at one from the 8th.
    #[test]
    fn a_second_backtest_never_retrained_of_the_span_or_from_later_trains_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk_of(dir.path(), never());
        let end = "2026-01-09T00:00:00Z";
        prepared(
            dir.path(),
            &walk,
            ("2026-01-07T12:00:00Z", end),
            Some(&never()),
        )
        .unwrap();

        for start in ["2026-01-07T12:00:00Z", "2026-01-08T00:00:00Z"] {
            let (_, trained) = prepared(dir.path(), &walk, (start, end), Some(&never())).unwrap();

            assert_eq!(trained, [], "from {start}");
        }
    }

    /// Never retrained, the train at the start published no side: the
    /// backtest would trade nothing, and is refused, naming the sides.
    #[test]
    fn a_backtest_never_retrained_with_no_sides_model_at_its_start_is_refused_naming_the_sides() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk_of(dir.path(), never());

        let err = prepared(
            dir.path(),
            &walk,
            ("2026-01-07T00:00:00Z", "2026-01-09T00:00:00Z"),
            None,
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("no side has a model in use"), "{err}");
        assert!(!err.contains("meta"), "{err}");
    }

    /// `with_meta`, never retrained.
    fn never_with_meta() -> ModelConfig {
        ModelConfig {
            retrain: Retrain::Never,
            ..with_meta()
        }
    }

    /// What `prepare` of the walk of `never_with_meta` in `dir` over the
    /// 7th and the 8th asked for, or its error, on an empty store. A train
    /// publishes as a train of two times would: if `sides`, a model of
    /// each side valid from the meta's window of 48 hours before it, and
    /// if `meta`, a meta model valid from it.
    fn prepared_two_time(dir: &Path, sides: bool, meta: bool) -> anyhow::Result<Asked> {
        let config = never_with_meta();
        let walk = walk_of(dir, config.clone());
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
                if sides {
                    publish(dir, &config, at - SignedDuration::from_hours(48));
                }
                if meta {
                    let fingerprint = dataset(dir, &config).fingerprint().to_string();
                    meta_store_of(dir, &config).publish(&model(at, fingerprint, 0.5))?;
                }
                Ok(())
            },
        )?;
        Ok((built.take(), trained.take()))
    }

    /// Never retrained, on an empty store, the plan of the train at the
    /// start, the 7th, fits the sides as of the 5th, on their window back
    /// from there: the backtest builds from the 4th, trains at the start
    /// alone, and runs, the sides of the 5th and the meta of the 7th in
    /// use.
    #[test]
    fn a_backtest_never_retrained_on_an_empty_store_builds_back_to_the_sides_window_and_has_both_in_use_at_its_start()
     {
        let dir = tempfile::tempdir().unwrap();

        let (built, trained) = prepared_two_time(dir.path(), true, true).unwrap();

        assert_eq!(
            built,
            [(at("2026-01-04T00:00:00Z"), at("2026-01-09T00:00:00Z"))]
        );
        assert_eq!(trained, [at("2026-01-07T00:00:00Z")]);
    }

    /// Never retrained, on an empty store, the train at the start
    /// publishes the sides as of the meta's window before it, and its
    /// meta's holdout falls short: the backtest is refused, naming the
    /// meta.
    #[test]
    fn a_backtest_never_retrained_whose_meta_was_refused_at_its_start_is_refused_naming_it() {
        let dir = tempfile::tempdir().unwrap();

        let err = prepared_two_time(dir.path(), true, false)
            .unwrap_err()
            .to_string();

        assert!(err.contains("meta"), "{err}");
        assert!(!err.contains("no side"), "{err}");
    }

    /// Never retrained, on an empty store, the train at the start
    /// publishes no side, and so leaves the meta waiting: the backtest is
    /// refused, naming the sides and the meta.
    #[test]
    fn a_backtest_never_retrained_whose_sides_were_refused_at_its_start_is_refused_naming_both() {
        let dir = tempfile::tempdir().unwrap();

        let err = prepared_two_time(dir.path(), false, false)
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("no side has a model in use") && err.contains("meta"),
            "{err}"
        );
    }

    /// Retrained, a backtest whose every train published nothing runs: a
    /// later retrain may publish, and its sides' scores stay.
    #[test]
    fn a_retrained_backtest_whose_trains_were_all_refused_still_runs() {
        let dir = tempfile::tempdir().unwrap();

        let (_, trained) = prepared(
            dir.path(),
            &walk(dir.path()),
            ("2026-01-07T00:00:00Z", "2026-01-09T00:00:00Z"),
            None,
        )
        .unwrap();

        assert_eq!(
            trained,
            [at("2026-01-07T00:00:00Z"), at("2026-01-08T00:00:00Z")]
        );
    }

    /// Never retrained, the models trained at the start, the 7th, are used
    /// over the whole span: one period, the 7th to the 9th, of the five
    /// rows decided in it.
    #[test]
    fn a_backtest_never_retrained_scores_the_span_as_one_period() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk_of(dir.path(), never());
        let span = ("2026-01-07T00:00:00Z", "2026-01-09T00:00:00Z");
        prepared(dir.path(), &walk, span, Some(&never())).unwrap();

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

    /// Never retrained, each side's model of the 7th is in use at a
    /// backtest from the 8th: its plan fits nothing, so it trains nothing
    /// and builds only the span's day, the scores reading no row before it.
    #[test]
    fn a_backtest_never_retrained_with_its_models_in_use_builds_only_its_span() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk_of(dir.path(), never());
        publish(dir.path(), &never(), at("2026-01-07T00:00:00Z"));

        let (built, trained) = prepared(
            dir.path(),
            &walk,
            ("2026-01-08T00:00:00Z", "2026-01-09T00:00:00Z"),
            Some(&never()),
        )
        .unwrap();

        assert_eq!(
            built,
            [(at("2026-01-08T00:00:00Z"), at("2026-01-09T00:00:00Z"))]
        );
        assert_eq!(trained, []);
    }

    /// Never retrained, the long's model of the 7th in use over a backtest
    /// from the 8th is scored beside its own baseline, the training mean of R
    /// its train fitted at the 7th, on the rows of the 6th before the
    /// holdout from 18:00, 1/3 (`trained_baseline`): on the 8th's two wins at
    /// 0.9 and 0.2, squared errors 0.65 against 8/9, a skill of 1 - 0.65 x
    /// 9/8, where the 8th's baseline, -1/3, would make 1 - 0.65 x 9/32.
    #[test]
    fn a_backtest_never_retrained_scores_a_model_beside_its_own_baseline() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk_of(dir.path(), never());
        publish(dir.path(), &never(), at("2026-01-07T00:00:00Z"));
        let span = ("2026-01-08T00:00:00Z", "2026-01-09T00:00:00Z");
        prepared(dir.path(), &walk, span, Some(&never())).unwrap();

        let scores = walk.scores(at(span.0), at(span.1)).unwrap();

        let long = scores.long.unwrap();
        let [period] = &long.periods[..] else {
            panic!("{long:?}");
        };
        let period = period.score;
        assert_eq!(period.rows, 2);
        close(period.skill, 1.0 - 0.65 * 9.0 / 8.0);
    }

    /// Never retrained, the long's model of the 7th has no short beside it,
    /// so no pair is in use at a backtest from the 8th: its train there is
    /// refused, and the backtest, which would trade nothing, is refused too,
    /// naming the sides.
    #[test]
    fn a_backtest_never_retrained_with_a_side_alone_at_its_start_is_refused_naming_the_sides() {
        let dir = tempfile::tempdir().unwrap();
        let walk = walk_of(dir.path(), never());
        let fingerprint = dataset(dir.path(), &never()).fingerprint().to_string();
        let seventh = at("2026-01-07T00:00:00Z");
        store(dir.path(), &never(), Side::Long)
            .publish(&model(
                seventh,
                fingerprint,
                trained_baseline(Side::Long, seventh),
            ))
            .unwrap();

        let err = prepared(
            dir.path(),
            &walk,
            ("2026-01-08T00:00:00Z", "2026-01-09T00:00:00Z"),
            None,
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("no side has a model in use"), "{err}");
    }

    /// Retrained, the models of the 7th are in use over both periods, the
    /// train of the 8th refused: on the 8th the long's model of the 7th is
    /// scored beside its own baseline, 1/3 (`trained_baseline`), not the
    /// training mean a train of the 8th would have fitted, -1/3: on the 8th's
    /// two wins, a skill of 1 - 0.65 x 9/8.
    #[test]
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
