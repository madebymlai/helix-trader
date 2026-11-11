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
//! is a child process in a backtest (one Nautilus node per process),
//! handed in by the caller; a train finds the days it reads from there
//! built, and a later train whose window reaches before them replays the
//! days missing itself (`Dataset::settled`), as any train does. A model in
//! the store is reused, so a second backtest of the same span builds and
//! trains nothing. A train that kept a model's current one published none,
//! so the next backtest trains at that time again.
//! The run then trades on the stores as they are: at each decision, the
//! latest pair of the sides valid then (`handle`).
//!
//! After the run (`WalkForward::scores`), each side's model is scored on
//! each decision's row of the dataset in the span against its side's
//! label, with the model the strategy used at it: the side's of the latest
//! pair valid then, a pair with a model failing its checks refused and the
//! current one kept, as the handle does, its probabilities calibrated. Every
//! model is out of sample on its rows: trained at its valid-from on rows
//! settled by then. Beside each, on the very rows it decided, its own
//! baseline, as its manifest has it (`store::ModelManifest::baseline`): the
//! training mean of the labels of the rows that model was fitted and
//! calibrated on, at its valid-from. A period whose train was refused so
//! scores the earlier model in use beside that model's baseline, a period of
//! models published off the schedule each row beside the baseline of the
//! model that decided it, and a row decided with no pair, as before the
//! sides' first, has no baseline either. The
//! AUC and log loss (`score`) of both are given per side, per retrain
//! period and over the span, on all the side's rows with a model, and what was
//! earned there (`holdout::Earnings`), beside every trade of those rows, its
//! own, at the commission and at 1.5 times it. A side's trades taken are those
//! picked for it on each row by the decision itself
//! (`deciding::DecidingModels::pick`), the picks a train's gate reads (`train`),
//! before any meta: a row where both sides clear counts one trade, for the side
//! expected the most, a tie none, so with no meta the sides' trades add up to
//! the decisions returned. A meta's are the trades of the side picked from its
//! threshold (`holdout::Threshold`), the figures its holdout is judged on, out
//! of sample. All this judges the decision the handle returns, on the target's
//! trade: no section adds the sides up, as the backtest's report gives what the
//! strategy actually traded, which can differ whenever it filters its decisions.

use std::{collections::BTreeSet, fmt, path::Path};

use anyhow::ensure;
use jiff::Timestamp;
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::StrategyId;

use super::{
    config::ModelConfig,
    dataset::{self, Dataset, DatasetRow, DatasetVenue},
    deciding::DecidingModels,
    features::FeatureSet,
    holdout::{Earnings, Example, STRESS, Threshold},
    meta,
    schedule::{Models, Schedule},
    score,
    store::{Baselined, ModelSet, Swap},
    targets::Side,
};
use crate::strategy::DataRequirements;

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
    /// The model's over the span, its rows with a model.
    pub model: Score,
    /// The baselines' over the span, each row's its model's, on the
    /// model's rows.
    pub baseline: Score,
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
    /// The model's, on the rows decided with one.
    pub model: Score,
    /// The baselines', on the same rows: each row's that of the model that
    /// decided it.
    pub baseline: Score,
    /// What was earned on the rows decided with one: the trades taken, a
    /// side's those picked for it, beside every trade of those rows.
    pub earnings: Earnings,
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
    /// The backtests of `strategy_id`'s models, deciding with `features`,
    /// on its dataset, for a strategy of `requirements`,
    /// run on `venue`, in the catalog at `catalog` and its models in the
    /// store under `models`, as `config` says.
    ///
    /// # Errors
    ///
    /// Returns an error if the target is invalid, or a store cannot be keyed.
    pub fn new(
        catalog: &Path,
        models: &Path,
        strategy_id: StrategyId,
        features: &FeatureSet,
        config: ModelConfig,
        requirements: &DataRequirements,
        venue: &DatasetVenue,
    ) -> anyhow::Result<Self> {
        let models = ModelSet::new(models, strategy_id, features, &config, requirements, venue)?;
        Ok(Self {
            dataset: Dataset::new(
                catalog,
                strategy_id,
                features,
                config.decision,
                config.target.clone(),
                requirements,
                venue,
            )?,
            models,
            schedule: Schedule::of_model(&config),
        })
    }

    /// Prepares a backtest over `[start, end)`: if the dataset lacks a day
    /// from where the replay of the plan of the span's first train time
    /// starts (`Plan::replay_from`), or that time if it fits no model, to
    /// the span's end, `build` builds it over that span (`Dataset::build`
    /// of its days); then `train` trains at each train time of the span
    /// (`Schedule::times`) whose plan fits a model (`Schedule::plan`,
    /// `Training::train_at`), in time order, each planned after the
    /// trains before it: a side's model they publish gives the meta its
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
                |pair, row, example| {
                    let model = pair.side(side);
                    Ok(Judged {
                        probability: model.predict(&example.features)?,
                        baseline: model.baseline(),
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
            let threshold = Threshold(level.config().threshold);
            let examples = meta::examples(&self.models, &rows.iter().collect::<Vec<_>>())?;
            scores.meta = Some(score(
                &times,
                (start, end),
                &examples,
                valid_froms,
                level.swap(),
                |model, _, example| {
                    let probability = model.predict(&example.features)?;
                    Ok(Judged {
                        probability,
                        baseline: model.baseline(),
                        taken: threshold.takes(probability),
                    })
                },
            )?);
        }
        Ok(scores)
    }

    /// The side picked on each of `rows`, in time order, by the decision
    /// itself, before any meta: on the row's units, among the sides of the
    /// pair in use then (`DecidingModels::pick`).
    fn picked(&self, rows: &[DatasetRow]) -> anyhow::Result<Vec<Option<Side>>> {
        let mut models = DecidingModels::new(&self.models)?;
        rows.iter()
            .map(|row| {
                let picked =
                    models.pick(Timestamp::from(row.ts_event), &row.features, row.units())?;
                Ok(picked.map(|picked| picked.side))
            })
            .collect()
    }
}

/// What the model in use on an example says of it.
struct Judged {
    /// Its calibrated probability.
    probability: f64,
    /// Its baseline's (`Baselined`).
    baseline: f64,
    /// Whether the example's trade was taken.
    taken: bool,
}

/// The scores over `[start, end)`, retrained at `times`, of the models
/// `models` swaps in on `examples`, in time order, among those valid from
/// `valid_froms`, each beside its own baseline on the rows it decided.
/// `judge` gives what the model in use on an example, at its position in
/// `examples`, says of it.
fn score<M>(
    times: &[Timestamp],
    (start, end): (Timestamp, Timestamp),
    examples: &[Example],
    valid_froms: &[Timestamp],
    mut models: Swap<M>,
    judge: impl Fn(&M, usize, &Example) -> anyhow::Result<Judged>,
) -> anyhow::Result<ModelScores> {
    let (mut model_rows, mut baseline_rows, mut model_trades) = (vec![], vec![], vec![]);
    let mut periods = vec![];
    for (i, &at) in times.iter().enumerate() {
        let (from, to) = (at.max(start), times.get(i + 1).copied().unwrap_or(end));
        let (from_ns, to_ns) = (UnixNanos::from(from), UnixNanos::from(to));
        let (mut period_models, mut by_model, mut by_baseline, mut trades) =
            (BTreeSet::new(), vec![], vec![], vec![]);
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
                    probability,
                    baseline,
                    taken,
                } = judge(model, row, example)?;
                by_model.push((probability, example.label));
                by_baseline.push((baseline, example.label));
                trades.push((taken, example.trade));
            }
        }
        periods.push(Period {
            from,
            to,
            models: period_models.into_iter().collect(),
            model: Score::of(&by_model),
            baseline: Score::of(&by_baseline),
            earnings: Earnings::of(&trades),
        });
        model_rows.extend(by_model);
        baseline_rows.extend(by_baseline);
        model_trades.extend(trades);
    }
    Ok(ModelScores {
        periods,
        model: Score::of(&model_rows),
        baseline: Score::of(&baseline_rows),
        earnings: Earnings::of(&model_trades),
    })
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
        lines(f, false, period.model)?;
        lines(f, true, period.baseline)?;
        earned(f, &period.earnings)?;
    }
    writeln!(f, "\n{model} scores, out of sample, over the span")?;
    lines(f, false, scores.model)?;
    lines(f, true, scores.baseline)?;
    earned(f, &scores.earnings)
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

/// The lines of `earnings`: the trades taken, a side's those picked for it,
/// every trade of the rows the model decided, and both at `STRESS` times the
/// commission.
fn earned(f: &mut fmt::Formatter<'_>, earnings: &Earnings) -> fmt::Result {
    let value = |value: Option<f64>| value.map_or_else(|| "n/a".to_string(), |v| format!("{v:.6}"));
    let Earnings {
        taken,
        every,
        taken_stressed,
        every_stressed,
    } = earnings;
    writeln!(f, "  Trades taken: {}", taken.count)?;
    writeln!(f, "  Net P&L per trade taken: {}", value(taken.expectancy))?;
    writeln!(f, "  Win rate of trades taken: {}", value(taken.win_rate))?;
    writeln!(f, "  Every trade: {}", every.count)?;
    writeln!(
        f,
        "  Net P&L per trade, every trade: {}",
        value(every.expectancy)
    )?;
    writeln!(f, "  Win rate, every trade: {}", value(every.win_rate))?;
    writeln!(
        f,
        "  Net P&L per trade taken at {STRESS}x the commission: {}",
        value(taken_stressed.expectancy)
    )?;
    writeln!(
        f,
        "  Net P&L per trade, every trade, at {STRESS}x the commission: {}",
        value(every_stressed.expectancy)
    )
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use jiff::SignedDuration;
    use nautilus_model::{
        identifiers::InstrumentId,
        types::{Money, Quantity},
    };
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        backtest::Commission,
        model::{
            calibration::Calibration,
            config::{Margins, MetaConfig},
            decide::{DecisionRule, FeatureRow},
            features::{meta_names, testing::last_mid},
            holdout::{Holdout, Trades},
            learner::{LearnerConfig, TrainingSet},
            payoff::Payoff,
            schedule::Retrain,
            store::{ModelManifest, ModelStore, StoredModel},
            targets::{
                BracketSpec, Distance, Exit, Outcome, Outcomes, Target, TargetConfig, TargetRow,
            },
        },
    };

    const GOLD: &str = "XAUUSD.ICMARKETS";
    const STRATEGY: &str = "WALK-001";

    fn features() -> Rc<FeatureSet> {
        last_mid(InstrumentId::from(GOLD)).build().unwrap()
    }

    /// The fake learner on hourly decisions, learning whether the long of
    /// a time-only target of 1 hour gains, trading each side from an expected 0,
    /// retrained every day on the day before, its last 6 hours held out.
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
                            .build(),
                    )
                    .build(),
            )
            .retrain(SignedDuration::from_hours(24))
            .window(SignedDuration::from_hours(24))
            .holdout(SignedDuration::from_hours(6))
            .margins(Margins {
                long: Some(0.0),
                short: Some(0.0),
            })
            .min_pnl(0.0)
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

    fn walk(dir: &Path) -> WalkForward {
        walk_of(dir, config())
    }

    /// The walk of `config` in `dir`, its catalog and store side by side.
    fn walk_of(dir: &Path, config: ModelConfig) -> WalkForward {
        std::fs::create_dir_all(dir.join("catalog")).unwrap();
        WalkForward::new(
            &dir.join("catalog"),
            &dir.join("models"),
            StrategyId::from(STRATEGY),
            &features(),
            config,
            &requirements(),
            &venue(),
        )
        .unwrap()
    }

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    /// The row of the decision at `time` on `feature`, its long gaining 1
    /// USD if `wins`, else losing 1, the short the reverse, each net of 0.1
    /// USD of commission and of a unit of 1, settled an hour later.
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
            outcomes: Outcomes {
                long: outcome(long),
                short: outcome(short),
            },
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

    /// The dataset of a walk in `dir` as `config` says, in its catalog.
    fn dataset(dir: &Path, config: &ModelConfig) -> Dataset {
        Dataset::new(
            &dir.join("catalog"),
            StrategyId::from(STRATEGY),
            &features(),
            config.decision,
            config.target.clone(),
            &requirements(),
            &venue(),
        )
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

    /// The calibration of the models of the tests: the odds `o` of a
    /// prediction become `o^0.9`.
    const CALIBRATION: Calibration = Calibration { a: -0.9, b: 0.0 };

    /// A fake model valid from `valid_from`, of `fingerprint`, calibrated
    /// by `CALIBRATION`, its baseline `baseline`.
    fn model(valid_from: Timestamp, fingerprint: String, baseline: f64) -> StoredModel {
        let set = TrainingSet::new(1, vec![0.5], vec![true]).unwrap();
        let model = LearnerConfig::Fake {}.fit(&set).unwrap();
        StoredModel {
            manifest: ModelManifest {
                strategy_id: StrategyId::from(STRATEGY),
                valid_from,
                fingerprint,
                feature_names: features().names().to_vec(),
                target: config().target,
                side: Some(Side::Long),
                learner: LearnerConfig::Fake {},
                calibration: CALIBRATION,
                baseline,
                payoff: Some(Payoff {
                    win: 1.0,
                    loss: 1.0,
                }),
                holdout: Holdout {
                    earnings: Earnings::default(),
                    log_loss: 0.6,
                    baseline_log_loss: 0.69,
                    auc: Some(0.5),
                },
                probes: vec![],
            },
            model: model.save().unwrap(),
        }
    }

    /// The models of a walk in `dir` as `config` says.
    fn models(dir: &Path, config: &ModelConfig) -> ModelSet {
        ModelSet::new(
            &dir.join("models"),
            StrategyId::from(STRATEGY),
            &features(),
            config,
            &requirements(),
            &venue(),
        )
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
                .publish(&model(valid_from, fingerprint.clone(), baseline))
                .unwrap();
        }
    }

    /// The baseline of the `side` model a train at `valid_from` fits on
    /// `rows`, the training mean of the side's labels before its holdout:
    /// at the 7th the long's two wins in three, 2/3, at the 8th its one in
    /// three, 1/3, the short's the reverse, winning where the long loses;
    /// 1/2 at any other time, whose models' baselines no test reads.
    fn trained_baseline(side: Side, valid_from: Timestamp) -> f64 {
        let long = if valid_from == at("2026-01-07T00:00:00Z") {
            2.0 / 3.0
        } else if valid_from == at("2026-01-08T00:00:00Z") {
            1.0 / 3.0
        } else {
            0.5
        };
        match side {
            Side::Long => long,
            Side::Short => 1.0 - long,
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
    /// training mean its train fitted (`trained_baseline`), on the rows it
    /// decided. The 7th, the fake's probabilities its feature `p`
    /// calibrated, the odds `o` of `p` becoming `o^0.9`: the win 0.8 above the
    /// losses 0.3 and 0.6 (AUC 1), log loss `-(ln c(0.8) + ln (1 - c(0.3)) +
    /// ln (1 - c(0.6))) / 3`; the 7th's model's baseline 2/3 throughout (AUC
    /// 0.5), `-(ln 2/3 + 2 ln 1/3) / 3`. The 8th, two wins (no AUC), `-(ln
    /// c(0.9) + ln c(0.2)) / 2`; the 8th's model's baseline 1/3, `-ln 1/3`.
    /// The span: of the six pairs of a win and a loss, the win above in
    /// four; the baselines' wins 2/3, 1/3 and 1/3 against their losses at
    /// 2/3, a tie each for the first: one of six. The short wins where the
    /// long loses: on the 7th its model ranks every pair the wrong way
    /// round, its model's baseline 1/3.
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
        assert_eq!((first.model.rows, first.baseline.rows), (3, 3));
        close(first.model.auc, 1.0);
        close(first.model.log_loss, 0.509_155);
        close(first.baseline.auc, 0.5);
        close(first.baseline.log_loss, 0.867_563);
        assert_eq!(second.models, [at("2026-01-08T00:00:00Z")]);
        assert_eq!((second.model.rows, second.model.auc), (2, None));
        close(second.model.log_loss, 0.814_876);
        close(second.baseline.log_loss, 1.098_612);
        assert_eq!((long.model.rows, long.baseline.rows), (5, 5));
        close(long.model.auc, 4.0 / 6.0);
        close(long.model.log_loss, 0.631_443);
        close(long.baseline.auc, 1.0 / 6.0);
        close(long.baseline.log_loss, 0.959_983);
        let short = &scores.short.as_ref().unwrap().periods[0];
        close(short.model.auc, 0.0);
        close(short.baseline.log_loss, 0.867_563);
    }

    /// The scores over the 7th and the 8th of a walk in `dir` trading `side`
    /// alone, a train publishing its model at each retrain time.
    fn one_sided_scores(dir: &Path, side: Side) -> Scores {
        let margin = |traded| (traded == side).then_some(0.0);
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

    /// Worked by hand, with no side to pick between: each model expects a win
    /// and a loss of 1 USD, so a side trades from a calibrated 1/2, a feature
    /// of 0.5. The 7th, the long takes the win at 0.8 and the loss at 0.6,
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

    /// The calibration of the short of `publish_opposed`: the odds `o` of a
    /// prediction become `o^-0.9`, its probability the complement of what
    /// `CALIBRATION` makes of the same prediction.
    const OPPOSED: Calibration = Calibration { a: 0.9, b: 0.0 };

    /// Publishes for `side` of a walk in `dir` as `config` says a fake model
    /// valid from `valid_from`, with the baseline a train then fits
    /// (`trained_baseline`), that expects a win of 3 USD and a loss of 1, so
    /// `4p - 1` at a calibrated probability `p`, trading from 1/4. On a feature
    /// `f` the long's `p` is `c(f)` (`CALIBRATION`), the short's `1 - c(f)`
    /// (`OPPOSED`).
    fn publish_opposed(dir: &Path, config: &ModelConfig, side: Side, valid_from: Timestamp) {
        let fingerprint = dataset(dir, config).fingerprint().to_string();
        let mut opposed = model(valid_from, fingerprint, trained_baseline(side, valid_from));
        opposed.manifest.payoff = Some(Payoff {
            win: 3.0,
            loss: 1.0,
        });
        if side == Side::Short {
            opposed.manifest.calibration = OPPOSED;
        }
        store(dir, config, side).publish(&opposed).unwrap();
    }

    /// The scores over the 7th and the 8th of `walk(dir)`, with `rows` and each
    /// side's opposed models (`publish_opposed`) valid from the 7th and the 8th.
    /// With c(0.2) = 0.223, c(0.3) = 0.318, c(0.6) = 0.590, c(0.8) = 0.777 and
    /// c(0.9) = 0.878, each side's trade and what its model expects of it:
    ///
    /// - 04:00 of the 7th, 0.8: the long a win, 2.11; the short a loss, -0.11.
    ///   Only the long clears.
    /// - 08:00 of the 7th, 0.3: the long a loss, 0.27; the short a win, 1.73.
    ///   Both clear, and the short is picked.
    /// - 12:00 of the 7th, 0.6: the long a loss, 1.36; the short a win, 0.64.
    ///   Both clear, and the long is picked.
    /// - 06:00 of the 8th, 0.9: the long a win, 2.51; the short a loss, -0.51.
    ///   Only the long clears.
    /// - 10:00 of the 8th, 0.2: the long a win, -0.11; the short a loss, 2.11.
    ///   Only the short clears.
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

    /// `trades` are `count` trades of `expectancy` net P&L per trade.
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
    /// on the rows of `opposed_scores` the short's probabilities are the
    /// complements of the long's and its labels the reverse, and its baseline
    /// the complement too, so on the 7th each side has the one AUC and log
    /// loss, on all three rows, though two are picked for the long and one for
    /// the short.
    #[test]
    fn each_sides_auc_log_loss_and_baseline_stay_on_all_its_rows_with_a_model() {
        let dir = tempfile::tempdir().unwrap();

        let scores = opposed_scores(dir.path());

        for side in [scores.long.unwrap(), scores.short.unwrap()] {
            let first = &side.periods[0];
            assert_eq!((first.model.rows, first.baseline.rows), (3, 3));
            close(first.model.auc, 1.0);
            close(first.model.log_loss, 0.509_155);
            close(first.baseline.auc, 0.5);
            close(first.baseline.log_loss, 0.867_563);
            assert_eq!((side.model.rows, side.baseline.rows), (5, 5));
        }
    }

    /// The models of both sides alike: on every row they expect the same, at
    /// 0.8, 0.6 and 0.9 clearing their margins, at 0.3 and 0.2 not.
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
            assert_eq!(side.model.rows, 5, "{side:?}");
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
            assert_eq!(first.model.rows, 0, "{first:?}");
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
    /// what `(long, short)` say.
    fn row_of_stops(time: &str, (long, short): (f64, f64)) -> TargetRow {
        let mut row = row(time, 0.8, true);
        row.outcomes.long.unit = long;
        row.outcomes.short.unit = short;
        row
    }

    /// The models of both sides alike, expecting the same in R, which each
    /// row's units then make money: stops worth 2 and 4 USD. At 04:00 the
    /// short's is worth more and it is picked, its trade a loss; at 08:00 the
    /// long's, its trade a win. On equal units they would tie.
    #[test]
    fn a_row_is_picked_on_each_sides_own_unit() {
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

        trades_are(scores.long.unwrap().earnings.taken, 1, 1.0);
        trades_are(scores.short.unwrap().earnings.taken, 1, -1.0);
    }

    /// A fake meta model valid from `valid_from`, of `fingerprint`, on the
    /// meta's features: its probability that of the row's first feature
    /// calibrated by `CALIBRATION`.
    fn meta_model(valid_from: Timestamp, fingerprint: String) -> StoredModel {
        let names = meta_names(features().names());
        let set = TrainingSet::new(names.len(), vec![0.5; names.len()], vec![true]).unwrap();
        let mut meta = model(valid_from, fingerprint, 0.5);
        meta.manifest.side = None;
        meta.manifest.payoff = None;
        meta.manifest.feature_names = names;
        meta.model = LearnerConfig::Fake {}.fit(&set).unwrap().save().unwrap();
        meta
    }

    /// The scores over the 7th and the 8th of `meta_walk(dir)`, with `rows`,
    /// each side's opposed models (`publish_opposed`) and a meta model
    /// (`meta_model`) valid from the 7th and the 8th, the meta taking a trade
    /// from 1/2. The sides pick as in `opposed_scores`.
    fn opposed_meta_scores(dir: &Path) -> Scores {
        let walk = meta_walk(dir);
        build(dir);
        let config = with_meta();
        let fingerprint = dataset(dir, &config).fingerprint().to_string();
        for valid_from in [at("2026-01-07T00:00:00Z"), at("2026-01-08T00:00:00Z")] {
            for side in Side::BOTH {
                publish_opposed(dir, &config, side, valid_from);
            }
            meta_store(dir)
                .publish(&meta_model(valid_from, fingerprint.clone()))
                .unwrap();
        }
        walk.scores(at("2026-01-07T00:00:00Z"), at("2026-01-09T00:00:00Z"))
            .unwrap()
    }

    /// On the five rows the sides pick on, the trade of the side picked, the
    /// meta taking c(0.8), c(0.6) and c(0.9), not c(0.3) and c(0.2). The 7th: a
    /// win, the short's win and a loss, 1/3, of which it takes the first and
    /// the last, nought. The 8th: a win and the short's loss, nought, of which
    /// it takes the win, 1. The span, 1/3 of three beside 1/5 of five.
    #[test]
    fn a_metas_earnings_read_the_trades_of_the_side_picked_from_its_threshold() {
        let dir = tempfile::tempdir().unwrap();

        let meta = opposed_meta_scores(dir.path()).meta.unwrap();

        assert_eq!(meta.model.rows, 5);
        trades_are(meta.periods[0].earnings.taken, 2, 0.0);
        trades_are(meta.periods[0].earnings.every, 3, 1.0 / 3.0);
        trades_are(meta.periods[1].earnings.taken, 1, 1.0);
        trades_are(meta.periods[1].earnings.every, 2, 0.0);
        trades_are(meta.earnings.taken, 3, 1.0 / 3.0);
        trades_are(meta.earnings.every, 5, 0.2);
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
            assert_eq!(second.model.rows, 2, "{second:?}");
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
        assert_eq!((first.model.rows, first.model.log_loss), (0, None));
        assert_eq!((first.baseline.rows, first.baseline.log_loss), (0, None));
        assert_eq!((long.model.rows, long.baseline.rows), (2, 2));
    }

    /// A pair published off the schedule, valid from 12:00 of the 7th, its
    /// long's baseline 1/4: the period of the 7th uses the 7th's long, its
    /// baseline 2/3, at 04:00 and 08:00, a win and a loss, and the later one
    /// at 12:00, a loss, each row scored beside the baseline of the model
    /// that decided it, `-(ln 2/3 + ln 1/3 + ln 3/4) / 3`.
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
        assert_eq!((first.model.rows, first.baseline.rows), (3, 3));
        close(first.baseline.log_loss, 0.597_253);
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
                    model: score,
                    baseline: score,
                    earnings,
                }],
                model: score,
                baseline: Score::default(),
                earnings: Earnings::default(),
            }),
            meta: None,
        };

        assert_eq!(
            scores.to_string(),
            "
Short model scores, out of sample, from 2026-01-07T00:00:00Z to 2026-01-08T00:00:00Z
  Model valid from: 2026-01-07T00:00:00Z
  AUC: 0.750000
  Log loss: n/a
  Rows: 3
  Baseline AUC: 0.750000
  Baseline log loss: n/a
  Baseline rows: 3
  Trades taken: 2
  Net P&L per trade taken: 0.500000
  Win rate of trades taken: 0.500000
  Every trade: 3
  Net P&L per trade, every trade: -0.250000
  Win rate, every trade: 0.500000
  Net P&L per trade taken at 1.5x the commission: 0.450000
  Net P&L per trade, every trade, at 1.5x the commission: -0.300000

Short model scores, out of sample, over the span
  AUC: 0.750000
  Log loss: n/a
  Rows: 3
  Baseline AUC: n/a
  Baseline log loss: n/a
  Baseline rows: 0
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
                .threshold(0.5)
                .min_pnl(0.0)
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
        let walk = WalkForward::new(
            &dir.path().join("catalog"),
            &dir.path().join("models"),
            StrategyId::from(STRATEGY),
            &features(),
            with_meta(),
            &requirements(),
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
        let score = Score {
            rows: 2,
            auc: None,
            log_loss: Some(0.5),
        };
        let scores = ModelScores {
            periods: vec![Period {
                from: at("2026-01-07T00:00:00Z"),
                to: at("2026-01-08T00:00:00Z"),
                models: vec![],
                model: score,
                baseline: score,
                earnings: Earnings::default(),
            }],
            model: score,
            baseline: score,
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
        assert_eq!(period.model.rows, 5);
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
    /// from the 8th is scored beside its own baseline, the training mean
    /// its train fitted at the 7th, on the rows of the 6th before the
    /// holdout from 18:00, two wins in three (`trained_baseline`); on the
    /// 8th's two wins, `-ln 2/3`.
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
        assert_eq!((period.model.rows, period.baseline.rows), (2, 2));
        close(period.baseline.log_loss, 0.405_465);
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
    /// scored beside its own baseline, 2/3 (`trained_baseline`), not the
    /// training mean a train of the 8th would have fitted, 1/3; on the
    /// 8th's two wins, `-ln 2/3`.
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
        assert_eq!((second.model.rows, second.baseline.rows), (2, 2));
        close(second.baseline.log_loss, 0.405_465);
    }
}
