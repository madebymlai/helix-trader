//! The model runs of the model tests: a probe deciding every hour on the
//! last mid of IC's gold through its model handle (`model::handle`),
//! either opening the target of each decision in a dataset run
//! (`Backtest::run_dataset`), the targets subscribing to the bars of
//! its ATR, if any, the rows they settle collected, or trading on the
//! models of a store, sending its trades, its decisions' rows, trades and
//! closed positions recorded; on IC's hedging venue both. And the
//! random walk of quotes they replay.

use std::{cell::RefCell, path::Path, rc::Rc};

use crate::common::{
    features::{SmaArgs, tables},
    ic,
};
use helix_trader::{
    backtest::{Backtest, commission_rate},
    model::{
        calibration::Calibration,
        config::{Margins, ModelConfig},
        dataset::{self, Commission, DatasetVenue},
        decide::{self, DecisionRule, FeatureRow},
        features::{Feature, FeatureSet, FeatureSetBuilder, PRIMARY_FEATURES},
        handle::{Decision, Mode, ModelHandle, SettledRows},
        holdout::{Earnings, Holdout},
        learner::{LearnerConfig, TrainingSet},
        payoff::Payoff,
        store::{ModelManifest, Probe, StoredModel},
        targets::{Distance, Side, Target, TargetConfig, TargetRow},
    },
    strategy::{DataRequirements, Declared, IndicatorArgs, IndicatorsConfig, TickSeries, orders},
};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::DataActor,
    logging::logger::LoggerConfig,
    msgbus::{self, ShareableMessageHandler, switchboard::get_custom_topic},
    timer::TimeEvent,
};
use nautilus_core::UnixNanos;
use nautilus_indicators::indicator::Indicator;
use nautilus_model::{
    data::{CustomData, NautilusDataType, QuoteTick},
    enums::OmsType,
    events::{OrderCanceled, OrderDenied, OrderRejected, PositionClosed},
    identifiers::{InstrumentId, StrategyId},
    types::{Money, Quantity},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rust_decimal::Decimal;

pub const STRATEGY: &str = "DATASET-001";

/// Decides every hour on the last mid of IC's gold through its model
/// handle; records the rows published for its decisions and the trades
/// they make, which it sends, and the positions they closed.
#[derive(Debug)]
pub struct ModelProbe {
    core: StrategyCore,
    model: Rc<ModelHandle>,
    record: Rc<RefCell<Record>>,
}

/// What a trading probe decided.
#[derive(Debug, Default)]
pub struct Record {
    /// The row of each decision on warm features.
    pub rows: Vec<FeatureRow>,
    /// Each decision to trade, and its time.
    pub trades: Vec<(UnixNanos, Decision)>,
    /// Each position closed, when it opened and its realised P&L.
    pub closed: Vec<(UnixNanos, Money)>,
}

nautilus_strategy!(ModelProbe, {
    fn on_order_canceled(&mut self, event: &OrderCanceled) {
        self.model.on_order_canceled(self, event);
    }

    fn on_order_denied(&mut self, event: OrderDenied) {
        self.model.on_order_denied(&event);
    }

    fn on_order_rejected(&mut self, event: OrderRejected) {
        self.model.on_order_rejected(&event);
    }

    fn on_position_closed(&mut self, event: PositionClosed) {
        if let Some(pnl) = event.realized_pnl {
            self.record.borrow_mut().closed.push((event.ts_opened, pnl));
        }
        Rc::clone(&self.model).on_position_closed(self, &event);
    }
});

impl DataActor for ModelProbe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let record = Rc::clone(&self.record);
        let handler = ShareableMessageHandler::from_typed(move |data: &CustomData| {
            let row = data.data.as_any().downcast_ref::<FeatureRow>().unwrap();
            record.borrow_mut().rows.push(row.clone());
        });
        let topic = get_custom_topic(&FeatureRow::data_type(StrategyId::from(STRATEGY)));
        msgbus::subscribe_any(topic.into(), handler, None);
        Rc::clone(&self.model).start(self)
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        if decide::is_decision(event) {
            if let Some(decision) = Rc::clone(&self.model).decide(self)? {
                let now = self.clock().timestamp_ns();
                self.record.borrow_mut().trades.push((now, decision));
                decision.trade().send(self)?;
            }
            return Ok(());
        }
        if !orders::on_time_event(self, event)? {
            Rc::clone(&self.model).on_time_event(self, event)?;
        }
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.model.on_stop();
        Ok(())
    }
}

pub fn rule() -> DecisionRule {
    DecisionRule::Every(SignedDuration::from_hours(1))
}

/// The last mid of a quote, Nautilus' simple average of one,
/// `[indicators.mid]`.
pub const MID: Declared<SmaArgs> = Declared::new("mid");

/// `[indicators.mid]`, as `[indicators]` holds it.
pub fn mid_tables() -> IndicatorsConfig {
    tables(r#"mid = { period = 1, price_type = "MID" }"#)
}

/// The probe's one feature, the last mid of IC's gold.
pub fn features() -> Rc<FeatureSet> {
    FeatureSet::builder()
        .quotes(InstrumentId::from(ic::INSTRUMENT_ID), MID, &mid_tables())
        .build()
        .unwrap()
}

/// A bracket 7.5 bps away on both sides closed after 2 hours, of one ounce.
pub fn target() -> TargetConfig {
    TargetConfig::builder()
        .instrument_id(InstrumentId::from(ic::INSTRUMENT_ID))
        .quantity(Quantity::from(1))
        .spec(
            Target::builder()
                .take_profit(Distance::BasisPoints(Decimal::new(75, 1)))
                .stop(Distance::BasisPoints(Decimal::new(75, 1)))
                .limit(SignedDuration::from_hours(2))
                .build(),
        )
        .build()
}

/// The probe's decisions and target, learning whether each side of the
/// bracket gains, on a window of 46 hours, the last 12 held out and the
/// 12 before them calibrated on, by the fake learner, published from
/// nought per trade over a trade; each side traded from an expected net
/// P&L of nought.
pub fn config() -> ModelConfig {
    ModelConfig::builder()
        .decision(rule())
        .target(target())
        .retrain(SignedDuration::from_hours(24))
        .window(SignedDuration::from_hours(46))
        .holdout(SignedDuration::from_hours(12))
        .margins(Margins {
            long: Some(0.0),
            short: Some(0.0),
        })
        .min_pnl(0.0)
        .min_trades(1)
        .learner(LearnerConfig::Fake {})
        .build()
}

/// A deterministic random walk in [0, 1).
pub struct Lcg(pub u64);

impl Lcg {
    pub fn next(&mut self) -> f64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// IC's gold over `[from, to)` in a catalog, the quotes of `walk_quotes`.
pub fn walk(from: Timestamp, to: Timestamp) -> tempfile::TempDir {
    ic::catalog_of(walk_quotes(from, to), from, to)
}

/// IC's gold's quotes over `[from, to)`, one every 5 s, the bid a random
/// walk of up to 0.30 a step, the ask 0.20 above.
pub fn walk_quotes(from: Timestamp, to: Timestamp) -> Vec<QuoteTick> {
    let mut walk = Lcg(7);
    let mut bid = 4000.0_f64;
    let mut quotes = vec![];
    let mut time = from;
    while time < to {
        bid = ((bid + (walk.next() - 0.5) * 0.6) * 100.0).round() / 100.0;
        let (b, a) = (format!("{bid:.2}"), format!("{:.2}", bid + 0.2));
        quotes.push(ic::quote(time, &b, &a));
        time += SignedDuration::from_secs(5);
    }
    quotes
}

/// The probe deciding on `features` as `config` says, in `mode`, and what
/// it records.
fn probe(
    features: Rc<FeatureSet>,
    config: &ModelConfig,
    mode: Mode,
) -> (ModelProbe, Rc<RefCell<Record>>) {
    probe_reading(features, config, &requirements(), mode)
}

/// The probe deciding on `features` as `config` says, for a strategy of
/// `requirements`, in `mode`, and what it records.
pub fn probe_reading(
    features: Rc<FeatureSet>,
    config: &ModelConfig,
    requirements: &DataRequirements,
    mode: Mode,
) -> (ModelProbe, Rc<RefCell<Record>>) {
    let record = Rc::new(RefCell::new(Record::default()));
    let probe = ModelProbe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from(STRATEGY)),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
        model: ModelHandle::new(
            StrategyId::from(STRATEGY),
            features,
            config,
            requirements,
            mode,
        )
        .unwrap(),
        record: Rc::clone(&record),
    };
    (probe, record)
}

/// What the probe reads, as `backtest` replays it: IC's gold quotes.
pub fn requirements() -> DataRequirements {
    DataRequirements {
        bar_types: vec![],
        ticks: vec![TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: InstrumentId::from(ic::INSTRUMENT_ID),
        }],
        warmup: SignedDuration::ZERO,
    }
}

/// The venue of `backtest`, as given: 30 USD per million traded, 10M USD,
/// no leverage.
pub fn venue() -> DatasetVenue {
    DatasetVenue {
        commission: Commission::Rate("0.00003".to_string()),
        balance: Money::from("10000000 USD"),
        leverage: Decimal::ONE,
    }
}

/// The run of the probe over `[from, to]` on IC's hedging venue, 30 USD
/// per million traded, 10M USD (`venue`).
pub fn backtest(catalog: &Path, from: Timestamp, to: Timestamp) -> Backtest {
    Backtest {
        catalog: catalog.to_path_buf(),
        bar_types: vec![],
        ticks: requirements().ticks,
        start: from,
        end: to,
        fee_model: commission_rate(Decimal::new(3, 5)).unwrap(),
        balance: venue().balance,
        leverage: venue().leverage,
        venue_oms: |_| OmsType::Hedging,
        logging: LoggerConfig::default(),
    }
}

/// The replay of a build or a train: the probe's dataset run over
/// `[from, to]`; the rows it settled.
pub fn replay(
    catalog: &tempfile::TempDir,
    from: Timestamp,
    to: Timestamp,
) -> anyhow::Result<Vec<TargetRow>> {
    replay_on(catalog, features(), from, to)
}

/// The replay of a build or a train of the probe deciding on `features`.
pub fn replay_on(
    catalog: &tempfile::TempDir,
    features: Rc<FeatureSet>,
    from: Timestamp,
    to: Timestamp,
) -> anyhow::Result<Vec<TargetRow>> {
    replay_of(catalog, features, &config(), from, to)
}

/// The replay of a build or a train of the probe deciding on `features`
/// as `config` says.
pub fn replay_of(
    catalog: &tempfile::TempDir,
    features: Rc<FeatureSet>,
    config: &ModelConfig,
    from: Timestamp,
    to: Timestamp,
) -> anyhow::Result<Vec<TargetRow>> {
    let rows = SettledRows::default();
    let (probe, _) = probe(
        features,
        config,
        Mode::Dataset {
            rows: Rc::clone(&rows),
        },
    );
    backtest(catalog.path(), from, to).run_dataset(probe)?;
    Ok(rows.take())
}

/// The probe of `config` trading over `[from, to]` on the models of the
/// store under `models`; what it decided.
pub fn trade(
    catalog: &tempfile::TempDir,
    from: Timestamp,
    to: Timestamp,
    config: &ModelConfig,
    models: &Path,
) -> Record {
    trade_on(catalog, features(), from, to, config, models)
}

/// What the probe deciding on `features` decided, trading as `trade`.
pub fn trade_on(
    catalog: &tempfile::TempDir,
    features: Rc<FeatureSet>,
    from: Timestamp,
    to: Timestamp,
    config: &ModelConfig,
    models: &Path,
) -> Record {
    let (probe, record) = probe(
        features,
        config,
        Mode::Backtest {
            models: models.to_path_buf(),
            venue: venue(),
        },
    );
    backtest(catalog.path(), from, to).run(probe).unwrap();
    record.take()
}

/// A feature initialized by the first quote, its value missing: the fake
/// learner then predicts the mean of its training labels, so each model's
/// probability names it.
#[derive(Default)]
pub struct Missing {
    quoted: bool,
}

impl Indicator for Missing {
    fn name(&self) -> String {
        "Missing".to_string()
    }

    fn has_inputs(&self) -> bool {
        self.quoted
    }

    fn initialized(&self) -> bool {
        self.quoted
    }

    fn handle_quote(&mut self, _quote: &QuoteTick) -> anyhow::Result<()> {
        self.quoted = true;
        Ok(())
    }

    fn reset(&mut self) {
        self.quoted = false;
    }
}

impl Feature for Missing {
    fn value(&self) -> f64 {
        f64::NAN
    }
}

/// The arguments of `Missing`: none.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MissingArgs {}

impl IndicatorArgs for MissingArgs {
    type Indicator = Missing;

    fn build(self) -> anyhow::Result<Missing> {
        Ok(Missing::default())
    }
}

/// The missing feature, `[indicators.missing]`.
pub const MISSING: Declared<MissingArgs> = Declared::new("missing");

/// The features of the missing one alone on the quotes of IC's gold.
pub fn missing() -> FeatureSetBuilder {
    FeatureSet::builder().quotes(
        InstrumentId::from(ic::INSTRUMENT_ID),
        MISSING,
        &tables("missing = {}"),
    )
}

/// The calibration of the baselines, the odds `o` of a prediction becoming
/// `o^0.9`, increasing: c(0.5) = 0.5.
pub const CALIBRATION: Calibration = Calibration { a: -0.9, b: 0.0 };

/// A baseline long or short model of the probe deciding on `features` as
/// `config` says, valid from `valid_from`, predicting `trues` tenths
/// whatever the row, calibrated by `CALIBRATION`, expecting a win and a
/// loss of 1R, as a train publishes one: above 0.5 a trade expected to
/// gain. For a test whose models come from no train: the fixture's walk
/// has no edge for a train to publish on (`backtest_train` trains on one).
pub fn baseline_of(
    features: &FeatureSet,
    config: &ModelConfig,
    side: Side,
    valid_from: Timestamp,
    trues: usize,
) -> StoredModel {
    stored_baseline(
        features,
        config,
        features.names().to_vec(),
        Some(side),
        valid_from,
        trues,
    )
}

/// A baseline meta model of the primary deciding on `features` as `config`
/// says (the primary's side and probability after its features), valid
/// from `valid_from`, predicting `trues` tenths whatever the row, as
/// `baseline_of`.
pub fn meta_baseline_of(
    features: &FeatureSet,
    config: &ModelConfig,
    valid_from: Timestamp,
    trues: usize,
) -> StoredModel {
    let names = features
        .names()
        .iter()
        .cloned()
        .chain(PRIMARY_FEATURES.map(String::from))
        .collect();
    stored_baseline(features, config, names, None, valid_from, trues)
}

/// A baseline of `feature_names`, of `side` for a primary and none for a
/// meta, of the probe deciding on `features`, as `baseline_of` says.
fn stored_baseline(
    features: &FeatureSet,
    config: &ModelConfig,
    feature_names: Vec<String>,
    side: Option<Side>,
    valid_from: Timestamp,
    trues: usize,
) -> StoredModel {
    let fingerprint = dataset::fingerprint(
        features,
        config.decision,
        &config.target,
        &requirements(),
        &venue(),
    )
    .unwrap();
    baseline_stored(
        StrategyId::from(STRATEGY),
        fingerprint.to_string(),
        feature_names,
        config,
        side,
        valid_from,
        trues,
    )
}

/// A baseline of `strategy_id`, of its features `feature_names` under
/// `fingerprint`, decided as `config` says, of `side` for a primary and
/// none for a meta, valid from `valid_from`, predicting `trues` tenths
/// whatever the row, as `baseline_of` says, its manifest's baseline the
/// mean of its training labels, `trues` tenths uncalibrated.
pub fn baseline_stored(
    strategy_id: StrategyId,
    fingerprint: String,
    feature_names: Vec<String>,
    config: &ModelConfig,
    side: Option<Side>,
    valid_from: Timestamp,
    trues: usize,
) -> StoredModel {
    let width = feature_names.len();
    let labels = (0..10).map(|i| i < trues).collect();
    let set = TrainingSet::new(width, vec![0.5; 10 * width], labels).unwrap();
    let model = LearnerConfig::Baseline {}.fit(&set).unwrap();
    let probe = vec![0.5; width];
    StoredModel {
        manifest: ModelManifest {
            strategy_id,
            valid_from,
            fingerprint,
            feature_names,
            target: config.target.clone(),
            side,
            learner: LearnerConfig::Baseline {},
            calibration: CALIBRATION,
            baseline: trues as f64 / 10.0,
            payoff: side.map(|_| Payoff {
                win: 1.0,
                loss: 1.0,
            }),
            holdout: Holdout {
                earnings: Earnings::default(),
                log_loss: 0.6,
                baseline_log_loss: 0.69,
                auc: Some(0.5),
            },
            probes: vec![Probe {
                prediction: CALIBRATION.apply(model.predict(&probe).unwrap()),
                row: probe,
            }],
        },
        model: model.save().unwrap(),
    }
}
