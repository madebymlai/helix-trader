//! Provides the model runs of the model tests.
//!
//! A probe decides every hour on the last mid of IC's gold, through its model handle
//! (`model::handle`). It runs two ways:
//! - In a dataset run (`Backtest::run_dataset`), it opens each decision's target and collects the
//!   rows the targets settle.
//! - On the models of a store, it trades and records its decisions' rows, trades and closed
//!   positions.
//!
//! On IC's hedging venue, it does both. The module also provides the random walk of quotes they
//! replay.

use std::{cell::RefCell, path::Path, rc::Rc};

use helix_trader::{
    backtest::{Backtest, Commission},
    model::{
        blueprint::ModelBlueprint,
        calibration::{Ksplit, Line},
        config::{Margins, ModelConfig},
        dataset::DatasetVenue,
        decide::{self, DecisionRule, FeatureRow},
        features::{Feature, FeatureSet, FeatureSetBuilder, PICKED_FEATURES, side_names},
        handle::{Decision, Mode, ModelHandle, SettledRows},
        learner::{LearnerConfig, TrainingSet},
        store::{ModelManifest, Probe, StoredModel, StoredModelSet},
        targets::{BracketSpec, Distance, Side, Target, TargetConfig, TargetRow},
    },
    strategy::{
        Declared, IndicatorArgs, IndicatorsConfig, Series,
        conversions::Conversions,
        orders::{self, EntryRisk},
        requirements::{DataRequirements, TickSeries},
        risk::{R, Stake},
    },
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
    events::{OrderCanceled, OrderDenied, OrderFilled, OrderRejected, PositionClosed},
    identifiers::{InstrumentId, StrategyId},
    types::{Money, Quantity},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rust_decimal::Decimal;

use crate::common::{
    features::{SmaArgs, tables},
    ic,
};

pub(crate) const STRATEGY: &str = "DATASET-001";

#[derive(Debug)]
pub(crate) struct ModelProbe {
    core: StrategyCore,
    model: Rc<ModelHandle>,

    stake: Stake,
    record: Rc<RefCell<Record>>,
}

#[derive(Debug, Default)]
pub(crate) struct Record {
    pub rows: Vec<FeatureRow>,

    pub trades: Vec<(UnixNanos, Decision)>,

    pub closed: Vec<(UnixNanos, Money)>,

    /// Each entry filled, with the risk its tags say it takes (`orders::entry_risk`).
    pub entries: Vec<(UnixNanos, Quantity, Option<EntryRisk>)>,
}

nautilus_strategy!(ModelProbe, {
    fn on_order_canceled(&mut self, event: &OrderCanceled) {
        self.model.on_order_canceled(self, event);
    }

    fn on_order_denied(&mut self, event: OrderDenied) {
        Rc::clone(&self.model).on_order_denied(self, &event);
    }

    fn on_order_filled(&mut self, event: &OrderFilled) {
        let risk = self
            .cache()
            .order(&event.client_order_id)
            .and_then(|order| orders::entry_risk(&order));
        if risk.is_some() {
            self.record
                .borrow_mut()
                .entries
                .push((event.ts_event, event.last_qty, risk));
        }
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

    helix_trader::model_data_hooks!(model);

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        if decide::is_decision(event) {
            if let Some(decision) = Rc::clone(&self.model).decide(self)? {
                let now = self.clock().timestamp_ns();
                self.record.borrow_mut().trades.push((now, decision));
                let unconverted = Conversions::default().rates();
                decision.trade().send(self, self.stake, unconverted)?;
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

pub(crate) fn rule() -> DecisionRule {
    DecisionRule::Every(SignedDuration::from_hours(1))
}

pub(crate) const MID: Declared<SmaArgs> = Declared::new("mid");

pub(crate) fn mid_tables() -> IndicatorsConfig {
    tables(r#"mid = { period = 1, price_type = "MID" }"#)
}

pub(crate) fn features() -> Rc<FeatureSet> {
    FeatureSet::builder()
        .feature(
            &[Series::quotes(InstrumentId::from(ic::INSTRUMENT_ID))],
            MID,
            &mid_tables(),
        )
        .build()
        .unwrap()
}

/// The risk is 4 USD of the dataset balance of 10M USD, which buys one ounce on a stop of 2 to 4 USD.
pub(crate) const RISK: &str = "0.00004%";

pub(crate) fn target() -> TargetConfig {
    TargetConfig::builder()
        .instrument_id(InstrumentId::from(ic::INSTRUMENT_ID))
        .risk(RISK.parse().unwrap())
        .spec(
            Target::builder()
                .bracket(BracketSpec {
                    take_profit: Distance::BasisPoints(Decimal::new(75, 1)),
                    stop: Distance::BasisPoints(Decimal::new(75, 1)),
                    atr: None,
                })
                .limit(SignedDuration::from_hours(2))
                .build(),
        )
        .build()
}

/// Builds the config of the fake learner: a window of 46 hours, the last 12 calibrated on.
pub(crate) fn config() -> ModelConfig {
    ModelConfig::builder()
        .decision(rule())
        .target(target())
        .retrain(SignedDuration::from_hours(24))
        .dataset(venue())
        .window(SignedDuration::from_hours(46))
        .calibration(SignedDuration::from_hours(12))
        .margins(Margins {
            long: Some(R::new(0.0)),
            short: Some(R::new(0.0)),
        })
        .min_rows(1)
        .learner(LearnerConfig::Fake {})
        .build()
}

/// Lets an hourly decision trade even when its predecessor reached its time limit.
pub(crate) fn hourly_trades() -> ModelConfig {
    let mut config = config();
    config.target.spec.limit = SignedDuration::from_mins(30);
    config
}

pub(crate) struct Lcg(pub u64);

impl Lcg {
    pub(crate) fn next(&mut self) -> f64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
}

pub(crate) fn walk(from: Timestamp, to: Timestamp) -> tempfile::TempDir {
    ic::catalog_of(walk_quotes(from, to), from, to)
}

pub(crate) fn walk_quotes(from: Timestamp, to: Timestamp) -> Vec<QuoteTick> {
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

fn probe(
    features: Rc<FeatureSet>,
    config: &ModelConfig,
    mode: &Mode,
) -> (ModelProbe, Rc<RefCell<Record>>) {
    probe_reading(features, config, &requirements(), mode)
}

pub(crate) fn probe_reading(
    features: Rc<FeatureSet>,
    config: &ModelConfig,
    requirements: &DataRequirements,
    mode: &Mode,
) -> (ModelProbe, Rc<RefCell<Record>>) {
    probe_staking(features, config, requirements, mode, Decimal::ONE)
}

fn probe_staking(
    features: Rc<FeatureSet>,
    config: &ModelConfig,
    requirements: &DataRequirements,
    mode: &Mode,
    multiple: Decimal,
) -> (ModelProbe, Rc<RefCell<Record>>) {
    let record = Rc::new(RefCell::new(Record::default()));
    let probe = ModelProbe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from(STRATEGY)),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
        model: ModelHandle::new(
            &blueprint_reading(features, config.clone(), requirements.clone()),
            mode,
        )
        .unwrap(),
        stake: Stake::new(config.target.risk).times(multiple).unwrap(),
        record: Rc::clone(&record),
    };
    (probe, record)
}

pub(crate) fn blueprint(features: Rc<FeatureSet>, config: ModelConfig) -> ModelBlueprint {
    blueprint_reading(features, config, requirements())
}

fn blueprint_reading(
    features: Rc<FeatureSet>,
    config: ModelConfig,
    requirements: DataRequirements,
) -> ModelBlueprint {
    ModelBlueprint::new(StrategyId::from(STRATEGY), features, config, requirements).unwrap()
}

pub(crate) fn requirements() -> DataRequirements {
    DataRequirements {
        bar_types: vec![],
        ticks: vec![TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: InstrumentId::from(ic::INSTRUMENT_ID),
        }],
        custom: vec![],
        warmup: SignedDuration::ZERO,
    }
}

pub(crate) fn venue() -> DatasetVenue {
    DatasetVenue {
        commission: Commission::Rate(Decimal::new(3, 5)),
        balance: Money::from("10000000 USD"),
        latency: None,
    }
}

pub(crate) fn backtest(catalog: &Path, from: Timestamp, to: Timestamp) -> Backtest {
    Backtest {
        catalog: catalog.to_path_buf(),
        bar_types: vec![],
        ticks: requirements().ticks,
        custom: vec![],
        start: from,
        end: to,
        commission: venue().commission,
        latency: venue().latency,
        pairs: vec![],
        balance: venue().balance,
        logging: LoggerConfig::default(),
    }
}

pub(crate) fn replay(
    catalog: &tempfile::TempDir,
    from: Timestamp,
    to: Timestamp,
) -> anyhow::Result<Vec<TargetRow>> {
    replay_on(catalog, features(), from, to)
}

pub(crate) fn replay_on(
    catalog: &tempfile::TempDir,
    features: Rc<FeatureSet>,
    from: Timestamp,
    to: Timestamp,
) -> anyhow::Result<Vec<TargetRow>> {
    replay_of(catalog, features, &config(), from, to)
}

pub(crate) fn replay_of(
    catalog: &tempfile::TempDir,
    features: Rc<FeatureSet>,
    config: &ModelConfig,
    from: Timestamp,
    to: Timestamp,
) -> anyhow::Result<Vec<TargetRow>> {
    let rows = SettledRows::default();
    let (probe, _) = probe(features, config, &Mode::Dataset { rows: rows.clone() });
    backtest(catalog.path(), from, to).run_dataset(probe)?;
    rows.take()
}

pub(crate) fn trade(
    catalog: &tempfile::TempDir,
    from: Timestamp,
    to: Timestamp,
    config: &ModelConfig,
    models: &Path,
) -> Record {
    trade_on(catalog, features(), from, to, config, models)
}

pub(crate) fn trade_on(
    catalog: &tempfile::TempDir,
    features: Rc<FeatureSet>,
    from: Timestamp,
    to: Timestamp,
    config: &ModelConfig,
    models: &Path,
) -> Record {
    let account = Account {
        balance: venue().balance,
        multiple: Decimal::ONE,
    };
    trade_staking(catalog, features, (from, to), config, models, &account)
}

pub(crate) struct Account {
    pub balance: Money,
    pub multiple: Decimal,
}

pub(crate) fn trade_staking(
    catalog: &tempfile::TempDir,
    features: Rc<FeatureSet>,
    (from, to): (Timestamp, Timestamp),
    config: &ModelConfig,
    models: &Path,
    account: &Account,
) -> Record {
    let (probe, record) = probe_staking(
        features,
        config,
        &requirements(),
        &Mode::Backtest {
            models: models.to_path_buf(),
        },
        account.multiple,
    );
    let backtest = Backtest {
        pairs: vec![],
        balance: account.balance,
        ..backtest(catalog.path(), from, to)
    };
    backtest.run(probe).unwrap();
    record.take()
}

/// Initialized by the first quote with its value missing, so that the fake learner predicts the
/// mean of its training labels and each model's prediction names it.
#[derive(Default)]
pub(crate) struct Missing {
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

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MissingArgs {}

impl IndicatorArgs for MissingArgs {
    type Indicator = Missing;

    fn build(self) -> anyhow::Result<Missing> {
        Ok(Missing::default())
    }
}

pub(crate) const MISSING: Declared<MissingArgs> = Declared::new("missing");

pub(crate) fn missing() -> FeatureSetBuilder {
    FeatureSet::builder().feature(
        &[Series::quotes(InstrumentId::from(ic::INSTRUMENT_ID))],
        MISSING,
        &tables("missing = {}"),
    )
}

pub(crate) const CALIBRATION: Line = Line {
    alpha: 0.1,
    beta: 0.5,
};

pub(crate) fn r(tenths: usize) -> f64 {
    (tenths as f64 - 5.0) / 10.0
}

/// Returns the calibration that leaves each side's prediction as it is, exact to the bit on one
/// side: the level line is `C' = C`, and for two sides the side call is `D' = D`.
pub(crate) fn unchanged(config: &ModelConfig) -> Ksplit {
    Ksplit {
        level: Line {
            alpha: 0.0,
            beta: 1.0,
        },
        side_call: (config.margins.sides().count() == 2).then_some(1.0),
    }
}

pub(crate) fn model_set(
    config: &ModelConfig,
    valid_from: Timestamp,
    sides: Vec<StoredModel>,
    meta: Option<StoredModel>,
) -> StoredModelSet {
    StoredModelSet {
        valid_from,
        sides,
        calibration: unchanged(config),
        meta,
    }
}

/// Builds a baseline side model of the probe, for tests that set the models' predictions, which no
/// train on the fixture's edgeless walk would. It predicts an R of `r(tenths)` whatever the row.
pub(crate) fn baseline_of(
    features: &Rc<FeatureSet>,
    config: &ModelConfig,
    side: Side,
    tenths: usize,
) -> StoredModel {
    let names = side_names(features.names(), &config.target.payoff_names());
    let width = names.len();
    let set = TrainingSet::new(width, vec![0.5; width], vec![r(tenths)]).unwrap();
    stored_baseline(features, config, names, Some(side), &set)
}

/// Builds a baseline meta model of the sides, predicting an R of `r(tenths)` whatever the row.
pub(crate) fn meta_baseline_of(
    features: &Rc<FeatureSet>,
    config: &ModelConfig,
    tenths: usize,
) -> StoredModel {
    let names: Vec<String> = features
        .names()
        .iter()
        .cloned()
        .chain(PICKED_FEATURES.map(String::from))
        .collect();
    let width = names.len();
    let set = TrainingSet::new(width, vec![0.5; width], vec![r(tenths)]).unwrap();
    stored_baseline(features, config, names, None, &set)
}

fn stored_baseline(
    features: &Rc<FeatureSet>,
    config: &ModelConfig,
    feature_names: Vec<String>,
    side: Option<Side>,
    set: &TrainingSet,
) -> StoredModel {
    let fingerprint = blueprint(Rc::clone(features), config.clone())
        .fingerprint()
        .unwrap();
    baseline_stored(fingerprint.to_string(), feature_names, config, side, set)
}

/// Builds a model that predicts the mean of its labels, its manifest's baseline, whatever the row.
pub(crate) fn baseline_stored(
    fingerprint: String,
    feature_names: Vec<String>,
    config: &ModelConfig,
    side: Option<Side>,
    set: &TrainingSet,
) -> StoredModel {
    let width = feature_names.len();
    let model = LearnerConfig::Baseline {}.fit(set).unwrap();
    let probe = vec![0.5; width];
    let prediction = model.predict(&probe).unwrap();
    let calibration = side.is_none().then_some(CALIBRATION);
    StoredModel {
        manifest: ModelManifest {
            fingerprint,
            feature_names,
            target: config.target.clone(),
            side,
            learner: LearnerConfig::Baseline {},
            calibration,
            baseline: prediction,
            out_of_fold: None,
            probes: vec![Probe {
                prediction,
                row: probe,
            }],
        },
        model: model.save().unwrap(),
    }
}
