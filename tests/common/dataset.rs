//! The model runs of the model tests: a probe deciding every hour on the
//! last mid of IC's gold through its model handle (`model::handle`),
//! either opening the targets of each decision in a dataset run
//! (`Backtest::run_dataset`), the targets subscribing to the bars of
//! their ATR, if any, the rows they settle collected, or trading on the
//! models of a store, its decisions' rows and trades recorded; on IC's
//! hedging venue both. And the random walk of quotes they replay.

use std::{cell::RefCell, path::Path, rc::Rc};

use crate::common::ic;
use helix_trader::{
    backtest::{Backtest, commission_rate},
    model::{
        config::{ModelConfig, Thresholds},
        dataset::Commission,
        decide::{self, DecisionRule, FeatureRow},
        features::{Feature, FeatureSet},
        handle::{Mode, ModelHandle, SettledRows, Trade},
        learner::LearnerConfig,
        targets::{Barrier, Distance, Label, TargetRow, TargetsConfig},
    },
    strategy::TickSeries,
};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::DataActor,
    logging::logger::LoggerConfig,
    msgbus::{self, ShareableMessageHandler, switchboard::get_custom_topic},
    timer::TimeEvent,
};
use nautilus_core::UnixNanos;
use nautilus_indicators::{average::sma::SimpleMovingAverage, indicator::Indicator};
use nautilus_model::{
    data::{CustomData, NautilusDataType, QuoteTick},
    enums::{OmsType, PriceType},
    events::{OrderDenied, OrderFilled, OrderRejected, PositionClosed},
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
/// they make, which it does not send.
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
    /// The trade of each decision making one, and its time.
    pub trades: Vec<(UnixNanos, Trade)>,
}

nautilus_strategy!(ModelProbe, {
    fn on_order_filled(&mut self, event: &OrderFilled) {
        self.model.on_order_filled(event);
    }

    fn on_order_denied(&mut self, event: OrderDenied) {
        self.model.on_order_denied(&event);
    }

    fn on_order_rejected(&mut self, event: OrderRejected) {
        self.model.on_order_rejected(&event);
    }

    fn on_position_closed(&mut self, event: PositionClosed) {
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
        self.subscribe_quotes(InstrumentId::from(ic::INSTRUMENT_ID), None, None);
        Rc::clone(&self.model).start(self, &[])
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        if decide::is_decision(event) {
            if let Some(trade) = Rc::clone(&self.model).decide(self)? {
                let now = self.clock().timestamp_ns();
                self.record.borrow_mut().trades.push((now, trade));
            }
            return Ok(());
        }
        Rc::clone(&self.model).on_time_event(self, event)
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.model.on_stop();
        Ok(())
    }
}

pub fn rule() -> DecisionRule {
    DecisionRule::Every(SignedDuration::from_hours(1))
}

/// The probe's one feature, the last mid of IC's gold.
pub fn features() -> Rc<FeatureSet> {
    FeatureSet::builder()
        .quotes(
            InstrumentId::from(ic::INSTRUMENT_ID),
            "mid",
            SimpleMovingAverage::new(1, Some(PriceType::Mid)),
        )
        .build()
        .unwrap()
}

/// A bracket 7.5 bps away on both sides closed after 2 hours, the longest
/// target, and a horizon of 1 hour, of one ounce.
pub fn targets() -> TargetsConfig {
    TargetsConfig::builder()
        .instrument_id(InstrumentId::from(ic::INSTRUMENT_ID))
        .quantity(Quantity::from(1))
        .barrier(Barrier {
            take_profit: Distance::BasisPoints(Decimal::new(75, 1)),
            stop: Distance::BasisPoints(Decimal::new(75, 1)),
            limit: SignedDuration::from_hours(2),
            atr: None,
        })
        .horizon(SignedDuration::from_hours(1))
        .build()
}

/// The probe's decisions and targets, learning whether the bracket's long
/// gains, on a window of 46 hours, the last 12 held out, by the fake
/// learner from an AUC of 0.5; a long from 0.6, a short to 0.4.
pub fn config() -> ModelConfig {
    ModelConfig::builder()
        .decision(rule())
        .targets(targets())
        .label(Label::Barrier)
        .retrain(SignedDuration::from_hours(24))
        .window(SignedDuration::from_hours(46))
        .holdout(SignedDuration::from_hours(12))
        .thresholds(Thresholds {
            long: 0.6,
            short: 0.4,
        })
        .min_score(0.5)
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

/// IC's gold over `[from, to)`, a quote every 5 s, the bid a random walk
/// of up to 0.30 a step, the ask 0.20 above.
pub fn walk(from: Timestamp, to: Timestamp) -> tempfile::TempDir {
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
    ic::catalog_of(quotes, from, to)
}

/// The probe deciding on `features` as `config` says, in `mode`, and what
/// it records.
fn probe(
    features: Rc<FeatureSet>,
    config: &ModelConfig,
    mode: Mode,
) -> (ModelProbe, Rc<RefCell<Record>>) {
    let record = Rc::new(RefCell::new(Record::default()));
    let probe = ModelProbe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from(STRATEGY)),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
        model: ModelHandle::new(StrategyId::from(STRATEGY), features, config, mode).unwrap(),
        record: Rc::clone(&record),
    };
    (probe, record)
}

/// The commission of `backtest`, as given: 30 USD per million traded.
pub fn commission() -> Commission {
    Commission::Rate("0.00003".to_string())
}

/// The run of the probe over `[from, to]` on IC's hedging venue, 30 USD
/// per million traded, 10M USD.
pub fn backtest(catalog: &Path, from: Timestamp, to: Timestamp) -> Backtest {
    Backtest {
        catalog: catalog.to_path_buf(),
        bar_types: vec![],
        ticks: vec![TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: InstrumentId::from(ic::INSTRUMENT_ID),
        }],
        start: from,
        end: to,
        warmup: SignedDuration::ZERO,
        fee_model: commission_rate(Decimal::new(3, 5)).unwrap(),
        balance: Money::from("10000000 USD"),
        leverage: Decimal::ONE,
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
            commission: commission(),
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
