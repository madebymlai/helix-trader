//! The dataset run of the model tests: a probe deciding every hour on the
//! last mid of IC's gold opens the targets of each decision through
//! `Backtest::run_dataset`, on IC's hedging venue, and records the target
//! rows published for it; and the random walk of quotes it replays.

use std::{cell::RefCell, rc::Rc};

use crate::common::ic;
use helix_trader::{
    backtest::{Backtest, commission_rate},
    model::{
        decide::{self, DecisionRule},
        features::FeatureSet,
        targets::{Barrier, TargetRow, Targets, TargetsConfig},
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
use nautilus_indicators::average::sma::SimpleMovingAverage;
use nautilus_model::{
    data::{CustomData, NautilusDataType},
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

/// Decides every hour on the last mid of IC's gold and opens the targets
/// of each decision; records the rows published for it.
#[derive(Debug)]
pub struct DatasetProbe {
    core: StrategyCore,
    features: Rc<FeatureSet>,
    targets: Rc<Targets>,
    rows: Rc<RefCell<Vec<TargetRow>>>,
}

nautilus_strategy!(DatasetProbe, {
    fn on_order_filled(&mut self, event: &OrderFilled) {
        self.targets.on_order_filled(event);
    }

    fn on_order_denied(&mut self, event: OrderDenied) {
        self.targets.on_order_denied(&event);
    }

    fn on_order_rejected(&mut self, event: OrderRejected) {
        self.targets.on_order_rejected(&event);
    }

    fn on_position_closed(&mut self, event: PositionClosed) {
        Rc::clone(&self.targets).on_position_closed(self, &event);
    }
});

impl DataActor for DatasetProbe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let features = Rc::clone(&self.features);
        features.register(self);
        let rows = Rc::clone(&self.rows);
        let handler = ShareableMessageHandler::from_typed(move |data: &CustomData| {
            let row = data.data.as_any().downcast_ref::<TargetRow>().unwrap();
            rows.borrow_mut().push(row.clone());
        });
        let topic = get_custom_topic(&TargetRow::data_type(StrategyId::from(STRATEGY)));
        msgbus::subscribe_any(topic.into(), handler, None);
        self.subscribe_quotes(InstrumentId::from(ic::INSTRUMENT_ID), None, None);
        decide::start(self, rule(), SignedDuration::ZERO)
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        if decide::is_decision(event) {
            if let Some(row) = decide::decide(self, &self.features) {
                Rc::clone(&self.targets).open(self, row)?;
            }
            return Ok(());
        }
        Rc::clone(&self.targets).on_time_event(self, event)
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.targets.on_stop();
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

/// A bracket 3.00 away on both sides closed after 2 hours, the longest
/// target, and a horizon of 1 hour, of one ounce.
pub fn targets() -> TargetsConfig {
    TargetsConfig::builder()
        .instrument_id(InstrumentId::from(ic::INSTRUMENT_ID))
        .quantity(Quantity::from(1))
        .barrier(Barrier {
            take_profit: Decimal::from(3),
            stop: Decimal::from(3),
            limit: SignedDuration::from_hours(2),
        })
        .horizon(SignedDuration::from_hours(1))
        .build()
}

/// A deterministic random walk in [0, 1).
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> f64 {
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

/// The replay of a build or a train: the probe over `[from, to]` on IC's hedging
/// venue, 30 USD per million traded, 10M USD; the rows it settled.
pub fn replay(
    catalog: &tempfile::TempDir,
    from: Timestamp,
    to: Timestamp,
) -> anyhow::Result<Vec<TargetRow>> {
    let rows = Rc::new(RefCell::new(vec![]));
    let probe = DatasetProbe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from(STRATEGY)),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
        features: features(),
        targets: Targets::new(targets()).unwrap(),
        rows: Rc::clone(&rows),
    };
    let backtest = Backtest {
        catalog: catalog.path().to_path_buf(),
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
    };
    backtest.run_dataset(probe)?;
    Ok(rows.take())
}
