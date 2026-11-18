//! A strategy declaring book deltas as a tick series and custom data as a
//! custom series, beside IC's gold quotes (`common::ic`): the run replays
//! CME gold's deltas and its `Imbalance`s (`common::glbx`), the custom
//! series under its id, from the catalog to the probe's handlers, in time
//! order with the quotes, and nothing else the catalog stores. The probe
//! subscribes to the quotes of IC's gold, the deltas of CME gold and its
//! imbalances, keyed by its id, and records each it gets.
//! A raw feature on the imbalances reads each from Nautilus' cache, where
//! the data engine puts it before the strategy's handler runs; a feature on
//! the imbalances and the quotes, of a model strategy's dataset run, gets
//! both from its data hooks. One node per thread, so one run per test.

use std::{cell::RefCell, rc::Rc};

use helix_trader::{
    backtest::Backtest,
    model::{
        features::{CustomHandler, Feature, FeatureSet},
        handle::{Mode, SettledRows},
    },
    strategy::{
        Declared, IndicatorArgs, Series,
        requirements::{CustomSeries, DataRequirements, TickSeries},
    },
};
use jiff::SignedDuration;
use nautilus_common::actor::DataActor;
use nautilus_core::UnixNanos;
use nautilus_indicators::indicator::Indicator;
use nautilus_model::{
    data::{CustomData, NautilusDataType, OrderBookDeltas, QuoteTick},
    enums::BookType,
    identifiers::{InstrumentId, StrategyId},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rstest::rstest;

use crate::common::{dataset, features::tables, glbx, ic};

/// What the probe got, at its time.
#[derive(Debug, Clone, PartialEq)]
enum Got {
    Quote(UnixNanos),
    /// An event of the book: its deltas' times.
    Deltas(Vec<UnixNanos>),
    Imbalance(f64, UnixNanos),
}

impl Got {
    fn time(&self) -> UnixNanos {
        match self {
            Self::Quote(at) | Self::Imbalance(_, at) => *at,
            Self::Deltas(times) => *times.last().unwrap(),
        }
    }
}

/// Subscribes to the quotes of IC's gold, the deltas of CME gold and its
/// imbalances, and records each it gets.
#[derive(Debug)]
struct Probe {
    core: StrategyCore,
    got: Rc<RefCell<Vec<Got>>>,
}

nautilus_strategy!(Probe);

impl DataActor for Probe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let gold = InstrumentId::from(glbx::INSTRUMENT_ID);
        self.subscribe_quotes(InstrumentId::from(ic::INSTRUMENT_ID), None, None);
        self.subscribe_book_deltas(gold, BookType::L3_MBO, None, None, false, None);
        self.subscribe_data(glbx::imbalance_type(), None, None);
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        self.got.borrow_mut().push(Got::Quote(quote.ts_init));
        Ok(())
    }

    fn on_book_deltas(&mut self, deltas: &OrderBookDeltas) -> anyhow::Result<()> {
        let times = deltas.deltas.iter().map(|delta| delta.ts_init).collect();
        self.got.borrow_mut().push(Got::Deltas(times));
        Ok(())
    }

    fn on_data(&mut self, data: &CustomData) -> anyhow::Result<()> {
        let imbalance = data
            .data
            .as_any()
            .downcast_ref::<glbx::Imbalance>()
            .expect("an imbalance");
        let got = Got::Imbalance(imbalance.value, imbalance.ts_init);
        self.got.borrow_mut().push(got);
        Ok(())
    }
}

/// Registers a raw feature on CME gold's imbalances at its start,
/// subscribes to them, and records the feature's values at each it gets.
#[derive(Debug)]
struct Reader {
    core: StrategyCore,
    features: Rc<FeatureSet>,
    read: Rc<RefCell<Vec<Option<Vec<f64>>>>>,
}

nautilus_strategy!(Reader);

impl DataActor for Reader {
    fn on_start(&mut self) -> anyhow::Result<()> {
        Rc::clone(&self.features).register(self);
        self.subscribe_data(glbx::imbalance_type(), None, None);
        Ok(())
    }

    fn on_data(&mut self, _data: &CustomData) -> anyhow::Result<()> {
        self.read.borrow_mut().push(self.features.values());
        Ok(())
    }
}

/// A catalog of IC's gold and its quotes, CME gold, its book deltas and
/// its imbalances, all stored over 08:00 to 12:00.
fn catalog() -> tempfile::TempDir {
    let dir = ic::catalog();
    glbx::store(dir.path());
    glbx::store_deltas(dir.path());
    glbx::store_imbalances(dir.path());
    dir
}

/// Runs the probe over `[09:00, 12:00]` on `catalog`, declaring `ticks`,
/// `custom` and no bar types, and returns what it got, in order.
fn run(catalog: &tempfile::TempDir, ticks: Vec<TickSeries>, custom: Vec<CustomSeries>) -> Vec<Got> {
    let got = Rc::new(RefCell::new(Vec::new()));
    let probe = Probe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("PROBE-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
        got: Rc::clone(&got),
    };
    let backtest = Backtest {
        bar_types: vec![],
        ticks,
        custom,
        ..ic::backtest(catalog)
    };
    backtest.run(probe).unwrap();
    got.take()
}

fn tick(data: NautilusDataType, instrument_id: &str) -> TickSeries {
    TickSeries {
        data,
        instrument_id: InstrumentId::from(instrument_id),
    }
}

/// CME gold's imbalances, a custom series under its id.
fn imbalances() -> CustomSeries {
    CustomSeries::new(glbx::IMBALANCE, glbx::INSTRUMENT_ID.to_string(), None).unwrap()
}

fn at(time: &str) -> UnixNanos {
    ic::at(time).into()
}

/// What `got` holds but the quotes.
fn unquoted(got: &[Got]) -> Vec<Got> {
    got.iter()
        .filter(|got| !matches!(got, Got::Quote(_)))
        .cloned()
        .collect()
}

#[rstest]
fn declared_deltas_and_custom_data_of_the_run_reach_the_handlers_in_time_order() {
    let catalog = catalog();

    let got = run(
        &catalog,
        vec![
            tick(NautilusDataType::QuoteTick, ic::INSTRUMENT_ID),
            tick(NautilusDataType::OrderBookDelta, glbx::INSTRUMENT_ID),
        ],
        vec![imbalances()],
    );

    // The run's window only: not the event of 08:40 nor the imbalance of
    // 08:50; the two deltas of 09:20 as one event.
    assert_eq!(
        unquoted(&got),
        [
            Got::Deltas(vec![at("09:20:00"), at("09:20:00")]),
            Got::Imbalance(-0.25, at("09:30:00")),
            Got::Deltas(vec![at("10:40:00")]),
            Got::Imbalance(0.75, at("11:00:00")),
        ]
    );
    // Among the 360 quotes of the run, each at its time.
    assert_eq!(got.len(), 360 + 4);
    let times: Vec<UnixNanos> = got.iter().map(Got::time).collect();
    assert!(times.is_sorted(), "{got:?}");
}

#[rstest]
fn deltas_and_custom_data_the_strategy_does_not_declare_are_not_replayed() {
    let catalog = catalog();

    let got = run(
        &catalog,
        vec![tick(NautilusDataType::QuoteTick, ic::INSTRUMENT_ID)],
        vec![],
    );

    assert_eq!(unquoted(&got), []);
    assert_eq!(got.len(), 360);
}

/// The feature reads each imbalance the run replays as the strategy gets
/// it, the data engine having cached it first, from no strategy code.
#[rstest]
fn a_raw_feature_reads_each_replayed_imbalance_from_the_cache() {
    let catalog = catalog();
    let features = FeatureSet::builder()
        .custom(&imbalances(), "imbalance", |data| {
            data.data
                .as_any()
                .downcast_ref::<glbx::Imbalance>()
                .map_or(f64::NAN, |imbalance| imbalance.value)
        })
        .build()
        .unwrap();
    let read = Rc::new(RefCell::new(Vec::new()));
    let reader = Reader {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("READER-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
        features,
        read: Rc::clone(&read),
    };
    let backtest = Backtest {
        bar_types: vec![],
        ticks: vec![tick(NautilusDataType::QuoteTick, ic::INSTRUMENT_ID)],
        custom: vec![imbalances()],
        ..ic::backtest(&catalog)
    };

    backtest.run(reader).unwrap();

    assert_eq!(read.take(), [Some(vec![-0.25]), Some(vec![0.75])]);
}

thread_local! {
    /// What the `Arrivals` feature of the run on this thread got.
    static ARRIVED: RefCell<Vec<Got>> = const { RefCell::new(Vec::new()) };
}

/// A feature recording each quote and imbalance it gets in `ARRIVED`,
/// never initialized.
struct Arrivals;

impl Indicator for Arrivals {
    fn name(&self) -> String {
        "Arrivals".to_string()
    }

    fn has_inputs(&self) -> bool {
        false
    }

    fn initialized(&self) -> bool {
        false
    }

    fn handle_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        ARRIVED.with_borrow_mut(|got| got.push(Got::Quote(quote.ts_init)));
        Ok(())
    }

    fn reset(&mut self) {}
}

impl Feature for Arrivals {
    fn value(&self) -> f64 {
        0.0
    }

    fn custom_handler(&mut self) -> Option<&mut dyn CustomHandler> {
        Some(self)
    }
}

impl CustomHandler for Arrivals {
    fn handle_custom(&mut self, data: &CustomData) -> anyhow::Result<()> {
        let imbalance = data
            .data
            .as_any()
            .downcast_ref::<glbx::Imbalance>()
            .ok_or_else(|| anyhow::anyhow!("{} is no imbalance", data.data_type))?;
        let got = Got::Imbalance(imbalance.value, imbalance.ts_init);
        ARRIVED.with_borrow_mut(|arrived| arrived.push(got));
        Ok(())
    }
}

/// The arguments of `Arrivals`: none.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ArrivalsArgs {}

impl IndicatorArgs for ArrivalsArgs {
    type Indicator = Arrivals;

    fn build(self) -> anyhow::Result<Arrivals> {
        Ok(Arrivals)
    }
}

/// `[indicators.arrivals]`.
const ARRIVALS: Declared<ArrivalsArgs> = Declared::new("arrivals");

/// A model strategy's dataset run over `[09:00, 12:00]`, its one feature on
/// IC's gold quotes and CME gold's imbalances: the strategy's data hooks
/// (`model_data_hooks!`) hand the feature each replayed imbalance among
/// the quotes, in time order, as a strategy's handlers get them.
#[rstest]
fn a_feature_on_custom_data_and_quotes_gets_both_replayed_in_time_order() {
    let catalog = catalog();
    let quotes = tick(NautilusDataType::QuoteTick, ic::INSTRUMENT_ID);
    let features = FeatureSet::builder()
        .feature(
            &[Series::Ticks(quotes.clone()), Series::Custom(imbalances())],
            ARRIVALS,
            &tables("arrivals = {}"),
        )
        .build()
        .unwrap();
    let requirements = DataRequirements {
        bar_types: vec![],
        ticks: vec![quotes],
        custom: vec![imbalances()],
        warmup: SignedDuration::ZERO,
    };
    let (probe, _) = dataset::probe_reading(
        features,
        &dataset::config(),
        &requirements,
        &Mode::Dataset {
            rows: SettledRows::default(),
        },
    );
    let backtest = Backtest {
        ticks: requirements.ticks.clone(),
        custom: requirements.custom,
        ..dataset::backtest(catalog.path(), ic::at("09:00:00"), ic::at("12:00:00"))
    };

    backtest.run_dataset(probe).unwrap();

    let got = ARRIVED.take();
    assert_eq!(
        unquoted(&got),
        [
            Got::Imbalance(-0.25, at("09:30:00")),
            Got::Imbalance(0.75, at("11:00:00")),
        ]
    );
    assert_eq!(got.len(), 360 + 2);
    let times: Vec<UnixNanos> = got.iter().map(Got::time).collect();
    assert!(times.is_sorted(), "{got:?}");
}
