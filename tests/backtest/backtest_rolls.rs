//! Tests a strategy that declares a rolled root (`common::rolls`).
//!
//! The root GC.GLBX rolls from GCQ26.GLBX (August, `PRE`) to GCZ26.GLBX (December, `POST`) at 00:00
//! UTC on 29 July 2026. The run goes from 23:00 to 01:00, across the roll, and replays each
//! contract's series over its slice only: August's up to 1 ns before the roll, December's from it.
//! A `Roller` tells the strategy of the roll, and its `Subscriptions` switch contracts. The
//! contracts' imbalances form a custom series under the root's id, which rolls alike.
//!
//! August's ticks are at 23:30, 23:45, 23:59:59.999999999 and, past its slice, 00:00, 00:10 and
//! 00:30. December's are at 23:15 and 23:50, before its slice, then 00:00, 00:20 and 00:45.

use std::{cell::RefCell, num::NonZeroUsize, path::Path, rc::Rc, sync::Arc};

use helix_trader::{
    backtest::{Backtest, Commission},
    model::features::FeatureSet,
    rolls::{Roller, Transition},
    strategy::{
        requirements::{CustomSeries, TickSeries},
        subscriptions::Subscriptions,
    },
};
use nautilus_common::{actor::DataActor, logging::logger::LoggerConfig, timer::TimeEvent};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{
        BookOrder, CustomData, DataType, NautilusDataType, OrderBookDelta, OrderBookDeltas,
        OrderBookDepth, TradeTick,
    },
    enums::{BookAction, BookType, OrderSide, RecordFlag},
    identifiers::{InstrumentId, StrategyId},
    types::{Money, Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rstest::rstest;
use rust_decimal::Decimal;

use crate::common::{
    glbx::{self, Imbalance},
    rolls::{self, NANOSECOND, POST, PRE, ROOT, roll, tuesday, wednesday},
};

#[derive(Debug, Clone, PartialEq)]
enum Got {
    Alerts(Vec<UnixNanos>),
    Tick(NautilusDataType, InstrumentId, UnixNanos),
    Roll(Transition, UnixNanos),
    Read(Option<Vec<f64>>),
}

#[derive(Debug)]
enum Subscribe {
    Rolled(Roller, Subscriptions),
    /// Works as `Rolled`, with its features registered and rolled as a model strategy's handle does.
    Featured(Roller, Subscriptions, Rc<FeatureSet>),
    /// Subscribes directly to both contracts' trades and book depth.
    Contracts,
    ContractImbalances,
}

#[derive(Debug)]
struct RollProbe {
    core: StrategyCore,
    subscribe: Subscribe,
    got: Rc<RefCell<Vec<Got>>>,
}

nautilus_strategy!(RollProbe);

impl RollProbe {
    fn record(&self, observed: Got) {
        self.got.borrow_mut().push(observed);
    }
}

impl DataActor for RollProbe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        match &self.subscribe {
            Subscribe::Rolled(roller, subscriptions) => {
                let (roller, subscriptions) = (roller.clone(), subscriptions.clone());
                let now = self.clock().utc_now();
                subscriptions.start(self, now);
                roller.start(self, now)?;
            }
            Subscribe::Featured(roller, subscriptions, features) => {
                let (roller, subscriptions) = (roller.clone(), subscriptions.clone());
                Rc::clone(features).register(self);
                let now = self.clock().utc_now();
                subscriptions.start(self, now);
                roller.start(self, now)?;
            }
            Subscribe::ContractImbalances => {
                for id in [PRE, POST] {
                    let data_type = DataType::new(glbx::IMBALANCE, None, Some(id.to_string()));
                    self.subscribe_data(data_type, None, None);
                }
            }
            Subscribe::Contracts => {
                for id in [PRE, POST].map(InstrumentId::from) {
                    self.subscribe_trades(id, None, None);
                    self.subscribe_book_depth(
                        id,
                        BookType::L2_MBP,
                        NonZeroUsize::new(10),
                        None,
                        false,
                        None,
                    );
                }
            }
        }
        let clock = self.clock();
        let alerts = clock
            .timer_names()
            .iter()
            .filter_map(|name| clock.next_time_ns(name))
            .collect();
        drop(clock);
        self.record(Got::Alerts(alerts));
        Ok(())
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        let (roller, subscriptions, features) = match &self.subscribe {
            Subscribe::Rolled(roller, subscriptions) => {
                (roller.clone(), subscriptions.clone(), None)
            }
            Subscribe::Featured(roller, subscriptions, features) => (
                roller.clone(),
                subscriptions.clone(),
                Some(Rc::clone(features)),
            ),
            Subscribe::Contracts | Subscribe::ContractImbalances => return Ok(()),
        };
        if let Some(roll) = roller.on_time_event(self, event)? {
            subscriptions.roll(self, &roll);
            let now = self.clock().timestamp_ns();
            self.record(Got::Roll(roll, now));
            if let Some(features) = features {
                features.roll(&roll);
                self.record(Got::Read(features.values()));
            }
        }
        Ok(())
    }

    fn on_trade(&mut self, trade: &TradeTick) -> anyhow::Result<()> {
        self.record(Got::Tick(
            NautilusDataType::TradeTick,
            trade.instrument_id,
            trade.ts_init,
        ));
        Ok(())
    }

    fn on_book_depth(&mut self, depth: &OrderBookDepth) -> anyhow::Result<()> {
        self.record(Got::Tick(
            NautilusDataType::OrderBookDepth,
            depth.instrument_id,
            depth.ts_init,
        ));
        Ok(())
    }

    fn on_book_deltas(&mut self, deltas: &OrderBookDeltas) -> anyhow::Result<()> {
        self.record(Got::Tick(
            NautilusDataType::OrderBookDelta,
            deltas.instrument_id,
            deltas.ts_init,
        ));
        Ok(())
    }

    fn on_data(&mut self, data: &CustomData) -> anyhow::Result<()> {
        let id = data.data_type.identifier().expect("an instrument id");
        self.record(Got::Tick(
            imbalances(),
            InstrumentId::from(id),
            data.data.ts_init(),
        ));
        if let Subscribe::Featured(_, _, features) = &self.subscribe {
            self.record(Got::Read(features.values()));
        }
        Ok(())
    }
}

fn ticks(instrument_id: &str) -> Vec<TickSeries> {
    [
        NautilusDataType::TradeTick,
        NautilusDataType::OrderBookDepth,
    ]
    .map(|data| TickSeries {
        data,
        instrument_id: InstrumentId::from(instrument_id),
    })
    .to_vec()
}

fn rolled(ticks: Vec<TickSeries>) -> Subscribe {
    let instrument_ids = ticks.iter().map(|tick| tick.instrument_id).collect();
    Subscribe::Rolled(
        Roller::replayed(instrument_ids),
        Subscriptions::new(vec![], ticks, vec![]),
    )
}

fn backtest(catalog: &tempfile::TempDir, ticks: Vec<TickSeries>) -> Backtest {
    Backtest {
        catalog: catalog.path().to_path_buf(),
        bar_types: vec![],
        ticks,
        custom: vec![],
        start: tuesday("23:00:00"),
        end: wednesday("01:00:00"),
        commission: Commission::Rate(Decimal::ZERO),
        latency: None,
        pairs: vec![],
        balance: Money::from("1000000 USD"),
        logging: LoggerConfig::default(),
    }
}

fn run(backtest: &Backtest, subscribe: Subscribe) -> Vec<Got> {
    let got = Rc::new(RefCell::new(Vec::new()));
    let probe = RollProbe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("PROBE-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
        subscribe,
        got: Rc::clone(&got),
    };
    backtest.run(probe).unwrap();
    got.take()
}

fn tick(data: NautilusDataType, instrument_id: &str, time: &str) -> Got {
    let time = if let Some(time) = time.strip_prefix("thu ") {
        tuesday(time)
    } else {
        wednesday(time)
    };
    Got::Tick(data, InstrumentId::from(instrument_id), time.into())
}

fn before_roll() -> UnixNanos {
    (roll() - NANOSECOND).into()
}

#[rstest]
fn a_rolled_root_replays_each_contract_only_within_its_slice() {
    let catalog = rolls::catalog();

    let got = run(&backtest(&catalog, ticks(ROOT)), Subscribe::Contracts);

    assert_eq!(
        got,
        [
            Got::Alerts(vec![]),
            tick(NautilusDataType::TradeTick, PRE, "thu 23:30:00"),
            tick(NautilusDataType::OrderBookDepth, PRE, "thu 23:45:00"),
            tick(NautilusDataType::TradeTick, PRE, "thu 23:59:59.999999999"),
            tick(NautilusDataType::TradeTick, POST, "00:00:00"),
            tick(NautilusDataType::OrderBookDepth, POST, "00:20:00"),
            tick(NautilusDataType::TradeTick, POST, "00:45:00"),
        ]
    );
}

#[rstest]
fn the_roller_tells_the_strategy_of_the_roll_at_which_its_subscriptions_switch() {
    let catalog = rolls::catalog();
    let backtest = backtest(&catalog, ticks(ROOT));

    let got = run(&backtest, rolled(ticks(ROOT)));

    assert_eq!(
        got,
        [
            Got::Alerts(vec![before_roll()]),
            tick(NautilusDataType::TradeTick, PRE, "thu 23:30:00"),
            tick(NautilusDataType::OrderBookDepth, PRE, "thu 23:45:00"),
            tick(NautilusDataType::TradeTick, PRE, "thu 23:59:59.999999999"),
            Got::Roll(
                Transition {
                    time: roll(),
                    pre: InstrumentId::from(PRE),
                    post: InstrumentId::from(POST),
                },
                before_roll(),
            ),
            tick(NautilusDataType::TradeTick, POST, "00:00:00"),
            tick(NautilusDataType::OrderBookDepth, POST, "00:20:00"),
            tick(NautilusDataType::TradeTick, POST, "00:45:00"),
        ]
    );
}

#[rstest]
fn the_subscriptions_alone_keep_the_strategy_on_the_contract_standing_for_the_root() {
    let catalog = rolls::catalog();

    // Both contracts declared as themselves, each replayed over the whole run
    let backtest = backtest(&catalog, [ticks(PRE), ticks(POST)].concat());

    let got = run(&backtest, rolled(ticks(ROOT)));

    assert_eq!(
        got,
        [
            Got::Alerts(vec![before_roll()]),
            tick(NautilusDataType::TradeTick, PRE, "thu 23:30:00"),
            tick(NautilusDataType::OrderBookDepth, PRE, "thu 23:45:00"),
            tick(NautilusDataType::TradeTick, PRE, "thu 23:59:59.999999999"),
            Got::Roll(
                Transition {
                    time: roll(),
                    pre: InstrumentId::from(PRE),
                    post: InstrumentId::from(POST),
                },
                before_roll(),
            ),
            tick(NautilusDataType::TradeTick, POST, "00:00:00"),
            tick(NautilusDataType::OrderBookDepth, POST, "00:20:00"),
            tick(NautilusDataType::TradeTick, POST, "00:45:00"),
        ]
    );
}

#[rstest]
fn a_plain_instrument_is_subscribed_once_and_gets_no_roll_alert() {
    let catalog = rolls::catalog();
    let backtest = backtest(&catalog, ticks(POST));

    let got = run(&backtest, rolled(ticks(POST)));

    assert_eq!(
        got,
        [
            Got::Alerts(vec![]),
            tick(NautilusDataType::TradeTick, POST, "thu 23:15:00"),
            tick(NautilusDataType::OrderBookDepth, POST, "thu 23:50:00"),
            tick(NautilusDataType::TradeTick, POST, "00:00:00"),
            tick(NautilusDataType::OrderBookDepth, POST, "00:20:00"),
            tick(NautilusDataType::TradeTick, POST, "00:45:00"),
        ]
    );
}

fn ticks_of(got: &[Got], instrument_id: &str) -> Vec<Got> {
    got.iter()
        .filter(
            |got| matches!(got, Got::Tick(_, id, _) if *id == InstrumentId::from(instrument_id)),
        )
        .cloned()
        .collect()
}

fn pre_ticks_of_the_run() -> Vec<Got> {
    vec![
        tick(NautilusDataType::TradeTick, PRE, "thu 23:30:00"),
        tick(NautilusDataType::OrderBookDepth, PRE, "thu 23:45:00"),
        tick(NautilusDataType::TradeTick, PRE, "thu 23:59:59.999999999"),
        tick(NautilusDataType::TradeTick, PRE, "00:00:00"),
        tick(NautilusDataType::OrderBookDepth, PRE, "00:10:00"),
        tick(NautilusDataType::TradeTick, PRE, "00:30:00"),
    ]
}

#[rstest]
fn december_trades_declared_beside_the_root_replay_once_over_the_whole_run() {
    let catalog = rolls::catalog();
    let trades = [ROOT, POST].map(|id| TickSeries {
        data: NautilusDataType::TradeTick,
        instrument_id: InstrumentId::from(id),
    });
    let got = run(&backtest(&catalog, trades.to_vec()), Subscribe::Contracts);

    assert_eq!(
        got,
        [
            Got::Alerts(vec![]),
            tick(NautilusDataType::TradeTick, POST, "thu 23:15:00"),
            tick(NautilusDataType::TradeTick, PRE, "thu 23:30:00"),
            tick(NautilusDataType::TradeTick, PRE, "thu 23:59:59.999999999"),
            tick(NautilusDataType::TradeTick, POST, "00:00:00"),
            tick(NautilusDataType::TradeTick, POST, "00:45:00"),
        ]
    );
}

#[rstest]
fn a_contract_declared_beside_its_root_is_replayed_once() {
    let catalog = rolls::catalog();
    let backtest = backtest(&catalog, [ticks(ROOT), ticks(PRE)].concat());

    let got = run(&backtest, Subscribe::Contracts);

    assert_eq!(ticks_of(&got, PRE), pre_ticks_of_the_run());
}

#[rstest]
fn the_subscriptions_keep_a_contract_declared_beside_its_root_after_the_root_rolls_off_it() {
    let catalog = rolls::catalog();
    let declared = [ticks(ROOT), ticks(PRE)].concat();
    let backtest = backtest(&catalog, declared.clone());

    let got = run(&backtest, rolled(declared));

    assert_eq!(ticks_of(&got, PRE), pre_ticks_of_the_run());
}

fn imbalances() -> NautilusDataType {
    NautilusDataType::Custom {
        type_name: glbx::IMBALANCE.to_string(),
    }
}

fn deltas(instrument_id: &str) -> TickSeries {
    TickSeries {
        data: NautilusDataType::OrderBookDelta,
        instrument_id: InstrumentId::from(instrument_id),
    }
}

fn store_deltas_and_imbalances(
    dir: &Path,
    instrument_id: &str,
    deltas: &[&str],
    imbalances: &[&str],
) {
    nautilus_serialization::ensure_custom_data_registered::<Imbalance>();
    let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    let (from, to) = (
        Some(tuesday("22:00:00").into()),
        Some(wednesday("02:00:00").into()),
    );
    let id = InstrumentId::from(instrument_id);
    let at = |time: &&str| -> UnixNanos {
        let Got::Tick(_, _, at) = tick(NautilusDataType::OrderBookDelta, instrument_id, time)
        else {
            unreachable!()
        };
        at
    };
    let deltas: Vec<OrderBookDelta> = deltas
        .iter()
        .map(|time| {
            let order =
                BookOrder::new(OrderSide::Buy, Price::from("4000.00"), Quantity::from(1), 1);
            let flags = RecordFlag::F_LAST as u8;
            OrderBookDelta::new(id, BookAction::Add, order, flags, 1, at(time), at(time))
        })
        .collect();
    catalog.write_to_parquet(&deltas, from, to, None).unwrap();
    let imbalances: Vec<(&str, f64)> = imbalances.iter().map(|&time| (time, 0.5)).collect();
    store_imbalances(dir, instrument_id, &imbalances);
}

/// Registers `Imbalance` with Nautilus first, as any reader of it must.
fn store_imbalances(dir: &Path, instrument_id: &str, values: &[(&str, f64)]) {
    nautilus_serialization::ensure_custom_data_registered::<Imbalance>();
    let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    let at = |time: &str| -> UnixNanos {
        let Got::Tick(_, _, at) = tick(imbalances(), instrument_id, time) else {
            unreachable!()
        };
        at
    };
    let data_type = DataType::new(glbx::IMBALANCE, None, Some(instrument_id.to_string()));
    let imbalances: Vec<CustomData> = values
        .iter()
        .map(|&(time, value)| {
            let imbalance = Imbalance::new(value, at(time), at(time));
            CustomData::new(Arc::new(imbalance), data_type.clone())
        })
        .collect();
    catalog
        .write_custom_data_batch(
            imbalances,
            Some(tuesday("22:00:00").into()),
            Some(wednesday("02:00:00").into()),
            None,
        )
        .unwrap();
}

#[rstest]
fn the_subscriptions_roll_book_deltas_and_custom_data_keyed_by_the_contract() {
    let catalog = rolls::catalog();
    store_deltas_and_imbalances(
        catalog.path(),
        PRE,
        &["thu 23:40:00", "00:05:00"],
        &["thu 23:42:00", "00:07:00"],
    );
    store_deltas_and_imbalances(
        catalog.path(),
        POST,
        &["thu 23:35:00", "00:15:00"],
        &["thu 23:37:00", "00:17:00"],
    );

    let backtest = Backtest {
        custom: vec![imbalance_series(PRE), imbalance_series(POST)],
        ..backtest(&catalog, vec![deltas(PRE), deltas(POST)])
    };
    let custom = vec![imbalance_series(ROOT)];
    let roller = Roller::replayed(vec![InstrumentId::from(ROOT)])
        .custom_roots(custom.iter().filter_map(CustomSeries::root));

    let got = run(
        &backtest,
        Subscribe::Rolled(
            roller,
            Subscriptions::new(vec![], vec![deltas(ROOT)], custom),
        ),
    );

    assert_eq!(
        got,
        [
            Got::Alerts(vec![before_roll()]),
            tick(NautilusDataType::OrderBookDelta, PRE, "thu 23:40:00"),
            tick(imbalances(), PRE, "thu 23:42:00"),
            Got::Roll(
                Transition {
                    time: roll(),
                    pre: InstrumentId::from(PRE),
                    post: InstrumentId::from(POST),
                },
                before_roll(),
            ),
            tick(NautilusDataType::OrderBookDelta, POST, "00:15:00"),
            tick(imbalances(), POST, "00:17:00"),
        ]
    );
}

fn imbalance_series(identifier: &str) -> CustomSeries {
    CustomSeries::new(glbx::IMBALANCE, identifier.to_string(), None).unwrap()
}

fn store_both_contracts_imbalances(dir: &Path) {
    store_imbalances(dir, PRE, &[("thu 23:42:00", 0.25), ("00:07:00", 0.5)]);
    store_imbalances(dir, POST, &[("thu 23:37:00", 0.75), ("00:17:00", 1.0)]);
}

fn backtest_of(catalog: &tempfile::TempDir, custom: Vec<CustomSeries>) -> Backtest {
    Backtest {
        custom,
        ..backtest(catalog, ticks(ROOT))
    }
}

#[rstest]
fn a_roots_custom_series_replays_each_contracts_imbalances_only_within_its_slice() {
    let catalog = rolls::catalog();
    store_both_contracts_imbalances(catalog.path());
    let backtest = backtest_of(&catalog, vec![imbalance_series(ROOT)]);

    let got = run(&backtest, Subscribe::ContractImbalances);

    assert_eq!(
        got,
        [
            Got::Alerts(vec![]),
            tick(imbalances(), PRE, "thu 23:42:00"),
            tick(imbalances(), POST, "00:17:00"),
        ]
    );
}

#[rstest]
fn a_feature_over_a_roots_custom_series_reads_the_pre_then_the_post_contract() {
    let catalog = rolls::catalog();
    store_both_contracts_imbalances(catalog.path());

    // Both contracts' own series beside the root's, so that the cache holds both sides
    let custom = vec![
        imbalance_series(ROOT),
        imbalance_series(PRE),
        imbalance_series(POST),
    ];
    let backtest = backtest_of(&catalog, custom.clone());
    let features = FeatureSet::builder()
        .custom(&imbalance_series(ROOT), "imbalance", |data| {
            data.data
                .as_any()
                .downcast_ref::<Imbalance>()
                .map_or(f64::NAN, |imbalance| imbalance.value)
        })
        .build()
        .unwrap();
    let roller =
        Roller::replayed(vec![]).custom_roots(custom.iter().filter_map(CustomSeries::root));
    let subscriptions = Subscriptions::new(vec![], vec![], custom);

    let got = run(
        &backtest,
        Subscribe::Featured(roller, subscriptions, features),
    );

    assert_eq!(
        got,
        [
            Got::Alerts(vec![before_roll()]),
            tick(imbalances(), POST, "thu 23:37:00"),
            Got::Read(None),
            tick(imbalances(), PRE, "thu 23:42:00"),
            Got::Read(Some(vec![0.25])),
            Got::Roll(
                Transition {
                    time: roll(),
                    pre: InstrumentId::from(PRE),
                    post: InstrumentId::from(POST),
                },
                before_roll(),
            ),
            Got::Read(Some(vec![0.75])),
            tick(imbalances(), PRE, "00:07:00"),
            Got::Read(Some(vec![0.75])),
            tick(imbalances(), POST, "00:17:00"),
            Got::Read(Some(vec![1.0])),
        ]
    );
}
