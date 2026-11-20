//! Tests custom data written into the catalog from outside helix, of a type no row in the routing
//! table routes. The data is gold's `Imbalance`s (`common::glbx`), stored by hand under
//! `GOLD-IMBALANCE`, which keys no instrument, beside IC's gold (`common::ic`). The backtest
//! replays the series by data type, each point at its time. It replays them to a strategy
//! subscribed through `strategy::subscriptions`, and to a raw feature, which reads each from
//! Nautilus' cache, where the data engine puts each before the strategy's handler runs. A run with
//! nothing stored goes on.

use std::{cell::RefCell, rc::Rc};

use helix_trader::{
    backtest::Backtest,
    model::features::FeatureSet,
    strategy::{
        Part, Series, StandardConfig, requirements::CustomSeries, subscriptions::Subscriptions,
    },
    venues,
};
use nautilus_common::{actor::DataActor, config::ConfigResult};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{CustomData, HasTsInit},
    identifiers::StrategyId,
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rstest::rstest;
use serde::Deserialize;
use tempfile::TempDir;

use crate::common::{glbx, ic};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reader {}

impl Part for Reader {
    const INSTRUMENTS: usize = 1;

    fn default_base() -> StrategyConfig {
        StrategyConfig {
            strategy_id: Some(StrategyId::from("READER-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }
    }

    fn reads(config: &StandardConfig<Self>) -> ConfigResult<Vec<Series>> {
        Ok(config.data.series.clone())
    }
}

/// Returns the custom series, loaded after registering helix's custom data types and the tests'
/// `Imbalance`, as the process does at its start.
fn declared() -> CustomSeries {
    venues::register_custom_data();
    nautilus_serialization::ensure_custom_data_registered::<glbx::Imbalance>();
    let config: StandardConfig<Reader> = StandardConfig::load(&format!(
        r#"
        [data]
        series = [
            {{ data = "quotes", instrument_id = "{gold}" }},
            {{ data = "Custom:Imbalance", identifier = "GOLD-IMBALANCE" }},
        ]
        warmup = "1h"

        [trade]
        instruments = [{{ instrument_id = "{gold}" }}]
        risk = "0.5%"
        "#,
        gold = ic::INSTRUMENT_ID,
    ))
    .unwrap();
    let [series] = <[CustomSeries; 1]>::try_from(config.data_requirements().custom).unwrap();
    series
}

fn catalog() -> TempDir {
    let catalog = ic::catalog();
    glbx::store_imbalances_as(catalog.path(), declared().data_type());
    catalog
}

fn backtest(catalog: &TempDir) -> Backtest {
    Backtest {
        custom: vec![declared()],
        ..ic::backtest(catalog)
    }
}

fn value(data: &CustomData) -> f64 {
    data.data
        .as_any()
        .downcast_ref::<glbx::Imbalance>()
        .expect("an imbalance")
        .value
}

type Got = (f64, UnixNanos, UnixNanos);

fn at(time: &str) -> UnixNanos {
    ic::at(time).into()
}

#[derive(Debug)]
struct Subscribed {
    core: StrategyCore,
    subscriptions: Subscriptions,
    got: Rc<RefCell<Vec<Got>>>,
}

nautilus_strategy!(Subscribed);

impl DataActor for Subscribed {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let now = self.clock().utc_now();
        self.subscriptions.clone().start(self, now);
        Ok(())
    }

    fn on_data(&mut self, data: &CustomData) -> anyhow::Result<()> {
        let got = (value(data), data.ts_init(), self.clock().timestamp_ns());
        self.got.borrow_mut().push(got);
        Ok(())
    }
}

fn core() -> StrategyCore {
    StrategyCore::new(StrategyConfig {
        strategy_id: Some(StrategyId::from("PROBE-001")),
        order_id_tag: Some("001".to_string()),
        ..Default::default()
    })
}

fn got_through_subscriptions(backtest: Backtest) -> Vec<Got> {
    std::thread::spawn(move || {
        let got = Rc::new(RefCell::new(Vec::new()));
        let probe = Subscribed {
            core: core(),
            subscriptions: Subscriptions::new(vec![], vec![], backtest.custom.clone()),
            got: Rc::clone(&got),
        };
        backtest.run(probe).unwrap();
        got.take()
    })
    .join()
    .unwrap()
}

/// The imbalances in the run's window reach the strategy at their times, but not the 08:50 one.
#[rstest]
fn a_strategy_gets_a_custom_series_of_the_catalog_s_through_its_subscriptions() {
    let catalog = catalog();

    let got = got_through_subscriptions(backtest(&catalog));

    assert_eq!(
        got,
        [
            (-0.25, at("09:30:00"), at("09:30:00")),
            (0.75, at("11:00:00"), at("11:00:00")),
        ]
    );
}

/// With none of the series stored, the run warns and goes on. The strategy gets no data.
#[rstest]
fn a_run_of_a_custom_series_the_catalog_holds_nothing_of_goes_on() {
    let catalog = ic::catalog();

    let got = got_through_subscriptions(backtest(&catalog));

    assert_eq!(got, []);
}

#[derive(Debug)]
struct Featured {
    core: StrategyCore,
    series: CustomSeries,
    features: Rc<FeatureSet>,
    read: Rc<RefCell<Vec<Option<Vec<f64>>>>>,
}

nautilus_strategy!(Featured);

impl DataActor for Featured {
    fn on_start(&mut self) -> anyhow::Result<()> {
        Rc::clone(&self.features).register(self);
        self.subscribe_data(self.series.data_type().clone(), None, None);
        Ok(())
    }

    fn on_data(&mut self, _data: &CustomData) -> anyhow::Result<()> {
        self.read.borrow_mut().push(self.features.values());
        Ok(())
    }
}

#[rstest]
fn a_raw_feature_over_a_custom_series_of_the_catalog_s_reads_each_replayed_point() {
    let catalog = catalog();
    let backtest = backtest(&catalog);

    let read = std::thread::spawn(move || {
        let series = declared();
        let features = FeatureSet::builder()
            .custom(&series, "imbalance", value)
            .build()
            .unwrap();
        let read = Rc::new(RefCell::new(Vec::new()));
        let featured = Featured {
            core: core(),
            series,
            features,
            read: Rc::clone(&read),
        };
        backtest.run(featured).unwrap();
        read.take()
    })
    .join()
    .unwrap();

    assert_eq!(read, [Some(vec![-0.25]), Some(vec![0.75])]);
}
