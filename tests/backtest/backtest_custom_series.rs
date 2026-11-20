//! Tests LSE's daily bond yields (`lse::LseRow`) as a custom series in a config's `[data] series`.
//! The series uses the identifier `TREASURY-10Y`, which the config chooses, not the symbol.
//! fill-catalog's node fill fills it from a mock LSE peer that replays a captured export
//! (`lse::testing`). The rows are US10Y's for 9, 10 and 12 November 2026. Each is stamped at the
//! end of its date, when its close is known. The backtest replays the rows in its window once
//! known, beside IC's gold (`common::ic`). It replays them to a strategy subscribed through
//! `strategy::subscriptions` and to a raw feature. A config that names the identifier with other
//! metadata fails the run, and the error names both data types.

use std::{cell::RefCell, path::Path, rc::Rc};

use helix_trader::{
    backtest::Backtest,
    catalog::{NodeClient, NodeFill, missing_custom},
    model::features::FeatureSet,
    strategy::{
        Part, Series, StandardConfig, requirements::CustomSeries, subscriptions::Subscriptions,
    },
    venues,
};
use jiff::Timestamp;
use lse::{
    LseDataClientConfig, LseDataClientFactory, LseRow,
    testing::{API_KEY, MockLse, REFERENCE_CATALOGUE, Reply, US10Y_BOND_YIELDS},
};
use nautilus_common::{actor::DataActor, config::ConfigResult};
use nautilus_core::{Params, UnixNanos, string::secret::SecretString};
use nautilus_model::{
    data::{CustomData, DataType},
    identifiers::StrategyId,
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rstest::rstest;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::common::{self, ic};

fn november(time: &str) -> Timestamp {
    format!("2026-11-{time}Z").parse().unwrap()
}

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

const TREASURY: &str = "TREASURY-10Y";

/// Returns the series, loaded after registering helix's custom data types, as the process does at
/// its start.
fn declared(symbol: &str) -> CustomSeries {
    venues::register_custom_data();
    let config: StandardConfig<Reader> = StandardConfig::load(&format!(
        r#"
        [data]
        series = [
            {{ data = "quotes", instrument_id = "{gold}" }},
            {{ data = "Custom:LseRow", identifier = "{TREASURY}", metadata = {{ source = "bond_yields", symbol = "{symbol}" }} }},
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

fn us10y() -> CustomSeries {
    declared("US10Y")
}

fn us10y_type() -> DataType {
    let metadata = Params::new(
        [
            ("source".to_string(), json!("bond_yields")),
            ("symbol".to_string(), json!("US10Y")),
        ]
        .into(),
    );
    DataType::new("LseRow", Some(metadata), Some(TREASURY.to_string()))
}

/// Fills the catalog in `dir` with the rows known from 00:00 on the 10th to 00:00 on the 13th.
///
/// The mock LSE peer supplies them through the node on the 16th, as fill-catalog does.
fn fill_bond_yields(dir: &Path) {
    let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    let requests = missing_custom(
        &catalog,
        &us10y().slices(november("10T00:00:00"), november("13T00:00:00")),
    )
    .unwrap();
    let lse = MockLse::start(vec![
        Reply::json(REFERENCE_CATALOGUE),
        Reply::file(US10Y_BOND_YIELDS),
    ]);
    let config = LseDataClientConfig::builder()
        .api_key(SecretString::from(API_KEY))
        .base_url_http(lse.url().to_string())
        .build();
    let mut node = NodeFill::new();
    node.add_with_custom(
        NodeClient {
            factory: Box::new(LseDataClientFactory::new()),
            config: Box::new(config),
        },
        &[],
        &requests,
        |_| unreachable!("no series of an instrument"),
    );
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(node.run(dir, common::clock_at(november("16T12:00:00"))))
        .unwrap();
}

type Got = (DataType, UnixNanos, Value, UnixNanos);

fn got(data: &CustomData, now: UnixNanos) -> Got {
    let row = data
        .data
        .as_any()
        .downcast_ref::<LseRow>()
        .expect("an LSE row");
    (
        data.data_type.clone(),
        row.ts_init,
        row.fields["close"].clone(),
        now,
    )
}

fn core() -> StrategyCore {
    StrategyCore::new(StrategyConfig {
        strategy_id: Some(StrategyId::from("PROBE-001")),
        order_id_tag: Some("001".to_string()),
        ..Default::default()
    })
}

#[derive(Debug)]
struct Probe {
    core: StrategyCore,
    got: Rc<RefCell<Vec<Got>>>,
}

nautilus_strategy!(Probe);

impl DataActor for Probe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.subscribe_data(us10y_type(), None, None);
        Ok(())
    }

    fn on_data(&mut self, data: &CustomData) -> anyhow::Result<()> {
        let got = got(data, self.clock().timestamp_ns());
        self.got.borrow_mut().push(got);
        Ok(())
    }
}

/// Starts its subscriptions when it starts, as a strategy does.
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
        let got = got(data, self.clock().timestamp_ns());
        self.got.borrow_mut().push(got);
        Ok(())
    }
}

fn bond_yields_backtest() -> (tempfile::TempDir, Backtest) {
    let catalog = ic::catalog();
    let dir = catalog.path().to_path_buf();
    std::thread::spawn(move || fill_bond_yields(&dir))
        .join()
        .unwrap();
    let backtest = Backtest {
        custom: vec![us10y()],
        start: november("12T00:00:00"),
        end: november("13T12:00:00"),
        ..ic::backtest(&catalog)
    };
    (catalog, backtest)
}

/// A config that names `TREASURY`, which holds the 10-year yields, with the 2-year's metadata
/// would replay them under a data type its strategy is not subscribed to. fill-catalog would find
/// nothing missing. The run fails before it starts, the error names both, and the probe gets none.
#[rstest]
fn a_backtest_of_rows_stored_under_other_metadata_fails_naming_both() {
    let (_catalog, backtest) = bond_yields_backtest();
    let us02y = declared("US02Y");
    let backtest = Backtest {
        custom: vec![us02y.clone()],
        ..backtest
    };

    let (err, replayed) = std::thread::spawn(move || {
        let replayed = Rc::new(RefCell::new(Vec::new()));
        let probe = Probe {
            core: core(),
            got: Rc::clone(&replayed),
        };
        let err = backtest.run(probe).unwrap_err();
        (format!("{err:#}"), replayed.take())
    })
    .join()
    .unwrap();

    for named in [us02y.data_type().clone(), us10y_type()] {
        assert!(err.contains(&named.to_string()), "{named}: {err}");
    }
    assert_eq!(replayed, []);
}

/// Subscriptions from `DataRequirements::custom` subscribe a strategy to the series.
///
/// - Its `on_data` gets the 12th's close at 00:00 on the 13th, when the day has ended, and at no
///   time on the 12th.
/// - The 10th's close is known at 00:00 on the 11th, before the run, so it does not reach the
///   strategy.
#[rstest]
fn a_strategy_gets_a_custom_series_through_its_subscriptions() {
    let (_catalog, backtest) = bond_yields_backtest();

    let got = std::thread::spawn(move || {
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
    .unwrap();

    let thirteenth = UnixNanos::from(november("13T00:00:00"));
    assert_eq!(got, [(us10y_type(), thirteenth, json!(5.133), thirteenth)]);
}

#[derive(Debug)]
struct Featured {
    core: StrategyCore,
    features: Rc<FeatureSet>,
    read: Rc<RefCell<Vec<Option<Vec<f64>>>>>,
}

nautilus_strategy!(Featured);

impl DataActor for Featured {
    fn on_start(&mut self) -> anyhow::Result<()> {
        Rc::clone(&self.features).register(self);
        self.subscribe_data(us10y_type(), None, None);
        Ok(())
    }

    fn on_data(&mut self, _data: &CustomData) -> anyhow::Result<()> {
        self.read.borrow_mut().push(self.features.values());
        Ok(())
    }
}

/// A raw feature reads the 12th's close from the cache. The backtest replays it at 00:00 on the
/// 13th, and the data engine puts it in the cache before the strategy's handler runs.
#[rstest]
fn a_raw_feature_over_a_custom_series_reads_each_replayed_row_from_the_cache() {
    let (_catalog, backtest) = bond_yields_backtest();

    let read = std::thread::spawn(move || {
        let features = FeatureSet::builder()
            .custom(&us10y(), "close", |data| {
                data.data
                    .as_any()
                    .downcast_ref::<LseRow>()
                    .and_then(|row| row.fields["close"].as_f64())
                    .unwrap_or(f64::NAN)
            })
            .build()
            .unwrap();
        let read = Rc::new(RefCell::new(Vec::new()));
        let featured = Featured {
            core: core(),
            features,
            read: Rc::clone(&read),
        };
        backtest.run(featured).unwrap();
        read.take()
    })
    .join()
    .unwrap();

    assert_eq!(read, [Some(vec![5.133])]);
}
