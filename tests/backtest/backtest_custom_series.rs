//! A strategy reading LSE's daily bond yields (`lse::LseRow`), a custom
//! series of its config's `[data] series` under the identifier
//! `TREASURY-10Y`, which it chooses, not the symbol, and filled
//! into the catalog under it through fill-catalog's node fill from a mock
//! LSE peer replaying a
//! captured export (`lse::testing`): US10Y's rows of 9, 10 and 12 November
//! 2026, each stamped at its date's end, when its close is known. The
//! backtest declares the series by the config's data type, unchanged,
//! beside IC's gold (`common::ic`), the run's venue, and replays the rows
//! of its window to the strategy, each of the data type it is subscribed
//! to, when it is known: to one whose subscriptions
//! (`strategy::subscriptions`) subscribe it, as a strategy's do; and a raw
//! feature of a model's features over the series reads each row from
//! Nautilus' cache. The fill and the run each on a thread of its own, one
//! node per thread. A config naming that identifier with other metadata
//! fails the run naming both data types, before a probe subscribing itself
//! to the stored rows' data type gets any, rather than replay rows under a
//! data type its strategy is not subscribed to.

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

use crate::common::ic;

fn november(time: &str) -> Timestamp {
    format!("2026-11-{time}Z").parse().unwrap()
}

/// A part reading every series its config lists, trading one instrument.
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

/// The identifier the config keys the series by, not its symbol.
const TREASURY: &str = "TREASURY-10Y";

/// The daily bond yields of `symbol` as a config declares them, under the
/// identifier `TREASURY`: the one custom series of its `[data] series`,
/// loaded after registering the custom data types helix compiles, as the
/// process does at its start.
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

/// US10Y's daily bond yields, the series filled.
fn us10y() -> CustomSeries {
    declared("US10Y")
}

/// The data type of US10Y's daily bond yields under `TREASURY`, as
/// Nautilus builds it.
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

/// Fills the catalog in `dir` with `us10y()` over the rows of the
/// captured days, known from the 10th's 00:00 to the 13th's, from the mock
/// LSE peer, as fill-catalog does: what the catalog lacks, asked of LSE's
/// client through the node.
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
        .block_on(node.run(dir))
        .unwrap();
}

/// What the probe got: each row's data type, time and close, and the time
/// of the run when it got it.
type Got = (DataType, UnixNanos, Value, UnixNanos);

/// What a strategy got of `data`, a row, at `now`.
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

/// The probe's core.
fn core() -> StrategyCore {
    StrategyCore::new(StrategyConfig {
        strategy_id: Some(StrategyId::from("PROBE-001")),
        order_id_tag: Some("001".to_string()),
        ..Default::default()
    })
}

/// Subscribes to `us10y()` and records each row it gets.
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

/// Starts its subscriptions at its start, as a strategy does, subscribing
/// to nothing itself, and records each row it gets.
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

/// The catalog filled with `us10y()` (`fill_bond_yields`) and a backtest
/// on it from the 12th's 00:00 to noon on the 13th declaring the series.
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

/// The catalog holds the 10-year yields under `TREASURY`; a config naming
/// that identifier with the 2-year's metadata would replay them under the
/// 10-year's data type, which its strategy is not subscribed to, so that it
/// would get nothing, and fill-catalog would find nothing missing: the run
/// fails before it starts, naming both, and the probe, subscribed to the
/// 10-year's data type, gets none of them.
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

/// A strategy's subscriptions of the backtest's custom series, as a
/// strategy builds them of its data requirements
/// (`DataRequirements::custom`), subscribe it to the series: its `on_data`
/// gets the 12th's close at the 13th's 00:00, when the day it closes has
/// ended, and at no time of the 12th, though it subscribes to nothing
/// itself; the 10th's, known at the 11th's 00:00, before the run, does not
/// reach it.
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

/// Registers a raw feature on the 10-year yields' closes at its start,
/// subscribes to them, and records the feature's values at each row it
/// gets.
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

/// A model's raw feature over the custom series reads the 12th's close,
/// replayed at the 13th's 00:00, from the cache, where the data engine
/// put it before the strategy's handler, with no strategy code.
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
