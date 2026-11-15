//! A strategy reading LSE's daily bond yields, custom data of no instrument
//! (`lse::LseRow`), declared in its config's `[data].custom` under the
//! identifier `US10Y`, which it chooses, and filled into the catalog under
//! it through fill-catalog's node fill from a mock LSE peer replaying a
//! captured export (`lse::testing`): US10Y's rows of 9, 10 and 12 November
//! 2026, each stamped at its date's end, when its close is known. The
//! backtest declares the series by the config's data type, unchanged,
//! beside IC's gold (`common::ic`), the run's venue, and replays the rows
//! of its window to the strategy, each of the data type it is subscribed
//! to, when it is known: to a probe subscribing itself, and to one whose
//! subscriptions (`strategy::subscriptions`) subscribe it, as a
//! strategy's do. The fill and the run each on a thread of its own, one
//! node per thread.

use std::{cell::RefCell, path::Path, rc::Rc};

use crate::common::ic;
use helix_trader::{
    backtest::Backtest,
    catalog::{NodeClient, NodeFill, missing_custom},
    strategy::{Part, Series, StandardConfig, subscriptions::Subscriptions},
};
use jiff::Timestamp;
use lse::{
    LseDataClientConfig, LseDataClientFactory, LseRow,
    testing::{API_KEY, MockLse, REFERENCE_CATALOGUE, Reply, US10Y_BOND_YIELDS},
};
use nautilus_common::{actor::DataActor, config::ConfigResult};
use nautilus_core::{UnixNanos, string::secret::SecretString};
use nautilus_model::{
    data::{CustomData, CustomDataTrait, Data, DataType},
    identifiers::StrategyId,
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use serde::Deserialize;
use serde_json::{Value, json};

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

/// US10Y's daily bond yields as a config declares them, under the
/// identifier `US10Y`: the one data type of its `[data].custom`.
fn us10y() -> DataType {
    let config: StandardConfig<Reader> = StandardConfig::load(&format!(
        r#"
        [data]
        series = [{{ data = "quotes", instrument_id = "{gold}" }}]
        custom = [{{ type_name = "LseRow", identifier = "US10Y", metadata = {{ source = "bond_yields", symbol = "US10Y" }} }}]
        warmup = "1h"

        [trade]
        instruments = [{{ instrument_id = "{gold}" }}]
        risk = "0.5%"
        "#,
        gold = ic::INSTRUMENT_ID,
    ))
    .unwrap();
    let [data_type] = <[DataType; 1]>::try_from(config.data_requirements().custom).unwrap();
    data_type
}

/// Fills the catalog in `dir` with `us10y()` over the rows of the
/// captured days, known from the 10th's 00:00 to the 13th's, from the mock
/// LSE peer, as fill-catalog does: what the catalog lacks, asked of LSE's
/// client through the node.
fn fill_bond_yields(dir: &Path) {
    let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    let requests = missing_custom(
        &catalog,
        &[us10y()],
        november("10T00:00:00"),
        november("13T00:00:00"),
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
        self.subscribe_data(us10y(), None, None);
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

/// The fill stores the rows under the identifier the config chose, each
/// of the config's data type, unchanged.
#[test]
fn a_fill_stores_the_rows_under_the_identifier_the_config_names() {
    let dir = tempfile::tempdir().unwrap();
    let filled = dir.path().to_path_buf();
    std::thread::spawn(move || fill_bond_yields(&filled))
        .join()
        .unwrap();

    LseRow::register();
    let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    let stored = catalog
        .query_custom_data_dynamic(
            LseRow::type_name_static(),
            Some(&["US10Y".to_string()]),
            None,
            None,
            None,
            None,
            true,
        )
        .unwrap();
    let closes: Vec<(DataType, Value)> = stored
        .iter()
        .map(|data| {
            let Data::Custom(custom) = data else {
                panic!("not custom data: {data:?}");
            };
            let row = custom.data.as_any().downcast_ref::<LseRow>().unwrap();
            (custom.data_type.clone(), row.fields["close"].clone())
        })
        .collect();
    assert_eq!(
        closes,
        [json!(4.884), json!(5.0), json!(5.133)].map(|close| (us10y(), close))
    );
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

/// From the 12th's 00:00 to noon on the 13th: the 12th's close reaches
/// the strategy at the 13th's 00:00, when the day it closes has ended, and
/// at no time of the 12th; the 10th's, known at the 11th's 00:00, before
/// the run, does not.
#[test]
fn a_bond_yield_of_a_date_reaches_the_strategy_at_the_date_s_end() {
    let (_catalog, backtest) = bond_yields_backtest();

    let got = std::thread::spawn(move || {
        let got = Rc::new(RefCell::new(Vec::new()));
        let probe = Probe {
            core: core(),
            got: Rc::clone(&got),
        };
        backtest.run(probe).unwrap();
        got.take()
    })
    .join()
    .unwrap();

    let thirteenth = UnixNanos::from(november("13T00:00:00"));
    assert_eq!(got, [(us10y(), thirteenth, json!(5.133), thirteenth)]);
}

/// A strategy's subscriptions of the backtest's custom data, as a strategy
/// builds them of its data requirements (`DataRequirements::custom`,
/// `[data].custom`), subscribe it to the series: its `on_data` gets the
/// 12th's close at the 13th's 00:00, though it subscribes to nothing
/// itself.
#[test]
fn a_strategy_gets_the_custom_data_of_no_instrument_through_its_subscriptions() {
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
    assert_eq!(got, [(us10y(), thirteenth, json!(5.133), thirteenth)]);
}
