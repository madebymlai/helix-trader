//! A strategy reading LSE's daily bond yields, custom data of no instrument
//! (`lse::LseRow`), filled into the catalog through fill-catalog's node
//! fill from a mock LSE peer replaying a captured export (`lse::testing`):
//! US10Y's rows of 9, 10 and 12 November 2026. The backtest declares the
//! series by its data type, beside IC's gold (`common::ic`), the run's
//! venue, and replays the rows of its window to the strategy, each of the
//! data type the strategy subscribed to. The fill and the run each on a
//! thread of its own, one node per thread.

use std::{cell::RefCell, path::Path, rc::Rc};

use crate::common::ic;
use helix_trader::catalog::{NodeClient, NodeFill, missing_custom};
use jiff::Timestamp;
use lse::{
    LseDataClientConfig, LseDataClientFactory, LseRow,
    testing::{API_KEY, MockLse, REFERENCE_CATALOGUE, Reply, US10Y_BOND_YIELDS},
};
use nautilus_common::actor::DataActor;
use nautilus_core::{UnixNanos, string::secret::SecretString};
use nautilus_model::{
    data::{CustomData, DataType},
    identifiers::StrategyId,
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use serde_json::{Value, json};

fn november(time: &str) -> Timestamp {
    format!("2026-11-{time}Z").parse().unwrap()
}

/// US10Y's daily bond yields.
fn us10y() -> DataType {
    LseRow::data_type("bond_yields", "US10Y")
}

/// Fills the catalog in `dir` with `us10y()` over the captured days, from
/// the mock LSE peer, as fill-catalog does: what the catalog lacks, asked
/// of LSE's client through the node.
fn fill_bond_yields(dir: &Path) {
    let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    let requests = missing_custom(
        &catalog,
        &[us10y()],
        november("09T00:00:00"),
        november("12T23:59:59.999999999"),
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

/// What the probe got: each row's data type, time and close.
type Got = (DataType, UnixNanos, Value);

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
        let row = data
            .data
            .as_any()
            .downcast_ref::<LseRow>()
            .expect("an LSE row");
        let got = (
            data.data_type.clone(),
            row.ts_init,
            row.fields["close"].clone(),
        );
        self.got.borrow_mut().push(got);
        Ok(())
    }
}

/// From the 10th to noon on the 12th: the rows of the 10th and the 12th,
/// not the 9th's, each of the data type subscribed to, in time order.
#[test]
fn a_custom_series_of_no_instrument_is_replayed_to_the_strategy_over_the_run_s_window() {
    let catalog = ic::catalog();
    let dir = catalog.path().to_path_buf();
    std::thread::spawn(move || fill_bond_yields(&dir))
        .join()
        .unwrap();
    // As the code declaring a custom series must (see `Backtest::custom`).
    LseRow::register();
    let backtest = helix_trader::backtest::Backtest {
        custom: vec![us10y()],
        start: november("10T00:00:00"),
        end: november("12T12:00:00"),
        ..ic::backtest(&catalog)
    };

    let got = std::thread::spawn(move || {
        let got = Rc::new(RefCell::new(Vec::new()));
        let probe = Probe {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from("PROBE-001")),
                order_id_tag: Some("001".to_string()),
                ..Default::default()
            }),
            got: Rc::clone(&got),
        };
        backtest.run(probe).unwrap();
        got.take()
    })
    .join()
    .unwrap();

    let row = |day: &str, close: f64| {
        (
            us10y(),
            UnixNanos::from(november(&format!("{day}T00:00:00"))),
            json!(close),
        )
    };
    assert_eq!(got, [row("10", 5.0), row("12", 5.133)]);
}
