//! A backtest trading IC's gold (`common::ic`) while reading LSE's gold
//! futures trades, `GOLD-FUTURES.LSEDGE`, filled into the catalog through
//! fill-catalog's node fill from a mock LSE peer replaying a captured
//! export (`lse::testing`). No broker trades LSEDGE: its simulated venue
//! starts with nothing, so the run ends with the balances and return
//! statistics of the same run without the series. The fill and each run
//! on a thread of its own, one node per thread.

use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    path::Path,
    rc::Rc,
};

use crate::common::ic;
use helix_trader::{
    catalog::{NodeClient, NodeFill, missing},
    strategy::{DataRequirements, TickSeries},
};
use jiff::SignedDuration;
use lse::{
    LseDataClientConfig, LseDataClientFactory,
    testing::{API_KEY, GOLD_FUTURES, GOLD_TRADES, MockLse, Reply, gold_futures},
};
use nautilus_backtest::result::BacktestResult;
use nautilus_common::actor::DataActor;
use nautilus_core::string::secret::SecretString;
use nautilus_model::{
    data::{Bar, BarType, NautilusDataType, TradeTick},
    enums::OrderSide,
    identifiers::{InstrumentId, StrategyId},
    types::Quantity,
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};

/// LSE's gold trades over the captured second, `[10:00:00, 10:00:01)`.
fn gold_trades() -> DataRequirements {
    DataRequirements {
        bar_types: vec![],
        ticks: vec![TickSeries {
            data: NautilusDataType::TradeTick,
            instrument_id: InstrumentId::from(GOLD_FUTURES),
        }],
        custom: vec![],
        warmup: SignedDuration::ZERO,
    }
}

/// Fills the catalog in `dir`, which holds the hand-written definition of
/// `GOLD_FUTURES`, with its trades from the mock LSE peer, as fill-catalog
/// does: what the catalog lacks, asked of LSE's client through the node.
fn fill_gold(dir: &Path) {
    let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
    catalog.write_instruments(vec![gold_futures()]).unwrap();
    let slices = gold_trades().slices(ic::at("10:00:00"), ic::at("10:00:00.999999999"));
    let requests = missing(&catalog, &slices).unwrap();
    let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);
    let config = LseDataClientConfig::builder()
        .api_key(SecretString::from(API_KEY))
        .base_url_http(lse.url().to_string())
        .build();
    let mut node = NodeFill::new();
    node.add(
        NodeClient {
            factory: Box::new(LseDataClientFactory::new()),
            config: Box::new(config),
        },
        &requests,
        |_| SignedDuration::from_hours(24),
    );
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(node.run(dir))
        .unwrap();
}

/// Buys 10 oz of XAUUSD on IC's first hour bar, reading LSE's gold trades
/// if told to.
#[derive(Debug)]
struct GoldReader {
    core: StrategyCore,
    reads_lse: bool,
    trades: Rc<RefCell<Vec<TradeTick>>>,
    bought: bool,
}

nautilus_strategy!(GoldReader);

impl DataActor for GoldReader {
    fn on_start(&mut self) -> anyhow::Result<()> {
        if self.reads_lse {
            self.subscribe_trades(InstrumentId::from(GOLD_FUTURES), None, None);
        }
        self.subscribe_bars(BarType::from(ic::HOUR_BARS), None, None);
        Ok(())
    }

    fn on_trade(&mut self, trade: &TradeTick) -> anyhow::Result<()> {
        self.trades.borrow_mut().push(*trade);
        Ok(())
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        if std::mem::replace(&mut self.bought, true) {
            return Ok(());
        }
        let order = self.order().market(
            bar.bar_type.instrument_id(),
            OrderSide::Buy,
            Quantity::from(10),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        self.submit_order(order, None, None, None)
    }
}

/// IC's backtest on `catalog`, replaying LSE's gold trades if `reads_lse`,
/// on a thread of its own: its result and the LSE trades the strategy
/// read.
fn run(catalog: &tempfile::TempDir, reads_lse: bool) -> (BacktestResult, usize) {
    let mut backtest = ic::backtest(catalog);
    if reads_lse {
        backtest.ticks = gold_trades().ticks;
    }
    std::thread::spawn(move || {
        let trades = Rc::new(RefCell::new(Vec::new()));
        let reader = GoldReader {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from("READER-001")),
                order_id_tag: Some("001".to_string()),
                ..Default::default()
            }),
            reads_lse,
            trades: Rc::clone(&trades),
            bought: false,
        };
        let outcome = backtest.run(reader).unwrap();
        let read = trades.borrow().len();
        (outcome.result, read)
    })
    .join()
    .unwrap()
}

/// Statistics by name, to the bit: NaN equals NaN.
fn bits<'a>(stats: impl IntoIterator<Item = (&'a String, &'a f64)>) -> BTreeMap<String, u64> {
    stats
        .into_iter()
        .map(|(name, value)| (name.clone(), value.to_bits()))
        .collect()
}

fn pnls(result: &BacktestResult) -> HashMap<String, BTreeMap<String, u64>> {
    result
        .stats_pnls
        .iter()
        .map(|(currency, stats)| (currency.clone(), bits(stats)))
        .collect()
}

#[test]
fn reading_an_lsedge_series_leaves_the_balances_and_returns_of_ic_s_trade_alone() {
    let catalog = ic::catalog();
    let dir = catalog.path().to_path_buf();
    std::thread::spawn(move || fill_gold(&dir)).join().unwrap();

    let (reading, read) = run(&catalog, true);
    let (alone, read_alone) = run(&catalog, false);

    assert_eq!((read, read_alone), (6, 0));
    assert_eq!(reading.total_orders, 1);
    let ic_balance = "account.ICMARKETS.balance.USD.total";
    assert_eq!(reading.summary[ic_balance], alone.summary[ic_balance]);
    assert_eq!(
        reading.summary["account.LSEDGE.balance.USD.total"],
        "0.00 USD"
    );
    assert_eq!(bits(&reading.stats_returns), bits(&alone.stats_returns));
    assert_eq!(pnls(&reading), pnls(&alone));
    assert_eq!(reading.returns_series, alone.returns_series);
}
