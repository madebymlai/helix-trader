//! A strategy whose config reads the instrument it trades only through
//! data that gives Nautilus no market price for it, its custom data, its
//! book deltas or its depth: the data requirements of its config
//! (`StandardConfig::data_requirements`) still replay the instrument's
//! quotes, so its market order on IC's gold (`common::ic`) is not denied
//! for want of a price, nor of the instrument, and fills. One node per
//! thread, so one run per test.

use std::{cell::RefCell, num::NonZeroUsize, rc::Rc, sync::Arc};

use crate::common::{glbx, ic};
use helix_trader::{
    backtest::Backtest,
    strategy::{Part, StandardConfig},
};
use nautilus_common::{actor::DataActor, config::ConfigResult};
use nautilus_model::{
    data::{
        BookOrder, CustomData, DataType, OrderBookDelta, OrderBookDeltas, OrderBookDepth,
        stubs::stub_depth10,
    },
    enums::{BookAction, BookType, OrderSide, RecordFlag},
    identifiers::{InstrumentId, StrategyId},
    types::{Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};
use serde::Deserialize;

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

    fn reads(config: &StandardConfig<Self>) -> ConfigResult<Vec<helix_trader::strategy::Series>> {
        Ok(config.data.series.clone())
    }
}

/// The config trading IC's gold, reading it through `data` alone.
fn config(data: &str) -> StandardConfig<Reader> {
    StandardConfig::load(&format!(
        r#"
        [data]
        series = [{{ data = "{data}", instrument_id = "{gold}" }}]
        warmup = "1h"

        [trade]
        instruments = [{{ instrument_id = "{gold}" }}]
        risk = "0.5%"
        "#,
        gold = ic::INSTRUMENT_ID,
    ))
    .unwrap()
}

/// What of IC's gold the buyer subscribes to.
#[derive(Debug, Clone, Copy)]
enum Reads {
    Imbalances,
    Deltas,
    Depth,
}

/// Buys 1 oz of IC's gold on the first data of it it gets.
#[derive(Debug)]
struct Buyer {
    core: StrategyCore,
    reads: Reads,
    bought: Rc<RefCell<bool>>,
}

nautilus_strategy!(Buyer);

impl Buyer {
    fn buy(&mut self) -> anyhow::Result<()> {
        if std::mem::replace(&mut *self.bought.borrow_mut(), true) {
            return Ok(());
        }
        let order = self.order().market(
            InstrumentId::from(ic::INSTRUMENT_ID),
            OrderSide::Buy,
            Quantity::from(1),
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

impl DataActor for Buyer {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let gold = InstrumentId::from(ic::INSTRUMENT_ID);
        match self.reads {
            Reads::Imbalances => self.subscribe_data(imbalance_type(), None, None),
            Reads::Deltas => {
                self.subscribe_book_deltas(gold, BookType::L3_MBO, None, None, false, None);
            }
            Reads::Depth => self.subscribe_book_depth(
                gold,
                BookType::L2_MBP,
                NonZeroUsize::new(10),
                None,
                true,
                None,
            ),
        }
        Ok(())
    }

    fn on_data(&mut self, _data: &CustomData) -> anyhow::Result<()> {
        self.buy()
    }

    fn on_book_deltas(&mut self, _deltas: &OrderBookDeltas) -> anyhow::Result<()> {
        self.buy()
    }

    fn on_book_depth(&mut self, _depth: &OrderBookDepth) -> anyhow::Result<()> {
        self.buy()
    }
}

/// The data type of IC gold's `Imbalance`s, keyed by its id.
fn imbalance_type() -> DataType {
    DataType::new(glbx::IMBALANCE, None, Some(ic::INSTRUMENT_ID.to_string()))
}

/// Stores `glbx::imbalances()` as IC gold's, over 08:00 to 12:00.
fn store_imbalances(catalog: &tempfile::TempDir) {
    nautilus_serialization::ensure_custom_data_registered::<glbx::Imbalance>();
    let data: Vec<CustomData> = glbx::imbalances()
        .into_iter()
        .map(|imbalance| CustomData::new(Arc::new(imbalance), imbalance_type()))
        .collect();
    ParquetDataCatalog::new(catalog.path(), None, None, None, None)
        .write_custom_data_batch(
            data,
            Some(ic::at("08:00:00").into()),
            Some(ic::at("12:00:00").into()),
            None,
        )
        .unwrap();
}

/// Stores an event of one bid on IC's gold at 09:20, over 08:00 to 12:00.
fn store_deltas(catalog: &tempfile::TempDir) {
    let time = ic::at("09:20:00");
    let delta = OrderBookDelta::new(
        InstrumentId::from(ic::INSTRUMENT_ID),
        BookAction::Add,
        BookOrder::new(OrderSide::Buy, Price::from("4001.00"), Quantity::from(1), 1),
        RecordFlag::F_LAST as u8,
        1,
        time.into(),
        time.into(),
    );
    ParquetDataCatalog::new(catalog.path(), None, None, None, None)
        .write_to_parquet(
            &[delta],
            Some(ic::at("08:00:00").into()),
            Some(ic::at("12:00:00").into()),
            None,
        )
        .unwrap();
}

/// Stores a snapshot of IC gold's book at 09:20, its best bid 4001.00,
/// over 08:00 to 12:00.
fn store_depth(catalog: &tempfile::TempDir) {
    let mut depth = stub_depth10();
    let shift = Price::from("4001.00") - depth.bids[0].price;
    for order in depth.bids.iter_mut().chain(depth.asks.iter_mut()) {
        order.price = order.price + shift;
    }
    depth.instrument_id = InstrumentId::from(ic::INSTRUMENT_ID);
    depth.ts_event = ic::at("09:20:00").into();
    depth.ts_init = depth.ts_event;
    ParquetDataCatalog::new(catalog.path(), None, None, None, None)
        .write_to_parquet(
            &[depth],
            Some(ic::at("08:00:00").into()),
            Some(ic::at("12:00:00").into()),
            None,
        )
        .unwrap();
}

/// Runs a buyer on the requirements of `config` over IC's backtest window
/// and returns how many positions it opened.
fn positions(catalog: &tempfile::TempDir, config: &StandardConfig<Reader>, reads: Reads) -> usize {
    let requirements = config.data_requirements();
    let backtest = Backtest {
        bar_types: requirements.bar_types,
        ticks: requirements.ticks,
        ..ic::backtest(catalog)
    };
    let bought = Rc::new(RefCell::new(false));
    let buyer = Buyer {
        core: StrategyCore::new(config.base.clone()),
        reads,
        bought: Rc::clone(&bought),
    };
    let outcome = backtest.run(buyer).unwrap();
    assert!(*bought.borrow(), "the buyer got no data of gold");
    assert_eq!(outcome.result.total_orders, 1);
    outcome.result.total_positions
}

#[test]
fn a_traded_instrument_read_only_through_its_custom_data_fills_its_orders() {
    let catalog = ic::catalog();
    store_imbalances(&catalog);

    let opened = positions(&catalog, &config("Custom:Imbalance"), Reads::Imbalances);

    assert_eq!(opened, 1);
}

#[test]
fn a_traded_instrument_read_only_through_its_book_deltas_fills_its_orders() {
    let catalog = ic::catalog();
    store_deltas(&catalog);

    let opened = positions(&catalog, &config("order_book_deltas"), Reads::Deltas);

    assert_eq!(opened, 1);
}

#[test]
fn a_traded_instrument_read_only_through_its_depth_fills_its_orders() {
    let catalog = ic::catalog();
    store_depth(&catalog);

    let opened = positions(&catalog, &config("order_book_depths"), Reads::Depth);

    assert_eq!(opened, 1);
}
