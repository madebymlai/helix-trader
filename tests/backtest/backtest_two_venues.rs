//! A backtest on two venues: the data of one, CME gold's book on GLBX
//! (`common::glbx`), the trades on the other, IC's gold (`common::ic`). A
//! strategy reading gold's book buys 10 oz of XAUUSD on IC's first hour
//! bar: each venue is simulated, keeping positions as the real one does
//! (`venue_oms_type`), each with the backtest's balance, and the report
//! shows each venue's account. One node per thread, so one run per test.

use std::{cell::RefCell, num::NonZeroUsize, rc::Rc};

use crate::common::{glbx, ic};
use helix_trader::backtest::report;
use nautilus_common::actor::DataActor;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, OrderBookDepth},
    enums::{BookType, OrderSide},
    identifiers::{InstrumentId, StrategyId},
    types::Quantity,
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};

/// What the strategy got: the times of gold's snapshots, and whether it
/// bought.
#[derive(Debug, Default)]
struct Record {
    snapshots: Vec<UnixNanos>,
    bought: bool,
}

/// Reads gold's book, buys XAUUSD on IC's first hour bar.
#[derive(Debug)]
struct BookReader {
    core: StrategyCore,
    record: Rc<RefCell<Record>>,
}

nautilus_strategy!(BookReader);

impl DataActor for BookReader {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.subscribe_book_depth(
            InstrumentId::from(glbx::INSTRUMENT_ID),
            BookType::L2_MBP,
            NonZeroUsize::new(10),
            None,
            true,
            None,
        );
        self.subscribe_bars(BarType::from(ic::HOUR_BARS), None, None);
        Ok(())
    }

    fn on_book_depth(&mut self, depth: &OrderBookDepth) -> anyhow::Result<()> {
        self.record.borrow_mut().snapshots.push(depth.ts_event);
        Ok(())
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        if std::mem::replace(&mut self.record.borrow_mut().bought, true) {
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

#[test]
fn golds_book_from_glbx_and_a_trade_on_ic_each_venue_with_its_account() {
    let catalog = ic::catalog();
    glbx::store(catalog.path());
    let mut backtest = ic::backtest(&catalog);
    backtest.bar_types.push(BarType::from(glbx::MINUTE_BARS));
    let record = Rc::new(RefCell::new(Record::default()));
    let reader = BookReader {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("READER-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
        record: Rc::clone(&record),
    };

    let outcome = backtest.run(reader).unwrap();

    let record = record.borrow();
    let at = |time| UnixNanos::from(ic::at(time));
    assert_eq!(
        record.snapshots,
        [at("09:00:30"), at("10:15:00"), at("11:45:00")]
    );
    let result = &outcome.result;
    assert_eq!(result.total_orders, 1);
    assert_eq!(result.total_positions, 1);
    // Bought at the ask of the last quote of the 10:00 bar, 4006.10: 10 oz
    // of 40,061.00 USD, IC's commission of 30 USD per million on it, 1.20.
    let summary = &result.summary;
    assert_eq!(summary["venues.total"], "2");
    assert_eq!(
        summary["account.ICMARKETS.balance.USD.total"],
        "1999998.80 USD"
    );
    assert_eq!(summary["account.GLBX.balance.USD.total"], "2000000.00 USD");
    let printed = report(&outcome);
    for line in [
        "account.GLBX.balance.USD.total: 2000000.00 USD",
        "account.ICMARKETS.balance.USD.total: 1999998.80 USD",
    ] {
        assert!(printed.contains(line), "{printed}");
    }
}
