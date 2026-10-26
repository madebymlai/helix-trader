//! CME gold's book in a backtest (`common::glbx`): a strategy subscribed to
//! GC.v.0.GLBX's book depth gets the snapshots the catalog stores of the
//! run's window, in order, and the cache's book (`cache.order_book`) holds
//! each as it arrives. Gold's bars have no source in the catalog (no
//! quotes): they are left out, and the run goes on. One node per thread,
//! so one run per test.

use std::{cell::RefCell, num::NonZeroUsize, rc::Rc};

use crate::common::{glbx, ic::at};
use helix_trader::backtest::{Backtest, commission_rate, venue_oms_type};
use jiff::SignedDuration;
use nautilus_common::{actor::DataActor, logging::logger::LoggerConfig};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{BarType, OrderBookDepth},
    enums::BookType,
    identifiers::{InstrumentId, StrategyId},
    types::{Money, Price},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rust_decimal::Decimal;

/// Each snapshot got: its time, its best bid, and the cache's book's then.
type Snapshot = (UnixNanos, Price, Option<Price>);

/// Subscribes to gold's book depth, managed, and records each snapshot.
#[derive(Debug)]
struct BookProbe {
    core: StrategyCore,
    snapshots: Rc<RefCell<Vec<Snapshot>>>,
}

nautilus_strategy!(BookProbe);

impl DataActor for BookProbe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.subscribe_book_depth(
            InstrumentId::from(glbx::INSTRUMENT_ID),
            BookType::L2_MBP,
            NonZeroUsize::new(10),
            None,
            true,
            None,
        );
        Ok(())
    }

    fn on_book_depth(&mut self, depth: &OrderBookDepth) -> anyhow::Result<()> {
        let cached = self
            .cache()
            .order_book(&depth.instrument_id)
            .and_then(|book| book.best_bid_price());
        self.snapshots
            .borrow_mut()
            .push((depth.ts_event, depth.bids[0].price, cached));
        Ok(())
    }
}

#[test]
fn a_strategy_subscribed_to_golds_book_gets_its_stored_snapshots() {
    let dir = tempfile::tempdir().unwrap();
    glbx::store(dir.path());
    let snapshots = Rc::new(RefCell::new(Vec::new()));
    let probe = BookProbe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("PROBE-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
        snapshots: Rc::clone(&snapshots),
    };
    let backtest = Backtest {
        catalog: dir.path().to_path_buf(),
        bar_types: vec![BarType::from(glbx::MINUTE_BARS)],
        start: at("09:00:00"),
        end: at("12:00:00"),
        warmup: SignedDuration::ZERO,
        fee_model: commission_rate(Decimal::ZERO).unwrap(),
        balance: Money::from("1000000 USD"),
        leverage: Decimal::ONE,
        venue_oms: venue_oms_type,
        logging: LoggerConfig::default(),
    };

    backtest.run(probe).unwrap();

    let price = Price::from;
    // The run's window only: not the snapshot of 08:30.
    assert_eq!(
        *snapshots.borrow(),
        [
            (
                at("09:00:30").into(),
                price("4011.00"),
                Some(price("4011.00"))
            ),
            (
                at("10:15:00").into(),
                price("4012.50"),
                Some(price("4012.50"))
            ),
            (
                at("11:45:00").into(),
                price("4009.00"),
                Some(price("4009.00"))
            ),
        ]
    );
}
