//! A strategy declaring tick series and no bar types: the run replays
//! exactly the declared series, IC's gold quotes (`common::ic`) and CME
//! gold's trades and book depth (`common::glbx`), and nothing else the
//! catalog stores. The probe subscribes to the quotes, trades and book
//! depth of both instruments and records each it gets. One node per
//! thread, so one run per test.

use std::{cell::RefCell, num::NonZeroUsize, rc::Rc};

use crate::common::{glbx, ic};
use helix_trader::{
    backtest::Backtest,
    strategy::{TickData, TickSeries},
};
use nautilus_common::actor::DataActor;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{OrderBookDepth, QuoteTick, TradeTick},
    enums::BookType,
    identifiers::{InstrumentId, StrategyId},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};

/// What the probe got: its kind, its instrument and its time.
type Got = (TickData, InstrumentId, UnixNanos);

/// Subscribes to every tick series of both instruments and records each
/// tick it gets.
#[derive(Debug)]
struct TickProbe {
    core: StrategyCore,
    got: Rc<RefCell<Vec<Got>>>,
}

nautilus_strategy!(TickProbe);

impl DataActor for TickProbe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        for id in [ic::INSTRUMENT_ID, glbx::INSTRUMENT_ID].map(InstrumentId::from) {
            self.subscribe_quotes(id, None, None);
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
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        let got = (TickData::Quotes, quote.instrument_id, quote.ts_init);
        self.got.borrow_mut().push(got);
        Ok(())
    }

    fn on_trade(&mut self, trade: &TradeTick) -> anyhow::Result<()> {
        let got = (TickData::Trades, trade.instrument_id, trade.ts_init);
        self.got.borrow_mut().push(got);
        Ok(())
    }

    fn on_book_depth(&mut self, depth: &OrderBookDepth) -> anyhow::Result<()> {
        let got = (TickData::Depth, depth.instrument_id, depth.ts_init);
        self.got.borrow_mut().push(got);
        Ok(())
    }
}

/// A catalog of IC's gold and its quotes, CME gold, its book depth and its
/// trades, all stored over 08:00 to 12:00.
fn catalog() -> tempfile::TempDir {
    let dir = ic::catalog();
    glbx::store(dir.path());
    glbx::store_trades(dir.path());
    dir
}

/// Runs the probe over `[09:00, 12:00]` on `catalog`, declaring `ticks`
/// and no bar types, and returns what it got of each kind and instrument.
fn run(catalog: &tempfile::TempDir, ticks: Vec<TickSeries>) -> Vec<Got> {
    let got = Rc::new(RefCell::new(Vec::new()));
    let probe = TickProbe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("PROBE-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
        got: Rc::clone(&got),
    };
    let backtest = Backtest {
        bar_types: vec![],
        ticks,
        ..ic::backtest(catalog)
    };
    backtest.run(probe).unwrap();
    got.take()
}

fn tick(data: TickData, instrument_id: &str) -> TickSeries {
    TickSeries {
        data,
        instrument_id: InstrumentId::from(instrument_id),
    }
}

/// The times of what `got` holds of `data` of `instrument_id`.
fn times(got: &[Got], data: TickData, instrument_id: &str) -> Vec<UnixNanos> {
    let id = InstrumentId::from(instrument_id);
    got.iter()
        .filter(|&&(d, i, _)| d == data && i == id)
        .map(|&(_, _, ts)| ts)
        .collect()
}

fn at(time: &str) -> UnixNanos {
    ic::at(time).into()
}

#[test]
fn a_strategy_declaring_ticks_and_no_bar_types_gets_the_declared_ticks_of_the_run() {
    let catalog = catalog();

    let got = run(
        &catalog,
        vec![
            tick(TickData::Quotes, ic::INSTRUMENT_ID),
            tick(TickData::Trades, glbx::INSTRUMENT_ID),
            tick(TickData::Depth, glbx::INSTRUMENT_ID),
        ],
    );

    // Two IC quotes a minute, at :07.250 and :37.750, over the run's three
    // hours.
    let quotes = times(&got, TickData::Quotes, ic::INSTRUMENT_ID);
    assert_eq!(quotes.len(), 360);
    assert_eq!(quotes[0], at("09:00:07.250"));
    assert_eq!(quotes[359], at("11:59:37.750"));
    // The run's window only: not the trade of 08:45 nor the snapshot of
    // 08:30.
    assert_eq!(
        times(&got, TickData::Trades, glbx::INSTRUMENT_ID),
        [at("09:10:00"), at("11:50:00")]
    );
    assert_eq!(
        times(&got, TickData::Depth, glbx::INSTRUMENT_ID),
        [at("09:00:30"), at("10:15:00"), at("11:45:00")]
    );
    // Nothing else: no IC trades or book, no CME quotes.
    assert_eq!(got.len(), 360 + 2 + 3);
}

#[test]
fn a_series_the_catalog_stores_but_the_strategy_does_not_declare_is_not_replayed() {
    let catalog = catalog();

    let got = run(
        &catalog,
        vec![
            tick(TickData::Quotes, ic::INSTRUMENT_ID),
            tick(TickData::Trades, glbx::INSTRUMENT_ID),
        ],
    );

    assert_eq!(
        times(&got, TickData::Depth, glbx::INSTRUMENT_ID),
        Vec::<UnixNanos>::new()
    );
    assert_eq!(
        times(&got, TickData::Trades, glbx::INSTRUMENT_ID),
        [at("09:10:00"), at("11:50:00")]
    );
    assert_eq!(got.len(), 360 + 2);
}
