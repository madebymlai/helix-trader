//! A recording strategy: it warms up through `strategy::warmup` as a real
//! strategy does, on live bars if told to (`warmup::WarmUp`), records every bar it gets, warmup and replayed, and
//! every fill, and can rest a sell stop on its first replayed bar or send
//! market orders of its choosing on the first replayed bars. For the tests
//! that need what the EMA crossover cannot show: the bars' open, high and
//! low, stop orders, and fills of a given side and size.

use std::{cell::RefCell, rc::Rc};

use helix_trader::strategy::warmup::{self, WarmUp};
use jiff::SignedDuration;
use nautilus_common::actor::DataActor;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, QuoteTick},
    enums::OrderSide,
    events::OrderFilled,
    identifiers::{InstrumentId, StrategyId},
    types::{Price, Quantity},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};

/// What the recorder got, in order.
#[derive(Debug, Default)]
pub struct Record {
    /// The warmup bars `warmup::bars` returned.
    pub warmup: Vec<Bar>,
    /// The bars of `on_bar`.
    pub bars: Vec<Bar>,
    /// Side, price and time of every fill.
    pub fills: Vec<(OrderSide, Price, UnixNanos)>,
}

#[derive(Debug)]
pub struct Recorder {
    core: StrategyCore,
    bar_type: BarType,
    warmup: SignedDuration,
    /// The trigger of the sell stop of 10 shares sent on the first bar.
    stop: Option<Price>,
    /// The market order sent on each of the first replayed bars, in order.
    market: Vec<(OrderSide, Quantity)>,
    /// The instruments warmed up on live bars.
    live_bars: Vec<InstrumentId>,
    record: Rc<RefCell<Record>>,
}

impl Recorder {
    /// A recorder of `bar_type` after `warmup`, resting a sell stop at
    /// `stop` if given, and the record it fills.
    pub fn new(
        bar_type: BarType,
        warmup: SignedDuration,
        stop: Option<Price>,
    ) -> (Self, Rc<RefCell<Record>>) {
        let record = Rc::new(RefCell::new(Record::default()));
        let recorder = Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from("RECORDER-001")),
                order_id_tag: Some("001".to_string()),
                ..Default::default()
            }),
            bar_type,
            warmup,
            stop,
            market: vec![],
            live_bars: vec![],
            record: Rc::clone(&record),
        };
        (recorder, record)
    }

    /// A recorder of `bar_type` after `warmup` sending the market order
    /// `market[i]` on the `i`-th replayed bar, and the record it fills.
    pub fn trading(
        bar_type: BarType,
        warmup: SignedDuration,
        market: Vec<(OrderSide, Quantity)>,
    ) -> (Self, Rc<RefCell<Record>>) {
        let (recorder, record) = Self::new(bar_type, warmup, None);
        (Self { market, ..recorder }, record)
    }
}

impl WarmUp for Recorder {
    fn warm_up_on_live_bars(&mut self, instrument_ids: &[InstrumentId]) {
        self.live_bars = instrument_ids.to_vec();
    }
}

nautilus_strategy!(Recorder, {
    fn on_order_filled(&mut self, event: &OrderFilled) {
        self.record
            .borrow_mut()
            .fills
            .push((event.order_side, event.last_px, event.ts_event));
    }
});

impl DataActor for Recorder {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let (bar_types, warmup) = ([self.bar_type], self.warmup);
        let live_bars = self.live_bars.clone();
        warmup::start(self, &bar_types, warmup, &live_bars)
    }

    fn on_historical_quotes(&mut self, quotes: &[QuoteTick]) -> anyhow::Result<()> {
        let bars = warmup::bars(self, quotes);
        self.record.borrow_mut().warmup.extend(bars);
        Ok(())
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        let index = self.record.borrow().bars.len();
        let first = index == 0;
        self.record.borrow_mut().bars.push(*bar);
        if let Some(&(side, quantity)) = self.market.get(index) {
            let order = self.order().market(
                bar.bar_type.instrument_id(),
                side,
                quantity,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            );
            self.submit_order(order, None, None, None)?;
        }
        let Some(trigger) = self.stop.filter(|_| first) else {
            return Ok(());
        };
        let order = self.order().stop_market(
            bar.bar_type.instrument_id(),
            OrderSide::Sell,
            Quantity::from(10),
            trigger,
            None,
            None,
            None,
            None,
            None,
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
