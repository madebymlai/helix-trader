//! EMA crossover: per instrument, long while the fast EMA is above the slow
//! one, short otherwise, on the bars inside the session only. A minimal
//! strategy to drive data, backtest and paper. Its bars are built by
//! Nautilus from the quotes, and warmed up through `strategy::warmup`.

mod book;
mod config;
mod session;

pub use config::EmaCrossConfig;
pub use session::Session;

use nautilus_common::actor::DataActor;
use nautilus_model::{
    data::{Bar, QuoteTick},
    enums::OrderSide,
    instruments::Instrument,
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyCore},
};
use rust_decimal::Decimal;

use self::book::Book;
use crate::strategy::warmup;

#[derive(Debug)]
pub struct EmaCross {
    core: StrategyCore,
    config: EmaCrossConfig,
    book: Book,
}

impl EmaCross {
    /// # Errors
    ///
    /// Returns an error if the config is invalid.
    pub fn new(config: EmaCrossConfig) -> anyhow::Result<Self> {
        config.validate()?;
        Ok(Self {
            core: StrategyCore::new_checked(config.base.clone())?,
            book: Book::new(&config),
            config,
        })
    }

    /// Submits a market order moving the position of the bar's instrument
    /// to the book's target on `bar`, unless an order for the instrument is
    /// still pending.
    fn rebalance(&mut self, bar: &Bar) -> anyhow::Result<()> {
        let Some(target) = self.book.target(bar) else {
            return Ok(());
        };
        let instrument_id = bar.bar_type.instrument_id();
        let delta = target - self.portfolio().net_position(&instrument_id);
        if delta == Decimal::ZERO {
            return Ok(());
        }
        let instrument = {
            let cache = self.cache();
            let id = Some(&instrument_id);
            if !cache.orders_open(None, id, None, None, None).is_empty()
                || !cache.orders_inflight(None, id, None, None, None).is_empty()
            {
                return Ok(());
            }
            cache
                .instrument(&instrument_id)
                .ok_or_else(|| anyhow::anyhow!("instrument {instrument_id} not in cache"))?
        };
        let side = if delta > Decimal::ZERO {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        let quantity = instrument.make_qty_from_decimal(delta.abs(), None);
        let order = self.order().market(
            instrument_id,
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
        self.submit_order(order, None, None, None)
    }
}

nautilus_strategy!(EmaCross);

impl DataActor for EmaCross {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let (bar_types, warmup) = (self.config.bar_types(), self.config.warmup);
        warmup::start(self, &bar_types, warmup)
    }

    fn on_historical_quotes(&mut self, quotes: &[QuoteTick]) -> anyhow::Result<()> {
        for bar in warmup::bars(self, quotes) {
            self.book.update(&bar);
        }
        Ok(())
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        self.book.update(bar);
        self.rebalance(bar)
    }
}
