//! EMA crossover: per instrument, long while the fast EMA is above the slow
//! one, short otherwise. A minimal strategy to drive data, backtest and paper.

mod book;
mod config;

pub use config::EmaCrossConfig;

use nautilus_common::actor::DataActor;
use nautilus_model::{
    data::Bar,
    enums::OrderSide,
    identifiers::InstrumentId,
    instruments::Instrument,
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyCore},
};
use rust_decimal::Decimal;

use self::book::Book;

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

    /// Submits a market order moving the position to the book's target,
    /// unless an order for the instrument is still pending.
    fn rebalance(&mut self, instrument_id: InstrumentId) -> anyhow::Result<()> {
        let Some(target) = self.book.target(&instrument_id) else {
            return Ok(());
        };
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
        let side = if delta > Decimal::ZERO { OrderSide::Buy } else { OrderSide::Sell };
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
        let start = self.clock().utc_now().checked_sub(self.config.warmup)?;
        for bar_type in self.config.bar_types() {
            self.request_bars(bar_type, Some(start), None, None, None, None)?;
            self.subscribe_bars(bar_type, None, None);
        }
        Ok(())
    }

    fn on_historical_bars(&mut self, bars: &[Bar]) -> anyhow::Result<()> {
        for bar in bars {
            self.book.update(bar);
        }
        Ok(())
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        self.book.update(bar);
        self.rebalance(bar.bar_type.instrument_id())
    }
}
