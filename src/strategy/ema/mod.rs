//! EMA crossover: per instrument, long while the fast EMA is above the slow
//! one, short otherwise, on the bars inside its session, if it has one. It
//! turns by closing its position by id, then opening the other side, so it
//! trades alike on netting and hedging venues. A
//! minimal strategy to drive data, backtest and paper. Its bars are built
//! by Nautilus from the quotes or from source bars, and warmed up through
//! `strategy::warmup`.

mod book;
mod config;

pub use config::EmaCrossConfig;

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

    /// Moves the position of the bar's instrument to the book's target on
    /// `bar`, unless an order for the instrument is still pending: a target
    /// on the other side of the position closes the open positions
    /// (`close_position`, each by its id), then a market order opens the
    /// target; else a market order adds the difference. So a hedging venue
    /// reduces as a netting one does, never holding opposite positions.
    /// The order is rounded to the instrument's size precision: one
    /// rounding to zero, as a trade size off that grid leaves, is an error
    /// and no order, logged by Nautilus.
    fn rebalance(&mut self, bar: &Bar) -> anyhow::Result<()> {
        let Some(target) = self.book.target(bar) else {
            return Ok(());
        };
        let instrument_id = bar.bar_type.instrument_id();
        let net = self.portfolio().net_position(&instrument_id);
        if target == net {
            return Ok(());
        }
        let (instrument, open) = {
            let cache = self.cache();
            let id = Some(&instrument_id);
            if cache.has_orders_open(None, id, None, None, None)
                || cache.has_orders_inflight(None, id, None, None, None)
            {
                return Ok(());
            }
            let instrument = cache
                .instrument(&instrument_id)
                .ok_or_else(|| anyhow::anyhow!("instrument {instrument_id} not in cache"))?;
            let open = cache.positions_open(None, id, None, None, None);
            (instrument, open)
        };
        let crosses = target == Decimal::ZERO || (target > Decimal::ZERO) != (net > Decimal::ZERO);
        let delta = if net != Decimal::ZERO && crosses {
            for position in &open {
                self.close_position(position, None, None, None, None, None, None)?;
            }
            target
        } else {
            target - net
        };
        if delta == Decimal::ZERO {
            return Ok(());
        }
        let side = if delta > Decimal::ZERO {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        let quantity = instrument
            .try_make_qty_from_decimal(delta.abs(), None)
            // In the message, not as context: Nautilus logs only the top
            // error of the chain.
            .map_err(|e| anyhow::anyhow!("ordering {delta} of {instrument_id}: {e}"))?;
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
        let (bar_types, warmup) = (self.config.bars.bar_types(), self.config.bars.warmup);
        warmup::start(self, &bar_types, warmup)
    }

    fn on_historical_quotes(&mut self, quotes: &[QuoteTick]) -> anyhow::Result<()> {
        for bar in warmup::bars(self, quotes) {
            self.book.update(&bar);
        }
        Ok(())
    }

    fn on_historical_bars(&mut self, bars: &[Bar]) -> anyhow::Result<()> {
        for bar in warmup::bars(self, bars) {
            self.book.update(&bar);
        }
        Ok(())
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        self.book.update(bar);
        self.rebalance(bar)
    }
}
