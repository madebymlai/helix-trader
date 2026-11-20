//! EMA crossover, an example strategy that trades one instrument.
//!
//! Each time the fast EMA crosses the slow EMA on the instrument's bars, the strategy opens a
//! Nautilus bracket in the cross direction through `strategy::orders`. Crosses count only inside
//! the instrument's session, if it has one. The bracket has:
//! - a take profit and a stop loss at `TAKE_PROFIT_BPS` and `STOP_LOSS_BPS` from the close of the
//!   deciding bar
//! - a hold of `HOLD` from the send
//!
//! The stake sizes the trade to risk `[trade] risk` of equity over the stop's distance, which is
//! one R. Trades may overlap. On a hedging venue each is its own position. On a netting venue
//! they add into one.
//!
//! The config has:
//! - `[data]` lists the one bars series of the traded instrument.
//! - `[trade]` names the instrument and the FX pairs its equity converts through.
//! - `[indicators]` holds the fast and slow EMAs.
//!
//! The signal is a Nautilus indicator fed by the warmup (`Warmup::on_bar`). The strategy decides
//! only once the signal is initialized and the warmup has ended.

mod signal;

use std::rc::Rc;

use jiff::SignedDuration;
use nautilus_common::{
    actor::{
        DataActor,
        indicators::{ActorIndicator, SharedActorIndicator},
    },
    config::{ConfigError, ConfigErrorCollector, ConfigResult},
    messages::data::DataResponseEnvelope,
    timer::TimeEvent,
};
use nautilus_indicators::average::ema::ExponentialMovingAverage;
use nautilus_model::{
    data::{Bar, BarType},
    enums::OrderSide,
    identifiers::{InstrumentId, StrategyId},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rust_decimal::{Decimal, prelude::Signed};
use serde::Deserialize;

use self::signal::Signal;
use crate::strategy::{
    Declaration, Declared, EmaArgs, Part, PlainStrategy, Series, StandardConfig, Visitor,
    orders::{self, Bracket, Exits, Trade},
    requirements::DataRequirements,
    subscriptions::Subscriptions,
    warmup::Warmup,
};

const TAKE_PROFIT_BPS: Decimal = Decimal::from_parts(100, 0, 0, false, 0);

const STOP_LOSS_BPS: Decimal = Decimal::from_parts(50, 0, 0, false, 0);

const HOLD: SignedDuration = SignedDuration::from_hours(4);

/// The config of the EMA crossover strategy.
///
/// Its part, `EmaParams`, has no fields of its own.
pub type EmaCrossConfig = StandardConfig<EmaParams>;

/// The part of the EMA crossover config.
///
/// It has two `ExponentialMovingAverage`s, `[indicators.fast]` and `[indicators.slow]`. The fast
/// one has the shorter period. Exits are fixed in code.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmaParams {}

const FAST: Declared<EmaArgs> = Declared::new("fast");
const SLOW: Declared<EmaArgs> = Declared::new("slow");

impl Part for EmaParams {
    const INSTRUMENTS: usize = 1;
    const INDICATORS: &'static [&'static dyn Declaration] = &[&FAST, &SLOW];

    /// Sets only the strategy id, which Nautilus tags orders with.
    ///
    /// Leaves `oms_type` unset. Nautilus keeps positions as the venue's execution client does, in
    /// a backtest as in live.
    fn default_base() -> StrategyConfig {
        StrategyConfig {
            strategy_id: Some(StrategyId::from("EMA_CROSS-001")),
            ..Default::default()
        }
    }

    /// Returns the bars series of the traded instrument.
    ///
    /// # Errors
    ///
    /// Returns an error if `[data]` lists no bars series of the traded instrument. The error is
    /// reported under the instrument's field.
    fn reads(config: &EmaCrossConfig) -> ConfigResult<Vec<Series>> {
        let id = config.trade_instrument_id();
        config
            .data
            .bar_types()
            .find(|bar_type| bar_type.instrument_id() == id)
            .map(|bar_type| vec![Series::Bars(bar_type)])
            .ok_or_else(|| {
                ConfigError::invalid_reference(
                    "trade.instruments[0].instrument_id",
                    id.to_string(),
                    "has no bars series in data.series, the crossover reads one",
                )
            })
    }

    fn validate(config: &EmaCrossConfig, errors: &mut ConfigErrorCollector) {
        let args = |declared| config.indicators.args(declared);
        if let (Some(fast), Some(slow)) = (args(FAST), args(SLOW)) {
            errors.check(
                fast.period < slow.period,
                ConfigError::range(
                    "indicators.fast.period",
                    format!("must be below the slow period {}", slow.period),
                ),
            );
        }
    }
}

impl EmaCrossConfig {
    /// The one instrument traded.
    #[must_use]
    pub fn trade_instrument_id(&self) -> InstrumentId {
        self.trade.instruments[0].instrument_id
    }

    /// The fast EMA.
    ///
    /// # Panics
    ///
    /// Panics if the EMA is missing or its arguments are refused. `validate` rules this out.
    #[must_use]
    pub fn fast_ema(&self) -> ExponentialMovingAverage {
        self.indicators.build(FAST)
    }

    /// The slow EMA.
    ///
    /// # Panics
    ///
    /// Panics if the EMA is missing or its arguments are refused. `validate` rules this out.
    #[must_use]
    pub fn slow_ema(&self) -> ExponentialMovingAverage {
        self.indicators.build(SLOW)
    }

    /// Returns the bars the crossover reads (`EmaParams::reads`).
    ///
    /// # Panics
    ///
    /// Panics if the traded instrument has no bars series. `validate` refuses such a config.
    #[must_use]
    pub fn trade_bar_type(&self) -> BarType {
        match EmaParams::reads(self).as_deref() {
            Ok([Series::Bars(bar_type)]) => *bar_type,
            read => panic!("a validated config reads the traded instrument's bars: {read:?}"),
        }
    }
}

#[derive(Debug)]
pub struct EmaCross {
    core: StrategyCore,
    config: EmaCrossConfig,
    /// The warmup feeds it bars. It is not registered with Nautilus (Temporary, helix-trader#16
    /// and helix-trader#46).
    signal: Rc<Signal>,
    warmup: Rc<Warmup>,
    subscriptions: Subscriptions,
}

impl EmaCross {
    /// Creates the strategy as a backtest runs it. It requests no warmup, and the indicators
    /// warm on the replay.
    ///
    /// # Errors
    ///
    /// Returns an error if the config is invalid.
    pub fn new(config: EmaCrossConfig) -> anyhow::Result<Self> {
        Self::built(config, Warmup::replayed)
    }

    /// Creates the strategy as a live node runs it. It requests its warmup at start.
    ///
    /// # Errors
    ///
    /// Returns an error if the config is invalid.
    pub fn live(config: EmaCrossConfig) -> anyhow::Result<Self> {
        let ema = Self::built(config, Warmup::live)?;
        Ok(Self {
            subscriptions: ema.subscriptions.live(),
            ..ema
        })
    }

    fn built(
        config: EmaCrossConfig,
        warmup: fn(&DataRequirements) -> Warmup,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let signal = Rc::new(Signal::new(&config));
        let requirements = config.data_requirements();
        let read: Vec<BarType> = requirements
            .bar_types
            .iter()
            .filter(|bar_type| bar_type.instrument_id() == config.trade_instrument_id())
            .copied()
            .collect();
        let warmup =
            warmup(&requirements).feeding(&read, Rc::clone(&signal) as SharedActorIndicator);
        Ok(Self {
            core: StrategyCore::new_checked(config.base.clone())?,
            signal,
            config,
            subscriptions: Subscriptions::new(
                requirements.bar_types,
                requirements.ticks,
                requirements.custom,
            ),
            warmup: Rc::new(warmup),
        })
    }

    /// Returns the warmup. A live node sizes its cache from it.
    #[must_use]
    pub fn warmup(&self) -> Rc<Warmup> {
        Rc::clone(&self.warmup)
    }

    /// Builds the trade on `side` for `bar`.
    ///
    /// Exits land on the nearest tick, and a tie goes away from the close. One R is the distance
    /// from the close to the stop loss.
    fn trade(&self, bar: &Bar, side: OrderSide) -> anyhow::Result<Trade> {
        let instrument_id = self.config.trade_instrument_id();
        let instrument = self
            .cache()
            .instrument(&instrument_id)
            .ok_or_else(|| anyhow::anyhow!("instrument {instrument_id} not in cache"))?;
        let close = bar.close.as_decimal();
        let (take_profit, stop_loss) = exit_prices(close, side);
        let on_tick =
            |exit: Decimal| orders::nearest_tick(&instrument, exit, (exit - close).signum());
        let stop_loss = on_tick(stop_loss)?;
        Ok(Trade::new(
            instrument_id,
            side,
            bar.close,
            (close - stop_loss.as_decimal()).abs(),
            Exits {
                bracket: Some(Bracket {
                    take_profit: on_tick(take_profit)?,
                    stop_loss,
                }),
                time_limit: Some(HOLD),
            },
        ))
    }
}

fn exit_prices(close: Decimal, side: OrderSide) -> (Decimal, Decimal) {
    let favor = match side {
        OrderSide::Sell => -Decimal::ONE,
        _ => Decimal::ONE,
    };
    let bps = |bps: Decimal| close * bps / Decimal::from(10_000);
    (
        close + favor * bps(TAKE_PROFIT_BPS),
        close - favor * bps(STOP_LOSS_BPS),
    )
}

/// Hands the strategy to `visitor` as a plain strategy (`strategy::Visitor`).
pub fn visit<V: Visitor>(visitor: V) -> V::Output {
    visitor.plain::<EmaCross>()
}

impl PlainStrategy for EmaCross {
    type Params = EmaParams;

    fn backtest(config: EmaCrossConfig) -> anyhow::Result<Self> {
        Self::new(config)
    }

    fn live(config: EmaCrossConfig) -> anyhow::Result<Self> {
        Self::live(config)
    }

    fn warmup(&self) -> Rc<Warmup> {
        Self::warmup(self)
    }
}

nautilus_strategy!(EmaCross, {
    /// The instruments the config claims, which the live node registers.
    fn external_order_instrument_ids(&self) -> Option<Vec<InstrumentId>> {
        self.config().external_order_instrument_ids.clone()
    }
});

impl DataActor for EmaCross {
    /// Refuses an instrument that a stake cannot size and re-arms open trades. Live, it then
    /// requests the warmup and subscribes.
    fn on_start(&mut self) -> anyhow::Result<()> {
        orders::refuse_inverse(self, self.config.trade_instrument_id())?;
        orders::arm_open_trades(self)?;
        let now = Rc::clone(&self.warmup).start(self)?;
        self.subscriptions.clone().start(self, now);
        Ok(())
    }

    fn on_data_response(&mut self, response: &DataResponseEnvelope) -> anyhow::Result<()> {
        self.warmup.on_data_response(self, response)
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        // Temporary: live bars through the warmup, helix-trader#16 and helix-trader#46
        self.warmup.on_bar(bar)?;
        if self.warmup.is_warming() || !self.signal.initialized()? {
            return Ok(());
        }
        match self.signal.cross(bar) {
            Some(side) => {
                self.trade(bar, side)?
                    .send(self, self.config.stake(), self.config.rates())
            }
            None => Ok(()),
        }
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        orders::on_time_event(self, event)?;
        Ok(())
    }
}

#[cfg(all(test, feature = "strategy-tests"))]
mod tests;
