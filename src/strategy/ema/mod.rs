//! EMA crossover, an example strategy trading one instrument: each time the
//! fast EMA crosses the slow one on that instrument's bars, inside its
//! session if it has one, it opens a trade in the cross direction through
//! the shared order module (`strategy::orders`): a Nautilus bracket, its
//! take profit and stop loss 100 and 50 bps from the close of the bar it
//! decided on (`TAKE_PROFIT_BPS`, `STOP_LOSS_BPS`), held 4 hours (`HOLD`)
//! from its send unless either ends it first. It hands the module its time
//! events, which close a trade at its time, and the module sets the time of
//! the trades still open again at start. Trades may overlap: on a hedging
//! venue each is a position of its own, on a netting venue they add into
//! one, as the venue keeps them.
//!
//! The config (`EmaCrossConfig`) lists the series read in `[data]`, whose
//! bars it subscribes to through `strategy::warmup`, its warmup requested
//! there live; `[trade]` names the one instrument traded, read on its one
//! bars series, the only series `[data]` lists, and `[indicators]` its
//! fast and slow EMAs. Its signal is its Nautilus indicator, and it
//! decides only once its indicators are initialized, on the replay in a
//! backtest, on the warmup live. A minimal strategy to drive data,
//! backtest and paper.

mod signal;

use std::rc::Rc;

use jiff::SignedDuration;
use nautilus_common::{
    actor::{DataActor, DataActorNative, indicators::SharedActorIndicator},
    config::{ConfigError, ConfigErrorCollector, ConfigResult},
    messages::data::DataResponseEnvelope,
    timer::TimeEvent,
};
use nautilus_indicators::average::ema::ExponentialMovingAverage;
use nautilus_model::{
    data::{Bar, BarType},
    enums::OrderSide,
    identifiers::{InstrumentId, StrategyId},
    instruments::Instrument,
    types::Quantity,
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rust_decimal::Decimal;
use serde::Deserialize;

use self::signal::Signal;
use crate::strategy::{
    DataRequirements, Declaration, Declared, EmaArgs, Part, Series, StandardConfig,
    orders::{self, Exits, Trade},
    warmup::Warmup,
};

/// The take profit of each trade, in basis points of the close of the bar
/// it was decided on, in the trade's favour.
const TAKE_PROFIT_BPS: Decimal = Decimal::from_parts(100, 0, 0, false, 0);

/// The stop loss of each trade, in basis points of the close of the bar it
/// was decided on, against the trade.
const STOP_LOSS_BPS: Decimal = Decimal::from_parts(50, 0, 0, false, 0);

/// How long a trade is held after its send, unless its take profit or stop
/// loss ends it first.
const HOLD: SignedDuration = SignedDuration::from_hours(4);

/// Config of the EMA crossover strategy, loaded from TOML: the standard
/// tables, its EMAs in `[indicators]`, and no fields of its own
/// (`EmaParams`).
pub type EmaCrossConfig = StandardConfig<EmaParams>;

/// The crossover's part: no fields of its own. It reads two Nautilus
/// `ExponentialMovingAverage`s, `[indicators.fast]` and
/// `[indicators.slow]`, the fast one of the shorter period; it trades one
/// instrument, the one of `[trade]`, on its one bars series of `[data]`,
/// the only series it reads. Its exits are its code's (`TAKE_PROFIT_BPS`,
/// `STOP_LOSS_BPS`, `HOLD`).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmaParams {}

/// The fast EMA, `[indicators.fast]`.
const FAST: Declared<EmaArgs> = Declared::new("fast");
/// The slow EMA, `[indicators.slow]`.
const SLOW: Declared<EmaArgs> = Declared::new("slow");

impl Part for EmaParams {
    const INSTRUMENTS: usize = 1;
    const INDICATORS: &'static [&'static dyn Declaration] = &[&FAST, &SLOW];

    /// The strategy's id alone, `EMA_CROSS-001`: Nautilus tags its orders
    /// with the id's tag. `oms_type` is left unset: Nautilus keeps the
    /// strategy's positions as the execution client of the venue does,
    /// netting or hedging, in a backtest as live.
    fn default_base() -> StrategyConfig {
        StrategyConfig {
            strategy_id: Some(StrategyId::from("EMA_CROSS-001")),
            ..Default::default()
        }
    }

    /// The bars series of the traded instrument, the first `[data]` lists:
    /// the crossover reads these bars alone (`trade_bar_type`).
    ///
    /// # Errors
    ///
    /// Returns an error under the traded instrument if `[data]` lists no
    /// bars series of it.
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

    /// The fast period not below the slow one.
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

    /// The size of each trade.
    #[must_use]
    pub fn trade_size(&self) -> Quantity {
        self.trade.instruments[0].trade_size
    }

    /// The fast EMA, `[indicators.fast]`, built.
    ///
    /// # Panics
    ///
    /// Panics if it is missing or of arguments it or Nautilus refuses,
    /// which `validate` refuses.
    #[must_use]
    pub fn fast_ema(&self) -> ExponentialMovingAverage {
        self.indicators.build(FAST)
    }

    /// The slow EMA, `[indicators.slow]`, built.
    ///
    /// # Panics
    ///
    /// As `fast_ema`.
    #[must_use]
    pub fn slow_ema(&self) -> ExponentialMovingAverage {
        self.indicators.build(SLOW)
    }

    /// The bars the crossover reads, its part's answer
    /// (`EmaParams::reads`).
    ///
    /// # Panics
    ///
    /// Panics if the traded instrument has no bars series, which
    /// `validate` refuses.
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
    /// The crossover, registered with Nautilus as the strategy's indicator.
    signal: Rc<Signal>,
    /// Its warmup, which feeds the signal.
    warmup: Rc<Warmup>,
}

impl EmaCross {
    /// The strategy as a backtest runs it: it requests no warmup, and its
    /// indicators warm on the data replayed.
    ///
    /// # Errors
    ///
    /// Returns an error if the config is invalid.
    pub fn new(config: EmaCrossConfig) -> anyhow::Result<Self> {
        Self::built(config, Warmup::replayed)
    }

    /// The strategy as a live node runs it: it requests its warmup
    /// (`DataConfig::warmup`) at start, which hydrates its indicators.
    ///
    /// # Errors
    ///
    /// Returns an error if the config is invalid.
    pub fn live(config: EmaCrossConfig) -> anyhow::Result<Self> {
        Self::built(config, Warmup::live)
    }

    /// The strategy of `config`, its warmup `warmup` of its data
    /// requirements, set to feed the signal.
    fn built(
        config: EmaCrossConfig,
        warmup: fn(&DataRequirements) -> Warmup,
    ) -> anyhow::Result<Self> {
        config.validate()?;
        let signal = Rc::new(Signal::new(&config));
        let warmup =
            warmup(&config.data_requirements()).feeding(Rc::clone(&signal) as SharedActorIndicator);
        Ok(Self {
            core: StrategyCore::new_checked(config.base.clone())?,
            signal,
            config,
            warmup: Rc::new(warmup),
        })
    }

    /// Its warmup: a live node sizes its cache from it.
    #[must_use]
    pub fn warmup(&self) -> Rc<Warmup> {
        Rc::clone(&self.warmup)
    }

    /// The trade on `side` decided on `bar`: `trade_size` of the traded
    /// instrument, its take profit and stop loss at their distances from
    /// the bar's close, rounded to the instrument's prices, held `HOLD`.
    /// The module rounds the size to the instrument's sizes: one rounding
    /// to zero, as a trade size off that grid leaves, is an error and no
    /// order, logged by Nautilus.
    fn trade(&self, bar: &Bar, side: OrderSide) -> anyhow::Result<Trade> {
        let instrument_id = self.config.trade_instrument_id();
        let instrument = self
            .cache()
            .instrument(&instrument_id)
            .ok_or_else(|| anyhow::anyhow!("instrument {instrument_id} not in cache"))?;
        let (take_profit, stop_loss) = exit_prices(bar.close.as_decimal(), side);
        Ok(Trade {
            instrument_id,
            side,
            quantity: self.config.trade_size(),
            exits: Exits {
                take_profit: Some(instrument.try_make_price_from_decimal(take_profit)?),
                stop_loss: Some(instrument.try_make_price_from_decimal(stop_loss)?),
                time_limit: Some(HOLD),
            },
        })
    }
}

/// The take profit and the stop loss, unrounded, of a trade on `side`
/// decided on a bar closing at `close`: `TAKE_PROFIT_BPS` of it in the
/// trade's favour, `STOP_LOSS_BPS` against it.
fn exit_prices(close: Decimal, side: OrderSide) -> (Decimal, Decimal) {
    let favour = match side {
        OrderSide::Sell => -Decimal::ONE,
        _ => Decimal::ONE,
    };
    let bps = |bps: Decimal| close * bps / Decimal::from(10_000);
    (
        close + favour * bps(TAKE_PROFIT_BPS),
        close - favour * bps(STOP_LOSS_BPS),
    )
}

nautilus_strategy!(EmaCross);

impl DataActor for EmaCross {
    /// Sets the time of each trade still open again, as after a restart
    /// (`orders::arm_open_trades`), registers the signal for the traded
    /// instrument's bars, then subscribes to the bars, live after
    /// requesting the warmup.
    fn on_start(&mut self) -> anyhow::Result<()> {
        orders::arm_open_trades(self)?;
        let bar_type = self.config.trade_bar_type();
        let signal = Rc::clone(&self.signal) as SharedActorIndicator;
        self.core_mut()
            .register_indicator_for_bars(bar_type, signal);
        Rc::clone(&self.warmup).start(self)
    }

    /// The warmup's bars reach only the cache: the warmup hands them to
    /// the signal, and logs its end; Nautilus logs a failed one as an error.
    fn on_data_response(&mut self, response: &DataResponseEnvelope) -> anyhow::Result<()> {
        self.warmup.on_data_response(self, response)
    }

    /// Nautilus has fed the bar to the signal; the strategy decides on it
    /// once its indicators are initialized.
    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        if !self.core().indicators_initialized()? {
            return Ok(());
        }
        match self.signal.cross(bar) {
            Some(side) => self.trade(bar, side)?.send(self),
            None => Ok(()),
        }
    }

    /// A trade's time closes it; the strategy has no other timer.
    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        orders::on_time_event(self, event)?;
        Ok(())
    }
}

#[cfg(all(test, feature = "strategy-tests"))]
mod tests;
