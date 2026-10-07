use std::collections::HashMap;

use nautilus_indicators::{
    average::ema::ExponentialMovingAverage,
    indicator::{Indicator, MovingAverage},
};
use nautilus_model::{data::Bar, identifiers::InstrumentId};
use rust_decimal::Decimal;

use super::config::EmaCrossConfig;

/// Target book of the EMA crossover: long `trade_size` while the fast EMA is
/// above the slow one, short otherwise.
#[derive(Debug)]
pub(super) struct Book {
    signals: HashMap<InstrumentId, Crossover>,
    trade_size: Decimal,
}

impl Book {
    pub(super) fn new(config: &EmaCrossConfig) -> Self {
        let signals = config
            .instrument_ids
            .iter()
            .map(|id| (*id, Crossover::new(config.fast_period, config.slow_period)))
            .collect();
        Self {
            signals,
            trade_size: config.trade_size.as_decimal(),
        }
    }

    /// Feeds a bar of a traded instrument; bars of other instruments are ignored.
    pub(super) fn update(&mut self, bar: &Bar) {
        if let Some(signal) = self.signals.get_mut(&bar.bar_type.instrument_id()) {
            signal.update(bar);
        }
    }

    /// Signed target position, or `None` until both EMAs are warm.
    pub(super) fn target(&self, instrument_id: &InstrumentId) -> Option<Decimal> {
        let signal = self.signals.get(instrument_id)?;
        signal.is_long().map(|long| {
            if long {
                self.trade_size
            } else {
                -self.trade_size
            }
        })
    }
}

#[derive(Debug)]
struct Crossover {
    fast: ExponentialMovingAverage,
    slow: ExponentialMovingAverage,
}

impl Crossover {
    fn new(fast_period: usize, slow_period: usize) -> Self {
        Self {
            fast: ExponentialMovingAverage::new(fast_period, None),
            slow: ExponentialMovingAverage::new(slow_period, None),
        }
    }

    fn update(&mut self, bar: &Bar) {
        self.fast.handle_bar(bar);
        self.slow.handle_bar(bar);
    }

    fn is_long(&self) -> Option<bool> {
        (self.fast.initialized() && self.slow.initialized())
            .then(|| self.fast.value() > self.slow.value())
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::data::BarType;

    use super::*;

    fn config() -> EmaCrossConfig {
        toml::from_str(
            r#"
            instrument_ids = ["ESM4.CME"]
            bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }
            fast_period = 2
            slow_period = 4
            trade_size = "3"
            warmup = "24h"
            "#,
        )
        .unwrap()
    }

    fn bar(close: &str, ts: u64) -> Bar {
        let bar_type = BarType::from("ESM4.CME-1-HOUR-LAST-EXTERNAL");
        Bar::new(
            bar_type,
            close.into(),
            close.into(),
            close.into(),
            close.into(),
            "1".into(),
            ts.into(),
            ts.into(),
        )
    }

    fn feed(book: &mut Book, closes: &[&str]) {
        for (i, close) in closes.iter().enumerate() {
            book.update(&bar(close, i as u64 + 1));
        }
    }

    #[test]
    fn no_target_until_warm() {
        let mut book = Book::new(&config());
        feed(&mut book, &["100", "101", "102"]);
        assert_eq!(book.target(&InstrumentId::from("ESM4.CME")), None);
    }

    #[test]
    fn long_in_an_uptrend() {
        let mut book = Book::new(&config());
        feed(&mut book, &["100", "101", "102", "103", "104", "105"]);
        assert_eq!(
            book.target(&InstrumentId::from("ESM4.CME")),
            Some(Decimal::from(3))
        );
    }

    #[test]
    fn short_in_a_downtrend() {
        let mut book = Book::new(&config());
        feed(&mut book, &["105", "104", "103", "102", "101", "100"]);
        assert_eq!(
            book.target(&InstrumentId::from("ESM4.CME")),
            Some(Decimal::from(-3))
        );
    }

    #[test]
    fn ignores_untraded_instruments() {
        let mut book = Book::new(&config());
        feed(&mut book, &["100", "101", "102", "103", "104", "105"]);
        assert_eq!(book.target(&InstrumentId::from("ESH4.CME")), None);
    }
}
