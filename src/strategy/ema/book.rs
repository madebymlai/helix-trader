use std::collections::HashMap;

use jiff::SignedDuration;
use nautilus_indicators::{
    average::ema::ExponentialMovingAverage,
    indicator::{Indicator, MovingAverage},
};
use nautilus_model::{data::Bar, identifiers::InstrumentId};
use rust_decimal::Decimal;

use super::config::EmaCrossConfig;
use crate::strategy::Session;

/// Target book of the EMA crossover: long `trade_size` while the fast EMA is
/// above the slow one, short otherwise. With a session, only bars wholly
/// inside it count: the others neither move the EMAs nor get a target.
#[derive(Debug)]
pub(super) struct Book {
    signals: HashMap<InstrumentId, Crossover>,
    trade_size: Decimal,
    session: Option<Session>,
    bar_length: SignedDuration,
}

impl Book {
    pub(super) fn new(config: &EmaCrossConfig) -> Self {
        let signals = config
            .bars
            .instrument_ids
            .iter()
            .map(|id| (*id, Crossover::new(config.fast_period, config.slow_period)))
            .collect();
        Self {
            signals,
            trade_size: config.trade_size.as_decimal(),
            session: config.bars.session.clone(),
            bar_length: config.bars.bar_spec.timedelta(),
        }
    }

    /// Feeds a bar of a traded instrument inside the session; other bars are
    /// ignored.
    pub(super) fn update(&mut self, bar: &Bar) {
        if !self.in_session(bar) {
            return;
        }
        if let Some(signal) = self.signals.get_mut(&bar.bar_type.instrument_id()) {
            signal.update(bar);
        }
    }

    /// Signed target position to trade on `bar`, or `None` if the bar is
    /// outside the session or until both EMAs are warm.
    pub(super) fn target(&self, bar: &Bar) -> Option<Decimal> {
        if !self.in_session(bar) {
            return None;
        }
        let signal = self.signals.get(&bar.bar_type.instrument_id())?;
        signal.is_long().map(|long| {
            if long {
                self.trade_size
            } else {
                -self.trade_size
            }
        })
    }

    /// Whether the bar, stamped on its close, lies wholly inside the
    /// session; every bar does without one.
    fn in_session(&self, bar: &Bar) -> bool {
        self.session
            .as_ref()
            .is_none_or(|session| session.contains(bar.ts_event.into(), self.bar_length))
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
    use jiff::{SignedDuration, Timestamp};
    use nautilus_model::data::BarType;

    use super::*;

    fn config() -> EmaCrossConfig {
        toml::from_str(
            r#"
            instrument_ids = ["ESM4.CME"]
            bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" }
            fast_period = 2
            slow_period = 4
            trade_size = "3"
            warmup = "24h"
            session = { time_zone = "Europe/Berlin", open = "09:00", close = "17:30" }
            "#,
        )
        .unwrap()
    }

    fn bar_on(instrument: &str, close: &str, ts: Timestamp) -> Bar {
        let bar_type = BarType::from(format!("{instrument}-1-HOUR-MID-INTERNAL").as_str());
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

    fn bar(close: &str, ts: Timestamp) -> Bar {
        bar_on("ESM4.CME", close, ts)
    }

    /// Hourly bars inside the session on 2026-01-07: the first closes at
    /// 10:00 Frankfurt time (09:00 UTC in winter), the eighth and last at
    /// 17:00.
    fn inside(n: usize) -> Timestamp {
        assert!(n < 8);
        let first: Timestamp = "2026-01-07T09:00:00Z".parse().unwrap();
        first + SignedDuration::from_hours(n as i64)
    }

    /// Hourly bars after the close the same evening, from 18:00 to 23:00.
    fn outside(n: usize) -> Timestamp {
        assert!(n < 6);
        inside(7) + SignedDuration::from_hours(n as i64 + 1)
    }

    /// The first bar inside the session the next day.
    fn next_morning() -> Timestamp {
        inside(0) + SignedDuration::from_hours(24)
    }

    fn feed(book: &mut Book, closes: &[&str], at: fn(usize) -> Timestamp) {
        for (i, close) in closes.iter().enumerate() {
            book.update(&bar(close, at(i)));
        }
    }

    const UPTREND: [&str; 6] = ["100", "101", "102", "103", "104", "105"];
    const DOWNTREND: [&str; 6] = ["105", "104", "103", "102", "101", "100"];
    const CRASH: [&str; 6] = ["90", "80", "70", "60", "50", "40"];

    #[test]
    fn no_target_until_warm() {
        let mut book = Book::new(&config());
        feed(&mut book, &UPTREND[..3], inside);
        assert_eq!(book.target(&bar("102", inside(3))), None);
    }

    #[test]
    fn long_in_an_uptrend() {
        let mut book = Book::new(&config());
        feed(&mut book, &UPTREND, inside);
        assert_eq!(book.target(&bar("105", inside(6))), Some(Decimal::from(3)));
    }

    #[test]
    fn short_in_a_downtrend() {
        let mut book = Book::new(&config());
        feed(&mut book, &DOWNTREND, inside);
        assert_eq!(book.target(&bar("100", inside(6))), Some(Decimal::from(-3)));
    }

    #[test]
    fn ignores_untraded_instruments() {
        let mut book = Book::new(&config());
        feed(&mut book, &UPTREND, inside);
        assert_eq!(book.target(&bar_on("ESH4.CME", "105", inside(6))), None);
    }

    #[test]
    fn bars_outside_the_session_do_not_warm_the_emas() {
        let mut book = Book::new(&config());
        feed(&mut book, &UPTREND[..3], inside);
        feed(&mut book, &UPTREND, outside);
        assert_eq!(book.target(&bar("105", next_morning())), None);
    }

    #[test]
    fn bars_outside_the_session_do_not_move_the_emas() {
        let mut book = Book::new(&config());
        feed(&mut book, &UPTREND, inside);
        feed(&mut book, &CRASH, outside);
        assert_eq!(
            book.target(&bar("105", next_morning())),
            Some(Decimal::from(3))
        );
    }

    #[test]
    fn no_target_on_a_bar_outside_the_session() {
        let mut book = Book::new(&config());
        feed(&mut book, &UPTREND, inside);
        assert_eq!(book.target(&bar("105", outside(0))), None);
    }
}
