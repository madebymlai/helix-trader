use jiff::SignedDuration;
use nautilus_indicators::{
    average::ema::ExponentialMovingAverage,
    indicator::{Indicator, MovingAverage},
};
use nautilus_model::{data::Bar, enums::OrderSide, identifiers::InstrumentId};

use super::config::EmaCrossConfig;
use crate::strategy::Session;

/// The crossings of the fast EMA over the slow one on the traded
/// instrument's bars: long while the fast EMA is above the slow one, short
/// otherwise, and a cross where a bar turns that side. Only bars of the
/// traded instrument wholly inside the session count, if there is one:
/// the others neither move the EMAs nor cross.
#[derive(Debug)]
pub(super) struct Signal {
    instrument_id: InstrumentId,
    crossover: Crossover,
    session: Option<Session>,
    bar_length: SignedDuration,
}

impl Signal {
    pub(super) fn new(config: &EmaCrossConfig) -> Self {
        Self {
            instrument_id: config.trade_instrument_id,
            crossover: Crossover::new(config.fast_period, config.slow_period),
            session: config.bars.session.clone(),
            bar_length: config.bars.bar_spec.timedelta(),
        }
    }

    /// Feeds a bar; one that does not count is ignored.
    pub(super) fn update(&mut self, bar: &Bar) {
        if self.counts(bar) {
            self.crossover.update(bar);
        }
    }

    /// The side `bar`, the last one fed, turned to: a buy if it took the
    /// fast EMA above the slow one, a sell if below; `None` if it kept the
    /// side, does not count, or the EMAs were not both warm before it.
    pub(super) fn cross(&self, bar: &Bar) -> Option<OrderSide> {
        if !self.counts(bar) {
            return None;
        }
        self.crossover.cross()
    }

    /// Whether the bar is of the traded instrument and, stamped on its
    /// close, lies wholly inside the session; every such bar does without
    /// one.
    fn counts(&self, bar: &Bar) -> bool {
        bar.bar_type.instrument_id() == self.instrument_id
            && self
                .session
                .as_ref()
                .is_none_or(|session| session.contains(bar.ts_event.into(), self.bar_length))
    }
}

#[derive(Debug)]
struct Crossover {
    fast: ExponentialMovingAverage,
    slow: ExponentialMovingAverage,
    /// Whether the fast EMA was above the slow one after the bar before the
    /// last, and after the last; `None` while they were not both warm.
    long: [Option<bool>; 2],
}

impl Crossover {
    fn new(fast_period: usize, slow_period: usize) -> Self {
        Self {
            fast: ExponentialMovingAverage::new(fast_period, None),
            slow: ExponentialMovingAverage::new(slow_period, None),
            long: [None, None],
        }
    }

    fn update(&mut self, bar: &Bar) {
        self.fast.handle_bar(bar);
        self.slow.handle_bar(bar);
        let long = (self.fast.initialized() && self.slow.initialized())
            .then(|| self.fast.value() > self.slow.value());
        self.long = [self.long[1], long];
    }

    fn cross(&self) -> Option<OrderSide> {
        match self.long {
            [Some(false), Some(true)] => Some(OrderSide::Buy),
            [Some(true), Some(false)] => Some(OrderSide::Sell),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use jiff::{SignedDuration, Timestamp};
    use nautilus_model::data::BarType;

    use super::*;

    /// The shortest crossover, 1 against 2 bars, on ESM4, reading ESH4 too.
    /// The fast EMA is the close; the slow one starts as the average of
    /// its first two closes and then moves two thirds of the way to each
    /// new close. A close above the slow EMA of the bars before it is long.
    fn config() -> EmaCrossConfig {
        toml::from_str(
            r#"
            instrument_ids = ["ESH4.CME", "ESM4.CME"]
            trade_instrument_id = "ESM4.CME"
            bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" }
            fast_period = 1
            slow_period = 2
            trade_size = "3"
            stop_loss_bps = 100
            take_profit_bps = 100
            hold = "1h"
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

    /// Feeds `closes` at `at(0)`, `at(1)`, ... and returns the cross of the
    /// last.
    fn feed(signal: &mut Signal, closes: &[&str], at: fn(usize) -> Timestamp) -> Option<OrderSide> {
        let mut last = None;
        for (i, close) in closes.iter().enumerate() {
            let bar = bar(close, at(i));
            signal.update(&bar);
            last = signal.cross(&bar);
        }
        last
    }

    #[test]
    fn no_cross_while_cold() {
        let mut signal = Signal::new(&config());
        assert_eq!(feed(&mut signal, &["100"], inside), None);
    }

    #[test]
    fn the_first_warm_bar_sets_the_side_without_a_cross() {
        // 101 above the slow EMA of 100.5: long, but nothing before it.
        let mut signal = Signal::new(&config());
        assert_eq!(feed(&mut signal, &["100", "101"], inside), None);
    }

    #[test]
    fn a_close_turning_above_the_slow_ema_crosses_up() {
        // 100 below 100.5, short; 102 above 100.5, long.
        let mut signal = Signal::new(&config());
        assert_eq!(
            feed(&mut signal, &["101", "100", "102"], inside),
            Some(OrderSide::Buy)
        );
    }

    #[test]
    fn a_close_turning_below_the_slow_ema_crosses_down() {
        // 101 above 100.5, long; 99 below 100.5, short.
        let mut signal = Signal::new(&config());
        assert_eq!(
            feed(&mut signal, &["100", "101", "99"], inside),
            Some(OrderSide::Sell)
        );
    }

    #[test]
    fn keeping_the_side_is_no_cross() {
        // 101 above 100.5, long; 102 above 100.5, still long.
        let mut signal = Signal::new(&config());
        assert_eq!(feed(&mut signal, &["100", "101", "102"], inside), None);
    }

    #[test]
    fn each_turn_is_a_cross() {
        // Short at 100 (below 100.5), long at 102 (above 100.5, the slow EMA
        // going to 101.5), short at 99 (below 101.5).
        let mut signal = Signal::new(&config());
        let crosses: Vec<_> = ["101", "100", "102", "99"]
            .iter()
            .enumerate()
            .map(|(i, close)| {
                let bar = bar(close, inside(i));
                signal.update(&bar);
                signal.cross(&bar)
            })
            .collect();
        assert_eq!(
            crosses,
            [None, None, Some(OrderSide::Buy), Some(OrderSide::Sell)]
        );
    }

    #[test]
    fn bars_of_an_instrument_read_but_not_traded_neither_move_the_emas_nor_cross() {
        // ESH4 turning long and short in between leaves ESM4 short at 100
        // then long at 102.
        let mut signal = Signal::new(&config());
        feed(&mut signal, &["101", "100"], inside);
        for (n, close) in [(2, "90"), (3, "120")] {
            let other = bar_on("ESH4.CME", close, inside(n));
            signal.update(&other);
            assert_eq!(signal.cross(&other), None);
        }
        let bar = bar("102", inside(4));
        signal.update(&bar);
        assert_eq!(signal.cross(&bar), Some(OrderSide::Buy));
    }

    #[test]
    fn bars_outside_the_session_do_not_warm_the_emas() {
        // Had the 100 outside counted, the EMAs would be warm and short
        // (100 below 100.5), and 102 the next morning a cross up; without
        // it, 102 is the first warm bar.
        let mut signal = Signal::new(&config());
        feed(&mut signal, &["101"], inside);
        feed(&mut signal, &["100"], outside);
        assert_eq!(feed(&mut signal, &["102"], |_| next_morning()), None);
    }

    #[test]
    fn bars_outside_the_session_do_not_move_the_emas() {
        // Had the 102 outside counted, it would be long, the slow EMA going
        // to 101.5, and 99 the next morning a cross down; without it, 99
        // stays below 100.5, short as 100 was.
        let mut signal = Signal::new(&config());
        feed(&mut signal, &["101", "100"], inside);
        feed(&mut signal, &["102"], outside);
        assert_eq!(feed(&mut signal, &["99"], |_| next_morning()), None);
    }

    #[test]
    fn no_cross_on_a_bar_outside_the_session() {
        // 102 outside would cross up from 100 inside.
        let mut signal = Signal::new(&config());
        feed(&mut signal, &["101", "100"], inside);
        assert_eq!(feed(&mut signal, &["102"], outside), None);
    }
}
