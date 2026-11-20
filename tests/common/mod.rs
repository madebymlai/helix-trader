//! Fixture for the backtest tests: one ETF, SXR8.IBIS in EUR, with 1-minute quotes in a temporary
//! catalog. The EMA crossover trades it on hourly MID bars.
//!
//! Quotes come as a wave, as trades (a level per hour bar, chosen by the test) or as noise (every
//! bar price, for `recorder.rs`).

#![allow(dead_code, reason = "each test module uses part of the fixture")]

pub(crate) mod cache;
pub(crate) mod dataset;
pub(crate) mod features;
pub(crate) mod glbx;
pub(crate) mod ic;
pub(crate) mod idealpro;
pub(crate) mod recorder;
pub(crate) mod rolls;

use std::{
    cell::RefCell,
    collections::BTreeMap,
    path::{Path, PathBuf},
    rc::Rc,
};

use helix_trader::{
    backtest::{Backtest, Commission},
    strategy::{
        DataConfig, IndicatorsConfig, Part, Series, Session, TradeConfig, TradedInstrument,
        conversions::Conversions,
        ema::{EmaCross, EmaCrossConfig, EmaParams},
    },
};
use jiff::{RoundMode, SignedDuration, Timestamp, TimestampRound, Unit, civil::time, tz::TimeZone};
use nautilus_backtest::result::BacktestResult;
use nautilus_common::{
    clock::{Clock, VirtualClock},
    logging::logger::LoggerConfig,
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarSpecification, BarType, QuoteTick},
    enums::{AggregationSource, BarAggregation, PriceType},
    identifiers::{InstrumentId, Symbol},
    instruments::{InstrumentAny, stubs::equity_aapl},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use tempfile::TempDir;

pub(crate) const INSTRUMENT_ID: &str = "SXR8.IBIS";
pub(crate) const MID_BAR_TYPE: &str = "SXR8.IBIS-1-HOUR-MID-INTERNAL";

const MINUTE: SignedDuration = SignedDuration::from_mins(1);
const NANOSECOND: SignedDuration = SignedDuration::from_nanos(1);

pub(crate) fn ts(s: &str) -> Timestamp {
    s.parse().unwrap()
}

/// Returns the clocks for a fill's node at `now` (`NodeFill::run`).
pub(crate) fn clock_at(now: Timestamp) -> impl Fn() -> Rc<RefCell<dyn Clock>> + 'static {
    move || {
        let clock = VirtualClock::new();
        clock.set_time(UnixNanos::from(now));
        Rc::new(RefCell::new(clock))
    }
}

pub(crate) fn instrument() -> InstrumentAny {
    let mut equity = equity_aapl();
    equity.id = InstrumentId::from(INSTRUMENT_ID);
    equity.raw_symbol = Symbol::from("SXR8");
    equity.isin = Some("IE00B5BMR087".into());
    equity.currency = Currency::EUR();
    InstrumentAny::Equity(equity)
}

pub(crate) fn half_spread() -> Price {
    Price::from("0.05")
}

/// Returns a quote stamped 1 ns before `close`.
///
/// Bid and ask are `mid` less and plus the half spread. Both sizes are 1,000,000.
pub(crate) fn quote(close: Timestamp, mid: f64) -> QuoteTick {
    let size = Quantity::from(1_000_000);
    let mid = Price::new(mid, 2);
    let ts: UnixNanos = (close - NANOSECOND).into();
    QuoteTick::new(
        InstrumentId::from(INSTRUMENT_ID),
        mid - half_spread(),
        mid + half_spread(),
        size,
        size,
        ts,
        ts,
    )
}

/// Returns one quote per minute closing in `(from, to]`.
pub(crate) fn minute_quotes(
    from: Timestamp,
    to: Timestamp,
    mid: impl Fn(Timestamp) -> Option<f64>,
) -> Vec<QuoteTick> {
    let mut quotes = vec![];
    let mut close = from + MINUTE;
    while close <= to {
        if let Some(mid) = mid(close) {
            quotes.push(quote(close, mid));
        }
        close += MINUTE;
    }
    quotes
}

fn bar_close(close: Timestamp) -> Timestamp {
    let hour = TimestampRound::new()
        .smallest(Unit::Hour)
        .mode(RoundMode::Ceil);
    close.round(hour).unwrap()
}

pub(crate) type Ohlc = (Timestamp, Price, Price, Price, Price);

pub(crate) fn ohlc(bar: &Bar) -> Ohlc {
    (bar.ts_event.into(), bar.open, bar.high, bar.low, bar.close)
}

/// Returns hour MID bars of the quotes stamped at or after `from`.
///
/// Each bar covers `(close − 1 h, close]`, less the 1 ns stamp.
pub(crate) fn hour_bars(quotes: &[QuoteTick], from: Timestamp) -> Vec<Ohlc> {
    let mut bars: Vec<Ohlc> = vec![];
    for quote in quotes {
        let stamp: Timestamp = quote.ts_event.into();
        if stamp < from {
            continue;
        }
        let mid = Price::new(
            (quote.bid_price.as_f64() + quote.ask_price.as_f64()) / 2.0,
            2,
        );
        let close = bar_close(stamp + NANOSECOND);
        match bars.last_mut() {
            Some(bar) if bar.0 == close => {
                bar.2 = bar.2.max(mid);
                bar.3 = bar.3.min(mid);
                bar.4 = mid;
            }
            _ => bars.push((close, mid, mid, mid, mid)),
        }
    }
    bars
}

/// Returns stepped quotes.
///
/// Minute `m` (1 to 60) of the hour bar closing at `c` quotes `level(c) + 0.01 m`. It quotes
/// nothing if `level(c)` is `None`. A bar closes at its level + 0.60.
pub(crate) fn stepped_quotes(
    from: Timestamp,
    to: Timestamp,
    level: impl Fn(Timestamp) -> Option<f64>,
) -> Vec<QuoteTick> {
    minute_quotes(from, to, |close| {
        let bar = bar_close(close);
        let minute = 60 - bar.duration_since(close).as_mins();
        level(bar).map(|level| level + 0.01 * minute as f64)
    })
}

pub(crate) fn catalog_without_quotes() -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    catalog.write_instruments(vec![instrument()]).unwrap();
    dir
}

pub(crate) fn catalog(quotes: &[QuoteTick], from: Timestamp, to: Timestamp) -> TempDir {
    catalog_of(instrument(), quotes, from, to)
}

pub(crate) fn catalog_of(
    instrument: InstrumentAny,
    quotes: &[QuoteTick],
    from: Timestamp,
    to: Timestamp,
) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    catalog.write_instruments(vec![instrument]).unwrap();
    catalog
        .write_to_parquet(quotes, Some(from.into()), Some(to.into()), None)
        .unwrap();
    dir
}

/// Returns the path that goes up to the root, then down to `dir`.
pub(crate) fn relative(dir: &Path) -> PathBuf {
    let cwd = std::env::current_dir().unwrap();
    let mut relative: PathBuf = std::iter::repeat_n("..", cwd.components().count() - 1).collect();
    relative.push(dir.strip_prefix("/").unwrap());
    relative
}

/// Returns a backtest on hour MID bars over `[start, end]`, starting with 10,000 EUR.
pub(crate) fn backtest(
    catalog: &Path,
    start: Timestamp,
    end: Timestamp,
    commission: &str,
) -> Backtest {
    Backtest {
        catalog: catalog.to_path_buf(),
        bar_types: vec![BarType::from(MID_BAR_TYPE)],
        start,
        end,
        ticks: vec![],
        custom: vec![],
        commission: Commission::PerOrder(Money::from(commission)),
        latency: None,
        pairs: vec![],
        balance: Money::from("10000 EUR"),
        logging: LoggerConfig::default(),
    }
}

/// Returns Xetra's continuous trading.
///
/// It covers the hour bars closing 10:00 to 17:00 Frankfurt time (09:00 to 16:00 UTC in January),
/// eight a day.
pub(crate) fn session() -> Session {
    Session {
        time_zone: TimeZone::get("Europe/Berlin").unwrap(),
        open: time(9, 0, 0, 0),
        close: time(17, 30, 0, 0),
    }
}

/// The risk is 0.32% of equity.
///
/// Stops of 50 bps from closes about 600 EUR are 2.97 to 3.10 EUR a share. So 9,700 to 10,100 EUR
/// buys 10 shares, rounded down (32 / 3.10 = 10.3). The EMA backtests are worked out at this size.
pub(crate) const RISK: &str = "0.32%";

pub(crate) const LIVE_WARMUP: SignedDuration = SignedDuration::from_hours(1);

/// Returns an EMA crossover config on 1-hour MID bars, with `fast` against `slow`.
///
/// It risks `RISK` over the stop. Its exits are:
///
/// - Stop loss 50 bps and take profit 100 bps from the deciding bar's close, rounded to the cent.
/// - A 4-hour hold.
pub(crate) fn ema_config(fast: usize, slow: usize, session: Option<Session>) -> EmaCrossConfig {
    let instrument_id = InstrumentId::from(INSTRUMENT_ID);
    EmaCrossConfig {
        base: EmaParams::default_base(),
        data: DataConfig {
            series: vec![Series::Bars(BarType::new(
                instrument_id,
                BarSpecification::new(1, BarAggregation::Hour, PriceType::Mid),
                AggregationSource::Internal,
            ))],
            warmup: LIVE_WARMUP,
            session,
        },
        indicators: IndicatorsConfig(BTreeMap::from([("fast", fast), ("slow", slow)].map(
            |(name, period)| {
                let period = i64::try_from(period).unwrap();
                let ema = toml::Table::from_iter([("period".to_string(), period.into())]);
                (name.to_string(), ema)
            },
        ))),
        trade: TradeConfig {
            instruments: vec![TradedInstrument { instrument_id }],
            risk: Some(RISK.parse().unwrap()),
            conversions: Conversions::default(),
        },
        model: None,
        own: EmaParams {},
    }
}

pub(crate) fn ema_cross(fast: usize, slow: usize, session: Option<Session>) -> EmaCross {
    EmaCross::new(ema_config(fast, slow, session)).unwrap()
}

/// Returns the EMA cross of 1 against 2 bars.
///
/// The fast EMA is the close. The slow EMA starts as the average of its first two closes, then
/// moves two thirds to each new close. From two bars on, a close above the slow EMA of the bars
/// before it is long, and a close at or below it is short. A side change is a cross.
pub(crate) fn shortest_ema_cross() -> EmaCross {
    ema_cross(1, 2, Some(session()))
}

pub(crate) fn balance(result: &BacktestResult) -> String {
    result.summary["account.IBIS.balance.EUR.total"].clone()
}

/// A wave of quotes every minute for 192 hours from Monday 5 January 2026.
///
/// The mid is 600 EUR plus 5 EUR of sine over 30 hours. The run starts half an hour after the 71st
/// hour.
pub(crate) mod wave {
    use super::*;

    fn first() -> Timestamp {
        ts("2026-01-05T00:00:00Z")
    }

    pub(crate) fn start() -> Timestamp {
        first() + SignedDuration::from_mins(71 * 60 + 30)
    }

    pub(crate) fn end() -> Timestamp {
        first() + SignedDuration::from_hours(192)
    }

    pub(crate) fn quotes() -> Vec<QuoteTick> {
        minute_quotes(first(), end(), |close| {
            let hours = close.duration_since(first()).as_secs_f64() / 3600.0;
            let mid = 600.0 + 5.0 * (std::f64::consts::TAU * hours / 30.0).sin();
            Some((mid * 100.0).round() / 100.0)
        })
    }

    pub(crate) fn catalog() -> TempDir {
        super::catalog(&quotes(), first(), end())
    }

    pub(crate) fn backtest(catalog: &Path, commission: &str) -> Backtest {
        super::backtest(catalog, start(), end(), commission)
    }

    pub(crate) fn ema_cross() -> EmaCross {
        super::ema_cross(5, 15, Some(session()))
    }
}

/// Quotes on 7 January 2026 from 09:00 UTC, in steps (a bar closes at its level + 0.60).
///
/// A run from 10:00 replays no quote before it, so the bar closing 10:00 at 800.60 is not
/// replayed. The shortest crossover warms on 11:00 at 610.60 and 12:00 at 600.60. It is
/// initialized at 12:00 with slow EMA 605.60, short (600.60 is below it).
///
/// From 13:00 the bar closing at 13:00 + i hours is each test's `levels[i]`. From 09:00 the 10:00
/// bar would count, and the slow EMA would be 705.60 at 11:00 and 635.60 at 12:00.
pub(crate) mod trades {
    use super::*;

    pub(crate) fn at(hh_mm: &str) -> Timestamp {
        ts(&format!("2026-01-07T{hh_mm}:00Z"))
    }

    /// Returns the close of the 10:00 bar. The first bar replayed, closing 11:00, then holds its
    /// whole hour.
    pub(crate) fn start() -> Timestamp {
        at("10:00")
    }

    pub(crate) fn end(levels: &[f64]) -> Timestamp {
        at("12:00") + SignedDuration::from_hours(levels.len() as i64)
    }

    pub(crate) fn quotes(levels: &[f64]) -> Vec<QuoteTick> {
        stepped_quotes(at("09:00"), end(levels), |close| {
            Some(if close <= at("10:00") {
                800.0
            } else if close == at("11:00") {
                610.0
            } else if close == at("12:00") {
                600.0
            } else {
                let hours = close.duration_since(at("13:00")).as_hours();
                levels[usize::try_from(hours).unwrap()]
            })
        })
    }

    pub(crate) fn catalog(levels: &[f64]) -> TempDir {
        super::catalog(&quotes(levels), at("09:00"), end(levels))
    }

    pub(crate) fn backtest(
        catalog: &Path,
        start: Timestamp,
        levels: &[f64],
        commission: &str,
    ) -> Backtest {
        super::backtest(catalog, start, end(levels), commission)
    }

    /// A trade ended by time on the shortest crossover, with the code's exits (`ema_config`).
    ///
    /// - At 13:00 the close is 620.60, above 605.60. It buys 10 at the ask 620.65 and moves the
    ///   slow EMA to 615.60. The stop loss is 617.50 and the take profit 626.81 (620.60 less 0.5%
    ///   and plus 1%, to the cent).
    /// - At 14:00 (624.60), 15:00 and 16:00 (624.80) the close is above the slow EMA (615.60,
    ///   621.60, 623.73). It stays long with no cross, and the slow EMA moves to 624.44. 17:00 and
    ///   18:00 are out of the session. Bids from 623.96 to 624.85 touch neither exit.
    /// - At 17:00, 4 hours after the fill, it sells 10 at the bid, 624.30 + 0.60 − 0.05 = 624.85,
    ///   and cancels both legs. An hour sooner it sells at 624.75, an hour later at 624.95.
    ///
    /// 10 shares gain 4.20 each, giving 10042.00 EUR after 4 orders (entry, two legs, close). From
    /// 09:00, 13:00 to 16:00 are all short (below 635.60, 625.60, 624.93, 624.84), so there is no
    /// trade. From 12:30 the crossover is cold until 14:00, then long, so there is no trade
    /// (`backtest_warmup.rs`).
    pub(crate) const TIME_EXIT: [f64; 6] = [620.0, 624.0, 624.2, 624.2, 624.3, 624.4];
    pub(crate) const TIME_EXIT_ORDERS: usize = 4;
    pub(crate) const TIME_EXIT_BALANCE: &str = "10042.00 EUR";

    pub(crate) fn ema_cross() -> EmaCross {
        shortest_ema_cross()
    }
}

/// A quote a minute on 7 January 2026 from 06:00 to 15:00 UTC. The mid for the k-th minute is 600
/// plus 0.01 times `37 k mod 101`. So every hour bar has its own open, high, low and close.
pub(crate) mod noise {
    use super::*;

    pub(crate) fn at(hh_mm: &str) -> Timestamp {
        ts(&format!("2026-01-07T{hh_mm}:00Z"))
    }

    pub(crate) fn quotes() -> Vec<QuoteTick> {
        minute_quotes(at("06:00"), at("15:00"), |close| {
            let k = close.duration_since(at("06:00")).as_mins();
            Some(600.0 + 0.01 * ((37 * k) % 101) as f64)
        })
    }

    pub(crate) fn catalog() -> TempDir {
        super::catalog(&quotes(), at("06:00"), at("15:00"))
    }

    pub(crate) fn backtest(catalog: &Path, start: Timestamp) -> Backtest {
        super::backtest(catalog, start, at("15:00"), "0 EUR")
    }

    pub(crate) fn expected_bars(start: Timestamp) -> Vec<Ohlc> {
        hour_bars(&quotes(), start)
    }

    pub(crate) fn recorder() -> (
        recorder::Recorder,
        std::rc::Rc<std::cell::RefCell<recorder::Record>>,
    ) {
        recorder::Recorder::new(BarType::from(MID_BAR_TYPE))
    }
}
