//! The fixture of the backtest tests: a temporary catalog holding one ETF,
//! SXR8.IBIS in EUR, and its 1-minute quotes only, and the EMA crossover
//! trading it on hourly MID bars Nautilus builds from those quotes.
//!
//! Kinds of quotes: a wave, so that a fast and a slow EMA cross several
//! times over days; steps, a level per hour bar chosen by the test, so
//! that a run with the shortest crossover trades at hours worked out by
//! hand; noise, for the recording strategy of `recorder.rs` to check every
//! price of the bars; a fall, for its stop.

#![allow(dead_code, reason = "each test file uses part of the fixture")]

pub mod recorder;

use std::path::Path;

use helix_trader::{
    backtest::Backtest,
    strategy::ema::{EmaCross, EmaCrossConfig, Session},
};
use jiff::{RoundMode, SignedDuration, Timestamp, TimestampRound, Unit, civil::time, tz::TimeZone};
use nautilus_common::logging::logger::LoggerConfig;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarSpecification, BarType, QuoteTick},
    enums::{BarAggregation, PriceType},
    identifiers::{InstrumentId, Symbol},
    instruments::{InstrumentAny, stubs::equity_aapl},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use tempfile::TempDir;

pub const INSTRUMENT_ID: &str = "SXR8.IBIS";
pub const MID_BAR_TYPE: &str = "SXR8.IBIS-1-HOUR-MID-INTERNAL";

const MINUTE: SignedDuration = SignedDuration::from_mins(1);
const NANOSECOND: SignedDuration = SignedDuration::from_nanos(1);

pub fn ts(s: &str) -> Timestamp {
    s.parse().unwrap()
}

pub fn instrument() -> InstrumentAny {
    let mut equity = equity_aapl();
    equity.id = InstrumentId::from(INSTRUMENT_ID);
    equity.raw_symbol = Symbol::from("SXR8");
    equity.isin = Some("IE00B5BMR087".into());
    equity.currency = Currency::EUR();
    InstrumentAny::Equity(equity)
}

/// Half the spread of every quote.
pub fn half_spread() -> Price {
    Price::from("0.05")
}

/// The quote of the minute closing at `close`: `mid` less (bid) and plus
/// (ask) the half spread, sizes 1,000,000, stamped 1 ns before the close.
pub fn quote(close: Timestamp, mid: f64) -> QuoteTick {
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

/// One quote per minute closing in `(from, to]`, at the mid `mid` gives
/// for the minute's close; none for a minute it gives `None` for.
pub fn minute_quotes(
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

/// The close of the hour bar holding the minute closing at `close`.
fn bar_close(close: Timestamp) -> Timestamp {
    let hour = TimestampRound::new()
        .smallest(Unit::Hour)
        .mode(RoundMode::Ceil);
    close.round(hour).unwrap()
}

/// A bar's close time and its open, high, low and close.
pub type Ohlc = (Timestamp, Price, Price, Price, Price);

pub fn ohlc(bar: &Bar) -> Ohlc {
    (bar.ts_event.into(), bar.open, bar.high, bar.low, bar.close)
}

/// The hour MID bars of `quotes` stamped at or after `from`, worked out
/// here from the quotes alone: each bar holds the quotes of its hour,
/// `(close − 1 h, close]` less the 1 ns stamp; the oldest bar, which lacks
/// the quotes before `from`, is left out.
pub fn hour_bars(quotes: &[QuoteTick], from: Timestamp) -> Vec<Ohlc> {
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
    bars.remove(0);
    bars
}

/// Stepped quotes over `(from, to]`: the minute `m` (1 to 60) of the hour
/// bar closing at `c` quotes the mid `level(c) + 0.01 m`, or nothing if
/// `level(c)` is `None`. So that bar closes at its level + 0.60, and each
/// quote differs from its neighbours.
pub fn stepped_quotes(
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

/// The quote among `quotes` stamped 1 ns before `close`.
pub fn quote_before(quotes: &[QuoteTick], close: Timestamp) -> QuoteTick {
    let ts: UnixNanos = (close - NANOSECOND).into();
    *quotes
        .iter()
        .find(|quote| quote.ts_event == ts)
        .unwrap_or_else(|| panic!("no quote 1 ns before {close}"))
}

/// A new temporary catalog holding the instrument and no quotes.
pub fn catalog_without_quotes() -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    catalog.write_instruments(vec![instrument()]).unwrap();
    dir
}

/// A new temporary catalog holding the instrument and `quotes`, stored
/// over `[from, to]` as a fill over that window stores them.
pub fn catalog(quotes: &[QuoteTick], from: Timestamp, to: Timestamp) -> TempDir {
    let dir = catalog_without_quotes();
    let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    catalog
        .write_to_parquet(quotes, Some(from.into()), Some(to.into()), None)
        .unwrap();
    dir
}

/// A backtest of the instrument over `[start, end]` after `warmup`, with
/// `commission` per order and 10,000 EUR to start.
pub fn backtest(
    catalog: &Path,
    start: Timestamp,
    end: Timestamp,
    warmup: SignedDuration,
    commission: &str,
) -> Backtest {
    Backtest {
        catalog: catalog.to_path_buf(),
        instrument_ids: vec![InstrumentId::from(INSTRUMENT_ID)],
        start,
        end,
        warmup,
        commission: Money::from(commission),
        balance: Money::from("10000 EUR"),
        logging: LoggerConfig::default(),
    }
}

/// Xetra's continuous trading: the hour bars closing 10:00 to 17:00
/// Frankfurt time (09:00 to 16:00 UTC in January) count, eight a day.
pub fn session() -> Session {
    Session {
        time_zone: TimeZone::get("Europe/Berlin").unwrap(),
        open: time(9, 0, 0, 0),
        close: time(17, 30, 0, 0),
    }
}

/// The EMA crossover on 1-hour MID bars of the instrument: `fast` against
/// `slow` bars, 10 shares long or short, inside the session, after
/// `warmup`.
pub fn ema_cross(fast: usize, slow: usize, warmup: SignedDuration) -> EmaCross {
    let config = EmaCrossConfig::builder()
        .instrument_ids(vec![InstrumentId::from(INSTRUMENT_ID)])
        .bar_spec(BarSpecification::new(
            1,
            BarAggregation::Hour,
            PriceType::Mid,
        ))
        .fast_period(fast)
        .slow_period(slow)
        .trade_size(Quantity::from(10))
        .warmup(warmup)
        .session(session())
        .build();
    EmaCross::new(config).unwrap()
}

/// The shortest crossover, 1 against 2 bars: once two bars inside the
/// session are in, it is long while a bar closes above the slow EMA of
/// the bars before it, short otherwise.
pub fn shortest_ema_cross(warmup: SignedDuration) -> EmaCross {
    ema_cross(1, 2, warmup)
}

/// The final EUR balance of the account.
pub fn balance(result: &nautilus_backtest::result::BacktestResult) -> String {
    result.summary["account.IBIS.balance.EUR.total"].clone()
}

/// The wave: a mid of 600 EUR plus 5 EUR of sine over 30 hours, quoted
/// every minute for 192 hours from Monday 5 January 2026; the run starts
/// half an hour after the 71st hour and ends with the quotes, after 48
/// hours of warmup.
pub mod wave {
    use super::*;

    pub const WARMUP: SignedDuration = SignedDuration::from_hours(48);

    /// The orders the EMA crossover sends on the wave.
    pub const EMA_ORDERS: usize = 4;
    /// Its PnL (total) without commission: the spread paid included.
    ///
    /// The orders and the PnL are those of the earlier fixture, which
    /// stored the wave as external hour bars, one quote per bar: the
    /// internal bars close on the same mids, at the same hours, and the
    /// orders fill on the same quotes.
    pub const EMA_PNL_WITHOUT_COMMISSION: f64 = -234.0;

    fn first() -> Timestamp {
        ts("2026-01-05T00:00:00Z")
    }

    pub fn start() -> Timestamp {
        first() + SignedDuration::from_mins(71 * 60 + 30)
    }

    pub fn end() -> Timestamp {
        first() + SignedDuration::from_hours(192)
    }

    pub fn quotes() -> Vec<QuoteTick> {
        minute_quotes(first(), end(), |close| {
            let hours = close.duration_since(first()).as_secs_f64() / 3600.0;
            let mid = 600.0 + 5.0 * (std::f64::consts::TAU * hours / 30.0).sin();
            Some((mid * 100.0).round() / 100.0)
        })
    }

    /// The instrument and the wave, stored from its start to its end.
    pub fn catalog() -> TempDir {
        super::catalog(&quotes(), first(), end())
    }

    pub fn backtest(catalog: &Path, commission: &str) -> Backtest {
        super::backtest(catalog, start(), end(), WARMUP, commission)
    }

    /// The EMA crossover on the wave: 5 against 15 bars.
    pub fn ema_cross() -> EmaCross {
        super::ema_cross(5, 15, WARMUP)
    }
}

/// Steps on Wednesday 7 January 2026, all hours inside the session: the
/// hour bars close at 700.60 (11:00 UTC), 600.60 (12:00), 620.60 (13:00)
/// and 580.60 (14:00 and 15:00), quoted from 10:00 to 15:00. With two
/// hours of warmup and a run from 12:30 or 13:00, the oldest warmup bar,
/// closing at 11:00, is partial and the shortest crossover sees:
///
/// - 12:00 and 13:00: 620.60 above 600.60, long: it buys 10 at the ask of
///   the quote closing 13:00, 620.65;
/// - 14:00: 580.60 below the slow EMA of 610.60, short: it sells 20 at the
///   bid of the quote closing 14:00, 580.55.
///
/// The 10 shares bought and sold lose 40.10 each: the balance ends at
/// 9599.00 EUR, after 2 orders. Had the partial bar counted, the slow EMA
/// would be 650.60 at 13:00: one sell, no buy; without the warmup, the
/// crossover would still be cold at 13:00: one sell, at 14:00.
pub mod steps {
    use super::*;

    pub const WARMUP: SignedDuration = SignedDuration::from_hours(2);
    pub const ORDERS: usize = 2;
    pub const BALANCE: &str = "9599.00 EUR";

    /// `hh:mm` UTC on the day of the steps.
    pub fn at(hh_mm: &str) -> Timestamp {
        ts(&format!("2026-01-07T{hh_mm}:00Z"))
    }

    pub fn end() -> Timestamp {
        at("15:00")
    }

    pub fn quotes() -> Vec<QuoteTick> {
        stepped_quotes(at("10:00"), end(), |close| {
            Some(if close <= at("11:00") {
                700.0
            } else if close == at("12:00") {
                600.0
            } else if close == at("13:00") {
                620.0
            } else {
                580.0
            })
        })
    }

    /// The instrument and the steps, stored from 10:00 to the end.
    pub fn catalog() -> TempDir {
        super::catalog(&quotes(), at("10:00"), end())
    }

    /// A backtest of the steps without commission, from `start`.
    pub fn backtest(catalog: &Path, start: Timestamp) -> Backtest {
        super::backtest(catalog, start, end(), WARMUP, "0 EUR")
    }
}

/// Noise on Wednesday 7 January 2026: a quote a minute from 06:00 to 15:00
/// UTC, its mid 600 plus 0.01 times `37 k mod 101` for the k-th minute, so
/// that every hour bar has its own open, high, low and close.
pub mod noise {
    use super::*;

    pub const WARMUP: SignedDuration = SignedDuration::from_hours(4);

    /// `hh:mm` UTC on the day of the noise.
    pub fn at(hh_mm: &str) -> Timestamp {
        ts(&format!("2026-01-07T{hh_mm}:00Z"))
    }

    pub fn quotes() -> Vec<QuoteTick> {
        minute_quotes(at("06:00"), at("15:00"), |close| {
            let k = close.duration_since(at("06:00")).as_mins();
            Some(600.0 + 0.01 * ((37 * k) % 101) as f64)
        })
    }

    pub fn catalog() -> TempDir {
        super::catalog(&quotes(), at("06:00"), at("15:00"))
    }

    pub fn backtest(catalog: &Path, start: Timestamp) -> Backtest {
        super::backtest(catalog, start, at("15:00"), WARMUP, "0 EUR")
    }

    /// The bars a run from `start` should get, warmup and replay: those
    /// from the warmup start, 1 ns before `start` less the warmup.
    pub fn expected_bars(start: Timestamp) -> Vec<Ohlc> {
        hour_bars(&quotes(), start - NANOSECOND - WARMUP)
    }

    /// A recorder of the MID bars after the warmup, without orders.
    pub fn recorder() -> (
        recorder::Recorder,
        std::rc::Rc<std::cell::RefCell<recorder::Record>>,
    ) {
        recorder::Recorder::new(BarType::from(MID_BAR_TYPE), WARMUP, None)
    }
}

/// A fall on Wednesday 7 January 2026: a mid of 620 a minute from 10:00
/// UTC to 13:00, then 0.50 lower each minute: 619.50 at 13:01, 600.00 at
/// 13:40, 590.00 from 14:00 to 15:00.
pub mod fall {
    use super::*;

    pub const WARMUP: SignedDuration = SignedDuration::from_hours(2);

    /// `hh:mm` UTC on the day of the fall.
    pub fn at(hh_mm: &str) -> Timestamp {
        ts(&format!("2026-01-07T{hh_mm}:00Z"))
    }

    pub fn quotes() -> Vec<QuoteTick> {
        minute_quotes(at("10:00"), at("15:00"), |close| {
            let since = close.duration_since(at("13:00")).as_mins();
            Some(620.0 - 0.5 * since.clamp(0, 60) as f64)
        })
    }

    pub fn catalog() -> TempDir {
        super::catalog(&quotes(), at("10:00"), at("15:00"))
    }

    pub fn backtest(catalog: &Path) -> Backtest {
        super::backtest(catalog, at("12:30"), at("15:00"), WARMUP, "0 EUR")
    }
}
