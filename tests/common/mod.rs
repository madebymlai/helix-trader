//! The fixture of the backtest tests: a temporary catalog holding one ETF,
//! SXR8.IBIS in EUR, and its 1-minute quotes only, and the EMA crossover
//! trading it on hourly MID bars Nautilus builds from those quotes.
//!
//! Kinds of quotes: a wave, so that a fast and a slow EMA cross several
//! times over days; trades, a level per hour bar chosen by the test, so
//! that a run with the shortest crossover trades and exits at times and
//! prices worked out by hand; steps, for the recording strategy's
//! overlapping trades; noise, for the recording strategy of `recorder.rs`
//! to check every price of the bars; a fall, for its stop.

#![allow(dead_code, reason = "each test module uses part of the fixture")]

pub mod cache;
pub mod glbx;
pub mod ic;
pub mod recorder;

use std::path::Path;

use helix_trader::{
    backtest::{Backtest, commission_per_order, venue_oms_type},
    strategy::{
        BarsConfig, Session,
        ema::{Bps, EmaCross, EmaCrossConfig},
    },
};
use jiff::{RoundMode, SignedDuration, Timestamp, TimestampRound, Unit, civil::time, tz::TimeZone};
use nautilus_common::logging::logger::LoggerConfig;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarSpecification, BarType, QuoteTick},
    enums::{BarAggregation, PriceType},
    identifiers::{InstrumentId, Symbol, Venue},
    instruments::{InstrumentAny, stubs::equity_aapl},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rust_decimal::Decimal;
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

/// A backtest of the instrument over `[start, end]` after `warmup` of its
/// hour MID bars, with `commission` per order and 10,000 EUR to start.
pub fn backtest(
    catalog: &Path,
    start: Timestamp,
    end: Timestamp,
    warmup: SignedDuration,
    commission: &str,
) -> Backtest {
    Backtest {
        catalog: catalog.to_path_buf(),
        bar_types: vec![BarType::from(MID_BAR_TYPE)],
        start,
        end,
        warmup,
        fee_model: commission_per_order(Money::from(commission)).unwrap(),
        balance: Money::from("10000 EUR"),
        leverage: Decimal::ONE,
        venue_oms: venue_oms_type,
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

/// How an EMA trade ends: its stop loss and take profit, in basis points
/// from the close of the bar it was decided on, and its hold.
#[derive(Debug, Clone, Copy)]
pub struct Exits {
    pub stop_loss_bps: i64,
    pub take_profit_bps: i64,
    pub hold: SignedDuration,
}

impl Exits {
    /// Exits by time alone on the fixtures, whose prices move less than
    /// 10%: stop loss and take profit 1000 bps away.
    pub fn after(hold: SignedDuration) -> Self {
        Self {
            stop_loss_bps: 1000,
            take_profit_bps: 1000,
            hold,
        }
    }
}

/// The config of the EMA crossover reading and trading the instrument on
/// its 1-hour MID bars: `fast` against `slow` bars, trades of 10 shares
/// ending by `exits`, inside `session` if given, after `warmup`.
pub fn ema_config(
    fast: usize,
    slow: usize,
    warmup: SignedDuration,
    session: Option<Session>,
    exits: Exits,
) -> EmaCrossConfig {
    let bars = BarsConfig::builder()
        .instrument_ids(vec![InstrumentId::from(INSTRUMENT_ID)])
        .bar_spec(BarSpecification::new(
            1,
            BarAggregation::Hour,
            PriceType::Mid,
        ))
        .warmup(warmup)
        .maybe_session(session)
        .build();
    EmaCrossConfig::builder()
        .bars(bars)
        .trade_instrument_id(InstrumentId::from(INSTRUMENT_ID))
        .fast_period(fast)
        .slow_period(slow)
        .trade_size(Quantity::from(10))
        .stop_loss_bps(Bps::from(Decimal::from(exits.stop_loss_bps)))
        .take_profit_bps(Bps::from(Decimal::from(exits.take_profit_bps)))
        .hold(exits.hold)
        .build()
}

/// The EMA crossover of `ema_config`.
pub fn ema_cross(
    fast: usize,
    slow: usize,
    warmup: SignedDuration,
    session: Option<Session>,
    exits: Exits,
) -> EmaCross {
    EmaCross::new(ema_config(fast, slow, warmup, session, exits)).unwrap()
}

/// The shortest crossover, 1 against 2 bars, inside the session: the fast
/// EMA is the close, the slow one starts as the average of its first two
/// closes and then moves two thirds of the way to each new close. So once
/// two bars are in, a bar closing above the slow EMA of the bars before it
/// is long, at or below it short, and a bar changing the side is a cross.
pub fn shortest_ema_cross(warmup: SignedDuration, exits: Exits) -> EmaCross {
    ema_cross(1, 2, warmup, Some(session()), exits)
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

    /// The EMA crossover on the wave: 5 against 15 bars, stop loss and
    /// take profit 50 bps away, held 4 hours at most.
    pub fn ema_cross() -> EmaCross {
        let exits = Exits {
            stop_loss_bps: 50,
            take_profit_bps: 50,
            hold: SignedDuration::from_hours(4),
        };
        super::ema_cross(5, 15, WARMUP, Some(session()), exits)
    }
}

/// Steps on Wednesday 7 January 2026, all hours inside the session: the
/// hour bars close at 700.60 (11:00 UTC), 600.60 (12:00), 620.60 (13:00)
/// and 580.60 (14:00 and 15:00), quoted from 10:00 to 15:00, for the
/// recorder's overlapping trades (`overlap`).
pub mod steps {
    use super::*;

    pub const WARMUP: SignedDuration = SignedDuration::from_hours(2);

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

/// The EMA's trades on Wednesday 7 January 2026, all hours inside the
/// session, quoted from 09:00 UTC in steps (each hour bar closes at its
/// level + 0.60). With three hours of warmup and a run from 12:30, the
/// warmup holds the bars closing:
///
/// - 10:00 at 800.60, partial (from 09:30; one quote on a run from 13:00),
///   dropped;
/// - 11:00 at 610.60 and 12:00 at 600.60: the shortest crossover is warm
///   at 12:00, its slow EMA at 605.60, short (600.60 below it).
///
/// The hours from 13:00 are each test's `levels`, the bar closing at
/// 13:00 + i hours on `levels[i]`, and the run ends with the last. Had the
/// partial bar counted, the slow EMA would be 705.60 at 11:00 and 635.60
/// at 12:00.
pub mod trades {
    use super::*;

    pub const WARMUP: SignedDuration = SignedDuration::from_hours(3);

    /// `hh:mm` UTC on the day of the trades.
    pub fn at(hh_mm: &str) -> Timestamp {
        ts(&format!("2026-01-07T{hh_mm}:00Z"))
    }

    /// The run start: off an hour, so the 13:00 bar spans the warmup end.
    pub fn start() -> Timestamp {
        at("12:30")
    }

    /// The close of the last hour of `levels`.
    pub fn end(levels: &[f64]) -> Timestamp {
        at("12:00") + SignedDuration::from_hours(levels.len() as i64)
    }

    pub fn quotes(levels: &[f64]) -> Vec<QuoteTick> {
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

    /// The instrument and the quotes of `levels`, stored from 09:00 to
    /// their end.
    pub fn catalog(levels: &[f64]) -> TempDir {
        super::catalog(&quotes(levels), at("09:00"), end(levels))
    }

    /// A backtest of `levels` from `start` with `commission` per order.
    pub fn backtest(
        catalog: &Path,
        start: Timestamp,
        levels: &[f64],
        commission: &str,
    ) -> Backtest {
        super::backtest(catalog, start, end(levels), WARMUP, commission)
    }

    /// A trade ended by time:
    ///
    /// - 13:00 at 620.60, above 605.60: a cross up, buy 10 at the ask of
    ///   the quote closing 13:00, 620.65, the slow EMA going to 615.60;
    ///   its stop loss at 614.39 and take profit at 626.81 (620.60 less
    ///   and plus 1%, to the cent);
    /// - 14:00 at 624.60 and 15:00 at 624.80, above the slow EMA (615.60,
    ///   then 621.60): long, no cross. The bids after the entry, 623.96 to
    ///   624.75, touch neither exit;
    /// - 14:00, an hour after the entry filled: the time alert closes the
    ///   long, a sell of 10 at the bid of the quote closing 14:00, 624.55,
    ///   and cancels its stop loss and take profit.
    ///
    /// The 10 shares gain 3.90 each: the balance ends at 10039.00 EUR,
    /// after 4 orders (the entry, its two legs, the close). Had the
    /// partial bar counted, 13:00 to 15:00 would all be short (below 635.60,
    /// 625.60, 624.93): no trade; without the warmup, the crossover would
    /// be cold until 14:00, then long: no trade either.
    pub const TIME_EXIT: [f64; 3] = [620.0, 624.0, 624.2];
    pub const TIME_EXIT_ORDERS: usize = 4;
    pub const TIME_EXIT_BALANCE: &str = "10039.00 EUR";

    /// The exits of `TIME_EXIT`: 100 bps, held an hour.
    pub fn time_exit() -> Exits {
        Exits {
            stop_loss_bps: 100,
            take_profit_bps: 100,
            hold: SignedDuration::from_hours(1),
        }
    }

    /// The shortest crossover with `exits`, after the warmup.
    pub fn ema_cross(exits: Exits) -> EmaCross {
        shortest_ema_cross(WARMUP, exits)
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

/// Two overlapping trades on the steps, from 12:30 without warmup: a
/// recorder, its `oms_type` unset, buys 10 shares on the 13:00 bar, at the
/// ask of the quote closing it, 620.65, and 10 more on the 14:00 bar, at
/// 580.65, and closes neither. A netting venue holds them as one long
/// position of 20 at their average, 600.65; a hedging venue as two of 10,
/// one at each price.
pub mod overlap {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_model::enums::{OmsType, OrderSide};

    use super::*;

    /// Runs the recorder on a venue keeping positions as `venue_oms` says
    /// and returns its record.
    pub fn run(venue_oms: fn(Venue) -> OmsType) -> Rc<RefCell<recorder::Record>> {
        let catalog = steps::catalog();
        let backtest = Backtest {
            venue_oms,
            ..steps::backtest(catalog.path(), steps::at("12:30"))
        };
        let (recorder, record) = recorder::Recorder::trading_without_warmup(
            BarType::from(MID_BAR_TYPE),
            vec![
                (OrderSide::Buy, Quantity::from(10)),
                (OrderSide::Buy, Quantity::from(10)),
            ],
        );
        backtest.run(recorder).unwrap();
        record
    }
}
