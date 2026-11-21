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

/// Returns stepped quotes: minute `m` (1 to 60) of the hour bar closing at `c` quotes
/// `level(c) + 0.01 m`, or nothing if `level(c)` is `None`. A bar closes at its level + 0.60.
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

pub(crate) fn relative(dir: &Path) -> PathBuf {
    let cwd = std::env::current_dir().unwrap();
    let mut relative: PathBuf = std::iter::repeat_n("..", cwd.components().count() - 1).collect();
    relative.push(dir.strip_prefix("/").unwrap());
    relative
}

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

pub(crate) fn session() -> Session {
    Session {
        time_zone: TimeZone::get("Europe/Berlin").unwrap(),
        open: time(9, 0, 0, 0),
        close: time(17, 30, 0, 0),
    }
}

/// The risk is 0.32% of equity, which on stops of 50 bps from closes about 600 EUR buys 10 shares.
pub(crate) const RISK: &str = "0.32%";

pub(crate) const LIVE_WARMUP: SignedDuration = SignedDuration::from_hours(1);

/// Returns an EMA crossover config with a stop loss of 50 bps, a take profit of 100 bps and a
/// 4-hour hold.
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
/// The slow EMA starts as the average of its first two closes, then moves two thirds to each new
/// close. A close above the slow EMA of the bars before it is long, and a side change is a cross.
pub(crate) fn shortest_ema_cross() -> EmaCross {
    ema_cross(1, 2, Some(session()))
}

pub(crate) fn balance(result: &BacktestResult) -> String {
    result.summary["account.IBIS.balance.EUR.total"].clone()
}

/// A wave of quotes every minute for 192 hours from Monday 5 January 2026, at 600 EUR plus 5 EUR of
/// sine.
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
/// replayed. The shortest crossover initializes short at 12:00, with slow EMA 605.60. From 13:00
/// the bar closing at 13:00 + i hours is each test's `levels[i]`.
pub(crate) mod trades {
    use super::*;

    pub(crate) fn at(hh_mm: &str) -> Timestamp {
        ts(&format!("2026-01-07T{hh_mm}:00Z"))
    }

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

    /// A trade ended by time on the shortest crossover (`ema_config`).
    ///
    /// At 13:00 the close is 620.60, above 605.60, so it buys 10 at the ask 620.65, with the stop
    /// loss at 617.50 and the take profit at 626.81. Neither exit is touched, and at 17:00 it sells
    /// at the bid 624.85. The 10 shares gain 4.20 each, 10042.00 EUR after 4 orders (entry, two
    /// legs, close). From 09:00 the 10:00 bar would count and there would be no trade.
    pub(crate) const TIME_EXIT: [f64; 6] = [620.0, 624.0, 624.2, 624.2, 624.3, 624.4];
    pub(crate) const TIME_EXIT_ORDERS: usize = 4;
    pub(crate) const TIME_EXIT_BALANCE: &str = "10042.00 EUR";

    pub(crate) fn ema_cross() -> EmaCross {
        shortest_ema_cross()
    }

    /// Returns the config of `ema_cross` on SXR8's hour bars, for the binary, with `more` as lines
    /// of TOML at the end of `[trade]`.
    pub(crate) fn ema_toml(more: &str) -> String {
        format!(
            r#"
            [data]
            series = [
                {{ data = "bars", instrument_id = "{INSTRUMENT_ID}", bar_spec = {{ step = 1, aggregation = "HOUR", price_type = "MID" }} }},
            ]
            warmup = "1h"

            [indicators]
            fast = {{ period = 1 }}
            slow = {{ period = 2 }}

            [trade]
            instruments = [{{ instrument_id = "{INSTRUMENT_ID}" }}]
            risk = "{RISK}"
            {more}
            "#
        )
    }
}

/// A quote a minute on 7 January 2026 from 06:00 to 15:00 UTC, at mid 600 plus 0.01 times
/// `37 k mod 101` for the k-th minute, so every hour bar has its own open, high, low and close.
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
