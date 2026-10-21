//! Hours without quotes: the market is closed (a night, a weekend, a
//! holiday), so the data engine builds no bar for them, in the warmup as
//! in the replay. One node per process, so one run per file.

mod common;

use common::{hour_bars, minute_quotes, noise, ohlc, recorder::Recorder};
use jiff::{SignedDuration, Timestamp};
use nautilus_model::data::BarType;

#[test]
fn an_hour_without_quotes_yields_no_bar() {
    // A quote a minute from 06:00 to 15:00 UTC on Wednesday 7 January
    // 2026, none in the hours closing 09:00 (in the warmup) and 13:00 (in
    // the replay). The run starts at 11:30 after 4 hours of warmup.
    let at = noise::at;
    let gap = |close: Timestamp| {
        (close > at("08:00") && close <= at("09:00"))
            || (close > at("12:00") && close <= at("13:00"))
    };
    let quotes = minute_quotes(at("06:00"), at("15:00"), |close| {
        (!gap(close)).then_some(600.0 + close.duration_since(at("06:00")).as_mins() as f64 / 100.0)
    });
    let catalog = common::catalog(&quotes, at("06:00"), at("15:00"));
    let (start, warmup) = (at("11:30"), SignedDuration::from_hours(4));
    let (recorder, record) = Recorder::new(BarType::from(common::MID_BAR_TYPE), warmup, None);

    common::backtest(catalog.path(), start, at("15:00"), warmup, "0 EUR")
        .run(recorder)
        .unwrap();

    // The warmup from 07:29:59.999999999 drops the partial bar closing
    // 08:00 and builds those closing 10:00 and 11:00; the replay gets
    // those closing 12:00, 14:00 and 15:00. None closes 09:00 or 13:00.
    let record = record.borrow();
    let expected = hour_bars(&quotes, start - SignedDuration::from_nanos(1) - warmup);
    let warmup: Vec<_> = record.warmup.iter().map(ohlc).collect();
    let replay: Vec<_> = record.bars.iter().map(ohlc).collect();
    let closes = |bars: &[common::Ohlc]| bars.iter().map(|bar| bar.0).collect::<Vec<_>>();
    assert_eq!(closes(&warmup), [at("10:00"), at("11:00")]);
    assert_eq!(closes(&replay), [at("12:00"), at("14:00"), at("15:00")]);
    assert_eq!([warmup, replay].concat(), expected);
}
