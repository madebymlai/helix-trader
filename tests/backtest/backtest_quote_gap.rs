//! Hours without quotes: the market is closed (a night, a weekend, a
//! holiday), so the data engine builds no bar for them. One node per
//! thread, so one run per test.

use crate::common::{self, hour_bars, minute_quotes, noise, ohlc, recorder::Recorder};
use jiff::Timestamp;
use nautilus_model::data::BarType;

#[test]
fn an_hour_without_quotes_yields_no_bar() {
    // A quote a minute from 06:00 to 15:00 UTC on Wednesday 7 January
    // 2026, none in the hours closing 09:00 and 13:00. The run starts at
    // 07:00.
    let at = noise::at;
    let gap = |close: Timestamp| {
        (close > at("08:00") && close <= at("09:00"))
            || (close > at("12:00") && close <= at("13:00"))
    };
    let quotes = minute_quotes(at("06:00"), at("15:00"), |close| {
        (!gap(close)).then_some(600.0 + close.duration_since(at("06:00")).as_mins() as f64 / 100.0)
    });
    let catalog = common::catalog(&quotes, at("06:00"), at("15:00"));
    let start = at("07:00");
    let (recorder, record) = Recorder::new(BarType::from(common::MID_BAR_TYPE), None);

    common::backtest(catalog.path(), start, at("15:00"), "0 EUR")
        .run(recorder)
        .unwrap();

    // The bars closing 08:00, 10:00 to 12:00, 14:00 and 15:00; none closes
    // 09:00 or 13:00.
    let record = record.borrow();
    let bars: Vec<_> = record.bars.iter().map(ohlc).collect();
    let closes: Vec<_> = bars.iter().map(|bar| bar.0).collect();
    assert_eq!(
        closes,
        [
            at("08:00"),
            at("10:00"),
            at("11:00"),
            at("12:00"),
            at("14:00"),
            at("15:00")
        ]
    );
    assert_eq!(bars, hour_bars(&quotes, start));
}
