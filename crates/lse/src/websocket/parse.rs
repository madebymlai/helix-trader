//! A tick into Nautilus' quote, at the instrument's precision: LSE sends
//! the bid and ask as doubles, each converted once through its shortest
//! decimal form, which must fit the precision, as an export's prices are.

use anyhow::Context;
use jiff::Timestamp;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::QuoteTick,
    identifiers::InstrumentId,
    types::{Price, Quantity},
};

use super::messages::LseTick;
use crate::http::parse::decimal;

/// `tick` as a quote of `instrument_id`: its bid and ask at
/// `price_precision`, with no sizes, LSE's socket ticks carrying none, at
/// `size_precision`; stamped at LSE's time of it (`ts_event`) and at
/// `ts_init`.
///
/// # Errors
///
/// Returns an error if the tick has no bid or ask, if either has more
/// decimals than `price_precision`, or if its time is no timestamp.
pub(crate) fn parse_quote(
    tick: &LseTick,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) -> anyhow::Result<QuoteTick> {
    let price = |side: Option<f64>, what: &str| -> anyhow::Result<Price> {
        let value = side.with_context(|| format!("a tick of {} with no {what}", tick.symbol))?;
        Ok(Price::from_decimal_dp(
            decimal(value, price_precision, what)?,
            price_precision,
        )?)
    };
    let ts_event: Timestamp = tick
        .ts
        .parse()
        .with_context(|| format!("a tick of {} at {:?}", tick.symbol, tick.ts))?;
    let none = Quantity::zero(size_precision);
    QuoteTick::new_checked(
        instrument_id,
        price(tick.bid, "bid")?,
        price(tick.ask, "ask")?,
        none,
        none,
        UnixNanos::from(ts_event),
        ts_init,
    )
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::{
        testing::{GOLD_FUTURES, WS_TICKS_GOLD},
        websocket::messages::LseWsFrame,
    };

    fn captured(line: usize) -> LseTick {
        let text = WS_TICKS_GOLD.lines().nth(line).unwrap();
        let LseWsFrame::Tick(tick) = serde_json::from_str(text).unwrap() else {
            panic!("not a tick")
        };
        tick
    }

    fn at(time: &str) -> UnixNanos {
        UnixNanos::from(time.parse::<Timestamp>().unwrap())
    }

    /// The first tick captured: GOLD.F at 16:44:55.499567 on 13 November
    /// 2026, bid 3888.0, ask 3888.2, price 3888.0, volume 1.
    #[rstest]
    fn a_tick_is_a_quote_of_its_bid_and_ask_with_no_sizes_at_lse_s_time() {
        let received = at("2026-11-13T16:44:55.8Z");

        let quote = parse_quote(
            &captured(0),
            InstrumentId::from(GOLD_FUTURES),
            1,
            0,
            received,
        )
        .unwrap();

        assert_eq!(
            quote,
            QuoteTick::new(
                InstrumentId::from(GOLD_FUTURES),
                Price::from("3888.0"),
                Price::from("3888.2"),
                Quantity::from(0),
                Quantity::from(0),
                at("2026-11-13T16:44:55.499567Z"),
                received,
            )
        );
    }

    /// The forms LSE's documentation gives of its feeds' times: commodities'
    /// with a space, forex majors' to the nanosecond in Z, UK stocks' to
    /// the second.
    #[rstest]
    #[case("2026-10-23 20:59:56.585500+00:00", "2026-10-23T20:59:56.5855Z")]
    #[case("2026-10-23T20:59:05.029000000Z", "2026-10-23T20:59:05.029Z")]
    #[case("2026-10-23T16:30:00+00:00", "2026-10-23T16:30:00Z")]
    fn a_tick_s_time_is_read_in_each_form_lse_writes_it(#[case] ts: &str, #[case] utc: &str) {
        let tick = LseTick {
            ts: ts.to_string(),
            ..captured(0)
        };

        let quote = parse_quote(
            &tick,
            InstrumentId::from(GOLD_FUTURES),
            1,
            0,
            UnixNanos::default(),
        )
        .unwrap();

        assert_eq!(quote.ts_event, at(utc));
    }

    /// Rounding it would change LSE's price.
    #[rstest]
    fn a_price_finer_than_the_instrument_s_precision_is_refused() {
        let tick = LseTick {
            ask: Some(3888.25),
            ..captured(0)
        };

        let err = parse_quote(
            &tick,
            InstrumentId::from(GOLD_FUTURES),
            1,
            0,
            UnixNanos::default(),
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("3888.25"), "{err}");
    }

    #[rstest]
    fn a_tick_without_a_bid_is_refused_naming_its_symbol() {
        let tick = LseTick {
            bid: None,
            ..captured(0)
        };

        let err = parse_quote(
            &tick,
            InstrumentId::from(GOLD_FUTURES),
            1,
            0,
            UnixNanos::default(),
        )
        .unwrap_err()
        .to_string();

        assert!(err.contains("GOLD.F") && err.contains("bid"), "{err}");
    }
}
