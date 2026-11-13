//! A tick into Nautilus data, at the instrument's precision: a series'
//! into a quote, an option contract's into a trade. LSE sends prices and
//! sizes as doubles, each converted once through its shortest decimal
//! form, which must fit the precision, as an export's are.

use anyhow::{Context, ensure};
use jiff::Timestamp;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{QuoteTick, TradeTick},
    enums::AggressorSide,
    identifiers::InstrumentId,
    types::{Price, Quantity},
};

use super::messages::LseTick;
use crate::http::parse::{TradeIds, decimal};

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
    let none = Quantity::zero(size_precision);
    QuoteTick::new_checked(
        instrument_id,
        price(tick.bid, "bid")?,
        price(tick.ask, "ask")?,
        none,
        none,
        ts_event(tick)?,
        ts_init,
    )
}

/// `tick`, an option contract's, as a trade of `instrument_id`: its price
/// at `price_precision` and its volume, the trade's size, at
/// `size_precision`, with no aggressor side, LSE sending none, and the id
/// `ids` gives it, as an export's trade gets one; stamped at LSE's time of
/// it (`ts_event`) and at `ts_init`.
///
/// # Errors
///
/// Returns an error if the tick has no price, no volume or a volume of 0,
/// a quote's, if its price or volume has more decimals than its precision,
/// or if its time is no timestamp.
pub(crate) fn parse_trade(
    tick: &LseTick,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
    ids: &mut TradeIds,
    ts_init: UnixNanos,
) -> anyhow::Result<TradeTick> {
    let symbol = &tick.symbol;
    let price = tick
        .price
        .with_context(|| format!("a tick of {symbol} with no price"))?;
    let volume = tick.volume.unwrap_or_default();
    ensure!(
        volume > 0.0,
        "a tick of {symbol} with no trade size: no volume or a volume of 0, a quote's"
    );
    let price = Price::from_decimal_dp(decimal(price, price_precision, "price")?, price_precision)?;
    let size = Quantity::from_decimal_dp(decimal(volume, size_precision, "size")?, size_precision)?;
    let ts_event = ts_event(tick)?;
    TradeTick::new_checked(
        instrument_id,
        price,
        size,
        AggressorSide::NoAggressor,
        ids.next(instrument_id, ts_event, price, size),
        ts_event,
        ts_init,
    )
}

/// LSE's time of `tick`.
fn ts_event(tick: &LseTick) -> anyhow::Result<UnixNanos> {
    let at: Timestamp = tick
        .ts
        .parse()
        .with_context(|| format!("a tick of {} at {:?}", tick.symbol, tick.ts))?;
    Ok(UnixNanos::from(at))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use bytes::Bytes;
    use nautilus_model::{data::TradeTick, enums::AggressorSide};

    use super::*;
    use crate::{
        http::parse::{TradeIds, parse_trade_slice},
        testing::{
            AAPL_CALL, AAPL_CALL_TRADES, GOLD_FUTURES, WS_TICKS_AAPL_OPTIONS, WS_TICKS_GOLD,
        },
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

    /// The `n`th option tick of LSE's documentation's chain of AAPL.
    fn option_tick(n: usize) -> LseTick {
        let text = WS_TICKS_AAPL_OPTIONS.lines().nth(n).unwrap();
        let LseWsFrame::Tick(tick) = serde_json::from_str(text).unwrap() else {
            panic!("not a tick")
        };
        tick
    }

    fn trade(tick: &LseTick, ids: &mut TradeIds) -> anyhow::Result<TradeTick> {
        parse_trade(
            tick,
            InstrumentId::from(AAPL_CALL),
            2,
            0,
            ids,
            at("2026-11-12T14:30:01Z"),
        )
    }

    /// The first, a trade of AAPL's 355 call of 1 contract at 6.70 at
    /// 14:30:00.816861166 on 12 November 2026, as LSE exported it, which
    /// its export's tick slice holds first too: the same trade, the same id.
    #[rstest]
    fn an_option_tick_is_a_trade_of_its_price_and_volume_with_no_aggressor_at_lse_s_time() {
        let exported = parse_trade_slice(
            Bytes::from_static(AAPL_CALL_TRADES),
            InstrumentId::from(AAPL_CALL),
            2,
            0,
        )
        .unwrap()[0];

        let trade = trade(&option_tick(0), &mut TradeIds::default()).unwrap();

        assert_eq!(
            trade,
            TradeTick::new(
                InstrumentId::from(AAPL_CALL),
                Price::from("6.70"),
                Quantity::from(1),
                AggressorSide::NoAggressor,
                exported.trade_id,
                at("2026-11-12T14:30:00.816861166Z"),
                at("2026-11-12T14:30:01Z"),
            )
        );
        assert_eq!(exported.ts_event, trade.ts_event);
    }

    /// Two trades alike, of one time, price and size, as OPRA reports a
    /// contract's fills of one order, keep ids apart, and the same trades
    /// get the same ids again.
    #[rstest]
    fn trades_alike_get_ids_of_their_own_and_the_same_ones_each_time() {
        let tick = option_tick(0);
        let ids_of = || {
            let mut ids = TradeIds::default();
            [&tick, &tick, &option_tick(1)].map(|tick| trade(tick, &mut ids).unwrap().trade_id)
        };

        let ids = ids_of();

        assert_ne!(ids[0], ids[1]);
        assert_ne!(ids[1], ids[2]);
        assert_eq!(ids, ids_of());
    }

    /// Its volume is the trade's size, 0 on a quote (LSE's WebSocket
    /// documentation, "The tick"), which Nautilus' trade cannot have.
    #[rstest]
    #[case::of_no_volume(None)]
    #[case::of_volume_zero(Some(0.0))]
    fn an_option_tick_of_no_trade_size_is_refused_naming_its_symbol(#[case] volume: Option<f64>) {
        let tick = LseTick {
            volume,
            ..option_tick(0)
        };

        let err = trade(&tick, &mut TradeIds::default())
            .unwrap_err()
            .to_string();

        assert!(err.contains("AAPL261120C00355000"), "{err}");
    }

    /// Rounding it would change LSE's price.
    #[rstest]
    fn an_option_price_finer_than_the_contract_s_precision_is_refused() {
        let tick = LseTick {
            price: Some(6.705),
            ..option_tick(0)
        };

        let err = trade(&tick, &mut TradeIds::default())
            .unwrap_err()
            .to_string();

        assert!(err.contains("6.705"), "{err}");
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
