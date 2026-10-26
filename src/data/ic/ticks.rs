//! cTrader's tick history and the quotes made from it.
//!
//! cTrader serves the ticks of a symbol as two series, BID and ASK, each
//! asked for over `[fromTimestamp, toTimestamp)` in Unix milliseconds
//! (`ProtoOAGetTickDataReq`) and answered newest first, a page at a time
//! (`hasMore`). A tick carries a time and a price, no size. Quotes are
//! made by merging the two series (see `merge`).

use anyhow::{Context, ensure};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::QuoteTick,
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
    types::{Price, Quantity},
};
use rust_decimal::Decimal;

use super::messages::ProtoOaTickData;

/// The longest window one tick request may span (Spotware's docs: one
/// week).
pub(super) const WEEK_MS: i64 = 7 * 24 * 60 * 60 * 1000;

/// The decimals of cTrader's integer prices: a tick of 412847000 is
/// 4128.47000.
const PRICE_SCALE: u32 = 5;

const NANOS_PER_MS: u64 = 1_000_000;

/// The nominal size of both sides of a quote, in units, unless the
/// instrument's maximum order is smaller (see `quote_size`).
const QUOTE_SIZE: f64 = 100_000.0;

/// A tick of one side: its time in Unix milliseconds and its price in
/// 1/100000.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Tick {
    pub(crate) ms: i64,
    pub(crate) price: i64,
}

/// The ticks of `tick_data`, newest first, absolute: the first element
/// holds the time and price, each following one the difference in both
/// from the element before.
///
/// # Errors
///
/// Returns an error if a tick is newer than the one before it.
pub(super) fn decode(tick_data: &[ProtoOaTickData]) -> anyhow::Result<Vec<Tick>> {
    let mut ticks: Vec<Tick> = Vec::with_capacity(tick_data.len());
    for data in tick_data {
        let tick = match ticks.last() {
            None => Tick {
                ms: data.timestamp,
                price: data.tick,
            },
            Some(newer) => {
                ensure!(
                    data.timestamp <= 0,
                    "cTrader's ticks are not newest first: a tick {} ms after the one before",
                    data.timestamp
                );
                Tick {
                    ms: newer.ms + data.timestamp,
                    price: newer.price + data.tick,
                }
            }
        };
        ticks.push(tick);
    }
    Ok(ticks)
}

/// The ticks of one side over `[from_ms, to_ms)`, oldest first, through
/// `page(from, to)`, which answers cTrader's tick request over `[from,
/// to)`: the newest ticks first, at most a page, and whether there are
/// more. Requests span at most `WEEK_MS`, walking back from `to_ms`.
/// `page` returns a future rather than being an async closure, whose
/// futures the compiler cannot prove `Send` for a spawned request.
///
/// Within a window, the next page ends 1 ms after the oldest tick of the
/// page before, whose ticks of that millisecond are dropped: the next page
/// holds all of them, so ticks sharing a millisecond across a page boundary
/// are neither lost nor doubled.
///
/// # Errors
///
/// Returns an error if a request fails, or if a page with more after it
/// holds a single millisecond (no progress).
pub(super) async fn side_ticks<Page>(
    from_ms: i64,
    to_ms: i64,
    mut page: impl FnMut(i64, i64) -> Page,
) -> anyhow::Result<Vec<Tick>>
where
    Page: Future<Output = anyhow::Result<(Vec<Tick>, bool)>>,
{
    // Newest first until reversed at the end.
    let mut ticks = Vec::new();
    let mut window_to = to_ms;
    while window_to > from_ms {
        let window_from = from_ms.max(window_to - WEEK_MS);
        let mut to = window_to;
        loop {
            let (answer, more) = page(window_from, to).await?;
            let (Some(newest), Some(oldest)) = (answer.first(), answer.last()) else {
                break;
            };
            if !more {
                ticks.extend(answer);
                break;
            }
            ensure!(
                newest.ms > oldest.ms,
                "cTrader answered a full page of ticks all at {} ms: no way to page past it",
                oldest.ms
            );
            let oldest = oldest.ms;
            ticks.extend(answer.into_iter().filter(|t| t.ms > oldest));
            to = oldest + 1;
        }
        window_to = window_from;
    }
    ticks.reverse();
    Ok(ticks)
}

/// The last tick of one side before `ms`, if any in the week before, as
/// `side_ticks` asks.
///
/// # Errors
///
/// Returns an error if the request fails.
pub(super) async fn last_before<Page>(
    ms: i64,
    mut page: impl FnMut(i64, i64) -> Page,
) -> anyhow::Result<Option<Tick>>
where
    Page: Future<Output = anyhow::Result<(Vec<Tick>, bool)>>,
{
    let (answer, _) = page(ms - WEEK_MS, ms).await?;
    Ok(answer.first().copied())
}

/// One side's ticks for `merge`: the last one before the window, and those
/// in it, oldest first.
#[derive(Debug, Default)]
pub(super) struct SideTicks {
    pub(super) before: Option<Tick>,
    pub(super) ticks: Vec<Tick>,
}

/// The quotes merged from a BID and an ASK series, and what was dropped.
#[derive(Debug)]
pub(super) struct MergedQuotes {
    /// Sorted by `ts_init`, one per millisecond.
    pub(super) quotes: Vec<QuoteTick>,
    /// Ticks merged into no quote: before both sides were known.
    pub(super) one_sided: usize,
    /// Quotes whose ask is below the bid.
    pub(super) crossed: usize,
}

/// The quotes of `instrument` from `bid` and `ask`: one per millisecond
/// with a tick of either side, ticks of the same millisecond combined (the
/// last of each side wins), by the rule of `QuoteMerge`, the ticks before
/// the window seeding it. `ts_event = ts_init` = the tick's millisecond.
///
/// # Errors
///
/// Returns an error if a price is not on the instrument's precision.
pub(super) fn merge(
    instrument: &InstrumentAny,
    bid: &SideTicks,
    ask: &SideTicks,
) -> anyhow::Result<MergedQuotes> {
    let mut rule = QuoteMerge::new(instrument)?;
    rule.bid = bid.before.map(|t| t.price);
    rule.ask = ask.before.map(|t| t.price);
    let mut merged = MergedQuotes {
        quotes: Vec::with_capacity(bid.ticks.len().max(ask.ticks.len())),
        one_sided: 0,
        crossed: 0,
    };
    let (mut bids, mut asks) = (bid.ticks.iter().peekable(), ask.ticks.iter().peekable());
    loop {
        let ms = match (bids.peek(), asks.peek()) {
            (None, None) => break,
            (Some(b), None) => b.ms,
            (None, Some(a)) => a.ms,
            (Some(b), Some(a)) => b.ms.min(a.ms),
        };
        let (mut last_bid, mut last_ask, mut ticks) = (None, None, 0);
        while let Some(b) = bids.next_if(|b| b.ms == ms) {
            last_bid = Some(b.price);
            ticks += 1;
        }
        while let Some(a) = asks.next_if(|a| a.ms == ms) {
            last_ask = Some(a.price);
            ticks += 1;
        }
        match rule.next(ms, last_bid, last_ask, None)? {
            Merged::Quote(quote) => merged.quotes.push(quote),
            Merged::OneSided => merged.one_sided += ticks,
            Merged::Crossed => merged.crossed += 1,
        }
    }
    Ok(merged)
}

/// The rule making an instrument's quotes of its BID and ASK ticks, one
/// millisecond at a time, the same for history and live spots: the side
/// without a tick is carried from its last one; no quote before both sides
/// are known; a crossed quote (ask below bid) is dropped, so an order fills
/// on the quote before it; a locked one (ask = bid) is kept. Prices at the
/// instrument's precision, sizes `quote_size`.
#[derive(Debug)]
pub(crate) struct QuoteMerge {
    instrument_id: InstrumentId,
    precision: u8,
    size: Quantity,
    /// The last price of each side, in 1/100000.
    bid: Option<i64>,
    ask: Option<i64>,
}

/// What a millisecond of ticks made.
#[derive(Debug, PartialEq)]
pub(crate) enum Merged {
    Quote(QuoteTick),
    /// No quote: a side is not known yet.
    OneSided,
    /// No quote: the ask is below the bid.
    Crossed,
}

impl QuoteMerge {
    /// The rule for `instrument`, no side known yet.
    ///
    /// # Errors
    ///
    /// Returns an error if the quote size is not on the instrument's size
    /// precision.
    pub(crate) fn new(instrument: &InstrumentAny) -> anyhow::Result<Self> {
        Ok(Self {
            instrument_id: instrument.id(),
            precision: instrument.price_precision(),
            size: quote_size(instrument)?,
            bid: None,
            ask: None,
        })
    }

    /// The quote at millisecond `ms` after its last `bid` and `ask` ticks
    /// (in 1/100000), either of them absent when that side has no tick
    /// then; stamped `ts_event` = `ms` and `ts_init` = `ts_init`, or `ms`
    /// without one.
    ///
    /// # Errors
    ///
    /// Returns an error if a price is not on the instrument's precision, or
    /// `ms` is before 1970.
    pub(crate) fn next(
        &mut self,
        ms: i64,
        bid: Option<i64>,
        ask: Option<i64>,
        ts_init: Option<UnixNanos>,
    ) -> anyhow::Result<Merged> {
        self.bid = bid.or(self.bid);
        self.ask = ask.or(self.ask);
        let (Some(b), Some(a)) = (self.bid, self.ask) else {
            return Ok(Merged::OneSided);
        };
        if a < b {
            return Ok(Merged::Crossed);
        }
        let ts_event = UnixNanos::from(
            u64::try_from(ms).with_context(|| format!("a tick at {ms} ms, before 1970"))?
                * NANOS_PER_MS,
        );
        Ok(Merged::Quote(QuoteTick::new(
            self.instrument_id,
            price(b, self.precision)?,
            price(a, self.precision)?,
            self.size,
            self.size,
            ts_event,
            ts_init.unwrap_or(ts_event),
        )))
    }
}

/// The size of both sides of `instrument`'s quotes: cTrader's ticks have
/// none. `QUOTE_SIZE`, or the instrument's maximum order if smaller, which
/// the risk engine never lets an order exceed, at its size precision.
///
/// The matching engine fills a market order up to the quote's size and
/// the rest one tick worse; the data engine adds each quote's size to the
/// volume of the bar it builds, a `Quantity` that panics past
/// 18,446,744,073. The size bounds both: orders up to it fill whole at the
/// quote, and a bar may hold up to 18,446,744,073 / size quotes (see
/// `ic/README.md`, "Quote size").
fn quote_size(instrument: &InstrumentAny) -> anyhow::Result<Quantity> {
    let precision = instrument.size_precision();
    let nominal = Quantity::new_checked(QUOTE_SIZE, precision)?;
    Ok(match instrument.max_quantity() {
        Some(max) if max < nominal => Quantity::from_raw(max.raw(), precision),
        _ => nominal,
    })
}

/// The price of `tick` at `precision`.
fn price(tick: i64, precision: u8) -> anyhow::Result<Price> {
    let value = Decimal::new(tick, PRICE_SCALE);
    ensure!(
        value.normalize().scale() <= u32::from(precision),
        "cTrader price {value} is finer than the instrument's {precision} decimals"
    );
    Ok(Price::from_decimal_dp(value, precision)?)
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        identifiers::InstrumentId,
        instruments::{CurrencyPair, stubs::audusd_sim},
    };
    use prost::Message as _;

    use super::*;
    use crate::data::ic::{
        messages::ProtoOaGetTickDataRes,
        testing::{TICK_DATA_ASK_RES, TICK_DATA_BID_RES, venue_page},
    };

    fn tick(ms: i64, price: i64) -> Tick {
        Tick { ms, price }
    }

    #[test]
    fn a_captured_response_decodes_newest_first_from_its_differences() {
        let res = ProtoOaGetTickDataRes::decode(TICK_DATA_BID_RES).unwrap();

        let ticks = decode(&res.tick_data).unwrap();

        // As captured: the first absolute, the second 350 ms older and
        // 0.15 higher.
        assert_eq!(ticks.len(), 14);
        assert_eq!(ticks[0], tick(1_792_591_204_967, 412_847_000));
        assert_eq!(ticks[1], tick(1_792_591_204_617, 412_862_000));
        assert_eq!(ticks[13], tick(1_792_591_200_040, 412_929_000));
        assert!(!res.has_more);
    }

    /// Synthetic: a difference forward in time.
    #[test]
    fn a_tick_newer_than_the_one_before_is_refused() {
        let data = |timestamp, tick| ProtoOaTickData { timestamp, tick };

        let err = decode(&[data(1_000, 5), data(-1, 1), data(2, 1)]).unwrap_err();

        assert!(err.to_string().contains("not newest first"), "{err}");
    }

    /// Answers like cTrader over `ticks` (oldest first), pages of 3, and
    /// records each request's window.
    fn venue(
        ticks: &[Tick],
        asked: &mut Vec<(i64, i64)>,
    ) -> impl FnMut(i64, i64) -> std::future::Ready<anyhow::Result<(Vec<Tick>, bool)>> {
        move |from, to| {
            asked.push((from, to));
            std::future::ready(Ok(venue_page(ticks, from, to, 3)))
        }
    }

    #[tokio::test]
    async fn pages_walk_back_and_keep_every_tick_sharing_a_millisecond_across_a_boundary() {
        // Pages of 3, newest first: 15, 9, 9 | 9, 9, 7 | 7, 7, 3 | 3, 1:
        // the two ticks of 7 ms straddle the second and third pages.
        let all = [
            tick(1, 10),
            tick(3, 11),
            tick(7, 12),
            tick(7, 13),
            tick(9, 14),
            tick(9, 15),
            tick(15, 16),
        ];
        let mut asked = vec![];

        let ticks = side_ticks(0, 20, venue(&all, &mut asked)).await.unwrap();

        assert_eq!(ticks, all);
        assert_eq!(asked, [(0, 20), (0, 10), (0, 8), (0, 4)]);
    }

    #[tokio::test]
    async fn only_ticks_in_the_window_come_back_from_its_first_millisecond_to_before_its_end() {
        let all = [tick(9, 1), tick(10, 2), tick(19, 3), tick(20, 4)];

        let ticks = side_ticks(10, 20, venue(&all, &mut vec![])).await.unwrap();

        assert_eq!(ticks, [tick(10, 2), tick(19, 3)]);
    }

    #[tokio::test]
    async fn a_long_window_is_asked_a_week_at_a_time_from_its_end() {
        let all = [tick(1, 1), tick(WEEK_MS + 5, 2), tick(2 * WEEK_MS + 5, 3)];
        let mut asked = vec![];
        let end = 2 * WEEK_MS + 100;

        let ticks = side_ticks(0, end, venue(&all, &mut asked)).await.unwrap();

        assert_eq!(ticks, all);
        assert_eq!(
            asked,
            [
                (end - WEEK_MS, end),
                (end - 2 * WEEK_MS, end - WEEK_MS),
                (0, 100)
            ]
        );
    }

    #[tokio::test]
    async fn a_window_without_ticks_is_empty_not_an_error() {
        let ticks = side_ticks(0, 20, venue(&[], &mut vec![])).await.unwrap();

        assert_eq!(ticks, []);
    }

    /// Synthetic: more ticks in one millisecond than a page holds.
    #[tokio::test]
    async fn a_full_page_of_one_millisecond_is_an_error() {
        let all = [tick(5, 1), tick(5, 2), tick(5, 3), tick(5, 4)];

        let err = side_ticks(0, 20, venue(&all, &mut vec![]))
            .await
            .unwrap_err();

        assert!(err.to_string().contains("5"), "{err}");
    }

    #[tokio::test]
    async fn the_last_tick_before_comes_from_the_week_before() {
        let all = [tick(1, 1), tick(50, 2), tick(99, 3), tick(100, 4)];
        let mut asked = vec![];

        let before = last_before(100, venue(&all, &mut asked)).await.unwrap();

        assert_eq!(before, Some(tick(99, 3)));
        assert_eq!(asked, [(100 - WEEK_MS, 100)]);
        assert_eq!(
            last_before(1, venue(&all, &mut vec![])).await.unwrap(),
            None
        );
    }

    /// EUR/USD-like, 5 decimals, sizes in whole units.
    fn instrument() -> InstrumentAny {
        let mut pair: CurrencyPair = audusd_sim();
        pair.id = InstrumentId::from("EURUSD.ICMARKETS");
        InstrumentAny::CurrencyPair(pair)
    }

    fn side(before: Option<Tick>, ticks: &[Tick]) -> SideTicks {
        SideTicks {
            before,
            ticks: ticks.to_vec(),
        }
    }

    /// (ms, bid, ask) of `quotes`.
    fn prices(merged: &MergedQuotes) -> Vec<(u64, String, String)> {
        merged
            .quotes
            .iter()
            .map(|q| {
                assert_eq!(q.ts_event, q.ts_init);
                (
                    q.ts_init.as_u64() / 1_000_000,
                    q.bid_price.to_string(),
                    q.ask_price.to_string(),
                )
            })
            .collect()
    }

    fn quote(ms: u64, bid: &str, ask: &str) -> (u64, String, String) {
        (ms, bid.to_owned(), ask.to_owned())
    }

    #[test]
    fn a_quote_per_tick_time_with_the_other_side_carried_and_none_before_both_sides() {
        let bid = side(
            None,
            &[tick(1, 116_000), tick(3, 116_010), tick(4, 116_020)],
        );
        let ask = side(None, &[tick(2, 116_030), tick(3, 116_040)]);

        let merged = merge(&instrument(), &bid, &ask).unwrap();

        assert_eq!(
            prices(&merged),
            [
                quote(2, "1.16000", "1.16030"),
                quote(3, "1.16010", "1.16040"),
                quote(4, "1.16020", "1.16040"),
            ]
        );
        assert_eq!(merged.one_sided, 1);
    }

    #[test]
    fn the_ticks_before_the_window_seed_both_sides() {
        let bid = side(Some(tick(-5, 116_000)), &[tick(1, 116_010)]);
        let ask = side(Some(tick(-9, 116_020)), &[tick(2, 116_030)]);

        let merged = merge(&instrument(), &bid, &ask).unwrap();

        assert_eq!(
            prices(&merged),
            [
                quote(1, "1.16010", "1.16020"),
                quote(2, "1.16010", "1.16030")
            ]
        );
        assert_eq!(merged.one_sided, 0);
    }

    #[test]
    fn ticks_of_one_millisecond_make_one_quote_the_last_of_each_side_winning() {
        let bid = side(None, &[tick(1, 116_000), tick(1, 116_005)]);
        let ask = side(
            None,
            &[tick(1, 116_020), tick(1, 116_015), tick(2, 116_025)],
        );

        let merged = merge(&instrument(), &bid, &ask).unwrap();

        assert_eq!(
            prices(&merged),
            [
                quote(1, "1.16005", "1.16015"),
                quote(2, "1.16005", "1.16025")
            ]
        );
    }

    #[test]
    fn a_crossed_quote_is_dropped_and_counted_a_locked_one_kept() {
        let bid = side(
            None,
            &[tick(1, 116_000), tick(2, 116_050), tick(3, 116_030)],
        );
        let ask = side(None, &[tick(1, 116_030)]);

        let merged = merge(&instrument(), &bid, &ask).unwrap();

        assert_eq!(
            prices(&merged),
            [
                quote(1, "1.16000", "1.16030"),
                quote(3, "1.16030", "1.16030")
            ]
        );
        assert_eq!(merged.crossed, 1);
    }

    #[test]
    fn quotes_are_stamped_at_their_millisecond_with_a_size_of_100000() {
        let bid = side(None, &[tick(1_792_591_200_040, 116_000)]);
        let ask = side(None, &[tick(1_792_591_200_040, 116_010)]);
        // The stub's maximum order is 1,000,000.
        let instrument = instrument();

        let merged = merge(&instrument, &bid, &ask).unwrap();

        let q = merged.quotes[0];
        assert_eq!(q.ts_init, UnixNanos::from(1_792_591_200_040_000_000));
        assert_eq!(q.ts_event, q.ts_init);
        assert_eq!(q.instrument_id, instrument.id());
        assert_eq!(q.bid_size, Quantity::from(100_000));
        assert_eq!(q.ask_size, Quantity::from(100_000));
    }

    #[test]
    fn the_size_is_the_maximum_order_where_that_is_smaller() {
        let bid = side(None, &[tick(1, 116_000)]);
        let ask = side(None, &[tick(1, 116_010)]);
        let mut pair: CurrencyPair = audusd_sim();
        // XAUUSD's at IC: 10,000 oz.
        pair.max_quantity = Some(Quantity::from(10_000));

        let merged = merge(&InstrumentAny::CurrencyPair(pair), &bid, &ask).unwrap();

        assert_eq!(merged.quotes[0].bid_size, Quantity::from(10_000));
        assert_eq!(merged.quotes[0].ask_size, Quantity::from(10_000));
    }

    #[test]
    fn a_captured_bid_and_ask_merge_into_gold_quotes() {
        let side_of = |bytes| {
            let res = ProtoOaGetTickDataRes::decode(bytes).unwrap();
            let mut ticks = decode(&res.tick_data).unwrap();
            ticks.reverse();
            side(None, &ticks)
        };
        let mut xau: CurrencyPair = audusd_sim();
        xau.id = InstrumentId::from("XAUUSD.ICMARKETS");
        xau.price_precision = 2;
        xau.price_increment = Price::from("0.01");

        let merged = merge(
            &InstrumentAny::CurrencyPair(xau),
            &side_of(TICK_DATA_BID_RES),
            &side_of(TICK_DATA_ASK_RES),
        )
        .unwrap();

        // The 14 ticks of each side share their milliseconds (see the
        // .txtpb fixtures).
        assert_eq!(merged.quotes.len(), 14);
        assert_eq!(
            prices(&merged)[0],
            quote(1_792_591_200_040, "4129.29", "4129.34")
        );
        assert_eq!(
            prices(&merged)[13],
            quote(1_792_591_204_967, "4128.47", "4128.52")
        );
    }

    /// Synthetic: a price finer than the instrument's precision.
    #[test]
    fn a_price_off_the_instruments_precision_is_refused() {
        let bid = side(None, &[tick(1, 116_000)]);
        let ask = side(None, &[tick(1, 116_011)]);
        let mut pair: CurrencyPair = audusd_sim();
        pair.price_precision = 4;
        pair.price_increment = Price::from("0.0001");

        let err = merge(&InstrumentAny::CurrencyPair(pair), &bid, &ask).unwrap_err();

        assert!(err.to_string().contains("1.16011"), "{err}");
    }
}
