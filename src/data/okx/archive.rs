//! OKX's order-book archive turned into quotes: one day's `.tar.gz` file
//! holds one JSON-lines file per contract (one for a swap, one per contract
//! of a futures chain), named after it: a full snapshot of the book every
//! 15 minutes and the changed levels in between. Each line becomes an
//! `OKXBookMsg`, the adapter's `parse_book_msg` makes its
//! `OrderBookDeltas`, and a Nautilus `OrderBook` replays them; a quote is
//! the book's top whenever it changes. To be removed if Nautilus gains a loader for this archive.

use std::io::{BufRead, BufReader, Read};

use anyhow::{Context, bail};
use flate2::read::GzDecoder;
use nautilus_core::{UnixNanos, serialization::deserialize_string_to_u64};
use nautilus_model::{
    data::QuoteTick,
    enums::BookType,
    instruments::{Instrument, InstrumentAny},
    orderbook::OrderBook,
    types::{Price, Quantity},
};
use nautilus_okx::{
    common::{enums::OKXBookAction, parse::parse_millisecond_timestamp},
    websocket::{
        messages::{OKXBookMsg, OrderBookEntry},
        parse::parse_book_msg,
    },
};
use serde::Deserialize;

/// The quotes of a day of the archive, and the tops dropped.
#[derive(Debug)]
pub(super) struct BookQuotes {
    /// Sorted by `ts_init`, one per change of the top.
    pub quotes: Vec<QuoteTick>,
    /// Tops whose ask was below the bid.
    pub crossed: usize,
    /// Tops with no bid or no ask.
    pub one_sided: usize,
}

/// One line of the archive: its contract and the fields of an `OKXBookMsg`
/// except the sequence numbers and checksum, which the archive does not
/// carry. Levels are `[price, size, order count]`; a size of `0` deletes
/// the level.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BookLine {
    inst_id: String,
    action: OKXBookAction,
    #[serde(deserialize_with = "deserialize_string_to_u64")]
    ts: u64,
    #[serde(default)]
    asks: Vec<OrderBookEntry>,
    #[serde(default)]
    bids: Vec<OrderBookEntry>,
}

/// The quotes of `instrument` in `[from, through]` from a day file of the
/// archive (`.tar.gz` of `.data` files), read as a stream: its contract's
/// file, `<symbol>-L2orderbook-….data`; the others are skipped.
///
/// # Errors
///
/// Returns an error if the file is not one such archive or holds no file
/// of the contract, or as `quotes_from_book`.
pub(super) fn quotes_from_archive(
    archive: impl Read,
    instrument: &InstrumentAny,
    from: UnixNanos,
    through: UnixNanos,
) -> anyhow::Result<BookQuotes> {
    let id = instrument.id();
    let prefix = format!("{}-L2orderbook-", id.symbol);
    let mut archive = tar::Archive::new(GzDecoder::new(archive));
    for entry in archive.entries().context("reading the archive")? {
        let entry = entry.context("reading the archive")?;
        let name = entry.path()?.display().to_string();
        if name.starts_with(&prefix) && name.ends_with(".data") {
            return quotes_from_book(BufReader::new(entry), instrument, from, through)
                .with_context(|| format!("reading {name}"));
        }
    }
    bail!("the archive holds no order-book file of {id} ({prefix}….data)")
}

/// The quotes of `instrument` in `[from, through]` from the lines of a
/// day of the archive, starting with a snapshot.
///
/// Replays every line from the start (the book needs the snapshot), and
/// stops after `through`. Each quote is the book's top (best bid and ask,
/// with their sizes) after a line, stamped with the line's exchange time
/// (`ts_event = ts_init`), at the instrument's precisions. The first line
/// at or after `from` gives a quote, then only a line that changes the top. A
/// crossed top (ask below bid) or a one-sided one is dropped and counted; a
/// locked one (ask = bid) is kept. A line with no level changes nothing.
///
/// # Errors
///
/// Returns an error naming the line if it cannot be read or parsed, if it
/// is of another contract, or if its levels do not fit the instrument's
/// precisions.
pub(super) fn quotes_from_book(
    lines: impl BufRead,
    instrument: &InstrumentAny,
    from: UnixNanos,
    through: UnixNanos,
) -> anyhow::Result<BookQuotes> {
    let id = instrument.id();
    let mut book = OrderBook::new(id, BookType::L2_MBP);
    let mut made = BookQuotes {
        quotes: Vec::new(),
        crossed: 0,
        one_sided: 0,
    };
    let mut last_top = None;
    for (index, line) in lines.lines().enumerate() {
        let number = index + 1;
        let line: BookLine =
            serde_json::from_str(&line?).with_context(|| format!("parsing line {number}"))?;
        if line.inst_id != id.symbol.as_str() {
            bail!("line {number} is of {}, not of {id}", line.inst_id);
        }
        let ts = parse_millisecond_timestamp(line.ts);
        if ts > through {
            break;
        }
        apply_line(&mut book, line, instrument, ts)
            .with_context(|| format!("applying line {number}"))?;
        if ts < from {
            continue;
        }
        let Some(top) = book_top(&book) else {
            made.one_sided += 1;
            continue;
        };
        if top.ask_price < top.bid_price {
            made.crossed += 1;
            continue;
        }
        if last_top == Some(top) {
            continue;
        }
        last_top = Some(top);
        made.quotes.push(QuoteTick::new_checked(
            id,
            top.bid_price,
            top.ask_price,
            top.bid_size,
            top.ask_size,
            ts,
            ts,
        )?);
    }
    Ok(made)
}

/// Applies `line` to `book` through the adapter's parsing of a book
/// message, as the live data client does with the same message.
fn apply_line(
    book: &mut OrderBook,
    line: BookLine,
    instrument: &InstrumentAny,
    ts: UnixNanos,
) -> anyhow::Result<()> {
    if line.asks.is_empty() && line.bids.is_empty() && line.action == OKXBookAction::Update {
        return Ok(());
    }
    let msg = OKXBookMsg {
        asks: line.asks,
        bids: line.bids,
        checksum: None,
        prev_seq_id: None,
        seq_id: 0,
        ts: line.ts,
    };
    let deltas = parse_book_msg(
        &msg,
        instrument.id(),
        instrument.price_precision(),
        instrument.size_precision(),
        &line.action,
        ts,
    )?;
    book.apply_deltas(&deltas)?;
    Ok(())
}

/// The best bid and ask of a book with their sizes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Top {
    bid_price: Price,
    bid_size: Quantity,
    ask_price: Price,
    ask_size: Quantity,
}

/// The top of `book`; `None` if a side is empty.
fn book_top(book: &OrderBook) -> Option<Top> {
    Some(Top {
        bid_price: book.best_bid_price()?,
        bid_size: book.best_bid_size()?,
        ask_price: book.best_ask_price()?,
        ask_size: book.best_ask_size()?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::okx::tests::{btc_instrument, xperp_instrument};

    /// A sample of the archive file of BTC-USDT-SWAP for 2026-10-19 UTC:
    /// lines 1-200 (from the day's first snapshot) and 75,005-75,060 (the
    /// snapshot of 00:15 and what follows), unchanged.
    const SAMPLE: &[u8] =
        include_bytes!("testdata/BTC-USDT-SWAP-L2orderbook-400lv-2026-10-19-sample.tar.gz");

    fn sample_quotes(from: u64, through: u64) -> BookQuotes {
        quotes_from_archive(
            SAMPLE,
            &btc_instrument(),
            UnixNanos::from(from),
            UnixNanos::from(through),
        )
        .unwrap()
    }

    const MS: u64 = 1_000_000;

    /// The stamp, bid, bid size, ask and ask size of a quote.
    fn row(quote: &QuoteTick) -> (u64, String, String, String, String) {
        (
            quote.ts_init.as_u64() / MS,
            quote.bid_price.to_string(),
            quote.bid_size.to_string(),
            quote.ask_price.to_string(),
            quote.ask_size.to_string(),
        )
    }

    fn rows(quotes: &[QuoteTick]) -> Vec<(u64, String, String, String, String)> {
        quotes.iter().map(row).collect()
    }

    fn r(
        ts: u64,
        bid: &str,
        bid_size: &str,
        ask: &str,
        ask_size: &str,
    ) -> (u64, String, String, String, String) {
        (ts, bid.into(), bid_size.into(), ask.into(), ask_size.into())
    }

    // Expected values computed independently from the same sample by a
    // plain replay of the levels (dictionaries per side, a snapshot
    // clearing both).

    #[test]
    fn the_sample_gives_one_quote_per_change_of_the_top() {
        let made = sample_quotes(0, u64::MAX);
        assert_eq!(made.quotes.len(), 150);
        assert_eq!((made.crossed, made.one_sided), (0, 0));
        let all = rows(&made.quotes);
        assert_eq!(
            all[..3],
            [
                r(1_792_368_000_008, "82069.5", "337.18", "82069.6", "1644.01"),
                r(1_792_368_000_018, "82069.5", "252.27", "82069.6", "1647.06"),
                r(1_792_368_000_028, "82069.5", "289.47", "82069.6", "1647.06"),
            ]
        );
        assert_eq!(
            all[147..],
            [
                r(1_792_368_900_670, "82093.3", "2186.33", "82093.4", "9.27"),
                r(1_792_368_900_710, "82093.3", "2186.29", "82093.4", "9.27"),
                r(1_792_368_900_730, "82093.3", "2186.53", "82093.4", "9.27"),
            ]
        );
        for pair in made.quotes.windows(2) {
            assert!(pair[0].ts_init < pair[1].ts_init);
            let (mut first, mut second) = (row(&pair[0]), row(&pair[1]));
            (first.0, second.0) = (0, 0);
            assert_ne!(first, second);
        }
    }

    #[test]
    fn quotes_carry_the_instrument_its_precisions_and_the_exchange_time() {
        let made = sample_quotes(0, u64::MAX);
        let quote = made.quotes[0];
        assert_eq!(
            quote.instrument_id,
            nautilus_model::identifiers::InstrumentId::from("BTC-USDT-SWAP.OKX")
        );
        assert_eq!(
            (quote.bid_price.precision, quote.ask_price.precision),
            (1, 1)
        );
        assert_eq!((quote.bid_size.precision, quote.ask_size.precision), (2, 2));
        assert_eq!(quote.ts_event, quote.ts_init);
        assert_eq!(quote.ts_init.as_u64(), 1_792_368_000_008 * MS);
    }

    #[test]
    fn only_quotes_from_from_up_to_through_are_given_the_first_one_whatever_the_change() {
        let made = sample_quotes(1_792_368_000_998 * MS + 1, u64::MAX);
        assert_eq!(made.quotes.len(), 81);
        assert_eq!(
            row(&made.quotes[0]),
            r(1_792_368_001_008, "82063.5", "76.14", "82063.6", "1790.69")
        );
        let made = sample_quotes(0, 1_792_368_000_028 * MS);
        assert_eq!(made.quotes.len(), 3);
        assert_eq!(made.quotes[2].ts_init.as_u64(), 1_792_368_000_028 * MS);
    }

    #[test]
    fn a_line_stamped_at_from_gives_the_first_quote() {
        let made = sample_quotes(1_792_368_001_008 * MS, u64::MAX);
        assert_eq!(made.quotes.len(), 81);
        assert_eq!(
            row(&made.quotes[0]),
            r(1_792_368_001_008, "82063.5", "76.14", "82063.6", "1790.69")
        );
    }

    fn lines(text: &str) -> BookQuotes {
        quotes_from_book(
            text.as_bytes(),
            &btc_instrument(),
            UnixNanos::default(),
            UnixNanos::from(u64::MAX),
        )
        .unwrap()
    }

    const SNAPSHOT: &str = r#"{"instId":"BTC-USDT-SWAP","action":"snapshot","ts":"1000","asks":[["100.1","2","1"],["100.2","3","1"]],"bids":[["100","1","1"],["99.9","4","1"]]}"#;

    fn update(ts: u64, asks: &str, bids: &str) -> String {
        format!(
            r#"{{"instId":"BTC-USDT-SWAP","action":"update","ts":"{ts}","asks":[{asks}],"bids":[{bids}]}}"#
        )
    }

    #[test]
    fn a_change_below_the_top_gives_no_quote_and_one_at_the_top_does() {
        let text = [
            SNAPSHOT.to_string(),
            update(1001, r#"["100.2","5","1"]"#, ""),
            update(1002, "", r#"["100","1.5","1"]"#),
            update(1003, r#"["100.1","0","0"]"#, ""),
        ]
        .join("\n");
        let made = lines(&text);
        assert_eq!(
            rows(&made.quotes),
            [
                r(1000, "100.0", "1.00", "100.1", "2.00"),
                r(1002, "100.0", "1.50", "100.1", "2.00"),
                r(1003, "100.0", "1.50", "100.2", "5.00"),
            ]
        );
    }

    #[test]
    fn a_snapshot_resets_the_book_and_an_unchanged_top_gives_no_quote() {
        let later = |ts: &str| {
            SNAPSHOT
                .replace(r#""ts":"1000""#, &format!(r#""ts":"{ts}""#))
                .replace(r#"["99.9","4","1"]"#, r#"["99.8","7","1"]"#)
        };
        let text = [
            SNAPSHOT.to_string(),
            update(1001, "", r#"["100","0","0"]"#),
            later("2000"),
            later("2500"),
            update(3000, "", r#"["100","0","0"]"#),
        ]
        .join("\n");
        let made = lines(&text);
        // After the second snapshot the bid below 100 is 99.8, not the
        // 99.9 it dropped; the third changes nothing.
        assert_eq!(
            rows(&made.quotes),
            [
                r(1000, "100.0", "1.00", "100.1", "2.00"),
                r(1001, "99.9", "4.00", "100.1", "2.00"),
                r(2000, "100.0", "1.00", "100.1", "2.00"),
                r(3000, "99.8", "7.00", "100.1", "2.00"),
            ]
        );
    }

    #[test]
    fn crossed_and_one_sided_tops_are_dropped_and_counted_and_a_locked_one_kept() {
        let text = [
            SNAPSHOT.to_string(),
            update(1001, "", r#"["100.1","1","1"]"#),
            update(1002, "", r#"["100.1","0","0"],["100.2","1","1"]"#),
            update(
                1003,
                "",
                r#"["100.2","0","0"],["100","0","0"],["99.9","0","0"]"#,
            ),
            update(1004, "", r#"["100","2","1"]"#),
        ]
        .join("\n");
        let made = lines(&text);
        assert_eq!(
            rows(&made.quotes),
            [
                r(1000, "100.0", "1.00", "100.1", "2.00"),
                r(1001, "100.1", "1.00", "100.1", "2.00"),
                r(1004, "100.0", "2.00", "100.1", "2.00"),
            ]
        );
        assert_eq!((made.crossed, made.one_sided), (1, 1));
    }

    #[test]
    fn a_line_without_levels_changes_nothing() {
        let text = [SNAPSHOT.to_string(), update(1001, "", "")].join("\n");
        assert_eq!(lines(&text).quotes.len(), 1);
    }

    /// A sample of the X-Perp chain's archive file for 2026-10-19 UTC:
    /// lines 1-200 (from the day's first snapshot) and 25,521-25,560 (the
    /// snapshot of 00:15 and what follows) of its one contract,
    /// BTC-USD_UM_XPERP-310404, unchanged, under the file's own name.
    const XPERP_SAMPLE: &[u8] = include_bytes!(
        "testdata/BTC-USD_UM_XPERP-futureschain-L2orderbook-400lv-2026-10-19-sample.tar.gz"
    );

    #[test]
    fn the_x_perp_sample_gives_one_quote_per_change_of_its_contracts_top() {
        let made = quotes_from_archive(
            XPERP_SAMPLE,
            &xperp_instrument(),
            UnixNanos::default(),
            UnixNanos::from(u64::MAX),
        )
        .unwrap();
        assert_eq!(made.quotes.len(), 145);
        assert_eq!((made.crossed, made.one_sided), (0, 0));
        let all = rows(&made.quotes);
        assert_eq!(
            all[..3],
            [
                r(1_792_368_000_004, "82057.1", "4236", "82057.2", "8979"),
                r(1_792_368_000_014, "82057.1", "1", "82057.2", "16950"),
                r(1_792_368_000_164, "82046.7", "6152", "82046.8", "6753"),
            ]
        );
        assert_eq!(
            all[142..],
            [
                r(1_792_368_901_344, "82094.7", "6721", "82094.8", "1572"),
                r(1_792_368_901_354, "82094.7", "6734", "82094.8", "1572"),
                r(1_792_368_901_374, "82094.7", "6734", "82094.8", "1420"),
            ]
        );
        assert_eq!(
            made.quotes[0].instrument_id,
            nautilus_model::identifiers::InstrumentId::from("BTC-USD_UM_XPERP-310404.OKX")
        );
    }

    /// A `.tar.gz` holding `files` (name, lines) in order, as a chain's
    /// archive file holds one file per contract.
    fn chain(files: &[(&str, &str)]) -> Vec<u8> {
        let mut builder = tar::Builder::new(flate2::write::GzEncoder::new(
            Vec::new(),
            flate2::Compression::fast(),
        ));
        for (name, lines) in files {
            let mut header = tar::Header::new_gnu();
            header.set_size(lines.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, name, lines.as_bytes())
                .unwrap();
        }
        builder.into_inner().unwrap().finish().unwrap()
    }

    #[test]
    fn of_a_chain_only_the_file_of_the_instruments_contract_is_read() {
        let other = SNAPSHOT
            .replace("BTC-USDT-SWAP", "BTC-USDT-261030")
            .replace("100.1", "200.1");
        let archive = chain(&[
            ("BTC-USDT-261030-L2orderbook-400lv-2026-10-19.data", &other),
            ("BTC-USDT-SWAP-L2orderbook-400lv-2026-10-19.data", SNAPSHOT),
        ]);
        let made = quotes_from_archive(
            archive.as_slice(),
            &btc_instrument(),
            UnixNanos::default(),
            UnixNanos::from(u64::MAX),
        )
        .unwrap();
        assert_eq!(
            rows(&made.quotes),
            [r(1000, "100.0", "1.00", "100.1", "2.00")]
        );
    }

    #[test]
    fn a_chain_without_the_instruments_contract_is_an_error_naming_it() {
        let other = SNAPSHOT.replace("BTC-USDT-SWAP", "BTC-USDT-261030");
        let archive = chain(&[("BTC-USDT-261030-L2orderbook-400lv-2026-10-19.data", &other)]);
        let err = quotes_from_archive(
            archive.as_slice(),
            &btc_instrument(),
            UnixNanos::default(),
            UnixNanos::from(u64::MAX),
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("BTC-USDT-SWAP.OKX"), "{err:#}");
    }

    #[test]
    fn a_line_of_another_contract_is_an_error_naming_it() {
        let other =
            update(1001, r#"["100.2","5","1"]"#, "").replace("BTC-USDT-SWAP", "BTC-USDT-261030");
        let text = [SNAPSHOT, &other].join("\n");
        let err = quotes_from_book(
            text.as_bytes(),
            &btc_instrument(),
            UnixNanos::default(),
            UnixNanos::from(u64::MAX),
        )
        .unwrap_err();
        let err = format!("{err:#}");
        assert!(
            err.contains("line 2") && err.contains("BTC-USDT-261030"),
            "{err}"
        );
    }

    #[test]
    fn a_bad_line_is_an_error_naming_it() {
        let text = [SNAPSHOT, "{not json"].join("\n");
        let err = quotes_from_book(
            text.as_bytes(),
            &btc_instrument(),
            UnixNanos::default(),
            UnixNanos::from(u64::MAX),
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("line 2"), "{err:#}");
    }
}
