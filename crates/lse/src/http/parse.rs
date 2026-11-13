//! An export's Parquet file into Nautilus data, trades or quotes, at the
//! instrument's precision: LSE sends prices and sizes as doubles, each
//! converted once here through its shortest decimal form, which must fit
//! the precision.

use std::{collections::HashMap, str::FromStr};

use anyhow::{Context, bail, ensure};
use arrow::{
    array::{Array, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray},
    datatypes::{DataType, TimeUnit},
};
use bytes::Bytes;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{QuoteTick, TradeTick},
    enums::AggressorSide,
    identifiers::{InstrumentId, TradeId},
    types::{Price, Quantity},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rust_decimal::Decimal;

/// The kind of every row of a tick slice: a trade.
const TRADE: &str = "trade";

/// The trades of a tick slice (`time, price, size, kind`), each stamped at
/// its time, both `ts_event` and `ts_init`, as history is, with no
/// aggressor side, LSE sending none, and a trade id of its own (`trade_id`).
///
/// # Errors
///
/// Returns an error if the file is no tick slice, if a row is not a trade,
/// or if a price or a size has more decimals than `price_precision` or
/// `size_precision`, the instrument's, or is not positive.
pub(crate) fn parse_trade_slice(
    file: Bytes,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
) -> anyhow::Result<Vec<TradeTick>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .context("reading LSE's Parquet file")?
        .build()?;
    let mut trades = Vec::new();
    // The rows met so far at the time of the last row, by price and size.
    let mut same: HashMap<(Price, Quantity), u32> = HashMap::new();
    let mut last_time = None;
    for batch in reader {
        let batch = batch.context("reading LSE's Parquet file")?;
        let times = times(&batch)?;
        let prices = column::<Float64Array>(&batch, "price")?;
        let sizes = column::<Float64Array>(&batch, "size")?;
        let kinds = column::<StringArray>(&batch, "kind")?;
        for row in 0..batch.num_rows() {
            let kind = kinds.value(row);
            ensure!(kind == TRADE, "a row of kind {kind:?}, not a {TRADE}");
            let time =
                UnixNanos::from(u64::try_from(times.value(row)).context("a time before 1970")?);
            let price = Price::from_decimal_dp(
                decimal(prices.value(row), price_precision, "price")?,
                price_precision,
            )?;
            let size = Quantity::from_decimal_dp(
                decimal(sizes.value(row), size_precision, "size")?,
                size_precision,
            )?;
            if last_time != Some(time) {
                same.clear();
                last_time = Some(time);
            }
            let occurrence = same.entry((price, size)).or_default();
            let trade_id = trade_id(instrument_id, time, price, size, *occurrence);
            *occurrence += 1;
            trades.push(TradeTick::new_checked(
                instrument_id,
                price,
                size,
                AggressorSide::NoAggressor,
                trade_id,
                time,
                time,
            )?);
        }
    }
    Ok(trades)
}

/// The quotes of a file of an exchange market's quotes (`time, bid_price,
/// ask_price, bid_size, ask_size`), each stamped at its time, both
/// `ts_event` and `ts_init`, as history is, with the sizes LSE sends. A
/// quote with an empty side, a price LSE leaves empty, gives none, as
/// Nautilus' Databento decoder gives no quote of a record with an undefined
/// side (`decode_mbp1_msg`).
///
/// # Errors
///
/// Returns an error if the file holds no quotes, or if a price or a size
/// has more decimals than `price_precision` or `size_precision`, the
/// instrument's.
pub(crate) fn parse_quotes(
    file: Bytes,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
) -> anyhow::Result<Vec<QuoteTick>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .context("reading LSE's Parquet file")?
        .build()?;
    let mut quotes = Vec::new();
    let mut one_sided = 0_usize;
    for batch in reader {
        let batch = batch.context("reading LSE's Parquet file")?;
        let times = times(&batch)?;
        let bid_prices = nullable_column::<Float64Array>(&batch, "bid_price")?;
        let ask_prices = nullable_column::<Float64Array>(&batch, "ask_price")?;
        let bid_sizes = column::<Float64Array>(&batch, "bid_size")?;
        let ask_sizes = column::<Float64Array>(&batch, "ask_size")?;
        for row in 0..batch.num_rows() {
            if bid_prices.is_null(row) || ask_prices.is_null(row) {
                one_sided += 1;
                continue;
            }
            let price = |prices: &Float64Array, what| -> anyhow::Result<Price> {
                Ok(Price::from_decimal_dp(
                    decimal(prices.value(row), price_precision, what)?,
                    price_precision,
                )?)
            };
            let size = |sizes: &Float64Array, what| -> anyhow::Result<Quantity> {
                Ok(Quantity::from_decimal_dp(
                    decimal(sizes.value(row), size_precision, what)?,
                    size_precision,
                )?)
            };
            let time =
                UnixNanos::from(u64::try_from(times.value(row)).context("a time before 1970")?);
            quotes.push(QuoteTick::new_checked(
                instrument_id,
                price(bid_prices, "bid price")?,
                price(ask_prices, "ask price")?,
                size(bid_sizes, "bid size")?,
                size(ask_sizes, "ask size")?,
                time,
                time,
            )?);
        }
    }
    if one_sided > 0 {
        log::debug!("{one_sided} quotes of {instrument_id} with an empty side: no quote of them");
    }
    Ok(quotes)
}

/// The `time` column of `batch`: UTC timestamps in nanoseconds, with no
/// empty row.
fn times(batch: &RecordBatch) -> anyhow::Result<&TimestampNanosecondArray> {
    let times = column::<TimestampNanosecondArray>(batch, "time")?;
    match times.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, Some(zone)) if zone.as_ref() == "UTC" => {}
        other => bail!("the times of LSE's file are {other}, not UTC nanoseconds"),
    }
    Ok(times)
}

/// The column `name` of `batch`, LSE's type `A` checked, with no empty row.
fn column<'a, A: Array + 'static>(batch: &'a RecordBatch, name: &str) -> anyhow::Result<&'a A> {
    let column = nullable_column::<A>(batch, name)?;
    ensure!(
        column.null_count() == 0,
        "the column {name} of LSE's file has empty rows"
    );
    Ok(column)
}

/// The column `name` of `batch`, LSE's type `A` checked, empty rows and all.
fn nullable_column<'a, A: Array + 'static>(
    batch: &'a RecordBatch,
    name: &str,
) -> anyhow::Result<&'a A> {
    let column = batch
        .column_by_name(name)
        .with_context(|| format!("no column {name} in LSE's file"))?;
    column
        .as_any()
        .downcast_ref::<A>()
        .with_context(|| format!("the column {name} of LSE's file is {}", column.data_type()))
}

/// `value`, a double, as the decimal it is written as at its shortest,
/// which must have at most `precision` decimals.
fn decimal(value: f64, precision: u8, what: &str) -> anyhow::Result<Decimal> {
    ensure!(value.is_finite(), "a {what} of {value}");
    let decimal = Decimal::from_str(&value.to_string())
        .with_context(|| format!("a {what} of {value}"))?
        .normalize();
    ensure!(
        decimal.scale() <= u32::from(precision),
        "a {what} of {value}, finer than the instrument's precision, {precision}"
    );
    Ok(decimal)
}

// FNV-1a 64-bit, as Nautilus' Databento adapter derives the trade ids of
// records that carry none.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0100_0000_01b3;

/// The id of a trade LSE gives none: a hash of its instrument, time,
/// price and size, and of how many trades of that time, price and size
/// came before it in the file, so that trades alike keep ids apart and the
/// same rows always get the same ids.
fn trade_id(
    instrument_id: InstrumentId,
    time: UnixNanos,
    price: Price,
    size: Quantity,
    occurrence: u32,
) -> TradeId {
    let mut hash = FNV_OFFSET_BASIS;
    let mut mix = |bytes: &[u8]| {
        for &byte in bytes {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        // Ends the field, so that no two fields run into one.
        hash ^= 0xff;
        hash = hash.wrapping_mul(FNV_PRIME);
    };
    mix(instrument_id.to_string().as_bytes());
    mix(&time.as_u64().to_le_bytes());
    mix(&price.raw().to_le_bytes());
    mix(&size.raw().to_le_bytes());
    mix(&occurrence.to_le_bytes());
    TradeId::new(format!("{hash:016x}"))
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use arrow::datatypes::{Field, Schema};
    use parquet::arrow::ArrowWriter;

    use super::*;
    use crate::testing::{GCZ26_QUOTES, GCZ26_TRADES, GOLD_TRADES};

    fn gold() -> InstrumentId {
        InstrumentId::from("GOLD-FUTURES.LSEDGE")
    }

    fn at(time: &str) -> UnixNanos {
        UnixNanos::from(
            format!("2026-10-21T{time}Z")
                .parse::<jiff::Timestamp>()
                .unwrap(),
        )
    }

    /// The captured second, as pyarrow reads the file: two trades alike at
    /// .619501, three at .690600 at three prices, one at .901218.
    #[test]
    fn a_tick_slice_is_its_trades_at_the_instrument_s_precision_with_no_aggressor() {
        let trades = parse_trade_slice(Bytes::from_static(GOLD_TRADES), gold(), 1, 0).unwrap();

        let rows: Vec<_> = trades
            .iter()
            .map(|t| (t.ts_event, t.ts_init, t.price, t.size, t.aggressor_side))
            .collect();
        let row = |time, price, size| {
            (
                at(time),
                at(time),
                Price::from(price),
                Quantity::from(size),
                AggressorSide::NoAggressor,
            )
        };
        assert_eq!(
            rows,
            [
                row("10:00:00.619501", "4136.4", 1),
                row("10:00:00.619501", "4136.4", 1),
                row("10:00:00.690600", "4136.3", 1),
                row("10:00:00.690600", "4136.2", 2),
                row("10:00:00.690600", "4136.1", 2),
                row("10:00:00.901218", "4136.2", 1),
            ]
        );
        assert!(trades.iter().all(|t| t.instrument_id == gold()));
    }

    #[test]
    fn every_trade_gets_an_id_of_its_own_and_the_same_one_each_time() {
        let parse = || parse_trade_slice(Bytes::from_static(GOLD_TRADES), gold(), 1, 0).unwrap();
        let ids: Vec<TradeId> = parse().iter().map(|t| t.trade_id).collect();

        let again: Vec<TradeId> = parse().iter().map(|t| t.trade_id).collect();

        assert_eq!(ids, again);
        assert_eq!(
            ids.iter().collect::<HashSet<_>>().len(),
            ids.len(),
            "{ids:?}"
        );
    }

    /// A definition too coarse for the prices LSE sends: refused, not
    /// rounded.
    #[test]
    fn a_price_finer_than_the_instrument_s_precision_is_refused() {
        let err = parse_trade_slice(Bytes::from_static(GOLD_TRADES), gold(), 0, 0).unwrap_err();

        assert!(err.to_string().contains("4136.4"), "{err:#}");
    }

    fn gcz26() -> InstrumentId {
        InstrumentId::from("GCZ26.GLBX")
    }

    /// A time of 12 November 2026, as nanoseconds from 1970.
    fn on_the_12th(nanos: u64) -> UnixNanos {
        UnixNanos::from(nanos)
    }

    /// The captured second of GCZ6's quotes, as pyarrow reads the file: 636
    /// quotes, its first and last two below, at Databento's precision of
    /// GCZ26.GLBX, two decimals.
    #[test]
    fn quotes_are_quote_ticks_with_lse_s_sizes_at_the_instrument_s_precision() {
        let quotes = parse_quotes(Bytes::from_static(GCZ26_QUOTES), gcz26(), 2, 0).unwrap();

        let row = |time, bid, ask, bid_size, ask_size| {
            (
                on_the_12th(time),
                on_the_12th(time),
                Price::from(bid),
                Price::from(ask),
                Quantity::from(bid_size),
                Quantity::from(ask_size),
            )
        };
        let rows: Vec<_> = quotes
            .iter()
            .map(|q| {
                (
                    q.ts_event,
                    q.ts_init,
                    q.bid_price,
                    q.ask_price,
                    q.bid_size,
                    q.ask_size,
                )
            })
            .collect();
        assert_eq!(rows.len(), 636);
        assert_eq!(
            rows[..2],
            [
                row(1_794_493_800_000_184_205, "3908.10", "3908.30", 2, 9),
                row(1_794_493_800_001_418_191, "3908.10", "3908.30", 1, 9),
            ]
        );
        assert_eq!(
            rows[634..],
            [
                row(1_794_493_800_998_844_133, "3908.90", "3909.20", 3, 2),
                row(1_794_493_800_998_982_251, "3908.90", "3909.20", 2, 2),
            ]
        );
        assert!(quotes.iter().all(|q| q.instrument_id == gcz26()));
        assert_eq!(quotes[0].bid_price.precision, 2);
    }

    /// The captured second of GCZ6's trades, LSE's `data=trades`, read as a
    /// tick slice is: 38 trades, three of them at one time and three prices.
    #[test]
    fn a_cme_contract_s_trades_are_read_as_a_tick_slice_is() {
        let trades = parse_trade_slice(Bytes::from_static(GCZ26_TRADES), gcz26(), 2, 0).unwrap();

        assert_eq!(trades.len(), 38);
        let row = |t: &TradeTick| (t.ts_event, t.price, t.size, t.aggressor_side);
        assert_eq!(
            row(&trades[0]),
            (
                on_the_12th(1_794_493_800_001_418_191),
                Price::from("3908.10"),
                Quantity::from(1),
                AggressorSide::NoAggressor
            )
        );
        assert_eq!(
            row(&trades[37]),
            (
                on_the_12th(1_794_493_800_985_967_727),
                Price::from("3909.00"),
                Quantity::from(1),
                AggressorSide::NoAggressor
            )
        );
        let ids: HashSet<_> = trades.iter().map(|t| t.trade_id).collect();
        assert_eq!(ids.len(), 38);
    }

    /// A quote with an empty side, which LSE's schema allows (its prices
    /// are nullable) and no capture holds: built here, not captured. As
    /// Nautilus' Databento decoder gives no quote of an MBP-1 record with
    /// an undefined side (`decode_mbp1_msg`), the row gives none.
    #[test]
    fn a_quote_with_an_empty_side_gives_no_quote_tick() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("bid_price", DataType::Float64, true),
            Field::new("ask_price", DataType::Float64, true),
            Field::new("bid_size", DataType::Float64, false),
            Field::new("ask_size", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![1, 2, 3]).with_timezone("UTC")),
                Arc::new(Float64Array::from(vec![Some(3908.1), None, Some(3908.2)])),
                Arc::new(Float64Array::from(vec![Some(3908.3), Some(3908.3), None])),
                Arc::new(Float64Array::from(vec![2.0, 0.0, 1.0])),
                Arc::new(Float64Array::from(vec![9.0, 9.0, 0.0])),
            ],
        )
        .unwrap();
        let mut file = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let quotes = parse_quotes(Bytes::from(file), gcz26(), 2, 0).unwrap();

        let times: Vec<_> = quotes.iter().map(|q| q.ts_event).collect();
        assert_eq!(times, [UnixNanos::from(1)]);
    }
}
