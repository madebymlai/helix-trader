//! An export's Parquet file into Nautilus data, at the instrument's
//! precision: LSE sends prices and sizes as doubles, each converted once
//! here through its shortest decimal form, which must fit the precision.

use std::{collections::HashMap, str::FromStr};

use anyhow::{Context, bail, ensure};
use arrow::{
    array::{Array, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray},
    datatypes::{DataType, TimeUnit},
};
use bytes::Bytes;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::TradeTick,
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
        let (times, prices, sizes, kinds) = tick_columns(&batch)?;
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

/// The columns of a tick slice, LSE's types checked.
fn tick_columns(
    batch: &RecordBatch,
) -> anyhow::Result<(
    &TimestampNanosecondArray,
    &Float64Array,
    &Float64Array,
    &StringArray,
)> {
    fn column<'a, A: Array + 'static>(batch: &'a RecordBatch, name: &str) -> anyhow::Result<&'a A> {
        let column = batch
            .column_by_name(name)
            .with_context(|| format!("no column {name} in LSE's file"))?;
        ensure!(
            column.null_count() == 0,
            "the column {name} of LSE's file has empty rows"
        );
        column
            .as_any()
            .downcast_ref::<A>()
            .with_context(|| format!("the column {name} of LSE's file is {}", column.data_type()))
    }
    let times = column::<TimestampNanosecondArray>(batch, "time")?;
    match times.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, Some(zone)) if zone.as_ref() == "UTC" => {}
        other => bail!("the times of LSE's file are {other}, not UTC nanoseconds"),
    }
    Ok((
        times,
        column(batch, "price")?,
        column(batch, "size")?,
        column(batch, "kind")?,
    ))
}

/// `value`, a double, as the decimal it is written as at its shortest,
/// which must have at most `precision` decimals.
pub(crate) fn decimal(value: f64, precision: u8, what: &str) -> anyhow::Result<Decimal> {
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
    use std::collections::HashSet;

    use super::*;
    use crate::testing::GOLD_TRADES;

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
}
