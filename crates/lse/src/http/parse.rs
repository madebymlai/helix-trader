//! An export's Parquet file into Nautilus data: trades, quotes and bars at
//! the instrument's precision, LSE sending prices and sizes as doubles, each
//! converted once here through its shortest decimal form, which must fit
//! the precision; and the rows of a reference collection as `LseRow`s.

use std::{collections::HashMap, str::FromStr};

use anyhow::{Context, bail, ensure};
use arrow::{
    array::{Array, AsArray, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray},
    compute::cast,
    datatypes::{DataType, TimeUnit, TimestampNanosecondType},
    json::{WriterBuilder, writer::JsonArray},
};
use bytes::Bytes;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, QuoteTick, TradeTick},
    enums::AggressorSide,
    identifiers::{InstrumentId, TradeId},
    types::{Price, Quantity},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rust_decimal::Decimal;
use serde_json::{Map, Value};

use super::models::RowKey;
use crate::types::LseRow;

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
            let time = UnixNanos::from(time(&times, row)?);
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

/// The bars of a file of candles (`time, open, high, low, close, volume,
/// trades`), each candle, which LSE stamps at its open, a bar of
/// `bar_type` stamped at its close, its open and the bar's width (Nautilus'
/// `BarSpecification::timedelta`), both `ts_event` and `ts_init`, as
/// history is.
///
/// # Errors
///
/// Returns an error if the file holds no candles, if a price or a volume
/// has more decimals than `price_precision` or `size_precision`, the
/// instrument's, if a volume is negative, or if a candle's prices are not
/// a bar's (its high below another price, its low above one).
pub(crate) fn parse_candles(
    file: Bytes,
    bar_type: BarType,
    price_precision: u8,
    size_precision: u8,
) -> anyhow::Result<Vec<Bar>> {
    let width = u64::try_from(bar_type.spec().timedelta().as_nanos())
        .with_context(|| format!("the width of {bar_type}"))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .context("reading LSE's Parquet file")?
        .build()?;
    let mut bars = Vec::new();
    for batch in reader {
        let batch = batch.context("reading LSE's Parquet file")?;
        let times = times(&batch)?;
        let open = column::<Float64Array>(&batch, "open")?;
        let high = column::<Float64Array>(&batch, "high")?;
        let low = column::<Float64Array>(&batch, "low")?;
        let close = column::<Float64Array>(&batch, "close")?;
        let volume = column::<Float64Array>(&batch, "volume")?;
        for row in 0..batch.num_rows() {
            let price = |prices: &Float64Array, what| -> anyhow::Result<Price> {
                Ok(Price::from_decimal_dp(
                    decimal(prices.value(row), price_precision, what)?,
                    price_precision,
                )?)
            };
            let closed = UnixNanos::from(
                time(&times, row)?
                    .checked_add(width)
                    .context("a candle closing past 2554")?,
            );
            bars.push(Bar::new_checked(
                bar_type,
                price(open, "open")?,
                price(high, "high")?,
                price(low, "low")?,
                price(close, "close")?,
                Quantity::from_decimal_dp(
                    decimal(volume.value(row), size_precision, "volume")?,
                    size_precision,
                )?,
                closed,
                closed,
            )?);
        }
    }
    Ok(bars)
}

/// The rows of a reference export of `symbol` in LSE's collection
/// `source`, each stamped when its values are known, both `ts_event` and
/// `ts_init`: its value of `time_column`, the column LSE names its time,
/// and the span of `key`, the collection's (`RowKey::span`), a date at its
/// end in UTC, an interval at its close. Every other column but `symbol` is
/// a field of the row, as LSE sent it, in JSON (Arrow's): numbers, strings,
/// `null`, and dates and times in ISO 8601.
///
/// # Errors
///
/// Returns an error if the file is not Parquet, or if its time column is
/// missing, holds an empty row or a time before 1970, or is not of `key`:
/// dates for a collection keyed by date, times for one of intervals.
pub(crate) fn parse_rows(
    file: Bytes,
    source: &str,
    symbol: &str,
    time_column: &str,
    key: RowKey,
) -> anyhow::Result<Vec<LseRow>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .context("reading LSE's Parquet file")?
        .build()?;
    let span = u64::try_from(key.span().as_nanos()).expect("a positive span");
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.context("reading LSE's Parquet file")?;
        let times = row_times(&batch, time_column, key)?;
        let mut writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, JsonArray>(Vec::new());
        writer.write(&batch)?;
        writer.finish()?;
        let objects: Vec<Map<String, Value>> =
            serde_json::from_slice(&writer.into_inner()).context("reading LSE's rows as JSON")?;
        for (mut fields, time) in objects.into_iter().zip(times) {
            fields.remove(time_column);
            fields.remove("symbol");
            let known = UnixNanos::from(
                time.as_u64()
                    .checked_add(span)
                    .context("a row known past 2554")?,
            );
            rows.push(LseRow::new(
                source.to_owned(),
                symbol.to_owned(),
                fields,
                known,
                known,
            ));
        }
    }
    Ok(rows)
}

/// The times of the rows of `batch`, its column `time_column`'s, in UTC:
/// a date at its start, a time as it is, the column's type checked
/// against `key`.
fn row_times(
    batch: &RecordBatch,
    time_column: &str,
    key: RowKey,
) -> anyhow::Result<Vec<UnixNanos>> {
    let column = batch
        .column_by_name(time_column)
        .with_context(|| format!("no time column {time_column} in LSE's file"))?;
    match (key, column.data_type()) {
        (RowKey::Date, DataType::Date32 | DataType::Date64)
        | (RowKey::Open(_), DataType::Timestamp(_, _)) => {}
        (key, other) => bail!(
            "the time column {time_column} of LSE's file is {other}, not the {} its collection keys its rows by",
            match key {
                RowKey::Date => "dates",
                RowKey::Open(_) => "times",
            }
        ),
    }
    let nanos = cast(
        column,
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
    )?;
    ensure!(
        nanos.null_count() == 0,
        "the time column {time_column} of LSE's file has empty rows, or times past 2262"
    );
    let nanos = nanos.as_primitive::<TimestampNanosecondType>();
    (0..nanos.len())
        .map(|row| time(nanos, row).map(UnixNanos::from))
        .collect()
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

/// The `time` column of `batch` in nanoseconds: UTC timestamps, which LSE
/// gives in nanoseconds in a tick slice and in milliseconds in candles.
fn times(batch: &RecordBatch) -> anyhow::Result<TimestampNanosecondArray> {
    let column = batch
        .column_by_name("time")
        .context("no column time in LSE's file")?;
    match column.data_type() {
        DataType::Timestamp(_, Some(zone)) if zone.as_ref() == "UTC" => {}
        other => bail!("the times of LSE's file are {other}, not UTC timestamps"),
    }
    let nanos = cast(
        column,
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
    )?;
    ensure!(
        nanos.null_count() == 0,
        "the column time of LSE's file has empty rows, or times past 2262"
    );
    Ok(nanos.as_primitive::<TimestampNanosecondType>().clone())
}

/// The time of `row` in nanoseconds from 1970.
fn time(times: &TimestampNanosecondArray, row: usize) -> anyhow::Result<u64> {
    u64::try_from(times.value(row)).context("a time before 1970")
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
    use std::{collections::HashSet, sync::Arc};

    use arrow::datatypes::{Field, Schema};
    use parquet::arrow::ArrowWriter;
    use rstest::rstest;

    use super::*;
    use jiff::SignedDuration;

    use crate::testing::{
        GCZ26_QUOTES, GCZ26_TRADES, GOLD_FUTURES_4H, GOLD_SPOT_MINUTES, GOLD_TRADES,
        US10Y_BOND_YIELDS, US10Y_BOND_YIELDS_FOUR_HOURS,
    };

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

    #[rstest]
    #[case::malformed(b"not a Parquet file")]
    #[case::short(&GOLD_TRADES[..GOLD_TRADES.len() / 2])]
    fn a_file_that_is_no_whole_parquet_file_is_refused(#[case] file: &'static [u8]) {
        let err = parse_trade_slice(Bytes::from_static(file), gold(), 1, 0).unwrap_err();

        assert!(
            err.to_string().contains("reading LSE's Parquet file"),
            "{err:#}"
        );
    }

    /// The captured rows of US10Y, as pyarrow reads LSE's file.
    fn us10y_rows() -> Vec<LseRow> {
        parse_rows(
            Bytes::from_static(US10Y_BOND_YIELDS),
            "bond_yields",
            "US10Y",
            "date",
            RowKey::Date,
        )
        .unwrap()
    }

    fn day(date: &str) -> UnixNanos {
        nov(&format!("{date}T00:00:00"))
    }

    fn nov(time: &str) -> UnixNanos {
        UnixNanos::from(
            format!("2026-11-{time}Z")
                .parse::<jiff::Timestamp>()
                .unwrap(),
        )
    }

    /// The day's close is known when the day ends: the 9th's row at the
    /// 10th's 00:00 UTC, and the 12th's at the 13th's.
    #[rstest]
    fn a_row_of_a_date_is_stamped_at_the_date_s_end() {
        let rows: Vec<_> = us10y_rows()
            .into_iter()
            .map(|row| {
                (
                    row.ts_event,
                    row.ts_init,
                    row.source,
                    row.symbol,
                    row.fields["close"].clone(),
                )
            })
            .collect();

        let row = |stamped, close: f64| {
            (
                day(stamped),
                day(stamped),
                "bond_yields".to_string(),
                "US10Y".to_string(),
                serde_json::json!(close),
            )
        };
        assert_eq!(rows, [row("10", 4.884), row("11", 5.0), row("13", 5.133)]);
    }

    /// The 4-hour rows of the 10th, which LSE stamps at their open, 00:00
    /// to 20:00: each stamped at its close, 04:00 to the 11th's 00:00, as
    /// pyarrow reads LSE's file.
    #[rstest]
    fn a_row_of_an_interval_is_stamped_at_its_close() {
        let rows: Vec<_> = parse_rows(
            Bytes::from_static(US10Y_BOND_YIELDS_FOUR_HOURS),
            "bond_yields_four_hour",
            "US10Y",
            "ts",
            RowKey::Open(SignedDuration::from_hours(4)),
        )
        .unwrap()
        .into_iter()
        .map(|row| (row.ts_event, row.ts_init, row.fields["close"].clone()))
        .collect();

        let row =
            |close_at: &str, close: f64| (nov(close_at), nov(close_at), serde_json::json!(close));
        assert_eq!(
            rows,
            [
                row("10T04:00:00", 4.868),
                row("10T08:00:00", 4.868),
                row("10T12:00:00", 4.892),
                row("10T16:00:00", 4.988),
                row("10T20:00:00", 4.983),
                row("11T00:00:00", 5.0),
            ]
        );
    }

    /// Its stamp would not be when its values are known.
    #[rstest]
    #[case::dates_as_intervals(
        US10Y_BOND_YIELDS,
        "date",
        RowKey::Open(SignedDuration::from_hours(4))
    )]
    #[case::times_as_dates(US10Y_BOND_YIELDS_FOUR_HOURS, "ts", RowKey::Date)]
    fn a_time_column_not_of_the_key_of_its_collection_is_refused_naming_it(
        #[case] file: &'static [u8],
        #[case] time_column: &str,
        #[case] key: RowKey,
    ) {
        let err = parse_rows(
            Bytes::from_static(file),
            "bond_yields",
            "US10Y",
            time_column,
            key,
        )
        .unwrap_err();

        assert!(err.to_string().contains(time_column), "{err:#}");
    }

    /// fetched it, a time in UTC.
    #[rstest]
    fn a_row_s_fields_are_its_other_columns_as_lse_sent_them() {
        let fields: Vec<_> = us10y_rows()
            .into_iter()
            .map(|row| serde_json::Value::Object(row.fields))
            .collect();

        let row = |id: u32, [open, high, low, close]: [f64; 4]| {
            serde_json::json!({
                "id": id,
                "country_iso2": "US",
                "country_name": "United States",
                "maturity": "10Y",
                "maturity_days": 3650,
                "currency": "USD",
                "open": open,
                "high": high,
                "low": low,
                "close": close,
                "fetched_at": "2026-11-13T01:34:04Z",
            })
        };
        assert_eq!(
            fields,
            [
                row(2_606_679, [4.864, 4.888, 4.831, 4.884]),
                row(2_606_680, [4.876, 5.021, 4.855, 5.0]),
                row(2_606_681, [4.988, 5.137, 4.98, 5.133]),
            ]
        );
    }

    #[rstest]
    fn a_time_column_the_file_lacks_is_refused_naming_it() {
        let err = parse_rows(
            Bytes::from_static(US10Y_BOND_YIELDS),
            "bond_yields",
            "US10Y",
            "ts",
            RowKey::Date,
        )
        .unwrap_err();

        assert!(err.to_string().contains("ts"), "{err:#}");
    }

    /// A definition too coarse for the prices LSE sends: refused, not
    /// rounded.
    #[test]
    fn a_price_finer_than_the_instrument_s_precision_is_refused() {
        let err = parse_trade_slice(Bytes::from_static(GOLD_TRADES), gold(), 0, 0).unwrap_err();

        assert!(err.to_string().contains("4136.4"), "{err:#}");
    }

    /// The captured four hours, as pyarrow reads the file: candles LSE
    /// stamps at 00:00, 04:00 and 08:00, their open.
    #[test]
    fn candles_are_bars_stamped_at_their_close_at_the_instrument_s_precision() {
        let bar_type = BarType::from("GOLD-FUTURES.LSEDGE-4-HOUR-LAST-EXTERNAL");

        let bars = parse_candles(Bytes::from_static(GOLD_FUTURES_4H), bar_type, 1, 0).unwrap();

        let rows: Vec<_> = bars
            .iter()
            .map(|b| {
                (
                    b.bar_type, b.ts_event, b.ts_init, b.open, b.high, b.low, b.close, b.volume,
                )
            })
            .collect();
        let row = |close, [open, high, low, last]: [&str; 4], volume| {
            (
                bar_type,
                at(close),
                at(close),
                Price::from(open),
                Price::from(high),
                Price::from(low),
                Price::from(last),
                Quantity::from(volume),
            )
        };
        assert_eq!(
            rows,
            [
                row("04:00:00", ["4132.1", "4143.8", "4124.1", "4135.9"], 13191),
                row("08:00:00", ["4135.6", "4147.8", "4128.3", "4142.2"], 12585),
                row("12:00:00", ["4142.1", "4160.0", "4129.4", "4158.7"], 15369),
            ]
        );
    }

    /// A malformed input: a tick slice, a real file of LSE's, read as
    /// candles.
    #[test]
    fn a_file_of_no_candles_is_refused_naming_the_column_it_lacks() {
        let bar_type = BarType::from("GOLD-FUTURES.LSEDGE-1-MINUTE-LAST-EXTERNAL");

        let err = parse_candles(Bytes::from_static(GOLD_TRADES), bar_type, 1, 0).unwrap_err();

        assert!(err.to_string().contains("no column open"), "{err:#}");
    }

    #[test]
    fn a_candle_price_finer_than_the_instrument_s_precision_is_refused() {
        let bar_type = BarType::from("XAU/USD-COMMODITIES.LSEDGE-1-MINUTE-LAST-EXTERNAL");

        let err = parse_candles(Bytes::from_static(GOLD_SPOT_MINUTES), bar_type, 1, 0).unwrap_err();

        assert!(err.to_string().contains("4118.01"), "{err:#}");
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
