//! An export's Parquet file into Nautilus data: trades and bars at the
//! instrument's precision, LSE sending prices and sizes as doubles, each
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
    data::{Bar, BarType, TradeTick},
    enums::AggressorSide,
    identifiers::{InstrumentId, TradeId},
    types::{Price, Quantity},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rust_decimal::Decimal;
use serde_json::{Map, Value};

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
/// `source`, each stamped at its value of `time_column`, the column LSE
/// names its time, both `ts_event` and `ts_init`: a date at its start in
/// UTC, a time as it is. Every other column but `symbol` is a field of the
/// row, as LSE sent it, in JSON (Arrow's): numbers, strings, `null`, and
/// dates and times in ISO 8601.
///
/// # Errors
///
/// Returns an error if the file is not Parquet, or if its time column is
/// missing, holds an empty row or a time before 1970, or is not a date or
/// a time.
pub(crate) fn parse_rows(
    file: Bytes,
    source: &str,
    symbol: &str,
    time_column: &str,
) -> anyhow::Result<Vec<LseRow>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .context("reading LSE's Parquet file")?
        .build()?;
    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.context("reading LSE's Parquet file")?;
        let times = row_times(&batch, time_column)?;
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
            rows.push(LseRow::new(
                source.to_owned(),
                symbol.to_owned(),
                fields,
                time,
                time,
            ));
        }
    }
    Ok(rows)
}

/// The times of the rows of `batch`, its column `time_column`'s, in UTC:
/// a date at its start, a time as it is.
fn row_times(batch: &RecordBatch, time_column: &str) -> anyhow::Result<Vec<UnixNanos>> {
    let column = batch
        .column_by_name(time_column)
        .with_context(|| format!("no time column {time_column} in LSE's file"))?;
    match column.data_type() {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {}
        other => bail!("the time column {time_column} of LSE's file is {other}"),
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
    use std::collections::HashSet;

    use rstest::rstest;

    use super::*;
    use crate::testing::{GOLD_FUTURES_4H, GOLD_SPOT_MINUTES, GOLD_TRADES, US10Y_BOND_YIELDS};

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

    /// The captured rows of US10Y, as pyarrow reads LSE's file.
    fn us10y_rows() -> Vec<LseRow> {
        parse_rows(
            Bytes::from_static(US10Y_BOND_YIELDS),
            "bond_yields",
            "US10Y",
            "date",
        )
        .unwrap()
    }

    fn day(date: &str) -> UnixNanos {
        UnixNanos::from(
            format!("{date}T00:00:00Z")
                .parse::<jiff::Timestamp>()
                .unwrap(),
        )
    }

    /// A date is its day's start, in UTC, LSE's time.
    #[rstest]
    fn a_reference_export_is_its_rows_each_at_the_time_of_its_time_column() {
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

        let row = |date, close: f64| {
            (
                day(date),
                day(date),
                "bond_yields".to_string(),
                "US10Y".to_string(),
                serde_json::json!(close),
            )
        };
        assert_eq!(
            rows,
            [
                row("2026-11-09", 4.884),
                row("2026-11-10", 5.0),
                row("2026-11-12", 5.133),
            ]
        );
    }

    /// As pyarrow reads them: LSE's row id, the yield's country, maturity
    /// and currency, its day's open, high, low and close, and when LSE
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
}
