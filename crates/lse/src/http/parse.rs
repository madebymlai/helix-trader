//! Turns an export's Parquet file into Nautilus data: trades, quotes, book depth snapshots, book
//! deltas and bars at the instrument's precision.
//!
//! LSE sends prices and sizes as doubles. They are converted once through their shortest decimal
//! form, which must fit the precision. History is stamped at its time, as both `ts_event` and
//! `ts_init`. The rows of a reference collection or a macro series become `LseRow`s, stamped when
//! their values are known.

use std::{collections::HashMap, str::FromStr};

use anyhow::Context;
use arrow::{
    array::{
        Array, ArrayRef, AsArray, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray,
    },
    compute::{CastOptions, cast, cast_with_options},
    datatypes::{DataType, TimeUnit, TimestampNanosecondType, UInt32Type, UInt64Type},
    json::{WriterBuilder, writer::JsonArray},
};
use bytes::Bytes;
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{
        Bar, BarType, BookOrder, DEPTH10_LEN, NULL_ORDER, OrderBookDelta, OrderBookDepth,
        QuoteTick, TradeTick,
    },
    enums::{AggressorSide, BookAction, OrderSide, RecordFlag},
    identifiers::{InstrumentId, TradeId},
    types::{Price, Quantity},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use rust_decimal::Decimal;
use serde_json::{Map, Value};

use super::models::Stamp;
use crate::types::LseRow;

const TRADE: &str = "trade";

/// Returns the trades of a tick slice (`time, price, size, kind`).
///
/// A trade has no aggressor side, because LSE sends none. It has an id of its own (`trade_id`).
///
/// # Errors
///
/// Returns an error if:
/// - The file is no tick slice.
/// - A row is not a trade.
/// - A price or a size exceeds its precision or is not positive.
pub fn parse_trade_slice(
    file: Bytes,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
) -> anyhow::Result<Vec<TradeTick>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .context("reading LSE's Parquet file")?
        .build()?;
    let mut trades = Vec::new();
    let mut ids = TradeIds::default();

    for batch in reader {
        let batch = batch.context("reading LSE's Parquet file")?;
        let times = times(&batch)?;
        let prices = column::<Float64Array>(&batch, "price")?;
        let sizes = column::<Float64Array>(&batch, "size")?;
        let kinds = column::<StringArray>(&batch, "kind")?;
        for row in 0..batch.num_rows() {
            let kind = kinds.value(row);
            anyhow::ensure!(kind == TRADE, "a row of kind {kind:?}, not a {TRADE}");
            let time = UnixNanos::from(time(&times, row)?);
            let price = Price::from_decimal_dp(
                decimal(prices.value(row), price_precision, "price")?,
                price_precision,
            )?;
            let size = Quantity::from_decimal_dp(
                decimal(sizes.value(row), size_precision, "size")?,
                size_precision,
            )?;
            let trade_id = ids.next(instrument_id, time, price, size);
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

/// Returns the values of an index's tick slice (`time, price, size, kind`) as quotes.
///
/// LSE's socket sends an index's ticks the same way. Bid and ask are both the value, with zero
/// sizes.
///
/// # Errors
///
/// Returns an error if:
/// - The file is no tick slice.
/// - A row is not of kind trade.
/// - A value exceeds `price_precision`.
pub fn parse_index_quotes(
    file: Bytes,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
) -> anyhow::Result<Vec<QuoteTick>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .context("reading LSE's Parquet file")?
        .build()?;
    let none = Quantity::zero(size_precision);
    let mut quotes = Vec::new();

    for batch in reader {
        let batch = batch.context("reading LSE's Parquet file")?;
        let times = times(&batch)?;
        let values = column::<Float64Array>(&batch, "price")?;
        let kinds = column::<StringArray>(&batch, "kind")?;
        for row in 0..batch.num_rows() {
            let kind = kinds.value(row);
            anyhow::ensure!(kind == TRADE, "a row of kind {kind:?}, not a {TRADE}");
            let time = UnixNanos::from(time(&times, row)?);
            let value = Price::from_decimal_dp(
                decimal(values.value(row), price_precision, "value")?,
                price_precision,
            )?;
            quotes.push(QuoteTick::new_checked(
                instrument_id,
                value,
                value,
                none,
                none,
                time,
                time,
            )?);
        }
    }
    Ok(quotes)
}

/// Returns the quotes of an exchange market's quotes file (`time, bid_price, ask_price, bid_size,
/// ask_size`).
///
/// A quote with an empty side yields no quote, as in Nautilus' Databento decoder
/// (`decode_mbp1_msg`).
///
/// # Errors
///
/// Returns an error if:
/// - The file holds no quotes.
/// - A price or a size exceeds its precision.
pub fn parse_quotes(
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

/// Returns the depth snapshots of a book export (`time, sequence, action, side, price, size,
/// depth`, then ten bid and ten ask levels of price, size and orders, the best first).
///
/// Each row is the book after its event, with its sequence. As in Nautilus' Databento decoder
/// (`decode_mbp10_msg`), the snapshot drops the event's own action, side, price, size and depth. A
/// level with no price is no level. The last row of each time ends its event (see `end_events`).
///
/// # Errors
///
/// Returns an error if:
/// - The file holds no book.
/// - A level with a price has no size or orders.
/// - A price or a size exceeds its precision.
pub fn parse_book(
    file: Bytes,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
) -> anyhow::Result<Vec<OrderBookDepth>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .context("reading LSE's Parquet file")?
        .build()?;
    let mut depths = Vec::new();

    for batch in reader {
        let batch = batch.context("reading LSE's Parquet file")?;
        let times = times(&batch)?;
        let sequences = cast_column(&batch, "sequence", &DataType::UInt64)?;
        anyhow::ensure!(
            sequences.null_count() == 0,
            "the column sequence of LSE's file has empty rows"
        );
        let sequences = sequences.as_primitive::<UInt64Type>();
        let bids = BookSide::of(&batch, OrderSide::Buy)?;
        let asks = BookSide::of(&batch, OrderSide::Sell)?;
        for row in 0..batch.num_rows() {
            let time = UnixNanos::from(time(&times, row)?);
            let (bid_orders, bid_counts) = bids.levels(row, price_precision, size_precision)?;
            let (ask_orders, ask_counts) = asks.levels(row, price_precision, size_precision)?;
            depths.push(OrderBookDepth::new_checked(
                instrument_id,
                bid_orders,
                ask_orders,
                bid_counts,
                ask_counts,
                0,
                sequences.value(row),
                time,
                time,
            )?);
        }
    }
    end_events(
        &mut depths,
        |depth| depth.ts_event,
        |depth| &mut depth.flags,
    );
    Ok(depths)
}

/// Returns the book deltas of an orders export (`time, action, side, order_id, price, size`).
///
/// A delta has no sequence, because LSE gives none, as in Nautilus' Tardis loader.
///
/// LSE's Level 3 actions ("The record" of its CME stream docs) map as in Nautilus' Databento
/// decoder (`decode_mbo_msg`, `parse_book_action`):
/// - A (added), M (changed) and C (removed) each give a delta of the order.
/// - N (none) gives no delta.
///
/// The last delta of each time ends its event (see `end_events`).
///
/// # Errors
///
/// Returns an error if:
/// - The file holds no orders.
/// - An action or a side is not one LSE gives. A cleared book (R), given at Level 2 alone, is such
///   an action, as LSE's docs do not say whether its orders open a day with a book.
/// - An order has no price.
/// - A price or a size exceeds its precision.
pub fn parse_orders(
    file: Bytes,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
) -> anyhow::Result<Vec<OrderBookDelta>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .context("reading LSE's Parquet file")?
        .build()?;
    let mut deltas = Vec::new();

    for batch in reader {
        let batch = batch.context("reading LSE's Parquet file")?;
        let times = times(&batch)?;
        let actions = cast_column(&batch, "action", &DataType::Utf8)?;
        let sides = cast_column(&batch, "side", &DataType::Utf8)?;
        let order_ids = cast_column(&batch, "order_id", &DataType::UInt64)?;
        let (actions, sides) = (actions.as_string::<i32>(), sides.as_string::<i32>());
        let order_ids = order_ids.as_primitive::<UInt64Type>();
        let prices = nullable_column::<Float64Array>(&batch, "price")?;
        let sizes = column::<Float64Array>(&batch, "size")?;
        for row in 0..batch.num_rows() {
            let action = match actions.value(row) {
                "A" => BookAction::Add,
                "M" => BookAction::Update,
                "C" => BookAction::Delete,
                "N" => continue,
                other => {
                    anyhow::bail!(
                        "an order of action {other:?}, none of LSE's at Level 3: A, M, C and N"
                    )
                }
            };
            let side = match sides.value(row) {
                "B" => Some(OrderSide::Buy),
                "A" => Some(OrderSide::Sell),
                "N" => None,
                other => anyhow::bail!("an order of side {other:?}, none of LSE's: B, A and N"),
            };
            anyhow::ensure!(
                !order_ids.is_null(row) && !prices.is_null(row),
                "an order of action {} with no id or price",
                actions.value(row)
            );
            let time = UnixNanos::from(time(&times, row)?);
            let order = BookOrder::new(
                side,
                Price::from_decimal_dp(
                    decimal(prices.value(row), price_precision, "price")?,
                    price_precision,
                )?,
                Quantity::from_decimal_dp(
                    decimal(sizes.value(row), size_precision, "size")?,
                    size_precision,
                )?,
                order_ids.value(row),
            );
            deltas.push(OrderBookDelta::new_checked(
                instrument_id,
                action,
                order,
                0,
                0,
                time,
                time,
            )?);
        }
    }
    end_events(
        &mut deltas,
        |delta| delta.ts_event,
        |delta| &mut delta.flags,
    );
    Ok(deltas)
}

/// The ten levels on one side of a book export, as LSE's Databank reads them.
///
/// They run from `bid_price_0`, the best bid, to `ask_orders_9`, the tenth ask.
struct BookSide {
    side: OrderSide,
    prices: Vec<Float64Array>,
    sizes: Vec<Float64Array>,
    orders: Vec<ArrayRef>,
}

impl BookSide {
    fn of(batch: &RecordBatch, side: OrderSide) -> anyhow::Result<Self> {
        let name = match side {
            OrderSide::Buy => "bid",
            _ => "ask",
        };
        let mut levels = Self {
            side,
            prices: Vec::with_capacity(DEPTH10_LEN),
            sizes: Vec::with_capacity(DEPTH10_LEN),
            orders: Vec::with_capacity(DEPTH10_LEN),
        };

        for level in 0..DEPTH10_LEN {
            levels.prices.push(
                nullable_column::<Float64Array>(batch, &format!("{name}_price_{level}"))?.clone(),
            );
            levels.sizes.push(
                nullable_column::<Float64Array>(batch, &format!("{name}_size_{level}"))?.clone(),
            );
            levels.orders.push(cast_column(
                batch,
                &format!("{name}_orders_{level}"),
                &DataType::UInt32,
            )?);
        }
        Ok(levels)
    }

    // An empty level is a null order, as in Nautilus' Databento decoder (`decode_mbp10_msg`)
    fn levels(
        &self,
        row: usize,
        price_precision: u8,
        size_precision: u8,
    ) -> anyhow::Result<([BookOrder; DEPTH10_LEN], [u32; DEPTH10_LEN])> {
        let mut orders = [NULL_ORDER; DEPTH10_LEN];
        let mut counts = [0; DEPTH10_LEN];
        for level in 0..DEPTH10_LEN {
            let (prices, sizes) = (&self.prices[level], &self.sizes[level]);
            let count = self.orders[level].as_primitive::<UInt32Type>();

            if prices.is_null(row) {
                continue;
            }
            anyhow::ensure!(
                !sizes.is_null(row) && !count.is_null(row),
                "a {} level {level} of LSE's book with a price and no size or orders",
                self.side
            );
            let price = decimal(prices.value(row), price_precision, "price")?;
            let size = decimal(sizes.value(row), size_precision, "size")?;
            orders[level] = BookOrder::new(
                self.side,
                Price::from_decimal_dp(price, price_precision)?,
                Quantity::from_decimal_dp(size, size_precision)?,
                0,
            );
            counts[level] = count.value(row);
        }
        Ok((orders, counts))
    }
}

// LSE's exports carry no event boundary and the records of one exchange event share its time, so
// one timestamp's rows form one event, as in Nautilus' Tardis loader (`csv::load`, `load_deltas`),
// and the last record ends an event too, as exports are split between times, never within one.
fn end_events<T>(
    records: &mut [T],
    time: impl Fn(&T) -> UnixNanos,
    flags: impl Fn(&mut T) -> &mut u8,
) {
    for index in 0..records.len() {
        let ends = records
            .get(index + 1)
            .is_none_or(|next| time(next) != time(&records[index]));
        if ends {
            *flags(&mut records[index]) |= RecordFlag::F_LAST as u8;
        }
    }
}

/// Returns the bars of a file of candles (`time, open, high, low, close, volume, trades`).
///
/// LSE stamps a candle at its open, so a bar is stamped at its close. The close is the open plus
/// the width of `bar_type` (`BarSpecification::timedelta`).
///
/// # Errors
///
/// Returns an error if:
/// - The file holds no candles.
/// - A price or a volume exceeds its precision.
/// - A volume is negative.
/// - A candle's prices are not a bar's (its high is below another price, or its low is above
///   one).
pub fn parse_candles(
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

const DAY: SignedDuration = SignedDuration::from_hours(24);

const REPORT_TYPE: &str = "report_type";
const STATEMENTS: &[&str] = &["income", "balance", "cashflow"];
const DERIVED: &[&str] = &["metrics", "ratios", "growth"];
const PERIOD: &str = "period";

/// A source's rows, and the rows left out as known at no time, each with why.
#[derive(Debug, Default)]
pub struct Parsed {
    pub rows: Vec<Keyed>,
    pub refused: Vec<String>,
}

/// A row and its key, which is its `time_column` value.
#[derive(Debug)]
pub struct Keyed {
    pub key: Timestamp,
    pub row: LseRow,
}

struct Pending {
    key: UnixNanos,
    fields: Map<String, Value>,
    known: Result<UnixNanos, String>,
}

/// Returns the rows of an export of `symbol` in LSE's `source`.
///
/// The `source` is a reference collection or a macro series. Rows are stamped when `stamp` says
/// their values are known. A row that says when it was known nowhere is left out and named,
/// rather than stamped at a time the client chooses. Every other column but `symbol` is a field of
/// the row in JSON, as Arrow reads it.
///
/// # Errors
///
/// Returns an error if:
/// - The file is not Parquet.
/// - The time column is missing.
/// - The time column holds an empty row or a time before 1970.
/// - The time column does not match `stamp` (dates or times).
/// - A column `stamp` reads is missing or not of its kind.
pub fn parse_rows(
    file: Bytes,
    source: &str,
    symbol: &str,
    time_column: &str,
    stamp: Stamp,
) -> anyhow::Result<Parsed> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .context("reading LSE's Parquet file")?
        .build()?;
    let mut pending = Vec::new();

    for batch in reader {
        let batch = batch.context("reading LSE's Parquet file")?;
        let keys = row_times(&batch, time_column, stamp)?;
        let known = known_times(&batch, &keys, stamp)?;
        let mut writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, JsonArray>(Vec::new());
        writer.write(&batch)?;
        writer.finish()?;
        let objects: Vec<Map<String, Value>> =
            serde_json::from_slice(&writer.into_inner()).context("reading LSE's rows as JSON")?;

        for ((mut fields, key), known) in objects.into_iter().zip(keys).zip(known) {
            fields.remove(time_column);
            fields.remove("symbol");
            pending.push(Pending { key, fields, known });
        }
    }

    if stamp == Stamp::Filing {
        file_derived(&mut pending);
    }
    let mut parsed = Parsed::default();

    for Pending { key, fields, known } in pending {
        let key = Timestamp::from(key);

        match known {
            Ok(known) => parsed.rows.push(Keyed {
                key,
                row: LseRow::new(source.to_owned(), symbol.to_owned(), fields, known, known),
            }),
            Err(why) => parsed.refused.push(format!(
                "the row of {symbol} keyed {key} in {source}: {why}"
            )),
        }
    }
    Ok(parsed)
}

fn known_times(
    batch: &RecordBatch,
    keys: &[UnixNanos],
    stamp: Stamp,
) -> anyhow::Result<Vec<Result<UnixNanos, String>>> {
    match stamp {
        Stamp::DateEnd | Stamp::DateAt(..) | Stamp::Close(_) | Stamp::Key => Ok(keys
            .iter()
            .map(|&key| {
                let known = stamp
                    .known(Timestamp::from(key))
                    .expect("a row of the stamp is known when its key says");
                Ok(UnixNanos::from(known))
            })
            .collect()),
        Stamp::At(column) => Ok(column_times(batch, column, false)?
            .into_iter()
            .map(|time| time.ok_or_else(|| format!("no {column}")))
            .collect()),
        Stamp::Release => {
            let released = column_times(batch, "release_date", true)?;
            let ingested = column_times(batch, "created_at", false)?;
            released
                .into_iter()
                .zip(ingested)
                .map(|(released, ingested)| {
                    Ok(released
                        .map(|date| later(date, DAY))
                        .transpose()?
                        .or(ingested)
                        .ok_or_else(|| "no release_date or created_at".to_string()))
                })
                .collect()
        }
        Stamp::Filing => Ok(column_times(batch, "accepted_date", false)?
            .into_iter()
            .map(|accepted| accepted.ok_or_else(|| "no accepted_date".to_string()))
            .collect()),
    }
}

// A statement is known when it is accepted, a derived row at the latest acceptance among the
// statements of its period (its key and `period`), and any other row, or a derived row for a
// period with no accepted statement, at no time.
fn file_derived(rows: &mut [Pending]) {
    let text = |fields: &Map<String, Value>, name: &str| {
        fields.get(name).and_then(Value::as_str).map(str::to_owned)
    };
    let mut accepted: HashMap<(UnixNanos, Option<String>), UnixNanos> = HashMap::new();

    for row in rows.iter_mut() {
        let kind = text(&row.fields, REPORT_TYPE);
        let period = text(&row.fields, PERIOD);

        match kind.as_deref() {
            Some(kind) if STATEMENTS.contains(&kind) => {
                if let Ok(known) = row.known {
                    accepted
                        .entry((row.key, period))
                        .and_modify(|latest| *latest = (*latest).max(known))
                        .or_insert(known);
                }
            }
            Some(kind) if DERIVED.contains(&kind) => {}
            _ => {
                row.known = Err(format!(
                    "a report of the kind {}, neither a statement ({}) nor derived from them ({})",
                    kind.as_deref().unwrap_or("none"),
                    STATEMENTS.join(", "),
                    DERIVED.join(", ")
                ));
            }
        }
    }

    for row in rows.iter_mut() {
        let kind = text(&row.fields, REPORT_TYPE);
        if kind.as_deref().is_some_and(|kind| DERIVED.contains(&kind)) {
            let period = text(&row.fields, PERIOD);
            row.known = accepted
                .get(&(row.key, period.clone()))
                .copied()
                .ok_or_else(|| {
                    format!(
                        "{} of the period {}, of which no statement was accepted",
                        kind.as_deref().unwrap_or_default(),
                        period.as_deref().unwrap_or("none")
                    )
                });
        }
    }
}

fn later(time: UnixNanos, lag: SignedDuration) -> anyhow::Result<UnixNanos> {
    let lag = u64::try_from(lag.as_nanos()).expect("a lag is not negative");
    time.as_u64()
        .checked_add(lag)
        .map(UnixNanos::from)
        .context("a row known past 2554")
}

fn row_times(
    batch: &RecordBatch,
    time_column: &str,
    stamp: Stamp,
) -> anyhow::Result<Vec<UnixNanos>> {
    let times = column_times(batch, time_column, stamp.keyed_by_date()).map_err(|e| {
        e.context(format!(
            "the time column of the source, by which LSE selects its rows, is {time_column}"
        ))
    })?;
    times
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .with_context(|| format!("the time column {time_column} of LSE's file has empty rows"))
}

fn column_times(
    batch: &RecordBatch,
    name: &str,
    date: bool,
) -> anyhow::Result<Vec<Option<UnixNanos>>> {
    let column = batch
        .column_by_name(name)
        .with_context(|| format!("no column {name} in LSE's file"))?;

    match (date, column.data_type()) {
        (true, DataType::Date32 | DataType::Date64) | (false, DataType::Timestamp(_, _)) => {}
        (date, other) => anyhow::bail!(
            "the column {name} of LSE's file is {other}, not {}",
            if date { "dates" } else { "times" }
        ),
    }
    let options = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    let nanos = cast_with_options(
        column,
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        &options,
    )
    .with_context(|| format!("the column {name} of LSE's file holds a time past 2262"))?;
    let nanos = nanos.as_primitive::<TimestampNanosecondType>();
    (0..nanos.len())
        .map(|row| {
            nanos
                .is_valid(row)
                .then(|| time(nanos, row).map(UnixNanos::from))
                .transpose()
        })
        .collect()
}

fn column<'a, A: Array + 'static>(batch: &'a RecordBatch, name: &str) -> anyhow::Result<&'a A> {
    let column = nullable_column::<A>(batch, name)?;
    anyhow::ensure!(
        column.null_count() == 0,
        "the column {name} of LSE's file has empty rows"
    );
    Ok(column)
}

// For the book and orders columns, whose type LSE's docs do not give
fn cast_column(batch: &RecordBatch, name: &str, to: &DataType) -> anyhow::Result<ArrayRef> {
    let column = batch
        .column_by_name(name)
        .with_context(|| format!("no column {name} in LSE's file"))?;
    let options = CastOptions {
        safe: false,
        ..CastOptions::default()
    };
    cast_with_options(column, to, &options).with_context(|| {
        format!(
            "the column {name} of LSE's file is {}, not {to}",
            column.data_type()
        )
    })
}

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

// LSE gives UTC timestamps in nanoseconds in a tick slice and in milliseconds in candles
fn times(batch: &RecordBatch) -> anyhow::Result<TimestampNanosecondArray> {
    let column = batch
        .column_by_name("time")
        .context("no column time in LSE's file")?;

    match column.data_type() {
        DataType::Timestamp(_, Some(zone)) if zone.as_ref() == "UTC" => {}
        other => anyhow::bail!("the times of LSE's file are {other}, not UTC timestamps"),
    }
    let nanos = cast(
        column,
        &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
    )?;
    anyhow::ensure!(
        nanos.null_count() == 0,
        "the column time of LSE's file has empty rows, or times past 2262"
    );
    Ok(nanos.as_primitive::<TimestampNanosecondType>().clone())
}

fn time(times: &TimestampNanosecondArray, row: usize) -> anyhow::Result<u64> {
    u64::try_from(times.value(row)).context("a time before 1970")
}

/// Returns the double `value` as its shortest decimal, which must fit `precision`.
pub fn decimal(value: f64, precision: u8, what: &str) -> anyhow::Result<Decimal> {
    anyhow::ensure!(value.is_finite(), "a {what} of {value}");
    let decimal = Decimal::from_str(&value.to_string())
        .with_context(|| format!("a {what} of {value}"))?
        .normalize();
    anyhow::ensure!(
        decimal.scale() <= u32::from(precision),
        "a {what} of {value}, finer than the instrument's precision, {precision}"
    );
    Ok(decimal)
}

// FNV-1a 64-bit, as Nautilus' Databento adapter derives trade ids for records with none
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0100_0000_01b3;

/// The ids of an instrument's trades, in arrival order, as LSE gives trades none.
///
/// Each id hashes the trade's instrument, time, price and size, plus how many identical trades
/// came before it. So identical trades get distinct ids, and the ids are the same each time.
#[derive(Debug, Default)]
pub struct TradeIds {
    same: HashMap<(Price, Quantity), u32>,
    last_time: Option<UnixNanos>,
}

impl TradeIds {
    /// Returns the id of the next trade, whose `time` is no earlier than the last's.
    pub fn next(
        &mut self,
        instrument_id: InstrumentId,
        time: UnixNanos,
        price: Price,
        size: Quantity,
    ) -> TradeId {
        if self.last_time != Some(time) {
            self.same.clear();
            self.last_time = Some(time);
        }
        let occurrence = self.same.entry((price, size)).or_default();
        let id = trade_id(instrument_id, time, price, size, *occurrence);
        *occurrence += 1;
        id
    }
}

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

        // Ends the field, so that no two run into one
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
    use crate::testing::{
        AAPL_DIVIDENDS, AAPL_EARNINGS_CALENDAR, AAPL_FINANCIAL_REPORTS, AAPL_STOCK_SPLITS,
        AHMA_COMPANY_PROFILES, AHMA_STOCK_FUNDAMENTALS, AT016322245_CORPORATE_BONDS,
        AUDUSD_FX_DERIVATIVES, AUGB10Y_SOVEREIGN_YIELDS, CNH_CREDIT_INDICES, GCZ26_BOOK,
        GCZ26_ORDERS, GCZ26_QUOTES, GCZ26_TRADES, GOLD_FUTURES_4H, GOLD_SPOT_MINUTES, GOLD_TRADES,
        US_CORE_CPI_CALENDAR, US_CORE_CPI_RELEASES, US10Y_BOND_YIELDS,
        US10Y_BOND_YIELDS_FIFTEEN_MINUTES, US10Y_BOND_YIELDS_FIVE_MINUTES,
        US10Y_BOND_YIELDS_FOUR_HOURS, US10Y_BOND_YIELDS_HOURLY, US10Y_BOND_YIELDS_INTRADAY,
        US10Y_BOND_YIELDS_MINUTE, US10Y_BOND_YIELDS_THIRTY_MINUTES, US10Y_YIELDS, ZN_COT,
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

    #[rstest]
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

    #[rstest]
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

    fn rows_of(
        file: &'static [u8],
        source: &str,
        symbol: &str,
        time_column: &str,
        stamp: Stamp,
    ) -> Vec<LseRow> {
        let parsed =
            parse_rows(Bytes::from_static(file), source, symbol, time_column, stamp).unwrap();
        assert_eq!(parsed.refused, Vec::<String>::new());
        parsed.rows.into_iter().map(|keyed| keyed.row).collect()
    }

    fn us10y_rows() -> Vec<LseRow> {
        rows_of(
            US10Y_BOND_YIELDS,
            "bond_yields",
            "US10Y",
            "date",
            Stamp::DateEnd,
        )
    }

    fn day(date: &str) -> UnixNanos {
        nov(&format!("{date}T00:00:00"))
    }

    fn nov(time: &str) -> UnixNanos {
        utc(&format!("2026-11-{time}"))
    }

    fn utc(time: &str) -> UnixNanos {
        UnixNanos::from(format!("{time}Z").parse::<jiff::Timestamp>().unwrap())
    }

    #[rstest]
    #[case::bond_yields(US10Y_BOND_YIELDS, "bond_yields", "date")]
    #[case::macro_yield(US10Y_YIELDS, "macro_yield", "day")]
    fn a_row_of_a_date_is_stamped_at_the_date_s_end(
        #[case] file: &'static [u8],
        #[case] source: &str,
        #[case] time_column: &str,
    ) {
        let rows: Vec<_> = rows_of(file, source, "US10Y", time_column, Stamp::DateEnd)
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
                source.to_string(),
                "US10Y".to_string(),
                serde_json::json!(close),
            )
        };
        assert_eq!(rows, [row("10", 4.884), row("11", 5.0), row("13", 5.133)]);
    }

    #[rstest]
    fn a_row_of_an_interval_is_stamped_at_its_close() {
        let rows: Vec<_> = rows_of(
            US10Y_BOND_YIELDS_FOUR_HOURS,
            "bond_yields_four_hour",
            "US10Y",
            "ts",
            Stamp::Close(SignedDuration::from_hours(4)),
        )
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

    #[rstest]
    #[case::a_minute(US10Y_BOND_YIELDS_MINUTE, "bond_yields_minute", 1, 971, ("10T00:09:00", 4.876), "10T22:26:00")]
    #[case::five_minutes(US10Y_BOND_YIELDS_FIVE_MINUTES, "bond_yields_five_minute", 5, 265, ("10T00:10:00", 4.878), "10T22:30:00")]
    #[case::fifteen_minutes(US10Y_BOND_YIELDS_FIFTEEN_MINUTES, "bond_yields_fifteen_minute", 15, 90, ("10T00:15:00", 4.88), "10T22:30:00")]
    #[case::thirty_minutes(US10Y_BOND_YIELDS_THIRTY_MINUTES, "bond_yields_thirty_minute", 30, 45, ("10T00:30:00", 4.88), "10T22:30:00")]
    #[case::an_hour(US10Y_BOND_YIELDS_HOURLY, "bond_yields_hourly", 60, 23, ("10T01:00:00", 4.88), "10T23:00:00")]
    #[case::hourly(US10Y_BOND_YIELDS_INTRADAY, "bond_yields_intraday", 60, 23, ("10T01:00:00", 4.88), "10T23:00:00")]
    fn a_row_of_each_interval_is_stamped_at_its_close(
        #[case] file: &'static [u8],
        #[case] source: &str,
        #[case] minutes: i64,
        #[case] count: usize,
        #[case] (first, close): (&str, f64),
        #[case] last: &str,
    ) {
        let rows = rows_of(
            file,
            source,
            "US10Y",
            "ts",
            Stamp::Close(SignedDuration::from_mins(minutes)),
        );

        assert_eq!(rows.len(), count);
        assert_eq!(
            (rows[0].ts_init, rows[0].fields["close"].clone()),
            (nov(first), serde_json::json!(close))
        );
        assert_eq!(rows[count - 1].ts_init, nov(last));
        assert!(rows.iter().all(|row| row.ts_event == row.ts_init));
    }

    #[rstest]
    #[case::sovereign_yields(AUGB10Y_SOVEREIGN_YIELDS, "sovereign_yields", "AUGB10Y", 72, ("11", "ask", 94.373), ("12", "z_spread", -11.9303))]
    #[case::corporate_bonds(AT016322245_CORPORATE_BONDS, "corporate_bonds", "AT016322245", 3, ("10", "accrued_interest", 0.666), ("12", "accrued_interest", 0.6824))]
    #[case::credit_indices(CNH_CREDIT_INDICES, "credit_indices", "CNH_NONFIN_IG_0_1", 12, ("10", "high", 101.26864), ("12", "open", 101.2486))]
    #[case::fx_derivatives(AUDUSD_FX_DERIVATIVES, "fx_derivatives", "AUDUSD_1M_ATM", 30, ("10", "ask", 7.55), ("12", "mid_price", 7.7))]
    fn a_day_s_market_value_is_stamped_at_the_day_s_end(
        #[case] file: &'static [u8],
        #[case] source: &str,
        #[case] symbol: &str,
        #[case] count: usize,
        #[case] first: (&str, &str, f64),
        #[case] last: (&str, &str, f64),
    ) {
        let rows = rows_of(file, source, symbol, "date", Stamp::DateEnd);

        let read = |row: &LseRow| {
            (
                row.ts_init,
                row.fields["field"].clone(),
                row.fields["value"].clone(),
            )
        };
        let expected = |(stamped, field, value): (&str, &str, f64)| {
            (
                day(stamped),
                serde_json::json!(field),
                serde_json::json!(value),
            )
        };
        assert_eq!(rows.len(), count);
        assert_eq!(read(&rows[0]), expected(first));
        assert_eq!(read(&rows[count - 1]), expected(last));
        assert!(
            rows.iter()
                .all(|row| !row.fields.contains_key("date") && !row.fields.contains_key("symbol"))
        );
    }

    #[rstest]
    #[case::earnings_calendar(AAPL_EARNINGS_CALENDAR, "earnings_calendar", "AAPL", "date", &["2026-01-30", "2026-05-01", "2026-07-31", "2026-10-30", "2026-11-03"])]
    #[case::dividends(AAPL_DIVIDENDS, "dividends", "AAPL", "effective_date", &["2026-02-10", "2026-05-12", "2026-08-11", "2026-11-13"])]
    #[case::stock_splits(AAPL_STOCK_SPLITS, "stock_splits", "AAPL", "effective_date", &["2014-06-10", "2020-09-01"])]
    fn a_row_keyed_by_a_date_is_stamped_at_the_date_s_end(
        #[case] file: &'static [u8],
        #[case] source: &str,
        #[case] symbol: &str,
        #[case] time_column: &str,
        #[case] stamps: &[&str],
    ) {
        let rows = rows_of(file, source, symbol, time_column, Stamp::DateEnd);

        let stamped: Vec<_> = rows.iter().map(|row| row.ts_init).collect();
        let expected: Vec<_> = stamps
            .iter()
            .map(|date| utc(&format!("{date}T00:00:00")))
            .collect();
        assert_eq!(stamped, expected);
    }

    #[rstest]
    #[case::company_profiles(AHMA_COMPANY_PROFILES, "company_profiles")]
    #[case::stock_fundamentals(AHMA_STOCK_FUNDAMENTALS, "stock_fundamentals")]
    fn a_snapshot_is_stamped_when_it_was_updated(
        #[case] file: &'static [u8],
        #[case] source: &str,
    ) {
        let rows = rows_of(file, source, "AHMA", "updated_at", Stamp::Key);

        let read: Vec<_> = rows
            .iter()
            .map(|row| (row.ts_event, row.ts_init, row.fields["market_cap"].clone()))
            .collect();
        let at = utc("2026-10-13T05:08:01");
        assert_eq!(read, [(at, at, serde_json::json!(37_156_248.0))]);
    }

    fn core_cpi_releases() -> Vec<(UnixNanos, Option<&'static str>)> {
        [
            ("2025-12-18T13:30:00", Some("2.6%")),
            ("2026-01-13T13:30:00", Some("2.6%")),
            ("2026-02-11T13:30:00", None),
            ("2026-02-13T13:30:00", Some("2.5%")),
            ("2026-03-11T12:30:00", Some("2.5%")),
            ("2026-04-10T12:30:00", Some("2.6%")),
            ("2026-05-12T12:30:00", Some("2.8%")),
            ("2026-06-10T12:30:00", Some("2.9%")),
            ("2026-07-14T12:30:00", Some("2.6%")),
            ("2026-08-12T12:30:00", Some("2.5%")),
            ("2026-09-11T12:30:00", Some("2.4%")),
            ("2026-10-14T12:30:00", Some("2.6%")),
            ("2026-11-10T13:30:00", None),
            ("2026-12-10T13:30:00", None),
        ]
        .map(|(at, actual)| (utc(at), actual))
        .to_vec()
    }

    fn released(rows: &[LseRow]) -> Vec<(UnixNanos, Option<&str>)> {
        rows.iter()
            .map(|row| (row.ts_init, row.fields["actual"].as_str()))
            .collect()
    }

    #[rstest]
    fn an_economic_release_is_stamped_at_the_time_it_came_out() {
        let rows = rows_of(
            US_CORE_CPI_CALENDAR,
            "economic_calendar",
            "US.Core Inflation Rate YoY",
            "date",
            Stamp::At("datetime"),
        );

        assert_eq!(released(&rows), core_cpi_releases());
        assert!(rows.iter().all(|row| row.ts_event == row.ts_init));
        assert!(rows.iter().all(|row| row.fields.contains_key("datetime")));
    }

    #[rstest]
    fn a_macro_release_is_stamped_at_the_time_it_came_out() {
        let rows = rows_of(
            US_CORE_CPI_RELEASES,
            "macro_release",
            "US.Core Inflation Rate YoY",
            "released_at",
            Stamp::Key,
        );

        assert_eq!(released(&rows), core_cpi_releases());
    }

    /// Since 28 April 2026 there is no `release_date`, so `created_at` is used.
    #[rstest]
    fn a_cot_report_is_stamped_at_its_release_date_s_end_or_when_lse_ingested_it() {
        let parsed = parse_rows(
            Bytes::from_static(ZN_COT),
            "cot",
            "ZN",
            "date",
            Stamp::Release,
        )
        .unwrap();

        let stamped: Vec<_> = parsed
            .rows
            .iter()
            .map(|keyed| (UnixNanos::from(keyed.key), keyed.row.ts_init))
            .collect();
        assert_eq!(parsed.refused, Vec::<String>::new());
        assert_eq!(stamped.len(), 614);
        let report = |tuesday: &str, known: &str| (utc(&format!("{tuesday}T00:00:00")), utc(known));
        assert_eq!(stamped[0], report("2015-01-06", "2015-01-10T00:00:00"));
        let first_unreleased = stamped
            .iter()
            .position(|&(tuesday, _)| tuesday == utc("2026-04-28T00:00:00"))
            .unwrap();
        assert_eq!(
            stamped[first_unreleased - 1..=first_unreleased],
            [
                report("2026-04-21", "2026-04-25T00:00:00"),
                report("2026-04-28", "2026-05-01T20:30:18"),
            ]
        );
        assert_eq!(stamped[613], report("2026-10-06", "2026-10-09T20:30:10"));
    }

    fn keyed_with_empty(symbol: &str, day: i32, columns: &[(&str, DataType)]) -> Bytes {
        let mut fields = vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("date", DataType::Date32, false),
        ];
        let mut arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![symbol])),
            Arc::new(arrow::array::Date32Array::from(vec![day])),
        ];

        for (name, type_) in columns {
            fields.push(Field::new(*name, type_.clone(), true));
            arrays.push(arrow::array::new_null_array(type_, 1));
        }
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays).unwrap();
        let mut file = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(file)
    }

    const SEPTEMBER_22: i32 = 20_718;

    #[rstest]
    #[case::a_report_of_no_release_or_ingest(
        "cot",
        Stamp::Release,
        &[
            ("release_date", DataType::Date32),
            ("created_at", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))),
        ]
    )]
    #[case::a_release_of_no_time(
        "economic_calendar",
        Stamp::At("datetime"),
        &[("datetime", DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())))]
    )]
    fn a_row_known_at_no_time_is_left_out_naming_it(
        #[case] source: &str,
        #[case] stamp: Stamp,
        #[case] columns: &[(&str, DataType)],
    ) {
        let file = keyed_with_empty("ZN", SEPTEMBER_22, columns);

        let parsed = parse_rows(file, source, "ZN", "date", stamp).unwrap();

        assert_eq!(parsed.rows.len(), 0);
        let [refused] = &parsed.refused[..] else {
            panic!("not one row refused: {:?}", parsed.refused);
        };
        let empty = columns.iter().map(|&(column, _)| column);
        for part in ["ZN", "2026-09-22", source].into_iter().chain(empty) {
            assert!(refused.contains(part), "{part}: {refused}");
        }
    }

    /// The trailing twelve months' metrics and ratios of 7 October 2026 have no statement, so they
    /// are left out and named.
    #[rstest]
    fn a_financial_report_is_stamped_when_its_period_s_statements_were_accepted() {
        let parsed = parse_rows(
            Bytes::from_static(AAPL_FINANCIAL_REPORTS),
            "financial_reports",
            "AAPL",
            "date",
            Stamp::Filing,
        )
        .unwrap();

        let stamped: HashSet<_> = parsed
            .rows
            .iter()
            .map(|keyed| {
                (
                    UnixNanos::from(keyed.key),
                    keyed.row.fields["period"].as_str().unwrap().to_string(),
                    keyed.row.ts_init,
                )
            })
            .collect();
        let period = |end: &str, period: &str, accepted: &str| {
            (
                utc(&format!("{end}T00:00:00")),
                period.to_string(),
                utc(accepted),
            )
        };
        assert_eq!(
            stamped,
            HashSet::from([
                period("2025-06-28", "Q3", "2025-08-01T10:00:42"),
                period("2025-09-27", "FY", "2025-10-31T10:01:26"),
                period("2025-09-27", "Q4", "2025-10-31T10:01:26"),
                period("2025-12-27", "Q1", "2026-01-30T11:01:32"),
                period("2026-03-28", "Q2", "2026-05-01T14:01:00"),
                period("2026-06-27", "Q3", "2026-07-31T10:01:02"),
            ])
        );
        assert_eq!(parsed.rows.len(), 36);
        let mut refused = parsed.refused;
        refused.sort();
        assert_eq!(refused.len(), 2, "{refused:?}");
        for (refused, report_type) in refused.iter().zip(["metrics", "ratios"]) {
            for part in [report_type, "TTM", "2026-10-07"] {
                assert!(refused.contains(part), "{part}: {refused}");
            }
        }
    }

    #[rstest]
    #[case::dates_as_intervals(
        US10Y_BOND_YIELDS,
        "date",
        Stamp::Close(SignedDuration::from_hours(4)),
        Some("Date32")
    )]
    #[case::times_as_dates(US10Y_BOND_YIELDS_FOUR_HOURS, "ts", Stamp::DateEnd, Some("Timestamp"))]
    #[case::missing(US10Y_BOND_YIELDS, "ts", Stamp::DateEnd, None)]
    fn a_time_column_not_of_the_key_of_its_collection_is_refused_naming_it(
        #[case] file: &'static [u8],
        #[case] time_column: &str,
        #[case] stamp: Stamp,
        #[case] lse_type: Option<&str>,
    ) {
        let err = parse_rows(
            Bytes::from_static(file),
            "bond_yields",
            "US10Y",
            time_column,
            stamp,
        )
        .unwrap_err();

        let err = format!("{err:#}");
        assert!(
            err.split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                .any(|word| word == time_column),
            "{err}"
        );
        assert!(
            lse_type.is_none_or(|lse_type| err.contains(lse_type)),
            "{err}"
        );
    }

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

    /// A definition too coarse for the prices LSE sends: refused, not rounded.
    #[rstest]
    fn a_price_finer_than_the_instrument_s_precision_is_refused() {
        let err = parse_trade_slice(Bytes::from_static(GOLD_TRADES), gold(), 0, 0).unwrap_err();

        assert!(err.to_string().contains("4136.4"), "{err:#}");
    }

    #[rstest]
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

    #[rstest]
    fn a_file_of_no_candles_is_refused_naming_the_column_it_lacks() {
        let bar_type = BarType::from("GOLD-FUTURES.LSEDGE-1-MINUTE-LAST-EXTERNAL");

        let err = parse_candles(Bytes::from_static(GOLD_TRADES), bar_type, 1, 0).unwrap_err();

        assert!(err.to_string().contains("no column open"), "{err:#}");
    }

    #[rstest]
    fn a_candle_price_finer_than_the_instrument_s_precision_is_refused() {
        let bar_type = BarType::from("XAU/USD-COMMODITIES.LSEDGE-1-MINUTE-LAST-EXTERNAL");

        let err = parse_candles(Bytes::from_static(GOLD_SPOT_MINUTES), bar_type, 1, 0).unwrap_err();

        assert!(err.to_string().contains("4118.01"), "{err:#}");
    }

    fn gcz26() -> InstrumentId {
        InstrumentId::from("GCZ26.GLBX")
    }

    fn on_the_12th(nanos: u64) -> UnixNanos {
        UnixNanos::from(nanos)
    }

    #[rstest]
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

    #[rstest]
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

    type Level = (Price, Quantity, u32);

    fn levels(orders: &[BookOrder], counts: &[u32]) -> Vec<Level> {
        orders
            .iter()
            .zip(counts)
            .map(|(order, &count)| (order.price, order.size, count))
            .collect()
    }

    fn first_book_levels() -> (Vec<Level>, Vec<Level>) {
        let level =
            |price: &str, size: u64, count: u32| (Price::from(price), Quantity::from(size), count);
        (
            vec![
                level("3908.10", 2, 1),
                level("3908.00", 5, 3),
                level("3907.90", 3, 2),
                level("3907.80", 4, 2),
                level("3907.70", 6, 4),
                level("3907.60", 1, 1),
                level("3907.50", 7, 5),
                level("3907.40", 2, 1),
                level("3907.30", 3, 2),
                level("3907.20", 8, 6),
            ],
            vec![
                level("3908.30", 9, 4),
                level("3908.40", 4, 2),
                level("3908.50", 6, 3),
                level("3908.60", 2, 1),
                level("3908.70", 5, 3),
                level("3908.80", 3, 2),
                level("3908.90", 8, 5),
                level("3909.00", 1, 1),
                level("3909.10", 4, 2),
                level("3909.20", 7, 4),
            ],
        )
    }

    /// As Nautilus' Databento decoder makes one of an MBP-10 record (`decode_mbp10_msg`).
    #[rstest]
    fn a_book_export_is_a_depth_snapshot_a_row_at_the_instrument_s_precision() {
        let depths = parse_book(Bytes::from_static(GCZ26_BOOK), gcz26(), 2, 0).unwrap();

        assert_eq!(depths.len(), 4);
        let first = &depths[0];
        assert_eq!(first.instrument_id, gcz26());
        assert_eq!(
            (first.ts_event, first.ts_init, first.sequence),
            (
                on_the_12th(1_794_493_800_000_184_205),
                on_the_12th(1_794_493_800_000_184_205),
                1001
            )
        );
        let (bids, asks) = first_book_levels();
        assert_eq!(levels(&first.bids, &first.bid_counts), bids);
        assert_eq!(levels(&first.asks, &first.ask_counts), asks);
        assert!(
            first
                .bids
                .iter()
                .all(|order| order.side == Some(OrderSide::Buy))
        );
        assert!(
            first
                .asks
                .iter()
                .all(|order| order.side == Some(OrderSide::Sell))
        );
        assert_eq!(first.bids[0].price.precision, 2);
        let sequences: Vec<u64> = depths.iter().map(|depth| depth.sequence).collect();
        assert_eq!(sequences, [1001, 1002, 1003, 1180]);
    }

    /// The last row's book holds eight asks: its two last ask levels have no price.
    #[rstest]
    fn an_empty_level_of_the_book_is_no_level_of_the_snapshot() {
        let depths = parse_book(Bytes::from_static(GCZ26_BOOK), gcz26(), 2, 0).unwrap();

        let last = &depths[3];
        assert_eq!(last.bids.len(), 10);
        assert_eq!(last.asks.len(), 8);
        assert_eq!(last.ask_counts.len(), 8);
        assert_eq!(last.asks[7].price, Price::from("3909.00"));
    }

    /// The last row of one time ends the event (`F_LAST`), as in Nautilus' Tardis loader.
    #[rstest]
    fn the_last_row_of_each_time_of_the_book_ends_its_event() {
        let depths = parse_book(Bytes::from_static(GCZ26_BOOK), gcz26(), 2, 0).unwrap();

        let flags: Vec<u8> = depths.iter().map(|depth| depth.flags).collect();
        let last = RecordFlag::F_LAST as u8;
        assert_eq!(flags, [last, 0, last, last]);
    }

    #[rstest]
    fn a_book_price_finer_than_the_instrument_s_precision_is_refused() {
        let err = parse_book(Bytes::from_static(GCZ26_BOOK), gcz26(), 0, 0).unwrap_err();

        let err = err.to_string();
        assert!(
            err.contains("3908.") && err.contains("precision, 0"),
            "{err}"
        );
    }

    /// N gives no delta, as Nautilus' Databento decoder gives none for an MBO record of no action
    /// (`decode_mbo_msg`).
    #[rstest]
    fn an_orders_export_is_the_deltas_of_its_orders() {
        let deltas = parse_orders(Bytes::from_static(GCZ26_ORDERS), gcz26(), 2, 0).unwrap();

        let rows: Vec<_> = deltas
            .iter()
            .map(|d| {
                (
                    d.action,
                    d.order.side,
                    d.order.order_id,
                    d.order.price,
                    d.order.size,
                    d.flags,
                    d.ts_event,
                )
            })
            .collect();
        let last = RecordFlag::F_LAST as u8;
        let row = |action, side, id, price, size, flags, time| {
            (
                action,
                Some(side),
                id,
                Price::from(price),
                Quantity::from(size),
                flags,
                on_the_12th(time),
            )
        };
        assert_eq!(
            rows,
            [
                row(
                    BookAction::Add,
                    OrderSide::Buy,
                    6_411_893_001,
                    "3908.10",
                    1,
                    0,
                    1_794_493_800_000_184_205
                ),
                row(
                    BookAction::Add,
                    OrderSide::Sell,
                    6_411_893_002,
                    "3908.30",
                    2,
                    last,
                    1_794_493_800_000_184_205
                ),
                row(
                    BookAction::Update,
                    OrderSide::Buy,
                    6_411_893_001,
                    "3908.10",
                    3,
                    last,
                    1_794_493_800_001_418_191
                ),
                row(
                    BookAction::Delete,
                    OrderSide::Sell,
                    6_411_893_002,
                    "3908.30",
                    2,
                    last,
                    1_794_493_800_027_098_067
                ),
            ]
        );
        assert!(
            deltas
                .iter()
                .all(|d| d.instrument_id == gcz26() && d.ts_init == d.ts_event)
        );
    }

    fn orders_of(action: &str) -> Bytes {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                false,
            ),
            Field::new("action", DataType::Utf8, false),
            Field::new("side", DataType::Utf8, false),
            Field::new("order_id", DataType::Utf8, false),
            Field::new("price", DataType::Float64, true),
            Field::new("size", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![1]).with_timezone("UTC")),
                Arc::new(StringArray::from(vec![action])),
                Arc::new(StringArray::from(vec!["B"])),
                Arc::new(StringArray::from(vec!["6411893001"])),
                Arc::new(Float64Array::from(vec![Some(3908.1)])),
                Arc::new(Float64Array::from(vec![1.0])),
            ],
        )
        .unwrap();
        let mut file = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(file)
    }

    /// LSE's docs give R at Level 2 alone, and a trade (T) at Level 1.
    #[rstest]
    #[case::cleared("R")]
    #[case::traded("T")]
    #[case::a_word("add")]
    fn an_order_action_lse_does_not_give_at_level_3_is_refused_naming_it(#[case] action: &str) {
        let err = parse_orders(orders_of(action), gcz26(), 2, 0).unwrap_err();

        assert!(err.to_string().contains(&format!("{action:?}")), "{err:#}");
    }

    #[rstest]
    fn an_order_price_finer_than_the_instrument_s_precision_is_refused() {
        let err = parse_orders(Bytes::from_static(GCZ26_ORDERS), gcz26(), 0, 0).unwrap_err();

        let err = err.to_string();
        assert!(
            err.contains("3908.1") && err.contains("precision, 0"),
            "{err}"
        );
    }

    #[rstest]
    fn a_file_of_no_book_is_refused_naming_the_column_it_lacks() {
        let err = parse_book(Bytes::from_static(GCZ26_QUOTES), gcz26(), 2, 0).unwrap_err();

        assert!(err.to_string().contains("no column sequence"), "{err:#}");
    }

    /// An empty side, which LSE's schema allows and no capture holds, so built here, gives no
    /// quote, as in Nautilus' Databento decoder (`decode_mbp1_msg`).
    #[rstest]
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

        let [quote] = &quotes[..] else {
            panic!("not one quote: {quotes:?}");
        };
        assert_eq!(
            (
                quote.instrument_id,
                quote.bid_price,
                quote.ask_price,
                quote.bid_size,
                quote.ask_size,
                quote.ts_event,
                quote.ts_init,
            ),
            (
                gcz26(),
                Price::from("3908.10"),
                Price::from("3908.30"),
                Quantity::from(2),
                Quantity::from(9),
                UnixNanos::from(1),
                UnixNanos::from(1),
            )
        );
        assert_eq!(
            (
                quote.bid_price.precision,
                quote.ask_price.precision,
                quote.bid_size.precision,
                quote.ask_size.precision,
            ),
            (2, 2, 0, 0)
        );
    }
}
