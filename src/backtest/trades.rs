//! The trades file of a run: each position of the run, one row each, in Parquet.
//!
//! The positions are those Nautilus' own positions report reads (`ReportProvider`, which the
//! engine reaches from Python alone): every position in the cache, and every closed cycle Nautilus
//! archived as a snapshot before reusing its ID on a netting venue. Each is encoded by Nautilus'
//! display encoder of positions (`encode_positions`), its columns as it names them. A position
//! still open at the end of the run has no close (`ts_closed`, `avg_px_close`,
//! `closing_order_id` are null) and its realized P&L so far.
//!
//! Three columns follow Nautilus' own, read from the tags of the position's opening entry:
//! - `r_amount` and `r_currency`: the R in money its trade was sent at (`orders::entry_risk`).
//! - `expected_r`: the expected net R of the model decision that sent it
//!   (`orders::entry_expected_r`), null for a trade no decision sent.
//!
//! They are set only when the position holds its opening entry's trade whole: every fill that adds
//! to it is of that entry, and every fill of that entry adds to it. A netting position that another
//! entry added to, or one opened by the rest of an entry that flipped the position before it, has
//! no single trade, so its three are null. Its net R is `realized_pnl_amount` over `r_amount`,
//! Nautilus netting a commission into the P&L in its own currency alone (`commissions` lists them).

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context;
use arrow::{
    array::{ArrayRef, Float64Builder, StringBuilder},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use nautilus_common::{cache::Cache, live::block_on_nautilus_with};
use nautilus_model::{orders::Order, position::Position};
use nautilus_persistence::backend::parquet::io::{
    create_object_store_from_path, write_batches_to_object_store,
};
use nautilus_serialization::arrow::display::position::encode_positions;
use object_store::path::Path as ObjectPath;
use rust_decimal::Decimal;

use crate::strategy::orders::{self, EntryRisk};

/// The positions of a run, as the trades file holds them.
#[derive(Debug, Clone)]
pub struct Trades(RecordBatch);

impl Trades {
    /// Returns the positions in `cache` and its position snapshots, in the order they opened.
    ///
    /// # Errors
    ///
    /// Returns an error if Nautilus fails to encode them.
    pub(super) fn of(cache: &Cache) -> anyhow::Result<Self> {
        let mut positions: Vec<Position> = cache
            .positions(None, None, None, None, None)
            .iter()
            .map(|position| (**position).clone())
            .chain(cache.position_snapshots(None, None))
            .collect();
        positions.sort_by_key(|position| (position.ts_opened, position.id));
        let mut r_amount = Float64Builder::with_capacity(positions.len());
        let mut r_currency = StringBuilder::new();
        let mut expected_r = Float64Builder::with_capacity(positions.len());
        for position in &positions {
            let trade = whole_trade(cache, position);
            let risk = trade.and_then(|(risk, _)| risk);
            r_amount.append_option(risk.map(|risk| risk.r.as_f64()));
            r_currency.append_option(risk.map(|risk| risk.r.currency.code.to_string()));
            expected_r.append_option(trade.and_then(|(_, expected)| expected).map(|r| r.as_f64()));
        }
        let encoded = encode_positions(&positions).context("encoding the positions of the run")?;
        let mut fields: Vec<Field> = encoded
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect();
        fields.extend([
            Field::new("r_amount", DataType::Float64, true),
            Field::new("r_currency", DataType::Utf8, true),
            Field::new("expected_r", DataType::Float64, true),
        ]);
        let mut columns: Vec<ArrayRef> = encoded.columns().to_vec();
        columns.extend([
            Arc::new(r_amount.finish()) as ArrayRef,
            Arc::new(r_currency.finish()),
            Arc::new(expected_r.finish()),
        ]);
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
            .context("adding the trade columns to the positions of the run")?;
        Ok(Self(batch))
    }

    /// Returns the rows: Nautilus' columns of a position, then the trade columns.
    #[must_use]
    pub const fn batch(&self) -> &RecordBatch {
        &self.0
    }

    /// Writes the rows to the Parquet file at `path`, through Nautilus' Parquet writer, creating
    /// its directory. An existing file is replaced.
    ///
    /// # Errors
    ///
    /// Returns an error if `path` names no file, or the directory or the file cannot be written.
    pub fn write(&self, path: &Path) -> anyhow::Result<()> {
        let written = || format!("writing the trades file {}", path.display());
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .with_context(|| format!("{}: no file name", written()))?;
        let dir = match path.parent() {
            Some(dir) if !dir.as_os_str().is_empty() => dir.to_path_buf(),
            _ => PathBuf::from("."),
        };
        fs::create_dir_all(&dir).with_context(written)?;
        let dir = std::path::absolute(&dir).with_context(written)?;
        let dir = dir
            .to_str()
            .with_context(|| format!("{}: a directory that is not UTF-8", written()))?;
        let (store, _, _) = create_object_store_from_path(dir, None).with_context(written)?;
        let object = ObjectPath::from(name);
        let batches = [self.0.clone()];
        block_on_nautilus_with(|| {
            write_batches_to_object_store(&batches, store, &object, None, None, None)
        })
        .with_context(written)
    }
}

/// Returns the tags of `position`'s opening entry, if the position holds its trade whole (see the
/// module doc): what it risks, if it was sent at a stake, and the expected R of the decision that
/// sent it, if any.
fn whole_trade(
    cache: &Cache,
    position: &Position,
) -> Option<(Option<EntryRisk>, Option<crate::strategy::risk::R>)> {
    let entry = cache.order(&position.opening_order_id)?;
    let adding = position
        .events
        .iter()
        .filter(|fill| fill.order_side == position.entry);
    let mut added = Decimal::ZERO;

    for fill in adding {
        if fill.client_order_id != position.opening_order_id {
            return None;
        }
        added += fill.last_qty.as_decimal();
    }
    (added == entry.filled_qty().as_decimal())
        .then(|| (orders::entry_risk(&entry), orders::entry_expected_r(&entry)))
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        enums::{OrderSide, OrderType},
        events::{OrderEventAny, OrderFilled},
        identifiers::{ClientOrderId, PositionId, TradeId},
        instruments::{Instrument, InstrumentAny, stubs::equity_aapl},
        orders::{
            OrderAny, OrderTestBuilder,
            stubs::{OrderFilledTestBuilder, TestOrderStubs},
        },
        types::{Money, Quantity},
    };
    use rstest::rstest;
    use ustr::Ustr;

    use super::*;
    use crate::strategy::risk::R;

    const TAGGED: [&str; 3] = ["R:31.00 USD", "MULTIPLE:1", "EXPECTED_R:0.42R"];

    fn aapl() -> InstrumentAny {
        InstrumentAny::Equity(equity_aapl())
    }

    fn order(id: &str, side: OrderSide, qty: u64, tags: &[&str]) -> OrderAny {
        let order = OrderTestBuilder::new(OrderType::Market)
            .client_order_id(ClientOrderId::from(id))
            .instrument_id(aapl().id())
            .side(side)
            .quantity(Quantity::from(qty))
            .tags(tags.iter().map(|tag| Ustr::from(tag)).collect())
            .build();
        TestOrderStubs::make_accepted_order(&order)
    }

    /// Fills `qty` of `order` into the one position of the tests, as trade `trade`.
    fn fill(order: &mut OrderAny, trade: &str, qty: u64) -> OrderFilled {
        let event = OrderFilledTestBuilder::new(order, &aapl())
            .trade_id(TradeId::from(trade))
            .position_id(PositionId::from("AAPL.XNAS-NETTED"))
            .last_qty(Quantity::from(qty))
            .build();
        order.apply(event.clone()).unwrap();
        let OrderEventAny::Filled(fill) = event else {
            unreachable!("a fill")
        };
        fill
    }

    fn cached(orders: &[&OrderAny]) -> Cache {
        let mut cache = Cache::default();
        cache.add_instrument(aapl()).unwrap();
        for order in orders {
            cache
                .add_order((*order).clone(), None, None, false)
                .unwrap();
        }
        cache
    }

    fn position_of(fills: &[OrderFilled]) -> Position {
        let mut position = Position::new(&aapl(), fills[0].clone());
        for fill in &fills[1..] {
            position.apply(fill);
        }
        position
    }

    /// The entry fills in two parts and an untagged leg closes the position.
    #[rstest]
    fn an_entry_filled_whole_into_its_position_is_its_trade_whatever_closes_it() {
        let mut entry = order("E-1", OrderSide::Buy, 10, &TAGGED);
        let mut leg = order("E-1-SL", OrderSide::Sell, 10, &[]);
        let fills = [
            fill(&mut entry, "T-1", 4),
            fill(&mut entry, "T-2", 6),
            fill(&mut leg, "T-3", 10),
        ];

        let trade = whole_trade(&cached(&[&entry, &leg]), &position_of(&fills));

        let risk = EntryRisk {
            r: Money::from("31.00 USD"),
            multiple: Decimal::ONE,
        };
        assert_eq!(trade, Some((Some(risk), Some(R::new(0.42)))));
    }

    #[rstest]
    fn a_position_another_entry_added_to_holds_no_one_trade() {
        let mut first = order("E-1", OrderSide::Buy, 10, &TAGGED);
        let mut second = order("E-2", OrderSide::Buy, 5, &TAGGED);
        let fills = [fill(&mut first, "T-1", 10), fill(&mut second, "T-2", 5)];

        let trade = whole_trade(&cached(&[&first, &second]), &position_of(&fills));

        assert_eq!(trade, None);
    }

    /// Nautilus opens the cycle after a flip on the rest of the flipping fill, 5 of the sell's 15.
    #[rstest]
    fn the_rest_of_an_entry_that_flipped_a_position_is_no_trade_of_its_own() {
        let mut flip = order("E-2", OrderSide::Sell, 15, &TAGGED);
        fill(&mut flip, "T-2", 15);
        let rest = OrderFilledTestBuilder::new(&flip, &aapl())
            .trade_id(TradeId::from("T-2"))
            .position_id(PositionId::from("AAPL.XNAS-NETTED"))
            .last_qty(Quantity::from(5))
            .build();
        let OrderEventAny::Filled(rest) = rest else {
            unreachable!("a fill")
        };

        let trade = whole_trade(&cached(&[&flip]), &position_of(&[rest]));

        assert_eq!(trade, None);
    }
}
