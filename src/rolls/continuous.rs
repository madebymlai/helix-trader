//! Nautilus' continuous bars on roots: the params of a bar request or
//! subscription on a root, its transition table's rows with their roll
//! prices, read from the catalog, and an adjustment mode.

use std::{
    collections::{BTreeMap, btree_map::Entry},
    path::Path,
};

use anyhow::{Context, ensure};
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{BarType, TradeTick},
    enums::ContinuousFutureAdjustmentType,
    identifiers::InstrumentId,
    types::Price,
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use serde_json::{Value, json};

use super::{Slice, TransitionTable};

/// The params Nautilus' data engine reads a continuous future from.
const TRANSITIONS: &str = "continuous_future_transitions";
const ADJUSTMENT_MODE: &str = "continuous_future_adjustment_mode";

/// How far a read for a roll price reaches at a time: a slice spans months
/// of trades, of which the price needs one.
const READ_WINDOW: SignedDuration = SignedDuration::from_hours(24);

/// The continuous futures of a strategy's bars on roots over a span: the
/// `continuous_future_*` params Nautilus' data engine builds a root's bars
/// from (see `docs/concepts/continuous_futures.md` in Nautilus), the same
/// for its warmup request and its subscription. The default holds none: a
/// strategy reading no root.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ContinuousBars {
    /// The `continuous_future_transitions` of each root.
    transitions: BTreeMap<InstrumentId, Value>,
    mode: ContinuousFutureAdjustmentType,
}

impl ContinuousBars {
    /// The continuous futures of the roots among the instruments of
    /// `bar_types` over `[start, end]`, adjusted in `mode`, their roll
    /// prices read from the catalog at `catalog`: at each transition, the
    /// pre contract's last trade of its slice and the post contract's
    /// first, as Nautilus takes them. A plain instrument has none. Bars on
    /// a root are built from its trades: `DataRequirements::slices` refuses
    /// others.
    ///
    /// # Errors
    ///
    /// Returns an error if a root does not roll over the span (Nautilus'
    /// continuous bars need a transition), or if a contract has no trade in
    /// its slice to price a roll, or its trades cannot be read.
    pub fn read(
        catalog: &Path,
        bar_types: &[BarType],
        start: Timestamp,
        end: Timestamp,
        mode: ContinuousFutureAdjustmentType,
    ) -> anyhow::Result<Self> {
        let mut catalog = ParquetDataCatalog::new(catalog, None, None, None, None);
        let mut transitions = BTreeMap::new();
        for bar_type in bar_types {
            let instrument_id = bar_type.instrument_id();
            let table = TransitionTable::of(instrument_id, start, end);
            // A plain instrument stands for itself.
            if table.contract_at(start) == instrument_id {
                continue;
            }
            ensure!(
                !table.transitions().is_empty(),
                "no roll of {instrument_id} over [{start}, {end}]: Nautilus' continuous \
                 bars need one; read its contract {} instead",
                table.contract_at(start)
            );
            if let Entry::Vacant(entry) = transitions.entry(instrument_id) {
                entry.insert(table.priced(&mut catalog)?);
            }
        }
        Ok(Self { transitions, mode })
    }

    /// The params of a request or subscription of bars on `instrument_id`
    /// that make them its continuous future, none if it is a plain
    /// instrument.
    #[must_use]
    pub fn params(&self, instrument_id: InstrumentId) -> Option<[(String, Value); 2]> {
        let transitions = self.transitions.get(&instrument_id)?;
        Some([
            (TRANSITIONS.to_string(), transitions.clone()),
            (ADJUSTMENT_MODE.to_string(), json!(self.mode.to_string())),
        ])
    }
}

impl TransitionTable {
    /// The transitions as Nautilus' `continuous_future_transitions` rows,
    /// each priced from `catalog`: its pre contract's last trade of its
    /// slice, its post contract's first.
    fn priced(&self, catalog: &mut ParquetDataCatalog) -> anyhow::Result<Value> {
        let slices = self.slices();
        let rows = self
            .transitions()
            .iter()
            .zip(slices.windows(2))
            .map(|(transition, pair)| {
                let price = |catalog: &mut ParquetDataCatalog, slice: &Slice, last: bool| {
                    roll_price(catalog, slice, last).with_context(|| {
                        format!(
                            "pricing the roll of {} to {} at {}",
                            transition.pre, transition.post, transition.time
                        )
                    })
                };
                Ok(json!({
                    "transition_time_ns": UnixNanos::from(transition.time).as_u64(),
                    "pre_instrument_id": transition.pre.to_string(),
                    "post_instrument_id": transition.post.to_string(),
                    "pre_price": price(catalog, &pair[0], true)?.to_string(),
                    "post_price": price(catalog, &pair[1], false)?.to_string(),
                }))
            })
            .collect::<anyhow::Result<Vec<Value>>>()?;
        Ok(Value::Array(rows))
    }
}

/// The price of the last trade of `slice`'s contract in it if `last`, of
/// the first otherwise, read `READ_WINDOW` at a time from that end.
fn roll_price(
    catalog: &mut ParquetDataCatalog,
    slice: &Slice,
    last: bool,
) -> anyhow::Result<Price> {
    let mut windows = Vec::new();
    let mut from = slice.from;
    while from <= slice.to {
        let to = (from + READ_WINDOW - SignedDuration::from_nanos(1)).min(slice.to);
        windows.push((from, to));
        from = to + SignedDuration::from_nanos(1);
    }
    if last {
        windows.reverse();
    }
    let id = slice.instrument_id;
    for (from, to) in windows {
        let trades: Vec<TradeTick> = catalog
            .query(
                Some(vec![id.to_string()]),
                Some(UnixNanos::from(from)),
                Some(UnixNanos::from(to)),
                None,
                None,
                true,
            )
            .with_context(|| format!("reading the trades of {id} over [{from}, {to}]"))?;
        let trade = if last { trades.last() } else { trades.first() };
        if let Some(trade) = trade {
            return Ok(trade.price);
        }
    }
    anyhow::bail!(
        "no trade of {id} in its slice [{}, {}]",
        slice.from,
        slice.to
    )
}

#[cfg(test)]
mod tests {
    use nautilus_model::{enums::AggressorSide, identifiers::TradeId, types::Quantity};

    use super::*;

    fn at(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    /// A trade of `id` at `time` and `price`.
    fn trade(id: &str, time: &str, price: &str) -> TradeTick {
        let time = at(time);
        TradeTick::new(
            InstrumentId::from(id),
            Price::from(price),
            Quantity::from(1),
            AggressorSide::Buy,
            TradeId::from(time.as_nanosecond().to_string().as_str()),
            time.into(),
            time.into(),
        )
    }

    #[test]
    fn a_roll_is_priced_from_the_trades_nearest_it_days_away() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        // August's contract trades days before the roll (00:00 on Wednesday
        // 29 July) and not on its eve; December's not on the day of the
        // roll.
        let pre = [
            trade("GCQ6.GLBX", "2026-07-24T10:00Z", "3980.00"),
            trade("GCQ6.GLBX", "2026-07-27T10:00Z", "3985.00"),
        ];
        let post = [
            trade("GCZ6.GLBX", "2026-08-03T10:00Z", "4040.00"),
            trade("GCZ6.GLBX", "2026-08-04T10:00Z", "4045.00"),
        ];
        catalog.write_to_parquet(&pre, None, None, None).unwrap();
        catalog.write_to_parquet(&post, None, None, None).unwrap();

        let continuous = ContinuousBars::read(
            dir.path(),
            &[BarType::from("GC.GLBX-1-MINUTE-LAST-INTERNAL")],
            at("2026-07-24T00:00Z"),
            at("2026-08-07T00:00Z"),
            ContinuousFutureAdjustmentType::BackwardSpread,
        )
        .unwrap();

        let [(_, transitions), _] = continuous.params(InstrumentId::from("GC.GLBX")).unwrap();
        assert_eq!(transitions[0]["pre_price"], json!("3985.00"));
        assert_eq!(transitions[0]["post_price"], json!("4040.00"));
    }

    /// Reads the continuous bars of `bar_type` over July 2026 from an
    /// empty catalog.
    fn read(bar_type: &str) -> anyhow::Result<ContinuousBars> {
        let dir = tempfile::tempdir().unwrap();
        ContinuousBars::read(
            dir.path(),
            &[BarType::from(bar_type)],
            at("2026-07-01T00:00Z"),
            at("2026-07-31T12:00Z"),
            ContinuousFutureAdjustmentType::BackwardSpread,
        )
    }

    #[test]
    fn bars_on_a_plain_instrument_read_nothing() {
        let continuous = read("GCZ6.GLBX-1-MINUTE-LAST-INTERNAL").unwrap();
        assert_eq!(continuous, ContinuousBars::default());
    }

    #[test]
    fn a_root_without_a_roll_over_the_span_is_refused_naming_its_contract() {
        let dir = tempfile::tempdir().unwrap();
        let err = ContinuousBars::read(
            dir.path(),
            &[BarType::from("GC.GLBX-1-MINUTE-LAST-INTERNAL")],
            at("2026-08-03T00:00Z"),
            at("2026-08-07T00:00Z"),
            ContinuousFutureAdjustmentType::BackwardSpread,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("no roll of GC.GLBX"), "{err}");
        assert!(err.contains("GCZ6.GLBX"), "{err}");
    }
}
