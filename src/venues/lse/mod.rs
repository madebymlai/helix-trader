//! LSE history of `LSEDGE` instruments, London Strategic Edge's own
//! derived series, each an index instrument named after its LSE market
//! (`GOLD-FUTURES.LSEDGE`: GOLD in `futures`), filled through the node (see
//! `catalog::NodeFill`) by the LSE adapter's data client (`lse`), whose
//! answers the data engine writes back to the catalog: trades, from LSE's
//! tick slices, and bars, from its candles, each stamped at its close.
//! Their definitions are the catalog's alone, written there by
//! hand: LSE publishes none, the data engine answers a definition request
//! from the catalog before asking the client, and the client converts
//! LSE's prices at the definition's precision. See `README.md`.

use ::lse::{LseDataClientConfig, LseDataClientFactory};
use jiff::SignedDuration;
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

use crate::{
    catalog::{FillData, NodeClient, NodeFill, missing},
    strategy::SeriesSlice,
};

/// The window of one request of a series: a day, as Databento's. The
/// client asks LSE a UTC day an export whatever the window.
const DAY: SignedDuration = SignedDuration::from_hours(24);

/// Adds to `node` the fill of what the catalog lacks of `slices`, series of
/// `LSEDGE` instruments (see `catalog::missing`), asked of the LSE client
/// `config` configures, its key read only when the node creates it, so
/// only if something is missing. Each contract's definition is asked of
/// the client too: the data engine answers it from the catalog, before the
/// client, and puts it in the cache, where the client reads the precision
/// of its prices; a definition the catalog lacks leaves the series' request
/// to fail naming the instrument.
///
/// # Errors
///
/// Returns an error if a series is not one the node fills (see `missing`),
/// or if the catalog cannot be read.
pub(super) fn add_fill(
    node: &mut NodeFill,
    catalog: &ParquetDataCatalog,
    slices: &[SeriesSlice],
    config: LseDataClientConfig,
) -> anyhow::Result<()> {
    let requests = missing(catalog, slices)?;
    node.add(node_client(config), &requests, period);
    Ok(())
}

/// The window one request of any series holds at most (see
/// `NodeFill::add`): a day.
fn period(_data: FillData) -> SignedDuration {
    DAY
}

/// The node's data client: the LSE adapter's, on no venue, so asked only
/// by the requests naming it (see `NodeFill::add`).
fn node_client(config: LseDataClientConfig) -> NodeClient {
    NodeClient {
        factory: Box::new(LseDataClientFactory::new()),
        config: Box::new(config),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, path::Path};

    use ::lse::testing::{
        API_KEY, GOLD_FUTURES, GOLD_SPOT, GOLD_SPOT_MINUTES, GOLD_TRADES, MockLse, PLAN_REQUIRED,
        Reply, SYMBOL_UNKNOWN, gold_futures, gold_spot,
    };
    use jiff::Timestamp;
    use nautilus_core::{UnixNanos, string::secret::SecretString};
    use nautilus_model::{
        data::{BarType, NautilusDataType, TradeTick},
        enums::AggressorSide,
        identifiers::InstrumentId,
        instruments::InstrumentAny,
        types::{Price, Quantity},
    };

    use super::*;
    use crate::strategy::{DataRequirements, TickSeries};

    /// LSE lists no such series.
    const NOSUCH: &str = "NOSUCH-FUTURES.LSEDGE";

    fn at(time: &str) -> Timestamp {
        format!("2026-10-21T{time}Z").parse().unwrap()
    }

    /// `GOLD_FUTURES`'s definition, as `id`'s.
    fn defined_as(id: &str) -> InstrumentAny {
        let InstrumentAny::IndexInstrument(mut index) = gold_futures() else {
            unreachable!("an index");
        };
        index.id = InstrumentId::from(id);
        InstrumentAny::IndexInstrument(index)
    }

    /// fill-catalog's fill of the trades of `id` over the captured second,
    /// `[10:00:00, 10:00:01)`, into the catalog in `dir`, from `lse`, the
    /// catalog holding `definitions`.
    fn fill(
        dir: &Path,
        lse: &MockLse,
        id: &str,
        definitions: Vec<InstrumentAny>,
    ) -> anyhow::Result<()> {
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: vec![TickSeries {
                data: NautilusDataType::TradeTick,
                instrument_id: InstrumentId::from(id),
            }],
            warmup: SignedDuration::ZERO,
        };
        fill_over(
            dir,
            lse,
            &requirements,
            (at("10:00:00"), at("10:00:00.999999999")),
            definitions,
        )
    }

    /// fill-catalog's fill of `requirements` over `[from, to]` into the
    /// catalog in `dir`, from `lse`, the catalog holding `definitions`.
    fn fill_over(
        dir: &Path,
        lse: &MockLse,
        requirements: &DataRequirements,
        (from, to): (Timestamp, Timestamp),
        definitions: Vec<InstrumentAny>,
    ) -> anyhow::Result<()> {
        let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
        if !definitions.is_empty() {
            catalog.write_instruments(definitions).unwrap();
        }
        let slices = requirements.slices(from, to);
        let config = LseDataClientConfig::builder()
            .api_key(SecretString::from(API_KEY))
            .base_url_http(lse.url().to_string())
            .build();
        let mut node = NodeFill::new();
        add_fill(&mut node, &catalog, &slices, config)?;
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(node.run(dir))
    }

    fn stored_trades(dir: &Path) -> Vec<TradeTick> {
        ParquetDataCatalog::new(dir, None, None, None, None)
            .trade_ticks(Some(vec![GOLD_FUTURES.to_string()]), None, None)
            .unwrap()
    }

    /// The captured second, as pyarrow reads LSE's file.
    #[test]
    fn the_trades_lse_exports_are_written_back_to_the_catalog() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);

        fill(dir.path(), &lse, GOLD_FUTURES, vec![gold_futures()]).unwrap();

        let stored = stored_trades(dir.path());
        let rows: Vec<_> = stored
            .iter()
            .map(|t| (t.ts_event, t.price, t.size, t.aggressor_side))
            .collect();
        let row = |time, price, size| {
            (
                UnixNanos::from(at(time)),
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
        let ids: HashSet<_> = stored.iter().map(|t| t.trade_id).collect();
        assert_eq!(ids.len(), stored.len());
        let [export] = &lse.calls()[..] else {
            panic!("{:?}", lse.calls())
        };
        assert_eq!(
            (export.param("from"), export.param("to")),
            (Some("2026-10-21T10:00:00Z"), Some("2026-10-21T10:00:01Z"))
        );
    }

    /// The captured five minutes of gold spot, candles LSE stamps at 10:00
    /// to 10:04, their open, as pyarrow reads them; the bars closing over
    /// `[10:01, 10:05]`.
    #[test]
    fn lse_s_candles_are_written_back_to_the_catalog_as_bars_stamped_at_their_close() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![Reply::file(GOLD_SPOT_MINUTES)]);
        let bars = format!("{GOLD_SPOT}-1-MINUTE-LAST-EXTERNAL");
        let requirements = DataRequirements {
            bar_types: vec![BarType::from(bars.as_str())],
            ticks: vec![],
            warmup: SignedDuration::ZERO,
        };

        fill_over(
            dir.path(),
            &lse,
            &requirements,
            (at("10:01:00"), at("10:05:00")),
            vec![gold_spot()],
        )
        .unwrap();

        let stored = ParquetDataCatalog::new(dir.path(), None, None, None, None)
            .bars(Some(vec![bars.clone()]), None, None)
            .unwrap();
        let rows: Vec<_> = stored
            .iter()
            .map(|b| (b.ts_event, b.open, b.high, b.low, b.close, b.volume))
            .collect();
        let row = |close, [open, high, low, last]: [&str; 4]| {
            (
                UnixNanos::from(at(close)),
                Price::from(open),
                Price::from(high),
                Price::from(low),
                Price::from(last),
                Quantity::from(0),
            )
        };
        assert_eq!(
            rows,
            [
                row("10:01:00", ["4118.01", "4119.21", "4116.91", "4118.01"]),
                row("10:02:00", ["4118.15", "4118.24", "4116.25", "4116.94"]),
                row("10:03:00", ["4116.97", "4117.27", "4115.27", "4116.17"]),
                row("10:04:00", ["4116.02", "4116.93", "4115.62", "4116.12"]),
                row("10:05:00", ["4116.25", "4118.36", "4116.25", "4118.16"]),
            ]
        );
        let [export] = &lse.calls()[..] else {
            panic!("{:?}", lse.calls())
        };
        assert_eq!(
            (
                export.param("timeframe"),
                export.param("from"),
                export.param("to")
            ),
            (
                Some("1m"),
                Some("2026-10-21T10:00:00Z"),
                Some("2026-10-21T10:05:00Z")
            )
        );
    }

    #[test]
    fn lse_s_symbol_unknown_fails_the_fill_naming_the_request() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![Reply::refusal(404, SYMBOL_UNKNOWN)]);

        let err = fill(dir.path(), &lse, NOSUCH, vec![defined_as(NOSUCH)])
            .unwrap_err()
            .to_string();

        assert!(err.contains(&format!("the trades of {NOSUCH}")), "{err}");
        assert!(err.contains("symbol-unknown"), "{err}");
    }

    /// LSE is never asked: its prices could not be read.
    #[test]
    fn a_fill_of_an_lsedge_instrument_with_no_definition_fails_naming_it() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);

        let err = fill(dir.path(), &lse, GOLD_FUTURES, vec![])
            .unwrap_err()
            .to_string();

        assert!(
            err.contains(&format!("{GOLD_FUTURES} is not in the cache")),
            "{err}"
        );
        assert!(lse.calls().is_empty(), "{:?}", lse.calls());
        assert_eq!(stored_trades(dir.path()), []);
    }

    #[test]
    fn lse_s_refusal_fails_the_fill_with_its_code_message_and_required_plan() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![Reply::refusal(403, PLAN_REQUIRED)]);

        let err = fill(dir.path(), &lse, GOLD_FUTURES, vec![gold_futures()])
            .unwrap_err()
            .to_string();

        for part in [
            "plan-required",
            "This feed requires a subscription",
            "CME Group Lite",
        ] {
            assert!(err.contains(part), "{part}: {err}");
        }
    }
}
