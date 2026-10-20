//! Runs the strategy on a Nautilus live node.
//!
//! Build step 3: paper, then live. One node per process. The start brings
//! the catalog up to now, then the node serves the strategy's warmup from
//! it.

mod ib;
mod node;

pub use ib::IbAccount;

use std::{
    fmt::Debug,
    fs,
    path::{Path, PathBuf},
};

use anyhow::Context;
use jiff::Timestamp;
use nautilus_common::{
    actor::DataActorNative,
    component::Component,
    logging::{bridge::init_tracing, init_logging, logger::LoggerConfig},
};
use nautilus_core::UUID4;
use nautilus_model::{data::QuoteTick, identifiers::InstrumentId, instruments::InstrumentAny};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_trading::strategy::{Strategy, StrategyNative};

use crate::{
    data::{IbGateway, IbHistory, fill_catalog},
    strategy::{DataRequirements, warmup},
};

/// One live run: the strategy trades `account` through the IB Gateway or
/// TWS at `gateway`, on bars the node builds from IB's quotes, warmed up
/// from the catalog.
#[derive(Debug, Clone)]
pub struct Live {
    /// The catalog directory: the start fills it up to now, and the node
    /// serves the warmup from it.
    pub catalog: PathBuf,
    /// The gateway, for the history the start fills the catalog with and
    /// for the node's clients, one after the other with the same client id.
    pub gateway: IbGateway,
    /// The account traded; its kind (paper or live) must match the port.
    pub account: IbAccount,
    /// The Nautilus logger, installed for the start and kept by the node.
    pub logging: LoggerConfig,
}

impl Live {
    /// Fills the catalog with the quotes of the instruments of
    /// `requirements` up to now, from `now - warmup` or from where the
    /// catalog ends, then runs `strategy` on a live node until SIGINT or
    /// SIGTERM. The node asks IB only for the quotes since the fill.
    ///
    /// # Errors
    ///
    /// Returns an error if the account does not match the port, if the
    /// cache cannot hold the warmup bars of a bar type (see
    /// `warmup::cache_config`), if the catalog cannot be filled (IB
    /// unreachable, an instrument IB does not know, a catalog starting after
    /// `now - warmup`), or if the node fails (see `node::run`).
    pub async fn run<S>(&self, requirements: &DataRequirements, strategy: S) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        let cache = warmup::cache_config(&requirements.bar_types, requirements.warmup)?;
        let instrument_ids = requirements.instrument_ids();
        let clients = ib::clients(&self.gateway, &self.account, &instrument_ids)?;
        // The node would install the logger when built, after the fill:
        // install it now with the node's trader and config, and the node
        // keeps it. The adapter's `tracing` output joins it.
        let file_config = self.logging.file_config.clone().unwrap_or_default();
        let _log_guard = init_logging(
            node::trader_id(),
            UUID4::new(),
            self.logging.clone(),
            file_config,
        )?;
        init_tracing()?;
        self.fill_catalog(requirements).await?;
        node::run(
            &self.catalog,
            self.logging.clone(),
            cache,
            clients.into_node_clients(),
            &instrument_ids,
            strategy,
        )
        .await
    }

    /// Fills the catalog up to now from IB history, as `fill-catalog` does,
    /// and disconnects before the node connects with the same client id.
    async fn fill_catalog(&self, requirements: &DataRequirements) -> anyhow::Result<()> {
        let now = Timestamp::now();
        let end = IbHistory::latest_end(now)?;
        let ib = IbHistory::connect(self.gateway.clone(), end, now).await?;
        fill_catalog_to_now(
            &self.catalog,
            requirements,
            now,
            async |ids: &[InstrumentId]| ib.instruments(ids).await,
            async |id, from, to| ib.quotes(id, from, to).await,
        )
        .await
    }
}

/// Fills the catalog in `catalog_dir`, created if missing, with the
/// instruments of `requirements`, fetched through `fetch_instruments`, and
/// with their quotes over `[now - warmup, IbHistory::latest_end(now)]`,
/// fetched one instrument at a time through `fetch_quotes`; the node asks
/// IB for the quotes since. A stored series resumes
/// after its end (see `data::fill_catalog`).
async fn fill_catalog_to_now(
    catalog_dir: &Path,
    requirements: &DataRequirements,
    now: Timestamp,
    fetch_instruments: impl AsyncFnOnce(&[InstrumentId]) -> anyhow::Result<Vec<InstrumentAny>>,
    fetch_quotes: impl AsyncFnMut(InstrumentId, Timestamp, Timestamp) -> anyhow::Result<Vec<QuoteTick>>,
) -> anyhow::Result<()> {
    let start = now
        .checked_sub(requirements.warmup)
        .with_context(|| format!("no warmup start {} before {now}", requirements.warmup))?;
    let instruments = fetch_instruments(&requirements.instrument_ids()).await?;
    fs::create_dir_all(catalog_dir)
        .with_context(|| format!("creating the catalog {}", catalog_dir.display()))?;
    let mut catalog = ParquetDataCatalog::new(catalog_dir, None, None, None, None);
    fill_catalog(
        &mut catalog,
        instruments,
        start,
        IbHistory::latest_end(now)?,
        fetch_quotes,
    )
    .await
}

#[cfg(test)]
mod tests {
    use jiff::{SignedDuration, Timestamp};
    use nautilus_core::UnixNanos;
    use nautilus_model::{
        data::{BarType, QuoteTick},
        identifiers::InstrumentId,
        instruments::{
            Instrument, InstrumentAny,
            stubs::{equity_aapl, futures_contract_es},
        },
        types::{Price, Quantity},
    };
    use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

    use super::*;
    use crate::strategy::DataRequirements;

    fn instruments() -> Vec<InstrumentAny> {
        vec![
            InstrumentAny::Equity(equity_aapl()),
            InstrumentAny::FuturesContract(futures_contract_es(None, None)),
        ]
    }

    fn requirements() -> DataRequirements {
        DataRequirements {
            bar_types: instruments()
                .iter()
                .map(|instrument| {
                    BarType::from(format!("{}-1-HOUR-MID-INTERNAL", instrument.id()).as_str())
                })
                .collect(),
            warmup: SignedDuration::from_hours(2),
        }
    }

    fn at(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    /// A quote of `id` stamped `ts`.
    fn quote(id: InstrumentId, ts: Timestamp) -> QuoteTick {
        let ts = UnixNanos::from(u64::try_from(ts.as_nanosecond()).unwrap());
        QuoteTick::new(
            id,
            Price::from("100.00"),
            Price::from("100.25"),
            Quantity::from(1_000_000),
            Quantity::from(1_000_000),
            ts,
            ts,
        )
    }

    /// Fills the catalog in `dir` up to `now` from fake sources answering
    /// each window with one quote at its end; returns the instruments and
    /// windows asked for.
    async fn fill(
        dir: &std::path::Path,
        now: Timestamp,
    ) -> (Vec<InstrumentId>, Vec<(InstrumentId, Timestamp, Timestamp)>) {
        let mut asked_instruments = Vec::new();
        let mut asked_quotes = Vec::new();
        fill_catalog_to_now(
            dir,
            &requirements(),
            now,
            async |ids: &[InstrumentId]| {
                asked_instruments.extend_from_slice(ids);
                Ok(instruments())
            },
            async |id, from, to| {
                asked_quotes.push((id, from, to));
                Ok(vec![quote(id, to)])
            },
        )
        .await
        .unwrap();
        (asked_instruments, asked_quotes)
    }

    #[tokio::test]
    async fn the_start_fills_a_new_catalog_over_the_warmup_up_to_a_minute_ago() {
        let dir = tempfile::tempdir().unwrap();
        let catalog_dir = dir.path().join("catalog");
        let now = at("2026-10-20T10:30:00Z");
        let (asked_instruments, asked_quotes) = fill(&catalog_dir, now).await;
        let ids = requirements().instrument_ids();
        assert_eq!(asked_instruments, ids);
        let (from, to) = (at("2026-10-20T08:30:00Z"), at("2026-10-20T10:29:00Z"));
        assert_eq!(asked_quotes, [(ids[0], from, to), (ids[1], from, to)]);
        let mut catalog = ParquetDataCatalog::new(&catalog_dir, None, None, None, None);
        assert_eq!(catalog.instruments(None, None, None).unwrap().len(), 2);
        let stored = catalog
            .quote_ticks(Some(vec![ids[0].to_string()]), None, None)
            .unwrap();
        assert_eq!(stored, [quote(ids[0], to)]);
    }

    #[tokio::test]
    async fn a_later_start_fetches_only_the_quotes_since_the_last_one() {
        let dir = tempfile::tempdir().unwrap();
        let first = at("2026-10-20T10:30:00Z");
        fill(dir.path(), first).await;
        let now = at("2026-10-20T11:00:00Z");
        let (_, asked_quotes) = fill(dir.path(), now).await;
        let ids = requirements().instrument_ids();
        let resume = at("2026-10-20T10:29:00.000000001Z");
        let to = at("2026-10-20T10:59:00Z");
        assert_eq!(asked_quotes, [(ids[0], resume, to), (ids[1], resume, to)]);
    }
}
