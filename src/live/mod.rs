//! Runs the strategy on a Nautilus live node.
//!
//! Build step 3: paper, then live. One node per process. The clients are
//! those of the instruments' venues: OKX's for OKX instruments (`okx/`),
//! IC's data client for IC instruments (`ic/`), IB's for the others
//! (`ib/`), several if the instruments span several. The start brings the
//! catalog up to now for the IB and IC instruments, and the node serves
//! their warmup from it, IC's data client the rest up to now. OKX's data
//! client serves the warmup of the OKX instruments, OKX's candles up to now
//! (see `okx/`).

mod ib;
mod ic;
mod node;
mod okx;

pub use ic::{IcDataClientConfig, IcDataClientFactory};

pub use crate::data::IbAccount;

use std::{
    fmt::Debug,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, ensure};
use jiff::Timestamp;
use nautilus_common::{
    actor::DataActorNative,
    component::Component,
    logging::{bridge::init_tracing, init_logging, logger::LoggerConfig},
};
use nautilus_core::UUID4;
use nautilus_model::{
    data::{Data, NautilusDataType},
    identifiers::InstrumentId,
    instruments::InstrumentAny,
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_trading::strategy::{Strategy, StrategyNative};
use rust_decimal::Decimal;

use crate::{
    data::{IbHistory, IcConnection, IcHistory, OkxHistory, fill_catalog},
    strategy::{DataRequirements, catalog_series, warmup},
};

/// One live run: the strategy trades its IB instruments in `account`
/// through IB with `client_id`, and its OKX instruments on the OKX
/// environment `OKX_ENVIRONMENT` names at `leverage`; it gets the data of
/// its IC instruments on the IC environment `IC_ENVIRONMENT` names, with no
/// execution client yet; on bars the node builds, warmed up from the
/// catalog and the venues.
#[derive(Debug, Clone)]
pub struct Live {
    /// The catalog directory: the start fills it up to now with the IB
    /// history, and the node serves the warmup from it, asking the
    /// venues for what it misses.
    pub catalog: PathBuf,
    /// The IB account traded; needed only if the strategy trades IB
    /// instruments.
    pub account: Option<IbAccount>,
    /// The IB client id, for the history the start fills the catalog with
    /// and for the node's clients, one after the other.
    pub client_id: i32,
    /// The leverage the run expects. OKX applies the account's leverage,
    /// set by hand: the start refuses an OKX instrument at another one.
    pub leverage: Decimal,
    /// The Nautilus logger, installed for the start and kept by the node.
    pub logging: LoggerConfig,
}

impl Live {
    /// Sets up the clients of the instruments of `requirements`, OKX's, IC's
    /// then IB's, fills the catalog with the quotes of the IC and IB ones up
    /// to now, from `now - warmup` or from where the catalog ends, then runs
    /// `strategy` on a live node until SIGINT or SIGTERM. The node asks IC
    /// and IB only for the quotes since the fill.
    ///
    /// # Errors
    ///
    /// Returns an error if the cache cannot hold the warmup bars of a bar
    /// type (see `warmup::cache_config`), if the strategy trades IC
    /// instruments with IB ones, or IC's environment or credential is
    /// missing (see `ic::clients`, checked before anything connects), if
    /// OKX's clients cannot be set up or the OKX account does not suit the
    /// run (see `okx::clients`), if the strategy trades IB instruments and
    /// no account is given, if IB's clients cannot be set up (see
    /// `ib::clients`), if the catalog cannot be filled (IC or IB
    /// unreachable, an instrument they do not know, a catalog starting after
    /// `now - warmup`, IC's market closed over the whole warmup), or if the
    /// node fails (see `node::run`).
    pub async fn run<S>(&self, requirements: &DataRequirements, strategy: S) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        let cache = warmup::cache_config(&requirements.bar_types, requirements.warmup)?;
        let data_engine = warmup::live_data_engine_config(&requirements.bar_types);
        let instrument_ids = requirements.instrument_ids();
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
        let (okx_ids, others): (Vec<InstrumentId>, Vec<InstrumentId>) = instrument_ids
            .iter()
            .partition(|&&id| OkxHistory::serves(id));
        let (ic_ids, ib_ids): (Vec<InstrumentId>, Vec<InstrumentId>) =
            others.into_iter().partition(|&id| IcHistory::serves(id));
        // Read before anything connects.
        let ic = if ic_ids.is_empty() {
            None
        } else {
            ensure!(
                ib_ids.is_empty(),
                "IC instruments cannot be traded with IB ones yet: IB's execution client is \
                 the default route, so it would get IC's orders until IC has its own"
            );
            Some(ic::clients(&ic_ids, |name| std::env::var(name).ok())?)
        };
        let mut clients = Vec::new();
        if !okx_ids.is_empty() {
            let okx = okx::clients(&okx_ids, self.leverage).await?;
            clients.push(okx.into_node_clients());
        }
        if let Some(ic) = ic {
            self.fill_ic_catalog(ic.connection(), &requirements.of_instruments(&ic_ids))
                .await?;
            clients.push(ic.into_node_clients());
        }
        if !ib_ids.is_empty() {
            let account = self.account.as_ref().context(
                "the strategy trades IB instruments: give the IB account \
                 (--account or NAUTILUS_IB_ACCOUNT_ID)",
            )?;
            let ib = ib::clients(account, self.client_id, &ib_ids).await?;
            clients.push(ib.into_node_clients());
            self.fill_catalog(&requirements.of_instruments(&ib_ids))
                .await?;
        }
        // The node opens the catalog, which a run on OKX alone never fills.
        fs::create_dir_all(&self.catalog)
            .with_context(|| format!("creating the catalog {}", self.catalog.display()))?;
        node::run(
            &self.catalog,
            self.logging.clone(),
            cache,
            data_engine,
            clients,
            &instrument_ids,
            strategy,
        )
        .await
    }

    /// Fills the catalog up to now from IC's history, as `fill-catalog`
    /// does, over `[now - warmup, now]`; the node asks IC's data client for
    /// the quotes since.
    async fn fill_ic_catalog(
        &self,
        connection: &IcConnection,
        requirements: &DataRequirements,
    ) -> anyhow::Result<()> {
        let now = Timestamp::now();
        let start = now
            .checked_sub(requirements.warmup)
            .with_context(|| format!("no warmup start {} before {now}", requirements.warmup))?;
        fs::create_dir_all(&self.catalog)
            .with_context(|| format!("creating the catalog {}", self.catalog.display()))?;
        let mut catalog = ParquetDataCatalog::new(&self.catalog, None, None, None, None);
        let ic = IcHistory::connect_as(connection).await?;
        let filled = ic
            .fill(
                &mut catalog,
                &catalog_series(&requirements.bar_types),
                start,
                now,
            )
            .await;
        ic.disconnect().await;
        filled
    }

    /// Fills the catalog up to now from IB history, as `fill-catalog` does,
    /// and disconnects before the node connects with the same client id.
    async fn fill_catalog(&self, requirements: &DataRequirements) -> anyhow::Result<()> {
        let now = Timestamp::now();
        let end = IbHistory::latest_end(now)?;
        let ib = IbHistory::connect(self.client_id, end, now).await?;
        fill_catalog_to_now(
            &self.catalog,
            requirements,
            now,
            async |ids: &[InstrumentId]| ib.instruments(ids).await,
            async |data_type, identifier, from, to| ib.data(data_type, identifier, from, to).await,
        )
        .await
    }
}

/// Fills the catalog in `catalog_dir`, created if missing, with the
/// instruments of `requirements`, fetched through `fetch_instruments`, and
/// with the series their bars are built from (see
/// `strategy::catalog_series`) over `[now - warmup,
/// IbHistory::latest_end(now)]`, fetched one series at a time through
/// `fetch`; the node asks IB for the data since. A stored series resumes
/// after its end (see `data::fill_catalog`).
async fn fill_catalog_to_now(
    catalog_dir: &Path,
    requirements: &DataRequirements,
    now: Timestamp,
    fetch_instruments: impl AsyncFnOnce(&[InstrumentId]) -> anyhow::Result<Vec<InstrumentAny>>,
    fetch: impl AsyncFnMut(&NautilusDataType, &str, Timestamp, Timestamp) -> anyhow::Result<Vec<Data>>,
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
        &catalog_series(&requirements.bar_types),
        start,
        IbHistory::latest_end(now)?,
        fetch,
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
        let ts = UnixNanos::from(ts);
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
            async |data_type, identifier, from, to| {
                assert_eq!(*data_type, NautilusDataType::QuoteTick);
                let id = InstrumentId::from(identifier);
                asked_quotes.push((id, from, to));
                Ok(vec![Data::Quote(quote(id, to))])
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
