//! Runs the strategy on a Nautilus live node.
//!
//! Build step 3: paper, then live. One node per process. The clients are
//! those of the instruments' venues: OKX's for OKX instruments (`okx.rs`),
//! IB's for the others (`ib.rs`), both if the instruments span both. The
//! start brings the catalog up to now for the IB instruments, and the node
//! serves their warmup from it. OKX instruments have no recent quotes to
//! warm up on yet, so a run with any refuses to start (see `okx.rs`).

mod ib;
mod node;
mod okx;

pub use crate::data::IbAccount;

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
use nautilus_model::{
    data::{BarType, QuoteTick},
    identifiers::InstrumentId,
    instruments::InstrumentAny,
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_trading::strategy::{Strategy, StrategyNative};
use rust_decimal::Decimal;

use crate::{
    data::{IbHistory, OkxHistory, fill_catalog},
    strategy::{DataRequirements, warmup},
};

/// One live run: the strategy trades its IB instruments in `account`
/// through IB with `client_id`, and its OKX instruments on OKX's demo at
/// `leverage`; on bars the node builds from the venues' quotes, warmed up
/// from the catalog.
#[derive(Debug, Clone)]
pub struct Live {
    /// The catalog directory: the start fills it up to now, and the node
    /// serves the warmup from it.
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
    /// Sets up the clients of the instruments of `requirements`, OKX's then
    /// IB's, fills
    /// the catalog with the quotes of the IB ones up to now, from
    /// `now - warmup` or from where the catalog ends, then runs `strategy`
    /// on a live node until SIGINT or SIGTERM. The node asks IB only for
    /// the quotes since the fill.
    ///
    /// # Errors
    ///
    /// Returns an error if the cache cannot hold the warmup bars of a bar
    /// type (see `warmup::cache_config`), if OKX's clients cannot be set up
    /// or the OKX account does not suit the run (see `okx::clients`), if
    /// the strategy trades any OKX instrument, which has no quotes to warm
    /// up on yet (see `okx::ensure_quote_history`), if
    /// the strategy trades IB instruments and no account is given, if IB's
    /// clients cannot be set up (see `ib::clients`), if the catalog cannot
    /// be filled (IB unreachable, an instrument IB does not know, a catalog
    /// starting after `now - warmup`), or if the node fails (see
    /// `node::run`).
    pub async fn run<S>(&self, requirements: &DataRequirements, strategy: S) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        let cache = warmup::cache_config(&requirements.bar_types, requirements.warmup)?;
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
        let (okx_ids, ib_ids): (Vec<InstrumentId>, Vec<InstrumentId>) = instrument_ids
            .iter()
            .partition(|&&id| OkxHistory::serves(id));
        let mut clients = Vec::new();
        if !okx_ids.is_empty() {
            let okx = okx::clients(&okx_ids, self.leverage).await?;
            okx::ensure_quote_history(&okx_ids)?;
            clients.push(okx.into_node_clients());
        }
        if !ib_ids.is_empty() {
            let account = self.account.as_ref().context(
                "the strategy trades IB instruments: give the IB account \
                 (--account or NAUTILUS_IB_ACCOUNT_ID)",
            )?;
            let ib = ib::clients(account, self.client_id, &ib_ids).await?;
            clients.push(ib.into_node_clients());
            self.fill_catalog(&of_instruments(requirements, &ib_ids))
                .await?;
        }
        node::run(
            &self.catalog,
            self.logging.clone(),
            cache,
            clients,
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
        let ib = IbHistory::connect(self.client_id, end, now).await?;
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

/// The requirements of `requirements` on `instrument_ids` alone.
fn of_instruments(
    requirements: &DataRequirements,
    instrument_ids: &[InstrumentId],
) -> DataRequirements {
    DataRequirements {
        bar_types: requirements
            .bar_types
            .iter()
            .filter(|bar_type| instrument_ids.contains(&bar_type.instrument_id()))
            .copied()
            .collect::<Vec<BarType>>(),
        warmup: requirements.warmup,
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
