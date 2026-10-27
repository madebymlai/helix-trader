//! Runs the strategy on a Nautilus live node.
//!
//! Build step 3: paper, then live. One node per process. The clients are
//! those of the instruments' venues: OKX's for OKX instruments (`okx/`),
//! IC's for IC instruments (`ic/`, a hedging account), IB's for the others
//! (`ib/`), several if the instruments span several. Each venue's data
//! client serves the warmup of its instruments up to now: OKX's candles
//! (see `okx/`), IC's quotes from cTrader's tick history (see `ic/`), IB's
//! 1-minute bars (see `ib/`). The node registers the catalog fill-catalog
//! fills and backtests read: the warmup comes from it first, from the
//! venues for what it lacks, and IC's answers are written back to it (see
//! `strategy::warmup`).

mod ib;
mod ic;
mod node;
mod okx;
mod redis;

pub use ic::{
    IcDataClientConfig, IcDataClientFactory, IcExecutionClientConfig, IcExecutionClientFactory,
};

pub use crate::data::IbAccount;

use std::{fmt::Debug, path::PathBuf};

use anyhow::Context;
use nautilus_common::{
    actor::DataActorNative,
    component::Component,
    logging::{bridge::init_tracing, init_logging, logger::LoggerConfig},
};
use nautilus_core::UUID4;
use nautilus_model::identifiers::{InstrumentId, TraderId};
use nautilus_persistence::config::DataCatalogConfig;
use nautilus_trading::strategy::{Strategy, StrategyNative};
use rust_decimal::Decimal;

use crate::{
    data::{IcHistory, OkxHistory, catalog_path},
    strategy::{DataRequirements, warmup},
};

/// One live run: the strategy trades its IB instruments in `account`
/// through IB with `client_id`, and its OKX instruments on the OKX
/// environment `OKX_ENVIRONMENT` names at `leverage`, and its IC
/// instruments on the IC environment `IC_ENVIRONMENT` names, a hedging
/// account; on bars the node builds, warmed up from the venues.
#[derive(Debug, Clone)]
pub struct Live {
    /// The node's trader. The node's cache in Redis is the trader's, so a
    /// restart under the same trader loads what the last run left.
    pub trader_id: TraderId,
    /// The IB account traded; needed only if the strategy trades IB
    /// instruments.
    pub account: Option<IbAccount>,
    /// The IB client id of the node's IB clients.
    pub client_id: i32,
    /// The leverage the run expects. OKX applies the account's leverage,
    /// set by hand: the start refuses an OKX instrument at another one.
    pub leverage: Decimal,
    /// The Nautilus logger, installed for the start and kept by the node.
    pub logging: LoggerConfig,
    /// The catalog directory, fill-catalog's and the backtests': it serves
    /// the warmup, and keeps the history the venues certify complete (see
    /// `node::run`). Created if missing.
    pub catalog: PathBuf,
}

impl Live {
    /// Sets up the clients of the instruments of `requirements`, OKX's, IC's
    /// then IB's, then runs `strategy` on a live node until SIGINT or
    /// SIGTERM. The strategy's warmup requests go to the data clients.
    ///
    /// # Errors
    ///
    /// Returns an error if the cache cannot hold the warmup bars of a bar
    /// type (see `warmup::cache_config`), if the catalog cannot be created
    /// or its path is not UTF-8, if an IC bar type is not built from
    /// quotes or IC's environment or credential is missing (see
    /// `ic::clients`, checked before anything connects), if
    /// OKX's clients cannot be set up or the OKX account does not suit the
    /// run (see `okx::clients`), if IC is unreachable or its account is not
    /// hedging or not `IC_ACCOUNT_ID`'s (see `ic::IcClients::check_account`),
    /// if the strategy trades IB instruments and no account is given, if
    /// IB's clients cannot be set up (see `ib::clients`), or if the node
    /// fails (see `node::run`).
    pub async fn run<S>(&self, requirements: &DataRequirements, strategy: S) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        let cache = warmup::cache_config(&requirements.bar_types, requirements.warmup)?;
        std::fs::create_dir_all(&self.catalog)
            .with_context(|| format!("creating the catalog {}", self.catalog.display()))?;
        let catalog = DataCatalogConfig::new(catalog_path(&self.catalog)?, None, None);
        let data_engine = warmup::live_data_engine_config(&requirements.bar_types);
        let instrument_ids = requirements.instrument_ids();
        // The node would install the logger when built, after the clients
        // are set up: install it now with the node's trader and config, and
        // the node keeps it. The adapter's `tracing` output joins it.
        let file_config = self.logging.file_config.clone().unwrap_or_default();
        let _log_guard = init_logging(
            self.trader_id,
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
            Some(ic::clients(
                &requirements.of_instruments(&ic_ids).bar_types,
                |name| std::env::var(name).ok(),
            )?)
        };
        let mut clients = Vec::new();
        if !okx_ids.is_empty() {
            let okx = okx::clients(&okx_ids, self.leverage).await?;
            clients.push(okx.into_node_clients());
        }
        if let Some(ic) = ic {
            ic.check_account().await?;
            clients.push(ic.into_node_clients());
        }
        if !ib_ids.is_empty() {
            let account = self.account.as_ref().context(
                "the strategy trades IB instruments: give the IB account \
                 (--account or NAUTILUS_IB_ACCOUNT_ID)",
            )?;
            let ib = ib::clients(account, self.client_id, &ib_ids).await?;
            clients.push(ib.into_node_clients());
        }
        node::run(
            self.trader_id,
            self.logging.clone(),
            cache,
            data_engine,
            catalog,
            clients,
            &instrument_ids,
            strategy,
        )
        .await
    }
}
