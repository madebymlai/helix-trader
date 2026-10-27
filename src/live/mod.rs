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

#[cfg(test)]
mod tests {
    use nautilus_common::messages::execution::{SubmitOrder, TradingCommand};
    use nautilus_live::config::LiveNodeConfig;
    use nautilus_model::{
        data::BarType,
        events::OrderInitialized,
        identifiers::{ClientId, ClientOrderId, StrategyId, Venue},
    };

    use super::*;
    use crate::data::IbGateway;

    /// IC's clients for gold's hourly bars on the demo, and IB's for SXR8
    /// on a paper gateway, as `Live::run` sets them up; nothing connects.
    fn ic_and_ib() -> Vec<node::Clients> {
        let ic = ic::clients(
            &[BarType::from("XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL")],
            |name| {
                let value = match name {
                    "IC_ENVIRONMENT" => "demo",
                    "IC_CLIENT_ID" => "12345_clientIdValue",
                    "IC_CLIENT_SECRET" => "clientSecretValue",
                    "IC_ACCESS_TOKEN" => "accessTokenValue",
                    "IC_REFRESH_TOKEN" => "refreshTokenValue",
                    "IC_ACCOUNT_ID" => "1000001",
                    "HOME" => "/home/u",
                    _ => return None,
                };
                Some(value.to_owned())
            },
        )
        .unwrap();
        let gateway = IbGateway {
            host: "127.0.0.1".to_string(),
            port: 4002,
            client_id: 11,
        };
        let account: IbAccount = "DU1234567".parse().unwrap();
        let ib = ib::configs(&gateway, &account, &[InstrumentId::from("SXR8.IBIS")]);
        vec![ic.into_node_clients(), ib.into_node_clients()]
    }

    fn submit(instrument_id: &str) -> TradingCommand {
        TradingCommand::SubmitOrder(SubmitOrder::new(
            TraderId::from("HELIX-001"),
            None,
            StrategyId::from("S-001"),
            InstrumentId::from(instrument_id),
            ClientOrderId::from("O-001"),
            OrderInitialized::default(),
            None,
            None,
            None,
            UUID4::new(),
            0.into(),
            None,
        ))
    }

    #[test]
    fn with_ib_present_orders_go_to_ic_on_icmarkets_and_to_ib_elsewhere() {
        let node = node::builder(LiveNodeConfig::default(), ic_and_ib())
            .unwrap()
            .build()
            .unwrap();
        let exec_engine = node.kernel().exec_engine().borrow();
        let client_of = |instrument_id| {
            exec_engine
                .find_client_for_command(&submit(instrument_id))
                .map(|client| client.client_id)
        };

        assert_eq!(
            client_of("XAUUSD.ICMARKETS"),
            Some(ClientId::from("ICMARKETS"))
        );
        assert_eq!(client_of("SXR8.IBIS"), Some(ClientId::from("IB")));
    }

    #[test]
    fn with_ib_present_icmarkets_data_goes_to_ic_and_other_venues_to_ib() {
        let node = node::builder(LiveNodeConfig::default(), ic_and_ib())
            .unwrap()
            .build()
            .unwrap();
        let mut data_engine = node.kernel().data_engine.borrow_mut();
        let mut client_of = |venue| {
            data_engine
                .get_client(None, Some(&Venue::new(venue)))
                .map(|client| client.client_id)
        };

        assert_eq!(client_of("ICMARKETS"), Some(ClientId::from("ICMARKETS")));
        assert_eq!(client_of("IBIS"), Some(ClientId::from("IB")));
    }
}
