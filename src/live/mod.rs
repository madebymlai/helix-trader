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
//! `strategy::warmup`). A futures root's instruments are its contract in
//! front at the start and the next one, which the strategy's roller
//! switches to (see `rolls::contracts_at`); bars on a root are refused.

mod ib;
mod ic;
mod node;
mod okx;
mod recording;
mod redis;

pub use ic::{
    IcDataClientConfig, IcDataClientFactory, IcExecutionClientConfig, IcExecutionClientFactory,
};
pub use recording::{Recording, promote_staged};

pub use crate::data::IbAccount;

use std::{
    fmt::Debug,
    path::{Path, PathBuf},
};

use anyhow::{Context, bail};
use jiff::Timestamp;
use nautilus_common::{
    actor::DataActorNative,
    component::Component,
    logging::{bridge::init_tracing, init_logging, logger::LoggerConfig},
};
use nautilus_model::identifiers::{InstrumentId, TraderId};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, config::DataCatalogConfig,
};
use nautilus_trading::strategy::{Strategy, StrategyNative};
use rust_decimal::Decimal;

use crate::{
    data::{IcHistory, OkxHistory, catalog_path, live_runs, record_run_start},
    rolls::{Transition, contracts_at},
    strategy::{DataRequirements, SeriesSlice, warmup},
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
    /// Where the node stages the data it streams into the catalog, and how
    /// often it promotes it there; also keeps the trader's instance id.
    pub recording: Recording,
}

impl Live {
    /// Sets up the clients of the instruments of `requirements`, OKX's, IC's
    /// then IB's, then runs `strategy` on a live node until SIGINT or
    /// SIGTERM. The strategy's warmup requests go to the data clients. A
    /// root's instruments are its contract in front at the start and the
    /// next one (see `README.md`, "Rolled roots"). Once the node is built,
    /// with what the last run left staged promoted (`promote_staged`), the
    /// gaps the last runs streamed across in the series of the warmup are
    /// joined, and the run's start is recorded in the catalog
    /// (`data::live_runs`), before it runs.
    ///
    /// # Errors
    ///
    /// Returns an error if a bar type is on a root (live builds no
    /// continuous bars), if the cache cannot hold the warmup bars of a bar
    /// type (see `warmup::cache_config`), if the catalog cannot be created
    /// or its path is not UTF-8, if the staging directory is inside the
    /// catalog, the trader's instance id or the run's start cannot be kept
    /// (see `Recording`, `data::record_run_start`), if what the last run left
    /// staged cannot be promoted (`promote_staged`), if a gap cannot be
    /// joined (`data::live_runs::join`), if an IC bar type
    /// is not built from quotes or IC's environment or credential is missing
    /// (see `ic::clients`, checked before anything connects), if
    /// OKX's clients cannot be set up or the OKX account does not suit the
    /// run (see `okx::clients`), if IC is unreachable or its account is not
    /// hedging or not `IC_ACCOUNT_ID`'s (see `ic::IcClients::check_account`),
    /// if the strategy trades IB instruments and no account is given, if
    /// IB's clients cannot be set up (see `ib::clients`), or if the node
    /// fails (see `node::build`, `node::run`).
    pub async fn run<S>(&self, requirements: &DataRequirements, strategy: S) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        let cache = warmup::cache_config(&requirements.bar_types, requirements.warmup)?;
        std::fs::create_dir_all(&self.catalog)
            .with_context(|| format!("creating the catalog {}", self.catalog.display()))?;
        let catalog = DataCatalogConfig::new(catalog_path(&self.catalog)?, None, None);
        let streaming = self.recording.streaming(&catalog)?;
        let instance_id = self.recording.instance_id(self.trader_id)?;
        let data_engine = warmup::live_data_engine_config(&requirements.bar_types);
        let instrument_ids = loaded_instrument_ids(requirements, Timestamp::now())?;
        // The node would install the logger when built, after the clients
        // are set up: install it now with the node's trader and config, and
        // the node keeps it. The adapter's `tracing` output joins it.
        let file_config = self.logging.file_config.clone().unwrap_or_default();
        let _log_guard = init_logging(
            self.trader_id,
            instance_id,
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
        // Built first, which promotes what the last run left staged: the
        // gaps are joined, and the warmup reads the catalog, with it there.
        let node = node::build(
            self.trader_id,
            instance_id,
            self.logging.clone(),
            cache,
            data_engine,
            streaming,
            catalog,
            clients,
        )
        .await?;
        join_warmup(&self.catalog, requirements)?;
        // Before the node runs, which starts the recording.
        record_run_start(&self.catalog)?;
        node::run(node, &instrument_ids, strategy).await
    }
}

/// Joins the gaps the last live runs streamed across in the series the
/// warmup of `requirements` reads in the catalog at `catalog` (see
/// `data::live_runs`): the warmup reads the catalog first and asks the
/// venue for what it lacks, a request per gap, whose answer would hold the
/// quotes the node received after it.
///
/// # Errors
///
/// Returns an error if the window cannot be sliced (see
/// `DataRequirements::slices`) or a gap cannot be joined.
fn join_warmup(catalog: &Path, requirements: &DataRequirements) -> anyhow::Result<()> {
    let now = Timestamp::now();
    let reader = ParquetDataCatalog::new(catalog, None, None, None, None);
    for SeriesSlice {
        series: (data_type, identifier),
        ..
    } in requirements.slices(now.checked_sub(requirements.warmup)?, now)?
    {
        live_runs::join(&reader, &data_type, &identifier)
            .with_context(|| format!("joining the live runs of the {data_type} of {identifier}"))?;
    }
    Ok(())
}

/// The instruments the node loads, at `now`: each one `requirements`
/// reads, a root replaced by the contracts it needs up to its next roll
/// (see `rolls::contracts_at`), the one standing for it now and the one it
/// rolls into, which the strategy's `Roller` subscribes to at the roll. A
/// plain instrument stands for itself. Each once, in order.
///
/// # Errors
///
/// Returns an error if a bar type is on a root: live builds no continuous
/// bars (see `README.md`, "Rolled roots").
fn loaded_instrument_ids(
    requirements: &DataRequirements,
    now: Timestamp,
) -> anyhow::Result<Vec<InstrumentId>> {
    for bar_type in &requirements.bar_types {
        let instrument_id = bar_type.instrument_id();
        if let Some(next) = Transition::after(instrument_id, now) {
            bail!(
                "bars {bar_type} on the root {instrument_id}: live builds no continuous \
                 bars, whose rolls Nautilus prices up front; read bars on its contract {} \
                 or the root's ticks",
                next.pre
            );
        }
    }
    let mut ids: Vec<InstrumentId> = Vec::new();
    for instrument_id in requirements.instrument_ids() {
        for contract in contracts_at(instrument_id, now) {
            if !ids.contains(&contract) {
                ids.push(contract);
            }
        }
    }
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use jiff::SignedDuration;
    use nautilus_model::data::{BarType, NautilusDataType};

    use super::*;
    use crate::strategy::TickSeries;

    fn at(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    fn ids(ids: &[&str]) -> Vec<InstrumentId> {
        ids.iter().map(|&id| InstrumentId::from(id)).collect()
    }

    fn ticks(data: NautilusDataType, instrument_ids: &[&str]) -> DataRequirements {
        DataRequirements {
            bar_types: vec![],
            ticks: instrument_ids
                .iter()
                .map(|&id| TickSeries {
                    data: data.clone(),
                    instrument_id: InstrumentId::from(id),
                })
                .collect(),
            warmup: SignedDuration::ZERO,
        }
    }

    /// GC rolls into GCZ6 on 2026-07-29 and out of it into GCG7 on
    /// 2026-11-26 (`rolls/README.md`, "GC against Databento").
    #[test]
    fn a_root_loads_the_contract_in_front_and_the_next() {
        let requirements = DataRequirements {
            bar_types: vec![BarType::from("XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL")],
            ..ticks(NautilusDataType::TradeTick, &["GC.GLBX"])
        };
        assert_eq!(
            loaded_instrument_ids(&requirements, at("2026-08-10T14:30Z")).unwrap(),
            ids(&["XAUUSD.ICMARKETS", "GCZ6.GLBX", "GCG7.GLBX"])
        );
    }

    #[test]
    fn a_root_loads_at_its_roll_the_post_contract_and_the_next() {
        let requirements = ticks(NautilusDataType::OrderBookDepth, &["GC.GLBX"]);
        assert_eq!(
            loaded_instrument_ids(&requirements, at("2026-07-29T00:00Z")).unwrap(),
            ids(&["GCZ6.GLBX", "GCG7.GLBX"])
        );
        assert_eq!(
            loaded_instrument_ids(
                &requirements,
                at("2026-07-29T00:00Z") - SignedDuration::from_nanos(1)
            )
            .unwrap(),
            ids(&["GCQ6.GLBX", "GCZ6.GLBX"])
        );
    }

    #[test]
    fn a_contract_read_beside_its_root_loads_once() {
        let requirements = ticks(NautilusDataType::TradeTick, &["GCZ6.GLBX", "GC.GLBX"]);
        assert_eq!(
            loaded_instrument_ids(&requirements, at("2026-08-10T14:30Z")).unwrap(),
            ids(&["GCZ6.GLBX", "GCG7.GLBX"])
        );
    }

    #[test]
    fn plain_instruments_load_as_they_are_read() {
        let requirements = DataRequirements {
            bar_types: vec![
                BarType::from("SXR8.IBIS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"),
                BarType::from("BTC-USDT-SWAP.OKX-1-HOUR-MID-INTERNAL"),
            ],
            ..ticks(
                NautilusDataType::QuoteTick,
                &["XAUUSD.ICMARKETS", "GC.v.0.GLBX"],
            )
        };
        assert_eq!(
            loaded_instrument_ids(&requirements, at("2026-08-10T14:30Z")).unwrap(),
            ids(&[
                "SXR8.IBIS",
                "BTC-USDT-SWAP.OKX",
                "XAUUSD.ICMARKETS",
                "GC.v.0.GLBX"
            ])
        );
    }

    #[test]
    fn bars_on_a_root_are_refused_naming_the_bar_type_and_its_contract() {
        let requirements = DataRequirements {
            bar_types: vec![BarType::from("GC.GLBX-1-MINUTE-LAST-INTERNAL")],
            ..ticks(NautilusDataType::TradeTick, &[])
        };
        let err = loaded_instrument_ids(&requirements, at("2026-08-10T14:30Z"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("GC.GLBX-1-MINUTE-LAST-INTERNAL"), "{err}");
        assert!(err.contains("GCZ6.GLBX"), "{err}");
    }
}
