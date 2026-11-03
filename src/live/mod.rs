//! Runs the strategy on a Nautilus live node.
//!
//! Build step 3: paper, then live. One node per process. The clients are
//! those of the instruments' live brokers, several if the instruments span
//! several, which the venues module sets up (`venues::live_clients`). Each
//! broker's data client serves the warmup of its instruments up to now.
//! The node registers the catalog fill-catalog fills and backtests read:
//! the warmup comes from it first, from the venues for what it lacks, and
//! the answers of the clients that certify them are written back to it
//! (see `strategy::warmup`). A futures root's instruments are its contract in
//! front at the start and the one it rolls into next (see
//! `rolls::Transition::after`); the strategy's roller loads each later one
//! before switching to it (see `rolls::Roller::live`), so the node rolls
//! without a restart; bars on a root are refused.

pub mod node;
mod recording;
mod redis;

pub use recording::Recording;

use std::{fmt::Debug, path::PathBuf};

use anyhow::{Context, bail};
use jiff::Timestamp;
use nautilus_common::{
    actor::DataActorNative,
    component::Component,
    logging::{bridge::init_tracing, init_logging, logger::LoggerConfig},
};
use nautilus_model::identifiers::{InstrumentId, TraderId};
use nautilus_persistence::config::DataCatalogConfig;
use nautilus_trading::strategy::{Strategy, StrategyNative};
use rust_decimal::Decimal;

use crate::{
    catalog::catalog_path,
    model::launcher::RetrainActor,
    rolls::Transition,
    strategy::{
        DataRequirements,
        warmup::{self, Warmup},
    },
    venues::{self, LiveVenueArgs},
};

/// One live run: the strategy trades each instrument on its live broker,
/// with the venues' settings `venues`, at `leverage`; on bars the node
/// builds, warmed up from the venues.
#[derive(Debug, Clone)]
pub struct Live {
    /// The node's trader. The node's cache in Redis is the trader's, so a
    /// restart under the same trader loads what the last run left.
    pub trader_id: TraderId,
    /// The venues' settings of the run, the live command's venue flags,
    /// handed to the venues module with the instruments (see
    /// `venues::live_clients`).
    pub venues: LiveVenueArgs,
    /// The leverage the run expects, as in a backtest, handed to the venues
    /// module with the instruments: a venue applying the account's refuses
    /// an instrument at another one (see `venues::live_clients`).
    pub leverage: Decimal,
    /// The Nautilus logger, installed for the start and kept by the node.
    pub logging: LoggerConfig,
    /// The catalog directory, fill-catalog's and the backtests': it serves
    /// the warmup, and keeps the history the venues certify complete (see
    /// `node::run`). Created if missing.
    pub catalog: PathBuf,
    /// Where the node stages the data it streams into the catalog, which it
    /// promotes there daily; also keeps the trader's instance id.
    pub recording: Recording,
}

impl Live {
    /// Sets up the clients of the instruments of `requirements` (see
    /// `venues::live_clients`), then runs `strategy` on a live node until
    /// SIGINT or SIGTERM, beside `retraining`, the actor of its retrains if it is
    /// retrained (`RetrainActor`), its cache sized for `warmup`, the
    /// strategy's (see `Warmup::cache_config`). The strategy's warmup
    /// requests go to the data clients. A root's instruments are its
    /// contract in front at the start and the next one (see `README.md`,
    /// "Rolled roots"). The node is built, and what the last run left
    /// staged promoted (`promote_staged`), before it runs.
    ///
    /// # Errors
    ///
    /// Returns an error if a bar type is on a root (live builds no
    /// continuous bars), if the cache cannot hold the warmup bars of a bar
    /// type the warmup builds (see `Warmup::cache_config`), if the catalog
    /// cannot be created or its path is not UTF-8, if the staging directory is inside the
    /// catalog, the trader's instance id cannot be kept (see `Recording`),
    /// if what the last run left staged cannot be promoted
    /// (`promote_staged`), if the clients cannot be set up (see
    /// `venues::live_clients`, a venue's environment read before anything
    /// connects), if Redis cannot be started (see `redis::start`), or if
    /// the node fails (see
    /// `node::build`, `node::run`).
    pub async fn run<S>(
        &self,
        requirements: &DataRequirements,
        warmup: &Warmup,
        strategy: S,
        retraining: Option<RetrainActor>,
    ) -> anyhow::Result<()>
    where
        S: Strategy + StrategyNative + DataActorNative + Component + Debug + 'static,
    {
        let cache = warmup.cache_config()?;
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
        let clients =
            venues::live_clients(requirements, &instrument_ids, &self.venues, self.leverage)
                .await?;
        let cache_database = redis::start().await?;
        // Built first, which promotes what the last run left staged: the
        // warmup reads the catalog with it there.
        let node = node::build(
            self.trader_id,
            instance_id,
            self.logging.clone(),
            cache,
            data_engine,
            streaming,
            catalog,
            Some(Box::new(cache_database)),
            clients,
        )?;
        node::run(node, &instrument_ids, strategy, retraining).await
    }
}

/// The instruments the node loads at its start, at `now`: each one
/// `requirements` reads, a root replaced by the contracts of its next roll
/// (see `rolls::Transition::after`), the one standing for it now and the
/// one it rolls into, so the strategy's `Roller` subscribes to a loaded
/// contract even if that roll passes before the strategy starts or while
/// the roller's request for its post contract is in flight; the roller
/// requests the post contract of each later roll (see
/// `rolls::Roller::live`). A plain instrument stands for itself. Each
/// once, in order.
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
        let contracts = Transition::after(instrument_id, now)
            .map_or_else(|| vec![instrument_id], |next| vec![next.pre, next.post]);
        for contract in contracts {
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
    /// 2026-11-26 (`rolls/README.md`, "GC against Databento"): the roller
    /// requests the contracts of the later rolls (`rolls::Roller::live`).
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

    /// A start just before a roll loads the roll's post contract too, so
    /// the roller subscribes to it even if the roll passes before the
    /// strategy starts or its request for it is still in flight.
    #[test]
    fn a_root_loads_just_before_its_roll_the_pre_and_post_contracts() {
        let requirements = ticks(NautilusDataType::OrderBookDepth, &["GC.GLBX"]);
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
    fn a_root_loads_at_its_roll_the_post_contract_and_the_next() {
        let requirements = ticks(NautilusDataType::OrderBookDepth, &["GC.GLBX"]);
        assert_eq!(
            loaded_instrument_ids(&requirements, at("2026-07-29T00:00Z")).unwrap(),
            ids(&["GCZ6.GLBX", "GCG7.GLBX"])
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
