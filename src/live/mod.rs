//! Runs the strategy on a Nautilus live node (build step 3: paper, then live). Each process runs
//! one node.
//!
//! The clients are the live data sources of the instruments and the custom series. Each client
//! is also the broker of its instruments (`venues::live_clients`).
//!
//! The warmup comes from the catalog first, then from the venues. The venues' certified answers
//! are written back (see `strategy::warmup`).
//!
//! A futures root has two instruments: its front contract and the next one (see
//! `rolls::Transition::after`). The roller loads each later contract before switching to it (see
//! `rolls::Roller::live`), so the node rolls without a restart. Bars on a root are refused.

pub mod node;
mod recording;
mod redis;

use std::{fmt::Debug, path::PathBuf};

use anyhow::Context;
use jiff::Timestamp;
use nautilus_common::{
    actor::DataActorNative,
    component::Component,
    logging::{bridge::init_tracing, init_logging, logger::LoggerConfig},
};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::identifiers::{InstrumentId, TraderId};
use nautilus_trading::strategy::{Strategy, StrategyNative};
pub use recording::Recording;

use crate::{
    catalog::catalog_config,
    model::launcher::RetrainActor,
    rolls::Transition,
    strategy::{
        requirements::DataRequirements,
        warmup::{self, Warmup},
    },
    venues::{self, LiveVenueArgs},
};

/// One live run: the strategy trades each instrument on its live broker.
#[derive(Debug, Clone)]
pub struct Live {
    /// The node's trader. A restart loads the trader's cache from Redis.
    pub trader_id: TraderId,
    /// The venue flags of the live command.
    pub venues: LiveVenueArgs,
    /// The Nautilus logger config.
    pub logging: LoggerConfig,
    /// The catalog directory, created if missing.
    ///
    /// It serves the warmup and keeps the history that venues certify complete (see `node::run`).
    /// It also holds the definitions that a venue checks its account against.
    pub catalog: PathBuf,
    /// Where the node stages the data it streams.
    pub recording: Recording,
}

impl Live {
    /// Sets up the clients for the instruments in `requirements` (see `venues::live_clients`), then
    /// runs `strategy` on a live node until SIGINT or SIGTERM.
    ///
    /// The node claims external orders for the instruments the strategy trades, and for no other
    /// instrument it reads. The run first promotes what the last run left staged.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A bar type is on a root (live builds no continuous bars).
    /// - The cache cannot hold the warmup bars (see `Warmup::cache_config`).
    /// - The catalog cannot be created or its path is not UTF-8.
    /// - The staging directory is inside the catalog.
    /// - The trader's instance id cannot be kept (see `Recording`).
    /// - What the last run left staged cannot be promoted (`promote_staged`).
    /// - The clients cannot be set up (see `venues::live_clients`).
    /// - Redis cannot be started (see `redis::start`).
    /// - The node fails (see `node::build`, `node::run`).
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
        let catalog = catalog_config(&self.catalog)?;
        let streaming = self.recording.streaming(&catalog)?;
        let instance_id = self.recording.instance_id(self.trader_id)?;
        let data_engine = warmup::live_data_engine_config(&requirements.bar_types);
        let instrument_ids = loaded_instrument_ids(
            requirements,
            Timestamp::from(get_atomic_clock_realtime().get_time_ns()),
        )?;
        // The node would install the logger only when built, after the clients are set up. The
        // adapters' `tracing` output goes to stdout, filtered by RUST_LOG (warn by default).
        let file_config = self.logging.file_config.clone().unwrap_or_default();
        let _log_guard = init_logging(
            self.trader_id,
            instance_id,
            self.logging.clone(),
            file_config,
        )?;
        init_tracing()?;
        let clients =
            venues::live_clients(requirements, &instrument_ids, &self.venues, &self.catalog)
                .await?;
        let cache_database = redis::start().await?;
        // Built first, because it promotes what the last run left staged and the warmup reads that
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
        node::run(node, strategy, retraining).await
    }
}

/// Returns the instruments the node loads at `now`, each once, in order.
///
/// Each root is replaced by the contracts of its next roll (see `rolls::Transition::after`).
///
/// # Errors
///
/// Returns an error if a bar type is on a root: live builds no continuous bars.
fn loaded_instrument_ids(
    requirements: &DataRequirements,
    now: Timestamp,
) -> anyhow::Result<Vec<InstrumentId>> {
    for bar_type in &requirements.bar_types {
        let instrument_id = bar_type.instrument_id();
        if let Some(next) = Transition::after(instrument_id, now) {
            anyhow::bail!(
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
    use rstest::rstest;

    use super::*;
    use crate::strategy::requirements::TickSeries;

    fn at(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    fn ids(ids: &[&str]) -> Vec<InstrumentId> {
        ids.iter().map(|&id| InstrumentId::from(id)).collect()
    }

    fn ticks(data: &NautilusDataType, instrument_ids: &[&str]) -> DataRequirements {
        DataRequirements {
            bar_types: vec![],
            ticks: instrument_ids
                .iter()
                .map(|&id| TickSeries {
                    data: data.clone(),
                    instrument_id: InstrumentId::from(id),
                })
                .collect(),
            custom: vec![],
            warmup: SignedDuration::ZERO,
        }
    }

    #[rstest]
    fn a_root_loads_the_contract_in_front_and_the_next() {
        let requirements = DataRequirements {
            bar_types: vec![BarType::from("XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL")],
            ..ticks(&NautilusDataType::TradeTick, &["GC.GLBX"])
        };
        assert_eq!(
            loaded_instrument_ids(&requirements, at("2026-08-10T14:30Z")).unwrap(),
            ids(&["XAUUSD.ICMARKETS", "GCZ26.GLBX", "GCG27.GLBX"])
        );
    }

    #[rstest]
    #[case::just_before_it(
        at("2026-07-29T00:00Z") - SignedDuration::from_nanos(1),
        &["GCQ26.GLBX", "GCZ26.GLBX"],
    )]
    #[case::at_it(at("2026-07-29T00:00Z"), &["GCZ26.GLBX", "GCG27.GLBX"])]
    fn a_root_loads_around_its_roll_the_contract_in_front_and_the_next(
        #[case] start: Timestamp,
        #[case] loaded: &[&str],
    ) {
        let requirements = ticks(&NautilusDataType::OrderBookDepth, &["GC.GLBX"]);
        assert_eq!(
            loaded_instrument_ids(&requirements, start).unwrap(),
            ids(loaded)
        );
    }

    #[rstest]
    fn a_contract_read_beside_its_root_loads_once() {
        let requirements = ticks(&NautilusDataType::TradeTick, &["GCZ26.GLBX", "GC.GLBX"]);
        assert_eq!(
            loaded_instrument_ids(&requirements, at("2026-08-10T14:30Z")).unwrap(),
            ids(&["GCZ26.GLBX", "GCG27.GLBX"])
        );
    }

    #[rstest]
    fn plain_instruments_load_as_they_are_read() {
        let requirements = DataRequirements {
            bar_types: vec![
                BarType::from("SXR8.IBIS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"),
                BarType::from("BTC-USDT-SWAP.OKX-1-HOUR-MID-INTERNAL"),
            ],
            ..ticks(
                &NautilusDataType::QuoteTick,
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

    #[rstest]
    fn bars_on_a_root_are_refused_naming_the_bar_type_and_its_contract() {
        let requirements = DataRequirements {
            bar_types: vec![BarType::from("GC.GLBX-1-MINUTE-LAST-INTERNAL")],
            ..ticks(&NautilusDataType::TradeTick, &[])
        };
        let err = loaded_instrument_ids(&requirements, at("2026-08-10T14:30Z"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("GC.GLBX-1-MINUTE-LAST-INTERNAL"), "{err}");
        assert!(err.contains("GCZ26.GLBX"), "{err}");
    }
}
