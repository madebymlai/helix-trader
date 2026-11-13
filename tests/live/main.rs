//! The end to end tests on real venues: one test binary, a module per venue
//! run, each gated by its own variables and passing at once, saying it was
//! skipped, without them. Run one module per process, by its path:
//!
//! ```sh
//! NAUTILUS_CTRADER_RUN=pepperstone cargo test --test live ctrader_demo:: -- --nocapture
//! ```
//!
//! A module's tests share the process as they did when each module was its
//! own binary; two modules opted in together would share it too, and they
//! cannot: a node stops on the SIGINT its test sends to the whole process,
//! the `tracing` subscriber `Live::run` installs can be installed only once
//! per process, there is one node per process (`docs/ARCHITECTURE.md`,
//! "Process constraints"), and modules of one venue read and trade the
//! same account. So the first module to opt in claims the process
//! (`claim_process`), and another module's test fails before connecting.
//!
//! `streaming` is the exception: hermetic and ungated, it runs its nodes in
//! child processes, none in this one, so it claims nothing and runs beside
//! any module.

use std::{cell::RefCell, path::Path, sync::OnceLock};

use clap::{Args, Command, FromArgMatches};
use helix_trader::{live::Recording, venues::LiveVenueArgs};
use nautilus_common::actor::indicators::ActorIndicator;
use nautilus_model::data::{Bar, QuoteTick, TradeTick};

#[path = "../common/mod.rs"]
mod common;

mod ctrader_demo;
mod ctrader_live_demo;
mod ctrader_orders_demo;
mod ctrader_restart_demo;
mod ctrader_token_demo;
mod demo_broker;
mod ib_gateway;
mod ib_minute_bars;
mod ib_restart;
mod ib_warmup_days;
mod lse_history;
mod lse_markets;
mod lse_quotes;
mod okx_demo;
mod streaming;

/// An indicator a warmup feeds, recording each bar it hands over.
#[derive(Debug, Default)]
struct FedBars {
    bars: RefCell<Vec<Bar>>,
}

impl ActorIndicator for FedBars {
    fn key(&self) -> usize {
        std::ptr::from_ref(self).addr()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn initialized(&self) -> anyhow::Result<bool> {
        Ok(true)
    }

    fn handle_quote(&self, _quote: &QuoteTick) -> anyhow::Result<()> {
        Ok(())
    }

    fn handle_trade(&self, _trade: &TradeTick) -> anyhow::Result<()> {
        Ok(())
    }

    fn handle_bar(&self, bar: &Bar) -> anyhow::Result<()> {
        self.bars.borrow_mut().push(*bar);
        Ok(())
    }
}

/// The module whose gated tests run in this process.
static CLAIMED: OnceLock<&'static str> = OnceLock::new();

/// Claims the process for the gated tests of `module`, its
/// `module_path!()`, once they are opted in.
///
/// # Panics
///
/// Panics if another module has claimed the process.
fn claim_process(module: &'static str) {
    let owner = *CLAIMED.get_or_init(|| module);
    assert_eq!(
        owner, module,
        "{owner} runs in this process: run one live module at a time, \
         `cargo test --test live <module>::`"
    );
}

/// The venues' settings of a venue test's node from `flags`, the live
/// command's venue flags, parsed as `live` parses them: the IB account
/// falls back on `NAUTILUS_IB_ACCOUNT_ID`.
fn venues(flags: &[&str]) -> LiveVenueArgs {
    let matches = LiveVenueArgs::augment_args(Command::new("live"))
        .try_get_matches_from(["live"].into_iter().chain(flags.iter().copied()))
        .unwrap();
    LiveVenueArgs::from_arg_matches(&matches).unwrap()
}

/// The recording of a venue test's node: staged in `staging`, a directory
/// of the test's own, promoted daily, as every node, and on its stop.
fn recording(staging: &Path) -> Recording {
    Recording {
        staging: staging.to_path_buf(),
    }
}
