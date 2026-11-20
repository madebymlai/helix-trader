//! End to end tests on real venues, one module per venue run, in one test binary.
//!
//! Each module is gated by its own variables: without them its tests return at once and say they
//! were skipped. Run one module per process, by its path:
//!
//! ```sh
//! NAUTILUS_CTRADER_RUN=pepperstone cargo test --test live ctrader_demo:: -- --nocapture
//! ```
//!
//! Two opted-in modules cannot share a process:
//!
//! - A node stops on the SIGINT its test sends to the whole process.
//! - `Live::run` installs its `tracing` subscriber once per process.
//! - There is one node per process (`docs/architecture.md`, "Process constraints").
//! - Modules of one venue trade the same account.
//!
//! The first module to opt in claims the process (`claim_process`), and another module's test fails
//! before connecting. `streaming` is hermetic and ungated: its nodes run in child processes, so it
//! claims nothing.

use std::{cell::RefCell, path::Path, sync::OnceLock};

use clap::{Args, Command, FromArgMatches};
use helix_trader::{live::Recording, venues::LiveVenueArgs};
use nautilus_common::actor::indicators::ActorIndicator;
use nautilus_model::data::{Bar, QuoteTick, TradeTick};

/// Returns the current time on Nautilus' realtime clock.
pub(crate) fn now() -> jiff::Timestamp {
    jiff::Timestamp::from(nautilus_core::time::get_atomic_clock_realtime().get_time_ns())
}

#[path = "../common/mod.rs"]
mod common;

mod ctrader_conversions_demo;
mod ctrader_demo;
mod ctrader_live_demo;
mod ctrader_orders_demo;
mod ctrader_restart_demo;
mod ctrader_token_demo;
mod demo_broker;
mod ib_gateway;
mod ib_minute_bars;
mod ib_pair_warmup;
mod ib_quote_fill;
mod ib_restart;
mod ib_warmup_days;
mod lse_history;
mod lse_markets;
mod lse_option_trades;
mod lse_quotes;
mod okx_demo;
mod streaming;

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

static CLAIMED: OnceLock<&'static str> = OnceLock::new();

/// Claims the process for the gated tests of `module`, which is its `module_path!()`.
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

/// Returns the venue settings of `flags`, parsed as the live command parses them.
///
/// The IB account falls back on `NAUTILUS_IB_ACCOUNT_ID`.
fn venues(flags: &[&str]) -> LiveVenueArgs {
    let matches = LiveVenueArgs::augment_args(Command::new("live"))
        .try_get_matches_from(["live"].into_iter().chain(flags.iter().copied()))
        .unwrap();
    LiveVenueArgs::from_arg_matches(&matches).unwrap()
}

/// Returns the recording of a venue test's node, staged in `staging`.
fn recording(staging: &Path) -> Recording {
    Recording {
        staging: staging.to_path_buf(),
    }
}
