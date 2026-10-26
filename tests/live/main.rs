//! The end to end tests on real venues: one test binary, a module per venue
//! run, each gated by its own variables and passing at once, saying it was
//! skipped, without them. Run one module per process, by its path:
//!
//! ```sh
//! NAUTILUS_IC_RUN=1 cargo test --test live ic_demo:: -- --nocapture
//! ```
//!
//! A module's tests share the process as they did when each module was its
//! own binary; two modules opted in together would share it too, and they
//! cannot: a node stops on the SIGINT its test sends to the whole process,
//! the `tracing` subscriber `Live::run` installs can be installed only once
//! per process, there is one node per process (`docs/ARCHITECTURE.md`,
//! "Nautilus v2 constraints"), and modules of one venue read and trade the
//! same account. So the first module to opt in claims the process
//! (`claim_process`), and another module's test fails before connecting.

use std::sync::OnceLock;

#[path = "../common/mod.rs"]
mod common;

mod ib_gateway;
mod ib_minute_bars;
mod ib_restart;
mod ic_demo;
mod ic_live_demo;
mod ic_orders_demo;
mod okx_demo;

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
