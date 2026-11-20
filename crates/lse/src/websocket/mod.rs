//! LSE's sockets: the frames of the protocol four of them share (`messages`), the client of one
//! socket (`client`), and tick parsing (`parse`).
//!
//! The exchange streams (CME Globex, Nasdaq TotalView) speak a protocol of their own (`exchange`).
//! Both clients reconnect and disconnect alike.

pub mod client;
pub mod exchange;
pub mod messages;
pub mod parse;

use nautilus_common::live::dst::time::Duration;
use nautilus_live::task::{TaskJoinOutcome, TaskSlot, finish_task};

const RECONNECT_DELAY_INITIAL_MS: u64 = 1_000;
const RECONNECT_DELAY_MAX_MS: u64 = 30_000;
const RECONNECT_BACKOFF_FACTOR: f64 = 2.0;
const RECONNECT_JITTER_MS: u64 = 500;

const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const ABORT_TIMEOUT: Duration = Duration::from_secs(2);

async fn finish_handler(handler: &mut TaskSlot<()>, name: &str) {
    match finish_task(handler, DISCONNECT_TIMEOUT, ABORT_TIMEOUT).await {
        None | Some(TaskJoinOutcome::Completed(())) => {}
        Some(TaskJoinOutcome::Aborted) => {
            log::warn!("the handler of LSE's {name} did not stop in time: aborted");
        }
        Some(TaskJoinOutcome::Failed(e)) => {
            log::error!("the handler of LSE's {name} failed: {e}");
        }
        Some(TaskJoinOutcome::Incomplete) => {
            log::error!("the handler of LSE's {name} did not stop after its abort");
        }
    }
}
