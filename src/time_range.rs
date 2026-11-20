//! Mirrors Nautilus' time-range requests (`time_range_generator`,
//! `crates/data/src/engine/time_range.rs`).
//!
//! The data engine asks for one window at a time and ends the request with an empty envelope,
//! logging a warning. Each window ends at a multiple of the window length from the request's
//! start, and the next starts 1 ns later.

use jiff::{SignedDuration, Timestamp};
use serde_json::{Value, json};

/// Returns the params that split a request over `[from, to]` into windows of `window`.
///
/// `durations_seconds` holds the window once for each window the span can hold. The data engine:
/// - Takes the next duration after an empty window.
/// - Takes the first duration again after a window with data.
/// - Ends the request, reporting success, when the durations run out.
///
/// Nautilus documents none of this. `node_fill` and `node_warmup` test it.
pub(crate) fn params(
    from: Timestamp,
    to: Timestamp,
    window: SignedDuration,
) -> [(String, Value); 2] {
    let span = to.duration_since(from).as_nanos();
    let windows = span / window.as_nanos() + 1;
    let windows = usize::try_from(windows).expect("a request of a sane number of windows");
    [
        ("time_range_generator".to_string(), json!("")),
        (
            "durations_seconds".to_string(),
            json!(vec![window.as_secs_f64(); windows]),
        ),
    ]
}
