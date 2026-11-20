//! Mirrors Nautilus' time-range requests (`time_range_generator`,
//! `crates/data/src/engine/time_range.rs`): the data engine asks for one window at a time and ends
//! the request with an empty envelope, logging a warning. Windows end at each multiple of the
//! window from the request's start, the next starting 1 ns later.

use jiff::{SignedDuration, Timestamp};
use serde_json::{Value, json};

/// The params making a request over `[from, to]` one in windows of `window`.
///
/// `durations_seconds` holds the window once per window the span can hold: the data engine takes
/// the next duration after an empty window, the first again after one with data, and ends the
/// request, reporting success, when they run out. Nautilus documents neither; `node_fill` and
/// `node_warmup` test it.
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
