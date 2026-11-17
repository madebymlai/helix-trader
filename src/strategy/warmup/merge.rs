//! The merge of a live warmup's history across its series: the warmup
//! queues each requested series' responses, quotes, trades, book depth and
//! deltas, and custom data alike, and hands them on in time order
//! (`ts_event`), to the hold of live ticks (`hold`) and through it to the
//! features. A replay requests no warmup and merges nothing: Nautilus
//! replays its data in time order already.
//!
//! Temporary until Nautilus hands a warmup's history in time order across
//! data types: helix-trader#46
//! (<https://github.com/madebymlai/helix-trader/issues/46>), which holds
//! the evidence and the upstream fix that retires it. Each of Nautilus'
//! requests (`request_quotes`, `request_trades`, `request_book_depth`,
//! `request_book_deltas`, `request_data`) runs its own window pipeline
//! (`dispatch_next_time_range_pipeline_request` in
//! `crates/data/src/engine/time_range.rs`) and its responses reach the
//! actor as they arrive, so a feature reading several series would read
//! their history out of order; `RequestJoin`, which orders legs, joins one
//! data type alone. Once it goes, the warmup requests each window's series
//! as one join and hands its responses on as they come. It is separate
//! from the hold, which goes with helix-trader#16: the hold's seams move as
//! the merge hands ticks on, not as they arrive.
//!
//! The merge hands a tick on once no series still pending can bring an
//! older one: its watermark is, over the series whose requests are still
//! pending, the earliest of the latest `ts_event` each has brought, a
//! series that has brought nothing yet holding everything back. A request
//! answers its windows in order, so a pending series brings nothing older
//! than its latest. Ties across series go in a fixed order of kinds,
//! quotes, trades, depth, deltas, then custom data, then in the order the
//! series were requested, so that a replay and a live warmup agree on a
//! tie; within a series, the ticks go in the order they came. A series
//! whose request completes, answered or failed, holds the others back no
//! longer, and the rest of its queue goes on in order. When the group
//! ends, everything held goes on in order. A response of a series the
//! warmup did not request goes on at once: it is no part of the warmup.
//!
//! The history held is bounded (`HELD`): past the bound the merge hands on
//! everything it holds, in order, and leaves out the rest of the warmup's
//! history, older than what the features then read, and the warmup ends
//! incomplete, naming the series held back and those holding them. It logs
//! a series of which it holds more than a window, once a warmup, naming
//! those it waits on, so that a slow series shows.

use std::{cell::RefCell, collections::VecDeque, fmt::Display};

use nautilus_core::{UUID4, UnixNanos};

use super::tick::{Series, Tick};

/// The most ticks the merge holds, of all its series together: under a
/// gigabyte, a tick held taking 168 bytes besides what a batch of deltas,
/// a snapshot of depth or a point of custom data keeps on the heap. A
/// series answered from the catalog at once while another waits on its
/// venue is held whole, so the bound is that of a whole warmup of the
/// series answered first.
pub(super) const HELD: usize = 5_000_000;

/// What merges a live warmup's history across its series (see the module).
#[derive(Debug)]
pub(super) struct Merge {
    /// The most ticks it holds.
    limit: usize,
    state: RefCell<State>,
}

#[derive(Debug)]
enum State {
    /// No live warmup merging: a replay, a warmup whose group ended, or one
    /// before its start. History goes on as it comes.
    Passing,
    /// A live warmup's group pending.
    Merging(Vec<Queue>),
    /// Past its bound: the rest of the warmup's history is left out.
    Cut,
}

/// A series requested and what the merge holds of it.
#[derive(Debug)]
struct Queue {
    series: Series,
    request: UUID4,
    /// Its ticks held, in the order they came.
    held: VecDeque<Tick>,
    /// The latest `ts_event` it brought, none before its first tick.
    latest: Option<UnixNanos>,
    /// Whether its request has not ended yet.
    pending: bool,
    /// Whether the merge logged holding more than a window of it.
    logged: bool,
}

/// The merge past its bound: what it held, of which series, and the series
/// it waited on.
#[derive(Debug)]
pub(super) struct Overflow {
    limit: usize,
    held: Vec<(Series, usize)>,
    waiting: Vec<Series>,
}

impl Merge {
    /// A merge holding at most `limit` ticks, passing history on as it
    /// comes until a live warmup starts it.
    pub(super) fn new(limit: usize) -> Self {
        Self {
            limit,
            state: RefCell::new(State::Passing),
        }
    }

    /// Merges the history of `requests`, each a request's id and its
    /// series, in the order requested, a live start's group pending; what
    /// it held of a warmup before is forgotten.
    pub(super) fn start(&self, requests: impl IntoIterator<Item = (UUID4, Series)>) {
        let queues = requests
            .into_iter()
            .map(|(request, series)| Queue {
                series,
                request,
                held: VecDeque::new(),
                latest: None,
                pending: true,
                logged: false,
            })
            .collect();
        *self.state.borrow_mut() = State::Merging(queues);
    }

    /// Takes `ticks`, a response of one series, and hands on through
    /// `hand`, in order, what no pending series can bring an older tick
    /// than; passing, hands `ticks` on as they come, and cut, none.
    ///
    /// # Errors
    ///
    /// Returns an error if `hand` fails, the first of which stops the
    /// hand-over, the ticks after it kept held. Returns the overflow if the
    /// merge holds more than its bound once it handed on what it could:
    /// having handed on all it held, it is cut.
    pub(super) fn history(
        &self,
        ticks: impl IntoIterator<Item = Tick>,
        mut hand: impl FnMut(&Tick) -> anyhow::Result<()>,
    ) -> anyhow::Result<Option<Overflow>> {
        let mut state = self.state.borrow_mut();
        let queues = match &mut *state {
            State::Passing => {
                return ticks
                    .into_iter()
                    .try_for_each(|tick| hand(&tick))
                    .map(|()| None);
            }
            State::Cut => return Ok(None),
            State::Merging(queues) => queues,
        };
        // The queues this response brought ticks to, and whether each held
        // some of an earlier one then.
        let mut brought: Vec<(usize, bool)> = Vec::new();

        for tick in ticks {
            let series = tick.series();
            let Some(at) = queues.iter().position(|queue| queue.series == series) else {
                hand(&tick)?;
                continue;
            };
            let queue = &mut queues[at];
            if !brought.iter().any(|&(of, _)| of == at) {
                brought.push((at, !queue.held.is_empty()));
            }
            let made = tick.ts_event();
            queue.latest = Some(queue.latest.map_or(made, |latest| latest.max(made)));
            queue.held.push_back(tick);
        }
        hand_on(queues, &mut hand)?;
        for (at, _) in brought.into_iter().filter(|&(_, earlier)| earlier) {
            log_window(queues, at);
        }
        let held: usize = queues.iter().map(|queue| queue.held.len()).sum();
        if held <= self.limit {
            return Ok(None);
        }
        let overflow = Overflow {
            limit: self.limit,
            held: queues
                .iter()
                .filter(|queue| !queue.held.is_empty())
                .map(|queue| (queue.series.clone(), queue.held.len()))
                .collect(),
            waiting: waiting(queues),
        };

        for queue in queues.iter_mut() {
            queue.pending = false;
        }
        let handed = hand_on(queues, &mut hand);
        *state = State::Cut;
        handed.map(|()| Some(overflow))
    }

    /// Takes the end of `request`, answered or failed: its series holds the
    /// others back no longer, and what that frees goes on through `hand`.
    ///
    /// # Errors
    ///
    /// Returns an error if `hand` fails, as `history`.
    pub(super) fn complete(
        &self,
        request: &UUID4,
        mut hand: impl FnMut(&Tick) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let mut state = self.state.borrow_mut();
        let State::Merging(queues) = &mut *state else {
            return Ok(());
        };
        let Some(queue) = queues.iter_mut().find(|queue| queue.request == *request) else {
            return Ok(());
        };
        queue.pending = false;
        hand_on(queues, &mut hand)
    }

    /// Takes the end of the group: hands on everything held, in order, and
    /// passes history on as it comes from now on.
    ///
    /// # Errors
    ///
    /// Returns an error if `hand` fails, as `history`; the merge passes
    /// history on all the same, the ticks after the failing one lost.
    pub(super) fn end(
        &self,
        mut hand: impl FnMut(&Tick) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let state = std::mem::replace(&mut *self.state.borrow_mut(), State::Passing);
        let State::Merging(mut queues) = state else {
            return Ok(());
        };

        for queue in &mut queues {
            queue.pending = false;
        }
        hand_on(&mut queues, &mut hand)
    }

    /// Whether the merge went past its bound in the warmup now ending, the
    /// rest of its history left out.
    pub(super) fn is_cut(&self) -> bool {
        matches!(*self.state.borrow(), State::Cut)
    }
}

/// Hands on through `hand`, in order, the ticks of `queues` that no pending
/// series can bring an older tick than.
fn hand_on(
    queues: &mut [Queue],
    hand: &mut impl FnMut(&Tick) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    loop {
        let next = queues
            .iter()
            .enumerate()
            .filter_map(|(at, queue)| {
                queue
                    .held
                    .front()
                    .map(|tick| (tick.ts_event(), order(queues, at), at))
            })
            .min();
        let Some((made, rank, at)) = next else {
            return Ok(());
        };
        let free = queues.iter().enumerate().all(|(other, queue)| {
            other == at
                || !queue.pending
                || queue.latest.is_some_and(|latest| {
                    made < latest || (made == latest && rank < order(queues, other))
                })
        });

        if !free {
            return Ok(());
        }
        let tick = queues[at].held.pop_front().expect("the queue's front");
        hand(&tick)?;
    }
}

/// Where the series of `queues[at]` goes on a tie: by its kind, quotes,
/// trades, depth, deltas, then custom data, then in the order requested.
fn order(queues: &[Queue], at: usize) -> (u8, usize) {
    let kind = match queues[at].series {
        Series::Quotes(_) => 0,
        Series::Trades(_) => 1,
        Series::Depth(_) => 2,
        Series::Deltas(_) => 3,
        Series::Custom(_) => 4,
    };
    (kind, at)
}

/// The pending series of `queues` the merge waits on: those that brought
/// nothing yet if any did, else those whose latest is the earliest.
fn waiting(queues: &[Queue]) -> Vec<Series> {
    let pending = queues.iter().filter(|queue| queue.pending);
    let earliest = pending.clone().map(|queue| queue.latest).min().flatten();
    pending
        .filter(|queue| queue.latest.is_none() || queue.latest == earliest)
        .map(|queue| queue.series.clone())
        .collect()
}

/// Logs that the merge holds more than a window of the series of
/// `queues[at]`, if it still holds some of its earlier windows' ticks,
/// once a warmup.
fn log_window(queues: &mut [Queue], at: usize) {
    let waiting = waiting(queues);
    let queue = &mut queues[at];
    if queue.logged || queue.held.is_empty() {
        return;
    }
    queue.logged = true;
    log::info!(
        "Warmup holds more than a window of {}, {} ticks, waiting on {}",
        queue.series,
        queue.held.len(),
        listed(&waiting)
    );
}

/// `series`, written one after another.
fn listed(series: &[Series]) -> String {
    let named: Vec<String> = series.iter().map(ToString::to_string).collect();
    named.join(", ")
}

impl Display for Overflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let held: Vec<String> = self
            .held
            .iter()
            .map(|(series, count)| format!("{count} of {series}"))
            .collect();
        write!(
            f,
            "its merge of series held more than its bound of {} ticks of history, {}, \
             waiting on {}; the rest of the history is left out",
            self.limit,
            held.join(", "),
            listed(&self.waiting)
        )
    }
}
