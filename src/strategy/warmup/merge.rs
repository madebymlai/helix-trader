//! The merge of a live warmup's history across its series: the warmup
//! queues the responses of each requested series an indicator it feeds
//! reads, quotes, trades, book depth and deltas, and custom data alike,
//! and the bars each request builds that one reads, and
//! hands them on in time order (`ts_init`), to the hold of live ticks and
//! bars (`hold`) and through it to the features. A replay requests no
//! warmup and merges nothing: Nautilus replays its data in time order
//! already.
//!
//! Its time is `ts_init` throughout, its order, its watermark and its
//! ties, because that is the order a replay hands data in (a backtest
//! streams its data by `ts_init`) and the one a request's windows are cut
//! by (`DataResponse::trim_to_bounds`, `range_for_bounds` in
//! `crates/common/src/messages/data/response.rs`), so a pending series
//! brings nothing initialized before its latest. Every history a warmup
//! reads stamps `ts_init` so that it means this: a tick's as it was made
//! (IB, OKX's trades, cTrader, LSE) or received at the venue, which is
//! its `ts_event` too (Databento), a bar's at its close, whether its
//! `ts_event` is its close (IB, Databento, the bars Nautilus builds) or
//! its open (OKX's history bars). The hold drops a live tick the history holds by
//! `ts_event` within its series, not by `ts_init`: a live tick's `ts_init`
//! is when its client received it (see `hold`).
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
//! pending, the earliest of the latest `ts_init` each has brought, a
//! series that has brought nothing yet holding everything back. A request
//! answers its windows in order, so a pending series brings nothing older
//! than its latest. The bars a request builds are a series of their own,
//! one per bar type, read from the cache at each response of the request
//! (`Warmup::on_data_response`): Nautilus builds a time bar once data past
//! its close comes, stamping it with that close
//! (`TimeBarAggregator::build_bar` in `crates/data/src/aggregation.rs`), so
//! a bar the request builds later closes no earlier than the latest
//! `ts_init` its data has brought, a history's data initialized as made,
//! where its bars stand. Ties across
//! series, at one `ts_init`, go in the order the warmup starts the merge
//! with, the order a replay hands them in, so that a replay and a live
//! warmup agree on a tie (`tie_rank` in `strategy::warmup`): custom data
//! first, a backtest adding its stream before the catalog's; then the
//! ticks in the order a backtest reads its catalog series
//! (`DataRequirements::catalog_series`); then the bars, a backtest firing
//! the timer building a bar after the data of its instant
//! (`BacktestEngine::flush_accumulator_events` in
//! `crates/backtest/src/engine.rs`). Within a series, the ticks go in the
//! order they came. A series
//! whose request completes, answered or failed, holds the others back no
//! longer, and the rest of its queue goes on in order, its bars' too: held
//! past their request's completion while others are pending, they reach
//! the features before the live bars Nautilus builds from then on all the
//! same, which the hold holds (see `hold`). A bar a request builds at its
//! completion from the live data Nautilus held for it, closing after the
//! warmup's end, never reaches the merge: the warmup hands it the hold as
//! a live one (`Warmup::on_data_response`). When the group
//! ends, everything held goes on in order. A response of a series no fed
//! indicator reads, a conversion pair's quotes or the traded instrument's,
//! goes on at once, holding nothing back and held by nothing (see
//! `Warmup::feeding`).
//!
//! The merge relies on each series answering in time order, and checks
//! it: a tick of a series initialized before the latest the series
//! brought, in a response out of time order or a window older than one
//! before it, or a bar before the latest its request's data brought, ends
//! the merge as its bound does
//! (below), the ticks the response brought before it taken, and the
//! warmup ends incomplete, naming the series, rather than hand on a
//! history out of time order.
//!
//! The history held is bounded by a budget of bytes (`HELD`, 2 GiB), each
//! tick and bar counted by what it takes, its own size and what it keeps
//! on the heap, a batch of deltas its deltas, a snapshot of depth its box,
//! a point of custom data its data (`Tick::size`): past the bound the
//! merge hands on everything it holds, in order, and leaves out the rest
//! of the warmup's history, older than what the features then read, and
//! the warmup ends incomplete, naming the series held back and those
//! holding them. The bound is the merge's own, set by no configuration: it
//! goes with the merge. It logs
//! a series of which it holds more than a window, once a warmup, naming
//! those it waits on, so that a slow series shows.

use std::{cell::RefCell, collections::VecDeque, fmt::Display};

use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::data::Bar;

use super::tick::{Series, Tick};

/// The most bytes the merge holds, of all its series together, each tick
/// and bar held counted by what it takes (`Tick::size`): 2 GiB. A series
/// answered from the catalog at once while another waits on its venue is
/// held whole, so the bound is that of a whole warmup of the series
/// answered first. It goes with the merge, so no configuration sets it.
pub(super) const HELD: usize = 2 << 30;

/// What merges a live warmup's history across its series (see the module).
#[derive(Debug)]
pub(super) struct Merge {
    /// The most bytes it holds.
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
    /// Its ticks held, in the order they came, each with what it takes
    /// (`Tick::size`).
    held: VecDeque<(Tick, usize)>,
    /// What its ticks held take together.
    bytes: usize,
    /// The latest `ts_init` it brought, none before its first tick.
    latest: Option<UnixNanos>,
    /// Whether its request has not ended yet.
    pending: bool,
    /// Whether the merge logged holding more than a window of it.
    logged: bool,
}

/// Why the merge ended a warmup incomplete, handing on all it held and
/// leaving out the rest of the history.
#[derive(Debug)]
pub(super) enum Cut {
    /// Past its bound: what it held, the ticks and bytes of each series,
    /// and the series it waited on.
    Overflow {
        limit: usize,
        held: Vec<(Series, usize, usize)>,
        waiting: Vec<Series>,
    },
    /// A response of `series` brought a tick initialized at `at`, before
    /// `latest`, the latest the series had brought: out of time order.
    Unordered {
        series: Series,
        at: UnixNanos,
        latest: UnixNanos,
    },
}

impl Queue {
    /// Whether it holds the bars a request builds.
    const fn is_bars(&self) -> bool {
        matches!(self.series, Series::Bars(_))
    }
}

impl Merge {
    /// A merge holding at most `limit` bytes, passing history on as it
    /// comes until a live warmup starts it.
    pub(super) fn new(limit: usize) -> Self {
        Self {
            limit,
            state: RefCell::new(State::Passing),
        }
    }

    /// Merges the history of `requests`, each a request's id and a series
    /// of it, its ticks' or the bars of a bar type it builds, in the order
    /// their ticks go on a tie, a live start's group pending; what it held
    /// of a warmup before is forgotten.
    pub(super) fn start(&self, requests: impl IntoIterator<Item = (UUID4, Series)>) {
        let queues = requests
            .into_iter()
            .map(|(request, series)| Queue {
                series,
                request,
                held: VecDeque::new(),
                bytes: 0,
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
    /// hand-over, the ticks after it kept held, or if a tick cannot be
    /// sized (`Tick::size`). Returns the cut if a tick of a series is
    /// initialized before the latest the series brought, the ticks before
    /// it taken and the rest left out, or if the merge holds more bytes
    /// than its bound once it handed on what it could: having handed on all
    /// it held, it is cut.
    pub(super) fn history(
        &self,
        ticks: impl IntoIterator<Item = Tick>,
        hand: impl FnMut(&Tick) -> anyhow::Result<()>,
    ) -> anyhow::Result<Option<Cut>> {
        self.take(ticks, None, hand)
    }

    /// Takes `bars`, those `request` built from a response of it, oldest
    /// first, and `through`, the latest `ts_init` of that response's data,
    /// if it held any: a bar it builds from then on closes no earlier (see
    /// the module). Hands on as `history` does.
    ///
    /// # Errors
    ///
    /// Returns an error if `hand` fails, or the cut, as `history`.
    pub(super) fn built(
        &self,
        request: &UUID4,
        bars: impl IntoIterator<Item = Bar>,
        through: Option<UnixNanos>,
        hand: impl FnMut(&Tick) -> anyhow::Result<()>,
    ) -> anyhow::Result<Option<Cut>> {
        self.take(
            bars.into_iter().map(Tick::Bar),
            Some((request, through)),
            hand,
        )
    }

    /// Takes `ticks`, and, `built` the request of bars among them and the
    /// latest `ts_init` of its response, moves the latest of that
    /// request's bars there; hands on as `history` does.
    fn take(
        &self,
        ticks: impl IntoIterator<Item = Tick>,
        built: Option<(&UUID4, Option<UnixNanos>)>,
        mut hand: impl FnMut(&Tick) -> anyhow::Result<()>,
    ) -> anyhow::Result<Option<Cut>> {
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
            let at_init = tick.ts_init();
            if let Some(latest) = queue.latest.filter(|&latest| at_init < latest) {
                let unordered = Cut::Unordered {
                    series,
                    at: at_init,
                    latest,
                };
                let handed = cut_off(queues, &mut hand);
                *state = State::Cut;
                return handed.map(|()| Some(unordered));
            }

            if !brought.iter().any(|&(of, _)| of == at) {
                brought.push((at, !queue.held.is_empty()));
            }
            queue.latest = Some(queue.latest.map_or(at_init, |latest| latest.max(at_init)));
            let size = tick.size()?;
            queue.bytes += size;
            queue.held.push_back((tick, size));
        }

        if let Some((request, Some(through))) = built {
            for queue in queues
                .iter_mut()
                .filter(|queue| queue.request == *request && queue.is_bars())
            {
                queue.latest = Some(queue.latest.map_or(through, |latest| latest.max(through)));
            }
        }
        hand_on(queues, &mut hand)?;
        for (at, _) in brought.into_iter().filter(|&(_, earlier)| earlier) {
            log_window(queues, at);
        }
        let held: usize = queues.iter().map(|queue| queue.bytes).sum();
        if held <= self.limit {
            return Ok(None);
        }
        let overflow = Cut::Overflow {
            limit: self.limit,
            held: queues
                .iter()
                .filter(|queue| !queue.held.is_empty())
                .map(|queue| (queue.series.clone(), queue.held.len(), queue.bytes))
                .collect(),
            waiting: waiting(queues),
        };
        let handed = cut_off(queues, &mut hand);
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

        for queue in queues.iter_mut().filter(|queue| queue.request == *request) {
            queue.pending = false;
        }
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

/// Hands on through `hand`, in order, all `queues` hold, as no series is
/// pending any longer.
fn cut_off(
    queues: &mut [Queue],
    hand: &mut impl FnMut(&Tick) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    for queue in queues.iter_mut() {
        queue.pending = false;
    }
    hand_on(queues, hand)
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
            .filter_map(|(at, queue)| queue.held.front().map(|(tick, _)| (tick.ts_init(), at)))
            .min();
        let Some((at_init, at)) = next else {
            return Ok(());
        };
        let free = queues.iter().enumerate().all(|(other, queue)| {
            other == at
                || !queue.pending
                || queue
                    .latest
                    .is_some_and(|latest| at_init < latest || (at_init == latest && at < other))
        });

        if !free {
            return Ok(());
        }
        let queue = &mut queues[at];
        let (tick, size) = queue.held.pop_front().expect("the queue's front");
        queue.bytes -= size;
        hand(&tick)?;
    }
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

impl Display for Cut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Overflow {
                limit,
                held,
                waiting,
            } => {
                let held: Vec<String> = held
                    .iter()
                    .map(|(series, count, bytes)| {
                        format!("{count} ticks of {series}, {bytes} bytes")
                    })
                    .collect();
                write!(
                    f,
                    "its merge of series held more than its bound of {limit} bytes of history, \
                     {}, waiting on {}; the rest of the history is left out",
                    held.join(", "),
                    listed(waiting)
                )
            }
            Self::Unordered { series, at, latest } => write!(
                f,
                "the history of {series} came out of time order, a tick initialized at {at} \
                 after one at {latest}; the rest of the history is left out"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nautilus_model::{
        data::{
            BarType, CustomData, DataType, OrderBookDelta, OrderBookDeltas, OrderBookDepth,
            QuoteTick, TradeTick, stubs::stub_depth10,
        },
        enums::AggressorSide,
        identifiers::{InstrumentId, TradeId},
        types::{Price, Quantity},
    };
    use rstest::rstest;

    use super::*;

    /// How many warmups the property draws, each from its own seed, the
    /// seeds `0..CASES`, so that a failure names the seed reproducing it.
    const CASES: u64 = 10_000;

    /// The times a warmup's history is initialized at are `0..SPAN` ns: few,
    /// so that ties across series and within one are common.
    const SPAN: u64 = 12;

    /// What the property reads of a tick handed on: its series, its
    /// `ts_init`, and its serial, the `ts_event` it was drawn with, unique
    /// over a warmup.
    type Key = (Series, UnixNanos, UnixNanos);

    fn key(tick: &Tick) -> Key {
        (tick.series(), tick.ts_init(), tick.ts_event())
    }

    /// A request of a drawn warmup: its id, the series it answers and, if
    /// it builds bars, their bar type.
    struct Request {
        id: UUID4,
        series: Series,
        bars: Option<BarType>,
    }

    /// What a drawn warmup brings the merge, in the order the node brings
    /// it: a response's history, the bars a request built at a response of
    /// it and the latest `ts_init` of that response's data, or the end of a
    /// request.
    enum Event {
        History(Vec<Tick>),
        Built(UUID4, Vec<Bar>, Option<UnixNanos>),
        Complete(UUID4),
    }

    /// The tick of `series` initialized at `at`, its serial `serial`.
    fn tick_of(series: &Series, serial: u64, at: u64) -> Tick {
        let (made, at) = (UnixNanos::from(serial), UnixNanos::from(at));
        let price = Price::from("100.00");
        let size = Quantity::from(1);

        match series {
            Series::Quotes(id) => {
                Tick::Quote(QuoteTick::new(*id, price, price, size, size, made, at))
            }
            Series::Trades(id) => Tick::Trade(TradeTick::new(
                *id,
                price,
                size,
                AggressorSide::Buy,
                TradeId::new(serial.to_string()),
                made,
                at,
            )),
            Series::Depth(id) => Tick::Depth(Box::new(OrderBookDepth {
                instrument_id: *id,
                ts_event: made,
                ts_init: at,
                ..stub_depth10()
            })),
            Series::Deltas(id) => Tick::Deltas(OrderBookDeltas::new(
                *id,
                vec![OrderBookDelta::clear(*id, serial, made, at)],
            )),
            Series::Custom(data_type) => {
                let row = ::lse::LseRow::new(
                    "bond_yields".into(),
                    data_type.identifier().unwrap_or_default().into(),
                    serde_json::Map::new(),
                    made,
                    at,
                );
                Tick::Custom(CustomData::new(Arc::new(row), data_type.clone()))
            }
            Series::Bars(bar_type) => Tick::Bar(Bar::new(
                *bar_type, price, price, price, price, size, made, at,
            )),
        }
    }

    /// `count` times drawn from `0..SPAN`, oldest first.
    fn times(rng: &mut fastrand::Rng, count: usize) -> Vec<u64> {
        let mut times: Vec<u64> = (0..count).map(|_| rng.u64(0..SPAN)).collect();
        times.sort_unstable();
        times
    }

    /// The tick series a warmup draws its requests from, and the bar type
    /// each of quotes and trades may build.
    fn pool() -> Vec<(Series, Option<BarType>)> {
        let (a, b) = (
            InstrumentId::from("AAA.XNAS"),
            InstrumentId::from("BBB.XNAS"),
        );
        let custom = |identifier: &str| {
            Series::Custom(DataType::new("LseRow", None, Some(identifier.to_string())))
        };
        vec![
            (
                Series::Quotes(a),
                Some(BarType::from("AAA.XNAS-1-MINUTE-MID-INTERNAL")),
            ),
            (
                Series::Trades(a),
                Some(BarType::from("AAA.XNAS-1-MINUTE-LAST-INTERNAL")),
            ),
            (
                Series::Quotes(b),
                Some(BarType::from("BBB.XNAS-1-MINUTE-MID-INTERNAL")),
            ),
            (Series::Depth(a), None),
            (Series::Deltas(b), None),
            (custom("US10Y"), None),
            (custom("US2Y"), None),
        ]
    }

    /// The events of `request` as a node brings them, its `serial` the
    /// next serial free: its history in windows, each in time order and no
    /// older than the window before, the bars it builds read at each of its
    /// responses, each initialized no earlier than the latest data its
    /// windows brought before, as Nautilus builds a bar once data past its
    /// close comes; then, unless the group ends first, its end, answered
    /// or failed, a failed request answering only its first windows.
    fn events_of(rng: &mut fastrand::Rng, request: &Request, serial: &mut u64) -> Vec<Event> {
        let mut next = |series: &Series, at: u64| {
            *serial += 1;
            tick_of(series, *serial, at)
        };
        let count = rng.usize(0..=10);
        let ticks = times(rng, count);
        let count = if request.bars.is_some() {
            rng.usize(0..=6)
        } else {
            0
        };
        let mut bars = times(rng, count);
        bars.reverse();
        let count = rng.usize(0..=2);
        let mut cuts = times(rng, count);
        cuts.push(SPAN);
        let failed_after = (rng.u8(0..4) == 0).then(|| rng.usize(0..=cuts.len()));
        let mut events = Vec::new();
        let mut from = 0;

        for (window, &to) in cuts.iter().enumerate() {
            if failed_after == Some(window) {
                break;
            }
            let window: Vec<Tick> = ticks
                .iter()
                .filter(|&&at| (from..to).contains(&at))
                .map(|&at| next(&request.series, at))
                .collect();
            from = to;
            let through = window.iter().map(Tick::ts_init).max();
            events.push(Event::History(window));

            if let Some(bar_type) = request.bars {
                let mut built = Vec::new();

                while bars
                    .last()
                    .is_some_and(|&at| through.is_some_and(|through| at < through.as_u64()))
                {
                    let at = bars.pop().expect("a bar");
                    let Tick::Bar(bar) = next(&Series::Bars(bar_type), at) else {
                        unreachable!("a bar's tick")
                    };
                    built.push(bar);
                }
                events.push(Event::Built(request.id, built, through));
            }
        }

        if let (Some(bar_type), None) = (request.bars, failed_after) {
            let built = bars
                .iter()
                .rev()
                .map(|&at| match next(&Series::Bars(bar_type), at) {
                    Tick::Bar(bar) => bar,
                    _ => unreachable!("a bar's tick"),
                })
                .collect();
            events.push(Event::Built(request.id, built, None));
        }

        if rng.u8(0..8) != 0 {
            events.push(Event::Complete(request.id));
        }
        events
    }

    /// What the merge was brought and handed on over a drawn warmup.
    #[derive(Default)]
    struct Tally {
        /// Each tick it took and holds or handed, in the order brought,
        /// none it left out past its bound, with what it takes.
        taken: Vec<(Key, usize)>,
        taken_bytes: usize,
        /// Each tick it handed on, in order.
        handed: Vec<Key>,
        handed_bytes: usize,
        /// How many ticks it had taken when it went past its bound.
        cut: Option<usize>,
    }

    impl Tally {
        fn held(&self) -> usize {
            self.taken_bytes - self.handed_bytes
        }
    }

    /// Draws the warmup of `seed`, brings it to a merge, and checks what it
    /// handed on; returns whether it went past its bound and whether ties
    /// across series were handed on, so that the draws are seen to reach
    /// both.
    fn check(seed: u64) -> (bool, bool) {
        let mut rng = fastrand::Rng::with_seed(seed);
        let mut pool = pool();
        rng.shuffle(&mut pool);
        let requests: Vec<Request> = pool
            .into_iter()
            .take(rng.usize(1..=4))
            .map(|(series, bars)| Request {
                id: UUID4::new(),
                series,
                bars: bars.filter(|_| rng.bool()),
            })
            .collect();
        let mut merged: Vec<(UUID4, Series)> = requests
            .iter()
            .flat_map(|request| {
                let bars = request
                    .bars
                    .map(|bar_type| (request.id, Series::Bars(bar_type)));
                [(request.id, request.series.clone())]
                    .into_iter()
                    .chain(bars)
            })
            .collect();
        rng.shuffle(&mut merged);
        let rank = |series: &Series| {
            merged
                .iter()
                .position(|(_, of)| of == series)
                .expect("a series merged")
        };
        let limit = if rng.u8(0..4) == 0 {
            rng.usize(1..=12) * size_of::<Tick>()
        } else {
            HELD
        };
        let mut serial = 0;
        let mut pending: Vec<Vec<Event>> = requests
            .iter()
            .map(|request| {
                let mut events = events_of(&mut rng, request, &mut serial);
                events.reverse();
                events
            })
            .collect();

        let merge = Merge::new(limit);
        merge.start(merged.clone());
        let mut tally = Tally::default();

        loop {
            let open: Vec<usize> = (0..pending.len())
                .filter(|&at| !pending[at].is_empty())
                .collect();
            let Some(&at) = rng.choice(&open) else {
                break;
            };
            let event = pending[at].pop().expect("an event");
            let brought: Vec<Tick> = match &event {
                Event::History(ticks) => ticks.clone(),
                Event::Built(_, bars, _) => bars.iter().copied().map(Tick::Bar).collect(),
                Event::Complete(_) => Vec::new(),
            };
            let (held, handed) = (tally.held(), tally.handed.len());
            let brought_bytes: usize = brought.iter().map(|tick| tick.size().unwrap()).sum();
            if tally.cut.is_none() {
                for tick in &brought {
                    tally.taken.push((key(tick), tick.size().unwrap()));
                }
                tally.taken_bytes += brought_bytes;
            }
            let hand = |tick: &Tick| {
                tally.handed.push(key(tick));
                tally.handed_bytes += tick.size()?;
                Ok(())
            };
            let cut = match event {
                Event::History(ticks) => merge.history(ticks, hand),
                Event::Built(request, bars, through) => merge.built(&request, bars, through, hand),
                Event::Complete(request) => merge.complete(&request, hand).map(|()| None),
            }
            .unwrap_or_else(|e| panic!("seed {seed}: {e:#}"));
            let overflow = match cut {
                Some(Cut::Unordered { .. }) => panic!("seed {seed}: {}", cut.unwrap()),
                overflow => overflow,
            };

            if tally.cut.is_some() {
                assert_eq!(
                    tally.handed.len(),
                    handed,
                    "seed {seed}: handed past its bound"
                );
            } else if overflow.is_some() {
                assert!(
                    held + brought_bytes > limit,
                    "seed {seed}: past a bound of {limit} bytes holding {held} and brought \
                     {brought_bytes}"
                );
                assert_eq!(tally.held(), 0, "seed {seed}: held past its bound");
                tally.cut = Some(tally.taken.len());
            } else {
                assert!(
                    tally.held() <= limit,
                    "seed {seed}: holds {} bytes, bounded at {limit}",
                    tally.held()
                );
            }
        }
        merge
            .end(|tick| {
                tally.handed.push(key(tick));
                Ok(())
            })
            .unwrap_or_else(|e| panic!("seed {seed}: {e:#}"));

        let mut expected: Vec<Key> = tally.taken.iter().map(|(key, _)| key.clone()).collect();
        expected.sort_by_key(|(series, at, _)| (*at, rank(series)));
        assert_eq!(tally.handed, expected, "seed {seed}");
        let tied = expected
            .windows(2)
            .any(|pair| pair[0].1 == pair[1].1 && pair[0].0 != pair[1].0);
        (tally.cut.is_some(), tied)
    }

    /// The merge hands on every tick it takes once, in `ts_init` order, ties
    /// across series in the order it was started with, ticks of one series in
    /// the order brought, whatever the series, kinds and bars among them, the
    /// windows they come in, the order their responses arrive in, and which
    /// of their requests fail or never end before the group does; and it
    /// never holds more than its bound, past which it hands on all it holds
    /// and takes no more. Each draw is a warmup of up to four requests, a
    /// quarter of them bounded at a few ticks' worth.
    #[rstest]
    fn prop_the_merge_hands_on_every_tick_once_in_ts_init_and_tie_order_within_its_bound() {
        let (mut cut, mut tied) = (0, 0);

        for seed in 0..CASES {
            let (was_cut, was_tied) = check(seed);
            cut += usize::from(was_cut);
            tied += usize::from(was_tied);
        }

        assert!(cut > 0 && tied > 0, "{cut} draws cut, {tied} tied");
    }
}
