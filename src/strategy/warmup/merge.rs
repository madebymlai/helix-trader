//! Merges a live warmup's history across its series, handing it to the hold (`hold`) in `ts_init`
//! order. A replay requests no warmup and merges nothing.
//!
//! Time is `ts_init` throughout, its order, watermark and ties: a backtest streams data by it, and
//! requests cut their windows by it (`DataResponse::trim_to_bounds`, `range_for_bounds` in
//! `crates/common/src/messages/data/response.rs`). Every history a warmup reads stamps it so: a
//! tick's as made (IB, OKX's trades, cTrader, LSE) or received (Databento, equal to its
//! `ts_event`), a bar's at its close (its `ts_event` is the close for IB, Databento and Nautilus'
//! built bars, the open for OKX's history bars). The hold drops a live tick the history holds by
//! `ts_event` within its series, as a live tick's `ts_init` is when its client received it.
//!
//! Temporary until Nautilus hands a warmup's history in time order across data types:
//! helix-trader#46 (<https://github.com/madebymlai/helix-trader/issues/46>), which holds the
//! evidence and the upstream fix. Each Nautilus request runs its own window pipeline
//! (`dispatch_next_time_range_pipeline_request` in `crates/data/src/engine/time_range.rs`) and its
//! responses reach the actor as they arrive, while `RequestJoin` orders the legs of one data type
//! alone. Once it goes, the warmup requests each window's series as one join. It is separate from
//! the hold, which goes with helix-trader#16.
//!
//! The merge hands a tick on once no pending series can bring an older one. Its watermark is the
//! earliest, over the pending series, of the latest `ts_init` each has brought, and a series that
//! has brought nothing holds everything back. The bars a request builds are a series per bar type,
//! read from the cache at each response (`Merge::response`). Nautilus builds a time bar once data
//! past its close comes, stamping it with that close (`TimeBarAggregator::build_bar` in
//! `crates/data/src/aggregation.rs`), so a bar built later closes no earlier than the latest
//! `ts_init` its data has brought.
//!
//! Ties across series at one `ts_init` go in the order a replay hands them (`tie_rank`): custom
//! data, then ticks in the order a backtest reads its catalog series
//! (`DataRequirements::catalog_series`), then bars, a backtest firing the bar-building timer after
//! the data of its instant (`BacktestEngine::flush_accumulator_events` in
//! `crates/backtest/src/engine.rs`). Within a series, ticks go in the order they came.
//!
//! A series whose request completes, answered or failed, holds the others back no longer, and the
//! rest of its queue goes on in order, its bars too, ahead of the live bars Nautilus builds from
//! then on. A bar built at completion from live data Nautilus held, closing after the warmup's end,
//! goes to the hold as a live one. When the group ends, everything held goes on in order. A
//! response of a series no fed indicator reads goes on at once (see `Warmup::feeding`).
//!
//! Each series must answer in time order. A tick initialized before the latest its series brought,
//! or a bar before the latest its request's data brought, ends the merge as its bound does, and
//! the warmup ends incomplete, naming the series.
//!
//! The history held is bounded by `HELD` bytes (`Tick::size`), set by no configuration. Past it
//! the merge hands on everything held, leaves out the rest of the history, and the warmup ends
//! incomplete, naming the series held back and those holding them; the live ticks the hold holds
//! are handed over and the warmup stops warming (`Merge::is_cut`). The merge logs a series of which
//! it holds more than a window, once a warmup, naming those it waits on.
//!
//! The warmup reaches it through `Merge` alone, at five points, each marked `Temporary` with
//! helix-trader#46: `Merge::start`, `Merge::history`, `Merge::response`, `Merge::end` and
//! `Merge::is_cut`. `Queues` knows nothing of Nautilus; `Merge` reads built bars from the cache.

use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    fmt::Display,
};

use jiff::Timestamp;
use nautilus_common::{
    actor::{DataActor, DataActorNative},
    messages::data::{DataResponse, DataResponseEnvelope},
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::data::{Bar, BarType};

use super::{
    Requested,
    hold::Hold,
    tick::{Series, Tick},
};
use crate::{
    rolls::front,
    strategy::requirements::{CatalogSeries, DataRequirements, TickSeries},
};

/// The most bytes the merge holds across its series (`Tick::size`): 2 GiB.
///
/// A series answered from the catalog at once while another waits on its venue is held whole.
pub(super) const HELD: usize = 2 << 30;

#[derive(Debug)]
pub(super) struct Merge {
    queues: Queues,
    building: RefCell<HashMap<UUID4, Vec<BarType>>>,
    /// The close of the latest bar of each bar type read from the cache.
    read: RefCell<HashMap<BarType, UnixNanos>>,
}

#[derive(Debug)]
struct Queues {
    limit: usize,
    state: RefCell<State>,
}

#[derive(Debug)]
enum State {
    /// A replay, a warmup whose group ended, or one before its start.
    Passing,
    Merging(Vec<Queue>),
    Cut,
}

#[derive(Debug)]
struct Queue {
    series: Series,
    request: UUID4,
    /// Each tick with its `Tick::size`.
    held: VecDeque<(Tick, usize)>,
    bytes: usize,
    /// The latest `ts_init` brought, none before the first tick.
    latest: Option<UnixNanos>,
    pending: bool,
    logged: bool,
}

/// Why the merge ended a warmup incomplete.
#[derive(Debug)]
pub(super) enum Cut {
    /// Past its bound: the ticks and bytes held of each series, and the series waited on.
    Overflow {
        limit: usize,
        held: Vec<(Series, usize, usize)>,
        waiting: Vec<Series>,
    },
    /// A response of `series` brought a tick initialized at `at`, before its `latest`.
    Unordered {
        series: Series,
        at: UnixNanos,
        latest: UnixNanos,
    },
}

impl Queue {
    const fn is_bars(&self) -> bool {
        matches!(self.series, Series::Bars(_))
    }
}

impl Merge {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            queues: Queues::new(limit),
            building: RefCell::default(),
            read: RefCell::default(),
        }
    }

    /// Starts merging the `fed` series among `requested`, ranked on a tie as a replay of `own`
    /// hands them (`tie_rank`). What it held of a warmup before is forgotten.
    pub(super) fn start(
        &self,
        requested: &[Requested],
        own: &DataRequirements,
        fed: &DataRequirements,
        now: Timestamp,
    ) {
        let fed = fed_series(fed, now);
        let mut merged: Vec<(UUID4, Series)> = requested
            .iter()
            .flat_map(|request| {
                let bars = request
                    .built
                    .iter()
                    .map(|bar_type| Series::Bars(bar_type.standard()));
                request
                    .series
                    .clone()
                    .into_iter()
                    .chain(bars)
                    .map(|series| (request.id, series))
            })
            .filter(|(_, series)| fed.contains(series))
            .collect();
        let catalog = own.catalog_series();
        merged.sort_by_cached_key(|(_, series)| tie_rank(series, &catalog, now));
        self.queues.start(merged);
        self.read.borrow_mut().clear();
        *self.building.borrow_mut() = requested
            .iter()
            .map(|request| (request.id, request.built.clone()))
            .collect();
    }

    /// Takes the `ticks` of a response and hands on to `hold` those no pending series can precede.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - An indicator reading a tick fails, the first of which ends the hand-over.
    /// - The merge goes past its bound, ending the warmup incomplete (`ended`).
    /// - A series' history comes out of time order, ending the warmup incomplete (`ended`).
    pub(super) fn history(
        &self,
        ticks: impl IntoIterator<Item = Tick>,
        hold: &Hold,
    ) -> anyhow::Result<()> {
        ended(self.queues.history(ticks, |tick| hold.history(tick))?, hold)
    }

    /// Takes the bars the request of `response` built since its last response, oldest first.
    ///
    /// Bars closing after `end`, the warmup's end, go to `hold` as live ones. At the request's
    /// completion, failed or not, its series holds the others back no longer.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - An indicator fails to read a bar or tick handed on.
    /// - The merge goes past its bound, ending the warmup incomplete (`ended`).
    /// - The bars of a series come out of time order, ending the warmup incomplete (`ended`).
    pub(super) fn response<A>(
        &self,
        actor: &A,
        response: &DataResponseEnvelope,
        end: UnixNanos,
        hold: &Hold,
    ) -> anyhow::Result<()>
    where
        A: DataActor + DataActorNative,
    {
        let request = response.correlation_id();
        let built = self.building.borrow().get(request).cloned();
        if let Some(built) = built {
            let (live, bars): (Vec<Bar>, Vec<Bar>) = self
                .built_bars(actor, &built)
                .into_iter()
                .partition(|bar| bar.ts_init > end);
            for bar in live {
                hold.built_live(Tick::Bar(bar))?;
            }
            let through = response.response().and_then(latest_initialized);
            let cut = self
                .queues
                .built(request, bars, through, |tick| hold.history(tick))?;
            ended(cut, hold)?;
        }

        if response.completed() {
            self.queues.complete(request, |tick| hold.history(tick))?;
            self.building.borrow_mut().remove(request);
        }
        Ok(())
    }

    /// Hands on all the history held. Returns whether the merge was cut in the warmup now ending.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator reading a tick fails; the merge passes history on all the
    /// same, the ticks after the failing one lost.
    pub(super) fn end(&self, hold: &Hold) -> anyhow::Result<bool> {
        let cut = self.queues.is_cut();
        self.queues.end(|tick| hold.history(tick))?;
        Ok(cut)
    }

    pub(super) fn is_cut(&self) -> bool {
        self.queues.is_cut()
    }

    #[cfg(test)]
    pub(super) fn pending(&self) -> Vec<UUID4> {
        self.building.borrow().keys().copied().collect()
    }

    /// A node caches a bar type's bars under its standard type, a composite's without its source
    /// (`BarType::standard`, `TimeBarAggregator::new`).
    fn built_bars<A>(&self, actor: &A, bar_types: &[BarType]) -> Vec<Bar>
    where
        A: DataActor + DataActorNative,
    {
        let cache = actor.cache();
        let mut read = self.read.borrow_mut();
        let mut bars = Vec::new();

        for bar_type in bar_types {
            let standard = bar_type.standard();
            let after = read.get(&standard).copied();

            // The cache holds the newest first
            let mut new: Vec<Bar> = cache
                .bars(&standard)
                .unwrap_or_default()
                .into_iter()
                .take_while(|bar| after.is_none_or(|after| bar.ts_event > after))
                .collect();
            new.reverse();
            if let Some(latest) = new.last() {
                read.insert(standard, latest.ts_event);
            }
            bars.extend(new);
        }
        bars
    }
}

/// Ends the warmup incomplete if `cut`, handing over the live ticks `hold` holds.
///
/// # Errors
///
/// Returns an error if:
/// - An indicator reading a live tick fails.
/// - The merge was cut, saying why.
fn ended(cut: Option<Cut>, hold: &Hold) -> anyhow::Result<()> {
    let Some(cut) = cut else {
        return Ok(());
    };
    hold.release()?;
    anyhow::bail!("the warmup is incomplete: {cut}")
}

// Bars by their standard type, a root's ticks and custom series on its front contract
// (`rolls::front`, `CustomSeries::at`).
fn fed_series(fed: &DataRequirements, now: Timestamp) -> Vec<Series> {
    let bars = fed
        .bar_types
        .iter()
        .map(|bar_type| Series::Bars(bar_type.standard()));
    let ticks = fed.ticks.iter().map(|tick| {
        Series::of(&TickSeries {
            data: tick.data.clone(),
            instrument_id: front(tick.instrument_id, now),
        })
    });
    let custom = fed
        .custom
        .iter()
        .map(|series| Series::Custom(series.at(now).data_type().clone()));
    bars.chain(ticks).chain(custom).collect()
}

// Custom data first, a backtest adding its stream before the catalog's (`backtest::node`), then
// ticks in the order of `catalog` with the harness's bars left out, then built bars, the
// aggregator firing after the data of its instant. Within one, the order requested.
fn tie_rank(series: &Series, catalog: &[CatalogSeries], now: Timestamp) -> (u8, usize) {
    match series {
        Series::Custom(_) => (0, 0),
        Series::Bars(_) => (2, 0),
        ticks => {
            let at = catalog
                .iter()
                .position(|source| match source {
                    CatalogSeries::Ticks(tick) => {
                        Series::of(&TickSeries {
                            data: tick.data.clone(),
                            instrument_id: front(tick.instrument_id, now),
                        }) == *ticks
                    }
                    CatalogSeries::Bars(_) => false,
                })
                .expect("a tick series requested is among the catalog series");
            (1, at)
        }
    }
}

// Nautilus builds bars from quotes, trades and bars alone
fn latest_initialized(response: &DataResponse) -> Option<UnixNanos> {
    match response {
        DataResponse::Quotes(quotes) => quotes.data.iter().map(|quote| quote.ts_init).max(),
        DataResponse::Trades(trades) => trades.data.iter().map(|trade| trade.ts_init).max(),
        DataResponse::Bars(bars) => bars.data.iter().map(|bar| bar.ts_init).max(),
        _ => None,
    }
}

impl Queues {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            state: RefCell::new(State::Passing),
        }
    }

    /// `requests` are in the order their ticks go on a tie.
    fn start(&self, requests: impl IntoIterator<Item = (UUID4, Series)>) {
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

    /// Passing, hands `ticks` on as they come, and cut, none.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `hand` fails, the ticks after the failure kept held.
    /// - A tick cannot be sized (`Tick::size`).
    ///
    /// Returns the cut if a tick is initialized before the latest its series brought, or if the
    /// merge holds more than its bound once it handed on what it could. Having handed on all it
    /// held, it is cut.
    fn history(
        &self,
        ticks: impl IntoIterator<Item = Tick>,
        hand: impl FnMut(&Tick) -> anyhow::Result<()>,
    ) -> anyhow::Result<Option<Cut>> {
        self.take(ticks, None, hand)
    }

    /// `through` is the latest `ts_init` of the response's data: a bar built later closes no
    /// earlier.
    ///
    /// # Errors
    ///
    /// Returns an error if `hand` fails, or the cut, as `history`.
    fn built(
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

        // The queues this response brought ticks to, and whether each held earlier ticks
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

    /// Ends `request`: its series holds the others back no longer.
    ///
    /// # Errors
    ///
    /// Returns an error if `hand` fails, as `history`.
    fn complete(
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

    /// Ends the group: hands on everything held, then passes history on.
    ///
    /// # Errors
    ///
    /// Returns an error if `hand` fails, as `history`; the ticks after the failing one are lost.
    fn end(&self, mut hand: impl FnMut(&Tick) -> anyhow::Result<()>) -> anyhow::Result<()> {
        let state = std::mem::replace(&mut *self.state.borrow_mut(), State::Passing);
        let State::Merging(mut queues) = state else {
            return Ok(());
        };

        for queue in &mut queues {
            queue.pending = false;
        }
        hand_on(&mut queues, &mut hand)
    }

    fn is_cut(&self) -> bool {
        matches!(*self.state.borrow(), State::Cut)
    }
}

/// Hands on all `queues` hold.
fn cut_off(
    queues: &mut [Queue],
    hand: &mut impl FnMut(&Tick) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    for queue in queues.iter_mut() {
        queue.pending = false;
    }
    hand_on(queues, hand)
}

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

/// The pending series the merge waits on: those that brought nothing yet, else those whose latest
/// is the earliest.
fn waiting(queues: &[Queue]) -> Vec<Series> {
    let pending = queues.iter().filter(|queue| queue.pending);
    let earliest = pending.clone().map(|queue| queue.latest).min().flatten();
    pending
        .filter(|queue| queue.latest.is_none() || queue.latest == earliest)
        .map(|queue| queue.series.clone())
        .collect()
}

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

    /// Warmups drawn, each from a seed in `0..CASES`, so a failure names its seed.
    const CASES: u64 = 10_000;

    /// History is initialized at `0..SPAN` ns: few, so ties are common.
    const SPAN: u64 = 12;

    /// Series, `ts_init` and serial, the unique `ts_event` a tick was drawn with.
    type Key = (Series, UnixNanos, UnixNanos);

    fn key(tick: &Tick) -> Key {
        (tick.series(), tick.ts_init(), tick.ts_event())
    }

    struct Request {
        id: UUID4,
        series: Series,
        bars: Option<BarType>,
    }

    enum Event {
        History(Vec<Tick>),
        Built(UUID4, Vec<Bar>, Option<UnixNanos>),
        Complete(UUID4),
    }

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

    fn times(rng: &mut fastrand::Rng, count: usize) -> Vec<u64> {
        let mut times: Vec<u64> = (0..count).map(|_| rng.u64(0..SPAN)).collect();
        times.sort_unstable();
        times
    }

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

    /// History in windows, each in time order and no older than the last, and built bars, each
    /// initialized no earlier than the data brought before, as Nautilus builds a bar once data
    /// past its close comes; then, unless the group ends first, the end. A failed request answers
    /// only its first windows.
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

    #[derive(Default)]
    struct Tally {
        /// Ticks taken in the order brought, none past its bound, with their sizes.
        taken: Vec<(Key, usize)>,
        taken_bytes: usize,
        handed: Vec<Key>,
        handed_bytes: usize,
        /// Ticks taken when it went past its bound.
        cut: Option<usize>,
    }

    impl Tally {
        fn held(&self) -> usize {
            self.taken_bytes - self.handed_bytes
        }
    }

    /// Returns whether the warmup of `seed` went past its bound and whether it handed on ties
    /// across series.
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

        let merge = Queues::new(limit);
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

    /// The merge hands on every tick once, in `ts_init` order, ties across series in start order,
    /// whatever the windows, response order and failed or unended requests. It never holds more
    /// than its bound, past which it hands on all and takes no more. Each draw is up to four
    /// requests, a quarter bounded at a few ticks' worth.
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
