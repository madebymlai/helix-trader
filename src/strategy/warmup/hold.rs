//! The hold of a live warmup's quotes, trades, book depth and deltas: the
//! warmup hands the indicators reading them, a model strategy's features,
//! those of its responses as they come, holds the live ones the strategy
//! hands it until its group ends, the last window answered, and then hands
//! those over in the order they came; from then on live ones go straight
//! through. A replay requests no warmup and holds nothing.
//!
//! It stands in for Nautilus. Nautilus hands a live tick to the indicators
//! an actor registered for it as the tick arrives (`DataActor::handle_quote`
//! in `common/src/actor/data_actor.rs`), and a response's ticks to the same
//! indicators (`handle_quotes_response`), holding nothing while a request is
//! pending; it holds live ticks for its bar aggregators alone (`WarmupBuffer`
//! in `data/src/engine/handlers.rs`, which `data/src/engine/bar.rs` gives
//! them). A warmup is answered a window at a time,
//! the node running between, so features registered for ticks read the live
//! ones before the older windows, and time runs backwards for them. So the
//! features are not registered for quotes and trades (`FeatureSet::register`)
//! and the strategy hands the warmup its live ones (`model::handle`'s
//! `model_data_hooks!`). Book depth and deltas Nautilus hands no indicator
//! at all (`ActorIndicator` reads quotes, trades and bars): an actor gets
//! them in its own handlers (`DataActor::on_book_depth`, `on_book_deltas`),
//! as it gets a response's in `on_historical_book_depth` and
//! `on_historical_book_deltas`, and Nautilus holds nothing for the handlers
//! while a request is pending either. So the strategy hands the warmup its
//! live depth and deltas from its handlers too, held as ticks are. Once
//! Nautilus holds live data for an actor's indicators and handlers while a
//! request of it with `update_subscriptions` is pending, releasing them in
//! time order after the response and deduplicated against it, this module
//! goes: the features are registered for quotes and trades again, the
//! strategy hands the warmup no ticks, and its handlers hand its depth and
//! deltas to the features straight: helix-trader issue #16
//! (<https://github.com/madebymlai/helix-trader/issues/16>).
//!
//! A live tick the history holds is dropped, held or not, judged within its
//! series, the quotes, the trades, the depth or the deltas of one
//! instrument, a batch of deltas by its last delta's time. The series' seam is
//! the latest `ts_event` of its ticks the warmup handed over; a live tick of
//! the series made at or before it (`ts_event`) is dropped, one made after
//! it handed over. Made at the seam itself, it is the venue's resend of the
//! history's last tick, which IC and IB send on a subscription, or another
//! tick of that instant, which the history, answered after it, is taken to
//! hold; made before it, it is older than what the features read, and would
//! run their time backwards. `ts_init` is not compared: a client stamps a
//! live tick's on receipt (cTrader, `crates/ctrader/src/data/spots.rs`) and
//! a history's as the venue or the catalog gives it, so a resent tick's
//! comes after the history's with its `ts_event` the same, and Nautilus
//! trims a response to the warmup's end by `ts_init`. A series with no
//! history drops nothing; a new warmup forgets the seams of the one before.

use std::{
    cell::RefCell,
    collections::HashMap,
    fmt::{self, Debug},
    rc::Rc,
};

use nautilus_common::actor::indicators::SharedActorIndicator;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{OrderBookDeltas, OrderBookDepth, QuoteTick, TradeTick},
    identifiers::InstrumentId,
};

use super::BookIndicator;

/// A quote, a trade, a snapshot of a book's depth or a batch of its deltas.
#[derive(Debug, Clone)]
pub(super) enum Tick {
    Quote(QuoteTick),
    Trade(TradeTick),
    /// Boxed: ten levels a side, against a quote's few fields.
    Depth(Box<OrderBookDepth>),
    Deltas(OrderBookDeltas),
}

/// The series of a tick, within which a live tick is judged against the
/// history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Series {
    Quotes(InstrumentId),
    Trades(InstrumentId),
    Depth(InstrumentId),
    Deltas(InstrumentId),
}

/// What hands a warmup's quotes, trades, depth and deltas to the
/// indicators reading them, holding the live ones while a live start's
/// group is pending.
#[derive(Default)]
pub(super) struct Hold {
    /// What the quotes and trades are handed to.
    indicators: Vec<SharedActorIndicator>,
    /// What the depth and deltas are handed to.
    books: Vec<Rc<dyn BookIndicator>>,
    /// The live ticks held while a group is pending, in the order they
    /// came; none when live ticks go straight through.
    held: RefCell<Option<Vec<Tick>>>,
    /// The seam of each series: the latest `ts_event` of its ticks the
    /// history handed over.
    seams: RefCell<HashMap<Series, UnixNanos>>,
}

impl Hold {
    /// Hands the quotes and trades to `indicator` too.
    pub(super) fn feeding(&mut self, indicator: SharedActorIndicator) {
        self.indicators.push(indicator);
    }

    /// Hands the depth and deltas to `books` too.
    pub(super) fn feeding_books(&mut self, books: Rc<dyn BookIndicator>) {
        self.books.push(books);
    }

    /// Holds the live ticks from now, a live start's group pending, with
    /// no seam yet.
    pub(super) fn hold(&self) {
        *self.held.borrow_mut() = Some(Vec::new());
        self.seams.borrow_mut().clear();
    }

    /// Hands `tick`, of a response, to the indicators, its series' seam
    /// moved to it if it is the latest.
    ///
    /// # Errors
    ///
    /// Returns the first error of an indicator reading it.
    pub(super) fn history(&self, tick: Tick) -> anyhow::Result<()> {
        self.seams
            .borrow_mut()
            .entry(tick.series())
            .and_modify(|seam| *seam = (*seam).max(tick.ts_event()))
            .or_insert(tick.ts_event());
        self.hand(tick)
    }

    /// Takes `tick`, a live one: held while a group is pending, else handed
    /// to the indicators unless the history holds it.
    ///
    /// # Errors
    ///
    /// Returns the first error of an indicator reading it.
    pub(super) fn live(&self, tick: Tick) -> anyhow::Result<()> {
        if let Some(held) = self.held.borrow_mut().as_mut() {
            held.push(tick);
            return Ok(());
        }
        self.pass(tick)
    }

    /// Hands the ticks held over, in the order they came, but those the
    /// history holds, the group having ended; from now on live ticks go
    /// straight through. Nothing held, nothing to do.
    ///
    /// # Errors
    ///
    /// Returns the first error of an indicator reading a tick, which ends
    /// the hand-over: the ticks after it are lost.
    pub(super) fn release(&self) -> anyhow::Result<()> {
        let Some(held) = self.held.take() else {
            return Ok(());
        };
        let count = held.len();
        let mut dropped = 0;
        for tick in held {
            if self.holds(&tick) {
                dropped += 1;
            } else {
                self.hand(tick)?;
            }
        }
        if count > 0 {
            log::info!(
                "Warmup handed over {} live ticks held, {dropped} of the history dropped",
                count - dropped
            );
        }
        Ok(())
    }

    /// Hands `tick` to the indicators unless the history holds it.
    fn pass(&self, tick: Tick) -> anyhow::Result<()> {
        if self.holds(&tick) {
            return Ok(());
        }
        self.hand(tick)
    }

    /// Whether the history holds `tick`: made at or before its series'
    /// seam.
    fn holds(&self, tick: &Tick) -> bool {
        self.seams
            .borrow()
            .get(&tick.series())
            .is_some_and(|seam| tick.ts_event() <= *seam)
    }

    fn hand(&self, tick: Tick) -> anyhow::Result<()> {
        match &tick {
            Tick::Quote(quote) => {
                for indicator in &self.indicators {
                    indicator.handle_quote(quote)?;
                }
            }
            Tick::Trade(trade) => {
                for indicator in &self.indicators {
                    indicator.handle_trade(trade)?;
                }
            }
            Tick::Depth(depth) => {
                for books in &self.books {
                    books.handle_depth(depth);
                }
            }
            Tick::Deltas(deltas) => {
                for books in &self.books {
                    books.handle_deltas(deltas);
                }
            }
        }
        Ok(())
    }
}

impl Tick {
    fn series(&self) -> Series {
        match self {
            Self::Quote(quote) => Series::Quotes(quote.instrument_id),
            Self::Trade(trade) => Series::Trades(trade.instrument_id),
            Self::Depth(depth) => Series::Depth(depth.instrument_id),
            Self::Deltas(deltas) => Series::Deltas(deltas.instrument_id),
        }
    }

    /// When the tick was made, a batch of deltas when its last delta was.
    fn ts_event(&self) -> UnixNanos {
        match self {
            Self::Quote(quote) => quote.ts_event,
            Self::Trade(trade) => trade.ts_event,
            Self::Depth(depth) => depth.ts_event,
            Self::Deltas(deltas) => deltas.ts_event,
        }
    }
}

impl Debug for Hold {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Hold))
            .field("indicators", &self.indicators.len())
            .field("books", &self.books.len())
            .field("held", &self.held.borrow().as_ref().map(Vec::len))
            .field("seams", &self.seams.borrow())
            .finish()
    }
}
