//! The hold of a live warmup's quotes, trades, book depth, deltas, custom data and bars.
//!
//! The warmup hands the indicators reading them, a model strategy's features, those of its
//! responses as its merge (`merge`) hands them on, in time order across series. It holds the live
//! ones the strategy hands it until its group ends, the last window answered, and then hands those
//! over in the order they came; from then on live ones go straight through. A replay requests no
//! warmup and holds nothing.
//!
//! It stands in for Nautilus. Nautilus hands a live tick to the indicators an actor registered for
//! it as the tick arrives (`DataActor::handle_quote` in `common/src/actor/data_actor.rs`), and a
//! response's ticks to the same indicators (`handle_quotes_response`), holding nothing while a
//! request is pending. It holds live ticks for its bar aggregators alone (`WarmupBuffer` in
//! `data/src/engine/handlers.rs`, which `data/src/engine/bar.rs` gives them). A warmup is answered
//! a window at a time, the node running between, so features registered for ticks read the live
//! ones before the older windows, and time runs backwards for them. So the features are not
//! registered for quotes and trades (`FeatureSet::register`), and the strategy hands the warmup its
//! live ones (`model::handle`'s `model_data_hooks!`).
//!
//! Nautilus hands book depth and deltas to no indicator (`ActorIndicator` reads quotes, trades and
//! bars): an actor gets them in its own handlers (`DataActor::on_book_depth`, `on_book_deltas`), as
//! it gets a response's in `on_historical_book_depth` and `on_historical_book_deltas`, and Nautilus
//! holds nothing for the handlers while a request is pending either. So the strategy hands the
//! warmup its live depth and deltas from its handlers too, held as ticks are, and likewise its
//! custom data, which Nautilus hands an actor's `on_data` and `on_historical_data` alone.
//!
//! Nautilus holds live data for a request's bar aggregator only until that request completes
//! (`cleanup_request_bar_aggregators` in `data/src/engine/bar.rs`), so the live bars it builds then
//! would reach indicators registered for them while the other requests still answer older windows.
//! The bar indicators are therefore registered with Nautilus for nothing either, and the strategy
//! hands the warmup its live and replayed bars (`on_bar`), held as ticks are.
//!
//! Once Nautilus holds live data for an actor's indicators and handlers while a request of it with
//! `update_subscriptions` is pending, releasing them in time order after the response and
//! deduplicated against it, this module goes: helix-trader issue #16
//! (<https://github.com/madebymlai/helix-trader/issues/16>), upstream nautilus_trader#5675. Until
//! the merge goes too (`merge`, helix-trader#46), which hands a request's history on after its
//! completion, the features stay fed through the warmup, live data then passed straight through.
//! After it, they are registered for quotes, trades and bars again, the strategy hands the warmup
//! no ticks and no bars, and its handlers hand its depth, deltas and custom data to the features
//! straight.
//!
//! A live tick the history holds is dropped, held or not, judged within its series: the quotes, the
//! trades, the depth or the deltas of one instrument, the custom data of one data type, or the bars
//! of one bar type, a batch of deltas by its last delta's time, a bar by its close. The series'
//! seam is the latest `ts_event` of its ticks the warmup handed over. A live tick of the series
//! made at or before it (`ts_event`) is dropped, one made after it handed over. Made at the seam
//! itself, it is the venue's resend of the history's last tick, which IC and IB send on a
//! subscription, or another tick of that instant, which the history, answered after it, is taken to
//! hold. Made before it, it is older than what the features read, and would run their time
//! backwards. `ts_init` is not compared: a client stamps a live tick's on receipt (cTrader,
//! `crates/ctrader/src/data/spots.rs`) and a history's as the venue or the catalog gives it, so a
//! resent tick's comes after the history's with its `ts_event` the same, and Nautilus trims a
//! response to the warmup's end by `ts_init`. A series with no history drops nothing; a new warmup
//! forgets the seams of the one before.

use std::{cell::RefCell, collections::HashMap, fmt::Debug, rc::Rc};

use nautilus_common::actor::indicators::SharedActorIndicator;
use nautilus_core::UnixNanos;

use super::{
    BookIndicator, CustomIndicator,
    tick::{Series, Tick},
};

/// What hands a warmup's quotes, trades, depth, deltas, custom data and bars to the indicators
/// reading them, holding the live ones while a live start's group is pending.
#[derive(Default)]
pub(super) struct Hold {
    /// What the quotes and trades are handed to.
    indicators: Vec<SharedActorIndicator>,
    /// What the bars are handed to.
    bars: Vec<SharedActorIndicator>,
    /// What the depth and deltas are handed to.
    books: Vec<Rc<dyn BookIndicator>>,
    /// What the custom data is handed to.
    custom: Vec<Rc<dyn CustomIndicator>>,
    /// The live ticks held while a group is pending, in the order they came; none when live ticks
    /// go straight through.
    held: RefCell<Option<Vec<Tick>>>,
    /// The seam of each series: the latest `ts_event` of its ticks the history handed over.
    seams: RefCell<HashMap<Series, UnixNanos>>,
}

impl Hold {
    /// Hands the quotes and trades to `indicator` too.
    pub(super) fn feeding(&mut self, indicator: SharedActorIndicator) {
        self.indicators.push(indicator);
    }

    /// Hands the bars to `indicator` too.
    pub(super) fn feeding_bars(&mut self, indicator: SharedActorIndicator) {
        self.bars.push(indicator);
    }

    /// Hands the depth and deltas to `books` too.
    pub(super) fn feeding_books(&mut self, books: Rc<dyn BookIndicator>) {
        self.books.push(books);
    }

    /// Hands the custom data to `custom` too.
    pub(super) fn feeding_custom(&mut self, custom: Rc<dyn CustomIndicator>) {
        self.custom.push(custom);
    }

    /// Holds the live ticks from now, a live start's group pending, with no seam yet.
    pub(super) fn hold(&self) {
        *self.held.borrow_mut() = Some(Vec::new());
        self.seams.borrow_mut().clear();
    }

    /// Hands `tick`, of a response, as the merge hands it on, to the indicators, its series' seam
    /// moved to it if it is the latest.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator reading it fails: the first such error.
    pub(super) fn history(&self, tick: &Tick) -> anyhow::Result<()> {
        self.seams
            .borrow_mut()
            .entry(tick.series())
            .and_modify(|seam| *seam = (*seam).max(tick.ts_event()))
            .or_insert(tick.ts_event());
        self.hand(tick)
    }

    /// Takes `tick`, a live one: held while a group is pending, else handed to the indicators
    /// unless the history holds it.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator reading it fails: the first such error.
    pub(super) fn live(&self, tick: Tick) -> anyhow::Result<()> {
        if let Some(held) = self.held.borrow_mut().as_mut() {
            held.push(tick);
            return Ok(());
        }
        self.pass(&tick)
    }

    /// Takes `tick`, a live one Nautilus built from the live data it held for a request and hands
    /// no handler, a bar it built into the cache alone at the request's completion. Held among the
    /// live ticks held in time order, before the first initialized after it (`ts_init`), the ticks
    /// it was built from before it, as a replay hands a bar after the data of its instant; with
    /// none held, handed as `live` hands one.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator reading it fails, as `live`.
    pub(super) fn built_live(&self, tick: Tick) -> anyhow::Result<()> {
        if let Some(held) = self.held.borrow_mut().as_mut() {
            let at = held
                .iter()
                .position(|other| other.ts_init() > tick.ts_init())
                .unwrap_or(held.len());
            held.insert(at, tick);
            return Ok(());
        }
        self.pass(&tick)
    }

    /// Hands the ticks held over, in the order they came, but those the history holds, the group
    /// having ended; from now on live ticks go straight through. Nothing held, nothing to do.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator reading a tick fails, the first of which ends the
    /// hand-over: the ticks after it are lost.
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
                self.hand(&tick)?;
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
    fn pass(&self, tick: &Tick) -> anyhow::Result<()> {
        if self.holds(tick) {
            return Ok(());
        }
        self.hand(tick)
    }

    /// Whether the history holds `tick`: made at or before its series' seam.
    fn holds(&self, tick: &Tick) -> bool {
        self.seams
            .borrow()
            .get(&tick.series())
            .is_some_and(|seam| tick.ts_event() <= *seam)
    }

    fn hand(&self, tick: &Tick) -> anyhow::Result<()> {
        match tick {
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
            Tick::Custom(data) => {
                for custom in &self.custom {
                    custom.handle_custom(data);
                }
            }
            Tick::Bar(bar) => {
                for indicator in &self.bars {
                    indicator.handle_bar(bar)?;
                }
            }
        }
        Ok(())
    }
}

impl Debug for Hold {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(Hold))
            .field("indicators", &self.indicators.len())
            .field("bars", &self.bars.len())
            .field("books", &self.books.len())
            .field("custom", &self.custom.len())
            .field("held", &self.held.borrow().as_ref().map(Vec::len))
            .field("seams", &self.seams.borrow())
            .finish()
    }
}
