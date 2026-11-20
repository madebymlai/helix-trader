//! The hold of a live warmup's quotes, trades, book depth, deltas, custom data and bars.
//!
//! The warmup hands the features, as the indicators reading them, a response's ticks as `merge`
//! hands them on, in time order across series. It holds live ticks until its group ends, then
//! hands them over in the order they came; after that live ones go straight through. A replay
//! requests no warmup and holds nothing.
//!
//! It stands in for Nautilus, which hands a live tick to an actor's indicators as it arrives
//! (`DataActor::handle_quote` in `common/src/actor/data_actor.rs`) and holds nothing while a
//! request is pending, except for bar aggregators (`WarmupBuffer` in
//! `data/src/engine/handlers.rs`). A warmup is answered a window at a time, so features
//! registered for ticks would read the live ones before the older windows, and time would run
//! backwards. So the features are not registered for quotes, trades and bars
//! (`FeatureSet::register`), and the strategy hands the warmup its live ticks
//! (`model::handle`'s `model_data_hooks!`) and bars (`on_bar`).
//!
//! Nautilus hands book depth and deltas to no indicator, and custom data to `on_data` alone, so
//! the strategy hands the warmup these from its handlers too.
//!
//! Nautilus holds live data for a request's bar aggregator only until that request completes
//! (`cleanup_request_bar_aggregators` in `data/src/engine/bar.rs`), so the bars it builds then
//! would reach indicators while other requests still answer older windows.
//!
//! Once Nautilus holds live data for an actor's indicators and handlers while a request with
//! `update_subscriptions` is pending, releasing it in time order after the response and
//! deduplicated against it, this module goes: helix-trader issue #16
//! (<https://github.com/madebymlai/helix-trader/issues/16>), upstream nautilus_trader#5675. Until
//! the merge goes too (`merge`, helix-trader#46), the features stay fed through the warmup.
//!
//! A live tick the history holds is dropped, held or not, judged within its series: the quotes,
//! trades, depth or deltas of one instrument, the custom data of one data type, or the bars of
//! one bar type, a batch of deltas by its last delta's time, a bar by its close. A series' seam
//! is the latest `ts_event` the warmup handed over for it. A live tick made at or before the seam
//! is dropped: at it, it is the venue's resend of the history's last tick (IC and IB send one on
//! a subscription) or another tick of that instant; before it, it would run the features' time
//! backwards. `ts_init` is not compared: a client stamps a live tick's on receipt (cTrader,
//! `crates/ctrader/src/data/spots.rs`), so a resent tick's comes after the history's, and
//! Nautilus trims a response to the warmup's end by `ts_init`. A series with no history drops
//! nothing; a new warmup forgets the seams of the one before.

use std::{cell::RefCell, collections::HashMap, fmt::Debug, rc::Rc};

use nautilus_common::actor::indicators::SharedActorIndicator;
use nautilus_core::UnixNanos;

use super::{
    BookIndicator, CustomIndicator,
    tick::{Series, Tick},
};

#[derive(Default)]
pub(super) struct Hold {
    indicators: Vec<SharedActorIndicator>,
    bars: Vec<SharedActorIndicator>,
    books: Vec<Rc<dyn BookIndicator>>,
    custom: Vec<Rc<dyn CustomIndicator>>,
    /// The live ticks held while a group is pending, `None` when they go straight through.
    held: RefCell<Option<Vec<Tick>>>,
    /// Per series, the latest `ts_event` the history handed over.
    seams: RefCell<HashMap<Series, UnixNanos>>,
}

impl Hold {
    pub(super) fn feeding(&mut self, indicator: SharedActorIndicator) {
        self.indicators.push(indicator);
    }

    pub(super) fn feeding_bars(&mut self, indicator: SharedActorIndicator) {
        self.bars.push(indicator);
    }

    pub(super) fn feeding_books(&mut self, books: Rc<dyn BookIndicator>) {
        self.books.push(books);
    }

    pub(super) fn feeding_custom(&mut self, custom: Rc<dyn CustomIndicator>) {
        self.custom.push(custom);
    }

    pub(super) fn hold(&self) {
        *self.held.borrow_mut() = Some(Vec::new());
        self.seams.borrow_mut().clear();
    }

    /// Hands `tick`, of a response, to the indicators, moving its series' seam to it if latest.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator fails.
    pub(super) fn history(&self, tick: &Tick) -> anyhow::Result<()> {
        self.seams
            .borrow_mut()
            .entry(tick.series())
            .and_modify(|seam| *seam = (*seam).max(tick.ts_event()))
            .or_insert(tick.ts_event());
        self.hand(tick)
    }

    /// Holds `tick`, a live one, while a group is pending, else hands it on unless the history
    /// holds it.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator fails.
    pub(super) fn live(&self, tick: Tick) -> anyhow::Result<()> {
        if let Some(held) = self.held.borrow_mut().as_mut() {
            held.push(tick);
            return Ok(());
        }
        self.pass(&tick)
    }

    /// As `live`, for a bar Nautilus built into the cache alone at a request's completion. Held
    /// before the first held tick initialized after it (`ts_init`), as a replay hands a bar after
    /// the data of its instant.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator fails.
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

    /// Hands the held ticks over in the order they came, but those the history holds; live ticks
    /// then go straight through.
    ///
    /// # Errors
    ///
    /// Returns an error if an indicator fails, which ends the hand-over: the ticks after it are
    /// lost.
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

    fn pass(&self, tick: &Tick) -> anyhow::Result<()> {
        if self.holds(tick) {
            return Ok(());
        }
        self.hand(tick)
    }

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
