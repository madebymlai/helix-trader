//! The hold of a live warmup's quotes and trades: the warmup hands the
//! indicators reading ticks, a model strategy's features, the quotes and
//! trades of its responses as they come, holds the live ones the strategy
//! hands it until its group ends, the last window answered, and then hands
//! those over in the order they came; from then on live ticks go straight
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
//! `model_data_hooks!`). Once Nautilus holds live ticks for an actor's
//! indicators while a request of it with `update_subscriptions` is pending,
//! releasing them in time order after the response and deduplicated against
//! it, this module goes, the features are registered for quotes and trades
//! again, and the strategy hands the warmup no ticks: helix-trader issue #16
//! (<https://github.com/madebymlai/helix-trader/issues/16>).
//!
//! A live tick the history holds is dropped, held or not, judged within its
//! series, the quotes or the trades of one instrument. The series' seam is
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
};

use nautilus_common::actor::indicators::SharedActorIndicator;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{QuoteTick, TradeTick},
    identifiers::InstrumentId,
};

/// A quote or a trade.
#[derive(Debug, Clone, Copy)]
pub(super) enum Tick {
    Quote(QuoteTick),
    Trade(TradeTick),
}

/// The series of a tick, within which a live tick is judged against the
/// history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Series {
    Quotes(InstrumentId),
    Trades(InstrumentId),
}

/// What hands a warmup's quotes and trades to the indicators reading them,
/// holding the live ones while a live start's group is pending.
#[derive(Default)]
pub(super) struct Hold {
    /// What the ticks are handed to.
    indicators: Vec<SharedActorIndicator>,
    /// The live ticks held while a group is pending, in the order they
    /// came; none when live ticks go straight through.
    held: RefCell<Option<Vec<Tick>>>,
    /// The seam of each series: the latest `ts_event` of its ticks the
    /// history handed over.
    seams: RefCell<HashMap<Series, UnixNanos>>,
}

impl Hold {
    /// Hands the ticks to `indicator` too.
    pub(super) fn feeding(&mut self, indicator: SharedActorIndicator) {
        self.indicators.push(indicator);
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
        for indicator in &self.indicators {
            match &tick {
                Tick::Quote(quote) => indicator.handle_quote(quote)?,
                Tick::Trade(trade) => indicator.handle_trade(trade)?,
            }
        }
        Ok(())
    }
}

impl Tick {
    const fn series(&self) -> Series {
        match self {
            Self::Quote(quote) => Series::Quotes(quote.instrument_id),
            Self::Trade(trade) => Series::Trades(trade.instrument_id),
        }
    }

    const fn ts_event(&self) -> UnixNanos {
        match self {
            Self::Quote(quote) => quote.ts_event,
            Self::Trade(trade) => trade.ts_event,
        }
    }
}

impl Debug for Hold {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(stringify!(Hold))
            .field("indicators", &self.indicators.len())
            .field("held", &self.held.borrow().as_ref().map(Vec::len))
            .field("seams", &self.seams.borrow())
            .finish()
    }
}
