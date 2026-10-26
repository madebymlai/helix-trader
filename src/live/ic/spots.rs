//! Live quotes: cTrader's spot events merged into quotes by the rule of the
//! tick history (`ticks::QuoteMerge` in `data/ic`).
//!
//! A spot event carries a side only when it changed, and the time of the
//! change (`subscribeToSpotTimestamp`); the first one after a subscription
//! carries both. On the demo the spot events of a minute were the ticks
//! history serves for it, side for side and millisecond for millisecond
//! (`ic/README.md`, "Live quotes: spot events"), so live quotes are the
//! quotes a fill stores. Each quote is stamped `ts_event` = the spot's time
//! and `ts_init` = its receipt.
//!
//! While a quote request of an instrument is answered, its live quotes are
//! held, then sent after the response, those the response already holds
//! dropped: the node gets its warmup before any live quote (see
//! `ic/README.md`, "Warmup: quote requests").

use std::collections::HashMap;

use anyhow::Context;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::QuoteTick,
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};

use crate::data::ic::{
    messages::ProtoOaSpotEvent,
    ticks::{Merged, QuoteMerge},
};

/// The live quotes of the instruments of a session, by cTrader symbol.
#[derive(Debug)]
pub(super) struct SpotQuotes {
    streams: HashMap<i64, Stream>,
    symbol_ids: HashMap<InstrumentId, i64>,
}

/// One instrument's live quotes.
#[derive(Debug)]
struct Stream {
    merge: QuoteMerge,
    subscribed: bool,
    /// Its quote requests being answered.
    requests: usize,
    /// The live quotes made while a request was answered.
    held: Vec<QuoteTick>,
}

impl SpotQuotes {
    /// The live quotes of `instruments`, each with its cTrader `symbolId`;
    /// none subscribed.
    ///
    /// # Errors
    ///
    /// Returns an error if an instrument's quote size is off its size
    /// precision.
    pub(super) fn new(instruments: &[(InstrumentAny, i64)]) -> anyhow::Result<Self> {
        let mut streams = HashMap::new();
        let mut symbol_ids = HashMap::new();
        for (instrument, symbol_id) in instruments {
            let stream = Stream {
                merge: QuoteMerge::new(instrument)?,
                subscribed: false,
                requests: 0,
                held: Vec::new(),
            };
            streams.insert(*symbol_id, stream);
            symbol_ids.insert(instrument.id(), *symbol_id);
        }
        Ok(Self {
            streams,
            symbol_ids,
        })
    }

    /// The cTrader `symbolId` of `id`.
    ///
    /// # Errors
    ///
    /// Returns an error if `id` is not one of the session's instruments.
    pub(super) fn symbol_id(&self, id: InstrumentId) -> anyhow::Result<i64> {
        self.symbol_ids
            .get(&id)
            .copied()
            .with_context(|| format!("{id} is not an instrument the IC data client loaded"))
    }

    /// Sends the live quotes of `id` from now on; returns its `symbolId`.
    ///
    /// # Errors
    ///
    /// As `symbol_id`.
    pub(super) fn subscribe(&mut self, id: InstrumentId) -> anyhow::Result<i64> {
        let symbol_id = self.symbol_id(id)?;
        self.stream(symbol_id).subscribed = true;
        Ok(symbol_id)
    }

    /// Sends no more live quotes of `id`; returns its `symbolId`.
    ///
    /// # Errors
    ///
    /// As `symbol_id`.
    pub(super) fn unsubscribe(&mut self, id: InstrumentId) -> anyhow::Result<i64> {
        let symbol_id = self.symbol_id(id)?;
        self.stream(symbol_id).subscribed = false;
        Ok(symbol_id)
    }

    /// Takes in `spot`, received at `ts_init`, and returns the quote to
    /// send now: none for an instrument not subscribed, while a request
    /// holds its quotes, before both sides are known, or for a crossed
    /// quote.
    ///
    /// # Errors
    ///
    /// Returns an error if the spot is of a symbol the session did not
    /// load, has no time, or a price off the instrument's precision.
    pub(super) fn spot(
        &mut self,
        spot: &ProtoOaSpotEvent,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Option<QuoteTick>> {
        let stream = self
            .streams
            .get_mut(&spot.symbol_id)
            .with_context(|| format!("a spot event of symbol {}, not loaded", spot.symbol_id))?;
        let ms = spot.timestamp.with_context(|| {
            format!("a spot event of symbol {} without its time", spot.symbol_id)
        })?;
        let side = |price: Option<u64>| price.map(i64::try_from).transpose();
        let (bid, ask) = (side(spot.bid)?, side(spot.ask)?);
        let Merged::Quote(quote) = stream.merge.next(ms, bid, ask, Some(ts_init))? else {
            return Ok(None);
        };
        if !stream.subscribed {
            return Ok(None);
        }
        if stream.requests > 0 {
            stream.held.push(quote);
            return Ok(None);
        }
        Ok(Some(quote))
    }

    /// Holds the live quotes of `id` while a quote request of it is
    /// answered.
    ///
    /// # Errors
    ///
    /// As `symbol_id`.
    pub(super) fn hold(&mut self, id: InstrumentId) -> anyhow::Result<()> {
        let symbol_id = self.symbol_id(id)?;
        self.stream(symbol_id).requests += 1;
        Ok(())
    }

    /// Ends a hold of `id` once its request is answered, the last quote of
    /// the response stamped `answered` (`None` for none, or a failed
    /// request), and returns the quotes held, if no other request holds
    /// them, without those stamped at or before `answered`.
    pub(super) fn release(
        &mut self,
        id: InstrumentId,
        answered: Option<UnixNanos>,
    ) -> Vec<QuoteTick> {
        let Ok(symbol_id) = self.symbol_id(id) else {
            return Vec::new();
        };
        let stream = self.stream(symbol_id);
        stream.requests = stream.requests.saturating_sub(1);
        if stream.requests > 0 {
            return Vec::new();
        }
        let mut held = std::mem::take(&mut stream.held);
        held.retain(|q| answered.is_none_or(|last| q.ts_event > last));
        held
    }

    fn stream(&mut self, symbol_id: i64) -> &mut Stream {
        self.streams
            .get_mut(&symbol_id)
            .expect("symbol_ids and streams hold the same symbols")
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        instruments::{CurrencyPair, stubs::audusd_sim},
        types::Quantity,
    };
    use prost::Message as _;

    use super::*;
    use crate::data::ic::testing::{
        SPOT_EVENT_EURUSD_ASK, SPOT_EVENT_EURUSD_BID, SPOT_EVENT_EURUSD_FIRST,
    };

    /// EUR/USD-like, 5 decimals, maximum order 1,000,000; cTrader's
    /// EURUSD symbol on the server the fixtures come from.
    fn eurusd() -> (InstrumentAny, i64) {
        let mut pair: CurrencyPair = audusd_sim();
        pair.id = InstrumentId::from("EURUSD.ICMARKETS");
        (InstrumentAny::CurrencyPair(pair), 1)
    }

    fn id() -> InstrumentId {
        InstrumentId::from("EURUSD.ICMARKETS")
    }

    fn spot(bytes: &[u8]) -> ProtoOaSpotEvent {
        ProtoOaSpotEvent::decode(bytes).unwrap()
    }

    fn subscribed() -> SpotQuotes {
        let mut quotes = SpotQuotes::new(&[eurusd()]).unwrap();
        quotes.subscribe(id()).unwrap();
        quotes
    }

    const RECEIVED: UnixNanos = UnixNanos::new(1_793_017_476_613_000_000);

    /// (ms of ts_event, bid, ask) of `quote`.
    fn prices(quote: &QuoteTick) -> (u64, String, String) {
        (
            quote.ts_event.as_u64() / 1_000_000,
            quote.bid_price.to_string(),
            quote.ask_price.to_string(),
        )
    }

    fn quote(ms: u64, bid: &str, ask: &str) -> (u64, String, String) {
        (ms, bid.to_owned(), ask.to_owned())
    }

    #[test]
    fn the_first_spot_makes_a_quote_at_its_time_received_now_sized_as_history() {
        let mut quotes = subscribed();

        let made = quotes
            .spot(&spot(SPOT_EVENT_EURUSD_FIRST), RECEIVED)
            .unwrap()
            .unwrap();

        // As captured: bid 111964, ask 111965 at 1793017476361 ms.
        assert_eq!(
            prices(&made),
            quote(1_793_017_476_361, "1.11964", "1.11965")
        );
        assert_eq!(made.ts_init, RECEIVED);
        assert_eq!(made.instrument_id, id());
        assert_eq!(made.bid_size, Quantity::from(100_000));
        assert_eq!(made.ask_size, Quantity::from(100_000));
    }

    #[test]
    fn a_spot_of_one_side_carries_the_other() {
        let mut quotes = subscribed();
        quotes
            .spot(&spot(SPOT_EVENT_EURUSD_FIRST), RECEIVED)
            .unwrap();

        let made = quotes
            .spot(&spot(SPOT_EVENT_EURUSD_BID), RECEIVED)
            .unwrap()
            .unwrap();

        // The bid 111965 alone at 1793017480320 ms; the ask carried.
        assert_eq!(
            prices(&made),
            quote(1_793_017_480_320, "1.11965", "1.11965")
        );
    }

    #[test]
    fn no_quote_before_both_sides_are_known() {
        let mut quotes = subscribed();

        let made = quotes.spot(&spot(SPOT_EVENT_EURUSD_BID), RECEIVED).unwrap();

        assert_eq!(made, None);
    }

    /// The captured first spot, then an ask captured later, below its bid.
    #[test]
    fn a_crossed_quote_is_dropped() {
        let mut quotes = subscribed();
        quotes
            .spot(&spot(SPOT_EVENT_EURUSD_FIRST), RECEIVED)
            .unwrap();

        let made = quotes.spot(&spot(SPOT_EVENT_EURUSD_ASK), RECEIVED).unwrap();

        assert_eq!(made, None);
    }

    #[test]
    fn an_instrument_not_subscribed_gets_no_quote_but_its_sides_are_kept() {
        let mut quotes = SpotQuotes::new(&[eurusd()]).unwrap();
        quotes
            .spot(&spot(SPOT_EVENT_EURUSD_FIRST), RECEIVED)
            .unwrap();
        quotes.subscribe(id()).unwrap();
        quotes.unsubscribe(id()).unwrap();

        let unsubscribed = quotes.spot(&spot(SPOT_EVENT_EURUSD_BID), RECEIVED).unwrap();
        quotes.subscribe(id()).unwrap();
        let again = quotes
            .spot(&spot(SPOT_EVENT_EURUSD_BID), RECEIVED)
            .unwrap()
            .unwrap();

        assert_eq!(unsubscribed, None);
        assert_eq!(
            prices(&again),
            quote(1_793_017_480_320, "1.11965", "1.11965")
        );
    }

    #[test]
    fn quotes_held_by_a_request_come_after_it_without_those_it_answered() {
        let mut quotes = subscribed();
        quotes.hold(id()).unwrap();

        let first = quotes
            .spot(&spot(SPOT_EVENT_EURUSD_FIRST), RECEIVED)
            .unwrap();
        let second = quotes.spot(&spot(SPOT_EVENT_EURUSD_BID), RECEIVED).unwrap();
        // The response's last quote is the first spot's.
        let answered = UnixNanos::new(1_793_017_476_361_000_000);
        let released = quotes.release(id(), Some(answered));

        assert_eq!((first, second), (None, None));
        let released: Vec<_> = released.iter().map(prices).collect();
        assert_eq!(released, [quote(1_793_017_480_320, "1.11965", "1.11965")]);
        let after = quotes.spot(&spot(SPOT_EVENT_EURUSD_BID), RECEIVED).unwrap();
        assert!(after.is_some());
    }

    #[test]
    fn quotes_stay_held_until_the_last_request_is_answered() {
        let mut quotes = subscribed();
        quotes.hold(id()).unwrap();
        quotes.hold(id()).unwrap();
        quotes
            .spot(&spot(SPOT_EVENT_EURUSD_FIRST), RECEIVED)
            .unwrap();

        let after_one = quotes.release(id(), None);
        let after_both = quotes.release(id(), None);

        assert_eq!(after_one, []);
        assert_eq!(after_both.len(), 1);
    }

    /// Synthetic: a spot of a symbol the session did not load.
    #[test]
    fn a_spot_of_a_symbol_not_loaded_is_an_error() {
        let mut quotes = subscribed();
        let mut other = spot(SPOT_EVENT_EURUSD_FIRST);
        other.symbol_id = 7;

        let err = quotes.spot(&other, RECEIVED).unwrap_err();

        assert!(err.to_string().contains("symbol 7"), "{err}");
    }

    #[test]
    fn an_instrument_not_loaded_is_refused() {
        let mut quotes = subscribed();

        let err = quotes
            .subscribe(InstrumentId::from("GBPUSD.ICMARKETS"))
            .unwrap_err();

        assert!(err.to_string().contains("GBPUSD.ICMARKETS"), "{err}");
    }
}
