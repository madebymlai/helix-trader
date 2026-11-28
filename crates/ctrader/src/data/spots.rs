//! Merges cTrader's spot events into live quotes, by the rule the tick history uses
//! (`session::ticks::QuoteMerge`).
//!
//! A spot event carries a side only when that side changed, with the time of the change
//! (`subscribeToSpotTimestamp`). The first event after a subscription carries both sides. On the
//! demo, the spot events of one minute matched the ticks history served for it (`README.md`, "Live
//! quotes"). Each quote has the spot's time as `ts_event` and its receipt time as `ts_init`.

use std::collections::HashMap;

use anyhow::Context;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::QuoteTick,
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};

use crate::{
    common::messages::ProtoOaSpotEvent,
    session::ticks::{Merged, QuoteMerge},
};

#[derive(Debug)]
pub(super) struct SpotQuotes {
    streams: HashMap<i64, Stream>,
    symbol_ids: HashMap<InstrumentId, i64>,
}

#[derive(Debug)]
struct Stream {
    merge: QuoteMerge,
    subscribed: bool,
}

impl SpotQuotes {
    /// Returns the live quotes of `instruments`, each with its cTrader `symbolId`.
    ///
    /// No instrument is subscribed yet.
    ///
    /// # Errors
    ///
    /// Returns an error if an instrument's quote size is off its size precision.
    pub(super) fn new(instruments: &[(InstrumentAny, i64)]) -> anyhow::Result<Self> {
        let mut streams = HashMap::new();
        let mut symbol_ids = HashMap::new();

        for (instrument, symbol_id) in instruments {
            let stream = Stream {
                merge: QuoteMerge::new(instrument)?,
                subscribed: false,
            };
            streams.insert(*symbol_id, stream);
            symbol_ids.insert(instrument.id(), *symbol_id);
        }
        Ok(Self {
            streams,
            symbol_ids,
        })
    }

    /// Returns the cTrader `symbolId` of `id`.
    ///
    /// # Errors
    ///
    /// Returns an error if `id` is not one of the session's instruments.
    pub(super) fn symbol_id(&self, id: InstrumentId) -> anyhow::Result<i64> {
        self.symbol_ids
            .get(&id)
            .copied()
            .with_context(|| format!("{id} is not an instrument the data client loaded"))
    }

    /// Sends the live quotes of `id` from now on, and returns its `symbolId`.
    ///
    /// # Errors
    ///
    /// Returns an error if `symbol_id` fails.
    pub(super) fn subscribe(&mut self, id: InstrumentId) -> anyhow::Result<i64> {
        let symbol_id = self.symbol_id(id)?;
        self.stream(symbol_id).subscribed = true;
        Ok(symbol_id)
    }

    /// Stops sending live quotes of `id`, and returns its `symbolId`.
    ///
    /// # Errors
    ///
    /// Returns an error if `symbol_id` fails.
    pub(super) fn unsubscribe(&mut self, id: InstrumentId) -> anyhow::Result<i64> {
        let symbol_id = self.symbol_id(id)?;
        self.stream(symbol_id).subscribed = false;
        Ok(symbol_id)
    }

    /// Takes in `spot`, received at `ts_init`, and returns the quote to send now.
    ///
    /// Returns no quote if:
    /// - The instrument is not subscribed.
    /// - Both sides are not yet known.
    /// - The quote is crossed.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The spot is for a symbol the session did not load.
    /// - The spot has no time.
    /// - The spot's price is off the instrument's precision.
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
        Ok(Some(quote))
    }

    fn stream(&mut self, symbol_id: i64) -> &mut Stream {
        self.streams
            .get_mut(&symbol_id)
            .expect("symbol_ids and streams hold the same symbols")
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::types::Quantity;
    use prost::Message as _;
    use rstest::rstest;

    use super::*;
    use crate::testing::{
        self, SPOT_EVENT_EURUSD_ASK, SPOT_EVENT_EURUSD_BID, SPOT_EVENT_EURUSD_FIRST,
    };

    /// Returns EURUSD as the clients load it, with its cTrader symbol id in the fixtures.
    fn eurusd() -> (InstrumentAny, i64) {
        (InstrumentAny::Cfd(testing::instrument("EURUSD")), 1)
    }

    fn id() -> InstrumentId {
        InstrumentId::from("EURUSD.ACMEFX")
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

    #[rstest]
    fn the_first_spot_makes_a_quote_at_its_time_received_now_sized_as_history() {
        let mut quotes = subscribed();

        let made = quotes
            .spot(&spot(SPOT_EVENT_EURUSD_FIRST), RECEIVED)
            .unwrap()
            .unwrap();

        assert_eq!(
            prices(&made),
            quote(1_793_017_476_361, "1.11964", "1.11965")
        );
        assert_eq!(made.ts_init, RECEIVED);
        assert_eq!(made.instrument_id, id());
        assert_eq!(made.bid_size, Quantity::from(100_000));
        assert_eq!(made.ask_size, Quantity::from(100_000));
    }

    #[rstest]
    fn a_spot_of_one_side_carries_the_other() {
        let mut quotes = subscribed();
        quotes
            .spot(&spot(SPOT_EVENT_EURUSD_FIRST), RECEIVED)
            .unwrap();

        let made = quotes
            .spot(&spot(SPOT_EVENT_EURUSD_BID), RECEIVED)
            .unwrap()
            .unwrap();

        assert_eq!(
            prices(&made),
            quote(1_793_017_480_320, "1.11965", "1.11965")
        );
    }

    #[rstest]
    fn no_quote_before_both_sides_are_known() {
        let mut quotes = subscribed();

        let made = quotes.spot(&spot(SPOT_EVENT_EURUSD_BID), RECEIVED).unwrap();

        assert_eq!(made, None);
    }

    /// The captured first spot, then an ask captured later, below its bid.
    #[rstest]
    fn a_crossed_quote_is_dropped() {
        let mut quotes = subscribed();
        quotes
            .spot(&spot(SPOT_EVENT_EURUSD_FIRST), RECEIVED)
            .unwrap();

        let made = quotes.spot(&spot(SPOT_EVENT_EURUSD_ASK), RECEIVED).unwrap();

        assert_eq!(made, None);
    }

    #[rstest]
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

    #[rstest]
    fn a_spot_of_a_symbol_not_loaded_is_an_error() {
        let mut quotes = subscribed();
        let mut other = spot(SPOT_EVENT_EURUSD_FIRST);
        other.symbol_id = 7;

        let err = quotes.spot(&other, RECEIVED).unwrap_err();

        assert!(err.to_string().contains("symbol 7"), "{err}");
    }
}
