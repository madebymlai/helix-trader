//! The symbol boundary: an `LSEDGE` instrument id and the LSE market and
//! symbol it stands for, converted both ways here only.
//!
//! A derived market's series is one instrument under `LSEDGE`, its LSE
//! market the suffix of its symbol, as Bybit's product suffix (Nautilus'
//! adapter guide, "Symbols and instrument identity"): LSE lists one symbol
//! in several markets (GOLD in five). LSE's market names use underscores
//! and no hyphens, so the last hyphen splits symbol from market:
//! `GOLD-FUTURES.LSEDGE` is GOLD in `futures`. The market is written in
//! capitals in the id, LSE's folder name in lower case, and the symbol as
//! LSE lists it. An exchange market's listings take Databento's ids, so
//! none is an `LSEDGE` instrument.

use std::fmt;

use anyhow::{bail, ensure};
use nautilus_model::identifiers::InstrumentId;

use super::consts::{EXCHANGE_MARKETS, LSEDGE_VENUE};

/// A series of an LSE derived market: its market, LSE's folder name, and
/// its symbol, as `/v3/pairs` lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LseSymbol {
    market: String,
    symbol: String,
}

impl LseSymbol {
    /// `symbol` in LSE's derived market `market`.
    ///
    /// # Errors
    ///
    /// Returns an error if the symbol is empty, or if the market is not a
    /// derived market's name: empty, holding anything but lower-case
    /// letters, digits and underscores, or an exchange market's.
    pub fn new(market: &str, symbol: &str) -> anyhow::Result<Self> {
        ensure!(!symbol.is_empty(), "an LSE symbol is not empty");
        ensure!(
            !market.is_empty()
                && market
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
            "{market:?} is not an LSE market's name: lower-case letters, digits and underscores"
        );
        ensure!(
            !EXCHANGE_MARKETS.contains(&market),
            "{market} is an LSE exchange market: its listings are not LSEDGE series"
        );
        Ok(Self {
            market: market.to_owned(),
            symbol: symbol.to_owned(),
        })
    }

    /// The series `id` stands for.
    ///
    /// # Errors
    ///
    /// Returns an error naming `id` if it is not on `LSEDGE`, or if its
    /// symbol is not an LSE symbol, a hyphen and its market in capitals.
    pub fn from_instrument_id(id: InstrumentId) -> anyhow::Result<Self> {
        if id.venue != *LSEDGE_VENUE {
            bail!("{id} is not an LSEDGE instrument");
        }
        let Some((symbol, market)) = id.symbol.as_str().rsplit_once('-') else {
            bail!("{id} names no LSE market: its symbol is an LSE symbol, a hyphen and its market");
        };
        ensure!(
            market.chars().all(|c| !c.is_ascii_lowercase()),
            "{id} names its LSE market in lower case: write it in capitals"
        );
        Self::new(&market.to_ascii_lowercase(), symbol)
            .map_err(|e| anyhow::anyhow!("{id} is not an LSEDGE series: {e}"))
    }

    /// The `LSEDGE` instrument the series is: its symbol, a hyphen and its
    /// market in capitals.
    #[must_use]
    pub fn instrument_id(&self) -> InstrumentId {
        InstrumentId::from(
            format!(
                "{}-{}.{}",
                self.symbol,
                self.market.to_ascii_uppercase(),
                *LSEDGE_VENUE
            )
            .as_str(),
        )
    }

    /// LSE's name of the market, its folder.
    #[must_use]
    pub fn market(&self) -> &str {
        &self.market
    }

    /// The symbol, as LSE lists it.
    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }
}

impl fmt::Display for LseSymbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} in LSE's {}", self.symbol, self.market)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn of(id: &str) -> anyhow::Result<LseSymbol> {
        LseSymbol::from_instrument_id(InstrumentId::from(id))
    }

    #[test]
    fn an_lsedge_id_is_its_symbol_in_the_market_after_its_last_hyphen() {
        for (id, market, symbol) in [
            ("GOLD-FUTURES.LSEDGE", "futures", "GOLD"),
            ("US10Y-FUTURES.LSEDGE", "futures", "US10Y"),
            ("BRK-B-US_STOCKS.LSEDGE", "us_stocks", "BRK-B"),
            ("EUR_USD-FOREX.LSEDGE", "forex", "EUR_USD"),
            ("XAU/USD-COMMODITIES.LSEDGE", "commodities", "XAU/USD"),
        ] {
            let series = of(id).unwrap();

            assert_eq!((series.market(), series.symbol()), (market, symbol), "{id}");
        }
    }

    #[test]
    fn a_series_round_trips_through_its_instrument_id() {
        for (market, symbol, id) in [
            ("futures", "GOLD", "GOLD-FUTURES.LSEDGE"),
            ("us_stocks", "BRK-B", "BRK-B-US_STOCKS.LSEDGE"),
            ("ca_stocks", "RY", "RY-CA_STOCKS.LSEDGE"),
        ] {
            let series = LseSymbol::new(market, symbol).unwrap();

            assert_eq!(series.instrument_id(), InstrumentId::from(id));
            assert_eq!(of(id).unwrap(), series, "{id}");
        }
    }

    /// No hyphen, an empty symbol or market, or a market in lower case,
    /// which would make a second id of one series.
    #[test]
    fn an_id_without_its_market_in_capitals_is_refused_naming_it() {
        for id in [
            "GOLD.LSEDGE",
            "-FUTURES.LSEDGE",
            "GOLD-.LSEDGE",
            "GOLD-futures.LSEDGE",
            "GOLD-FU+TURES.LSEDGE",
        ] {
            let err = of(id).unwrap_err().to_string();

            assert!(err.contains(id), "{id}: {err}");
        }
    }

    #[test]
    fn an_id_of_another_venue_is_refused_naming_it() {
        let err = of("GOLD-FUTURES.GLBX").unwrap_err().to_string();

        assert!(err.contains("GOLD-FUTURES.GLBX"), "{err}");
    }

    /// Their listings take Databento's ids.
    #[test]
    fn an_exchange_market_is_no_lsedge_series() {
        for id in [
            "ESZ6-CME.LSEDGE",
            "NVDA-NASDAQ.LSEDGE",
            "AAPL261120C00100000-US_OPTIONS.LSEDGE",
            "AAPL-OPRA.LSEDGE",
        ] {
            let err = of(id).unwrap_err().to_string();

            assert!(err.contains("exchange market"), "{id}: {err}");
        }
    }
}
