//! The symbol boundary: an `LSEDGE` instrument id and the LSE market and
//! symbol it stands for, converted both ways here only, and the socket the
//! series streams on with its name there.
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

use super::consts::{EXCHANGE_MARKETS, LSEDGE_VENUE, LseSocket};

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

    /// The socket the series streams on, and its name there, as LSE's
    /// socket documentation spells it ("Symbols on each socket"): on
    /// data-ws, a US stock, a commodity and a forex pair under their
    /// symbol, the pair's underscore a slash (`EUR/USD`), a futures series
    /// suffixed `.F` (`GOLD.F`) and an index `.IDX` (`SPX.IDX`); on
    /// london-ws, a UK stock suffixed `.L` and a crypto pair in dollars
    /// (`BTCUSDT` is `BTC/USD`); on tsx-ws, a Canadian stock suffixed
    /// `.TO`. A UK or Canadian stock's dots are hyphens there, a trailing
    /// one dropped (`BT.A` is `BT-A.L`, `BP.` is `BP.L`).
    ///
    /// # Errors
    ///
    /// Returns an error naming the market if its series stream on no
    /// socket, or naming a crypto symbol not quoted in Tether, as LSE's
    /// crypto symbols are.
    pub(crate) fn stream(&self) -> anyhow::Result<(LseSocket, String)> {
        let symbol = &self.symbol;
        Ok(match self.market.as_str() {
            "us_stocks" | "commodities" => (LseSocket::Data, symbol.clone()),
            "forex" => (LseSocket::Data, symbol.replace('_', "/")),
            "futures" => (LseSocket::Data, format!("{symbol}.F")),
            "indices" => (LseSocket::Data, format!("{symbol}.IDX")),
            "crypto" => {
                let Some(base) = symbol.strip_suffix("USDT") else {
                    bail!(
                        "{symbol} in LSE's crypto is not quoted in Tether, as LSE's crypto \
                         symbols are (BTCUSDT): london-ws names no pair of it"
                    );
                };
                (LseSocket::London, format!("{base}/USD"))
            }
            "uk_stocks" => (LseSocket::London, format!("{}.L", listing(symbol))),
            "ca_stocks" => (LseSocket::Tsx, format!("{}.TO", listing(symbol))),
            market => bail!("the series of LSE's {market} stream on no LSE socket"),
        })
    }
}

impl fmt::Display for LseSymbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} in LSE's {}", self.symbol, self.market)
    }
}

/// A UK or Canadian stock's symbol as its socket spells it before its
/// exchange's suffix: its dots hyphens, a trailing one dropped.
fn listing(symbol: &str) -> String {
    symbol.strip_suffix('.').unwrap_or(symbol).replace('.', "-")
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

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

    /// As LSE's socket documentation spells them, and as data-ws lists
    /// them once authenticated (`test_data/ws_authenticated_futures.json`).
    #[rstest]
    #[case("GOLD-FUTURES.LSEDGE", "GOLD.F")]
    #[case("US10Y-FUTURES.LSEDGE", "US10Y.F")]
    fn a_futures_series_streams_on_data_ws_under_its_symbol_suffixed_f(
        #[case] id: &str,
        #[case] name: &str,
    ) {
        let (socket, streamed) = of(id).unwrap().stream().unwrap();

        assert_eq!((socket.name(), streamed.as_str()), ("data-ws", name));
    }

    /// As LSE's socket documentation spells them ("Symbols on each
    /// socket"), and as each socket acknowledged and ticked them on 13
    /// November 2026: a UK or Canadian stock's dots as hyphens, a UK
    /// stock's trailing dot dropped, crypto quoted in dollars where LSE's
    /// REST API quotes it in Tether.
    #[rstest]
    #[case("AAPL-US_STOCKS.LSEDGE", "data-ws", "AAPL")]
    #[case("BRK.B-US_STOCKS.LSEDGE", "data-ws", "BRK.B")]
    #[case("EUR_USD-FOREX.LSEDGE", "data-ws", "EUR/USD")]
    #[case("XAU/USD-COMMODITIES.LSEDGE", "data-ws", "XAU/USD")]
    #[case("SPX-INDICES.LSEDGE", "data-ws", "SPX.IDX")]
    #[case("BTCUSDT-CRYPTO.LSEDGE", "london-ws", "BTC/USD")]
    #[case("VOD-UK_STOCKS.LSEDGE", "london-ws", "VOD.L")]
    #[case("BT.A-UK_STOCKS.LSEDGE", "london-ws", "BT-A.L")]
    #[case("BP.-UK_STOCKS.LSEDGE", "london-ws", "BP.L")]
    #[case("RY-CA_STOCKS.LSEDGE", "tsx-ws", "RY.TO")]
    #[case("BBD.B-CA_STOCKS.LSEDGE", "tsx-ws", "BBD-B.TO")]
    #[case("AP.UN-CA_STOCKS.LSEDGE", "tsx-ws", "AP-UN.TO")]
    fn a_series_of_each_derived_market_streams_on_its_socket_under_that_socket_s_name(
        #[case] id: &str,
        #[case] socket: &str,
        #[case] name: &str,
    ) {
        let (streamed_on, streamed) = of(id).unwrap().stream().unwrap();

        assert_eq!((streamed_on.name(), streamed.as_str()), (socket, name));
    }

    /// LSE's crypto is quoted in Tether over REST, and london-ws spells
    /// a pair in dollars: another quote has no name there.
    #[rstest]
    fn a_crypto_series_not_quoted_in_tether_is_refused_naming_it() {
        let err = of("BTCEUR-CRYPTO.LSEDGE")
            .unwrap()
            .stream()
            .unwrap_err()
            .to_string();

        assert!(err.contains("BTCEUR"), "{err}");
    }

    /// LSE lists the London Stock Exchange as coming soon.
    #[rstest]
    fn a_series_of_a_market_streamed_on_no_socket_is_refused_naming_its_market() {
        let err = of("VOD-LONDON_STOCK_EXCHANGE.LSEDGE")
            .unwrap()
            .stream()
            .unwrap_err()
            .to_string();

        assert!(err.contains("london_stock_exchange"), "{err}");
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
