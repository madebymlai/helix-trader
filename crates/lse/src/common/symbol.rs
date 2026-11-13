//! The symbol boundary: an instrument id and what it stands for at LSE
//! (`LseInstrument`), converted both ways here only: an `LSEDGE` id and the
//! LSE market and symbol of a derived series (`LseSymbol`), with the socket
//! the series streams on and its name there, a `GLBX` id and the CME
//! contract LSE lists (`CmeContract`), and an `OPRA` id and the OSI symbol
//! LSE lists a US option under (`OpraContract`).
//!
//! A derived market's series is one instrument under `LSEDGE`, its LSE
//! market the suffix of its symbol, as Bybit's product suffix (Nautilus'
//! adapter guide, "Symbols and instrument identity"): LSE lists one symbol
//! in several markets (GOLD in five). LSE's market names use underscores
//! and no hyphens, so the last hyphen splits symbol from market:
//! `GOLD-FUTURES.LSEDGE` is GOLD in `futures`. The market is written in
//! capitals in the id, LSE's folder name in lower case, and the symbol as
//! LSE lists it. An exchange market's listings take Databento's ids, so
//! none is an `LSEDGE` instrument: a CME Globex contract is Databento's
//! `GLBX` id, found among LSE's listings of its exchange name (see
//! `CmeContract`), and a US option Databento's `OPRA` id, its OSI symbol,
//! which LSE lists too (see `OpraContract`).

use std::fmt;

use anyhow::{Context, bail, ensure};
use jiff::{ToSpan, civil::Date};
use nautilus_model::{
    enums::OptionKind,
    identifiers::{
        InstrumentId, Symbol, Venue,
        futures::{
            format_futures_symbol, futures_month, parse_futures_symbol, resolve_futures_year,
        },
        osi_option_symbol,
    },
    types::Price,
};
use rust_decimal::Decimal;

use super::{
    consts::{EXCHANGE_MARKETS, LSEDGE_VENUE, LseSocket, OPRA_VENUE},
    credential::LseProduct,
};
use crate::http::models::LsePair;

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

    /// Whether the series is an index, which has no trades: its ticks are
    /// its values, of no size, which LSE's tick slice and its socket send.
    pub(crate) fn is_index(&self) -> bool {
        self.market == "indices"
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
            "uk_stocks" => (LseSocket::London, format!("{}.L", hyphenated(symbol))),
            "ca_stocks" => (LseSocket::Tsx, format!("{}.TO", hyphenated(symbol))),
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
fn hyphenated(symbol: &str) -> String {
    symbol.strip_suffix('.').unwrap_or(symbol).replace('.', "-")
}

/// What an instrument stands for at LSE, by its venue: a series of a
/// derived market under `LSEDGE`, asked with the LSE key by its symbol; a
/// CME Globex contract under `GLBX`, asked with the CME key by its listing;
/// or a US option contract under `OPRA`, asked with the LSE key, which
/// opens LSE's `us_options`, by its OSI symbol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LseInstrument {
    Series(LseSymbol),
    Cme(CmeContract),
    Opra(OpraContract),
}

impl LseInstrument {
    /// What `id` stands for, a CME contract's two-digit year resolved
    /// against `reference_year` (see `CmeContract::from_instrument_id`).
    ///
    /// # Errors
    ///
    /// Returns an error naming `id` if it is on none of the venues, or not
    /// an instrument of its venue (see `LseSymbol::from_instrument_id`,
    /// `CmeContract::from_instrument_id` and
    /// `OpraContract::from_instrument_id`).
    pub(crate) fn from_instrument_id(
        id: InstrumentId,
        reference_year: i16,
    ) -> anyhow::Result<Self> {
        if id.venue == *LSEDGE_VENUE {
            LseSymbol::from_instrument_id(id).map(Self::Series)
        } else if id.venue == Venue::GLBX() {
            CmeContract::from_instrument_id(id, reference_year).map(Self::Cme)
        } else if id.venue == *OPRA_VENUE {
            OpraContract::from_instrument_id(id).map(Self::Opra)
        } else {
            bail!(
                "{id} is on {}, which LSE does not serve: LSE serves LSEDGE, its own series, \
                 GLBX, CME Globex, and OPRA, US options",
                id.venue
            )
        }
    }

    /// The product whose key asks LSE for the instrument.
    pub(crate) const fn product(&self) -> LseProduct {
        match self {
            Self::Series(_) | Self::Opra(_) => LseProduct::Global,
            Self::Cme(_) => LseProduct::Cme,
        }
    }
}

/// A CME Globex contract under `GLBX`, the venue Databento gives CME Globex
/// and Nautilus' IB adapter maps, written in Nautilus' default two-digit
/// year (`FuturesYearDigits::Two`), as Databento's client writes it:
/// `ESZ26.GLBX` is the E-mini S&P 500 of December 2026.
///
/// CME names a contract by its product, its month letter and the last digit
/// of its year, two for natural gas and, from 2030, crude oil, so a name
/// comes round again after ten years: LSE lists ESZ6 of 2016 and of 2026
/// under one name, and the number of each listing, its `pair_id`, names it
/// alone (LSE's docs, "The market: cme"). So a contract is asked of LSE by
/// its listing's number, the listing found among those of its exchange
/// names (`listing`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CmeContract {
    root: String,
    month_code: char,
    /// The first day of the contract's month.
    month: Date,
}

impl CmeContract {
    /// The contract `id` stands for, its two-digit year resolved against
    /// `reference_year` as Nautilus resolves one (`resolve_futures_year`:
    /// within 30 years before it and 69 after).
    ///
    /// # Errors
    ///
    /// Returns an error naming `id` if it is not on `GLBX`, or not a
    /// futures contract's symbol with a two-digit year, a one-digit year
    /// standing for contracts ten years apart.
    pub(crate) fn from_instrument_id(
        id: InstrumentId,
        reference_year: i16,
    ) -> anyhow::Result<Self> {
        ensure!(
            id.venue == Venue::GLBX(),
            "{id} is not a CME Globex contract, under GLBX"
        );
        let parsed = parse_futures_symbol(id.symbol.as_str())
            .filter(|parsed| {
                parsed.year_digits.len() == 2
                    && parsed
                        .root
                        .chars()
                        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
            })
            .with_context(|| {
                format!(
                    "{id} is not a CME futures contract with a two-digit year, as ESZ26.GLBX: \
                     a one-digit year names contracts ten years apart"
                )
            })?;
        let year = resolve_futures_year(parsed.year_digits, i32::from(reference_year))
            .and_then(|year| i16::try_from(year).ok())
            .with_context(|| format!("the year of {id}"))?;
        let month = futures_month(parsed.month_code)
            .and_then(|month| i8::try_from(month).ok())
            .with_context(|| format!("the month of {id}"))?;
        Ok(Self {
            root: parsed.root.to_owned(),
            month_code: parsed.month_code,
            month: Date::new(year, month, 1).with_context(|| format!("the month of {id}"))?,
        })
    }

    /// The contract's id, under `GLBX`, in Nautilus' two-digit year.
    #[must_use]
    pub(crate) fn instrument_id(&self) -> InstrumentId {
        InstrumentId::new(Symbol::from(self.name(2).as_str()), Venue::GLBX())
    }

    /// The exchange names the contract may go by at LSE, in the order to
    /// look them up: its year's last digit, CME's usual name, then its two
    /// last digits, natural gas's and crude oil's from 2030 (LSE's docs,
    /// "The market: cme": "read a name from the catalogue instead of
    /// building it").
    #[must_use]
    pub(crate) fn exchange_names(&self) -> [String; 2] {
        [self.name(1), self.name(2)]
    }

    /// The contract's name with `year_digits` digits of its year.
    fn name(&self, year_digits: u8) -> String {
        format_futures_symbol(
            &self.root,
            self.month_code,
            i32::from(self.month.year()),
            year_digits,
        )
        .expect("a root, a month code and a year of 1 to 9999 make a name")
    }

    /// The contract's listing among `pairs`, LSE's listings of one of its
    /// exchange names: the one whose name says the contract's month (`GC
    /// futures 2026-12`), or, for a listing LSE names by its symbol alone,
    /// the one delisted when a contract of its month expires, from three
    /// months before the month to its end (CME's crude oil of January
    /// expires in December, its gold of December in December). None if no
    /// listing is the contract's.
    ///
    /// # Errors
    ///
    /// Returns an error naming them if more than one listing is the
    /// contract's.
    pub(crate) fn listing<'a>(&self, pairs: &'a [LsePair]) -> anyhow::Result<Option<&'a LsePair>> {
        let listings: Vec<&LsePair> = pairs
            .iter()
            .filter(|pair| self.is_listed_as(pair))
            .collect();
        match listings[..] {
            [] => Ok(None),
            [listing] => Ok(Some(listing)),
            _ => bail!(
                "LSE lists {} listings of {self}: {}",
                listings.len(),
                listings
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }

    /// Whether `pair` is the contract's listing: by the month its name
    /// says, or, its name saying none, by the day it was delisted.
    fn is_listed_as(&self, pair: &LsePair) -> bool {
        match named_month(&pair.name) {
            Some(month) => month == self.month,
            None => pair.delisted_date.is_some_and(|delisted| {
                self.month - 3.months() <= delisted && delisted < self.month + 1.month()
            }),
        }
    }
}

/// The month a CME listing's name says, as in `GC futures 2026-12`.
fn named_month(name: &str) -> Option<Date> {
    let (_, month) = name.rsplit_once(" futures ")?;
    Date::strptime("%Y-%m-%d", format!("{month}-01")).ok()
}

/// A US option contract under `OPRA`, the venue Databento gives the
/// listings of OPRA's consolidated feed (`OPRA.PILLAR`, whose definitions
/// its publisher `OPRA` sends) and Nautilus' IB adapter maps, its symbol
/// the contract's OSI symbol as Databento writes it: the root padded to
/// six characters, the expiry's YYMMDD, C or P, and the strike in
/// thousandths to eight digits (Databento's OPRA specification,
/// "Symbology"). `AAPL  261120C00355000.OPRA` is AAPL's 355 call of 20
/// November 2026. LSE lists the contract under that symbol in `us_options`
/// (LSE's docs, "Symbols": "The OCC symbol exactly as listed: capitals,
/// the root padded to six characters"), and a symbol names one contract,
/// so it is asked by it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OpraContract {
    osi: String,
}

impl OpraContract {
    /// The contract `id` stands for.
    ///
    /// # Errors
    ///
    /// Returns an error naming `id` if it is not on `OPRA`, or if its
    /// symbol is not an OSI symbol as Databento and LSE write it: padded,
    /// its root in capitals, as Nautilus' OSI function writes one
    /// (`osi_option_symbol`), not as LSE's sockets do, unpadded.
    pub(crate) fn from_instrument_id(id: InstrumentId) -> anyhow::Result<Self> {
        ensure!(
            id.venue == *OPRA_VENUE,
            "{id} is not a US option contract, under OPRA"
        );
        let osi = id.symbol.as_str();
        ensure!(
            is_osi(osi),
            "{id} is not an OSI symbol under OPRA, as AAPL  261120C00355000.OPRA: its root in \
             capitals padded to six characters, its expiry's YYMMDD, C or P, and its strike in \
             thousandths to eight digits"
        );
        Ok(Self {
            osi: osi.to_owned(),
        })
    }

    /// The contract's id, its OSI symbol under `OPRA`.
    #[must_use]
    pub(crate) fn instrument_id(&self) -> InstrumentId {
        InstrumentId::new(Symbol::from(self.osi.as_str()), *OPRA_VENUE)
    }

    /// The contract's OSI symbol, padded, as LSE lists it.
    #[must_use]
    pub(crate) fn osi(&self) -> &str {
        &self.osi
    }
}

impl fmt::Display for OpraContract {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.instrument_id())
    }
}

/// Whether `symbol` is an OSI symbol as Nautilus writes one from its parts
/// (`osi_option_symbol`), its root in capitals: its root, the six
/// characters before the expiry, unpadded; the expiry's two-digit year of
/// this century, month and day; C or P; and the strike in thousandths.
fn is_osi(symbol: &str) -> bool {
    let written = (|| {
        let (root, expiry) = (symbol.get(..6)?, symbol.get(6..12)?);
        let (kind, strike) = (symbol.get(12..13)?, symbol.get(13..)?);
        let root = root.trim_end_matches(' ');
        if !root
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
        {
            return None;
        }
        let part = |range: std::ops::Range<usize>| expiry.get(range)?.parse::<i8>().ok();
        let expiry = Date::new(2000 + i16::from(part(0..2)?), part(2..4)?, part(4..6)?).ok()?;
        let kind = match kind {
            "C" => OptionKind::Call,
            "P" => OptionKind::Put,
            _ => return None,
        };
        let strike = Price::from_decimal_dp(Decimal::new(strike.parse().ok()?, 3), 3).ok()?;
        osi_option_symbol(root, expiry, kind, strike).ok()
    })();
    written.is_some_and(|written| written == symbol)
}

impl fmt::Display for CmeContract {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}, the {} futures of {}",
            self.instrument_id(),
            self.root,
            self.month.strftime("%Y-%m")
        )
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::{
        http::models::LsePairs,
        testing::{PAIRS_ESZ6, PAIRS_ESZ26, PAIRS_GCZ4, PAIRS_NGZ6, PAIRS_NGZ26},
    };

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

    /// A CME contract of `id` as of 2026.
    fn cme(id: &str) -> anyhow::Result<CmeContract> {
        CmeContract::from_instrument_id(InstrumentId::from(id), 2026)
    }

    fn pairs(answer: &str) -> Vec<LsePair> {
        serde_json::from_str::<LsePairs>(answer).unwrap().pairs
    }

    /// The pair id of the listing of `id` among LSE's listings `answer`.
    fn listed(id: &str, answer: &str) -> Option<u64> {
        cme(id)
            .unwrap()
            .listing(&pairs(answer))
            .unwrap()
            .map(|pair| pair.pair_id)
    }

    #[rstest]
    #[case::es_of_2026("ESZ26.GLBX", ["ESZ6", "ESZ26"])]
    #[case::es_of_2016("ESZ16.GLBX", ["ESZ6", "ESZ16"])]
    #[case::natural_gas("NGZ26.GLBX", ["NGZ6", "NGZ26"])]
    #[case::crude_of_2030("CLZ30.GLBX", ["CLZ0", "CLZ30"])]
    #[case::sofr("SR3H27.GLBX", ["SR3H7", "SR3H27"])]
    #[case::euro_fx("6EH27.GLBX", ["6EH7", "6EH27"])]
    fn a_glbx_contract_goes_by_its_year_s_last_digit_or_two_at_lse(
        #[case] id: &str,
        #[case] names: [&str; 2],
    ) {
        assert_eq!(cme(id).unwrap().exchange_names(), names.map(String::from));
    }

    #[rstest]
    #[case("ESZ26.GLBX")]
    #[case("GCZ16.GLBX")]
    #[case("SR3H27.GLBX")]
    #[case("6EH27.GLBX")]
    fn a_glbx_contract_round_trips_through_its_instrument_id(#[case] id: &str) {
        assert_eq!(cme(id).unwrap().instrument_id(), InstrumentId::from(id));
    }

    /// A one-digit year stands for contracts ten years apart; a root is no
    /// contract; a spread or an option is not served.
    #[rstest]
    #[case::one_digit_year("ESZ6.GLBX")]
    #[case::four_digit_year("ESZ2026.GLBX")]
    #[case::root("GC.GLBX")]
    #[case::continuous("GC.v.0.GLBX")]
    #[case::spread("ESU26-ESM27.GLBX")]
    #[case::no_month("ES26.GLBX")]
    fn a_glbx_id_that_is_no_contract_with_a_two_digit_year_is_refused_naming_it(#[case] id: &str) {
        let err = cme(id).unwrap_err().to_string();

        assert!(err.contains(id), "{id}: {err}");
    }

    #[rstest]
    fn an_id_of_another_venue_is_no_cme_contract() {
        let err = cme("ESZ26.CME").unwrap_err().to_string();

        assert!(err.contains("ESZ26.CME"), "{err}");
    }

    /// LSE's ESZ6 names the contracts of 2016 and of 2026, numbered
    /// 4000658762 and 4000000001.
    #[rstest]
    #[case::of_2026("ESZ26.GLBX", 4_000_000_001)]
    #[case::of_2016("ESZ16.GLBX", 4_000_658_762)]
    fn esz6_of_2016_and_of_2026_resolve_to_their_own_listings(
        #[case] id: &str,
        #[case] pair_id: u64,
    ) {
        assert_eq!(listed(id, PAIRS_ESZ6), Some(pair_id));
    }

    /// GCZ4 of 2014 is named `GC futures 2014-12`; GCZ4 of 2024 by its
    /// symbol alone, delisted on 27 December 2024.
    #[rstest]
    #[case::named_by_its_month("GCZ14.GLBX", 4_000_992_242)]
    #[case::named_by_its_symbol("GCZ24.GLBX", 4_000_402_563)]
    fn a_listing_named_by_its_symbol_alone_is_found_by_its_delisting(
        #[case] id: &str,
        #[case] pair_id: u64,
    ) {
        assert_eq!(listed(id, PAIRS_GCZ4), Some(pair_id));
    }

    /// LSE's NGZ6 is the contract of 2016 alone; 2026's is NGZ26.
    #[rstest]
    fn a_contract_none_of_a_name_s_listings_is_has_no_listing_there() {
        assert_eq!(listed("NGZ26.GLBX", PAIRS_NGZ6), None);
        assert_eq!(listed("NGZ26.GLBX", PAIRS_NGZ26), Some(4_000_000_128));
        assert_eq!(listed("GCZ34.GLBX", PAIRS_GCZ4), None);
        assert_eq!(listed("ESZ26.GLBX", PAIRS_ESZ26), None);
    }

    #[rstest]
    fn two_listings_of_one_contract_are_refused_naming_both() {
        let mut twice = pairs(PAIRS_ESZ6);
        twice[1].name = twice[0].name.clone();

        let err = cme("ESZ26.GLBX")
            .unwrap()
            .listing(&twice)
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("4000000001") && err.contains("4000658762"),
            "{err}"
        );
    }

    /// An OSI symbol, as Databento's OPRA listings and LSE's `us_options`
    /// spell it: the root padded to six characters, the expiry's YYMMDD, C
    /// or P, and the strike in thousandths to eight digits (Databento's
    /// OPRA specification, "Symbology").
    #[rstest]
    #[case::aapl_call("AAPL  261120C00355000.OPRA", "AAPL  261120C00355000")]
    #[case::spy_put("SPY   240319P00511000.OPRA", "SPY   240319P00511000")]
    #[case::one_letter_root("F     261218C00012500.OPRA", "F     261218C00012500")]
    #[case::six_letter_root("GOOGL1270115P00150000.OPRA", "GOOGL1270115P00150000")]
    #[case::weekly_root("SPXW  261120P05900000.OPRA", "SPXW  261120P05900000")]
    fn an_opra_contract_is_the_osi_symbol_lse_lists_and_back(#[case] id: &str, #[case] osi: &str) {
        let contract = OpraContract::from_instrument_id(InstrumentId::from(id)).unwrap();

        assert_eq!(contract.osi(), osi);
        assert_eq!(contract.instrument_id(), InstrumentId::from(id));
    }

    /// Unpadded, as LSE's sockets write it; a root in lower case; no such
    /// day; neither call nor put; a strike short of its eight digits; a
    /// stock; and another venue's id.
    #[rstest]
    #[case::unpadded("AAPL261120C00355000.OPRA")]
    #[case::lower_case_root("aapl  261120C00355000.OPRA")]
    #[case::no_such_day("AAPL  261131C00355000.OPRA")]
    #[case::neither_call_nor_put("AAPL  261120X00355000.OPRA")]
    #[case::short_strike("AAPL  261120C0035500.OPRA")]
    #[case::stock("AAPL.OPRA")]
    #[case::another_venue("AAPL  261120C00355000.XCBO")]
    fn an_id_that_is_no_osi_symbol_under_opra_is_refused_naming_it(#[case] id: &str) {
        let err = OpraContract::from_instrument_id(InstrumentId::from(id))
            .unwrap_err()
            .to_string();

        assert!(err.contains(id), "{id}: {err}");
    }

    #[rstest]
    #[case::series("GOLD-FUTURES.LSEDGE", LseProduct::Global)]
    #[case::cme_contract("GCZ26.GLBX", LseProduct::Cme)]
    #[case::us_option("AAPL  261120C00355000.OPRA", LseProduct::Global)]
    fn each_venue_s_instruments_are_asked_with_its_product_s_key(
        #[case] id: &str,
        #[case] product: LseProduct,
    ) {
        let instrument = LseInstrument::from_instrument_id(InstrumentId::from(id), 2026).unwrap();

        assert_eq!(instrument.product(), product);
    }

    #[rstest]
    fn an_instrument_of_a_venue_lse_does_not_serve_is_refused_naming_it() {
        let err = LseInstrument::from_instrument_id(InstrumentId::from("XAUUSD.ICMARKETS"), 2026)
            .unwrap_err()
            .to_string();

        assert!(err.contains("XAUUSD.ICMARKETS"), "{err}");
    }
}
