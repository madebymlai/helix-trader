//! The symbol boundary: an instrument id and what it stands for at LSE (`LseInstrument`), converted
//! both ways here only.
//!
//! The instruments are:
//! - An `LSEDGE` id and a derived series' LSE market and symbol (`LseSymbol`), with its socket and
//!   name there.
//! - A `GLBX` id and the CME contract LSE lists (`CmeContract`).
//! - An `XNAS` id and the ticker LSE lists and streams a Nasdaq TotalView stock by (`NasdaqStock`).
//! - An `OPRA` id and the OSI symbol LSE lists a US option under (`OpraContract`), with its name on
//!   data-ws and its chain's root.
//!
//! A derived series is one instrument under `LSEDGE`, its LSE market the suffix of its symbol, as
//! Bybit's product suffix (Nautilus' adapter guide, "Symbols and instrument identity"): LSE lists
//! one symbol in several markets (GOLD in five). LSE's market names have underscores and no
//! hyphens, so the last hyphen splits symbol from market: `GOLD-FUTURES.LSEDGE` is GOLD in
//! `futures`. The id writes the market in capitals, LSE's folder name in lower case.
//!
//! An exchange market's listings take Databento's ids, so none is an `LSEDGE` instrument: `GLBX`
//! for CME Globex, `XNAS` for Nasdaq stocks, `OPRA` for US options.
use std::fmt::Display;

use anyhow::Context;
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
    consts::{EXCHANGE_MARKETS, LSEDGE_VENUE, LseSocket, OPRA_VENUE, XNAS_VENUE},
    credential::LseProduct,
};
use crate::http::models::LsePair;

/// A series of an LSE derived market: its market, LSE's folder name, and its symbol as `/v3/pairs`
/// lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LseSymbol {
    market: String,
    symbol: String,
}

impl LseSymbol {
    /// Creates `symbol` in LSE's derived market `market`.
    ///
    /// # Errors
    ///
    /// Returns an error if the symbol is empty, or the market is empty, holds anything but
    /// lower-case letters, digits and underscores, or is an exchange market's.
    pub fn new(market: &str, symbol: &str) -> anyhow::Result<Self> {
        anyhow::ensure!(!symbol.is_empty(), "an LSE symbol is not empty");
        anyhow::ensure!(
            !market.is_empty()
                && market
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
            "{market:?} is not an LSE market's name: lower-case letters, digits and underscores"
        );
        anyhow::ensure!(
            !EXCHANGE_MARKETS.contains(&market),
            "{market} is an LSE exchange market: its listings are not LSEDGE series"
        );
        Ok(Self {
            market: market.to_owned(),
            symbol: symbol.to_owned(),
        })
    }

    /// Returns the series `id` stands for.
    ///
    /// # Errors
    ///
    /// Returns an error if `id` is not on `LSEDGE`, or its symbol is not an LSE symbol, a hyphen
    /// and its market in capitals.
    pub fn from_instrument_id(id: InstrumentId) -> anyhow::Result<Self> {
        if id.venue != *LSEDGE_VENUE {
            anyhow::bail!("{id} is not an LSEDGE instrument");
        }
        let Some((symbol, market)) = id.symbol.as_str().rsplit_once('-') else {
            anyhow::bail!(
                "{id} names no LSE market: its symbol is an LSE symbol, a hyphen and its market"
            );
        };
        anyhow::ensure!(
            market.chars().all(|c| !c.is_ascii_lowercase()),
            "{id} names its LSE market in lower case: write it in capitals"
        );
        Self::new(&market.to_ascii_lowercase(), symbol)
            .map_err(|e| anyhow::anyhow!("{id} is not an LSEDGE series: {e}"))
    }

    /// Returns LSE's name of the market, its folder.
    #[must_use]
    pub fn market(&self) -> &str {
        &self.market
    }

    /// Returns the symbol, as LSE lists it.
    #[must_use]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Returns whether the series is an index, which has no trades: its ticks are values of no
    /// size.
    pub fn is_index(&self) -> bool {
        self.market == "indices"
    }

    /// Returns the socket the series streams on and its name there (LSE's socket documentation,
    /// "Symbols on each socket").
    ///
    /// On data-ws, a US stock, commodity or forex pair keeps its symbol, a pair's underscore a
    /// slash (`EUR/USD`), a future is suffixed `.F` (`GOLD.F`) and an index `.IDX` (`SPX.IDX`). On
    /// london-ws, a UK stock is suffixed `.L` and a crypto pair is in dollars (`BTCUSDT` is
    /// `BTC/USD`). On tsx-ws, a Canadian stock is suffixed `.TO`. A UK or Canadian stock's dots are
    /// hyphens there, a trailing one dropped (`BT.A` is `BT-A.L`, `BP.` is `BP.L`).
    ///
    /// # Errors
    ///
    /// Returns an error if the market streams on no socket, or if a crypto symbol is not quoted in
    /// Tether.
    pub fn stream(&self) -> anyhow::Result<(LseSocket, String)> {
        let symbol = &self.symbol;
        Ok(match self.market.as_str() {
            "us_stocks" | "commodities" => (LseSocket::Data, symbol.clone()),
            "forex" => (LseSocket::Data, symbol.replace('_', "/")),
            "futures" => (LseSocket::Data, format!("{symbol}.F")),
            "indices" => (LseSocket::Data, format!("{symbol}.IDX")),
            "crypto" => {
                let Some(base) = symbol.strip_suffix("USDT") else {
                    anyhow::bail!(
                        "{symbol} in LSE's crypto is not quoted in Tether, as LSE's crypto \
                         symbols are (BTCUSDT): london-ws names no pair of it"
                    );
                };
                (LseSocket::London, format!("{base}/USD"))
            }
            "uk_stocks" => (LseSocket::London, format!("{}.L", hyphenated(symbol))),
            "ca_stocks" => (LseSocket::Tsx, format!("{}.TO", hyphenated(symbol))),
            market => anyhow::bail!("the series of LSE's {market} stream on no LSE socket"),
        })
    }
}

impl Display for LseSymbol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} in LSE's {}", self.symbol, self.market)
    }
}

// A UK or Canadian stock's symbol before its socket's suffix: dots as hyphens, trailing one dropped
fn hyphenated(symbol: &str) -> String {
    symbol.strip_suffix('.').unwrap_or(symbol).replace('.', "-")
}

/// What an instrument stands for at LSE, by venue, with the key that asks for it:
/// - `LSEDGE`: a derived series, by symbol, with the LSE key.
/// - `GLBX`: a CME Globex contract, by listing, with the CME key.
/// - `XNAS`: a Nasdaq TotalView stock, by ticker, with the Nasdaq key.
/// - `OPRA`: a US option, by OSI symbol, with the LSE key, which opens `us_options`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LseInstrument {
    Series(LseSymbol),
    Cme(CmeContract),
    Nasdaq(NasdaqStock),
    Opra(OpraContract),
}

impl LseInstrument {
    /// Returns what `id` stands for, a CME contract's two-digit year resolved against
    /// `reference_year` (see `CmeContract::from_instrument_id`).
    ///
    /// # Errors
    ///
    /// Returns an error if `id` is on none of the venues, or not an instrument of its venue.
    pub fn from_instrument_id(id: InstrumentId, reference_year: i16) -> anyhow::Result<Self> {
        if id.venue == *LSEDGE_VENUE {
            LseSymbol::from_instrument_id(id).map(Self::Series)
        } else if id.venue == Venue::GLBX() {
            CmeContract::from_instrument_id(id, reference_year).map(Self::Cme)
        } else if id.venue == *XNAS_VENUE {
            NasdaqStock::from_instrument_id(id).map(Self::Nasdaq)
        } else if id.venue == *OPRA_VENUE {
            OpraContract::from_instrument_id(id).map(Self::Opra)
        } else {
            anyhow::bail!(
                "{id} is on {}, which LSE does not serve: LSE serves LSEDGE, its own series, \
                 GLBX, CME Globex, XNAS, Nasdaq TotalView, and OPRA, US options",
                id.venue
            )
        }
    }

    pub const fn product(&self) -> LseProduct {
        match self {
            Self::Series(_) | Self::Opra(_) => LseProduct::Global,
            Self::Cme(_) => LseProduct::Cme,
            Self::Nasdaq(_) => LseProduct::Nasdaq,
        }
    }
}

/// A CME Globex contract under `GLBX`, in Nautilus' default two-digit year
/// (`FuturesYearDigits::Two`), as Databento's client writes it: `ESZ26.GLBX` is the E-mini S&P 500
/// of December 2026.
///
/// CME names a contract by product, month letter and the last digit of its year, two for natural
/// gas and, from 2030, crude oil, so a name recurs after ten years: LSE lists ESZ6 of 2016 and of
/// 2026 under one name, and only a listing's `pair_id` names it (LSE's docs, "The market: cme").
/// So a contract is asked by its listing's number, found among those of its exchange names
/// (`listing`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CmeContract {
    root: String,
    month_code: char,
    /// The first day of the contract's month.
    month: Date,
}

impl CmeContract {
    /// Returns the contract `id` stands for, its two-digit year resolved against `reference_year`
    /// as `resolve_futures_year` does: within 30 years before it and 69 after.
    ///
    /// # Errors
    ///
    /// Returns an error if `id` is not on `GLBX`, or not a futures symbol with a two-digit year, a
    /// one-digit year standing for contracts ten years apart.
    pub fn from_instrument_id(id: InstrumentId, reference_year: i16) -> anyhow::Result<Self> {
        anyhow::ensure!(
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

    #[must_use]
    pub fn instrument_id(&self) -> InstrumentId {
        InstrumentId::new(Symbol::from(self.name(2).as_str()), Venue::GLBX())
    }

    /// Returns the exchange names the contract may go by at LSE, in lookup order: its year's last
    /// digit, then its last two, as natural gas and, from 2030, crude oil (LSE's docs, "The market:
    /// cme": "read a name from the catalogue instead of building it").
    #[must_use]
    pub fn exchange_names(&self) -> [String; 2] {
        [self.name(1), self.name(2)]
    }

    fn name(&self, year_digits: u8) -> String {
        format_futures_symbol(
            &self.root,
            self.month_code,
            i32::from(self.month.year()),
            year_digits,
        )
        .expect("a root, a month code and a year of 1 to 9999 make a name")
    }

    /// Returns the contract's listing among `pairs`, LSE's listings of one of its exchange names.
    ///
    /// The listing is the one whose name says the contract's month (`GC futures 2026-12`) or, for a
    /// listing named by its symbol alone, the one delisted from three months before the month to
    /// its end (crude oil of January expires in December, gold of December in December).
    ///
    /// # Errors
    ///
    /// Returns an error if more than one listing is the contract's, naming them.
    pub fn listing<'a>(&self, pairs: &'a [LsePair]) -> anyhow::Result<Option<&'a LsePair>> {
        let listings: Vec<&LsePair> = pairs
            .iter()
            .filter(|pair| self.is_listed_as(pair))
            .collect();
        match listings[..] {
            [] => Ok(None),
            [listing] => Ok(Some(listing)),
            _ => anyhow::bail!(
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

    fn is_listed_as(&self, pair: &LsePair) -> bool {
        match pair.name.as_deref().and_then(named_month) {
            Some(month) => month == self.month,
            None => pair.delisted_date.is_some_and(|delisted| {
                self.month - 3.months() <= delisted && delisted < self.month + 1.month()
            }),
        }
    }
}

// The month a CME listing's name says, as in `GC futures 2026-12`
fn named_month(name: &str) -> Option<Date> {
    let (_, month) = name.rsplit_once(" futures ")?;
    Date::strptime("%Y-%m-%d", format!("{month}-01")).ok()
}

/// A Nasdaq TotalView stock under `XNAS`, Databento's venue for `XNAS.ITCH`: `NVDA.XNAS` is
/// NVIDIA's common stock.
///
/// Its symbol is the ticker with the class after a space (`BRK B.XNAS`), as Nautilus' Databento
/// client writes it, since a dot separates symbol and venue (`share_class_space`, on by default).
/// Its ticker at LSE is Nasdaq's, the class after a dot, as in Databento's raw `XNAS.ITCH` symbols
/// ("CMS vs. Nasdaq symbology") and LSE's `nasdaq` listing (`BRK.B`, captured), and the stock is
/// asked and streamed by it.
///
/// LSE takes a renamed ticker for the days it was listed (LSE's docs, "Symbols"). Its catalogue
/// lists some tickers more than once, a delisted listing beside a later one (`BRKH`, captured),
/// as Databento's ids name a ticker, not a listing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NasdaqStock {
    /// Nasdaq's ticker, a class after a dot, as LSE lists it.
    ticker: String,
}

impl NasdaqStock {
    /// Returns the stock `id` stands for.
    ///
    /// # Errors
    ///
    /// Returns an error if `id` is not on `XNAS`, or if its symbol is not a ticker in capitals with
    /// its class after one space: lower case, which LSE would match, or a class after a dot would
    /// make a second id of one stock.
    pub fn from_instrument_id(id: InstrumentId) -> anyhow::Result<Self> {
        anyhow::ensure!(
            id.venue == *XNAS_VENUE,
            "{id} is not a Nasdaq TotalView stock, under XNAS"
        );
        let symbol = id.symbol.as_str();
        anyhow::ensure!(
            symbol.split(' ').all(|part| {
                !part.is_empty()
                    && !part
                        .chars()
                        .any(|c| c.is_ascii_lowercase() || c.is_whitespace() || c == '.')
            }),
            "{id} is not a Nasdaq ticker in capitals, its class after a space, as NVDA.XNAS or \
             BRK B.XNAS, as Nautilus' Databento client writes it"
        );
        Ok(Self {
            ticker: symbol.replace(' ', "."),
        })
    }

    #[must_use]
    pub fn instrument_id(&self) -> InstrumentId {
        InstrumentId::new(
            Symbol::from(self.ticker.replace('.', " ").as_str()),
            *XNAS_VENUE,
        )
    }

    /// Returns the stock's ticker as LSE lists and streams it.
    #[must_use]
    pub fn ticker(&self) -> &str {
        &self.ticker
    }
}

impl Display for NasdaqStock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.instrument_id())
    }
}

/// A US option contract under `OPRA`, Databento's venue for `OPRA.PILLAR`, its symbol the OSI
/// symbol as Databento writes it.
///
/// The OSI symbol is the root padded to six characters, the expiry's YYMMDD, C or P, and the strike
/// in thousandths to eight digits (Databento's OPRA specification, "Symbology"). `AAPL
/// 261120C00355000.OPRA` is AAPL's 355 call of 20 November 2026. LSE lists the contract under that
/// symbol in `us_options` (LSE's docs, "Symbols": "The OCC symbol exactly as listed: capitals, the
/// root padded to six characters"), so it is asked by it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpraContract {
    osi: String,
}

impl OpraContract {
    /// Returns the contract `id` stands for.
    ///
    /// # Errors
    ///
    /// Returns an error if `id` is not on `OPRA`, or if its symbol is not a padded OSI symbol with
    /// its root in capitals, as `osi_option_symbol` writes one, not unpadded as LSE's sockets do.
    pub fn from_instrument_id(id: InstrumentId) -> anyhow::Result<Self> {
        anyhow::ensure!(
            id.venue == *OPRA_VENUE,
            "{id} is not a US option contract, under OPRA"
        );
        let osi = id.symbol.as_str();
        anyhow::ensure!(
            is_osi(osi),
            "{id} is not an OSI symbol under OPRA, as AAPL  261120C00355000.OPRA: its root in \
             capitals padded to six characters, its expiry's YYMMDD, C or P, and its strike in \
             thousandths to eight digits"
        );
        Ok(Self {
            osi: osi.to_owned(),
        })
    }

    #[must_use]
    pub fn instrument_id(&self) -> InstrumentId {
        InstrumentId::new(Symbol::from(self.osi.as_str()), *OPRA_VENUE)
    }

    /// Returns the contract's padded OSI symbol, as LSE lists it.
    #[must_use]
    pub fn osi(&self) -> &str {
        &self.osi
    }

    /// Returns data-ws and the contract's name there, its OSI symbol without spaces (LSE's socket
    /// documentation, "Option chains": `AAPL261120C00100000`), in its root's chain (`root`).
    #[must_use]
    pub fn stream(&self) -> (LseSocket, String) {
        (LseSocket::Data, self.osi.replace(' ', ""))
    }

    /// Returns the unpadded root of the OSI symbol, the underlying whose chain LSE streams the
    /// contract in (`subscribe_options`).
    #[must_use]
    pub fn root(&self) -> &str {
        self.osi[..6].trim_end_matches(' ')
    }
}

impl Display for OpraContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.instrument_id())
    }
}

// Whether `symbol` is what `osi_option_symbol` writes from its parts: the root in capitals, the
// expiry's two-digit year of this century, C or P, and the strike in thousandths.
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

impl Display for CmeContract {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
        testing::{
            PAIRS_ESZ6, PAIRS_ESZ26, PAIRS_GCZ4, PAIRS_NASDAQ_BRK, PAIRS_NASDAQ_NVDA, PAIRS_NGZ6,
            PAIRS_NGZ26,
        },
    };

    fn of(id: &str) -> anyhow::Result<LseSymbol> {
        LseSymbol::from_instrument_id(InstrumentId::from(id))
    }

    #[rstest]
    #[case::futures("GOLD-FUTURES.LSEDGE", "futures", "GOLD")]
    #[case::futures_of_digits("US10Y-FUTURES.LSEDGE", "futures", "US10Y")]
    #[case::hyphenated_symbol("BRK-B-US_STOCKS.LSEDGE", "us_stocks", "BRK-B")]
    #[case::underscored_symbol("EUR_USD-FOREX.LSEDGE", "forex", "EUR_USD")]
    #[case::slashed_symbol("XAU/USD-COMMODITIES.LSEDGE", "commodities", "XAU/USD")]
    #[case::underscored_market("RY-CA_STOCKS.LSEDGE", "ca_stocks", "RY")]
    fn an_lsedge_id_is_its_symbol_in_the_market_after_its_last_hyphen(
        #[case] id: &str,
        #[case] market: &str,
        #[case] symbol: &str,
    ) {
        let series = of(id).unwrap();

        assert_eq!((series.market(), series.symbol()), (market, symbol));
    }

    /// A lower-case market would make a second id of one series.
    #[rstest]
    #[case::no_hyphen("GOLD.LSEDGE")]
    #[case::empty_symbol("-FUTURES.LSEDGE")]
    #[case::empty_market("GOLD-.LSEDGE")]
    #[case::lower_case_market("GOLD-futures.LSEDGE")]
    #[case::no_market_s_name("GOLD-FU+TURES.LSEDGE")]
    #[case::another_venue("GOLD-FUTURES.GLBX")]
    fn an_id_without_its_market_in_capitals_is_refused_naming_it(#[case] id: &str) {
        let err = of(id).unwrap_err().to_string();

        assert!(err.contains(id), "{err}");
    }

    /// LSE's socket documentation ("Symbols on each socket"), as each socket acknowledged and
    /// ticked them on 13 November 2026, data-ws listing its futures so once authenticated
    /// (`test_data/ws_authenticated_futures.json`).
    #[rstest]
    #[case("GOLD-FUTURES.LSEDGE", "data-ws", "GOLD.F")]
    #[case("US10Y-FUTURES.LSEDGE", "data-ws", "US10Y.F")]
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

    /// london-ws spells a pair in dollars, so a quote but Tether has no name there.
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

    #[rstest]
    #[case::cme("ESZ6-CME.LSEDGE", "cme")]
    #[case::nasdaq("NVDA-NASDAQ.LSEDGE", "nasdaq")]
    #[case::us_options("AAPL261120C00100000-US_OPTIONS.LSEDGE", "us_options")]
    #[case::opra("AAPL-OPRA.LSEDGE", "opra")]
    fn an_exchange_market_is_no_lsedge_series(#[case] id: &str, #[case] market: &str) {
        let err = of(id).unwrap_err().to_string();

        assert!(err.contains(id) && err.contains(market), "{err}");
    }

    fn cme(id: &str) -> anyhow::Result<CmeContract> {
        CmeContract::from_instrument_id(InstrumentId::from(id), 2026)
    }

    fn pairs(answer: &str) -> Vec<LsePair> {
        serde_json::from_str::<LsePairs>(answer).unwrap().pairs
    }

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

    #[rstest]
    #[case::one_digit_year("ESZ6.GLBX")]
    #[case::four_digit_year("ESZ2026.GLBX")]
    #[case::root("GC.GLBX")]
    #[case::continuous("GC.v.0.GLBX")]
    #[case::spread("ESU26-ESM27.GLBX")]
    #[case::no_month("ES26.GLBX")]
    #[case::another_venue("ESZ26.CME")]
    fn a_glbx_id_that_is_no_contract_with_a_two_digit_year_is_refused_naming_it(#[case] id: &str) {
        let err = cme(id).unwrap_err().to_string();

        assert!(err.contains(id), "{err}");
    }

    /// LSE's ESZ6 names 2016 (4000658762) and 2026 (4000000001), its NGZ26 natural gas of 2026
    /// (4000000128).
    #[rstest]
    #[case::esz6_of_2026("ESZ26.GLBX", PAIRS_ESZ6, 4_000_000_001)]
    #[case::esz6_of_2016("ESZ16.GLBX", PAIRS_ESZ6, 4_000_658_762)]
    #[case::ngz26("NGZ26.GLBX", PAIRS_NGZ26, 4_000_000_128)]
    fn a_contract_resolves_to_its_own_year_s_listing(
        #[case] id: &str,
        #[case] answer: &str,
        #[case] pair_id: u64,
    ) {
        assert_eq!(listed(id, answer), Some(pair_id));
    }

    /// GCZ4 of 2014 is named `GC futures 2014-12`, GCZ4 of 2024 by its symbol alone, delisted on 27
    /// December 2024.
    #[rstest]
    #[case::named_by_its_month("GCZ14.GLBX", 4_000_992_242)]
    #[case::named_by_its_symbol("GCZ24.GLBX", 4_000_402_563)]
    fn a_listing_named_by_its_symbol_alone_is_found_by_its_delisting(
        #[case] id: &str,
        #[case] pair_id: u64,
    ) {
        assert_eq!(listed(id, PAIRS_GCZ4), Some(pair_id));
    }

    /// LSE's catalogue gives some delisted listings no name (captured on `nasdaq`).
    #[rstest]
    fn a_listing_of_no_name_is_found_by_its_delisting() {
        let mut unnamed = pairs(PAIRS_GCZ4);
        unnamed[0].name = None;

        let listing = cme("GCZ24.GLBX").unwrap().listing(&unnamed).unwrap();

        assert_eq!(listing.map(|pair| pair.pair_id), Some(4_000_402_563));
    }

    /// LSE's NGZ6 is 2016 alone, 2026 being NGZ26, GCZ4 is 2014 and 2024, not 2034, and LSE lists
    /// no ESZ26.
    #[rstest]
    #[case::ngz6_of_2016_alone("NGZ26.GLBX", PAIRS_NGZ6)]
    #[case::gcz4_of_past_decades("GCZ34.GLBX", PAIRS_GCZ4)]
    #[case::no_listing_at_all("ESZ26.GLBX", PAIRS_ESZ26)]
    fn a_contract_none_of_a_name_s_listings_is_has_no_listing_there(
        #[case] id: &str,
        #[case] answer: &str,
    ) {
        assert_eq!(listed(id, answer), None);
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

    /// Databento's OPRA specification, "Symbology".
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

    /// LSE's socket documentation, "Option chains": "the OSI form without spaces".
    #[rstest]
    #[case::aapl_call("AAPL  261120C00355000.OPRA", "AAPL261120C00355000", "AAPL")]
    #[case::one_letter_root("F     261218C00012500.OPRA", "F261218C00012500", "F")]
    #[case::six_letter_root("GOOGL1270115P00150000.OPRA", "GOOGL1270115P00150000", "GOOGL1")]
    fn an_opra_contract_streams_on_data_ws_unpadded_in_its_root_s_chain(
        #[case] id: &str,
        #[case] name: &str,
        #[case] root: &str,
    ) {
        let contract = OpraContract::from_instrument_id(InstrumentId::from(id)).unwrap();

        assert_eq!(contract.stream(), (LseSocket::Data, name.to_string()));
        assert_eq!(contract.root(), root);
    }

    /// Databento's raw `XNAS.ITCH` symbols are Nasdaq's tickers, a class after a dot ("CMS vs.
    /// Nasdaq symbology": `BRK.B`), as in LSE's `nasdaq` catalogue, captured (`answer`).
    #[rstest]
    #[case::common_stock("NVDA.XNAS", "NVDA", PAIRS_NASDAQ_NVDA)]
    #[case::class_b("BRK B.XNAS", "BRK.B", PAIRS_NASDAQ_BRK)]
    fn an_xnas_stock_is_its_ticker_at_lse_and_back(
        #[case] id: &str,
        #[case] ticker: &str,
        #[case] answer: &str,
    ) {
        let stock = NasdaqStock::from_instrument_id(InstrumentId::from(id)).unwrap();
        let listed: serde_json::Value = serde_json::from_str(answer).unwrap();

        assert_eq!(stock.ticker(), ticker);
        assert!(
            listed["pairs"]
                .as_array()
                .unwrap()
                .iter()
                .any(|pair| pair["symbol"] == stock.ticker())
        );
        assert_eq!(stock.instrument_id(), InstrumentId::from(id));
    }

    /// A lower-case ticker, which LSE would match, or a class after a dot, which Nautilus'
    /// Databento client turns into a space, would make a second id of one stock.
    #[rstest]
    #[case::lower_case("nvda.XNAS")]
    #[case::dotted_class("BRK.B.XNAS")]
    #[case::two_spaces("BRK  B.XNAS")]
    #[case::another_venue("NVDA.XNYS")]
    fn an_id_that_is_no_nasdaq_ticker_under_xnas_is_refused_naming_it(#[case] id: &str) {
        let err = NasdaqStock::from_instrument_id(InstrumentId::from(id))
            .unwrap_err()
            .to_string();

        assert!(err.contains(id), "{id}: {err}");
    }

    #[rstest]
    #[case::series("GOLD-FUTURES.LSEDGE", LseProduct::Global)]
    #[case::cme_contract("GCZ26.GLBX", LseProduct::Cme)]
    #[case::nasdaq_stock("NVDA.XNAS", LseProduct::Nasdaq)]
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
