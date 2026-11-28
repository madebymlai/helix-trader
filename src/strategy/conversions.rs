//! Converts an account's equity into the P&L currency of what a strategy trades, at Nautilus'
//! exchange rates for the FX pairs the config lists (`[trade] conversions`, `Conversions`).
//!
//! A trade is sized from a fraction of the account's equity in the P&L currency of its instrument
//! (`orders::Trade::send`). The account's money may be in another currency, such as an IC Markets
//! account in EUR trading gold quoted in USD.
//!
//! The config lists the pairs to convert through, all on one venue: `["EURUSD.ICMARKETS"]` or
//! `["EUR/USD.IDEALPRO"]`. The strategy reads each pair in the first form its venue's history
//! serves, trying the 1-minute BID and ASK bars, then the quotes (`venues::rate_series`,
//! `StandardConfig::data_requirements`). IB serves bars and a cTrader broker serves quotes.
//!
//! Nautilus' data engine caches each quote and bar it processes, including a warmup's. It reads
//! its exchange rates from them, using a pair's quotes before its bars. Before a pair's first
//! quote or bars there is no rate through it, and a trade sends nothing, which is logged
//! (`orders::Trade::send`).
//!
//! Both answers are Nautilus':
//!
//! - `Rates::convert` converts an equity per currency into one currency.
//!   - Each amount but zero uses Nautilus' cached rate on the pairs' venue (`Cache::get_xrate`),
//!     at the mid, as Nautilus' portfolio converts. The converted amounts are summed.
//!   - An account without a base currency, such as OKX's, holds its balance and the open P&L of
//!     its positions each in their own currency.
//!   - `get_xrate(from, to)` gives `to` per `from`.
//! - `reaches` tells whether a currency reaches another through the pairs, as their definitions
//!   give them.
//!   - It uses Nautilus' exchange-rate path search (`nautilus_common::xrate::get_exchange_rate`).
//!   - Each pair has a rate of 1 under the key `BASE/QUOTE`, as the cache keys a pair's quotes.
//!   - A pair without a base currency is refused, as the cache leaves it out.
//!   - Wherever this finds a path, a run's lookup finds a rate once the pairs are quoted.
//!
//! A config whose pairs could never convert is refused at load (`Conversions::validate`).

use std::fmt::Display;

use ahash::AHashMap;
use anyhow::Context;
use nautilus_common::{
    cache::Cache,
    config::{ConfigError, ConfigErrorCollector},
    xrate::get_exchange_rate,
};
use nautilus_model::{
    enums::PriceType,
    identifiers::{InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
    types::{Currency, Money},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use ustr::Ustr;

use super::Series;
use crate::venues;

/// `[trade] conversions`: the FX pairs a strategy converts equity through.
///
/// Each pair is an instrument id. All pairs are on one venue. The list is empty if the config
/// lists none.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Conversions(Vec<InstrumentId>);

impl Conversions {
    /// Returns the conversions through `pairs`.
    #[must_use]
    pub const fn new(pairs: Vec<InstrumentId>) -> Self {
        Self(pairs)
    }

    /// Returns the pairs, in the order of the config.
    #[must_use]
    pub fn pairs(&self) -> &[InstrumentId] {
        &self.0
    }

    /// Returns whether the config lists no pair.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Pushes onto `errors` the violations by the pairs of a strategy trading `traded`.
    ///
    /// The violations are:
    /// - Pairs on more than one venue, which it names. Rates are Nautilus' on one venue (`rates`).
    /// - A pair off the venue of a traded instrument whose broker's account has a base currency
    ///   (`venues::has_base_currency`). Nautilus converts the margins, commissions and P&L of such
    ///   an account only through rates on the traded instrument's venue. Without a base currency
    ///   only the sizer converts, so the venue may be any.
    /// - A pair whose venue's history serves no form to read a rate from, such as OKX's. The error
    ///   names why (`venues::rate_series`).
    pub(super) fn validate(&self, traded: &[InstrumentId], errors: &mut ConfigErrorCollector) {
        let mut on: Vec<Venue> = vec![];
        for pair in &self.0 {
            if !on.contains(&pair.venue) {
                on.push(pair.venue);
            }
        }

        if let [_, _, ..] = on.as_slice() {
            let named: Vec<String> = on.iter().map(ToString::to_string).collect();
            errors.push(ConfigError::invalid_value(
                "trade.conversions",
                format!(
                    "lists pairs on {}: a strategy converts at Nautilus' cached rates on one \
                     venue, its pairs'",
                    named.join(", ")
                ),
            ));
        }

        for (i, pair) in self.0.iter().enumerate() {
            if let Err(e) = venues::rate_series(*pair) {
                errors.push(ConfigError::invalid_value(
                    format!("trade.conversions[{i}]"),
                    format!("{e:#}"),
                ));
            }
            let based_elsewhere = traded.iter().find(|instrument| {
                instrument.venue != pair.venue && venues::has_base_currency(instrument.venue)
            });

            if let Some(instrument) = based_elsewhere {
                errors.push(ConfigError::invalid_value(
                    format!("trade.conversions[{i}]"),
                    format!(
                        "{pair} is not on {venue}, the venue of {instrument}, whose broker's \
                         account has a base currency: Nautilus converts its margins, \
                         commissions and P&L into it only through rates on {venue}",
                        venue = instrument.venue
                    ),
                ));
            }
        }
    }

    /// Returns the series each pair's rates are read from (`venues::rate_series`).
    ///
    /// An IB pair gives its 1-minute BID and ASK bars, and a cTrader pair gives its quotes. A pair
    /// whose venue serves neither gives none, which `validate` refuses. The rules that read the
    /// requirements while the config is checked read them too, and `validate` reports the pair
    /// once.
    pub(super) fn series(&self) -> impl Iterator<Item = Series> + '_ {
        self.0
            .iter()
            .flat_map(|&pair| venues::rate_series(pair).unwrap_or_default())
    }

    /// Returns Nautilus' cached rates on the pairs' venue.
    ///
    /// A loaded config has one venue for all pairs (`validate`). With no pair, there are no rates.
    #[must_use]
    pub fn rates(&self) -> Rates {
        Rates {
            venue: self.0.first().map(|pair| pair.venue),
        }
    }
}

/// Nautilus' cached exchange rates on the venue of the pairs of a config.
///
/// With no pair there are none, and an amount is then either in the wanted currency or not
/// converted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rates {
    venue: Option<Venue>,
}

impl Rates {
    /// Returns `equity`, an amount per currency, converted into `currency` (see the module docs).
    ///
    /// The result is held to Nautilus' native precision (`Money::from_decimal_native`). It is
    /// `None` if an amount but zero has no rate, because no pair reaches its currency or none of
    /// the pairs' quotes or bars is cached yet.
    ///
    /// # Errors
    ///
    /// Returns an error if the sum overflows or is more money than Nautilus holds.
    pub fn convert(
        self,
        cache: &Cache,
        equity: impl IntoIterator<Item = Money>,
        currency: Currency,
    ) -> anyhow::Result<Option<Money>> {
        let mut sum = Decimal::ZERO;

        for amount in equity.into_iter().filter(|amount| !amount.is_zero()) {
            let rate = match self.venue {
                Some(venue) => cache.get_xrate(venue, amount.currency, currency, PriceType::Mid),
                None => (amount.currency == currency).then_some(Decimal::ONE),
            };
            let Some(rate) = rate else {
                return Ok(None);
            };
            sum = amount
                .as_decimal()
                .checked_mul(rate)
                .and_then(|converted| sum.checked_add(converted))
                .with_context(|| format!("converting {amount} into {currency} overflows"))?;
        }
        Money::from_decimal_native(sum, currency)
            .map(Some)
            .map_err(|e| anyhow::anyhow!("{sum} {currency}: {e}"))
    }
}

impl Display for Rates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.venue {
            Some(venue) => write!(
                f,
                "the exchange rates Nautilus caches on {venue}: a currency no pair reaches, or \
                 no quote or bar of the pairs yet"
            ),
            None => f.write_str("no rate: [trade] conversions lists no pair"),
        }
    }
}

/// Returns whether `from` reaches `to` through `pairs`, the definitions of the config's pairs.
///
/// A currency reaches itself through no pair.
///
/// # Errors
///
/// Returns an error if:
/// - A pair in `pairs` has no base currency in its definition, whatever `from` and `to`. The
///   error names the pair. Nautilus' cache leaves such an instrument out of its rates.
/// - `get_exchange_rate` fails, which it does not on rates of 1.
pub fn reaches(pairs: &[InstrumentAny], from: Currency, to: Currency) -> anyhow::Result<bool> {
    let keys = pairs
        .iter()
        .map(|pair| {
            let base = pair.base_currency().with_context(|| {
                format!(
                    "{} has no base currency in its definition: Nautilus' exchange rates read \
                     a pair by its base and quote currencies and leave out an instrument \
                     without a base, so it gives no rate; [trade] conversions lists FX pairs",
                    pair.id()
                )
            })?;
            let key = format!("{}/{}", base.code, pair.quote_currency().code);
            Ok((Ustr::from(&key), Decimal::ONE))
        })
        .collect::<anyhow::Result<AHashMap<Ustr, Decimal>>>()?;
    // Nautilus' search refuses empty quote maps
    if keys.is_empty() {
        return Ok(from == to);
    }
    Ok(get_exchange_rate(from.code, to.code, PriceType::Mid, keys.clone(), keys)?.is_some())
}

#[cfg(test)]
mod tests {
    use nautilus_core::UnixNanos;
    use nautilus_model::{
        data::{Bar, BarSpecification, BarType, QuoteTick},
        enums::{AggregationSource, BarAggregation},
        instruments::stubs::default_fx_ccy,
        types::{Price, Quantity},
    };
    use rstest::rstest;

    use super::*;

    const EURUSD: &str = "EUR/USD.IDEALPRO";
    const USDJPY: &str = "USD/JPY.IDEALPRO";

    fn through(pairs: &[&str]) -> Rates {
        Conversions::new(pairs.iter().map(|&pair| InstrumentId::from(pair)).collect()).rates()
    }

    fn defining(pairs: &[&str]) -> Cache {
        let mut cache = Cache::default();

        for pair in pairs {
            let id = InstrumentId::from(*pair);
            let pair = default_fx_ccy(id.symbol, Some(id.venue));
            cache
                .add_instrument(InstrumentAny::CurrencyPair(pair))
                .unwrap();
        }
        cache
    }

    fn quote(pair: &str, bid: &str, ask: &str) -> QuoteTick {
        let size = Quantity::from(1_000_000);
        QuoteTick::new(
            InstrumentId::from(pair),
            Price::from(bid),
            Price::from(ask),
            size,
            size,
            UnixNanos::default(),
            UnixNanos::default(),
        )
    }

    fn quoted() -> Cache {
        let mut cache = defining(&[EURUSD, USDJPY]);
        cache
            .add_quote(quote(EURUSD, "1.24990", "1.25010"))
            .unwrap();
        cache
            .add_quote(quote(USDJPY, "149.990", "150.010"))
            .unwrap();
        cache
    }

    fn money(amounts: &[&str]) -> Vec<Money> {
        amounts.iter().map(|&amount| Money::from(amount)).collect()
    }

    #[rstest]
    fn an_equity_in_several_currencies_is_the_sum_of_each_converted() {
        let equity = money(&["100000 EUR", "-2000 USD", "150000 JPY"]);

        let converted = through(&[EURUSD, USDJPY])
            .convert(&quoted(), equity, Currency::USD())
            .unwrap();

        assert_eq!(converted, Some(Money::from("124000 USD")));
    }

    #[rstest]
    fn an_amount_of_zero_needs_no_rate() {
        let equity = money(&["100000 EUR", "0 GBP"]);

        let converted = through(&[EURUSD])
            .convert(&quoted(), equity, Currency::USD())
            .unwrap();

        assert_eq!(converted, Some(Money::from("125000 USD")));
    }

    #[rstest]
    #[case::of_a_currency_no_pair_reaches(quoted(), &["100000 EUR", "5 GBP"])]
    #[case::of_a_pair_not_quoted_yet(defining(&[EURUSD]), &["100000 EUR"])]
    fn an_amount_without_a_rate_converts_nothing(#[case] cache: Cache, #[case] equity: &[&str]) {
        let converted = through(&[EURUSD])
            .convert(&cache, money(equity), Currency::USD())
            .unwrap();

        assert_eq!(converted, None);
    }

    fn bar(pair: &str, price_type: PriceType, close: &str) -> Bar {
        let bar_type = BarType::new(
            InstrumentId::from(pair),
            BarSpecification::new(1, BarAggregation::Minute, price_type),
            AggregationSource::External,
        );
        let close = Price::from(close);
        Bar::new(
            bar_type,
            close,
            close,
            close,
            close,
            Quantity::from(0),
            UnixNanos::default(),
            UnixNanos::default(),
        )
    }

    /// Pins that Nautilus' exchange rate reads a pair with no cached quote at the closes of its
    /// latest BID and ASK bars (`Cache::build_quote_table`,
    /// `crates/common/src/cache/mod.rs:7463-7472`).
    ///
    /// `venues::rate_series` reads an IB pair as those bars alone, and `Rates::convert` takes its
    /// rate from `get_xrate`.
    #[rstest]
    fn drift_a_pair_without_quotes_is_read_at_its_bid_and_ask_bars() {
        let mut cache = defining(&[EURUSD]);
        cache
            .add_bar(bar(EURUSD, PriceType::Bid, "1.24990"))
            .unwrap();
        cache
            .add_bar(bar(EURUSD, PriceType::Ask, "1.25010"))
            .unwrap();

        let converted = through(&[EURUSD])
            .convert(&cache, money(&["100000 EUR"]), Currency::USD())
            .unwrap();

        assert_eq!(converted, Some(Money::from("125000 USD")));
    }
}
