//! Conversions: an account's equity into the P&L currency of what a
//! strategy trades, at Nautilus' exchange rates of the FX pairs its config
//! lists (`[trade] conversions`, `Conversions`).
//!
//! A trade is sized from a fraction of the account's equity in its
//! instrument's P&L currency (`orders::Trade::send`), and an account's
//! money may be in another currency: an IC Markets account in EUR trading
//! gold quoted in USD. The config lists the pairs to convert through, all
//! on one venue, `conversions = ["EURUSD.ICMARKETS"]` or
//! `["EUR/USD.IDEALPRO"]`, and the strategy reads each in the first form
//! its venue's history serves of its 1-minute BID and ASK bars, then its
//! quotes (`venues::rate_series`, `StandardConfig::data_requirements`),
//! IB's bars, a cTrader broker's quotes: fill-catalog fills them, a
//! backtest replays them, a live strategy warms up on them and subscribes
//! to them from its start (`strategy::subscriptions`), a cTrader pair's
//! quotes as any tick series, an IB pair's bars as any bar type, and Nautilus'
//! data engine caches each quote and bar it processes, a warmup's among
//! them, which its exchange rates are read from, a pair's quotes before its
//! bars. Before a pair's first quote or bars
//! there is no rate through it, and a trade sends nothing, logged
//! (`orders::Trade::send`).
//!
//! Both answers are Nautilus':
//!
//! - `Rates::convert`: an equity per currency into one currency, each
//!   amount but zero at Nautilus' cached rate on the pairs' venue
//!   (`Cache::get_xrate`, at the mid, as Nautilus' portfolio converts),
//!   the amounts summed: an account without a base currency, OKX's, holds
//!   its balance and its positions' open P&L each in its own currency.
//!   `get_xrate(from, to)` gives `to` per `from`: an amount in `from` times
//!   it is in `to`.
//! - `reaches`: whether a currency reaches another through the pairs as
//!   their definitions give them, by Nautilus' exchange-rate path search
//!   (`nautilus_common::xrate::get_exchange_rate`), each pair at a rate of
//!   1 under the key `BASE/QUOTE` of its base and quote currencies, as the
//!   cache keys a pair's quotes for it, a pair without a base currency
//!   refused, as the cache leaves it out: wherever this finds a path, a
//!   run's lookup finds a rate once the pairs are quoted.
//!
//! A config whose pairs could never convert is refused at load
//! (`Conversions::validate`): a pair whose venue's history serves no form
//! of it a rate is read from, an OKX pair, pairs on more than one venue, or
//! a pair off the venue of an instrument traded whose broker's account has
//! a base currency.

use std::fmt;

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

/// `[trade] conversions`: the FX pairs a strategy converts equity through,
/// each by its instrument id, `["EURUSD.ICMARKETS"]`, all on one venue;
/// none if the config lists none.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Conversions(Vec<InstrumentId>);

impl Conversions {
    /// The conversions through `pairs`, as a config listing them reads.
    #[must_use]
    pub const fn new(pairs: Vec<InstrumentId>) -> Self {
        Self(pairs)
    }

    /// The pairs, in the config's order.
    #[must_use]
    pub fn pairs(&self) -> &[InstrumentId] {
        &self.0
    }

    /// Whether the config lists no pair.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Pushes onto `errors` the violations of the pairs of a strategy
    /// trading `traded`: pairs on more than one venue, under
    /// `trade.conversions`, naming the venues, since its rates are Nautilus'
    /// on one venue (`rates`); and each pair off the venue of an instrument
    /// of `traded` whose broker's account has a base currency
    /// (`venues::has_base_currency`), under its field, naming the pair and
    /// the instrument, since Nautilus converts such an account's margins,
    /// commissions and P&L into its base currency only through rates on the
    /// traded instrument's venue. For an account without one only the
    /// sizer converts, at the pairs' rates, so their venue may be any. And
    /// each pair whose venue's history serves no form a rate is read from,
    /// under its field, naming the pair, its venue and why
    /// (`venues::rate_series`), OKX's.
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

    /// The series each pair's rates are read from, in order: the form its
    /// venue's history serves (`venues::rate_series`), an IB pair's
    /// 1-minute BID and ASK bars, a cTrader pair's quotes; none of a pair
    /// whose venue serves neither form, which `validate` refuses, so that
    /// the rules reading the requirements the pairs are in while the config
    /// is checked read them and `validate` reports the pair once.
    pub(super) fn series(&self) -> impl Iterator<Item = Series> + '_ {
        self.0
            .iter()
            .flat_map(|&pair| venues::rate_series(pair).unwrap_or_default())
    }

    /// The rates an equity is converted at: Nautilus' cached rates on the
    /// pairs' venue, one for all of them in a config loaded (`validate`);
    /// with no pair, none.
    #[must_use]
    pub fn rates(&self) -> Rates {
        Rates {
            venue: self.0.first().map(|pair| pair.venue),
        }
    }
}

/// Nautilus' cached exchange rates on the venue of a config's pairs
/// (`Conversions::rates`), which a strategy converts equity at; with no
/// pair, none: an amount is then in the currency wanted or not converted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rates {
    venue: Option<Venue>,
}

impl Rates {
    /// `equity`, an amount per currency, in `currency`: each amount but
    /// zero at Nautilus' cached rate into `currency` on the pairs' venue
    /// (`Cache::get_xrate`, at the mid), the amounts summed, held to
    /// Nautilus' native precision as it holds a converted amount
    /// (`Money::from_decimal_native`). None if an amount but zero has no
    /// rate into `currency`: no pair reaches its currency, or none of the
    /// pairs' quotes or bars is cached yet.
    ///
    /// # Errors
    ///
    /// Returns an error if the sum overflows or is more money than
    /// Nautilus holds.
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

/// Where the rates come from, and why one may be missing.
impl fmt::Display for Rates {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
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

/// Whether `from` reaches `to` through `pairs`, the definitions of a
/// config's pairs: by Nautilus' exchange-rate path search
/// (`get_exchange_rate`), each pair at a rate of 1 under its `BASE/QUOTE`
/// key, as Nautilus' cache keys its quotes; a currency reaches itself
/// through no pair.
///
/// # Errors
///
/// Returns an error if:
/// - A pair of `pairs`, which it names, has no base currency in its
///   definition, whatever `from` and `to`: Nautilus' cache leaves such an
///   instrument out of its rates, so it would give none.
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
    // Nautilus' search refuses empty quote maps.
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

    /// IB's euro in US dollars.
    const EURUSD: &str = "EUR/USD.IDEALPRO";
    /// IB's US dollar in yen.
    const USDJPY: &str = "USD/JPY.IDEALPRO";

    /// The rates of a config converting through `pairs`, all on IDEALPRO.
    fn through(pairs: &[&str]) -> Rates {
        Conversions::new(pairs.iter().map(|&pair| InstrumentId::from(pair)).collect()).rates()
    }

    /// A cache defining the pairs `pairs`, Nautilus' FX pairs, none quoted.
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

    /// `pair` quoted at `bid` and `ask`.
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

    /// A cache of EUR/USD quoted 1.24990 / 1.25010, its mid 1.25 USD per
    /// EUR, and USD/JPY quoted 149.990 / 150.010, its mid 150 JPY per USD.
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

    /// An account without a base currency holds each currency apart: 100,000
    /// EUR are 125,000 USD, a loss of 2,000 USD is itself, 150,000 JPY are
    /// 1,000 USD. Their sum, 124,000 USD, is the equity in USD; any one
    /// left out, or taken alone, gives another.
    #[rstest]
    fn an_equity_in_several_currencies_is_the_sum_of_each_converted() {
        let equity = money(&["100000 EUR", "-2000 USD", "150000 JPY"]);

        let converted = through(&[EURUSD, USDJPY])
            .convert(&quoted(), equity, Currency::USD())
            .unwrap();

        assert_eq!(converted, Some(Money::from("124000 USD")));
    }

    /// No pair reaches GBP, but none of it is held: the equity is the EUR's
    /// alone, converted.
    #[rstest]
    fn an_amount_of_zero_needs_no_rate() {
        let equity = money(&["100000 EUR", "0 GBP"]);

        let converted = through(&[EURUSD])
            .convert(&quoted(), equity, Currency::USD())
            .unwrap();

        assert_eq!(converted, Some(Money::from("125000 USD")));
    }

    /// No pair reaches GBP, and 5 GBP are held: the equity is not
    /// converted, rather than converted without them.
    #[rstest]
    fn an_amount_no_pair_reaches_converts_nothing() {
        let equity = money(&["100000 EUR", "5 GBP"]);

        let converted = through(&[EURUSD])
            .convert(&quoted(), equity, Currency::USD())
            .unwrap();

        assert_eq!(converted, None);
    }

    /// An amount of a currency with a pair but no rate cached yet converts
    /// nothing either.
    #[rstest]
    fn an_amount_of_a_pair_not_quoted_yet_converts_nothing() {
        let equity = money(&["100000 EUR"]);

        let converted = through(&[EURUSD])
            .convert(&defining(&[EURUSD]), equity, Currency::USD())
            .unwrap();

        assert_eq!(converted, None);
    }

    /// The 1-minute `price_type` bar of `pair` closing at `close`, flat.
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

    /// IB serves an FX pair's history as BID and ASK bars, not quotes:
    /// Nautilus reads a pair no quote of which is cached at the closes of
    /// its BID and ASK bars, 1.24990 and 1.25010, the mid 1.25 USD per EUR.
    #[rstest]
    fn a_pair_without_quotes_is_read_at_its_bid_and_ask_bars() {
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
