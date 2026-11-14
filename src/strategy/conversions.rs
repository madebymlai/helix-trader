//! Conversions: an account's equity into the P&L currency of what a
//! strategy trades, at Nautilus' exchange rates of the FX pairs its config
//! lists (`[trade] conversions`, `Conversions`).
//!
//! A trade is sized from a fraction of the account's equity in its
//! instrument's P&L currency (`orders::Trade::send`), and an account's
//! money may be in another currency: an IC Markets account in EUR trading
//! gold quoted in USD. The config lists the pairs to convert through, all
//! on one venue, `conversions = ["EURUSD.ICMARKETS"]`, and the strategy
//! reads their quotes (`StandardConfig::data_requirements`): fill-catalog
//! fills them, a backtest replays them, and Nautilus' data engine caches
//! each quote it processes, which its exchange rates are read from.
//!
//! Both answers are Nautilus':
//!
//! - `Rates::convert`: an equity per currency into one currency, each
//!   amount at Nautilus' cached rate on the pairs' venue
//!   (`Cache::get_xrate`, at the mid, as Nautilus' portfolio converts),
//!   the amounts summed. `get_xrate(from, to)` gives `to` per `from`: an
//!   amount in `from` times it is in `to`.
//! - `reaches`: whether a currency reaches another through the pairs as
//!   their definitions give them, by Nautilus' exchange-rate path search
//!   (`nautilus_common::xrate::get_exchange_rate`), each pair at a rate of
//!   1 under the key `BASE/QUOTE` of its base and quote currencies, as the
//!   cache keys a pair's quotes for it, a pair without a base currency
//!   refused, as the cache leaves it out: wherever this finds a path, a
//!   run's lookup finds a rate once the pairs are quoted.
//!
//! A config whose pairs could never convert is refused at load
//! (`Conversions::validate`): pairs on more than one venue, or off the
//! venue of an instrument traded whose broker's account has a base
//! currency.

use std::fmt;

use ahash::AHashMap;
use anyhow::Context;
use nautilus_common::{
    cache::Cache,
    config::{ConfigError, ConfigErrorCollector},
    xrate::get_exchange_rate,
};
use nautilus_model::{
    data::NautilusDataType,
    enums::PriceType,
    identifiers::{InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
    types::{Currency, Money},
};
use rust_decimal::Decimal;
use serde::Deserialize;
use ustr::Ustr;

use super::TickSeries;
use crate::venues;

/// `[trade] conversions`: the FX pairs a strategy converts equity through,
/// each by its instrument id, `["EURUSD.ICMARKETS"]`, all on one venue;
/// none if the config lists none.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
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

    /// Pushes onto `errors` the violations of the pairs of a strategy
    /// trading `traded`: pairs on more than one venue, under
    /// `trade.conversions`, naming the venues, since its rates are Nautilus'
    /// on one venue (`rates`); and each pair off the venue of an instrument
    /// of `traded` whose broker's account has a base currency
    /// (`venues::has_base_currency`), under its field, naming the pair and
    /// the instrument, since Nautilus converts such an account's margins,
    /// commissions and P&L into its base currency only through rates on the
    /// traded instrument's venue. For an account without one only the
    /// sizer converts, at the pairs' rates, so their venue may be any.
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

    /// The quotes of each pair, the series its rates are read from: the
    /// data a cTrader broker serves, history and live.
    pub(super) fn quotes(&self) -> impl Iterator<Item = TickSeries> + '_ {
        self.0.iter().map(|&instrument_id| TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id,
        })
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
                 no quote of the pairs yet"
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
/// Returns an error naming a pair of `pairs` whose definition has no base
/// currency, whatever `from` and `to`: Nautilus' cache leaves such an
/// instrument out of its rates, so it would give none. Otherwise as
/// `get_exchange_rate` does, which it does not on rates of 1.
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
