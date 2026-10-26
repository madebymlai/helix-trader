//! IC's symbols as Nautilus instruments, and their identity.
//!
//! An IC symbol is `XAUUSD.ICMARKETS`: the symbol name cTrader lists
//! (`ProtoOALightSymbol.symbolName`) on venue `ICMARKETS`. cTrader's
//! `symbolId` differs between servers, so it is looked up by name on each
//! connection, never stored.
//!
//! A symbol whose base and quote assets are both currencies Nautilus knows
//! (forex, and metals such as XAU) becomes a `CurrencyPair`:
//!
//! - prices: `digits` decimals, cTrader's integer prices being in
//!   1/100000;
//! - sizes in units of the base asset: cTrader's volumes are in cents of a
//!   unit, so `stepVolume` / 100 is the size increment (and its decimals
//!   the size precision), `minVolume`, `maxVolume` and `lotSize` / 100 the
//!   minimum, maximum and lot (XAUUSD: 100 oz a lot);
//! - margins of 1, as OKX's: a position locks its value divided by the
//!   backtest's leverage (backtest README, "Leverage");
//! - stamped at receipt: a symbol carries no time.
//!
//! Anything else (an index, a share) is refused, naming the symbol.

use std::collections::HashMap;

use anyhow::{Context, bail, ensure};
use nautilus_core::UnixNanos;
use nautilus_model::{
    identifiers::{InstrumentId, Symbol, Venue},
    instruments::CurrencyPair,
    types::{Currency, Price, Quantity},
};
use rust_decimal::Decimal;

use super::messages::{ProtoOaAsset, ProtoOaLightSymbol, ProtoOaSymbol};

/// The venue of IC's instruments.
pub(super) const IC_VENUE: &str = "ICMARKETS";

/// Whether `id` is on IC's venue.
pub(super) fn is_ic(id: InstrumentId) -> bool {
    id.venue == Venue::new(IC_VENUE)
}

/// The cTrader symbol name of `id`, an IC instrument: its symbol.
pub(super) fn symbol_name(id: InstrumentId) -> &'static str {
    id.symbol.inner().as_str()
}

/// The instrument id of the cTrader symbol `name`.
pub(super) fn instrument_id(name: &str) -> anyhow::Result<InstrumentId> {
    let symbol = Symbol::new_checked(name)
        .with_context(|| format!("the cTrader symbol name {name:?} is no Nautilus symbol"))?;
    Ok(InstrumentId::new(symbol, Venue::new(IC_VENUE)))
}

/// The `CurrencyPair` of the symbol listed as `light`, with the details
/// `symbol`; `assets` name the asset ids.
pub(super) fn currency_pair(
    light: &ProtoOaLightSymbol,
    symbol: &ProtoOaSymbol,
    assets: &HashMap<i64, ProtoOaAsset>,
    ts_init: UnixNanos,
) -> anyhow::Result<CurrencyPair> {
    let name = light
        .symbol_name
        .as_deref()
        .with_context(|| format!("cTrader symbol {} has no name", light.symbol_id))?;
    ensure!(
        light.symbol_id == symbol.symbol_id,
        "{name}: the details are of symbol {}, not {}",
        symbol.symbol_id,
        light.symbol_id
    );
    let currency = |asset_id: Option<i64>, side: &str| -> anyhow::Result<Currency> {
        let asset_id = asset_id.with_context(|| format!("{name} has no {side} asset"))?;
        let asset = assets
            .get(&asset_id)
            .with_context(|| format!("{name}: the {side} asset {asset_id} is not listed"))?;
        asset.name.parse().map_err(|_| {
            anyhow::anyhow!(
                "{name} is not a currency pair: its {side} asset {} is no currency \
                 Nautilus knows; only forex and metals are served",
                asset.name
            )
        })
    };
    let base_currency = currency(light.base_asset_id, "base")?;
    let quote_currency = currency(light.quote_asset_id, "quote")?;

    let price_precision = u8::try_from(symbol.digits)
        .ok()
        .filter(|digits| *digits <= 9)
        .with_context(|| format!("{name}: digits {} out of range", symbol.digits))?;
    let price_increment =
        Price::from_decimal_dp(Decimal::new(1, u32::from(price_precision)), price_precision)
            .with_context(|| format!("{name}: price increment"))?;

    let step = symbol
        .step_volume
        .filter(|cents| *cents > 0)
        .with_context(|| format!("{name}: no positive stepVolume"))?;
    let size_increment_units = units(step);
    let size_precision = u8::try_from(size_increment_units.scale())?;
    let quantity = |cents: Option<i64>, field: &str| -> anyhow::Result<Option<Quantity>> {
        let Some(cents) = cents else {
            return Ok(None);
        };
        ensure!(cents > 0, "{name}: {field} {cents} is not positive");
        let value = units(cents);
        ensure!(
            value.scale() <= u32::from(size_precision),
            "{name}: {field} of {value} units is finer than the step of {size_increment_units}"
        );
        Quantity::from_decimal_dp(value, size_precision)
            .map(Some)
            .with_context(|| format!("{name}: {field}"))
    };
    let size_increment =
        quantity(Some(step), "stepVolume")?.with_context(|| format!("{name}: no stepVolume"))?;

    let id = instrument_id(name)?;
    CurrencyPair::builder()
        .instrument_id(id)
        .raw_symbol(id.symbol)
        .base_currency(base_currency)
        .quote_currency(quote_currency)
        .price_precision(price_precision)
        .size_precision(size_precision)
        .price_increment(price_increment)
        .size_increment(size_increment)
        .maybe_lot_size(quantity(symbol.lot_size, "lotSize")?)
        .maybe_min_quantity(quantity(symbol.min_volume, "minVolume")?)
        .maybe_max_quantity(quantity(symbol.max_volume, "maxVolume")?)
        .margin_init(Decimal::ONE)
        .margin_maint(Decimal::ONE)
        .ts_event(ts_init)
        .ts_init(ts_init)
        .build()
        .with_context(|| format!("building the instrument of {name}"))
}

/// A cTrader volume, in cents of a unit, in units.
fn units(cents: i64) -> Decimal {
    Decimal::new(cents, 2).normalize()
}

/// The names of `assets`, by id.
pub(super) fn assets_by_id(assets: Vec<ProtoOaAsset>) -> HashMap<i64, ProtoOaAsset> {
    assets.into_iter().map(|a| (a.asset_id, a)).collect()
}

/// Fails unless `id` is an IC instrument.
pub(super) fn ensure_ic(id: InstrumentId) -> anyhow::Result<()> {
    if !is_ic(id) {
        bail!("{id} is not on {IC_VENUE}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use prost::Message as _;

    use super::*;
    use crate::data::ic::{
        messages::{ProtoOaAssetListRes, ProtoOaSymbolByIdRes, ProtoOaSymbolsListRes},
        testing::{ASSET_LIST_RES, SYMBOL_BY_ID_RES, SYMBOLS_LIST_RES},
    };

    /// The captured symbols, assets and details (see `testdata/`).
    struct Fixtures {
        light: HashMap<String, ProtoOaLightSymbol>,
        details: HashMap<i64, ProtoOaSymbol>,
        assets: HashMap<i64, ProtoOaAsset>,
    }

    fn fixtures() -> Fixtures {
        let list = ProtoOaSymbolsListRes::decode(SYMBOLS_LIST_RES).unwrap();
        let details = ProtoOaSymbolByIdRes::decode(SYMBOL_BY_ID_RES).unwrap();
        let assets = ProtoOaAssetListRes::decode(ASSET_LIST_RES).unwrap();
        Fixtures {
            light: list
                .symbol
                .into_iter()
                .map(|s| (s.symbol_name.clone().unwrap(), s))
                .collect(),
            details: details
                .symbol
                .into_iter()
                .map(|s| (s.symbol_id, s))
                .collect(),
            assets: assets_by_id(assets.asset),
        }
    }

    fn pair(fixtures: &Fixtures, name: &str) -> anyhow::Result<CurrencyPair> {
        let light = &fixtures.light[name];
        currency_pair(
            light,
            &fixtures.details[&light.symbol_id],
            &fixtures.assets,
            UnixNanos::from(7),
        )
    }

    #[test]
    fn xauusd_is_gold_in_ounces_quoted_in_dollars() {
        let xau = pair(&fixtures(), "XAUUSD").unwrap();

        assert_eq!(xau.id, InstrumentId::from("XAUUSD.ICMARKETS"));
        assert_eq!(xau.raw_symbol, Symbol::new("XAUUSD"));
        assert_eq!(xau.base_currency, Currency::from("XAU"));
        assert_eq!(xau.quote_currency, Currency::from("USD"));
        assert_eq!(xau.price_precision, 2);
        assert_eq!(xau.price_increment, Price::from("0.01"));
        assert_eq!(xau.size_precision, 0);
        assert_eq!(xau.size_increment, Quantity::from(1));
        assert_eq!(xau.lot_size, Some(Quantity::from(100)));
        assert_eq!(xau.min_quantity, Some(Quantity::from(1)));
        assert_eq!(xau.max_quantity, Some(Quantity::from(10_000)));
        assert_eq!(xau.multiplier, Quantity::from(1));
        assert_eq!(xau.margin_init, Decimal::ONE);
        assert_eq!(xau.margin_maint, Decimal::ONE);
        assert_eq!(
            (xau.ts_event, xau.ts_init),
            (UnixNanos::from(7), UnixNanos::from(7))
        );
    }

    #[test]
    fn eurusd_is_in_euros_by_the_thousand() {
        let eur = pair(&fixtures(), "EURUSD").unwrap();

        assert_eq!(eur.id, InstrumentId::from("EURUSD.ICMARKETS"));
        assert_eq!(eur.base_currency, Currency::from("EUR"));
        assert_eq!(eur.quote_currency, Currency::from("USD"));
        assert_eq!(eur.price_precision, 5);
        assert_eq!(eur.price_increment, Price::from("0.00001"));
        assert_eq!(eur.size_precision, 0);
        assert_eq!(eur.size_increment, Quantity::from(1_000));
        assert_eq!(eur.lot_size, Some(Quantity::from(100_000)));
        assert_eq!(eur.min_quantity, Some(Quantity::from(1_000)));
        assert_eq!(eur.max_quantity, Some(Quantity::from(100_000_000)));
    }

    #[test]
    fn an_index_is_not_a_currency_pair() {
        let err = pair(&fixtures(), "US500").unwrap_err().to_string();

        assert_eq!(
            err,
            "US500 is not a currency pair: its base asset US500 is no currency Nautilus \
             knows; only forex and metals are served"
        );
    }

    /// Synthetic: the XAUUSD fixture mutated.
    #[test]
    fn a_symbol_without_a_step_volume_is_refused() {
        let mut fixtures = fixtures();
        let id = fixtures.light["XAUUSD"].symbol_id;
        fixtures.details.get_mut(&id).unwrap().step_volume = None;

        let err = pair(&fixtures, "XAUUSD").unwrap_err().to_string();

        assert_eq!(err, "XAUUSD: no positive stepVolume");
    }

    /// Synthetic: the XAUUSD fixture mutated.
    #[test]
    fn a_minimum_finer_than_the_step_is_refused() {
        let mut fixtures = fixtures();
        let id = fixtures.light["XAUUSD"].symbol_id;
        fixtures.details.get_mut(&id).unwrap().min_volume = Some(50);

        let err = pair(&fixtures, "XAUUSD").unwrap_err().to_string();

        assert_eq!(
            err,
            "XAUUSD: minVolume of 0.5 units is finer than the step of 1"
        );
    }

    /// Synthetic: the EURUSD fixture mutated.
    #[test]
    fn digits_out_of_range_are_refused() {
        let mut fixtures = fixtures();
        let id = fixtures.light["EURUSD"].symbol_id;
        fixtures.details.get_mut(&id).unwrap().digits = 12;

        let err = pair(&fixtures, "EURUSD").unwrap_err().to_string();

        assert_eq!(err, "EURUSD: digits 12 out of range");
    }

    /// Synthetic: the EURUSD fixture with an unlisted quote asset.
    #[test]
    fn an_unlisted_asset_is_refused() {
        let mut fixtures = fixtures();
        fixtures.light.get_mut("EURUSD").unwrap().quote_asset_id = Some(999);

        let err = pair(&fixtures, "EURUSD").unwrap_err().to_string();

        assert_eq!(err, "EURUSD: the quote asset 999 is not listed");
    }

    #[test]
    fn details_of_another_symbol_are_refused() {
        let fixtures = fixtures();
        let eur = &fixtures.light["EURUSD"];
        let xau = &fixtures.details[&fixtures.light["XAUUSD"].symbol_id];

        let err = currency_pair(eur, xau, &fixtures.assets, UnixNanos::default())
            .unwrap_err()
            .to_string();

        assert!(
            err.starts_with("EURUSD: the details are of symbol"),
            "{err}"
        );
    }

    #[test]
    fn a_symbol_name_and_its_instrument_id_round_trip() {
        for name in ["XAUUSD", "EURUSD"] {
            let id = instrument_id(name).unwrap();

            assert!(is_ic(id));
            assert_eq!(symbol_name(id), name);
        }
        assert!(!is_ic(InstrumentId::from("EURUSD.IDEALPRO")));
        assert_eq!(
            ensure_ic(InstrumentId::from("EUR/USD.IDEALPRO"))
                .unwrap_err()
                .to_string(),
            "EUR/USD.IDEALPRO is not on ICMARKETS"
        );
    }
}
