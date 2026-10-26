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
//! - margins IC's own (`margin`): initial and maintenance 1 / the leverage
//!   IC applies to the symbol on the account, the lower of the account's
//!   (`ProtoOATrader.leverageInCents`) and the symbol's first dynamic
//!   leverage tier (`ProtoOASymbol.leverageId`), as Nautilus' AX adapter
//!   takes its venue's margin rates (`adapters/architect_ax/src/http/parse.rs`);
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

use super::messages::{ProtoOaAsset, ProtoOaDynamicLeverage, ProtoOaLightSymbol, ProtoOaSymbol};

/// The venue of IC's instruments.
pub(crate) const IC_VENUE: &str = "ICMARKETS";

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

/// The margin rate of the symbol `name` on the account, initial and
/// maintenance alike: 1 / the leverage IC applies to it, the lower of the
/// account's, `account_leverage` (`ProtoOATrader.leverageInCents`), and
/// the symbol's dynamic leverage, `symbol_leverage` (its `leverageId`'s),
/// if it has one (cTrader's help, "Dynamic leverage": "the account leverage
/// acts as an upper limit"). Both are in cents of a ratio: 3000 is 1:30.
///
/// The symbol's leverage is its first tier's, which covers the smallest
/// positions. A position whose USD value passes that tier gets the next
/// tiers' lower leverage on the excess at IC, so IC's margin is then above
/// this rate's; IC's demo symbols have one tier, which covers every size.
/// cTrader states no maintenance or stop-out rate of a symbol (the stop-out
/// level is the broker's, on the account's margin level), so maintenance
/// is the initial rate.
pub(super) fn margin(
    name: &str,
    account_leverage: Option<u32>,
    symbol_leverage: Option<&ProtoOaDynamicLeverage>,
) -> anyhow::Result<Decimal> {
    let account = account_leverage
        .filter(|cents| *cents > 0)
        .with_context(|| format!("{name}: IC states no leverage of the account"))?;
    let applied = match symbol_leverage {
        None => i64::from(account),
        Some(leverage) => {
            let tier = leverage.tiers.first().with_context(|| {
                format!("{name}: IC's leverage {} has no tier", leverage.leverage_id)
            })?;
            ensure!(
                tier.leverage > 0,
                "{name}: IC's leverage {} is not positive",
                leverage.leverage_id
            );
            i64::from(account).min(i64::from(tier.leverage))
        }
    };
    Ok(Decimal::ONE_HUNDRED / Decimal::from(applied))
}

/// The `CurrencyPair` of the symbol listed as `light`, with the details
/// `symbol`, at the margin rate `margin` (see `margin`); `assets` name the
/// asset ids.
pub(super) fn currency_pair(
    light: &ProtoOaLightSymbol,
    symbol: &ProtoOaSymbol,
    assets: &HashMap<i64, ProtoOaAsset>,
    margin: Decimal,
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
        .margin_init(margin)
        .margin_maint(margin)
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
        messages::{
            ProtoOaAssetListRes, ProtoOaDynamicLeverageTier, ProtoOaExpectedMarginRes,
            ProtoOaGetDynamicLeverageByIdRes, ProtoOaSymbolByIdRes, ProtoOaSymbolsListRes,
            ProtoOaTraderRes,
        },
        testing::{
            ASSET_LIST_RES, DYNAMIC_LEVERAGE_EURUSD_RES, DYNAMIC_LEVERAGE_XAUUSD_RES,
            EXPECTED_MARGIN_EURUSD_RES, SYMBOL_BY_ID_RES, SYMBOLS_LIST_RES, TRADER_RES,
        },
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
            d("0.05"),
            UnixNanos::from(7),
        )
    }

    fn d(value: &str) -> Decimal {
        value.parse().unwrap()
    }

    /// The demo account's leverage, in cents: 1:30.
    fn account_leverage() -> Option<u32> {
        ProtoOaTraderRes::decode(TRADER_RES)
            .unwrap()
            .trader
            .leverage_in_cents
    }

    fn gold_leverage() -> ProtoOaDynamicLeverage {
        ProtoOaGetDynamicLeverageByIdRes::decode(DYNAMIC_LEVERAGE_XAUUSD_RES)
            .unwrap()
            .leverage
    }

    #[test]
    fn gold_on_the_demo_takes_the_account_s_1_30_under_its_own_1_200() {
        let margin = margin("XAUUSD", account_leverage(), Some(&gold_leverage())).unwrap();

        assert_eq!(margin, Decimal::ONE / Decimal::from(30));
    }

    /// Synthetic: gold's tier at 1:20, under the demo account's 1:30.
    #[test]
    fn a_symbol_leverage_below_the_account_s_sets_the_margin() {
        let mut leverage = gold_leverage();
        leverage.tiers[0].leverage = 2_000;

        let margin = margin("XAUUSD", account_leverage(), Some(&leverage)).unwrap();

        assert_eq!(margin, d("0.05"));
    }

    /// Synthetic: gold's leverage with a second tier, 1:20 above 1,000,000 USD.
    #[test]
    fn the_first_tier_applies_whatever_the_tiers_above_it() {
        let mut leverage = gold_leverage();
        leverage.tiers.push(ProtoOaDynamicLeverageTier {
            volume: 1_000_000_000,
            leverage: 2_000,
        });

        let margin = margin("XAUUSD", Some(50_000), Some(&leverage)).unwrap();

        assert_eq!(margin, d("0.005"));
    }

    /// Synthetic: a symbol without a dynamic leverage.
    #[test]
    fn a_symbol_without_a_leverage_of_its_own_takes_the_account_s() {
        let margin = margin("XAUUSD", Some(2_000), None).unwrap();

        assert_eq!(margin, d("0.05"));
    }

    /// Synthetic: gold's leverage without tiers, and a trader without
    /// leverage.
    #[test]
    fn a_leverage_missing_is_refused_naming_the_symbol() {
        let mut leverage = gold_leverage();
        leverage.tiers.clear();

        let err = margin("XAUUSD", account_leverage(), Some(&leverage)).unwrap_err();
        assert!(err.to_string().contains("XAUUSD"), "{err}");
        let err = margin("XAUUSD", None, Some(&gold_leverage())).unwrap_err();
        assert!(err.to_string().contains("XAUUSD"), "{err}");
    }

    /// IC's own estimate on the demo: 1,000 EUR of EURUSD locks 33.33 EUR.
    #[test]
    fn the_margin_of_1000_euros_is_ic_s_estimate() {
        let leverage = ProtoOaGetDynamicLeverageByIdRes::decode(DYNAMIC_LEVERAGE_EURUSD_RES)
            .unwrap()
            .leverage;
        let estimate = ProtoOaExpectedMarginRes::decode(EXPECTED_MARGIN_EURUSD_RES).unwrap();
        let ic = Decimal::new(
            estimate.margin[0].buy_margin,
            estimate.money_digits.unwrap(),
        );

        let margin = margin("EURUSD", account_leverage(), Some(&leverage)).unwrap();

        assert_eq!((Decimal::from(1000) * margin).round_dp(2), ic);
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
        assert_eq!(xau.margin_init, d("0.05"));
        assert_eq!(xau.margin_maint, d("0.05"));
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

        let err = currency_pair(eur, xau, &fixtures.assets, d("0.05"), UnixNanos::default())
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
