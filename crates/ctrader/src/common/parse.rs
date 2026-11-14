//! The broker's symbols as Nautilus instruments, and their identity.
//!
//! A symbol's instrument is `XAUUSD.<venue>`: the symbol name cTrader
//! lists (`ProtoOALightSymbol.symbolName`) on the broker's venue, as
//! configured (`CTraderConnection::venue`). cTrader's `symbolId` differs
//! between servers, so it is looked up by name on each connection, never
//! stored.
//!
//! A symbol whose base and quote assets are both currencies Nautilus knows
//! (forex, and metals such as XAU) becomes a `Cfd`: a broker's cTrader
//! products are over-the-counter contracts for difference, cash-settled with
//! no ownership of the asset, which Nautilus' `Cfd` models
//! (`docs/concepts/instruments/cfd.md`), as its IB adapter defines IB's CFDs
//! (`adapters/interactive_brokers/src/providers/parse.rs`):
//!
//! - the asset class of its underlying, the base asset, by its Nautilus
//!   currency type: FX for a fiat currency, Commodity for a metal,
//!   Cryptocurrency for a crypto;
//! - prices: `digits` decimals, cTrader's integer prices being in
//!   1/100000;
//! - sizes in units of the base asset: cTrader's volumes are in cents of a
//!   unit, so `stepVolume` / 100 is the size increment (and its decimals
//!   the size precision), `minVolume`, `maxVolume` and `lotSize` / 100 the
//!   minimum, maximum and lot (XAUUSD: 100 oz a lot);
//! - margins the broker's own (`margin`): initial and maintenance 1 / the
//!   leverage the broker applies to the symbol on the account, the lower of
//!   the account's
//!   (`ProtoOATrader.leverageInCents`) and the symbol's first dynamic
//!   leverage tier (`ProtoOASymbol.leverageId`), as Nautilus' AX adapter
//!   takes its venue's margin rates (`adapters/architect_ax/src/http/parse.rs`);
//! - stamped at receipt: a symbol carries no time.
//!
//! Anything else (an index, a share) is refused, naming the symbol.

use std::{collections::HashMap, str::FromStr};

use anyhow::{Context, ensure};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::NautilusDataType,
    enums::{AssetClass, CurrencyType},
    identifiers::{InstrumentId, Symbol, Venue},
    instruments::Cfd,
    types::{Currency, Price, Quantity},
};
use rust_decimal::Decimal;

use super::messages::{ProtoOaAsset, ProtoOaDynamicLeverage, ProtoOaLightSymbol, ProtoOaSymbol};
use crate::SERVED_DATA;

/// The cTrader symbol name of `id`, an instrument of the broker: its
/// symbol.
pub(crate) fn symbol_name(id: InstrumentId) -> &'static str {
    id.symbol.inner().as_str()
}

/// The instrument id of the cTrader symbol `name` on `venue`.
pub(crate) fn instrument_id(name: &str, venue: Venue) -> anyhow::Result<InstrumentId> {
    let symbol = Symbol::new_checked(name)
        .with_context(|| format!("the cTrader symbol name {name:?} is no Nautilus symbol"))?;
    Ok(InstrumentId::new(symbol, venue))
}

/// The margin rate of the symbol `name` on the account at the broker of
/// `venue`, initial and maintenance alike: 1 / the leverage the broker
/// applies to it, the lower of the account's, `account_leverage`
/// (`ProtoOATrader.leverageInCents`), and the symbol's dynamic leverage,
/// `symbol_leverage` (its `leverageId`'s), if it has one (cTrader's help,
/// "Dynamic leverage": "the account leverage acts as an upper limit").
/// Both are in cents of a ratio: 3000 is 1:30.
///
/// The symbol's leverage is its first tier's, which covers the smallest
/// positions. A position whose USD value passes that tier gets the next
/// tiers' lower leverage on the excess, so the broker's margin is then
/// above this rate's. cTrader states no maintenance or stop-out rate of a
/// symbol (the stop-out level is the broker's, on the account's margin
/// level), so maintenance is the initial rate.
pub(crate) fn margin(
    name: &str,
    venue: Venue,
    account_leverage: Option<u32>,
    symbol_leverage: Option<&ProtoOaDynamicLeverage>,
) -> anyhow::Result<Decimal> {
    let account = account_leverage
        .filter(|cents| *cents > 0)
        .with_context(|| format!("{name}: {venue} states no leverage of the account"))?;
    let applied = match symbol_leverage {
        None => i64::from(account),
        Some(leverage) => {
            let tier = leverage.tiers.first().with_context(|| {
                format!(
                    "{name}: {venue} leverage {} has no tier",
                    leverage.leverage_id
                )
            })?;
            ensure!(
                tier.leverage > 0,
                "{name}: {venue} leverage {} is not positive",
                leverage.leverage_id
            );
            i64::from(account).min(i64::from(tier.leverage))
        }
    };
    Ok(Decimal::ONE_HUNDRED / Decimal::from(applied))
}

/// The `Cfd` on `venue` of the symbol listed as `light`, with the details
/// `symbol`, at the margin rate `margin` (see `margin`); `assets` name the
/// asset ids.
pub(crate) fn cfd(
    light: &ProtoOaLightSymbol,
    symbol: &ProtoOaSymbol,
    venue: Venue,
    assets: &HashMap<i64, ProtoOaAsset>,
    margin: Decimal,
    ts_init: UnixNanos,
) -> anyhow::Result<Cfd> {
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
                "{name} is not served: its {side} asset {} is no currency Nautilus \
                 knows; only forex and metals are served",
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

    let id = instrument_id(name, venue)?;
    Cfd::builder()
        .instrument_id(id)
        .raw_symbol(id.symbol)
        .asset_class(asset_class(base_currency))
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

/// The asset class of a CFD on `base`, by its currency type.
fn asset_class(base: Currency) -> AssetClass {
    match base.currency_type {
        CurrencyType::Fiat => AssetClass::FX,
        CurrencyType::CommodityBacked => AssetClass::Commodity,
        CurrencyType::Crypto => AssetClass::Cryptocurrency,
    }
}

/// A cTrader volume, in cents of a unit, in units.
fn units(cents: i64) -> Decimal {
    Decimal::new(cents, 2).normalize()
}

/// The names of `assets`, by id.
pub(crate) fn assets_by_id(assets: Vec<ProtoOaAsset>) -> HashMap<i64, ProtoOaAsset> {
    assets.into_iter().map(|a| (a.asset_id, a)).collect()
}

/// Fails unless `id` is on `venue`, an instrument of the broker.
pub(crate) fn ensure_on(id: InstrumentId, venue: Venue) -> anyhow::Result<()> {
    ensure!(id.venue == venue, "{id} is not on {venue}");
    Ok(())
}

/// The instruments of `series`, each the quotes of an instrument on
/// `venue`: those the broker's data client loads to serve them.
///
/// # Errors
///
/// Returns an error naming the first series that is not the quotes of an
/// instrument on `venue`: the adapter serves nothing else (`SERVED_DATA`).
pub fn quoted(
    venue: Venue,
    series: &[(NautilusDataType, String)],
) -> anyhow::Result<Vec<InstrumentId>> {
    series
        .iter()
        .map(|(data_type, identifier)| {
            ensure!(
                *data_type == SERVED_DATA,
                "{venue} serves quotes only, not the {data_type} of {identifier}"
            );
            let id = InstrumentId::from_str(identifier)
                .with_context(|| format!("{identifier} is no instrument id"))?;
            ensure_on(id, venue)?;
            Ok(id)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use nautilus_model::{enums::InstrumentClass, instruments::Instrument};
    use prost::Message as _;

    use super::*;
    use crate::{
        common::messages::{
            ProtoOaAssetListRes, ProtoOaDynamicLeverageTier, ProtoOaExpectedMarginRes,
            ProtoOaGetDynamicLeverageByIdRes, ProtoOaSymbolByIdRes, ProtoOaSymbolsListRes,
            ProtoOaTraderRes,
        },
        testing::{
            ASSET_LIST_RES, DYNAMIC_LEVERAGE_EURUSD_RES, DYNAMIC_LEVERAGE_XAUUSD_RES,
            EXPECTED_MARGIN_EURUSD_RES, SYMBOL_BY_ID_RES, SYMBOLS_LIST_RES, TRADER_RES, VENUE,
        },
    };

    fn venue() -> Venue {
        Venue::new(VENUE)
    }

    /// A series of gold's quotes on the made-up venue.
    fn gold_quotes() -> (NautilusDataType, String) {
        (NautilusDataType::QuoteTick, format!("XAUUSD.{VENUE}"))
    }

    #[test]
    fn the_quotes_of_the_venue_s_instruments_are_those_it_loads() {
        let eurusd = (NautilusDataType::QuoteTick, format!("EURUSD.{VENUE}"));

        let ids = quoted(venue(), &[gold_quotes(), eurusd]).unwrap();

        assert_eq!(
            ids,
            [
                InstrumentId::from(format!("XAUUSD.{VENUE}").as_str()),
                InstrumentId::from(format!("EURUSD.{VENUE}").as_str()),
            ]
        );
    }

    #[test]
    fn a_series_not_of_quotes_is_refused_naming_it() {
        let bars = format!("XAUUSD.{VENUE}-1-MINUTE-BID-EXTERNAL");
        let imbalance = NautilusDataType::Custom {
            type_name: "Imbalance".to_string(),
        };
        for data_type in [
            NautilusDataType::Bar,
            NautilusDataType::OrderBookDelta,
            imbalance,
        ] {
            let named = data_type.to_string();

            let err = quoted(venue(), &[gold_quotes(), (data_type, bars.clone())])
                .unwrap_err()
                .to_string();

            assert!(err.contains("serves quotes only"), "{err}");
            assert!(err.contains(&named) && err.contains(&bars), "{err}");
        }
    }

    #[test]
    fn the_quotes_of_an_instrument_on_another_venue_are_refused_naming_it() {
        let elsewhere = (NautilusDataType::QuoteTick, "EUR/USD.IDEALPRO".to_owned());

        let err = quoted(venue(), &[elsewhere]).unwrap_err().to_string();

        assert!(err.contains("EUR/USD.IDEALPRO"), "{err}");
    }

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

    fn instrument(fixtures: &Fixtures, name: &str) -> anyhow::Result<Cfd> {
        let light = &fixtures.light[name];
        cfd(
            light,
            &fixtures.details[&light.symbol_id],
            venue(),
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
        let margin = margin(
            "XAUUSD",
            venue(),
            account_leverage(),
            Some(&gold_leverage()),
        )
        .unwrap();

        assert_eq!(margin, Decimal::ONE / Decimal::from(30));
    }

    /// Synthetic: gold's tier at 1:20, under the demo account's 1:30.
    #[test]
    fn a_symbol_leverage_below_the_account_s_sets_the_margin() {
        let mut leverage = gold_leverage();
        leverage.tiers[0].leverage = 2_000;

        let margin = margin("XAUUSD", venue(), account_leverage(), Some(&leverage)).unwrap();

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

        let margin = margin("XAUUSD", venue(), Some(50_000), Some(&leverage)).unwrap();

        assert_eq!(margin, d("0.005"));
    }

    /// Synthetic: a symbol without a dynamic leverage.
    #[test]
    fn a_symbol_without_a_leverage_of_its_own_takes_the_account_s() {
        let margin = margin("XAUUSD", venue(), Some(2_000), None).unwrap();

        assert_eq!(margin, d("0.05"));
    }

    /// Synthetic: gold's leverage without tiers, and a trader without
    /// leverage.
    #[test]
    fn a_leverage_missing_is_refused_naming_the_symbol() {
        let mut leverage = gold_leverage();
        leverage.tiers.clear();

        let err = margin("XAUUSD", venue(), account_leverage(), Some(&leverage)).unwrap_err();
        assert_eq!(err.to_string(), "XAUUSD: ACMEFX leverage 52543 has no tier");
        let err = margin("XAUUSD", venue(), None, Some(&gold_leverage())).unwrap_err();
        assert_eq!(
            err.to_string(),
            "XAUUSD: ACMEFX states no leverage of the account"
        );
    }

    /// The broker's own estimate on the demo: 1,000 EUR of EURUSD locks
    /// 33.33 EUR.
    #[test]
    fn the_margin_of_1000_euros_is_the_broker_s_estimate() {
        let leverage = ProtoOaGetDynamicLeverageByIdRes::decode(DYNAMIC_LEVERAGE_EURUSD_RES)
            .unwrap()
            .leverage;
        let estimate = ProtoOaExpectedMarginRes::decode(EXPECTED_MARGIN_EURUSD_RES).unwrap();
        let broker = Decimal::new(
            estimate.margin[0].buy_margin,
            estimate.money_digits.unwrap(),
        );

        let margin = margin("EURUSD", venue(), account_leverage(), Some(&leverage)).unwrap();

        assert_eq!((Decimal::from(1000) * margin).round_dp(2), broker);
    }

    #[test]
    fn xauusd_is_a_commodity_cfd_on_gold_in_ounces_quoted_in_dollars() {
        let xau = instrument(&fixtures(), "XAUUSD").unwrap();

        assert_eq!(xau.id, InstrumentId::from("XAUUSD.ACMEFX"));
        assert_eq!(xau.raw_symbol, Symbol::new("XAUUSD"));
        assert_eq!(xau.instrument_class(), InstrumentClass::Cfd);
        assert_eq!(xau.asset_class, AssetClass::Commodity);
        assert_eq!(xau.base_currency, Some(Currency::from("XAU")));
        assert_eq!(xau.quote_currency, Currency::from("USD"));
        assert_eq!(xau.price_precision, 2);
        assert_eq!(xau.price_increment, Price::from("0.01"));
        assert_eq!(xau.size_precision, 0);
        assert_eq!(xau.size_increment, Quantity::from(1));
        assert_eq!(xau.lot_size, Some(Quantity::from(100)));
        assert_eq!(xau.min_quantity, Some(Quantity::from(1)));
        assert_eq!(xau.max_quantity, Some(Quantity::from(10_000)));
        assert_eq!(xau.multiplier(), Quantity::from(1));
        assert_eq!(xau.margin_init, d("0.05"));
        assert_eq!(xau.margin_maint, d("0.05"));
        assert_eq!(
            (xau.ts_event, xau.ts_init),
            (UnixNanos::from(7), UnixNanos::from(7))
        );
    }

    #[test]
    fn eurusd_is_an_fx_cfd_in_euros_by_the_thousand() {
        let eur = instrument(&fixtures(), "EURUSD").unwrap();

        assert_eq!(eur.id, InstrumentId::from("EURUSD.ACMEFX"));
        assert_eq!(eur.instrument_class(), InstrumentClass::Cfd);
        assert_eq!(eur.asset_class, AssetClass::FX);
        assert_eq!(eur.base_currency, Some(Currency::from("EUR")));
        assert_eq!(eur.quote_currency, Currency::from("USD"));
        assert_eq!(eur.price_precision, 5);
        assert_eq!(eur.price_increment, Price::from("0.00001"));
        assert_eq!(eur.size_precision, 0);
        assert_eq!(eur.size_increment, Quantity::from(1_000));
        assert_eq!(eur.lot_size, Some(Quantity::from(100_000)));
        assert_eq!(eur.min_quantity, Some(Quantity::from(1_000)));
        assert_eq!(eur.max_quantity, Some(Quantity::from(100_000_000)));
        assert_eq!(eur.margin_init, d("0.05"));
        assert_eq!(eur.margin_maint, d("0.05"));
    }

    /// Synthetic: the EURUSD fixture with its base asset renamed BTC.
    #[test]
    fn a_crypto_base_makes_a_cryptocurrency_cfd() {
        let mut fixtures = fixtures();
        let base = fixtures.light["EURUSD"].base_asset_id.unwrap();
        fixtures.assets.get_mut(&base).unwrap().name = "BTC".to_owned();

        let btc = instrument(&fixtures, "EURUSD").unwrap();

        assert_eq!(btc.asset_class, AssetClass::Cryptocurrency);
        assert_eq!(btc.base_currency, Some(Currency::from("BTC")));
    }

    #[test]
    fn an_index_is_refused_naming_its_asset() {
        let err = instrument(&fixtures(), "US500").unwrap_err().to_string();

        assert_eq!(
            err,
            "US500 is not served: its base asset US500 is no currency Nautilus knows; \
             only forex and metals are served"
        );
    }

    /// Synthetic: the XAUUSD fixture mutated.
    #[test]
    fn a_symbol_without_a_step_volume_is_refused() {
        let mut fixtures = fixtures();
        let id = fixtures.light["XAUUSD"].symbol_id;
        fixtures.details.get_mut(&id).unwrap().step_volume = None;

        let err = instrument(&fixtures, "XAUUSD").unwrap_err().to_string();

        assert_eq!(err, "XAUUSD: no positive stepVolume");
    }

    /// Synthetic: the XAUUSD fixture mutated.
    #[test]
    fn a_minimum_finer_than_the_step_is_refused() {
        let mut fixtures = fixtures();
        let id = fixtures.light["XAUUSD"].symbol_id;
        fixtures.details.get_mut(&id).unwrap().min_volume = Some(50);

        let err = instrument(&fixtures, "XAUUSD").unwrap_err().to_string();

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

        let err = instrument(&fixtures, "EURUSD").unwrap_err().to_string();

        assert_eq!(err, "EURUSD: digits 12 out of range");
    }

    /// Synthetic: the EURUSD fixture with an unlisted quote asset.
    #[test]
    fn an_unlisted_asset_is_refused() {
        let mut fixtures = fixtures();
        fixtures.light.get_mut("EURUSD").unwrap().quote_asset_id = Some(999);

        let err = instrument(&fixtures, "EURUSD").unwrap_err().to_string();

        assert_eq!(err, "EURUSD: the quote asset 999 is not listed");
    }

    #[test]
    fn details_of_another_symbol_are_refused() {
        let fixtures = fixtures();
        let eur = &fixtures.light["EURUSD"];
        let xau = &fixtures.details[&fixtures.light["XAUUSD"].symbol_id];

        let err = cfd(
            eur,
            xau,
            venue(),
            &fixtures.assets,
            d("0.05"),
            UnixNanos::default(),
        )
        .unwrap_err()
        .to_string();

        assert!(
            err.starts_with("EURUSD: the details are of symbol"),
            "{err}"
        );
    }

    #[test]
    fn a_symbol_name_and_its_instrument_id_on_the_venue_round_trip() {
        for name in ["XAUUSD", "EURUSD"] {
            let id = instrument_id(name, venue()).unwrap();

            assert_eq!(id, InstrumentId::from(format!("{name}.ACMEFX").as_str()));
            ensure_on(id, venue()).unwrap();
            assert_eq!(symbol_name(id), name);
        }
    }

    #[test]
    fn an_id_on_another_venue_is_refused_naming_the_venue() {
        let err = ensure_on(InstrumentId::from("XAUUSD.ELSEWHERE"), venue()).unwrap_err();

        assert_eq!(err.to_string(), "XAUUSD.ELSEWHERE is not on ACMEFX");
    }
}
