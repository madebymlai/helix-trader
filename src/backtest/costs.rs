//! Trading costs of the simulated venue.
//!
//! The commission of a run (`Commission`) is the one source of its costs: it
//! builds the Nautilus `FeeModelAny` (`nautilus_execution`) that
//! `BacktestVenueConfig` takes, which charges the fills of a backtest and of a
//! dataset run alike, and it gives what a fill costs as that fee model charges
//! it (`Commission::cost`), the cost a model strategy's target is handed
//! (`model::targets::TargetSetter`). A commission is fixed per order
//! (`FixedFeeModel`), a rate on the trade value (`MakerTakerFeeModel`), or an
//! amount per unit of quantity (`PerContractFeeModel`).
//! The venue sets no fill model, so Nautilus fills on its default
//! (`DefaultFillModel`), at the market with no slippage of its own: at the
//! quote, so the spread is the cost, or, on bars, at the bar's prices.

use anyhow::{Context, ensure};
use nautilus_execution::models::fee::{
    FeeModelAny, FixedFeeModel, MakerTakerFeeModel, PerContractFeeModel,
};
use nautilus_model::{
    fees::calculate_maker_taker_commission,
    instruments::{Instrument, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use rust_decimal::Decimal;

/// The commission of a run, as its command line gives it, which every cost
/// of the run reads: the fee model of its simulated venues (`fee_model`) and
/// the cost of a fill (`cost`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Commission {
    /// Charged once per order, on its first fill.
    PerOrder(Money),
    /// A share of the value (quantity × multiplier × price) of each fill,
    /// maker and taker alike, with no minimum: 0.0005 charges 0.05%.
    Rate(Decimal),
    /// Charged on each unit of the quantity of each fill, an amount exact
    /// rather than rounded to its currency's precision: 0.035 USD charges
    /// 3.50 USD on 100 oz of gold, Pepperstone's 3.5 USD a lot. Each fill's
    /// charge is rounded to the currency's precision, a tie to the even cent
    /// as Nautilus rounds money: 0.04 USD on 1 oz, 0.10 USD on 3.
    PerQuantity(Money),
}

impl Commission {
    /// The Nautilus fee model charging the commission on the fills of a
    /// simulated venue.
    ///
    /// # Errors
    ///
    /// Returns an error if the commission is negative: Nautilus refuses a
    /// negative amount, and would take a negative rate as a rebate.
    pub fn fee_model(&self) -> anyhow::Result<FeeModelAny> {
        Ok(match *self {
            Self::PerOrder(commission) => FeeModelAny::Fixed(FixedFeeModel::new(commission, None)?),
            Self::Rate(rate) => {
                ensure!(
                    !rate.is_sign_negative(),
                    "commission rate {rate} must not be negative"
                );
                FeeModelAny::MakerTaker(MakerTakerFeeModel::new(rate, rate))
            }
            Self::PerQuantity(amount) => {
                FeeModelAny::PerContract(PerContractFeeModel::new(amount)?)
            }
        })
    }

    /// What a fill of `quantity` of `instrument` at `price` costs, as the fee
    /// model charges it on an order's first fill, in the currency it charges
    /// it in: per order, the commission itself, in its own currency, so the
    /// cost of an order filled at once; a rate, its share of the fill's
    /// value as Nautilus computes it (`calculate_maker_taker_commission`), in
    /// the instrument's quote currency (its base on an inverse one, its
    /// settlement on a quanto); per quantity, its amount on each unit,
    /// rounded as the fee model rounds it, in the amount's currency. The fee
    /// model charges a generic spread per leg, which no target trades.
    ///
    /// # Errors
    ///
    /// Returns an error if the fill's value cannot be computed, or the cost
    /// overflows or is more money than Nautilus holds.
    pub fn cost(
        &self,
        quantity: Quantity,
        price: Price,
        instrument: &InstrumentAny,
    ) -> anyhow::Result<Money> {
        match *self {
            Self::PerOrder(commission) => Ok(commission),
            Self::Rate(rate) => {
                calculate_maker_taker_commission(instrument, quantity, price, rate, Some(false))
            }
            Self::PerQuantity(amount) => {
                let cost = amount
                    .as_decimal()
                    .checked_mul(quantity.as_decimal())
                    .with_context(|| format!("{amount} on each of {quantity} overflows"))?;
                Ok(Money::from_decimal(cost, amount.currency)?)
            }
        }
    }

    /// The currency it charges a fill of `instrument` in (see `cost`): an
    /// amount's own, per order or per quantity; a rate's, the currency of
    /// the fill's value, the instrument's cost currency.
    #[must_use]
    pub fn currency(&self, instrument: &InstrumentAny) -> Currency {
        match *self {
            Self::PerOrder(amount) | Self::PerQuantity(amount) => amount.currency,
            Self::Rate(_) => instrument.cost_currency(),
        }
    }

    /// The commission's amount, normalized, so that every spelling of one
    /// commission reads the same: a money without trailing zeros and its
    /// currency code, `"3 EUR"` for 3.00 EUR, a rate without trailing
    /// zeros, `"0.00003"` for 0.000030.
    #[must_use]
    pub fn normalized(&self) -> String {
        match *self {
            Self::PerOrder(money) | Self::PerQuantity(money) => {
                format!("{} {}", money.as_decimal().normalize(), money.currency.code)
            }
            Self::Rate(rate) => rate.normalize().to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use nautilus_execution::models::fee::FeeModel;
    use nautilus_model::{
        enums::{LiquiditySide, OrderSide, OrderType},
        instruments::{Instrument, stubs::cfd_gold},
        orders::{
            Order, OrderAny, OrderTestBuilder,
            stubs::{OrderFilledTestBuilder, TestOrderStubs},
        },
        types::Currency,
    };

    use super::*;

    /// Gold, an ounce a unit, quoted in USD.
    fn gold() -> InstrumentAny {
        InstrumentAny::Cfd(cfd_gold())
    }

    /// The price the fills are at.
    fn price() -> Price {
        Price::from("4000.20")
    }

    /// A market order for 100 oz of gold, accepted, as the matching engine
    /// charges its fills: a taker.
    fn order() -> OrderAny {
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(gold().id())
            .side(OrderSide::Buy)
            .quantity(Quantity::from(100))
            .build();
        let mut order = TestOrderStubs::make_accepted_order(&order);
        order.set_liquidity_side(LiquiditySide::Taker);
        order
    }

    /// `order` after a fill of `oz` at `price`.
    fn filled(mut order: OrderAny, oz: u64) -> OrderAny {
        let fill = OrderFilledTestBuilder::new(&order, &gold())
            .last_qty(Quantity::from(oz))
            .last_px(price())
            .build();
        order.apply(fill).unwrap();
        order
    }

    /// What the fee model of `commission` charges a fill of `oz` of `order`.
    fn charged(commission: Commission, order: &OrderAny, oz: u64) -> Money {
        commission
            .fee_model()
            .unwrap()
            .get_commission(order, Quantity::from(oz), price(), &gold())
            .unwrap()
    }

    /// What `commission` says a fill of `oz` costs.
    fn cost(commission: Commission, oz: u64) -> Money {
        commission
            .cost(Quantity::from(oz), price(), &gold())
            .unwrap()
    }

    /// 3 USD an order.
    fn per_order() -> Commission {
        Commission::PerOrder(Money::from("3 USD"))
    }

    /// 30 USD per million of value, IC's.
    fn rate() -> Commission {
        Commission::Rate(Decimal::new(3, 5))
    }

    /// 0.035 USD an ounce, Pepperstone's 3.5 USD a lot of 100 oz.
    fn per_ounce() -> Commission {
        Commission::PerQuantity(Money::from_str_exact("0.035 USD").unwrap())
    }

    #[test]
    fn a_commission_per_order_costs_its_amount_whatever_the_fill() {
        assert_eq!(cost(per_order(), 1), Money::from("3 USD"));
        assert_eq!(cost(per_order(), 100), Money::from("3 USD"));
    }

    #[test]
    fn a_commission_rate_costs_its_share_of_the_fills_value_to_the_cent() {
        // 0.00003 × 1 × 4000.20 = 0.120006, and × 100 = 12.0006.
        assert_eq!(cost(rate(), 1), Money::from("0.12 USD"));
        assert_eq!(cost(rate(), 100), Money::from("12.00 USD"));
    }

    #[test]
    fn a_commission_per_quantity_costs_its_amount_on_each_unit_to_the_cent() {
        // 0.035 × 100, Pepperstone's lot, and × 50; × 1 = 0.035, to the
        // cent 0.04, as Pepperstone charges a side of an ounce.
        assert_eq!(cost(per_ounce(), 100), Money::from("3.50 USD"));
        assert_eq!(cost(per_ounce(), 50), Money::from("1.75 USD"));
        assert_eq!(cost(per_ounce(), 1), Money::from("0.04 USD"));
    }

    #[test]
    fn a_commission_per_quantity_costs_the_same_at_any_price() {
        let at = |price: &str| {
            per_ounce()
                .cost(Quantity::from(100), Price::from(price), &gold())
                .unwrap()
        };
        assert_eq!(at("2000.00"), Money::from("3.50 USD"));
        assert_eq!(at("4000.20"), Money::from("3.50 USD"));
    }

    #[test]
    fn a_half_cent_of_a_commission_per_quantity_goes_to_the_even_cent() {
        // 0.035 × 3 = 0.105, × 5 = 0.175, × 7 = 0.245.
        assert_eq!(cost(per_ounce(), 3), Money::from("0.10 USD"));
        assert_eq!(cost(per_ounce(), 5), Money::from("0.18 USD"));
        assert_eq!(cost(per_ounce(), 7), Money::from("0.24 USD"));
    }

    /// The cost of a fill is what a backtest's and a dataset run's venue
    /// charge it, on each kind of commission.
    #[test]
    fn the_fee_model_charges_an_orders_first_fill_what_the_commission_costs() {
        for commission in [per_order(), rate(), per_ounce()] {
            for oz in [1, 3, 100] {
                assert_eq!(
                    charged(commission, &order(), oz),
                    cost(commission, oz),
                    "{commission:?} on {oz} oz"
                );
            }
        }
    }

    #[test]
    fn a_commission_per_order_charges_nothing_on_later_fills_of_the_same_order() {
        let order = filled(order(), 40);
        assert_eq!(charged(per_order(), &order, 60), Money::from("0 USD"));
    }

    #[test]
    fn a_commission_per_quantity_charges_every_fill_of_an_order() {
        // 0.035 × 60.
        let order = filled(order(), 40);
        assert_eq!(charged(per_ounce(), &order, 60), Money::from("2.10 USD"));
    }

    #[test]
    fn a_commission_rate_charges_maker_and_taker_alike() {
        let mut order = order();
        order.set_liquidity_side(LiquiditySide::Maker);
        // 0.00003 × 100 × 4000.20.
        assert_eq!(charged(rate(), &order, 100), Money::from("12.00 USD"));
    }

    #[test]
    fn a_negative_commission_has_no_fee_model() {
        for commission in [
            Commission::PerOrder(Money::from("-3 USD")),
            Commission::Rate(Decimal::new(-3, 5)),
            Commission::PerQuantity(Money::from_str_exact("-0.035 USD").unwrap()),
        ] {
            assert!(commission.fee_model().is_err(), "{commission:?}");
        }
    }

    /// An amount is charged in its own currency, a rate in the currency of
    /// the fill's value: gold's USD; an inverse perpetual's BTC, its base.
    #[test]
    fn a_commission_is_charged_in_its_amounts_currency_or_the_currency_of_the_value() {
        let inverse =
            InstrumentAny::CryptoPerpetual(nautilus_model::instruments::stubs::btcusd_bybit());

        assert_eq!(
            Commission::PerOrder(Money::from("3 EUR")).currency(&gold()),
            Currency::EUR()
        );
        assert_eq!(per_ounce().currency(&gold()), Currency::USD());
        assert_eq!(rate().currency(&gold()), Currency::USD());
        assert_eq!(rate().currency(&inverse), Currency::BTC());
    }

    /// Every spelling of one commission reads the same.
    #[test]
    fn a_commission_normalized_has_no_trailing_zeros() {
        assert_eq!(
            Commission::PerOrder(Money::from("3.00 EUR")).normalized(),
            "3 EUR"
        );
        assert_eq!(
            Commission::Rate(Decimal::new(30, 6)).normalized(),
            "0.00003"
        );
        assert_eq!(
            Commission::PerQuantity(Money::from_str_exact("0.0350 USD").unwrap()).normalized(),
            "0.035 USD"
        );
    }
}
