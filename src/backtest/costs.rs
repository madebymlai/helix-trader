//! Trading costs of the simulated venue.
//!
//! The `Commission` of a run is the only source of its costs. It builds the Nautilus
//! `FeeModelAny`, which charges the fills of a backtest and of a dataset run alike. It also gives
//! the cost of a fill as that model charges it (`Commission::cost`). A model strategy's target
//! receives that cost (`model::targets::TargetSetter`).
//!
//! The venue sets no fill model, so Nautilus uses its default (`DefaultFillModel`). It fills at
//! the quote, so the spread is the cost. On bars, it fills at the bar's prices.

use std::fmt::Display;

use anyhow::Context;
use nautilus_execution::models::fee::{
    FeeModelAny, FixedFeeModel, MakerTakerFeeModel, PerContractFeeModel,
};
use nautilus_model::{
    fees::calculate_maker_taker_commission,
    instruments::{Instrument, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use rust_decimal::Decimal;
use serde::Deserialize;

/// The commission of a run.
///
/// In TOML, it is a table of one kind, such as `{ per_quantity = "0.035 USD" }`,
/// `{ rate = "0.0005" }` or `{ per_order = "3 EUR" }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(try_from = "CommissionFields")]
pub enum Commission {
    /// Charged once per order, on its first fill.
    PerOrder(Money),
    /// A share of each fill's value (quantity × multiplier × price), for maker and taker alike.
    /// 0.0005 charges 0.05%.
    Rate(Decimal),
    /// Charged on each unit of each fill's quantity. The amount is exact.
    ///
    /// Each fill's charge is rounded to the currency's precision, and a tie goes to the even cent,
    /// as Nautilus rounds money. 0.035 USD charges 3.50 USD on 100 oz, 0.04 on 1 oz, 0.10 on 3.
    PerQuantity(Money),
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum CommissionFields {
    PerOrder(String),
    Rate(String),
    PerQuantity(String),
}

impl TryFrom<CommissionFields> for Commission {
    type Error = anyhow::Error;

    fn try_from(fields: CommissionFields) -> anyhow::Result<Self> {
        match fields {
            CommissionFields::PerOrder(amount) => Self::per_order(&amount),
            CommissionFields::Rate(rate) => Self::rate(&rate),
            CommissionFields::PerQuantity(amount) => Self::per_quantity(&amount),
        }
    }
}

impl Display for Commission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self {
            Self::PerOrder(_) => "per order",
            Self::Rate(_) => "rate",
            Self::PerQuantity(_) => "per quantity",
        };
        write!(f, "{kind} {}", self.normalized())
    }
}

impl Commission {
    /// Returns a commission per order of `amount`, such as `"3 EUR"`.
    ///
    /// # Errors
    ///
    /// Returns an error if `amount` is not a currency amount or is negative.
    pub fn per_order(amount: &str) -> anyhow::Result<Self> {
        Self::PerOrder(amount.parse::<Money>().map_err(anyhow::Error::msg)?).checked()
    }

    /// Returns a commission of `rate` on the value of each fill.
    ///
    /// The rate is a decimal such as `"0.0005"`.
    ///
    /// # Errors
    ///
    /// Returns an error if `rate` is not a decimal or is negative.
    pub fn rate(rate: &str) -> anyhow::Result<Self> {
        Self::Rate(rate.parse()?).checked()
    }

    /// Returns a commission of `amount` on each unit of quantity, such as `"0.035 USD"`.
    ///
    /// # Errors
    ///
    /// Returns an error if `amount` is not a currency amount or is negative.
    pub fn per_quantity(amount: &str) -> anyhow::Result<Self> {
        Self::PerQuantity(Money::from_str_exact(amount).map_err(anyhow::Error::msg)?).checked()
    }

    fn checked(self) -> anyhow::Result<Self> {
        self.fee_model()?;
        Ok(self)
    }

    /// Returns the Nautilus fee model that charges the commission on the fills of a simulated
    /// venue.
    ///
    /// # Errors
    ///
    /// Returns an error if the commission is negative, because Nautilus refuses a negative amount
    /// and would take a negative rate as a rebate.
    pub fn fee_model(&self) -> anyhow::Result<FeeModelAny> {
        Ok(match *self {
            Self::PerOrder(commission) => {
                FeeModelAny::Fixed(FixedFeeModel::new(commission, Some(true))?)
            }
            Self::Rate(rate) => {
                anyhow::ensure!(
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

    /// Returns what a fill of `quantity` of `instrument` at `price` costs.
    ///
    /// The cost is what the fee model charges on an order's first fill, in the currency that
    /// `currency` returns. A rate costs what `calculate_maker_taker_commission` returns. The fee
    /// model charges a generic spread per leg, which no target trades.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The fill's value cannot be computed.
    /// - The cost overflows.
    /// - The cost is more money than Nautilus holds.
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

    /// Returns whether the commission charges nothing.
    #[must_use]
    pub fn is_zero(&self) -> bool {
        match *self {
            Self::PerOrder(amount) | Self::PerQuantity(amount) => amount.is_zero(),
            Self::Rate(rate) => rate.is_zero(),
        }
    }

    /// Returns the currency charged on a fill of `instrument`.
    ///
    /// An amount charges in its own currency. A rate charges in the instrument's cost currency.
    #[must_use]
    pub fn currency(&self, instrument: &InstrumentAny) -> Currency {
        match *self {
            Self::PerOrder(amount) | Self::PerQuantity(amount) => amount.currency,
            Self::Rate(_) => instrument.cost_currency(),
        }
    }

    /// Returns the amount without trailing zeros, such as `"3 EUR"` for 3.00 EUR and `"0.00003"`
    /// for 0.000030.
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
    use rstest::rstest;

    use super::*;

    fn gold() -> InstrumentAny {
        InstrumentAny::Cfd(cfd_gold())
    }

    fn price() -> Price {
        Price::from("4000.20")
    }

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

    fn filled(mut order: OrderAny, oz: u64) -> OrderAny {
        let fill = OrderFilledTestBuilder::new(&order, &gold())
            .last_qty(Quantity::from(oz))
            .last_px(price())
            .build();
        order.apply(fill).unwrap();
        order
    }

    fn charged(commission: Commission, order: &OrderAny, oz: u64) -> Money {
        commission
            .fee_model()
            .unwrap()
            .get_commission(order, Quantity::from(oz), price(), &gold())
            .unwrap()
    }

    fn cost(commission: Commission, oz: u64) -> Money {
        commission
            .cost(Quantity::from(oz), price(), &gold())
            .unwrap()
    }

    fn per_order() -> Commission {
        Commission::PerOrder(Money::from("3 USD"))
    }

    /// Charges 30 USD per million of value, as IC does.
    fn rate() -> Commission {
        Commission::Rate(Decimal::new(3, 5))
    }

    fn per_ounce() -> Commission {
        Commission::PerQuantity(Money::from_str_exact("0.035 USD").unwrap())
    }

    #[rstest]
    fn a_commission_per_order_costs_its_amount_whatever_the_fill() {
        assert_eq!(cost(per_order(), 1), Money::from("3 USD"));
        assert_eq!(cost(per_order(), 100), Money::from("3 USD"));
    }

    #[rstest]
    fn a_commission_rate_costs_its_share_of_the_fills_value_to_the_cent() {
        assert_eq!(cost(rate(), 1), Money::from("0.12 USD"));
        assert_eq!(cost(rate(), 100), Money::from("12.00 USD"));
    }

    #[rstest]
    fn a_commission_per_quantity_costs_its_amount_on_each_unit_to_the_cent() {
        assert_eq!(cost(per_ounce(), 100), Money::from("3.50 USD"));
        assert_eq!(cost(per_ounce(), 50), Money::from("1.75 USD"));
        assert_eq!(cost(per_ounce(), 1), Money::from("0.04 USD"));
    }

    #[rstest]
    fn a_commission_per_quantity_costs_the_same_at_any_price() {
        let at = |price: &str| {
            per_ounce()
                .cost(Quantity::from(100), Price::from(price), &gold())
                .unwrap()
        };
        assert_eq!(at("2000.00"), Money::from("3.50 USD"));
        assert_eq!(at("4000.20"), Money::from("3.50 USD"));
    }

    #[rstest]
    fn a_half_cent_of_a_commission_per_quantity_goes_to_the_even_cent() {
        assert_eq!(cost(per_ounce(), 3), Money::from("0.10 USD"));
        assert_eq!(cost(per_ounce(), 5), Money::from("0.18 USD"));
        assert_eq!(cost(per_ounce(), 7), Money::from("0.24 USD"));
    }

    #[rstest]
    #[case::per_order_on_1_oz(per_order(), 1)]
    #[case::per_order_on_3_oz(per_order(), 3)]
    #[case::per_order_on_100_oz(per_order(), 100)]
    #[case::rate_on_1_oz(rate(), 1)]
    #[case::rate_on_3_oz(rate(), 3)]
    #[case::rate_on_100_oz(rate(), 100)]
    #[case::per_quantity_on_1_oz(per_ounce(), 1)]
    #[case::per_quantity_on_3_oz(per_ounce(), 3)]
    #[case::per_quantity_on_100_oz(per_ounce(), 100)]
    fn the_fee_model_charges_an_orders_first_fill_what_the_commission_costs(
        #[case] commission: Commission,
        #[case] oz: u64,
    ) {
        assert_eq!(charged(commission, &order(), oz), cost(commission, oz));
    }

    #[rstest]
    fn a_commission_per_order_is_charged_once_nothing_on_later_fills_of_the_order() {
        let order = filled(order(), 40);
        assert_eq!(charged(per_order(), &order, 60), Money::from("0 USD"));
    }

    #[rstest]
    fn a_commission_rate_charges_maker_and_taker_alike() {
        let mut order = order();
        order.set_liquidity_side(LiquiditySide::Maker);

        assert_eq!(charged(rate(), &order, 100), Money::from("12.00 USD"));
    }

    #[rstest]
    #[case::per_order(Commission::PerOrder(Money::from("-3 USD")))]
    #[case::per_quantity(Commission::PerQuantity(Money::from_str_exact("-0.035 USD").unwrap()))]
    fn a_negative_amount_has_no_fee_model(#[case] commission: Commission) {
        assert!(commission.fee_model().is_err());
    }

    #[rstest]
    fn a_negative_rate_has_no_fee_model_naming_the_rate() {
        let error = Commission::Rate(Decimal::new(-3, 5))
            .fee_model()
            .unwrap_err()
            .to_string();
        assert!(error.contains("-0.00003"), "{error}");
    }

    #[rstest]
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

    #[rstest]
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
