//! Trading costs of the simulated venue.
//!
//! Builds the Nautilus `FeeModelAny` and `FillModelAny`
//! (`nautilus_execution`) that `BacktestVenueConfig` takes: a commission
//! either fixed per order (`FixedFeeModel`) or a rate on the trade value
//! (`MakerTakerFeeModel`), and fills at the quote with no slippage of their
//! own (`DefaultFillModel`): the spread is the cost.

use anyhow::ensure;
use nautilus_execution::models::{
    fee::{FeeModelAny, FixedFeeModel, MakerTakerFeeModel},
    fill::{DefaultFillModel, FillModelAny},
};
use nautilus_model::types::Money;
use rust_decimal::Decimal;

/// Seed of the fill model, fixed so that runs are reproducible.
const FILL_SEED: u64 = 42;

/// `commission` charged once per order, on its first fill.
///
/// # Errors
///
/// Returns an error if `commission` is negative.
pub fn commission_per_order(commission: Money) -> anyhow::Result<FeeModelAny> {
    Ok(FeeModelAny::Fixed(FixedFeeModel::new(
        commission,
        Some(true),
    )?))
}

/// `rate` charged on the value (quantity × price) of each fill, maker and
/// taker alike, with no minimum: 0.0005 charges 0.05%.
///
/// # Errors
///
/// Returns an error if `rate` is negative, which `MakerTakerFeeModel`
/// would take as a rebate.
pub fn commission_rate(rate: Decimal) -> anyhow::Result<FeeModelAny> {
    ensure!(
        !rate.is_sign_negative(),
        "commission rate {rate} must not be negative"
    );
    Ok(FeeModelAny::MakerTaker(MakerTakerFeeModel::new(rate, rate)))
}

/// Market orders fill at the quote (a buy at the ask, a sell at the bid),
/// with no slippage; limit orders always fill when touched.
pub(super) fn fill_model() -> anyhow::Result<FillModelAny> {
    Ok(FillModelAny::Default(DefaultFillModel::new(
        1.0,
        0.0,
        Some(FILL_SEED),
    )?))
}

#[cfg(test)]
mod tests {
    use nautilus_execution::models::fee::FeeModel;
    use nautilus_model::{
        enums::{LiquiditySide, OrderSide, OrderType},
        instruments::{Instrument, InstrumentAny, stubs::equity_aapl},
        orders::{
            Order, OrderAny, OrderTestBuilder,
            stubs::{OrderFilledTestBuilder, TestOrderStubs},
        },
        types::{Price, Quantity},
    };

    use super::*;

    fn instrument() -> InstrumentAny {
        InstrumentAny::Equity(equity_aapl())
    }

    /// A limit order for 10 shares.
    fn limit_order() -> OrderAny {
        OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(instrument().id())
            .side(OrderSide::Buy)
            .price(Price::from("100.00"))
            .quantity(Quantity::from(10))
            .build()
    }

    /// The limit order, accepted.
    fn order() -> OrderAny {
        TestOrderStubs::make_accepted_order(&limit_order())
    }

    fn commission(order: &OrderAny, fill_qty: u64) -> Money {
        commission_per_order(Money::from("3 USD"))
            .unwrap()
            .get_commission(
                order,
                Quantity::from(fill_qty),
                Price::from("100.00"),
                &instrument(),
            )
            .unwrap()
    }

    #[test]
    fn charges_the_commission_on_the_first_fill_whatever_its_size() {
        assert_eq!(commission(&order(), 10), Money::from("3 USD"));
    }

    #[test]
    fn charges_nothing_on_later_fills_of_the_same_order() {
        let mut order = order();
        let fill = OrderFilledTestBuilder::new(&order, &instrument())
            .last_qty(Quantity::from(4))
            .build();
        order.apply(fill).unwrap();
        assert_eq!(commission(&order, 6), Money::from("0 USD"));
    }

    #[test]
    fn rejects_a_negative_commission() {
        assert!(commission_per_order(Money::from("-3 EUR")).is_err());
    }

    /// 0.05%.
    fn rate() -> Decimal {
        Decimal::new(5, 4)
    }

    #[test]
    fn charges_the_rate_on_the_value_of_each_fill_maker_and_taker_alike() {
        let model = commission_rate(rate()).unwrap();
        for side in [LiquiditySide::Maker, LiquiditySide::Taker] {
            // The rate model needs the order's liquidity side, which the
            // matching engine sets before it charges a fill.
            let order = TestOrderStubs::make_filled_order(&limit_order(), &instrument(), side);
            let commission = model
                .get_commission(
                    &order,
                    Quantity::from(4),
                    Price::from("100.00"),
                    &instrument(),
                )
                .unwrap();
            // 0.0005 × 4 × 100.00 USD
            assert_eq!(commission, Money::from("0.20 USD"), "{side:?}");
        }
    }

    #[test]
    fn rejects_a_negative_rate() {
        assert!(commission_rate(-rate()).is_err());
    }

    #[test]
    fn fills_at_the_quote_with_no_slippage() {
        assert_eq!(
            fill_model().unwrap().to_string(),
            "DefaultFillModel(prob_fill_on_limit=1, prob_slippage=0)"
        );
    }
}
