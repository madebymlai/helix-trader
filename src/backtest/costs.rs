//! Trading costs of the simulated venue.
//!
//! Builds the Nautilus `FeeModelAny` and `FillModelAny`
//! (`nautilus_execution`) that `BacktestVenueConfig` takes: a fixed
//! commission per order (`FixedFeeModel`) and one tick of slippage on every
//! fill (`OneTickSlippageFillModel`).

use nautilus_execution::models::{
    fee::{FeeModelAny, FixedFeeModel},
    fill::{FillModelAny, OneTickSlippageFillModel},
};
use nautilus_model::types::Money;

/// Seed of the fill model, fixed so that runs are reproducible.
const FILL_SEED: u64 = 42;

/// `commission` charged once per order, on its first fill.
///
/// # Errors
///
/// Returns an error if `commission` is negative.
pub(super) fn fee_model(commission: Money) -> anyhow::Result<FeeModelAny> {
    Ok(FeeModelAny::Fixed(FixedFeeModel::new(
        commission,
        Some(true),
    )?))
}

/// Every fill one tick worse than the best price; limit orders always fill
/// when touched.
pub(super) fn fill_model() -> anyhow::Result<FillModelAny> {
    Ok(FillModelAny::OneTickSlippage(
        OneTickSlippageFillModel::new(1.0, 0.0, Some(FILL_SEED))?,
    ))
}

#[cfg(test)]
mod tests {
    use nautilus_execution::models::fee::FeeModel;
    use nautilus_model::{
        enums::{OrderSide, OrderType},
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

    /// An accepted limit order for 10 shares.
    fn order() -> OrderAny {
        let order = OrderTestBuilder::new(OrderType::Limit)
            .instrument_id(instrument().id())
            .side(OrderSide::Buy)
            .price(Price::from("100.00"))
            .quantity(Quantity::from(10))
            .build();
        TestOrderStubs::make_accepted_order(&order)
    }

    fn commission(order: &OrderAny, fill_qty: u64) -> Money {
        fee_model(Money::from("3 USD"))
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
        assert!(fee_model(Money::from("-3 EUR")).is_err());
    }

    #[test]
    fn slips_one_tick() {
        assert!(matches!(
            fill_model().unwrap(),
            FillModelAny::OneTickSlippage(_)
        ));
    }
}
