use nautilus_model::enums::OrderSide;
use rust_decimal::Decimal;
use serde::Deserialize;

/// A distance from a price in basis points, hundredths of a percent:
/// `50` is 0.5%. Read from TOML as a number or a decimal string.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Deserialize)]
#[serde(transparent)]
pub struct Bps(Decimal);

impl Bps {
    /// The whole price, 100%.
    pub const WHOLE: Self = Self(Decimal::from_parts(10_000, 0, 0, false, 0));

    #[must_use]
    pub fn is_positive(self) -> bool {
        self.0 > Decimal::ZERO
    }

    /// The distance as a fraction of the price.
    fn fraction(self) -> Decimal {
        self.0 / Self::WHOLE.0
    }
}

impl From<Decimal> for Bps {
    fn from(bps: Decimal) -> Self {
        Self(bps)
    }
}

/// The exit prices of a trade, unrounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Exits {
    pub(super) stop_loss: Decimal,
    pub(super) take_profit: Decimal,
}

impl Exits {
    /// The exits of a trade on `side` decided on a bar closing at `close`:
    /// the stop loss `stop_loss` away against the trade, the take profit
    /// `take_profit` away in its favour.
    pub(super) fn new(close: Decimal, side: OrderSide, stop_loss: Bps, take_profit: Bps) -> Self {
        let (against, favour) = match side {
            OrderSide::Buy => (-Decimal::ONE, Decimal::ONE),
            OrderSide::Sell => (Decimal::ONE, -Decimal::ONE),
        };
        Self {
            stop_loss: close * (Decimal::ONE + against * stop_loss.fraction()),
            take_profit: close * (Decimal::ONE + favour * take_profit.fraction()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bps(value: i64) -> Bps {
        Bps::from(Decimal::from(value))
    }

    fn price(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[test]
    fn a_long_stops_below_the_close_and_takes_profit_above() {
        // 620.60 less 1%, plus 2%.
        let exits = Exits::new(price("620.60"), OrderSide::Buy, bps(100), bps(200));
        assert_eq!(exits.stop_loss, price("614.394"));
        assert_eq!(exits.take_profit, price("633.012"));
    }

    #[test]
    fn a_short_stops_above_the_close_and_takes_profit_below() {
        // 600.60 plus 1%, less 0.5%.
        let exits = Exits::new(price("600.60"), OrderSide::Sell, bps(100), bps(50));
        assert_eq!(exits.stop_loss, price("606.606"));
        assert_eq!(exits.take_profit, price("597.597"));
    }

    #[test]
    fn whole_is_ten_thousand_bps() {
        assert_eq!(Bps::WHOLE, bps(10_000));
        assert!(bps(9_999) < Bps::WHOLE);
        assert!(!bps(0).is_positive());
    }
}
