//! The features of the model tests, declared and set in `[indicators]` as
//! a strategy's are: Nautilus' simple moving average by its constructor's
//! arguments (`SmaArgs`), as the crate has its exponential one
//! (`strategy::EmaArgs`), and `[indicators]` tables of TOML text.

use helix_trader::strategy::{IndicatorArgs, IndicatorsConfig};
use nautilus_indicators::average::sma::SimpleMovingAverage;
use nautilus_model::enums::PriceType;
use serde::{Deserialize, Serialize};

/// The arguments of a Nautilus `SimpleMovingAverage`, built by its
/// `new_checked`: `period`, and `price_type`, the price it reads of quotes
/// (`LAST` left out; bars give their close).
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SmaArgs {
    pub period: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_type: Option<PriceType>,
}

impl IndicatorArgs for SmaArgs {
    type Indicator = SimpleMovingAverage;

    fn build(self) -> anyhow::Result<SimpleMovingAverage> {
        SimpleMovingAverage::new_checked(self.period, self.price_type)
    }
}

/// The `[indicators]` tables `toml` holds, each by its name.
pub fn tables(toml: &str) -> IndicatorsConfig {
    toml::from_str(toml).unwrap()
}
