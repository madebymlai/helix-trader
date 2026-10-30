//! The learned strategy's features: on the bars of each bar type it reads,
//! a fast and a slow exponential moving average of the closes, named after
//! the bar type, such as `XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL:ema_10`.

use std::rc::Rc;

use jiff::SignedDuration;
use nautilus_indicators::average::ema::ExponentialMovingAverage;
use nautilus_model::data::BarType;

use crate::model::features::FeatureSet;

/// The periods of the averages, in bars: the fast, then the slow.
const PERIODS: [usize; 2] = [10, 30];

/// The features of `bar_types`, the averages of each in turn, warmed by
/// the strategy's `warmup`, which a dataset build replays before its first
/// day.
///
/// # Errors
///
/// Returns an error if there is no bar type, or the warmup is negative.
pub fn features(bar_types: &[BarType], warmup: SignedDuration) -> anyhow::Result<Rc<FeatureSet>> {
    bar_types
        .iter()
        .flat_map(|&bar_type| PERIODS.map(|period| (bar_type, period)))
        .fold(
            FeatureSet::builder()
                .code(include_str!("features.rs"))
                .warmup(warmup),
            |builder, (bar_type, period)| {
                builder.bars(
                    bar_type,
                    &format!("{bar_type}:ema_{period}"),
                    ExponentialMovingAverage::new(period, None),
                )
            },
        )
        .build()
}

#[cfg(test)]
mod tests;
