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
mod tests {
    use nautilus_common::actor::indicators::ActorIndicator;
    use nautilus_model::data::Bar;

    use super::*;

    const GOLD: &str = "XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL";
    const HOUR: SignedDuration = SignedDuration::from_hours(1);

    /// A dataset build replays the strategy's warmup before its first day.
    #[test]
    fn the_features_declare_the_strategys_warmup() {
        let warmup = SignedDuration::from_hours(3);

        assert_eq!(
            features(&[BarType::from(GOLD)], warmup).unwrap().warmup(),
            warmup
        );
    }

    #[test]
    fn each_bar_type_has_a_fast_and_a_slow_average_in_order() {
        let silver = "XAGUSD.ICMARKETS-1-MINUTE-MID-INTERNAL";

        let features = features(&[BarType::from(GOLD), BarType::from(silver)], HOUR).unwrap();

        assert_eq!(
            features.names(),
            [
                format!("{GOLD}:ema_10"),
                format!("{GOLD}:ema_30"),
                format!("{silver}:ema_10"),
                format!("{silver}:ema_30"),
            ]
        );
    }

    /// Flat closes: each average is the close once warm, the slow after
    /// 30 bars.
    #[test]
    fn the_features_are_warm_after_the_slow_period() {
        let features = features(&[BarType::from(GOLD)], HOUR).unwrap();
        let bar = |close: &str| {
            Bar::new(
                BarType::from(GOLD),
                close.into(),
                close.into(),
                close.into(),
                close.into(),
                1.into(),
                0.into(),
                0.into(),
            )
        };

        for _ in 0..29 {
            features.handle_bar(&bar("4000.00")).unwrap();
        }
        assert_eq!(features.values(), None);
        features.handle_bar(&bar("4000.00")).unwrap();

        assert_eq!(features.values(), Some(vec![4000.0, 4000.0]));
    }

    #[test]
    fn no_bar_type_is_no_feature_set() {
        assert!(features(&[], HOUR).is_err());
    }
}
