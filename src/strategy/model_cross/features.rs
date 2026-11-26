//! Defines the features and the event filter of the `model_cross` strategy.
//!
//! The features are those of `model_ema`: the fast and slow `ExponentialMovingAverage` of the
//! closes of each bar type the strategy reads, built from `[indicators.fast]` and
//! `[indicators.slow]`. A feature is named after its table when one bar type is read (`fast`).
//! When several are read, it is named after the bar type and the table
//! (`XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL:fast`).
//!
//! The event filter is a cross of the averages of `[indicators.cross]` on the bars of the traded
//! instrument (`cross::Cross`), so the strategy decides only at the grid ticks after a cross.

use std::rc::Rc;

use crate::{
    model::features::FeatureSet,
    strategy::{Declared, EmaArgs, Series},
};

use super::{ModelCrossConfig, cross::CrossArgs};

pub(super) const FAST: Declared<EmaArgs> = Declared::new("fast");
pub(super) const SLOW: Declared<EmaArgs> = Declared::new("slow");
pub(super) const CROSS: Declared<CrossArgs> = Declared::new("cross");

/// The version of these features and the event filter, in the fingerprint of the strategy's
/// dataset and models.
///
/// Bumped when what they compute changes and their names and tables do not, such as the kind of
/// average or what makes a cross. The periods, the price read and the bar types are in the
/// fingerprint by value.
const VERSION: u32 = 1;

// panics-doc-ok (transitive via FeatureSetBuilder::feature and FeatureSetBuilder::event_filter)
/// Builds the averages of each bar type in turn, the fast then the slow, and the cross on the bars
/// of the traded instrument.
///
/// The strategy's warmup warms them. A dataset build replays that warmup before its first day.
///
/// # Errors
///
/// Returns an error if the traded instrument has not exactly one bar type among those it reads.
///
/// # Panics
///
/// Panics if `[indicators]` does not build the averages and the cross, which the load refuses.
pub(super) fn features(config: &ModelCrossConfig) -> anyhow::Result<Rc<FeatureSet>> {
    let traded = config.trade.instruments[0].instrument_id;
    let crossed: Vec<Series> = config
        .data
        .bar_types()
        .filter(|bar_type| bar_type.instrument_id() == traded)
        .map(Series::Bars)
        .collect();
    anyhow::ensure!(
        crossed.len() == 1,
        "the cross reads the bars of the traded instrument {traded}, and data.series lists {} \
         bars series of it: list one",
        crossed.len()
    );
    config
        .data
        .bar_types()
        .flat_map(|bar_type| [FAST, SLOW].map(|average| (bar_type, average)))
        .fold(
            FeatureSet::builder()
                .version(VERSION)
                .warmup(config.data.warmup),
            |builder, (bar_type, average)| {
                builder.feature(&[Series::Bars(bar_type)], average, &config.indicators)
            },
        )
        .event_filter(&crossed, CROSS, &config.indicators)
        .build()
}

#[cfg(all(test, feature = "strategy-tests"))]
mod tests {
    use rstest::rstest;
    use nautilus_common::actor::indicators::ActorIndicator;
    use nautilus_core::UnixNanos;
    use nautilus_model::data::{Bar, BarType};

    use super::*;
    use crate::{
        model::{ModelPart, features::NoEvent},
        strategy::model_cross::ModelCrossParams,
    };

    const GOLD: &str = "XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL";
    const GOLD_BARS: &str = r#"{ data = "bars", instrument_id = "XAUUSD.ICMARKETS", bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" } },"#;
    const MINUTE: u64 = 60_000_000_000;

    /// Test config:
    /// - gold's minute bars
    /// - features of a fast average of 2 bars and a slow average of 3
    /// - a cross of the same averages
    /// - a warmup of 3 hours
    /// - a time-only target
    /// - a unit of the ATR of 3 hourly bars
    const CONFIG: &str = r#"
        [data]
        series = [
            { data = "bars", instrument_id = "XAUUSD.ICMARKETS", bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" } },
        ]
        warmup = "3h"

        [indicators.fast]
        period = 2

        [indicators.slow]
        period = 3

        [indicators.cross]
        fast = { period = 2 }
        slow = { period = 3 }

        [trade]
        instruments = [{ instrument_id = "XAUUSD.ICMARKETS" }]
        risk = "0.5%"

        [model]
        dataset = { commission = { rate = "0.00003" }, balance = "10000000 USD" }
        decision = { every = "1h" }
        target = { limit = "30m", atr = { period = 3 } }
        retrain = "24h"
        window = "48h"
        calibration = "12h"
        margins = { long = "0.5R", short = "0.5R" }
        min_rows = 30
        learner = { kind = "baseline" }
    "#;

    fn features_of(toml: &str) -> Rc<FeatureSet> {
        ModelCrossParams::features(&ModelCrossConfig::load(toml).unwrap()).unwrap()
    }

    /// A bar of gold closing at `close`, at the end of minute `minute`.
    fn bar(minute: u64, close: &str) -> Bar {
        let ts = UnixNanos::from(minute * MINUTE);
        Bar::new(
            BarType::from(GOLD),
            close.into(),
            close.into(),
            close.into(),
            close.into(),
            1.into(),
            ts,
            ts,
        )
    }

    fn fed(closes: &[&str]) -> Rc<FeatureSet> {
        let features = features_of(CONFIG);
        for (minute, close) in (1..).zip(closes) {
            features.handle_bar(&bar(minute, close)).unwrap();
        }
        features
    }

    /// Two closes warm the fast average and not the slow, of three bars.
    #[rstest]
    fn a_cross_whose_slow_average_is_cold_marks_no_decision() {
        let features = fed(&["11.00", "12.00"]);

        assert_eq!(
            features.event(UnixNanos::default(), UnixNanos::from(3 * MINUTE)),
            Err(NoEvent::Cold)
        );
    }

    /// Closes of 11, 12, 13 then 14. The fast average starts from the mean of its first two, 11.5,
    /// then weighs each close 2/3: 12.5, then 13.5. The slow starts from the mean of its first
    /// three, 12, then weighs each close 1/2: 13. The fast is above the slow after the third bar
    /// and the fourth.
    #[rstest]
    fn averages_that_do_not_cross_mark_no_decision() {
        let features = fed(&["11.00", "12.00", "13.00", "14.00"]);

        assert_eq!(
            features.event(UnixNanos::from(3 * MINUTE), UnixNanos::from(5 * MINUTE)),
            Err(NoEvent::Unmarked)
        );
    }

    /// Closes of 11, 12, 13 then 10. After the third bar the fast average, 12.5, is above the
    /// slow, 12. The fourth brings the fast to 12.5 + 2/3 (10 - 12.5), 10.83, and the slow to
    /// 12 + 1/2 (10 - 12), 11: the fast is below, a cross at the fourth bar, minute 4. It is an
    /// event for a decision whose previous grid point is before minute 4, and none for one whose
    /// previous grid point is at it.
    #[rstest]
    #[case::between_the_grid_points(3 * MINUTE + 1, Ok(()))]
    #[case::at_the_previous_grid_point(4 * MINUTE, Err(NoEvent::Unmarked))]
    fn a_cross_is_an_event_for_the_decision_after_it(
        #[case] previous: u64,
        #[case] event: Result<(), NoEvent>,
    ) {
        let features = fed(&["11.00", "12.00", "13.00", "10.00"]);

        assert_eq!(
            features.event(UnixNanos::from(previous), UnixNanos::from(5 * MINUTE)),
            event
        );
    }

    /// The cross is no feature: the names and values are the averages'.
    #[rstest]
    fn the_features_are_the_averages_alone() {
        let features = fed(&["11.00", "12.00", "13.00"]);

        assert_eq!(features.names(), ["fast", "slow"]);
        let values = features.values().unwrap();
        assert!((values[0] - 12.5).abs() < 1e-9, "{values:?}");
        assert!((values[1] - 12.0).abs() < 1e-9, "{values:?}");
    }

    /// The cross reads the bars of the traded instrument, gold. Silver's bars alone, or gold's of
    /// two bar types, leave it no one bar type to read, and the load refuses them under the
    /// series.
    #[rstest]
    #[case::of_another_instrument_alone(CONFIG.replace("XAUUSD.ICMARKETS\", bar_spec", "XAGUSD.ICMARKETS\", bar_spec"), 0)]
    #[case::of_two_bar_types_of_the_traded_instrument(
        CONFIG.replace(GOLD_BARS, &format!("{GOLD_BARS}\n{}", GOLD_BARS.replace("step = 1,", "step = 5,"))),
        2
    )]
    fn a_config_without_one_bars_series_of_the_traded_instrument_is_refused(
        #[case] toml: String,
        #[case] listed: usize,
    ) {
        let err = ModelCrossConfig::load(&toml).unwrap_err();

        let message = format!("{err:#}");
        assert!(
            message.contains("data.series")
                && message.contains("XAUUSD.ICMARKETS")
                && message.contains(&format!("lists {listed} bars series")),
            "{message}"
        );
    }

    /// Averages of one period never cross, so the load refuses them under the cross's table.
    #[rstest]
    fn a_cross_whose_fast_period_is_not_below_its_slow_is_refused_under_its_table() {
        let toml = CONFIG.replace("fast = { period = 2 }", "fast = { period = 3 }");

        let message = format!("{:#}", ModelCrossConfig::load(&toml).unwrap_err());

        assert!(
            message.contains("indicators.cross") && message.contains("fast period 3"),
            "{message}"
        );
    }

    /// A changed period of the cross changes the fingerprint with no version bump.
    #[rstest]
    fn a_changed_period_of_the_cross_changes_the_fingerprint() {
        let changed = CONFIG.replace("slow = { period = 3 }", "slow = { period = 4 }");

        assert_ne!(
            features_of(&changed).fingerprint(),
            features_of(CONFIG).fingerprint()
        );
    }
}
