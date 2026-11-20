//! The `model_ema` strategy's features: the fast and slow `ExponentialMovingAverage` of the
//! closes of each bar type it reads, of `[indicators.fast]` and `[indicators.slow]`. Named after
//! the table when one bar type is read (`fast`), after the bar type and the table when several
//! are (`XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL:fast`).

use std::rc::Rc;

use crate::{
    model::features::FeatureSet,
    strategy::{Declared, EmaArgs, Series},
};

use super::ModelEmaConfig;

pub(super) const FAST: Declared<EmaArgs> = Declared::new("fast");
pub(super) const SLOW: Declared<EmaArgs> = Declared::new("slow");

/// The version of these features, in the fingerprint of the strategy's dataset and models.
/// Bumped when what they compute changes and their names and tables do not, such as the kind of
/// average; the periods, the price read and the bar type are in the fingerprint by value. The
/// golden rows (`tests/backtest/backtest_golden_rows.rs`) fail on a change without a bump.
const VERSION: u32 = 1;

// panics-doc-ok (transitive via FeatureSetBuilder::feature)
/// The averages of each bar type in turn, the fast then the slow, warmed by the strategy's
/// warmup, which a dataset build replays before its first day.
///
/// # Errors
///
/// Returns an error if it reads no bar type.
///
/// # Panics
///
/// Panics if `[indicators]` does not build both averages, which the load refuses.
pub(super) fn features(config: &ModelEmaConfig) -> anyhow::Result<Rc<FeatureSet>> {
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
        .build()
}

#[cfg(all(test, feature = "strategy-tests"))]
mod tests {
    use jiff::SignedDuration;
    use rstest::rstest;
    use nautilus_common::actor::indicators::ActorIndicator;
    use nautilus_model::data::{Bar, BarType};

    use super::*;
    use crate::{model::ModelPart, strategy::model_ema::ModelEmaParams};

    const GOLD: &str = "XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL";
    const SILVER: &str = "XAGUSD.ICMARKETS-1-MINUTE-MID-INTERNAL";
    const GOLD_BARS: &str = r#"{ data = "bars", instrument_id = "XAUUSD.ICMARKETS", bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" } },"#;

    /// Gold's minute bars, fast average 2 bars, slow 3, warmup 3 hours, time-only target, unit
    /// the ATR of 3 hourly bars.
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
        ModelEmaParams::features(&ModelEmaConfig::load(toml).unwrap()).unwrap()
    }

    fn with_silver() -> String {
        let silver_bars = GOLD_BARS.replace("XAUUSD", "XAGUSD");
        CONFIG.replace(GOLD_BARS, &format!("{GOLD_BARS}\n{silver_bars}"))
    }

    fn bar(close: &str) -> Bar {
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
    }

    #[rstest]
    fn the_features_declare_the_strategys_warmup() {
        assert_eq!(features_of(CONFIG).warmup(), SignedDuration::from_hours(3));
    }

    /// Each table feeds a feature of each bar type, named after the table alone for one bar
    /// type, after each bar type and the table for several.
    #[rstest]
    #[case::of_one_bar_type(CONFIG.to_string(), vec!["fast".to_string(), "slow".to_string()])]
    #[case::of_several_bar_types(
        with_silver(),
        vec![
            format!("{GOLD}:fast"),
            format!("{GOLD}:slow"),
            format!("{SILVER}:fast"),
            format!("{SILVER}:slow"),
        ]
    )]
    fn the_averages_are_named_after_their_tables_and_bar_types(
        #[case] toml: String,
        #[case] names: Vec<String>,
    ) {
        assert_eq!(features_of(&toml).names(), names);
    }

    /// Closes of 1, 2 and 3: the averages start from the mean of their first period, then weigh
    /// each close 2 / (period + 1). Fast: 1.5 after two bars, then 1.5 + 2/3 (3 - 1.5) = 2.5.
    /// Slow: warm after three, at 2.
    #[rstest]
    fn the_averages_are_of_the_periods_their_tables_set() {
        let features = features_of(CONFIG);

        features.handle_bar(&bar("1.00")).unwrap();
        features.handle_bar(&bar("2.00")).unwrap();
        assert_eq!(features.values(), None);
        features.handle_bar(&bar("3.00")).unwrap();

        let values = features.values().unwrap();
        assert!((values[0] - 2.5).abs() < 1e-9, "{values:?}");
        assert!((values[1] - 2.0).abs() < 1e-9, "{values:?}");
    }

    /// A changed period, bar spec or instrument changes the fingerprint with no version bump,
    /// though one series names the features after their tables alone.
    #[rstest]
    #[case::a_period(CONFIG.replace("period = 2", "period = 4"))]
    #[case::a_bar_spec(CONFIG.replace("step = 1,", "step = 5,"))]
    #[case::an_instrument(CONFIG.replace("XAUUSD", "XAGUSD"))]
    fn a_changed_period_bar_spec_or_instrument_changes_the_fingerprint(#[case] changed: String) {
        assert_ne!(
            features_of(&changed).fingerprint(),
            features_of(CONFIG).fingerprint()
        );
    }
}
