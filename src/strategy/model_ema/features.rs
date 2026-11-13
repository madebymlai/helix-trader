//! The `model_ema` strategy's features: on the bars of each bar type it
//! reads, its fast and its slow exponential moving average of the closes,
//! Nautilus' `ExponentialMovingAverage`s of `[indicators.fast]` and
//! `[indicators.slow]`, named by the feature set's rule: after the bar
//! type and the table, such as
//! `XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL:fast`, when several bar types
//! are read, `fast` and `slow` when one is.

use std::rc::Rc;

use crate::{
    model::features::FeatureSet,
    strategy::{Declared, EmaArgs},
};

use super::ModelEmaConfig;

/// The fast average of each bars series, `[indicators.fast]`.
pub(super) const FAST: Declared<EmaArgs> = Declared::new("fast");
/// The slow average of each bars series, `[indicators.slow]`.
pub(super) const SLOW: Declared<EmaArgs> = Declared::new("slow");

/// The version of these features, in the fingerprint of the strategy's
/// dataset and models: bumped whenever what they compute changes and
/// their names and tables do not, such as the kind of average. Their
/// arguments, their periods and the price they read, are in the
/// fingerprint by value, as is the bar type each reads. The golden rows
/// (`tests/backtest/backtest_golden_rows.rs`) fail on a change made
/// without a bump.
const VERSION: u32 = 1;

/// The features of `config`: the averages of each of its bar types in
/// turn, the fast then the slow, warmed by the strategy's warmup, which a
/// dataset build replays before its first day.
///
/// # Errors
///
/// Returns an error if it reads no bar type.
///
/// # Panics
///
/// Panics if `[indicators]` does not build both averages, which the load
/// refuses.
pub fn features(config: &ModelEmaConfig) -> anyhow::Result<Rc<FeatureSet>> {
    config
        .data
        .bar_types()
        .flat_map(|bar_type| [FAST, SLOW].map(|average| (bar_type, average)))
        .fold(
            FeatureSet::builder()
                .version(VERSION)
                .warmup(config.data.warmup),
            |builder, (bar_type, average)| builder.bars(bar_type, average, &config.indicators),
        )
        .build()
}

#[cfg(all(test, feature = "strategy-tests"))]
mod tests {
    use jiff::SignedDuration;
    use nautilus_common::actor::indicators::ActorIndicator;
    use nautilus_model::data::{Bar, BarType};

    use super::*;
    use crate::{model::ModelPart, strategy::model_ema::ModelEmaParams};

    const GOLD: &str = "XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL";
    const SILVER: &str = "XAGUSD.ICMARKETS-1-MINUTE-MID-INTERNAL";
    const GOLD_BARS: &str = r#"{ data = "bars", instrument_id = "XAUUSD.ICMARKETS", bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" } },"#;

    /// The strategy trading gold on its minute bars, its fast average of
    /// 2 bars and its slow one of 3, warmed up over 3 hours, on a time-only
    /// target, its unit measured in the ATR of 3 hourly bars.
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
        instruments = [{ instrument_id = "XAUUSD.ICMARKETS", trade_size = "1" }]
        risk = "0.5%"

        [model]
        decision = { every = "1h" }
        target = { limit = "30m", atr = { period = 3 } }
        retrain = "24h"
        window = "48h"
        holdout = "12h"
        margins = { long = 0.5, short = 0.5 }
        min_pnl = 0.5
        min_trades = 30
        learner = { kind = "baseline" }
    "#;

    /// The features of the strategy `toml` configures, as its part builds
    /// them of the loaded config.
    fn features_of(toml: &str) -> Rc<FeatureSet> {
        ModelEmaParams::features(&ModelEmaConfig::load(toml).unwrap()).unwrap()
    }

    /// `CONFIG` reading silver's minute bars after gold's.
    fn with_silver() -> String {
        let silver_bars = GOLD_BARS.replace("XAUUSD", "XAGUSD");
        CONFIG.replace(GOLD_BARS, &format!("{GOLD_BARS}\n{silver_bars}"))
    }

    /// A minute bar of gold closing at `close`.
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

    /// A dataset build replays the strategy's warmup before its first day.
    #[test]
    fn the_features_declare_the_strategys_warmup() {
        assert_eq!(features_of(CONFIG).warmup(), SignedDuration::from_hours(3));
    }

    /// Each table feeds one feature of one bar type.
    #[test]
    fn the_averages_of_one_bar_type_are_named_after_their_tables() {
        assert_eq!(features_of(CONFIG).names(), ["fast", "slow"]);
    }

    /// Each table feeds a feature of each bar type.
    #[test]
    fn the_averages_of_several_bar_types_are_named_after_each_and_their_tables_in_order() {
        assert_eq!(
            features_of(&with_silver()).names(),
            [
                format!("{GOLD}:fast"),
                format!("{GOLD}:slow"),
                format!("{SILVER}:fast"),
                format!("{SILVER}:slow"),
            ]
        );
    }

    /// Closes of 1, 2 and 3: Nautilus' averages start from the mean of
    /// their first period, then weigh each close 2 / (period + 1). The
    /// fast one is 1.5 after two bars, then 1.5 + 2/3 (3 - 1.5) = 2.5; the
    /// slow one is warm after three, at 2.
    #[test]
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

    /// Rows and models of a fast average of 2 bars are not those of one of
    /// 4, with no version bump.
    #[test]
    fn a_changed_period_changes_the_fingerprint() {
        let changed = CONFIG.replace("period = 2", "period = 4");

        assert_ne!(
            features_of(&changed).fingerprint(),
            features_of(CONFIG).fingerprint()
        );
    }

    /// Rows and models of gold's minute bars are not those of its
    /// 5-minute bars, though one series names the features after their
    /// tables alone.
    #[test]
    fn another_bar_spec_changes_the_fingerprint() {
        let five_minutes = CONFIG.replace("step = 1,", "step = 5,");

        assert_ne!(
            features_of(&five_minutes).fingerprint(),
            features_of(CONFIG).fingerprint()
        );
    }

    /// Rows and models of gold's bars are not those of silver's, though
    /// one series names the features after their tables alone.
    #[test]
    fn another_instrument_changes_the_fingerprint() {
        let silver = CONFIG.replace("XAUUSD", "XAGUSD");

        assert_ne!(
            features_of(&silver).fingerprint(),
            features_of(CONFIG).fingerprint()
        );
    }

    /// Nautilus parses a `PriceType` in any case: `"mid"` is the price
    /// `"MID"` is, and keys the same rows and models.
    #[test]
    fn a_price_type_written_in_another_case_keeps_the_fingerprint() {
        let price_type = |name: &str| {
            CONFIG.replace(
                "period = 2",
                &format!("period = 2\nprice_type = \"{name}\""),
            )
        };

        assert_eq!(
            features_of(&price_type("mid")).fingerprint(),
            features_of(&price_type("MID")).fingerprint()
        );
    }
}
