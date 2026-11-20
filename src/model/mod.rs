//! The model harness: what a strategy trading on a learned model shares with every other, in
//! backtest, live and the training table alike.
//!
//! - `features` is the strategy's feature set: Nautilus indicators fed by Nautilus.
//! - `decide` is when the strategy decides, and the feature row it publishes at each decision.
//! - `targets` is the outcome of each decision, read from real orders on the simulated venue of a
//!   dataset run.
//! - `dataset` is the settled rows of a strategy, stored by day in the catalog, its coverage the
//!   cache of the days built.
//! - `learner` is the model port: its learners fit and load the models that predict.
//! - `calibration` turns a pair's predictions of R into each side's expected R, and a meta's into
//!   its expected R.
//! - `payoff` is the side a decision trades on its expected R.
//! - `config` is the model section of a strategy's config.
//! - `blueprint` is what the config alone determines about the model, validated: the dataset, the
//!   stores, the training, the walk-forward and the handle built from it.
//! - `train` trains at a time: it fits a model of each side traded on a window of the dataset less
//!   its calibration slice, calibrates them together on that slice, and publishes them with the
//!   meta's as one model set in the `store`, judging none of their figures.
//! - `earnings` is what the decision took and earned on rows, and `score` how its expected R met
//!   the R realized, which a backtest's scores read.
//! - `deciding` is the decision itself: the side picked among the models valid at a time, and the
//!   meta's margin.
//! - `handle` is what the strategy holds and decides through. It trades on the latest model set
//!   valid at each decision, its sides and its meta together, or opens the decision's target in a
//!   dataset run.
//! - `schedule` is when a model is retrained, on a fixed UTC grid, or that it never is, and what
//!   each train does.
//! - `launcher` starts a live strategy's retrains, one at a time, in a child process.
//! - `walk` is a backtest on the models in the store, trained by `train`. It is refused before its
//!   run without a model to trade on at its start, and scores its models out of sample after.
//! - `meta` is meta-labeling: a meta model decides whether to take the trade of the side picked, on
//!   the net R it expects of it, trained on the out-of-sample predictions of the strategy's side
//!   models.
//!
//! `ModelStrategy` is what a strategy trading on a model gives the harness: the binary's backtest,
//! live, train and dataset runs are generic over it. Its part of its config builds its features
//! (`ModelPart`), and the harness answers from them what it reads (`reads`), which the load checks
//! `[data]` against. Its model's blueprint (`ModelStrategy::blueprint`) is built from them and from
//! its config, once for each strategy built, and handed to the strategy, whose model handle is
//! built from it (`ModelStrategy::with_blueprint`).

pub mod blueprint;
pub mod calibration;
pub mod config;
pub mod dataset;
pub mod decide;
pub mod deciding;
pub mod earnings;
pub mod features;
pub mod handle;
pub mod launcher;
pub mod learner;
pub mod meta;
pub mod payoff;
pub mod report;
pub mod schedule;
pub mod score;
pub mod store;
pub mod targets;
pub mod train;
pub mod walk;

use std::{fmt::Debug, path::Path, rc::Rc};

use nautilus_common::{actor::DataActorNative, component::Component, config::ConfigResult};
use nautilus_trading::strategy::{Strategy, StrategyNative};

use self::{
    blueprint::ModelBlueprint, dataset::Dataset, features::FeatureSet, handle::Mode,
    train::Training, walk::WalkForward,
};
use crate::strategy::{Part, Series, StandardConfig, requirements::catalog_keys, warmup::Warmup};

/// The part of a strategy trading on a model (`Part::TAKES_MODEL`), which builds the features its
/// model decides on from its config. The load and every run build them here, so the features
/// checked against `[data]` are the features run. The harness answers what the strategy reads from
/// them (`reads`), and the strategy decides on them (`ModelStrategy`).
pub trait ModelPart: Part {
    /// Builds the features its model decides on from `config`: one for each indicator it declares
    /// (`Part::INDICATORS`), built from its table in `config.indicators`
    /// (`features::FeatureSetBuilder`), or a raw feature of custom data. The load calls it once
    /// those tables build.
    ///
    /// # Errors
    ///
    /// Returns an error if the strategy cannot build them from `config`, reporting its own errors
    /// each under its field.
    fn features(config: &StandardConfig<Self>) -> ConfigResult<Rc<FeatureSet>>;
}

/// Returns what a strategy trading on a model reads (`Part::reads`, its part's answer): the series
/// its features read (`ModelPart::features`), each whole, as the cache keys its points. These are
/// their bar types (`FeatureSet::bar_types`), tick series (`FeatureSet::ticks`) and custom series
/// (`FeatureSet::custom`), then each series `[data]` lists that the target's ATR, if it has one,
/// reads: its bars, or the ticks they are built from (`catalog_keys`). The ATR is built from what
/// the strategy reads of the traded instrument (`TargetConfig::atr_bars`), so it reads a series of
/// `[data]` if one serves it, and needs none listed.
///
/// # Errors
///
/// Returns an error if the part fails to build the features (`ModelPart::features`).
pub fn reads<P: ModelPart>(config: &StandardConfig<P>) -> ConfigResult<Vec<Series>> {
    let features = P::features(config)?;
    let mut read: Vec<Series> = features
        .bar_types()
        .into_iter()
        .map(Series::Bars)
        .chain(features.ticks().into_iter().map(Series::Ticks))
        .chain(features.custom().into_iter().map(Series::Custom))
        .collect();
    let model = config.model();

    // An ATR without bars is the target's violation (`ModelConfig::validate`)
    if let Ok(Some(atr)) = model
        .target
        .atr_bars(model.decision, &config.data_requirements())
    {
        let built_of = catalog_keys(&[atr.bar_type]);
        let atr_reads = |series: &Series| match series {
            Series::Bars(bar_type) => *bar_type == atr.bar_type,
            Series::Ticks(ticks) => built_of.contains(&ticks.catalog_key()),
            Series::Custom(_) => false,
        };
        let by_atr: Vec<Series> = config
            .data
            .series
            .iter()
            .filter(|series| atr_reads(series) && !read.contains(series))
            .cloned()
            .collect();
        read.extend(by_atr);
    }
    Ok(read)
}

/// A strategy trading on a model, as the binary runs it: built from its config in a `Mode`, it
/// decides on the features its part builds from that config (`ModelPart::features`). Its model
/// handle, dataset, training and walk forward are built from its model's blueprint (`blueprint`). A
/// Nautilus strategy, as a node runs it.
pub trait ModelStrategy:
    Strategy + StrategyNative + DataActorNative + Component + Debug + Sized + 'static
{
    /// Its part of its config, which trades on a model and builds its features.
    type Params: ModelPart;

    /// Returns the strategy of `config`, deciding on the model `blueprint` describes
    /// (`ModelStrategy::blueprint`), with its model handle built from it in `mode`
    /// (`ModelHandle::new`).
    ///
    /// # Errors
    ///
    /// Returns an error if the handle cannot be built in `mode`, or if Nautilus refuses the base.
    fn with_blueprint(
        config: StandardConfig<Self::Params>,
        blueprint: ModelBlueprint,
        mode: Mode,
    ) -> anyhow::Result<Self>;

    /// Returns the warmup of its model handle, which a live node sizes its cache from.
    fn warmup(&self) -> Rc<Warmup>;

    /// Returns the strategy of `config`, deciding on the model of its blueprint, built once from
    /// `config` (`blueprint`), with its model handle in `mode`. Each call builds a blueprint of its
    /// own, and so features of its own, which the strategy's handle feeds.
    ///
    /// # Errors
    ///
    /// Returns an error if `blueprint` or `with_blueprint` fails.
    fn new(config: StandardConfig<Self::Params>, mode: Mode) -> anyhow::Result<Self> {
        let blueprint = Self::blueprint(&config)?;
        Self::with_blueprint(config, blueprint, mode)
    }

    /// Returns the blueprint of the model of the strategy `config` describes: its id, the features
    /// its part builds from `config` (`ModelPart::features`), the config's model
    /// (`StandardConfig::model`) and its data requirements.
    ///
    /// # Errors
    ///
    /// Returns an error if `ModelPart::features` or `ModelBlueprint::new` fails.
    fn blueprint(config: &StandardConfig<Self::Params>) -> anyhow::Result<ModelBlueprint> {
        let features = Self::Params::features(config)?;
        ModelBlueprint::new(
            config.strategy_id(),
            features,
            config.model(),
            config.data_requirements(),
        )
    }

    /// Returns the dataset of the strategy `config` describes, in the catalog at `catalog`. Its
    /// replay runs the strategy in `Mode::Dataset`.
    ///
    /// # Errors
    ///
    /// Returns an error if `blueprint` or `ModelBlueprint::dataset` fails.
    fn dataset(config: &StandardConfig<Self::Params>, catalog: &Path) -> anyhow::Result<Dataset> {
        Self::blueprint(config)?.dataset(catalog)
    }

    /// Returns the training of the model of the strategy `config` describes, on its dataset in the
    /// catalog at `catalog`, with its models in the store under `models`.
    ///
    /// # Errors
    ///
    /// Returns an error if `blueprint` or `ModelBlueprint::training` fails.
    fn training(
        config: &StandardConfig<Self::Params>,
        catalog: &Path,
        models: &Path,
    ) -> anyhow::Result<Training> {
        Self::blueprint(config)?.training(catalog, models)
    }

    /// Returns the backtests of the model of the strategy `config` describes, on its dataset in the
    /// catalog at `catalog`, with its models in the store under `models`: the check of their start
    /// and their scores.
    ///
    /// # Errors
    ///
    /// Returns an error if `blueprint` or `ModelBlueprint::walk_forward` fails.
    fn walk_forward(
        config: &StandardConfig<Self::Params>,
        catalog: &Path,
        models: &Path,
    ) -> anyhow::Result<WalkForward> {
        Self::blueprint(config)?.walk_forward(catalog, models)
    }
}

#[cfg(test)]
mod tests {
    use std::slice;

    use jiff::SignedDuration;
    use nautilus_common::config::ConfigError;
    use nautilus_indicators::indicator::Indicator;
    use nautilus_model::{data::NautilusDataType, identifiers::StrategyId};
    use nautilus_trading::strategy::StrategyConfig;
    use rstest::rstest;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::{
        model::features::Feature,
        strategy::{Declaration, Declared, EmaArgs, IndicatorArgs},
    };

    /// A custom indicator of the probe over a span of time: its value is the span in seconds,
    /// initialized from the start.
    struct Span(SignedDuration);

    impl Indicator for Span {
        fn name(&self) -> String {
            "Span".to_string()
        }

        fn has_inputs(&self) -> bool {
            false
        }

        fn initialized(&self) -> bool {
            true
        }

        fn reset(&mut self) {}
    }

    impl Feature for Span {
        fn value(&self) -> f64 {
            self.0.as_secs_f64()
        }
    }

    /// The arguments of `Span`: a positive duration, written as `[data] warmup` is.
    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct SpanArgs {
        span: SignedDuration,
    }

    impl IndicatorArgs for SpanArgs {
        type Indicator = Span;

        fn build(self) -> anyhow::Result<Span> {
            anyhow::ensure!(self.span.is_positive(), "span must be positive");
            Ok(Span(self.span))
        }
    }

    /// The probe's feature on each series it lists, `[indicators.span]`.
    const SPAN: Declared<SpanArgs> = Declared::new("span");
    /// An EMA the probe declares for its own logic, which its strategy would build itself
    /// (`config.indicators.build(FILTER)`), not as a feature, `[indicators.filter]`.
    const FILTER: Declared<EmaArgs> = Declared::new("filter");

    /// The part of a strategy trading on a model whose features read the series its own `features`
    /// lists, its span on each, a custom series' last value; its filter is its own logic's.
    #[derive(Debug, Clone, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ProbeParams {
        features: Vec<Series>,
    }

    impl Part for ProbeParams {
        const INSTRUMENTS: usize = 1;
        const INDICATORS: &'static [&'static dyn Declaration] = &[&SPAN, &FILTER];
        const TAKES_MODEL: bool = true;

        fn default_base() -> StrategyConfig {
            StrategyConfig {
                strategy_id: Some(StrategyId::from("PROBE-001")),
                ..Default::default()
            }
        }

        fn reads(config: &StandardConfig<Self>) -> ConfigResult<Vec<Series>> {
            reads(config)
        }
    }

    impl ModelPart for ProbeParams {
        fn features(config: &StandardConfig<Self>) -> ConfigResult<Rc<FeatureSet>> {
            let tables = &config.indicators;
            let builder = config.own.features.iter().enumerate().try_fold(
                FeatureSet::builder(),
                |builder, (i, series)| match series {
                    Series::Bars(_) => Ok(builder.feature(slice::from_ref(series), SPAN, tables)),
                    Series::Ticks(ticks) => match &ticks.data {
                        NautilusDataType::QuoteTick | NautilusDataType::TradeTick => {
                            Ok(builder.feature(slice::from_ref(series), SPAN, tables))
                        }
                        data => Err(ConfigError::invalid_value(
                            "features",
                            format!("the probe reads no {data}"),
                        )),
                    },
                    Series::Custom(custom) => Ok(builder.custom(custom, &i.to_string(), |_| 0.0)),
                },
            )?;
            builder
                .build()
                .map_err(|e| ConfigError::invalid_value("features", format!("{e:#}")))
        }
    }

    const GOLD_BARS: &str = r#"{ data = "bars", instrument_id = "XAUUSD.ICMARKETS", bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" } }"#;
    const GC_TRADES: &str = r#"{ data = "trades", instrument_id = "GC.GLBX" }"#;
    /// The MID bars of the GC root, which Nautilus never builds: it builds a root's bars from its
    /// contracts' trades.
    const GC_ROOT_BARS: &str = r#"{ data = "bars", instrument_id = "GC.GLBX", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } }"#;
    /// Gold's hourly bars of mids.
    const GOLD_HOURS: &str = r#"{ data = "bars", instrument_id = "XAUUSD.ICMARKETS", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } }"#;
    /// Gold's quotes.
    const GOLD_QUOTES: &str = r#"{ data = "quotes", instrument_id = "XAUUSD.ICMARKETS" }"#;

    /// The probe's tables: a span of 15 minutes, a filter of 5.
    const TABLES: &str = r#"
        [indicators.span]
        span = "15m"

        [indicators.filter]
        period = 5
    "#;

    /// The probe trading gold, loaded: its features read `features`, its `[data]` lists `series`,
    /// and it decides hourly on the `target`, of `TABLES`.
    fn load(
        features: &[&str],
        series: &[&str],
        target: &str,
    ) -> anyhow::Result<StandardConfig<ProbeParams>> {
        load_of(features, series, target, TABLES)
    }

    /// The probe as `load` has it, its `[indicators]` `tables`, loaded after registering the custom
    /// data types helix compiles, as the process does at its start.
    fn load_of(
        features: &[&str],
        series: &[&str],
        target: &str,
        tables: &str,
    ) -> anyhow::Result<StandardConfig<ProbeParams>> {
        crate::venues::register_custom_data();
        StandardConfig::load(&toml_of(features, series, target, tables))
    }

    /// The TOML of the probe as `load_of` has it.
    fn toml_of(features: &[&str], series: &[&str], target: &str, tables: &str) -> String {
        format!(
            r#"
            features = [{}]

            [data]
            series = [{}]
            warmup = "14h"

            {tables}

            [trade]
            instruments = [{{ instrument_id = "XAUUSD.ICMARKETS" }}]
            risk = "0.5%"

            [model]
            dataset = {{ commission = {{ rate = "0.00003" }}, balance = "10000000 USD" }}
            decision = {{ every = "1h" }}
            target = {target}
            retrain = "24h"
            window = "48h"
            calibration = "12h"
            margins = {{ long = "0.5R", short = "0.5R" }}
            min_rows = 30
            learner = {{ kind = "baseline" }}
            "#,
            features.join(", "),
            series.join(", "),
        )
    }

    /// A time-only target, its unit measured in Nautilus' ATR of 14 hourly bars of gold.
    const TIME_ONLY: &str = r#"{ limit = "30m", atr = { period = 14 } }"#;
    /// A bracket in ATRs: Nautilus' ATR of 14 hourly bars of gold.
    const BRACKET: &str =
        r#"{ bracket = { take_profit = { atr = 2 }, stop = { atr = 1 } }, limit = "45m" }"#;
    /// A bracket in basis points, which has no ATR.
    const IN_BPS: &str =
        r#"{ bracket = { take_profit = { bps = 20 }, stop = { bps = 10 } }, limit = "45m" }"#;

    /// Each config error refusing the probe's config of `features`, `series` and `target` (see
    /// `load`), as a field and a message.
    fn refusals(features: &[&str], series: &[&str], target: &str) -> Vec<(String, String)> {
        refusals_of(&load(features, series, target).unwrap_err())
    }

    /// Each config error of `err`: its field and its message.
    fn refusals_of(err: &anyhow::Error) -> Vec<(String, String)> {
        let errors = match err.downcast_ref::<ConfigError>() {
            Some(ConfigError::Multiple { errors }) => errors.clone(),
            Some(err) => vec![err.clone()],
            None => panic!("not a config error: {err}"),
        };
        errors
            .into_iter()
            .map(|e| match &e {
                ConfigError::InvalidValue { field, .. } | ConfigError::Duplicate { field, .. } => {
                    (field.clone(), e.to_string())
                }
                e => panic!("unexpected config error {e:?}"),
            })
            .collect()
    }

    /// Features reading every series `[data]` lists, in another order, gold's bars and GC's trades,
    /// or gold's bars and the 10-year yields, a custom series.
    #[rstest]
    #[case::of_bars_and_trades(&[GOLD_BARS, GC_TRADES], &[GC_TRADES, GOLD_BARS])]
    #[case::of_a_custom_series(&[GOLD_BARS, US10Y], &[GOLD_BARS, US10Y])]
    fn features_reading_every_series_of_data_and_no_other_load(
        #[case] features: &[&str],
        #[case] series: &[&str],
    ) {
        load(features, series, TIME_ONLY).unwrap();
    }

    /// One series mismatched, refused under its field, naming it and no other:
    ///
    /// - GC's trades, which `[data]` lists and no feature reads, under their listing. A bracket's
    ///   ATR does not read them either, as it is built of the quotes of the gold traded.
    /// - GC's trades, which the features read and `[data]` does not list, under `data.series`.
    /// - The MID bars of the GC root, listed and read, under their listing: Nautilus builds a
    ///   root's bars from its contracts' trades, so these would never come.
    /// - The 2-year yields, which the features read and `[data]` does not list beside the 10-year,
    ///   by their identifier.
    /// - The 2-year yields, which `[data]` lists beside the 10-year the features read, as any
    ///   series read by none: a strategy reading custom data in its `on_data` declares it in what
    ///   it reads.
    #[rstest]
    #[case::listed_and_read_by_no_feature(
        &[GOLD_BARS],
        &[GOLD_BARS, GC_TRADES],
        TIME_ONLY,
        "data.series[1]",
        "TradeTick of GC.GLBX",
        None
    )]
    #[case::listed_and_read_by_neither_a_feature_nor_the_target_atr(
        &[GOLD_BARS],
        &[GOLD_BARS, GC_TRADES],
        BRACKET,
        "data.series[1]",
        "TradeTick of GC.GLBX",
        None
    )]
    #[case::read_and_not_listed(
        &[GOLD_BARS, GC_TRADES],
        &[GOLD_BARS],
        TIME_ONLY,
        "data.series",
        "TradeTick of GC.GLBX",
        None
    )]
    #[case::bars_on_a_root_not_built_from_its_trades(
        &[GC_ROOT_BARS],
        &[GC_ROOT_BARS],
        TIME_ONLY,
        "data.series[0]",
        "GC.GLBX-1-HOUR-MID-INTERNAL",
        None
    )]
    #[case::a_custom_series_read_and_not_listed(
        &[GOLD_BARS, US10Y, US2Y],
        &[GOLD_BARS, US10Y],
        TIME_ONLY,
        "data.series",
        "TREASURY-2Y",
        Some("TREASURY-10Y")
    )]
    #[case::a_custom_series_listed_and_read_by_no_feature(
        &[GOLD_BARS, US10Y],
        &[GOLD_BARS, US2Y, US10Y],
        TIME_ONLY,
        "data.series[1]",
        "TREASURY-2Y",
        None
    )]
    fn a_series_of_data_no_feature_reads_is_refused_under_its_field_naming_it(
        #[case] features: &[&str],
        #[case] series: &[&str],
        #[case] target: &str,
        #[case] field: &str,
        #[case] named: &str,
        #[case] unnamed: Option<&str>,
    ) {
        let refusals = refusals(features, series, target);

        let [(refused, message)] = &refusals[..] else {
            panic!("not one refusal: {refusals:?}");
        };
        assert_eq!(refused, field);
        assert!(message.contains(named), "{message}");
        if let Some(unnamed) = unnamed {
            assert!(!message.contains(unnamed), "{message}");
        }
    }

    /// Hourly bars of silver the features read and `[data]` does not list, the trades of GC
    /// `[data]` lists and no feature reads: each refused under its field, naming it.
    #[rstest]
    fn both_mismatches_are_refused_at_once() {
        let silver_bars = GOLD_BARS
            .replace("XAUUSD", "XAGUSD")
            .replace("MINUTE", "HOUR");

        let mut refusals = refusals(
            &[GOLD_BARS, &silver_bars],
            &[GOLD_BARS, GC_TRADES],
            TIME_ONLY,
        );
        refusals.sort();

        let [(missing, of_missing), (unread, of_unread)] = &refusals[..] else {
            panic!("not two refusals: {refusals:?}");
        };
        assert_eq!(
            (missing.as_str(), unread.as_str()),
            ("data.series", "data.series[1]")
        );
        assert!(
            of_missing.contains("XAGUSD.ICMARKETS-1-HOUR-MID-INTERNAL"),
            "{of_missing}"
        );
        assert!(of_unread.contains("TradeTick of GC.GLBX"), "{of_unread}");
    }

    /// The features read GC's trades alone. The target's ATR, a bracket's or a time-only target's,
    /// is built of what `[data]` lists for it: the quotes of the gold traded, or, deciding hourly,
    /// gold's hourly bars of mids. A bracket in basis points has no ATR to read them.
    #[rstest]
    #[case::ticks(GOLD_QUOTES, "QuoteTick of XAUUSD.ICMARKETS")]
    #[case::bars(GOLD_HOURS, "XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL")]
    fn ticks_the_target_atr_reads_alone_are_read(#[case] read: &str, #[case] named: &str) {
        load(&[GC_TRADES], &[GC_TRADES, read], BRACKET).unwrap();
        load(&[GC_TRADES], &[GC_TRADES, read], TIME_ONLY).unwrap();
        let refusals = refusals(&[GC_TRADES], &[GC_TRADES, read], IN_BPS);
        let [(field, message)] = &refusals[..] else {
            panic!("not one refusal: {refusals:?}");
        };
        assert_eq!(field, "data.series[1]");
        assert!(message.contains(named), "{message}");
    }

    /// An unread series, or one listed twice, and a zero target limit are both reported, each under
    /// its field.
    #[rstest]
    #[case::unread(&[GOLD_BARS, GC_TRADES])]
    #[case::listed_twice(&[GOLD_BARS, GOLD_BARS])]
    fn the_series_refusals_are_reported_with_the_other_violations(#[case] series: &[&str]) {
        let mut fields: Vec<String> = refusals(
            &[GOLD_BARS],
            series,
            r#"{ limit = "0s", atr = { period = 14 } }"#,
        )
        .into_iter()
        .map(|(field, _)| field)
        .collect();
        fields.sort();

        assert_eq!(fields, ["data.series[1]", "model.target.limit"]);
    }

    /// The features read GC's trades once, and `[data]` lists them again: refused under their
    /// second listing, naming them.
    #[rstest]
    fn a_tick_series_listed_twice_is_refused_under_its_second_listing() {
        let err = load(
            &[GOLD_BARS, GC_TRADES],
            &[GC_TRADES, GOLD_BARS, GC_TRADES],
            TIME_ONLY,
        )
        .unwrap_err();

        let Some(ConfigError::Duplicate {
            field,
            value: Some(value),
        }) = err.downcast_ref::<ConfigError>()
        else {
            panic!("not one duplicate naming the series: {err}");
        };
        assert_eq!(field, "data.series[2]");
        assert!(value.contains("TradeTick of GC.GLBX"), "{value}");
    }

    /// The part's own error building the features is the answer: no series is then refused for it.
    #[rstest]
    fn the_parts_error_building_the_features_is_reported_under_its_field() {
        let refusals = refusals(&[], &[GOLD_BARS], TIME_ONLY);
        let fields: Vec<&str> = refusals.iter().map(|(field, _)| field.as_str()).collect();
        assert_eq!(fields, ["features"]);
    }

    /// LSE's daily 10-year US yield, under the identifier `TREASURY-10Y`.
    const US10Y: &str = r#"{ data = "Custom:LseRow", identifier = "TREASURY-10Y", metadata = { source = "bond_yields", symbol = "US10Y" } }"#;
    /// LSE's daily 2-year US yield, under the identifier `TREASURY-2Y`.
    const US2Y: &str = r#"{ data = "Custom:LseRow", identifier = "TREASURY-2Y", metadata = { source = "bond_yields", symbol = "US2Y" } }"#;

    /// The 2-year yields listed under the 10-year's identifier are another data type than the
    /// 10-year's that the features read there. The cache keys a point by its whole data type, so
    /// each is refused: the listed one as read by no feature, the read one as not listed.
    #[rstest]
    fn a_custom_series_listed_under_its_identifier_with_other_metadata_is_refused_both_ways() {
        let under_10y = US2Y.replace("TREASURY-2Y", "TREASURY-10Y");

        let fields: Vec<String> =
            refusals(&[GOLD_BARS, US10Y], &[GOLD_BARS, &under_10y], TIME_ONLY)
                .into_iter()
                .map(|(field, _)| field)
                .collect();

        assert_eq!(fields, ["data.series[1]", "data.series"]);
    }

    /// The probe's features on gold's bars, of the tables `tables`, as its part builds them of the
    /// loaded config.
    fn features_of(tables: &str) -> Rc<FeatureSet> {
        ProbeParams::features(&load_of(&[GOLD_BARS], &[GOLD_BARS], TIME_ONLY, tables).unwrap())
            .unwrap()
    }

    /// The span of 15 minutes, 900 s, is its feature's value.
    #[rstest]
    fn a_feature_is_built_of_its_table() {
        assert_eq!(features_of(TABLES).values(), Some(vec![900.0]));
    }

    /// Rows and models of a span of 15 minutes are not those of one of 30, with no code of the
    /// part's own.
    #[rstest]
    fn a_features_changed_argument_changes_the_fingerprint() {
        let changed = TABLES.replace(r#""15m""#, r#""30m""#);

        assert_ne!(
            features_of(&changed).fingerprint(),
            features_of(TABLES).fingerprint()
        );
    }

    /// The rows are the same, and so is the fingerprint: of 15 minutes written as 900 seconds, the
    /// same span; and of another filter, which the part declares for its own logic and never hands
    /// to its features.
    #[rstest]
    #[case::an_argument_written_otherwise(r#""15m""#, r#""900s""#)]
    #[case::an_indicator_of_the_parts_own_logic("period = 5", "period = 6")]
    fn a_features_argument_written_otherwise_keeps_the_fingerprint(
        #[case] written: &str,
        #[case] otherwise: &str,
    ) {
        let otherwise = TABLES.replace(written, otherwise);

        assert_eq!(
            features_of(&otherwise).fingerprint(),
            features_of(TABLES).fingerprint()
        );
    }

    /// A span its arguments refuse and the filter's table missing are reported, each under its
    /// table, with the model's violation; the features, which need the span, are not built.
    #[rstest]
    fn the_tables_refusals_are_reported_with_the_other_violations() {
        let tables = TABLES
            .replace(r#""15m""#, r#""0s""#)
            .replace("[indicators.filter]\n        period = 5\n", "");

        let err = load_of(
            &[GOLD_BARS],
            &[GOLD_BARS],
            r#"{ limit = "0s", atr = { period = 14 } }"#,
            &tables,
        )
        .unwrap_err();

        let Some(ConfigError::Multiple { errors }) = err.downcast_ref::<ConfigError>() else {
            panic!("not several config errors: {err}");
        };
        let mut fields: Vec<String> = errors
            .iter()
            .map(|e| match e {
                ConfigError::MissingField { field } | ConfigError::InvalidValue { field, .. } => {
                    field.clone()
                }
                e => panic!("unexpected config error {e:?}"),
            })
            .collect();
        fields.sort();
        assert_eq!(
            fields,
            ["indicators.filter", "indicators.span", "model.target.limit"]
        );
    }
}
