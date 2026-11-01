//! The model harness: what a strategy trading on a learned model shares
//! with every other, in backtest, live and the training table alike.
//!
//! `features` is the strategy's feature set, Nautilus indicators fed by
//! Nautilus; `decide` is when the strategy decides and the feature row it
//! publishes at each decision; `targets` is the outcome of each decision,
//! read from real orders on the simulated venue of a dataset run;
//! `dataset` is the settled rows of a strategy stored by day in the
//! catalog, its coverage the cache of the days built;
//! `learner` is the model port, its learners fitting and loading the
//! models that predict; `calibration` turns their predictions into win
//! rates; `payoff` is what a side's trade is expected to make and the
//! side a decision trades;
//! `config` is the model section of a strategy's config; `train` trains
//! at a time, fitting a model of each side traded on a window of the
//! dataset and publishing in the `store` each whose `holdout`, what its
//! decisions would have earned there and its `score`, meets the minimums;
//! `handle` is
//! what the strategy holds and decides through, trading on the latest
//! model of the store valid at each decision, or opening the decision's
//! targets in a dataset run; `schedule` is when a model is retrained, on
//! a fixed UTC grid, or that it never is, and what each train does to
//! each model; `launcher`
//! starts a live strategy's retrains, one
//! at a time, in a child process; `walk` is a backtest retraining on the
//! schedule before its run and scoring its models out of sample after.
//! `meta` is meta-labeling: a meta model deciding whether to take the
//! trade of the primary, the strategy's rule or its model, trained on
//! the primary's out-of-sample predictions.
//!
//! `ModelStrategy` is what a strategy trading on a model gives the
//! harness: the binary's backtest, live, train and dataset runs are
//! generic over it. Its part of its config builds its features
//! (`ModelPart`), and the harness answers from them what it reads
//! (`reads`), which the load checks `[data]` against.

pub mod calibration;
pub mod config;
pub mod dataset;
pub mod decide;
pub mod features;
pub mod handle;
pub mod holdout;
pub mod launcher;
pub mod learner;
pub mod meta;
pub mod payoff;
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
    dataset::{Dataset, DatasetVenue},
    features::FeatureSet,
    handle::Mode,
    train::Training,
    walk::WalkForward,
};
use crate::strategy::{Part, Series, StandardConfig, catalog_series, warmup::Warmup};

/// The part of a strategy trading on a model (`Part::TAKES_MODEL`): the
/// features its model decides on, built of its config. The load and every
/// run build them here, so the features checked against `[data]` are the
/// features run: its answer of what the strategy reads is the harness'
/// (`reads`), and its strategy decides on them (`ModelStrategy`).
pub trait ModelPart: Part {
    /// The features its model decides on, as its code builds them of
    /// `config`: each of an indicator it declares (`Part::INDICATORS`),
    /// built of its table in `config.indicators`
    /// (`features::FeatureSetBuilder`), or a raw feature of custom data.
    /// The load asks once those tables build.
    ///
    /// # Errors
    ///
    /// Returns the strategy's own errors building them of `config`, each
    /// under its field.
    fn features(config: &StandardConfig<Self>) -> ConfigResult<Rc<FeatureSet>>;
}

/// What a strategy trading on a model reads (`Part::reads`, its part's
/// answer): the series its features read (`ModelPart::features`), their
/// bar types (`FeatureSet::bar_types`) and their tick series
/// (`FeatureSet::ticks`), then each series `[data]` lists that the
/// barrier's ATR reads, its bars or the ticks they are built from
/// (`catalog_series`). The ATR is built of what the strategy reads of the
/// traded instrument (`TargetsConfig::atr_bars`), so it reads a series of
/// `[data]` if one serves it, and needs none listed.
///
/// # Errors
///
/// Returns the part's errors building the features.
pub fn reads<P: ModelPart>(config: &StandardConfig<P>) -> ConfigResult<Vec<Series>> {
    let features = P::features(config)?;
    let mut read: Vec<Series> = features
        .bar_types()
        .into_iter()
        .map(Series::Bars)
        .chain(features.ticks().into_iter().map(Series::Ticks))
        .collect();
    let model = config.model();
    // An ATR without bars is the targets' violation (`ModelConfig::validate`).
    if let Ok(Some(atr)) = model
        .targets
        .atr_bars(model.decision, &config.data_requirements())
    {
        let built_of = catalog_series(&[atr.bar_type]);
        let atr_reads = |series: &Series| match series {
            Series::Bars(bar_type) => *bar_type == atr.bar_type,
            Series::Ticks(ticks) => built_of.contains(&ticks.catalog_series()),
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

/// A strategy trading on a model, as the binary runs it: built of its
/// config in a `Mode`, deciding on the features its part builds of that
/// config (`ModelPart::features`), its dataset, its training and its walk
/// forward built of them and of the config's model
/// (`StandardConfig::model`). A Nautilus strategy, as a node runs it.
pub trait ModelStrategy:
    Strategy + StrategyNative + DataActorNative + Component + Debug + Sized + 'static
{
    /// Its own part of its config, which trades on a model and builds its
    /// features.
    type Params: ModelPart;

    /// The strategy of `config`, deciding on `features`, those its part
    /// builds of `config`, its model handle in `mode`.
    ///
    /// # Errors
    ///
    /// Returns an error if the handle cannot be built in `mode`, or if
    /// Nautilus refuses the base.
    fn with_features(
        config: StandardConfig<Self::Params>,
        features: Rc<FeatureSet>,
        mode: Mode,
    ) -> anyhow::Result<Self>;

    /// Its warmup, its model handle's: a live node sizes its cache from
    /// it.
    fn warmup(&self) -> Rc<Warmup>;

    /// The strategy of `config`, deciding on the features its part builds
    /// (`ModelPart::features`), its model handle in `mode`.
    ///
    /// # Errors
    ///
    /// Returns an error as `ModelPart::features` and `with_features` do.
    fn new(config: StandardConfig<Self::Params>, mode: Mode) -> anyhow::Result<Self> {
        let features = Self::Params::features(&config)?;
        Self::with_features(config, features, mode)
    }

    /// The dataset of the strategy `config` describes, run on `venue`, in
    /// the catalog at `catalog`; its replay runs the strategy in
    /// `Mode::Dataset`.
    ///
    /// # Errors
    ///
    /// Returns an error as `ModelPart::features` and `Dataset::new` do.
    fn dataset(
        config: &StandardConfig<Self::Params>,
        catalog: &Path,
        venue: &DatasetVenue,
    ) -> anyhow::Result<Dataset> {
        let (features, model) = (Self::Params::features(config)?, config.model());
        Dataset::new(
            catalog,
            config.strategy_id(),
            &features,
            model.decision,
            model.targets,
            &config.data_requirements(),
            venue,
        )
    }

    /// The training of the model of the strategy `config` describes, on
    /// its dataset, run on `venue`, in the catalog at `catalog`, and its
    /// models in the store under `models`.
    ///
    /// # Errors
    ///
    /// Returns an error as `ModelPart::features` and `Training::new` do.
    fn training(
        config: &StandardConfig<Self::Params>,
        catalog: &Path,
        models: &Path,
        venue: &DatasetVenue,
    ) -> anyhow::Result<Training> {
        let features = Self::Params::features(config)?;
        Training::new(
            catalog,
            models,
            config.strategy_id(),
            &features,
            config.model(),
            &config.data_requirements(),
            venue,
        )
    }

    /// The backtests of the model of the strategy `config` describes, on
    /// its dataset, run on `venue`, in the catalog at `catalog`, and its
    /// models in the store under `models`: their retrains and their
    /// scores.
    ///
    /// # Errors
    ///
    /// Returns an error as `ModelPart::features` and `WalkForward::new` do.
    fn walk_forward(
        config: &StandardConfig<Self::Params>,
        catalog: &Path,
        models: &Path,
        venue: &DatasetVenue,
    ) -> anyhow::Result<WalkForward> {
        let features = Self::Params::features(config)?;
        WalkForward::new(
            catalog,
            models,
            config.strategy_id(),
            &features,
            config.model(),
            &config.data_requirements(),
            venue,
        )
    }
}

#[cfg(test)]
mod tests {
    use jiff::SignedDuration;
    use nautilus_common::config::ConfigError;
    use nautilus_indicators::indicator::Indicator;
    use nautilus_model::{data::NautilusDataType, identifiers::StrategyId};
    use nautilus_trading::strategy::StrategyConfig;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::{
        model::features::Feature,
        strategy::{Declaration, Declared, EmaArgs, IndicatorArgs},
    };

    /// A custom indicator of the probe over a span of time: its value the
    /// span in seconds, initialized from the start.
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

    /// The arguments of `Span`: a positive duration, written as `[data]
    /// warmup` is.
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
    /// An EMA the probe declares for its own logic, which its strategy
    /// would build itself (`config.indicators.build(FILTER)`), not as a
    /// feature, `[indicators.filter]`.
    const FILTER: Declared<EmaArgs> = Declared::new("filter");

    /// The part of a strategy trading on a model whose features read the
    /// series its own `features` lists, its span on each, a custom
    /// data's last value; its filter is its own logic's.
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
            let builder = config.own.features.iter().enumerate().fold(
                FeatureSet::builder(),
                |builder, (i, series)| match series {
                    Series::Bars(bar_type) => builder.bars(*bar_type, SPAN, tables),
                    Series::Ticks(ticks) => match &ticks.data {
                        NautilusDataType::QuoteTick => {
                            builder.quotes(ticks.instrument_id, SPAN, tables)
                        }
                        NautilusDataType::TradeTick => {
                            builder.trades(ticks.instrument_id, SPAN, tables)
                        }
                        NautilusDataType::Custom { type_name } => {
                            builder.data(type_name, ticks.instrument_id, &i.to_string(), |_| 0.0)
                        }
                        data => panic!("the probe reads no {data}"),
                    },
                },
            );
            builder
                .build()
                .map_err(|e| ConfigError::invalid_value("features", format!("{e:#}")))
        }
    }

    const GOLD_BARS: &str = r#"{ data = "bars", instrument_id = "XAUUSD.ICMARKETS", bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" } }"#;
    const GC_TRADES: &str = r#"{ data = "trades", instrument_id = "GC.GLBX" }"#;

    /// The probe's tables: a span of 15 minutes, a filter of 5.
    const TABLES: &str = r#"
        [indicators.span]
        span = "15m"

        [indicators.filter]
        period = 5
    "#;

    /// The probe trading gold, its features reading `features`, its
    /// `[data]` listing `series`, deciding hourly on the `targets`, of
    /// `TABLES`, loaded.
    fn load(
        features: &[&str],
        series: &[&str],
        targets: &str,
    ) -> anyhow::Result<StandardConfig<ProbeParams>> {
        load_of(features, series, targets, TABLES)
    }

    /// The probe as `load` has it, its `[indicators]` `tables`.
    fn load_of(
        features: &[&str],
        series: &[&str],
        targets: &str,
        tables: &str,
    ) -> anyhow::Result<StandardConfig<ProbeParams>> {
        let label = if targets.contains("barrier") {
            "barrier"
        } else {
            "horizon"
        };
        StandardConfig::load(&format!(
            r#"
            features = [{}]

            [data]
            series = [{}]
            warmup = "14h"

            {tables}

            [trade]
            instruments = [{{ instrument_id = "XAUUSD.ICMARKETS", trade_size = "1" }}]

            [model]
            decision = {{ every = "1h" }}
            targets = {targets}
            label = "{label}"
            retrain = "24h"
            window = "48h"
            holdout = "12h"
            margins = {{ long = 0.5, short = 0.5 }}
            min_pnl = 0.5
            min_trades = 30
            learner = {{ kind = "baseline" }}
            "#,
            features.join(", "),
            series.join(", "),
        ))
    }

    const HORIZON: &str = r#"{ horizon = "30m" }"#;
    /// A barrier in ATRs: Nautilus' ATR of 14 hourly bars of gold.
    const BARRIER: &str =
        r#"{ barrier = { take_profit = { atr = 2 }, stop = { atr = 1 }, limit = "45m" } }"#;

    /// Each config error refusing the probe's config of `features`,
    /// `series` and `targets` (see `load`): its field and its message.
    fn refusals(features: &[&str], series: &[&str], targets: &str) -> Vec<(String, String)> {
        let err = load(features, series, targets).unwrap_err();
        let errors = match err.downcast_ref::<ConfigError>() {
            Some(ConfigError::Multiple { errors }) => errors.clone(),
            Some(err) => vec![err.clone()],
            None => panic!("not a config error: {err}"),
        };
        errors
            .into_iter()
            .map(|err| match &err {
                ConfigError::InvalidValue { field, .. } | ConfigError::Duplicate { field, .. } => {
                    (field.clone(), err.to_string())
                }
                err => panic!("unexpected config error {err:?}"),
            })
            .collect()
    }

    #[test]
    fn features_reading_every_series_of_data_and_no_other_load() {
        load(&[GOLD_BARS, GC_TRADES], &[GC_TRADES, GOLD_BARS], HORIZON).unwrap();
    }

    #[test]
    fn a_series_of_data_no_feature_reads_is_refused_under_its_field_naming_it() {
        let [(field, message)] = &refusals(&[GOLD_BARS], &[GOLD_BARS, GC_TRADES], HORIZON)[..]
        else {
            panic!("not one refusal");
        };

        assert_eq!(field, "data.series[1]");
        assert!(message.contains("TradeTick of GC.GLBX"), "{message}");
    }

    #[test]
    fn a_series_the_features_read_that_data_does_not_list_is_refused_naming_it() {
        let [(field, message)] = &refusals(&[GOLD_BARS, GC_TRADES], &[GOLD_BARS], HORIZON)[..]
        else {
            panic!("not one refusal");
        };

        assert_eq!(field, "data.series");
        assert!(message.contains("TradeTick of GC.GLBX"), "{message}");
    }

    /// Hourly bars of silver the features read and `[data]` does not list,
    /// the trades of GC `[data]` lists and no feature reads: each refused
    /// under its field, naming it.
    #[test]
    fn both_mismatches_are_refused_at_once() {
        let silver_bars = GOLD_BARS
            .replace("XAUUSD", "XAGUSD")
            .replace("MINUTE", "HOUR");

        let mut refusals = refusals(&[GOLD_BARS, &silver_bars], &[GOLD_BARS, GC_TRADES], HORIZON);
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

    /// Features on the MID bars of the GC root, listed: Nautilus builds a
    /// root's bars from its contracts' trades, so these would never come,
    /// refused under their series, naming them.
    #[test]
    fn bars_on_a_root_not_built_from_its_trades_are_refused_under_their_series() {
        let root_bars = r#"{ data = "bars", instrument_id = "GC.GLBX", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } }"#;

        let refusals = refusals(&[root_bars], &[root_bars], HORIZON);

        let [(field, message)] = &refusals[..] else {
            panic!("not one refusal: {refusals:?}");
        };
        assert_eq!(field, "data.series[0]");
        assert!(message.contains("GC.GLBX-1-HOUR-MID-INTERNAL"), "{message}");
    }

    /// The features read GC's trades alone; the barrier's ATR is built of
    /// the quotes of the gold traded, which `[data]` lists for it.
    #[test]
    fn ticks_the_barrier_atr_reads_alone_are_read() {
        let gold_quotes = r#"{ data = "quotes", instrument_id = "XAUUSD.ICMARKETS" }"#;

        load(&[GC_TRADES], &[GC_TRADES, gold_quotes], BARRIER).unwrap();
        let refusals = refusals(&[GC_TRADES], &[GC_TRADES, gold_quotes], HORIZON);
        let [(field, message)] = &refusals[..] else {
            panic!("not one refusal: {refusals:?}");
        };
        assert_eq!(field, "data.series[1]");
        assert!(
            message.contains("QuoteTick of XAUUSD.ICMARKETS"),
            "{message}"
        );
    }

    /// Deciding hourly, the barrier's ATR runs on gold's hourly bars of
    /// mids, which `[data]` lists and no feature reads.
    #[test]
    fn bars_the_barrier_atr_runs_on_alone_are_read() {
        let gold_hours = GOLD_BARS.replace("MINUTE", "HOUR");

        load(&[GC_TRADES], &[GC_TRADES, &gold_hours], BARRIER).unwrap();
        let refusals = refusals(&[GC_TRADES], &[GC_TRADES, &gold_hours], HORIZON);
        let [(field, message)] = &refusals[..] else {
            panic!("not one refusal: {refusals:?}");
        };
        assert_eq!(field, "data.series[1]");
        assert!(
            message.contains("XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL"),
            "{message}"
        );
    }

    /// The barrier's ATR reads what it is built from, and nothing more:
    /// its bars of gold's quotes leave the trades of GC unread.
    #[test]
    fn a_series_neither_the_features_nor_the_barrier_atr_read_is_refused() {
        let refusals = refusals(&[GOLD_BARS], &[GOLD_BARS, GC_TRADES], BARRIER);
        let [(field, message)] = &refusals[..] else {
            panic!("not one refusal: {refusals:?}");
        };
        assert_eq!(field, "data.series[1]");
        assert!(message.contains("TradeTick of GC.GLBX"), "{message}");
    }

    /// An unread series and a horizon of an hour decided every hour are
    /// both reported, each under its field.
    #[test]
    fn the_series_refusals_are_reported_with_the_other_violations() {
        let mut fields: Vec<String> = refusals(
            &[GOLD_BARS],
            &[GOLD_BARS, GC_TRADES],
            r#"{ horizon = "1h" }"#,
        )
        .into_iter()
        .map(|(field, _)| field)
        .collect();
        fields.sort();

        assert_eq!(fields, ["data.series[1]", "model.targets.horizon"]);
    }

    /// The features read GC's trades once, and `[data]` lists them again:
    /// refused under their second listing, naming them.
    #[test]
    fn a_tick_series_listed_twice_is_refused_under_its_second_listing() {
        let err = load(
            &[GOLD_BARS, GC_TRADES],
            &[GC_TRADES, GOLD_BARS, GC_TRADES],
            HORIZON,
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

    /// A series listed twice and a horizon of an hour decided every hour
    /// are both reported, each under its field.
    #[test]
    fn a_series_listed_twice_is_reported_with_the_other_violations() {
        let mut fields: Vec<String> = refusals(
            &[GOLD_BARS],
            &[GOLD_BARS, GOLD_BARS],
            r#"{ horizon = "1h" }"#,
        )
        .into_iter()
        .map(|(field, _)| field)
        .collect();
        fields.sort();

        assert_eq!(fields, ["data.series[1]", "model.targets.horizon"]);
    }

    /// The part's own error building the features is the answer: no
    /// series is then refused for it.
    #[test]
    fn the_parts_error_building_the_features_is_reported_under_its_field() {
        let refusals = refusals(&[], &[GOLD_BARS], HORIZON);
        let fields: Vec<&str> = refusals.iter().map(|(field, _)| field.as_str()).collect();
        assert_eq!(fields, ["features"]);
    }

    /// The probe's features on gold's bars, of the tables `tables`, as its
    /// part builds them of the loaded config.
    fn features_of(tables: &str) -> Rc<FeatureSet> {
        ProbeParams::features(&load_of(&[GOLD_BARS], &[GOLD_BARS], HORIZON, tables).unwrap())
            .unwrap()
    }

    /// The span of 15 minutes, 900 s, is its feature's value.
    #[test]
    fn a_feature_is_built_of_its_table() {
        assert_eq!(features_of(TABLES).values(), Some(vec![900.0]));
    }

    /// Rows and models of a span of 15 minutes are not those of one of
    /// 30, with no code of the part's own.
    #[test]
    fn a_features_changed_argument_changes_the_fingerprint() {
        let changed = TABLES.replace(r#""15m""#, r#""30m""#);

        assert_ne!(
            features_of(&changed).fingerprint(),
            features_of(TABLES).fingerprint()
        );
    }

    /// 15 minutes written as 900 seconds is the same span.
    #[test]
    fn a_features_argument_written_otherwise_keeps_the_fingerprint() {
        let otherwise = TABLES.replace(r#""15m""#, r#""900s""#);

        assert_eq!(
            features_of(&otherwise).fingerprint(),
            features_of(TABLES).fingerprint()
        );
    }

    /// The rows do not depend on the filter, which the part declares for
    /// its own logic and never hands to its features.
    #[test]
    fn an_indicator_of_the_parts_own_logic_is_not_in_the_fingerprint() {
        let changed = TABLES.replace("period = 5", "period = 6");

        assert_eq!(
            features_of(&changed).fingerprint(),
            features_of(TABLES).fingerprint()
        );
    }

    /// A span its arguments refuse and the filter's table missing are
    /// reported, each under its table, with the model's violation; the
    /// features, which need the span, are not built.
    #[test]
    fn the_tables_refusals_are_reported_with_the_other_violations() {
        let tables = TABLES
            .replace(r#""15m""#, r#""0s""#)
            .replace("[indicators.filter]\n        period = 5\n", "");

        let err =
            load_of(&[GOLD_BARS], &[GOLD_BARS], r#"{ horizon = "1h" }"#, &tables).unwrap_err();

        let Some(ConfigError::Multiple { errors }) = err.downcast_ref::<ConfigError>() else {
            panic!("not several config errors: {err}");
        };
        let mut fields: Vec<String> = errors
            .iter()
            .map(|err| match err {
                ConfigError::MissingField { field } | ConfigError::InvalidValue { field, .. } => {
                    field.clone()
                }
                err => panic!("unexpected config error {err:?}"),
            })
            .collect();
        fields.sort();
        assert_eq!(
            fields,
            [
                "indicators.filter",
                "indicators.span",
                "model.targets.horizon"
            ]
        );
    }
}
