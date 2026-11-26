use std::{collections::BTreeMap, marker::PhantomData};

use nautilus_common::config::{ConfigError, ConfigErrorCollector};
use nautilus_indicators::{
    average::{MovingAverageType, ema::ExponentialMovingAverage, sma::SimpleMovingAverage},
    indicator::Indicator,
    volatility::atr::AverageTrueRange,
};
use nautilus_model::enums::PriceType;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de, de::DeserializeOwned};

/// The `[indicators]` table.
///
/// Each `[indicators.<name>]` table holds the arguments of the indicator that the strategy
/// declares by that name (`Part::INDICATORS`), as the TOML has them. A table is not a feature on
/// its own. A model strategy makes one a feature by handing it to its feature set
/// (`model::features::FeatureSetBuilder`).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(transparent)]
pub struct IndicatorsConfig(pub BTreeMap<String, toml::Table>);

/// The arguments of an indicator, and how the indicator is built.
///
/// The arguments are the `[indicators.<name>]` table. For Nautilus' indicators they are the
/// constructor's arguments, under its names (`EmaArgs`, `SmaArgs`, `AtrArgs`). A custom indicator
/// brings its own. They serialize as they parse, so a feature's arguments enter its fingerprint by
/// value (`canonical_args`).
pub trait IndicatorArgs: DeserializeOwned + Serialize {
    type Indicator: Indicator;

    /// # Errors
    ///
    /// Returns an error if the indicator refuses an argument.
    fn build(self) -> anyhow::Result<Self::Indicator>;
}

/// An indicator the strategy reads, declared as a constant in `Part::INDICATORS`
/// (`IndicatorsConfig::build`).
///
/// It has a name in `[indicators]` and arguments `A`.
#[derive(Debug)]
pub struct Declared<A> {
    name: &'static str,
    args: PhantomData<fn() -> A>,
}

impl<A> Declared<A> {
    #[must_use]
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            args: PhantomData,
        }
    }
}

impl<A> Clone for Declared<A> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<A> Copy for Declared<A> {}

/// A declared indicator, whatever its arguments, as `[indicators]` checks it.
pub trait Declaration {
    fn name(&self) -> &'static str;

    /// # Errors
    ///
    /// Returns an error if `table` does not parse into the arguments or does not build.
    fn check(&self, table: &toml::Table) -> anyhow::Result<()>;
}

impl<A: IndicatorArgs> Declaration for Declared<A> {
    fn name(&self) -> &'static str {
        self.name
    }

    fn check(&self, table: &toml::Table) -> anyhow::Result<()> {
        parse::<A>(table)?.build()?;
        Ok(())
    }
}

/// The arguments of a Nautilus `ExponentialMovingAverage`, built by its `new_checked`.
///
/// The arguments are `period` and `price_type`, the price read from quotes. `LAST` is left out,
/// and bars give their close.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmaArgs {
    pub period: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_type: Option<PriceType>,
}

impl IndicatorArgs for EmaArgs {
    type Indicator = ExponentialMovingAverage;

    fn build(self) -> anyhow::Result<ExponentialMovingAverage> {
        ExponentialMovingAverage::new_checked(self.period, self.price_type)
    }
}

/// The arguments of a Nautilus `SimpleMovingAverage`, built by its `new_checked`.
///
/// The arguments are those of `EmaArgs`: `period` and `price_type`, the price read from quotes.
/// `LAST` is left out, and bars give their close.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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

/// The arguments of a Nautilus `AverageTrueRange`, built by its `new`.
///
/// `new_checked` is not public, and `new` panics on an argument it refuses. The arguments are
/// `period`, `ma_type` (the name its `FromStr` parses, `"WILDER"` left out), `use_previous` and
/// `value_floor`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AtrArgs {
    pub period: usize,
    #[serde(
        default,
        deserialize_with = "moving_average_type",
        serialize_with = "moving_average_type_name",
        skip_serializing_if = "Option::is_none"
    )]
    pub ma_type: Option<MovingAverageType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub use_previous: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_floor: Option<f64>,
}

impl IndicatorArgs for AtrArgs {
    type Indicator = AverageTrueRange;

    fn build(self) -> anyhow::Result<AverageTrueRange> {
        Ok(AverageTrueRange::new(
            self.period,
            self.ma_type,
            self.use_previous,
            self.value_floor,
        ))
    }
}

impl IndicatorsConfig {
    /// Returns the arguments of `declared`, or `None` if its table is missing or does not parse.
    #[must_use]
    pub fn args<A: IndicatorArgs>(&self, declared: Declared<A>) -> Option<A> {
        parse(self.0.get(declared.name)?).ok()
    }

    /// Builds the indicator `declared` from its table.
    ///
    /// # Panics
    ///
    /// Panics if the table is missing, does not parse or is refused by the builder. `validate`
    /// rules this out.
    #[must_use]
    pub fn build<A: IndicatorArgs>(&self, declared: Declared<A>) -> A::Indicator {
        self.parsed(declared)
            .build()
            .unwrap_or_else(|e| panic!("indicator {} validated: {e}", declared.name))
    }

    /// Returns the arguments of `declared` as JSON.
    ///
    /// Arguments that the type parses as equal give one string, however the TOML spells them
    /// (`"15m"` or `"900s"`, `"MID"` or `"mid"`, any key order).
    ///
    /// # Panics
    ///
    /// Panics if the table is missing or does not parse. `validate` rules this out.
    #[must_use]
    pub fn canonical_args<A: IndicatorArgs>(&self, declared: Declared<A>) -> String {
        serde_json::to_string(&self.parsed(declared))
            .unwrap_or_else(|e| panic!("indicator {} arguments serialize: {e}", declared.name))
    }

    /// # Panics
    ///
    /// Panics if its table is missing or does not parse (see `canonical_args`).
    fn parsed<A: IndicatorArgs>(&self, declared: Declared<A>) -> A {
        parse(&self.0[declared.name])
            .unwrap_or_else(|e| panic!("indicator {} validated: {e}", declared.name))
    }

    pub(super) fn builds(&self, declared: &[&dyn Declaration]) -> bool {
        declared.iter().all(|declaration| {
            self.0
                .get(declaration.name())
                .is_some_and(|table| declaration.check(table).is_ok())
        })
    }

    /// Pushes a violation under `indicators.<name>` for each declared indicator that is missing
    /// or refused, and for each table that is not declared.
    pub(super) fn validate(
        &self,
        declared: &[&dyn Declaration],
        errors: &mut ConfigErrorCollector,
    ) {
        for declaration in declared {
            let field = format!("indicators.{}", declaration.name());
            let Some(table) = self.0.get(declaration.name()) else {
                errors.push(ConfigError::missing_field(field));
                continue;
            };
            if let Err(e) = declaration.check(table) {
                errors.push(ConfigError::invalid_value(field, e.to_string()));
            }
        }
        for name in self.0.keys() {
            errors.check(
                declared
                    .iter()
                    .any(|declaration| declaration.name() == name),
                ConfigError::unsupported_field(
                    format!("indicators.{name}"),
                    "not declared by the strategy",
                ),
            );
        }
    }
}

fn parse<A: DeserializeOwned>(table: &toml::Table) -> anyhow::Result<A> {
    Ok(toml::Value::Table(table.clone()).try_into()?)
}

/// Deserializes a `MovingAverageType` from the name its `FromStr` parses.
///
/// The type has no serde of its own.
fn moving_average_type<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<MovingAverageType>, D::Error> {
    let name = String::deserialize(deserializer)?;
    name.parse()
        .map(Some)
        .map_err(|e| de::Error::custom(format!("invalid ma_type {name}: {e}")))
}

/// Serializes a `MovingAverageType` as the name its `Display` writes (`moving_average_type`).
fn moving_average_type_name<S: Serializer>(
    ma_type: &Option<MovingAverageType>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    ma_type
        .map(|ma_type| ma_type.to_string())
        .serialize(serializer)
}

#[cfg(test)]
mod tests {
    use nautilus_common::config::ConfigResult;
    use nautilus_model::data::Bar;
    use nautilus_trading::strategy::StrategyConfig;
    use rstest::rstest;

    use super::*;
    use crate::strategy::{Part, Series, StandardConfig};

    /// Initialized once a bar closes above its `level`.
    #[derive(Debug)]
    struct Breakout {
        level: f64,
        broken: bool,
    }

    impl Indicator for Breakout {
        fn name(&self) -> String {
            "Breakout".to_string()
        }

        fn has_inputs(&self) -> bool {
            self.broken
        }

        fn initialized(&self) -> bool {
            self.broken
        }

        fn handle_bar(&mut self, bar: &Bar) {
            self.broken |= bar.close.as_f64() > self.level;
        }

        fn reset(&mut self) {
            self.broken = false;
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct BreakoutArgs {
        level: f64,
    }

    impl IndicatorArgs for BreakoutArgs {
        type Indicator = Breakout;

        fn build(self) -> anyhow::Result<Breakout> {
            anyhow::ensure!(self.level > 0.0, "level must be positive");
            Ok(Breakout {
                level: self.level,
                broken: false,
            })
        }
    }

    const FAST: Declared<EmaArgs> = Declared::new("fast");
    const RANGE: Declared<AtrArgs> = Declared::new("range");
    const BREAKOUT: Declared<BreakoutArgs> = Declared::new("breakout");

    #[derive(Debug, Clone, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Trend {}

    impl Part for Trend {
        const INSTRUMENTS: usize = 1;
        const INDICATORS: &'static [&'static dyn Declaration] = &[&FAST, &RANGE, &BREAKOUT];

        fn default_base() -> StrategyConfig {
            StrategyConfig::default()
        }

        fn reads(config: &StandardConfig<Self>) -> ConfigResult<Vec<Series>> {
            Ok(config.data.series.clone())
        }

        fn validate(_config: &StandardConfig<Self>, _errors: &mut ConfigErrorCollector) {}
    }

    const TOML: &str = r#"
        [data]
        series = [
            { data = "bars", instrument_id = "ESM24.CME", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } },
        ]
        warmup = "24h"

        [indicators.fast]
        period = 5
        price_type = "BID"

        [indicators.range]
        period = 14
        ma_type = "SIMPLE"
        use_previous = false
        value_floor = 0.25

        [indicators.breakout]
        level = 100.5

        [trade]
        instruments = [{ instrument_id = "ESM24.CME" }]
        risk = "0.5%"
    "#;

    fn load(toml: &str) -> anyhow::Result<StandardConfig<Trend>> {
        StandardConfig::load(toml)
    }

    fn without(name: &str) -> String {
        let header = format!("[indicators.{name}]");
        let start = TOML.find(&header).unwrap();
        let end = start + 1 + TOML[start + 1..].find('[').unwrap();
        format!("{}{}", &TOML[..start], &TOML[end..])
    }

    fn error_fields(toml: &str) -> Vec<String> {
        let err = load(toml).unwrap_err();
        let errors = match err.downcast_ref::<ConfigError>() {
            Some(ConfigError::Multiple { errors }) => errors.clone(),
            Some(err) => vec![err.clone()],
            None => panic!("not a config error: {err}"),
        };
        errors
            .into_iter()
            .map(|e| match e {
                ConfigError::MissingField { field }
                | ConfigError::UnsupportedField { field, .. }
                | ConfigError::InvalidValue { field, .. }
                | ConfigError::Range { field, .. } => field,
                e => panic!("unexpected config error {e:?}"),
            })
            .collect()
    }

    #[rstest]
    fn the_strategy_receives_nautilus_indicators_built_from_their_tables() {
        let config = load(TOML).unwrap();
        let fast = config.indicators.build(FAST);
        assert_eq!((fast.period, fast.price_type), (5, PriceType::Bid));
        let range = config.indicators.build(RANGE);
        assert_eq!(
            (
                range.period,
                range.ma_type,
                range.use_previous,
                range.value_floor
            ),
            (14, MovingAverageType::Simple, false, 0.25)
        );
    }

    #[rstest]
    fn a_custom_indicator_is_received_built_from_its_table() {
        let config = load(TOML).unwrap();
        let breakout = config.indicators.build(BREAKOUT);
        assert_eq!(breakout.level, 100.5);
    }

    /// Pins the defaults Nautilus' indicators take for a left-out argument:
    /// - an EMA's `LAST` price (`crates/indicators/src/average/ema.rs:119`)
    /// - an ATR's Wilder average with the previous close
    ///   (`crates/indicators/src/volatility/atr.rs:131-132`)
    ///
    /// `EmaArgs` and `AtrArgs` hand them over as none, and `canonical_args` leaves them out of a
    /// fingerprint. A changed default therefore needs `model::dataset::VERSION` bumped.
    #[rstest]
    fn drift_arguments_left_out_take_nautilus_defaults() {
        let toml = TOML
            .replace("price_type = \"BID\"\n", "")
            .replace("ma_type = \"SIMPLE\"\n", "")
            .replace("use_previous = false\n", "");
        let config = load(&toml).unwrap();
        assert_eq!(config.indicators.build(FAST).price_type, PriceType::Last);
        let range = config.indicators.build(RANGE);
        assert_eq!(
            (range.ma_type, range.use_previous),
            (MovingAverageType::Wilder, true)
        );
    }

    #[rstest]
    fn the_strategy_reads_an_indicator_s_arguments() {
        let config = load(TOML).unwrap();
        assert_eq!(
            config.indicators.args(FAST),
            Some(EmaArgs {
                period: 5,
                price_type: Some(PriceType::Bid),
            })
        );
    }

    fn refusal(toml: &str) -> ConfigError {
        load(toml).unwrap_err().downcast().unwrap()
    }

    #[rstest]
    #[case::missing(without("breakout"), ConfigError::missing_field("indicators.breakout"))]
    #[case::not_declared(
        TOML.replace("[trade]", "[indicators.slow]\nperiod = 30\n\n[trade]"),
        ConfigError::unsupported_field("indicators.slow", "not declared by the strategy"),
    )]
    fn an_indicator_missing_or_not_declared_is_refused_naming_it(
        #[case] toml: String,
        #[case] expected: ConfigError,
    ) {
        assert_eq!(refusal(&toml), expected);
    }

    #[rstest]
    #[case::an_argument_nautilus_refuses("period = 5", "period = 0", "period", "indicators.fast")]
    #[case::an_argument_a_custom_builder_refuses(
        "level = 100.5",
        "level = -1.0",
        "level must be positive",
        "indicators.breakout"
    )]
    #[case::an_unknown_argument(
        "period = 5",
        "period = 5\nperiods = 6",
        "periods",
        "indicators.fast"
    )]
    #[case::an_argument_of_the_wrong_type(
        "level = 100.5",
        "level = \"high\"",
        "high",
        "indicators.breakout"
    )]
    #[case::an_unknown_moving_average_type(
        "\"SIMPLE\"",
        "\"TRIANGULAR\"",
        "TRIANGULAR",
        "indicators.range"
    )]
    fn a_table_refused_is_refused_naming_the_indicator_and_why(
        #[case] from: &str,
        #[case] to: &str,
        #[case] why: &str,
        #[case] field: &str,
    ) {
        let toml = TOML.replace(from, to);
        assert_ne!(toml, TOML);

        let err = load(&toml).unwrap_err();

        assert!(err.to_string().contains(why), "{err}");
        assert_eq!(error_fields(&toml), [field]);
    }

    /// `AverageTrueRange::new_checked` is not public, so `new` panics.
    #[rstest]
    #[should_panic(expected = "period must be in")]
    fn an_average_true_range_argument_nautilus_refuses_panics_at_load() {
        let _ = load(&TOML.replace("period = 14", "period = 0"));
    }

    #[rstest]
    fn indicator_violations_are_reported_with_the_others_at_once() {
        let toml = without("breakout")
            .replace("period = 5", "period = 0")
            .replace("\"24h\"", "\"0s\"")
            .replace("[trade]", "[indicators.slow]\nperiod = 30\n\n[trade]");
        assert_eq!(
            error_fields(&toml),
            [
                "data.warmup",
                "indicators.fast",
                "indicators.breakout",
                "indicators.slow"
            ]
        );
    }
}
