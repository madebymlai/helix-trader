use std::{collections::BTreeMap, marker::PhantomData};

use nautilus_common::config::{ConfigError, ConfigErrorCollector};
use nautilus_indicators::{
    average::{MovingAverageType, ema::ExponentialMovingAverage},
    indicator::Indicator,
    volatility::atr::AverageTrueRange,
};
use nautilus_model::enums::PriceType;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de, de::DeserializeOwned};

/// `[indicators]`: each `[indicators.<name>]` table as the TOML has it,
/// the arguments of the indicator the strategy declares by that name
/// (`Part::INDICATORS`), which parses and builds it (see `IndicatorArgs`).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(transparent)]
pub struct IndicatorsConfig(pub BTreeMap<String, toml::Table>);

/// The arguments of an indicator, its `[indicators.<name>]` table, and how
/// it is built: a Nautilus `Indicator`, Nautilus' own or the strategy's,
/// registered by the strategy as any other. For Nautilus' indicators the
/// arguments are its constructor's, by their Nautilus names (`EmaArgs`,
/// `AtrArgs`); a custom indicator brings its own.
pub trait IndicatorArgs: DeserializeOwned {
    type Indicator: Indicator;

    /// # Errors
    ///
    /// Returns the error of an argument the indicator refuses.
    fn build(self) -> anyhow::Result<Self::Indicator>;
}

/// An indicator the strategy reads, by its name in `[indicators]` and its
/// arguments `A`: a strategy declares it as a constant, lists it in
/// `Part::INDICATORS` and receives it built (`IndicatorsConfig::build`).
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

/// A declared indicator, of whatever arguments, as `[indicators]` checks
/// it: its name, and its table parsed and built.
pub trait Declaration {
    fn name(&self) -> &'static str;

    /// # Errors
    ///
    /// Returns the error parsing `table` into the arguments, or building
    /// the indicator of them.
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

/// The arguments of a Nautilus `ExponentialMovingAverage`, built by its
/// `new_checked`: `period`, and `price_type`, the price it reads of quotes
/// (`LAST` left out; bars give their close).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmaArgs {
    pub period: usize,
    #[serde(default)]
    pub price_type: Option<PriceType>,
}

impl IndicatorArgs for EmaArgs {
    type Indicator = ExponentialMovingAverage;

    fn build(self) -> anyhow::Result<ExponentialMovingAverage> {
        ExponentialMovingAverage::new_checked(self.period, self.price_type)
    }
}

/// The arguments of a Nautilus `AverageTrueRange`, built by its `new`
/// (its `new_checked` is not public, so an argument it refuses panics):
/// `period`, `ma_type` (a `MovingAverageType` by the name its `FromStr`
/// parses, `"WILDER"` left out), `use_previous` and `value_floor`.
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
    /// The arguments of `declared`, if its table is there and parses.
    #[must_use]
    pub fn args<A: IndicatorArgs>(&self, declared: Declared<A>) -> Option<A> {
        parse(self.0.get(declared.name)?).ok()
    }

    /// The indicator `declared`, built of its table.
    ///
    /// # Panics
    ///
    /// Panics if its table is missing, does not parse or is refused by its
    /// builder, which `validate` refuses of a declared indicator.
    #[must_use]
    pub fn build<A: IndicatorArgs>(&self, declared: Declared<A>) -> A::Indicator {
        let table = &self.0[declared.name];
        parse::<A>(table)
            .and_then(A::build)
            .unwrap_or_else(|e| panic!("indicator {} validated: {e}", declared.name))
    }

    /// Pushes the violations of `[indicators]` against the indicators the
    /// strategy declares: one of them missing, or its table refused by its
    /// arguments or its builder, and a table it does not declare, each
    /// under `indicators.<name>`.
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

/// The arguments `table` holds.
fn parse<A: DeserializeOwned>(table: &toml::Table) -> anyhow::Result<A> {
    Ok(toml::Value::Table(table.clone()).try_into()?)
}

/// A Nautilus `MovingAverageType` by the name its `FromStr` parses: it has
/// no serde of its own.
fn moving_average_type<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<MovingAverageType>, D::Error> {
    let name = String::deserialize(deserializer)?;
    name.parse()
        .map(Some)
        .map_err(|e| de::Error::custom(format!("invalid ma_type {name}: {e}")))
}

/// A Nautilus `MovingAverageType` by the name its `Display` writes, which
/// its `FromStr` parses back (`moving_average_type`).
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
    use std::{cell::RefCell, rc::Rc};

    use nautilus_common::actor::indicators::SharedActorIndicator;
    use nautilus_model::data::{Bar, BarType};

    use super::*;
    use crate::strategy::{Part, StandardConfig};

    /// A custom Nautilus indicator: initialized once a bar closes above
    /// its `level`.
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

    /// The custom indicator's own arguments: a positive `level`.
    #[derive(Debug, Deserialize)]
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

    /// A strategy reading an EMA `fast`, an ATR `range` and a custom
    /// `breakout`.
    #[derive(Debug, Clone, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Trend {}

    impl Part for Trend {
        const INSTRUMENTS: usize = 1;
        const INDICATORS: &'static [&'static dyn Declaration] = &[&FAST, &RANGE, &BREAKOUT];

        fn validate(_config: &StandardConfig<Self>, _errors: &mut ConfigErrorCollector) {}
    }

    const TOML: &str = r#"
        [base]
        strategy_id = "TREND-001"

        [data]
        series = [
            { data = "bars", instrument_id = "ESM4.CME", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } },
        ]
        warmup = "24h"

        [indicators.fast]
        period = 5
        price_type = "BID"

        [indicators.range]
        period = 14
        ma_type = "SIMPLE"
        use_previous = false

        [indicators.breakout]
        level = 100.5

        [trade]
        instruments = [{ instrument_id = "ESM4.CME", trade_size = "1" }]
    "#;

    fn load(toml: &str) -> anyhow::Result<StandardConfig<Trend>> {
        StandardConfig::load(toml)
    }

    /// The TOML without the table `[indicators.<name>]`.
    fn without(name: &str) -> String {
        let header = format!("[indicators.{name}]");
        let start = TOML.find(&header).unwrap();
        let end = start + 1 + TOML[start + 1..].find('[').unwrap();
        format!("{}{}", &TOML[..start], &TOML[end..])
    }

    /// The fields of the config errors refusing `toml`, in order.
    fn error_fields(toml: &str) -> Vec<String> {
        let err = load(toml).unwrap_err();
        let errors = match err.downcast_ref::<ConfigError>() {
            Some(ConfigError::Multiple { errors }) => errors.clone(),
            Some(err) => vec![err.clone()],
            None => panic!("not a config error: {err}"),
        };
        errors
            .into_iter()
            .map(|err| match err {
                ConfigError::MissingField { field }
                | ConfigError::UnsupportedField { field, .. }
                | ConfigError::InvalidValue { field, .. }
                | ConfigError::Range { field, .. } => field,
                err => panic!("unexpected config error {err:?}"),
            })
            .collect()
    }

    fn bar(close: &str) -> Bar {
        Bar::new(
            BarType::from("ESM4.CME-1-HOUR-MID-INTERNAL"),
            close.into(),
            close.into(),
            close.into(),
            close.into(),
            "1".into(),
            0.into(),
            0.into(),
        )
    }

    #[test]
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
            (14, MovingAverageType::Simple, false, 0.0)
        );
    }

    #[test]
    fn a_custom_indicator_is_received_built_from_its_table_and_registers_as_nautilus_ones() {
        let config = load(TOML).unwrap();
        let breakout = config.indicators.build(BREAKOUT);
        assert_eq!(breakout.level, 100.5);
        let registered = Rc::new(RefCell::new(breakout)) as SharedActorIndicator;
        registered.handle_bar(&bar("100.5")).unwrap();
        assert!(!registered.initialized().unwrap());
        registered.handle_bar(&bar("101")).unwrap();
        assert!(registered.initialized().unwrap());
    }

    #[test]
    fn arguments_left_out_take_nautilus_defaults() {
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

    #[test]
    fn the_strategy_reads_an_indicator_s_arguments() {
        let config = load(TOML).unwrap();
        assert_eq!(
            config.indicators.args(FAST).map(|args| args.period),
            Some(5)
        );
    }

    #[test]
    fn a_missing_indicator_is_refused_naming_it() {
        assert_eq!(error_fields(&without("breakout")), ["indicators.breakout"]);
    }

    #[test]
    fn an_indicator_the_strategy_does_not_declare_is_refused_naming_it() {
        let toml = TOML.replace("[trade]", "[indicators.slow]\nperiod = 30\n\n[trade]");
        assert_eq!(error_fields(&toml), ["indicators.slow"]);
    }

    #[test]
    fn an_argument_nautilus_refuses_is_refused_naming_the_indicator() {
        let toml = TOML.replace("period = 5", "period = 0");
        let err = load(&toml).unwrap_err();
        assert!(err.to_string().contains("period"), "{err}");
        assert_eq!(error_fields(&toml), ["indicators.fast"]);
    }

    #[test]
    fn an_argument_a_custom_builder_refuses_is_refused_naming_the_indicator() {
        let toml = TOML.replace("level = 100.5", "level = -1.0");
        let err = load(&toml).unwrap_err();
        assert!(err.to_string().contains("level must be positive"), "{err}");
        assert_eq!(error_fields(&toml), ["indicators.breakout"]);
    }

    /// `AverageTrueRange::new_checked` is not public: its `new` panics.
    #[test]
    #[should_panic(expected = "period must be in")]
    fn an_average_true_range_argument_nautilus_refuses_panics_at_load() {
        let _ = load(&TOML.replace("period = 14", "period = 0"));
    }

    #[test]
    fn an_unknown_argument_is_refused_naming_the_indicator_and_the_argument() {
        let toml = TOML.replace("period = 5", "period = 5\nperiods = 6");
        let err = load(&toml).unwrap_err();
        assert!(err.to_string().contains("periods"), "{err}");
        assert_eq!(error_fields(&toml), ["indicators.fast"]);
    }

    #[test]
    fn a_kind_is_an_unknown_argument() {
        let toml = TOML.replace(
            "period = 5",
            "kind = \"ExponentialMovingAverage\"\nperiod = 5",
        );
        assert_eq!(error_fields(&toml), ["indicators.fast"]);
    }

    #[test]
    fn an_argument_of_the_wrong_type_is_refused_naming_the_indicator() {
        let toml = TOML.replace("level = 100.5", "level = \"high\"");
        assert_eq!(error_fields(&toml), ["indicators.breakout"]);
    }

    #[test]
    fn an_unknown_moving_average_type_is_refused_naming_it() {
        let toml = TOML.replace("\"SIMPLE\"", "\"TRIANGULAR\"");
        let err = load(&toml).unwrap_err();
        assert!(err.to_string().contains("TRIANGULAR"), "{err}");
        assert_eq!(error_fields(&toml), ["indicators.range"]);
    }

    #[test]
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
