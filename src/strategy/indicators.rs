use std::{collections::BTreeMap, fmt};

use nautilus_common::config::{ConfigError, ConfigErrorCollector};
use nautilus_indicators::{
    average::{MovingAverageType, ema::ExponentialMovingAverage},
    volatility::atr::AverageTrueRange,
};
use nautilus_model::enums::PriceType;
use serde::{Deserialize, Deserializer, de};

/// `[indicators]`: the indicators the strategy's code reads, each in its
/// own `[indicators.<name>]` table, by name (see `IndicatorConfig`).
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(transparent)]
pub struct IndicatorsConfig(pub BTreeMap<String, IndicatorConfig>);

/// An indicator: `kind` its Nautilus type, the other keys its Nautilus
/// constructor's arguments, by their Nautilus names and types, those left
/// out taking Nautilus' defaults. `build` makes it.
///
/// - `ExponentialMovingAverage`: `period`, `price_type` (a Nautilus
///   `PriceType`, of the quotes it reads).
/// - `AverageTrueRange`: `period`, `ma_type` (a Nautilus
///   `MovingAverageType`, such as `"WILDER"`), `use_previous`,
///   `value_floor`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum IndicatorConfig {
    ExponentialMovingAverage {
        period: usize,
        #[serde(default)]
        price_type: Option<PriceType>,
    },
    AverageTrueRange {
        period: usize,
        #[serde(default, deserialize_with = "moving_average_type")]
        ma_type: Option<MovingAverageType>,
        #[serde(default)]
        use_previous: Option<bool>,
        #[serde(default)]
        value_floor: Option<f64>,
    },
}

/// The Nautilus type of an indicator, as a strategy declares the ones it
/// reads (`Part::INDICATORS`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndicatorKind {
    ExponentialMovingAverage,
    AverageTrueRange,
}

impl fmt::Display for IndicatorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// An indicator built, of the type its config names.
#[derive(Debug)]
pub enum Indicator {
    ExponentialMovingAverage(ExponentialMovingAverage),
    AverageTrueRange(AverageTrueRange),
}

impl TryFrom<Indicator> for ExponentialMovingAverage {
    type Error = Indicator;

    fn try_from(indicator: Indicator) -> Result<Self, Indicator> {
        match indicator {
            Indicator::ExponentialMovingAverage(ema) => Ok(ema),
            indicator => Err(indicator),
        }
    }
}

impl TryFrom<Indicator> for AverageTrueRange {
    type Error = Indicator;

    fn try_from(indicator: Indicator) -> Result<Self, Indicator> {
        match indicator {
            Indicator::AverageTrueRange(atr) => Ok(atr),
            indicator => Err(indicator),
        }
    }
}

impl IndicatorConfig {
    #[must_use]
    pub const fn kind(&self) -> IndicatorKind {
        match self {
            Self::ExponentialMovingAverage { .. } => IndicatorKind::ExponentialMovingAverage,
            Self::AverageTrueRange { .. } => IndicatorKind::AverageTrueRange,
        }
    }

    /// The indicator, built by its Nautilus constructor: `new_checked`
    /// where Nautilus makes it public, else `new`.
    ///
    /// # Errors
    ///
    /// Returns the error of `new_checked` on an argument it refuses.
    ///
    /// # Panics
    ///
    /// Panics on an `AverageTrueRange` argument its `new` refuses: its
    /// `new_checked` is not public.
    pub fn build(&self) -> anyhow::Result<Indicator> {
        Ok(match *self {
            Self::ExponentialMovingAverage { period, price_type } => {
                Indicator::ExponentialMovingAverage(ExponentialMovingAverage::new_checked(
                    period, price_type,
                )?)
            }
            Self::AverageTrueRange {
                period,
                ma_type,
                use_previous,
                value_floor,
            } => Indicator::AverageTrueRange(AverageTrueRange::new(
                period,
                ma_type,
                use_previous,
                value_floor,
            )),
        })
    }
}

impl IndicatorsConfig {
    /// The indicator `name`, built, if it is configured, of type `T`, and
    /// Nautilus takes its arguments.
    #[must_use]
    pub fn built<T: TryFrom<Indicator>>(&self, name: &str) -> Option<T> {
        self.0.get(name)?.build().ok()?.try_into().ok()
    }

    /// Pushes the violations of `[indicators]` against the indicators the
    /// strategy reads, `declared`: one of them missing or of another type,
    /// one configured it does not read, and the arguments of each Nautilus
    /// refuses, each under the indicator's name.
    pub(super) fn validate(
        &self,
        declared: &[(&str, IndicatorKind)],
        errors: &mut ConfigErrorCollector,
    ) {
        for &(name, kind) in declared {
            let field = format!("indicators.{name}");
            let Some(config) = self.0.get(name) else {
                errors.push(ConfigError::missing_field(field));
                continue;
            };
            if config.kind() != kind {
                errors.push(ConfigError::invalid_value(
                    format!("{field}.kind"),
                    format!("is {}, the strategy reads {name} as {kind}", config.kind()),
                ));
                continue;
            }
            if let Err(e) = config.build() {
                errors.push(ConfigError::invalid_value(field, e.to_string()));
            }
        }
        for name in self.0.keys() {
            errors.check(
                declared.iter().any(|&(read, _)| read == name),
                ConfigError::unsupported_field(
                    format!("indicators.{name}"),
                    "not read by the strategy",
                ),
            );
        }
    }
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

#[cfg(test)]
mod tests {
    use nautilus_trading::strategy::StrategyConfig;

    use super::*;
    use crate::strategy::{Part, StandardConfig};

    /// A strategy reading an EMA `fast` and an ATR `range`.
    #[derive(Debug, Clone, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Trend {}

    impl Part for Trend {
        const INSTRUMENTS: usize = 1;
        const INDICATORS: &'static [(&'static str, IndicatorKind)] = &[
            ("fast", IndicatorKind::ExponentialMovingAverage),
            ("range", IndicatorKind::AverageTrueRange),
        ];

        fn default_base() -> StrategyConfig {
            StrategyConfig::default()
        }

        fn validate(_config: &StandardConfig<Self>, _errors: &mut ConfigErrorCollector) {}
    }

    const TOML: &str = r#"
        [data]
        series = [
            { data = "bars", instrument_id = "ESM4.CME", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } },
        ]
        warmup = "24h"

        [indicators.fast]
        kind = "ExponentialMovingAverage"
        period = 5
        price_type = "BID"

        [indicators.range]
        kind = "AverageTrueRange"
        period = 14
        ma_type = "SIMPLE"
        use_previous = false

        [trade]
        instruments = [{ instrument_id = "ESM4.CME", trade_size = "1" }]
    "#;

    fn load(toml: &str) -> anyhow::Result<StandardConfig<Trend>> {
        StandardConfig::load(toml)
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

    #[test]
    fn the_strategy_receives_its_indicators_built_by_name_with_their_arguments() {
        let config = load(TOML).unwrap();
        let fast: ExponentialMovingAverage = config.indicators.built("fast").unwrap();
        assert_eq!((fast.period, fast.price_type), (5, PriceType::Bid));
        let range: AverageTrueRange = config.indicators.built("range").unwrap();
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
    fn arguments_left_out_take_nautilus_defaults() {
        let toml = TOML
            .replace("price_type = \"BID\"\n", "")
            .replace("ma_type = \"SIMPLE\"\n", "")
            .replace("use_previous = false\n", "");
        let config = load(&toml).unwrap();
        let fast: ExponentialMovingAverage = config.indicators.built("fast").unwrap();
        assert_eq!(fast.price_type, PriceType::Last);
        let range: AverageTrueRange = config.indicators.built("range").unwrap();
        assert_eq!(
            (range.ma_type, range.use_previous),
            (MovingAverageType::Wilder, true)
        );
    }

    #[test]
    fn an_indicator_of_another_type_is_not_received() {
        let config = load(TOML).unwrap();
        assert!(
            config
                .indicators
                .built::<AverageTrueRange>("fast")
                .is_none()
        );
    }

    #[test]
    fn a_missing_indicator_is_refused_naming_it() {
        let start = TOML.find("[indicators.range]").unwrap();
        let end = TOML.find("[trade]").unwrap();
        let toml = format!("{}{}", &TOML[..start], &TOML[end..]);
        assert_eq!(error_fields(&toml), ["indicators.range"]);
    }

    #[test]
    fn an_indicator_of_the_wrong_type_is_refused_naming_it() {
        let toml = TOML
            .replace(
                "kind = \"ExponentialMovingAverage\"",
                "kind = \"AverageTrueRange\"",
            )
            .replace("price_type = \"BID\"\n", "");
        assert_eq!(error_fields(&toml), ["indicators.fast.kind"]);
    }

    #[test]
    fn an_indicator_the_strategy_does_not_read_is_refused_naming_it() {
        let toml = TOML.replace(
            "[trade]",
            "[indicators.slow]\nkind = \"ExponentialMovingAverage\"\nperiod = 30\n\n[trade]",
        );
        assert_eq!(error_fields(&toml), ["indicators.slow"]);
    }

    #[test]
    fn an_argument_new_checked_refuses_is_refused_naming_the_indicator() {
        let toml = TOML.replace("period = 5", "period = 0");
        let err = load(&toml).unwrap_err();
        assert!(err.to_string().contains("period"), "{err}");
        assert_eq!(error_fields(&toml), ["indicators.fast"]);
    }

    /// `AverageTrueRange::new_checked` is not public: its `new` panics.
    #[test]
    #[should_panic(expected = "period must be in")]
    fn an_average_true_range_argument_nautilus_refuses_panics_at_load() {
        let _ = load(&TOML.replace("period = 14", "period = 0"));
    }

    #[test]
    fn an_unknown_argument_is_refused_naming_it() {
        let toml = TOML.replace("period = 5", "period = 5\nperiods = 6");
        let err = load(&toml).unwrap_err();
        assert!(err.downcast_ref::<ConfigError>().is_none(), "{err}");
        assert!(err.to_string().contains("periods"), "{err}");
    }

    #[test]
    fn an_unknown_moving_average_type_is_refused_naming_it() {
        let toml = TOML.replace("\"SIMPLE\"", "\"TRIANGULAR\"");
        let err = load(&toml).unwrap_err();
        assert!(err.to_string().contains("TRIANGULAR"), "{err}");
    }

    #[test]
    fn an_unknown_kind_is_refused_naming_it() {
        let toml = TOML.replace(
            "kind = \"ExponentialMovingAverage\"",
            "kind = \"KalmanFilter\"",
        );
        let err = load(&toml).unwrap_err();
        assert!(err.to_string().contains("KalmanFilter"), "{err}");
    }

    #[test]
    fn indicator_violations_are_reported_with_the_others_at_once() {
        let start = TOML.find("[indicators.range]").unwrap();
        let end = TOML.find("[trade]").unwrap();
        let toml = format!("{}{}", &TOML[..start], &TOML[end..])
            .replace("period = 5", "period = 0")
            .replace("\"24h\"", "\"0s\"");
        assert_eq!(
            error_fields(&toml),
            ["data.warmup", "indicators.fast", "indicators.range"]
        );
    }
}
