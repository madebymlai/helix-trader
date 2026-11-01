use std::{fmt, marker::PhantomData, str::FromStr};

use jiff::{SignedDuration, Timestamp};
use nautilus_common::config::{ConfigError, ConfigErrorCollector, ConfigResult};
use nautilus_model::{
    data::{BarSpecification, BarType, NautilusDataType},
    enums::{AggregationSource, BarAggregation, PriceType},
    identifiers::{InstrumentId, StrategyId},
    types::Quantity,
};
use nautilus_trading::strategy::StrategyConfig;
use serde::{
    Deserialize, Deserializer,
    de::{self, DeserializeOwned, MapAccess, Visitor},
};

use super::{
    DataRequirements, Declaration, IndicatorsConfig, TickSeries, session::Session, tick_data,
};
use crate::{
    model::config::{ModelConfig, ModelSection},
    rolls::Transition,
};

/// The config every strategy's TOML has: the standard tables, in Nautilus'
/// names and types, and the strategy's own part `P` (see `Part`).
///
/// - `[base]`: Nautilus' `StrategyConfig` as is, `P::default_base` without
///   one.
/// - `[data]`: what the strategy reads (see `DataConfig`), its series
///   those its part answers it reads (`Part::reads`), each once, and no
///   other.
/// - `[indicators]`: the indicators its code reads, by name (see
///   `IndicatorsConfig`), those it declares (`Part::INDICATORS`); none
///   without one.
/// - `[trade]`: what it trades (see `TradeConfig`).
/// - `[model]`: the model of a strategy trading on one
///   (`Part::TAKES_MODEL`), whose targets trade `[trade]`'s instrument (see
///   `ModelSection`, `model`); required of such a strategy, and a key no
///   table takes of any other.
/// - the strategy's own fields, `P` itself, at the top level beside them:
///   a TOML describes one strategy.
///
/// Every table denies unknown keys, and so does `P` at the top level.
/// `load` refuses a config of the wrong shape or types as serde reads it,
/// with Nautilus types' own deserializers, then every violation of the
/// rules at once (see `validate`).
#[derive(Debug, Clone)]
pub struct StandardConfig<P: Part> {
    pub base: StrategyConfig,
    pub data: DataConfig,
    pub indicators: IndicatorsConfig,
    pub trade: TradeConfig,
    /// The model, if the strategy trades on one: then always.
    pub model: Option<ModelSection>,
    pub own: P,
}

/// A strategy's part of its config: its own fields, at the top level of
/// its TOML (`deny_unknown_fields`, so a key no table takes is refused),
/// how many instruments it trades, the indicators it reads, whether it
/// trades on a model, its default `[base]`, the series it reads and its
/// own rules. The standard tables are not its to drop or rename.
pub trait Part: DeserializeOwned + fmt::Debug + Clone {
    /// How many instruments it trades: `[trade] instruments` lists as many.
    const INSTRUMENTS: usize;
    /// The indicators its code reads, each by name and arguments
    /// (`Declared`): `[indicators]` configures these and no other.
    const INDICATORS: &'static [&'static dyn Declaration] = &[];
    /// Whether it trades on a model (`model::ModelStrategy`): if so its
    /// TOML has a `[model]`, it trades one instrument (`INSTRUMENTS` 1),
    /// and it builds its features (`model::ModelPart`). None unless it
    /// says.
    const TAKES_MODEL: bool = false;
    /// Its `[base]` when the TOML has none: its strategy id alone, since
    /// Nautilus tags its orders with the id's tag when `order_id_tag` is
    /// unset (`StrategyCore::new_checked`).
    fn default_base() -> StrategyConfig;
    /// The series the strategy reads of `config`, those its code
    /// subscribes to and registers indicators for: `[data] series` lists
    /// these, each once, and no other (see `StandardConfig::validate`). The
    /// load asks only of a config whose `[trade]` lists `INSTRUMENTS`
    /// instruments and whose `[data]` lists each series once.
    ///
    /// # Errors
    ///
    /// Returns the strategy's own errors reading `config`, each under its
    /// field, as one `ConfigError::Multiple` if there are several.
    fn reads(config: &StandardConfig<Self>) -> ConfigResult<Vec<Series>>;
    /// Pushes onto `errors` every violation of its own rules in `config`,
    /// each under its field: none unless it says.
    fn validate(_config: &StandardConfig<Self>, _errors: &mut ConfigErrorCollector) {}
}

impl<P: Part> StandardConfig<P> {
    /// The config `toml` holds, validated.
    ///
    /// # Errors
    ///
    /// Returns serde's error if the TOML is not of the config's shape or
    /// types, or the `ConfigError` of `validate`.
    pub fn load(toml: &str) -> anyhow::Result<Self> {
        let config: Self = toml::from_str(toml)?;
        config.validate()?;
        Ok(config)
    }

    /// # Errors
    ///
    /// Returns every violation of the rules of `[data]` (see
    /// `DataConfig`), of `[indicators]` (those the strategy declares, each
    /// table parsed and built by its declaration; see `IndicatorsConfig`),
    /// of `[trade]` (as many instruments as the strategy trades, none a
    /// futures root, each `trade_size` positive; see `TradeConfig`), of a
    /// strategy trading on a model (see `validate_model`), of the series
    /// read (see `validate_series`) and of the strategy's own
    /// (`Part::validate`): Nautilus' `ConfigError` of each, with its field,
    /// as one `ConfigError::Multiple` if there are
    /// several.
    pub fn validate(&self) -> ConfigResult<()> {
        let mut errors = ConfigErrorCollector::new();
        self.data.validate(&mut errors);
        self.indicators.validate(P::INDICATORS, &mut errors);
        self.trade.validate(P::INSTRUMENTS, &mut errors);
        self.validate_model(&mut errors);
        self.validate_series(&mut errors);
        P::validate(self, &mut errors);
        errors.into_result()
    }

    /// Pushes onto `errors` the strategy's own errors reading the config
    /// (`Part::reads`), or else each series `[data]` lists that it does not
    /// read, under `data.series[i]`, then each it reads that `[data]` does
    /// not list, under `data.series`, each naming the series. Nothing of a
    /// `[data]` listing a series again, which is its violation.
    fn validate_series(&self, errors: &mut ConfigErrorCollector) {
        // What a strategy reads may depend on what it trades, and is asked
        // of each series listed once: another count of instruments is
        // `[trade]`'s violation, a series listed again `[data]`'s.
        if self.trade.instruments.len() != P::INSTRUMENTS || self.data.repeats().next().is_some() {
            return;
        }
        let read = match P::reads(self) {
            Ok(read) => read,
            Err(e) => {
                errors.push(e);
                return;
            }
        };
        let listed = &self.data.series;
        for (i, series) in listed.iter().enumerate() {
            errors.check(
                read.contains(series),
                ConfigError::invalid_value(
                    format!("data.series[{i}]"),
                    format!("the strategy does not read {series}"),
                ),
            );
        }
        for series in read.iter().filter(|series| !listed.contains(series)) {
            errors.push(ConfigError::invalid_value(
                "data.series",
                format!("the strategy reads {series}, which is not listed"),
            ));
        }
    }

    /// Pushes onto `errors`, if the config has a model, the violations of
    /// a strategy trading on it: the base without a strategy id, which
    /// keys the model's store, and the model invalid for the data read and
    /// the one instrument traded (see `ModelConfig::validate`), each under
    /// its field of `[model]`.
    fn validate_model(&self, errors: &mut ConfigErrorCollector) {
        let Some(model) = &self.model else {
            return;
        };
        errors.check(
            self.base.strategy_id.is_some(),
            ConfigError::missing_field("base.strategy_id"),
        );
        // Another count of instruments is `[trade]`'s violation.
        if let [traded] = self.trade.instruments.as_slice() {
            errors.collect(
                model
                    .clone()
                    .trading(traded)
                    .validate(&self.data_requirements()),
            );
        }
    }

    /// The strategy's id, `[base] strategy_id`.
    ///
    /// # Panics
    ///
    /// Panics if the base has none, which the rules of a strategy trading
    /// on a model, whose id keys its models, refuse.
    #[must_use]
    pub fn strategy_id(&self) -> StrategyId {
        self.base
            .strategy_id
            .expect("a validated config has a strategy id")
    }

    /// The model, its targets trading the instrument of `[trade]`, of its
    /// size.
    ///
    /// # Panics
    ///
    /// Panics if the config has no model, or `[trade]` does not list
    /// exactly one instrument, which a validated config of a strategy
    /// trading on a model has and lists.
    #[must_use]
    pub fn model(&self) -> ModelConfig {
        let (Some(model), [traded]) = (&self.model, self.trade.instruments.as_slice()) else {
            panic!("a validated config of a strategy trading on a model has it, of one instrument");
        };
        model.clone().trading(traded)
    }

    /// What the strategy needs from data: the bars and the ticks of its
    /// series, then the quotes of each instrument traded that no series
    /// reads (`TradeConfig::quotes_unread_by`), and the warmup.
    #[must_use]
    pub fn data_requirements(&self) -> DataRequirements {
        let ticks = self
            .data
            .series
            .iter()
            .filter_map(|series| match series {
                Series::Bars(_) => None,
                Series::Ticks(ticks) => Some(ticks.clone()),
            })
            .chain(self.trade.quotes_unread_by(&self.data))
            .collect();
        DataRequirements {
            bar_types: self.data.bar_types().collect(),
            ticks,
            warmup: self.data.warmup,
        }
    }
}

/// `[data]`: what the strategy reads, its warmup and its session.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataConfig {
    /// The series read, bars and ticks (see `Series`): those the
    /// strategy's part answers it reads (`Part::reads`), each once, and no
    /// other.
    pub series: Vec<Series>,
    /// How far back the warmup history a live node requests at start
    /// starts; a backtest requests none, its strategy warming on the
    /// replay.
    pub warmup: SignedDuration,
    /// The hours traded: only bars wholly inside count. Without one, as on
    /// a market open around the clock, every bar counts.
    #[serde(default)]
    pub session: Option<Session>,
}

impl DataConfig {
    /// The bar types of the bars series, in order.
    pub fn bar_types(&self) -> impl Iterator<Item = BarType> + '_ {
        self.series.iter().filter_map(|series| match series {
            Series::Bars(bar_type) => Some(*bar_type),
            Series::Ticks(_) => None,
        })
    }

    /// Whether a series reads `instrument_id`, its bars or its ticks.
    #[must_use]
    pub fn reads(&self, instrument_id: InstrumentId) -> bool {
        self.series.iter().any(|series| match series {
            Series::Bars(bar_type) => bar_type.instrument_id() == instrument_id,
            Series::Ticks(ticks) => ticks.instrument_id == instrument_id,
        })
    }

    /// Each listing of a series after its first, by its index.
    fn repeats(&self) -> impl Iterator<Item = (usize, &Series)> + '_ {
        self.series
            .iter()
            .enumerate()
            .filter(|&(i, series)| self.series[..i].contains(series))
    }

    /// Pushes the violations of `[data]`: no series; a series listed
    /// again, under each listing after its first, naming it; a bars series
    /// not aggregated on time (the session needs the bar's span), built
    /// from ticks but priced MARK (Nautilus builds bars from quotes or
    /// trades alone), on a venue other than the first bars', built from
    /// bars beside a session (live, such bars arrive after their close, so
    /// a session would drop its last bar there but not in a backtest),
    /// built from internal source bars (a live warmup requests a
    /// composite's source bars, and nothing stores internal ones), or on a
    /// futures root of the roll calendar (`rolls::Transition`) but not
    /// built from its trades (internal, LAST, not composite: Nautilus
    /// builds a root's continuous bars from its contracts' trades, and
    /// their rolls are priced from them, so other bars would never come),
    /// these two under `data.series[i]`, naming the bar type; a
    /// non-positive warmup; a session not opening before it closes.
    fn validate(&self, errors: &mut ConfigErrorCollector) {
        errors.check(
            !self.series.is_empty(),
            ConfigError::empty_field("data.series"),
        );
        for (i, series) in self.repeats() {
            errors.push(ConfigError::duplicate(
                format!("data.series[{i}]"),
                Some(series.to_string()),
            ));
        }
        let venue = self
            .bar_types()
            .next()
            .map(|bar_type| bar_type.instrument_id().venue);
        for (i, series) in self.series.iter().enumerate() {
            let Series::Bars(bar_type) = series else {
                continue;
            };
            let field = |name: &str| format!("data.series[{i}].{name}");
            let spec = bar_type.spec();
            errors.check(
                spec.is_time_aggregated(),
                ConfigError::invalid_value(
                    field("bar_spec"),
                    format!("{spec} is not aggregated on time"),
                ),
            );
            errors.check(
                bar_type.is_composite() || spec.price_type != PriceType::Mark,
                ConfigError::invalid_value(
                    field("bar_spec"),
                    format!(
                        "{spec} is priced MARK: bars are built from quotes (BID, ASK or MID) \
                         or from trades (LAST)"
                    ),
                ),
            );
            let instrument_id = bar_type.instrument_id();
            errors.check(
                venue == Some(instrument_id.venue),
                ConfigError::invalid_value(
                    field("instrument_id"),
                    format!("{instrument_id} is not on the venue of the first bars"),
                ),
            );
            errors.check(
                self.session.is_none() || !bar_type.is_composite(),
                ConfigError::mutually_exclusive_fields(["data.session".to_string(), field("from")]),
            );
            if bar_type.is_composite() {
                let source = bar_type.composite();
                errors.check(
                    source.is_externally_aggregated(),
                    ConfigError::invalid_value(
                        format!("data.series[{i}]"),
                        format!(
                            "bars {bar_type} are built from the internal bars {source}: the \
                             source bars of a composite bar type must be external, since a live \
                             warmup requests them"
                        ),
                    ),
                );
            }
            // A root rolls at any time, after the epoch too. A series'
            // bars are internal (see `Series`): LAST and not composite
            // is the rest of the rule.
            let on_root = Transition::after(instrument_id, Timestamp::UNIX_EPOCH).is_some();
            errors.check(
                !on_root || (!bar_type.is_composite() && spec.price_type == PriceType::Last),
                ConfigError::invalid_value(
                    format!("data.series[{i}]"),
                    format!(
                        "bars {bar_type} on the root {instrument_id} must be built from its \
                         trades (internal, LAST, not composite): its continuous bars are built \
                         and their rolls priced from trades"
                    ),
                ),
            );
        }
        errors.check(
            self.warmup.is_positive(),
            ConfigError::range("data.warmup", "must be positive"),
        );
        if let Some(session) = &self.session {
            errors.collect(
                session
                    .validate()
                    .map_err(|e| ConfigError::invalid_value("data.session", e.to_string())),
            );
        }
    }
}

/// A series the strategy reads, `{ data, instrument_id, ... }` in
/// `[data] series`, `data` a Nautilus data type by the name Nautilus
/// parses (`NautilusDataType::from_str`).
///
/// - `bars`, with `bar_spec` in Nautilus' `BarSpecification` fields, are
///   bars Nautilus builds of the instrument: from the ticks their price
///   type names (its trades if LAST, its quotes if BID, ASK or MID), or,
///   with `from`, from its source bars, the composite part of Nautilus'
///   bar type (`BarType::new_composite`). `from = { step = 1, aggregation
///   = "MINUTE", aggregation_source = "EXTERNAL" }` of a `1-HOUR` `LAST`
///   bar spec makes `<instrument>-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL`,
///   as OKX's 1-minute candles are.
/// - Any other is ticks the strategy reads as they are (see `TickSeries`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "SeriesFields")]
pub enum Series {
    Bars(BarType),
    Ticks(TickSeries),
}

/// A series by its bar type, `bars XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL`,
/// or as its ticks (see `TickSeries`).
impl fmt::Display for Series {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bars(bar_type) => write!(f, "bars {bar_type}"),
            Self::Ticks(ticks) => write!(f, "{ticks}"),
        }
    }
}

/// The keys of a series as the TOML has them.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SeriesFields {
    data: String,
    instrument_id: InstrumentId,
    #[serde(default)]
    bar_spec: Option<BarSpecification>,
    #[serde(default)]
    from: Option<CompositeSource>,
}

impl TryFrom<SeriesFields> for Series {
    type Error = String;

    /// Bars need a `bar_spec` and take a `from`, which Nautilus checks in
    /// `BarType::new_composite_checked`; ticks take neither.
    fn try_from(fields: SeriesFields) -> Result<Self, String> {
        let SeriesFields {
            data,
            instrument_id,
            bar_spec,
            from,
        } = fields;
        if NautilusDataType::from_str(&data).ok() != Some(NautilusDataType::Bar) {
            if bar_spec.is_some() || from.is_some() {
                return Err(format!(
                    "bar_spec and from are of bars, not of the {data} of {instrument_id}"
                ));
            }
            return Ok(Self::Ticks(TickSeries {
                data: tick_data(&data)?,
                instrument_id,
            }));
        }
        let spec =
            bar_spec.ok_or_else(|| format!("the bars of {instrument_id} need a bar_spec"))?;
        let bar_type = match from {
            None => BarType::new(instrument_id, spec, AggregationSource::Internal),
            Some(source) => BarType::new_composite_checked(
                instrument_id,
                spec,
                AggregationSource::Internal,
                source.step,
                source.aggregation,
                source.aggregation_source,
            )
            .map_err(|e| format!("invalid from {source:?} of the bars of {instrument_id}: {e}"))?,
        };
        Ok(Self::Bars(bar_type))
    }
}

/// The source bars of a composite bar type, a bars series' `from`: the
/// `composite_*` arguments of Nautilus' `BarType::new_composite`.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CompositeSource {
    pub step: usize,
    pub aggregation: BarAggregation,
    pub aggregation_source: AggregationSource,
}

/// `[trade]`: what the strategy trades, and the size of each trade. The
/// execution data of an instrument traded comes from it: its quotes,
/// when no series of `[data]` reads it (`quotes_unread_by`).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeConfig {
    /// The instruments traded, each with its size: as many as the
    /// strategy trades (`Part::INSTRUMENTS`).
    pub instruments: Vec<TradedInstrument>,
}

impl TradeConfig {
    /// Pushes the violations of `[trade]`, whatever the strategy: not
    /// `count` instruments listed; an instrument a futures root of the
    /// roll calendar (`rolls::Transition`), since an order goes to a
    /// contract; a `trade_size` not positive.
    fn validate(&self, count: usize, errors: &mut ConfigErrorCollector) {
        let listed = self.instruments.len();
        errors.check(
            listed == count,
            ConfigError::invalid_value(
                "trade.instruments",
                format!("lists {listed} instruments, the strategy trades {count}"),
            ),
        );
        for (i, traded) in self.instruments.iter().enumerate() {
            let instrument_id = traded.instrument_id;
            // A root rolls at any time: its first roll after the epoch
            // names a contract of it.
            if let Some(roll) = Transition::after(instrument_id, Timestamp::UNIX_EPOCH) {
                errors.push(ConfigError::invalid_value(
                    format!("trade.instruments[{i}].instrument_id"),
                    format!(
                        "{instrument_id} is a futures root: orders go to a contract, such as {}",
                        roll.pre
                    ),
                ));
            }
            errors.check(
                traded.trade_size.is_positive(),
                ConfigError::range(
                    format!("trade.instruments[{i}].trade_size"),
                    "must be positive",
                ),
            );
        }
    }

    /// The quotes of each instrument traded that no series of `data`
    /// reads, in order: its prices for the strategy's orders and their
    /// fills, and for a model harness' decisions, whatever the strategy; a
    /// series reading it gives them otherwise. They are not series of
    /// `[data]`: the strategy's data requirements add them
    /// (`StandardConfig::data_requirements`).
    fn quotes_unread_by<'a>(
        &'a self,
        data: &'a DataConfig,
    ) -> impl Iterator<Item = TickSeries> + 'a {
        self.instruments
            .iter()
            .filter(|traded| !data.reads(traded.instrument_id))
            .map(|traded| TickSeries {
                data: NautilusDataType::QuoteTick,
                instrument_id: traded.instrument_id,
            })
    }
}

/// An instrument traded and the size of each trade.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradedInstrument {
    pub instrument_id: InstrumentId,
    pub trade_size: Quantity,
}

impl<'de, P: Part> Deserialize<'de> for StandardConfig<P> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(Keys(PhantomData))
    }
}

/// Reads the top-level keys of a `StandardConfig<P>`: the standard tables,
/// `model` among them only of a strategy trading on a model
/// (`Part::TAKES_MODEL`), then every other key into the strategy's part
/// `P`, so its fields cannot stand for a standard table and its
/// `deny_unknown_fields` refuses any key neither takes.
struct Keys<P>(PhantomData<P>);

impl<'de, P: Part> Visitor<'de> for Keys<P> {
    type Value = StandardConfig<P>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(
            "the tables base, data, indicators, trade and, of a strategy trading on a model, \
             model, and the strategy's own fields",
        )
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let (mut base, mut data, mut indicators, mut trade) = (None, None, None, None);
        let mut model = None;
        let mut own = toml::Table::new();
        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "base" => base = Some(map.next_value()?),
                "data" => data = Some(map.next_value()?),
                "indicators" => indicators = Some(map.next_value()?),
                "trade" => trade = Some(map.next_value()?),
                // Of any other strategy, `model` is a key of its own, which
                // it refuses.
                "model" if P::TAKES_MODEL => model = Some(map.next_value()?),
                _ => {
                    let value = map.next_value::<toml::Value>()?;
                    own.insert(key, value);
                }
            }
        }
        // The strategy's fields first, so a misspelt table is named as the
        // unknown key it is rather than as the standard table it misses.
        let own = toml::Value::Table(own)
            .try_into()
            .map_err(de::Error::custom)?;
        Ok(StandardConfig {
            base: base.unwrap_or_else(P::default_base),
            data: data.ok_or_else(|| de::Error::missing_field("data"))?,
            indicators: indicators.unwrap_or_default(),
            trade: trade.ok_or_else(|| de::Error::missing_field("trade"))?,
            model: match model {
                None if P::TAKES_MODEL => return Err(de::Error::missing_field("model")),
                model => model,
            },
            own,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pairs strategy: two instruments traded, its own field a `spread`
    /// it requires positive.
    #[derive(Debug, Clone, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Pairs {
        #[serde(default = "one")]
        spread: u32,
    }

    const fn one() -> u32 {
        1
    }

    impl Part for Pairs {
        const INSTRUMENTS: usize = 2;

        fn default_base() -> StrategyConfig {
            StrategyConfig {
                strategy_id: Some(StrategyId::from("PAIRS-001")),
                ..Default::default()
            }
        }

        /// Every series it lists.
        fn reads(config: &StandardConfig<Self>) -> ConfigResult<Vec<Series>> {
            Ok(config.data.series.clone())
        }

        fn validate(config: &StandardConfig<Self>, errors: &mut ConfigErrorCollector) {
            errors.check(
                config.own.spread > 0,
                ConfigError::range("spread", "must be positive"),
            );
        }
    }

    const TOML: &str = r#"
        spread = 3

        [data]
        series = [
            { data = "bars", instrument_id = "ESH4.CME", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } },
            { data = "bars", instrument_id = "ESM4.CME", bar_spec = { step = 5, aggregation = "MINUTE", price_type = "BID" } },
            { data = "trades", instrument_id = "ESH4.CME" },
        ]
        warmup = "720h"
        session = { time_zone = "America/Chicago", open = "08:30", close = "15:00" }

        [trade]
        instruments = [
            { instrument_id = "ESH4.CME", trade_size = "1" },
            { instrument_id = "ESM4.CME", trade_size = "2" },
        ]
    "#;

    fn load(toml: &str) -> anyhow::Result<StandardConfig<Pairs>> {
        StandardConfig::load(toml)
    }

    /// The fields of the config errors of `toml`, in order.
    fn error_fields(toml: &str) -> Vec<String> {
        fields_of(&load(toml).unwrap_err())
    }

    /// The config errors `err` holds, in order.
    fn errors_of(err: &anyhow::Error) -> Vec<ConfigError> {
        let err = err
            .downcast_ref::<ConfigError>()
            .unwrap_or_else(|| panic!("not a config error: {err}"));
        match err {
            ConfigError::Multiple { errors } => errors.clone(),
            err => vec![err.clone()],
        }
    }

    /// The fields of the config errors `err` holds, in order.
    fn fields_of(err: &anyhow::Error) -> Vec<String> {
        errors_of(err)
            .iter()
            .flat_map(|err| match err {
                ConfigError::InvalidValue { field, .. }
                | ConfigError::Range { field, .. }
                | ConfigError::EmptyField { field }
                | ConfigError::MissingField { field }
                | ConfigError::Duplicate { field, .. }
                | ConfigError::InvalidReference { field, .. } => vec![field.clone()],
                ConfigError::MutuallyExclusiveFields { fields } => fields.clone(),
                err => panic!("unexpected config error {err:?}"),
            })
            .collect()
    }

    /// The message of the error refusing `toml`, which is not a config
    /// error: serde's, on the shape or the types.
    fn shape_error(toml: &str) -> String {
        shape_of(&load(toml).unwrap_err())
    }

    /// The message of `err`, which is not a config error: serde's.
    fn shape_of(err: &anyhow::Error) -> String {
        assert!(err.downcast_ref::<ConfigError>().is_none(), "{err}");
        err.to_string()
    }

    #[test]
    fn loads_the_standard_tables_and_the_strategys_own() {
        let config = load(TOML).unwrap();
        assert_eq!(
            config.data_requirements(),
            DataRequirements {
                bar_types: vec![
                    BarType::from("ESH4.CME-1-HOUR-MID-INTERNAL"),
                    BarType::from("ESM4.CME-5-MINUTE-BID-INTERNAL"),
                ],
                ticks: vec![TickSeries {
                    data: NautilusDataType::TradeTick,
                    instrument_id: InstrumentId::from("ESH4.CME"),
                }],
                warmup: SignedDuration::from_hours(720),
            }
        );
        assert_eq!(
            config.trade.instruments[1],
            TradedInstrument {
                instrument_id: InstrumentId::from("ESM4.CME"),
                trade_size: Quantity::from(2),
            }
        );
        assert_eq!(config.own.spread, 3);
        assert_eq!(config.data.session.unwrap().open.to_string(), "08:30:00");
    }

    #[test]
    fn without_a_base_the_strategys_default_base_is_used() {
        let config = load(TOML).unwrap();
        assert_eq!(config.base.strategy_id, Some(StrategyId::from("PAIRS-001")));
    }

    #[test]
    fn the_strategy_id_is_the_bases() {
        assert_eq!(
            load(TOML).unwrap().strategy_id(),
            StrategyId::from("PAIRS-001")
        );
    }

    #[test]
    fn the_base_is_nautilus_strategy_config() {
        let toml = format!(
            "{TOML}\n[base]\nstrategy_id = \"PAIRS-002\"\nuse_hyphens_in_client_order_ids = false\n"
        );
        let config = load(&toml).unwrap();
        assert_eq!(config.base.strategy_id, Some(StrategyId::from("PAIRS-002")));
        assert!(!config.base.use_hyphens_in_client_order_ids);
    }

    /// Nautilus derives the order id tag from the strategy id when the
    /// base sets none: the default base's `PAIRS-001`'s is `001`, so a
    /// strategy's default base sets its id alone.
    #[test]
    fn a_base_naming_the_strategy_id_alone_tags_its_orders_with_its_tag() {
        let config = load(TOML).unwrap();
        assert_eq!(config.base.order_id_tag, None);
        let core = nautilus_trading::strategy::StrategyCore::new_checked(config.base).unwrap();
        assert_eq!(core.order_id_tag(), Some("001"));
    }

    #[test]
    fn without_its_own_fields_the_strategys_part_takes_its_defaults() {
        let toml = TOML.replace("spread = 3", "");
        assert_eq!(load(&toml).unwrap().own.spread, 1);
    }

    #[test]
    fn an_unknown_key_in_any_table_is_refused_naming_it() {
        for (from, to, unknown) in [
            ("[data]", "[data]\nsesion = 1", "sesion"),
            (
                "{ data = \"trades\",",
                "{ data = \"trades\", instrument = 1,",
                "instrument",
            ),
            ("trade_size = \"2\"", "trade_size = \"2\", size = 1", "size"),
            ("[trade]", "[trade]\ninstrument_id = 1", "instrument_id"),
            ("spread = 3", "spread = 3\nspred = 1", "spred"),
            ("spread = 3", "spread = 3\ntrades = 1", "trades"),
        ] {
            let toml = TOML.replace(from, to);
            assert_ne!(toml, TOML, "{from}");
            let err = shape_error(&toml);
            assert!(err.contains(&format!("`{unknown}`")), "{unknown}: {err}");
        }
    }

    #[test]
    fn an_unknown_key_in_the_base_is_refused_naming_it() {
        let err = shape_error(&format!("{TOML}\n[base]\nstrategy_idd = \"X-001\"\n"));
        assert!(err.contains("strategy_idd"), "{err}");
    }

    #[test]
    fn an_unknown_table_is_refused_naming_it() {
        let err = shape_error(&format!("{TOML}\n[indicator]\nperiod = 3\n"));
        assert!(err.contains("indicator"), "{err}");
    }

    #[test]
    fn a_missing_standard_table_is_refused_naming_it() {
        let toml = TOML.replace("[trade]", "[pairs_trade]");
        let err = shape_error(&toml);
        assert!(err.contains("pairs_trade"), "{err}");
        let start = TOML.find("[trade]").unwrap();
        let toml = &TOML[..start];
        let err = shape_error(toml);
        assert!(err.contains("trade"), "{err}");
    }

    #[test]
    fn a_wrong_count_of_traded_instruments_is_refused() {
        let toml = TOML.replace("{ instrument_id = \"ESM4.CME\", trade_size = \"2\" },", "");
        assert_eq!(error_fields(&toml), ["trade.instruments"]);
    }

    #[test]
    fn a_non_positive_trade_size_is_refused() {
        let toml = TOML.replace("trade_size = \"2\"", "trade_size = \"0\"");
        assert_eq!(error_fields(&toml), ["trade.instruments[1].trade_size"]);
    }

    /// GC.GLBX is a root of the roll calendar: an order to it would go to
    /// no contract. Read by a series or not, it is refused as traded,
    /// under its field, naming it.
    #[test]
    fn a_futures_root_traded_is_refused_naming_it() {
        let toml = TOML.replace(
            "{ instrument_id = \"ESM4.CME\", trade_size",
            "{ instrument_id = \"GC.GLBX\", trade_size",
        );
        let err = load(&toml).unwrap_err();
        assert_eq!(fields_of(&err), ["trade.instruments[1].instrument_id"]);
        assert!(err.to_string().contains("GC.GLBX"), "{err}");
    }

    /// A root traded and a size not positive, of the same instrument, are
    /// each reported, with the other violations.
    #[test]
    fn a_futures_root_traded_is_reported_with_the_other_violations() {
        let toml = TOML.replace("\"720h\"", "\"0s\"").replace(
            "{ instrument_id = \"ESM4.CME\", trade_size = \"2\" }",
            "{ instrument_id = \"GC.GLBX\", trade_size = \"0\" }",
        );
        assert_eq!(
            error_fields(&toml),
            [
                "data.warmup",
                "trade.instruments[1].instrument_id",
                "trade.instruments[1].trade_size",
            ]
        );
    }

    #[test]
    fn a_bars_series_without_a_bar_spec_is_refused_naming_it() {
        let toml = TOML.replace(
            ", bar_spec = { step = 5, aggregation = \"MINUTE\", price_type = \"BID\" }",
            "",
        );
        let err = shape_error(&toml);
        assert!(
            err.contains("bar_spec") && err.contains("ESM4.CME"),
            "{err}"
        );
    }

    #[test]
    fn a_tick_series_with_a_bar_spec_is_refused_naming_it() {
        let toml = TOML.replace(
            "{ data = \"trades\", instrument_id = \"ESH4.CME\" }",
            "{ data = \"trades\", instrument_id = \"ESH4.CME\", bar_spec = { step = 1, aggregation = \"HOUR\", price_type = \"MID\" } }",
        );
        let err = shape_error(&toml);
        assert!(err.contains("bar_spec"), "{err}");
    }

    #[test]
    fn a_series_of_data_not_read_is_refused_naming_it() {
        for data in ["candles", "instruments"] {
            let toml = TOML.replace("data = \"trades\"", &format!("data = \"{data}\""));
            let err = shape_error(&toml);
            assert!(err.contains(data), "{err}");
        }
    }

    #[test]
    fn a_bar_spec_nautilus_refuses_is_refused_at_load() {
        let toml = TOML.replace(
            "step = 5, aggregation = \"MINUTE\"",
            "step = 60, aggregation = \"MINUTE\"",
        );
        let err = shape_error(&toml);
        assert!(err.contains("60"), "{err}");
    }

    #[test]
    fn a_malformed_instrument_id_is_refused_at_load() {
        let toml = TOML.replace(
            "{ data = \"trades\", instrument_id = \"ESH4.CME\" }",
            "{ data = \"trades\", instrument_id = \"ESH4\" }",
        );
        let err = shape_error(&toml);
        assert!(err.contains("ESH4"), "{err}");
    }

    const COMPOSITE_TOML: &str = r#"
        [data]
        series = [
            { data = "bars", instrument_id = "BTC-USDT-SWAP.OKX", bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }, from = { step = 1, aggregation = "MINUTE", aggregation_source = "EXTERNAL" } },
        ]
        warmup = "2h"

        [trade]
        instruments = [
            { instrument_id = "BTC-USDT-SWAP.OKX", trade_size = "1" },
            { instrument_id = "ETH-USDT-SWAP.OKX", trade_size = "1" },
        ]
    "#;

    #[test]
    fn bars_from_source_bars_are_nautilus_composite_bar_type() {
        let config = load(COMPOSITE_TOML).unwrap();
        assert_eq!(
            config.data_requirements().bar_types,
            [BarType::from(
                "BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"
            )]
        );
    }

    #[test]
    fn an_unknown_key_in_the_source_bars_is_refused_naming_it() {
        let toml =
            COMPOSITE_TOML.replace("aggregation_source = ", "source = 1, aggregation_source = ");
        let err = shape_error(&toml);
        assert!(err.contains("`source`"), "{err}");
    }

    #[test]
    fn invalid_source_bars_are_refused_naming_them() {
        let toml = COMPOSITE_TOML.replace(
            "from = { step = 1, aggregation = \"MINUTE\"",
            "from = { step = 60, aggregation = \"MINUTE\"",
        );
        let err = shape_error(&toml);
        assert!(err.contains("from"), "{err}");
    }

    #[test]
    fn a_session_is_refused_beside_bars_from_source_bars() {
        let toml = COMPOSITE_TOML.replace(
            "warmup = \"2h\"",
            "warmup = \"2h\"\nsession = { time_zone = \"UTC\", open = \"08:00\", close = \"16:00\" }",
        );
        assert_eq!(error_fields(&toml), ["data.session", "data.series[0].from"]);
    }

    /// The warmup requests a composite's source bars, which internal ones,
    /// built and never stored, cannot answer: refused under the series,
    /// naming it.
    #[test]
    fn bars_from_internal_source_bars_are_refused_naming_them() {
        let toml = COMPOSITE_TOML.replace(
            "aggregation_source = \"EXTERNAL\"",
            "aggregation_source = \"INTERNAL\"",
        );
        let err = load(&toml).unwrap_err();
        assert_eq!(fields_of(&err), ["data.series[0]"]);
        assert!(
            err.to_string()
                .contains("BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-INTERNAL"),
            "{err}"
        );
    }

    /// A config reading the bars of `instrument_id`, a bars series' other
    /// keys `bars`, and trading two contracts of the GC root.
    fn gold_futures(instrument_id: &str, bars: &str) -> String {
        format!(
            r#"
            [data]
            series = [{{ data = "bars", instrument_id = "{instrument_id}", {bars} }}]
            warmup = "720h"

            [trade]
            instruments = [
                {{ instrument_id = "GCQ6.GLBX", trade_size = "1" }},
                {{ instrument_id = "GCZ6.GLBX", trade_size = "1" }},
            ]
            "#
        )
    }

    const HOUR_LAST: &str = r#"bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }"#;
    const HOUR_MID: &str = r#"bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" }"#;

    /// Nautilus builds the continuous bars of GC.GLBX, a root of the roll
    /// calendar, from its contracts' trades, and their rolls are priced
    /// from them: its LAST bars load.
    #[test]
    fn bars_on_a_root_built_from_its_trades_load() {
        load(&gold_futures("GC.GLBX", HOUR_LAST)).unwrap();
    }

    /// Bars on a root built from its quotes or from source bars would
    /// never come: refused under the series, naming it.
    #[test]
    fn bars_on_a_root_not_built_from_its_trades_are_refused_naming_them() {
        let from_minutes = format!(
            "{HOUR_LAST}, from = {{ step = 1, aggregation = \"MINUTE\", aggregation_source = \"EXTERNAL\" }}"
        );
        for (bars, bar_type) in [
            (HOUR_MID, "GC.GLBX-1-HOUR-MID-INTERNAL"),
            (
                from_minutes.as_str(),
                "GC.GLBX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
            ),
        ] {
            let err = load(&gold_futures("GC.GLBX", bars)).unwrap_err();
            assert_eq!(fields_of(&err), ["data.series[0]"], "{bar_type}");
            assert!(err.to_string().contains(bar_type), "{err}");
        }
    }

    /// A contract of the root is a plain instrument: its bars may be
    /// built from its quotes.
    #[test]
    fn bars_on_a_contract_of_a_root_need_not_be_built_from_its_trades() {
        load(&gold_futures("GCZ6.GLBX", HOUR_MID)).unwrap();
    }

    /// The bars on a root not built from its trades, and those built from
    /// internal source bars, are reported with the other violations.
    #[test]
    fn the_rules_of_bars_on_a_root_and_of_source_bars_are_reported_with_the_others() {
        let toml = gold_futures("GC.GLBX", HOUR_MID)
            .replace(
                "price_type = \"MID\" } }]",
                "price_type = \"MID\" } }, { data = \"bars\", instrument_id = \"GCZ6.GLBX\", \
                 bar_spec = { step = 1, aggregation = \"HOUR\", price_type = \"LAST\" }, \
                 from = { step = 1, aggregation = \"MINUTE\", aggregation_source = \"INTERNAL\" } }]",
            )
            .replace("\"720h\"", "\"0s\"")
            .replace(
                "{ instrument_id = \"GCZ6.GLBX\", trade_size = \"1\" }",
                "{ instrument_id = \"GCZ6.GLBX\", trade_size = \"0\" }",
            );
        assert_eq!(
            error_fields(&toml),
            [
                "data.series[0]",
                "data.series[1]",
                "data.warmup",
                "trade.instruments[1].trade_size",
            ]
        );
    }

    #[test]
    fn bars_not_aggregated_on_time_are_refused() {
        let toml = TOML.replace(
            "step = 5, aggregation = \"MINUTE\"",
            "step = 100, aggregation = \"TICK\"",
        );
        assert_eq!(error_fields(&toml), ["data.series[1].bar_spec"]);
    }

    #[test]
    fn bars_of_ticks_priced_mark_are_refused() {
        let toml = TOML.replace("price_type = \"MID\"", "price_type = \"MARK\"");
        assert_eq!(error_fields(&toml), ["data.series[0].bar_spec"]);
    }

    /// LAST bars without `from` are built from the instrument's trades.
    #[test]
    fn bars_of_ticks_priced_last_load() {
        let toml = TOML.replace("price_type = \"MID\"", "price_type = \"LAST\"");
        let config = load(&toml).unwrap();
        assert_eq!(
            config.data.bar_types().next(),
            Some(BarType::from("ESH4.CME-1-HOUR-LAST-INTERNAL"))
        );
    }

    #[test]
    fn bars_on_a_second_venue_are_refused() {
        let toml = TOML.replace(
            "{ data = \"bars\", instrument_id = \"ESM4.CME\"",
            "{ data = \"bars\", instrument_id = \"ESM4.EUREX\"",
        );
        assert_eq!(error_fields(&toml), ["data.series[1].instrument_id"]);
    }

    #[test]
    fn ticks_may_be_of_another_venue() {
        let toml = TOML.replace(
            "{ data = \"trades\", instrument_id = \"ESH4.CME\" }",
            "{ data = \"trades\", instrument_id = \"FESX.EUREX\" }",
        );
        load(&toml).unwrap();
    }

    #[test]
    fn no_series_is_refused() {
        let start = TOML.find("series = [").unwrap();
        let end = TOML.find("warmup").unwrap();
        let toml = format!("{}series = []\n{}", &TOML[..start], &TOML[end..]);
        assert_eq!(error_fields(&toml), ["data.series"]);
    }

    /// The trades of ESH4 listed three times: each listing after the first
    /// is refused under its field, naming them, with the other violations.
    #[test]
    fn a_series_listed_again_is_refused_under_each_later_listing_with_the_other_violations() {
        let trades = r#"{ data = "trades", instrument_id = "ESH4.CME" },"#;
        let toml = TOML
            .replace(trades, &[trades; 3].join("\n"))
            .replace("\"720h\"", "\"0s\"")
            .replace("spread = 3", "spread = 0");

        let err = load(&toml).unwrap_err();

        assert_eq!(
            fields_of(&err),
            ["data.series[3]", "data.series[4]", "data.warmup", "spread"]
        );
        for err in &errors_of(&err)[..2] {
            let ConfigError::Duplicate {
                value: Some(value), ..
            } = err
            else {
                panic!("not a duplicate naming the series: {err:?}");
            };
            assert!(value.contains("TradeTick of ESH4.CME"), "{value}");
        }
    }

    #[test]
    fn a_non_positive_warmup_is_refused() {
        let toml = TOML.replace("\"720h\"", "\"0s\"");
        assert_eq!(error_fields(&toml), ["data.warmup"]);
    }

    #[test]
    fn a_session_not_opening_before_its_close_is_refused() {
        let toml = TOML.replace("open = \"08:30\"", "open = \"15:00\"");
        assert_eq!(error_fields(&toml), ["data.session"]);
    }

    #[test]
    fn an_unknown_time_zone_is_refused_at_load() {
        let err = shape_error(&TOML.replace("America/Chicago", "America/Gotham"));
        assert!(err.contains("America/Gotham"), "{err}");
    }

    /// A strategy trading on no model has no `[model]`: it is a key no
    /// table takes.
    #[test]
    fn a_strategy_taking_no_model_refuses_a_model_table() {
        let toml = format!("{TOML}\n[model]\nretrain = \"24h\"\n");
        let err = shape_error(&toml);
        assert!(err.contains("`model`"), "{err}");
    }

    /// Its id keys no model store: a base without one loads.
    #[test]
    fn a_strategy_taking_no_model_needs_no_strategy_id() {
        let config = load(&format!("{TOML}\n[base]\norder_id_tag = \"001\"\n")).unwrap();
        assert_eq!(config.base.strategy_id, None);
    }

    /// Execution needs the prices of what is traded, model or not: an
    /// instrument traded that no series reads adds its quotes, after the
    /// series' ticks; the series stay as listed.
    #[test]
    fn a_traded_instrument_no_series_reads_loads_its_quotes_into_the_requirements() {
        let toml = TOML.replace(
            "{ instrument_id = \"ESM4.CME\", trade_size = \"2\" }",
            "{ instrument_id = \"ESU4.CME\", trade_size = \"2\" }",
        );
        let config = load(&toml).unwrap();
        assert_eq!(
            config.data_requirements().ticks,
            [
                TickSeries {
                    data: NautilusDataType::TradeTick,
                    instrument_id: InstrumentId::from("ESH4.CME"),
                },
                TickSeries {
                    data: NautilusDataType::QuoteTick,
                    instrument_id: InstrumentId::from("ESU4.CME"),
                },
            ]
        );
        assert_eq!(config.data.series.len(), 3);
    }

    /// Each instrument traded is read by a series, its bars or its
    /// trades: no quotes are added.
    #[test]
    fn a_traded_instrument_a_series_reads_adds_no_quotes() {
        assert_eq!(
            load(TOML).unwrap().data_requirements().ticks,
            [TickSeries {
                data: NautilusDataType::TradeTick,
                instrument_id: InstrumentId::from("ESH4.CME"),
            }]
        );
    }

    #[test]
    fn the_strategys_own_rules_are_applied() {
        let toml = TOML.replace("spread = 3", "spread = 0");
        assert_eq!(error_fields(&toml), ["spread"]);
    }

    #[test]
    fn every_violation_is_reported_at_once() {
        let toml = TOML
            .replace("\"720h\"", "\"0s\"")
            .replace("price_type = \"MID\"", "price_type = \"MARK\"")
            .replace("trade_size = \"2\"", "trade_size = \"0\"")
            .replace("{ instrument_id = \"ESH4.CME\", trade_size = \"1\" },", "")
            .replace("spread = 3", "spread = 0");
        assert_eq!(
            error_fields(&toml),
            [
                "data.series[0].bar_spec",
                "data.warmup",
                "trade.instruments",
                "trade.instruments[0].trade_size",
                "spread",
            ]
        );
    }

    /// The rules of a strategy trading on a model, the shared config's:
    /// its `[model]`, its strategy id; and the quotes of what it trades,
    /// which its harness reads.
    mod model {
        use nautilus_indicators::average::MovingAverageType;

        use super::*;
        use crate::{
            model::{
                decide::DecisionRule,
                learner::LearnerConfig,
                targets::{Barrier, Distance, Label},
            },
            strategy::AtrArgs,
        };

        /// A strategy trading on a model: one instrument, no fields of its
        /// own.
        #[derive(Debug, Clone, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Modelled {}

        impl Part for Modelled {
            const INSTRUMENTS: usize = 1;
            const TAKES_MODEL: bool = true;

            fn default_base() -> StrategyConfig {
                StrategyConfig {
                    strategy_id: Some(StrategyId::from("MODELLED-001")),
                    ..Default::default()
                }
            }

            /// Every series it lists: the harness' answer is tested with
            /// the harness (`model::reads`).
            fn reads(config: &StandardConfig<Self>) -> ConfigResult<Vec<Series>> {
                Ok(config.data.series.clone())
            }
        }

        const TOML: &str = r#"
            [data]
            series = [
                { data = "bars", instrument_id = "XAUUSD.ICMARKETS", bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" } },
            ]
            warmup = "1h"

            [trade]
            instruments = [{ instrument_id = "XAUUSD.ICMARKETS", trade_size = "2" }]

            [model]
            decision = { every = "1h" }
            targets = { horizon = "30m" }
            label = "horizon"
            retrain = "24h"
            window = "48h"
            holdout = "12h"
            margins = { long = 0.5, short = 0.5 }
            min_pnl = 0.5
            min_trades = 30
            learner = { kind = "baseline" }
        "#;

        fn load(toml: &str) -> anyhow::Result<StandardConfig<Modelled>> {
            StandardConfig::load(toml)
        }

        /// The fields of the config errors refusing `toml`, in order.
        fn error_fields(toml: &str) -> Vec<String> {
            fields_of(&load(toml).unwrap_err())
        }

        /// The message of the error refusing `toml`, serde's on its shape.
        fn shape_error(toml: &str) -> String {
            shape_of(&load(toml).unwrap_err())
        }

        /// The reason of the one config error refusing `toml`, under a
        /// field of `[model]`.
        fn model_refusal(toml: &str) -> String {
            let err = load(toml).unwrap_err();
            match err.downcast_ref::<ConfigError>() {
                Some(ConfigError::InvalidValue { field, reason })
                    if field.starts_with("model.") =>
                {
                    reason.clone()
                }
                _ => panic!("not refused under model: {err}"),
            }
        }

        /// `TOML` labelled on a barrier 2 ATRs out and 1.5 ATRs against,
        /// closed after 45 minutes, its ATR `atr`, as the `targets` line
        /// has it, warmed up over the 3 hourly bars an ATR of 3 bars needs.
        fn with_atr(atr: &str) -> String {
            TOML.replace(r#"warmup = "1h""#, r#"warmup = "3h""#).replace(
                r#"targets = { horizon = "30m" }"#,
                &format!(
                    r#"targets = {{ barrier = {{ take_profit = {{ atr = 2 }}, stop = {{ atr = 1.5 }}, limit = "45m"{atr} }} }}"#
                ),
            )
            .replace(r#"label = "horizon""#, r#"label = "barrier""#)
        }

        #[test]
        fn loads_the_standard_tables_and_the_model() {
            let config = load(TOML).unwrap();
            assert_eq!(
                config.data_requirements().bar_types,
                [BarType::from("XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL")]
            );
            let model = config.model();
            assert_eq!(model.targets.horizon, Some(SignedDuration::from_mins(30)));
            assert_eq!(
                model.decision,
                DecisionRule::Every(SignedDuration::from_hours(1))
            );
            assert_eq!(model.label, Label::Horizon);
            assert_eq!(model.learner, LearnerConfig::Baseline {});
        }

        /// A strategy trading on a model has a `[model]`, as it has a
        /// `[trade]`.
        #[test]
        fn a_strategy_taking_a_model_requires_its_table() {
            let start = TOML.find("[model]").unwrap();
            let err = shape_error(&TOML[..start]);
            assert!(err.contains("`model`"), "{err}");
        }

        /// The targets trade what `[trade]` names: its one instrument, of
        /// its size.
        #[test]
        fn the_targets_trade_the_instrument_and_size_of_the_trade_table() {
            let targets = load(TOML).unwrap().model().targets;
            assert_eq!(
                targets.instrument_id,
                InstrumentId::from("XAUUSD.ICMARKETS")
            );
            assert_eq!(targets.quantity, Quantity::from(2));
        }

        #[test]
        fn targets_naming_an_instrument_or_a_quantity_are_refused_at_load() {
            for key in [r#"instrument_id = "XAUUSD.ICMARKETS""#, r#"quantity = "1""#] {
                let toml = TOML.replace(
                    r#"targets = { horizon"#,
                    &format!(r#"targets = {{ {key}, horizon"#),
                );
                let name = key.split(' ').next().unwrap();
                let err = shape_error(&toml);
                assert!(err.contains(&format!("`{name}`")), "{name}: {err}");
            }
        }

        #[test]
        fn a_second_traded_instrument_is_refused() {
            let toml = TOML.replace(
                r#"trade_size = "2" }]"#,
                r#"trade_size = "2" }, { instrument_id = "EURUSD.ICMARKETS", trade_size = "1" }]"#,
            );
            assert_eq!(error_fields(&toml), ["trade.instruments"]);
        }

        /// The harness reads the quotes of the instrument traded when no
        /// series reads it: the requirements hold them after the series'
        /// ticks.
        #[test]
        fn a_traded_instrument_not_read_loads_its_quotes_into_the_requirements() {
            let toml = TOML.replace(
                r#"instruments = [{ instrument_id = "XAUUSD.ICMARKETS""#,
                r#"instruments = [{ instrument_id = "EURUSD.ICMARKETS""#,
            );

            let requirements = load(&toml).unwrap().data_requirements();

            assert_eq!(
                requirements.ticks,
                [TickSeries {
                    data: NautilusDataType::QuoteTick,
                    instrument_id: InstrumentId::from("EURUSD.ICMARKETS"),
                }]
            );
        }

        /// The traded instrument's 1-minute bars are its own series of it:
        /// the harness reads them and adds no quotes.
        #[test]
        fn a_traded_instrument_a_series_reads_adds_no_quotes() {
            let requirements = load(TOML).unwrap().data_requirements();

            assert_eq!(requirements.ticks, []);
        }

        /// The series read the trades of the GC root and nothing of the
        /// gold traded: the harness reads its quotes.
        #[test]
        fn a_traded_instrument_beside_series_on_a_root_loads_its_quotes() {
            let toml = TOML.replace(
                r#"{ data = "bars", instrument_id = "XAUUSD.ICMARKETS", bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" } },"#,
                r#"{ data = "trades", instrument_id = "GC.GLBX" },"#,
            );

            let requirements = load(&toml).unwrap().data_requirements();

            assert_eq!(
                requirements.ticks,
                [
                    TickSeries {
                        data: NautilusDataType::TradeTick,
                        instrument_id: InstrumentId::from("GC.GLBX"),
                    },
                    TickSeries {
                        data: NautilusDataType::QuoteTick,
                        instrument_id: InstrumentId::from("XAUUSD.ICMARKETS"),
                    },
                ]
            );
        }

        /// Trading OKX on hourly bars built from its 1-minute candles, the
        /// one series its archive serves (`data::okx`): fill-catalog
        /// fetches those candles alone, no quotes.
        #[test]
        fn an_okx_strategy_on_composite_bars_requires_its_candles_alone() {
            let toml = TOML
                .replace(
                    r#"{ data = "bars", instrument_id = "XAUUSD.ICMARKETS", bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" } },"#,
                    r#"{ data = "bars", instrument_id = "BTC-USDT-SWAP.OKX", bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }, from = { step = 1, aggregation = "MINUTE", aggregation_source = "EXTERNAL" } },"#,
                )
                .replace(
                    r#"instruments = [{ instrument_id = "XAUUSD.ICMARKETS", trade_size = "2" }]"#,
                    r#"instruments = [{ instrument_id = "BTC-USDT-SWAP.OKX", trade_size = "0.01" }]"#,
                );

            let requirements = load(&toml).unwrap().data_requirements();

            assert_eq!(
                requirements.catalog_series(),
                [(
                    NautilusDataType::Bar,
                    "BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL".to_string()
                )]
            );
        }

        /// Gold's 1-minute LAST bars are built from its trades, and the
        /// strategy reads no quote of it: the barrier's ATR of 3 bars,
        /// decided hourly, runs on hourly bars of its trades.
        #[test]
        fn the_barrier_atr_of_a_strategy_reading_trades_runs_on_their_bars() {
            let config = load(
                &with_atr(", atr = { period = 3 }")
                    .replace(r#"price_type = "MID""#, r#"price_type = "LAST""#),
            )
            .unwrap();
            let model = config.model();

            let atr = model
                .targets
                .atr_bars(model.decision, &config.data_requirements())
                .unwrap()
                .unwrap();

            assert_eq!(
                atr.bar_type,
                BarType::from("XAUUSD.ICMARKETS-1-HOUR-LAST-INTERNAL")
            );
        }

        /// Quotes of the instrument traded the series list already are
        /// listed once.
        #[test]
        fn traded_quotes_the_series_read_are_required_once() {
            let toml = TOML.replace(
                "series = [",
                r#"series = [
                { data = "quotes", instrument_id = "XAUUSD.ICMARKETS" },"#,
            );

            let requirements = load(&toml).unwrap().data_requirements();

            assert_eq!(requirements.ticks.len(), 1, "{:?}", requirements.ticks);
        }

        /// The shared trade rules refuse the root, under its field of
        /// `[trade]`; the model, whose targets trade it, does not refuse it
        /// again.
        #[test]
        fn a_futures_root_traded_is_refused_at_load_by_the_trade_rules() {
            let toml = TOML.replace(
                r#"instruments = [{ instrument_id = "XAUUSD.ICMARKETS""#,
                r#"instruments = [{ instrument_id = "GC.GLBX""#,
            );
            let err = load(&toml).unwrap_err();
            assert_eq!(fields_of(&err), ["trade.instruments[0].instrument_id"]);
            assert!(err.to_string().contains("GC.GLBX"), "{err}");
        }

        /// A trade on a horizon of an hour, decided every hour, would still
        /// be open at the next decision.
        #[test]
        fn a_target_not_shorter_than_the_decision_cadence_is_refused() {
            let toml = TOML.replace(r#"horizon = "30m""#, r#"horizon = "1h""#);
            let reason = model_refusal(&toml);
            assert!(
                reason.contains("horizon 1h") && reason.contains("cadence 1h"),
                "{reason}"
            );
        }

        /// The strategy id keys the model's store.
        #[test]
        fn a_base_without_a_strategy_id_is_refused() {
            let toml = format!("{TOML}\n[base]\norder_id_tag = \"001\"\n");
            assert_eq!(error_fields(&toml), ["base.strategy_id"]);
        }

        #[test]
        fn a_misspelt_field_of_the_model_is_refused_at_load() {
            let err = shape_error(&TOML.replace("min_pnl", "min_pn"));
            assert!(err.contains("min_pn"), "{err}");
        }

        /// The barrier's ATR takes Nautilus' `AverageTrueRange` arguments
        /// by their names, the moving average by Nautilus' name of it.
        #[test]
        fn the_barrier_atr_takes_nautilus_average_true_range_arguments() {
            let toml = with_atr(
                r#", atr = { period = 3, ma_type = "simple", use_previous = false, value_floor = 0.5 }"#,
            );
            let barrier = load(&toml).unwrap().model().targets.barrier.unwrap();
            assert_eq!(
                barrier,
                Barrier {
                    take_profit: Distance::Atr(2.into()),
                    stop: Distance::Atr(rust_decimal::Decimal::new(15, 1)),
                    limit: SignedDuration::from_mins(45),
                    atr: Some(AtrArgs {
                        period: 3,
                        ma_type: Some(MovingAverageType::Simple),
                        use_previous: Some(false),
                        value_floor: Some(0.5),
                    }),
                }
            );
        }

        #[test]
        fn a_moving_average_nautilus_does_not_name_is_refused_at_load() {
            let err = shape_error(&with_atr(
                r#", atr = { period = 3, ma_type = "TRIANGULAR" }"#,
            ));
            assert!(err.contains("TRIANGULAR"), "{err}");
        }

        /// Nautilus' `AverageTrueRange::new` panics on an argument it
        /// refuses, and the load builds the ATR.
        #[test]
        #[should_panic(expected = "value_floor")]
        fn a_barrier_atr_argument_nautilus_refuses_panics_at_load() {
            let _ = load(&with_atr(r#", atr = { period = 3, value_floor = -1.0 }"#));
        }

        #[test]
        #[should_panic(expected = "period")]
        fn a_barrier_atr_of_no_bars_panics_at_load() {
            let _ = load(&with_atr(r#", atr = { period = 0 }"#));
        }

        /// The model's violation is its horizon of an hour, decided every
        /// hour, reported under the horizon's field, `model.targets.horizon`,
        /// after the standard ones and the strategy id's.
        #[test]
        fn reports_the_models_violations_with_the_standard_ones() {
            let toml = format!("{TOML}\n[base]\norder_id_tag = \"001\"\n")
                .replace("\"1h\"\n", "\"0s\"\n")
                .replace(r#"horizon = "30m""#, r#"horizon = "1h""#);
            assert_eq!(
                error_fields(&toml),
                ["data.warmup", "base.strategy_id", "model.targets.horizon"]
            );
        }

        /// Three violations of the model, each reported under its field at
        /// once: margins naming no side, no trade required on the holdout,
        /// and a horizon of an hour decided every hour.
        #[test]
        fn reports_every_violation_of_the_model_under_its_field() {
            let toml = TOML
                .replace("margins = { long = 0.5, short = 0.5 }", "margins = {}")
                .replace("min_trades = 30", "min_trades = 0")
                .replace(r#"horizon = "30m""#, r#"horizon = "1h""#);

            let mut fields = error_fields(&toml);
            fields.sort();

            assert_eq!(
                fields,
                ["model.margins", "model.min_trades", "model.targets.horizon"]
            );
        }

        /// TOML with a barrier labelled on, and `meta` after it.
        fn with_meta(meta: &str) -> String {
            format!(
                "{}\n{meta}",
                TOML.replace(
                    r#"targets = { horizon = "30m" }"#,
                    r#"targets = { horizon = "30m", barrier = { take_profit = { bps = 10 }, stop = { bps = 10 }, limit = "45m" } }"#,
                )
                .replace(r#"label = "horizon""#, r#"label = "barrier""#)
            )
        }

        /// The meta model's violations are reported under `[model.meta]`'s
        /// fields: a threshold of 1.5 is no probability, and it requires no
        /// trade on its holdout.
        #[test]
        fn reports_the_meta_models_violations_under_its_fields() {
            let toml = with_meta(
                r#"
                [model.meta]
                window = "48h"
                holdout = "12h"
                threshold = 1.5
                min_pnl = 0.5
                min_trades = 0
                learner = { kind = "baseline" }
                "#,
            );

            let mut fields = error_fields(&toml);
            fields.sort();

            assert_eq!(fields, ["model.meta.min_trades", "model.meta.threshold"]);
        }

        /// A meta model is a `[model.meta]` table, and no more: a meta of
        /// the meta is an unknown field. It learns the barrier trades of a
        /// primary labelled on the barrier.
        #[test]
        fn loads_a_meta_model_from_its_table_alone_and_no_deeper() {
            let toml = with_meta(
                r#"
                [model.meta]
                window = "48h"
                holdout = "12h"
                threshold = 0.55
                min_pnl = 0.5
                min_trades = 30
                learner = { kind = "baseline" }
                "#,
            );

            let config = load(&toml).unwrap();
            assert_eq!(config.model().meta.unwrap().threshold, 0.55);

            let nested = format!("{toml}\n[model.meta.meta]\nthreshold = 0.5\n");
            let err = shape_error(&nested);
            assert!(err.contains("unknown field `meta`"), "{err}");
        }
    }
}
