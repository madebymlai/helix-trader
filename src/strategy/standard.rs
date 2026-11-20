use std::{
    fmt::{Debug, Display},
    marker::PhantomData,
    str::FromStr,
};

use jiff::{SignedDuration, Timestamp};
use nautilus_common::config::{ConfigError, ConfigErrorCollector, ConfigResult};
use nautilus_core::Params;
use nautilus_model::{
    data::{BarSpecification, BarType, NautilusDataType, get_arrow_schema},
    enums::{AggregationSource, BarAggregation, PriceType},
    identifiers::{InstrumentId, StrategyId},
};
use nautilus_trading::strategy::StrategyConfig;
use serde::{
    Deserialize, Deserializer,
    de::{self, DeserializeOwned, DeserializeSeed, MapAccess, Visitor},
};

use super::{
    Declaration, IndicatorsConfig,
    conversions::{Conversions, Rates},
    requirements::{CustomSeries, DataRequirements, TickSeries, tick_data},
    risk::{Risk, Stake},
    session::Session,
};
use crate::{
    model::config::{ModelConfig, ModelSection},
    rolls::Transition,
    venues,
};

/// The config in every strategy's TOML.
///
/// It has the standard tables and the strategy's own part `P` at the top level. Every table and `P`
/// deny unknown keys. `load` first refuses a config of the wrong shape or types, as serde reads it.
/// It then refuses every violation of the rules at once (see `validate`).
///
/// - `[base]`: Nautilus' `StrategyConfig`. Without one, `P::default_base` applies. It also claims
///   the external orders of the traded instruments (see `claiming`).
/// - `[data]`, `[indicators]`, `[trade]`: see `DataConfig`, `IndicatorsConfig`, `TradeConfig`.
/// - `[model]`: required for a strategy trading on a model (`Part::TAKES_MODEL`), taken by no
///   other.
#[derive(Debug, Clone)]
pub struct StandardConfig<P: Part> {
    pub base: StrategyConfig,
    pub data: DataConfig,
    pub indicators: IndicatorsConfig,
    pub trade: TradeConfig,
    pub model: Option<ModelSection>,
    pub own: P,
}

/// A strategy's part of its config: its own fields at the top level of its TOML, and its rules.
pub trait Part: DeserializeOwned + Debug + Clone {
    /// The number of instruments the strategy trades, as `[trade] instruments` lists them.
    const INSTRUMENTS: usize;
    /// The indicators the strategy's code reads, features included. `[indicators]` configures no
    /// others.
    const INDICATORS: &'static [&'static dyn Declaration] = &[];
    /// Whether the strategy trades on a model (`model::ModelStrategy`).
    ///
    /// If so, its TOML has a `[model]`, it trades one instrument and it builds its features
    /// (`model::ModelPart`).
    const TAKES_MODEL: bool = false;
    /// Whether the strategy sizes by risk at `[trade] risk` (`StandardConfig::stake`).
    ///
    /// If so, its `[trade]` takes `risk` and `conversions`. Otherwise it takes neither. A model
    /// part sizes by risk, with its model priced at the reference stake.
    const SIZES_BY_RISK: bool = true;
    /// Returns the `[base]` to use when the TOML has none.
    ///
    /// It sets the strategy id alone. Nautilus tags orders with the id's tag when `order_id_tag` is
    /// unset (`StrategyCore::new_checked`).
    fn default_base() -> StrategyConfig;
    /// Returns the series the strategy reads for `config`, which `[data] series` lists.
    ///
    /// Asked only for a config with `INSTRUMENTS` instruments, no repeated series and every
    /// indicator built.
    ///
    /// # Errors
    ///
    /// Returns an error if the strategy cannot read `config`. With several errors, it is a
    /// `ConfigError::Multiple` of the strategy's own errors, each under its field.
    fn reads(config: &StandardConfig<Self>) -> ConfigResult<Vec<Series>>;
    /// Pushes every violation of the strategy's own rules in `config` onto `errors`, each under its
    /// field.
    fn validate(_config: &StandardConfig<Self>, _errors: &mut ConfigErrorCollector) {}
}

impl<P: Part> StandardConfig<P> {
    /// Loads and validates the config in `toml`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The TOML does not match the config's shape or types, as serde reads it.
    /// - `validate` fails with a `ConfigError`.
    pub fn load(toml: &str) -> anyhow::Result<Self> {
        let config: Self = toml::from_str(toml)?;
        config.validate()?;
        Ok(config)
    }

    /// Validates the config.
    ///
    /// # Errors
    ///
    /// Returns an error if the config violates a rule. With several violations, it is a
    /// `ConfigError::Multiple` of every violation, each with its field.
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

    /// Pushes onto `errors` the part's errors in reading the config.
    ///
    /// Otherwise pushes each series listed but not read, and each series read but not listed.
    fn validate_series(&self, errors: &mut ConfigErrorCollector) {
        if self.trade.instruments.len() != P::INSTRUMENTS
            || self.data.repeats().next().is_some()
            || !self.indicators.builds(P::INDICATORS)
        {
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

    fn validate_model(&self, errors: &mut ConfigErrorCollector) {
        let Some(model) = &self.model else {
            return;
        };
        errors.check(
            self.base.strategy_id.is_some(),
            ConfigError::missing_field("base.strategy_id"),
        );

        if let [traded] = self.trade.instruments.as_slice() {
            errors.collect(
                model
                    .clone()
                    .trading(traded, self.risk(), self.trade.conversions.clone())
                    .validate(&self.data_requirements()),
            );
        }
    }

    /// The strategy's id.
    ///
    /// # Panics
    ///
    /// Panics if the base has none. A model strategy's rules refuse that.
    #[must_use]
    pub fn strategy_id(&self) -> StrategyId {
        self.base
            .strategy_id
            .expect("a validated config has a strategy id")
    }

    /// Returns the model.
    ///
    /// Its target trades `[trade]`'s instrument at its risk, the reference stake, through its
    /// conversions.
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - The config has no model.
    /// - `[trade]` does not list exactly one instrument.
    /// - The config has no risk (see `stake`).
    #[must_use]
    pub fn model(&self) -> ModelConfig {
        let (Some(model), [traded]) = (&self.model, self.trade.instruments.as_slice()) else {
            panic!("a validated config of a strategy trading on a model has it, of one instrument");
        };
        model
            .clone()
            .trading(traded, self.risk(), self.trade.conversions.clone())
    }

    /// Returns the config's stake, `[trade] risk` at multiple 1.
    ///
    /// # Panics
    ///
    /// Panics if the config has no risk (see `Part::SIZES_BY_RISK`).
    #[must_use]
    pub fn stake(&self) -> Stake {
        Stake::new(self.risk())
    }

    /// Returns the rates equity converts at into the P&L currency.
    ///
    /// These are Nautilus' cached rates for `[trade] conversions` (`Conversions::rates`).
    #[must_use]
    pub fn rates(&self) -> Rates {
        self.trade.conversions.rates()
    }

    ///
    /// # Panics
    ///
    /// Panics if the config has none (see `stake`).
    fn risk(&self) -> Risk {
        self.trade.risk.expect(
            "the strategy does not size its trades by risk (`Part::SIZES_BY_RISK`): its \
             `[trade]` has no risk, so no stake",
        )
    }

    /// Returns what the strategy needs from data:
    /// - Its series.
    /// - The quotes of traded instruments that no series prices.
    /// - The series its conversion rates are read from.
    /// - The warmup.
    #[must_use]
    pub fn data_requirements(&self) -> DataRequirements {
        let mut ticks: Vec<TickSeries> = self
            .data
            .series
            .iter()
            .filter_map(|series| match series {
                Series::Ticks(ticks) => Some(ticks.clone()),
                Series::Bars(_) | Series::Custom(_) => None,
            })
            .chain(self.trade.quotes_unpriced_by(&self.data))
            .collect();
        let mut bar_types: Vec<BarType> = self.data.bar_types().collect();
        for series in self.trade.conversions.series() {
            match series {
                Series::Bars(bars) if !bar_types.contains(&bars) => bar_types.push(bars),
                Series::Ticks(read) if !ticks.contains(&read) => ticks.push(read),
                Series::Bars(_) | Series::Ticks(_) => {}
                Series::Custom(custom) => {
                    unreachable!("a pair's rates are read from its bars or quotes, not {custom}")
                }
            }
        }
        DataRequirements {
            bar_types,
            ticks,
            custom: self
                .data
                .custom()
                .map(|(_, custom)| custom.clone())
                .collect(),
            warmup: self.data.warmup,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DataConfig {
    /// The series read, as the part answers (`Part::reads`).
    ///
    /// A custom series has a type Nautilus holds a schema for. The history source in the row for
    /// that type serves it, or else the catalog does (`venues::serves_custom`). It sits under a
    /// catalog folder that no other series of its type takes.
    pub series: Vec<Series>,
    /// How far back a live node's warmup starts. A backtest requests none.
    pub warmup: SignedDuration,
    /// The hours traded. Only bars wholly inside count. Without a session, every bar counts.
    #[serde(default)]
    pub session: Option<Session>,
}

impl DataConfig {
    pub fn bar_types(&self) -> impl Iterator<Item = BarType> + '_ {
        self.series.iter().filter_map(|series| match series {
            Series::Bars(bar_type) => Some(*bar_type),
            Series::Ticks(_) | Series::Custom(_) => None,
        })
    }

    fn custom(&self) -> impl Iterator<Item = (usize, &CustomSeries)> + '_ {
        self.series
            .iter()
            .enumerate()
            .filter_map(|(i, series)| match series {
                Series::Custom(custom) => Some((i, custom)),
                Series::Bars(_) | Series::Ticks(_) => None,
            })
    }

    /// Returns whether a series prices `instrument_id`.
    ///
    /// Nautilus prices a market order and the top of a simulated L1 book from its bars, quotes or
    /// trades. The risk engine prices from the cache's bid or ask, last trade or external bar
    /// (`Cache::price` reads quotes and trades alone). An L1 book ignores deltas. So depth, deltas
    /// and custom series do not price it.
    #[must_use]
    pub fn prices(&self, instrument_id: InstrumentId) -> bool {
        self.series.iter().any(|series| match series {
            Series::Bars(bar_type) => bar_type.instrument_id() == instrument_id,
            Series::Ticks(ticks) => {
                ticks.instrument_id == instrument_id
                    && matches!(
                        ticks.data,
                        NautilusDataType::QuoteTick | NautilusDataType::TradeTick
                    )
            }
            Series::Custom(_) => false,
        })
    }

    fn repeats(&self) -> impl Iterator<Item = (usize, &Series)> + '_ {
        self.series
            .iter()
            .enumerate()
            .filter(|&(i, series)| self.series[..i].contains(series))
    }

    /// Pushes the violations of `[data]`.
    ///
    /// They include bars that are:
    /// - Not aggregated on time: the session needs the bar's span.
    /// - Priced MARK from ticks: Nautilus builds bars from quotes or trades alone.
    /// - Built from bars beside a session: live, such bars arrive after their close, so a session
    ///   would drop its last bar there but not in a backtest.
    /// - Built from internal source bars: a live warmup requests a composite's source bars, and
    ///   nothing stores internal ones.
    /// - On a futures root but not built from its trades: Nautilus builds a root's continuous bars
    ///   from its contracts' trades, and prices their rolls from them.
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

        for (i, custom) in self.custom() {
            // Listed again whole, this series is one violation, the repeat's
            if self.series[..i].contains(&self.series[i]) {
                continue;
            }
            let field = format!("data.series[{i}]");
            let folder = custom.catalog_folder();
            let before = || self.custom().take_while(|&(j, _)| j < i);
            if let Some((j, _)) = before().find(|(_, listed)| {
                listed.type_name() == custom.type_name() && listed.catalog_folder() == folder
            }) {
                errors.push(ConfigError::duplicate(
                    field.clone(),
                    Some(format!(
                        "{custom}, stored under the catalog folder {folder} of {} as \
                         data.series[{j}] is",
                        custom.type_name()
                    )),
                ));
            } else if let Some((j, listed, contract)) = before().find_map(|(j, listed)| {
                custom
                    .contract_folder_shared_with(listed)
                    .map(|contract| (j, listed, contract))
            }) {
                errors.push(ConfigError::duplicate(
                    field.clone(),
                    Some(format!(
                        "{custom}, stored under the catalog folder {contract} of {} as \
                         data.series[{j}], {listed}, is while {contract} stands for its root: a \
                         contract's series is read beside its root's only as the root's on it, \
                         of the root's metadata, else under an identifier of its own",
                        custom.type_name()
                    )),
                ));
            }
            if get_arrow_schema(custom.type_name()).is_none() {
                errors.push(ConfigError::invalid_value(
                    field.clone(),
                    format!(
                        "Nautilus has no custom data type {} registered, so it reads none of \
                         {custom}: it decodes custom data by its type's registered schema, and \
                         helix registers each type it compiles as it starts",
                        custom.type_name()
                    ),
                ));
            }
            if let Err(e) = venues::serves_custom(custom) {
                errors.push(ConfigError::invalid_value(field, format!("{e:#}")));
            }
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
            // A root rolls at any time, after the epoch too, and a series' bars are internal (see
            // `Series`), so LAST and not composite is the rest of the rule.
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

/// A series the strategy reads, in `[data] series`.
///
/// It is `{ data, instrument_id, ... }` or `{ data, identifier, ... }`. `data` is a Nautilus data
/// type, by the name `NautilusDataType::from_str` parses.
///
/// - `bars`, with `bar_spec` in Nautilus' `BarSpecification` fields, are bars Nautilus builds from
///   the ticks their price type names (trades if LAST, quotes if BID, ASK or MID). With `from`, it
///   builds them from source bars instead, the composite part of Nautilus' bar type
///   (`BarType::new_composite`). `from = { step = 1, aggregation = "MINUTE", aggregation_source =
///   "EXTERNAL" }` of a `1-HOUR` `LAST` bar spec makes
///   `<instrument>-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL`, as OKX's 1-minute candles are.
/// - `Custom:<type name>` with an `identifier`, and `metadata` if its type's source asks for any,
///   is a custom series (see `CustomSeries`). It brings no instrument. Under a root's id it rolls
///   as the root's other series do (`CustomSeries::root`). It is keyed by its identifier alone: an
///   `instrument_id` is refused, pointing at the identifier.
/// - Any other series, of an instrument, is ticks (see `TickSeries`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(try_from = "SeriesFields")]
pub enum Series {
    Bars(BarType),
    Ticks(TickSeries),
    Custom(CustomSeries),
}

impl Display for Series {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Bars(bar_type) => write!(f, "bars {bar_type}"),
            Self::Ticks(ticks) => write!(f, "{ticks}"),
            Self::Custom(custom) => write!(f, "{custom}"),
        }
    }
}

impl Series {
    /// The quotes of `instrument_id`.
    #[must_use]
    pub const fn quotes(instrument_id: InstrumentId) -> Self {
        Self::ticks(NautilusDataType::QuoteTick, instrument_id)
    }

    /// The trades of `instrument_id`.
    #[must_use]
    pub const fn trades(instrument_id: InstrumentId) -> Self {
        Self::ticks(NautilusDataType::TradeTick, instrument_id)
    }

    /// The book depth snapshots of `instrument_id`.
    #[must_use]
    pub const fn depth(instrument_id: InstrumentId) -> Self {
        Self::ticks(NautilusDataType::OrderBookDepth, instrument_id)
    }

    /// The book deltas of `instrument_id`.
    #[must_use]
    pub const fn deltas(instrument_id: InstrumentId) -> Self {
        Self::ticks(NautilusDataType::OrderBookDelta, instrument_id)
    }

    const fn ticks(data: NautilusDataType, instrument_id: InstrumentId) -> Self {
        Self::Ticks(TickSeries {
            data,
            instrument_id,
        })
    }
}

/// The keys of a series as the TOML has them.
///
/// They are the keys of Nautilus' `DataType::try_new` and of a custom series. They leave out the
/// `topic` key of Nautilus' `DataType` deserializer, which sets the topic the data is subscribed
/// and published under.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SeriesFields {
    data: String,
    #[serde(default)]
    instrument_id: Option<InstrumentId>,
    #[serde(default)]
    identifier: Option<String>,
    #[serde(default)]
    metadata: Option<Params>,
    #[serde(default)]
    bar_spec: Option<BarSpecification>,
    #[serde(default)]
    from: Option<CompositeSource>,
}

impl TryFrom<SeriesFields> for Series {
    type Error = String;

    /// Builds a series from its fields.
    ///
    /// - A custom series needs an `identifier` and takes `metadata`. It takes no instrument.
    /// - Any other series needs an `instrument_id` and takes neither.
    /// - Bars need a `bar_spec` and take a `from`, which Nautilus checks
    ///   (`BarType::new_composite_checked`).
    fn try_from(fields: SeriesFields) -> Result<Self, String> {
        let SeriesFields {
            data,
            instrument_id,
            identifier,
            metadata,
            bar_spec,
            from,
        } = fields;
        if let Ok(NautilusDataType::Custom { type_name }) = NautilusDataType::from_str(&data) {
            let Some(identifier) = identifier else {
                return Err(match instrument_id {
                    Some(instrument_id) => format!(
                        "the {data} is keyed by an identifier, an instrument's custom data by \
                         its id, not by an instrument_id: write identifier = \"{instrument_id}\""
                    ),
                    None => format!(
                        "the {data} needs an identifier, which keys a custom series in the catalog"
                    ),
                });
            };
            if instrument_id.is_some() || bar_spec.is_some() || from.is_some() {
                return Err(format!(
                    "the {data} under the identifier {identifier:?} is keyed by its identifier: \
                     it takes no instrument_id, bar_spec or from"
                ));
            }
            return CustomSeries::new(&type_name, identifier, metadata)
                .map(Self::Custom)
                .map_err(|e| format!("{e:#}"));
        }
        if identifier.is_some() || metadata.is_some() {
            return Err(format!(
                "identifier and metadata are of a custom series, not of the {data}"
            ));
        }
        let Some(instrument_id) = instrument_id else {
            return Err(format!("the {data} needs an instrument_id"));
        };
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

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CompositeSource {
    pub step: usize,
    pub aggregation: BarAggregation,
    pub aggregation_source: AggregationSource,
}

/// The `[trade]` table.
///
/// It lists what the strategy trades. If the strategy sizes by risk, it also gives how much a trade
/// risks and the FX pairs equity converts through.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeConfig {
    /// The instruments traded, each by its id alone.
    pub instruments: Vec<TradedInstrument>,
    /// The fraction of equity a trade risks at the config's stake (`StandardConfig::stake`), such
    /// as `"0.5%"`. Present if the strategy sizes by risk.
    pub risk: Option<Risk>,
    /// The FX pairs equity converts through into the instrument's P&L currency
    /// (`StandardConfig::rates`). They are given by id, all on one venue (`Conversions::validate`).
    /// A strategy that does not size by risk has none.
    #[serde(default)]
    pub conversions: Conversions,
}

impl TradeConfig {
    #[must_use]
    pub fn instrument_ids(&self) -> Vec<InstrumentId> {
        self.instruments
            .iter()
            .map(|traded| traded.instrument_id)
            .collect()
    }

    #[must_use]
    pub fn untraded_pairs(&self) -> Vec<InstrumentId> {
        let traded = self.instrument_ids();
        self.conversions
            .pairs()
            .iter()
            .filter(|pair| !traded.contains(pair))
            .copied()
            .collect()
    }

    /// Pushes the violations of `[trade]`.
    ///
    /// They are:
    /// - Listing a number of instruments other than `count`.
    /// - An instrument that is a futures root of the roll calendar (`rolls::Transition`), since an
    ///   order goes to a contract.
    /// - A `risk` that is not a proper fraction of equity (`Risk::is_proper`).
    /// - Pairs on more than one venue, or off the venue of a traded instrument whose broker account
    ///   has a base currency (`Conversions::validate`).
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
            // A root rolls at any time, so its first roll after the epoch names one of its
            // contracts
            if let Some(roll) = Transition::after(instrument_id, Timestamp::UNIX_EPOCH) {
                errors.push(ConfigError::invalid_value(
                    format!("trade.instruments[{i}].instrument_id"),
                    format!(
                        "{instrument_id} is a futures root: orders go to a contract, such as {}",
                        roll.pre
                    ),
                ));
            }
        }
        errors.check(
            self.risk.is_none_or(Risk::is_proper),
            ConfigError::range("trade.risk", "must be above 0% and below 100% of equity"),
        );
        self.conversions.validate(&self.instrument_ids(), errors);
    }

    /// Returns the quotes of each traded instrument that no series of `data` prices.
    ///
    /// `DataConfig::prices` decides which series price an instrument. Without these quotes:
    /// - An instrument read only through its depth or book deltas has its market orders denied for
    ///   want of a price.
    /// - An instrument read only through a custom series under its id is not loaded into a
    ///   backtest, because that series brings no instrument.
    ///
    /// The quotes are not `[data]` series: `StandardConfig::data_requirements` adds them.
    fn quotes_unpriced_by<'a>(
        &'a self,
        data: &'a DataConfig,
    ) -> impl Iterator<Item = TickSeries> + 'a {
        self.instruments
            .iter()
            .filter(|traded| !data.prices(traded.instrument_id))
            .map(|traded| TickSeries {
                data: NautilusDataType::QuoteTick,
                instrument_id: traded.instrument_id,
            })
    }
}

/// An instrument traded, by its id alone.
///
/// A trade is sized at a stake of `[trade] risk`, or by the strategy itself. A `trade_size` is
/// refused as it is read (`TradedFields`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(from = "TradedFields")]
pub struct TradedInstrument {
    pub instrument_id: InstrumentId,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TradedFields {
    instrument_id: InstrumentId,
    trade_size: Option<TradeSize>,
}

impl From<TradedFields> for TradedInstrument {
    fn from(fields: TradedFields) -> Self {
        let TradedFields {
            instrument_id,
            trade_size: None,
        } = fields;
        Self { instrument_id }
    }
}

/// A size of an instrument traded, which no value can be.
///
/// Any is refused, pointing to `[trade] risk`. So an old config is migrated knowingly rather than
/// refused as an unknown key.
enum TradeSize {}

impl<'de> Deserialize<'de> for TradeSize {
    fn deserialize<D: Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        Err(de::Error::custom(
            "an instrument traded has no `trade_size`: every trade is sized at the strategy's \
             stake, `[trade] risk`, the fraction of equity a trade risks, such as \"0.5%\", \
             times the multiple the strategy picks",
        ))
    }
}

impl<'de, P: Part> Deserialize<'de> for StandardConfig<P> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(Keys(PhantomData))
    }
}

/// Reads the top-level keys.
///
/// The standard tables come first, then every other key goes into `P`. So the fields of `P` cannot
/// stand for a standard table.
struct Keys<P>(PhantomData<P>);

impl<'de, P: Part> Visitor<'de> for Keys<P> {
    type Value = StandardConfig<P>;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
                "trade" => trade = Some(map.next_value_seed(TradeKeys::<P>(PhantomData))?),
                // For any other strategy, `model` is a key of its own, which that strategy refuses
                "model" if P::TAKES_MODEL => model = Some(map.next_value()?),
                _ => {
                    let value = map.next_value::<toml::Value>()?;
                    own.insert(key, value);
                }
            }
        }
        // The strategy's fields first, so a misspelt table is named as the unknown key, not as the
        // table it misses.
        let own = toml::Value::Table(own)
            .try_into()
            .map_err(de::Error::custom)?;
        let trade = trade.ok_or_else(|| de::Error::missing_field("trade"))?;
        Ok(StandardConfig {
            base: claiming(base.unwrap_or_else(P::default_base), &trade),
            data: data.ok_or_else(|| de::Error::missing_field("data"))?,
            indicators: indicators.unwrap_or_default(),
            trade,
            model: match model {
                None if P::TAKES_MODEL => return Err(de::Error::missing_field("model")),
                model => model,
            },
            own,
        })
    }
}

/// Returns `base`, claiming the external orders of the traded instruments too.
///
/// It adds the instruments `trade` trades to `external_order_instrument_ids`. A live node registers
/// a strategy's claims as it adds the strategy (`LiveNode::add_strategy`). So the orders and
/// positions on the account for those instruments, placed by hand or before a restart, are the
/// strategy's. The pairs `trade` converts through and the instruments the features read are not
/// traded, so none is claimed.
fn claiming(mut base: StrategyConfig, trade: &TradeConfig) -> StrategyConfig {
    let mut claimed = base
        .external_order_instrument_ids
        .take()
        .unwrap_or_default();
    for instrument_id in trade.instrument_ids() {
        if !claimed.contains(&instrument_id) {
            claimed.push(instrument_id);
        }
    }
    base.external_order_instrument_ids = Some(claimed);
    base
}

/// Reads `[trade]` with the keys the part takes.
///
/// A part that sizes by risk (`Part::SIZES_BY_RISK`) takes `risk`, which is required, and
/// `conversions`. Another part sizes no trade from equity, so each is a key it does not take
/// (`conversions = []` lists no pair, as leaving it out does).
struct TradeKeys<P>(PhantomData<P>);

impl<'de, P: Part> DeserializeSeed<'de> for TradeKeys<P> {
    type Value = TradeConfig;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<TradeConfig, D::Error> {
        let trade = TradeConfig::deserialize(deserializer)?;
        let converts = !trade.conversions.pairs().is_empty();
        match (P::SIZES_BY_RISK, trade.risk) {
            (true, None) => Err(de::Error::missing_field("risk")),
            (false, Some(_)) => Err(de::Error::unknown_field("risk", &["instruments"])),
            (false, None) if converts => {
                Err(de::Error::unknown_field("conversions", &["instruments"]))
            }
            _ => Ok(trade),
        }
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::data::DataType;
    use rstest::rstest;
    use rust_decimal::Decimal;

    use super::*;

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
            { data = "bars", instrument_id = "ESH24.CME", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" } },
            { data = "bars", instrument_id = "ESM24.CME", bar_spec = { step = 5, aggregation = "MINUTE", price_type = "BID" } },
            { data = "trades", instrument_id = "ESH24.CME" },
        ]
        warmup = "720h"
        session = { time_zone = "America/Chicago", open = "08:30", close = "15:00" }

        [trade]
        instruments = [
            { instrument_id = "ESH24.CME" },
            { instrument_id = "ESM24.CME" },
        ]
        risk = "0.5%"
    "#;

    fn load(toml: &str) -> anyhow::Result<StandardConfig<Pairs>> {
        venues::register_custom_data();
        StandardConfig::load(toml)
    }

    fn error_fields(toml: &str) -> Vec<String> {
        fields_of(&load(toml).unwrap_err())
    }

    fn errors_of(err: &anyhow::Error) -> Vec<ConfigError> {
        let err = err
            .downcast_ref::<ConfigError>()
            .unwrap_or_else(|| panic!("not a config error: {err}"));
        match err {
            ConfigError::Multiple { errors } => errors.clone(),
            err => vec![err.clone()],
        }
    }

    fn fields_of(err: &anyhow::Error) -> Vec<String> {
        errors_of(err)
            .iter()
            .flat_map(|e| match e {
                ConfigError::InvalidValue { field, .. }
                | ConfigError::Range { field, .. }
                | ConfigError::EmptyField { field }
                | ConfigError::MissingField { field }
                | ConfigError::Duplicate { field, .. }
                | ConfigError::InvalidReference { field, .. } => vec![field.clone()],
                ConfigError::MutuallyExclusiveFields { fields } => fields.clone(),
                e => panic!("unexpected config error {e:?}"),
            })
            .collect()
    }

    fn shape_error(toml: &str) -> String {
        shape_of(&load(toml).unwrap_err())
    }

    fn shape_of(err: &anyhow::Error) -> String {
        assert!(err.downcast_ref::<ConfigError>().is_none(), "{err}");
        err.to_string()
    }

    #[rstest]
    fn loads_the_standard_tables_and_the_strategys_own() {
        let config = load(TOML).unwrap();
        assert_eq!(
            config.data_requirements(),
            DataRequirements {
                bar_types: vec![
                    BarType::from("ESH24.CME-1-HOUR-MID-INTERNAL"),
                    BarType::from("ESM24.CME-5-MINUTE-BID-INTERNAL"),
                ],
                ticks: vec![TickSeries {
                    data: NautilusDataType::TradeTick,
                    instrument_id: InstrumentId::from("ESH24.CME"),
                }],
                custom: vec![],
                warmup: SignedDuration::from_hours(720),
            }
        );
        assert_eq!(
            config.trade.instruments,
            [
                TradedInstrument {
                    instrument_id: InstrumentId::from("ESH24.CME"),
                },
                TradedInstrument {
                    instrument_id: InstrumentId::from("ESM24.CME"),
                },
            ]
        );
        assert_eq!(config.trade.risk, Some("0.5%".parse().unwrap()));
        assert_eq!(config.own.spread, 3);
        let session = config.data.session.unwrap();
        assert_eq!(
            (session.time_zone.iana_name(), session.open, session.close),
            (
                Some("America/Chicago"),
                jiff::civil::time(8, 30, 0, 0),
                jiff::civil::time(15, 0, 0, 0)
            )
        );
    }

    #[rstest]
    fn without_a_base_the_strategys_default_base_is_used() {
        let config = load(TOML).unwrap();
        assert_eq!(config.base.strategy_id, Some(StrategyId::from("PAIRS-001")));
        assert_eq!(
            config.base.external_order_instrument_ids,
            Some(vec![
                InstrumentId::from("ESH24.CME"),
                InstrumentId::from("ESM24.CME")
            ])
        );
    }

    #[rstest]
    fn the_base_is_nautilus_strategy_config() {
        let toml = format!(
            "{TOML}\n[base]\nstrategy_id = \"PAIRS-002\"\nuse_hyphens_in_client_order_ids = false\n"
        );
        let config = load(&toml).unwrap();
        assert_eq!(config.base.strategy_id, Some(StrategyId::from("PAIRS-002")));
        assert!(!config.base.use_hyphens_in_client_order_ids);
    }

    /// Nautilus derives the order id tag from the strategy id when the base sets none
    /// (`docs/concepts/strategies.md`, "Order ID tag"). `PAIRS-001`'s tag is `001`, so the default
    /// base sets its id alone and the load adds no tag.
    #[rstest]
    fn without_a_base_no_order_id_tag_is_set() {
        let config = load(TOML).unwrap();
        assert_eq!(config.base.order_id_tag, None);
    }

    #[rstest]
    #[case::of_data(TOML, "[data]", "[data]\nsesion = 1", "sesion")]
    #[case::of_a_series(
        TOML,
        "{ data = \"trades\",",
        "{ data = \"trades\", instrument = 1,",
        "instrument"
    )]
    #[case::of_an_instrument_traded(
        TOML,
        "{ instrument_id = \"ESM24.CME\" }",
        "{ instrument_id = \"ESM24.CME\", size = 1 }",
        "size"
    )]
    #[case::of_trade(TOML, "[trade]", "[trade]\ninstrument_id = 1", "instrument_id")]
    #[case::of_the_strategy(TOML, "spread = 3", "spread = 3\nspred = 1", "spred")]
    #[case::of_the_strategy_named_as_series(TOML, "spread = 3", "spread = 3\ntrades = 1", "trades")]
    #[case::a_table(
        TOML,
        "risk = \"0.5%\"",
        "risk = \"0.5%\"\n\n[indicator]\nperiod = 3",
        "indicator"
    )]
    #[case::of_the_source_bars(
        COMPOSITE_TOML,
        "aggregation_source = ",
        "source = 1, aggregation_source = ",
        "source"
    )]
    fn an_unknown_key_in_any_table_is_refused_naming_it(
        #[case] base: &str,
        #[case] from: &str,
        #[case] to: &str,
        #[case] unknown: &str,
    ) {
        let toml = base.replace(from, to);
        assert_ne!(toml, base);

        let err = shape_error(&toml);

        assert!(err.contains(&format!("`{unknown}`")), "{err}");
    }

    #[rstest]
    #[case::misspelt(TOML.replace("[trade]", "[pairs_trade]"), "`pairs_trade`")]
    #[case::left_out(TOML[..TOML.find("[trade]").unwrap()].to_string(), "`trade`")]
    fn a_missing_standard_table_is_refused_naming_it(#[case] toml: String, #[case] named: &str) {
        let err = shape_error(&toml);

        assert!(err.contains(named), "{err}");
    }

    #[rstest]
    fn a_wrong_count_of_traded_instruments_is_refused() {
        let toml = TOML.replace("{ instrument_id = \"ESM24.CME\" },", "");
        assert_eq!(error_fields(&toml), ["trade.instruments"]);
    }

    #[rstest]
    fn a_trade_size_is_refused_at_its_field_pointing_to_the_risk() {
        let toml = TOML.replace(
            "{ instrument_id = \"ESM24.CME\" }",
            "{ instrument_id = \"ESM24.CME\", trade_size = \"2\" }",
        );
        assert_ne!(toml, TOML);
        let err = load(&toml).unwrap_err();
        let err = err
            .downcast_ref::<toml::de::Error>()
            .unwrap_or_else(|| panic!("not refused as read: {err}"));
        let at = err.span().unwrap();
        assert!(toml[..at.start].ends_with("trade_size = "), "{err}");
        assert!(err.message().contains("risk"), "{err}");
    }

    #[rstest]
    fn the_configs_stake_is_its_risk_alone() {
        let config = load(TOML).unwrap();
        assert_eq!(config.stake(), Stake::new("0.5%".parse().unwrap()));
        assert_eq!(config.stake().multiple(), Decimal::ONE);
    }

    #[rstest]
    fn a_missing_risk_is_refused_naming_it() {
        let toml = TOML.replace("risk = \"0.5%\"", "");
        let err = shape_error(&toml);
        assert!(err.contains("`risk`"), "{err}");
    }

    /// GC.GLBX is a root of the roll calendar, so an order to it would go to no contract.
    #[rstest]
    fn a_futures_root_traded_is_refused_naming_it() {
        let toml = TOML.replace(
            "{ instrument_id = \"ESM24.CME\" }",
            "{ instrument_id = \"GC.GLBX\" }",
        );
        let err = load(&toml).unwrap_err();
        assert_eq!(fields_of(&err), ["trade.instruments[1].instrument_id"]);
        assert!(err.to_string().contains("GC.GLBX"), "{err}");
    }

    #[rstest]
    fn a_futures_root_traded_is_reported_with_the_other_violations() {
        let toml = TOML
            .replace("\"720h\"", "\"0s\"")
            .replace(
                "{ instrument_id = \"ESM24.CME\" }",
                "{ instrument_id = \"GC.GLBX\" }",
            )
            .replace("\"0.5%\"", "\"0%\"");
        assert_eq!(
            error_fields(&toml),
            [
                "data.warmup",
                "trade.instruments[1].instrument_id",
                "trade.risk",
            ]
        );
    }

    #[rstest]
    fn a_bars_series_without_a_bar_spec_is_refused_naming_it() {
        let toml = TOML.replace(
            ", bar_spec = { step = 5, aggregation = \"MINUTE\", price_type = \"BID\" }",
            "",
        );
        let err = shape_error(&toml);
        assert!(
            err.contains("bar_spec") && err.contains("ESM24.CME"),
            "{err}"
        );
    }

    #[rstest]
    fn a_tick_series_with_a_bar_spec_is_refused_naming_it() {
        let toml = TOML.replace(
            "{ data = \"trades\", instrument_id = \"ESH24.CME\" }",
            "{ data = \"trades\", instrument_id = \"ESH24.CME\", bar_spec = { step = 1, aggregation = \"HOUR\", price_type = \"MID\" } }",
        );
        let err = shape_error(&toml);
        assert!(
            err.contains("bar_spec") && err.contains("ESH24.CME"),
            "{err}"
        );
    }

    #[rstest]
    #[case::quotes(Series::quotes(InstrumentId::from("ESH24.CME")), "quotes")]
    #[case::trades(Series::trades(InstrumentId::from("ESH24.CME")), "trades")]
    #[case::depth(Series::depth(InstrumentId::from("ESH24.CME")), "order_book_depths")]
    #[case::deltas(Series::deltas(InstrumentId::from("ESH24.CME")), "order_book_deltas")]
    fn a_tick_series_built_in_code_is_the_one_its_entry_names(
        #[case] built: Series,
        #[case] data: &str,
    ) {
        let entry = format!("data = \"{data}\"\ninstrument_id = \"ESH24.CME\"");

        assert_eq!(built, toml::from_str::<Series>(&entry).unwrap());
    }

    /// `candles` is no data type Nautilus parses. `instruments` is one, of no series read as it is.
    #[rstest]
    #[case::nautilus_parses_none_of_the_name("candles")]
    #[case::not_read_as_it_is("instruments")]
    fn a_series_of_data_not_read_is_refused_naming_it(#[case] data: &str) {
        let toml = TOML.replace("data = \"trades\"", &format!("data = \"{data}\""));

        let err = shape_error(&toml);

        assert!(err.contains(data), "{err}");
    }

    const COMPOSITE_TOML: &str = r#"
        [data]
        series = [
            { data = "bars", instrument_id = "BTC-USDT-SWAP.OKX", bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }, from = { step = 1, aggregation = "MINUTE", aggregation_source = "EXTERNAL" } },
        ]
        warmup = "2h"

        [trade]
        instruments = [
            { instrument_id = "BTC-USDT-SWAP.OKX" },
            { instrument_id = "ETH-USDT-SWAP.OKX" },
        ]
        risk = "0.5%"
    "#;

    #[rstest]
    fn bars_from_source_bars_are_nautilus_composite_bar_type() {
        let config = load(COMPOSITE_TOML).unwrap();
        assert_eq!(
            config.data_requirements().bar_types,
            [BarType::from(
                "BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"
            )]
        );
    }

    #[rstest]
    fn a_session_is_refused_beside_bars_from_source_bars() {
        let toml = COMPOSITE_TOML.replace(
            "warmup = \"2h\"",
            "warmup = \"2h\"\nsession = { time_zone = \"UTC\", open = \"08:00\", close = \"16:00\" }",
        );
        assert_eq!(error_fields(&toml), ["data.session", "data.series[0].from"]);
    }

    #[rstest]
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

    fn gold_futures(instrument_id: &str, bars: &str) -> String {
        format!(
            r#"
            [data]
            series = [{{ data = "bars", instrument_id = "{instrument_id}", {bars} }}]
            warmup = "720h"

            [trade]
            instruments = [
                {{ instrument_id = "GCQ26.GLBX" }},
                {{ instrument_id = "GCZ26.GLBX" }},
            ]
            risk = "0.5%"
            "#
        )
    }

    const HOUR_LAST: &str = r#"bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }"#;
    const HOUR_MID: &str = r#"bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" }"#;

    /// Nautilus builds the continuous bars of GC.GLBX, a root, from its contracts' trades, and
    /// prices their rolls from them. So its LAST bars load. A contract of the root is a plain
    /// instrument, so its bars may be built from quotes.
    #[rstest]
    #[case::on_a_root_built_from_its_trades("GC.GLBX", HOUR_LAST)]
    #[case::on_a_contract_built_from_its_quotes("GCZ26.GLBX", HOUR_MID)]
    fn bars_on_a_root_built_from_its_trades_or_on_a_contract_load(
        #[case] instrument_id: &str,
        #[case] bars: &str,
    ) {
        load(&gold_futures(instrument_id, bars)).unwrap();
    }

    #[rstest]
    #[case::from_its_quotes(HOUR_MID.to_string(), "GC.GLBX-1-HOUR-MID-INTERNAL")]
    #[case::from_source_bars(
        format!(
            "{HOUR_LAST}, from = {{ step = 1, aggregation = \"MINUTE\", aggregation_source = \"EXTERNAL\" }}"
        ),
        "GC.GLBX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"
    )]
    fn bars_on_a_root_not_built_from_its_trades_are_refused_naming_them(
        #[case] bars: String,
        #[case] bar_type: &str,
    ) {
        let err = load(&gold_futures("GC.GLBX", &bars)).unwrap_err();

        assert_eq!(fields_of(&err), ["data.series[0]"]);
        assert!(err.to_string().contains(bar_type), "{err}");
    }

    #[rstest]
    fn the_rules_of_bars_on_a_root_and_of_source_bars_are_reported_with_the_others() {
        let toml = gold_futures("GC.GLBX", HOUR_MID)
            .replace(
                "price_type = \"MID\" } }]",
                "price_type = \"MID\" } }, { data = \"bars\", instrument_id = \"GCZ26.GLBX\", \
                 bar_spec = { step = 1, aggregation = \"HOUR\", price_type = \"LAST\" }, \
                 from = { step = 1, aggregation = \"MINUTE\", aggregation_source = \"INTERNAL\" } }]",
            )
            .replace("\"720h\"", "\"0s\"")
            .replace("\"0.5%\"", "\"0%\"");
        assert_eq!(
            error_fields(&toml),
            [
                "data.series[0]",
                "data.series[1]",
                "data.warmup",
                "trade.risk",
            ]
        );
    }

    /// A session needs a bar's span, and Nautilus builds bars from quotes or trades alone.
    #[rstest]
    #[case::not_aggregated_on_time(
        "step = 5, aggregation = \"MINUTE\"",
        "step = 100, aggregation = \"TICK\"",
        "data.series[1].bar_spec",
        "100-TICK-BID"
    )]
    #[case::of_ticks_priced_mark(
        "price_type = \"MID\"",
        "price_type = \"MARK\"",
        "data.series[0].bar_spec",
        "1-HOUR-MARK"
    )]
    fn bars_not_aggregated_on_time_or_priced_mark_are_refused_naming_their_spec(
        #[case] from: &str,
        #[case] to: &str,
        #[case] field: &str,
        #[case] spec: &str,
    ) {
        let toml = TOML.replace(from, to);
        assert_ne!(toml, TOML);

        let err = load(&toml).unwrap_err();

        assert_eq!(fields_of(&err), [field]);
        assert!(err.to_string().contains(spec), "{err}");
    }

    #[rstest]
    fn bars_of_ticks_priced_last_load() {
        let toml = TOML.replace("price_type = \"MID\"", "price_type = \"LAST\"");
        let config = load(&toml).unwrap();
        assert_eq!(
            config.data.bar_types().next(),
            Some(BarType::from("ESH24.CME-1-HOUR-LAST-INTERNAL"))
        );
    }

    #[rstest]
    fn bars_on_a_second_venue_are_refused() {
        let toml = TOML.replace(
            "{ data = \"bars\", instrument_id = \"ESM24.CME\"",
            "{ data = \"bars\", instrument_id = \"ESM24.EUREX\"",
        );
        assert_eq!(error_fields(&toml), ["data.series[1].instrument_id"]);
    }

    #[rstest]
    fn ticks_may_be_of_another_venue() {
        let toml = TOML.replace(
            "{ data = \"trades\", instrument_id = \"ESH24.CME\" }",
            "{ data = \"trades\", instrument_id = \"FESX.EUREX\" }",
        );
        load(&toml).unwrap();
    }

    #[rstest]
    fn no_series_is_refused() {
        let start = TOML.find("series = [").unwrap();
        let end = TOML.find("warmup").unwrap();
        let toml = format!("{}series = []\n{}", &TOML[..start], &TOML[end..]);
        assert_eq!(error_fields(&toml), ["data.series"]);
    }

    #[rstest]
    fn a_series_listed_again_is_refused_under_each_later_listing_with_the_other_violations() {
        let trades = r#"{ data = "trades", instrument_id = "ESH24.CME" },"#;
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
            assert!(value.contains("TradeTick of ESH24.CME"), "{value}");
        }
    }

    fn with_custom(entries: &[&str]) -> String {
        let trades = r#"{ data = "trades", instrument_id = "ESH24.CME" },"#;
        let toml = TOML.replace(
            trades,
            &format!("{trades}\n            {}", entries.join(",\n            ")),
        );
        assert_ne!(toml, TOML);
        toml
    }

    const US10Y: &str = r#"{ data = "Custom:LseRow", identifier = "US10Y", metadata = { source = "bond_yields", symbol = "US10Y" } }"#;

    fn bond_yields(symbol: &str, identifier: &str) -> DataType {
        let metadata = Params::new(
            [
                ("source".to_string(), serde_json::json!("bond_yields")),
                ("symbol".to_string(), serde_json::json!(symbol)),
            ]
            .into(),
        );
        DataType::new("LseRow", Some(metadata), Some(identifier.to_string()))
    }

    fn custom_of(config: &StandardConfig<Pairs>) -> Vec<DataType> {
        config
            .data_requirements()
            .custom
            .iter()
            .map(|series| series.data_type().clone())
            .collect()
    }

    fn shape_message(toml: &str) -> String {
        let err = load(toml).unwrap_err();
        err.downcast_ref::<toml::de::Error>()
            .unwrap_or_else(|| panic!("not serde's error: {err}"))
            .message()
            .to_string()
    }

    fn invalid_value(toml: &str, field: &str) -> String {
        let err = load(toml).unwrap_err();
        let [ConfigError::InvalidValue { field: at, reason }] = &errors_of(&err)[..] else {
            panic!("not one invalid value: {err:?}");
        };
        assert_eq!(at, field, "{reason}");
        reason.clone()
    }

    #[rstest]
    fn a_custom_series_is_keyed_by_the_identifier_its_entry_names() {
        let config =
            load(&with_custom(&[&US10Y.replace(
                r#"identifier = "US10Y""#,
                r#"identifier = "TREASURY-10Y""#,
            )]))
            .unwrap();

        assert_eq!(custom_of(&config), [bond_yields("US10Y", "TREASURY-10Y")]);
    }

    /// Metadata is optional, and empty metadata is none. Nautilus' data type has one topic with
    /// either, and the topic keys its points in the cache.
    #[rstest]
    fn a_custom_series_with_empty_metadata_is_the_one_with_none() {
        let series = |metadata: &str| {
            toml::from_str::<Series>(&format!(
                "data = \"Custom:Imbalance\"\nidentifier = \"GCZ26.GLBX\"\n{metadata}"
            ))
            .unwrap()
        };

        let (empty, none) = (series("metadata = {}"), series(""));

        let Series::Custom(custom) = &none else {
            panic!("not a custom series: {none:?}");
        };
        assert_eq!(
            custom.data_type(),
            &DataType::new("Imbalance", None, Some("GCZ26.GLBX".to_string()))
        );
        assert_eq!(custom.data_type().metadata(), None);
        assert_eq!(empty, none);
        let Series::Custom(empty) = empty else {
            panic!("not a custom series");
        };
        assert_eq!(empty.data_type().metadata(), None);
    }

    #[rstest]
    fn lse_s_rows_naming_no_metadata_are_refused_by_lse_s_answer_under_their_entry() {
        let toml = with_custom(&[r#"{ data = "Custom:LseRow", identifier = "US10Y" }"#]);

        let reason = invalid_value(&toml, "data.series[3]");

        assert!(reason.contains("source"), "{reason}");
    }

    #[rstest]
    fn a_custom_series_naming_no_identifier_is_refused_naming_the_key() {
        let toml = with_custom(&[&US10Y.replace(r#"identifier = "US10Y", "#, "")]);

        let err = shape_message(&toml);

        assert!(err.contains("identifier"), "{err}");
    }

    #[rstest]
    fn a_custom_series_naming_an_instrument_beside_its_identifier_is_refused_naming_both() {
        let toml = with_custom(&[&US10Y.replace(
            r#"identifier = "US10Y","#,
            r#"identifier = "US10Y", instrument_id = "ESH24.CME","#,
        )]);

        let err = shape_message(&toml);

        assert!(
            err.contains("instrument_id") && err.contains("identifier"),
            "{err}"
        );
    }

    #[rstest]
    fn a_custom_series_naming_an_instrument_in_place_of_an_identifier_is_refused_pointing_at_it() {
        let toml = with_custom(&[r#"{ data = "Custom:Imbalance", instrument_id = "ESM24.CME" }"#]);

        let err = shape_message(&toml);

        assert!(
            err.contains("instrument_id") && err.contains(r#"identifier = "ESM24.CME""#),
            "{err}"
        );
    }

    #[rstest]
    #[case::trades_naming_an_identifier(
        r#"{ data = "trades", instrument_id = "ESH24.CME", identifier = "ES" }"#,
        "identifier"
    )]
    #[case::quotes_naming_metadata(
        r#"{ data = "quotes", instrument_id = "ESH24.CME", metadata = { a = 1 } }"#,
        "metadata"
    )]
    #[case::bars_naming_an_identifier(
        r#"{ data = "bars", instrument_id = "ESH24.CME", bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" }, identifier = "ES" }"#,
        "identifier"
    )]
    fn a_series_of_ticks_or_bars_naming_an_identifier_or_metadata_is_refused_naming_it(
        #[case] series: &str,
        #[case] key: &str,
    ) {
        let err = shape_message(&with_custom(&[series]));

        assert!(err.contains(key), "{err}");
    }

    /// The catalog stores a series under the folder for its type, then the folder for its
    /// identifier, made URI-safe by removing `/`. An identifier left empty so keys the type's
    /// folder alone, as none does.
    #[rstest]
    #[case::empty("")]
    #[case::blank("  ")]
    #[case::a_slash("/")]
    #[case::slashes("//")]
    fn a_custom_series_of_an_identifier_naming_no_catalog_folder_is_refused_naming_it(
        #[case] identifier: &str,
    ) {
        let toml = with_custom(&[&US10Y.replace(
            r#"identifier = "US10Y""#,
            &format!(r#"identifier = "{identifier}""#),
        )]);

        let err = shape_message(&toml);

        assert!(err.contains(&format!("identifier {identifier:?}")), "{err}");
    }

    /// Nautilus writes the series under `US/10Y`, where a backslash is a folder separator, and
    /// reads it back under `US\10Y`. A fill would find nothing missing, and a backtest would read
    /// nothing.
    #[rstest]
    // These are TOML basic strings, so a backslash is written escaped
    #[case::within(r"US\\10Y", r"US\10Y")]
    #[case::leading(r"\\US10Y", r"\US10Y")]
    #[case::alone(r"\\", r"\")]
    fn a_custom_series_of_an_identifier_holding_a_backslash_is_refused_naming_it(
        #[case] written: &str,
        #[case] identifier: &str,
    ) {
        let toml = with_custom(&[&US10Y.replace(
            r#"identifier = "US10Y""#,
            &format!(r#"identifier = "{written}""#),
        )]);

        let err = shape_message(&toml);

        assert!(
            err.contains("backslash") && err.contains(&format!("{identifier:?}")),
            "{err}"
        );
    }

    #[rstest]
    fn a_custom_series_naming_an_identifier_in_its_metadata_is_refused_naming_it() {
        let toml = with_custom(&[&US10Y.replace(
            r#"symbol = "US10Y""#,
            r#"symbol = "US10Y", identifier = "X""#,
        )]);

        let err = shape_message(&toml);

        assert!(err.contains("`identifier`"), "{err}");
    }

    #[nautilus_serialization::arrow_custom_data]
    #[nautilus_model::custom_data]
    pub struct Signal {
        pub value: f64,
        pub ts_event: nautilus_core::UnixNanos,
        pub ts_init: nautilus_core::UnixNanos,
    }

    #[rstest]
    fn a_custom_series_of_a_registered_type_no_row_routes_loads_as_the_catalog_s() {
        nautilus_serialization::ensure_custom_data_registered::<Signal>();
        let toml = with_custom(&[
            r#"{ data = "Custom:Signal", identifier = "GOLD-SIGNAL" }"#,
            r#"{ data = "Custom:Signal", identifier = "GOLD-SIGNAL-FAST", metadata = { window = "5m" } }"#,
        ]);

        let config = load(&toml).unwrap();

        let window = Params::new([("window".to_string(), serde_json::json!("5m"))].into());
        assert_eq!(
            custom_of(&config),
            [
                DataType::new("Signal", None, Some("GOLD-SIGNAL".to_string())),
                DataType::new("Signal", Some(window), Some("GOLD-SIGNAL-FAST".to_string())),
            ]
        );
    }

    #[rstest]
    fn a_custom_series_of_a_type_nautilus_has_no_registration_of_is_refused_under_its_entry_naming_it()
     {
        let toml = with_custom(&[US10Y, &US10Y.replace("LseRow", "LseRows")]);

        let reason = invalid_value(&toml, "data.series[4]");

        assert!(
            reason.contains("LseRows") && reason.contains("registered"),
            "{reason}"
        );
    }

    /// Nautilus' Databento adapter is compiled in, so helix registers its imbalances and
    /// statistics. No row routes them, so each is the catalog's.
    #[rstest]
    fn databento_s_custom_series_load_as_the_catalog_s() {
        let toml = with_custom(&[
            r#"{ data = "Custom:DatabentoImbalance", identifier = "GC.GLBX" }"#,
            r#"{ data = "Custom:DatabentoStatistics", identifier = "GC.GLBX" }"#,
        ]);

        let config = load(&toml).unwrap();

        assert_eq!(
            custom_of(&config),
            [
                DataType::new("DatabentoImbalance", None, Some("GC.GLBX".to_string())),
                DataType::new("DatabentoStatistics", None, Some("GC.GLBX".to_string())),
            ]
        );
    }

    #[rstest]
    fn custom_series_of_one_type_under_identifiers_of_their_own_load() {
        let us02y = US10Y.replace("US10Y", "US02Y");

        let config = load(&with_custom(&[US10Y, &us02y])).unwrap();

        assert_eq!(
            custom_of(&config),
            [bond_yields("US10Y", "US10Y"), bond_yields("US02Y", "US02Y")]
        );
    }

    #[rstest]
    fn a_custom_series_of_a_type_under_an_identifier_listed_again_is_refused_under_its_later_listing_naming_it()
     {
        let us02y = US10Y.replace(r#"symbol = "US10Y""#, r#"symbol = "US02Y""#);

        let err = load(&with_custom(&[US10Y, &us02y])).unwrap_err();

        assert_eq!(fields_of(&err), ["data.series[4]"]);
        let [
            ConfigError::Duplicate {
                value: Some(value), ..
            },
        ] = &errors_of(&err)[..]
        else {
            panic!("not a duplicate naming the data: {err:?}");
        };
        assert!(
            value.contains("LseRow") && value.contains("US10Y"),
            "{value}"
        );
    }

    /// Nautilus removes `/` and makes `^` `_`.
    #[rstest]
    #[case::a_slash_removed("US10Y", "US/10Y")]
    #[case::a_caret_made_an_underscore("US_10Y", "US^10Y")]
    fn a_custom_series_of_an_identifier_stored_under_the_folder_of_one_listed_already_is_refused_under_its_later_listing(
        #[case] listed: &str,
        #[case] again: &str,
    ) {
        let keyed = |identifier: &str| {
            US10Y.replace(
                r#"identifier = "US10Y""#,
                &format!(r#"identifier = "{identifier}""#),
            )
        };
        let toml = with_custom(&[&keyed(listed), &keyed(again)]);

        let err = load(&toml).unwrap_err();

        assert_eq!(fields_of(&err), ["data.series[4]"]);
        let [
            ConfigError::Duplicate {
                value: Some(value), ..
            },
        ] = &errors_of(&err)[..]
        else {
            panic!("not a duplicate naming the folder: {err:?}");
        };
        assert!(
            value.contains(&format!("folder {listed} ")) && value.contains("data.series[3]"),
            "{value}"
        );
    }

    const GC_SIGNALS: &str =
        r#"{ data = "Custom:Signal", identifier = "GC.GLBX", metadata = { window = "5m" } }"#;
    const GCZ26_SIGNALS: &str = r#"{ data = "Custom:Signal", identifier = "GCZ26.GLBX" }"#;

    #[rstest]
    #[case::the_root_s_first([GC_SIGNALS, GCZ26_SIGNALS])]
    #[case::the_contract_s_first([GCZ26_SIGNALS, GC_SIGNALS])]
    fn a_root_s_custom_series_and_its_contract_s_of_other_metadata_are_refused_under_the_later_naming_both(
        #[case] entries: [&str; 2],
    ) {
        nautilus_serialization::ensure_custom_data_registered::<Signal>();

        let err = load(&with_custom(&entries)).unwrap_err();

        assert_eq!(fields_of(&err), ["data.series[4]"]);
        let [
            ConfigError::Duplicate {
                value: Some(value), ..
            },
        ] = &errors_of(&err)[..]
        else {
            panic!("not a duplicate naming the series: {err:?}");
        };
        assert!(
            value.contains("data.series[3]")
                && value.contains("identifier=GC.GLBX")
                && value.contains("identifier=GCZ26.GLBX"),
            "{value}"
        );
    }

    /// GCV26 is no contract GC rolls through, so its folder is its own.
    #[rstest]
    fn a_contract_s_custom_series_beside_its_root_s_loads_if_it_is_the_root_s_on_it_or_one_it_never_rolls_through()
     {
        nautilus_serialization::ensure_custom_data_registered::<Signal>();
        let root =
            r#"{ data = "Custom:Signal", identifier = "GC.GLBX", metadata = { window = "5m" } }"#;
        let on_it = r#"{ data = "Custom:Signal", identifier = "GCZ26.GLBX", metadata = { window = "5m" } }"#;
        let unrolled = r#"{ data = "Custom:Signal", identifier = "GCV26.GLBX" }"#;

        load(&with_custom(&[root, on_it, unrolled])).unwrap();
    }

    #[rstest]
    fn a_custom_series_listed_again_is_refused_once_under_its_later_listing() {
        let err = load(&with_custom(&[US10Y, US10Y])).unwrap_err();

        assert_eq!(fields_of(&err), ["data.series[4]"]);
    }

    #[rstest]
    fn a_non_positive_warmup_is_refused() {
        let toml = TOML.replace("\"720h\"", "\"0s\"");
        assert_eq!(error_fields(&toml), ["data.warmup"]);
    }

    #[rstest]
    fn a_session_not_opening_before_its_close_is_refused() {
        let toml = TOML.replace("open = \"08:30\"", "open = \"15:00\"");
        assert_eq!(error_fields(&toml), ["data.session"]);
    }

    #[rstest]
    fn a_strategy_taking_no_model_refuses_a_model_table() {
        let toml = format!("{TOML}\n[model]\nretrain = \"24h\"\n");
        let err = shape_error(&toml);
        assert!(err.contains("`model`"), "{err}");
    }

    #[rstest]
    fn a_strategy_taking_no_model_needs_no_strategy_id() {
        let config = load(&format!("{TOML}\n[base]\norder_id_tag = \"001\"\n")).unwrap();
        assert_eq!(config.base.strategy_id, None);
    }

    #[rstest]
    fn a_traded_instrument_no_series_reads_loads_its_quotes_into_the_requirements() {
        let toml = TOML.replace(
            "{ instrument_id = \"ESM24.CME\" }",
            "{ instrument_id = \"ESU24.CME\" }",
        );
        let config = load(&toml).unwrap();
        assert_eq!(
            config.data_requirements().ticks,
            [
                TickSeries {
                    data: NautilusDataType::TradeTick,
                    instrument_id: InstrumentId::from("ESH24.CME"),
                },
                TickSeries {
                    data: NautilusDataType::QuoteTick,
                    instrument_id: InstrumentId::from("ESU24.CME"),
                },
            ]
        );
        assert_eq!(config.data.series.len(), 3);
    }

    /// Depth and book deltas give Nautilus no price for a market order, and deltas move no L1 book.
    #[rstest]
    #[case::depth("order_book_depths")]
    #[case::deltas("order_book_deltas")]
    fn a_traded_instrument_read_only_through_depth_or_deltas_loads_its_quotes(#[case] data: &str) {
        let toml = TOML.replace(
            r#"{ data = "bars", instrument_id = "ESM24.CME", bar_spec = { step = 5, aggregation = "MINUTE", price_type = "BID" } },"#,
            &format!(r#"{{ data = "{data}", instrument_id = "ESM24.CME" }},"#),
        );

        let config = load(&toml).unwrap();

        let es = |data: &str, instrument_id: &str| TickSeries {
            data: NautilusDataType::from_str(data).unwrap(),
            instrument_id: InstrumentId::from(instrument_id),
        };
        assert_eq!(
            config.data_requirements().ticks,
            [
                es(data, "ESM24.CME"),
                es("trades", "ESH24.CME"),
                es("quotes", "ESM24.CME"),
            ]
        );
    }

    /// A custom series under a traded instrument's id gives Nautilus no price and brings no
    /// instrument.
    #[rstest]
    fn a_traded_instrument_read_only_through_a_custom_series_under_its_id_loads_its_quotes() {
        nautilus_serialization::ensure_custom_data_registered::<Signal>();
        let toml = TOML.replace(
            r#"{ data = "bars", instrument_id = "ESM24.CME", bar_spec = { step = 5, aggregation = "MINUTE", price_type = "BID" } },"#,
            r#"{ data = "Custom:Signal", identifier = "ESM24.CME" },"#,
        );

        let requirements = load(&toml).unwrap().data_requirements();

        let es = |data, instrument_id: &str| TickSeries {
            data,
            instrument_id: InstrumentId::from(instrument_id),
        };
        assert_eq!(
            requirements.ticks,
            [
                es(NautilusDataType::TradeTick, "ESH24.CME"),
                es(NautilusDataType::QuoteTick, "ESM24.CME"),
            ]
        );
        assert_eq!(
            requirements.custom,
            [CustomSeries::new("Signal", "ESM24.CME".to_string(), None).unwrap()]
        );
    }

    fn converting(pairs: &str) -> String {
        let toml = TOML.replace(
            "risk = \"0.5%\"",
            &format!("risk = \"0.5%\"\nconversions = {pairs}"),
        );
        assert_ne!(toml, TOML);
        toml
    }

    #[rstest]
    fn the_pairs_converted_through_load_their_quotes_into_the_requirements() {
        let config = load(&converting(r#"["EURUSD.ICMARKETS", "USDJPY.ICMARKETS"]"#)).unwrap();

        let quotes = |id: &str| TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: InstrumentId::from(id),
        };
        assert_eq!(
            config.data_requirements().ticks,
            [
                TickSeries {
                    data: NautilusDataType::TradeTick,
                    instrument_id: InstrumentId::from("ESH24.CME"),
                },
                quotes("EURUSD.ICMARKETS"),
                quotes("USDJPY.ICMARKETS"),
            ]
        );
        assert_eq!(config.data.series.len(), 3);
    }

    #[rstest]
    fn a_pair_whose_quotes_a_series_reads_is_read_once() {
        let toml = converting(r#"["EURUSD.ICMARKETS"]"#).replace(
            r#"{ data = "trades", instrument_id = "ESH24.CME" }"#,
            r#"{ data = "quotes", instrument_id = "EURUSD.ICMARKETS" }"#,
        );

        assert_eq!(
            load(&toml).unwrap().data_requirements().ticks,
            [TickSeries {
                data: NautilusDataType::QuoteTick,
                instrument_id: InstrumentId::from("EURUSD.ICMARKETS"),
            }]
        );
    }

    /// IB's history serves an FX pair as 1-minute BID and ASK bars, not quotes. Nautilus' rates
    /// read those bars when a pair has no quotes.
    #[rstest]
    fn an_ib_pair_converted_through_loads_its_bid_and_ask_minute_bars_into_the_requirements() {
        let config = load(&converting(r#"["EUR/USD.IDEALPRO"]"#)).unwrap();

        let requirements = config.data_requirements();
        assert_eq!(
            requirements.bar_types,
            [
                BarType::from("ESH24.CME-1-HOUR-MID-INTERNAL"),
                BarType::from("ESM24.CME-5-MINUTE-BID-INTERNAL"),
                BarType::from("EUR/USD.IDEALPRO-1-MINUTE-BID-EXTERNAL"),
                BarType::from("EUR/USD.IDEALPRO-1-MINUTE-ASK-EXTERNAL"),
            ]
        );
        assert_eq!(
            requirements.ticks,
            [TickSeries {
                data: NautilusDataType::TradeTick,
                instrument_id: InstrumentId::from("ESH24.CME"),
            }]
        );
        assert_eq!(config.data.series.len(), 3);
    }

    /// OKX's history serves no pair's BID and ASK bars, because its candles are of trades, and
    /// serves no quotes. So no rate of an OKX pair could be read.
    #[rstest]
    fn a_pair_whose_venue_serves_neither_its_bars_nor_its_quotes_is_refused_naming_both() {
        let err = load(&converting(r#"["USDC-USDT.OKX"]"#)).unwrap_err();

        assert_eq!(fields_of(&err), ["trade.conversions[0]"]);
        assert!(
            ["USDC-USDT.OKX", "OKX's history"]
                .iter()
                .all(|named| err.to_string().contains(named)),
            "{err}"
        );
    }

    #[rstest]
    fn pairs_on_more_than_one_venue_are_refused_naming_the_venues() {
        let err = load(&converting(r#"["EURUSD.ICMARKETS", "EUR/USD.IDEALPRO"]"#)).unwrap_err();

        assert_eq!(fields_of(&err), ["trade.conversions"]);
        assert!(
            ["ICMARKETS", "IDEALPRO"]
                .iter()
                .all(|venue| err.to_string().contains(venue)),
            "{err}"
        );
    }

    fn trading_gold_converting(pairs: &str) -> String {
        let toml = converting(pairs).replace(
            "{ instrument_id = \"ESM24.CME\" }",
            "{ instrument_id = \"XAUUSD.ICMARKETS\" }",
        );
        assert!(toml.contains("XAUUSD.ICMARKETS"));
        toml
    }

    /// An IC Markets account has a base currency. Nautilus converts its margins, commissions and
    /// P&L into it only through rates on the traded instrument's venue.
    #[rstest]
    fn a_pair_off_the_venue_of_an_instrument_whose_account_has_a_base_currency_is_refused() {
        let err = load(&trading_gold_converting(r#"["EUR/USD.IDEALPRO"]"#)).unwrap_err();

        assert_eq!(fields_of(&err), ["trade.conversions[0]"]);
        assert!(
            ["EUR/USD.IDEALPRO", "XAUUSD.ICMARKETS"]
                .iter()
                .all(|named| err.to_string().contains(named)),
            "{err}"
        );
    }

    /// IB's account, as helix configures it, has no base currency, so only the sizer converts, at
    /// the pairs' venue.
    #[rstest]
    #[case::of_ib_for_futures_on_cme(converting(r#"["EUR/USD.IDEALPRO"]"#), "EUR/USD.IDEALPRO")]
    #[case::of_ic_for_futures_on_cme(converting(r#"["EURUSD.ICMARKETS"]"#), "EURUSD.ICMARKETS")]
    #[case::of_ic_for_ic_s_gold(
        trading_gold_converting(r#"["EURUSD.ICMARKETS"]"#),
        "EURUSD.ICMARKETS"
    )]
    fn a_pair_is_taken_of_any_venue_for_an_account_of_no_base_currency_and_of_its_own_for_one_of_a_base_currency(
        #[case] toml: String,
        #[case] pair: &str,
    ) {
        let config = load(&toml).unwrap();

        assert_eq!(config.trade.conversions.pairs(), [InstrumentId::from(pair)]);
    }

    #[rstest]
    fn the_strategys_own_rules_are_applied() {
        let toml = TOML.replace("spread = 3", "spread = 0");
        assert_eq!(error_fields(&toml), ["spread"]);
    }

    #[rstest]
    fn every_violation_is_reported_at_once() {
        let toml = TOML
            .replace("\"720h\"", "\"0s\"")
            .replace("price_type = \"MID\"", "price_type = \"MARK\"")
            .replace("\"0.5%\"", "\"0%\"")
            .replace("{ instrument_id = \"ESH24.CME\" },", "")
            .replace("spread = 3", "spread = 0");
        assert_eq!(
            error_fields(&toml),
            [
                "data.series[0].bar_spec",
                "data.warmup",
                "trade.instruments",
                "trade.risk",
                "spread",
            ]
        );
    }

    mod sizing_itself {
        use rstest::rstest;

        use super::*;

        #[derive(Debug, Clone, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Rebalancer {}

        impl Part for Rebalancer {
            const INSTRUMENTS: usize = 1;
            const SIZES_BY_RISK: bool = false;

            fn default_base() -> StrategyConfig {
                StrategyConfig {
                    strategy_id: Some(StrategyId::from("REBALANCER-001")),
                    ..Default::default()
                }
            }

            fn reads(config: &StandardConfig<Self>) -> ConfigResult<Vec<Series>> {
                Ok(config.data.series.clone())
            }
        }

        const TOML: &str = r#"
            [data]
            series = [
                { data = "bars", instrument_id = "SPY.XNAS", bar_spec = { step = 1, aggregation = "DAY", price_type = "LAST" } },
            ]
            warmup = "720h"

            [trade]
            instruments = [{ instrument_id = "SPY.XNAS" }]
        "#;

        fn load(toml: &str) -> anyhow::Result<StandardConfig<Rebalancer>> {
            StandardConfig::load(toml)
        }

        #[rstest]
        fn a_strategy_not_sizing_by_risk_loads_without_a_risk() {
            let config = load(TOML).unwrap();
            assert_eq!(
                config.trade.instrument_ids(),
                [InstrumentId::from("SPY.XNAS")]
            );
            assert_eq!(config.trade.risk, None);
            assert_eq!(config.trade.conversions.pairs(), []);
        }

        #[rstest]
        #[case::a_risk("risk = \"0.5%\"", "risk")]
        #[case::conversions("conversions = [\"EUR/USD.IDEALPRO\"]", "conversions")]
        fn a_strategy_not_sizing_by_risk_refuses_a_risk_or_conversions_as_an_unknown_key(
            #[case] line: &str,
            #[case] key: &str,
        ) {
            let toml = TOML.replace(
                r#"instruments = [{ instrument_id = "SPY.XNAS" }]"#,
                &format!("instruments = [{{ instrument_id = \"SPY.XNAS\" }}]\n{line}"),
            );
            assert_ne!(toml, TOML);

            let err = shape_of(&load(&toml).unwrap_err());

            assert!(
                err.contains("unknown") && err.contains(&format!("`{key}`")),
                "{err}"
            );
        }

        #[rstest]
        #[should_panic(expected = "does not size its trades by risk")]
        fn a_strategy_not_sizing_by_risk_has_no_stake() {
            let _ = load(TOML).unwrap().stake();
        }
    }

    mod model {
        use nautilus_indicators::average::MovingAverageType;
        use rstest::rstest;

        use super::*;
        use crate::{
            model::{
                decide::DecisionRule,
                learner::LearnerConfig,
                targets::{BracketSpec, Distance, Target},
            },
            strategy::AtrArgs,
        };

        #[derive(Debug, Clone, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Modeled {}

        impl Part for Modeled {
            const INSTRUMENTS: usize = 1;
            const TAKES_MODEL: bool = true;

            fn default_base() -> StrategyConfig {
                StrategyConfig {
                    strategy_id: Some(StrategyId::from("MODELED-001")),
                    ..Default::default()
                }
            }

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
            instruments = [{ instrument_id = "XAUUSD.ICMARKETS" }]
            risk = "0.5%"

            [model]
            dataset = { commission = { rate = "0.00003" }, balance = "10000000 USD" }
            decision = { every = "1h" }
            target = { limit = "30m", atr = { period = 1 } }
            retrain = "24h"
            window = "48h"
            calibration = "12h"
            margins = { long = "0.5R", short = "0.5R" }
            min_rows = 30
            learner = { kind = "baseline" }
        "#;

        fn load(toml: &str) -> anyhow::Result<StandardConfig<Modeled>> {
            StandardConfig::load(toml)
        }

        fn error_fields(toml: &str) -> Vec<String> {
            fields_of(&load(toml).unwrap_err())
        }

        fn shape_error(toml: &str) -> String {
            shape_of(&load(toml).unwrap_err())
        }

        /// Returns `TOML` with its ATR set to `atr`.
        fn with_atr(atr: &str) -> String {
            TOML.replace(r#"warmup = "1h""#, r#"warmup = "3h""#).replace(
                r#"target = { limit = "30m", atr = { period = 1 } }"#,
                &format!(
                    r#"target = {{ bracket = {{ take_profit = {{ atr = 2 }}, stop = {{ atr = 1.5 }}{atr} }}, limit = "45m" }}"#
                ),
            )
        }

        /// An OKX pair, no rate of which can be read, is refused once, as a strategy trading on no
        /// model refuses it. It is also off the venue of the gold traded, whose IC account has a
        /// base currency, so it is refused once for that too.
        #[rstest]
        fn a_pair_whose_venue_serves_neither_its_bars_nor_its_quotes_is_refused_once() {
            let toml = TOML.replace(
                r#"risk = "0.5%""#,
                "risk = \"0.5%\"\nconversions = [\"USDC-USDT.OKX\"]",
            );

            let err = load(&toml).unwrap_err();

            assert_eq!(
                fields_of(&err),
                ["trade.conversions[0]", "trade.conversions[0]"]
            );
            let [no_rate, off_venue] = &errors_of(&err)[..] else {
                panic!("not two refusals: {err:?}");
            };
            let (no_rate, off_venue) = (no_rate.to_string(), off_venue.to_string());
            assert!(
                no_rate.contains("USDC-USDT.OKX") && no_rate.contains("OKX's history"),
                "{no_rate}"
            );
            assert!(
                off_venue.contains("USDC-USDT.OKX") && off_venue.contains("XAUUSD.ICMARKETS"),
                "{off_venue}"
            );
        }

        #[rstest]
        fn loads_the_standard_tables_and_the_model() {
            let config = load(TOML).unwrap();
            assert_eq!(
                config.data_requirements().bar_types,
                [BarType::from("XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL")]
            );
            let model = config.model();
            assert_eq!(model.target.spec.limit, SignedDuration::from_mins(30));
            assert_eq!(model.target.spec.bracket, None);
            assert_eq!(
                model.decision,
                DecisionRule::Every(SignedDuration::from_hours(1))
            );
            assert_eq!(model.learner, LearnerConfig::Baseline {});
        }

        #[rstest]
        fn a_strategy_taking_a_model_requires_its_table() {
            let start = TOML.find("[model]").unwrap();
            let err = shape_error(&TOML[..start]);
            assert!(err.contains("`model`"), "{err}");
        }

        #[rstest]
        fn the_target_trades_the_instrument_risk_and_conversions_of_the_trade_table() {
            let toml = TOML.replace(
                r#"risk = "0.5%""#,
                "risk = \"0.5%\"\nconversions = [\"EURUSD.ICMARKETS\"]",
            );
            assert_ne!(toml, TOML);

            let target = load(&toml).unwrap().model().target;

            assert_eq!(target.instrument_id, InstrumentId::from("XAUUSD.ICMARKETS"));
            assert_eq!(target.risk, "0.5%".parse().unwrap());
            assert_eq!(
                target.conversions,
                Conversions::new(vec![InstrumentId::from("EURUSD.ICMARKETS")])
            );
        }

        #[rstest]
        #[case::an_instrument(r#"instrument_id = "XAUUSD.ICMARKETS""#, "instrument_id")]
        #[case::a_quantity(r#"quantity = "1""#, "quantity")]
        fn a_target_naming_an_instrument_or_a_quantity_is_refused_at_load(
            #[case] key: &str,
            #[case] name: &str,
        ) {
            let toml = TOML.replace("target = { limit", &format!("target = {{ {key}, limit"));

            let err = shape_error(&toml);

            assert!(err.contains(&format!("`{name}`")), "{err}");
        }

        #[rstest]
        fn a_second_traded_instrument_is_refused() {
            let toml = TOML.replace(
                r#""XAUUSD.ICMARKETS" }]"#,
                r#""XAUUSD.ICMARKETS" }, { instrument_id = "EURUSD.ICMARKETS" }]"#,
            );
            assert_eq!(error_fields(&toml), ["trade.instruments"]);
        }

        /// Trading OKX on hourly bars built from its 1-minute candles, where fill-catalog fetches
        /// those candles alone, with no quotes.
        #[rstest]
        fn an_okx_strategy_on_composite_bars_requires_its_candles_alone() {
            let toml = TOML
                .replace(
                    r#"{ data = "bars", instrument_id = "XAUUSD.ICMARKETS", bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" } },"#,
                    r#"{ data = "bars", instrument_id = "BTC-USDT-SWAP.OKX", bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }, from = { step = 1, aggregation = "MINUTE", aggregation_source = "EXTERNAL" } },"#,
                )
                .replace(
                    r#"instruments = [{ instrument_id = "XAUUSD.ICMARKETS" }]"#,
                    r#"instruments = [{ instrument_id = "BTC-USDT-SWAP.OKX" }]"#,
                );

            let requirements = load(&toml).unwrap().data_requirements();

            assert_eq!(
                requirements.catalog_keys(),
                [(
                    NautilusDataType::Bar,
                    "BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL".to_string()
                )]
            );
        }

        #[rstest]
        fn traded_quotes_the_series_read_are_required_once() {
            let toml = TOML.replace(
                r#"{ data = "bars", instrument_id = "XAUUSD.ICMARKETS", bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" } },"#,
                r#"{ data = "quotes", instrument_id = "XAUUSD.ICMARKETS" },"#,
            );

            let requirements = load(&toml).unwrap().data_requirements();

            assert_eq!(
                requirements.ticks,
                [TickSeries {
                    data: NautilusDataType::QuoteTick,
                    instrument_id: InstrumentId::from("XAUUSD.ICMARKETS"),
                }]
            );
        }

        /// The shared trade rules refuse the root, under its `[trade]` field. The model does not
        /// refuse it again.
        #[rstest]
        fn a_futures_root_traded_is_refused_at_load_by_the_trade_rules() {
            let toml = TOML.replace(
                r#"instruments = [{ instrument_id = "XAUUSD.ICMARKETS""#,
                r#"instruments = [{ instrument_id = "GC.GLBX""#,
            );
            let err = load(&toml).unwrap_err();
            assert_eq!(fields_of(&err), ["trade.instruments[0].instrument_id"]);
            assert!(err.to_string().contains("GC.GLBX"), "{err}");
        }

        #[rstest]
        fn a_base_without_a_strategy_id_is_refused() {
            let toml = format!("{TOML}\n[base]\norder_id_tag = \"001\"\n");
            assert_eq!(error_fields(&toml), ["base.strategy_id"]);
        }

        #[rstest]
        fn the_target_atr_takes_nautilus_average_true_range_arguments() {
            let toml = with_atr(
                r#", atr = { period = 3, ma_type = "simple", use_previous = false, value_floor = 0.5 }"#,
            );
            let spec = load(&toml).unwrap().model().target.spec;
            assert_eq!(
                spec,
                Target {
                    bracket: Some(BracketSpec {
                        take_profit: Distance::Atr(2.into()),
                        stop: Distance::Atr(rust_decimal::Decimal::new(15, 1)),
                        atr: Some(
                            AtrArgs {
                                period: 3,
                                ma_type: Some(MovingAverageType::Simple),
                                use_previous: Some(false),
                                value_floor: Some(0.5),
                            }
                            .into()
                        ),
                    }),
                    limit: SignedDuration::from_mins(45),
                    atr: None,
                }
            );
        }

        /// Reports the model's violations after the standard ones and the strategy id's.
        #[rstest]
        fn reports_the_models_violations_with_the_standard_ones() {
            let toml = format!("{TOML}\n[base]\norder_id_tag = \"001\"\n")
                .replace("\"1h\"\n", "\"0s\"\n")
                .replace(r#"limit = "30m""#, r#"limit = "0s""#);
            assert_eq!(
                error_fields(&toml),
                [
                    "data.warmup",
                    "base.strategy_id",
                    "model.target.limit",
                    "model.target.atr"
                ]
            );
        }
    }
}
