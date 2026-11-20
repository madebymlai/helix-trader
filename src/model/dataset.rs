//! The training table of a strategy, stored by day in the Nautilus catalog, whose coverage is the
//! day cache.
//!
//! A dataset is the settled rows of one strategy (`TargetRow`, from a dataset run), stored as
//! Nautilus custom data, `DatasetRow`. A row holds everything needed to read its trades, so
//! training, scoring and the meta's examples read nothing else: each side's payoff features
//! (`targets::SideTarget::payoff`), P&L, commission, exit and unit, beside the decision's features.
//! A side's model reads the decision's features, then that side's payoff features
//! (`DatasetRow::side`, `deciding::SideRow`).
//!
//! Rows are stored under an identifier of the strategy's id and fingerprint (`decide::fingerprint`
//! extended by `targets::fingerprint`, the dataset runs' `DatasetVenue` and the dataset `VERSION`).
//! A change to any of them finds no row and rebuilds the dataset: the feature names, their
//! `[indicators]` arguments, warmup or version, the decision rule, the target, what its ATR is
//! built from, the commission, balance or latency. The config is read by value, the code by
//! version, each bumped by hand: the dataset's for the code making every strategy's rows, the
//! strategy's for its features (`FeatureSetBuilder::version`).
//!
//! The feature names and the payoff names (`TargetConfig::payoff_names`) are in the metadata of the
//! rows' data type. `read` checks them and each row's widths.
//!
//! The catalog's intervals of the rows are the cache: `missing` asks it for the days of a span it
//! does not cover, and `build` replays them once, from the first missing day to the last plus the
//! time a decision settles within, then writes them. That time is the target's limit
//! (`TargetConfig::settles_within`) and, at a latency, the longest its time close waits to be
//! processed (`DatasetVenue::delay`).
//!
//! The replay starts the features' warmup (`FeatureSet::warmup`) before the first day, so the
//! indicators read the data before it at the day's first decisions. Rows decided before the day are
//! not stored. A feature that is a function of the data within the warmup has the same value
//! however long the build; one keeping a running sum may differ in its last bits. A recursive one,
//! an exponential average or Wilder's ATR, never forgets its start value, whose weight decays by
//! e^-1 (Wilder's) to e^-2 (an exponential average's) a period: a warmup of 5 to 10 times the
//! longest recursive period makes that negligible, never zero to the bit.
//!
//! `write` files only the days a replay settled: those replayed whole, its end that far past
//! theirs. Days without rows (a weekend's, cold features') go in the file of the day with rows
//! before them, or the first after, because Nautilus writes no file without rows and such a day
//! would stay missing and be replayed by every build. A filed day's decisions have all settled or
//! been dropped for a reason of their own (a denial, a rejection, an entry unfilled by its time).
//! The time close, a market order, fills at its time on the venue's last quote, even with no quote
//! since (a weekend). At a latency it fills on the first quote after its arrival or, none coming,
//! at the run's next timer, on the last, within the delay the replay runs past the day. So every
//! build replaying a day stores it the same.
//!
//! A train at a time sees only what was settled then: `settled` replays the days it lacks to that
//! time and no further, as `build` does, files the days the replay settled, and takes the rows of
//! the others, the last before that time, from the replay alone.
//!
//! Nautilus skips writing a file whose name, its interval, exists, and refuses one overlapping
//! another, so `write` first deletes what the catalog holds over its days. The directory is never
//! consolidated, which would merge the days' files and lose their intervals, the cache.
//!
//! `manifest.json` beside the files holds the strategy, fingerprint, feature and payoff names and
//! target spec, rewritten whole (write, then rename) at each write.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Display,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Context;
use jiff::{SignedDuration, Timestamp, civil::Date, tz::TimeZone};
use nautilus_core::UnixNanos;
use nautilus_model::{
    custom_data,
    data::{CustomData, CustomDataTrait, Data, DataType, NautilusDataType},
    identifiers::StrategyId,
    types::Money,
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_serialization::arrow_custom_data;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{
    blueprint::ModelBlueprint,
    decide::{self, DecisionRule},
    deciding::SideRow,
    earnings::Trade,
    features::{FeatureSet, Fingerprint},
    targets::{self, PerSide, Side, TargetConfig, TargetRow},
};
use crate::{
    backtest::{Commission, Latency},
    strategy::{requirements::DataRequirements, warmup},
};

/// The version of the code that makes every strategy's rows (`decide`, `targets`, `features` and
/// the Nautilus indicators, matching engine and fill model they run on) and of the `Manifest`. It
/// is in every dataset's fingerprint, bumped by hand whenever that code makes something else of the
/// same data and config, a Nautilus pin bump included. The golden rows
/// (`tests/backtest/backtest_golden_rows.rs`) fail on a change of their rows made without a bump; a
/// change reaching none of them is bumped all the same.
pub const VERSION: u32 = 10;

const FEATURE_NAMES: &str = "feature_names";

const PAYOFF_NAMES: &str = "payoff_names";

const MANIFEST: &str = "manifest.json";

/// A settled decision as the catalog stores it.
#[arrow_custom_data]
#[custom_data]
pub struct DatasetRow {
    /// The value of each feature, in the order of the dataset's names.
    pub features: Vec<f64>,
    /// The long's payoff features (`targets::SideTarget::payoff`), in the order of the payoff
    /// names.
    pub long_payoff: Vec<f64>,
    /// The long's realized P&L, net of its commission, in the target's P&L currency.
    pub long_pnl: f64,
    /// The commission the long's P&L is net of.
    pub long_commission: f64,
    /// How the long closed (`targets::Exit::value`).
    pub long_exit: f64,
    /// What one R at the reference stake is worth (`targets::SideTarget::unit`), in the target's
    /// P&L currency.
    pub long_unit: f64,
    /// The short's payoff features, as the long's.
    pub short_payoff: Vec<f64>,
    /// The short's realized P&L, as the long's.
    pub short_pnl: f64,
    /// The commission the short's P&L is net of.
    pub short_commission: f64,
    /// How the short closed.
    pub short_exit: f64,
    /// The unit of the short's payoff, as the long's.
    pub short_unit: f64,
    /// When the last of the decision's positions closed.
    pub settled_at: UnixNanos,
    /// The time of the decision.
    pub ts_event: UnixNanos,
    /// The time of the decision too, which files the row in its day.
    pub ts_init: UnixNanos,
}

/// The error of a dataset holding rows of other feature or payoff names, or of another width.
#[derive(Debug)]
pub struct SchemaMismatch(String);

impl Display for SchemaMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SchemaMismatch {}

/// What a dataset holds, enough to explain and rebuild it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub strategy_id: StrategyId,
    /// The fingerprint of the features, the decision rule, the target and the dataset venue.
    pub fingerprint: String,
    /// The names of the features, in the order of a row's features.
    pub feature_names: Vec<String>,
    /// The target's names of each side's payoff features, in their order.
    pub payoff_names: Vec<String>,
    /// The target spec.
    pub target: TargetConfig,
}

/// The venue settings a dataset run takes, which its rows depend on: the commission its targets are
/// net of, the balance, the equity the reference stake is sized at, and the latency, if any, which
/// moves its fills. Its accounts margin nothing (`backtest::node`), so no leverage is among them.
/// Normalized in the fingerprint. Declared in `ModelConfig::dataset`, such as
/// `{ commission = { per_quantity = "0.035 USD" }, balance = "10000 USD" }`, the balance positive,
/// and optionally `latency = "150ms"`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetVenue {
    pub commission: Commission,
    #[serde(deserialize_with = "positive_balance")]
    pub balance: Money,
    /// Left out, none: each command reaches the venue as it is sent.
    #[serde(default)]
    pub latency: Option<Latency>,
}

/// Refuses a balance that is not positive: a reference stake of none sizes no target.
fn positive_balance<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Money, D::Error> {
    let text = String::deserialize(deserializer)?;
    let balance: Money = text.parse().map_err(serde::de::Error::custom)?;
    if balance.is_positive() {
        Ok(balance)
    } else {
        Err(serde::de::Error::custom(format!(
            "a dataset balance must be positive, not {balance}"
        )))
    }
}

impl DatasetVenue {
    /// Normalized so every spelling is one key. No latency reads as it did before latency was a
    /// setting.
    fn fingerprint_part(&self) -> String {
        self.to_string()
    }
}

impl Display for DatasetVenue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "commission {}, balance {} {}",
            self.commission,
            self.balance.as_decimal().normalize(),
            self.balance.currency.code
        )?;
        match self.latency {
            Some(latency) => write!(f, ", latency {latency}"),
            None => Ok(()),
        }
    }
}

impl DatasetVenue {
    /// How much later than the target's limit a decision settles: the longest its time close waits
    /// to be processed (`Latency::dataset_delay`), none without a latency.
    pub(super) fn delay(&self) -> SignedDuration {
        self.latency
            .map_or(SignedDuration::ZERO, Latency::dataset_delay)
    }
}

/// The days a train read (`Dataset::settled`): built by its replay or cached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DaysRead {
    pub built: usize,
    pub cached: usize,
}

/// The dataset of one strategy in a catalog.
#[derive(Debug, Clone)]
pub struct Dataset {
    catalog: PathBuf,
    strategy_id: StrategyId,
    fingerprint: Fingerprint,
    feature_names: Vec<String>,
    payoff_names: Vec<String>,
    warmup: SignedDuration,
    /// The target's limit (`TargetConfig::settles_within`) and the venue's delay
    /// (`DatasetVenue::delay`).
    settles_within: SignedDuration,
    target: TargetConfig,
}

impl Dataset {
    /// Registers `DatasetRow` with Nautilus, as any reader of the catalog must.
    pub(super) fn new(
        catalog: &Path,
        blueprint: &ModelBlueprint,
        fingerprint: Fingerprint,
    ) -> Self {
        nautilus_serialization::ensure_custom_data_registered::<DatasetRow>();
        let (features, config) = (blueprint.features(), blueprint.config());
        let target = &config.target;
        Self {
            catalog: catalog.to_path_buf(),
            strategy_id: blueprint.strategy_id(),
            fingerprint,
            warmup: features.warmup(),
            settles_within: target.settles_within() + config.dataset.delay(),
            feature_names: features.names().to_vec(),
            payoff_names: target.payoff_names(),
            target: target.clone(),
        }
    }

    /// Builds the days `first` to `last` the dataset is missing: `replay` runs the strategy from
    /// the warmup before the first missing day to the last's end plus the time a decision settles
    /// within, and the rows it settled are written (see `write`). With no day missing, nothing is
    /// replayed. Returns the missing days.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be read or written, or as `replay` does.
    pub fn build(
        &self,
        first: Date,
        last: Date,
        replay: impl FnOnce(Timestamp, Timestamp) -> anyhow::Result<Vec<TargetRow>>,
    ) -> anyhow::Result<Vec<Date>> {
        let missing = self.missing(first, last)?;
        let (Some(&first), Some(&last)) = (missing.first(), missing.last()) else {
            return Ok(missing);
        };
        let from = start_of(first)?;
        let to = start_of(last.tomorrow()?)? + self.settles_within;
        let rows = replay(from - self.warmup, to)?;
        self.write(&rows, from, to)?;
        Ok(missing)
    }

    /// The rows of the decisions in `[from, at)` settled at `at`, in time order. `replay` runs once
    /// over the days the dataset lacks, from the warmup before the first to `at`, and the days it
    /// settled are written (see `write`). The unsettled days' rows come from the replay alone, and
    /// a row whose positions close after `at` is dropped. Also returns the days read.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be read or written, or as `replay` does.
    pub fn settled(
        &self,
        from: Timestamp,
        at: Timestamp,
        replay: impl FnOnce(Timestamp, Timestamp) -> anyhow::Result<Vec<TargetRow>>,
    ) -> anyhow::Result<(Vec<DatasetRow>, DaysRead)> {
        let (first, last) = days(from, at);
        let mut rows = vec![];
        let first_missing = self.missing(first, last)?.first().copied();
        let built = match first_missing {
            Some(day) => days_from(day, last)?,
            None => 0,
        };
        let read = DaysRead {
            built,
            cached: days_from(first, last)? - built,
        };

        if let Some(day) = first_missing {
            let start = start_of(day)?;
            let replayed = replay(start - self.warmup, at)?;
            self.write(&replayed, start, at)?;
            let unsettled: BTreeSet<Date> = self.missing(first, last)?.into_iter().collect();
            rows.extend(
                replayed
                    .iter()
                    .map(DatasetRow::of)
                    .filter(|row| unsettled.contains(&day_of(row.ts_init))),
            );
        }
        rows.extend(self.read(from, at)?);
        let (from, at) = (UnixNanos::from(from), UnixNanos::from(at));
        rows.retain(|row| from <= row.ts_event && row.ts_event < at && row.settled_at <= at);
        rows.sort_by_key(|row| row.ts_init);
        Ok((rows, read))
    }

    /// The fingerprint the rows are stored under.
    #[must_use]
    pub const fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }

    /// The days `first` to `last` the catalog does not cover, a partly covered day included.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be read.
    pub fn missing(&self, first: Date, last: Date) -> anyhow::Result<Vec<Date>> {
        let identifier = self.identifier();
        let missing = self
            .catalog()
            .get_missing_intervals_for_request(
                UnixNanos::from(start_of(first)?).as_u64(),
                end_of(last)?.as_u64(),
                &row_type().into(),
                Some(&identifier),
            )
            .with_context(|| format!("reading the days of the dataset {identifier}"))?;
        let mut days = BTreeSet::new();
        for (start, end) in missing {
            let (mut day, last) = (day_of(start.into()), day_of(end.into()));
            while day <= last {
                days.insert(day);
                day = day.tomorrow()?;
            }
        }
        Ok(days.into_iter().collect())
    }

    /// Writes the `rows` of a replay over `[from, to]` for the days it settled, and the manifest.
    /// What the catalog holds over those days is deleted first; the rows of other days are
    /// dropped.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be read or written.
    pub fn write(&self, rows: &[TargetRow], from: Timestamp, to: Timestamp) -> anyhow::Result<()> {
        let days = self.settled_days(from, to)?;
        let (Some(&first), Some(&last)) = (days.first(), days.last()) else {
            return Ok(());
        };
        let data_type = self.data_type()?;
        let mut by_day: BTreeMap<Date, Vec<CustomData>> = BTreeMap::new();
        for row in rows {
            let stored = DatasetRow::of(row);
            by_day
                .entry(day_of(stored.ts_init))
                .or_default()
                .push(CustomData::new(Arc::new(stored), data_type.clone()));
        }

        // A file holds a day with rows and the rowless days after it, the first also those before
        let mut files: Vec<(Date, Date, Vec<CustomData>)> = vec![];
        for day in days {
            match (files.last_mut(), by_day.remove(&day)) {
                (Some(file), None) => file.1 = day,
                (Some(file), Some(data)) if file.2.is_empty() => *file = (file.0, day, data),
                (_, data) => files.push((day, day, data.unwrap_or_default())),
            }
        }
        let identifier = self.identifier();
        let mut catalog = self.catalog();
        catalog
            .delete_data_range(
                &row_type(),
                Some(&identifier),
                Some(UnixNanos::from(start_of(first)?)),
                Some(end_of(last)?),
            )
            .with_context(|| format!("deleting the days {first} to {last} of {identifier}"))?;
        for (first, last, mut data) in files {
            data.sort_by_key(|data| data.data.ts_init());
            catalog
                .write_custom_data_batch(
                    data,
                    Some(UnixNanos::from(start_of(first)?)),
                    Some(end_of(last)?),
                    None,
                )
                .with_context(|| format!("writing the days {first} to {last} of {identifier}"))?;
        }
        self.write_manifest()
    }

    /// The rows of the decisions in `[from, to]`, in time order.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be read, or a `SchemaMismatch` if it holds rows of
    /// other feature or payoff names or widths.
    pub fn read(&self, from: Timestamp, to: Timestamp) -> anyhow::Result<Vec<DatasetRow>> {
        let identifier = self.identifier();
        let data = self
            .catalog()
            .query_custom_data_dynamic(
                DatasetRow::type_name_static(),
                Some(std::slice::from_ref(&identifier)),
                Some(UnixNanos::from(from)),
                Some(UnixNanos::from(to)),
                None,
                None,
                true,
            )
            .with_context(|| format!("reading the dataset {identifier}"))?;
        let want = self.data_type()?;
        data.into_iter()
            .map(|data| {
                let Data::Custom(custom) = data else {
                    anyhow::bail!("the dataset {identifier} holds {data:?}");
                };

                if custom.data_type.metadata() != want.metadata() {
                    anyhow::bail!(SchemaMismatch(format!(
                        "the dataset {identifier} holds rows of the features {:?}, not {:?} and \
                         the payoff {:?}",
                        custom.data_type.metadata(),
                        self.feature_names,
                        self.payoff_names
                    )));
                }
                let row = custom
                    .data
                    .as_any()
                    .downcast_ref::<DatasetRow>()
                    .cloned()
                    .with_context(|| format!("the dataset {identifier} holds {custom:?}"))?;
                if row.features.len() != self.feature_names.len() {
                    anyhow::bail!(SchemaMismatch(format!(
                        "the dataset {identifier} holds a row of {} features, not {}",
                        row.features.len(),
                        self.feature_names.len()
                    )));
                }
                for payoff in [&row.long_payoff, &row.short_payoff] {
                    if payoff.len() != self.payoff_names.len() {
                        anyhow::bail!(SchemaMismatch(format!(
                            "the dataset {identifier} holds a row of {} payoff features, not {}",
                            payoff.len(),
                            self.payoff_names.len()
                        )));
                    }
                }
                Ok(row)
            })
            .collect()
    }

    /// The manifest of the dataset, as last written.
    ///
    /// # Errors
    ///
    /// Returns an error if the dataset was never written, or its manifest cannot be read.
    pub fn manifest(&self) -> anyhow::Result<Manifest> {
        let path = self.directory()?.join(MANIFEST);
        let json = fs::read_to_string(&path)
            .with_context(|| format!("reading the manifest {}", path.display()))?;
        serde_json::from_str(&json)
            .with_context(|| format!("reading the manifest {}", path.display()))
    }

    fn settled_days(&self, from: Timestamp, to: Timestamp) -> anyhow::Result<Vec<Date>> {
        let first = day_of(from.into());
        let mut day = if start_of(first)? == from {
            first
        } else {
            first.tomorrow()?
        };
        let mut days = vec![];
        while start_of(day.tomorrow()?)? + self.settles_within <= to {
            days.push(day);
            day = day.tomorrow()?;
        }
        Ok(days)
    }

    fn write_manifest(&self) -> anyhow::Result<()> {
        let manifest = Manifest {
            strategy_id: self.strategy_id,
            fingerprint: self.fingerprint.to_string(),
            feature_names: self.feature_names.clone(),
            payoff_names: self.payoff_names.clone(),
            target: self.target.clone(),
        };
        let directory = self.directory()?;
        fs::create_dir_all(&directory)
            .with_context(|| format!("creating {}", directory.display()))?;
        let path = directory.join(MANIFEST);
        let written = path.with_extension("json.tmp");
        fs::write(&written, serde_json::to_string_pretty(&manifest)?)
            .and_then(|()| fs::rename(&written, &path))
            .with_context(|| format!("writing the manifest {}", path.display()))
    }

    fn directory(&self) -> anyhow::Result<PathBuf> {
        let directory = self
            .catalog()
            .make_path_custom_data(DatasetRow::type_name_static(), Some(&self.identifier()))?;
        Ok(self.catalog.join(directory))
    }

    fn identifier(&self) -> String {
        format!("{}-{}", self.strategy_id, self.fingerprint)
    }

    fn data_type(&self) -> anyhow::Result<DataType> {
        let metadata = serde_json::from_value(json!({
            FEATURE_NAMES: self.feature_names,
            PAYOFF_NAMES: self.payoff_names,
        }))?;
        Ok(DataType::new(
            DatasetRow::type_name_static(),
            Some(metadata),
            Some(self.identifier()),
        ))
    }

    fn catalog(&self) -> ParquetDataCatalog {
        ParquetDataCatalog::new(&self.catalog, None, None, None, None)
    }
}

impl DatasetRow {
    /// The row storing `row`.
    #[must_use]
    pub fn of(row: &TargetRow) -> Self {
        let (long, short) = (row.outcomes.long, row.outcomes.short);
        Self {
            features: row.decision.features.clone(),
            long_payoff: row.payoff.long.clone(),
            long_pnl: long.pnl.as_f64(),
            long_commission: long.commission.as_f64(),
            long_exit: long.exit.value(),
            long_unit: long.unit.as_f64(),
            short_payoff: row.payoff.short.clone(),
            short_pnl: short.pnl.as_f64(),
            short_commission: short.commission.as_f64(),
            short_exit: short.exit.value(),
            short_unit: short.unit.as_f64(),
            settled_at: row.settled_at,
            ts_event: row.decision.ts_event,
            ts_init: row.decision.ts_init,
        }
    }

    /// `side`'s net P&L, commission and unit.
    #[must_use]
    pub const fn trade(&self, side: Side) -> Trade {
        match side {
            Side::Long => Trade {
                pnl: self.long_pnl,
                commission: self.long_commission,
                unit: self.long_unit,
            },
            Side::Short => Trade {
                pnl: self.short_pnl,
                commission: self.short_commission,
                unit: self.short_unit,
            },
        }
    }

    /// `side`'s row (`deciding::SideRow`).
    #[must_use]
    pub fn side(&self, side: Side) -> SideRow {
        match side {
            Side::Long => SideRow::new(&self.features, &self.long_payoff),
            Side::Short => SideRow::new(&self.features, &self.short_payoff),
        }
    }

    /// Each side's row (`side`).
    #[must_use]
    pub fn sides(&self) -> PerSide<SideRow> {
        PerSide {
            long: self.side(Side::Long),
            short: self.side(Side::Short),
        }
    }
}

/// The fingerprint of a dataset: the decisions' (`decide::fingerprint`, with the bar build delay of
/// `warmup::bar_build_delay`) extended by the target's (`targets::fingerprint`), the venue's
/// settings and the dataset `VERSION`. Only a model's blueprint computes it
/// (`ModelBlueprint::fingerprint`).
///
/// # Errors
///
/// Returns an error if `targets::fingerprint` fails.
pub(super) fn fingerprint(
    features: &FeatureSet,
    rule: DecisionRule,
    target: &TargetConfig,
    requirements: &DataRequirements,
    venue: &DatasetVenue,
) -> anyhow::Result<Fingerprint> {
    versioned_fingerprint(VERSION, features, rule, target, requirements, venue)
}

fn versioned_fingerprint(
    version: u32,
    features: &FeatureSet,
    rule: DecisionRule,
    target: &TargetConfig,
    requirements: &DataRequirements,
    venue: &DatasetVenue,
) -> anyhow::Result<Fingerprint> {
    let delay = warmup::bar_build_delay(&requirements.bar_types);
    Ok(targets::fingerprint(
        decide::fingerprint(features, rule, delay),
        target,
        rule,
        requirements,
    )?
    .with(venue.fingerprint_part().as_bytes())
    .with(format!("dataset version {version}").as_bytes()))
}

fn row_type() -> NautilusDataType {
    NautilusDataType::Custom {
        type_name: DatasetRow::type_name_static().to_string(),
    }
}

/// The first and last UTC days of `[from, to)`.
#[must_use]
pub fn days(from: Timestamp, to: Timestamp) -> (Date, Date) {
    (
        day_of(from.into()),
        day_of((to - SignedDuration::from_nanos(1)).into()),
    )
}

fn days_from(first: Date, last: Date) -> anyhow::Result<usize> {
    Ok(usize::try_from(first.until(last)?.get_days())? + 1)
}

fn day_of(time: UnixNanos) -> Date {
    Timestamp::from(time).to_zoned(TimeZone::UTC).date()
}

fn start_of(day: Date) -> anyhow::Result<Timestamp> {
    Ok(day.to_zoned(TimeZone::UTC)?.timestamp())
}

fn end_of(day: Date) -> anyhow::Result<UnixNanos> {
    Ok(UnixNanos::from(
        start_of(day.tomorrow()?)? - SignedDuration::from_nanos(1),
    ))
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use nautilus_model::{data::BarType, identifiers::InstrumentId};
    use rstest::rstest;
    use rust_decimal::Decimal;

    use super::*;
    use crate::{
        model::{
            config::{Margins, ModelConfig},
            decide::FeatureRow,
            learner::LearnerConfig,
            targets::{BracketSpec, Distance, Exit, Outcome, Outcomes, Target},
        },
        strategy::{
            Declared, EmaArgs, IndicatorsConfig, Series, conversions::Conversions, risk::R,
        },
    };

    const GOLD: &str = "XAUUSD.ICMARKETS";
    const STRATEGY: &str = "DATASET-001";

    const MIDS: [Declared<EmaArgs>; 3] = [
        Declared::new("mid_0"),
        Declared::new("mid_1"),
        Declared::new("mid_2"),
    ];

    fn tables() -> IndicatorsConfig {
        toml::from_str(
            r#"
            mid_0 = { period = 1, price_type = "MID" }
            mid_1 = { period = 2, price_type = "MID" }
            mid_2 = { period = 3, price_type = "MID" }
            "#,
        )
        .unwrap()
    }

    fn features(width: usize) -> std::rc::Rc<FeatureSet> {
        let tables = tables();
        MIDS[..width]
            .iter()
            .fold(FeatureSet::builder(), |builder, &mid| {
                builder.feature(&[Series::quotes(InstrumentId::from(GOLD))], mid, &tables)
            })
            .build()
            .unwrap()
    }

    fn target() -> TargetConfig {
        TargetConfig::builder()
            .instrument_id(InstrumentId::from(GOLD))
            .risk("0.5%".parse().unwrap())
            .spec(
                Target::builder()
                    .bracket(BracketSpec {
                        take_profit: Distance::BasisPoints(Decimal::new(75, 1)),
                        stop: Distance::BasisPoints(Decimal::from(5)),
                        atr: None,
                    })
                    .limit(SignedDuration::from_hours(2))
                    .build(),
            )
            .build()
    }

    fn requirements() -> DataRequirements {
        DataRequirements {
            bar_types: vec![],
            ticks: vec![crate::strategy::requirements::TickSeries {
                data: NautilusDataType::QuoteTick,
                instrument_id: InstrumentId::from(GOLD),
            }],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        }
    }

    /// A commission rate of 30 per million, 10 million USD.
    fn venue() -> DatasetVenue {
        DatasetVenue {
            commission: Commission::Rate(Decimal::new(3, 5)),
            balance: Money::from("10000000 USD"),
            latency: None,
        }
    }

    fn blueprint(
        strategy: &str,
        features: Rc<FeatureSet>,
        rule: DecisionRule,
        target: TargetConfig,
        requirements: DataRequirements,
    ) -> ModelBlueprint {
        blueprint_on(strategy, features, rule, target, requirements, venue())
    }

    fn blueprint_on(
        strategy: &str,
        features: Rc<FeatureSet>,
        rule: DecisionRule,
        target: TargetConfig,
        requirements: DataRequirements,
        venue: DatasetVenue,
    ) -> ModelBlueprint {
        let config = ModelConfig::builder()
            .decision(rule)
            .target(target)
            .dataset(venue)
            .window(SignedDuration::from_hours(24))
            .calibration(SignedDuration::from_hours(6))
            .margins(Margins {
                long: Some(R::new(0.0)),
                short: Some(R::new(0.0)),
            })
            .min_rows(1)
            .learner(LearnerConfig::Fake {})
            .build();
        ModelBlueprint::new(StrategyId::from(strategy), features, config, requirements).unwrap()
    }

    fn dataset(catalog: &Path, strategy: &str, width: usize) -> Dataset {
        blueprint(
            strategy,
            features(width),
            DecisionRule::Every(SignedDuration::from_hours(1)),
            target(),
            requirements(),
        )
        .dataset(catalog)
        .unwrap()
    }

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    fn payoff() -> PerSide<Vec<f64>> {
        PerSide {
            long: vec![1.4, 1.1, 0.07],
            short: vec![1.3, 1.2, 0.08],
        }
    }

    /// Settled an hour later: a long winning `pnl` at its take profit, a short losing as much.
    fn row(time: &str, features: Vec<f64>, pnl: &str) -> TargetRow {
        let ns = UnixNanos::from(at(time));
        let loss = format!("-{pnl}");
        let outcome = |pnl: &str, exit| Outcome {
            pnl: Money::from(&*format!("{pnl} USD")),
            commission: Money::from("0.1 USD"),
            exit,
            unit: Money::from("2 USD"),
        };
        TargetRow {
            decision: FeatureRow {
                strategy_id: StrategyId::from(STRATEGY),
                features,
                ts_event: ns,
                ts_init: ns,
            },
            payoff: payoff(),
            outcomes: Outcomes {
                long: outcome(pnl, Exit::TakeProfit),
                short: outcome(&loss, Exit::Stop),
            },
            settled_at: UnixNanos::from(at(time) + SignedDuration::from_hours(1)),
        }
    }

    fn stored(time: &str, features: Vec<f64>, pnl: f64) -> DatasetRow {
        let ns = UnixNanos::from(at(time));
        DatasetRow {
            features,
            long_payoff: payoff().long,
            long_pnl: pnl,
            long_commission: 0.1,
            long_exit: 1.0,
            long_unit: 2.0,
            short_payoff: payoff().short,
            short_pnl: -pnl,
            short_commission: 0.1,
            short_exit: -1.0,
            short_unit: 2.0,
            settled_at: UnixNanos::from(at(time) + SignedDuration::from_hours(1)),
            ts_event: ns,
            ts_init: ns,
        }
    }

    fn assert_bits_eq(got: &[DatasetRow], want: &[DatasetRow]) {
        let bits = |rows: &[DatasetRow]| {
            rows.iter()
                .map(|row| {
                    let values = |v: &[f64]| v.iter().map(|x| x.to_bits()).collect::<Vec<_>>();
                    (
                        values(&row.features),
                        values(&row.long_payoff),
                        values(&row.short_payoff),
                        values(&[
                            row.long_pnl,
                            row.long_commission,
                            row.long_exit,
                            row.long_unit,
                            row.short_pnl,
                            row.short_commission,
                            row.short_exit,
                            row.short_unit,
                        ]),
                        row.settled_at,
                        row.ts_event,
                        row.ts_init,
                    )
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(bits(got), bits(want), "{got:#?}");
    }

    #[rstest]
    fn a_row_stores_each_sides_payoff_pnl_commission_exit_and_unit_under_its_names() {
        let mut settled = row("2026-01-05T09:00:00Z", vec![1.5], "2.5");
        settled.outcomes.short = Outcome {
            pnl: Money::from("-3.5 USD"),
            commission: Money::from("0.12 USD"),
            exit: Exit::Time,
            unit: Money::from("4.5 USD"),
        };

        let stored = DatasetRow::of(&settled);

        assert_eq!(
            stored,
            DatasetRow {
                long_pnl: 2.5,
                long_commission: 0.1,
                long_exit: 1.0,
                long_unit: 2.0,
                short_pnl: -3.5,
                short_commission: 0.12,
                short_exit: 0.0,
                short_unit: 4.5,
                ..self::stored("2026-01-05T09:00:00Z", vec![1.5], 0.0)
            }
        );
    }

    #[rstest]
    fn a_row_answers_each_sides_trade_row_and_unit() {
        let row = DatasetRow {
            long_pnl: 0.25,
            long_commission: 0.75,
            long_unit: 3.0,
            short_pnl: 0.0,
            short_commission: 0.5,
            short_unit: 1.5,
            ..stored("2026-01-05T09:00:00Z", vec![4000.5, -2.0], 0.0)
        };

        assert_eq!(
            row.trade(Side::Long),
            Trade {
                pnl: 0.25,
                commission: 0.75,
                unit: 3.0
            }
        );
        assert_eq!(
            row.trade(Side::Short),
            Trade {
                pnl: 0.0,
                commission: 0.5,
                unit: 1.5
            }
        );
        assert_eq!(
            row.sides(),
            PerSide {
                long: SideRow {
                    features: vec![4000.5, -2.0, 1.4, 1.1, 0.07],
                },
                short: SideRow {
                    features: vec![4000.5, -2.0, 1.3, 1.2, 0.08],
                },
            }
        );
    }

    #[rstest]
    fn rows_read_back_bit_for_bit() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = dataset(dir.path(), STRATEGY, 3);
        let odd = vec![f64::NAN, -0.0, f64::MIN_POSITIVE / 4.0];
        let rows = [
            row("2026-01-06T10:00:00Z", odd.clone(), "2.5"),
            row("2026-01-05T09:00:00Z", vec![1.0, 2.0, 3.0], "0.75"),
        ];

        dataset
            .write(
                &rows,
                at("2026-01-05T00:00:00Z"),
                at("2026-01-07T02:00:00Z"),
            )
            .unwrap();

        assert_bits_eq(
            &dataset
                .read(at("2026-01-05T00:00:00Z"), at("2026-01-07T00:00:00Z"))
                .unwrap(),
            &[
                stored("2026-01-05T09:00:00Z", vec![1.0, 2.0, 3.0], 0.75),
                stored("2026-01-06T10:00:00Z", odd, 2.5),
            ],
        );
    }
    fn day(date: &str) -> Date {
        date.parse().unwrap()
    }

    #[rstest]
    fn every_day_is_missing_from_an_empty_catalog() {
        let dir = tempfile::tempdir().unwrap();

        assert_eq!(
            dataset(dir.path(), STRATEGY, 1)
                .missing(day("2026-01-05"), day("2026-01-07"))
                .unwrap(),
            [day("2026-01-05"), day("2026-01-06"), day("2026-01-07")]
        );
    }

    #[rstest]
    fn a_day_is_written_once_replay_has_passed_its_end_plus_its_longest_target() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = dataset(dir.path(), STRATEGY, 1);
        let rows = [
            row("2026-01-05T09:00:00Z", vec![1.0], "1"),
            row("2026-01-06T09:00:00Z", vec![2.0], "1"),
        ];

        dataset
            .write(
                &rows,
                at("2026-01-05T00:00:00Z"),
                at("2026-01-07T01:59:59Z"),
            )
            .unwrap();

        assert_eq!(
            dataset
                .missing(day("2026-01-05"), day("2026-01-06"))
                .unwrap(),
            [day("2026-01-06")]
        );
        assert_bits_eq(
            &dataset
                .read(at("2026-01-05T00:00:00Z"), at("2026-01-07T00:00:00Z"))
                .unwrap(),
            &[stored("2026-01-05T09:00:00Z", vec![1.0], 1.0)],
        );
    }

    /// The time close sent at the 2 hour limit reaches the venue 30 s later and fills at the next
    /// settlement point, at most a minute after (the execution engine's purge timer): the day is
    /// settled 2 hours, 30 s and a minute after its end.
    #[rstest]
    fn at_a_latency_a_day_is_written_once_replay_has_passed_its_end_its_limit_its_latency_and_a_minute()
     {
        let dir = tempfile::tempdir().unwrap();
        let dataset = blueprint_on(
            STRATEGY,
            features(1),
            DecisionRule::Every(SignedDuration::from_hours(1)),
            target(),
            requirements(),
            DatasetVenue {
                latency: Some(latency(30_000)),
                ..venue()
            },
        )
        .dataset(dir.path())
        .unwrap();
        let rows = [row("2026-01-05T09:00:00Z", vec![1.0], "1")];
        let from = at("2026-01-05T00:00:00Z");
        let missing = || {
            dataset
                .missing(day("2026-01-05"), day("2026-01-05"))
                .unwrap()
        };

        dataset
            .write(&rows, from, at("2026-01-06T02:01:29.999Z"))
            .unwrap();
        assert_eq!(missing(), [day("2026-01-05")]);
        dataset
            .write(&rows, from, at("2026-01-06T02:01:30Z"))
            .unwrap();
        assert!(missing().is_empty(), "{:?}", missing());
    }

    /// The last day is replayed to its end plus 2 hours, 30 s and a minute.
    #[rstest]
    fn at_a_latency_a_build_replays_past_the_last_day_by_its_limit_its_latency_and_a_minute() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = blueprint_on(
            STRATEGY,
            features(1),
            DecisionRule::Every(SignedDuration::from_hours(1)),
            target(),
            requirements(),
            DatasetVenue {
                latency: Some(latency(30_000)),
                ..venue()
            },
        )
        .dataset(dir.path())
        .unwrap();
        let mut replays = vec![];

        dataset
            .build(day("2026-01-05"), day("2026-01-05"), |from, to| {
                replays.push((from, to));
                Ok(vec![])
            })
            .unwrap();

        assert_eq!(
            replays,
            [(at("2026-01-05T00:00:00Z"), at("2026-01-06T02:01:30Z"))]
        );
    }

    #[rstest]
    fn a_day_the_replay_started_within_is_not_written() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = dataset(dir.path(), STRATEGY, 1);
        let rows = [
            row("2026-01-05T09:00:00Z", vec![1.0], "1"),
            row("2026-01-06T09:00:00Z", vec![2.0], "1"),
        ];

        dataset
            .write(
                &rows,
                at("2026-01-05T08:00:00Z"),
                at("2026-01-07T02:00:00Z"),
            )
            .unwrap();

        assert_eq!(
            dataset
                .missing(day("2026-01-05"), day("2026-01-06"))
                .unwrap(),
            [day("2026-01-05")]
        );
    }
    #[rstest]
    fn recomputing_a_day_replaces_its_rows() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = dataset(dir.path(), STRATEGY, 1);
        let (from, to) = (at("2026-01-05T00:00:00Z"), at("2026-01-06T02:00:00Z"));

        dataset
            .write(&[row("2026-01-05T09:00:00Z", vec![1.0], "1")], from, to)
            .unwrap();
        dataset
            .write(&[row("2026-01-05T10:00:00Z", vec![2.0], "3")], from, to)
            .unwrap();

        assert_bits_eq(
            &dataset.read(from, to).unwrap(),
            &[stored("2026-01-05T10:00:00Z", vec![2.0], 3.0)],
        );
    }

    #[rstest]
    fn days_without_rows_are_covered_by_the_files_of_the_days_beside_them() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = dataset(dir.path(), STRATEGY, 1);

        // A Thursday without decisions, a Friday, a weekend, a Monday
        let rows = [
            row("2026-01-02T09:00:00Z", vec![1.0], "1"),
            row("2026-01-05T09:00:00Z", vec![2.0], "1"),
        ];

        dataset
            .write(
                &rows,
                at("2026-01-01T00:00:00Z"),
                at("2026-01-06T02:00:00Z"),
            )
            .unwrap();

        assert_eq!(
            dataset
                .missing(day("2026-01-01"), day("2026-01-05"))
                .unwrap(),
            []
        );
        assert_bits_eq(
            &dataset
                .read(at("2026-01-01T00:00:00Z"), at("2026-01-06T00:00:00Z"))
                .unwrap(),
            &[
                stored("2026-01-02T09:00:00Z", vec![1.0], 1.0),
                stored("2026-01-05T09:00:00Z", vec![2.0], 1.0),
            ],
        );
    }
    #[rstest]
    fn strategies_with_different_feature_widths_coexist() {
        let dir = tempfile::tempdir().unwrap();
        let (narrow, wide) = (
            dataset(dir.path(), "NARROW-001", 1),
            dataset(dir.path(), "WIDE-001", 3),
        );
        let (from, to) = (at("2026-01-05T00:00:00Z"), at("2026-01-06T02:00:00Z"));

        narrow
            .write(&[row("2026-01-05T09:00:00Z", vec![1.0], "1")], from, to)
            .unwrap();
        wide.write(
            &[row("2026-01-05T09:00:00Z", vec![1.0, 2.0, 3.0], "2")],
            from,
            to,
        )
        .unwrap();

        assert_bits_eq(
            &narrow.read(from, to).unwrap(),
            &[stored("2026-01-05T09:00:00Z", vec![1.0], 1.0)],
        );
        assert_bits_eq(
            &wide.read(from, to).unwrap(),
            &[stored("2026-01-05T09:00:00Z", vec![1.0, 2.0, 3.0], 2.0)],
        );
    }

    fn changed(
        dir: &Path,
        rule: DecisionRule,
        target: TargetConfig,
        commission: Commission,
    ) -> Dataset {
        blueprint_on(
            STRATEGY,
            features(1),
            rule,
            target,
            requirements(),
            DatasetVenue {
                commission,
                ..venue()
            },
        )
        .dataset(dir)
        .unwrap()
    }

    const HOURLY: DecisionRule = DecisionRule::Every(SignedDuration::from_hours(1));

    #[rstest]
    #[case::of_the_features(|dir: &Path| dataset(dir, STRATEGY, 2))]
    #[case::of_the_decisions(|dir: &Path| {
        changed(
            dir,
            DecisionRule::Every(SignedDuration::from_mins(30)),
            target(),
            venue().commission,
        )
    })]
    #[case::of_the_target(|dir: &Path| {
        changed(
            dir,
            HOURLY,
            TargetConfig {
                spec: Target {
                    limit: SignedDuration::from_mins(30),
                    ..target().spec
                },
                ..target()
            },
            venue().commission,
        )
    })]
    #[case::of_the_commissions_rate(|dir: &Path| {
        changed(dir, HOURLY, target(), Commission::Rate(Decimal::new(4, 5)))
    })]
    #[case::of_the_commissions_kind_per_order(|dir: &Path| {
        changed(
            dir,
            HOURLY,
            target(),
            Commission::PerOrder(Money::from_str_exact("0.00003 USD").unwrap()),
        )
    })]
    #[case::of_the_commissions_kind_per_quantity(|dir: &Path| {
        changed(
            dir,
            HOURLY,
            target(),
            Commission::PerQuantity(Money::from_str_exact("0.00003 USD").unwrap()),
        )
    })]
    fn a_change_of_the_features_the_decisions_the_target_or_the_commission_finds_an_empty_cache(
        #[case] changed: fn(&Path) -> Dataset,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let (from, to) = (at("2026-01-05T00:00:00Z"), at("2026-01-06T02:00:00Z"));
        dataset(dir.path(), STRATEGY, 1)
            .write(&[row("2026-01-05T09:00:00Z", vec![1.0], "1")], from, to)
            .unwrap();
        let changed = changed(dir.path());

        assert!(
            dataset(dir.path(), STRATEGY, 1)
                .missing(day("2026-01-05"), day("2026-01-05"))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            changed
                .missing(day("2026-01-05"), day("2026-01-05"))
                .unwrap(),
            [day("2026-01-05")]
        );
        assert!(changed.read(from, to).unwrap().is_empty());
    }
    #[rstest]
    fn the_manifest_names_the_features_the_payoff_the_target_and_the_fingerprint() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = dataset(dir.path(), STRATEGY, 2);
        for (time, from, to) in [
            (
                "2026-01-05T09:00:00Z",
                "2026-01-05T00:00:00Z",
                "2026-01-06T02:00:00Z",
            ),
            (
                "2026-01-06T09:00:00Z",
                "2026-01-06T00:00:00Z",
                "2026-01-07T02:00:00Z",
            ),
            (
                "2026-01-08T09:00:00Z",
                "2026-01-08T00:00:00Z",
                "2026-01-09T02:00:00Z",
            ),
        ] {
            dataset
                .write(&[row(time, vec![1.0, 2.0], "1")], at(from), at(to))
                .unwrap();
        }

        let manifest = dataset.manifest().unwrap();

        assert_eq!(manifest.strategy_id, StrategyId::from(STRATEGY));
        assert_eq!(manifest.feature_names, ["mid_0", "mid_1"]);
        assert_eq!(
            manifest.payoff_names,
            ["take_profit_r", "stop_loss_r", "entry_cost"]
        );
        assert_eq!(manifest.target, target());
        assert_eq!(manifest.fingerprint, dataset.fingerprint().to_string());
    }

    #[rstest]
    fn bumping_the_dataset_version_changes_the_fingerprint() {
        let rule = DecisionRule::Every(SignedDuration::from_hours(1));
        let at = |version| {
            versioned_fingerprint(
                version,
                &features(1),
                rule,
                &target(),
                &requirements(),
                &venue(),
            )
            .unwrap()
        };
        let current = blueprint(STRATEGY, features(1), rule, target(), requirements())
            .fingerprint()
            .unwrap();

        assert_ne!(current, at(VERSION + 1));
    }

    /// The identifier of a fixed strategy's rows as a literal: its id, then the fingerprint of its
    /// features, hourly decisions, bracket, requirements and venue. A change that rekeyed every
    /// dataset, leaving rows and trained models behind, fails here. A deliberate rekey, a bump of
    /// `VERSION` or a new fingerprint input, updates the literal and the store's pinned directory
    /// (`store::tests::a_fixed_strategys_model_sets_keep_their_directory`).
    #[rstest]
    fn a_fixed_strategys_dataset_keeps_its_identifier() {
        let dataset = dataset(Path::new("catalog"), STRATEGY, 1);

        assert_eq!(
            dataset.identifier(),
            "DATASET-001-ebabdcfdc08b9b3f91bb4242f96cab59906b9cbfea0e52869e73ef5d85125798"
        );
    }

    /// A composite bar type makes a strategy decide 5 s after its grid (`warmup::bar_build_delay`).
    #[rstest]
    fn a_strategy_on_composite_bars_is_keyed_by_its_build_delay() {
        let of = |requirements: &DataRequirements| {
            blueprint(
                STRATEGY,
                features(1),
                DecisionRule::Every(SignedDuration::from_hours(1)),
                target(),
                requirements.clone(),
            )
            .fingerprint()
            .unwrap()
        };
        let composite = DataRequirements {
            bar_types: vec![BarType::from(
                "XAUUSD.ICMARKETS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
            )],
            ..requirements()
        };

        assert_ne!(of(&composite), of(&requirements()));
    }

    /// Another balance is another fingerprint, another spelling of one is not.
    #[rstest]
    fn another_balance_of_the_dataset_run_is_another_fingerprint() {
        let of = |venue: DatasetVenue| {
            blueprint_on(
                STRATEGY,
                features(1),
                DecisionRule::Every(SignedDuration::from_hours(1)),
                target(),
                requirements(),
                venue,
            )
            .fingerprint()
            .unwrap()
        };

        assert_ne!(
            of(venue()),
            of(DatasetVenue {
                balance: Money::from("20000000 USD"),
                ..venue()
            })
        );
        assert_eq!(
            of(venue()),
            of(DatasetVenue {
                balance: Money::from("10000000.00 USD"),
                ..venue()
            })
        );
    }

    /// A commission reads as its kind and its amount without trailing zeros. No leverage keys it.
    #[rstest]
    #[case::a_rate(
        Commission::Rate(Decimal::new(30, 6)),
        "commission rate 0.00003, balance 10000000 USD"
    )]
    #[case::per_order(
        Commission::PerOrder(Money::from("3.00 USD")),
        "commission per order 3 USD, balance 10000000 USD"
    )]
    #[case::per_quantity(
        Commission::PerQuantity(Money::from_str_exact("0.0350 USD").unwrap()),
        "commission per quantity 0.035 USD, balance 10000000 USD"
    )]
    fn a_dataset_runs_venue_is_keyed_by_its_commissions_kind_and_amount(
        #[case] commission: Commission,
        #[case] text: &str,
    ) {
        let venue = DatasetVenue {
            commission,
            ..venue()
        };

        assert_eq!(venue.fingerprint_part(), text);
    }

    fn latency(millis: i64) -> Latency {
        Latency::try_from(SignedDuration::from_millis(millis)).unwrap()
    }

    /// A latency keys the venue only when declared.
    #[rstest]
    #[case::none(None, "commission rate 0.00003, balance 10000000 USD")]
    #[case::of_milliseconds(
        Some(latency(150)),
        "commission rate 0.00003, balance 10000000 USD, latency 150ms"
    )]
    #[case::of_seconds_and_milliseconds(
        Some(latency(1500)),
        "commission rate 0.00003, balance 10000000 USD, latency 1s 500ms"
    )]
    fn a_dataset_runs_latency_keys_its_venue_only_when_declared(
        #[case] latency: Option<Latency>,
        #[case] text: &str,
    ) {
        let venue = DatasetVenue { latency, ..venue() };

        assert_eq!(venue.fingerprint_part(), text);
    }

    #[rstest]
    fn another_latency_of_the_dataset_run_is_another_fingerprint() {
        let of = |latency| {
            blueprint_on(
                STRATEGY,
                features(1),
                DecisionRule::Every(SignedDuration::from_hours(1)),
                target(),
                requirements(),
                DatasetVenue { latency, ..venue() },
            )
            .fingerprint()
            .unwrap()
        };

        assert_ne!(of(None), of(Some(latency(150))));
        assert_ne!(of(Some(latency(150))), of(Some(latency(200))));
    }

    fn converting(pairs: &[&str]) -> Fingerprint {
        let pairs: Vec<InstrumentId> = pairs.iter().map(|&pair| InstrumentId::from(pair)).collect();
        let mut requirements = requirements();
        requirements
            .ticks
            .extend(
                pairs
                    .iter()
                    .map(|&instrument_id| crate::strategy::requirements::TickSeries {
                        data: NautilusDataType::QuoteTick,
                        instrument_id,
                    }),
            );
        let target = TargetConfig {
            conversions: Conversions::new(pairs),
            ..target()
        };
        blueprint(
            STRATEGY,
            features(1),
            DecisionRule::Every(SignedDuration::from_hours(1)),
            target,
            requirements,
        )
        .fingerprint()
        .unwrap()
    }

    /// Declaring pairs, or other pairs, is another fingerprint; none keeps the earlier one
    /// (`a_fixed_strategys_dataset_keeps_its_identifier`).
    #[rstest]
    fn the_conversions_key_the_dataset_only_when_declared() {
        let none = converting(&[]);
        let eurusd = converting(&["EURUSD.ICMARKETS"]);

        assert_eq!(
            none,
            blueprint(
                STRATEGY,
                features(1),
                DecisionRule::Every(SignedDuration::from_hours(1)),
                target(),
                requirements()
            )
            .fingerprint()
            .unwrap()
        );
        assert_ne!(none, eurusd);
        assert_ne!(eurusd, converting(&["GBPUSD.ICMARKETS"]));
        assert_ne!(
            eurusd,
            converting(&["EURUSD.ICMARKETS", "USDJPY.ICMARKETS"])
        );
    }

    #[rstest]
    fn a_dataset_never_written_has_no_manifest() {
        let dir = tempfile::tempdir().unwrap();

        let err = format!(
            "{:#}",
            dataset(dir.path(), STRATEGY, 1).manifest().unwrap_err()
        );

        assert!(err.contains(&dir.path().display().to_string()), "{err}");
        assert!(err.contains("manifest.json"), "{err}");
    }
    #[rstest]
    fn a_build_replays_the_missing_days_once_through_their_settling() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = dataset(dir.path(), STRATEGY, 1);
        let rows = [
            row("2026-01-05T09:00:00Z", vec![1.0], "1"),
            row("2026-01-07T09:00:00Z", vec![3.0], "1"),
        ];
        dataset
            .write(
                &[row("2026-01-06T09:00:00Z", vec![2.0], "1")],
                at("2026-01-06T00:00:00Z"),
                at("2026-01-07T02:00:00Z"),
            )
            .unwrap();
        let mut replays = vec![];

        let built = dataset
            .build(day("2026-01-05"), day("2026-01-07"), |from, to| {
                replays.push((from, to));
                Ok(rows.to_vec())
            })
            .unwrap();

        assert_eq!(built, [day("2026-01-05"), day("2026-01-07")]);
        assert_eq!(
            replays,
            [(at("2026-01-05T00:00:00Z"), at("2026-01-08T02:00:00Z"))]
        );
        assert!(
            dataset
                .missing(day("2026-01-05"), day("2026-01-07"))
                .unwrap()
                .is_empty()
        );
    }

    fn warmed(catalog: &Path) -> Dataset {
        let features = FeatureSet::builder()
            .feature(
                &[Series::quotes(InstrumentId::from(GOLD))],
                MIDS[0],
                &tables(),
            )
            .warmup(SignedDuration::from_hours(2))
            .build()
            .unwrap();
        blueprint(
            STRATEGY,
            features,
            DecisionRule::Every(SignedDuration::from_hours(1)),
            target(),
            requirements(),
        )
        .dataset(catalog)
        .unwrap()
    }

    #[rstest]
    fn a_build_replays_the_features_warmup_before_its_first_day_and_stores_none_of_it() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = warmed(dir.path());
        let rows = [
            row("2026-01-04T23:00:00Z", vec![1.0], "1"),
            row("2026-01-05T09:00:00Z", vec![2.0], "1"),
        ];
        let mut replays = vec![];

        let built = dataset
            .build(day("2026-01-05"), day("2026-01-05"), |from, to| {
                replays.push((from, to));
                Ok(rows.to_vec())
            })
            .unwrap();

        assert_eq!(built, [day("2026-01-05")]);
        assert_eq!(
            replays,
            [(at("2026-01-04T22:00:00Z"), at("2026-01-06T02:00:00Z"))]
        );
        assert_bits_eq(
            &dataset
                .read(at("2026-01-04T00:00:00Z"), at("2026-01-06T00:00:00Z"))
                .unwrap(),
            &[stored("2026-01-05T09:00:00Z", vec![2.0], 1.0)],
        );
        assert_eq!(
            dataset
                .missing(day("2026-01-04"), day("2026-01-05"))
                .unwrap(),
            [day("2026-01-04")]
        );
    }

    #[rstest]
    fn a_train_replays_the_features_warmup_before_the_first_missing_day() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = warmed(dir.path());
        let rows = [
            row("2026-01-04T23:00:00Z", vec![1.0], "1"),
            row("2026-01-05T09:00:00Z", vec![2.0], "1"),
        ];
        let mut replays = vec![];

        let (settled, _) = dataset
            .settled(
                at("2026-01-05T00:00:00Z"),
                at("2026-01-05T12:00:00Z"),
                |from, to| {
                    replays.push((from, to));
                    Ok(rows.to_vec())
                },
            )
            .unwrap();

        assert_eq!(
            replays,
            [(at("2026-01-04T22:00:00Z"), at("2026-01-05T12:00:00Z"))]
        );
        assert_bits_eq(&settled, &[stored("2026-01-05T09:00:00Z", vec![2.0], 1.0)]);
    }

    #[rstest]
    fn a_second_build_of_the_same_span_replays_no_day() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = dataset(dir.path(), STRATEGY, 1);
        let rows = [row("2026-01-05T09:00:00Z", vec![1.0], "1")];
        dataset
            .build(day("2026-01-05"), day("2026-01-05"), |_, _| {
                Ok(rows.to_vec())
            })
            .unwrap();

        let built = dataset
            .build(day("2026-01-05"), day("2026-01-05"), |_, _| {
                panic!("a second build replayed")
            })
            .unwrap();

        assert!(built.is_empty());
    }

    fn of_a_narrow_payoff() -> TargetRow {
        let mut narrow = row("2026-01-05T09:00:00Z", vec![1.0], "1");
        narrow.payoff = PerSide {
            long: vec![0.1],
            short: vec![0.2],
        };
        narrow
    }

    #[rstest]
    #[case::of_features(row("2026-01-05T09:00:00Z", vec![1.0, 2.0], "1"))]
    #[case::of_the_payoff(of_a_narrow_payoff())]
    fn rows_of_another_width_than_the_names_are_a_schema_mismatch(#[case] row: TargetRow) {
        let dir = tempfile::tempdir().unwrap();
        let dataset = dataset(dir.path(), STRATEGY, 1);
        let (from, to) = (at("2026-01-05T00:00:00Z"), at("2026-01-06T02:00:00Z"));
        dataset.write(&[row], from, to).unwrap();

        let err = dataset.read(from, to).unwrap_err();

        assert!(err.downcast_ref::<SchemaMismatch>().is_some(), "{err:#}");
    }

    #[rstest]
    fn the_settled_rows_are_those_of_the_span_settled_at_its_end() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = dataset(dir.path(), STRATEGY, 1);

        let rows = [
            row("2026-01-05T08:00:00Z", vec![1.0], "1"),
            row("2026-01-05T09:00:00Z", vec![2.0], "1"),
            row("2026-01-07T08:30:00Z", vec![3.0], "1"),
            row("2026-01-07T09:30:00Z", vec![4.0], "1"),
        ];
        let mut replays = vec![];

        let (settled, days) = dataset
            .settled(
                at("2026-01-05T09:00:00Z"),
                at("2026-01-07T10:00:00Z"),
                |from, to| {
                    replays.push((from, to));
                    Ok(rows.to_vec())
                },
            )
            .unwrap();

        assert_eq!(
            replays,
            [(at("2026-01-05T00:00:00Z"), at("2026-01-07T10:00:00Z"))]
        );
        assert_eq!(
            days,
            DaysRead {
                built: 3,
                cached: 0
            }
        );
        assert_bits_eq(
            &settled,
            &[
                stored("2026-01-05T09:00:00Z", vec![2.0], 1.0),
                stored("2026-01-07T08:30:00Z", vec![3.0], 1.0),
            ],
        );
    }

    /// The 5th and 6th, settled by the first train's end, are cached; the unsettled 7th is built
    /// again.
    #[rstest]
    fn the_days_settled_at_the_end_are_cached_and_the_others_replayed_again() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = dataset(dir.path(), STRATEGY, 1);
        let (from, end) = (at("2026-01-05T00:00:00Z"), at("2026-01-07T10:00:00Z"));
        let rows = [
            row("2026-01-05T09:00:00Z", vec![1.0], "1"),
            row("2026-01-06T09:00:00Z", vec![2.0], "1"),
            row("2026-01-07T08:00:00Z", vec![3.0], "1"),
        ];
        dataset
            .settled(from, end, |_, _| Ok(rows.to_vec()))
            .unwrap();
        let mut replays = vec![];

        // The 2 hour limit settles the 6th by 02:00 of the 7th
        let missing = dataset
            .missing(day("2026-01-05"), day("2026-01-07"))
            .unwrap();
        let (settled, days) = dataset
            .settled(from, end, |from, to| {
                replays.push((from, to));
                Ok(rows[2..].to_vec())
            })
            .unwrap();

        assert_eq!(missing, [day("2026-01-07")]);
        assert_eq!(replays, [(at("2026-01-07T00:00:00Z"), end)]);
        assert_eq!(
            days,
            DaysRead {
                built: 1,
                cached: 2
            }
        );
        assert_bits_eq(
            &settled,
            &[
                stored("2026-01-05T09:00:00Z", vec![1.0], 1.0),
                stored("2026-01-06T09:00:00Z", vec![2.0], 1.0),
                stored("2026-01-07T08:00:00Z", vec![3.0], 1.0),
            ],
        );
    }

    #[rstest]
    fn settled_rows_of_cached_days_replay_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = dataset(dir.path(), STRATEGY, 1);
        dataset
            .write(
                &[row("2026-01-05T09:00:00Z", vec![1.0], "1")],
                at("2026-01-05T00:00:00Z"),
                at("2026-01-06T02:00:00Z"),
            )
            .unwrap();

        let (settled, days) = dataset
            .settled(
                at("2026-01-05T00:00:00Z"),
                at("2026-01-06T00:00:00Z"),
                |_, _| panic!("a cached span replayed"),
            )
            .unwrap();

        assert_bits_eq(&settled, &[stored("2026-01-05T09:00:00Z", vec![1.0], 1.0)]);
        assert_eq!(
            days,
            DaysRead {
                built: 0,
                cached: 1
            }
        );
    }
}
