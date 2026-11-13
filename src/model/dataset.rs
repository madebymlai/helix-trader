//! The training table of a strategy, stored by day in the Nautilus catalog,
//! whose coverage is the day cache.
//!
//! A dataset is the settled rows of one strategy (`TargetRow`, from a
//! dataset run), stored as Nautilus custom data, `DatasetRow`, one type for
//! every strategy and target. A row holds everything needed to read its
//! trades, so that training, scoring and the meta's examples read nothing
//! else: each side's payoff features (`targets::SideTarget::payoff`), P&L,
//! commission, exit and unit, beside its decision's features. A side's model
//! reads the decision's features, then that side's payoff features, the row
//! a decision builds alike (`DatasetRow::side`, `deciding::SideRow`). The
//! features and each side's payoff features are list columns, so each
//! strategy and target has the widths of its own, and its rows are stored
//! under an identifier
//! of its id and its fingerprint (`decide::fingerprint` extended by
//! `targets::fingerprint`, the venue settings of its dataset runs,
//! `DatasetVenue`, and the dataset `VERSION`): a change to the feature
//! names, the arguments of their `[indicators]` tables, their warmup or
//! version, the decision rule, the target, what the
//! strategy reads that its ATR is built from, the commission,
//! balance or leverage of its dataset runs, or the dataset version finds
//! no row, and the dataset is rebuilt. The
//! config is read by value, the code by version: the dataset's for the
//! code making every strategy's rows, the strategy's own for its features
//! (`FeatureSetBuilder::version`), each bumped by hand.
//! The feature names and the target's payoff names
//! (`TargetConfig::payoff_names`) are in the metadata of the rows' data
//! type, which the catalog stores with each file and gives back with each
//! row, so the rows do not repeat them; `read` checks them, and each row's
//! widths.
//!
//! The catalog's intervals of the rows are the cache: `missing` asks the
//! catalog for the days of a span it does not cover, and `build` replays
//! them, once, from the first missing day to the last plus the target's
//! limit (`TargetConfig::settles_within`), then writes them. The replay
//! starts the features' warmup (`FeatureSet::warmup`) before the first
//! day, so that the indicators read at the day's first decisions the data
//! before it, as inside a longer build; the rows decided before the day
//! are not stored. A feature whose value is a function of the data within
//! the warmup then has the same value at every decision of a day, however
//! long the build; one keeping a running sum may still differ in its last
//! bits. A recursive one, an exponential average or Wilder's ATR, never
//! forgets the value it started from, whose weight decays by e^-1
//! (Wilder's) to e^-2 (an exponential average's) a period: after a warmup
//! of one period a day's first rows still depend on where the build
//! started by up to about a third of the difference. A warmup of
//! several times the longest recursive period, 5 to 10, makes the rows of
//! a day depend on the cut negligibly, never to the bit. `write` files
//! only the days a replay settled: those it replayed whole, its end that
//! far past theirs. Each day's file is written with the day's bounds,
//! the interval it covers, the days without rows (a weekend's, cold
//! features') within the file of the day with rows before them, or the
//! first after: Nautilus writes no file without rows, so a day without
//! rows would otherwise stay missing and be replayed by every build. By
//! then every decision of a day filed has settled or been dropped for a
//! reason of its own (a denial, a rejection, an entry unfilled by its
//! time), wherever the replay ends: its time close, a market order, fills
//! at its time on the venue's last quote, even with no quote since (a
//! weekend), as any market order between quotes does. So a day is stored
//! the same by every build replaying it.
//!
//! A train at a time sees only what was settled then: `settled` replays
//! the days it lacks to that time and no further, from the warmup before
//! the first, as `build` does, files the days the
//! replay settled, and takes the rows of the days it did not, the last
//! before that time, from the replay alone, so those are replayed again
//! by the next train.
//!
//! Nautilus skips writing a file whose name, its interval, exists, and
//! refuses one overlapping another, so `write` deletes what the catalog
//! holds over the days it writes first: recomputing a day replaces it.
//! Consolidating the directory would merge the days' files and lose their
//! intervals, the cache: it is never consolidated.
//!
//! Beside the files, each dataset has a manifest, `manifest.json`: its
//! strategy, fingerprint, feature names and payoff names in order, and
//! target spec, rewritten whole (write, then rename) at each write.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, bail};
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
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{
    blueprint::ModelBlueprint,
    decide::{self, DecisionRule},
    deciding::SideRow,
    features::{FeatureSet, Fingerprint},
    holdout::Trade,
    targets::{self, PerSide, Side, TargetConfig, TargetRow},
};
use crate::{
    backtest::Commission,
    strategy::{DataRequirements, warmup},
};

/// The version of the code that makes every strategy's rows: the
/// decisions (`decide`), the target's (`targets`), the shared feature code
/// (`features`), and the Nautilus they run on, its indicators, matching
/// engine and fill model; and of what a dataset's manifest holds
/// (`Manifest`). In every dataset's fingerprint: bumped by hand whenever
/// what that code makes of the same data and config changes, a Nautilus pin
/// bump changing indicators or fills included, or that manifest does, which
/// then rebuilds every dataset and model. The golden rows
/// (`tests/backtest/backtest_golden_rows.rs`) fail on a change of their rows
/// made without a bump; a change reaching none of them is bumped all the same.
pub const VERSION: u32 = 9;

/// The key of the feature names in the metadata of the rows' data type.
const FEATURE_NAMES: &str = "feature_names";

/// The key of the payoff names in the metadata of the rows' data type.
const PAYOFF_NAMES: &str = "payoff_names";

/// The name of the manifest file in the directory of a dataset's rows.
const MANIFEST: &str = "manifest.json";

/// A settled decision as the catalog stores it.
#[arrow_custom_data]
#[custom_data]
pub struct DatasetRow {
    /// The value of each feature, in the order of the dataset's names.
    pub features: Vec<f64>,
    /// The long's payoff features, as its decision set them
    /// (`targets::SideTarget::payoff`), in the order of the dataset's payoff
    /// names.
    pub long_payoff: Vec<f64>,
    /// The long's realised P&L, net of its commission, in the target's P&L
    /// currency.
    pub long_pnl: f64,
    /// The commission the long's P&L is net of.
    pub long_commission: f64,
    /// How the long closed (`targets::Exit::value`).
    pub long_exit: f64,
    /// The unit of the long's payoff, as its decision set it
    /// (`targets::SideTarget::unit`).
    pub long_unit: f64,
    /// The short's payoff features, as the long's.
    pub short_payoff: Vec<f64>,
    /// The short's realised P&L, as the long's.
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

/// The error of a dataset holding rows that do not match its schema: other
/// feature or payoff names, or a row of another width than its names.
#[derive(Debug)]
pub struct SchemaMismatch(String);

impl fmt::Display for SchemaMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SchemaMismatch {}

/// What a dataset holds, enough to explain and rebuild it. The days it
/// holds are the catalog's intervals of its rows (`Dataset::missing`),
/// which it does not repeat.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub strategy_id: StrategyId,
    /// The fingerprint of the features, the decision rule, the target and
    /// the venue of its dataset runs.
    pub fingerprint: String,
    /// The names of the features, in the order of a row's features.
    pub feature_names: Vec<String>,
    /// The names of the payoff features of each side, the target's, in the
    /// order of a row's side's payoff features.
    pub payoff_names: Vec<String>,
    /// The target spec.
    pub target: TargetConfig,
}

/// The venue settings a dataset run takes, each of which its rows depend
/// on: the commission its targets are net of, and its account's starting
/// balance and leverage, under which Nautilus' risk engine denies an
/// order the account cannot margin. Part of the dataset's fingerprint,
/// normalized, so neither rows nor models of other settings are ever read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetVenue {
    pub commission: Commission,
    pub balance: Money,
    pub leverage: Decimal,
}

impl DatasetVenue {
    /// The settings as the fingerprint reads them: the commission's kind
    /// and its amount normalized (`Commission::normalized`), and the
    /// balance and the leverage without trailing zeros, so that every
    /// spelling of one is one key.
    fn fingerprint_part(&self) -> String {
        let kind = match self.commission {
            Commission::PerOrder(_) => "per order",
            Commission::Rate(_) => "rate",
            Commission::PerQuantity(_) => "per quantity",
        };
        format!(
            "commission {kind} {}, balance {} {}, leverage {}",
            self.commission.normalized(),
            self.balance.as_decimal().normalize(),
            self.balance.currency.code,
            self.leverage.normalize()
        )
    }
}

/// The dataset of one strategy in a catalog.
#[derive(Debug, Clone)]
pub struct Dataset {
    catalog: PathBuf,
    strategy_id: StrategyId,
    fingerprint: Fingerprint,
    feature_names: Vec<String>,
    /// The target's payoff names (`TargetConfig::payoff_names`).
    payoff_names: Vec<String>,
    /// How much a replay runs before its first day: the features' warmup.
    warmup: SignedDuration,
    target: TargetConfig,
}

impl Dataset {
    /// The dataset of the model of `blueprint`, stored under `fingerprint`,
    /// its blueprint's on the venue of its runs
    /// (`ModelBlueprint::fingerprint`), in the catalog at `catalog`.
    /// Registers `DatasetRow` with Nautilus, as any reader of the catalog
    /// must.
    pub(super) fn new(
        catalog: &Path,
        blueprint: &ModelBlueprint,
        fingerprint: Fingerprint,
    ) -> Self {
        nautilus_serialization::ensure_custom_data_registered::<DatasetRow>();
        let (features, target) = (blueprint.features(), &blueprint.config().target);
        Self {
            catalog: catalog.to_path_buf(),
            strategy_id: blueprint.strategy_id(),
            fingerprint,
            warmup: features.warmup(),
            feature_names: features.names().to_vec(),
            payoff_names: target.payoff_names(),
            target: target.clone(),
        }
    }

    /// Builds the days `first` to `last` the dataset is missing: `replay`
    /// runs the strategy from the features' warmup before the first missing
    /// day's start to the last's end plus the target's limit and returns
    /// the rows it settled, which are written (see `write`), but those
    /// decided before the first day. The cached days among them are written
    /// again, recomputed. With no day missing, nothing is replayed.
    /// Returns the missing days, replayed.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be read or written, or as
    /// `replay` does.
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
        let to = start_of(last.tomorrow()?)? + self.target.settles_within();
        let rows = replay(from - self.warmup, to)?;
        self.write(&rows, from, to)?;
        Ok(missing)
    }

    /// The rows of the decisions in `[from, at)` settled at `at`, in time
    /// order, as a train at `at` sees them: the days the dataset lacks are
    /// replayed, once, by `replay` from the features' warmup before the
    /// first one's start to `at`, no further, and the days it settled
    /// written (see `write`), the rows before the first not; the rows of
    /// the days still unsettled, the last before `at`, are taken from the
    /// replay alone. A row whose positions close after `at` is dropped.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be read or written, or as
    /// `replay` does.
    pub fn settled(
        &self,
        from: Timestamp,
        at: Timestamp,
        replay: impl FnOnce(Timestamp, Timestamp) -> anyhow::Result<Vec<TargetRow>>,
    ) -> anyhow::Result<Vec<DatasetRow>> {
        let (first, last) = days(from, at);
        let mut rows = vec![];
        if let Some(&day) = self.missing(first, last)?.first() {
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
        Ok(rows)
    }

    /// The fingerprint the rows are stored under.
    #[must_use]
    pub const fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }

    /// The days `first` to `last` the catalog does not cover, from its
    /// missing intervals: a day partly covered is missing.
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

    /// Writes the `rows` of a replay over `[from, to]` for the days it
    /// settled, those it replayed whole and whose end plus the target's
    /// limit is not after `to`, and the manifest. What the catalog holds
    /// over those days is deleted first; the rows of other days are
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
        // Each file: its first and last day and its rows, a day with rows
        // and the days without rows after it, the first also those before.
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
    /// Returns an error if the catalog cannot be read, or a
    /// `SchemaMismatch` if it holds rows of other feature or payoff names or
    /// widths.
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
                    bail!("the dataset {identifier} holds {data:?}");
                };
                if custom.data_type.metadata() != want.metadata() {
                    bail!(SchemaMismatch(format!(
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
                    bail!(SchemaMismatch(format!(
                        "the dataset {identifier} holds a row of {} features, not {}",
                        row.features.len(),
                        self.feature_names.len()
                    )));
                }
                for payoff in [&row.long_payoff, &row.short_payoff] {
                    if payoff.len() != self.payoff_names.len() {
                        bail!(SchemaMismatch(format!(
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
    /// Returns an error if the dataset was never written, or its manifest
    /// cannot be read.
    pub fn manifest(&self) -> anyhow::Result<Manifest> {
        let path = self.directory()?.join(MANIFEST);
        let json = fs::read_to_string(&path)
            .with_context(|| format!("reading the manifest {}", path.display()))?;
        serde_json::from_str(&json)
            .with_context(|| format!("reading the manifest {}", path.display()))
    }

    /// The days a replay over `[from, to]` settles: those it replayed
    /// whole, its end at least the target's limit past theirs.
    fn settled_days(&self, from: Timestamp, to: Timestamp) -> anyhow::Result<Vec<Date>> {
        let first = day_of(from.into());
        let mut day = if start_of(first)? == from {
            first
        } else {
            first.tomorrow()?
        };
        let mut days = vec![];
        while start_of(day.tomorrow()?)? + self.target.settles_within() <= to {
            days.push(day);
            day = day.tomorrow()?;
        }
        Ok(days)
    }

    /// Writes the manifest, whole: beside the last, then renamed over it.
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

    /// The directory of the rows' files, where the manifest is too: the
    /// catalog gives its path from the catalog's root.
    fn directory(&self) -> anyhow::Result<PathBuf> {
        let directory = self
            .catalog()
            .make_path_custom_data(DatasetRow::type_name_static(), Some(&self.identifier()))?;
        Ok(self.catalog.join(directory))
    }

    /// The identifier the rows are stored under: the strategy's id and the
    /// fingerprint.
    fn identifier(&self) -> String {
        format!("{}-{}", self.strategy_id, self.fingerprint)
    }

    /// The data type of the rows, the feature and payoff names in its
    /// metadata.
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
            long_unit: long.unit,
            short_payoff: row.payoff.short.clone(),
            short_pnl: short.pnl.as_f64(),
            short_commission: short.commission.as_f64(),
            short_exit: short.exit.value(),
            short_unit: short.unit,
            settled_at: row.settled_at,
            ts_event: row.decision.ts_event,
            ts_init: row.decision.ts_init,
        }
    }

    /// `side`'s trade: its net P&L and its commission.
    #[must_use]
    pub const fn trade(&self, side: Side) -> Trade {
        match side {
            Side::Long => Trade {
                pnl: self.long_pnl,
                commission: self.long_commission,
            },
            Side::Short => Trade {
                pnl: self.short_pnl,
                commission: self.short_commission,
            },
        }
    }

    /// `side`'s row, the row its model reads, and its unit
    /// (`deciding::SideRow`).
    #[must_use]
    pub fn side(&self, side: Side) -> SideRow {
        match side {
            Side::Long => SideRow::new(&self.features, &self.long_payoff, self.long_unit),
            Side::Short => SideRow::new(&self.features, &self.short_payoff, self.short_unit),
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

/// The fingerprint of a dataset of `features` deciding on `rule`, of
/// `target` for a strategy of `requirements`, run on `venue`: the
/// decisions' (`decide::fingerprint`), deciding the bar build delay of the
/// requirements' bar types after the grid (`warmup::bar_build_delay`),
/// extended by the target's (`targets::fingerprint`), the venue's settings
/// and the dataset `VERSION`. A model's blueprint computes it, and nothing
/// else (`ModelBlueprint::fingerprint`).
///
/// # Errors
///
/// Returns an error as `targets::fingerprint` does.
pub(super) fn fingerprint(
    features: &FeatureSet,
    rule: DecisionRule,
    target: &TargetConfig,
    requirements: &DataRequirements,
    venue: &DatasetVenue,
) -> anyhow::Result<Fingerprint> {
    versioned_fingerprint(VERSION, features, rule, target, requirements, venue)
}

/// `fingerprint` at the dataset version `version`.
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

/// The catalog type of the rows.
fn row_type() -> NautilusDataType {
    NautilusDataType::Custom {
        type_name: DatasetRow::type_name_static().to_string(),
    }
}

/// The UTC days of `[from, to)`: the first and the last, the day of the
/// last nanosecond before `to`.
#[must_use]
pub fn days(from: Timestamp, to: Timestamp) -> (Date, Date) {
    (
        day_of(from.into()),
        day_of((to - SignedDuration::from_nanos(1)).into()),
    )
}

/// The UTC day of `time`.
fn day_of(time: UnixNanos) -> Date {
    Timestamp::from(time).to_zoned(TimeZone::UTC).date()
}

/// When `day` starts, in UTC.
fn start_of(day: Date) -> anyhow::Result<Timestamp> {
    Ok(day.to_zoned(TimeZone::UTC)?.timestamp())
}

/// The last nanosecond of `day`, in UTC.
fn end_of(day: Date) -> anyhow::Result<UnixNanos> {
    Ok(UnixNanos::from(
        start_of(day.tomorrow()?)? - SignedDuration::from_nanos(1),
    ))
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use nautilus_model::{identifiers::InstrumentId, types::Quantity};

    use super::*;
    use crate::{
        model::{
            config::{Margins, ModelConfig},
            decide::FeatureRow,
            learner::LearnerConfig,
            targets::{BracketSpec, Distance, Exit, Outcome, Outcomes, Target},
        },
        strategy::{Declared, EmaArgs, IndicatorsConfig, r::R},
    };

    const GOLD: &str = "XAUUSD.ICMARKETS";
    const STRATEGY: &str = "DATASET-001";

    /// Nautilus' exponential averages of gold's mids, of 1, 2 and 3,
    /// `[indicators.mid_<i>]`.
    const MIDS: [Declared<EmaArgs>; 3] = [
        Declared::new("mid_0"),
        Declared::new("mid_1"),
        Declared::new("mid_2"),
    ];

    /// The tables of `MIDS`, as `[indicators]` holds them.
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

    /// A feature set of `width` features on gold's quotes, `mid_0` and on.
    fn features(width: usize) -> std::rc::Rc<FeatureSet> {
        let tables = tables();
        MIDS[..width]
            .iter()
            .fold(FeatureSet::builder(), |builder, &mid| {
                builder.quotes(InstrumentId::from(GOLD), mid, &tables)
            })
            .build()
            .unwrap()
    }

    /// A bracket with a limit of 2 hours.
    fn target() -> TargetConfig {
        TargetConfig::builder()
            .instrument_id(InstrumentId::from(GOLD))
            .quantity(Quantity::from(1))
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

    /// What the strategy reads: the quotes of gold.
    fn requirements() -> DataRequirements {
        DataRequirements {
            bar_types: vec![],
            ticks: vec![crate::strategy::TickSeries {
                data: nautilus_model::data::NautilusDataType::QuoteTick,
                instrument_id: InstrumentId::from(GOLD),
            }],
            warmup: SignedDuration::ZERO,
        }
    }

    /// The venue of the dataset runs: a commission rate of 30 per million,
    /// 10 million USD, no leverage.
    fn venue() -> DatasetVenue {
        DatasetVenue {
            commission: Commission::Rate(Decimal::new(3, 5)),
            balance: Money::from("10000000 USD"),
            leverage: Decimal::ONE,
        }
    }

    /// The model of `strategy` deciding with `features` on `rule`, with
    /// `target`, for a strategy of `requirements`; its training settings,
    /// which no dataset reads, the fake learner's on a day.
    fn blueprint(
        strategy: &str,
        features: Rc<FeatureSet>,
        rule: DecisionRule,
        target: TargetConfig,
        requirements: DataRequirements,
    ) -> ModelBlueprint {
        let config = ModelConfig::builder()
            .decision(rule)
            .target(target)
            .window(SignedDuration::from_hours(24))
            .holdout(SignedDuration::from_hours(6))
            .margins(Margins {
                long: Some(R::new(0.0)),
                short: Some(R::new(0.0)),
            })
            .min_pnl(0.0)
            .min_trades(1)
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
        .dataset(catalog, &venue())
        .unwrap()
    }

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    /// The payoff features of each side of the rows of `row`: the take
    /// profit's R, the stop's nominal loss and the entry cost.
    fn payoff() -> PerSide<Vec<f64>> {
        PerSide {
            long: vec![1.4, 1.1, 0.07],
            short: vec![1.3, 1.2, 0.08],
        }
    }

    /// The row of the decision at `time`, settled an hour later: its
    /// features, a long winning `pnl` at its take profit and a short losing
    /// as much at its stop, each stop worth 2, each side's payoff features
    /// `payoff`'s.
    fn row(time: &str, features: Vec<f64>, pnl: &str) -> TargetRow {
        let ns = UnixNanos::from(at(time));
        let loss = format!("-{pnl}");
        let outcome = |pnl: &str, exit| Outcome {
            pnl: Money::from(&*format!("{pnl} USD")),
            commission: Money::from("0.1 USD"),
            exit,
            unit: 2.0,
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

    /// The row `row` stores.
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

    /// Whether `got` equals `want` bit for bit, NaN included.
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

    /// A row of a long at its take profit and a short at its time, worked by
    /// hand: every value of a side under that side's names, whatever the
    /// target, each side's its own.
    #[test]
    fn a_row_stores_each_sides_payoff_pnl_commission_exit_and_unit_under_its_names() {
        let mut settled = row("2026-01-05T09:00:00Z", vec![1.5], "2.5");
        settled.outcomes.short = Outcome {
            pnl: Money::from("-3.5 USD"),
            commission: Money::from("0.12 USD"),
            exit: Exit::Time,
            unit: 4.5,
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

    /// A long gaining 0.25 net of 0.75 and a short at nought: each answers
    /// its own trade, and its row, the decision's features then its own
    /// payoff features, and unit.
    #[test]
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
                commission: 0.75
            }
        );
        assert_eq!(
            row.trade(Side::Short),
            Trade {
                pnl: 0.0,
                commission: 0.5
            }
        );
        assert_eq!(
            row.sides(),
            PerSide {
                long: SideRow {
                    features: vec![4000.5, -2.0, 1.4, 1.1, 0.07],
                    unit: 3.0
                },
                short: SideRow {
                    features: vec![4000.5, -2.0, 1.3, 1.2, 0.08],
                    unit: 1.5
                },
            }
        );
    }

    /// The target's payoff names are three, a bracket's: a row of a payoff
    /// of one under them is refused as read.
    #[test]
    fn a_row_of_another_payoff_width_is_a_schema_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = dataset(dir.path(), STRATEGY, 1);
        let mut narrow = row("2026-01-05T09:00:00Z", vec![1.0], "1");
        narrow.payoff = PerSide {
            long: vec![0.1],
            short: vec![0.2],
        };
        let (from, to) = (at("2026-01-05T00:00:00Z"), at("2026-01-06T02:00:00Z"));
        dataset.write(&[narrow], from, to).unwrap();

        let err = dataset.read(from, to).unwrap_err();

        assert!(err.downcast_ref::<SchemaMismatch>().is_some(), "{err:#}");
    }

    #[test]
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

    #[test]
    fn every_day_is_missing_from_an_empty_catalog() {
        let dir = tempfile::tempdir().unwrap();

        assert_eq!(
            dataset(dir.path(), STRATEGY, 1)
                .missing(day("2026-01-05"), day("2026-01-07"))
                .unwrap(),
            [day("2026-01-05"), day("2026-01-06"), day("2026-01-07")]
        );
    }

    #[test]
    fn a_day_is_written_once_replay_has_passed_its_end_plus_its_longest_target() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = dataset(dir.path(), STRATEGY, 1);
        let rows = [
            row("2026-01-05T09:00:00Z", vec![1.0], "1"),
            row("2026-01-06T09:00:00Z", vec![2.0], "1"),
        ];

        // The target's limit is 2 hours.
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

    #[test]
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
    #[test]
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

    #[test]
    fn days_without_rows_are_covered_by_the_files_of_the_days_beside_them() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = dataset(dir.path(), STRATEGY, 1);
        // A Thursday without decisions, a Friday, a weekend, a Monday.
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
    #[test]
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

    #[test]
    fn a_change_of_the_features_the_decisions_the_target_or_the_commission_finds_an_empty_cache() {
        let dir = tempfile::tempdir().unwrap();
        let (from, to) = (at("2026-01-05T00:00:00Z"), at("2026-01-06T02:00:00Z"));
        dataset(dir.path(), STRATEGY, 1)
            .write(&[row("2026-01-05T09:00:00Z", vec![1.0], "1")], from, to)
            .unwrap();
        let changed = |rule, target, commission| {
            blueprint(STRATEGY, features(1), rule, target, requirements())
                .dataset(
                    dir.path(),
                    &DatasetVenue {
                        commission,
                        ..venue()
                    },
                )
                .unwrap()
        };
        let hourly = DecisionRule::Every(SignedDuration::from_hours(1));
        let changed = [
            dataset(dir.path(), STRATEGY, 2),
            changed(
                DecisionRule::Every(SignedDuration::from_mins(30)),
                target(),
                venue().commission,
            ),
            changed(
                hourly,
                TargetConfig {
                    spec: Target {
                        limit: SignedDuration::from_mins(30),
                        ..target().spec
                    },
                    ..target()
                },
                venue().commission,
            ),
            changed(hourly, target(), Commission::Rate(Decimal::new(4, 5))),
            changed(
                hourly,
                target(),
                Commission::PerOrder(Money::from_str_exact("0.00003 USD").unwrap()),
            ),
            changed(
                hourly,
                target(),
                Commission::PerQuantity(Money::from_str_exact("0.00003 USD").unwrap()),
            ),
        ];

        assert!(
            dataset(dir.path(), STRATEGY, 1)
                .missing(day("2026-01-05"), day("2026-01-05"))
                .unwrap()
                .is_empty()
        );
        for dataset in changed {
            assert_eq!(
                dataset
                    .missing(day("2026-01-05"), day("2026-01-05"))
                    .unwrap(),
                [day("2026-01-05")],
                "{dataset:?}"
            );
            assert!(dataset.read(from, to).unwrap().is_empty(), "{dataset:?}");
        }
    }
    #[test]
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

    /// Bumping the dataset version is a new fingerprint, so every dataset
    /// and model is rebuilt.
    #[test]
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
            .fingerprint(&venue())
            .unwrap();

        assert_ne!(current, at(VERSION + 1));
    }

    /// The identifier the rows of a fixed strategy are stored under, as a
    /// literal: its id, then the fingerprint of its features, its hourly
    /// decisions, its bracket, what it reads and the venue of its dataset
    /// runs. A change that rekeyed every dataset, leaving its rows and the
    /// models trained on them behind, fails here. A deliberate rekey, a
    /// bump of `VERSION` or a new input to the fingerprint, updates the
    /// literal here, and the pinned directory of a store, which the
    /// fingerprint keys too
    /// (`store::tests::a_fixed_strategys_side_store_keeps_its_directory`).
    #[test]
    fn a_fixed_strategys_dataset_keeps_its_identifier() {
        let dataset = dataset(Path::new("catalog"), STRATEGY, 1);

        assert_eq!(
            dataset.identifier(),
            "DATASET-001-3d0e48528c359f4fdf8c9f3273cca169421759001ebfef577bacfa99940ea28d"
        );
    }

    /// A strategy reading a composite bar type decides 5 s after its grid
    /// (`warmup::bar_build_delay`), so its rows are another dataset than
    /// those of the same strategy with the delay ignored: the one reading
    /// gold's quotes alone, its requirements keying nothing else, the
    /// target having no ATR.
    #[test]
    fn a_strategy_on_composite_bars_is_keyed_by_its_build_delay() {
        let of = |requirements: &DataRequirements| {
            blueprint(
                STRATEGY,
                features(1),
                DecisionRule::Every(SignedDuration::from_hours(1)),
                target(),
                requirements.clone(),
            )
            .fingerprint(&venue())
            .unwrap()
        };
        let composite = DataRequirements {
            bar_types: vec![nautilus_model::data::BarType::from(
                "XAUUSD.ICMARKETS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
            )],
            ..requirements()
        };

        assert_ne!(of(&composite), of(&requirements()));
    }

    /// Nautilus' risk engine denies an order its account cannot margin, so
    /// a dataset run's balance and leverage can change its rows: another
    /// of either is another fingerprint, another spelling of one is not.
    #[test]
    fn another_balance_or_leverage_of_the_dataset_run_is_another_fingerprint() {
        let of = |venue: DatasetVenue| {
            blueprint(
                STRATEGY,
                features(1),
                DecisionRule::Every(SignedDuration::from_hours(1)),
                target(),
                requirements(),
            )
            .fingerprint(&venue)
            .unwrap()
        };

        assert_ne!(
            of(venue()),
            of(DatasetVenue {
                balance: Money::from("20000000 USD"),
                ..venue()
            })
        );
        assert_ne!(
            of(venue()),
            of(DatasetVenue {
                leverage: Decimal::from(10),
                ..venue()
            })
        );
        assert_eq!(
            of(venue()),
            of(DatasetVenue {
                balance: Money::from("10000000.00 USD"),
                leverage: Decimal::new(100, 2),
                ..venue()
            })
        );
    }

    /// The text of the venue settings keys every dataset and model store
    /// built: a commission reads as its kind and its amount without
    /// trailing zeros, beside the balance and the leverage.
    #[test]
    fn a_dataset_runs_venue_is_keyed_by_its_commissions_kind_and_amount() {
        let of = |commission| {
            DatasetVenue {
                commission,
                ..venue()
            }
            .fingerprint_part()
        };

        assert_eq!(
            of(Commission::Rate(Decimal::new(30, 6))),
            "commission rate 0.00003, balance 10000000 USD, leverage 1"
        );
        assert_eq!(
            of(Commission::PerOrder(Money::from("3.00 USD"))),
            "commission per order 3 USD, balance 10000000 USD, leverage 1"
        );
        assert_eq!(
            of(Commission::PerQuantity(
                Money::from_str_exact("0.0350 USD").unwrap()
            )),
            "commission per quantity 0.035 USD, balance 10000000 USD, leverage 1"
        );
    }

    #[test]
    fn a_dataset_never_written_has_no_manifest() {
        let dir = tempfile::tempdir().unwrap();

        assert!(dataset(dir.path(), STRATEGY, 1).manifest().is_err());
    }
    #[test]
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

    /// The dataset of one feature declaring a warmup of 2 hours.
    fn warmed(catalog: &Path) -> Dataset {
        let features = FeatureSet::builder()
            .quotes(InstrumentId::from(GOLD), MIDS[0], &tables())
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
        .dataset(catalog, &venue())
        .unwrap()
    }

    #[test]
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

    #[test]
    fn a_train_replays_the_features_warmup_before_the_first_missing_day() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = warmed(dir.path());
        let rows = [
            row("2026-01-04T23:00:00Z", vec![1.0], "1"),
            row("2026-01-05T09:00:00Z", vec![2.0], "1"),
        ];
        let mut replays = vec![];

        let settled = dataset
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

    #[test]
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

    #[test]
    fn rows_of_another_width_than_the_names_are_a_schema_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = dataset(dir.path(), STRATEGY, 1);
        let (from, to) = (at("2026-01-05T00:00:00Z"), at("2026-01-06T02:00:00Z"));
        dataset
            .write(
                &[row("2026-01-05T09:00:00Z", vec![1.0, 2.0], "1")],
                from,
                to,
            )
            .unwrap();

        let err = dataset.read(from, to).unwrap_err();

        assert!(err.downcast_ref::<SchemaMismatch>().is_some(), "{err:#}");
    }

    #[test]
    fn the_settled_rows_are_those_of_the_span_settled_at_its_end() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = dataset(dir.path(), STRATEGY, 1);
        // Each row settles an hour after its decision.
        let rows = [
            row("2026-01-05T08:00:00Z", vec![1.0], "1"),
            row("2026-01-05T09:00:00Z", vec![2.0], "1"),
            row("2026-01-07T08:30:00Z", vec![3.0], "1"),
            row("2026-01-07T09:30:00Z", vec![4.0], "1"),
        ];
        let mut replays = vec![];

        let settled = dataset
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
        assert_bits_eq(
            &settled,
            &[
                stored("2026-01-05T09:00:00Z", vec![2.0], 1.0),
                stored("2026-01-07T08:30:00Z", vec![3.0], 1.0),
            ],
        );
    }

    #[test]
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

        // The target's limit, 2 hours, settles the 6th by 02:00 of the 7th.
        let missing = dataset
            .missing(day("2026-01-05"), day("2026-01-07"))
            .unwrap();
        let settled = dataset
            .settled(from, end, |from, to| {
                replays.push((from, to));
                Ok(rows[2..].to_vec())
            })
            .unwrap();

        assert_eq!(missing, [day("2026-01-07")]);
        assert_eq!(replays, [(at("2026-01-07T00:00:00Z"), end)]);
        assert_bits_eq(
            &settled,
            &[
                stored("2026-01-05T09:00:00Z", vec![1.0], 1.0),
                stored("2026-01-06T09:00:00Z", vec![2.0], 1.0),
                stored("2026-01-07T08:00:00Z", vec![3.0], 1.0),
            ],
        );
    }

    #[test]
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

        let settled = dataset
            .settled(
                at("2026-01-05T00:00:00Z"),
                at("2026-01-06T00:00:00Z"),
                |_, _| panic!("a cached span replayed"),
            )
            .unwrap();

        assert_bits_eq(&settled, &[stored("2026-01-05T09:00:00Z", vec![1.0], 1.0)]);
    }
}
