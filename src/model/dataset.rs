//! The training table of a strategy, stored by day in the Nautilus catalog,
//! whose coverage is the day cache.
//!
//! A dataset is the settled rows of one strategy (`TargetRow`, from a
//! dataset run), each its decision's features and the values of its
//! targets, stored as Nautilus custom data, `DatasetRow`, one type for
//! every strategy. Features and targets are list columns, so each strategy
//! has the width of its own, and its rows are stored under an identifier
//! of its id and its fingerprint (`decide::fingerprint` extended by
//! `targets::fingerprint` and the commission of its dataset runs,
//! `Commission`): a change to the feature code, names or warmup, the
//! decision rule, a target or the commission finds no row, and the dataset
//! is rebuilt.
//! The feature names are in the metadata of the rows' data type, which
//! the catalog stores with each file and gives back with each row, so the
//! rows do not repeat them; `read` checks them.
//!
//! The catalog's intervals of the rows are the cache: `missing` asks the
//! catalog for the days of a span it does not cover, and `build` replays
//! them, once, from the first missing day to the last plus the longest
//! target (`TargetsConfig::settles_within`), then writes them. The replay
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
//! strategy, fingerprint, feature names in order, target spec and names,
//! and the days it holds, rewritten whole (write, then rename) at each
//! write.

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
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_serialization::arrow_custom_data;
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{
    decide::{self, DecisionRule},
    features::{FeatureSet, Fingerprint},
    targets::{self, TargetRow, TargetsConfig},
};

/// The key of the feature names in the metadata of the rows' data type.
const FEATURE_NAMES: &str = "feature_names";

/// The name of the manifest file in the directory of a dataset's rows.
const MANIFEST: &str = "manifest.json";

/// A settled decision as the catalog stores it.
#[arrow_custom_data]
#[custom_data]
pub struct DatasetRow {
    /// The value of each feature, in the order of the dataset's names.
    pub features: Vec<f64>,
    /// The value of each target, in the order of the dataset's target
    /// names (`TargetsConfig::target_names`).
    pub targets: Vec<f64>,
    /// When the last of the decision's positions closed.
    pub settled_at: UnixNanos,
    /// The time of the decision.
    pub ts_event: UnixNanos,
    /// The time of the decision too, which files the row in its day.
    pub ts_init: UnixNanos,
}

/// The error of a dataset holding rows that do not match its schema: other
/// feature names, or a row of another width than its feature or target
/// names.
#[derive(Debug)]
pub struct SchemaMismatch(String);

impl fmt::Display for SchemaMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SchemaMismatch {}

/// What a dataset holds, enough to explain and rebuild it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub strategy_id: StrategyId,
    /// The fingerprint of the features, the decision rule, the targets and
    /// the commission.
    pub fingerprint: String,
    /// The names of the features, in the order of a row's values.
    pub feature_names: Vec<String>,
    /// The names of the targets, in the order of a row's values.
    pub target_names: Vec<String>,
    /// The target spec.
    pub targets: TargetsConfig,
    /// The days stored, in runs of consecutive days.
    pub days: Vec<Days>,
}

/// Consecutive UTC days, the first to the last.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Days {
    pub first: Date,
    pub last: Date,
}

/// The commission of a dataset run, which its targets are net of, as given
/// on the command line: per order (`--commission 3 USD`) or a rate on the
/// value of each fill (`--commission-rate 0.00003`). Part of the dataset's
/// fingerprint, so neither rows nor models of other costs are ever read;
/// the run's balance and leverage, which deny no target of a dataset run,
/// are not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Commission {
    PerOrder(String),
    Rate(String),
}

impl Commission {
    /// The commission as the fingerprint reads it.
    fn fingerprint_part(&self) -> String {
        match self {
            Self::PerOrder(commission) => format!("commission per order {commission}"),
            Self::Rate(rate) => format!("commission rate {rate}"),
        }
    }
}

/// The dataset of one strategy in a catalog.
#[derive(Debug, Clone)]
pub struct Dataset {
    catalog: PathBuf,
    strategy_id: StrategyId,
    fingerprint: Fingerprint,
    feature_names: Vec<String>,
    /// How much a replay runs before its first day: the features' warmup.
    warmup: SignedDuration,
    targets: TargetsConfig,
}

impl Dataset {
    /// The dataset of `strategy_id`, deciding with `features` on `rule`,
    /// of `targets` net of `commission`, in the catalog at `catalog`. Registers `DatasetRow`
    /// with Nautilus, as any reader of the catalog must.
    ///
    /// # Errors
    ///
    /// Returns an error if `targets` is invalid.
    pub fn new(
        catalog: &Path,
        strategy_id: StrategyId,
        features: &FeatureSet,
        rule: DecisionRule,
        targets: TargetsConfig,
        commission: &Commission,
    ) -> anyhow::Result<Self> {
        targets.validate()?;
        nautilus_serialization::ensure_custom_data_registered::<DatasetRow>();
        Ok(Self {
            catalog: catalog.to_path_buf(),
            strategy_id,
            fingerprint: fingerprint(features, rule, &targets, commission),
            warmup: features.warmup(),
            feature_names: features.names().to_vec(),
            targets,
        })
    }

    /// Builds the days `first` to `last` the dataset is missing: `replay`
    /// runs the strategy from the features' warmup before the first missing
    /// day's start to the last's end plus the longest target and returns
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
        let to = start_of(last.tomorrow()?)? + self.targets.settles_within();
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
    /// settled, those it replayed whole and whose end plus the longest
    /// target is not after `to`, and the manifest. What the catalog holds
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
    /// `SchemaMismatch` if it holds rows of other feature names or widths.
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
                        "the dataset {identifier} holds rows of the features {:?}, not {:?}",
                        custom.data_type.metadata(),
                        self.feature_names
                    )));
                }
                let row = custom
                    .data
                    .as_any()
                    .downcast_ref::<DatasetRow>()
                    .cloned()
                    .with_context(|| format!("the dataset {identifier} holds {custom:?}"))?;
                let targets = self.targets.target_names().len();
                if row.features.len() != self.feature_names.len() || row.targets.len() != targets {
                    bail!(SchemaMismatch(format!(
                        "the dataset {identifier} holds a row of {} features and {} targets, \
                         not {} and {targets}",
                        row.features.len(),
                        row.targets.len(),
                        self.feature_names.len()
                    )));
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
    /// whole, its end at least the longest target past theirs.
    fn settled_days(&self, from: Timestamp, to: Timestamp) -> anyhow::Result<Vec<Date>> {
        let first = day_of(from.into());
        let mut day = if start_of(first)? == from {
            first
        } else {
            first.tomorrow()?
        };
        let mut days = vec![];
        while start_of(day.tomorrow()?)? + self.targets.settles_within() <= to {
            days.push(day);
            day = day.tomorrow()?;
        }
        Ok(days)
    }

    /// Writes the manifest of what the catalog holds, whole: beside the
    /// last, then renamed over it.
    fn write_manifest(&self) -> anyhow::Result<()> {
        let identifier = self.identifier();
        let intervals = self
            .catalog()
            .get_intervals(&row_type().into(), Some(&identifier))
            .with_context(|| format!("reading the days of the dataset {identifier}"))?;
        let mut days: Vec<Days> = vec![];
        for (start, end) in intervals {
            let (first, last) = (day_of(start.into()), day_of(end.into()));
            match days.last_mut() {
                Some(days) if days.last.tomorrow()? == first => days.last = last,
                _ => days.push(Days { first, last }),
            }
        }
        let manifest = Manifest {
            strategy_id: self.strategy_id,
            fingerprint: self.fingerprint.to_string(),
            feature_names: self.feature_names.clone(),
            target_names: self.targets.target_names(),
            targets: self.targets.clone(),
            days,
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

    /// The data type of the rows, the feature names in its metadata.
    fn data_type(&self) -> anyhow::Result<DataType> {
        let metadata = serde_json::from_value(json!({ FEATURE_NAMES: self.feature_names }))?;
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
    fn of(row: &TargetRow) -> Self {
        Self {
            features: row.decision.features.clone(),
            targets: row.values(),
            settled_at: row.settled_at,
            ts_event: row.decision.ts_event,
            ts_init: row.decision.ts_init,
        }
    }
}

/// The fingerprint of a dataset of `features` deciding on `rule`, of
/// `targets` net of `commission`: the decisions' (`decide::fingerprint`)
/// extended by the targets' (`targets::fingerprint`) and the commission.
#[must_use]
pub fn fingerprint(
    features: &FeatureSet,
    rule: DecisionRule,
    targets: &TargetsConfig,
    commission: &Commission,
) -> Fingerprint {
    targets::fingerprint(decide::fingerprint(features, rule), targets)
        .with(commission.fingerprint_part().as_bytes())
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
    use nautilus_indicators::average::sma::SimpleMovingAverage;
    use nautilus_model::{
        enums::PriceType,
        identifiers::InstrumentId,
        types::{Money, Quantity},
    };
    use rust_decimal::Decimal;

    use super::*;
    use crate::model::{
        decide::FeatureRow,
        targets::{Barrier, Distance, Exit, Outcome, Outcomes},
    };

    const GOLD: &str = "XAUUSD.ICMARKETS";
    const STRATEGY: &str = "DATASET-001";

    /// A feature set of `width` features on gold's quotes.
    fn features(width: usize) -> std::rc::Rc<FeatureSet> {
        (0..width)
            .fold(FeatureSet::builder(), |builder, i| {
                builder.quotes(
                    InstrumentId::from(GOLD),
                    &format!("mid_{i}"),
                    SimpleMovingAverage::new(i + 1, Some(PriceType::Mid)),
                )
            })
            .build()
            .unwrap()
    }

    /// A barrier with a limit of 2 hours and a horizon of 1 hour.
    fn targets() -> TargetsConfig {
        TargetsConfig::builder()
            .instrument_id(InstrumentId::from(GOLD))
            .quantity(Quantity::from(1))
            .barrier(Barrier {
                take_profit: Distance::BasisPoints(Decimal::new(75, 1)),
                stop: Distance::BasisPoints(Decimal::from(5)),
                limit: SignedDuration::from_hours(2),
                atr: None,
            })
            .horizon(SignedDuration::from_hours(1))
            .build()
    }

    /// A commission rate of 30 per million.
    fn commission() -> Commission {
        Commission::Rate("0.00003".to_string())
    }

    fn dataset(catalog: &Path, strategy: &str, width: usize) -> Dataset {
        Dataset::new(
            catalog,
            StrategyId::from(strategy),
            &features(width),
            DecisionRule::Every(SignedDuration::from_hours(1)),
            targets(),
            &commission(),
        )
        .unwrap()
    }

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    /// The row of the decision at `time`, settled an hour later: its
    /// features, a long winning `pnl` at its take profit and a short losing
    /// as much at its stop on the barrier, the reverse at the horizon.
    fn row(time: &str, features: Vec<f64>, pnl: &str) -> TargetRow {
        let ns = UnixNanos::from(at(time));
        let loss = format!("-{pnl}");
        let outcome = |pnl: &str, exit| Outcome {
            pnl: Money::from(&*format!("{pnl} USD")),
            exit,
        };
        TargetRow {
            decision: FeatureRow {
                strategy_id: StrategyId::from(STRATEGY),
                features,
                ts_event: ns,
                ts_init: ns,
            },
            barrier: Some(Outcomes {
                long: outcome(pnl, Exit::TakeProfit),
                short: outcome(&loss, Exit::Stop),
            }),
            horizon: Some(Outcomes {
                long: outcome(&loss, Exit::Time),
                short: outcome(pnl, Exit::Time),
            }),
            settled_at: UnixNanos::from(at(time) + SignedDuration::from_hours(1)),
        }
    }

    /// The row `row` stores.
    fn stored(time: &str, features: Vec<f64>, pnl: f64) -> DatasetRow {
        let ns = UnixNanos::from(at(time));
        DatasetRow {
            features,
            targets: vec![pnl, 1.0, -pnl, -1.0, -pnl, 0.0, pnl, 0.0],
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
                        values(&row.targets),
                        row.settled_at,
                        row.ts_event,
                        row.ts_init,
                    )
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(bits(got), bits(want), "{got:#?}");
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
    fn a_day_is_written_once_replay_has_passed_its_end_plus_the_horizon() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = dataset(dir.path(), STRATEGY, 1);
        let rows = [
            row("2026-01-05T09:00:00Z", vec![1.0], "1"),
            row("2026-01-06T09:00:00Z", vec![2.0], "1"),
        ];

        // The barrier's limit, 2 hours, is the longest target.
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
    fn a_change_of_the_features_the_decisions_the_targets_or_the_commission_finds_an_empty_cache() {
        let dir = tempfile::tempdir().unwrap();
        let (from, to) = (at("2026-01-05T00:00:00Z"), at("2026-01-06T02:00:00Z"));
        dataset(dir.path(), STRATEGY, 1)
            .write(&[row("2026-01-05T09:00:00Z", vec![1.0], "1")], from, to)
            .unwrap();
        let changed = |rule, targets, commission| {
            Dataset::new(
                dir.path(),
                StrategyId::from(STRATEGY),
                &features(1),
                rule,
                targets,
                &commission,
            )
            .unwrap()
        };
        let hourly = DecisionRule::Every(SignedDuration::from_hours(1));
        let changed = [
            dataset(dir.path(), STRATEGY, 2),
            changed(
                DecisionRule::Every(SignedDuration::from_mins(30)),
                targets(),
                commission(),
            ),
            changed(
                hourly,
                TargetsConfig {
                    horizon: Some(SignedDuration::from_mins(30)),
                    ..targets()
                },
                commission(),
            ),
            changed(hourly, targets(), Commission::Rate("0.00004".to_string())),
            changed(
                hourly,
                targets(),
                Commission::PerOrder("0.00003 USD".to_string()),
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
    fn the_manifest_names_the_features_the_targets_the_days_and_the_fingerprint() {
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
        assert_eq!(manifest.targets, targets());
        assert_eq!(
            manifest.target_names,
            [
                "barrier_long_pnl",
                "barrier_long_exit",
                "barrier_short_pnl",
                "barrier_short_exit",
                "horizon_long_pnl",
                "horizon_long_exit",
                "horizon_short_pnl",
                "horizon_short_exit",
            ]
        );
        assert_eq!(
            manifest.days,
            [
                Days {
                    first: day("2026-01-05"),
                    last: day("2026-01-06")
                },
                Days {
                    first: day("2026-01-08"),
                    last: day("2026-01-08")
                },
            ]
        );
        assert_eq!(manifest.fingerprint, dataset.fingerprint().to_string());
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
            .quotes(
                InstrumentId::from(GOLD),
                "mid_0",
                SimpleMovingAverage::new(1, Some(PriceType::Mid)),
            )
            .warmup(SignedDuration::from_hours(2))
            .build()
            .unwrap();
        Dataset::new(
            catalog,
            StrategyId::from(STRATEGY),
            &features,
            DecisionRule::Every(SignedDuration::from_hours(1)),
            targets(),
            &commission(),
        )
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

        // The barrier's limit, 2 hours, settles the 6th by 02:00 of the 7th.
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
