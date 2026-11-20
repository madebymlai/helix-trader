//! Builds the store and models for the decision tests.
//!
//! The store has a strategy on the last mid of AUD/USD. The models are baselines that predict a
//! known R whatever the row, and IC's gold.

use std::{path::Path, rc::Rc};

use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::NautilusDataType,
    enums::AssetClass,
    identifiers::{InstrumentId, StrategyId, Symbol},
    instruments::{Cfd, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use rust_decimal::Decimal;

use crate::{
    backtest::Commission,
    model::{
        blueprint::ModelBlueprint,
        calibration::{Ksplit, Line},
        config::{Margins, MetaConfig, ModelConfig},
        dataset::DatasetVenue,
        decide::DecisionRule,
        features::{FeatureSet, meta_names, side_names, testing::last_mid},
        learner::{LearnerConfig, TrainingSet},
        store::{ModelManifest, ModelStore, Probe, StoredModel, StoredModelSet},
        targets::{DEFAULT_ATR, Side, Target, TargetConfig},
    },
    strategy::{
        AtrArgs,
        requirements::{DataRequirements, TickSeries},
        risk::R,
    },
};

pub(crate) const STRATEGY: &str = "HANDLE-001";
pub(crate) const AUD: &str = "AUD/USD.SIM";
pub(crate) const GOLD: &str = "XAUUSD.ICMARKETS";

/// Returns XAUUSD as IC defines it (`crates/ctrader/README.md`, "Instruments and account").
///
/// It trades whole ounces quoted in USD, on ticks of a cent.
pub(crate) fn gold() -> InstrumentAny {
    let margin = Decimal::ONE / Decimal::from(30);
    InstrumentAny::Cfd(
        Cfd::builder()
            .instrument_id(InstrumentId::from(GOLD))
            .raw_symbol(Symbol::from("XAUUSD"))
            .asset_class(AssetClass::Commodity)
            .base_currency(Currency::from("XAU"))
            .quote_currency(Currency::USD())
            .price_precision(2)
            .size_precision(0)
            .price_increment(Price::from("0.01"))
            .size_increment(Quantity::from(1))
            .lot_size(Quantity::from(100))
            .min_quantity(Quantity::from(1))
            .max_quantity(Quantity::from(10_000))
            .margin_init(margin)
            .margin_maint(margin)
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .unwrap(),
    )
}

/// The level line `0.1 + 0.5 E` of every meta model.
///
/// A meta predicting 0.3 expects 0.25, and one predicting 0 expects 0.1.
pub(crate) const CALIBRATION: Line = Line {
    alpha: 0.1,
    beta: 0.5,
};

/// The R a baseline of `tenths` predicts: `(tenths - 5) / 10`.
pub(crate) fn r(tenths: usize) -> f64 {
    (tenths as f64 - 5.0) / 10.0
}

/// The pair calibration, up to rounding: `C' = C`, and `D' = D` for two sides.
pub(crate) fn unchanged(config: &ModelConfig) -> Ksplit {
    Ksplit {
        level: Line {
            alpha: 0.0,
            beta: 1.0,
        },
        side_call: (config.margins.sides().count() == 2).then_some(1.0),
    }
}

pub(crate) fn at(time: &str) -> Timestamp {
    time.parse().unwrap()
}

pub(crate) fn features() -> Rc<FeatureSet> {
    last_mid(InstrumentId::from(AUD)).build().unwrap()
}

/// Configures hourly decisions on a time-only target.
///
/// - The target is 1 hour of AUD/USD at a risk of 0.5%.
/// - The unit is the ATR of one hourly bar of the mids.
/// - The long is traded from 0 R.
/// - The learner is the baseline.
pub(crate) fn config() -> ModelConfig {
    ModelConfig::builder()
        .decision(DecisionRule::Every(SignedDuration::from_hours(1)))
        .target(
            TargetConfig::builder()
                .instrument_id(InstrumentId::from(AUD))
                .risk("0.5%".parse().unwrap())
                .spec(
                    Target::builder()
                        .limit(SignedDuration::from_hours(1))
                        .atr(AtrArgs {
                            period: 1,
                            ..DEFAULT_ATR
                        })
                        .build(),
                )
                .build(),
        )
        .retrain(SignedDuration::from_hours(24))
        .dataset(venue())
        .window(SignedDuration::from_hours(48))
        .calibration(SignedDuration::from_hours(12))
        .margins(Margins {
            long: Some(R::new(0.0)),
            short: None,
        })
        .min_rows(1)
        .learner(LearnerConfig::Baseline {})
        .build()
}

/// Returns `config` with the short traded too, from 0 R.
pub(crate) fn both_sides() -> ModelConfig {
    ModelConfig {
        margins: Margins {
            long: Some(R::new(0.0)),
            short: Some(R::new(0.0)),
        },
        ..config()
    }
}

/// A meta that takes a trade from an expected net R of 0.15.
pub(crate) fn meta() -> MetaConfig {
    MetaConfig {
        window: SignedDuration::from_hours(48),
        calibration: SignedDuration::from_hours(12),
        margin: R::new(0.15),
        min_rows: 1,
        learner: LearnerConfig::Baseline {},
    }
}

pub(crate) fn with_meta(meta: MetaConfig) -> ModelConfig {
    ModelConfig {
        meta: Some(meta),
        ..config()
    }
}

pub(crate) fn requirements() -> DataRequirements {
    DataRequirements {
        bar_types: vec![],
        ticks: vec![TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: InstrumentId::from(AUD),
        }],
        custom: vec![],
        warmup: SignedDuration::ZERO,
    }
}

/// A commission rate of 30 per million and a balance of 10 million USD.
pub(crate) fn venue() -> DatasetVenue {
    DatasetVenue {
        commission: Commission::Rate(Decimal::new(3, 5)),
        balance: Money::from("10000000 USD"),
        latency: None,
    }
}

pub(crate) fn blueprint_of(
    features: &Rc<FeatureSet>,
    config: &ModelConfig,
    requirements: DataRequirements,
) -> ModelBlueprint {
    ModelBlueprint::new(
        StrategyId::from(STRATEGY),
        Rc::clone(features),
        config.clone(),
        requirements,
    )
    .unwrap()
}

pub(crate) fn model_store(
    models: &Path,
    features: &Rc<FeatureSet>,
    config: &ModelConfig,
) -> ModelStore {
    blueprint_of(features, config, requirements())
        .model_store(models)
        .unwrap()
}

/// The store of `config`, with only its long traded.
pub(crate) fn store(models: &Path) -> ModelStore {
    model_store(models, &features(), &config())
}

/// A model set valid from `valid_from`, with a baseline long that predicts `r(tenths)`.
pub(crate) fn baseline(valid_from: &str, tenths: usize) -> StoredModelSet {
    model_set_of(
        &features(),
        &config(),
        valid_from,
        [(Side::Long, tenths)],
        None,
    )
}

/// A model set valid from `valid_from`.
///
/// It holds a baseline per side of `sides` (`baseline_for`), the pair calibrated by `unchanged`,
/// and `meta`, if any.
pub(crate) fn model_set_of(
    features: &Rc<FeatureSet>,
    config: &ModelConfig,
    valid_from: &str,
    sides: impl IntoIterator<Item = (Side, usize)>,
    meta: Option<StoredModel>,
) -> StoredModelSet {
    StoredModelSet {
        valid_from: at(valid_from),
        sides: sides
            .into_iter()
            .map(|(side, tenths)| baseline_for(features, config, side, tenths))
            .collect(),
        calibration: unchanged(config),
        meta,
    }
}

/// A baseline for `config`'s `side` that predicts `r(tenths)` whatever the row, probed on one row.
pub(crate) fn baseline_for(
    features: &Rc<FeatureSet>,
    config: &ModelConfig,
    side: Side,
    tenths: usize,
) -> StoredModel {
    let names = side_names(features.names(), &config.target.payoff_names());
    let width = names.len();
    let set = TrainingSet::new(width, vec![0.5; width], vec![r(tenths)]).unwrap();
    let model = LearnerConfig::Baseline {}.fit(&set).unwrap();
    let probe = vec![0.7; width];
    let config = config.clone();
    StoredModel {
        manifest: ModelManifest {
            // The strategy reads the quotes of what it trades, which the target's ATR is built from
            fingerprint: blueprint_of(
                features,
                &config,
                DataRequirements {
                    ticks: vec![TickSeries {
                        data: NautilusDataType::QuoteTick,
                        instrument_id: config.target.instrument_id,
                    }],
                    ..requirements()
                },
            )
            .fingerprint()
            .unwrap()
            .to_string(),
            feature_names: names,
            calibration: None,
            target: config.target,
            side: Some(side),
            learner: config.learner,
            baseline: r(tenths),
            out_of_fold: None,
            probes: vec![Probe {
                prediction: model.predict(&probe).unwrap(),
                row: probe,
            }],
        },
        model: model.save().unwrap(),
    }
}

/// A baseline meta model that predicts `r(tenths)` whatever the row, calibrated by `CALIBRATION`,
/// with no probe.
pub(crate) fn meta_baseline(
    features: &Rc<FeatureSet>,
    config: &ModelConfig,
    tenths: usize,
) -> StoredModel {
    let names = meta_names(features.names());
    let set = TrainingSet::new(names.len(), vec![0.5; names.len()], vec![r(tenths)]).unwrap();
    let mut meta = baseline_for(features, config, Side::Long, tenths);
    meta.manifest.side = None;
    meta.manifest.feature_names = names;
    meta.manifest.calibration = Some(CALIBRATION);
    meta.manifest.probes.clear();
    meta.model = LearnerConfig::Baseline {}
        .fit(&set)
        .unwrap()
        .save()
        .unwrap();
    meta
}
