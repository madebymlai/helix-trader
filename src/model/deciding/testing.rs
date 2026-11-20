//! The store and models of the decision tests: a strategy on the last mid of AUD/USD, baselines
//! predicting a known R whatever the row, and IC's gold.

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

/// XAUUSD as IC defines it (`crates/ctrader/README.md`, "Instruments and account"): whole ounces
/// quoted in USD on ticks of a cent.
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

/// The level line `0.1 + 0.5 E` of every meta model: a meta predicting 0.3 expects 0.25, one
/// predicting 0 expects 0.1.
pub(crate) const CALIBRATION: Line = Line {
    alpha: 0.1,
    beta: 0.5,
};

/// The R a baseline of `tenths` predicts: `(tenths - 5) / 10`.
pub(crate) fn r(tenths: usize) -> f64 {
    (tenths as f64 - 5.0) / 10.0
}

/// The pair calibration `C' = C`, and for two sides `D' = D`, up to rounding.
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

/// Hourly decisions on a time-only target of 1 hour of AUD/USD at a risk of 0.5%, its unit the
/// ATR of one hourly bar of the mids, the long traded from 0 R, the baseline learner.
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

/// `config`, the short traded too, from 0 R.
pub(crate) fn both_sides() -> ModelConfig {
    ModelConfig {
        margins: Margins {
            long: Some(R::new(0.0)),
            short: Some(R::new(0.0)),
        },
        ..config()
    }
}

/// A meta taking a trade from an expected net R of 0.15.
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

/// A commission rate of 30 per million, 10 million USD.
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

/// The store of `config`, its long alone traded.
pub(crate) fn store(models: &Path) -> ModelStore {
    model_store(models, &features(), &config())
}

/// A model set valid from `valid_from`, its long a baseline predicting `r(tenths)`.
pub(crate) fn baseline(valid_from: &str, tenths: usize) -> StoredModelSet {
    model_set_of(
        &features(),
        &config(),
        valid_from,
        [(Side::Long, tenths)],
        None,
    )
}

/// A model set valid from `valid_from`: a baseline per side of `sides` (`baseline_for`), the pair
/// calibrated by `unchanged`, and `meta`, if any.
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

/// A baseline of `config`'s `side` predicting `r(tenths)` whatever the row, probed on one row.
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

/// A baseline meta model predicting `r(tenths)` whatever the row, calibrated by `CALIBRATION`,
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
