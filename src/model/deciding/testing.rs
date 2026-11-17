//! The store and the models of the tests of a decision, the handle's among
//! them: a strategy on the last mid of AUD/USD, and baselines predicting a
//! known R, a side's or a meta's, whatever the row, published under a temp
//! dir; and IC's gold for a target traded on it.

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
        store::{ModelManifest, ModelStore, Probe, StoredGeneration, StoredModel},
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

/// XAUUSD as IC defines it (`crates/ctrader/README.md`, "Instruments and
/// account"): whole ounces quoted in USD on ticks of a cent.
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

/// The calibration of every meta model of the tests, its level line `0.1 +
/// 0.5 E`: a meta predicting an R of 0.3 expects 0.25, one predicting nought
/// 0.1.
pub(crate) const CALIBRATION: Line = Line {
    alpha: 0.1,
    beta: 0.5,
};

/// The R a side's baseline of `tenths` predicts: `tenths` less 5, over 10,
/// so above 5 a gain, below a loss and 5 nought.
pub(crate) fn r(tenths: usize) -> f64 {
    (tenths as f64 - 5.0) / 10.0
}

/// The calibration of the pair of `config`'s sides leaving each side's
/// prediction as it is, up to rounding: the level line `C' = C`, and of two
/// sides the side call `D' = D`.
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

/// The last mid of AUD/USD, warm once a quote came.
pub(crate) fn features() -> Rc<FeatureSet> {
    last_mid(InstrumentId::from(AUD)).build().unwrap()
}

/// Hourly decisions on a time-only target of 1 hour of AUD/USD at a risk
/// of 0.5%, its unit the worth of its ATR of one hourly bar of the mids on
/// its size at the reference stake, the long traded from an expected net R
/// of nought, the baseline learner.
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

/// `config`, the short traded too, from nought.
pub(crate) fn both_sides() -> ModelConfig {
    ModelConfig {
        margins: Margins {
            long: Some(R::new(0.0)),
            short: Some(R::new(0.0)),
        },
        ..config()
    }
}

/// The meta model taking a trade from an expected net R of 0.15.
pub(crate) fn meta() -> MetaConfig {
    MetaConfig {
        window: SignedDuration::from_hours(48),
        calibration: SignedDuration::from_hours(12),
        margin: R::new(0.15),
        min_rows: 1,
        learner: LearnerConfig::Baseline {},
    }
}

/// `config`, its trades taken by `meta`.
pub(crate) fn with_meta(meta: MetaConfig) -> ModelConfig {
    ModelConfig {
        meta: Some(meta),
        ..config()
    }
}

/// What the strategy reads: the quotes of AUD/USD.
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

/// The venue of the dataset runs: a commission rate of 30 per million,
/// 10 million USD.
pub(crate) fn venue() -> DatasetVenue {
    DatasetVenue {
        commission: Commission::Rate(Decimal::new(3, 5)),
        balance: Money::from("10000000 USD"),
        latency: None,
    }
}

/// The model of `features` and `config`, for a strategy of
/// `requirements`.
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

/// The models of `features` and `config` under `models`.
pub(crate) fn model_store(
    models: &Path,
    features: &Rc<FeatureSet>,
    config: &ModelConfig,
) -> ModelStore {
    blueprint_of(features, config, requirements())
        .model_store(models)
        .unwrap()
}

/// The models of `config` under `models`, its long alone traded.
pub(crate) fn store(models: &Path) -> ModelStore {
    model_store(models, &features(), &config())
}

/// A generation of `config` valid from `valid_from`, its long a baseline
/// predicting an R of `r(tenths)` whatever the row, as `baseline_for` says.
pub(crate) fn baseline(valid_from: &str, tenths: usize) -> StoredGeneration {
    generation_of(
        &features(),
        &config(),
        valid_from,
        [(Side::Long, tenths)],
        None,
    )
}

/// A generation of `config` valid from `valid_from`: each side of `sides`,
/// every side traded, the long first, a baseline predicting an R of
/// `r(tenths)` of its tenths (`baseline_for`), their pair's calibration
/// leaving them as they are (`unchanged`), and the meta model `meta`, if
/// any.
pub(crate) fn generation_of(
    features: &Rc<FeatureSet>,
    config: &ModelConfig,
    valid_from: &str,
    sides: impl IntoIterator<Item = (Side, usize)>,
    meta: Option<StoredModel>,
) -> StoredGeneration {
    StoredGeneration {
        valid_from: at(valid_from),
        sides: sides
            .into_iter()
            .map(|(side, tenths)| baseline_for(features, config, side, tenths))
            .collect(),
        calibration: unchanged(config),
        meta,
    }
}

/// A baseline model of `config`'s `side`, on the side's features of
/// `features` and the target's payoff, predicting an R of `r(tenths)`
/// whatever the row, its baseline that R, as the store keeps it, probed on
/// one row.
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
            // Its strategy reads the quotes of what it trades, which the
            // target's ATR, if any, is built from.
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
            probes: vec![Probe {
                prediction: model.predict(&probe).unwrap(),
                row: probe,
            }],
        },
        model: model.save().unwrap(),
    }
}

/// A baseline meta model of `config` predicting an R of `r(tenths)`
/// whatever the row, calibrated by `CALIBRATION`, on the meta's features of
/// `features`, its baseline that R, with no probe.
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
