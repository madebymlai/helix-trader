//! The store and the models of the tests of a decision, the handle's among
//! them: a strategy on the last mid of AUD/USD, and baselines predicting a
//! known probability whatever the row, published under a temp dir; and IC's
//! gold for a target traded on it.

use std::{path::Path, rc::Rc};

use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::NautilusDataType,
    identifiers::{InstrumentId, StrategyId, Symbol},
    instruments::{CurrencyPair, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use rust_decimal::Decimal;

use crate::{
    backtest::Commission,
    model::{
        calibration::Calibration,
        config::{Margins, MetaConfig, ModelConfig, Sizing},
        dataset::{self, DatasetVenue},
        decide::DecisionRule,
        features::{FeatureSet, meta_names, testing::last_mid},
        holdout::{Earnings, Holdout, Trades},
        learner::{LearnerConfig, TrainingSet},
        payoff::Payoff,
        store::{ModelManifest, ModelSet, ModelStore, Probe, StoredModel},
        targets::{Side, Target, TargetConfig},
    },
    strategy::{DataRequirements, TickSeries},
};

/// The holdout of the models of the tests, as a train publishes it.
const HOLDOUT: Holdout = Holdout {
    earnings: Earnings {
        taken: NO_TRADE,
        every: NO_TRADE,
        taken_stressed: NO_TRADE,
        every_stressed: NO_TRADE,
    },
    log_loss: 0.6,
    baseline_log_loss: 0.69,
    auc: Some(0.6),
};

const NO_TRADE: Trades = Trades {
    count: 0,
    expectancy: None,
    win_rate: None,
};

pub const STRATEGY: &str = "HANDLE-001";
pub const AUD: &str = "AUD/USD.SIM";
pub const GOLD: &str = "XAUUSD.ICMARKETS";

/// XAUUSD as IC defines it (`crates/ctrader/README.md`, "Instruments and
/// account"): whole ounces quoted in USD on ticks of a cent.
pub fn gold() -> InstrumentAny {
    let margin = Decimal::ONE / Decimal::from(30);
    InstrumentAny::CurrencyPair(
        CurrencyPair::builder()
            .instrument_id(InstrumentId::from(GOLD))
            .raw_symbol(Symbol::from("XAUUSD"))
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

/// The calibration of every model of the tests, the odds `o` of a
/// prediction becoming `o^0.9`: c(0.5) = 0.5, c(0.6) = 0.590231, c(0.7)
/// = 0.681909, c(0.8) = 0.776895, c(0.9) = 0.878415.
pub const CALIBRATION: Calibration = Calibration { a: -0.9, b: 0.0 };

/// The calibrated probability of a prediction of `p`.
pub fn c(p: f64) -> f64 {
    CALIBRATION.apply(p)
}

pub fn at(time: &str) -> Timestamp {
    time.parse().unwrap()
}

/// The last mid of AUD/USD, warm once a quote came.
pub fn features() -> Rc<FeatureSet> {
    last_mid(InstrumentId::from(AUD)).build().unwrap()
}

/// Hourly decisions on a time-only target of 1 hour of 1000 AUD/USD, each
/// side traded from an expected net P&L of nought, the baseline learner.
pub fn config() -> ModelConfig {
    ModelConfig::builder()
        .decision(DecisionRule::Every(SignedDuration::from_hours(1)))
        .target(
            TargetConfig::builder()
                .instrument_id(InstrumentId::from(AUD))
                .quantity(Quantity::from(1000))
                .spec(
                    Target::builder()
                        .limit(SignedDuration::from_hours(1))
                        .build(),
                )
                .build(),
        )
        .retrain(SignedDuration::from_hours(24))
        .window(SignedDuration::from_hours(48))
        .holdout(SignedDuration::from_hours(12))
        .margins(Margins {
            long: Some(0.0),
            short: Some(0.0),
        })
        .min_pnl(0.0)
        .min_trades(1)
        .learner(LearnerConfig::Baseline {})
        .build()
}

/// The meta model taking a trade from 0.6, sizing it if `size`.
pub fn meta(size: Option<Sizing>) -> MetaConfig {
    MetaConfig {
        window: SignedDuration::from_hours(48),
        holdout: SignedDuration::from_hours(12),
        threshold: 0.6,
        min_pnl: 0.0,
        min_trades: 1,
        learner: LearnerConfig::Baseline {},
        size,
    }
}

/// `config`, its trades taken by `meta`.
pub fn with_meta(meta: MetaConfig) -> ModelConfig {
    ModelConfig {
        meta: Some(meta),
        ..config()
    }
}

/// What the strategy reads: the quotes of AUD/USD.
pub fn requirements() -> DataRequirements {
    DataRequirements {
        bar_types: vec![],
        ticks: vec![TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: InstrumentId::from(AUD),
        }],
        warmup: SignedDuration::ZERO,
    }
}

/// The venue of the dataset runs: a commission rate of 30 per million,
/// 10 million USD, no leverage.
pub fn venue() -> DatasetVenue {
    DatasetVenue {
        commission: Commission::Rate(Decimal::new(3, 5)),
        balance: Money::from("10000000 USD"),
        leverage: Decimal::ONE,
    }
}

/// The models of `features` and `config` under `models`.
pub fn model_set(models: &Path, features: &FeatureSet, config: &ModelConfig) -> ModelSet {
    ModelSet::new(
        models,
        StrategyId::from(STRATEGY),
        features,
        config,
        &requirements(),
        &venue(),
    )
    .unwrap()
}

/// The store of the `side` models of `features` and `config` under
/// `models`.
pub fn side_store(
    models: &Path,
    features: &FeatureSet,
    config: &ModelConfig,
    side: Side,
) -> ModelStore {
    model_set(models, features, config)
        .side(side)
        .unwrap()
        .store()
        .clone()
}

/// The store of the long models under `models`.
pub fn store(models: &Path) -> ModelStore {
    side_store(models, &features(), &config(), Side::Long)
}

/// A baseline long model of `config` valid from `valid_from` predicting
/// `trues` tenths whatever the row, calibrated by `CALIBRATION`,
/// expecting a win and a loss of 1, its baseline the mean of its
/// training labels, as the store keeps it for the probe's features,
/// probed on one row.
pub fn baseline(valid_from: &str, trues: usize) -> StoredModel {
    baseline_for(&features(), &config(), Side::Long, valid_from, trues)
}

/// The baseline of `baseline`, of `features` and `config`'s `side`.
pub fn baseline_for(
    features: &FeatureSet,
    config: &ModelConfig,
    side: Side,
    valid_from: &str,
    trues: usize,
) -> StoredModel {
    let labels = (0..10).map(|i| i < trues).collect();
    let set = TrainingSet::new(1, vec![0.5; 10], labels).unwrap();
    let model = LearnerConfig::Baseline {}.fit(&set).unwrap();
    let config = config.clone();
    StoredModel {
        manifest: ModelManifest {
            strategy_id: StrategyId::from(STRATEGY),
            valid_from: at(valid_from),
            fingerprint: dataset::fingerprint(
                features,
                config.decision,
                &config.target,
                &requirements(),
                &venue(),
            )
            .unwrap()
            .to_string(),
            feature_names: vec!["mid".to_string()],
            target: config.target,
            side: Some(side),
            learner: config.learner,
            calibration: CALIBRATION,
            baseline: trues as f64 / 10.0,
            payoff: Some(Payoff {
                win: 1.0,
                loss: 1.0,
            }),
            holdout: HOLDOUT,
            probes: vec![Probe {
                row: vec![0.7],
                prediction: CALIBRATION.apply(model.predict(&[0.7]).unwrap()),
            }],
        },
        model: model.save().unwrap(),
    }
}

/// A baseline meta model of `config` valid from `valid_from` predicting
/// `trues` tenths whatever the row, calibrated by `CALIBRATION`, on the
/// meta's features of `features`, with no probe.
pub fn meta_baseline(
    features: &FeatureSet,
    config: &ModelConfig,
    valid_from: &str,
    trues: usize,
) -> StoredModel {
    let names = meta_names(features.names());
    let labels = (0..10).map(|i| i < trues).collect();
    let set = TrainingSet::new(names.len(), vec![0.5; 10 * names.len()], labels).unwrap();
    let mut meta = baseline_for(features, config, Side::Long, valid_from, trues);
    meta.manifest.side = None;
    meta.manifest.feature_names = names;
    meta.manifest.probes.clear();
    meta.model = LearnerConfig::Baseline {}
        .fit(&set)
        .unwrap()
        .save()
        .unwrap();
    meta
}
