//! The store and the models of the tests of a decision, the handle's among
//! them: a strategy on the last mid of AUD/USD, and baselines predicting a
//! known R, a side's or a meta's, whatever the row, published under a temp
//! dir; and IC's gold for a target traded on it.

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
        calibration::{Ksplit, Line},
        config::{Margins, MetaConfig, ModelConfig},
        dataset::{self, DatasetVenue},
        decide::DecisionRule,
        features::{FeatureSet, meta_names, side_names, testing::last_mid},
        holdout::{Earnings, Holdout, Trades},
        learner::{LearnerConfig, TrainingSet},
        score::ExpectedScore,
        store::{
            ModelCalibration, ModelManifest, ModelSet, ModelStore, PairCalibration, Probe,
            StoredModel,
        },
        targets::{DEFAULT_ATR, Side, Target, TargetConfig},
    },
    strategy::{AtrArgs, DataRequirements, TickSeries},
};

/// The holdout of the models of the tests, as a train publishes it: a
/// skill of 0.1.
pub const HOLDOUT: Holdout = Holdout {
    earnings: Earnings {
        taken: NO_TRADE,
        every: NO_TRADE,
        taken_stressed: NO_TRADE,
        every_stressed: NO_TRADE,
    },
    score: ExpectedScore {
        rows: 2,
        skill: Some(0.1),
        mean_error: Some(0.0),
        slope: None,
        top_decile_gap: Some(0.0),
    },
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

/// The calibration of every meta model of the tests, its level line `0.1 +
/// 0.5 E`: a meta predicting an R of 0.3 expects 0.25, one predicting nought
/// 0.1.
pub const CALIBRATION: Line = Line {
    alpha: 0.1,
    beta: 0.5,
};

/// The R a side's baseline of `tenths` predicts: `tenths` less 5, over 10,
/// so above 5 a gain, below a loss and 5 nought.
pub fn r(tenths: usize) -> f64 {
    (tenths as f64 - 5.0) / 10.0
}

/// The calibration of the pair of `config`'s sides valid from
/// `valid_from` leaving each side's prediction as it is, up to rounding:
/// the level line `C' = C`, and of two sides the side call `D' = D`.
pub fn unchanged(config: &ModelConfig, valid_from: Timestamp) -> ModelCalibration {
    let sides: Vec<Side> = config.margins.sides().map(|(side, _)| side).collect();
    ModelCalibration::Pair(PairCalibration {
        valid_from,
        lines: Ksplit {
            level: Line {
                alpha: 0.0,
                beta: 1.0,
            },
            side_call: (sides.len() == 2).then_some(1.0),
        },
        sides,
    })
}

pub fn at(time: &str) -> Timestamp {
    time.parse().unwrap()
}

/// The last mid of AUD/USD, warm once a quote came.
pub fn features() -> Rc<FeatureSet> {
    last_mid(InstrumentId::from(AUD)).build().unwrap()
}

/// Hourly decisions on a time-only target of 1 hour of 1000 AUD/USD, its
/// unit the worth of its ATR of one hourly bar of the mids, the long traded
/// from an expected net P&L of nought, the baseline learner.
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
                        .atr(AtrArgs {
                            period: 1,
                            ..DEFAULT_ATR
                        })
                        .build(),
                )
                .build(),
        )
        .retrain(SignedDuration::from_hours(24))
        .window(SignedDuration::from_hours(48))
        .holdout(SignedDuration::from_hours(12))
        .margins(Margins {
            long: Some(0.0),
            short: None,
        })
        .min_pnl(0.0)
        .min_trades(1)
        .learner(LearnerConfig::Baseline {})
        .build()
}

/// `config`, the short traded too, from nought.
pub fn both_sides() -> ModelConfig {
    ModelConfig {
        margins: Margins {
            long: Some(0.0),
            short: Some(0.0),
        },
        ..config()
    }
}

/// The meta model taking a trade from an expected net P&L of 0.15 USD.
pub fn meta() -> MetaConfig {
    MetaConfig {
        window: SignedDuration::from_hours(48),
        holdout: SignedDuration::from_hours(12),
        margin: 0.15,
        min_pnl: 0.0,
        min_trades: 1,
        learner: LearnerConfig::Baseline {},
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

/// A baseline long model of `config` valid from `valid_from` predicting an
/// R of `r(tenths)` whatever the row, as `baseline_for` says.
pub fn baseline(valid_from: &str, tenths: usize) -> StoredModel {
    baseline_for(&features(), &config(), Side::Long, valid_from, tenths)
}

/// A baseline model of `config`'s `side` valid from `valid_from`, on the
/// side's features of `features` and the target's payoff, predicting an R
/// of `r(tenths)` whatever the row, its pair's calibration leaving it as it
/// is (`unchanged`), its baseline that R, as the store keeps it, probed on
/// one row.
pub fn baseline_for(
    features: &FeatureSet,
    config: &ModelConfig,
    side: Side,
    valid_from: &str,
    tenths: usize,
) -> StoredModel {
    let names = side_names(features.names(), &config.target.payoff_names());
    let width = names.len();
    let set = TrainingSet::regression(width, vec![0.5; width], vec![r(tenths)]).unwrap();
    let model = LearnerConfig::Baseline {}.fit(&set).unwrap();
    let probe = vec![0.7; width];
    let config = config.clone();
    StoredModel {
        manifest: ModelManifest {
            strategy_id: StrategyId::from(STRATEGY),
            valid_from: at(valid_from),
            // Its strategy reads the quotes of what it trades, which the
            // target's ATR, if any, is built from.
            fingerprint: dataset::fingerprint(
                features,
                config.decision,
                &config.target,
                &DataRequirements {
                    ticks: vec![TickSeries {
                        data: NautilusDataType::QuoteTick,
                        instrument_id: config.target.instrument_id,
                    }],
                    ..requirements()
                },
                &venue(),
            )
            .unwrap()
            .to_string(),
            feature_names: names,
            calibration: unchanged(&config, at(valid_from)),
            target: config.target,
            side: Some(side),
            learner: config.learner,
            baseline: r(tenths),
            holdout: HOLDOUT,
            probes: vec![Probe {
                prediction: model.predict(&probe).unwrap(),
                row: probe,
            }],
        },
        model: model.save().unwrap(),
    }
}

/// A baseline meta model of `config` valid from `valid_from` predicting an
/// R of `r(tenths)` whatever the row, calibrated by `CALIBRATION`, on the
/// meta's features of `features`, its baseline that R, with no probe.
pub fn meta_baseline(
    features: &FeatureSet,
    config: &ModelConfig,
    valid_from: &str,
    tenths: usize,
) -> StoredModel {
    let names = meta_names(features.names());
    let set =
        TrainingSet::regression(names.len(), vec![0.5; names.len()], vec![r(tenths)]).unwrap();
    let mut meta = baseline_for(features, config, Side::Long, valid_from, tenths);
    meta.manifest.side = None;
    meta.manifest.feature_names = names;
    meta.manifest.calibration = ModelCalibration::Meta(CALIBRATION);
    meta.manifest.probes.clear();
    meta.model = LearnerConfig::Baseline {}
        .fit(&set)
        .unwrap()
        .save()
        .unwrap();
    meta
}
