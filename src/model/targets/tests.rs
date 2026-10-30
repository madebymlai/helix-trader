use std::str::FromStr;

use nautilus_indicators::average::sma::SimpleMovingAverage;
use nautilus_model::{
    data::NautilusDataType,
    enums::PriceType,
    identifiers::Symbol,
    instruments::{CryptoPerpetual, InstrumentAny},
    types::Currency,
};

use super::*;
use crate::{
    model::{decide, features::FeatureSet},
    strategy::TickSeries,
};

const GOLD: &str = "XAUUSD.ICMARKETS";

/// Deciding every `minutes` minutes.
fn every(minutes: i64) -> DecisionRule {
    DecisionRule::Every(SignedDuration::from_mins(minutes))
}

/// The decisions of every test but where it says: every 15 minutes.
fn rule() -> DecisionRule {
    every(15)
}

/// A take profit 20 bps away, a stop 1.5 ATRs of 14 bars.
fn config() -> TargetsConfig {
    TargetsConfig::builder()
        .instrument_id(InstrumentId::from(GOLD))
        .quantity(Quantity::from(1))
        .barrier(Barrier {
            take_profit: Distance::BasisPoints(Decimal::from(20)),
            stop: Distance::Atr(Decimal::new(15, 1)),
            limit: SignedDuration::from_hours(2),
            atr: Some(AtrConfig { period: 14 }),
        })
        .horizon(SignedDuration::from_hours(1))
        .build()
}

/// The barrier of `config` with both distances in basis points and no
/// ATR.
fn in_bps(take_profit: i64, stop: i64) -> Barrier {
    Barrier {
        take_profit: Distance::BasisPoints(Decimal::from(take_profit)),
        stop: Distance::BasisPoints(Decimal::from(stop)),
        atr: None,
        ..config().barrier.unwrap()
    }
}

#[test]
fn a_config_reads_from_toml() {
    let toml = r#"
        instrument_id = "XAUUSD.ICMARKETS"
        quantity = "1"
        horizon = "1h"

        [barrier]
        take_profit = { bps = 20 }
        stop = { atr = 1.5 }
        limit = "2h"
        atr = { period = 14 }
    "#;

    assert_eq!(toml::from_str::<TargetsConfig>(toml).unwrap(), config());
}

/// The ATR's bars are the decision cadence's: naming them is refused.
#[test]
fn a_barrier_atr_naming_its_bar_type_is_refused_at_load() {
    let toml = r#"
        instrument_id = "XAUUSD.ICMARKETS"
        quantity = "1"
        barrier = { take_profit = { atr = 2 }, stop = { atr = 1.5 }, limit = "2h", atr = { bar_type = "XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL", period = 14 } }
    "#;

    let err = toml::from_str::<TargetsConfig>(toml).unwrap_err();

    assert!(err.to_string().contains("bar_type"), "{err}");
}

/// The requirements of a strategy reading `ticks` of gold, each by the
/// name Nautilus parses (`"quotes"`, `"trades"`, ..), and `bar_types`.
fn reading(ticks: &[&str], bar_types: &[&str]) -> DataRequirements {
    DataRequirements {
        bar_types: bar_types
            .iter()
            .map(|&bar_type| BarType::from(bar_type))
            .collect(),
        ticks: ticks
            .iter()
            .map(|data| TickSeries {
                data: NautilusDataType::from_str(data).unwrap(),
                instrument_id: InstrumentId::from(GOLD),
            })
            .collect(),
        warmup: SignedDuration::ZERO,
    }
}

/// What every test's strategy reads but where it says: the quotes of
/// gold.
fn quotes() -> DataRequirements {
    reading(&["quotes"], &[])
}

/// The ATR of `config` deciding on `rule`, for a strategy reading the
/// quotes of gold: its bar type, as text, and its period.
fn atr_bars(config: &TargetsConfig, rule: DecisionRule) -> Option<(String, usize)> {
    atr_bars_reading(config, rule, &quotes())
}

/// The ATR of `config` deciding on `rule`, for a strategy of
/// `requirements`: its bar type, as text, and its period.
fn atr_bars_reading(
    config: &TargetsConfig,
    rule: DecisionRule,
    requirements: &DataRequirements,
) -> Option<(String, usize)> {
    config
        .atr_bars(rule, requirements)
        .unwrap()
        .map(|atr| (atr.bar_type.to_string(), atr.period))
}

/// The bar type of the ATR of `config()` deciding on `rule`, for a
/// strategy of `requirements`, as text.
fn atr_bar_type(rule: DecisionRule, requirements: &DataRequirements) -> String {
    atr_bars_reading(&config(), rule, requirements).unwrap().0
}

/// The refusal of the ATR of `config()` deciding on `rule`, for a
/// strategy of `requirements`, by `validate` and by `Targets::new`.
fn atr_refused(rule: DecisionRule, requirements: &DataRequirements) -> String {
    assert!(Targets::new(config(), rule, requirements).is_err());
    format!("{:#}", config().validate(rule, requirements).unwrap_err())
}

#[test]
fn the_atr_is_on_the_instruments_mids_built_at_the_decision_cadence() {
    let on = |rule| atr_bars(&config(), rule).unwrap().0;

    assert_eq!(on(every(15)), "XAUUSD.ICMARKETS-15-MINUTE-MID-INTERNAL");
    assert_eq!(on(every(60)), "XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL");
    assert_eq!(on(every(120)), "XAUUSD.ICMARKETS-2-HOUR-MID-INTERNAL");
    assert_eq!(on(every(24 * 60)), "XAUUSD.ICMARKETS-1-DAY-MID-INTERNAL");
    assert_eq!(
        on(DecisionRule::Every(SignedDuration::from_secs(30))),
        "XAUUSD.ICMARKETS-30-SECOND-MID-INTERNAL"
    );
    assert_eq!(
        on(DecisionRule::Every(SignedDuration::from_millis(250))),
        "XAUUSD.ICMARKETS-250-MILLISECOND-MID-INTERNAL"
    );
}

/// Quotes read through bars built from them count as quotes read.
#[test]
fn the_atr_of_a_strategy_on_bars_of_quotes_is_on_the_mids() {
    let requirements = reading(&[], &["XAUUSD.ICMARKETS-1-HOUR-BID-INTERNAL"]);

    assert_eq!(
        atr_bar_type(rule(), &requirements),
        "XAUUSD.ICMARKETS-15-MINUTE-MID-INTERNAL"
    );
}

/// Quotes come first: with trades read beside them, the ATR is still on
/// the mids.
#[test]
fn the_atr_of_a_strategy_reading_quotes_and_trades_is_on_the_mids() {
    let requirements = reading(&["trades", "quotes"], &[]);

    assert_eq!(
        atr_bar_type(rule(), &requirements),
        "XAUUSD.ICMARKETS-15-MINUTE-MID-INTERNAL"
    );
}

#[test]
fn the_atr_of_a_strategy_reading_trades_alone_is_on_the_last_prices() {
    for requirements in [
        reading(&["trades"], &[]),
        reading(&[], &["XAUUSD.ICMARKETS-1-HOUR-LAST-INTERNAL"]),
    ] {
        assert_eq!(
            atr_bar_type(rule(), &requirements),
            "XAUUSD.ICMARKETS-15-MINUTE-LAST-INTERNAL",
            "{requirements:?}"
        );
    }
}

/// On bars alone, the ATR's bars are Nautilus' composite bars of the
/// cadence built from the source bars the strategy reads, priced as they
/// are.
#[test]
fn the_atr_of_a_strategy_reading_bars_alone_is_built_from_its_source_bars() {
    let requirements = reading(
        &[],
        &["XAUUSD.ICMARKETS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"],
    );

    assert_eq!(
        atr_bar_type(rule(), &requirements),
        "XAUUSD.ICMARKETS-15-MINUTE-LAST-INTERNAL@1-MINUTE-EXTERNAL"
    );
    assert_eq!(
        atr_bar_type(every(60), &requirements),
        "XAUUSD.ICMARKETS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"
    );
}

/// Of several source bars, those whose length divides the cadence, the
/// longest: the fewest source bars to a bar of the cadence.
#[test]
fn the_atr_on_bars_alone_is_built_from_the_longest_source_bars_dividing_the_cadence() {
    let requirements = reading(
        &[],
        &[
            "XAUUSD.ICMARKETS-1-HOUR-BID-INTERNAL@1-MINUTE-EXTERNAL",
            "XAUUSD.ICMARKETS-4-HOUR-ASK-INTERNAL@5-MINUTE-EXTERNAL",
            "XAUUSD.ICMARKETS-1-DAY-LAST-INTERNAL@1-HOUR-EXTERNAL",
        ],
    );

    assert_eq!(
        atr_bar_type(rule(), &requirements),
        "XAUUSD.ICMARKETS-15-MINUTE-ASK-INTERNAL@5-MINUTE-EXTERNAL"
    );
    assert_eq!(
        atr_bar_type(every(120), &requirements),
        "XAUUSD.ICMARKETS-2-HOUR-LAST-INTERNAL@1-HOUR-EXTERNAL"
    );
    assert_eq!(
        atr_bar_type(every(3), &requirements),
        "XAUUSD.ICMARKETS-3-MINUTE-BID-INTERNAL@1-MINUTE-EXTERNAL"
    );
}

/// The ATR's bars are of the cadence: source bars it is not a whole
/// multiple of cannot build them, nor bars of no length.
#[test]
fn an_atr_on_bars_alone_not_dividing_the_cadence_is_refused_naming_both() {
    let hours = reading(
        &[],
        &["XAUUSD.ICMARKETS-1-DAY-LAST-INTERNAL@1-HOUR-EXTERNAL"],
    );
    let err = atr_refused(rule(), &hours);
    assert!(err.contains("15m"), "{err}");
    assert!(
        err.contains("XAUUSD.ICMARKETS-1-HOUR-LAST-EXTERNAL"),
        "{err}"
    );

    let ticks = reading(
        &[],
        &["XAUUSD.ICMARKETS-1-HOUR-LAST-INTERNAL@100-TICK-EXTERNAL"],
    );
    let err = atr_refused(rule(), &ticks);
    assert!(
        err.contains("XAUUSD.ICMARKETS-100-TICK-LAST-EXTERNAL"),
        "{err}"
    );
}

/// Neither quotes, trades nor bars of the targets' instrument: nothing to
/// build the ATR's bars from, whatever else the strategy reads.
#[test]
fn an_atr_on_an_instrument_the_strategy_reads_no_prices_of_is_refused() {
    let elsewhere = DataRequirements {
        bar_types: vec![BarType::from(
            "EURUSD.ICMARKETS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
        )],
        ticks: vec![
            TickSeries {
                data: NautilusDataType::QuoteTick,
                instrument_id: InstrumentId::from("EURUSD.ICMARKETS"),
            },
            TickSeries {
                data: NautilusDataType::OrderBookDepth,
                instrument_id: InstrumentId::from(GOLD),
            },
        ],
        warmup: SignedDuration::ZERO,
    };

    let err = atr_refused(rule(), &elsewhere);

    assert!(err.contains(GOLD), "{err}");
    let bps = TargetsConfig {
        barrier: Some(in_bps(20, 10)),
        ..config()
    };
    assert!(bps.validate(rule(), &elsewhere).is_ok());
}

/// Wilder's period: an `atr` distance without an `atr` table has an ATR of
/// 14 bars, which `atr = { period = .. }` overrides.
#[test]
fn an_atr_distance_without_an_atr_table_has_an_atr_of_14_bars() {
    let barrier = config().barrier.unwrap();
    let default = TargetsConfig {
        barrier: Some(Barrier {
            atr: None,
            ..barrier
        }),
        ..config()
    };
    let overridden = TargetsConfig {
        barrier: Some(Barrier {
            atr: Some(AtrConfig { period: 20 }),
            ..barrier
        }),
        ..config()
    };

    assert!(default.validate(rule(), &quotes()).is_ok());
    assert_eq!(atr_bars(&default, rule()).unwrap().1, 14);
    assert_eq!(atr_bars(&overridden, rule()).unwrap().1, 20);
    assert_eq!(
        atr_bars(
            &TargetsConfig {
                barrier: Some(in_bps(20, 10)),
                ..config()
            },
            rule()
        ),
        None
    );
}

#[test]
fn an_atr_distance_without_an_atr_table_reads_from_toml() {
    let toml = r#"
        instrument_id = "XAUUSD.ICMARKETS"
        quantity = "1"
        barrier = { take_profit = { atr = 2.0 }, stop = { atr = 1.5 }, limit = "2h" }
    "#;
    let config = toml::from_str::<TargetsConfig>(toml).unwrap();

    assert!(config.validate(rule(), &quotes()).is_ok());
    assert_eq!(atr_bars(&config, rule()).unwrap().1, 14);
}

/// Decisions on events have no cadence to build the ATR's bars at.
#[test]
fn an_atr_with_decisions_on_events_is_refused() {
    let err = format!(
        "{:#}",
        config()
            .validate(DecisionRule::Event, &quotes())
            .unwrap_err()
    );
    let bps = TargetsConfig {
        barrier: Some(in_bps(20, 10)),
        ..config()
    };

    assert!(err.contains("ATR"), "{err}");
    assert!(Targets::new(config(), DecisionRule::Event, &quotes()).is_err());
    assert!(bps.validate(DecisionRule::Event, &quotes()).is_ok());
}

/// Nautilus builds only bars whose step divides the next unit
/// (`BarSpecification::new_checked`): neither 90 nor 7 minutes divide an hour.
#[test]
fn an_atr_on_a_cadence_nautilus_builds_no_bars_of_is_refused() {
    for minutes in [90, 7] {
        let err = format!(
            "{:#}",
            config().validate(every(minutes), &quotes()).unwrap_err()
        );
        assert!(err.contains("ATR"), "{minutes}: {err}");
    }
}

#[test]
fn a_distance_has_one_known_unit() {
    let barrier = |take_profit: &str| {
        let toml = format!(
            r#"
            instrument_id = "XAUUSD.ICMARKETS"
            quantity = "1"
            barrier = {{ take_profit = {take_profit}, stop = {{ bps = 10 }}, limit = "2h" }}
            "#
        );
        toml::from_str::<TargetsConfig>(&toml).map(|config| config.barrier.unwrap())
    };

    assert_eq!(barrier("{ bps = 7.5 }").unwrap().take_profit, {
        Distance::BasisPoints(Decimal::new(75, 1))
    });
    for invalid in ["3.0", "{ ticks = 3 }", "{ bps = 10, atr = 1 }", "{}"] {
        assert!(barrier(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn a_config_refuses_an_unknown_field() {
    let toml = r#"
        instrument_id = "XAUUSD.ICMARKETS"
        quantity = "1"
        horizon = "1h"
        horizn = "2h"
    "#;

    assert!(toml::from_str::<TargetsConfig>(toml).is_err());
}

#[test]
fn either_target_alone_is_valid() {
    let barrier = TargetsConfig {
        horizon: None,
        ..config()
    };
    let horizon = TargetsConfig {
        barrier: None,
        ..config()
    };

    assert!(config().validate(rule(), &quotes()).is_ok());
    assert!(barrier.validate(rule(), &quotes()).is_ok());
    assert!(horizon.validate(rule(), &quotes()).is_ok());
}

#[test]
fn a_config_without_a_target_is_invalid() {
    let none = TargetsConfig {
        barrier: None,
        horizon: None,
        ..config()
    };

    assert!(none.validate(rule(), &quotes()).is_err());
}

#[test]
fn sizes_distances_and_times_must_be_positive() {
    let barrier = config().barrier.unwrap();
    let invalid = [
        TargetsConfig {
            quantity: Quantity::from(0),
            ..config()
        },
        TargetsConfig {
            barrier: Some(in_bps(0, 10)),
            ..config()
        },
        TargetsConfig {
            barrier: Some(in_bps(10, -1)),
            ..config()
        },
        TargetsConfig {
            barrier: Some(Barrier {
                stop: Distance::Atr(Decimal::ZERO),
                ..barrier
            }),
            ..config()
        },
        TargetsConfig {
            barrier: Some(Barrier {
                atr: Some(AtrConfig { period: 0 }),
                ..barrier
            }),
            ..config()
        },
        TargetsConfig {
            barrier: Some(Barrier {
                limit: SignedDuration::ZERO,
                ..barrier
            }),
            ..config()
        },
        TargetsConfig {
            horizon: Some(SignedDuration::from_mins(-5)),
            ..config()
        },
    ];

    for config in invalid {
        assert!(config.validate(rule(), &quotes()).is_err(), "{config:?}");
        assert!(Targets::new(config, rule(), &quotes()).is_err());
    }
}

#[test]
fn an_atr_table_is_refused_unless_a_distance_uses_the_atr() {
    let barrier = config().barrier.unwrap();
    let valid = [
        TargetsConfig {
            barrier: Some(in_bps(20, 10)),
            ..config()
        },
        TargetsConfig {
            barrier: Some(Barrier {
                take_profit: Distance::Atr(Decimal::from(2)),
                ..barrier
            }),
            ..config()
        },
    ];
    let unused = TargetsConfig {
        barrier: Some(Barrier {
            atr: barrier.atr,
            ..in_bps(20, 10)
        }),
        ..config()
    };

    for config in valid {
        assert!(config.validate(rule(), &quotes()).is_ok(), "{config:?}");
    }
    let err = format!("{:#}", unused.validate(rule(), &quotes()).unwrap_err());
    assert!(err.contains("no distance uses"), "{err}");
    assert!(Targets::new(unused, rule(), &quotes()).is_err());
}

/// A perpetual priced in half points, 0.5 a tick, as some futures are.
fn half_point() -> InstrumentAny {
    InstrumentAny::CryptoPerpetual(
        CryptoPerpetual::builder()
            .instrument_id(InstrumentId::from("HALF-PERP.TEST"))
            .raw_symbol(Symbol::from("HALF-PERP"))
            .base_currency(Currency::BTC())
            .quote_currency(Currency::USDT())
            .settlement_currency(Currency::USDT())
            .is_inverse(false)
            .price_precision(1)
            .size_precision(0)
            .price_increment(Price::from("0.5"))
            .size_increment(Quantity::from(1))
            .multiplier(Quantity::from(1))
            .lot_size(Quantity::from(1))
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .unwrap(),
    )
}

/// The take profit and stop of the long and the short, from `barrier`
/// at `reference` with `atr`, as text.
fn exits(barrier: Barrier, reference: &str, atr: Option<f64>) -> Vec<(String, String)> {
    let reference = Decimal::from_str_exact(reference).unwrap();
    barrier
        .exits(&half_point(), reference, atr)
        .unwrap()
        .into_iter()
        .map(|(_, take_profit, stop)| (take_profit.to_string(), stop.to_string()))
        .collect()
}

#[test]
fn exits_are_basis_points_of_the_reference_on_the_nearest_tick() {
    // 10 bps of 1000.0 is 1.0, on the tick; 12 bps 1.2, nearer 1.0
    // than 1.5; 13 bps 1.3, nearer 1.5.
    assert_eq!(
        exits(in_bps(10, 12), "1000.0", None),
        [
            ("1001.0".into(), "999.0".into()),
            ("999.0".into(), "1001.0".into())
        ]
    );
    assert_eq!(
        exits(in_bps(13, 10), "1000.0", None),
        [
            ("1001.5".into(), "999.0".into()),
            ("998.5".into(), "1001.0".into())
        ]
    );
}

#[test]
fn a_tie_between_two_ticks_goes_away_from_the_reference() {
    // 12.5 bps of 1000.0 is 1.25, half way between 1.0 and 1.5.
    let barrier = Barrier {
        take_profit: Distance::BasisPoints(Decimal::new(125, 1)),
        ..in_bps(10, 10)
    };

    assert_eq!(
        exits(barrier, "1000.0", None),
        [
            ("1001.5".into(), "999.0".into()),
            ("998.5".into(), "1001.0".into())
        ]
    );
}

#[test]
fn exits_are_multiples_of_the_atr_on_the_nearest_tick() {
    // 1.5 ATRs of 2.1 is 3.15, nearer 3.0 than 3.5; 2 ATRs 4.2,
    // nearer 4.0.
    let barrier = Barrier {
        take_profit: Distance::Atr(Decimal::new(15, 1)),
        stop: Distance::Atr(Decimal::from(2)),
        ..config().barrier.unwrap()
    };

    assert_eq!(
        exits(barrier, "1000.0", Some(2.1)),
        [
            ("1003.0".into(), "996.0".into()),
            ("997.0".into(), "1004.0".into())
        ]
    );
}

#[test]
fn the_fingerprint_changes_with_every_target_spec() {
    let base = FeatureSet::builder()
        .quotes(
            InstrumentId::from(GOLD),
            "mid",
            SimpleMovingAverage::new(1, Some(PriceType::Mid)),
        )
        .build()
        .unwrap()
        .fingerprint();
    let barrier = config().barrier.unwrap();
    let changed = [
        TargetsConfig {
            quantity: Quantity::from(2),
            ..config()
        },
        TargetsConfig {
            barrier: Some(Barrier {
                take_profit: Distance::BasisPoints(Decimal::from(21)),
                ..barrier
            }),
            ..config()
        },
        TargetsConfig {
            barrier: Some(Barrier {
                take_profit: Distance::Atr(Decimal::from(20)),
                ..barrier
            }),
            ..config()
        },
        TargetsConfig {
            barrier: Some(Barrier {
                stop: Distance::Atr(Decimal::from(2)),
                ..barrier
            }),
            ..config()
        },
        TargetsConfig {
            barrier: Some(Barrier {
                atr: Some(AtrConfig { period: 15 }),
                ..barrier
            }),
            ..config()
        },
        TargetsConfig {
            barrier: Some(Barrier {
                limit: SignedDuration::from_hours(3),
                ..barrier
            }),
            ..config()
        },
        TargetsConfig {
            horizon: None,
            ..config()
        },
        TargetsConfig {
            horizon: Some(SignedDuration::from_mins(30)),
            ..config()
        },
    ];

    assert_eq!(
        fingerprint(base, &config(), rule(), &quotes()).unwrap(),
        fingerprint(base, &config(), rule(), &quotes()).unwrap()
    );
    assert_ne!(
        fingerprint(base, &config(), rule(), &quotes()).unwrap(),
        base
    );
    for config in changed {
        assert_ne!(
            fingerprint(base, &config, rule(), &quotes()).unwrap(),
            fingerprint(base, &self::config(), rule(), &quotes()).unwrap(),
            "{config:?}"
        );
    }
}

/// The ATR's period is fingerprinted as it runs: left to its default it
/// keys the same targets as 14 written out.
#[test]
fn the_fingerprint_reads_the_atrs_default_period_as_written_out() {
    let base = FeatureSet::builder()
        .quotes(
            InstrumentId::from(GOLD),
            "mid",
            SimpleMovingAverage::new(1, Some(PriceType::Mid)),
        )
        .build()
        .unwrap()
        .fingerprint();
    let barrier = config().barrier.unwrap();
    let default = TargetsConfig {
        barrier: Some(Barrier {
            atr: None,
            ..barrier
        }),
        ..config()
    };

    assert_eq!(
        fingerprint(base, &default, rule(), &quotes()).unwrap(),
        fingerprint(base, &config(), rule(), &quotes()).unwrap()
    );
}

/// The ATR's bars are the cadence's: deciding every hour, not every 15
/// minutes, keys other targets, through the decisions' fingerprint they
/// extend, as `dataset::fingerprint` composes them.
#[test]
fn a_cadence_change_changes_the_fingerprint_of_targets_with_an_atr() {
    let features = FeatureSet::builder()
        .quotes(
            InstrumentId::from(GOLD),
            "mid",
            SimpleMovingAverage::new(1, Some(PriceType::Mid)),
        )
        .build()
        .unwrap();
    let keyed = |rule| {
        fingerprint(
            decide::fingerprint(&features, rule),
            &config(),
            rule,
            &quotes(),
        )
        .unwrap()
    };

    assert_ne!(keyed(every(15)), keyed(every(60)));
}

/// The ATR's bars are built from what the strategy reads: the same config
/// on quotes, on trades and on bars keys three targets, whose rows differ.
/// Targets without an ATR are keyed alike whatever the strategy reads.
#[test]
fn the_source_of_the_atrs_bars_changes_the_fingerprint() {
    let base = Fingerprint::default();
    let keyed = |config: &TargetsConfig, requirements: &DataRequirements| {
        fingerprint(base, config, rule(), requirements).unwrap()
    };
    let trades = reading(&["trades"], &[]);
    let bars = reading(
        &[],
        &["XAUUSD.ICMARKETS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"],
    );

    assert_ne!(keyed(&config(), &quotes()), keyed(&config(), &trades));
    assert_ne!(keyed(&config(), &quotes()), keyed(&config(), &bars));
    assert_ne!(keyed(&config(), &trades), keyed(&config(), &bars));
    let bps = TargetsConfig {
        barrier: Some(in_bps(20, 10)),
        ..config()
    };
    assert_eq!(keyed(&bps, &quotes()), keyed(&bps, &trades));
}

#[test]
fn a_decision_settles_within_its_longest_target() {
    let horizon = TargetsConfig {
        barrier: None,
        ..config()
    };

    assert_eq!(config().settles_within(), SignedDuration::from_hours(2));
    assert_eq!(horizon.settles_within(), SignedDuration::from_hours(1));
}

#[test]
fn a_row_has_a_value_per_target_name_the_pnl_and_the_exit_of_each_side() {
    let outcome = |pnl: &str, exit| Outcome {
        pnl: Money::from(pnl),
        exit,
    };
    let row = TargetRow {
        decision: FeatureRow {
            strategy_id: StrategyId::from("S-001"),
            features: vec![1.0],
            ts_event: UnixNanos::from(1),
            ts_init: UnixNanos::from(1),
        },
        barrier: Some(Outcomes {
            long: outcome("2.5 USD", Exit::TakeProfit),
            short: outcome("-3.5 USD", Exit::Stop),
        }),
        horizon: Some(Outcomes {
            long: outcome("-0.25 USD", Exit::Time),
            short: outcome("0.25 USD", Exit::Time),
        }),
        settled_at: UnixNanos::from(2),
    };
    let horizon = TargetsConfig {
        barrier: None,
        ..config()
    };

    assert_eq!(
        config().target_names(),
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
    assert_eq!(row.values(), [2.5, 1.0, -3.5, -1.0, -0.25, 0.0, 0.25, 0.0]);
    assert_eq!(horizon.target_names().len(), 4);
}

#[test]
fn an_outcome_has_the_sign_of_its_pnl() {
    let sign = |pnl: &str| {
        Outcome {
            pnl: Money::from(pnl),
            exit: Exit::Time,
        }
        .sign()
    };

    assert_eq!(sign("0.0005 USDT"), 1);
    assert_eq!(sign("-0.44 USD"), -1);
    assert_eq!(sign("0 USD"), 0);
}

#[test]
fn a_label_is_the_long_pnl_of_its_target_among_a_rows_values() {
    let horizon = TargetsConfig {
        barrier: None,
        ..config()
    };

    assert_eq!(config().label_index(Label::Barrier), Some(0));
    assert_eq!(config().label_index(Label::Horizon), Some(4));
    assert_eq!(horizon.label_index(Label::Horizon), Some(0));
    assert_eq!(horizon.label_index(Label::Barrier), None);
}

#[test]
fn a_sides_pnl_is_its_target_names_pnl_among_a_rows_values() {
    let names = config().target_names();
    let name = |label, side| names[config().pnl_index(label, side).unwrap()].clone();

    assert_eq!(name(Label::Barrier, OrderSide::Buy), "barrier_long_pnl");
    assert_eq!(name(Label::Barrier, OrderSide::Sell), "barrier_short_pnl");
    assert_eq!(name(Label::Horizon, OrderSide::Sell), "horizon_short_pnl");
}

#[test]
fn a_label_reads_from_toml() {
    #[derive(Deserialize)]
    struct Section {
        label: Label,
    }
    let label = |toml: &str| toml::from_str::<Section>(toml).map(|section| section.label);

    assert_eq!(label(r#"label = "barrier""#).unwrap(), Label::Barrier);
    assert_eq!(label(r#"label = "horizon""#).unwrap(), Label::Horizon);
    assert!(label(r#"label = "direction""#).is_err());
}

/// The fingerprint hashes this module's code: its tests live in a
/// file of their own, so editing one changes no fingerprint.
#[test]
fn the_fingerprinted_code_holds_no_test() {
    assert!(!include_str!("../targets.rs").contains("#[test]"));
}
