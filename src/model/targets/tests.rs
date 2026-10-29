use nautilus_indicators::average::sma::SimpleMovingAverage;
use nautilus_model::{
    enums::PriceType,
    identifiers::Symbol,
    instruments::{CryptoPerpetual, InstrumentAny},
    types::Currency,
};

use super::*;
use crate::model::features::FeatureSet;

const GOLD: &str = "XAUUSD.ICMARKETS";
const GOLD_MINUTE: &str = "XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL";

/// A take profit 20 bps away, a stop 1.5 ATRs of 14 minute bars.
fn config() -> TargetsConfig {
    TargetsConfig::builder()
        .instrument_id(InstrumentId::from(GOLD))
        .quantity(Quantity::from(1))
        .barrier(Barrier {
            take_profit: Distance::BasisPoints(Decimal::from(20)),
            stop: Distance::Atr(Decimal::new(15, 1)),
            limit: SignedDuration::from_hours(2),
            atr: Some(AtrConfig {
                bar_type: BarType::from(GOLD_MINUTE),
                period: 14,
            }),
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
        atr = { bar_type = "XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL", period = 14 }
    "#;

    assert_eq!(toml::from_str::<TargetsConfig>(toml).unwrap(), config());
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

    assert!(config().validate().is_ok());
    assert!(barrier.validate().is_ok());
    assert!(horizon.validate().is_ok());
}

#[test]
fn a_config_without_a_target_is_invalid() {
    let none = TargetsConfig {
        barrier: None,
        horizon: None,
        ..config()
    };

    assert!(none.validate().is_err());
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
                atr: Some(AtrConfig {
                    period: 0,
                    ..barrier.atr.unwrap()
                }),
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
        assert!(config.validate().is_err(), "{config:?}");
        assert!(Targets::new(config).is_err());
    }
}

#[test]
fn an_atr_is_declared_on_the_instruments_bars_exactly_when_a_distance_uses_it() {
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
    let invalid = [
        // An ATR distance with no ATR.
        TargetsConfig {
            barrier: Some(Barrier {
                atr: None,
                ..barrier
            }),
            ..config()
        },
        // An ATR no distance uses.
        TargetsConfig {
            barrier: Some(Barrier {
                atr: barrier.atr,
                ..in_bps(20, 10)
            }),
            ..config()
        },
        // An ATR of another instrument's bars.
        TargetsConfig {
            barrier: Some(Barrier {
                atr: Some(AtrConfig {
                    bar_type: BarType::from("EURUSD.ICMARKETS-1-MINUTE-MID-INTERNAL"),
                    period: 14,
                }),
                ..barrier
            }),
            ..config()
        },
    ];

    for config in valid {
        assert!(config.validate().is_ok(), "{config:?}");
    }
    for config in invalid {
        assert!(config.validate().is_err(), "{config:?}");
        assert!(Targets::new(config).is_err());
    }
}

#[test]
fn a_config_with_an_atr_distance_without_its_atr_is_refused_at_load() {
    let toml = r#"
        instrument_id = "XAUUSD.ICMARKETS"
        quantity = "1"
        barrier = { take_profit = { bps = 20 }, stop = { atr = 1.5 }, limit = "2h" }
    "#;
    let config = toml::from_str::<TargetsConfig>(toml).unwrap();

    assert!(config.validate().is_err());
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
                atr: Some(AtrConfig {
                    period: 15,
                    ..barrier.atr.unwrap()
                }),
                ..barrier
            }),
            ..config()
        },
        TargetsConfig {
            barrier: Some(Barrier {
                atr: Some(AtrConfig {
                    bar_type: BarType::from("XAUUSD.ICMARKETS-5-MINUTE-MID-INTERNAL"),
                    ..barrier.atr.unwrap()
                }),
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

    assert_eq!(fingerprint(base, &config()), fingerprint(base, &config()));
    assert_ne!(fingerprint(base, &config()), base);
    for config in changed {
        assert_ne!(
            fingerprint(base, &config),
            fingerprint(base, &self::config()),
            "{config:?}"
        );
    }
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
