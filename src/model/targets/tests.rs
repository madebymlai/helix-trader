use std::str::FromStr;

use nautilus_indicators::average::MovingAverageType;
use nautilus_model::{
    data::NautilusDataType,
    identifiers::Symbol,
    instruments::{CryptoPerpetual, InstrumentAny},
    types::Currency,
};

use super::*;
use crate::{model::features::testing::last_mid, strategy::TickSeries};

const GOLD: &str = "XAUUSD.ICMARKETS";

/// Deciding every `minutes` minutes.
fn every(minutes: i64) -> DecisionRule {
    DecisionRule::Every(SignedDuration::from_mins(minutes))
}

/// The decisions of every test but where it says: every 15 minutes.
fn rule() -> DecisionRule {
    every(15)
}

/// A bracket: a take profit 20 bps away, a stop 1.5 ATRs of 14 bars,
/// closed after 2 hours.
fn config() -> TargetConfig {
    TargetConfig::builder()
        .instrument_id(InstrumentId::from(GOLD))
        .quantity(Quantity::from(1))
        .spec(Target {
            take_profit: Some(Distance::BasisPoints(Decimal::from(20))),
            stop: Some(Distance::Atr(Decimal::new(15, 1))),
            limit: SignedDuration::from_hours(2),
            atr: Some(AtrArgs {
                period: 14,
                ..DEFAULT_ATR
            }),
        })
        .build()
}

/// `config` with `spec`.
fn of(spec: Target) -> TargetConfig {
    TargetConfig { spec, ..config() }
}

/// The target of `config`.
fn bracket() -> Target {
    config().spec
}

/// The time-only target of `config`'s instrument and size, closed after
/// `limit`.
fn time_only(limit: SignedDuration) -> TargetConfig {
    of(Target::builder().limit(limit).build())
}

/// The target of `config` with both distances in basis points and no
/// ATR.
fn in_bps(take_profit: i64, stop: i64) -> Target {
    Target {
        take_profit: Some(Distance::BasisPoints(Decimal::from(take_profit))),
        stop: Some(Distance::BasisPoints(Decimal::from(stop))),
        atr: None,
        ..bracket()
    }
}

/// The ATR's bars are the decision cadence's: naming them is refused.
#[test]
fn a_target_atr_naming_its_bar_type_is_refused_at_load() {
    let toml = r#"
        take_profit = { atr = 2 }
        stop = { atr = 1.5 }
        limit = "2h"
        atr = { bar_type = "XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL", period = 14 }
    "#;

    let err = toml::from_str::<Target>(toml).unwrap_err();

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
fn atr_bars(config: &TargetConfig, rule: DecisionRule) -> Option<(String, usize)> {
    atr_bars_reading(config, rule, &quotes())
}

/// The ATR of `config` deciding on `rule`, for a strategy of
/// `requirements`: its bar type, as text, and its period.
fn atr_bars_reading(
    config: &TargetConfig,
    rule: DecisionRule,
    requirements: &DataRequirements,
) -> Option<(String, usize)> {
    config
        .atr_bars(rule, requirements)
        .unwrap()
        .map(|atr| (atr.bar_type.to_string(), atr.args.period))
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

/// Neither quotes, trades nor bars of the target's instrument: nothing to
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
    assert!(of(in_bps(20, 10)).validate(rule(), &elsewhere).is_ok());
    assert!(
        time_only(SignedDuration::from_hours(1))
            .validate(rule(), &elsewhere)
            .is_ok()
    );
}

/// Wilder's period: an `atr` distance without an `atr` table has an ATR of
/// 14 bars, which `atr = { period = .. }` overrides.
#[test]
fn an_atr_distance_without_an_atr_table_has_an_atr_of_14_bars() {
    let default = of(Target {
        atr: None,
        ..bracket()
    });
    let overridden = of(Target {
        atr: Some(AtrArgs {
            period: 20,
            ..DEFAULT_ATR
        }),
        ..bracket()
    });

    assert!(default.validate(rule(), &quotes()).is_ok());
    assert_eq!(atr_bars(&default, rule()).unwrap().1, 14);
    assert_eq!(atr_bars(&overridden, rule()).unwrap().1, 20);
    assert_eq!(atr_bars(&of(in_bps(20, 10)), rule()), None);
    assert_eq!(
        atr_bars(&time_only(SignedDuration::from_hours(1)), rule()),
        None
    );
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
fn a_bracket_and_a_time_only_target_are_valid() {
    assert!(config().validate(rule(), &quotes()).is_ok());
    assert!(
        time_only(SignedDuration::from_hours(1))
            .validate(rule(), &quotes())
            .is_ok()
    );
}

/// `Trade::send_as` has both exits or neither: one alone is refused, by
/// `validate` and by `Targets::new`, a dataset run sending no order of it.
#[test]
fn one_exit_alone_is_invalid() {
    for spec in [
        Target {
            stop: None,
            ..in_bps(20, 10)
        },
        Target {
            take_profit: None,
            ..in_bps(20, 10)
        },
    ] {
        let err = of(spec).validate(rule(), &quotes()).unwrap_err();
        let ConfigError::InvalidValue { field, .. } = err else {
            panic!("not one invalid value: {err:?}");
        };
        assert_eq!(field, "model.target");
        assert!(Targets::new(of(spec), rule(), &quotes()).is_err());
    }
}

#[test]
fn sizes_distances_and_times_must_be_positive() {
    let invalid = [
        TargetConfig {
            quantity: Quantity::from(0),
            ..config()
        },
        of(in_bps(0, 10)),
        of(in_bps(10, -1)),
        of(Target {
            stop: Some(Distance::Atr(Decimal::ZERO)),
            ..bracket()
        }),
        of(Target {
            limit: SignedDuration::ZERO,
            ..bracket()
        }),
        time_only(SignedDuration::from_mins(-5)),
    ];

    for config in invalid {
        assert!(config.validate(rule(), &quotes()).is_err(), "{config:?}");
        assert!(Targets::new(config, rule(), &quotes()).is_err());
    }
}

/// The ATR of no bars, which this test refused as an error before the
/// target's ATR took Nautilus' arguments: Nautilus' `AverageTrueRange::new`
/// checks them as it builds the ATR, panicking on one it refuses, and
/// validating builds it, so the load stops there.
#[test]
#[should_panic(expected = "period")]
fn an_atr_of_no_bars_panics_as_validated() {
    let config = of(Target {
        atr: Some(AtrArgs {
            period: 0,
            ..DEFAULT_ATR
        }),
        ..bracket()
    });
    let _ = config.validate(rule(), &quotes());
}

/// The ATR's arguments are Nautilus' `AverageTrueRange`'s, by their names,
/// the moving average by Nautilus' name of it, case aside.
#[test]
fn an_atr_reads_nautilus_arguments_from_toml() {
    let toml = r#"
        take_profit = { atr = 2 }
        stop = { atr = 1.5 }
        limit = "2h"
        atr = { period = 20, ma_type = "Exponential", use_previous = false, value_floor = 0.25 }
    "#;

    let atr = toml::from_str::<Target>(toml).unwrap().atr;

    assert_eq!(
        atr,
        Some(AtrArgs {
            period: 20,
            ma_type: Some(MovingAverageType::Exponential),
            use_previous: Some(false),
            value_floor: Some(0.25),
        })
    );
}

/// What the target trades is the strategy's `[trade]`: the table naming
/// it is refused.
#[test]
fn a_table_naming_an_instrument_or_a_quantity_is_refused() {
    for key in [r#"instrument_id = "XAUUSD.ICMARKETS""#, r#"quantity = "1""#] {
        let toml = format!("{key}\nlimit = \"1h\"");
        let err = toml::from_str::<Target>(&toml).unwrap_err();
        let name = key.split(' ').next().unwrap();
        assert!(err.to_string().contains(name), "{err}");
    }
}

/// The table trades the instrument and size it is given.
#[test]
fn a_table_trades_the_instrument_and_size_it_is_given() {
    let table = toml::from_str::<Target>("limit = \"1h\"").unwrap();
    let traded = TradedInstrument {
        instrument_id: InstrumentId::from("EURUSD.ICMARKETS"),
        trade_size: Quantity::from(3),
    };

    let target = table.trading(&traded);

    assert_eq!(target.instrument_id, traded.instrument_id);
    assert_eq!(target.quantity, traded.trade_size);
    assert_eq!(target.settles_within(), SignedDuration::from_hours(1));
}

#[test]
fn an_atr_table_is_refused_unless_a_distance_uses_the_atr() {
    let valid = [
        of(in_bps(20, 10)),
        of(Target {
            take_profit: Some(Distance::Atr(Decimal::from(2))),
            ..bracket()
        }),
    ];
    let unused = of(Target {
        atr: bracket().atr,
        ..in_bps(20, 10)
    });

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

/// The take profit and stop of the long and the short, from `target`
/// at `reference` with `atr`, as text.
fn exits(target: Target, reference: &str, atr: Option<f64>) -> Vec<(String, String)> {
    let reference = Decimal::from_str_exact(reference).unwrap();
    target
        .exits(&half_point(), reference, atr)
        .unwrap()
        .expect("exits off the reference")
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
    let target = Target {
        take_profit: Some(Distance::BasisPoints(Decimal::new(125, 1))),
        ..in_bps(10, 10)
    };

    assert_eq!(
        exits(target, "1000.0", None),
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
    let target = Target {
        take_profit: Some(Distance::Atr(Decimal::new(15, 1))),
        stop: Some(Distance::Atr(Decimal::from(2))),
        ..bracket()
    };

    assert_eq!(
        exits(target, "1000.0", Some(2.1)),
        [
            ("1003.0".into(), "996.0".into()),
            ("997.0".into(), "1004.0".into())
        ]
    );
}

/// A tiny ATR of 0.1 on half-point ticks from 1000.0: a stop of 1 ATR,
/// 0.1, is nearer 1000.0 than 999.5, and a take profit of 1.5 ATRs,
/// 0.15, nearer 1000.0 than 1000.5. An exit on the decision's price is no
/// distance, a P&L in R of no stop, so neither is a target, as with a
/// cold ATR; 10 ATRs, 1.0, are.
#[test]
fn an_exit_rounding_onto_the_reference_gives_no_exits() {
    let exits_of = |take_profit: Decimal, stop: Decimal| {
        Target {
            take_profit: Some(Distance::Atr(take_profit)),
            stop: Some(Distance::Atr(stop)),
            ..bracket()
        }
        .exits(&half_point(), Decimal::from(1000), Some(0.1))
        .unwrap()
    };

    assert_eq!(exits_of(Decimal::from(10), Decimal::ONE), None);
    assert_eq!(exits_of(Decimal::new(15, 1), Decimal::from(10)), None);
    assert!(exits_of(Decimal::from(10), Decimal::from(10)).is_some());
}

/// A time-only target sets no exit, and so rounds none onto the reference.
#[test]
fn a_time_only_target_has_no_exits() {
    let target = time_only(SignedDuration::from_hours(1)).spec;

    assert_eq!(
        exits(target, "1000.0", None),
        Vec::<(String, String)>::new()
    );
}

#[test]
fn the_fingerprint_changes_with_every_target_spec() {
    let base = last_mid(InstrumentId::from(GOLD))
        .build()
        .unwrap()
        .fingerprint();
    let changed = [
        TargetConfig {
            quantity: Quantity::from(2),
            ..config()
        },
        of(Target {
            take_profit: Some(Distance::BasisPoints(Decimal::from(21))),
            ..bracket()
        }),
        of(Target {
            take_profit: Some(Distance::Atr(Decimal::from(20))),
            ..bracket()
        }),
        of(Target {
            stop: Some(Distance::Atr(Decimal::from(2))),
            ..bracket()
        }),
        of(Target {
            atr: Some(AtrArgs {
                period: 15,
                ..DEFAULT_ATR
            }),
            ..bracket()
        }),
        of(Target {
            atr: Some(AtrArgs {
                ma_type: Some(MovingAverageType::Simple),
                ..AtrArgs {
                    period: 14,
                    ..DEFAULT_ATR
                }
            }),
            ..bracket()
        }),
        of(Target {
            atr: Some(AtrArgs {
                use_previous: Some(false),
                ..AtrArgs {
                    period: 14,
                    ..DEFAULT_ATR
                }
            }),
            ..bracket()
        }),
        of(Target {
            atr: Some(AtrArgs {
                value_floor: Some(0.5),
                ..AtrArgs {
                    period: 14,
                    ..DEFAULT_ATR
                }
            }),
            ..bracket()
        }),
        of(Target {
            limit: SignedDuration::from_hours(3),
            ..bracket()
        }),
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

/// A trade with exits is not one closed at its time alone, whatever the
/// limit they share, nor one closed at another time: rows and models of
/// one are never read for the other.
#[test]
fn a_time_only_and_a_bracket_target_of_one_limit_and_two_limits_are_keyed_apart() {
    let base = Fingerprint::default();
    let keyed = |config: &TargetConfig| fingerprint(base, config, rule(), &quotes()).unwrap();
    let limit = SignedDuration::from_hours(1);
    let time = time_only(limit);
    let with_exits = of(Target {
        limit,
        ..in_bps(20, 10)
    });

    assert_ne!(keyed(&time), keyed(&with_exits));
    assert_ne!(keyed(&time), keyed(&time_only(limit * 2)));
    assert_ne!(
        keyed(&with_exits),
        keyed(&of(Target {
            limit: limit * 2,
            ..in_bps(20, 10)
        }))
    );
    assert_eq!(keyed(&time), keyed(&time_only(limit)));
}

/// The ATR is fingerprinted as it runs: left to its defaults, its period
/// and Nautilus' arguments, it keys the same target as one written out.
#[test]
fn the_fingerprint_reads_the_atrs_default_period_as_written_out() {
    let base = last_mid(InstrumentId::from(GOLD))
        .build()
        .unwrap()
        .fingerprint();
    let default = of(Target {
        atr: None,
        ..bracket()
    });
    let written_out = of(Target {
        atr: Some(AtrArgs {
            period: 14,
            ma_type: Some(MovingAverageType::Wilder),
            use_previous: Some(true),
            value_floor: Some(0.0),
        }),
        ..bracket()
    });

    assert_eq!(
        fingerprint(base, &default, rule(), &quotes()).unwrap(),
        fingerprint(base, &config(), rule(), &quotes()).unwrap()
    );
    assert_eq!(
        fingerprint(base, &written_out, rule(), &quotes()).unwrap(),
        fingerprint(base, &config(), rule(), &quotes()).unwrap()
    );
}

/// The ATR's bars are built from what the strategy reads: the same config
/// on quotes, on trades and on bars keys three targets, whose rows differ.
/// A target without an ATR is keyed alike whatever the strategy reads.
#[test]
fn the_source_of_the_atrs_bars_changes_the_fingerprint() {
    let base = Fingerprint::default();
    let keyed = |config: &TargetConfig, requirements: &DataRequirements| {
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
    let bps = of(in_bps(20, 10));
    assert_eq!(keyed(&bps, &quotes()), keyed(&bps, &trades));
    let time = time_only(SignedDuration::from_hours(1));
    assert_eq!(keyed(&time, &quotes()), keyed(&time, &trades));
}

#[test]
fn a_decision_settles_within_its_limit() {
    let time = time_only(SignedDuration::from_hours(1));

    assert_eq!(config().settles_within(), SignedDuration::from_hours(2));
    assert_eq!(time.settles_within(), SignedDuration::from_hours(1));
}

/// The row of `outcomes`.
fn row_of(outcomes: Outcomes) -> TargetRow {
    TargetRow {
        decision: FeatureRow {
            strategy_id: StrategyId::from("S-001"),
            features: vec![1.0],
            ts_event: UnixNanos::from(1),
            ts_init: UnixNanos::from(1),
        },
        outcomes,
        settled_at: UnixNanos::from(2),
    }
}

/// The outcome of a position closed by `exit` with `pnl`, net of 0.12 USD,
/// its `distances`.
fn outcome(pnl: &str, exit: Exit, distances: Option<Distances>) -> Outcome {
    Outcome {
        pnl: Money::from(pnl),
        commission: Money::from("0.12 USD"),
        exit,
        distances,
    }
}

#[test]
fn a_bracket_has_a_column_per_value_of_each_side_its_distances_among_them() {
    let distances = |take_profit, stop| {
        Some(Distances {
            take_profit: Decimal::new(take_profit, 2),
            stop: Decimal::new(stop, 2),
        })
    };
    let row = row_of(Outcomes {
        long: outcome("2.5 USD", Exit::TakeProfit, distances(300, 150)),
        short: outcome("-3.5 USD", Exit::Stop, distances(325, 175)),
    });

    assert_eq!(
        config().target_names(),
        [
            "long_pnl",
            "long_commission",
            "long_exit",
            "long_take_profit",
            "long_stop",
            "short_pnl",
            "short_commission",
            "short_exit",
            "short_take_profit",
            "short_stop",
        ]
    );
    assert_eq!(
        row.values(),
        [2.5, 0.12, 1.0, 3.0, 1.5, -3.5, 0.12, -1.0, 3.25, 1.75]
    );
}

#[test]
fn a_time_only_target_has_the_pnl_commission_and_exit_columns_of_each_side() {
    let row = row_of(Outcomes {
        long: outcome("-0.25 USD", Exit::Time, None),
        short: outcome("0.25 USD", Exit::Time, None),
    });

    assert_eq!(
        time_only(SignedDuration::from_hours(1)).target_names(),
        [
            "long_pnl",
            "long_commission",
            "long_exit",
            "short_pnl",
            "short_commission",
            "short_exit",
        ]
    );
    assert_eq!(row.values(), [-0.25, 0.12, 0.0, 0.25, 0.12, 0.0]);
}

#[test]
fn an_outcome_has_the_sign_of_its_pnl() {
    let sign = |pnl: &str| {
        Outcome {
            pnl: Money::from(pnl),
            commission: Money::from("0.1 USD"),
            exit: Exit::Time,
            distances: None,
        }
        .sign()
    };

    assert_eq!(sign("0.0005 USDT"), 1);
    assert_eq!(sign("-0.44 USD"), -1);
    assert_eq!(sign("0 USD"), 0);
}

#[test]
fn a_sides_pnl_commission_and_stop_are_its_target_names_among_a_rows_values() {
    let names = config().target_names();
    let pnl = |side| names[config().pnl_index(side)].clone();
    let commission = |side| names[config().commission_index(side)].clone();
    let stop = |side| names[config().stop_index(side).unwrap()].clone();
    let time = time_only(SignedDuration::from_hours(1));

    assert_eq!(pnl(Side::Long), "long_pnl");
    assert_eq!(pnl(Side::Short), "short_pnl");
    assert_eq!(commission(Side::Short), "short_commission");
    assert_eq!(commission(Side::Long), "long_commission");
    assert_eq!(stop(Side::Short), "short_stop");
    assert_eq!(time.stop_index(Side::Long), None);
    assert_eq!(time.pnl_index(Side::Long), 0);
    assert_eq!(time.pnl_index(Side::Short), 3);
    assert_eq!(time.commission_index(Side::Short), 4);
}

/// 1000 AUD/USD with a stop 10 pips away: 1 USD at risk.
#[test]
fn a_stop_is_worth_its_distance_on_the_targets_quantity() {
    let instrument = InstrumentAny::CurrencyPair(nautilus_model::instruments::stubs::audusd_sim());

    let worth = stop_worth(&instrument, Quantity::from(1000), 0.001).unwrap();

    assert!((worth - 1.0).abs() < 1e-9, "{worth}");
}

/// Bybit's inverse BTCUSD: Nautilus' notional value of a distance on it
/// divides by the price, which is not what the stop risks.
#[test]
fn a_stop_on_an_inverse_instrument_is_refused() {
    let instrument =
        InstrumentAny::CryptoPerpetual(nautilus_model::instruments::stubs::btcusd_bybit());

    let err = stop_worth(&instrument, Quantity::from(1), 10.0).unwrap_err();

    assert!(err.to_string().contains("inverse"), "{err}");
}
