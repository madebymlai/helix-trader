use std::str::FromStr;

use nautilus_common::{
    cache::Cache,
    clock::{Clock, VirtualClock},
    timer::{TimeEvent, TimeEventCallback},
};
use nautilus_indicators::average::MovingAverageType;
use nautilus_model::{
    data::NautilusDataType,
    identifiers::{Symbol, TraderId},
    instruments::{
        CryptoPerpetual, InstrumentAny,
        stubs::{audusd_sim, btcusd_bybit},
    },
    types::Currency,
};
use nautilus_portfolio::portfolio::Portfolio;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
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

/// A take profit 20 bps away, a stop 1.5 ATRs of 14 bars.
fn exits_of_config() -> BracketSpec {
    BracketSpec {
        take_profit: Distance::BasisPoints(Decimal::from(20)),
        stop: Distance::Atr(Decimal::new(15, 1)),
        atr: Some(AtrArgs {
            period: 14,
            ..DEFAULT_ATR
        }),
    }
}

/// A bracket, `exits_of_config`, closed after 2 hours.
fn config() -> TargetConfig {
    TargetConfig::builder()
        .instrument_id(InstrumentId::from(GOLD))
        .quantity(Quantity::from(1))
        .spec(Target {
            bracket: Some(exits_of_config()),
            limit: SignedDuration::from_hours(2),
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

/// The target of `config` with the exits `exits`.
fn exiting(exits: BracketSpec) -> Target {
    Target {
        bracket: Some(exits),
        ..bracket()
    }
}

/// The time-only target of `config`'s instrument and size, closed after
/// `limit`.
fn time_only(limit: SignedDuration) -> TargetConfig {
    of(Target::builder().limit(limit).build())
}

/// The target of `config` with both distances in basis points and no
/// ATR.
fn in_bps(take_profit: i64, stop: i64) -> Target {
    exiting(BracketSpec {
        take_profit: Distance::BasisPoints(Decimal::from(take_profit)),
        stop: Distance::BasisPoints(Decimal::from(stop)),
        atr: None,
    })
}

/// The ATR's bars are the decision cadence's: naming them is refused.
#[test]
fn a_target_atr_naming_its_bar_type_is_refused_at_load() {
    let toml = r#"
        limit = "2h"
        [bracket]
        take_profit = { atr = 2 }
        stop = { atr = 1.5 }
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
    let default = of(exiting(BracketSpec {
        atr: None,
        ..exits_of_config()
    }));
    let overridden = of(exiting(BracketSpec {
        atr: Some(AtrArgs {
            period: 20,
            ..DEFAULT_ATR
        }),
        ..exits_of_config()
    }));

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

/// A bracket has both exits: one alone does not parse.
#[test]
fn one_exit_alone_does_not_parse() {
    for (toml, missing) in [
        ("bracket = { take_profit = { bps = 20 } }", "stop"),
        ("bracket = { stop = { bps = 10 } }", "take_profit"),
    ] {
        let err = toml::from_str::<Target>(&format!("{toml}\nlimit = \"2h\"")).unwrap_err();

        assert!(
            err.to_string()
                .contains(&format!("missing field `{missing}`")),
            "{err}"
        );
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
        of(exiting(BracketSpec {
            stop: Distance::Atr(Decimal::ZERO),
            ..exits_of_config()
        })),
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
    let config = of(exiting(BracketSpec {
        atr: Some(AtrArgs {
            period: 0,
            ..DEFAULT_ATR
        }),
        ..exits_of_config()
    }));
    let _ = config.validate(rule(), &quotes());
}

/// The ATR's arguments are Nautilus' `AverageTrueRange`'s, by their names,
/// the moving average by Nautilus' name of it, case aside.
#[test]
fn an_atr_reads_nautilus_arguments_from_toml() {
    let toml = r#"
        limit = "2h"
        [bracket]
        take_profit = { atr = 2 }
        stop = { atr = 1.5 }
        atr = { period = 20, ma_type = "Exponential", use_previous = false, value_floor = 0.25 }
    "#;

    let atr = toml::from_str::<Target>(toml).unwrap().bracket.unwrap().atr;

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
        of(exiting(BracketSpec {
            take_profit: Distance::Atr(Decimal::from(2)),
            ..exits_of_config()
        })),
    ];
    let unused = of(exiting(BracketSpec {
        take_profit: Distance::BasisPoints(Decimal::from(20)),
        stop: Distance::BasisPoints(Decimal::from(10)),
        ..exits_of_config()
    }));

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
fn exits(target: Target, reference: &str, atr: Option<f64>) -> [(String, String); 2] {
    let reference = Decimal::from_str_exact(reference).unwrap();
    let exits = super::exits(target.bracket.unwrap(), &half_point(), reference, atr)
        .unwrap()
        .expect("exits off the reference");
    [exits.long, exits.short].map(|bracket| {
        (
            bracket.take_profit.to_string(),
            bracket.stop_loss.to_string(),
        )
    })
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
    let target = exiting(BracketSpec {
        take_profit: Distance::BasisPoints(Decimal::new(125, 1)),
        stop: Distance::BasisPoints(Decimal::from(10)),
        atr: None,
    });

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
    let target = exiting(BracketSpec {
        take_profit: Distance::Atr(Decimal::new(15, 1)),
        stop: Distance::Atr(Decimal::from(2)),
        ..exits_of_config()
    });

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
        let bracket = BracketSpec {
            take_profit: Distance::Atr(take_profit),
            stop: Distance::Atr(stop),
            atr: None,
        };
        super::exits(bracket, &half_point(), Decimal::from(1000), Some(0.1)).unwrap()
    };

    assert_eq!(exits_of(Decimal::from(10), Decimal::ONE), None);
    assert_eq!(exits_of(Decimal::new(15, 1), Decimal::from(10)), None);
    assert!(exits_of(Decimal::from(10), Decimal::from(10)).is_some());
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
        of(exiting(BracketSpec {
            take_profit: Distance::BasisPoints(Decimal::from(21)),
            ..exits_of_config()
        })),
        of(exiting(BracketSpec {
            take_profit: Distance::Atr(Decimal::from(20)),
            ..exits_of_config()
        })),
        of(exiting(BracketSpec {
            stop: Distance::Atr(Decimal::from(2)),
            ..exits_of_config()
        })),
        of(exiting(BracketSpec {
            atr: Some(AtrArgs {
                period: 15,
                ..DEFAULT_ATR
            }),
            ..exits_of_config()
        })),
        of(exiting(BracketSpec {
            atr: Some(AtrArgs {
                ma_type: Some(MovingAverageType::Simple),
                ..AtrArgs {
                    period: 14,
                    ..DEFAULT_ATR
                }
            }),
            ..exits_of_config()
        })),
        of(exiting(BracketSpec {
            atr: Some(AtrArgs {
                use_previous: Some(false),
                ..AtrArgs {
                    period: 14,
                    ..DEFAULT_ATR
                }
            }),
            ..exits_of_config()
        })),
        of(exiting(BracketSpec {
            atr: Some(AtrArgs {
                value_floor: Some(0.5),
                ..AtrArgs {
                    period: 14,
                    ..DEFAULT_ATR
                }
            }),
            ..exits_of_config()
        })),
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
    let default = of(exiting(BracketSpec {
        atr: None,
        ..exits_of_config()
    }));
    let written_out = of(exiting(BracketSpec {
        atr: Some(AtrArgs {
            period: 14,
            ma_type: Some(MovingAverageType::Wilder),
            use_previous: Some(true),
            value_floor: Some(0.0),
        }),
        ..exits_of_config()
    }));

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

#[test]
fn an_outcome_has_the_sign_of_its_pnl() {
    let sign = |pnl: &str| {
        Outcome {
            pnl: Money::from(pnl),
            commission: Money::from("0.1 USD"),
            exit: Exit::Time,
            unit: 1.0,
        }
        .sign()
    };

    assert_eq!(sign("0.0005 USDT"), 1);
    assert_eq!(sign("-0.44 USD"), -1);
    assert_eq!(sign("0 USD"), 0);
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

/// A strategy registered without a node, as the handle's tests do.
#[derive(Debug)]
struct Probe {
    core: StrategyCore,
}

nautilus_strategy!(Probe);

impl DataActor for Probe {}

impl Probe {
    /// The probe, `instrument` in its cache if given, and no price yet.
    fn holding(instrument: Option<InstrumentAny>) -> Self {
        let mut probe = Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from("S-001")),
                order_id_tag: Some("001".to_string()),
                ..Default::default()
            }),
        };
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        clock
            .borrow_mut()
            .register_default_handler(TimeEventCallback::from(|_: TimeEvent| {}));
        let cache = Rc::new(RefCell::new(Cache::default()));
        if let Some(instrument) = instrument {
            cache.borrow_mut().add_instrument(instrument).unwrap();
        }
        let portfolio = Rc::new(RefCell::new(Portfolio::new(
            clock.clone(),
            cache.clone(),
            None,
        )));
        probe
            .core
            .register(TraderId::from("HELIX-001"), clock, cache, portfolio)
            .unwrap();
        probe
    }

    /// Adds a quote of `instrument_id` of the mid `mid` to its cache, a tick
    /// either side on five places.
    fn quoted(&self, instrument_id: InstrumentId, mid: &str) {
        let mid = Decimal::from_str_exact(mid).unwrap();
        let tick = Decimal::new(1, 5);
        let price = |value: Decimal| Price::from(value.to_string().as_str());
        self.cache_rc()
            .borrow_mut()
            .add_quote(QuoteTick::new(
                instrument_id,
                price(mid - tick),
                price(mid + tick),
                Quantity::from(1),
                Quantity::from(1),
                UnixNanos::default(),
                UnixNanos::default(),
            ))
            .unwrap();
    }

    /// The client order ids of the orders it sent, in their order.
    fn orders(&self) -> Vec<String> {
        let cache = self.cache();
        let mut orders: Vec<String> = cache
            .orders(None, None, None, None, None)
            .iter()
            .map(|order| order.client_order_id().to_string())
            .collect();
        orders.sort();
        orders
    }
}

const AUD: &str = "AUD/USD.SIM";

/// AUD/USD, which `Probe::holding` takes.
fn aud() -> Option<InstrumentAny> {
    Some(InstrumentAny::CurrencyPair(audusd_sim()))
}

/// Bybit's inverse BTCUSD, settled in BTC.
fn inverse() -> InstrumentAny {
    InstrumentAny::CryptoPerpetual(btcusd_bybit())
}

/// `spec`, of `quantity` of `instrument_id`.
fn trading(spec: Target, instrument_id: InstrumentId, quantity: u64) -> TargetConfig {
    TargetConfig::builder()
        .instrument_id(instrument_id)
        .quantity(Quantity::from(quantity))
        .spec(spec)
        .build()
}

/// `spec` of 1000 AUD/USD.
fn on_aud(spec: Target) -> TargetConfig {
    trading(spec, InstrumentId::from(AUD), 1000)
}

/// A target closed after an hour, with no exits.
fn no_exits() -> Target {
    Target::builder()
        .limit(SignedDuration::from_hours(1))
        .build()
}

/// A take profit 20 bps away and a stop a tenth of a basis point away: on
/// AUD/USD's ticks of 0.00001, a tick from 0.70000, none from 0.07000.
fn tight_stop() -> Target {
    exiting(BracketSpec {
        take_profit: Distance::BasisPoints(Decimal::from(20)),
        stop: Distance::BasisPoints(Decimal::new(1, 1)),
        atr: None,
    })
}

fn setter(config: &TargetConfig) -> TargetSetter {
    TargetSetter::new(config, rule(), &quotes()).unwrap()
}

fn targets(config: TargetConfig) -> Rc<Targets> {
    Targets::new(config, rule(), &quotes()).unwrap()
}

/// A decision of the probe, as `decide` publishes it.
fn decision() -> FeatureRow {
    FeatureRow {
        strategy_id: StrategyId::from("S-001"),
        features: vec![1.0],
        ts_event: UnixNanos::from(1),
        ts_init: UnixNanos::from(1),
    }
}

/// From the mid of 0.70000, 10 bps is 0.0007, and on 1000 AUD/USD that stop
/// is worth 0.70 USD; 20 bps is 0.0014. The long's stop is below the price, its
/// take profit above; the short's the reverse.
#[test]
fn a_bracket_is_set_on_each_side_with_the_worth_of_its_stop() {
    let probe = Probe::holding(aud());
    probe.quoted(InstrumentId::from(AUD), "0.70000");

    let set = setter(&on_aud(in_bps(20, 10))).at(&probe).unwrap().unwrap();

    assert_eq!(set.long.reference, Price::from("0.70000"));
    assert_eq!(
        set.long.bracket,
        Some(Bracket {
            take_profit: Price::from("0.70140"),
            stop_loss: Price::from("0.69930"),
        })
    );
    assert_eq!(
        set.short.bracket,
        Some(Bracket {
            take_profit: Price::from("0.69860"),
            stop_loss: Price::from("0.70070"),
        })
    );
    for unit in [set.long.unit, set.short.unit] {
        assert!((unit - 0.70).abs() < 1e-9, "{unit}");
    }
}

/// With no exits there is no stop to weigh: the unit is 1, and no instrument
/// is read, so one missing from the cache refuses nothing.
#[test]
fn a_time_only_target_is_set_with_a_unit_of_one_and_reads_no_instrument() {
    let probe = Probe::holding(None);
    probe.quoted(InstrumentId::from(AUD), "0.70000");

    let set = setter(&on_aud(no_exits())).at(&probe).unwrap().unwrap();

    assert_eq!(set.long.unit, 1.0);
    assert_eq!(set.short.unit, 1.0);
    assert_eq!(set.long.bracket, None);
    assert_eq!(set.short.bracket, None);
    assert_eq!(set.long.reference, Price::from("0.70000"));
}

#[test]
fn a_target_with_no_price_to_set_it_from_says_so() {
    let probe = Probe::holding(aud());

    for spec in [in_bps(20, 10), no_exits()] {
        let set = setter(&on_aud(spec)).at(&probe).unwrap();

        assert_eq!(set, Err(NoTarget::NoPrice), "{spec:?}");
    }
}

#[test]
fn a_target_on_a_cold_atr_says_so_and_names_its_bars() {
    let probe = Probe::holding(None);

    let set = setter(&config()).at(&probe).unwrap();

    assert_eq!(
        set,
        Err(NoTarget::ColdAtr(BarType::from(
            "XAUUSD.ICMARKETS-15-MINUTE-MID-INTERNAL"
        )))
    );
}

/// From 0.07000, the stop's 0.0000007 rounds onto the price.
#[test]
fn a_target_with_an_exit_on_the_price_says_so_and_names_the_price() {
    let probe = Probe::holding(aud());
    probe.quoted(InstrumentId::from(AUD), "0.07000");

    let set = setter(&on_aud(tight_stop())).at(&probe).unwrap();

    assert_eq!(set, Err(NoTarget::ExitOnPrice(Price::from("0.07000"))));
}

/// Nautilus' notional value of an inverse instrument divides by the price, so
/// a stop's distance has no worth in R on it.
#[test]
fn a_bracket_on_an_inverse_instrument_is_refused_as_the_targets_start() {
    let instrument = inverse();
    let mut probe = Probe::holding(Some(instrument.clone()));
    let bracket = trading(in_bps(20, 10), instrument.id(), 1);

    let err = targets(bracket).start(&mut probe).unwrap_err();

    assert!(format!("{err:#}").contains("inverse"), "{err:#}");
}

/// In money, a time-only target has no stop to weigh, and works on an inverse
/// instrument.
#[test]
fn a_time_only_target_on_an_inverse_instrument_starts() {
    let instrument = inverse();
    let mut probe = Probe::holding(Some(instrument.clone()));
    let time_only = trading(no_exits(), instrument.id(), 1);

    targets(time_only).start(&mut probe).unwrap();
}

/// Only an instrument seen to be inverse is refused: one missing from the cache
/// is the error of each decision, as `TargetSetter::at` has it.
#[test]
fn a_bracket_on_an_instrument_not_in_the_cache_starts() {
    let mut probe = Probe::holding(None);

    targets(on_aud(in_bps(20, 10))).start(&mut probe).unwrap();
}

#[test]
fn a_bracket_on_a_linear_instrument_starts() {
    let mut probe = Probe::holding(aud());

    targets(on_aud(in_bps(20, 10))).start(&mut probe).unwrap();
}

/// Past the start's refusal, a stop's worth failing is still an error before
/// any order, as an exit outside the instrument's prices is: no order sent.
#[test]
fn a_stop_with_no_worth_at_a_decision_is_an_error_before_any_order() {
    let instrument = inverse();
    let mut probe = Probe::holding(Some(instrument.clone()));
    probe.quoted(instrument.id(), "100000.00000");
    let targets = targets(trading(in_bps(20, 10), instrument.id(), 1));

    let err = targets.open(&mut probe, decision()).unwrap_err();

    assert!(format!("{err:#}").contains("inverse"), "{err:#}");
    assert_eq!(probe.orders(), Vec::<String>::new());
}

/// A time-only target reads no instrument to be set, so one missing surfaces
/// as the error of sending its orders, which takes the decision's number: the
/// next decision, the instrument there, is the second.
#[test]
fn a_time_only_target_missing_its_instrument_errs_as_it_sends_its_orders() {
    let mut probe = Probe::holding(None);
    probe.quoted(InstrumentId::from(AUD), "0.70000");
    let targets = targets(on_aud(no_exits()));

    let err = targets.open(&mut probe, decision()).unwrap_err();

    assert!(format!("{err:#}").contains("not in cache"), "{err:#}");
    probe
        .cache_rc()
        .borrow_mut()
        .add_instrument(aud().unwrap())
        .unwrap();
    targets.open(&mut probe, decision()).unwrap();
    assert_eq!(probe.orders(), ["D1-L-E", "D1-S-E"]);
}

/// A decision with no price is unsettled, its number taken; one with an exit on
/// the price is none, as on cold features, and takes no number: the first
/// orders are of the decision after the first.
#[test]
fn a_decision_with_no_price_takes_a_number_and_one_with_an_exit_on_the_price_none() {
    let mut probe = Probe::holding(aud());
    let aud = InstrumentId::from(AUD);
    let targets = targets(on_aud(tight_stop()));

    targets.open(&mut probe, decision()).unwrap();
    assert_eq!(probe.orders(), Vec::<String>::new());
    probe.quoted(aud, "0.07000");
    targets.open(&mut probe, decision()).unwrap();
    assert_eq!(probe.orders(), Vec::<String>::new());
    probe.quoted(aud, "0.70000");
    targets.open(&mut probe, decision()).unwrap();

    assert_eq!(
        probe.orders(),
        [
            "D1-L-E", "D1-L-SL", "D1-L-TP", "D1-S-E", "D1-S-SL", "D1-S-TP"
        ]
    );
}
