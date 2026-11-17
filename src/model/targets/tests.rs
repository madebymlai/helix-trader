use std::str::FromStr;

use jiff::Timestamp;
use nautilus_common::{
    cache::Cache,
    clock::{Clock, VirtualClock},
    timer::{TimeEvent, TimeEventCallback},
};
use nautilus_indicators::average::MovingAverageType;
use nautilus_model::{
    data::NautilusDataType,
    enums::AggressorSide,
    identifiers::{Symbol, TradeId, TraderId, Venue},
    instruments::{
        CryptoPerpetual, InstrumentAny,
        stubs::{audusd_sim, btcusd_bybit, default_fx_ccy},
    },
    types::Currency,
};
use nautilus_portfolio::portfolio::Portfolio;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rstest::rstest;

use super::*;
use crate::{
    model::{
        blueprint::ModelBlueprint,
        config::ModelConfig,
        dataset::DatasetRow,
        deciding::testing::{self, GOLD, gold},
        earnings::Example,
        features::testing::last_mid,
    },
    strategy::requirements::TickSeries,
};

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

/// The tests' reference stake: 0.3% of the dataset balance, 100,000 USD
/// (`venue_of`), 300 USD.
fn risk() -> Risk {
    "0.3%".parse().unwrap()
}

/// The venue of the tests' dataset runs at `commission`: 100,000 USD.
fn venue_of(commission: Commission) -> DatasetVenue {
    DatasetVenue {
        commission,
        balance: Money::from("100000 USD"),
        latency: None,
    }
}

/// A bracket, `exits_of_config`, closed after 2 hours, at the tests'
/// reference stake.
fn config() -> TargetConfig {
    TargetConfig::builder()
        .instrument_id(InstrumentId::from(GOLD))
        .risk(risk())
        .spec(Target {
            bracket: Some(exits_of_config()),
            limit: SignedDuration::from_hours(2),
            atr: None,
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

/// The time-only target of `config`'s instrument and risk, closed after
/// `limit`, its unit measured in Wilder's ATR of 14 bars.
fn time_only(limit: SignedDuration) -> TargetConfig {
    of(Target::builder().limit(limit).atr(DEFAULT_ATR).build())
}

/// The ATR of `period` bars, Nautilus' defaults besides.
fn atr_of(period: usize) -> AtrArgs {
    AtrArgs {
        period,
        ..DEFAULT_ATR
    }
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
#[rstest]
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
        custom: vec![],
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
    assert!(Targets::new(config(), rule, requirements, &venue_of(commission())).is_err());
    format!("{:#}", config().validate(rule, requirements).unwrap_err())
}

/// The ATR's bars are of the decision cadence, in its largest whole unit,
/// on the instrument's mids where the strategy reads its quotes. Quotes read
/// through bars built from them count as quotes read, and come first: with
/// trades read beside them, the ATR is still on the mids. On trades alone it
/// is on the last prices. On bars alone, the ATR's bars are Nautilus'
/// composite bars of the cadence built from the source bars the strategy
/// reads, priced as they are; of several, those whose length divides the
/// cadence, the longest: the fewest source bars to a bar of the cadence.
#[rstest]
#[case::quarters(quotes(), every(15), "XAUUSD.ICMARKETS-15-MINUTE-MID-INTERNAL")]
#[case::hours(quotes(), every(60), "XAUUSD.ICMARKETS-1-HOUR-MID-INTERNAL")]
#[case::two_hours(quotes(), every(120), "XAUUSD.ICMARKETS-2-HOUR-MID-INTERNAL")]
#[case::days(quotes(), every(24 * 60), "XAUUSD.ICMARKETS-1-DAY-MID-INTERNAL")]
#[case::seconds(
    quotes(),
    DecisionRule::Every(SignedDuration::from_secs(30)),
    "XAUUSD.ICMARKETS-30-SECOND-MID-INTERNAL"
)]
#[case::milliseconds(
    quotes(),
    DecisionRule::Every(SignedDuration::from_millis(250)),
    "XAUUSD.ICMARKETS-250-MILLISECOND-MID-INTERNAL"
)]
#[case::bars_of_quotes(
    reading(&[], &["XAUUSD.ICMARKETS-1-HOUR-BID-INTERNAL"]),
    rule(),
    "XAUUSD.ICMARKETS-15-MINUTE-MID-INTERNAL"
)]
#[case::quotes_and_trades(
    reading(&["trades", "quotes"], &[]),
    rule(),
    "XAUUSD.ICMARKETS-15-MINUTE-MID-INTERNAL"
)]
#[case::trades(
    reading(&["trades"], &[]),
    rule(),
    "XAUUSD.ICMARKETS-15-MINUTE-LAST-INTERNAL"
)]
#[case::bars_of_trades(
    reading(&[], &["XAUUSD.ICMARKETS-1-HOUR-LAST-INTERNAL"]),
    rule(),
    "XAUUSD.ICMARKETS-15-MINUTE-LAST-INTERNAL"
)]
#[case::source_bars(
    reading(&[], &["XAUUSD.ICMARKETS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"]),
    rule(),
    "XAUUSD.ICMARKETS-15-MINUTE-LAST-INTERNAL@1-MINUTE-EXTERNAL"
)]
#[case::source_bars_hourly(
    reading(&[], &["XAUUSD.ICMARKETS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"]),
    every(60),
    "XAUUSD.ICMARKETS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"
)]
#[case::longest_of_five_minutes(
    several_source_bars(),
    rule(),
    "XAUUSD.ICMARKETS-15-MINUTE-ASK-INTERNAL@5-MINUTE-EXTERNAL"
)]
#[case::longest_of_an_hour(
    several_source_bars(),
    every(120),
    "XAUUSD.ICMARKETS-2-HOUR-LAST-INTERNAL@1-HOUR-EXTERNAL"
)]
#[case::longest_of_a_minute(
    several_source_bars(),
    every(3),
    "XAUUSD.ICMARKETS-3-MINUTE-BID-INTERNAL@1-MINUTE-EXTERNAL"
)]
fn the_atr_is_on_the_instruments_prices_built_at_the_decision_cadence(
    #[case] requirements: DataRequirements,
    #[case] rule: DecisionRule,
    #[case] expected: &str,
) {
    assert_eq!(atr_bar_type(rule, &requirements), expected);
}

/// Source bars of 1 minute, 5 minutes and an hour, each priced its own way.
fn several_source_bars() -> DataRequirements {
    reading(
        &[],
        &[
            "XAUUSD.ICMARKETS-1-HOUR-BID-INTERNAL@1-MINUTE-EXTERNAL",
            "XAUUSD.ICMARKETS-4-HOUR-ASK-INTERNAL@5-MINUTE-EXTERNAL",
            "XAUUSD.ICMARKETS-1-DAY-LAST-INTERNAL@1-HOUR-EXTERNAL",
        ],
    )
}

/// The ATR's bars are of the cadence: source bars it is not a whole
/// multiple of cannot build them, nor bars of no length. The refusal names
/// the cadence, 15 minutes, and the source bars.
#[rstest]
#[case::hours(
    "XAUUSD.ICMARKETS-1-DAY-LAST-INTERNAL@1-HOUR-EXTERNAL",
    "XAUUSD.ICMARKETS-1-HOUR-LAST-EXTERNAL"
)]
#[case::ticks(
    "XAUUSD.ICMARKETS-1-HOUR-LAST-INTERNAL@100-TICK-EXTERNAL",
    "XAUUSD.ICMARKETS-100-TICK-LAST-EXTERNAL"
)]
fn an_atr_on_bars_alone_not_dividing_the_cadence_is_refused_naming_both(
    #[case] bar_type: &str,
    #[case] source: &str,
) {
    let err = atr_refused(rule(), &reading(&[], &[bar_type]));

    assert!(err.contains("15m"), "{err}");
    assert!(err.contains(source), "{err}");
}

/// Neither quotes, trades nor bars of the target's instrument: nothing to
/// build the ATR's bars from, whatever else the strategy reads.
#[rstest]
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
        custom: vec![],
        warmup: SignedDuration::ZERO,
    };

    let err = atr_refused(rule(), &elsewhere);

    assert!(err.contains(GOLD), "{err}");
    assert!(err.contains("model.target.bracket.atr"), "{err}");
    assert!(of(in_bps(20, 10)).validate(rule(), &elsewhere).is_ok());
    let time = time_only(SignedDuration::from_hours(1));
    let err = format!("{:#}", time.validate(rule(), &elsewhere).unwrap_err());
    assert!(err.contains(GOLD), "{err}");
    assert!(err.contains("model.target.atr"), "{err}");
}

/// Wilder's period: an `atr` distance without an `atr` table has an ATR of
/// 14 bars, which `atr = { period = .. }` overrides.
#[rstest]
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
}

/// The ConfigError refusing `config` on `rule()` for a strategy reading the
/// quotes of gold, one alone: its field and its reason.
fn refused_alone(config: &TargetConfig) -> (String, String) {
    match config.validate(rule(), &quotes()).unwrap_err() {
        ConfigError::InvalidValue { field, reason } => (field, reason),
        err => panic!("not one invalid value: {err:?}"),
    }
}

/// The model of a strategy on the last mid of gold, reading its quotes,
/// deciding on `rule()` with `target`, which the blueprint checks as it is
/// built, before any target is run.
fn blueprint(target: TargetConfig) -> anyhow::Result<ModelBlueprint> {
    ModelBlueprint::new(
        StrategyId::from("TARGETS-001"),
        last_mid(InstrumentId::from(GOLD)).build().unwrap(),
        ModelConfig {
            decision: rule(),
            target,
            ..testing::config()
        },
        quotes(),
    )
}

/// A bracket's ATR is its own `atr` table's: one on the target beside it
/// would be a second, and is refused under the target's.
#[rstest]
fn an_atr_table_beside_a_bracket_is_refused_under_the_targets() {
    let config = of(Target {
        atr: Some(DEFAULT_ATR),
        ..bracket()
    });

    let (field, reason) = refused_alone(&config);

    assert_eq!(field, "model.target.atr");
    assert!(reason.contains("bracket"), "{reason}");
}

/// Nautilus builds only bars whose step divides the next unit
/// (`BarSpecification::new_checked`): neither 90 nor 7 minutes divide an hour.
/// The refusal names the cadence.
#[rstest]
#[case::ninety_minutes(90, "1h 30m")]
#[case::seven_minutes(7, "7m")]
fn an_atr_on_a_cadence_nautilus_builds_no_bars_of_is_refused(
    #[case] minutes: i64,
    #[case] cadence: &str,
) {
    let err = format!(
        "{:#}",
        config().validate(every(minutes), &quotes()).unwrap_err()
    );

    assert!(err.contains(&format!("cadence {cadence}")), "{err}");
}

#[rstest]
fn a_bracket_and_a_time_only_target_are_valid() {
    assert!(config().validate(rule(), &quotes()).is_ok());
    assert!(
        time_only(SignedDuration::from_hours(1))
            .validate(rule(), &quotes())
            .is_ok()
    );
}

/// A distance in ATRs or a limit not positive is refused under its field, a
/// time-only target's limit as a bracket's, and the blueprint built on the
/// target refuses it as well.
#[rstest]
#[case::stop_of_no_atr(
    of(exiting(BracketSpec {
        stop: Distance::Atr(Decimal::ZERO),
        ..exits_of_config()
    })),
    "model.target.bracket"
)]
#[case::bracket_of_no_limit(
    of(Target {
        limit: SignedDuration::ZERO,
        ..bracket()
    }),
    "model.target.limit"
)]
#[case::time_only_of_a_negative_limit(
    time_only(SignedDuration::from_mins(-5)),
    "model.target.limit"
)]
fn a_distance_or_a_limit_not_positive_is_refused_under_its_field(
    #[case] config: TargetConfig,
    #[case] field: &str,
) {
    let (refused, _) = refused_alone(&config);
    let err = format!("{:#}", blueprint(config).unwrap_err());

    assert_eq!(refused, field);
    assert!(err.contains(field), "{err}");
}

/// The ATR of no bars, which this test refused as an error before the
/// target's ATR took Nautilus' arguments: Nautilus' `AverageTrueRange::new`
/// checks them as it builds the ATR, panicking on one it refuses, and
/// validating builds it, so the load stops there.
#[rstest]
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

/// Each exit on the nearest half-point tick. 10 bps of 1000.0 is 1.0, on the
/// tick; 12 bps 1.2, nearer 1.0 than 1.5; 13 bps 1.3, nearer 1.5. 12.5 bps is
/// 1.25, half way between 1.0 and 1.5: a tie goes away from the reference.
/// 1.5 ATRs of 2.1 is 3.15, nearer 3.0 than 3.5; 2 ATRs 4.2, nearer 4.0.
#[rstest]
#[case::basis_points_on_and_below_a_half_tick(
    in_bps(10, 12),
    None,
    [("1001.0", "999.0"), ("999.0", "1001.0")]
)]
#[case::basis_points_above_a_half_tick(
    in_bps(13, 10),
    None,
    [("1001.5", "999.0"), ("998.5", "1001.0")]
)]
#[case::a_tie_between_two_ticks(
    exiting(BracketSpec {
        take_profit: Distance::BasisPoints(Decimal::new(125, 1)),
        stop: Distance::BasisPoints(Decimal::from(10)),
        atr: None,
    }),
    None,
    [("1001.5", "999.0"), ("998.5", "1001.0")]
)]
#[case::multiples_of_the_atr(
    exiting(BracketSpec {
        take_profit: Distance::Atr(Decimal::new(15, 1)),
        stop: Distance::Atr(Decimal::from(2)),
        ..exits_of_config()
    }),
    Some(2.1),
    [("1003.0", "996.0"), ("997.0", "1004.0")]
)]
fn exits_are_their_distances_from_the_reference_on_the_nearest_tick(
    #[case] target: Target,
    #[case] atr: Option<f64>,
    #[case] expected: [(&str, &str); 2],
) {
    assert_eq!(
        exits(target, "1000.0", atr),
        expected.map(|(take_profit, stop)| (take_profit.to_string(), stop.to_string()))
    );
}

/// A tiny ATR of 0.1 on half-point ticks from 1000.0: a stop of 1 ATR,
/// 0.1, is nearer 1000.0 than 999.5, and a take profit of 1.5 ATRs,
/// 0.15, nearer 1000.0 than 1000.5. An exit on the decision's price is no
/// distance, a P&L in R of no stop, so neither is a target, as with a
/// cold ATR; 10 ATRs, 1.0, are.
#[rstest]
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

/// `config`, its ATR of 14 bars with `change` made to its arguments.
fn config_atr(change: impl FnOnce(&mut AtrArgs)) -> TargetConfig {
    let mut atr = atr_of(14);
    change(&mut atr);
    of(exiting(BracketSpec {
        atr: Some(atr),
        ..exits_of_config()
    }))
}

#[rstest]
#[case::risk(TargetConfig {
    risk: "0.4%".parse().unwrap(),
    ..config()
})]
#[case::take_profit_in_bps(of(exiting(BracketSpec {
    take_profit: Distance::BasisPoints(Decimal::from(21)),
    ..exits_of_config()
})))]
#[case::take_profit_in_atrs(of(exiting(BracketSpec {
    take_profit: Distance::Atr(Decimal::from(20)),
    ..exits_of_config()
})))]
#[case::stop(of(exiting(BracketSpec {
    stop: Distance::Atr(Decimal::from(2)),
    ..exits_of_config()
})))]
#[case::atr_period(config_atr(|atr| atr.period = 15))]
#[case::atr_moving_average(config_atr(|atr| atr.ma_type = Some(MovingAverageType::Simple)))]
#[case::atr_use_previous(config_atr(|atr| atr.use_previous = Some(false)))]
#[case::atr_value_floor(config_atr(|atr| atr.value_floor = Some(0.5)))]
#[case::limit(of(Target {
    limit: SignedDuration::from_hours(3),
    ..bracket()
}))]
fn the_fingerprint_changes_with_every_target_spec(#[case] changed: TargetConfig) {
    let base = last_mid(InstrumentId::from(GOLD))
        .build()
        .unwrap()
        .fingerprint();

    assert_eq!(
        fingerprint(base, &config(), rule(), &quotes()).unwrap(),
        fingerprint(base, &config(), rule(), &quotes()).unwrap()
    );
    assert_ne!(
        fingerprint(base, &config(), rule(), &quotes()).unwrap(),
        base
    );
    assert_ne!(
        fingerprint(base, &changed, rule(), &quotes()).unwrap(),
        fingerprint(base, &config(), rule(), &quotes()).unwrap()
    );
}

/// A trade with exits is not one closed at its time alone, whatever the
/// limit they share, nor one closed at another time: rows and models of
/// one are never read for the other.
#[rstest]
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
#[rstest]
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

/// A time-only target's ATR is its unit's: another period keys another
/// target, whose rows realize their P&L over another unit.
#[rstest]
fn a_time_only_targets_atr_changes_the_fingerprint() {
    let keyed = |period| {
        let time = of(Target::builder()
            .limit(SignedDuration::from_hours(1))
            .atr(atr_of(period))
            .build());
        fingerprint(Fingerprint::default(), &time, rule(), &quotes()).unwrap()
    };

    assert_ne!(keyed(14), keyed(15));
}

/// The ATR's bars are built from what the strategy reads: the same config
/// on quotes, on trades and on bars keys three targets, whose rows differ,
/// a time-only target's ATR as a bracket's. A target without an ATR, a
/// bracket in basis points, is keyed alike whatever the strategy reads.
#[rstest]
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
    assert_ne!(keyed(&time, &quotes()), keyed(&time, &trades));
}

#[rstest]
fn a_decision_settles_within_its_limit() {
    let time = time_only(SignedDuration::from_hours(1));

    assert_eq!(config().settles_within(), SignedDuration::from_hours(2));
    assert_eq!(time.settles_within(), SignedDuration::from_hours(1));
}

#[rstest]
#[case::a_gain("0.0005 USDT", 1)]
#[case::a_loss("-0.44 USD", -1)]
#[case::nought("0 USD", 0)]
fn an_outcome_has_the_sign_of_its_pnl(#[case] pnl: &str, #[case] sign: i8) {
    let outcome = Outcome {
        pnl: Money::from(pnl),
        commission: Money::from("0.1 USD"),
        exit: Exit::Time,
        unit: Money::from("1 USD"),
    };

    assert_eq!(outcome.sign(), sign);
}

// ------------------------------------------------------- reference stake

/// A stop of 3.00 on gold, whole ounces: on a dataset balance of 100 USD
/// the reference stake, 0.30 USD, buys 0.1 oz, rounded down to none, on
/// either side, so no target, saying which side and its one R, rather than
/// a unit of nought.
#[rstest]
fn a_target_whose_size_at_the_reference_stake_rounds_to_none_is_none() {
    let probe = Probe::holding(Some(gold()));
    probe.quoted_at(InstrumentId::from(GOLD), "4001.50", "4001.70");
    let venue = DatasetVenue {
        balance: Money::from("100 USD"),
        ..venue_of(commission())
    };

    let set = setter_on(&of(wide_take_profit()), &venue)
        .at(&probe)
        .unwrap();

    assert_eq!(
        set,
        Err(NoTarget::NoSize {
            side: Side::Long,
            one_r: Decimal::from(3)
        })
    );
}

/// A dataset run sends each side at the reference stake of the dataset
/// balance, whatever the account holds (the probe holds none): the 428,571
/// AUD/USD of a stop of 0.0007 at 300 USD, its entry tagged with the R the
/// row's unit is, 299.9997 USD, and the multiple 1.
#[rstest]
fn a_dataset_run_sends_each_side_at_the_size_its_row_is_priced_at() {
    let mut probe = Probe::holding(Some(aud()));
    probe.quoted(InstrumentId::from(AUD), "0.70000");
    let targets = targets(on_aud(in_bps(20, 10)));

    targets.open(&mut probe, decision()).unwrap();

    for entry in ["D0-L-E", "D0-S-E"] {
        let order = probe.cache().order(&ClientOrderId::from(entry)).unwrap();
        assert_eq!(order.quantity(), Quantity::from(428_571), "{entry}");
        assert_eq!(
            orders::entry_risk(&order),
            Some(orders::EntryRisk {
                r: Money::from_str_exact("299.9997 USD").unwrap(),
                multiple: Decimal::ONE,
            }),
            "{entry}"
        );
    }
}

/// Gold's P&L is in USD. A dataset balance in EUR: the reference stake is
/// not sized on it, which is not converted. A commission of 3 EUR an order
/// (`in_euros`): the target's costs are not converted. Either way the
/// targets do not start, naming the currency.
#[rstest]
#[case::a_balance_in_euros(
    DatasetVenue {
        balance: Money::from("100000 EUR"),
        ..venue_of(commission())
    },
    &["EUR", "balance"]
)]
#[case::a_commission_in_euros(venue_of(in_euros()), &["EUR"])]
fn a_dataset_venue_in_another_currency_than_the_targets_pnl_is_refused_as_the_targets_start(
    #[case] venue: DatasetVenue,
    #[case] named: &[&str],
) {
    let mut probe = Probe::holding(Some(gold()));
    let targets = Targets::new(lot(no_exits()), rule(), &quotes(), &venue).unwrap();

    let err = format!("{:#}", targets.start(&mut probe).unwrap_err());

    for named in named {
        assert!(err.contains(named), "{named}: {err}");
    }
}

/// EUR/USD on the simulated venue, AUD/USD's: the pair a target on
/// AUD/USD converts a dataset balance in EUR through.
const EURUSD: &str = "EUR/USD.SIM";

/// `on_aud(in_bps(20, 10))` converting through EUR/USD.
fn converting_through_eurusd() -> TargetConfig {
    TargetConfig {
        conversions: Conversions::new(vec![InstrumentId::from(EURUSD)]),
        ..on_aud(in_bps(20, 10))
    }
}

/// The venue of the tests' dataset runs holding `balance`.
fn holding(balance: &str) -> DatasetVenue {
    DatasetVenue {
        balance: Money::from(balance),
        ..venue_of(commission())
    }
}

/// The probe holding AUD/USD and EUR/USD.
fn holding_eurusd() -> Probe {
    let probe = Probe::holding(Some(aud()));
    probe
        .cache_rc()
        .borrow_mut()
        .add_instrument(InstrumentAny::CurrencyPair(default_fx_ccy(
            Symbol::from("EUR/USD"),
            Some(Venue::from("SIM")),
        )))
        .unwrap();
    probe
}

/// On 100,000 EUR, EUR/USD at 1.24990 / 1.25010, its mid 1.25000 USD per
/// EUR: the reference stake is of 125,000 USD, 0.3% of it 375 USD, over
/// one R of 0.0007 on one AUD/USD 535,714.29, rounded down, 535,714, its
/// stop worth 374.9998 USD, the unit. Unconverted, 100,000 taken as USD,
/// it would be 428,571; at the rate swapped, 80,000 USD, 342,857.
#[rstest]
fn the_reference_stake_is_of_the_dataset_balance_converted_at_the_decisions_rate() {
    let probe = holding_eurusd();
    probe.quoted(InstrumentId::from(AUD), "0.70000");
    probe.quoted_at(InstrumentId::from(EURUSD), "1.24990", "1.25010");

    let set = setter_on(&converting_through_eurusd(), &holding("100000 EUR"))
        .at(&probe)
        .unwrap()
        .unwrap();

    for side in [&set.long, &set.short] {
        assert_eq!(side.equity, Money::from("125000 USD"));
        assert_eq!(side.unit, Money::from_str_exact("374.9998 USD").unwrap());
    }
}

/// No quote of EUR/USD is cached yet: the dataset balance has no rate into
/// USD, and no target is set, saying so.
#[rstest]
fn a_decision_with_no_rate_of_the_dataset_balance_sets_no_target() {
    let probe = holding_eurusd();
    probe.quoted(InstrumentId::from(AUD), "0.70000");
    let config = converting_through_eurusd();

    let set = setter_on(&config, &holding("100000 EUR"))
        .at(&probe)
        .unwrap();

    assert_eq!(
        set,
        Err(NoTarget::NoRate {
            balance: Money::from("100000 EUR"),
            pnl: Currency::USD(),
            rates: config.conversions.rates(),
        })
    );
}

/// EUR reaches USD, AUD/USD's P&L currency, through EUR/USD as the cache
/// defines it: the targets start on a dataset balance in EUR, and refuse
/// one in JPY, which EUR/USD does not reach, naming it.
#[rstest]
fn a_dataset_balance_the_pairs_reach_the_pnl_from_starts_the_targets_and_another_is_refused() {
    let targets_on = |balance| {
        let config = converting_through_eurusd();
        let requirements = reading_its_quotes(&config);
        Targets::new(config, rule(), &requirements, &holding(balance)).unwrap()
    };
    let mut probe = holding_eurusd();

    targets_on("100000 EUR").start(&mut probe).unwrap();
    let err = format!(
        "{:#}",
        targets_on("15000000 JPY").start(&mut probe).unwrap_err()
    );

    assert!(err.contains("JPY") && err.contains(EURUSD), "{err}");
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
        self.quoted_at(
            instrument_id,
            &(mid - tick).to_string(),
            &(mid + tick).to_string(),
        );
    }

    /// Adds a quote of `instrument_id` at `bid` and `ask` to its cache.
    fn quoted_at(&self, instrument_id: InstrumentId, bid: &str, ask: &str) {
        self.cache_rc()
            .borrow_mut()
            .add_quote(QuoteTick::new(
                instrument_id,
                Price::from(bid),
                Price::from(ask),
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

/// AUD/USD.
fn aud() -> InstrumentAny {
    InstrumentAny::CurrencyPair(audusd_sim())
}

/// Bybit's inverse BTCUSD, settled in BTC.
fn inverse() -> InstrumentAny {
    InstrumentAny::CryptoPerpetual(btcusd_bybit())
}

/// `spec` on `instrument_id` at the tests' reference stake.
fn trading(spec: Target, instrument_id: InstrumentId) -> TargetConfig {
    TargetConfig::builder()
        .instrument_id(instrument_id)
        .risk(risk())
        .spec(spec)
        .build()
}

/// `spec` on AUD/USD.
fn on_aud(spec: Target) -> TargetConfig {
    trading(spec, InstrumentId::from(AUD))
}

/// A target closed after an hour, with no exits, its unit measured in the ATR
/// of one bar: the true range of the last bar of the cadence.
fn no_exits() -> Target {
    Target::builder()
        .limit(SignedDuration::from_hours(1))
        .atr(atr_of(1))
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

/// IC's commission, 30 USD per million of value.
fn commission() -> Commission {
    Commission::Rate(Decimal::new(3, 5))
}

/// What a strategy trading `config` reads: the quotes of its instrument.
fn reading_its_quotes(config: &TargetConfig) -> DataRequirements {
    DataRequirements {
        bar_types: vec![],
        ticks: vec![TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: config.instrument_id,
        }],
        custom: vec![],
        warmup: SignedDuration::ZERO,
    }
}

fn setter(config: &TargetConfig) -> TargetSetter {
    setter_on(config, &venue_of(commission()))
}

/// The setter of `config` of the dataset runs on `venue`.
fn setter_on(config: &TargetConfig, venue: &DatasetVenue) -> TargetSetter {
    TargetSetter::new(config, rule(), &reading_its_quotes(config), venue).unwrap()
}

fn targets(config: TargetConfig) -> Rc<Targets> {
    let requirements = reading_its_quotes(&config);
    Targets::new(config, rule(), &requirements, &venue_of(commission())).unwrap()
}

/// `setter` started on `probe` and handed `bar`, a bar of its ATR's, if it
/// has one.
fn warmed(setter: TargetSetter, probe: &mut Probe, bar: &Bar) -> TargetSetter {
    setter.start(probe);
    probe.handle_bar(bar);
    setter
}

/// A 15-minute bar of the mids of `instrument_id`, of `open`, `high`, `low`
/// and `close`: the bars its target's ATR runs on deciding every 15 minutes
/// on its quotes.
fn quarter(instrument_id: &str, [open, high, low, close]: [&str; 4]) -> Bar {
    Bar::new(
        BarType::from(format!("{instrument_id}-15-MINUTE-MID-INTERNAL").as_str()),
        Price::from(open),
        Price::from(high),
        Price::from(low),
        Price::from(close),
        Quantity::from(1),
        UnixNanos::default(),
        UnixNanos::default(),
    )
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

/// From the mid of 0.70000, 10 bps is 0.0007, its one R; 20 bps is 0.0014.
/// The long's stop is below the price, its take profit above; the short's the
/// reverse. At the reference stake, 300 USD, over 0.0007 on one AUD/USD, each
/// side is 428,571.43 AUD/USD rounded down, 428,571, its stop worth 299.9997
/// USD, the unit. Each side is of the dataset balance, 100,000 USD, and
/// closes after the target's 2 hours.
#[rstest]
fn a_bracket_is_set_on_each_side_with_the_worth_of_its_stop_at_the_reference_stake() {
    let probe = Probe::holding(Some(aud()));
    probe.quoted(InstrumentId::from(AUD), "0.70000");

    let set = setter(&on_aud(in_bps(20, 10))).at(&probe).unwrap().unwrap();

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

    for side in [&set.long, &set.short] {
        assert_eq!(side.reference, Price::from("0.70000"));
        assert_eq!(side.time_limit, SignedDuration::from_hours(2));
        assert_eq!(side.one_r, Decimal::new(70, 5));
        assert_eq!(side.equity, Money::from("100000 USD"));
        assert_eq!(side.unit, Money::from_str_exact("299.9997 USD").unwrap());
    }
}

/// AUD/USD at 0.70000/0.70001: 3 bps of the mid of 0.700005 is 0.00021, and
/// the nearest ticks put each stop 0.000215 from the mid, outside the spread.
/// On a dataset balance of 1 USD the reference stake, 0.003 USD, buys 13.95
/// AUD/USD over it, rounded down 13, on which one R is worth 0.002795 USD.
/// A trade losing 0.00559 USD on it realized -2R.
#[rstest]
fn a_trade_on_a_stop_worth_under_half_a_cent_realizes_over_its_exact_worth() {
    let probe = Probe::holding(Some(aud()));
    let aud = InstrumentId::from(AUD);
    probe.quoted_at(aud, "0.70000", "0.70001");
    let venue = DatasetVenue {
        balance: Money::from("1 USD"),
        ..venue_of(commission())
    };

    let set = setter_on(&trading(in_bps(20, 3), aud), &venue)
        .at(&probe)
        .unwrap()
        .unwrap();

    assert_eq!(
        set.long.unit,
        Money::from_str_exact("0.002795 USD").unwrap()
    );
    let outcome = |unit| Outcome {
        pnl: Money::from_str_exact("-0.00559 USD").unwrap(),
        commission: Money::from("0 USD"),
        exit: Exit::Stop,
        unit,
    };
    let row = DatasetRow::of(&TargetRow {
        decision: decision(),
        payoff: PerSide {
            long: set.long.payoff.clone(),
            short: set.short.payoff.clone(),
        },
        outcomes: Outcomes {
            long: outcome(set.long.unit),
            short: outcome(set.short.unit),
        },
        settled_at: UnixNanos::from(2),
    });
    for side in [Side::Long, Side::Short] {
        let realized = Example::of_side(&row, side).realized();
        assert!((realized + 2.0).abs() < 1e-12, "{side:?}: {realized}");
    }
}

const SWAP: &str = "BTC-USDT-SWAP.OKX";

/// BTC-USDT-SWAP as OKX defines it: 0.01 BTC a contract, settled in USDT, on
/// ticks of 0.1 and lots of 0.01 contracts.
fn swap() -> InstrumentAny {
    InstrumentAny::CryptoPerpetual(
        CryptoPerpetual::builder()
            .instrument_id(InstrumentId::from(SWAP))
            .raw_symbol(Symbol::from("BTC-USDT-SWAP"))
            .base_currency(Currency::BTC())
            .quote_currency(Currency::USDT())
            .settlement_currency(Currency::USDT())
            .is_inverse(false)
            .price_precision(1)
            .size_precision(2)
            .price_increment(Price::from("0.1"))
            .size_increment(Quantity::from("0.01"))
            .multiplier(Quantity::from("0.01"))
            .lot_size(Quantity::from("0.01"))
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .unwrap(),
    )
}

/// Worked by hand. With no exits, one R is the target's ATR: of one bar of
/// BTC-USDT-SWAP from 99,925 to 100,075, 150. The reference stake, 0.3% of
/// 100,000 USDT, 300 USDT, over 150 on a contract of 0.01 BTC, 1.5 USDT, buys
/// 200 contracts; one R is worth 300 USDT on them, on either side. The trade
/// is a market entry from the mid of 99,999.9/100,000.1, closed after its
/// hour.
#[rstest]
fn a_time_only_target_is_set_with_the_worth_of_its_atr_through_the_multiplier() {
    let mut probe = Probe::holding(Some(swap()));
    probe.quoted_at(InstrumentId::from(SWAP), "99999.9", "100000.1");
    let config = trading(no_exits(), InstrumentId::from(SWAP));
    let bar = quarter(SWAP, ["100000.0", "100075.0", "99925.0", "100000.0"]);
    let venue = DatasetVenue {
        balance: Money::from("100000 USDT"),
        ..venue_of(Commission::Rate(Decimal::ZERO))
    };
    let setter = warmed(setter_on(&config, &venue), &mut probe, &bar);

    let set = setter.at(&probe).unwrap().unwrap();

    for side in [&set.long, &set.short] {
        assert_eq!(side.one_r, Decimal::from(150));
        assert_eq!(side.unit, Money::from("300 USDT"));
        assert_eq!(side.bracket, None);
        assert_eq!(side.reference, Price::from("100000.00"));
        assert_eq!(side.time_limit, SignedDuration::from_hours(1));
    }
}

/// The ATR of a target, a time-only target's as a bracket's in ATRs, of one
/// bar going nowhere, 4000 throughout, is nought: no distance in ATRs and no
/// worth of a time-only target's unit, so no target, which names the ATR's
/// bars.
#[rstest]
#[case::time_only(no_exits())]
#[case::a_bracket_in_atrs(exiting(BracketSpec {
    take_profit: Distance::Atr(Decimal::from(2)),
    stop: Distance::Atr(Decimal::ONE),
    atr: Some(atr_of(1)),
}))]
fn a_target_on_an_atr_of_nought_says_so_and_names_its_bars(#[case] spec: Target) {
    let mut probe = Probe::holding(Some(gold()));
    probe.quoted_at(InstrumentId::from(GOLD), "3999.90", "4000.10");
    let flat = quarter(GOLD, ["4000.00"; 4]);

    let set = warmed(setter(&of(spec)), &mut probe, &flat)
        .at(&probe)
        .unwrap();

    assert_eq!(
        set,
        Err(NoTarget::FlatAtr(BarType::from(
            "XAUUSD.ICMARKETS-15-MINUTE-MID-INTERNAL"
        )))
    );
}

#[rstest]
#[case::a_bracket(in_bps(20, 10))]
#[case::time_only(no_exits())]
fn a_target_with_no_price_to_set_it_from_says_so(#[case] spec: Target) {
    let mut probe = Probe::holding(Some(gold()));

    let set = warmed(setter(&of(spec)), &mut probe, &gold_bar())
        .at(&probe)
        .unwrap();

    assert_eq!(set, Err(NoTarget::NoPrice));
}

/// Gold's bracket 0.3 of Wilder's ATR of 4 bars a side, closed after 5
/// minutes: the gold scalpers'.
fn symmetric() -> TargetConfig {
    let distance = Distance::Atr(Decimal::new(3, 1));
    of(Target {
        bracket: Some(BracketSpec {
            take_profit: distance,
            stop: distance,
            atr: Some(AtrArgs {
                period: 4,
                ..DEFAULT_ATR
            }),
        }),
        limit: SignedDuration::from_mins(5),
        atr: None,
    })
}

/// A 15-minute bar of gold's mids from 3999 to 4001, closing at 4000: a
/// true range of 2, after a close of 4000 as before it.
fn gold_bar() -> Bar {
    let price = |p: &str| Price::from(p);
    Bar::new(
        BarType::from("XAUUSD.ICMARKETS-15-MINUTE-MID-INTERNAL"),
        price("4000.00"),
        price("4001.00"),
        price("3999.00"),
        price("4000.00"),
        Quantity::from(1),
        UnixNanos::default(),
        UnixNanos::default(),
    )
}

/// A 15-minute bar of gold's mids from 3997 to 4003, closing at 4000: a
/// true range of 6, after a close of 4000 as before it.
fn wide_gold_bar() -> Bar {
    let price = |p: &str| Price::from(p);
    Bar::new(
        BarType::from("XAUUSD.ICMARKETS-15-MINUTE-MID-INTERNAL"),
        price("4000.00"),
        price("4003.00"),
        price("3997.00"),
        price("4000.00"),
        Quantity::from(1),
        UnixNanos::default(),
        UnixNanos::default(),
    )
}

/// Worked by hand. The setter of `symmetric` sharing its width's ATR,
/// started alone, sets the long's stop from gold's mid of 4000.00 at the
/// width read at the same moment: none for 3 bars; after a 4th, Wilder's
/// ATR seeded by the mean of 4 true ranges of 2 is 2, the width 0.6 and
/// the stop 3999.40; after a 5th of 6, (3 × 2 + 6) / 4 = 3, the width 0.9
/// and the stop 3999.10.
#[rstest]
fn a_setter_sharing_a_width_sets_its_exits_at_the_width_read_then() {
    let config = symmetric();
    let width = config.width(rule(), &quotes()).unwrap();
    let setter = TargetSetter::sharing(
        &config,
        rule(),
        &quotes(),
        &venue_of(commission()),
        Some(&width),
    )
    .unwrap();
    let mut probe = Probe::holding(Some(gold()));
    probe.quoted_at(InstrumentId::from(GOLD), "3999.99", "4000.01");
    setter.start(&mut probe);
    let read = |probe: &Probe| {
        let set = setter.at(probe).unwrap().ok();
        let stop = set.map(|set| set.long.bracket.unwrap().stop_loss);
        (width.length(4000.0), stop)
    };

    for _ in 0..3 {
        probe.handle_bar(&gold_bar());
    }
    let cold = read(&probe);
    probe.handle_bar(&gold_bar());
    let warm = read(&probe);
    probe.handle_bar(&wide_gold_bar());
    let wider = read(&probe);

    assert_eq!(cold, (None, None));
    assert_eq!(warm, (Some(0.6), Some(Price::from("3999.40"))));
    assert_eq!(wider, (Some(0.9), Some(Price::from("3999.10"))));
}

/// A setter shares only the ATR it would run itself, of 4 bars: a width's of
/// another period, 14, or a width in basis points with none, is refused,
/// naming both.
#[rstest]
#[case::of_another_period(
    Target {
        bracket: symmetric().spec.bracket.map(|bracket| BracketSpec {
            atr: Some(DEFAULT_ATR),
            ..bracket
        }),
        ..symmetric().spec
    },
    "period: 14"
)]
#[case::in_basis_points(in_bps(10, 10), "ATR None")]
fn a_setter_refuses_to_share_the_atr_of_another_width(#[case] other: Target, #[case] shared: &str) {
    let width = of(other).width(rule(), &quotes()).unwrap();

    let err = TargetSetter::sharing(
        &symmetric(),
        rule(),
        &quotes(),
        &venue_of(commission()),
        Some(&width),
    )
    .unwrap_err()
    .to_string();

    assert!(err.contains(shared), "{err}");
    assert!(err.contains("period: 4,"), "{err}");
}

/// 10 bps of 4000 is 4, of 2000 is 2, with no ATR to warm.
#[rstest]
fn a_width_in_basis_points_is_of_the_price_it_is_read_at() {
    let width = of(in_bps(10, 10)).width(rule(), &quotes()).unwrap();

    assert!(width.initialized());
    assert_eq!(width.length(4000.0), Some(4.0));
    assert_eq!(width.length(2000.0), Some(2.0));
}

/// A bracket has one width only if its take profit and stop are as far: a
/// time-only target has none, saying it has no bracket, and a bracket of 20
/// bps and 10 bps none, naming both distances.
#[rstest]
#[case::time_only(time_only(SignedDuration::from_hours(1)), &["bracket"])]
#[case::two_distances(of(in_bps(20, 10)), &["20 bps", "10 bps"])]
fn a_width_needs_a_bracket_of_one_distance(#[case] config: TargetConfig, #[case] named: &[&str]) {
    let err = config.width(rule(), &quotes()).unwrap_err().to_string();

    for named in named {
        assert!(err.contains(named), "{named}: {err}");
    }
}

/// Read at the decisions of the target's cadence, every 15 minutes, each
/// firing 10 ms after its grid time: a quote then is the decision's, one
/// later the next one's. Its target closes after 5 minutes.
#[rstest]
fn a_width_is_read_at_the_decisions_of_its_cadence() {
    let width = symmetric().width(rule(), &quotes()).unwrap();
    let quarter = "2026-10-21T10:15:00Z".parse::<Timestamp>().unwrap();
    let at = |offset: SignedDuration| UnixNanos::from(quarter + offset);

    assert_eq!(
        width.decision(at(SignedDuration::from_millis(10))),
        at(SignedDuration::ZERO)
    );
    assert_eq!(
        width.decision(at(SignedDuration::from_nanos(10_000_001))),
        at(SignedDuration::from_mins(15))
    );
    assert_eq!(width.limit(), SignedDuration::from_mins(5));
}

#[rstest]
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
#[rstest]
fn a_target_with_an_exit_on_the_price_says_so_and_names_the_price() {
    let probe = Probe::holding(Some(aud()));
    probe.quoted(InstrumentId::from(AUD), "0.07000");

    let set = setter(&on_aud(tight_stop())).at(&probe).unwrap();

    assert_eq!(set, Err(NoTarget::ExitOnPrice(Price::from("0.07000"))));
}

/// Only an instrument seen to be inverse is refused: one missing from the cache
/// is the error of each decision, as `TargetSetter::at` has it.
#[rstest]
fn a_bracket_on_an_instrument_not_in_the_cache_starts() {
    let mut probe = Probe::holding(None);

    targets(on_aud(in_bps(20, 10))).start(&mut probe).unwrap();
}

/// Past the start's refusal, a stop's worth failing is still an error before
/// any order, as an exit outside the instrument's prices is: no order sent.
#[rstest]
fn a_stop_with_no_worth_at_a_decision_is_an_error_before_any_order() {
    let instrument = inverse();
    let mut probe = Probe::holding(Some(instrument.clone()));
    probe.quoted(instrument.id(), "100000.00000");
    let targets = targets(trading(in_bps(20, 10), instrument.id()));

    let err = targets.open(&mut probe, decision()).unwrap_err();

    assert!(format!("{err:#}").contains("inverse"), "{err:#}");
    assert_eq!(probe.orders(), Vec::<String>::new());
}

/// A time-only target reads its instrument to be set, its unit the worth of
/// its ATR on it and its entry cost costing its fills, as a bracket does: one
/// missing is the setter's error, its ATR warm, before any order, and takes no
/// number, so the next decision, the instrument there, is the first.
#[rstest]
fn a_time_only_target_missing_its_instrument_errs_before_any_order() {
    let mut probe = Probe::holding(None);
    probe.quoted(InstrumentId::from(AUD), "0.70000");
    let targets = targets(on_aud(no_exits()));
    targets.start(&mut probe).unwrap();
    probe.handle_bar(&quarter(AUD, ["0.70000", "0.70050", "0.69950", "0.70000"]));

    let err = targets.open(&mut probe, decision()).unwrap_err();

    assert!(format!("{err:#}").contains(AUD), "{err:#}");
    assert_eq!(probe.orders(), Vec::<String>::new());
    probe.cache_rc().borrow_mut().add_instrument(aud()).unwrap();
    targets.open(&mut probe, decision()).unwrap();
    assert_eq!(probe.orders(), ["D0-L-E", "D0-S-E"]);
}

/// Gold on `spec` at the reference stake, 300 USD: a lot, 100 oz, on a
/// stop of 3.00, 150 oz on an ATR of 2.
fn lot(spec: Target) -> TargetConfig {
    of(spec)
}

/// A take profit 15 bps and a stop 7.5 bps of the decision's price away.
fn wide_take_profit() -> Target {
    exiting(BracketSpec {
        take_profit: Distance::BasisPoints(Decimal::from(15)),
        stop: Distance::BasisPoints(Decimal::new(75, 1)),
        atr: None,
    })
}

/// 3 USD an order.
fn per_order() -> Commission {
    Commission::PerOrder(Money::from("3 USD"))
}

/// Pepperstone's 0.035 USD an ounce, 3.50 USD a fill of a lot.
fn per_ounce() -> Commission {
    Commission::PerQuantity(Money::from_str_exact("0.035 USD").unwrap())
}

/// The payoff features of each side of a lot of `spec` at `commission`, on
/// `probe`, its ATR, if any, of the bar `gold_bar` alone: a true range of 2.
fn payoff_on(probe: &mut Probe, spec: Target, commission: Commission) -> PerSide<Vec<f64>> {
    let setter = TargetSetter::new(&lot(spec), rule(), &quotes(), &venue_of(commission)).unwrap();
    warmed(setter, probe, &gold_bar())
        .at(probe)
        .unwrap()
        .unwrap()
        .map(|target| target.payoff)
}

/// The payoff features of each side of a lot of `spec` at `commission`,
/// gold quoted at 4001.50/4001.70.
fn payoff_quoted(spec: Target, commission: Commission) -> PerSide<Vec<f64>> {
    let mut probe = Probe::holding(Some(gold()));
    probe.quoted_at(InstrumentId::from(GOLD), "4001.50", "4001.70");
    payoff_on(&mut probe, spec, commission)
}

/// Each of `got` within 1e-12 of `want`'s, as many.
fn assert_near(got: &PerSide<Vec<f64>>, want: &PerSide<Vec<f64>>) {
    for side in Side::BOTH {
        let (got, want) = (got.of(side), want.of(side));
        assert_eq!(got.len(), want.len(), "{side:?}: {got:?}, not {want:?}");
        for (g, w) in got.iter().zip(want) {
            assert!((g - w).abs() < 1e-12, "{side:?}: {got:?}, not {want:?}");
        }
    }
}

/// A bracket has the take profit's R, the stop's nominal loss and the entry
/// cost; a time-only target the entry cost alone.
#[rstest]
fn a_target_names_the_payoff_features_of_the_barriers_it_has() {
    assert_eq!(
        lot(wide_take_profit()).payoff_names(),
        ["take_profit_r", "stop_loss_r", "entry_cost"]
    );
    assert_eq!(lot(no_exits()).payoff_names(), ["entry_cost"]);
}

/// Worked by hand. A lot of gold quoted 4001.50/4001.70: the mid, 4001.60;
/// 15 bps of it, 6.0024, and 7.5 bps, 3.0012, on the nearest cent: the long's
/// take profit 4007.60 and stop 3998.60, the short's take profit 3995.60 and
/// stop 4004.60; each stop 3.00 away, worth 300 USD on 100 oz, the unit. The
/// long enters at the ask, 4001.70, the short at the bid, 4001.50, a spread
/// of 0.20, 20 USD. At IC's 30 USD per million, a fill of a lot costs 0.003 of
/// its price, to the cent: 12.01 at 4001.70, 12.00 at 4001.50, 12.02 at
/// 4007.60, 12.00 at 3998.60, 11.99 at 3995.60, 12.01 at 4004.60. The long's
/// take profit nets 590 − 12.01 − 12.02, its stop loses 310 + 12.01 + 12.00,
/// its entry costs 20 + 12.01; the short's take profit nets 590 − 12.00 −
/// 11.99, its stop loses 310 + 12.00 + 12.01, its entry costs 20 + 12.00.
/// At 3 USD an order each fill costs 3, the take profit nets 590 − 6, the
/// stop loses 310 + 6 and the entry costs 20 + 3, on either side.
#[rstest]
#[case::at_a_rate(
    commission(),
    [565.97, 334.01, 32.01],
    [566.01, 334.01, 32.00]
)]
#[case::per_order(per_order(), [584.0, 316.0, 23.0], [584.0, 316.0, 23.0])]
fn a_brackets_payoff_is_worked_out_by_hand_on_each_side(
    #[case] commission: Commission,
    #[case] long: [f64; 3],
    #[case] short: [f64; 3],
) {
    let payoff = payoff_quoted(wide_take_profit(), commission);

    assert_near(
        &payoff,
        &PerSide {
            long: long.map(|usd| usd / 300.0).to_vec(),
            short: short.map(|usd| usd / 300.0).to_vec(),
        },
    );
}

/// The quote of the bracket's tests and no exits: the reference stake, 300
/// USD, over the ATR of 2 buys 150 oz, the unit the ATR's worth on them, 300
/// USD, and the entry cost the spread's 0.20 on them, 30 USD, and the
/// entry's commission over it: at IC's rate, 0.003 of 150 oz's price to the
/// cent, 18.01 at the long's ask, 4001.70 (18.00765), and at the short's
/// bid, 4001.50 (18.00675); 3 an order; 5.25 at 0.035 an ounce.
#[rstest]
#[case::at_a_rate(commission(), 48.01, 48.01)]
#[case::per_order(per_order(), 33.0, 33.0)]
#[case::per_ounce(per_ounce(), 35.25, 35.25)]
fn a_time_only_targets_entry_cost_is_over_the_worth_of_its_atr_on_each_side(
    #[case] commission: Commission,
    #[case] long: f64,
    #[case] short: f64,
) {
    let payoff = payoff_quoted(no_exits(), commission);

    assert_near(
        &payoff,
        &PerSide {
            long: vec![long / 300.0],
            short: vec![short / 300.0],
        },
    );
}

/// A trade of gold at 4001.60, the reference of the bracket's tests: its exits
/// and unit theirs. With no quote both sides enter at the trade, no spread: the
/// long's take profit nets 600 − 12.00 − 12.02 and its stop loses
/// 300 + 12.00 + 12.00, the short's take profit nets 600 − 12.00 − 11.99 and
/// its stop loses 300 + 12.00 + 12.01, each entry costing its commission
/// alone, 12.00 at 4001.60.
#[rstest]
fn a_bracket_on_trades_alone_reads_its_payoff_from_the_last_trade_with_no_spread() {
    let mut probe = Probe::holding(Some(gold()));
    probe
        .cache_rc()
        .borrow_mut()
        .add_trade(TradeTick::new(
            InstrumentId::from(GOLD),
            Price::from("4001.60"),
            Quantity::from(1),
            AggressorSide::NoAggressor,
            TradeId::from("1"),
            UnixNanos::default(),
            UnixNanos::default(),
        ))
        .unwrap();

    let payoff = payoff_on(&mut probe, wide_take_profit(), commission());

    assert_near(
        &payoff,
        &PerSide {
            long: vec![575.98 / 300.0, 324.0 / 300.0, 12.0 / 300.0],
            short: vec![576.01 / 300.0, 324.01 / 300.0, 12.0 / 300.0],
        },
    );
}

/// 3 EUR an order on gold, quoted in USD: the target's P&L, its positions'
/// realized P&L, is in USD, and the commission is not converted.
fn in_euros() -> Commission {
    Commission::PerOrder(Money::from("3 EUR"))
}

/// Live, the instrument may come after the start: the setter refuses the
/// commission at each decision instead.
#[rstest]
#[case::a_bracket(wide_take_profit())]
#[case::time_only(no_exits())]
fn a_commission_in_another_currency_than_the_targets_pnl_is_an_error_at_a_decision(
    #[case] spec: Target,
) {
    let mut probe = Probe::holding(Some(gold()));
    probe.quoted_at(InstrumentId::from(GOLD), "4001.50", "4001.70");
    let setter = TargetSetter::new(&lot(spec), rule(), &quotes(), &venue_of(in_euros())).unwrap();

    let err = warmed(setter, &mut probe, &gold_bar())
        .at(&probe)
        .unwrap_err();

    assert!(format!("{err:#}").contains("EUR"), "{err:#}");
}

/// A decision with no price is unsettled, its number taken; one with an exit on
/// the price is none, as on cold features, and takes no number: the first
/// orders are of the decision after the first.
#[rstest]
fn a_decision_with_no_price_takes_a_number_and_one_with_an_exit_on_the_price_none() {
    let mut probe = Probe::holding(Some(aud()));
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
