//! A model strategy sends its trades at a stake of the account's equity
//! (`strategy::orders::Trade::send`), while its targets, the dataset's rows and the payoff
//! features are priced at the reference stake: the config's risk, multiple 1, of the dataset
//! balance (`model::targets`). On the hand-worked quotes of `backtest_targets` the probe of
//! `common::dataset` trades a baseline long at every decision on a 25M USD account where its
//! dataset runs hold 10M: each entry is its stake of the account rounded down, a bracket's over
//! its stop, a time-only target's over its ATR, at a multiple of it, while the rows stay at the
//! reference stake whatever the dataset run's P&L or replay start.

use std::{path::Path, thread};

use helix_trader::{
    model::{
        config::ModelConfig,
        targets::{DEFAULT_ATR, Side, Target, TargetConfig, TargetRow},
    },
    strategy::{AtrArgs, orders::EntryRisk},
};
use jiff::{SignedDuration, Timestamp};
use nautilus_core::UnixNanos;
use nautilus_model::types::{Money, Quantity};
use rstest::rstest;
use rust_decimal::Decimal;

use crate::{
    backtest_model::hand_worked,
    backtest_targets::hand_worked_quotes,
    common::{
        dataset::{
            Account, Record, baseline_of, blueprint, features, model_set, replay_of, trade_staking,
        },
        ic,
    },
};

/// 25M USD where the dataset runs hold 10M: the config's stake of it, 0.00004%, is 10 USD.
fn account(multiple: i64) -> Account {
    Account {
        balance: Money::from("25000000 USD"),
        multiple: Decimal::from(multiple),
    }
}

/// The hand-worked quotes' decisions at 09:00 to 12:00.
fn window() -> (Timestamp, Timestamp) {
    (ic::at("08:30:00"), ic::at("12:30:00"))
}

fn rows_of(config: &ModelConfig, from: Timestamp, to: Timestamp) -> Vec<TargetRow> {
    let config = config.clone();
    thread::spawn(move || {
        let catalog = ic::catalog_of(hand_worked_quotes(), ic::at("08:00:00"), ic::at("13:00:00"));
        replay_of(&catalog, features(), &config, from, to).unwrap()
    })
    .join()
    .unwrap()
}

fn traded(config: &ModelConfig, models: &Path, account: Account) -> Record {
    let (config, models) = (config.clone(), models.to_path_buf());
    thread::spawn(move || {
        let (from, to) = window();
        blueprint(features(), config.clone())
            .model_store(&models)
            .unwrap()
            .publish(&model_set(
                &config,
                from,
                vec![baseline_of(&features(), &config, Side::Long, 8)],
                None,
            ))
            .unwrap();
        let catalog = ic::catalog_of(hand_worked_quotes(), ic::at("08:00:00"), ic::at("13:00:00"));
        trade_staking(&catalog, features(), (from, to), &config, &models, &account)
    })
    .join()
    .unwrap()
}

fn entries(record: &Record) -> Vec<(Timestamp, Quantity, Option<EntryRisk>)> {
    record
        .entries
        .iter()
        .map(|&(ts, quantity, risk)| (Timestamp::from(ts), quantity, risk))
        .collect()
}

fn decided(hh_mm: &str) -> Timestamp {
    ic::at(&format!("{hh_mm}:00.010"))
}

fn units(rows: &[TargetRow]) -> Vec<(UnixNanos, Money, Money)> {
    rows.iter()
        .map(|row| {
            (
                row.decision.ts_event,
                row.outcomes.long.unit,
                row.outcomes.short.unit,
            )
        })
        .collect()
}

/// The hand-worked bracket's long: 7.5 bps of the 4000.10 mid on both sides, stop 3.00 away
/// (4001.10 and 3.00 after the rise at 11:20). Rows at the reference stake, 4 USD of 10M
/// (`RISK`): one ounce, R 3 USD. On 25M USD at the config's stake, 10 USD: 10 / 3.00 = 3.33, 3 oz,
/// R 9 USD; at twice the stake, 20 / 3.00 = 6.67, 6 oz, R 18 USD. P&L before an entry moves the
/// equity by a few dollars, not the size.
#[rstest]
fn a_model_strategys_bracket_trade_fills_at_its_stake_of_the_accounts_equity() {
    let config = hand_worked(Side::Long);
    let (from, to) = window();
    let rows = rows_of(&config, from, to);
    let three = Money::from("3 USD");

    for (multiple, oz, r) in [(1, 3, "9 USD"), (2, 6, "18 USD")] {
        let models = tempfile::tempdir().unwrap();
        let record = traded(&config, models.path(), account(multiple));

        let expected: Vec<_> = ["09", "10", "11", "12"]
            .into_iter()
            .map(|hour| {
                let risk = EntryRisk {
                    r: Money::from(r),
                    multiple: Decimal::from(multiple),
                };
                (
                    decided(&format!("{hour}:00")),
                    Quantity::from(oz),
                    Some(risk),
                )
            })
            .collect();
        assert_eq!(entries(&record), expected, "multiple {multiple}");
    }
    let decision = |hour: &str| UnixNanos::from(decided(&format!("{hour}:00")));
    assert_eq!(
        units(&rows),
        ["09", "10", "11"].map(|hour| (decision(hour), three, three))
    );
}

/// The time-only target of `backtest_targets`: closed after 30 minutes, its R the ATR of 2 hourly
/// mid bars: none at 09:00, 1.60 at 10:00, 2.40 at 11:00, and at 12:00 (the hour from 4000.10 to
/// 4001.10, the rise at 11:20, true range 1.00) (2.40 + 1.00) / 2 = 1.70.
fn time_only() -> ModelConfig {
    let spec = Target::builder()
        .limit(SignedDuration::from_mins(30))
        .atr(AtrArgs {
            period: 2,
            ..DEFAULT_ATR
        })
        .build();
    ModelConfig {
        target: TargetConfig {
            spec,
            ..hand_worked(Side::Long).target
        },
        ..hand_worked(Side::Long)
    }
}

/// The time-only target's long on its ATR. Rows at 4 USD of 10M: 4 / 1.60 = 2.5, 2 oz, R 3.20 USD
/// at 10:00; 4 / 2.40 = 1.67, 1 oz, 2.40 USD at 11:00. Entries at 10 USD on 25M: 10 / 1.60 = 6.25,
/// 6 oz, and 10 / 2.40 = 4.17, 4 oz, each R 9.60 USD; at 12:00 10 / 1.70 = 5.88, 5 oz, R 5 x 1.70
/// = 8.50 USD.
#[rstest]
fn a_model_strategys_time_only_trade_fills_at_its_stake_over_its_atr() {
    let config = time_only();
    let (from, to) = window();
    let rows = rows_of(&config, from, to);
    let models = tempfile::tempdir().unwrap();

    let record = traded(&config, models.path(), account(1));

    let risk = |r: &str| {
        Some(EntryRisk {
            r: Money::from(r),
            multiple: Decimal::ONE,
        })
    };
    assert_eq!(
        entries(&record),
        [
            (decided("10:00"), Quantity::from(6), risk("9.60 USD")),
            (decided("11:00"), Quantity::from(4), risk("9.60 USD")),
            (decided("12:00"), Quantity::from(5), risk("8.50 USD")),
        ]
    );
    let unit = |unit: &str| Money::from(unit);
    let decision = |hour: &str| UnixNanos::from(decided(&format!("{hour}:00")));
    assert_eq!(
        units(&rows),
        [
            (decision("10"), unit("3.20 USD"), unit("3.20 USD")),
            (decision("11"), unit("2.40 USD"), unit("2.40 USD")),
        ]
    );
}

/// At a reference stake of 3 USD of 10M, a stop of 3.00 buys exactly one ounce. The 09:00 long
/// loses 3.64 USD at 09:20, which on the dataset run's equity would leave 2.99999891 USD to stake
/// at 10:00, under an ounce. Rows are sized at the balance, not the equity: the 10:00 and 11:00
/// rows are one ounce, R 3 USD, whether the replay started at 08:30 or after the loss at 09:30.
#[rstest]
fn the_rows_are_at_the_reference_stake_whatever_the_runs_pnl_and_its_start() {
    let config = ModelConfig {
        target: TargetConfig {
            risk: "0.00003%".parse().unwrap(),
            ..hand_worked(Side::Long).target
        },
        ..hand_worked(Side::Long)
    };
    let to = window().1;

    let from_start = rows_of(&config, ic::at("08:30:00"), to);
    let after_loss = rows_of(&config, ic::at("09:30:00"), to);

    let three = Money::from("3 USD");
    assert_eq!(units(&from_start).len(), 3, "{from_start:#?}");
    assert!(
        units(&from_start)
            .iter()
            .all(|&(_, long, short)| (long, short) == (three, three))
    );
    assert_eq!(from_start[0].outcomes.long.pnl, Money::from("-3.64 USD"));
    assert_eq!(&from_start[1..], after_loss.as_slice());
}
