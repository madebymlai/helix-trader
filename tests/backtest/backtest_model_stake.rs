//! A model strategy sends its trades at a stake of the account's equity
//! (`strategy::orders::Trade::send`). Its targets, the dataset's rows and the payoff features are
//! priced at the reference stake: the config's risk, multiple 1, of the dataset balance
//! (`model::targets`).
//!
//! On the hand-worked quotes of `backtest_targets`, the probe of `common::dataset` trades a
//! baseline long at every decision on a 25M USD account, where its dataset runs hold 10M. Each
//! entry is its stake of the account, rounded down, over the stop for a bracket and over the ATR
//! for a time-only target, at a multiple of the stake. The rows stay at the reference stake
//! whatever the dataset run's P&L or replay start.
//!
//! The reference stake is 4 USD of 10M. The entries' stake is 10 USD of the 25M account.

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

fn account(multiple: i64) -> Account {
    Account {
        balance: Money::from("25000000 USD"),
        multiple: Decimal::from(multiple),
    }
}

/// Returns the window of the hand-worked quotes' decisions, 09:00 to 12:00.
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

/// P&L before an entry moves the equity by a few dollars, not the size.
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

/// The time-only target of `backtest_targets` closes after 30 minutes. Its R is the ATR of 2 hourly
/// mid bars: none at 09:00, 1.60 at 10:00, 2.40 at 11:00 and 1.70 at 12:00.
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
/// loses 3.64 USD at 09:20, which on the run's equity would leave under an ounce to stake at 10:00.
/// Rows are sized at the balance, not the equity, whether the replay started at 08:30 or 09:30.
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
