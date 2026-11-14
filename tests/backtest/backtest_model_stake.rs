//! A model strategy sends its decisions' trades at a stake of the
//! account's equity (`strategy::orders::Trade::send`), while its targets,
//! the dataset's rows and the decisions' payoff features, are priced at the
//! reference stake: the config's risk, multiple 1, of the dataset balance,
//! a constant equity (`model::targets`). On the hand-worked targets' quotes
//! (`backtest_targets`) the probe of `common::dataset` trades the long of a
//! baseline model at every decision, on an account of 25M USD where its
//! dataset runs hold 10M USD: each entry fills at its stake of the account,
//! rounded down, a bracket's over its stop, a time-only target's over its
//! ATR, and at a multiple of it, while the rows are at the reference stake.
//! The rows do not move with the dataset run's P&L, nor with where its
//! replay starts. One node per thread.

use std::{path::Path, thread};

use crate::{
    backtest_model::hand_worked,
    backtest_targets::hand_worked_quotes,
    common::{
        dataset::{Account, Record, baseline_of, blueprint, features, replay_of, trade_staking},
        ic,
    },
};
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
use rust_decimal::Decimal;

/// The account the probe trades on, holding 25M USD where its dataset runs
/// hold 10M: the config's stake of it, 0.00004%, is 10 USD.
fn account(multiple: i64) -> Account {
    Account {
        balance: Money::from("25000000 USD"),
        multiple: Decimal::from(multiple),
    }
}

/// The window of the hand-worked quotes the probe decides over, its
/// decisions at 09:00 to 12:00.
fn window() -> (Timestamp, Timestamp) {
    (ic::at("08:30:00"), ic::at("12:30:00"))
}

/// The rows the dataset run of `config` settles over `[from, to]` of the
/// hand-worked quotes.
fn rows_of(config: &ModelConfig, from: Timestamp, to: Timestamp) -> Vec<TargetRow> {
    let config = config.clone();
    thread::spawn(move || {
        let catalog = ic::catalog_of(hand_worked_quotes(), ic::at("08:00:00"), ic::at("13:00:00"));
        replay_of(&catalog, features(), &config, from, to).unwrap()
    })
    .join()
    .unwrap()
}

/// What the probe of `config` records trading the long of a baseline model
/// published under `models`, on `account`, over `window`.
fn traded(config: &ModelConfig, models: &Path, account: Account) -> Record {
    let (config, models) = (config.clone(), models.to_path_buf());
    thread::spawn(move || {
        let (from, to) = window();
        blueprint(features(), config.clone())
            .model_set(&models)
            .unwrap()
            .side(Side::Long)
            .unwrap()
            .store()
            .publish(&baseline_of(&features(), &config, Side::Long, from, 8))
            .unwrap();
        let catalog = ic::catalog_of(hand_worked_quotes(), ic::at("08:00:00"), ic::at("13:00:00"));
        trade_staking(&catalog, features(), (from, to), &config, &models, account)
    })
    .join()
    .unwrap()
}

/// Each entry `record` filled: when, of how much, and what it risks.
fn entries(record: &Record) -> Vec<(Timestamp, Quantity, Option<EntryRisk>)> {
    record
        .entries
        .iter()
        .map(|&(ts, quantity, risk)| (Timestamp::from(ts), quantity, risk))
        .collect()
}

/// The decision at `hh_mm`, firing 10 ms after it.
fn decided(hh_mm: &str) -> Timestamp {
    ic::at(&format!("{hh_mm}:00.010"))
}

/// The units of each side of `rows`, in USD.
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

/// The hand-worked bracket's long, 7.5 bps of the mid of 4000.10 away on
/// both sides, its stop 3.00 away, 4001.10 and 3.00 after the rise at
/// 11:20. The dataset run prices its rows at the reference stake, 4 USD of
/// 10M (`RISK`): one ounce, IC's whole ounces, one R worth 3 USD. Trading
/// on 25M USD at the config's stake, 10 USD, each entry is 10 / 3.00 = 3.33
/// oz rounded down, 3 oz, its R 9 USD; at twice the stake, 20 USD, 6.67
/// rounded down, 6 oz, its R 18 USD. The trades' P&L before an entry moves
/// the account's equity by a few dollars, not the size.
#[test]
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

/// The time-only target of the hand-worked targets (`backtest_targets`):
/// closed 30 minutes after its decision, its one R the ATR of 2 hourly bars
/// of the mids, none at 09:00, 1.60 at 10:00 and 2.40 at 11:00, and at
/// 12:00, after the hour from 4000.10 to 4001.10 (the rise at 11:20), a
/// true range of 1.00, (2.40 + 1.00) / 2 = 1.70.
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

/// The time-only target's long on its ATR. At the reference stake, 4 USD
/// of 10M, the dataset's rows are of 4 / 1.60 = 2.5 oz rounded down, 2 oz,
/// one R worth 3.20 USD, at 10:00, and of 4 / 2.40 = 1.67 oz, 1 oz, worth
/// 2.40 USD, at 11:00. Trading on 25M USD at the config's stake, 10 USD,
/// the entries are 10 / 1.60 = 6.25 oz, 6 oz, and 10 / 2.40 = 4.17 oz, 4
/// oz, each one R worth 9.60 USD, and at 12:00 10 / 1.70 = 5.88 oz, 5 oz.
#[test]
fn a_model_strategys_time_only_trade_fills_at_its_stake_over_its_atr() {
    let config = time_only();
    let (from, to) = window();
    let rows = rows_of(&config, from, to);
    let models = tempfile::tempdir().unwrap();

    let record = traded(&config, models.path(), account(1));

    let filled: Vec<(Timestamp, Quantity, Decimal)> = entries(&record)
        .into_iter()
        .map(|(at, quantity, risk)| (at, quantity, risk.unwrap().multiple))
        .collect();
    assert_eq!(
        filled,
        [
            (decided("10:00"), Quantity::from(6), Decimal::ONE),
            (decided("11:00"), Quantity::from(4), Decimal::ONE),
            (decided("12:00"), Quantity::from(5), Decimal::ONE),
        ]
    );
    for (_, _, risk) in entries(&record).iter().take(2) {
        let r = risk.unwrap().r.as_f64();
        assert!((r - 9.6).abs() < 1e-6, "{r}");
    }
    let units: Vec<(f64, f64)> = units(&rows)
        .into_iter()
        .map(|(_, long, short)| (long.as_f64(), short.as_f64()))
        .collect();
    assert_eq!(units.len(), 2, "{units:?}");
    for ((long, short), unit) in units.into_iter().zip([3.2, 2.4]) {
        assert!((long - unit).abs() < 1e-6 && (short - unit).abs() < 1e-6);
    }
}

/// At a reference stake of 3 USD of 10M, a stop of 3.00 buys exactly one
/// ounce. The 09:00 decision's long loses 3.64 USD at 09:20, which on the
/// dataset run's equity would leave 2.99999891 USD to stake at 10:00, under
/// an ounce. The rows are sized at the balance, not the equity: the 10:00
/// and 11:00 rows are one ounce, one R worth 3 USD, and the same row by row
/// whether the replay started at 08:30, before that loss, or at 09:30,
/// after it.
#[test]
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
