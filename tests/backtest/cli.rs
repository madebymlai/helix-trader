//! The binary's refusals of a command line before it runs anything: a flag its subcommand does not
//! take, a required one left out, two that conflict, a value it cannot read, an unknown strategy, a
//! window or a train's span not ending after its start, a config it cannot load, and a train of a
//! strategy with no model. Each exits non-zero, its stderr naming the flag or the facts ahead of
//! clap's usage, printing nothing and creating no catalog. The binary runs in a directory of its
//! own with no environment, so that neither the shell nor a `.env` above the crate's root gives a
//! flag its value: the flags fall back on the environment, which `main` loads from `.env`.

use std::process::Command;

use rstest::rstest;

const FILL_CATALOG: [&str; 8] = [
    "fill-catalog",
    "ema",
    "--config",
    "ema.toml",
    "--start",
    "2024-01-02",
    "--end",
    "2024-03-01T12:30:00Z",
];

const BACKTEST: [&str; 8] = [
    "backtest",
    "ema",
    "--config",
    "ema.toml",
    "--start",
    "2024-01-02",
    "--end",
    "2024-03-01T12:30:00Z",
];

const COSTS: [&str; 4] = ["--commission", "3 EUR", "--balance", "10000 EUR"];

const LIVE: [&str; 4] = ["live", "ema", "--config", "ema.toml"];

const TRAIN: [&str; 6] = [
    "train",
    "model_ema",
    "--config",
    "model_ema.toml",
    "--at",
    "2026-10-23",
];

const BAD_CONFIG: &str = r#"
[data]
series = []
warmup = "720h"

[trade]
instruments = [{ instrument_id = "ESM24.CME" }]
risk = "0.5%"
"#;

fn argv<'a>(parts: &[&[&'a str]]) -> Vec<&'a str> {
    parts.concat()
}

/// Each value is joined to its flag, so that clap reads `-0.035 USD` or `-1s` as a value, not a
/// flag.
#[rstest]
#[case::fill_catalog_given_an_ib_account(
    argv(&[&FILL_CATALOG, &["--account", "DU1234567"]]),
    &["--account"]
)]
#[case::fill_catalog_given_a_leverage(argv(&[&FILL_CATALOG, &["--leverage", "2"]]), &["--leverage"])]
#[case::a_backtest_without_a_commission(
    argv(&[&BACKTEST, &["--balance", "10000 EUR"]]),
    &["--commission", "--commission-rate", "--commission-per-quantity"]
)]
#[case::a_backtest_without_a_balance(
    argv(&[&BACKTEST, &["--commission", "3 EUR"]]),
    &["--balance"]
)]
#[case::a_backtest_given_an_amount_and_a_rate(
    argv(&[&BACKTEST, &COSTS, &["--commission-rate", "0.0005"]]),
    &["--commission <", "--commission-rate"]
)]
#[case::a_backtest_given_an_amount_and_an_amount_per_quantity(
    argv(&[&BACKTEST, &COSTS, &["--commission-per-quantity", "0.035 EUR"]]),
    &["--commission <", "--commission-per-quantity"]
)]
#[case::a_backtest_given_a_rate_and_an_amount_per_quantity(
    argv(&[
        &BACKTEST,
        &["--balance", "10000 EUR", "--commission-rate", "0.0005"],
        &["--commission-per-quantity", "0.035 EUR"],
    ]),
    &["--commission-rate", "--commission-per-quantity"]
)]
#[case::a_negative_commission_per_quantity(
    argv(&[&BACKTEST, &["--balance", "10000 EUR", "--commission-per-quantity=-0.035 USD"]]),
    &["--commission-per-quantity", "-0.035 USD"]
)]
#[case::a_commission_per_quantity_of_no_currency(
    argv(&[&BACKTEST, &["--balance", "10000 EUR", "--commission-per-quantity=0.035"]]),
    &["--commission-per-quantity", "0.035"]
)]
#[case::a_commission_per_quantity_of_no_amount(
    argv(&[&BACKTEST, &["--balance", "10000 EUR", "--commission-per-quantity=USD"]]),
    &["--commission-per-quantity", "USD"]
)]
#[case::a_commission_per_quantity_of_no_currency_nautilus_has(
    argv(&[&BACKTEST, &["--balance", "10000 EUR", "--commission-per-quantity=0.035 XYZ"]]),
    &["--commission-per-quantity", "0.035 XYZ"]
)]
#[case::a_commission_of_no_currency(
    argv(&[&BACKTEST, &["--commission", "3", "--balance", "10000 EUR"]]),
    &["--commission", "'3'"]
)]
#[case::a_commission_of_no_amount(
    argv(&[&BACKTEST, &["--commission", "EUR", "--balance", "10000 EUR"]]),
    &["--commission", "EUR"]
)]
#[case::a_commission_of_no_currency_nautilus_has(
    argv(&[&BACKTEST, &["--commission", "3 XYZ", "--balance", "10000 EUR"]]),
    &["--commission", "3 XYZ"]
)]
#[case::a_commission_in_words(
    argv(&[&BACKTEST, &["--commission", "three EUR", "--balance", "10000 EUR"]]),
    &["--commission", "three EUR"]
)]
#[case::a_balance_of_no_currency(
    argv(&[&BACKTEST, &["--commission", "3 EUR", "--balance", "3"]]),
    &["--balance", "'3'"]
)]
#[case::a_balance_of_no_amount(
    argv(&[&BACKTEST, &["--commission", "3 EUR", "--balance", "EUR"]]),
    &["--balance", "'EUR'"]
)]
#[case::a_balance_of_no_currency_nautilus_has(
    argv(&[&BACKTEST, &["--commission", "3 EUR", "--balance", "3 XYZ"]]),
    &["--balance", "3 XYZ"]
)]
#[case::a_balance_in_words(
    argv(&[&BACKTEST, &["--commission", "3 EUR", "--balance", "three EUR"]]),
    &["--balance", "three EUR"]
)]
#[case::a_negative_latency(argv(&[&BACKTEST, &COSTS, &["--latency=-1s"]]), &["--latency", "-1s"])]
#[case::a_latency_of_nothing(argv(&[&BACKTEST, &COSTS, &["--latency=0s"]]), &["--latency", "0s"])]
#[case::a_latency_of_no_unit(argv(&[&BACKTEST, &COSTS, &["--latency=150"]]), &["--latency", "150"])]
#[case::a_latency_in_words(argv(&[&BACKTEST, &COSTS, &["--latency=fast"]]), &["--latency", "fast"])]
#[case::a_backtest_given_a_leverage(
    argv(&[&BACKTEST, &COSTS, &["--leverage", "10"]]),
    &["--leverage"]
)]
#[case::a_backtest_given_a_dataset_balance(
    argv(&[&BACKTEST, &COSTS, &["--dataset-balance", "10000000 EUR"]]),
    &["--dataset-balance"]
)]
#[case::a_trader_id_nautilus_refuses(
    argv(&[&LIVE, &["--trader-id", "HELIX"]]),
    &["--trader-id", "HELIX"]
)]
#[case::live_given_a_leverage(argv(&[&LIVE, &["--leverage", "10"]]), &["--leverage"])]
#[case::live_given_a_retrain_commission(
    argv(&[&LIVE, &["--retrain-commission", "3 USD"]]),
    &["--retrain-commission"]
)]
#[case::live_given_a_retrain_commission_rate(
    argv(&[&LIVE, &["--retrain-commission-rate", "0.00003"]]),
    &["--retrain-commission-rate"]
)]
#[case::live_given_a_retrain_commission_per_quantity(
    argv(&[&LIVE, &["--retrain-commission-per-quantity", "0.035 USD"]]),
    &["--retrain-commission-per-quantity"]
)]
#[case::live_given_a_retrain_balance(
    argv(&[&LIVE, &["--retrain-balance", "10000 USD"]]),
    &["--retrain-balance"]
)]
#[case::a_train_given_a_leverage(argv(&[&TRAIN, &["--leverage", "2"]]), &["--leverage"])]
#[case::a_train_given_a_commission(argv(&[&TRAIN, &["--commission", "3 EUR"]]), &["--commission"])]
#[case::a_train_given_a_commission_rate(
    argv(&[&TRAIN, &["--commission-rate", "0.00003"]]),
    &["--commission-rate"]
)]
#[case::a_train_given_a_commission_per_quantity(
    argv(&[&TRAIN, &["--commission-per-quantity", "0.035 USD"]]),
    &["--commission-per-quantity"]
)]
#[case::a_train_given_a_balance(argv(&[&TRAIN, &["--balance", "10000 USD"]]), &["--balance"])]
#[case::a_train_without_its_time(argv(&[&TRAIN[..4]]), &["--at"])]
#[case::a_train_given_a_time_and_a_span(
    argv(&[&TRAIN, &["--from", "2026-10-21", "--to", "2026-10-23"]]),
    &["--at", "--from"]
)]
#[case::a_train_over_a_span_without_its_end(
    argv(&[&TRAIN[..4], &["--from", "2026-10-21"]]),
    &["--to"]
)]
#[case::a_train_over_a_span_without_its_start(
    argv(&[&TRAIN[..4], &["--to", "2026-10-23"]]),
    &["--from"]
)]
#[case::a_train_over_a_span_ending_before_it_starts(
    argv(&[&TRAIN[..4], &["--from", "2026-10-23", "--to", "2026-10-21"]]),
    &["2026-10-23T00:00:00Z", "2026-10-21T00:00:00Z"]
)]
#[case::fill_catalog_without_a_strategy(
    argv(&[&FILL_CATALOG[..1], &FILL_CATALOG[2..]]),
    &["<STRATEGY>"]
)]
#[case::a_backtest_without_a_strategy(
    argv(&[&BACKTEST[..1], &BACKTEST[2..], &COSTS]),
    &["<STRATEGY>"]
)]
#[case::live_without_a_strategy(argv(&[&LIVE[..1], &LIVE[2..]]), &["<STRATEGY>"])]
#[case::a_train_without_a_strategy(argv(&[&TRAIN[..1], &TRAIN[2..]]), &["<STRATEGY>"])]
#[case::fill_catalog_of_a_strategy_unknown(
    argv(&[&FILL_CATALOG[..1], &["EMA_CROSS"], &FILL_CATALOG[2..]]),
    &["EMA_CROSS"]
)]
#[case::a_backtest_of_a_strategy_unknown(
    argv(&[&BACKTEST[..1], &["EMA_CROSS"], &BACKTEST[2..], &COSTS]),
    &["EMA_CROSS"]
)]
#[case::live_of_a_strategy_unknown(argv(&[&LIVE[..1], &["EMA_CROSS"], &LIVE[2..]]), &["EMA_CROSS"])]
#[case::a_train_of_a_strategy_unknown(
    argv(&[&TRAIN[..1], &["EMA_CROSS"], &TRAIN[2..]]),
    &["EMA_CROSS"]
)]
#[case::a_fill_starting_after_its_end(
    argv(&[&FILL_CATALOG[..5], &["2024-03-02"], &FILL_CATALOG[6..]]),
    &["2024-03-02T00:00:00Z", "2024-03-01T12:30:00Z"]
)]
#[case::a_fill_starting_at_its_end(
    argv(&[&FILL_CATALOG[..5], &["2024-03-01T12:30:00Z"], &FILL_CATALOG[6..]]),
    &["2024-03-01T12:30:00Z"]
)]
#[case::a_backtest_starting_after_its_end(
    argv(&[&BACKTEST[..5], &["2024-03-02"], &BACKTEST[6..], &COSTS]),
    &["2024-03-02T00:00:00Z", "2024-03-01T12:30:00Z"]
)]
#[case::a_config_the_strategy_cannot_load(
    argv(&[&FILL_CATALOG[..3], &["bad.toml"], &FILL_CATALOG[4..]]),
    &["bad.toml", "data.series"]
)]
#[case::a_config_missing(
    argv(&[&FILL_CATALOG[..3], &["missing.toml"], &FILL_CATALOG[4..]]),
    &["missing.toml"]
)]
#[case::a_train_of_a_strategy_with_no_model(
    argv(&[&TRAIN[..1], &["ema"], &TRAIN[2..]]),
    &[" ema "]
)]
fn the_binary_refuses_a_command_line_naming_why_and_runs_nothing(
    #[case] argv: Vec<&str>,
    #[case] named: &[&str],
) {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("bad.toml"), BAD_CONFIG).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_helix-trader"))
        .env_clear()
        .current_dir(dir.path())
        .args(&argv)
        .output()
        .unwrap();

    let (stdout, stderr) = (
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(!output.status.success(), "{stdout}{stderr}");

    // Clap follows its refusal with the subcommand's usage, which lists every flag
    let refusal = stderr.split("\nUsage:").next().unwrap();
    for part in named {
        assert!(refusal.contains(part), "{part}: {stderr}");
    }
    assert_eq!(stdout, "");
    assert!(!dir.path().join("catalog").exists());
}
