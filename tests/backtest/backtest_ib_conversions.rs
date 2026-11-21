//! Tests CME gold on GLBX (`common::glbx`), with P&L in USD, traded through IB. It converts through
//! IB's EUR/USD on IDEALPRO (`common::idealpro`), read as 1-minute BID and ASK bars at 1.19990 and
//! 1.20010, a mid of 1.2 USD per EUR. IB's account has no base currency. Nautilus books such a
//! simulated account in each currency apart, and books no loss in a currency it does not hold. So a
//! backtest on a balance in a different currency from the gold's is refused, and one in USD runs. A
//! dataset run has a frozen account and takes a balance in EUR. Its reference is converted at the
//! pair's bars.
//!
//! The gold quotes 4000.00 / 4000.20 from 08:00 to 13:00. The commands run the shortest EMA
//! crossover on hour bars from 09:00 to 12:00. The dataset run's bracket is 7.5 bps of the mid, and
//! its reference stake is 0.00004% of 20M EUR.

use std::{path::Path, process::Command};

use helix_trader::{
    backtest::Backtest,
    model::{
        ModelStrategy,
        handle::{Mode, SettledRows},
        targets::TargetRow,
    },
    strategy::model_ema::{ModelEma, ModelEmaConfig},
};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::logging::logger::LoggerConfig;
use nautilus_model::{
    data::QuoteTick,
    types::{Currency, Money},
};
use rstest::rstest;

use crate::common::{glbx, ic, idealpro};

fn gold_quotes() -> Vec<QuoteTick> {
    let mut quotes = vec![];
    let mut time = ic::at("08:00:00");
    while time < ic::at("13:00:00") {
        quotes.push(glbx::quote(time, "4000.00", "4000.20"));
        time += SignedDuration::from_secs(5);
    }
    quotes
}

fn catalog(pair_from: Timestamp) -> tempfile::TempDir {
    let catalog = tempfile::tempdir().unwrap();
    glbx::store_quotes(
        catalog.path(),
        &gold_quotes(),
        ic::at("08:00:00"),
        ic::at("13:00:00"),
    );
    idealpro::store(catalog.path(), "1.19990", "1.20010", pair_from);
    catalog
}

fn backtest_command(catalog: &Path, balance: &str) -> std::process::Output {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("ema.toml");
    std::fs::write(
        &config,
        format!(
            r#"
            [data]
            series = [
                {{ data = "bars", instrument_id = "{gold}", bar_spec = {{ step = 1, aggregation = "HOUR", price_type = "MID" }} }},
            ]
            warmup = "1h"

            [indicators]
            fast = {{ period = 1 }}
            slow = {{ period = 2 }}

            [trade]
            instruments = [{{ instrument_id = "{gold}" }}]
            risk = "1%"
            conversions = ["{eurusd}"]
            "#,
            gold = glbx::INSTRUMENT_ID,
            eurusd = idealpro::EURUSD,
        ),
    )
    .unwrap();
    Command::new(env!("CARGO_BIN_EXE_helix-trader"))
        .args(["backtest", "ema", "--config"])
        .arg(&config)
        .arg("--catalog")
        .arg(catalog)
        .args(["--start", &ic::at("09:00:00").to_string()])
        .args(["--end", &ic::at("12:00:00").to_string()])
        .args(["--balance", balance, "--commission", "0 USD"])
        .output()
        .unwrap()
}

#[rstest]
fn a_backtest_through_ib_on_a_balance_off_the_pnl_currency_is_refused_naming_why() {
    let catalog = catalog(ic::at("08:00:00"));

    let output = backtest_command(catalog.path(), "100000 EUR");

    let (stdout, stderr) = (
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(!output.status.success(), "{stdout}{stderr}");
    for named in [
        glbx::INSTRUMENT_ID,
        "GLBX",
        "EUR",
        "USD",
        "no base currency",
    ] {
        assert!(stderr.contains(named), "{named}: {stderr}");
    }
    assert!(!stdout.contains("Orders:"), "{stdout}");
}

#[rstest]
fn a_backtest_through_ib_on_a_balance_in_the_pnl_currency_runs() {
    let catalog = catalog(ic::at("08:00:00"));

    let output = backtest_command(catalog.path(), "100000 USD");

    let (stdout, stderr) = (
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(output.status.success(), "{stdout}{stderr}");
    assert!(
        stdout.contains("account.GLBX.balance.USD.total"),
        "{stdout}"
    );
}

/// Nothing is traded on IDEALPRO, so its account starts empty.
#[rstest]
fn a_venue_the_run_reads_for_its_pairs_alone_starts_with_nothing() {
    let catalog = catalog(ic::at("08:00:00"));

    let output = backtest_command(catalog.path(), "100000 USD");

    let (stdout, stderr) = (
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(output.status.success(), "{stdout}{stderr}");
    assert!(
        stdout.contains("account.IDEALPRO.balance.USD.total: 0.00 USD"),
        "{stdout}"
    );
}

fn model_config() -> String {
    format!(
        r#"
        [data]
        series = [
            {{ data = "bars", instrument_id = "{gold}", bar_spec = {{ step = 1, aggregation = "MINUTE", price_type = "MID" }} }},
        ]
        warmup = "1h"

        [indicators.fast]
        period = 1

        [indicators.slow]
        period = 2

        [trade]
        instruments = [{{ instrument_id = "{gold}" }}]
        risk = "0.00004%"
        conversions = ["{eurusd}"]

        [model]
        dataset = {{ commission = {{ rate = "0.00003" }}, balance = "20000000 EUR" }}
        decision = {{ every = "1h" }}
        target = {{ bracket = {{ take_profit = {{ bps = 7.5 }}, stop = {{ bps = 7.5 }} }}, limit = "45m" }}
        retrain = "24h"
        window = "46h"
        calibration = "12h"
        margins = {{ long = "0R", short = "0R" }}
        min_rows = 1
        learner = {{ kind = "fake" }}
        "#,
        gold = glbx::INSTRUMENT_ID,
        eurusd = idealpro::EURUSD,
    )
}

fn rows() -> Vec<TargetRow> {
    let config = ModelEmaConfig::load(&model_config()).unwrap();
    let catalog = catalog(ic::at("09:30:00"));
    let requirements = config.data_requirements();
    let venue = config.model().dataset;
    let backtest = Backtest {
        catalog: catalog.path().to_path_buf(),
        bar_types: requirements.bar_types,
        ticks: requirements.ticks,
        custom: requirements.custom,
        start: ic::at("08:00:00"),
        end: ic::at("13:00:00"),
        commission: venue.commission,
        latency: venue.latency,
        pairs: config.trade.untraded_pairs(),
        balance: venue.balance,
        logging: LoggerConfig::default(),
    };
    let rows = SettledRows::default();
    let mode = Mode::Dataset { rows: rows.clone() };

    backtest
        .run_dataset(ModelEma::new(config, mode).unwrap())
        .unwrap();

    rows.take().unwrap()
}

/// At 09:00 no EUR/USD bar is cached, so there is no row. From 10:00, 24M USD at 0.00004% is 9.60
/// USD, over the stop's 3.00 USD is 3.2 contracts, whole 3, so one R is 9 USD.
#[rstest]
fn a_dataset_runs_reference_is_converted_at_its_ib_pairs_bid_and_ask_bars() {
    let rows = std::thread::spawn(rows).join().unwrap();

    let nine = Money::new(9.0, Currency::USD());
    let units: Vec<(Timestamp, Money, Money)> = rows
        .iter()
        .map(|row| {
            (
                Timestamp::from(row.decision.ts_event),
                row.outcomes.long.unit,
                row.outcomes.short.unit,
            )
        })
        .collect();
    assert_eq!(
        units,
        [
            (ic::at("10:00:00.010"), nine, nine),
            (ic::at("11:00:00.010"), nine, nine),
            (ic::at("12:00:00.010"), nine, nine),
        ]
    );
}
