//! CME gold on GLBX (`common::glbx`), its P&L in USD, traded through IB,
//! the strategy converting through IB's EUR/USD on IDEALPRO
//! (`common::idealpro`), read as its 1-minute BID and ASK bars, as its data
//! requirements carry them. IB's account has no base currency, and Nautilus
//! books such a simulated account in each currency apart, booking no loss
//! in a currency it does not hold: a backtest on a balance in another
//! currency than the gold's is refused, one in USD runs. A model's dataset
//! run, whose account is frozen and books nothing, takes a balance in EUR,
//! its reference converted at the pair's bars. One node per thread, so one
//! run per test.

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
    enums::OmsType,
    types::{Currency, Money},
};
use rstest::rstest;

use crate::common::{glbx, ic, idealpro};

/// The gold at 4000.00 / 4000.20 every 5 s from 08:00 to 13:00: its mid
/// 4000.10.
fn gold_quotes() -> Vec<QuoteTick> {
    let mut quotes = vec![];
    let mut time = ic::at("08:00:00");
    while time < ic::at("13:00:00") {
        quotes.push(glbx::quote(time, "4000.00", "4000.20"));
        time += SignedDuration::from_secs(5);
    }
    quotes
}

/// A catalog of the gold and its quotes, from 08:00 to 13:00, and of
/// EUR/USD and its BID and ASK bars at 1.19990 and 1.20010, their mid 1.2
/// USD per EUR, from `pair_from` to 12:00.
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

/// The backtest command running the EMA crossover on the gold's hour MID
/// bars, the shortest crossover, at a stake of 1%, converting through
/// IB's EUR/USD, on the catalog at `catalog` from 09:00 to 12:00 on
/// `balance`, no commission: its output.
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

/// EUR reaches USD through EUR/USD, but IB's simulated account, holding
/// EUR alone, would book none of the gold's USD losses: the backtest is
/// refused before the run, naming the gold, its venue, both currencies
/// and the Nautilus code booking such an account.
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
        "update_balance_multi_currency",
    ] {
        assert!(stderr.contains(named), "{named}: {stderr}");
    }
    assert!(!stdout.contains("Orders:"), "{stdout}");
}

/// On a balance in USD, the gold's P&L currency, the same backtest runs,
/// IB's account keeping USD.
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

/// IDEALPRO, IB's as GLBX is, is read for EUR/USD alone, the pair the gold
/// converts through, and nothing is traded on it: its account starts with
/// nothing in the balance's currency, as a venue no broker trades does, so
/// that the run's money is GLBX's 100,000 USD alone, and the report lists
/// IDEALPRO's account empty.
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

/// Hourly decisions on the gold's 1-minute bars, its averages of 1 and 2
/// of them; a bracket 7.5 bps of the decision's mid away on both sides,
/// closed after 45 minutes, at the reference stake of 0.00004% of the
/// dataset balance of 20M EUR, converted into USD through IB's EUR/USD.
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
        holdout = "12h"
        margins = {{ long = "0R", short = "0R" }}
        min_expectancy = "0R"
        min_trades = 1
        learner = {{ kind = "fake" }}
        "#,
        gold = glbx::INSTRUMENT_ID,
        eurusd = idealpro::EURUSD,
    )
}

/// The rows the dataset run of `model_config` settles from 08:00 to
/// 13:00, EUR/USD's bars from 09:30, its data the strategy's requirements,
/// its venue the config's dataset venue, as the commands' dataset runs
/// are.
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
        venue_oms: |_| OmsType::Hedging,
        logging: LoggerConfig::default(),
    };
    let rows = SettledRows::default();
    let mode = Mode::Dataset { rows: rows.clone() };

    backtest
        .run_dataset(ModelEma::new(config, mode).unwrap())
        .unwrap();

    rows.take().unwrap()
}

/// At 09:00 none of EUR/USD's bars is cached: no rate, no row. From 10:00
/// its last BID and ASK bars close at 1.19990 and 1.20010, their mid 1.2:
/// 20M EUR is 24M USD, 0.00004% of it 9.60 USD, over the stop's 3.00 USD
/// on a contract, 7.5 bps of 4000.10 on the nearest cent, 3.2 contracts,
/// whole contracts 3, one R worth 9 USD. Unconverted, 20M taken as USD
/// would be 2 contracts, as would the rate swapped.
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
