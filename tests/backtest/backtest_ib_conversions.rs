//! CME gold on GLBX (`common::glbx`), its P&L in USD, traded through IB on
//! an account in EUR, the strategy converting through IB's EUR/USD on
//! IDEALPRO (`common::idealpro`), whose 1-minute BID and ASK bars the run
//! replays beside the gold's quotes, as its data requirements carry them.
//! IB's simulated account has no base currency, as IB's execution client
//! keeps none: it holds each currency apart. The hours closing 10:00,
//! 11:00 and 12:00 are last quoted 4005.90 / 4006.10, 4011.90 / 4012.15
//! and 4000.90 / 4001.10, so their MID bars close at 4006.00, 4012.025 and
//! 4001.00. No commission is charged: one in USD is not booked on an
//! account holding no USD. One node per thread, so one run per test.

use std::{cell::RefCell, path::Path, process::Command, rc::Rc};

use helix_trader::{
    backtest::{Backtest, Commission},
    strategy::{
        ema::EmaCrossConfig,
        orders::{EntryRisk, entry_risk},
        risk::Stake,
    },
    venues,
};
use jiff::Timestamp;
use nautilus_common::logging::logger::LoggerConfig;
use nautilus_model::{
    data::BarType,
    enums::OrderSide,
    orders::Order,
    types::{Money, Quantity},
};
use rust_decimal::Decimal;

use crate::common::{
    glbx, ic, idealpro,
    recorder::{Record, Recorder},
};

/// The EMA crossover trading gold on its hour MID bars, the shortest
/// crossover, at a stake of 1%, converting through IB's EUR/USD.
fn toml() -> String {
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
    )
}

/// A catalog of gold and its quotes, and of EUR/USD and its bars from
/// 08:00, BID at 1.24990 and ASK at 1.25010, their mid 1.25 USD per EUR.
fn catalog() -> tempfile::TempDir {
    let catalog = tempfile::tempdir().unwrap();
    glbx::store_quotes(catalog.path());
    idealpro::store(catalog.path(), "1.24990", "1.25010", ic::at("08:00:00"));
    catalog
}

/// A run from `start` to 12:00 of what the config reads, its data
/// requirements, on 100,000 EUR, each venue as its broker keeps it: the
/// recorder buys on each hour bar in turn at 1%, one R of `one_rs[i]` USD
/// a contract from the bar's close, a market entry, on equity converted at
/// the config's rates. Its record.
fn run(start: Timestamp, one_rs: &[&str]) -> Rc<RefCell<Record>> {
    let catalog = catalog();
    let config = EmaCrossConfig::load(&toml()).unwrap();
    let requirements = config.data_requirements();
    let backtest = Backtest {
        catalog: catalog.path().to_path_buf(),
        bar_types: requirements.bar_types,
        ticks: requirements.ticks,
        custom: requirements.custom,
        start,
        end: ic::at("12:00:00"),
        commission: Commission::Rate(Decimal::ZERO),
        latency: None,
        balance: Money::from("100000 EUR"),
        venue_oms: venues::oms_type,
        logging: LoggerConfig::default(),
    };
    let stake = Stake::new("1%".parse().unwrap());
    let staked = one_rs
        .iter()
        .map(|one_r| (OrderSide::Buy, one_r.parse().unwrap(), stake))
        .collect();
    let gold = BarType::from(format!("{}-1-HOUR-MID-INTERNAL", glbx::INSTRUMENT_ID));
    let (recorder, record) = Recorder::staking(gold, staked);

    backtest.run(recorder.converting(config.rates())).unwrap();

    record
}

/// The quantity filled and the risk tagged of each order of `record`.
fn filled(record: &Rc<RefCell<Record>>) -> Vec<(Quantity, Option<EntryRisk>)> {
    record
        .borrow()
        .orders
        .iter()
        .map(|order| (order.filled_qty(), entry_risk(order)))
        .collect()
}

/// On 100,000 EUR, EUR/USD's last BID and ASK bars closing at 1.24990 and
/// 1.25010, their mid 1.25000 USD per EUR: 125,000 USD. 1% of it, 1,250
/// USD, over one R of 10.00 USD a contract from the 10:00 close buys 125,
/// whose R is 1,250 USD. The contract margins nothing, so the account's
/// lack of USD denies nothing. At the BID alone it would buy 124, taken as
/// USD 100, at the rate swapped 80.
#[test]
fn a_glbx_trade_through_ib_is_sized_on_the_eur_balance_converted_at_idealpro_s_bars() {
    let record = run(ic::at("09:00:00"), &["10"]);

    assert_eq!(record.borrow().denials, Vec::<String>::new());
    assert_eq!(
        filled(&record),
        [(
            Quantity::from(125),
            Some(EntryRisk {
                r: Money::from("1250 USD"),
                multiple: Decimal::ONE,
            })
        )]
    );
}

/// From 10:00, the buy of 11:00, one R of 2.005 USD from its close
/// 4012.025, its stop on the tick 4010.02, is 623 contracts (1,250 USD
/// over 2.005), bought at the ask 4012.15. Still open at 12:00, its bid
/// 4000.90, it has lost 623 times 11.25, 7,008.75 USD, so the account's
/// equity holds 100,000 EUR and -7,008.75 USD: 117,991.25 USD. 1% of it
/// over one R of 2.00 USD from 4001.00 buys 589. On the EUR alone it would
/// buy 625; on the USD alone, below nothing, none.
#[test]
fn a_trade_sent_while_a_usd_position_is_open_is_sized_on_the_eur_balance_and_the_usd_pnl() {
    let record = run(ic::at("10:00:00"), &["2.005", "2"]);

    assert_eq!(record.borrow().denials, Vec::<String>::new());
    let quantities: Vec<Quantity> = filled(&record).into_iter().map(|(q, _)| q).collect();
    assert_eq!(quantities, [Quantity::from(623), Quantity::from(589)]);
}

/// The backtest command running the config on the catalog at `catalog`
/// from 09:00 to 12:00 on 100,000 EUR, no commission: its output.
fn backtest_command(catalog: &Path) -> std::process::Output {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("ema.toml");
    std::fs::write(&config, toml()).unwrap();
    Command::new(env!("CARGO_BIN_EXE_helix-trader"))
        .args(["backtest", "ema", "--config"])
        .arg(&config)
        .arg("--catalog")
        .arg(catalog)
        .args(["--start", &ic::at("09:00:00").to_string()])
        .args(["--end", &ic::at("12:00:00").to_string()])
        .args(["--balance", "100000 EUR", "--commission", "0 USD"])
        .output()
        .unwrap()
}

/// The crossover warms on the bars of 10:00 and 11:00, long, and crosses
/// down at 12:00, 4001.00 below its slow EMA, 4009.0125. The catalog
/// defines EUR/USD but holds none of its bars, so no rate is cached: the
/// sell is not sent, and the log says why, naming the account's EUR, the
/// gold's USD and the rates of IDEALPRO it is not converted at.
#[test]
fn with_no_rate_cached_a_trade_is_not_sent_and_the_log_says_why() {
    let catalog = tempfile::tempdir().unwrap();
    glbx::store_quotes(catalog.path());
    idealpro::store_definition(catalog.path());

    let output = backtest_command(catalog.path());

    let (stdout, stderr) = (
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(output.status.success(), "{stdout}{stderr}");
    assert!(stdout.contains("Orders: 0"), "{stdout}");
    let log = format!("{stdout}{stderr}");
    let line = log
        .lines()
        .find(|line| line.contains("not sending"))
        .unwrap_or_else(|| panic!("{log}"));
    assert!(
        [glbx::INSTRUMENT_ID, "EUR", "USD", "IDEALPRO"]
            .iter()
            .all(|named| line.contains(named)),
        "{line}"
    );
}
