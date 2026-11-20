//! Tests trades sized at a stake (`strategy::orders::Trade::send`) on 10,000 EUR.
//!
//! The EMA crossover trades at the config's stake, on its trades of `common::trades`. A recorder
//! trades at stakes of its choosing, on the same quotes. Its trades are buys set from their bar's
//! close, with one R of 30 EUR a share: the 11:00 bar (closing 610.60) at 1% of equity and the
//! 12:00 bar (600.60) at twice that.

use std::{path::Path, process::Command};

use helix_trader::strategy::{
    ema::EmaCross,
    orders::{EntryRisk, entry_risk},
    risk::Stake,
};
use nautilus_backtest::result::BacktestResult;
use nautilus_model::{
    data::BarType,
    enums::OrderSide,
    instruments::InstrumentAny,
    orders::Order,
    types::{Money, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rstest::rstest;
use rust_decimal::Decimal;

use crate::common::{self, glbx, recorder::Recorder, trades};

fn decimal(s: &str) -> Decimal {
    s.parse().unwrap()
}

fn crossover_at(risk: &str) -> BacktestResult {
    let mut config = common::ema_config(1, 2, Some(common::session()));
    config.trade.risk = Some(risk.parse().unwrap());
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);
    trades::backtest(catalog.path(), trades::start(), &levels, "0 EUR")
        .run(EmaCross::new(config).unwrap())
        .unwrap()
        .result
}

/// One R of the buy at 13:00 (`trades::TIME_EXIT`) is 3.10 EUR a share. At 0.01% it would buy a
/// third of a share, so the trade sends nothing and the run completes.
#[rstest]
#[case::fourteen_shares("0.45%", trades::TIME_EXIT_ORDERS, "10058.80 EUR")]
#[case::less_than_a_share("0.01%", 0, "10000.00 EUR")]
fn the_crossovers_trade_risks_its_stake_of_the_balance_over_its_stop(
    #[case] risk: &str,
    #[case] orders: usize,
    #[case] balance: &str,
) {
    let result = crossover_at(risk);

    assert_eq!(result.total_orders, orders);
    assert_eq!(common::balance(&result), balance);
}

fn staked(
    catalog: &tempfile::TempDir,
) -> std::rc::Rc<std::cell::RefCell<common::recorder::Record>> {
    let levels = trades::TIME_EXIT;
    let backtest = trades::backtest(catalog.path(), trades::start(), &levels, "0 EUR");
    let stake = Stake::new("1%".parse().unwrap());
    let one_r = decimal("30");
    let (recorder, record) = Recorder::staking(
        BarType::from(common::MID_BAR_TYPE),
        vec![
            (OrderSide::Buy, one_r, stake),
            (OrderSide::Buy, one_r, stake.times(decimal("2")).unwrap()),
        ],
    );
    backtest.run(recorder).unwrap();
    record
}

/// 1% buys 3.3 shares, rounded down to 3, and twice the stake 6.6, rounded down to 6.
#[rstest]
fn a_trade_at_twice_the_stake_fills_twice_the_size_tagged_with_its_multiple() {
    let catalog = trades::catalog(&trades::TIME_EXIT);

    let record = staked(&catalog);

    let filled: Vec<(Quantity, Option<EntryRisk>)> = record
        .borrow()
        .orders
        .iter()
        .map(|order| (order.filled_qty(), entry_risk(order)))
        .collect();
    let risk = |r: &str, multiple: &str| {
        Some(EntryRisk {
            r: Money::from(r),
            multiple: decimal(multiple),
        })
    };
    assert_eq!(
        filled,
        [
            (Quantity::from(3), risk("90 EUR", "1")),
            (Quantity::from(6), risk("180 EUR", "2")),
        ]
    );
}

/// SXR8 has a minimum of 5 shares, so Nautilus' risk engine denies the first trade's 3.
#[rstest]
fn a_size_below_the_instruments_minimum_is_denied_and_the_run_goes_on() {
    let InstrumentAny::Equity(mut sxr8) = common::instrument() else {
        unreachable!("SXR8 is an equity")
    };
    sxr8.min_quantity = Some(Quantity::from(5));
    let levels = trades::TIME_EXIT;
    let catalog = common::catalog_of(
        InstrumentAny::Equity(sxr8),
        &trades::quotes(&levels),
        trades::at("09:00"),
        trades::end(&levels),
    );

    let record = staked(&catalog);

    let record = record.borrow();
    let [denial] = record.denials.as_slice() else {
        panic!("{:#?}", record.denials)
    };
    assert!(denial.starts_with("QUANTITY_BELOW_MINIMUM"), "{denial}");
    let filled: Vec<Quantity> = record.orders.iter().map(Order::filled_qty).collect();
    assert_eq!(filled, [Quantity::from(0), Quantity::from(6)]);
}

/// Runs the backtest command on the crossover, with `more` as lines of TOML at the end of
/// `[trade]`, and returns whether it succeeded, its stdout and its stderr.
fn crossover_command(
    catalog: &Path,
    levels: &[f64],
    balance: &str,
    more: &str,
) -> (bool, String, String) {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("ema.toml");
    std::fs::write(
        &config,
        format!(
            r#"
            [data]
            series = [
                {{ data = "bars", instrument_id = "{id}", bar_spec = {{ step = 1, aggregation = "HOUR", price_type = "MID" }} }},
            ]
            warmup = "1h"

            [indicators]
            fast = {{ period = 1 }}
            slow = {{ period = 2 }}

            [trade]
            instruments = [{{ instrument_id = "{id}" }}]
            risk = "{risk}"
            {more}
            "#,
            id = common::INSTRUMENT_ID,
            risk = common::RISK,
        ),
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_helix-trader"))
        .args(["backtest", "ema", "--config"])
        .arg(&config)
        .arg("--catalog")
        .arg(catalog)
        .args(["--start", &trades::start().to_string()])
        .args(["--end", &trades::end(levels).to_string()])
        .args(["--balance", balance, "--commission", "0 USD"])
        .output()
        .unwrap();

    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

/// SXR8's P&L is in EUR. The command names both currencies and prints no result.
#[rstest]
fn a_backtest_whose_balance_is_not_in_a_traded_instruments_pnl_currency_is_refused() {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);

    let (succeeded, stdout, stderr) = crossover_command(catalog.path(), &levels, "10000 USD", "");

    assert!(!succeeded, "{stdout}{stderr}");
    assert!(
        [common::INSTRUMENT_ID, "USD", "EUR"]
            .iter()
            .all(|named| stderr.contains(named)),
        "{stderr}"
    );
    assert!(!stdout.contains("Orders:"), "{stdout}");
}

/// CME's gold future is a conversion pair and a `FuturesContract` with no base currency.
///
/// Nautilus' exchange rates read a pair by its base currency, so there is no rate.
#[rstest]
fn a_backtest_converting_through_a_pair_defined_without_a_base_currency_is_refused() {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);
    ParquetDataCatalog::new(catalog.path(), None, None, None, None)
        .write_instruments(vec![glbx::instrument()])
        .unwrap();

    let (succeeded, stdout, stderr) = crossover_command(
        catalog.path(),
        &levels,
        "10000 EUR",
        &format!("conversions = [\"{}\"]", glbx::INSTRUMENT_ID),
    );

    assert!(!succeeded, "{stdout}{stderr}");
    assert!(
        stderr.contains(glbx::INSTRUMENT_ID) && stderr.contains("base currency"),
        "{stderr}"
    );
    assert!(!stdout.contains("Orders:"), "{stdout}");
}
