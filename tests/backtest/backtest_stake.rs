//! Trades sized at a stake (`strategy::orders::Trade::send`) on 10,000
//! EUR: the EMA crossover's at the config's stake, on its trades of
//! `common::trades`, and a recorder's at stakes of its choosing, on the
//! same quotes, each a buy set from its bar's close, one R of 30 EUR a
//! share. One node per thread, so one run per test.

use std::process::Command;

use helix_trader::strategy::{
    ema::EmaCross,
    orders::{EntryRisk, Stake, entry_risk},
};
use nautilus_model::{
    data::BarType,
    enums::OrderSide,
    instruments::InstrumentAny,
    orders::Order,
    types::{Money, Quantity},
};
use rust_decimal::Decimal;

use crate::common::{self, recorder::Recorder, trades};

fn decimal(s: &str) -> Decimal {
    s.parse().unwrap()
}

/// The crossover's trade of `trades::TIME_EXIT` at `risk`: its result.
fn crossover_at(risk: &str) -> nautilus_backtest::result::BacktestResult {
    let mut config = common::ema_config(1, 2, Some(common::session()));
    config.trade.risk = risk.parse().unwrap();
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);
    trades::backtest(catalog.path(), trades::start(), &levels, "0 EUR")
        .run(EmaCross::new(config).unwrap())
        .unwrap()
        .result
}

/// The crossover's one trade, the buy of 13:00 (`trades::TIME_EXIT`), is
/// set from the close, 620.60, its stop at 617.50: one R of 3.10 EUR a
/// share. At 0.45% of 10,000 EUR, 45 EUR, it buys 45 / 3.10 = 14.5
/// shares, 14 rounded down, and its time exit gains 4.20 a share: 58.80
/// EUR.
#[test]
fn the_crossovers_trade_risks_its_stake_of_the_balance_over_its_stop() {
    let result = crossover_at("0.45%");

    assert_eq!(result.total_orders, trades::TIME_EXIT_ORDERS);
    assert_eq!(common::balance(&result), "10058.80 EUR");
}

/// At 0.01% of 10,000 EUR, 1 EUR, a stop 3.10 EUR a share away buys a
/// third of a share: the trade sends nothing, and the run completes.
#[test]
fn a_stake_buying_less_than_a_share_sends_nothing() {
    let result = crossover_at("0.01%");

    assert_eq!(result.total_orders, 0);
    assert_eq!(common::balance(&result), "10000.00 EUR");
}

/// The recorder's trades on `bar_type`'s bars of `trades::TIME_EXIT`'s
/// quotes as `catalog` holds them, the first on the 11:00 bar (closing at
/// 610.60) at 1% of equity, the second on the 12:00 bar (600.60) at twice
/// that: its record.
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

/// 1% of 10,000 EUR, 100 EUR, buys 3.3 shares at 30 EUR a share, 3; twice
/// the stake, on about 9,970 EUR once the first long is 30 EUR down, buys
/// 6.6, 6: twice as many. Each entry is tagged with its R in money, 90 and
/// 180 EUR, and its stake's multiple.
#[test]
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

/// SXR8 defined with a minimum of 5 shares: the first trade's 3 shares
/// are below it, and Nautilus' risk engine denies its order; the run goes
/// on, and the second trade's 6, on the 10,000 EUR untouched, fill.
#[test]
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

/// The crossover trading SXR8, whose P&L is in EUR, on a balance of
/// 10,000 USD: its trades would be sized on equity in EUR, which the
/// account would not hold, and the backtest command refuses to run,
/// naming both currencies, before printing any result.
#[test]
fn a_backtest_whose_balance_is_not_in_a_traded_instruments_pnl_currency_is_refused() {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);
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
            instruments = [{{ instrument_id = "{id}", trade_size = "1" }}]
            risk = "{risk}"
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
        .arg(catalog.path())
        .args(["--start", &trades::start().to_string()])
        .args(["--end", &trades::end(&levels).to_string()])
        .args(["--balance", "10000 USD", "--commission", "0 USD"])
        .output()
        .unwrap();

    let (stdout, stderr) = (
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(!output.status.success(), "{stdout}{stderr}");
    assert!(
        [common::INSTRUMENT_ID, "USD", "EUR"]
            .iter()
            .all(|named| stderr.contains(named)),
        "{stderr}"
    );
    assert!(!stdout.contains("Orders:"), "{stdout}");
}
