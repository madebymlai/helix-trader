//! IC Markets' gold, its P&L in USD (`common::ic`), on an account in
//! another currency, the strategy converting through one of IC's FX pairs
//! (`[trade] conversions`), whose quotes the run replays beside the
//! gold's. A recorder buys at a stake on the 10:00 bar, which closes at
//! 4006.00, the mid of its last quote, 4005.90 / 4006.10, its one R a
//! distance from that close to its stop. IC's simulated account has the
//! balance's currency as its base, as IC's live account has its deposit
//! currency. One node per thread, so one run per test.

use std::{cell::RefCell, path::Path, process::Command, rc::Rc};

use helix_trader::{
    backtest::{Commission, Outcome},
    strategy::{
        TickSeries,
        conversions::Conversions,
        orders::{EntryRisk, entry_risk},
        risk::Stake,
    },
};
use nautilus_model::{
    data::{BarType, NautilusDataType},
    enums::OrderSide,
    identifiers::InstrumentId,
    orders::Order,
    types::{Currency, Money, Quantity},
};
use rust_decimal::Decimal;

use crate::common::{
    ic,
    recorder::{Record, Recorder},
};

fn decimal(s: &str) -> Decimal {
    s.parse().unwrap()
}

/// A run on `balance` converting through `pair`, its id, base and quote
/// currencies and price precision, quoted at `[bid, ask]` every minute, at
/// `commission`: the recorder buys at 1% of equity on the 10:00 bar, one R
/// of `one_r` USD an ounce, and again on the bars of `again`, after closing
/// its positions on them. Its outcome and record.
fn converted_run(
    balance: &str,
    pair: (&str, Currency, Currency, u8),
    [bid, ask]: [&str; 2],
    commission: Commission,
    one_r: &str,
    again: Vec<usize>,
) -> (Outcome, Rc<RefCell<Record>>) {
    let (id, base, quote, precision) = pair;
    let catalog = ic::catalog();
    ic::store_pair(
        catalog.path(),
        ic::pair(id, base, quote, precision),
        ic::pair_quotes(id, bid, ask),
    );
    let mut backtest = ic::backtest(&catalog);
    backtest.balance = Money::from(balance);
    backtest.commission = commission;
    backtest.ticks = vec![TickSeries {
        data: NautilusDataType::QuoteTick,
        instrument_id: InstrumentId::from(id),
    }];
    let stake = Stake::new("1%".parse().unwrap());
    let staked = std::iter::repeat_n((OrderSide::Buy, decimal(one_r), stake), 1 + again.len());
    let (recorder, record) = Recorder::staking(BarType::from(ic::HOUR_BARS), staked.collect());
    let rates = Conversions::new(vec![InstrumentId::from(id)]).rates();

    let outcome = backtest
        .run(recorder.converting(rates).closing_on(again))
        .unwrap();

    (outcome, record)
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

/// On 100,000 EUR, EURUSD at 1.24990 / 1.25010, its mid 1.25000 USD per
/// EUR: 125,000 USD. 1% of it, 1,250 USD, over one R of 10.00 USD an
/// ounce buys 125 oz, whose R is 1,250 USD, in the gold's P&L currency.
/// At the bid it would buy 124, taken as USD 100, at the rate swapped 80.
#[test]
fn a_trade_is_sized_on_the_equity_converted_into_its_pnl_currency_at_the_mid() {
    let eurusd = (ic::EURUSD, Currency::EUR(), Currency::USD(), 5);

    let (_, record) = converted_run(
        "100000 EUR",
        eurusd,
        ["1.24990", "1.25010"],
        Commission::Rate(Decimal::new(3, 5)),
        "10",
        vec![],
    );

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

/// On 15,000,000 JPY, USDJPY at 149.990 / 150.010, its mid 150.000 JPY
/// per USD, a pair far from 1: 100,000 USD. 1% of it, 1,000 USD, over one
/// R of 7.00 USD an ounce buys 142 oz. At the rate swapped, 2,250,000,000
/// USD, it would buy the gold's maximum, 10,000 oz, whose margin the
/// account cannot hold.
#[test]
fn a_rate_into_the_pnl_currency_is_read_in_its_direction_on_a_pair_far_from_one() {
    let usdjpy = (ic::USDJPY, Currency::USD(), Currency::JPY(), 3);

    let (_, record) = converted_run(
        "15000000 JPY",
        usdjpy,
        ["149.990", "150.010"],
        Commission::Rate(Decimal::new(3, 5)),
        "7",
        vec![],
    );

    let filled: Vec<Quantity> = filled(&record).into_iter().map(|(q, _)| q).collect();
    assert_eq!(filled, [Quantity::from(142)]);
}

/// EURUSD at 1.25000 both sides, 0.80 EUR per USD, and 4 USD an order:
/// the account books in EUR alone, as IC's account in EUR does. The buy
/// of 10:00, 125 oz, is charged 3.20 EUR; closed at 11:00, it gains 125
/// oz times 4011.90 less 4006.10, 725 USD, 580 EUR, less its close's 3.20
/// EUR. Bought again then, on 100,573.60 EUR, 125,717 USD, 125 oz at
/// 4012.15, charged 3.20 EUR: the balance is 100,000 less 9.60 plus 580
/// EUR, and the position open at the end holds IC's margin, 1/30 of
/// 501,518.75 USD, 16,717.29 USD, in EUR: 13,373.83.
#[test]
fn the_account_books_its_margin_commissions_and_pnl_in_the_balances_currency() {
    let eurusd = (ic::EURUSD, Currency::EUR(), Currency::USD(), 5);

    let (outcome, record) = converted_run(
        "100000 EUR",
        eurusd,
        ["1.25000", "1.25000"],
        Commission::PerOrder(Money::from("4 USD")),
        "10",
        vec![1],
    );

    let summary = &outcome.result.summary;
    assert_eq!(summary["account.ICMARKETS.base_currency"], "EUR");
    let balances: Vec<(&String, &String)> = summary
        .iter()
        .filter(|(key, _)| key.starts_with("account.ICMARKETS.balance.") && key.ends_with(".total"))
        .collect();
    assert_eq!(
        balances,
        [(
            &"account.ICMARKETS.balance.EUR.total".to_string(),
            &"100570.40 EUR".to_string()
        )]
    );
    let record = record.borrow();
    let [margin] = record.margins.as_slice() else {
        panic!("{:#?}", record.margins)
    };
    assert_eq!(margin.maintenance, Money::from("13373.83 EUR"));
}

/// The backtest command running the EMA crossover on the gold's hour
/// bars of the catalog at `catalog`, from 09:00 to 12:00, at a stake of
/// 1%, converting through IC's EURUSD, on `balance`: its output.
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
            gold = ic::INSTRUMENT_ID,
            eurusd = ic::EURUSD,
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

/// A catalog of the gold and IC's EURUSD, quoted 1.24990 / 1.25010.
fn catalog_with_eurusd() -> tempfile::TempDir {
    let catalog = ic::catalog();
    ic::store_pair(
        catalog.path(),
        ic::pair(ic::EURUSD, Currency::EUR(), Currency::USD(), 5),
        ic::pair_quotes(ic::EURUSD, "1.24990", "1.25010"),
    );
    catalog
}

/// EUR reaches USD, the gold's P&L currency, through EURUSD, as the
/// catalog defines it: the backtest command runs on a balance in EUR,
/// IC's account keeping it in EUR.
#[test]
fn a_backtest_runs_on_a_balance_its_conversions_reach_the_pnl_currency_from() {
    let catalog = catalog_with_eurusd();

    let output = backtest_command(catalog.path(), "100000 EUR");

    let (stdout, stderr) = (
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(output.status.success(), "{stdout}{stderr}");
    assert!(
        stdout.contains("account.ICMARKETS.balance.EUR.total"),
        "{stdout}"
    );
}

/// EURUSD reaches no yen: a balance in JPY is refused before the run,
/// naming the yen and the dollar, the gold's P&L currency.
#[test]
fn a_backtest_on_a_balance_its_conversions_do_not_reach_the_pnl_currency_from_is_refused() {
    let catalog = catalog_with_eurusd();

    let output = backtest_command(catalog.path(), "15000000 JPY");

    let (stdout, stderr) = (
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(!output.status.success(), "{stdout}{stderr}");
    assert!(
        [ic::INSTRUMENT_ID, "JPY", "USD"]
            .iter()
            .all(|named| stderr.contains(named)),
        "{stderr}"
    );
    assert!(!stdout.contains("Orders:"), "{stdout}");
}
