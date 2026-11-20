//! IC Markets' gold, its P&L in USD (`common::ic`), on an account in another currency, the strategy
//! converting through one of IC's FX pairs (`[trade] conversions`), whose quotes the run replays
//! beside the gold's. A recorder buys at a stake on the 10:00 bar, which closes at 4006.00, the mid
//! of its last quote, 4005.90 / 4006.10, its one R a distance from that close to its stop. IC's
//! simulated account has the balance's currency as its base, as IC's live account has its deposit
//! currency.

use std::{cell::RefCell, path::Path, process::Command, rc::Rc};

use helix_trader::{
    backtest::{Commission, Outcome},
    strategy::{
        conversions::Conversions,
        orders::{EntryRisk, entry_risk},
        requirements::TickSeries,
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
use rstest::rstest;
use rust_decimal::Decimal;

use crate::common::{
    ic,
    recorder::{Record, Recorder},
};

fn decimal(s: &str) -> Decimal {
    s.parse().unwrap()
}

/// A run on `balance` converting through `pair`, its id, base and quote currencies and price
/// precision, quoted at `[bid, ask]` every minute, at `commission`: the recorder buys at 1% of
/// equity on the 10:00 bar, one R of `one_r` USD an ounce, and again on the bars of `again`, after
/// closing its positions on them. Its outcome and record.
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

/// On 100,000 EUR, EURUSD at 1.24990 / 1.25010, its mid 1.25000 USD per EUR: 125,000 USD. 1% of it,
/// 1,250 USD, over one R of 10.00 USD an ounce buys 125 oz, whose R is 1,250 USD, in the gold's P&L
/// currency. At the bid it would buy 124, taken as USD 100, at the rate swapped 80.
#[rstest]
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

/// EURUSD at 1.25000 both sides, 0.80 EUR per USD, and 4 USD an order: the account books in EUR
/// alone, as IC's account in EUR does. The buy of 10:00, 125 oz, is charged 3.20 EUR; closed at
/// 11:00, it gains 125 oz times 4011.90 less 4006.10, 725 USD, 580 EUR, less its close's 3.20 EUR.
/// Bought again then, on 100,573.60 EUR, 125,717 USD, 125 oz at 4012.15, charged 3.20 EUR: the
/// balance is 100,000 less 9.60 plus 580 EUR, and the position open at the end holds IC's margin,
/// 1/30 of 501,518.75 USD, 16,717.29 USD, in EUR: 13,373.83.
#[rstest]
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

/// The backtest command running the EMA crossover trading `traded` on its hour bars of the catalog
/// at `catalog`, from 09:00 to 12:00, at a stake of 1%, converting through IC's EURUSD, on
/// `balance`: its output.
fn backtest_command(catalog: &Path, traded: &str, balance: &str) -> std::process::Output {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("ema.toml");
    std::fs::write(
        &config,
        format!(
            r#"
            [data]
            series = [
                {{ data = "bars", instrument_id = "{traded}", bar_spec = {{ step = 1, aggregation = "HOUR", price_type = "MID" }} }},
            ]
            warmup = "1h"

            [indicators]
            fast = {{ period = 1 }}
            slow = {{ period = 2 }}

            [trade]
            instruments = [{{ instrument_id = "{traded}" }}]
            risk = "1%"
            conversions = ["{eurusd}"]
            "#,
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

/// EUR reaches USD, the gold's P&L currency, through EURUSD, as the catalog defines it: the
/// backtest command runs on a balance in EUR, IC's account keeping it in EUR. The crossover,
/// initialized at 11:00, long, crosses down at 12:00, its close 4001.00 below the slow EMA: it
/// sells at the bid, 4000.90, 1% of 125,000 USD, the balance at EURUSD's mid, over its stop 0.5%
/// away, 4021.01, 1,250 / 20.01 = 62.5 oz, 62, a bracket of 3 orders left open at the run's end.
/// Nothing closes and no commission is charged: the balance ends at 100000.00 EUR.
#[rstest]
fn a_backtest_runs_on_a_balance_its_conversions_reach_the_pnl_currency_from() {
    let catalog = catalog_with_eurusd();

    let output = backtest_command(catalog.path(), ic::INSTRUMENT_ID, "100000 EUR");

    let (stdout, stderr) = (
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(output.status.success(), "{stdout}{stderr}");
    assert!(
        stdout.contains("account.ICMARKETS.balance.EUR.total: 100000.00 EUR"),
        "{stdout}"
    );
    assert!(stdout.contains("Orders: 3"), "{stdout}");
}

/// EURUSD reaches no yen: a balance in JPY is refused before the run, naming the yen and the
/// dollar, the gold's P&L currency.
#[rstest]
fn a_backtest_on_a_balance_its_conversions_do_not_reach_the_pnl_currency_from_is_refused() {
    let catalog = catalog_with_eurusd();

    let output = backtest_command(catalog.path(), ic::INSTRUMENT_ID, "15000000 JPY");

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

/// EURUSD traded on a balance in EUR, its P&L in USD, converts through itself: it is one of the
/// strategy's instruments, so IC, the venue it trades on, starts with the balance, not with
/// nothing, as a venue read for its pairs alone does. Its quotes, flat, cross no average: nothing
/// is traded, and the balance ends as it starts.
#[rstest]
fn a_venue_trading_one_of_its_pairs_starts_with_the_balance() {
    let catalog = catalog_with_eurusd();

    let output = backtest_command(catalog.path(), ic::EURUSD, "100000 EUR");

    let (stdout, stderr) = (
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(output.status.success(), "{stdout}{stderr}");
    assert!(
        stdout.contains("account.ICMARKETS.balance.EUR.total: 100000.00 EUR"),
        "{stdout}"
    );
}
