//! Tests a dataset balance in a different currency from the target's P&L, converted through
//! `[trade] conversions`. The `model_ema` strategy trades IC's gold with P&L in USD (`common::ic`),
//! on a balance in EUR. It converts through IC's EURUSD, whose quotes the dataset run replays. Each
//! row's reference stake is a share of the balance, converted at the rate cached at its decision.
//!
//! - Decisions are hourly on 1-minute bars. The bracket is 7.5 bps of the mid away and closes after
//!   45 minutes, so with gold's mid at 4000.10 one R is 3.00 USD an ounce. The stake is 0.00004% of
//!   20M EUR.
//! - EURUSD is 1.20000 from 09:30, and 1.60000 from 10:30 to 12:00. At 09:00 none is cached, so
//!   there is no rate and no row.
//! - At 10:00 the stake is 9.60 USD, so 3 whole oz and one R of 9 USD. At 11:00 and 12:00 it is
//!   12.80 USD, so 4 oz and one R of 12 USD. Unconverted it would be 2 oz at each.
//! - On the flat quotes neither exit is reached, and each side closes at its limit across the
//!   spread, losing 0.20 USD an ounce plus the commission on its two fills at 30 USD per million.

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
use nautilus_model::{
    data::QuoteTick,
    types::{Currency, Money},
};
use rstest::rstest;

use crate::common::ic;

/// Hourly decisions on 1-minute bars, with averages of 1 and 2.
const CONFIG: &str = r#"
    [data]
    series = [
        { data = "bars", instrument_id = "XAUUSD.ICMARKETS", bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" } },
    ]
    warmup = "1h"

    [indicators.fast]
    period = 1

    [indicators.slow]
    period = 2

    [trade]
    instruments = [{ instrument_id = "XAUUSD.ICMARKETS" }]
    risk = "0.00004%"
    conversions = ["EURUSD.ICMARKETS"]

    [model]
    dataset = { commission = { rate = "0.00003" }, balance = "20000000 EUR" }
    decision = { every = "1h" }
    target = { bracket = { take_profit = { bps = 7.5 }, stop = { bps = 7.5 } }, limit = "45m" }
    retrain = "24h"
    window = "46h"
    calibration = "12h"
    margins = { long = "0R", short = "0R" }
    min_rows = 1
    learner = { kind = "fake" }
"#;

fn gold_quotes() -> Vec<QuoteTick> {
    let mut quotes = vec![];
    let mut time = ic::at("08:00:00");
    while time < ic::at("13:00:00") {
        quotes.push(ic::quote(time, "4000.00", "4000.20"));
        time += SignedDuration::from_secs(5);
    }
    quotes
}

fn eurusd_quotes() -> Vec<QuoteTick> {
    let between = |bid, ask, from: &str, to: &str| {
        let (from, to) = (ic::at(from), ic::at(to));
        ic::pair_quotes(ic::EURUSD, bid, ask)
            .into_iter()
            .filter(move |quote| {
                let time = Timestamp::from(quote.ts_event);
                from <= time && time < to
            })
    };
    between("1.19990", "1.20010", "09:30:00", "10:30:00")
        .chain(between("1.59990", "1.60010", "10:30:00", "12:00:00"))
        .collect()
}

fn rows() -> Vec<TargetRow> {
    let config = ModelEmaConfig::load(CONFIG).unwrap();
    let catalog = ic::catalog_of(gold_quotes(), ic::at("08:00:00"), ic::at("13:00:00"));
    ic::store_pair(
        catalog.path(),
        ic::pair(ic::EURUSD, Currency::EUR(), Currency::USD(), 5),
        eurusd_quotes(),
    );
    let requirements = config.data_requirements();
    let venue = config.model().dataset;
    let backtest = Backtest {
        bar_types: requirements.bar_types,
        ticks: requirements.ticks,
        start: ic::at("08:00:00"),
        end: ic::at("13:00:00"),
        commission: venue.commission,
        latency: venue.latency,
        pairs: config.trade.untraded_pairs(),
        balance: venue.balance,
        ..ic::backtest(&catalog)
    };
    let rows = SettledRows::default();
    let mode = Mode::Dataset { rows: rows.clone() };

    backtest
        .run_dataset(ModelEma::new(config, mode).unwrap())
        .unwrap();

    rows.take().unwrap()
}

fn units(rows: &[TargetRow]) -> Vec<(Timestamp, Money, Money)> {
    rows.iter()
        .map(|row| {
            (
                Timestamp::from(row.decision.ts_event),
                row.outcomes.long.unit,
                row.outcomes.short.unit,
            )
        })
        .collect()
}

#[rstest]
fn each_rows_reference_stake_is_of_the_balance_converted_at_its_decisions_rate() {
    let rows = std::thread::spawn(rows).join().unwrap();

    let [nine, twelve] = [Money::from("9 USD"), Money::from("12 USD")];
    assert_eq!(
        units(&rows),
        [
            (ic::at("10:00:00.010"), nine, nine),
            (ic::at("11:00:00.010"), twelve, twelve),
            (ic::at("12:00:00.010"), twelve, twelve),
        ]
    );
}

fn pnls(rows: &[TargetRow]) -> Vec<(Timestamp, Money, Money)> {
    rows.iter()
        .map(|row| {
            (
                Timestamp::from(row.decision.ts_event),
                row.outcomes.long.pnl,
                row.outcomes.short.pnl,
            )
        })
        .collect()
}

/// Each side is sent at its row's size: 3 oz loses 0.60 + 2 x 0.36 = -1.32 USD, and 4 oz 0.80 + 2 x
/// 0.48 = -1.76 USD.
#[rstest]
fn the_dataset_run_sends_each_side_at_the_size_its_row_is_priced_at() {
    let rows = std::thread::spawn(rows).join().unwrap();

    let [three, four] = [Money::from("-1.32 USD"), Money::from("-1.76 USD")];
    assert_eq!(
        pnls(&rows),
        [
            (ic::at("10:00:00.010"), three, three),
            (ic::at("11:00:00.010"), four, four),
            (ic::at("12:00:00.010"), four, four),
        ]
    );
}
