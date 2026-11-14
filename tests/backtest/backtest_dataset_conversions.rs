//! A model strategy's dataset balance in another currency than its
//! target's P&L, converted through the pairs of `[trade] conversions`: the
//! `model_ema` strategy on IC's gold, its P&L in USD (`common::ic`), on a
//! dataset balance in EUR, converting through IC's EURUSD, whose quotes the
//! dataset run replays beside the gold's. Each row's reference stake is of
//! the balance converted at the rate cached at its decision. One node per
//! thread, so one run per test.

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
    enums::OmsType,
    types::{Currency, Money},
};

use crate::common::ic;

/// Hourly decisions on IC's gold's 1-minute bars, its averages of 1 and 2
/// of them; a bracket 7.5 bps of the decision's mid away on both sides,
/// closed after 45 minutes, at the reference stake of 0.00004% of the
/// dataset balance of 20M EUR, converted into USD through IC's EURUSD.
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
    holdout = "12h"
    margins = { long = "0R", short = "0R" }
    min_expectancy = "0R"
    min_trades = 1
    learner = { kind = "fake" }
"#;

/// The gold at 4000.00 / 4000.20 every 5 s from 08:00 to 13:00: its mid
/// 4000.10, each exit 7.5 bps of it away, 3.000075, on the nearest cent,
/// 3.00, one R of 3.00 USD an ounce.
fn gold_quotes() -> Vec<QuoteTick> {
    let mut quotes = vec![];
    let mut time = ic::at("08:00:00");
    while time < ic::at("13:00:00") {
        quotes.push(ic::quote(time, "4000.00", "4000.20"));
        time += SignedDuration::from_secs(5);
    }
    quotes
}

/// EURUSD's quotes a minute apart, none before 09:30, at 1.19990 / 1.20010
/// from then, at 1.59990 / 1.60010 from 10:30 to 12:00.
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

/// The rows the dataset run of `CONFIG` settles from 08:00 to 13:00, on a
/// catalog of the gold's quotes and EURUSD's, its data the strategy's
/// requirements, its venue the config's dataset venue, as the commands'
/// dataset runs are.
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
        pairs: config.trade.conversions.pairs().to_vec(),
        balance: venue.balance,
        venue_oms: |_| OmsType::Hedging,
        ..ic::backtest(&catalog)
    };
    let rows = SettledRows::default();
    let mode = Mode::Dataset { rows: rows.clone() };

    backtest
        .run_dataset(ModelEma::new(config, mode).unwrap())
        .unwrap();

    rows.take().unwrap()
}

/// Each row's decision time, 10 ms after its hour, and the unit of each
/// side: one R in USD on its size at the reference stake.
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

/// At 09:00 no EURUSD quote is cached: no rate, no row. At 10:00 EURUSD's
/// mid is 1.20000: 20M EUR is 24M USD, 0.00004% of it 9.60 USD, over the
/// stop's 3.00 USD on an ounce 3.2 oz, IC's whole ounces 3, one R worth
/// 9 USD. At 11:00 and 12:00 it is 1.60000: 32M USD, 12.80 USD, 4.27 oz,
/// 4, one R worth 12 USD. Unconverted, 20M taken as USD would be 2 oz at
/// each; at the rate swapped, 2 oz at 10:00 and 1 at 11:00.
#[test]
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

/// Each row's decision time and the realised P&L of each side, net of its
/// commissions, in USD.
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

/// The dataset run sends each side at the size its row is priced at: on
/// the flat quotes neither exit is reached, and each side closes at its
/// limit across the spread, losing 0.20 USD an ounce and its two fills'
/// commission, 30 USD per million: of 3 oz, 0.60 and 0.36 a fill, -1.32
/// USD; of 4 oz, 0.80 and 0.48 a fill, -1.76 USD.
#[test]
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
