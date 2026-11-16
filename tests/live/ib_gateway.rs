//! End to end on the Docker IB Gateway: the history, fill-catalog and the
//! live node start or reuse Nautilus' paper container, wait for its login
//! and work through it. They need Docker (`DOCKER_HOST` for podman), the
//! paper login (`TWS_USERNAME`, `TWS_PASSWORD`) and a paper account
//! (`NAUTILUS_IB_ACCOUNT_ID`, for example DU1234567), from the environment
//! or `.env`, and nothing else on port 4002: the container takes it, and
//! its login ends any other session of the same user.
//!
//! Gated as Nautilus' IB examples (`node_exec_tester`, `node_data_tester`)
//! are: each test returns at once, saying it was skipped, unless
//! `NAUTILUS_IB_RUN=1` is set in the shell. One places orders,
//! `net_liquidation_moves_with_an_open_positions_pnl`, which holds SXR8 for
//! some minutes, so it also needs `NAUTILUS_IB_LIVE_ORDERS=1`, Xetra open,
//! and a paper account in EUR holding no position; no other places an
//! order (the live run's strategy is never warm, so it never trades). Run
//! them with:
//!
//! ```sh
//! NAUTILUS_IB_RUN=1 cargo test --test live ib_gateway::
//! NAUTILUS_IB_RUN=1 NAUTILUS_IB_LIVE_ORDERS=1 cargo test --test live ib_gateway::net_liquidation -- --nocapture
//! ```
//!
//! The live node keeps its cache in Redis (`live::redis`)
//! under the test's trader, `HELIXTEST-043`, whose records the test deletes
//! once the node has stopped cleanly. The gateway container is left
//! running: the next run reuses it. Client ids 40 to 43, 48 and 49, apart
//! from `fill-catalog`'s 10 and `live`'s 1.

use std::{
    collections::{BTreeMap, BTreeSet},
    process::Command,
    time::{Duration, Instant},
};

use clap::{Args, FromArgMatches};
use helix_trader::{
    live::Live,
    strategy::{
        ema::{EmaCross, EmaCrossConfig},
        requirements::DataRequirements,
    },
    venues::{
        HistoryVenueArgs, fill_history,
        ib::{IbAccount, IbHistory},
    },
};
use ibapi::{
    Client,
    accounts::{
        AccountSummary, AccountSummaryResult, AccountSummaryTags, AccountUpdate, AccountValue,
        PositionUpdate,
        types::{AccountGroup, AccountId as IbAccountId},
    },
    contracts::Contract,
};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{live::get_runtime, logging::logger::LoggerConfig};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{BarType, NautilusDataType},
    identifiers::{InstrumentId, TraderId},
    instruments::Instrument,
    types::{Currency, Price},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rust_decimal::Decimal;

use crate::common;

/// One test at a time: the adapter's start is not safe against another
/// process or task starting the same container at the same moment, and
/// one node per process.
static GATEWAY: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Whether the run is opted in, as the IB examples check it: from the
/// shell, before `.env` is read, so that the file cannot opt in.
/// An opted-in run claims the process for this module (`claim_process`).
fn opted_in(test: &str) -> bool {
    let run = std::env::var("NAUTILUS_IB_RUN").is_ok_and(|value| value == "1");
    if run {
        crate::claim_process(module_path!());
    } else {
        println!("{test}: skipped; set NAUTILUS_IB_RUN=1 to run it against IB");
    }
    run
}

/// Reads `.env`; a variable already set wins.
fn load_env() {
    dotenvy::dotenv().ok();
}

fn sxr8() -> InstrumentId {
    InstrumentId::from("SXR8.IBIS")
}

#[tokio::test(flavor = "multi_thread")]
async fn history_starts_or_reuses_the_paper_gateway_and_a_second_connection_reuses_it() {
    if !opted_in("history_starts_or_reuses_the_paper_gateway_and_a_second_connection_reuses_it") {
        return;
    }
    let _gateway = GATEWAY.lock().await;
    load_env();
    let now = crate::now();
    let end = IbHistory::latest_end(now).unwrap();

    let first = IbHistory::connect(40, end, now).await.unwrap();
    let instruments = first.instruments(&[sxr8()]).await.unwrap();
    assert_eq!(instruments.len(), 1);
    drop(first);

    let started = Instant::now();
    let second = IbHistory::connect(41, end, now).await.unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "a logged-in container is reused at once, not recreated: {:?}",
        started.elapsed()
    );
    assert_eq!(second.instruments(&[sxr8()]).await.unwrap().len(), 1);
}

/// EUR/USD on IDEALPRO, IB's cash FX pair, which IB serves no TRADES bars
/// of: its history is its 1-MINUTE BID and ASK bars.
const EUR_USD: &str = "EUR/USD.IDEALPRO";

/// The venues' settings of fill-catalog, on client id 42.
fn history_venues() -> HistoryVenueArgs {
    let matches = HistoryVenueArgs::augment_args(clap::Command::new("fill-catalog"))
        .try_get_matches_from(["fill-catalog", "--client-id", "42"])
        .unwrap();
    HistoryVenueArgs::from_arg_matches(&matches).unwrap()
}

/// fill-catalog fills the pair's definition and its BID and ASK minute
/// bars over an hour FX trades through, Thursday 12 November 2026 from
/// 10:00 UTC, as a strategy converting through the pair asks for them.
#[tokio::test(flavor = "multi_thread")]
async fn fill_catalog_fills_an_idealpro_pairs_bid_and_ask_minute_bars() {
    if !opted_in("fill_catalog_fills_an_idealpro_pairs_bid_and_ask_minute_bars") {
        return;
    }
    let _gateway = GATEWAY.lock().await;
    load_env();
    let [bid, ask] = ["BID", "ASK"]
        .map(|price| BarType::from(format!("{EUR_USD}-1-MINUTE-{price}-EXTERNAL").as_str()));
    let requirements = DataRequirements {
        bar_types: vec![bid, ask],
        ticks: vec![],
        custom: vec![],
        warmup: SignedDuration::ZERO,
    };
    let start: Timestamp = "2026-11-12T10:00:00Z".parse().unwrap();
    let end = start + SignedDuration::from_hours(1);
    let dir = tempfile::tempdir().unwrap();

    fill_history(&requirements, start, end, &history_venues(), dir.path())
        .await
        .unwrap();

    let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    let defined = catalog
        .instruments(Some(&[EUR_USD.to_string()]), None, None)
        .unwrap();
    assert_eq!(defined.len(), 1, "{EUR_USD} is not defined once");
    // A pair Nautilus' exchange rates read: one with a base currency.
    assert_eq!(defined[0].base_currency(), Some(Currency::EUR()));
    let (from, to) = (UnixNanos::from(start), UnixNanos::from(end));
    let [bids, asks] = [bid, ask].map(|bar_type| {
        let identifier = bar_type.to_string();
        let bars = catalog
            .bars(Some(vec![identifier.clone()]), None, None)
            .unwrap();
        println!("{identifier}: {} bars", bars.len());
        assert!(!bars.is_empty(), "{identifier}: no bars");
        assert!(
            bars.iter().all(|bar| bar.bar_type == bar_type
                && (from..=to).contains(&bar.ts_event)
                && bar.ts_event.as_u64() % 60_000_000_000 == 0),
            "{identifier}: a bar of another type, outside the hour or off the minute"
        );
        // IB gives BID and ASK bars no volume.
        assert!(
            bars.iter().all(|bar| bar.volume.is_zero()),
            "{identifier}: a bar with a volume"
        );
        let lacking = catalog
            .get_missing_intervals_for_request(
                from.as_u64(),
                to.as_u64(),
                &NautilusDataType::Bar.into(),
                Some(&identifier),
            )
            .unwrap();
        assert_eq!(lacking, [], "{identifier}");
        bars.iter()
            .map(|bar| (bar.ts_event, bar.close))
            .collect::<BTreeMap<UnixNanos, Price>>()
    });
    // The BID bars are the bid's and the ASK bars the ask's: in each minute
    // both close, the bid closes at or below the ask, and below it in some.
    let closes: Vec<(Price, Price)> = bids
        .iter()
        .filter_map(|(ts, bid)| asks.get(ts).map(|ask| (*bid, *ask)))
        .collect();
    assert!(
        !closes.is_empty(),
        "no minute closing both a BID and an ASK bar"
    );
    assert!(
        closes.iter().all(|(bid, ask)| bid <= ask),
        "a bid closing above its minute's ask: {closes:?}"
    );
    assert!(
        closes.iter().any(|(bid, ask)| bid < ask),
        "no spread in the hour: the BID and ASK bars are one series"
    );
}

/// The EMA crossover on SXR8.IBIS hour bars of IB's minute bars, warmed
/// up over two hours: its slow EMA needs 30 bars, so it never trades.
fn never_trading_ema_cross() -> EmaCrossConfig {
    EmaCrossConfig::load(
        r#"
        [data]
        series = [
            { data = "bars", instrument_id = "SXR8.IBIS", bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }, from = { step = 1, aggregation = "MINUTE", aggregation_source = "EXTERNAL" } },
        ]
        warmup = "2h"

        [indicators]
        fast = { period = 10 }
        slow = { period = 30 }

        [trade]
        instruments = [{ instrument_id = "SXR8.IBIS" }]
        risk = "0.5%"
        "#,
    )
    .unwrap()
}

/// Long enough for the gateway and the node to start.
const LIVE_RUN: Duration = Duration::from_secs(90);

#[tokio::test(flavor = "multi_thread")]
async fn live_runs_on_the_paper_gateway_and_stops_cleanly_on_sigint() {
    if !opted_in("live_runs_on_the_paper_gateway_and_stops_cleanly_on_sigint") {
        return;
    }
    let _gateway = GATEWAY.lock().await;
    load_env();
    let account: IbAccount = std::env::var("NAUTILUS_IB_ACCOUNT_ID")
        .expect("NAUTILUS_IB_ACCOUNT_ID, a paper account")
        .parse()
        .unwrap();
    assert!(
        account.to_string().starts_with('D'),
        "{account} is not a paper account"
    );
    let config = never_trading_ema_cross();
    let requirements = config.data_requirements();
    // An empty catalog of its own: the venue answers the whole warmup.
    let catalog = tempfile::tempdir().unwrap();
    // Outside the catalog, as `Live` requires.
    let staging = tempfile::tempdir().unwrap();
    let live = Live {
        trader_id: TraderId::from("HELIXTEST-043"),
        venues: crate::venues(&["--account", &account.to_string(), "--client-id", "43"]),
        logging: LoggerConfig::default(),
        catalog: catalog.path().to_path_buf(),
        recording: crate::recording(staging.path()),
    };
    // As Ctrl-C would, once the node runs: its signal handler stops it.
    // The node is not `Send`, so it runs here and the signal is sent from
    // another task.
    let interrupt = get_runtime().spawn(async {
        tokio::time::sleep(LIVE_RUN).await;
        let status = Command::new("kill")
            .args(["-INT", &std::process::id().to_string()])
            .status()
            .unwrap();
        assert!(status.success());
    });
    let strategy = EmaCross::live(config).unwrap();
    let ran = Box::pin(tokio::time::timeout(
        LIVE_RUN + Duration::from_secs(60),
        live.run(&requirements, &strategy.warmup(), strategy, None),
    ))
    .await;
    let interrupted = interrupt.is_finished();
    interrupt.abort();
    ran.expect("the node stops within a minute of SIGINT")
        .unwrap();
    assert!(interrupted, "the node stopped before SIGINT");
    // The node's records in Redis, under the test's trader.
    common::cache::delete_trader("HELIXTEST-043").await;
}

/// The client id the account tests start or reuse the gateway with,
/// through IB's history connection (`IbHistory::connect`).
const ACCOUNT_GATEWAY: i32 = 48;

/// The client id the account tests read IB with, through `ibapi`, as
/// Nautilus' IB adapter reads it.
const ACCOUNT_CLIENT: i32 = 49;

/// The paper account of `NAUTILUS_IB_ACCOUNT_ID`.
fn paper_account() -> IbAccount {
    let account: IbAccount = std::env::var("NAUTILUS_IB_ACCOUNT_ID")
        .expect("NAUTILUS_IB_ACCOUNT_ID, a paper account")
        .parse()
        .unwrap();
    assert!(
        account.to_string().starts_with('D'),
        "{account} is not a paper account"
    );
    account
}

/// A client of the paper gateway, started or reused.
async fn paper_client() -> Client {
    let now = crate::now();
    let end = IbHistory::latest_end(now).unwrap();
    drop(IbHistory::connect(ACCOUNT_GATEWAY, end, now).await.unwrap());
    Client::connect("127.0.0.1:4002", ACCOUNT_CLIENT)
        .await
        .unwrap()
}

/// One snapshot of IB's account updates of `account` (`reqAccountUpdates`,
/// which Nautilus' IB adapter reads into its account state's info), as IB
/// sends it until the end of its download: the account's values, its
/// positions' valuations and when IB last updated them.
async fn account_updates(client: &Client, account: &IbAccount) -> Vec<AccountUpdate> {
    let mut subscription = client
        .account_updates(&IbAccountId(account.to_string()))
        .await
        .unwrap();
    let updates = subscription
        .collect_until(Duration::from_secs(30), |updates| {
            matches!(updates.last(), Some(AccountUpdate::End))
        })
        .await;
    assert!(
        matches!(updates.last(), Some(AccountUpdate::End)),
        "no end of account updates from IB: {updates:?}"
    );
    updates
}

/// The values among `updates`.
fn account_values(updates: &[AccountUpdate]) -> Vec<&AccountValue> {
    updates
        .iter()
        .filter_map(|update| match update {
            AccountUpdate::AccountValue(value) => Some(value),
            _ => None,
        })
        .collect()
}

/// The base currency of the account whose values these are: the one
/// currency IB gives an exchange rate of 1 (`ExchangeRate`, "the exchange
/// rate of the currency to your base currency"), besides its `BASE`
/// ledger. A value of a currency may carry IB's `$LEDGER-` prefix.
fn base_currency(values: &[&AccountValue]) -> String {
    let at_one: BTreeSet<&str> = values
        .iter()
        .filter(|value| {
            value.key.trim_start_matches("$LEDGER-") == "ExchangeRate"
                && value.currency != "BASE"
                && value
                    .value
                    .parse::<Decimal>()
                    .is_ok_and(|rate| rate == Decimal::ONE)
        })
        .map(|value| value.currency.as_str())
        .collect();
    let [base] = at_one.into_iter().collect::<Vec<_>>()[..] else {
        panic!("not one currency at an exchange rate of 1: {values:?}");
    };
    base.to_string()
}

/// The tags of IB's account summary Nautilus' IB adapter requests and
/// builds its account state from (`execution/account.rs`,
/// `open_account_summary`), each balance under its row's currency.
const SUMMARY_TAGS: &[&str] = &[
    AccountSummaryTags::NET_LIQUIDATION,
    AccountSummaryTags::TOTAL_CASH_VALUE,
    AccountSummaryTags::SETTLED_CASH,
    AccountSummaryTags::BUYING_POWER,
    AccountSummaryTags::EQUITY_WITH_LOAN_VALUE,
    AccountSummaryTags::AVAILABLE_FUNDS,
    AccountSummaryTags::EXCESS_LIQUIDITY,
    AccountSummaryTags::INIT_MARGIN_REQ,
    AccountSummaryTags::MAINT_MARGIN_REQ,
    AccountSummaryTags::CUSHION,
];

/// A trade on IB is sized on the balance Nautilus' IB adapter builds from
/// IB's account summary, one amount, its net liquidation, if IB reports
/// the summary in one currency: the account's base currency, which IB's
/// account updates give an exchange rate of 1. Rows of a ratio, such as
/// the cushion, carry no currency.
#[tokio::test(flavor = "multi_thread")]
async fn ib_reports_its_account_summary_in_the_accounts_base_currency_alone() {
    if !opted_in("ib_reports_its_account_summary_in_the_accounts_base_currency_alone") {
        return;
    }
    let _gateway = GATEWAY.lock().await;
    load_env();
    let account = paper_account();
    let client = paper_client().await;
    let base = base_currency(&account_values(&account_updates(&client, &account).await));

    let mut subscription = client
        .account_summary(&AccountGroup("All".to_string()), SUMMARY_TAGS)
        .await
        .unwrap();
    let results = subscription
        .collect_until(Duration::from_secs(30), |results| {
            matches!(results.last(), Some(AccountSummaryResult::End))
        })
        .await;

    assert!(
        matches!(results.last(), Some(AccountSummaryResult::End)),
        "no end of the account summary from IB: {results:?}"
    );
    let rows: Vec<&AccountSummary> = results
        .iter()
        .filter_map(|result| match result {
            AccountSummaryResult::Summary(row) if row.account == account.to_string() => Some(row),
            _ => None,
        })
        .collect();
    println!("{account}, based in {base}: {rows:#?}");
    assert!(
        rows.iter()
            .any(|row| row.tag == AccountSummaryTags::NET_LIQUIDATION),
        "no net liquidation of {account}: {rows:?}"
    );
    let currencies: BTreeSet<&str> = rows
        .iter()
        .map(|row| row.currency.as_str())
        .filter(|currency| !currency.is_empty())
        .collect();
    assert_eq!(currencies, BTreeSet::from([base.as_str()]), "{rows:?}");
}

/// What `net_liquidation_moves_with_an_open_positions_pnl` holds open:
/// shares of SXR8 on Xetra, in EUR, routed SMART as Nautilus' adapter
/// routes it.
fn held_contract() -> Contract {
    Contract::stock("SXR8")
        .on_exchange("SMART")
        .primary("IBIS")
        .in_currency("EUR")
        .build()
}

/// How many SXR8 shares the test holds: a cent on SXR8 moves their P&L by
/// 0.50 EUR.
const HELD_SHARES: f64 = 50.0;

/// How far the held shares' P&L must move for the test to judge the net
/// liquidation by it.
const PNL_MOVE: f64 = 1.0;

/// How long the test waits for that move, sampling every `SAMPLED`: IB
/// updates its account values every three minutes.
const PNL_WAIT: Duration = Duration::from_secs(12 * 60);
const SAMPLED: Duration = Duration::from_secs(30);

/// What a snapshot of IB's account updates says of the held position: when
/// IB last updated the account, its net liquidation over its cash, the
/// value of its positions, and the position's P&L, in EUR.
#[derive(Debug, Clone, PartialEq)]
struct Sample {
    updated: String,
    net_over_cash: f64,
    pnl: f64,
}

/// The sample of `updates` of an account based in EUR.
fn sample(updates: &[AccountUpdate]) -> Sample {
    let values = account_values(updates);
    let amount = |key: &str| {
        let [value] = values
            .iter()
            .filter(|value| value.key == key && value.currency == "EUR")
            .collect::<Vec<_>>()[..]
        else {
            panic!("not one {key} in EUR: {values:?}");
        };
        value.value.parse::<f64>().unwrap()
    };
    let updated = updates
        .iter()
        .find_map(|update| match update {
            AccountUpdate::UpdateTime(time) => Some(time.timestamp.clone()),
            _ => None,
        })
        .unwrap_or_default();
    let pnl = updates
        .iter()
        .find_map(|update| match update {
            AccountUpdate::PortfolioValue(held) if held.contract.symbol == "SXR8" => {
                Some(held.unrealized_pnl)
            }
            _ => None,
        })
        .unwrap_or_else(|| panic!("SXR8 is not held: {updates:?}"));
    Sample {
        updated,
        net_over_cash: amount("NetLiquidation") - amount("TotalCashValue"),
        pnl,
    }
}

/// The positions `account` holds at IB, each its symbol and size, none
/// flat.
async fn positions(client: &Client, account: &IbAccount) -> Vec<(String, f64)> {
    let mut subscription = client.positions().await.unwrap();
    let updates = subscription
        .collect_until(Duration::from_secs(30), |updates| {
            matches!(updates.last(), Some(PositionUpdate::PositionEnd))
        })
        .await;
    assert!(
        matches!(updates.last(), Some(PositionUpdate::PositionEnd)),
        "no end of positions from IB: {updates:?}"
    );
    updates
        .iter()
        .filter_map(|update| match update {
            PositionUpdate::Position(position)
                if position.account == account.to_string() && position.position != 0.0 =>
            {
                Some((position.contract.symbol.to_string(), position.position))
            }
            _ => None,
        })
        .collect()
}

/// Sends a market order of `shares` SXR8, bought if positive, in
/// `account`, and waits for IB to report the account holding `holding`
/// SXR8 and nothing else.
async fn trade_until_holding(client: &Client, account: &IbAccount, shares: f64, holding: f64) {
    let contract = held_contract();
    let order = client.order(&contract);
    let side = if shares > 0.0 {
        order.buy(shares)
    } else {
        order.sell(-shares)
    };
    let order_id = side
        .market()
        .account(account.to_string())
        .submit()
        .await
        .unwrap();
    println!("traded {shares} SXR8 at market: IB order {order_id}");
    let expected: Vec<(String, f64)> = if holding == 0.0 {
        vec![]
    } else {
        vec![("SXR8".to_string(), holding)]
    };
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_secs(2)).await;
        if positions(client, account).await == expected {
            return;
        }
    }
    panic!("{account} does not hold {holding} SXR8 alone after the trade");
}

/// Samples IB's account updates of `account` every `SAMPLED` from its
/// first, until IB has updated the account since it and the held shares'
/// P&L has moved by `PNL_MOVE`, or for `PNL_WAIT`: the first sample and
/// the last.
async fn samples_apart(client: &Client, account: &IbAccount) -> (Sample, Sample) {
    let first = sample(&account_updates(client, account).await);
    println!("first: {first:?}");
    let started = Instant::now();
    let mut last = first.clone();
    while started.elapsed() < PNL_WAIT
        && (last.updated == first.updated || (last.pnl - first.pnl).abs() < PNL_MOVE)
    {
        tokio::time::sleep(SAMPLED).await;
        last = sample(&account_updates(client, account).await);
        println!("then: {last:?}");
    }
    (first, last)
}

/// Nautilus' IB adapter takes IB's net liquidation as the account's
/// balance total, and Nautilus' `Portfolio::equity` adds a reported margin
/// account's open P&L to it: a trade on IB is sized on the balance alone
/// because the net liquidation holds that P&L already. Holding 50 SXR8 on
/// an account based in EUR and holding nothing else, the net liquidation
/// over the cash, the value of the shares, moves with the shares' P&L as
/// IB reports it, between two of IB's account updates the P&L moved by a
/// euro or more between: by as much, give or take half, where a net
/// liquidation leaving the position out would not move. The shares are
/// sold before anything is judged.
#[tokio::test(flavor = "multi_thread")]
async fn net_liquidation_moves_with_an_open_positions_pnl() {
    let set = |name| std::env::var(name).is_ok_and(|value| value == "1");
    if !(set("NAUTILUS_IB_RUN") && set("NAUTILUS_IB_LIVE_ORDERS")) {
        println!(
            "net_liquidation_moves_with_an_open_positions_pnl: skipped; set NAUTILUS_IB_RUN=1 \
             and NAUTILUS_IB_LIVE_ORDERS=1 to place orders"
        );
        return;
    }
    crate::claim_process(module_path!());
    let _gateway = GATEWAY.lock().await;
    load_env();
    let account = paper_account();
    let client = paper_client().await;
    let base = base_currency(&account_values(&account_updates(&client, &account).await));
    assert_eq!(
        base, "EUR",
        "{account} is based in {base}, not in SXR8's EUR"
    );
    let held = positions(&client, &account).await;
    assert_eq!(held, [], "{account} holds positions: flatten it first");

    trade_until_holding(&client, &account, HELD_SHARES, HELD_SHARES).await;
    let (first, last) = samples_apart(&client, &account).await;
    trade_until_holding(&client, &account, -HELD_SHARES, 0.0).await;

    let moved = last.pnl - first.pnl;
    assert!(
        moved.abs() >= PNL_MOVE && last.updated != first.updated,
        "SXR8's P&L did not move by {PNL_MOVE} EUR across IB's account updates in \
         {PNL_WAIT:?}: run while Xetra trades ({first:?}, {last:?})"
    );
    let net_moved = last.net_over_cash - first.net_over_cash;
    assert!(
        (net_moved - moved).abs() < moved.abs() / 2.0,
        "the net liquidation over the cash moved by {net_moved} EUR, the P&L by {moved} EUR \
         ({first:?}, {last:?})"
    );
}
