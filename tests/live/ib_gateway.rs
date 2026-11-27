//! End to end on the Docker IB Gateway. The history, fill-catalog and the live node start or reuse
//! Nautilus' paper container and work through it.
//!
//! The tests need:
//!
//! - Docker (`DOCKER_HOST` for podman).
//! - The paper login (`TWS_USERNAME`, `TWS_PASSWORD`) and a paper account
//!   (`NAUTILUS_IB_ACCOUNT_ID`, for example DU1234567), from the environment or `.env`.
//! - Nothing else on port 4002: the container takes it, and its login ends any other session of
//!   the same user.
//!
//! Gated by `NAUTILUS_IB_RUN=1` in the shell, as Nautilus' IB examples are (`node_exec_tester`,
//! `node_data_tester`).
//!
//! `net_liquidation_moves_with_an_open_positions_pnl` holds SXR8 for some minutes. It also needs
//! `NAUTILUS_IB_LIVE_ORDERS=1`, Xetra open, and a paper account in EUR holding no position. No
//! other test places an order (the live run's strategy is never warm). Run them with:
//!
//! ```sh
//! NAUTILUS_IB_RUN=1 cargo test --test live ib_gateway::
//! NAUTILUS_IB_RUN=1 NAUTILUS_IB_LIVE_ORDERS=1 cargo test --test live ib_gateway::net_liquidation -- --nocapture
//! ```
//!
//! The test deletes the live node's Redis records (trader `HELIXTEST-043`) once the node has
//! stopped cleanly. The gateway container is left running for the next run. Client ids 40 to 43, 48
//! and 49 are separate from `fill-catalog`'s 10 and `live`'s 1.

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

/// Lets one test run at a time.
///
/// The adapter's start is not safe against another task or process starting the same container.
/// Only one node runs per process.
static GATEWAY: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Returns whether the run is opted in, reading the shell before `.env`, and claims the process.
fn opted_in(test: &str) -> bool {
    let run = std::env::var("NAUTILUS_IB_RUN").is_ok_and(|value| value == "1");
    if run {
        crate::claim_process(module_path!());
    } else {
        println!("{test}: skipped; set NAUTILUS_IB_RUN=1 to run it against IB");
    }
    run
}

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

/// IB's cash FX pair. IB serves no TRADES bars for it, so its history is its 1-MINUTE BID and ASK
/// bars.
const EUR_USD: &str = "EUR/USD.IDEALPRO";

fn history_venues() -> HistoryVenueArgs {
    let matches = HistoryVenueArgs::augment_args(clap::Command::new("fill-catalog"))
        .try_get_matches_from(["fill-catalog", "--client-id", "42"])
        .unwrap();
    HistoryVenueArgs::from_arg_matches(&matches).unwrap()
}

/// fill-catalog fills the pair's definition and BID and ASK minute bars over Thursday 12 November
/// 2026 from 10:00 UTC, an hour that FX trades through.
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

    // Nautilus' exchange rates read a pair that has a base currency
    assert_eq!(defined[0].base_currency(), Some(Currency::EUR()));
    let (from, to) = (UnixNanos::from(start), UnixNanos::from(end));
    let [bids, asks] = [bid, ask].map(|bar_type| {
        let identifier = bar_type.to_string();
        let bars = catalog
            .bars()
            .identifiers(vec![identifier.clone()])
            .execute()
            .unwrap();
        println!("{identifier}: {} bars", bars.len());
        assert!(!bars.is_empty(), "{identifier}: no bars");
        assert!(
            bars.iter().all(|bar| bar.bar_type == bar_type
                && (from..=to).contains(&bar.ts_event)
                && bar.ts_event.as_u64() % 60_000_000_000 == 0),
            "{identifier}: a bar of another type, outside the hour or off the minute"
        );

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

/// Returns the EMA crossover on SXR8.IBIS hour bars, built from IB's minute bars and warmed up over
/// two hours. Its slow EMA needs 30 bars, so it never trades.
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

    let catalog = tempfile::tempdir().unwrap();

    let staging = tempfile::tempdir().unwrap();
    let live = Live {
        trader_id: TraderId::from("HELIXTEST-043"),
        venues: crate::venues(&["--account", &account.to_string(), "--client-id", "43"]),
        logging: LoggerConfig::default(),
        catalog: catalog.path().to_path_buf(),
        recording: crate::recording(staging.path()),
    };

    // Once the node runs, interrupts it as Ctrl-C would, so its signal handler stops it, and since
    // the node is not `Send`, it runs here and the signal is sent from another task.
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

    common::cache::delete_trader("HELIXTEST-043").await;
    ran.expect("the node stops within a minute of SIGINT")
        .unwrap();
    assert!(interrupted, "the node stopped before SIGINT");
}

const ACCOUNT_GATEWAY: i32 = 48;

const ACCOUNT_CLIENT: i32 = 49;

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

async fn paper_client() -> Client {
    let now = crate::now();
    let end = IbHistory::latest_end(now).unwrap();
    drop(IbHistory::connect(ACCOUNT_GATEWAY, end, now).await.unwrap());
    Client::connect("127.0.0.1:4002", ACCOUNT_CLIENT)
        .await
        .unwrap()
}

/// Returns one snapshot of IB's account updates, until the end of IB's download. Nautilus' IB
/// adapter reads `reqAccountUpdates` into the info of its account state.
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

fn account_values(updates: &[AccountUpdate]) -> Vec<&AccountValue> {
    updates
        .iter()
        .filter_map(|update| match update {
            AccountUpdate::AccountValue(value) => Some(value),
            _ => None,
        })
        .collect()
}

/// Returns the one currency with an `ExchangeRate` of 1, besides the `BASE` ledger. Values may
/// carry a `$LEDGER-` prefix.
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

/// The tags Nautilus' IB adapter requests to build its account state (`execution/account.rs`,
/// `open_account_summary`).
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

/// A trade on IB is sized on the balance that Nautilus' IB adapter builds from IB's account
/// summary: one amount, its net liquidation, in the base currency, if IB reports so. Rows that
/// hold a ratio, such as the cushion, carry no currency.
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

fn held_contract() -> Contract {
    Contract::stock("SXR8")
        .on_exchange("SMART")
        .primary("IBIS")
        .in_currency("EUR")
        .build()
}

/// How many SXR8 shares the test holds. A cent on SXR8 moves their P&L by 0.50 EUR.
const HELD_SHARES: f64 = 50.0;

const PNL_MOVE: f64 = 1.0;

/// How long the test waits for that move, sampling every `SAMPLED`.
const PNL_WAIT: Duration = Duration::from_secs(12 * 60);
const SAMPLED: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, PartialEq)]
struct Sample {
    updated: String,
    net_over_cash: f64,
    pnl: f64,
}

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

/// Nautilus' IB adapter takes IB's net liquidation as the balance total, and `Portfolio::equity`
/// adds a margin account's open P&L to it. A trade is sized on the balance alone, because the net
/// liquidation holds that P&L already.
///
/// With 50 SXR8 held on an account based in EUR and nothing else, the net liquidation over the cash
/// moves with their P&L between two account updates, by a euro or more, give or take half. The
/// shares are sold before anything is judged.
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
