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
//! `NAUTILUS_IB_RUN=1` is set in the shell. None places an order (the live
//! run's strategy is never warm, so it never trades), so none needs
//! `NAUTILUS_IB_LIVE_ORDERS=1`. Run them with:
//!
//! ```sh
//! NAUTILUS_IB_RUN=1 cargo test --test live ib_gateway::
//! ```
//!
//! The live node keeps its cache in Redis (`live::redis`)
//! under the test's trader, `HELIXTEST-043`, whose records the test deletes
//! once the node has stopped cleanly. The gateway container is left
//! running: the next run reuses it. Client ids 40 to 43, apart from
//! `fill-catalog`'s 10 and `live`'s 1.

use crate::common;

use std::{
    collections::BTreeMap,
    process::Command,
    time::{Duration, Instant},
};

use clap::{Args, FromArgMatches};
use helix_trader::{
    live::Live,
    strategy::{
        DataRequirements,
        ema::{EmaCross, EmaCrossConfig},
    },
    venues::{
        HistoryVenueArgs, fill_history,
        ib::{IbAccount, IbHistory},
    },
};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::logging::logger::LoggerConfig;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{BarType, NautilusDataType},
    identifiers::{InstrumentId, TraderId},
    instruments::Instrument,
    types::{Currency, Price},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use tokio::sync::Mutex;

/// One test at a time: the adapter's start is not safe against another
/// process or task starting the same container at the same moment, and
/// one node per process.
static GATEWAY: Mutex<()> = Mutex::const_new(());

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
    let now = Timestamp::now();
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
    let interrupt = tokio::spawn(async {
        tokio::time::sleep(LIVE_RUN).await;
        let status = Command::new("kill")
            .args(["-INT", &std::process::id().to_string()])
            .status()
            .unwrap();
        assert!(status.success());
    });
    let strategy = EmaCross::live(config).unwrap();
    let ran = tokio::time::timeout(
        LIVE_RUN + Duration::from_secs(60),
        live.run(&requirements, &strategy.warmup(), strategy, None),
    )
    .await;
    let interrupted = interrupt.is_finished();
    interrupt.abort();
    ran.expect("the node stops within a minute of SIGINT")
        .unwrap();
    assert!(interrupted, "the node stopped before SIGINT");
    // The node's records in Redis, under the test's trader.
    common::cache::delete_trader("HELIXTEST-043").await;
}
