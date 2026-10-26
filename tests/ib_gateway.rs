//! End to end on the Docker IB Gateway: the history and the live node
//! start or reuse Nautilus' paper container, wait for its login and work
//! through it. They need Docker (`DOCKER_HOST` for podman), the paper login
//! (`TWS_USERNAME`, `TWS_PASSWORD`) and a paper account
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
//! NAUTILUS_IB_RUN=1 cargo test --test ib_gateway
//! ```
//!
//! The live node keeps its cache in Redis (`live::redis`)
//! under the test's trader, `HELIXTEST-043`, whose records the test deletes
//! once the node has stopped cleanly. The gateway container is left
//! running: the next run reuses it. Client ids 40 to 43, apart from
//! `fill-catalog`'s 10 and `live`'s 1.

mod common;

use std::{
    process::Command,
    time::{Duration, Instant},
};

use helix_trader::{
    data::{IbAccount, IbHistory, fill_catalog},
    live::Live,
    strategy::{
        catalog_series,
        ema::{EmaCross, EmaCrossConfig},
    },
};
use jiff::{Timestamp, ToSpan};
use nautilus_common::logging::logger::LoggerConfig;
use nautilus_model::{
    data::BarType,
    identifiers::{InstrumentId, TraderId},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rust_decimal::Decimal;
use tokio::sync::Mutex;

/// One test at a time: the adapter's start is not safe against another
/// process or task starting the same container at the same moment, and
/// one node per process.
static GATEWAY: Mutex<()> = Mutex::const_new(());

/// Whether the run is opted in, as the IB examples check it: from the
/// shell, before `.env` is read, so that the file cannot opt in.
fn opted_in(test: &str) -> bool {
    let run = std::env::var("NAUTILUS_IB_RUN").is_ok_and(|value| value == "1");
    if !run {
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

#[tokio::test(flavor = "multi_thread")]
async fn fill_catalog_stores_minute_bars_through_the_paper_gateway() {
    if !opted_in("fill_catalog_stores_minute_bars_through_the_paper_gateway") {
        return;
    }
    let _gateway = GATEWAY.lock().await;
    load_env();
    let now = Timestamp::now();
    // A day back, so the window holds Xetra bars whenever the test runs on
    // a trading day's evening or the next morning.
    let end = IbHistory::latest_end(now).unwrap();
    let start = end - 24.hours();
    let ib = IbHistory::connect(42, end, now).await.unwrap();
    let instruments = ib.instruments(&[sxr8()]).await.unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    // IB's 1-minute TRADES bars, which the hour bars are built from.
    let series = catalog_series(&[BarType::from(
        "SXR8.IBIS-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
    )]);
    assert_eq!(series.len(), 1);
    fill_catalog(
        &mut catalog,
        instruments,
        &series,
        start,
        end,
        async |data_type, identifier, from, to| ib.data(data_type, identifier, from, to).await,
    )
    .await
    .unwrap();
    assert_eq!(catalog.instruments(None, None, None).unwrap().len(), 1);
    for (_, bar_type) in series {
        let bars = catalog
            .bars(Some(vec![bar_type.clone()]), None, None)
            .unwrap();
        println!("{} bars of {bar_type} over the last day", bars.len());
    }
}

/// The EMA crossover on SXR8.IBIS hour bars of IB's minute bars, warmed
/// up over two hours: its slow EMA needs 30 bars, so it never trades.
fn never_trading_ema_cross() -> EmaCrossConfig {
    let config: EmaCrossConfig = toml::from_str(
        r#"
        instrument_ids = ["SXR8.IBIS"]
        trade_instrument_id = "SXR8.IBIS"
        bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }
        composite = { step = 1, aggregation = "MINUTE", aggregation_source = "EXTERNAL" }
        trade_size = "1"
        stop_loss_bps = 100
        take_profit_bps = 100
        hold = "1h"
        warmup = "2h"
        "#,
    )
    .unwrap();
    config.validate().unwrap();
    config
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
    let requirements = config.bars.data_requirements();
    // An empty catalog of its own: the venue answers the whole warmup.
    let catalog = tempfile::tempdir().unwrap();
    let live = Live {
        trader_id: TraderId::from("HELIXTEST-043"),
        account: Some(account),
        client_id: 43,
        leverage: Decimal::ONE,
        logging: LoggerConfig::default(),
        catalog: catalog.path().to_path_buf(),
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
    let ran = tokio::time::timeout(
        LIVE_RUN + Duration::from_secs(60),
        live.run(&requirements, EmaCross::new(config).unwrap()),
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
