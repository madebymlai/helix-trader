//! The live node streaming what it receives into the catalog, with no
//! venue: a node in a child process (this binary, run on
//! `streamed_node`) holds a stub data client on `SIM` that loads AUD/USD
//! at connect, as the venues' clients do, then sends a quote, a trade and
//! a depth snapshot every `TICK`, each numbered in sequence (the quote's
//! bid size less one, the trade's id, the depth's sequence); an actor
//! subscribes to them and to 1-second bars Nautilus builds from the
//! trades. The node streams as `Recording` configures it, into a temp-dir
//! catalog, but promoting every `PROMOTION_MS` rather than daily, so that a
//! test sees several promotions in seconds. The test, another process,
//! reads the catalog while the node runs and after it stops, and backtests
//! and builds a dataset over it with the binary, a third.
//!
//! Hermetic and ungated: no node runs in the test's own process, so it
//! claims none (`main.rs`). Run it with:
//!
//! ```sh
//! cargo test --test live streaming::
//! ```

use std::{
    any::Any,
    cell::RefCell,
    num::NonZeroUsize,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    process::{Child, Command, Output},
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::Context;
use async_trait::async_trait;
use helix_trader::live::{Recording, promote_staged};
use jiff::Timestamp;
use nautilus_common::{
    actor::{DataActor, DataActorConfig, DataActorCore},
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    enums::Environment,
    factories::{ClientConfig, DataClientFactory},
    live::{runner::get_data_event_sender, sender::EventSender},
    messages::DataEvent,
    nautilus_actor,
};
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::{builder::LiveNodeBuilder, config::LiveNodeConfig};
use nautilus_model::{
    data::{
        Bar, BarType, Data, NautilusDataType, OrderBookDepth, QuoteTick, TradeTick,
        stubs::stub_depth10,
    },
    enums::{AggressorSide, BookType},
    identifiers::{ClientId, InstrumentId, TradeId, TraderId, Venue},
    instruments::{Instrument, InstrumentAny, stubs::audusd_sim},
    types::{Price, Quantity},
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, config::DataCatalogConfig,
};

const INSTRUMENT_ID: &str = "AUD/USD.SIM";
/// The bars the actor subscribes to, which Nautilus builds from the trades.
const BUILT_BARS: &str = "AUD/USD.SIM-1-SECOND-LAST-INTERNAL";
/// The same bars as the catalog stores them.
const STORED_BARS: &str = "AUD/USD.SIM-1-SECOND-LAST-EXTERNAL";
const TRADER_ID: &str = "HELIXTEST-301";
const TICK: Duration = Duration::from_millis(20);

/// The directory of the child's catalog and staging directory; set only
/// in the child.
const ROOT: &str = "HELIX_STREAMING_ROOT";
/// The child's promotion interval, in milliseconds, set on the streaming
/// config `Recording` gives in place of its daily one.
const PROMOTION_MS: &str = "HELIX_STREAMING_PROMOTION_MS";
/// When the child's node stops itself, in milliseconds after it starts;
/// unset, it runs until killed.
const STOP_MS: &str = "HELIX_STREAMING_STOP_MS";
/// The number of the child's first tick.
const FIRST_SEQ: &str = "HELIX_STREAMING_FIRST_SEQ";
/// Set, the child writes a venue's answer back over what the catalog lacks
/// before its node runs, as a warmup does at the strategy's start.
const WRITE_BACK: &str = "HELIX_STREAMING_WRITE_BACK";

/// How long a child gets to do what the test waits for.
const PATIENCE: Duration = Duration::from_secs(60);

#[test]
fn a_reader_sees_a_clean_prefix_of_the_streamed_data_while_the_node_runs() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = spawn_node(dir.path(), Duration::from_secs(1), Some(6_000), 0);
    let mut prefixes_while_running = Vec::new();
    let deadline = Instant::now() + PATIENCE;
    while node.try_wait().unwrap().is_none() {
        assert!(Instant::now() < deadline, "the node did not stop");
        let read = Read::of(dir.path());
        if node.try_wait().unwrap().is_none() && read.quotes > 0 {
            prefixes_while_running.push(read);
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(node.wait().unwrap().success());
    let mut counts: Vec<u64> = prefixes_while_running.iter().map(|r| r.quotes).collect();
    counts.dedup();
    assert!(
        counts.len() >= 2,
        "the catalog did not grow under the reader while the node ran: {counts:?}"
    );
    let last = Read::of(dir.path());
    for read in &prefixes_while_running {
        assert!(read.quotes <= last.quotes && read.trades <= last.trades);
        assert!(read.depths <= last.depths && read.bars <= last.bars);
    }
    assert!(last.bars >= 3, "{} bars", last.bars);
}

#[test]
fn the_bars_the_node_builds_are_stored_under_the_external_source() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = spawn_node(dir.path(), Duration::from_secs(1), Some(4_000), 0);
    assert!(node.wait().unwrap().success());
    let mut catalog = catalog(dir.path());
    let built: Vec<Bar> = catalog
        .query(
            Some(vec![BUILT_BARS.to_string()]),
            None,
            None,
            None,
            None,
            true,
        )
        .unwrap();
    let stored = bar_seconds(dir.path());
    assert!(built.is_empty(), "{} bars under {BUILT_BARS}", built.len());
    assert!(!stored.is_empty());
}

#[test]
fn a_clean_stop_promotes_everything_and_leaves_no_staging_file() {
    let dir = tempfile::tempdir().unwrap();
    // No promotion before the stop: the stop promotes all.
    let mut node = spawn_node(dir.path(), Duration::from_secs(3_600), Some(3_000), 0);
    assert!(node.wait().unwrap().success());
    let read = Read::of(dir.path());
    assert!(read.quotes > 0 && read.trades > 0 && read.depths > 0 && read.bars > 0);
    assert_eq!(staged_files(dir.path()), Vec::<PathBuf>::new());
}

#[test]
fn the_instruments_the_clients_load_are_in_the_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = spawn_node(dir.path(), Duration::from_secs(1), Some(2_000), 0);
    assert!(node.wait().unwrap().success());
    // As a backtest node looks its instruments up.
    let instruments = catalog(dir.path())
        .instruments(Some(&[INSTRUMENT_ID.to_string()]), None, None)
        .unwrap();
    let ids: Vec<InstrumentId> = instruments.iter().map(Instrument::id).collect();
    assert_eq!(ids, vec![InstrumentId::from(INSTRUMENT_ID)]);
}

#[test]
fn a_restart_after_a_crash_promotes_what_the_crashed_run_staged() {
    let dir = tempfile::tempdir().unwrap();
    let mut crashed = spawn_node(dir.path(), Duration::from_secs(3_600), None, 0);
    wait_for_staged_quotes(dir.path(), &mut crashed);
    crashed.kill().unwrap();
    crashed.wait().unwrap();
    assert_eq!(Read::of(dir.path()).quotes, 0, "promoted before the crash");
    let mut restarted = spawn_node(
        dir.path(),
        Duration::from_secs(3_600),
        Some(2_000),
        1_000_000,
    );
    assert!(restarted.wait().unwrap().success());
    let seqs = quote_seqs(dir.path());
    let (first_run, second_run): (Vec<u64>, Vec<u64>) =
        seqs.into_iter().partition(|&seq| seq < 1_000_000);
    assert!(!first_run.is_empty(), "the crashed run's quotes were lost");
    assert_contiguous("the crashed run's quotes", &first_run, 0);
    assert!(!second_run.is_empty());
    assert_contiguous("the restart's quotes", &second_run, 1_000_000);
    assert_eq!(staged_files(dir.path()), Vec::<PathBuf>::new());
}

/// The restart's warmup writes the venue's answer back over what the
/// catalog lacks up to now: the crashed run's staged rows are in the
/// catalog by then, so the answer lands after them, not over them.
#[test]
fn a_restart_promotes_what_the_crashed_run_staged_before_its_warmup_writes_back() {
    let dir = tempfile::tempdir().unwrap();
    let mut crashed = spawn_node(dir.path(), Duration::from_secs(3_600), None, 0);
    wait_for_staged_quotes(dir.path(), &mut crashed);
    crashed.kill().unwrap();
    crashed.wait().unwrap();
    let mut restarted = NodeProcess(
        node_command(
            dir.path(),
            Duration::from_secs(3_600),
            Some(2_000),
            1_000_000,
        )
        .env(WRITE_BACK, "1")
        .spawn()
        .unwrap(),
    );

    assert!(restarted.wait().unwrap().success());

    let seqs = quote_seqs(dir.path());
    let crashed_run: Vec<u64> = seqs.iter().copied().filter(|&s| s < 1_000_000).collect();
    assert!(
        !crashed_run.is_empty(),
        "the crashed run's quotes were lost"
    );
    assert_contiguous("the crashed run's quotes", &crashed_run, 0);
    let after: Vec<u64> = seqs[crashed_run.len()..].to_vec();
    assert_eq!(after.first(), Some(&WRITTEN_BACK), "{after:?}");
    assert_contiguous("the restart's quotes", &after[1..], 1_000_000);
    assert_eq!(staged_files(dir.path()), Vec::<PathBuf>::new());
}

/// No check of ours stands before a backtest: one across a crash runs on
/// what the catalog stores, the gap the crash left replayed as a quiet
/// period.
#[test]
fn a_backtest_across_a_crash_runs_through_the_gap_it_left() {
    let dir = tempfile::tempdir().unwrap();
    let mut crashed = spawn_node(dir.path(), Duration::from_secs(3_600), None, 0);
    wait_for_staged_quotes(dir.path(), &mut crashed);
    crashed.kill().unwrap();
    crashed.wait().unwrap();
    let mut restarted = spawn_node(dir.path(), Duration::from_secs(1), Some(3_000), 1_000_000);
    assert!(restarted.wait().unwrap().success());
    let quotes: Vec<QuoteTick> = catalog(dir.path())
        .query(None, None, None, None, None, true)
        .unwrap();
    let (crashed_run, restart): (Vec<&QuoteTick>, Vec<&QuoteTick>) = quotes
        .iter()
        .partition(|q| q.bid_size.as_f64() < 1_000_000.0);
    let crash = Timestamp::from(crashed_run.last().unwrap().ts_init);

    let backtest = backtest(dir.path(), crash, restart.last().unwrap().ts_init.into());

    assert!(backtest.status.success(), "{}", backtest_output(&backtest));
}

/// A backtest over the streamed data runs through the gaps between the
/// files Nautilus promoted, which stay in the catalog: it reads the rows,
/// not the catalog's intervals.
#[test]
fn a_backtest_over_the_streamed_data_runs_through_the_gaps_between_promotions() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = spawn_node(dir.path(), Duration::from_secs(1), Some(5_000), 0);
    assert!(node.wait().unwrap().success());
    let promoted = catalog(dir.path())
        .get_intervals(&NautilusDataType::QuoteTick.into(), Some(INSTRUMENT_ID))
        .unwrap();
    assert!(promoted.len() >= 2, "one promotion: {promoted:?}");
    let (first, last) = streamed_span(dir.path());

    let backtest = backtest(dir.path(), first, last);

    assert!(backtest.status.success(), "{}", backtest_output(&backtest));
    let missing = catalog(dir.path())
        .get_missing_intervals_for_request(
            UnixNanos::from(first).as_u64(),
            UnixNanos::from(last).as_u64(),
            &NautilusDataType::QuoteTick.into(),
            Some(INSTRUMENT_ID),
        )
        .unwrap();
    assert!(!missing.is_empty(), "no gap between promotions");
}

/// A dataset build, a retrain's replay, over the streamed data runs through
/// the gaps between the files Nautilus promoted, as a backtest does.
#[test]
fn a_dataset_build_over_the_streamed_data_runs_through_the_gaps_between_promotions() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = spawn_node(dir.path(), Duration::from_secs(1), Some(5_000), 0);
    assert!(node.wait().unwrap().success());
    let promoted = catalog(dir.path())
        .get_intervals(&NautilusDataType::QuoteTick.into(), Some(INSTRUMENT_ID))
        .unwrap();
    assert!(promoted.len() >= 2, "one promotion: {promoted:?}");
    let (first, last) = streamed_span(dir.path());

    let dataset = dataset(dir.path(), first, last);

    assert!(dataset.status.success(), "{}", backtest_output(&dataset));
    let stdout = String::from_utf8_lossy(&dataset.stdout);
    assert!(stdout.contains("built "), "{stdout}");
}

/// Not a test: the node the tests above run in a child process. Passes at
/// once outside one.
#[test]
fn streamed_node() {
    let Ok(root) = std::env::var(ROOT) else {
        return;
    };
    let number = |name: &str| std::env::var(name).ok().map(|v| v.parse::<u64>().unwrap());
    let root = PathBuf::from(root);
    let promotion_interval = Duration::from_millis(number(PROMOTION_MS).unwrap());
    let stop_after = number(STOP_MS).map(Duration::from_millis);
    let first_seq = number(FIRST_SEQ).unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_node(&root, promotion_interval, stop_after, first_seq))
        .unwrap();
}

/// Runs the node of `root` streaming as `Recording` configures it, but
/// promoting every `promotion_interval`, until `stop_after`, or until
/// killed.
async fn run_node(
    root: &Path,
    promotion_interval: Duration,
    stop_after: Option<Duration>,
    first_seq: u64,
) -> anyhow::Result<()> {
    let trader_id = TraderId::from(TRADER_ID);
    // Created first, as `Live` creates it.
    std::fs::create_dir_all(root.join("catalog"))?;
    let catalog = DataCatalogConfig::new(
        root.join("catalog").to_str().unwrap().to_string(),
        None,
        None,
    );
    let recording = Recording {
        staging: root.join("staging"),
    };
    let mut streaming = recording.streaming(&catalog)?;
    streaming.promotion_interval_ms = Some(u64::try_from(promotion_interval.as_millis())?);
    let config = LiveNodeConfig {
        environment: Environment::Live,
        trader_id,
        instance_id: Some(recording.instance_id(trader_id)?),
        streaming: Some(streaming),
        catalogs: vec![catalog],
        ..Default::default()
    };
    let mut node = LiveNodeBuilder::from_config(config)?
        .with_name("STREAMING")
        .with_delay_post_stop_secs(0)
        .add_data_client(
            None,
            Box::new(StubFactory),
            Box::new(StubConfig { first_seq }),
        )?
        .build()?;
    // As `Live` starts it: what the last run staged promoted before the
    // node runs.
    promote_staged(&mut node)?;
    if std::env::var(WRITE_BACK).is_ok() {
        write_back(root)?;
    }
    node.add_actor(Subscriber {
        core: DataActorCore::new(DataActorConfig::default()),
    })?;
    if let Some(stop_after) = stop_after {
        let handle = node.handle();
        tokio::spawn(async move {
            tokio::time::sleep(stop_after).await;
            handle.stop();
        });
    }
    let result = node.run().await;
    node.dispose();
    result
}

/// The number the quote a venue answers a warmup with carries: none the
/// stub sends.
const WRITTEN_BACK: u64 = 2_000_000;

/// Writes back what a venue answers a warmup asking for the quotes the
/// catalog of `root` lacks up to now, as IC's history is (`update_catalog`):
/// one quote, at now, filed over the last missing window.
fn write_back(root: &Path) -> anyhow::Result<()> {
    let catalog = catalog(root);
    let now = get_atomic_clock_realtime().get_time_ns();
    let missing = catalog.get_missing_intervals_for_request(
        0,
        now.as_u64(),
        &NautilusDataType::QuoteTick.into(),
        Some(INSTRUMENT_ID),
    )?;
    let &(start, end) = missing.last().context("the catalog lacks nothing")?;
    let quote = QuoteTick::new(
        InstrumentId::from(INSTRUMENT_ID),
        Price::from("0.80000"),
        Price::from("0.80010"),
        Quantity::new((WRITTEN_BACK + 1) as f64, 0),
        Quantity::from("1"),
        UnixNanos::from(end),
        UnixNanos::from(end),
    );
    catalog.write_to_parquet(
        &[quote],
        Some(UnixNanos::from(start)),
        Some(UnixNanos::from(end)),
        None,
    )?;
    Ok(())
}

/// What a reader finds in the catalog: the number of quotes, trades,
/// depth snapshots and stored bars, each checked to be a clean prefix of
/// what the node streamed.
#[derive(Debug)]
struct Read {
    quotes: u64,
    trades: u64,
    depths: u64,
    bars: u64,
}

impl Read {
    /// Reads the catalog of `root` as another process does.
    ///
    /// # Panics
    ///
    /// Panics if a series has a gap or a duplicate, or does not start at
    /// the first tick, or a bar is not a second after the one before.
    fn of(root: &Path) -> Self {
        let quotes = quote_seqs(root);
        assert_contiguous("quotes", &quotes, 0);
        let mut catalog = catalog(root);
        let trades: Vec<TradeTick> = catalog.query(None, None, None, None, None, true).unwrap();
        let trades: Vec<u64> = trades
            .iter()
            .map(|t| t.trade_id.as_str().parse().unwrap())
            .collect();
        assert_contiguous("trades", &trades, 0);
        let depths: Vec<OrderBookDepth> =
            catalog.query(None, None, None, None, None, true).unwrap();
        let depths: Vec<u64> = depths.iter().map(|d| d.sequence).collect();
        assert_contiguous("depths", &depths, 0);
        let bars = bar_seconds(root);
        if let Some(&first) = bars.first() {
            assert_contiguous("bars", &bars, first);
        }
        Self {
            quotes: quotes.len() as u64,
            trades: trades.len() as u64,
            depths: depths.len() as u64,
            bars: bars.len() as u64,
        }
    }
}

/// Asserts that `seqs`, in the order read, are `first`, `first + 1`, and
/// so on: no gap, no duplicate.
fn assert_contiguous(what: &str, seqs: &[u64], first: u64) {
    let expected: Vec<u64> = (first..first + seqs.len() as u64).collect();
    assert_eq!(seqs, expected, "{what} are not a clean prefix");
}

fn catalog(root: &Path) -> ParquetDataCatalog {
    ParquetDataCatalog::new(&root.join("catalog"), None, None, None, None)
}

/// The numbers of the quotes in the catalog of `root`, in order.
fn quote_seqs(root: &Path) -> Vec<u64> {
    let quotes: Vec<QuoteTick> = catalog(root)
        .query(None, None, None, None, None, true)
        .unwrap();
    quotes
        .iter()
        .map(|q| q.bid_size.as_f64() as u64 - 1)
        .collect()
}

/// The second each stored bar closes on, in order.
fn bar_seconds(root: &Path) -> Vec<u64> {
    let bars: Vec<Bar> = catalog(root)
        .query(
            Some(vec![STORED_BARS.to_string()]),
            None,
            None,
            None,
            None,
            true,
        )
        .unwrap();
    bars.iter()
        .map(|b| b.ts_event.as_u64() / 1_000_000_000)
        .collect()
}

/// When the quotes, trades and depth of the catalog of `root` all start,
/// and when they all end.
fn streamed_span(root: &Path) -> (Timestamp, Timestamp) {
    let mut catalog = catalog(root);
    let quotes: Vec<QuoteTick> = catalog.query(None, None, None, None, None, true).unwrap();
    let trades: Vec<TradeTick> = catalog.query(None, None, None, None, None, true).unwrap();
    let depths: Vec<OrderBookDepth> = catalog.query(None, None, None, None, None, true).unwrap();
    let spans = [
        quotes
            .first()
            .zip(quotes.last())
            .map(|(f, l)| (f.ts_init, l.ts_init)),
        trades
            .first()
            .zip(trades.last())
            .map(|(f, l)| (f.ts_init, l.ts_init)),
        depths
            .first()
            .zip(depths.last())
            .map(|(f, l)| (f.ts_init, l.ts_init)),
    ]
    .map(|span| span.expect("a series streamed nothing"));
    let first = spans.iter().map(|&(first, _)| first).max().unwrap();
    let last = spans.iter().map(|&(_, last)| last).min().unwrap();
    (first.into(), last.into())
}

/// The EMA crossover on the stub's AUD/USD: 1-second bars of its quotes,
/// its trades and its stored depth replayed as they are. Too slow to trade in a few seconds.
const EMA_CONFIG: &str = r#"
    instrument_ids = ["AUD/USD.SIM"]
    trade_instrument_id = "AUD/USD.SIM"
    bar_spec = { step = 1, aggregation = "SECOND", price_type = "MID" }
    warmup = "1s"
    ticks = [{ data = "trades", instrument_id = "AUD/USD.SIM" }]
    trade_size = "1"
    stop_loss_bps = 100
    take_profit_bps = 100
    hold = "1h"
"#;

/// The binary's backtest of the EMA crossover (`EMA_CONFIG`) over
/// `[start, end]` of the catalog of `root`, in a process of its own.
fn backtest(root: &Path, start: Timestamp, end: Timestamp) -> Output {
    let config = root.join("ema.toml");
    std::fs::write(&config, EMA_CONFIG).unwrap();
    Command::new(env!("CARGO_BIN_EXE_helix-trader"))
        .args(["backtest", "ema", "--config"])
        .arg(&config)
        .arg("--catalog")
        .arg(root.join("catalog"))
        .args(["--start", &start.to_string(), "--end", &end.to_string()])
        .args(["--commission", "1 USD", "--balance", "1000000 USD"])
        .output()
        .unwrap()
}

/// The learned strategy on the stub's AUD/USD: a decision every second on
/// its 1-second bars, each target held a second.
const LEARNED_CONFIG: &str = r#"
    instrument_ids = ["AUD/USD.SIM"]
    bar_spec = { step = 1, aggregation = "SECOND", price_type = "MID" }
    warmup = "1s"
    hold = "1s"

    [model]
    decision = { every = "1s" }
    targets = { instrument_id = "AUD/USD.SIM", quantity = "1", horizon = "1s" }
    label = "horizon"
    retrain = "24h"
    window = "46h"
    holdout = "12h"
    thresholds = { long = 0.6, short = 0.4 }
    min_score = 0.5
    learner = { kind = "fake" }
"#;

/// The binary's dataset build of the learned strategy (`LEARNED_CONFIG`)
/// over the days of `[start, end]` of the catalog of `root`, in a process of
/// its own.
fn dataset(root: &Path, start: Timestamp, end: Timestamp) -> Output {
    let config = root.join("learned.toml");
    std::fs::write(&config, LEARNED_CONFIG).unwrap();
    Command::new(env!("CARGO_BIN_EXE_helix-trader"))
        .args(["dataset", "learned", "--config"])
        .arg(&config)
        .arg("--catalog")
        .arg(root.join("catalog"))
        .args(["--start", &start.to_string(), "--end", &end.to_string()])
        .args(["--commission", "1 USD", "--balance", "1000000 USD"])
        .output()
        .unwrap()
}

fn backtest_output(output: &Output) -> String {
    format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// The Feather files left in the staging directory of `root`.
fn staged_files(root: &Path) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut dirs = vec![root.join("staging")];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.to_string_lossy().contains(".feather") {
                found.push(path);
            }
        }
    }
    found
}

/// Waits until the staged quotes of `root` grow after their first write,
/// a batch flushed past the file's header.
fn wait_for_staged_quotes(root: &Path, node: &mut Child) {
    let size = || -> u64 {
        if !root.join("staging").exists() {
            return 0;
        }
        staged_files(root)
            .iter()
            .filter(|path| path.to_string_lossy().contains("quotes"))
            .map(|path| std::fs::metadata(path).map_or(0, |m| m.len()))
            .sum()
    };
    let deadline = Instant::now() + PATIENCE;
    let mut first = 0;
    loop {
        assert!(Instant::now() < deadline, "no quotes staged");
        assert!(node.try_wait().unwrap().is_none(), "the node stopped");
        let now = size();
        if first == 0 {
            first = now;
        } else if now > first {
            return;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Spawns the node of `root` in a child process (`streamed_node`). Its
/// catalog is created first, so that the test reads it from the start.
fn spawn_node(
    root: &Path,
    promotion_interval: Duration,
    stop_ms: Option<u64>,
    first_seq: u64,
) -> NodeProcess {
    NodeProcess(
        node_command(root, promotion_interval, stop_ms, first_seq)
            .spawn()
            .unwrap(),
    )
}

/// The command of the child process of `spawn_node`.
fn node_command(
    root: &Path,
    promotion_interval: Duration,
    stop_ms: Option<u64>,
    first_seq: u64,
) -> Command {
    std::fs::create_dir_all(root.join("catalog")).unwrap();
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["streaming::streamed_node", "--exact", "--nocapture"])
        .env(ROOT, root)
        .env(PROMOTION_MS, promotion_interval.as_millis().to_string())
        .env(FIRST_SEQ, first_seq.to_string())
        .env_remove(STOP_MS)
        .env_remove(WRITE_BACK);
    if let Some(stop_ms) = stop_ms {
        command.env(STOP_MS, stop_ms.to_string());
    }
    command
}

/// A node's child process, killed if the test ends before it does, so that
/// a failing test leaves no node running.
struct NodeProcess(Child);

impl Deref for NodeProcess {
    type Target = Child;

    fn deref(&self) -> &Child {
        &self.0
    }
}

impl DerefMut for NodeProcess {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        if self.0.try_wait().is_ok_and(|status| status.is_none()) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

/// Subscribes to the stub's quotes, trades and depth, and to the bars
/// Nautilus builds from the trades, as a strategy would.
#[derive(Debug)]
struct Subscriber {
    core: DataActorCore,
}

nautilus_actor!(Subscriber);

impl DataActor for Subscriber {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let instrument_id = InstrumentId::from(INSTRUMENT_ID);
        self.subscribe_quotes(instrument_id, None, None);
        self.subscribe_trades(instrument_id, None, None);
        self.subscribe_book_depth(
            instrument_id,
            BookType::L2_MBP,
            NonZeroUsize::new(10),
            None,
            false,
            None,
        );
        self.subscribe_bars(BarType::from(BUILT_BARS), None, None);
        Ok(())
    }
}

#[derive(Debug)]
struct StubConfig {
    first_seq: u64,
}

impl ClientConfig for StubConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
struct StubFactory;

impl DataClientFactory for StubFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config.as_any().downcast_ref::<StubConfig>().unwrap();
        Ok(Box::new(Stub {
            id: ClientId::from(name),
            first_seq: config.first_seq,
            sender: get_data_event_sender(),
            feed: None,
        }))
    }

    fn name(&self) -> &str {
        "STUB"
    }

    fn config_type(&self) -> &str {
        "StubConfig"
    }
}

/// A data client on `SIM`: loads AUD/USD at connect, then sends a
/// numbered quote, trade and depth snapshot every `TICK`.
#[derive(Debug)]
struct Stub {
    id: ClientId,
    first_seq: u64,
    sender: EventSender<DataEvent>,
    feed: Option<tokio::task::JoinHandle<()>>,
}

impl Stub {
    fn stop_feed(&mut self) {
        if let Some(feed) = self.feed.take() {
            feed.abort();
        }
    }
}

#[async_trait(?Send)]
impl DataClient for Stub {
    fn client_id(&self) -> ClientId {
        self.id
    }
    fn venue(&self) -> Option<Venue> {
        Some(Venue::from("SIM"))
    }
    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
    fn stop(&mut self) -> anyhow::Result<()> {
        self.stop_feed();
        Ok(())
    }
    fn reset(&mut self) -> anyhow::Result<()> {
        self.stop_feed();
        Ok(())
    }
    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop_feed();
        Ok(())
    }
    fn is_connected(&self) -> bool {
        self.feed.is_some()
    }
    fn is_disconnected(&self) -> bool {
        self.feed.is_none()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        // Stamped when loaded, as the venues' clients stamp theirs: a restart
        // streams a definition of its own.
        let mut definition = audusd_sim();
        let loaded = get_atomic_clock_realtime().get_time_ns();
        definition.ts_event = loaded;
        definition.ts_init = loaded;
        self.sender
            .send(DataEvent::Instrument(InstrumentAny::CurrencyPair(
                definition,
            )))?;
        let sender = self.sender.clone();
        let first_seq = self.first_seq;
        self.feed = Some(tokio::spawn(async move {
            let instrument_id = InstrumentId::from(INSTRUMENT_ID);
            let clock = get_atomic_clock_realtime();
            let mut ticks = tokio::time::interval(TICK);
            for seq in first_seq.. {
                ticks.tick().await;
                let ts = clock.get_time_ns();
                let quote = QuoteTick::new(
                    instrument_id,
                    Price::from("0.80000"),
                    Price::from("0.80010"),
                    Quantity::new((seq + 1) as f64, 0),
                    Quantity::from("1"),
                    ts,
                    ts,
                );
                let trade = TradeTick::new(
                    instrument_id,
                    Price::from("0.80005"),
                    Quantity::from("1"),
                    AggressorSide::Buy,
                    TradeId::new(seq.to_string()),
                    ts,
                    ts,
                );
                let mut depth = stub_depth10();
                depth.instrument_id = instrument_id;
                // AUD/USD's prices, a backtest replaying them as stored: the
                // quote's bid and ask, then a pip further a level.
                for (level, order) in (0_u32..).zip(depth.bids.iter_mut()) {
                    order.price = Price::new(0.8 - f64::from(level) * 0.000_01, 5);
                }
                for (level, order) in (0_u32..).zip(depth.asks.iter_mut()) {
                    order.price = Price::new(0.8001 + f64::from(level) * 0.000_01, 5);
                }
                depth.sequence = seq;
                depth.ts_event = ts;
                depth.ts_init = ts;
                for data in [
                    Data::Quote(quote),
                    Data::Trade(trade),
                    Data::BookDepth(Box::new(depth)),
                ] {
                    if sender.send(DataEvent::Data(data)).is_err() {
                        return;
                    }
                }
            }
        }));
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.stop_feed();
        Ok(())
    }
}
