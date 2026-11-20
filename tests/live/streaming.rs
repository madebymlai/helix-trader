//! Tests that the live node streams what it receives into the catalog, with no venue.
//!
//! A node runs in a child process (this binary, run on `streamed_node`). It holds a stub data
//! client on `SIM`, which loads AUD/USD at connect, as the venues' clients do. Every `TICK`, the
//! client sends a quote, a trade and a depth snapshot. Each is numbered in sequence: the quote's
//! bid size less one, the trade's id, the depth's sequence. An actor subscribes to them, and to
//! 1-second bars Nautilus builds from the trades.
//!
//! The node is built as `Live` builds it (`live::node::build`), on the stub alone, with its cache
//! in memory. It streams as `Recording` configures it into a temp-dir catalog, but promotes every
//! `PROMOTION_MS` rather than daily, so a test sees several promotions in seconds.
//!
//! The test runs in another process. It reads the catalog while the node runs and after it stops,
//! backtests over it with the binary, and builds a dataset over it in a child of this binary
//! (`streamed_dataset`).
//!
//! The module is hermetic and ungated: no node runs in the test's own process, so it claims none
//! (`main.rs`). Run it with:
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
    process::{Child, Command, ExitStatus, Output},
    rc::Rc,
    time::{Duration, Instant},
};

use anyhow::Context;
use async_trait::async_trait;
use helix_trader::{
    backtest::Backtest,
    live::{self, Recording},
    model::{
        ModelStrategy, dataset,
        handle::{Mode, SettledRows},
    },
    strategy::model_ema::{ModelEma, ModelEmaConfig},
};
use jiff::Timestamp;
use nautilus_common::{
    actor::{DataActor, DataActorConfig, DataActorCore},
    cache::{CacheConfig, CacheView},
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
    live::{get_runtime, runner::get_data_event_sender, sender::EventSender},
    logging::logger::LoggerConfig,
    messages::DataEvent,
    nautilus_actor,
};
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::config::{LiveDataEngineConfig, RoutingConfig};
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
use rstest::rstest;

const INSTRUMENT_ID: &str = "AUD/USD.SIM";

const BUILT_BARS: &str = "AUD/USD.SIM-1-SECOND-LAST-INTERNAL";

/// `BUILT_BARS` as the catalog stores them.
const STORED_BARS: &str = "AUD/USD.SIM-1-SECOND-LAST-EXTERNAL";
const TRADER_ID: &str = "HELIXTEST-301";
const TICK: Duration = Duration::from_millis(20);

/// The child's catalog and staging directory, set only in the child.
const ROOT: &str = "HELIX_STREAMING_ROOT";

/// The child's promotion interval in milliseconds. It replaces `Recording`'s daily interval.
const PROMOTION_MS: &str = "HELIX_STREAMING_PROMOTION_MS";

const FIRST_SEQ: &str = "HELIX_STREAMING_FIRST_SEQ";

/// When set, the child writes a venue's answer back over what the catalog lacks before its node
/// runs, as a warmup does.
const WRITE_BACK: &str = "HELIX_STREAMING_WRITE_BACK";

const DATASET_SPAN: &str = "HELIX_STREAMING_DATASET_SPAN";

const PATIENCE: Duration = Duration::from_secs(60);

#[rstest]
fn a_reader_sees_a_clean_prefix_of_the_streamed_data_while_the_node_runs() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = spawn_node(dir.path(), Duration::from_secs(1), 0);
    let mut prefixes_while_running: Vec<Read> = Vec::new();
    let deadline = Instant::now() + PATIENCE;
    loop {
        let read = Read::of(dir.path());
        assert!(node.try_wait().unwrap().is_none(), "the node stopped");
        let grown = prefixes_while_running
            .first()
            .is_some_and(|first| first.quotes < read.quotes);
        let bars = read.bars;
        if read.quotes > 0 {
            prefixes_while_running.push(read);
        }
        if grown && bars >= 3 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the catalog did not grow under the reader to 3 bars while the node ran: \
             {prefixes_while_running:?}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(interrupt(&mut node).success());
    let last = Read::of(dir.path());
    for read in &prefixes_while_running {
        assert!(read.quotes <= last.quotes && read.trades <= last.trades);
        assert!(read.depths <= last.depths && read.bars <= last.bars);
    }
}

#[rstest]
fn the_bars_the_node_builds_are_stored_under_the_external_source() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = spawn_node(dir.path(), Duration::from_secs(1), 0);

    // Two promotions, two seconds of trades: a bar closed
    assert!(interrupt_once_promoted(dir.path(), &mut node, 2).success());
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

#[rstest]
fn a_clean_stop_promotes_everything_and_leaves_no_staging_file() {
    let dir = tempfile::tempdir().unwrap();

    // No promotion before the stop: the stop promotes all
    let mut node = spawn_node(dir.path(), Duration::from_secs(3_600), 0);
    assert!(interrupt_once_staged(dir.path(), &mut node, "bars").success());
    let read = Read::of(dir.path());
    assert!(read.quotes > 0 && read.trades > 0 && read.depths > 0 && read.bars > 0);
    assert_eq!(staged_files(dir.path()), Vec::<PathBuf>::new());
}

#[rstest]
fn the_instruments_the_clients_load_are_in_the_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = spawn_node(dir.path(), Duration::from_secs(1), 0);
    assert!(interrupt_once_promoted(dir.path(), &mut node, 1).success());

    // As a backtest node looks its instruments up
    let instruments = catalog(dir.path())
        .instruments(Some(&[INSTRUMENT_ID.to_string()]), None, None)
        .unwrap();
    let ids: Vec<InstrumentId> = instruments.iter().map(Instrument::id).collect();
    assert_eq!(ids, vec![InstrumentId::from(INSTRUMENT_ID)]);
}

#[rstest]
fn a_restart_after_a_crash_promotes_what_the_crashed_run_staged() {
    let dir = tempfile::tempdir().unwrap();
    let mut crashed = spawn_node(dir.path(), Duration::from_secs(3_600), 0);
    wait_for_staged_quotes(dir.path(), &mut crashed);
    crashed.kill().unwrap();
    crashed.wait().unwrap();
    assert_eq!(Read::of(dir.path()).quotes, 0, "promoted before the crash");
    let mut restarted = spawn_node(dir.path(), Duration::from_secs(1), 1_000_000);
    assert!(interrupt_once_promoted_from(dir.path(), &mut restarted, 1_000_000).success());
    let seqs = quote_seqs(dir.path());
    let (first_run, second_run): (Vec<u64>, Vec<u64>) =
        seqs.into_iter().partition(|&seq| seq < 1_000_000);
    assert!(!first_run.is_empty(), "the crashed run's quotes were lost");
    assert_contiguous("the crashed run's quotes", &first_run, 0);
    assert!(!second_run.is_empty());
    assert_contiguous("the restart's quotes", &second_run, 1_000_000);
    assert_eq!(staged_files(dir.path()), Vec::<PathBuf>::new());
}

/// The restart's warmup writes the venue's answer over what the catalog lacks up to now.
///
/// The crashed run's staged rows are in the catalog by then, so the answer lands after them.
#[rstest]
fn a_restart_promotes_what_the_crashed_run_staged_before_its_warmup_writes_back() {
    let dir = tempfile::tempdir().unwrap();
    let mut crashed = spawn_node(dir.path(), Duration::from_secs(3_600), 0);
    wait_for_staged_quotes(dir.path(), &mut crashed);
    crashed.kill().unwrap();
    crashed.wait().unwrap();
    let mut restarted = NodeProcess(
        node_command(dir.path(), Duration::from_secs(1), 1_000_000)
            .env(WRITE_BACK, "1")
            .spawn()
            .unwrap(),
    );

    assert!(interrupt_once_promoted_from(dir.path(), &mut restarted, 1_000_000).success());

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

/// No check of ours precedes a backtest.
///
/// A backtest across a crash runs on what the catalog stores, and replays the gap as a quiet
/// period.
#[rstest]
fn a_backtest_across_a_crash_runs_through_the_gap_it_left() {
    let dir = tempfile::tempdir().unwrap();
    let mut crashed = spawn_node(dir.path(), Duration::from_secs(3_600), 0);
    wait_for_staged_quotes(dir.path(), &mut crashed);
    crashed.kill().unwrap();
    crashed.wait().unwrap();
    let mut restarted = spawn_node(dir.path(), Duration::from_secs(1), 1_000_000);
    assert!(interrupt_once_promoted_from(dir.path(), &mut restarted, 1_000_000).success());
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

/// A backtest over the streamed data runs through the gaps between the promoted files.
///
/// The gaps stay in the catalog. The backtest reads the rows, not the catalog's intervals.
#[rstest]
fn a_backtest_over_the_streamed_data_runs_through_the_gaps_between_promotions() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = spawn_node(dir.path(), Duration::from_secs(1), 0);
    assert!(interrupt_once_promoted(dir.path(), &mut node, 2).success());
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

/// A dataset build over the streamed data runs through the gaps between the promoted files, as a
/// backtest does.
///
/// This is the build of a model backtest before its run, and of a retrain's replay.
#[rstest]
fn a_dataset_build_over_the_streamed_data_runs_through_the_gaps_between_promotions() {
    let dir = tempfile::tempdir().unwrap();
    let mut node = spawn_node(dir.path(), Duration::from_secs(1), 0);
    assert!(interrupt_once_promoted(dir.path(), &mut node, 2).success());
    let (first, last) = streamed_span(dir.path());

    let dataset = dataset(dir.path(), first, last);

    assert!(dataset.status.success(), "{}", backtest_output(&dataset));
    let stdout = String::from_utf8_lossy(&dataset.stdout);
    assert!(stdout.contains("built "), "{stdout}");
}

/// Not a test: runs the dataset build of the test above in a child process.
///
/// The build is over the days of `DATASET_SPAN` in the catalog of `ROOT`. Passes at once outside a
/// child process.
#[rstest]
fn streamed_dataset() {
    let (Ok(root), Ok(span)) = (std::env::var(ROOT), std::env::var(DATASET_SPAN)) else {
        return;
    };
    let (start, end) = span.split_once(' ').unwrap();
    let (start, end): (Timestamp, Timestamp) = (start.parse().unwrap(), end.parse().unwrap());
    let catalog = PathBuf::from(root).join("catalog");
    let config = ModelEmaConfig::load(MODEL_EMA_CONFIG).unwrap();
    let (first, last) = dataset::days(start, end);

    let built = ModelEma::dataset(&config, &catalog)
        .unwrap()
        .build(first, last, |from, to| {
            let requirements = config.data_requirements();
            let rows = SettledRows::default();
            let mode = Mode::Dataset { rows: rows.clone() };
            let venue = config.model().dataset;
            Backtest {
                catalog: catalog.clone(),
                bar_types: requirements.bar_types,
                ticks: requirements.ticks,
                custom: vec![],
                start: from,
                end: to,
                commission: venue.commission,
                latency: venue.latency,
                pairs: vec![],
                balance: venue.balance,
                logging: LoggerConfig::default(),
            }
            .run_dataset(ModelEma::new(config.clone(), mode)?)?;
            rows.take()
        })
        .unwrap();

    assert!(!built.is_empty(), "the dataset held every day");
    println!("built {} days of the dataset", built.len());
}

/// Not a test: runs the node the tests above use, in a child process. Passes at once outside one.
#[rstest]
fn streamed_node() {
    let Ok(root) = std::env::var(ROOT) else {
        return;
    };
    let number = |name: &str| std::env::var(name).ok().map(|v| v.parse::<u64>().unwrap());
    let root = PathBuf::from(root);
    let promotion_interval = Duration::from_millis(number(PROMOTION_MS).unwrap());
    let first_seq = number(FIRST_SEQ).unwrap();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_node(&root, promotion_interval, first_seq))
        .unwrap();
}

/// Runs the node of `root` until it is interrupted or killed.
///
/// The node has no cache database and promotes every `promotion_interval`.
async fn run_node(root: &Path, promotion_interval: Duration, first_seq: u64) -> anyhow::Result<()> {
    let trader_id = TraderId::from(TRADER_ID);

    // Created first, as `Live` creates it
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
    let stub = live::node::Clients {
        data_factory: Box::new(StubFactory),
        data_config: Box::new(StubConfig { first_seq }),
        data_routing: RoutingConfig::default(),
        exec: None,
    };
    let mut node = live::node::build(
        trader_id,
        recording.instance_id(trader_id)?,
        LoggerConfig::default(),
        CacheConfig::default(),
        LiveDataEngineConfig::default(),
        streaming,
        catalog,
        None,
        vec![stub],
    )?;
    if std::env::var(WRITE_BACK).is_ok() {
        write_back(root)?;
    }
    node.add_actor(Subscriber {
        core: DataActorCore::new(DataActorConfig::default()),
    })?;
    let result = node.run().await;
    node.dispose();
    result
}

/// The number carried by the quote a venue answers a warmup with. The stub sends no quote with it.
const WRITTEN_BACK: u64 = 2_000_000;

/// Writes back a venue's answer to a warmup (`update_catalog`) for the quotes the catalog of `root`
/// lacks up to now.
///
/// The answer is one quote, at now, filed over the last missing window.
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

/// What a reader finds in the catalog.
///
/// Each series is a clean prefix of what the node streamed.
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
    /// Panics if:
    /// - A series has a gap or a duplicate.
    /// - A series does not start at the first tick.
    /// - A bar is not a second after the one before.
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

/// Asserts that `seqs` are `first`, `first + 1`, and so on: no gap, no duplicate.
fn assert_contiguous(what: &str, seqs: &[u64], first: u64) {
    let expected: Vec<u64> = (first..first + seqs.len() as u64).collect();
    assert_eq!(seqs, expected, "{what} are not a clean prefix");
}

fn catalog(root: &Path) -> ParquetDataCatalog {
    ParquetDataCatalog::new(&root.join("catalog"), None, None, None, None)
}

fn quote_seqs(root: &Path) -> Vec<u64> {
    let quotes: Vec<QuoteTick> = catalog(root)
        .query(None, None, None, None, None, true)
        .unwrap();
    quotes
        .iter()
        .map(|q| q.bid_size.as_f64() as u64 - 1)
        .collect()
}

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

/// The EMA crossover on the stub's AUD/USD.
///
/// It uses 1-second bars of the quotes, and is too slow to trade in a few seconds.
const EMA_CONFIG: &str = r#"
    [data]
    series = [
        { data = "bars", instrument_id = "AUD/USD.SIM", bar_spec = { step = 1, aggregation = "SECOND", price_type = "MID" } },
    ]
    warmup = "1s"

    [indicators]
    fast = { period = 10 }
    slow = { period = 30 }

    [trade]
    instruments = [{ instrument_id = "AUD/USD.SIM" }]
    risk = "0.5%"
"#;

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

/// The `model_ema` strategy on the stub's AUD/USD.
///
/// The strategy:
///
/// - Decides every two seconds, on averages of 10 and 30 of its 1-second bars.
/// - Holds each target a second, and closes it before the next.
/// - Measures its unit in the ATR of one two-second bar, the warmup's.
const MODEL_EMA_CONFIG: &str = r#"
    [data]
    series = [
        { data = "bars", instrument_id = "AUD/USD.SIM", bar_spec = { step = 1, aggregation = "SECOND", price_type = "MID" } },
    ]
    warmup = "2s"

    [indicators.fast]
    period = 10

    [indicators.slow]
    period = 30

    [trade]
    instruments = [{ instrument_id = "AUD/USD.SIM" }]
    risk = "0.5%"

    [model]
    dataset = { commission = { per_order = "1 USD" }, balance = "1000000 USD" }
    decision = { every = "2s" }
    target = { limit = "1s", atr = { period = 1 } }
    retrain = "24h"
    window = "46h"
    calibration = "12h"
    margins = { long = "0R", short = "0R" }
    min_rows = 1
    learner = { kind = "fake" }
"#;

fn dataset(root: &Path, start: Timestamp, end: Timestamp) -> Output {
    Command::new(std::env::current_exe().unwrap())
        .args(["streaming::streamed_dataset", "--exact", "--nocapture"])
        .env(ROOT, root)
        .env(DATASET_SPAN, format!("{start} {end}"))
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

/// Waits until the staged quotes of `root` grow after their first write.
///
/// The growth is a batch flushed past the file's header.
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

/// Stops `node` cleanly, as Ctrl-C would, and returns how it exited.
fn interrupt(node: &mut Child) -> ExitStatus {
    let status = Command::new("kill")
        .args(["-INT", &node.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let deadline = Instant::now() + PATIENCE;
    loop {
        if let Some(exit) = node.try_wait().unwrap() {
            return exit;
        }
        assert!(Instant::now() < deadline, "the node did not stop on SIGINT");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Interrupts `node` once it has promoted `files` files of quotes into `root`'s catalog, and
/// returns how it exited.
/// exited.
fn interrupt_once_promoted(root: &Path, node: &mut Child, files: usize) -> ExitStatus {
    let deadline = Instant::now() + PATIENCE;
    loop {
        let promoted = catalog(root)
            .get_intervals(&NautilusDataType::QuoteTick.into(), Some(INSTRUMENT_ID))
            .unwrap();
        if promoted.len() >= files {
            return interrupt(node);
        }
        assert!(Instant::now() < deadline, "promoted {promoted:?}");
        assert!(node.try_wait().unwrap().is_none(), "the node stopped");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Interrupts `node` once the catalog of `root` holds a quote of its run, and returns how it
/// exited.
///
/// The quote is numbered `first_seq` or more. It is not the one written back (`WRITTEN_BACK`).
fn interrupt_once_promoted_from(root: &Path, node: &mut Child, first_seq: u64) -> ExitStatus {
    let deadline = Instant::now() + PATIENCE;

    loop {
        if quote_seqs(root)
            .iter()
            .any(|seq| (first_seq..WRITTEN_BACK).contains(seq))
        {
            return interrupt(node);
        }
        assert!(Instant::now() < deadline, "no quote of the run promoted");
        assert!(node.try_wait().unwrap().is_none(), "the node stopped");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Interrupts `node` once it has staged a file of `series` (`bars_…`, `quotes_…`) in the staging
/// directory of `root`, and returns how it exited.
///
/// The node has then received some of the series, which its writer promotes as it closes.
fn interrupt_once_staged(root: &Path, node: &mut Child, series: &str) -> ExitStatus {
    let deadline = Instant::now() + PATIENCE;

    loop {
        let staged = root.join("staging").exists()
            && staged_files(root).iter().any(|path| {
                path.file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with(series))
            });

        if staged {
            return interrupt(node);
        }
        assert!(Instant::now() < deadline, "no {series} staged");
        assert!(node.try_wait().unwrap().is_none(), "the node stopped");
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Spawns the node of `root` in a child process.
///
/// The catalog is created first, so the test reads it from the start.
fn spawn_node(root: &Path, promotion_interval: Duration, first_seq: u64) -> NodeProcess {
    NodeProcess(
        node_command(root, promotion_interval, first_seq)
            .spawn()
            .unwrap(),
    )
}

fn node_command(root: &Path, promotion_interval: Duration, first_seq: u64) -> Command {
    std::fs::create_dir_all(root.join("catalog")).unwrap();
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["streaming::streamed_node", "--exact", "--nocapture"])
        .env(ROOT, root)
        .env(PROMOTION_MS, promotion_interval.as_millis().to_string())
        .env(FIRST_SEQ, first_seq.to_string())
        .env_remove(WRITE_BACK);
    command
}

/// A node's child process, killed if the test ends before it does.
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

/// Subscribes to the stub's quotes, trades and depth, and the bars built from the trades.
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

    fn name(&self) -> &'static str {
        "STUB"
    }

    fn config_type(&self) -> &'static str {
        "StubConfig"
    }
}

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
        // Stamped when loaded, as the venues' clients stamp theirs, so a restart streams a
        // definition of its own.
        let mut definition = audusd_sim();
        let loaded = get_atomic_clock_realtime().get_time_ns();
        definition.ts_event = loaded;
        definition.ts_init = loaded;
        self.sender.send(DataEvent::Data(
            InstrumentAny::CurrencyPair(definition).into(),
        ))?;
        let sender = self.sender.clone();
        let first_seq = self.first_seq;
        self.feed = Some(get_runtime().spawn(async move {
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

                // AUD/USD's prices, so a backtest replays them as stored: the quote's bid and ask,
                // then a pip further each level.
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
