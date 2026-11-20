//! Command line: one subcommand per process (fill-catalog, backtest, live, train). Nautilus
//! runs no two nodes of a process at once, so a live node's retrain is a process of its own; a
//! backtest runs its nodes one after another, each disposed before the next. Every subcommand
//! takes the strategy name and its TOML config: the instruments and the warmup come from the
//! config's data requirements. A strategy trading on a model runs through
//! `model::ModelStrategy`, each subcommand generic over it.

use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    pin::Pin,
    str::FromStr,
};

use anyhow::Context;
use clap::{Args, Parser, Subcommand, ValueEnum};
use helix_trader::{
    backtest::{self, Backtest, Commission, Latency},
    catalog,
    live::{Live, Recording},
    model::{
        ModelStrategy, dataset,
        handle::{Mode, SettledRows},
        launcher::{Launcher, RetrainActor},
        report::{Meter, Report},
        schedule::Retrain,
        targets::TargetRow,
        train::Trained,
    },
    strategy::{
        Part, PlainStrategy, StandardConfig, StrategyName, Visitor, requirements::DataRequirements,
    },
    venues::{self, HistoryVenueArgs, LiveVenueArgs},
};
use jiff::Timestamp;
use nautilus_common::logging::{
    bridge::init_tracing, init_logging, logger::LoggerConfig, writer::FileWriterConfig,
};
use nautilus_core::{UUID4, UnixNanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    identifiers::{InstrumentId, TraderId},
    types::Money,
};

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Fills the Nautilus catalog with the strategy's history from OKX (OKX instruments), LSE
    /// (LSEDGE instruments, and GLBX, XNAS and OPRA instruments, defined by Databento), cTrader (IC
    /// Markets' and Pepperstone's instruments) or Interactive Brokers (the others).
    FillCatalog(FillCatalogArgs),
    /// Backtests the strategy over a window of the catalog and prints the results.
    Backtest(Box<BacktestArgs>),
    /// Runs the strategy on a live node with Interactive Brokers, IC Markets, Pepperstone or OKX;
    /// stops on Ctrl-C.
    Live(Box<LiveArgs>),
    /// Trains the strategy's models at a time, or at each retrain time of a span, those without one
    /// valid from it, replaying the days its dataset lacks, and prints a report of each train's run
    /// figures: its model set published or already published, or why it failed, its times, memory,
    /// dataset days and rows per slice; fails if any failed.
    Train(Box<TrainArgs>),
}

/// The arguments of a subcommand working on a window of the catalog.
#[derive(Debug, Args)]
struct RunArgs {
    /// The strategy whose instruments and warmup are used.
    strategy: StrategyName,
    /// The strategy's TOML config.
    #[arg(long)]
    config: PathBuf,
    /// Window start: a UTC date (2024-01-02) or an RFC 3339 timestamp.
    #[arg(long, value_parser = parse_time)]
    start: Timestamp,
    /// Window end, same format as --start.
    #[arg(long, value_parser = parse_time)]
    end: Timestamp,
    /// The catalog directory.
    #[arg(long, default_value = "catalog")]
    catalog: PathBuf,
}

impl RunArgs {
    /// The window `[start, end]`, checked.
    fn window(&self) -> anyhow::Result<(Timestamp, Timestamp)> {
        let (start, end) = (self.start, self.end);
        anyhow::ensure!(start < end, "start {start} must be before end {end}");
        Ok((start, end))
    }
}

#[derive(Debug, Args)]
struct FillCatalogArgs {
    #[command(flatten)]
    run: RunArgs,
    /// The venues' flags of a fill, each venue's own.
    #[command(flatten)]
    venues: HistoryVenueArgs,
}

#[derive(Debug, Args)]
struct BacktestArgs {
    #[command(flatten)]
    run: RunArgs,
    #[command(flatten)]
    commission: CommissionArgs,
    /// Starting balance of the simulated account, for example "10000 EUR"; a model strategy's
    /// dataset runs are at its config's (`[model] dataset`).
    #[arg(long, value_parser = parse_money)]
    balance: Money,
    /// The latency of the simulated account's venues, a duration such as "150ms": each order
    /// command reaches a venue that long after it is sent, and a market order fills on the first
    /// quote from then on. Left out, none. A model strategy's dataset runs are at its config's
    /// (`[model] dataset`).
    #[arg(long, value_parser = Latency::from_str)]
    latency: Option<Latency>,
    /// The model store a strategy trading on a model reads its models from: at each decision, the
    /// latest valid then. A backtest trains none, `train` does: it refuses to run if the store
    /// holds no model to trade on at its start.
    #[arg(long, default_value = "models")]
    models: PathBuf,
}

#[derive(Debug, Args)]
struct TrainArgs {
    /// The strategy whose model is trained.
    strategy: StrategyName,
    /// The strategy's TOML config.
    #[arg(long)]
    config: PathBuf,
    /// The one time trained at, the models' valid-from: same format as --start. A strategy never
    /// retrained is trained so alone.
    #[arg(long, value_parser = parse_time, required_unless_present = "from", conflicts_with = "from")]
    at: Option<Timestamp>,
    /// The start of a span trained over instead, at each of its retrain times in time order, those
    /// a backtest from --from to --to trades on: from the last not after --from. Same format as
    /// --start.
    #[arg(long, value_parser = parse_time, requires = "to")]
    from: Option<Timestamp>,
    /// The end of the span, same format: a retrain time at it is not trained, as a backtest ending
    /// there trades no model valid from it.
    #[arg(long, value_parser = parse_time, requires = "from")]
    to: Option<Timestamp>,
    /// The catalog directory: the data replayed, and the dataset.
    #[arg(long, default_value = "catalog")]
    catalog: PathBuf,
    /// The model store the model is published in.
    #[arg(long, default_value = "models")]
    models: PathBuf,
}

/// When a train trains (`TrainArgs::trains`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Trains {
    /// At one time, `--at`.
    At(Timestamp),
    /// At each retrain time of the span `[from, to)`, `--from` and `--to`.
    Over { from: Timestamp, to: Timestamp },
}

impl TrainArgs {
    /// When the train trains: at `--at`, or over `--from` to `--to`, checked.
    fn trains(&self) -> anyhow::Result<Trains> {
        match (self.at, self.from, self.to) {
            (Some(at), None, None) => Ok(Trains::At(at)),
            (None, Some(from), Some(to)) => {
                anyhow::ensure!(from < to, "--from {from} must be before --to {to}");
                Ok(Trains::Over { from, to })
            }
            times => unreachable!("clap requires --at or both --from and --to: {times:?}"),
        }
    }
}

/// The commission of a run: exactly one of the flags.
#[derive(Debug, Args)]
#[group(required = true, multiple = false)]
struct CommissionArgs {
    /// Commission charged once per order, for example "3 EUR".
    #[arg(long, value_parser = Commission::per_order)]
    commission: Option<Commission>,
    /// Commission rate on the value of each fill, with no minimum, for example 0.0005 (0.05%).
    #[arg(long, value_parser = Commission::rate)]
    commission_rate: Option<Commission>,
    /// Commission charged on each unit of the quantity of each fill, each fill's charge rounded to
    /// the currency's precision, for example "0.035 USD" (3.5 USD per 100 oz of gold,
    /// Pepperstone's).
    #[arg(long, value_parser = Commission::per_quantity)]
    commission_per_quantity: Option<Commission>,
}

impl CommissionArgs {
    /// The commission of the one flag given: the one source of the simulated venue's costs, its fee
    /// model (`backtest::Commission`); a model strategy's dataset runs are at its config's
    /// (`[model] dataset`).
    fn commission(&self) -> Commission {
        [
            self.commission,
            self.commission_rate,
            self.commission_per_quantity,
        ]
        .into_iter()
        .flatten()
        .next()
        .expect("clap requires one commission flag")
    }
}

#[derive(Debug, Args)]
struct LiveArgs {
    /// The strategy to run.
    strategy: StrategyName,
    /// The strategy's TOML config.
    #[arg(long)]
    config: PathBuf,
    /// The venues' flags of a live node, each venue's own.
    #[command(flatten)]
    venues: LiveVenueArgs,
    /// The node's trader: its cache in Redis is the trader's, which each run loads. Live processes
    /// running at once each need their own, and on one venue account their own tag, the part after
    /// the last hyphen: Nautilus puts only the tag in client order, order list and position ids, so
    /// `HELIX-001` and `OTHER-001` collide. Parsed by Nautilus' checked `FromStr`, since clap would
    /// otherwise pick `TraderId: From<&str>`, which panics.
    #[arg(long, default_value = "HELIX-001", value_parser = TraderId::from_str)]
    trader_id: TraderId,
    /// The catalog directory, fill-catalog's and the backtests': the warmup is read from it first,
    /// and a cTrader broker's history is written back to it. A strategy trading on a model requires
    /// it to exist and hold the instruments it trades, which its models and retrains are built
    /// from.
    #[arg(long, default_value = "catalog")]
    catalog: PathBuf,
    /// The directory the node stages the data it streams into the catalog, outside it; it also
    /// keeps each trader's instance id.
    #[arg(long, default_value = "staging")]
    staging: PathBuf,
    /// The model store a strategy trading on a model reads its models from, at each decision the
    /// latest valid then, and its retrains publish in: the store of its config, whose `[model]
    /// dataset` keys it.
    #[arg(long, default_value = "models")]
    models: PathBuf,
}

impl LiveArgs {
    /// The arguments of the `train` of the node's retrains but its `--at`, which the launcher gives
    /// (`Launcher::process`): the node's strategy, config, catalog and store, the train reading the
    /// commission, balance and latency of its dataset runs from the config (`[model] dataset`). A
    /// live node is the one node of its process while it runs, and a train replays the dataset days
    /// it lacks through a node of its own, so each retrain is a process.
    fn train_args(&self) -> Vec<OsString> {
        let strategy = self
            .strategy
            .to_possible_value()
            .expect("every strategy has a name");
        let mut args: Vec<OsString> = vec!["train".into(), strategy.get_name().into()];
        args.extend([
            "--config".into(),
            self.config.clone().into_os_string(),
            "--catalog".into(),
            self.catalog.clone().into_os_string(),
        ]);
        args.extend(["--models".into(), self.models.clone().into_os_string()]);
        args
    }

    /// The recording of the live node: staged in `--staging`.
    fn recording(&self) -> Recording {
        Recording {
            staging: self.staging.clone(),
        }
    }
}

/// Nautilus' own parser: an RFC 3339 timestamp, a date taken as midnight UTC, or Unix
/// nanoseconds or seconds.
fn parse_time(s: &str) -> Result<Timestamp, String> {
    s.parse::<UnixNanos>()
        .map(Timestamp::from)
        .map_err(|e| e.to_string())
}

/// An amount and a currency code, for example "3 EUR". Explicit, since clap would otherwise
/// pick `Money: From<&str>`, which panics.
fn parse_money(s: &str) -> Result<Money, String> {
    s.parse()
}

/// Reads the config of the strategy of part `P` from `path` and validates it.
fn load_config<P: Part>(path: &Path) -> anyhow::Result<StandardConfig<P>> {
    let read = || StandardConfig::load(&fs::read_to_string(path)?);
    read().with_context(|| format!("loading the config {}", path.display()))
}

/// The data requirements of `strategy`'s config at `path`.
fn data_requirements(strategy: StrategyName, path: &Path) -> anyhow::Result<DataRequirements> {
    strategy.visit(Requirements(path))
}

/// The visitor reading the data requirements of a strategy's config at its path.
struct Requirements<'a>(&'a Path);

impl Visitor for Requirements<'_> {
    type Output = anyhow::Result<DataRequirements>;

    fn plain<S: PlainStrategy>(self) -> Self::Output {
        Ok(load_config::<S::Params>(self.0)?.data_requirements())
    }

    fn model<S: ModelStrategy>(self) -> Self::Output {
        Ok(load_config::<S::Params>(self.0)?.data_requirements())
    }
}

/// The logger config from `NAUTILUS_LOG`: the default when it is unset, an error when it is
/// malformed.
fn logger_config(spec: Result<String, std::env::VarError>) -> anyhow::Result<LoggerConfig> {
    match spec {
        Err(std::env::VarError::NotPresent) => Ok(LoggerConfig::default()),
        spec => LoggerConfig::from_spec(&spec?).context("parsing NAUTILUS_LOG"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Before parsing, since arguments fall back on the environment; a variable already set wins
    // over the file.
    dotenvy::dotenv().ok();
    // Once, before any config loads, on every command: a config's custom series are checked
    // against Nautilus' registry, and every catalog and client of custom data decodes and encodes
    // it by its registration.
    venues::register_custom_data();
    match Cli::parse().command {
        Command::FillCatalog(args) => run_fill_catalog(args).await,
        Command::Backtest(args) => run_backtest(&args),
        Command::Live(args) => run_live(*args).await,
        Command::Train(args) => run_train(&args),
    }
}

/// Fills the catalog with the instruments and the series the strategy reads, each from its
/// history source (`venues::fill_history`).
async fn run_fill_catalog(args: FillCatalogArgs) -> anyhow::Result<()> {
    let (start, end) = args.run.window()?;
    let requirements = data_requirements(args.run.strategy, &args.run.config)?;
    // A fill may run no node (one whose catalog lacks nothing runs none), so no node installs
    // Nautilus' logger for it: install it here, as Nautilus' logging guide says for a run without
    // a kernel (the fill's node keeps it), so the `log` output of this crate and Nautilus, and the
    // adapters' `tracing` warnings (why a contract did not resolve), reach the console. Configured
    // by NAUTILUS_LOG and RUST_LOG.
    let _log_guard = init_logging(
        TraderId::default(),
        UUID4::new(),
        logger_config(std::env::var("NAUTILUS_LOG"))?,
        FileWriterConfig::default(),
    )?;
    init_tracing()?;
    let catalog_dir = &args.run.catalog;
    fs::create_dir_all(catalog_dir)
        .with_context(|| format!("creating the catalog {}", catalog_dir.display()))?;
    venues::fill_history(&requirements, start, end, &args.venues, catalog_dir).await
}

/// Builds the strategy from its config, runs it over the window of the catalog and prints the
/// results to stdout, refusing first a balance whose currency the config's conversions do not
/// reach a traded instrument's P&L currency from (`Backtest::refuse_balance`).
///
/// A strategy trading on a model trades on the models in the store, which `train` published
/// (`model::walk`): refused if the store holds none to trade on at its start, its dataset built
/// over the span, in this process, on the backtest's catalog, at the commission, balance and
/// latency of the config's `[model] dataset`. The out-of-sample scores of its models follow the
/// results, after the costs of the run and of its models' dataset runs.
fn run_backtest(args: &BacktestArgs) -> anyhow::Result<()> {
    let (start, end) = args.run.window()?;
    // The node installs Nautilus' logger from its engine config: pass the NAUTILUS_LOG config
    // through, rather than install a logger here.
    let logging = logger_config(std::env::var("NAUTILUS_LOG"))?;
    let dataset_logging = logging.clone();
    let backtest = |requirements: DataRequirements, pairs: &[InstrumentId]| Backtest {
        catalog: args.run.catalog.clone(),
        bar_types: requirements.bar_types,
        start,
        end,
        ticks: requirements.ticks,
        custom: requirements.custom,
        commission: args.commission.commission(),
        latency: args.latency,
        pairs: pairs.to_vec(),
        balance: args.balance,
        logging,
    };
    args.run.strategy.visit(Backtesting {
        args,
        dataset_logging,
        backtest,
    })
}

/// The visitor backtesting a strategy: `backtest` builds its `Backtest` of its data
/// requirements and the pairs of its `[trade] conversions` it does not trade; a model
/// strategy's dataset runs log as `dataset_logging` says (`backtest_on_models`).
struct Backtesting<'a, B> {
    args: &'a BacktestArgs,
    dataset_logging: LoggerConfig,
    backtest: B,
}

impl<B: FnOnce(DataRequirements, &[InstrumentId]) -> Backtest> Visitor for Backtesting<'_, B> {
    type Output = anyhow::Result<()>;

    fn plain<S: PlainStrategy>(self) -> Self::Output {
        let config = load_config::<S::Params>(&self.args.run.config)?;
        let backtest = (self.backtest)(config.data_requirements(), &config.trade.untraded_pairs());
        backtest.refuse_balance(&config.trade)?;
        let outcome = backtest.run(S::backtest(config)?)?;
        print!("{}", backtest::report(&outcome));
        Ok(())
    }

    fn model<S: ModelStrategy>(self) -> Self::Output {
        backtest_on_models::<S>(self.args, self.dataset_logging, self.backtest)
    }
}

/// The backtest of `args` of the model strategy `S`, its `Backtest` of its data requirements
/// and the pairs of its `[trade] conversions` it does not trade `backtest`, on the models in
/// its store: refused, before any replay, if the store holds no model to trade on at its start
/// (`WalkForward::untrained`), naming the retrain time there and the `train` that fills it;
/// else its dataset built over the span, for the scores, then run, its results printed and the
/// out-of-sample scores of its models after them. The dataset build and the run are one after
/// another in this process, each Nautilus node, a dataset run's (`DatasetRun`, logging as
/// `logging` says) or the backtest's, disposed before the next starts, which Nautilus supports.
/// The days built are printed.
fn backtest_on_models<S: ModelStrategy>(
    args: &BacktestArgs,
    logging: LoggerConfig,
    backtest: impl FnOnce(DataRequirements, &[InstrumentId]) -> Backtest,
) -> anyhow::Result<()> {
    let (start, end) = args.run.window()?;
    let config = load_config::<S::Params>(&args.run.config)?;
    let backtest = backtest(config.data_requirements(), &config.trade.untraded_pairs());
    backtest.refuse_balance(&config.trade)?;
    let (catalog, models) = (&args.run.catalog, &args.models);
    let run = DatasetRun {
        catalog: catalog.clone(),
        logging,
    };
    run.refuse_balance::<S>(&config)?;
    let walk = S::walk_forward(&config, catalog, models)?;
    if let Some(at) = walk.untrained(start, end)? {
        let train = format!(
            "helix-trader train {} --config {} --catalog {} --models {}",
            strategy_name(args.run.strategy),
            args.run.config.display(),
            catalog.display(),
            models.display()
        );
        let span = match config.model().retrain {
            Retrain::Every(_) => format!(", or over its span, `{train} --from {start} --to {end}`"),
            Retrain::Never => String::new(),
        };
        anyhow::bail!(
            "the store {} holds no model to trade on at the backtest's start {start}, and a \
             backtest trains none: train at its retrain time {at} first, `{train} --at {at}`{span}",
            models.display()
        );
    }
    let dataset = S::dataset(&config, catalog)?;
    let (first, last) = dataset::days(start, end);
    let built = dataset.build(first, last, |from, to| run.replay::<S>(&config, from, to))?;
    if let (Some(from), Some(to)) = (built.first(), built.last()) {
        println!("built {} days of the dataset, {from} to {to}", built.len());
    }
    let mode = Mode::Backtest {
        models: models.clone(),
    };
    let trained_at = config.model().dataset;
    let outcome = backtest.run(S::new(config, mode)?)?;
    print!("{}", backtest::report(&outcome));
    let (commission, balance) = (args.commission.commission(), args.balance);
    match args.latency {
        None => {
            println!(
                "Run at: commission {commission}, balance {balance} (--commission, --balance)"
            );
        }
        Some(latency) => println!(
            "Run at: commission {commission}, balance {balance}, latency {latency} (--commission, \
             --balance, --latency)"
        ),
    }
    println!("Models trained at: {trained_at} ([model] dataset)");
    print!("{}", walk.scores(start, end)?);
    Ok(())
}

/// Builds the strategy from its config and runs it live until Ctrl-C. A strategy trading on a
/// model retrains, if ever, in a child process, this binary's `train` (see
/// `LiveArgs::train_args`).
async fn run_live(args: LiveArgs) -> anyhow::Result<()> {
    let live = Live {
        trader_id: args.trader_id,
        venues: args.venues.clone(),
        logging: logger_config(std::env::var("NAUTILUS_LOG"))?,
        catalog: args.catalog.clone(),
        recording: args.recording(),
    };
    args.strategy
        .visit(Running {
            args: &args,
            live: &live,
        })
        .await
}

/// The visitor running a strategy on a live node until Ctrl-C: its output, the run, is boxed, a
/// future of each kind's own type.
struct Running<'a> {
    args: &'a LiveArgs,
    live: &'a Live,
}

impl<'a> Visitor for Running<'a> {
    type Output = Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'a>>;

    fn plain<S: PlainStrategy>(self) -> Self::Output {
        Box::pin(async move {
            let config = load_config::<S::Params>(&self.args.config)?;
            let requirements = config.data_requirements();
            let strategy = S::live(config)?;
            self.live
                .run(&requirements, &strategy.warmup(), strategy, None)
                .await
        })
    }

    fn model<S: ModelStrategy>(self) -> Self::Output {
        Box::pin(live_on_models::<S>(self.args, self.live))
    }
}

/// Runs the model strategy `S` of `args` on `live` until Ctrl-C, on the models of the store its
/// config keys, beside the actor of its retrains if it is retrained (`RetrainActor`), which
/// lists that store's model sets as it starts, each retrain in a child process
/// (`LiveArgs::train_args`) on the node's catalog.
///
/// Refuses first, before any node is built, a catalog without the traded instruments or the
/// pairs of `[trade] conversions`, which its models and retrains are built from
/// (`catalog::instruments`), and a dataset balance (`[model] dataset`) whose currency those
/// pairs do not reach a traded instrument's P&L currency from (`backtest::refuse_balance`), as
/// `backtest` and `train` refuse it. Warns, naming the store, when it holds no model to trade
/// on yet (`ModelStore::trades_by`): the strategy then makes no trade until one is published
/// there.
async fn live_on_models<S: ModelStrategy>(args: &LiveArgs, live: &Live) -> anyhow::Result<()> {
    let config = load_config::<S::Params>(&args.config)?;
    let program = std::env::current_exe().context("finding this binary to retrain")?;
    let traded = catalog::instruments(&args.catalog, &config.trade.instrument_ids()).context(
        "a model strategy runs live only on a --catalog holding the instruments it trades, \
         which its models and retrains are built from",
    )?;
    let pairs = catalog::instruments(&args.catalog, config.trade.conversions.pairs()).context(
        "a model strategy runs live only on a --catalog holding the pairs of [trade] \
         conversions, which its retrains convert the dataset balance through",
    )?;
    backtest::refuse_balance(config.model().dataset.balance, &traded, &pairs)
        .context(DATASET_BALANCE)?;
    let models = S::blueprint(&config)?.model_store(&args.models)?;
    if !models.trades_by(Timestamp::from(get_atomic_clock_realtime().get_time_ns()))? {
        eprintln!(
            "warning: no model to trade on yet in {}: the strategy makes no trade until one is \
             published there, by `train` or by a retrain",
            models.directory().display()
        );
    }
    let requirements = config.data_requirements();
    let retraining = RetrainActor::new(
        config.strategy_id(),
        config.model().retrain,
        args.catalog.clone(),
        requirements.clone(),
        models,
        Launcher::process(program, args.train_args()),
    );
    let mode = Mode::Live {
        models: args.models.clone(),
    };
    let strategy = S::new(config, mode)?;
    live.run(&requirements, &strategy.warmup(), strategy, retraining)
        .await
}

/// What a refused dataset balance is.
const DATASET_BALANCE: &str = "the dataset balance ([model] dataset), which a model's targets and \
                               the decisions' payoff features are sized at";

/// The dataset runs of a process: the strategy over the catalog in `Mode::Dataset`, on hedging
/// venues (`model::targets`), on the venue its config declares (`[model] dataset`): its
/// commission the venues' and the targets', its balance the account's and the equity the
/// targets' reference stake is of, which the runs' trades leave whole, margining nothing
/// (`Backtest::run_dataset`), and its latency, if any, the venues'; each replay a node of its
/// own, disposed at its end.
struct DatasetRun {
    catalog: PathBuf,
    logging: LoggerConfig,
}

impl DatasetRun {
    /// Refuses the runs of the strategy `config` describes, before any, unless the pairs of its
    /// `[trade] conversions` reach the P&L currency of each instrument it trades from their
    /// balance's, all as their catalog defines them (`catalog::instruments`,
    /// `backtest::refuse_balance`, the check of a backtest's balance): a target's trades are sized
    /// on the balance converted through them.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be read, a traded instrument or a pair is not in it,
    /// or the pairs do not reach a traded instrument's P&L currency from the balance's.
    fn refuse_balance<S: ModelStrategy>(
        &self,
        config: &StandardConfig<S::Params>,
    ) -> anyhow::Result<()> {
        let traded = catalog::instruments(&self.catalog, &config.trade.instrument_ids())
            .context("reading the instruments traded")?;
        let pairs = catalog::instruments(&self.catalog, config.trade.conversions.pairs())
            .context("reading the pairs of [trade] conversions")?;
        backtest::refuse_balance(config.model().dataset.balance, &traded, &pairs)
            .context(DATASET_BALANCE)
    }

    /// The rows the model strategy `S` of `config` settles over `[start, end]`.
    ///
    /// # Errors
    ///
    /// Returns an error if the strategy cannot be built, the run fails, or a target's order was
    /// denied (`SettledRows::take`).
    fn replay<S: ModelStrategy>(
        &self,
        config: &StandardConfig<S::Params>,
        start: Timestamp,
        end: Timestamp,
    ) -> anyhow::Result<Vec<TargetRow>> {
        let requirements = config.data_requirements();
        let rows = SettledRows::default();
        let mode = Mode::Dataset { rows: rows.clone() };
        let venue = config.model().dataset;
        let backtest = Backtest {
            catalog: self.catalog.clone(),
            bar_types: requirements.bar_types,
            ticks: requirements.ticks,
            custom: requirements.custom,
            start,
            end,
            commission: venue.commission,
            latency: venue.latency,
            pairs: config.trade.untraded_pairs(),
            balance: venue.balance,
            logging: self.logging.clone(),
        };
        backtest.run_dataset(S::new(config.clone(), mode)?)?;
        rows.take()
    }
}

/// Prints the report of the train at `at`, which did `trained`, as it ends (`report::Report`),
/// its process figures read by `meter` then.
fn print_report(meter: &mut Meter, at: Timestamp, trained: &Trained) {
    let usage = meter.lap();
    print!("{}", Report { at, trained, usage });
}

/// Prints the report of the train at `at`, which did `trained` (`print_report`). A train that
/// failed is an error with its reason, so the command fails and the live node that started it
/// knows why (`model::launcher`).
///
/// # Errors
///
/// Returns an error if the train failed.
fn reported(mut meter: Meter, at: Timestamp, trained: &Trained) -> anyhow::Result<()> {
    print_report(&mut meter, at, trained);
    anyhow::ensure!(trained.succeeded(), "{}", trained.outcome);
    Ok(())
}

/// Prints the report of each of `trains`, a train over a span, at its retrain time, as it comes
/// (`print_report`): a train that failed does not stop the next. Fails once all are done if any
/// failed, listing each such retrain time with its train's reason.
///
/// # Errors
///
/// Returns an error if a train failed.
fn reported_over(
    mut meter: Meter,
    trains: impl Iterator<Item = (Timestamp, Trained)>,
) -> anyhow::Result<()> {
    let (mut count, mut failed) = (0, vec![]);
    for (at, trained) in trains {
        count += 1;
        print_report(&mut meter, at, &trained);
        if !trained.succeeded() {
            failed.push(format!("  {at}: {}", trained.outcome));
        }
    }
    anyhow::ensure!(
        failed.is_empty(),
        "the trains at {} of the span's {count} retrain times failed:\n{}",
        failed.len(),
        failed.join("\n")
    );
    Ok(())
}

/// Trains the strategy's models at `--at`, or over `--from` to `--to`, and reports each train
/// (see `reported`), its process figures from the command's start: the days its dataset lacks
/// are replayed by a dataset run (`DatasetRun`).
fn run_train(args: &TrainArgs) -> anyhow::Result<()> {
    let meter = Meter::start();
    let trains = args.trains()?;
    let run = DatasetRun {
        catalog: args.catalog.clone(),
        logging: logger_config(std::env::var("NAUTILUS_LOG"))?,
    };
    args.strategy.visit(Training {
        args,
        trains,
        run: &run,
        meter,
    })
}

/// The visitor training a strategy's model (`train`): a strategy trading on no model has none.
struct Training<'a> {
    args: &'a TrainArgs,
    trains: Trains,
    run: &'a DatasetRun,
    meter: Meter,
}

impl Visitor for Training<'_> {
    type Output = anyhow::Result<()>;

    fn plain<S: PlainStrategy>(self) -> Self::Output {
        let name = strategy_name(self.args.strategy);
        anyhow::bail!("the {name} strategy trades on no model")
    }

    fn model<S: ModelStrategy>(self) -> Self::Output {
        train::<S>(self.args, self.trains, self.run, self.meter)
    }
}

/// Trains the model of the model strategy `S` of `args` as `trains` says, at its one time or at
/// each retrain time of a span in time order (`Training::retrain_times`), and reports each
/// train, its process figures read by `meter`, the days its dataset lacks replayed by `run`,
/// its dataset balance refused first if the pairs of `[trade] conversions` do not reach the
/// traded instrument's P&L currency from it (`DatasetRun::refuse_balance`).
fn train<S: ModelStrategy>(
    args: &TrainArgs,
    trains: Trains,
    run: &DatasetRun,
    meter: Meter,
) -> anyhow::Result<()> {
    let config = load_config::<S::Params>(&args.config)?;
    run.refuse_balance::<S>(&config)?;
    let training = S::training(&config, &args.catalog, &args.models)?;
    let replay = |start, end| run.replay::<S>(&config, start, end);
    match trains {
        Trains::At(at) => reported(meter, at, &training.train_at(at, replay)),
        Trains::Over { from, to } => {
            let times = training
                .retrain_times(from, to)
                .context("training over --from to --to; train it --at its one time")?;
            reported_over(
                meter,
                times
                    .into_iter()
                    .map(|at| (at, training.train_at(at, replay))),
            )
        }
    }
}

fn strategy_name(strategy: StrategyName) -> String {
    strategy
        .to_possible_value()
        .expect("every strategy has a name")
        .get_name()
        .to_string()
}

#[cfg(test)]
mod tests {

    use helix_trader::model::train::{Failure, Outcome};
    use jiff::SignedDuration;
    use rstest::rstest;

    use super::*;

    fn parse_command(subcommand: &str, args: &[&str]) -> Result<Command, clap::Error> {
        let argv = ["helix-trader", subcommand]
            .into_iter()
            .chain(args.iter().copied());
        Cli::try_parse_from(argv).map(|cli| cli.command)
    }

    /// The arguments of `command`, a fill-catalog.
    fn fill_catalog_args(command: Command) -> FillCatalogArgs {
        match command {
            Command::FillCatalog(args) => args,
            command => panic!("{command:?}"),
        }
    }

    fn parse(args: &[&str]) -> Result<FillCatalogArgs, clap::Error> {
        parse_command("fill-catalog", args).map(fill_catalog_args)
    }

    /// The arguments of `command`, a backtest.
    fn backtest_args(command: Command) -> BacktestArgs {
        match command {
            Command::Backtest(args) => *args,
            command => panic!("{command:?}"),
        }
    }

    fn parse_backtest(args: &[&str]) -> Result<BacktestArgs, clap::Error> {
        parse_command("backtest", args).map(backtest_args)
    }

    const REQUIRED: [&str; 7] = [
        "ema",
        "--config",
        "ema.toml",
        "--start",
        "2024-01-02",
        "--end",
        "2024-03-01T12:30:00Z",
    ];

    /// `REQUIRED` and the costs a backtest requires.
    fn backtest_required() -> Vec<&'static str> {
        let mut argv = REQUIRED.to_vec();
        argv.extend(["--commission", "3 EUR", "--balance", "10000 EUR"]);
        argv
    }

    /// Its IB client id is the venues' (`HistoryVenueArgs`).
    #[rstest]
    fn parses_fill_catalog_over_a_window_of_the_catalog() {
        let args = parse(&REQUIRED).unwrap();
        assert_eq!(args.run.strategy, StrategyName::Ema);
        assert_eq!(args.run.config, PathBuf::from("ema.toml"));
        assert_eq!(
            args.run.start,
            "2024-01-02T00:00:00Z".parse::<Timestamp>().unwrap()
        );
        assert_eq!(
            args.run.end,
            "2024-03-01T12:30:00Z".parse::<Timestamp>().unwrap()
        );
        assert_eq!(args.run.catalog, PathBuf::from("catalog"));
    }

    #[rstest]
    fn fill_catalog_takes_the_ib_client_id() {
        let mut argv = REQUIRED.to_vec();
        argv.extend(["--client-id", "12"]);
        assert!(parse(&argv).is_ok());
    }

    #[rstest]
    fn parses_a_backtest_with_its_costs() {
        let args = parse_backtest(&backtest_required()).unwrap();
        assert_eq!(args.run.strategy, StrategyName::Ema);
        assert_eq!(args.run.config, PathBuf::from("ema.toml"));
        assert_eq!(
            args.run.end,
            "2024-03-01T12:30:00Z".parse::<Timestamp>().unwrap()
        );
        assert_eq!(args.run.catalog, PathBuf::from("catalog"));
        assert_eq!(
            args.commission.commission(),
            Commission::PerOrder(Money::from("3 EUR"))
        );
        assert_eq!(args.balance, Money::from("10000 EUR"));
    }

    /// `REQUIRED`, the balance and `commission`.
    fn backtest_with<'a>(commission: &[&'a str]) -> Vec<&'a str> {
        let mut argv = REQUIRED.to_vec();
        argv.extend(["--balance", "10000 EUR"]);
        argv.extend(commission);
        argv
    }

    #[rstest]
    #[case::an_amount(["--commission", "3 EUR"])]
    #[case::a_rate(["--commission-rate", "0.0005"])]
    #[case::an_amount_per_quantity(["--commission-per-quantity", "0.035 EUR"])]
    fn a_backtest_takes_a_commission_of_any_of_the_three_kinds(#[case] one: [&str; 2]) {
        assert!(parse_backtest(&backtest_with(&one)).is_ok());
    }

    /// Pepperstone's gold: 3.5 USD per 100 oz, so 0.035 USD an ounce, not rounded to 0.04 USD.
    #[rstest]
    fn a_commission_per_quantity_is_its_exact_amount_per_unit() {
        let args =
            parse_backtest(&backtest_with(&["--commission-per-quantity", "0.035 USD"])).unwrap();
        let exact = Money::from_str_exact("0.035 USD").unwrap();
        assert_eq!(args.commission.commission(), Commission::PerQuantity(exact));
    }

    /// 150 ms, as jiff reads a duration: each command reaches the simulated account's venues 150 ms
    /// after it is sent.
    #[rstest]
    fn a_backtest_takes_the_latency_of_its_venues_as_a_duration() {
        let mut argv = backtest_required();
        argv.extend(["--latency", "150ms"]);
        let at_150ms = Latency::try_from(SignedDuration::from_millis(150)).unwrap();

        assert_eq!(parse_backtest(&argv).unwrap().latency, Some(at_150ms));
        let last = argv.len() - 1;
        argv[last] = "PT0.15S";
        assert_eq!(parse_backtest(&argv).unwrap().latency, Some(at_150ms));
    }

    /// Nautilus' default: each command reaches the venue as it is sent.
    #[rstest]
    fn a_backtest_without_a_latency_has_none() {
        assert_eq!(parse_backtest(&backtest_required()).unwrap().latency, None);
    }

    /// The arguments of `command`, a live run.
    fn live_args(command: Command) -> LiveArgs {
        match command {
            Command::Live(args) => *args,
            command => panic!("{command:?}"),
        }
    }

    fn parse_live(args: &[&str]) -> Result<LiveArgs, clap::Error> {
        parse_command("live", args).map(live_args)
    }

    /// Its IB account and client id are the venues' (`LiveVenueArgs`). It runs as trader HELIX-001,
    /// staging in `staging` and reading the catalog `catalog`, unless given another.
    #[rstest]
    #[case::by_default(&[], "HELIX-001", "staging", "catalog")]
    #[case::as_another_trader(&["--trader-id", "HELIX-002"], "HELIX-002", "staging", "catalog")]
    #[case::staging_elsewhere(&["--staging", "/data/staging"], "HELIX-001", "/data/staging", "catalog")]
    #[case::on_another_catalog(&["--catalog", "/data/catalog"], "HELIX-001", "staging", "/data/catalog")]
    fn parses_live_with_its_strategy_config_and_catalog(
        #[case] given: &[&str],
        #[case] trader_id: &str,
        #[case] staging: &str,
        #[case] catalog: &str,
    ) {
        let argv = [&["ema", "--config", "ema.toml"][..], given].concat();
        let args = parse_live(&argv).unwrap();
        assert_eq!(args.strategy, StrategyName::Ema);
        assert_eq!(args.config, PathBuf::from("ema.toml"));
        assert_eq!(args.trader_id, TraderId::from(trader_id));
        assert_eq!(args.staging, PathBuf::from(staging));
        assert_eq!(args.catalog, PathBuf::from(catalog));
    }

    /// Unset, Nautilus' default; not Unicode, an error.
    #[rstest]
    fn logger_config_defaults_when_unset_and_rejects_a_spec_not_unicode() {
        assert_eq!(
            logger_config(Err(std::env::VarError::NotPresent)).unwrap(),
            LoggerConfig::default()
        );
        let not_unicode = std::ffi::OsString::from("x");
        assert!(logger_config(Err(std::env::VarError::NotUnicode(not_unicode))).is_err());
    }

    /// The arguments of `command`, a train.
    fn train_args(command: Command) -> TrainArgs {
        match command {
            Command::Train(args) => *args,
            command => panic!("{command:?}"),
        }
    }

    fn parse_train(args: &[&str]) -> Result<TrainArgs, clap::Error> {
        parse_command("train", args).map(train_args)
    }

    /// The arguments a train requires.
    const TRAIN: [&str; 5] = [
        "model_ema",
        "--config",
        "model_ema.toml",
        "--at",
        "2026-10-23",
    ];

    #[rstest]
    fn parses_a_train_at_a_time_into_the_models_store() {
        let args = parse_train(&TRAIN).unwrap();
        assert_eq!(args.strategy, StrategyName::ModelEma);
        assert_eq!(args.config, PathBuf::from("model_ema.toml"));
        assert_eq!(
            args.trains().unwrap(),
            Trains::At("2026-10-23T00:00:00Z".parse().unwrap())
        );
        assert_eq!(args.catalog, PathBuf::from("catalog"));
        assert_eq!(args.models, PathBuf::from("models"));
    }

    /// The arguments of a train over the span from `from` to `to`.
    fn parse_train_over(from: &str, to: &str) -> Result<TrainArgs, clap::Error> {
        let argv = [&TRAIN[..3], &["--from", from, "--to", to]].concat();
        parse_train(&argv)
    }

    #[rstest]
    fn parses_a_train_over_a_span() {
        let args = parse_train_over("2026-10-21", "2026-10-23T12:00:00Z").unwrap();

        assert_eq!(
            args.trains().unwrap(),
            Trains::Over {
                from: "2026-10-21T00:00:00Z".parse().unwrap(),
                to: "2026-10-23T12:00:00Z".parse().unwrap(),
            }
        );
    }

    /// A span ending at or before its start trains at no time: refused, naming both ends.
    #[rstest]
    #[case::ending_before_it_starts("2026-10-23", "2026-10-21")]
    #[case::ending_where_it_starts("2026-10-23", "2026-10-23")]
    fn a_train_over_an_empty_span_is_refused(#[case] from: &str, #[case] to: &str) {
        let err = parse_train_over(from, to)
            .unwrap()
            .trains()
            .unwrap_err()
            .to_string();

        assert!(
            err.contains(&format!("{from}T00:00:00Z")) && err.contains(&format!("{to}T00:00:00Z")),
            "{err}"
        );
    }

    #[rstest]
    fn backtest_and_live_read_models_from_models_unless_told_otherwise() {
        assert_eq!(
            parse_backtest(&backtest_required()).unwrap().models,
            PathBuf::from("models")
        );
        let mut argv = backtest_required();
        argv.extend(["--models", "/data/models"]);
        assert_eq!(
            parse_backtest(&argv).unwrap().models,
            PathBuf::from("/data/models")
        );
        let args = parse_live(&["model_ema", "--config", "model_ema.toml"]).unwrap();
        assert_eq!(args.models, PathBuf::from("models"));
    }

    /// A train that failed fails the command with its reason; one that published, or found its
    /// model set already published, succeeds.
    #[rstest]
    fn a_failed_train_fails_with_the_reason() {
        let t: Timestamp = "2026-10-23T00:00:00Z".parse().unwrap();
        let kept = Outcome::Failed(Failure::Error(
            "34 rows in the fit slice, fewer than min_rows 35".to_string(),
        ));
        let published = Outcome::Published {
            model_set: PathBuf::from("models/2026-10-23T00:00:00Z"),
        };
        let already = Outcome::AlreadyPublished;

        let err = reported(Meter::start(), t, &kept.into())
            .unwrap_err()
            .to_string();

        assert!(err.contains("34") && err.contains("35"), "{err}");
        assert!(reported(Meter::start(), t, &published.into()).is_ok());
        assert!(reported(Meter::start(), t, &already.into()).is_ok());
    }

    /// The arguments of a `model_ema` live node.
    const LIVE: [&str; 3] = ["model_ema", "--config", "model_ema.toml"];

    /// The `train` the live node of `live` starts, given its `--at`, parsed as this binary parses
    /// it.
    fn started_train(live: &LiveArgs) -> TrainArgs {
        let argv: Vec<String> = live
            .train_args()
            .into_iter()
            .map(|arg| arg.into_string().unwrap())
            .collect();
        let (subcommand, args) = argv.split_first().unwrap();
        assert_eq!(subcommand, "train");
        let mut args: Vec<&str> = args.iter().map(String::as_str).collect();
        args.extend(["--at", "2026-10-23"]);
        parse_train(&args).unwrap()
    }

    /// A live node's retrain is a `train` on the node's strategy, config, catalog and store, which
    /// reads its dataset runs' commission and balance from the config.
    #[rstest]
    fn a_live_nodes_retrain_parses_as_train_on_its_files_and_store() {
        let mut argv = LIVE.to_vec();
        argv.extend(["--catalog", "/data/catalog", "--models", "/data/models"]);

        let train = started_train(&parse_live(&argv).unwrap());

        assert_eq!(train.strategy, StrategyName::ModelEma);
        assert_eq!(train.config, PathBuf::from("model_ema.toml"));
        assert_eq!(train.catalog, PathBuf::from("/data/catalog"));
        assert_eq!(train.models, PathBuf::from("/data/models"));
    }

    /// Every subcommand takes the strategy first, by its command-line name.
    #[rstest]
    fn every_subcommand_takes_the_strategy_by_name() {
        let backtest = backtest_required();
        let live = ["ema", "--config", "ema.toml"];
        for (subcommand, argv) in [
            ("fill-catalog", &REQUIRED[..]),
            ("backtest", &backtest[..]),
            ("live", &live[..]),
            ("train", &TRAIN[..]),
        ] {
            for name in ["ema", "model_ema"] {
                let named = [&[name][..], &argv[1..]].concat();
                assert!(
                    parse_command(subcommand, &named).is_ok(),
                    "{subcommand} {name}"
                );
            }
        }
    }
}
