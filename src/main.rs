//! Command line: one subcommand per process (fill-catalog, backtest, live,
//! train). Nautilus runs no two nodes of a process at once, so a live
//! node's retrain is a process of its own; a backtest runs its nodes one
//! after another, each disposed before the next. Every subcommand takes
//! the strategy name and its TOML config: the instruments and the warmup
//! come from the config's data requirements. A strategy trading on a
//! model runs through `model::ModelStrategy`, each subcommand generic
//! over it.

use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    pin::Pin,
    rc::Rc,
    str::FromStr,
};

use anyhow::{Context, bail, ensure};
use clap::{Args, Parser, Subcommand, ValueEnum};
use helix_trader::{
    backtest::{self, Backtest, Commission},
    live::{Live, Recording},
    model::{
        ModelStrategy, dataset,
        handle::{Mode, SettledRows},
        launcher::{Launcher, RetrainActor},
        targets::TargetRow,
        train::Trained,
    },
    strategy::{DataRequirements, Part, PlainStrategy, StandardConfig, StrategyName, Visitor},
    venues::{self, HistoryVenueArgs, LiveVenueArgs},
};
use jiff::Timestamp;
use nautilus_common::logging::{
    bridge::init_tracing, init_logging, logger::LoggerConfig, writer::FileWriterConfig,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{enums::OmsType, identifiers::TraderId, types::Money};
use rust_decimal::Decimal;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Fills the Nautilus catalog with the strategy's history from OKX
    /// (OKX instruments), LSE (LSEDGE instruments, and GLBX instruments,
    /// defined by Databento), cTrader (IC Markets' and Pepperstone's
    /// instruments) or Interactive Brokers (the others).
    FillCatalog(FillCatalogArgs),
    /// Backtests the strategy over a window of the catalog and prints the results.
    Backtest(Box<BacktestArgs>),
    /// Runs the strategy on a live node with Interactive Brokers, IC
    /// Markets, Pepperstone or OKX; stops on Ctrl-C.
    Live(Box<LiveArgs>),
    /// Trains the strategy's models at a time, those without one valid
    /// from it, replaying the days its dataset lacks, and prints each
    /// model published or in use, or fails with why a current one was
    /// kept.
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
        ensure!(start < end, "start {start} must be before end {end}");
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
    /// Starting balance of the account, for example "10000 EUR".
    #[arg(long, value_parser = parse_money)]
    balance: Money,
    /// The account's leverage, for example 10: a position locks its value
    /// divided by it, times the instrument's margin. 1 is no leverage. For
    /// venues whose instruments carry margins of 1 (OKX); the cTrader
    /// brokers' carry the broker's own margin, so their backtests keep 1.
    #[arg(long, default_value = "1")]
    leverage: Decimal,
    /// The model store a strategy trading on a model reads its models
    /// from: at each decision, the latest valid then. The backtest first
    /// trains at each retrain time of its span the models the store lacks;
    /// never retrained, at its start alone, and refuses to run if its
    /// sides or its meta then have no model in use there.
    #[arg(long, default_value = "models")]
    models: PathBuf,
    /// Starting balance of the account of the dataset runs that build a
    /// model strategy's dataset before the backtest, large enough that no
    /// target is denied, for example "10000000 USD"; required for a
    /// strategy trading on a model.
    #[arg(long, value_parser = parse_money)]
    dataset_balance: Option<Money>,
}

#[derive(Debug, Args)]
struct TrainArgs {
    /// The strategy whose model is trained.
    strategy: StrategyName,
    /// The strategy's TOML config.
    #[arg(long)]
    config: PathBuf,
    /// The time trained at, the model's valid-from: same format as
    /// --start.
    #[arg(long, value_parser = parse_time)]
    at: Timestamp,
    /// The catalog directory: the data replayed, and the dataset.
    #[arg(long, default_value = "catalog")]
    catalog: PathBuf,
    /// The model store the model is published in.
    #[arg(long, default_value = "models")]
    models: PathBuf,
    /// The costs of the dataset run, whose targets are net of them.
    #[command(flatten)]
    commission: CommissionArgs,
    /// Starting balance of the dataset run's account, large enough that
    /// no target is denied, for example "10000000 USD".
    #[arg(long, value_parser = parse_money)]
    balance: Money,
    /// The dataset run's leverage, as a backtest's.
    #[arg(long, default_value = "1")]
    leverage: Decimal,
}

impl BacktestArgs {
    /// The venue of the dataset runs of the backtest of a strategy trading
    /// on a model: its commission and leverage, and its dataset balance.
    ///
    /// # Errors
    ///
    /// Returns an error if the backtest has no dataset balance.
    fn dataset_venue(&self) -> anyhow::Result<dataset::DatasetVenue> {
        let balance = self
            .dataset_balance
            .context("a backtest of a strategy trading on a model needs --dataset-balance")?;
        Ok(self.commission.venue(balance, self.leverage))
    }
}

/// The commission of a run: exactly one of the flags.
#[derive(Debug, Args)]
#[group(required = true, multiple = false)]
struct CommissionArgs {
    /// Commission charged once per order, for example "3 EUR".
    #[arg(long, value_parser = parse_commission)]
    commission: Option<Commission>,
    /// Commission rate on the value of each fill, with no minimum, for
    /// example 0.0005 (0.05%).
    #[arg(long, value_parser = parse_commission_rate)]
    commission_rate: Option<Commission>,
    /// Commission charged on each unit of the quantity of each fill, each
    /// fill's charge rounded to the currency's precision, for example
    /// "0.035 USD" (3.5 USD per 100 oz of gold, Pepperstone's).
    #[arg(long, value_parser = parse_commission_per_quantity)]
    commission_per_quantity: Option<Commission>,
}

impl CommissionArgs {
    /// The commission of the one flag given: the one source of the run's
    /// costs, its fee model and its targets' (`backtest::Commission`).
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

    /// The venue of a dataset run at this commission, `balance` and
    /// `leverage`.
    fn venue(&self, balance: Money, leverage: Decimal) -> dataset::DatasetVenue {
        dataset::DatasetVenue {
            commission: self.commission(),
            balance,
            leverage,
        }
    }
}

/// The flag of `commission` and its value normalized
/// (`Commission::normalized`), as `train` takes them.
fn commission_args(commission: &Commission) -> [String; 2] {
    let flag = match commission {
        Commission::PerOrder(_) => "--commission",
        Commission::Rate(_) => "--commission-rate",
        Commission::PerQuantity(_) => "--commission-per-quantity",
    };
    [flag.to_string(), commission.normalized()]
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
    /// The leverage the run expects, as in a backtest. OKX applies the
    /// account's, set by hand per instrument: the start refuses an OKX
    /// instrument at another one.
    #[arg(long, default_value = "1")]
    leverage: Decimal,
    /// The node's trader: its cache in Redis is the trader's, which each run
    /// loads. Live processes running at once each need their own, and on
    /// one venue account their own tag, the part after the last hyphen:
    /// Nautilus puts only the tag in client order, order list and position
    /// ids, so `HELIX-001` and `OTHER-001` collide. Parsed by Nautilus'
    /// checked `FromStr`, since clap would otherwise pick
    /// `TraderId: From<&str>`, which panics.
    #[arg(long, default_value = "HELIX-001", value_parser = TraderId::from_str)]
    trader_id: TraderId,
    /// The catalog directory, fill-catalog's and the backtests': the warmup
    /// is read from it first, and a cTrader broker's history is written
    /// back to it.
    #[arg(long, default_value = "catalog")]
    catalog: PathBuf,
    /// The directory the node stages the data it streams into the
    /// catalog, outside it; it also keeps each trader's instance id.
    #[arg(long, default_value = "staging")]
    staging: PathBuf,
    /// The model store a strategy trading on a model reads its models
    /// from, at each decision the latest valid then, and its retrains
    /// publish in.
    #[arg(long, default_value = "models")]
    models: PathBuf,
    /// The costs of the dataset runs of the retrains, for a strategy
    /// trading on a model, retrained or not: they key the model store.
    #[command(flatten)]
    retrain: RetrainCosts,
}

/// The costs of the dataset runs of a live node's retrains, as `train`
/// takes them (its `--commission`, `--commission-rate` or
/// `--commission-per-quantity`, and `--balance`), its leverage the node's.
/// Checked as `train` checks them, for the `train` the node starts
/// (`Children::of_live`). Required of a
/// strategy never retrained too: they key the store its models are read
/// from (`dataset::fingerprint`), and the commission is its targets'.
#[derive(Debug, Args)]
struct RetrainCosts {
    /// Commission charged once per order in a retrain, for example "3 USD".
    #[arg(
        long,
        value_parser = parse_commission,
        conflicts_with_all = ["retrain_commission_rate", "retrain_commission_per_quantity"]
    )]
    retrain_commission: Option<Commission>,
    /// Commission rate on the value of each fill in a retrain, for example
    /// 0.00003.
    #[arg(
        long,
        value_parser = parse_commission_rate,
        conflicts_with = "retrain_commission_per_quantity"
    )]
    retrain_commission_rate: Option<Commission>,
    /// Commission charged on each unit of the quantity of each fill in a
    /// retrain, for example "0.035 USD".
    #[arg(long, value_parser = parse_commission_per_quantity)]
    retrain_commission_per_quantity: Option<Commission>,
    /// Starting balance of a retrain's dataset run, large enough that no
    /// target is denied, for example "10000000 USD".
    #[arg(long, value_parser = parse_money)]
    retrain_balance: Option<Money>,
}

impl LiveArgs {
    /// The recording of the live node: staged in `--staging`.
    fn recording(&self) -> Recording {
        Recording {
            staging: self.staging.clone(),
        }
    }
}

impl RetrainCosts {
    /// The commission of the retrains.
    ///
    /// # Errors
    ///
    /// Returns an error if no commission flag was given.
    fn commission(&self) -> anyhow::Result<Commission> {
        [
            self.retrain_commission,
            self.retrain_commission_rate,
            self.retrain_commission_per_quantity,
        ]
        .into_iter()
        .flatten()
        .next()
        .context(
            "the costs of a model strategy's dataset runs key the model store it reads its \
             models from: give their commission (--retrain-commission, \
             --retrain-commission-rate or --retrain-commission-per-quantity)",
        )
    }

    /// The starting balance of the retrains' dataset runs.
    ///
    /// # Errors
    ///
    /// Returns an error if it was not given.
    fn balance(&self) -> anyhow::Result<Money> {
        self.retrain_balance.context(
            "the costs of a model strategy's dataset runs key the model store it reads its \
             models from: give their starting balance (--retrain-balance)",
        )
    }

    /// The venue of the retrains' dataset runs, at the node's `leverage`.
    ///
    /// # Errors
    ///
    /// Returns an error if the commission or the balance was not given.
    fn venue(&self, leverage: Decimal) -> anyhow::Result<dataset::DatasetVenue> {
        Ok(dataset::DatasetVenue {
            commission: self.commission()?,
            balance: self.balance()?,
            leverage,
        })
    }
}

/// Nautilus' own parser: an RFC 3339 timestamp, a date taken as midnight
/// UTC, or Unix nanoseconds or seconds.
fn parse_time(s: &str) -> Result<Timestamp, String> {
    s.parse::<UnixNanos>()
        .map(Timestamp::from)
        .map_err(|e| e.to_string())
}

/// An amount and a currency code, for example "3 EUR". Explicit, since
/// clap would otherwise pick `Money: From<&str>`, which panics.
fn parse_money(s: &str) -> Result<Money, String> {
    s.parse()
}

/// A commission per order: an amount and a currency code, not negative,
/// rounded to the currency's precision.
fn parse_commission(s: &str) -> anyhow::Result<Commission> {
    checked(Commission::PerOrder(
        parse_money(s).map_err(anyhow::Error::msg)?,
    ))
}

/// A commission rate: a decimal, not negative.
fn parse_commission_rate(s: &str) -> anyhow::Result<Commission> {
    checked(Commission::Rate(s.parse()?))
}

/// A commission per unit of quantity: an amount and a currency code, not
/// negative, exact rather than rounded to the currency's precision, so
/// that "0.035 USD" charges 3.50 USD on 100 units.
fn parse_commission_per_quantity(s: &str) -> anyhow::Result<Commission> {
    checked(Commission::PerQuantity(
        Money::from_str_exact(s).map_err(anyhow::Error::msg)?,
    ))
}

/// `commission`, if it has a fee model: not negative.
fn checked(commission: Commission) -> anyhow::Result<Commission> {
    commission.fee_model()?;
    Ok(commission)
}

/// Reads the config of the strategy of part `P` from `path` and validates
/// it.
fn load_config<P: Part>(path: &Path) -> anyhow::Result<StandardConfig<P>> {
    let read = || StandardConfig::load(&fs::read_to_string(path)?);
    read().with_context(|| format!("loading the config {}", path.display()))
}

/// The data requirements of `strategy`'s config at `path`.
fn data_requirements(strategy: StrategyName, path: &Path) -> anyhow::Result<DataRequirements> {
    strategy.visit(Requirements(path))
}

/// The visitor reading the data requirements of a strategy's config at
/// its path.
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

/// The logger config from `NAUTILUS_LOG`: the default when it is unset, an
/// error when it is malformed.
fn logger_config(spec: Result<String, std::env::VarError>) -> anyhow::Result<LoggerConfig> {
    match spec {
        Err(std::env::VarError::NotPresent) => Ok(LoggerConfig::default()),
        spec => LoggerConfig::from_spec(&spec?).context("parsing NAUTILUS_LOG"),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Before parsing, since arguments fall back on the environment; a
    // variable already set wins over the file.
    dotenvy::dotenv().ok();
    match Cli::parse().command {
        Command::FillCatalog(args) => run_fill_catalog(args).await,
        Command::Backtest(args) => run_backtest(*args),
        Command::Live(args) => run_live(*args).await,
        Command::Train(args) => run_train(*args),
    }
}

/// Fills the catalog with the instruments and the series the strategy reads,
/// each from its history source (`venues::fill_history`).
async fn run_fill_catalog(args: FillCatalogArgs) -> anyhow::Result<()> {
    let (start, end) = args.run.window()?;
    let requirements = data_requirements(args.run.strategy, &args.run.config)?;
    // A fill may run no node (one whose catalog lacks nothing runs none),
    // so no node installs Nautilus' logger for it: install it here, as
    // Nautilus' logging guide says for a run without a kernel (the fill's
    // node keeps it), so the `log` output of this crate and Nautilus, and
    // the adapters' `tracing` warnings (why a contract did not resolve),
    // reach the console. Configured by NAUTILUS_LOG and RUST_LOG.
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

/// Builds the strategy from its config, runs it over the window of the
/// catalog and prints the results to stdout. A strategy trading on a
/// model is first trained on its schedule (`model::walk`): its dataset
/// built over the span and its models trained at each train time the
/// store lacks one, in this process, on the backtest's catalog, store
/// and costs and the dataset balance; the out-of-sample scores of its
/// models follow the results.
fn run_backtest(args: BacktestArgs) -> anyhow::Result<()> {
    let (start, end) = args.run.window()?;
    // The node installs Nautilus' logger from its engine config: pass the
    // NAUTILUS_LOG config through, rather than install a logger here.
    let logging = logger_config(std::env::var("NAUTILUS_LOG"))?;
    let dataset_logging = logging.clone();
    // Each simulated venue keeps positions as the real one does.
    let backtest = |requirements: DataRequirements| Backtest {
        catalog: args.run.catalog.clone(),
        bar_types: requirements.bar_types,
        start,
        end,
        ticks: requirements.ticks,
        commission: args.commission.commission(),
        balance: args.balance,
        leverage: args.leverage,
        venue_oms: venues::oms_type,
        logging,
    };
    args.run.strategy.visit(Backtesting {
        args: &args,
        dataset_logging,
        backtest,
    })
}

/// The visitor backtesting a strategy: `backtest` builds its `Backtest`
/// of its data requirements; a model strategy's dataset runs log as
/// `dataset_logging` says (`backtest_on_models`).
struct Backtesting<'a, B> {
    args: &'a BacktestArgs,
    dataset_logging: LoggerConfig,
    backtest: B,
}

impl<B: FnOnce(DataRequirements) -> Backtest> Visitor for Backtesting<'_, B> {
    type Output = anyhow::Result<()>;

    fn plain<S: PlainStrategy>(self) -> Self::Output {
        let config = load_config::<S::Params>(&self.args.run.config)?;
        let outcome = (self.backtest)(config.data_requirements()).run(S::backtest(config)?)?;
        print!("{}", backtest::report(&outcome));
        Ok(())
    }

    fn model<S: ModelStrategy>(self) -> Self::Output {
        backtest_on_models::<S>(self.args, self.dataset_logging, self.backtest)
    }
}

/// The backtest of `args` of the model strategy `S`, its `Backtest` of
/// its data requirements `backtest`: retrained on its schedule first
/// (`WalkForward::prepare`), then run, its results printed and the
/// out-of-sample scores of its models after them. The dataset build, each
/// train and the run are one after another in this process, a train as the
/// `train` subcommand's (`Training::train_at`), each Nautilus node, a
/// dataset run's (`DatasetRun`, logging as `logging` says) or the
/// backtest's, disposed before the next starts, which Nautilus supports.
/// The days built are printed, and each train's outcome: on standard
/// output when it kept no model, on standard error, with its time, when
/// it kept a current one, which does not stop the backtest.
fn backtest_on_models<S: ModelStrategy>(
    args: &BacktestArgs,
    logging: LoggerConfig,
    backtest: impl FnOnce(DataRequirements) -> Backtest,
) -> anyhow::Result<()> {
    let (start, end) = args.run.window()?;
    let config = load_config::<S::Params>(&args.run.config)?;
    let venue = args.dataset_venue()?;
    let (catalog, models) = (&args.run.catalog, &args.models);
    let walk = S::walk_forward(&config, catalog, models, &venue)?;
    let dataset = S::dataset(&config, catalog, &venue)?;
    let training = S::training(&config, catalog, models, &venue)?;
    let run = DatasetRun {
        catalog: catalog.clone(),
        commission: venue.commission,
        balance: venue.balance,
        leverage: venue.leverage,
        logging,
    };
    let replay = |from, to| run.replay::<S>(&config, from, to);
    walk.prepare(
        start,
        end,
        |from, to| {
            let (first, last) = dataset::days(from, to);
            let built = dataset.build(first, last, replay)?;
            if let (Some(from), Some(to)) = (built.first(), built.last()) {
                println!("built {} days of the dataset, {from} to {to}", built.len());
            }
            Ok(())
        },
        |at| {
            let trained = training.train_at(at, replay);
            if trained.succeeded() {
                println!("{trained}");
            } else {
                eprintln!("the retrain at {at}: {trained}");
            }
            Ok(())
        },
    )?;
    let mode = Mode::Backtest {
        models: models.clone(),
        venue,
    };
    let outcome = backtest(config.data_requirements()).run(S::new(config, mode)?)?;
    print!("{}", backtest::report(&outcome));
    print!("{}", walk.scores(start, end)?);
    Ok(())
}

/// The child processes of a live node whose strategy trades on a model:
/// its retrains, this binary's `train` on the node's strategy, config,
/// catalog and store, at the retrains' costs and the node's leverage
/// (`Launcher::process`). A live node is the one node of its process
/// while it runs, and a train replays the dataset days it lacks through a
/// node of its own, so each retrain is a process.
struct Children {
    strategy: StrategyName,
    config: PathBuf,
    catalog: PathBuf,
    models: PathBuf,
    /// The venue of the retrains' dataset runs: the retrain costs and the
    /// node's leverage.
    venue: dataset::DatasetVenue,
}

impl Children {
    /// The children of the live node of `args`: its retrains, at their
    /// costs and the node's leverage.
    ///
    /// # Errors
    ///
    /// Returns an error if the commission or the balance of the retrains is
    /// missing.
    fn of_live(args: &LiveArgs) -> anyhow::Result<Self> {
        Ok(Self {
            strategy: args.strategy,
            config: args.config.clone(),
            catalog: args.catalog.clone(),
            models: args.models.clone(),
            venue: args.retrain.venue(args.leverage)?,
        })
    }

    /// The arguments of `train` but its `--at`, which the launcher gives:
    /// the strategy, config, catalog and store, and the dataset runs'
    /// commission, balance and leverage.
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
        args.extend(commission_args(&self.venue.commission).map(OsString::from));
        args.extend(
            [
                "--balance".to_string(),
                self.venue.balance.to_string(),
                "--leverage".to_string(),
                self.venue.leverage.to_string(),
            ]
            .map(OsString::from),
        );
        args.extend(["--models".into(), self.models.clone().into_os_string()]);
        args
    }
}

/// Builds the strategy from its config and runs it live until Ctrl-C. A
/// strategy trading on a model retrains, if ever, in a child process,
/// this binary's `train` (see `Children`).
async fn run_live(args: LiveArgs) -> anyhow::Result<()> {
    let live = Live {
        trader_id: args.trader_id,
        venues: args.venues.clone(),
        leverage: args.leverage,
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

/// The visitor running a strategy on a live node until Ctrl-C: its
/// output, the run, is boxed, a future of each kind's own type.
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

/// Runs the model strategy `S` of `args` on `live` until Ctrl-C, on the
/// models of the store its dataset costs key, beside the actor of its
/// retrains if it is retrained (`RetrainActor`), each retrain in a child
/// process (`Children::of_live`) on the node's catalog.
async fn live_on_models<S: ModelStrategy>(args: &LiveArgs, live: &Live) -> anyhow::Result<()> {
    let config = load_config::<S::Params>(&args.config)?;
    let program = std::env::current_exe().context("finding this binary to retrain")?;
    let children = Children::of_live(args)?;
    let requirements = config.data_requirements();
    let retraining = RetrainActor::new(
        config.strategy_id(),
        config.model().retrain,
        args.catalog.clone(),
        requirements.clone(),
        Launcher::process(program, children.train_args()),
    );
    let mode = Mode::Live {
        models: args.models.clone(),
        venue: children.venue,
    };
    let strategy = S::new(config, mode)?;
    live.run(&requirements, &strategy.warmup(), strategy, retraining)
        .await
}

/// The dataset runs of a process: the strategy over the catalog in
/// `Mode::Dataset`, on hedging venues (`model::targets`), with the
/// commission, balance and leverage given, the commission the venues' and
/// the targets'; each replay a node of its own, disposed at its end.
struct DatasetRun {
    catalog: PathBuf,
    commission: Commission,
    balance: Money,
    leverage: Decimal,
    logging: LoggerConfig,
}

impl DatasetRun {
    /// The rows the model strategy `S` of `config` settles over `[start,
    /// end]`.
    fn replay<S: ModelStrategy>(
        &self,
        config: &StandardConfig<S::Params>,
        start: Timestamp,
        end: Timestamp,
    ) -> anyhow::Result<Vec<TargetRow>> {
        let requirements = config.data_requirements();
        let rows = SettledRows::default();
        let mode = Mode::Dataset {
            rows: Rc::clone(&rows),
            commission: self.commission,
        };
        let backtest = Backtest {
            catalog: self.catalog.clone(),
            bar_types: requirements.bar_types,
            ticks: requirements.ticks,
            start,
            end,
            commission: self.commission,
            balance: self.balance,
            leverage: self.leverage,
            venue_oms: |_| OmsType::Hedging,
            logging: self.logging.clone(),
        };
        backtest.run_dataset(S::new(config.clone(), mode)?)?;
        Ok(rows.take())
    }
}

/// Prints the outcome of a train that kept no model, each model published
/// or its current one in use; one that kept a current model, refused,
/// failed or a meta waiting, is an error, its reason: the command fails,
/// so that the live node that started it knows why (`model::launcher`).
///
/// # Errors
///
/// Returns an error if the train kept a current model.
fn reported(trained: &Trained) -> anyhow::Result<()> {
    ensure!(trained.succeeded(), "{trained}");
    println!("{trained}");
    Ok(())
}

/// Trains the strategy's model at `--at` and reports the outcome (see
/// `reported`): the days its dataset lacks are replayed by a dataset run
/// (`DatasetRun`).
fn run_train(args: TrainArgs) -> anyhow::Result<()> {
    let run = DatasetRun {
        catalog: args.catalog.clone(),
        commission: args.commission.commission(),
        balance: args.balance,
        leverage: args.leverage,
        logging: logger_config(std::env::var("NAUTILUS_LOG"))?,
    };
    let outcome = args.strategy.visit(Training {
        args: &args,
        run: &run,
    })?;
    reported(&outcome)
}

/// The visitor training a strategy's model (`train`): a strategy trading
/// on no model has none.
struct Training<'a> {
    args: &'a TrainArgs,
    run: &'a DatasetRun,
}

impl Visitor for Training<'_> {
    type Output = anyhow::Result<Trained>;

    fn plain<S: PlainStrategy>(self) -> Self::Output {
        let name = self
            .args
            .strategy
            .to_possible_value()
            .expect("every strategy has a name");
        bail!("the {} strategy trades on no model", name.get_name())
    }

    fn model<S: ModelStrategy>(self) -> Self::Output {
        train::<S>(self.args, self.run)
    }
}

/// Trains the model of the model strategy `S` of `args` at `--at`, the
/// days its dataset lacks replayed by `run`.
fn train<S: ModelStrategy>(args: &TrainArgs, run: &DatasetRun) -> anyhow::Result<Trained> {
    let config = load_config::<S::Params>(&args.config)?;
    let venue = args.commission.venue(args.balance, args.leverage);
    let training = S::training(&config, &args.catalog, &args.models, &venue)?;
    Ok(training.train_at(args.at, |start, end| run.replay::<S>(&config, start, end)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_trader::{
        model::{
            holdout::{Earnings, Holdout, Refusal},
            schedule::Retrain,
            score::ExpectedScore,
            train::{Kept, Outcome},
        },
        strategy::ema::EmaParams,
    };
    use jiff::SignedDuration;

    fn parse_command(subcommand: &str, args: &[&str]) -> Result<Command, clap::Error> {
        let argv = ["helix-trader", subcommand]
            .into_iter()
            .chain(args.iter().copied());
        Cli::try_parse_from(argv).map(|cli| cli.command)
    }

    fn parse(args: &[&str]) -> Result<FillCatalogArgs, clap::Error> {
        parse_command("fill-catalog", args).map(|command| match command {
            Command::FillCatalog(args) => args,
            command => panic!("{command:?}"),
        })
    }

    fn parse_backtest(args: &[&str]) -> Result<BacktestArgs, clap::Error> {
        parse_command("backtest", args).map(|command| match command {
            Command::Backtest(args) => *args,
            command => panic!("{command:?}"),
        })
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
    #[test]
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

    /// Fill-catalog takes IB's client id, not live's account or leverage.
    #[test]
    fn fill_catalog_takes_the_ib_client_id_but_not_lives_account_or_leverage() {
        let mut argv = REQUIRED.to_vec();
        argv.extend(["--client-id", "12"]);
        assert!(parse(&argv).is_ok());
        for flag in [["--account", "DU1234567"], ["--leverage", "2"]] {
            let mut argv = REQUIRED.to_vec();
            argv.extend(flag);
            let err = parse(&argv).unwrap_err();
            assert_eq!(
                err.kind(),
                clap::error::ErrorKind::UnknownArgument,
                "{flag:?}: {err}"
            );
        }
    }

    #[test]
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

    #[test]
    fn a_backtest_requires_the_commission_and_the_balance() {
        assert!(parse_backtest(&REQUIRED).is_err());
        for flag in ["--commission", "--balance"] {
            let argv = backtest_required();
            let at = argv.iter().position(|&arg| arg == flag).unwrap();
            let argv = [&argv[..at], &argv[at + 2..]].concat();
            let err = parse_backtest(&argv).unwrap_err().to_string();
            assert!(err.contains(flag), "{err}");
        }
    }

    #[test]
    fn a_backtest_takes_one_commission_of_the_three_kinds() {
        let flags = [
            ["--commission", "3 EUR"],
            ["--commission-rate", "0.0005"],
            ["--commission-per-quantity", "0.035 EUR"],
        ];
        for (i, one) in flags.iter().enumerate() {
            assert!(parse_backtest(&backtest_with(one)).is_ok(), "{one:?}");
            for other in &flags[i + 1..] {
                let both = backtest_with(&[one[0], one[1], other[0], other[1]]);
                let err = parse_backtest(&both).unwrap_err();
                assert_eq!(
                    err.kind(),
                    clap::error::ErrorKind::ArgumentConflict,
                    "{one:?} {other:?}: {err}"
                );
            }
        }
        let err = parse_backtest(&backtest_with(&[])).unwrap_err().to_string();
        assert!(
            err.contains("--commission-rate") && err.contains("--commission-per-quantity"),
            "{err}"
        );
    }

    /// Pepperstone's gold: 3.5 USD per 100 oz, so 0.035 USD an ounce, not
    /// rounded to 0.04 USD.
    #[test]
    fn a_commission_per_quantity_is_its_exact_amount_per_unit() {
        let args =
            parse_backtest(&backtest_with(&["--commission-per-quantity", "0.035 USD"])).unwrap();
        let exact = Money::from_str_exact("0.035 USD").unwrap();
        assert_ne!(exact, Money::from("0.04 USD"));
        assert_eq!(args.commission.commission(), Commission::PerQuantity(exact));
    }

    /// Each value joined to its flag, so that clap reads `-0.035 USD` as the
    /// value, not as a flag of its own.
    #[test]
    fn rejects_a_commission_per_quantity_it_cannot_charge() {
        for amount in ["-0.035 USD", "0.035", "USD", "0.035 XYZ"] {
            let flag = format!("--commission-per-quantity={amount}");
            let err = parse_backtest(&backtest_with(&[&flag])).unwrap_err();
            assert_eq!(
                err.kind(),
                clap::error::ErrorKind::ValueValidation,
                "{amount}: {err}"
            );
        }
    }

    #[test]
    fn rejects_malformed_money() {
        for flag in ["--commission", "--balance"] {
            for money in ["3", "EUR", "3 XYZ", "three EUR"] {
                let mut argv = backtest_required();
                let at = argv.iter().position(|&arg| arg == flag).unwrap();
                argv[at + 1] = money;
                let err = parse_backtest(&argv).unwrap_err().to_string();
                assert!(err.contains(flag), "{money}: {err}");
            }
        }
    }

    #[test]
    fn a_backtest_is_unleveraged_unless_given_a_leverage() {
        assert_eq!(
            parse_backtest(&backtest_required()).unwrap().leverage,
            Decimal::ONE
        );
        let mut argv = backtest_required();
        argv.extend(["--leverage", "10"]);
        assert_eq!(parse_backtest(&argv).unwrap().leverage, Decimal::TEN);
        let mut argv = backtest_required();
        argv.extend(["--leverage", "ten"]);
        let err = parse_backtest(&argv).unwrap_err().to_string();
        assert!(err.contains("--leverage"), "{err}");
    }

    fn parse_live(args: &[&str]) -> Result<LiveArgs, clap::Error> {
        parse_command("live", args).map(|command| match command {
            Command::Live(args) => *args,
            command => panic!("{command:?}"),
        })
    }

    #[test]
    fn live_runs_as_trader_helix_001_unless_given_another() {
        let args = parse_live(&["ema", "--config", "ema.toml"]).unwrap();
        assert_eq!(args.trader_id, TraderId::from("HELIX-001"));
        let args =
            parse_live(&["ema", "--config", "ema.toml", "--trader-id", "HELIX-002"]).unwrap();
        assert_eq!(args.trader_id, TraderId::from("HELIX-002"));
    }

    #[test]
    fn rejects_a_trader_id_nautilus_refuses() {
        // Nautilus' trader id is a name and a tag joined by a hyphen.
        assert!(parse_live(&["ema", "--config", "ema.toml", "--trader-id", "HELIX"]).is_err());
    }

    /// Its IB account and client id are the venues' (`LiveVenueArgs`).
    #[test]
    fn parses_live_with_its_strategy_config_and_catalog() {
        let args = parse_live(&["ema", "--config", "ema.toml"]).unwrap();
        assert_eq!(args.strategy, StrategyName::Ema);
        assert_eq!(args.config, PathBuf::from("ema.toml"));
        assert_eq!(args.catalog, PathBuf::from("catalog"));
    }

    #[test]
    fn live_is_unleveraged_unless_given_a_leverage() {
        let args = parse_live(&["ema", "--config", "ema.toml"]).unwrap();
        assert_eq!(args.leverage, Decimal::ONE);
        let args = parse_live(&["ema", "--config", "ema.toml", "--leverage", "10"]).unwrap();
        assert_eq!(args.leverage, Decimal::TEN);
    }

    #[test]
    fn live_stages_in_staging_unless_told_otherwise() {
        let args = parse_live(&["ema", "--config", "ema.toml"]).unwrap();
        assert_eq!(args.staging, PathBuf::from("staging"));
        let args =
            parse_live(&["ema", "--config", "ema.toml", "--staging", "/data/staging"]).unwrap();
        assert_eq!(args.staging, PathBuf::from("/data/staging"));
    }

    #[test]
    fn rejects_a_start_not_before_the_end() {
        let mut argv = REQUIRED.to_vec();
        argv[4] = "2024-03-01T12:30:00Z";
        let err = parse(&argv).unwrap().run.window().unwrap_err().to_string();
        assert!(err.contains("before"), "{err}");
        assert!(parse(&REQUIRED).unwrap().run.window().is_ok());
        let mut argv = backtest_required();
        argv[4] = "2024-03-01T12:30:00Z";
        let err = parse_backtest(&argv).unwrap().run.window().unwrap_err();
        assert!(err.to_string().contains("before"), "{err}");
    }

    #[test]
    fn a_config_error_names_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ema.toml");
        std::fs::write(
            &path,
            r#"
            [data]
            series = []
            warmup = "720h"

            [trade]
            instruments = [{ instrument_id = "ESM24.CME", trade_size = "1" }]
            "#,
        )
        .unwrap();
        let err = format!("{:#}", load_config::<EmaParams>(&path).unwrap_err());
        assert!(
            err.contains("ema.toml") && err.contains("data.series"),
            "{err}"
        );
        let missing = dir.path().join("missing.toml");
        let err = format!("{:#}", load_config::<EmaParams>(&missing).unwrap_err());
        assert!(err.contains("missing.toml"), "{err}");
    }

    #[test]
    fn logger_config_defaults_when_unset_and_rejects_a_malformed_spec() {
        assert!(logger_config(Err(std::env::VarError::NotPresent)).is_ok());
        let not_unicode = std::ffi::OsString::from("x");
        assert!(logger_config(Err(std::env::VarError::NotUnicode(not_unicode))).is_err());
        assert!(logger_config(Ok("stdout=Debug".to_string())).is_ok());
        assert!(logger_config(Ok("stdout=Loud".to_string())).is_err());
    }

    fn parse_train(args: &[&str]) -> Result<TrainArgs, clap::Error> {
        parse_command("train", args).map(|command| match command {
            Command::Train(args) => *args,
            command => panic!("{command:?}"),
        })
    }

    /// The arguments a train requires.
    const TRAIN: [&str; 9] = [
        "model_ema",
        "--config",
        "model_ema.toml",
        "--at",
        "2026-10-23",
        "--commission-rate",
        "0.00003",
        "--balance",
        "10000000 USD",
    ];

    #[test]
    fn parses_a_train_at_a_time_with_its_costs_into_the_models_store() {
        let args = parse_train(&TRAIN).unwrap();
        assert_eq!(args.strategy, StrategyName::ModelEma);
        assert_eq!(args.config, PathBuf::from("model_ema.toml"));
        assert_eq!(
            args.at,
            "2026-10-23T00:00:00Z".parse::<Timestamp>().unwrap()
        );
        assert_eq!(args.catalog, PathBuf::from("catalog"));
        assert_eq!(args.models, PathBuf::from("models"));
        assert_eq!(
            args.commission.commission(),
            Commission::Rate(Decimal::new(3, 5))
        );
        assert_eq!(args.balance, Money::from("10000000 USD"));
    }

    #[test]
    fn a_train_requires_its_time_and_its_costs() {
        for flag in ["--at", "--commission-rate", "--balance"] {
            let at = TRAIN.iter().position(|&arg| arg == flag).unwrap();
            let argv = [&TRAIN[..at], &TRAIN[at + 2..]].concat();
            let err = parse_train(&argv).unwrap_err().to_string();
            assert!(err.contains(flag), "{err}");
        }
    }

    #[test]
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

    #[test]
    fn a_train_keeping_the_model_fails_with_the_reason() {
        let holdout = Box::new(Holdout {
            earnings: Earnings::default(),
            score: ExpectedScore {
                rows: 2,
                skill: Some(0.1),
                ..ExpectedScore::default()
            },
        });
        let kept = Outcome::Kept(Kept::Refused {
            why: Refusal::MinPnl {
                pnl: 0.48,
                min_pnl: 0.55,
            },
            holdout: holdout.clone(),
        });
        let published = Outcome::Published {
            valid_from: "2026-10-23T00:00:00Z".parse().unwrap(),
            holdout,
        };

        let err = reported(&kept.clone().into()).unwrap_err().to_string();
        let meta_kept = Trained {
            long: Some(published.clone()),
            short: Some(published.clone()),
            meta: Some(kept),
        };

        assert!(err.contains("0.48") && err.contains("0.55"), "{err}");
        assert!(reported(&published.into()).is_ok());
        let err = reported(&meta_kept).unwrap_err().to_string();
        assert!(err.contains("meta") && err.contains("0.48"), "{err}");
    }

    /// A rerun of a train finds the long's model published: kept in use, it
    /// is no failure, and the train exits 0.
    #[test]
    fn a_train_keeping_no_model_succeeds_its_models_in_use_or_published() {
        let t: Timestamp = "2026-10-23T00:00:00Z".parse().unwrap();
        let trained = Trained {
            long: Some(Outcome::InUse {
                valid_from: t,
                retrain: Retrain::Every(SignedDuration::from_hours(24)),
            }),
            short: Some(Outcome::Published {
                valid_from: t,
                holdout: Box::new(Holdout {
                    earnings: Earnings::default(),
                    score: ExpectedScore {
                        rows: 2,
                        skill: Some(0.1),
                        ..ExpectedScore::default()
                    },
                }),
            }),
            meta: None,
        };

        assert!(reported(&trained).is_ok());
    }

    /// A meta with no side's model before the train waits: the
    /// train fails, saying why.
    #[test]
    fn a_train_whose_meta_waits_fails_saying_so() {
        let t: Timestamp = "2026-10-23T00:00:00Z".parse().unwrap();
        let trained = Trained {
            long: Some(Outcome::InUse {
                valid_from: t,
                retrain: Retrain::Every(SignedDuration::from_hours(24)),
            }),
            short: None,
            meta: Some(Outcome::Kept(Kept::Waiting)),
        };

        let err = reported(&trained).unwrap_err().to_string();

        assert!(err.contains("waiting for the sides' decisions"), "{err}");
    }

    /// The arguments of a `model_ema` live node retraining at a commission
    /// rate.
    const LIVE_RETRAINING: [&str; 7] = [
        "model_ema",
        "--config",
        "model_ema.toml",
        "--retrain-commission-rate",
        "0.00003",
        "--retrain-balance",
        "10000000 USD",
    ];

    /// The `train` `children` start, given its `--at`, parsed as this
    /// binary parses it.
    fn started_train(children: &Children) -> TrainArgs {
        let argv: Vec<String> = children
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

    /// A live node's retrain is a `train` on the node's strategy, config,
    /// catalog and store, at the retrains' commission and balance and the
    /// node's leverage.
    #[test]
    fn a_live_nodes_retrain_parses_as_train_on_its_files_store_leverage_and_costs() {
        let mut argv = LIVE_RETRAINING.to_vec();
        argv.extend(["--catalog", "/data/catalog", "--models", "/data/models"]);
        argv.extend(["--leverage", "2"]);

        let children = Children::of_live(&parse_live(&argv).unwrap()).unwrap();
        let train = started_train(&children);

        assert_eq!(train.strategy, StrategyName::ModelEma);
        assert_eq!(train.config, PathBuf::from("model_ema.toml"));
        assert_eq!(train.catalog, PathBuf::from("/data/catalog"));
        assert_eq!(train.models, PathBuf::from("/data/models"));
        assert_eq!(
            train.commission.venue(train.balance, train.leverage),
            dataset::DatasetVenue {
                commission: Commission::Rate(Decimal::new(3, 5)),
                balance: Money::from("10000000 USD"),
                leverage: Decimal::from(2),
            }
        );
        assert_eq!(
            children.venue,
            train.commission.venue(train.balance, train.leverage)
        );
    }

    #[test]
    fn a_live_nodes_retrain_parses_at_a_commission_per_order_or_per_quantity_too() {
        for (flag, amount, commission) in [
            (
                "--retrain-commission",
                "3 USD",
                Commission::PerOrder(Money::from("3 USD")),
            ),
            (
                "--retrain-commission-per-quantity",
                "0.035 USD",
                Commission::PerQuantity(Money::from_str_exact("0.035 USD").unwrap()),
            ),
        ] {
            let mut argv = LIVE_RETRAINING.to_vec();
            argv.splice(3..5, [flag, amount]);

            let train = started_train(&Children::of_live(&parse_live(&argv).unwrap()).unwrap());

            assert_eq!(train.commission.commission(), commission, "{flag}");
        }
    }

    #[test]
    fn live_retraining_without_its_costs_is_refused_naming_them() {
        for missing in [3, 5] {
            let argv = [&LIVE_RETRAINING[..missing], &LIVE_RETRAINING[missing + 2..]].concat();
            let err = Children::of_live(&parse_live(&argv).unwrap())
                .err()
                .unwrap()
                .to_string();
            assert!(err.contains(LIVE_RETRAINING[missing]), "{err}");
        }
    }

    /// A strategy never retrained reads its models from the store keyed by
    /// the costs of its dataset runs as well: the live node asks for them
    /// as it builds its venue, saying so.
    #[test]
    fn live_without_its_dataset_costs_is_refused_saying_they_key_the_model_store() {
        for missing in [3, 5] {
            let argv = [&LIVE_RETRAINING[..missing], &LIVE_RETRAINING[missing + 2..]].concat();
            let err = parse_live(&argv)
                .unwrap()
                .retrain
                .venue(Decimal::ONE)
                .unwrap_err()
                .to_string();
            assert!(err.contains("model store"), "{err}");
            assert!(err.contains(LIVE_RETRAINING[missing]), "{err}");
        }
    }

    /// Each value joined to its flag, so that clap reads `-0.1` as the
    /// value, not as a flag of its own.
    #[test]
    fn live_refuses_retrain_costs_it_cannot_read() {
        for cost in [
            "--retrain-commission-rate=-0.1",
            "--retrain-commission=3",
            "--retrain-balance=lots",
        ] {
            let argv = ["model_ema", "--config", "model_ema.toml", cost];
            let err = parse_live(&argv).unwrap_err();
            assert_eq!(
                err.kind(),
                clap::error::ErrorKind::ValueValidation,
                "{cost}: {err}"
            );
        }
        for other in [
            ["--retrain-commission", "3 USD"],
            ["--retrain-commission-per-quantity", "0.035 USD"],
        ] {
            let mut both = LIVE_RETRAINING.to_vec();
            both.extend(other);
            let err = parse_live(&both).unwrap_err();
            assert_eq!(
                err.kind(),
                clap::error::ErrorKind::ArgumentConflict,
                "{other:?}: {err}"
            );
        }
        let per_order_and_quantity = [
            "model_ema",
            "--config",
            "model_ema.toml",
            "--retrain-commission",
            "3 USD",
            "--retrain-commission-per-quantity",
            "0.035 USD",
        ];
        let err = parse_live(&per_order_and_quantity).unwrap_err();
        assert_eq!(
            err.kind(),
            clap::error::ErrorKind::ArgumentConflict,
            "{err}"
        );
    }

    #[test]
    fn the_ema_strategy_has_no_model_to_train() {
        let mut argv = TRAIN.to_vec();
        argv[0] = "ema";
        let err = run_train(parse_train(&argv).unwrap()).unwrap_err();
        assert!(err.to_string().contains("no model"), "{err}");
    }

    /// A live node's retrain is handed its commission normalized.
    #[test]
    fn a_commission_is_handed_to_a_train_normalized() {
        for (given, handed) in [
            (["--commission", "3.00 EUR"], ["--commission", "3 EUR"]),
            (
                ["--commission-rate", "0.000030"],
                ["--commission-rate", "0.00003"],
            ),
            (
                ["--commission-per-quantity", "0.0350 USD"],
                ["--commission-per-quantity", "0.035 USD"],
            ),
        ] {
            let args = parse_backtest(&backtest_with(&given)).unwrap();
            assert_eq!(commission_args(&args.commission.commission()), handed);
        }
    }

    /// The commission keys the dataset and its models: two spellings of
    /// one commission are one key, handed on to a train as such.
    #[test]
    fn two_spellings_of_a_commission_are_one_key() {
        for (one, other) in [
            (["--commission", "3 EUR"], ["--commission", "3.00 EUR"]),
            (
                ["--commission-rate", "0.00003"],
                ["--commission-rate", "0.000030"],
            ),
            (
                ["--commission-per-quantity", "0.035 USD"],
                ["--commission-per-quantity", "0.0350 USD"],
            ),
        ] {
            let key = |flags: [&'static str; 2]| {
                let commission = parse_backtest(&backtest_with(&flags))
                    .unwrap()
                    .commission
                    .commission();
                (commission, commission_args(&commission))
            };
            assert_eq!(key(one), key(other), "{one:?} {other:?}");
            let (_, handed) = key(other);
            let reparsed = parse_backtest(&backtest_with(&[&handed[0], &handed[1]])).unwrap();
            assert_eq!(reparsed.commission.commission(), key(one).0);
        }
    }

    /// A live node's retrains are keyed as `train` keys them.
    #[test]
    fn two_spellings_of_a_retrain_commission_are_one_key() {
        let retrain = |rate: &str| {
            parse_live(&[
                "model_ema",
                "--config",
                "model_ema.toml",
                "--retrain-commission-rate",
                rate,
            ])
            .unwrap()
            .retrain
            .commission()
            .unwrap()
        };
        assert_eq!(retrain("0.000030"), retrain("0.00003"));
        let backtest = parse_backtest(&backtest_with(&["--commission-rate", "0.00003"])).unwrap();
        assert_eq!(retrain("0.000030"), backtest.commission.commission());
    }

    #[test]
    fn a_backtest_of_a_model_strategy_needs_a_dataset_balance() {
        let err = parse_backtest(&backtest_required())
            .unwrap()
            .dataset_venue()
            .unwrap_err();
        assert!(err.to_string().contains("--dataset-balance"), "{err}");
        let mut argv = backtest_required();
        argv.extend(["--dataset-balance", "10000000 EUR"]);
        let venue = parse_backtest(&argv).unwrap().dataset_venue().unwrap();
        assert_eq!(venue.balance, Money::from("10000000 EUR"));
    }

    /// Every subcommand takes the strategy first, by its command-line
    /// name: without one, or with a name no strategy has, it is refused.
    #[test]
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
            let err = parse_command(subcommand, &argv[1..]).unwrap_err();
            assert_eq!(
                err.kind(),
                clap::error::ErrorKind::MissingRequiredArgument,
                "{subcommand}: {err}"
            );
            let unknown = [&["EMA_CROSS"][..], &argv[1..]].concat();
            let err = parse_command(subcommand, &unknown).unwrap_err();
            assert_eq!(
                err.kind(),
                clap::error::ErrorKind::InvalidValue,
                "{subcommand}: {err}"
            );
        }
    }
}
