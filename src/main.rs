//! Command line: one subcommand per process (fill-catalog, backtest, live,
//! train, dataset), since Nautilus allows one node per process. Every
//! subcommand takes the strategy name and its TOML config: the instruments
//! and the warmup come from the config's data requirements.

use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    rc::Rc,
    time::Duration,
};

use anyhow::{Context, bail, ensure};
use clap::{Args, Parser, Subcommand, ValueEnum};
use helix_trader::{
    backtest::{self, Backtest},
    data::{
        DatabentoHistory, IbHistory, IcConnection, IcHistory, NodeClient, NodeFill, OkxHistory,
        REQUEST_TIMEOUT, missing,
    },
    live::{IbAccount, IcDataClientConfig, IcDataClientFactory, Live, Recording},
    model::{
        dataset,
        handle::{Mode, SettledRows},
        launcher::{Launcher, Retrained},
        targets::TargetRow,
        train::Outcome,
    },
    strategy::{
        DataRequirements, SeriesSlice,
        ema::{EmaCross, EmaCrossConfig},
        learned::{self, Learned, LearnedConfig},
    },
};
use jiff::Timestamp;
use nautilus_common::logging::{
    bridge::init_tracing, init_logging, logger::LoggerConfig, writer::FileWriterConfig,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_execution::models::fee::FeeModelAny;
use nautilus_live::config::RoutingConfig;
use nautilus_model::{
    enums::OmsType,
    identifiers::{InstrumentId, TraderId},
    types::Money,
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rust_decimal::Decimal;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Fills the Nautilus catalog with the strategy's history from OKX's
    /// archive (OKX instruments), Databento (GLBX instruments), IC Markets'
    /// cTrader (IC instruments) or
    /// Interactive Brokers (the others).
    FillCatalog(FillCatalogArgs),
    /// Backtests the strategy over a window of the catalog and prints the results.
    Backtest(Box<BacktestArgs>),
    /// Runs the strategy on a live node with Interactive Brokers, IC
    /// Markets or OKX; stops on Ctrl-C.
    Live(LiveArgs),
    /// Trains the strategy's model at a time, replaying the days its
    /// dataset lacks, and prints the model published, or fails with why
    /// the current one was kept.
    Train(Box<TrainArgs>),
    /// Builds the strategy's dataset over the days from --start to --end,
    /// those it lacks, by a dataset run of the strategy over the catalog
    /// to the last day's end plus its longest target.
    Dataset(Box<DatasetArgs>),
}

/// The arguments of a subcommand working on a window of the catalog.
#[derive(Debug, Args)]
struct RunArgs {
    /// The strategy whose instruments and warmup are used.
    strategy: Strategy,
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
    /// IB client id of this process; not the adapter default 1, so a live
    /// node with the default id can run at the same time.
    #[arg(long, default_value_t = 10)]
    client_id: i32,
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
    /// venues whose instruments carry margins of 1 (OKX); IC's carry IC's
    /// own margin, so an IC backtest keeps 1.
    #[arg(long, default_value = "1")]
    leverage: Decimal,
    /// The model store a strategy trading on a model reads its models
    /// from: at each decision, the latest valid then. The backtest first
    /// trains at each retrain time of its span the store lacks.
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
struct DatasetArgs {
    /// The strategy, the catalog, and the span whose days are built.
    #[command(flatten)]
    run: RunArgs,
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

#[derive(Debug, Args)]
struct TrainArgs {
    /// The strategy whose model is trained.
    strategy: Strategy,
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

/// The commission of a backtest: exactly one of the flags.
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
}

/// A commission flag's value: as given, to hand on to a child process, and
/// its fee model.
#[derive(Debug, Clone)]
struct Commission {
    given: String,
    fee_model: FeeModelAny,
}

impl CommissionArgs {
    /// The fee model of the one flag given.
    fn fee_model(&self) -> FeeModelAny {
        self.given().1.fee_model.clone()
    }

    /// The one flag given and its value, as given.
    fn args(&self) -> [String; 2] {
        let (flag, commission) = self.given();
        [flag.to_string(), commission.given.clone()]
    }

    fn given(&self) -> (&str, &Commission) {
        match (&self.commission, &self.commission_rate) {
            (Some(commission), _) => ("--commission", commission),
            (None, Some(rate)) => ("--commission-rate", rate),
            (None, None) => unreachable!("clap requires one commission flag"),
        }
    }
}

#[derive(Debug, Args)]
struct LiveArgs {
    /// The strategy to run.
    strategy: Strategy,
    /// The strategy's TOML config.
    #[arg(long)]
    config: PathBuf,
    /// The IB account traded, for example DU1234567 (paper); needed only
    /// for IB instruments.
    #[arg(long, env = "NAUTILUS_IB_ACCOUNT_ID")]
    account: Option<IbAccount>,
    /// IB client id of the node, the adapter default.
    #[arg(long, default_value_t = 1)]
    client_id: i32,
    /// The leverage the run expects, as in a backtest. OKX applies the
    /// account's, set by hand per instrument: the start refuses an OKX
    /// instrument at another one.
    #[arg(long, default_value = "1")]
    leverage: Decimal,
    /// The node's trader: its cache in Redis is the trader's, which each run
    /// loads. Live processes running at once each need their own.
    #[arg(long, default_value = "HELIX-001", value_parser = parse_trader_id)]
    trader_id: TraderId,
    /// The catalog directory, fill-catalog's and the backtests': the warmup
    /// is read from it first, and IC's history is written back to it.
    #[arg(long, default_value = "catalog")]
    catalog: PathBuf,
    /// The directory the node stages the data it streams into the
    /// catalog, outside it; it also keeps each trader's instance id.
    #[arg(long, default_value = "staging")]
    staging: PathBuf,
    /// How often, in minutes, the staged data is promoted into the
    /// catalog: the delay before a retrain sees it.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u64).range(1..))]
    promotion_minutes: u64,
    /// The model store a strategy trading on a model reads its models
    /// from, at each decision the latest valid then, and its retrains
    /// publish in.
    #[arg(long, default_value = "models")]
    models: PathBuf,
    /// The costs of the dataset runs of the retrains, for a strategy
    /// trading on a model.
    #[command(flatten)]
    retrain: RetrainCosts,
}

/// The costs of the dataset runs of a live node's retrains, as `train`
/// takes them (its `--commission` or `--commission-rate`, and
/// `--balance`), its leverage the node's. Checked as `train` checks them,
/// and kept as given, for the `train` the node starts.
#[derive(Debug, Args)]
struct RetrainCosts {
    /// Commission charged once per order in a retrain, for example "3 USD".
    #[arg(long, value_parser = commission_text, conflicts_with = "retrain_commission_rate")]
    retrain_commission: Option<String>,
    /// Commission rate on the value of each fill in a retrain, for example
    /// 0.00003.
    #[arg(long, value_parser = commission_rate_text)]
    retrain_commission_rate: Option<String>,
    /// Starting balance of a retrain's dataset run, large enough that no
    /// target is denied, for example "10000000 USD".
    #[arg(long, value_parser = money_text)]
    retrain_balance: Option<String>,
}

/// The strategies), by command-line name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Strategy {
    Ema,
    Learned,
}

/// Nautilus' own parser: an RFC 3339 timestamp, a date taken as midnight
/// UTC, or Unix nanoseconds or seconds.
fn parse_time(s: &str) -> Result<Timestamp, String> {
    s.parse::<UnixNanos>()
        .map(Timestamp::from)
        .map_err(|e| e.to_string())
}

/// A Nautilus trader id, a name and a tag joined by a hyphen, checked by
/// Nautilus.
fn parse_trader_id(s: &str) -> Result<TraderId, String> {
    TraderId::new_checked(s).map_err(|e| e.to_string())
}

/// An amount and a currency code, for example "3 EUR". Explicit, since
/// clap would otherwise pick `Money: From<&str>`, which panics.
fn parse_money(s: &str) -> Result<Money, String> {
    s.parse()
}

/// A commission per order: an amount and a currency code, not negative.
fn parse_commission(s: &str) -> anyhow::Result<Commission> {
    Ok(Commission {
        given: s.to_string(),
        fee_model: backtest::commission_per_order(parse_money(s).map_err(anyhow::Error::msg)?)?,
    })
}

/// A commission rate: a decimal, not negative.
fn parse_commission_rate(s: &str) -> anyhow::Result<Commission> {
    Ok(Commission {
        given: s.to_string(),
        fee_model: backtest::commission_rate(s.parse::<Decimal>()?)?,
    })
}

/// A commission per order, checked, as given.
fn commission_text(s: &str) -> anyhow::Result<String> {
    parse_commission(s).map(|_| s.to_string())
}

/// A commission rate, checked, as given.
fn commission_rate_text(s: &str) -> anyhow::Result<String> {
    parse_commission_rate(s).map(|_| s.to_string())
}

/// An amount and a currency code, checked, as given.
fn money_text(s: &str) -> Result<String, String> {
    parse_money(s).map(|_| s.to_string())
}

/// Reads the EMA crossover config from `path` and validates it.
fn load_ema_config(path: &Path) -> anyhow::Result<EmaCrossConfig> {
    let read = || -> anyhow::Result<EmaCrossConfig> {
        let config: EmaCrossConfig = toml::from_str(&fs::read_to_string(path)?)?;
        config.validate()?;
        Ok(config)
    };
    read().with_context(|| format!("loading the EMA config {}", path.display()))
}

/// Reads the learned strategy's config from `path` and validates it.
fn load_learned_config(path: &Path) -> anyhow::Result<LearnedConfig> {
    let read = || -> anyhow::Result<LearnedConfig> {
        let config: LearnedConfig = toml::from_str(&fs::read_to_string(path)?)?;
        config.validate()?;
        Ok(config)
    };
    read().with_context(|| format!("loading the learned config {}", path.display()))
}

/// The data requirements of `strategy`'s config at `path`.
fn data_requirements(strategy: Strategy, path: &Path) -> anyhow::Result<DataRequirements> {
    Ok(match strategy {
        Strategy::Ema => load_ema_config(path)?.bars.data_requirements(),
        Strategy::Learned => load_learned_config(path)?.bars.data_requirements(),
    })
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
        Command::Live(args) => run_live(args).await,
        Command::Train(args) => run_train(*args),
        Command::Dataset(args) => run_dataset(*args),
    }
}

/// Checks everything that needs no gateway, then fills the catalog with the
/// instruments and the series the strategy reads, those its bars are built
/// from and its ticks (see `DataRequirements::catalog_series`), each on a
/// contract over its slice (`DataRequirements::slices`), every source given
/// its slices: GLBX's and IC's fill through a Nautilus node (see
/// `data::NodeFill`), by Databento's data client for GLBX instruments (the
/// series declared, Databento's cost of every request quoted first, before
/// the node starts) and IC's for IC instruments; OKX's archive fills OKX
/// instruments (their 1-minute candles) and IB (connected only if needed)
/// the others, without a node until their clients answer with response
/// envelopes. OKX, IC and IB refuse a series they do not serve.
async fn run_fill_catalog(args: FillCatalogArgs) -> anyhow::Result<()> {
    let (start, end) = args.run.window()?;
    let requirements = data_requirements(args.run.strategy, &args.run.config)?;
    // OKX's and IB's fills run no node, so no node installs Nautilus'
    // logger for them: install it here, as a node would (the fill's node
    // keeps it), so the `log` output of this crate and Nautilus, and the
    // adapters' `tracing` warnings (why a contract did not resolve), reach
    // the console. Configured by NAUTILUS_LOG and RUST_LOG. Temporary until
    // nautilus_trader#5472 and #5473 are fixed: IB and OKX then fill
    // through the node, which installs the logger.
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
    let mut catalog = ParquetDataCatalog::new(catalog_dir, None, None, None, None);
    // The source of an instrument is its venue's: OKX's archive for OKX,
    // Databento for GLBX, IC's cTrader for IC, IB for every other venue.
    let (okx_ids, others): (Vec<InstrumentId>, Vec<InstrumentId>) = requirements
        .instrument_ids()
        .into_iter()
        .partition(|&id| OkxHistory::serves(id));
    let (glbx_ids, others): (Vec<InstrumentId>, Vec<InstrumentId>) = others
        .into_iter()
        .partition(|&id| DatabentoHistory::serves(id));
    let (ic_ids, ib_ids): (Vec<InstrumentId>, Vec<InstrumentId>) =
        others.into_iter().partition(|&id| IcHistory::serves(id));
    // Every source takes the series slices of its instruments: a rolled
    // root's series contract by contract, each over its slice.
    let slices = requirements.slices(start, end)?;
    let slices_of = |serves: fn(InstrumentId) -> bool| -> Vec<SeriesSlice> {
        slices
            .iter()
            .filter(|slice| serves(slice.slice.instrument_id))
            .cloned()
            .collect()
    };
    let mut node = NodeFill::new();
    if !glbx_ids.is_empty() {
        let requests = missing(&catalog, &slices_of(DatabentoHistory::serves))?;
        if !requests.is_empty() {
            let api_key =
                std::env::var("DATABENTO_API_KEY").context("reading DATABENTO_API_KEY")?;
            let databento = DatabentoHistory::new(&api_key)?;
            databento.quote(&requests).await?;
            node.add(databento.node_client(), &requests, DatabentoHistory::period);
        }
    }
    if !ic_ids.is_empty() {
        let instrument_ids =
            IcHistory::quoted(&requirements.of_instruments(&ic_ids).catalog_series())?;
        let requests = missing(&catalog, &slices_of(IcHistory::serves))?;
        if !requests.is_empty() {
            let client = NodeClient {
                factory: Box::new(IcDataClientFactory),
                config: Box::new(IcDataClientConfig {
                    connection: IcConnection::from_env()?,
                    instrument_ids,
                }),
                routing: RoutingConfig::default(),
            };
            node.add(client, &requests, IcHistory::period);
        }
    }
    // Temporary until nautilus_trader#5473 is fixed: OKX then fills through
    // the node.
    if !okx_ids.is_empty() {
        OkxHistory::new()?
            .fill(&mut catalog, &slices_of(OkxHistory::serves))
            .await?;
    }
    node.run(catalog_dir, REQUEST_TIMEOUT).await?;
    // Temporary until nautilus_trader#5472 is fixed: IB then fills through
    // the node.
    if !ib_ids.is_empty() {
        let ib = IbHistory::connect(args.client_id, end, Timestamp::now()).await?;
        ib.fill(&mut catalog, &slices_of(is_ib)).await?;
    }
    Ok(())
}

/// Whether `id` is IB's: on a venue no other source serves. Temporary
/// until nautilus_trader#5472 is fixed: IB then fills through the node.
fn is_ib(id: InstrumentId) -> bool {
    !OkxHistory::serves(id) && !DatabentoHistory::serves(id) && !IcHistory::serves(id)
}

/// Builds the strategy from its config, runs it over the window of the
/// catalog and prints the results to stdout. A strategy trading on a
/// model is first retrained on its schedule (`model::walk`): its dataset
/// built over the span and a model trained at each retrain time the store
/// lacks, each by a child process of this binary (`dataset`, `train`),
/// on the backtest's catalog, store and costs and the dataset balance;
/// the out-of-sample scores of its models follow the results.
fn run_backtest(args: BacktestArgs) -> anyhow::Result<()> {
    let (start, end) = args.run.window()?;
    // The node installs Nautilus' logger from its engine config: pass the
    // NAUTILUS_LOG config through, rather than install a logger here.
    let logging = logger_config(std::env::var("NAUTILUS_LOG"))?;
    let fee_model = args.commission.fee_model();
    // Each simulated venue keeps positions as the real one does.
    let backtest = |requirements: DataRequirements| Backtest {
        catalog: args.run.catalog.clone(),
        bar_types: requirements.bar_types,
        start,
        end,
        ticks: requirements.ticks,
        warmup: requirements.warmup,
        fee_model,
        balance: args.balance,
        leverage: args.leverage,
        venue_oms: backtest::venue_oms_type,
        logging,
    };
    match args.run.strategy {
        Strategy::Ema => {
            let config = load_ema_config(&args.run.config)?;
            let outcome = backtest(config.bars.data_requirements()).run(EmaCross::new(config)?)?;
            print!("{}", backtest::report(&outcome));
        }
        Strategy::Learned => {
            let config = load_learned_config(&args.run.config)?;
            let walk = learned::walk_forward(&config, &args.run.catalog, &args.models)?;
            let children = Children::of(&args)?;
            walk.prepare(
                start,
                end,
                |from, to| children.dataset(from, to),
                |at| children.train(at),
            )?;
            let mode = Mode::Backtest {
                models: args.models.clone(),
            };
            let outcome =
                backtest(config.bars.data_requirements()).run(Learned::new(config, mode)?)?;
            print!("{}", backtest::report(&outcome));
            print!("{}", walk.scores(start, end)?);
        }
    }
    Ok(())
}

/// The child processes a backtest of a strategy trading on a model starts
/// before its run, a Nautilus node each: this binary's `dataset` and
/// `train`, on the backtest's strategy, config, catalog, store, commission
/// and leverage, and its dataset balance. Each inherits the backtest's
/// output and environment, so its logs and its outcome show as they come.
/// A train runs as live's do (`Launcher::process`): one that kept the
/// current model, which `train` reports by failing, is a kept retrain.
struct Children {
    strategy: Strategy,
    config: PathBuf,
    catalog: PathBuf,
    models: PathBuf,
    commission: [String; 2],
    balance: Money,
    leverage: Decimal,
}

impl Children {
    /// The children of the backtest of `args`.
    ///
    /// # Errors
    ///
    /// Returns an error if the backtest has no dataset balance.
    fn of(args: &BacktestArgs) -> anyhow::Result<Self> {
        let balance = args
            .dataset_balance
            .context("a backtest of a strategy trading on a model needs --dataset-balance")?;
        Ok(Self {
            strategy: args.run.strategy,
            config: args.run.config.clone(),
            catalog: args.run.catalog.clone(),
            models: args.models.clone(),
            commission: args.commission.args(),
            balance,
            leverage: args.leverage,
        })
    }

    /// Builds the dataset over the days of `[from, to)` it lacks.
    fn dataset(&self, from: Timestamp, to: Timestamp) -> anyhow::Result<()> {
        self.run(
            "dataset",
            &["--start", &from.to_string(), "--end", &to.to_string()],
        )
    }

    /// Trains the model at `at`: published, or the current one kept, its
    /// reason printed to standard error; either way no error.
    ///
    /// # Errors
    ///
    /// Returns an error if this binary cannot be found.
    fn train(&self, at: Timestamp) -> anyhow::Result<()> {
        let binary = std::env::current_exe().context("finding this binary")?;
        let mut args = self.args("train");
        args.extend(["--models".into(), self.models.clone().into_os_string()]);
        match Launcher::process(binary, args).launch(at).wait() {
            Retrained::Published => {}
            Retrained::Kept(why) => {
                eprintln!("the retrain at {at}: {why}");
            }
        }
        Ok(())
    }

    /// Runs `subcommand` with `args` and waits for it.
    ///
    /// # Errors
    ///
    /// Returns an error if it cannot start, or does not succeed.
    fn run(&self, subcommand: &str, args: &[&str]) -> anyhow::Result<()> {
        let binary = std::env::current_exe().context("finding this binary")?;
        let status = std::process::Command::new(binary)
            .args(self.args(subcommand))
            .args(args)
            .status()
            .with_context(|| format!("starting {subcommand} {}", args.join(" ")))?;
        ensure!(
            status.success(),
            "{subcommand} {} failed: {status}",
            args.join(" ")
        );
        Ok(())
    }

    /// The arguments of `subcommand` on the backtest's strategy, config,
    /// catalog, commission, dataset balance and leverage.
    fn args(&self, subcommand: &str) -> Vec<OsString> {
        let strategy = self
            .strategy
            .to_possible_value()
            .expect("every strategy has a name");
        let mut args: Vec<OsString> = vec![subcommand.into(), strategy.get_name().into()];
        args.extend([
            "--config".into(),
            self.config.clone().into_os_string(),
            "--catalog".into(),
            self.catalog.clone().into_os_string(),
        ]);
        args.extend(self.commission.iter().map(OsString::from));
        args.extend(
            [
                "--balance".to_string(),
                self.balance.to_string(),
                "--leverage".to_string(),
                self.leverage.to_string(),
            ]
            .map(OsString::from),
        );
        args
    }
}

/// Builds the strategy from its config and runs it live until Ctrl-C. A
/// strategy trading on a model retrains in a child process, this binary's
/// `train` (see `retrain_args`).
async fn run_live(args: LiveArgs) -> anyhow::Result<()> {
    let live = Live {
        trader_id: args.trader_id,
        account: args.account.clone(),
        client_id: args.client_id,
        leverage: args.leverage,
        logging: logger_config(std::env::var("NAUTILUS_LOG"))?,
        catalog: args.catalog.clone(),
        recording: Recording {
            staging: args.staging.clone(),
            promotion_interval: Duration::from_secs(args.promotion_minutes * 60),
        },
    };
    match args.strategy {
        Strategy::Ema => {
            let config = load_ema_config(&args.config)?;
            live.run(&config.bars.data_requirements(), EmaCross::new(config)?)
                .await
        }
        Strategy::Learned => {
            let config = load_learned_config(&args.config)?;
            let program = std::env::current_exe().context("finding this binary to retrain")?;
            let mode = Mode::Live {
                models: args.models.clone(),
                launcher: Launcher::process(program, retrain_args(&args)?),
                catalog_delay: live.recording.catalog_delay(),
            };
            live.run(
                &config.bars.data_requirements(),
                Learned::new(config, mode)?,
            )
            .await
        }
    }
}

/// The dataset runs of a process: the strategy over the catalog in
/// `Mode::Dataset`, on hedging venues (`model::targets`), with the costs,
/// balance and leverage given; the one node of the process.
struct DatasetRun {
    catalog: PathBuf,
    fee_model: FeeModelAny,
    balance: Money,
    leverage: Decimal,
    logging: LoggerConfig,
}

impl DatasetRun {
    /// The rows the learned strategy of `config` settles over `[start,
    /// end]`.
    fn replay(
        &self,
        config: &LearnedConfig,
        start: Timestamp,
        end: Timestamp,
    ) -> anyhow::Result<Vec<TargetRow>> {
        let requirements = config.bars.data_requirements();
        let rows = SettledRows::default();
        let mode = Mode::Dataset {
            rows: Rc::clone(&rows),
        };
        let backtest = Backtest {
            catalog: self.catalog.clone(),
            bar_types: requirements.bar_types,
            ticks: requirements.ticks,
            start,
            end,
            warmup: requirements.warmup,
            fee_model: self.fee_model.clone(),
            balance: self.balance,
            leverage: self.leverage,
            venue_oms: |_| OmsType::Hedging,
            logging: self.logging.clone(),
        };
        backtest.run_dataset(Learned::new(config.clone(), mode)?)?;
        Ok(rows.take())
    }
}

/// The arguments of the `train` a live node of `args` starts at each
/// retrain, but its `--at`: the node's strategy, config, catalog, model
/// store and leverage, and the retrains' costs.
///
/// # Errors
///
/// Returns an error if the commission or the balance of the retrains is
/// missing.
fn retrain_args(args: &LiveArgs) -> anyhow::Result<Vec<OsString>> {
    let costs = &args.retrain;
    let commission = match (&costs.retrain_commission, &costs.retrain_commission_rate) {
        (Some(commission), _) => ["--commission", commission],
        (None, Some(rate)) => ["--commission-rate", rate],
        (None, None) => bail!(
            "a strategy trading on a model retrains: give the commission of its \
             dataset runs (--retrain-commission or --retrain-commission-rate)"
        ),
    };
    let balance = costs.retrain_balance.as_deref().context(
        "a strategy trading on a model retrains: give the starting balance of its \
         dataset runs (--retrain-balance)",
    )?;
    let strategy = args
        .strategy
        .to_possible_value()
        .context("every strategy has a command-line name")?;
    let mut train: Vec<OsString> = vec!["train".into(), strategy.get_name().into()];
    for (flag, value) in [
        ("--config", args.config.as_os_str()),
        ("--catalog", args.catalog.as_os_str()),
        ("--models", args.models.as_os_str()),
    ] {
        train.extend([flag.into(), value.to_os_string()]);
    }
    train.extend(
        [
            "--leverage",
            &args.leverage.to_string(),
            commission[0],
            commission[1],
            "--balance",
            balance,
        ]
        .map(OsString::from),
    );
    Ok(train)
}

/// Prints the outcome of a train that published; one that kept the
/// current model is an error, its reason: the command fails, so that the
/// live node that started it knows why (`model::launcher`).
///
/// # Errors
///
/// Returns an error if the train kept the current model.
fn reported(outcome: &Outcome) -> anyhow::Result<()> {
    match outcome {
        Outcome::Published { .. } => {
            println!("{outcome}");
            Ok(())
        }
        Outcome::Kept(_) => bail!("{outcome}"),
    }
}

/// Trains the strategy's model at `--at` and reports the outcome (see
/// `reported`): the days its dataset lacks are replayed by a dataset run
/// (`DatasetRun`).
fn run_train(args: TrainArgs) -> anyhow::Result<()> {
    let run = DatasetRun {
        catalog: args.catalog.clone(),
        fee_model: args.commission.fee_model(),
        balance: args.balance,
        leverage: args.leverage,
        logging: logger_config(std::env::var("NAUTILUS_LOG"))?,
    };
    let outcome = match args.strategy {
        Strategy::Ema => bail!("the EMA strategy trades on no model"),
        Strategy::Learned => {
            let config = load_learned_config(&args.config)?;
            let training = learned::training(&config, &args.catalog, &args.models)?;
            training.train_at(args.at, |start, end| run.replay(&config, start, end))
        }
    };
    reported(&outcome)
}

/// Builds the strategy's dataset over the days from `--start` to the day
/// before `--end` (`dataset::days`), those it lacks, by one dataset run
/// (`DatasetRun`) to the last one's end plus the longest target, and
/// prints the days built.
fn run_dataset(args: DatasetArgs) -> anyhow::Result<()> {
    let (start, end) = args.run.window()?;
    let run = DatasetRun {
        catalog: args.run.catalog.clone(),
        fee_model: args.commission.fee_model(),
        balance: args.balance,
        leverage: args.leverage,
        logging: logger_config(std::env::var("NAUTILUS_LOG"))?,
    };
    let (first, last) = dataset::days(start, end);
    let built = match args.run.strategy {
        Strategy::Ema => bail!("the EMA strategy trades on no model"),
        Strategy::Learned => {
            let config = load_learned_config(&args.run.config)?;
            learned::dataset(&config, &args.run.catalog)?
                .build(first, last, |from, to| run.replay(&config, from, to))?
        }
    };
    match (built.first(), built.last()) {
        (Some(from), Some(to)) => {
            println!("built {} days of the dataset, {from} to {to}", built.len())
        }
        _ => println!("the dataset holds every day from {first} to {last}"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use helix_trader::model::train::Kept;

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

    #[test]
    fn parses_fill_catalog_with_its_client_id() {
        let args = parse(&REQUIRED).unwrap();
        assert_eq!(args.run.strategy, Strategy::Ema);
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
        assert_eq!(args.client_id, 10);
    }

    #[test]
    fn parses_a_backtest_with_its_costs() {
        let args = parse_backtest(&backtest_required()).unwrap();
        assert_eq!(args.run.strategy, Strategy::Ema);
        assert_eq!(args.run.config, PathBuf::from("ema.toml"));
        assert_eq!(
            args.run.end,
            "2024-03-01T12:30:00Z".parse::<Timestamp>().unwrap()
        );
        assert_eq!(args.run.catalog, PathBuf::from("catalog"));
        assert!(matches!(args.commission.fee_model(), FeeModelAny::Fixed(_)));
        assert_eq!(args.balance, Money::from("10000 EUR"));
    }

    /// `REQUIRED`, the balance and `commission`.
    fn backtest_with(commission: &[&'static str]) -> Vec<&'static str> {
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
    fn a_backtest_takes_either_the_commission_or_its_rate_not_both() {
        let both = backtest_with(&["--commission", "3 EUR", "--commission-rate", "0.0005"]);
        let err = parse_backtest(&both).unwrap_err().to_string();
        assert!(err.contains("cannot be used with"), "{err}");
        let err = parse_backtest(&backtest_with(&[])).unwrap_err().to_string();
        assert!(err.contains("--commission-rate"), "{err}");
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
            Command::Live(args) => args,
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

    #[test]
    fn parses_live_with_the_adapter_client_id() {
        let args = parse_live(&["ema", "--config", "ema.toml", "--account", "DU1234567"]).unwrap();
        assert_eq!(args.strategy, Strategy::Ema);
        assert_eq!(args.config, PathBuf::from("ema.toml"));
        assert_eq!(args.account, Some("DU1234567".parse().unwrap()));
        assert_eq!(args.client_id, 1);
        assert_eq!(args.leverage, Decimal::ONE);
        assert_eq!(args.catalog, PathBuf::from("catalog"));
    }

    #[test]
    fn live_stages_in_staging_and_promotes_every_five_minutes_unless_told_otherwise() {
        let args = parse_live(&["ema", "--config", "ema.toml"]).unwrap();
        assert_eq!(args.staging, PathBuf::from("staging"));
        assert_eq!(args.promotion_minutes, 5);
        let args = parse_live(&[
            "ema",
            "--config",
            "ema.toml",
            "--staging",
            "/data/staging",
            "--promotion-minutes",
            "15",
        ])
        .unwrap();
        assert_eq!(args.staging, PathBuf::from("/data/staging"));
        assert_eq!(args.promotion_minutes, 15);
    }

    #[test]
    fn live_refuses_promoting_every_zero_minutes() {
        let err = parse_live(&["ema", "--config", "ema.toml", "--promotion-minutes", "0"])
            .unwrap_err()
            .to_string();
        assert!(err.contains("--promotion-minutes"), "{err}");
    }

    #[test]
    fn live_takes_the_leverage_it_expects() {
        let args = parse_live(&["ema", "--config", "okx.toml", "--leverage", "3"]).unwrap();
        assert_eq!(args.leverage, Decimal::from(3));
        let err = parse_live(&["ema", "--config", "okx.toml", "--leverage", "x"]).unwrap_err();
        assert!(err.to_string().contains("--leverage"), "{err}");
    }

    /// Read from the arguments' definition: setting the variable would race
    /// the other tests, and the developer's environment may have it. Not
    /// required: a run on OKX alone has no IB account.
    #[test]
    fn the_live_account_falls_back_on_nautilus_ib_account_id() {
        use clap::CommandFactory;
        let cli = Cli::command();
        let live = cli.find_subcommand("live").unwrap();
        let account = live
            .get_arguments()
            .find(|arg| arg.get_id() == "account")
            .unwrap();
        assert_eq!(
            account.get_env(),
            Some(std::ffi::OsStr::new("NAUTILUS_IB_ACCOUNT_ID"))
        );
        assert!(!account.is_required_set());
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
            instrument_ids = []
            trade_instrument_id = "ESM4.CME"
            bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" }
            trade_size = "1"
            stop_loss_bps = 50
            take_profit_bps = 50
            hold = "1h"
            warmup = "720h"
            session = { time_zone = "America/Chicago", open = "08:30", close = "15:00" }
            "#,
        )
        .unwrap();
        let err = format!("{:#}", load_ema_config(&path).unwrap_err());
        assert!(
            err.contains("ema.toml") && err.contains("no instruments"),
            "{err}"
        );
        let missing = dir.path().join("missing.toml");
        let err = format!("{:#}", load_ema_config(&missing).unwrap_err());
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
        "learned",
        "--config",
        "learned.toml",
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
        assert_eq!(args.strategy, Strategy::Learned);
        assert_eq!(args.config, PathBuf::from("learned.toml"));
        assert_eq!(
            args.at,
            "2026-10-23T00:00:00Z".parse::<Timestamp>().unwrap()
        );
        assert_eq!(args.catalog, PathBuf::from("catalog"));
        assert_eq!(args.models, PathBuf::from("models"));
        assert!(matches!(
            args.commission.fee_model(),
            FeeModelAny::MakerTaker(_)
        ));
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
        let args = parse_live(&["learned", "--config", "learned.toml"]).unwrap();
        assert_eq!(args.models, PathBuf::from("models"));
    }

    #[test]
    fn a_train_keeping_the_model_fails_with_the_reason() {
        let kept = Outcome::Kept(Kept::BelowMinScore {
            score: 0.48,
            min_score: 0.55,
        });
        let published = Outcome::Published {
            valid_from: "2026-10-23T00:00:00Z".parse().unwrap(),
            score: 0.6,
        };

        let err = reported(&kept).unwrap_err().to_string();

        assert!(err.contains("0.48") && err.contains("0.55"), "{err}");
        assert!(reported(&published).is_ok());
    }

    /// The arguments of a learned live node retraining at a commission
    /// rate.
    const LIVE_RETRAINING: [&str; 7] = [
        "learned",
        "--config",
        "learned.toml",
        "--retrain-commission-rate",
        "0.00003",
        "--retrain-balance",
        "10000000 USD",
    ];

    #[test]
    fn live_retrains_through_train_with_its_files_leverage_and_costs() {
        let mut argv = LIVE_RETRAINING.to_vec();
        argv.extend(["--catalog", "/data/catalog", "--models", "/data/models"]);
        argv.extend(["--leverage", "2"]);

        let train = retrain_args(&parse_live(&argv).unwrap()).unwrap();

        assert_eq!(
            train,
            [
                "train",
                "learned",
                "--config",
                "learned.toml",
                "--catalog",
                "/data/catalog",
                "--models",
                "/data/models",
                "--leverage",
                "2",
                "--commission-rate",
                "0.00003",
                "--balance",
                "10000000 USD",
            ]
            .map(std::ffi::OsString::from)
        );
    }

    #[test]
    fn live_retrains_at_a_commission_per_order_too() {
        let mut argv = LIVE_RETRAINING.to_vec();
        argv.splice(3..5, ["--retrain-commission", "3 USD"]);

        let train = retrain_args(&parse_live(&argv).unwrap()).unwrap();

        assert!(
            train
                .windows(2)
                .any(|pair| pair == ["--commission", "3 USD"]),
            "{train:?}"
        );
    }

    #[test]
    fn live_retraining_without_its_costs_is_refused_naming_them() {
        for missing in [3, 5] {
            let argv = [&LIVE_RETRAINING[..missing], &LIVE_RETRAINING[missing + 2..]].concat();
            let err = retrain_args(&parse_live(&argv).unwrap())
                .unwrap_err()
                .to_string();
            assert!(err.contains(LIVE_RETRAINING[missing]), "{err}");
        }
    }

    #[test]
    fn live_refuses_retrain_costs_it_cannot_read() {
        for (flag, value) in [
            ("--retrain-commission-rate", "-0.1"),
            ("--retrain-commission", "3"),
            ("--retrain-balance", "lots"),
        ] {
            let argv = ["learned", "--config", "learned.toml", flag, value];
            assert!(parse_live(&argv).is_err(), "{flag} {value}");
        }
        let mut both = LIVE_RETRAINING.to_vec();
        both.extend(["--retrain-commission", "3 USD"]);
        assert!(parse_live(&both).is_err());
    }

    #[test]
    fn the_ema_strategy_has_no_model_to_train() {
        let mut argv = TRAIN.to_vec();
        argv[0] = "ema";
        let err = run_train(parse_train(&argv).unwrap()).unwrap_err();
        assert!(err.to_string().contains("no model"), "{err}");
    }

    fn parse_dataset(args: &[&str]) -> Result<DatasetArgs, clap::Error> {
        parse_command("dataset", args).map(|command| match command {
            Command::Dataset(args) => *args,
            command => panic!("{command:?}"),
        })
    }

    /// The arguments a dataset build requires.
    const DATASET: [&str; 11] = [
        "learned",
        "--config",
        "learned.toml",
        "--start",
        "2026-10-20",
        "--end",
        "2026-10-23",
        "--commission-rate",
        "0.00003",
        "--balance",
        "10000000 USD",
    ];

    #[test]
    fn parses_a_dataset_build_over_a_span_with_its_costs() {
        let args = parse_dataset(&DATASET).unwrap();
        assert_eq!(args.run.strategy, Strategy::Learned);
        assert_eq!(
            args.run.window().unwrap(),
            (
                "2026-10-20T00:00:00Z".parse::<Timestamp>().unwrap(),
                "2026-10-23T00:00:00Z".parse::<Timestamp>().unwrap()
            )
        );
        assert_eq!(args.run.catalog, PathBuf::from("catalog"));
        assert_eq!(args.balance, Money::from("10000000 USD"));
        assert_eq!(args.leverage, Decimal::ONE);
    }

    #[test]
    fn the_ema_strategy_has_no_dataset_to_build() {
        let mut argv = DATASET.to_vec();
        argv[0] = "ema";
        let err = run_dataset(parse_dataset(&argv).unwrap()).unwrap_err();
        assert!(err.to_string().contains("no model"), "{err}");
    }

    #[test]
    fn a_backtest_hands_its_children_the_commission_as_given() {
        let args = parse_backtest(&backtest_required()).unwrap();
        assert_eq!(args.commission.args(), ["--commission", "3 EUR"]);
        let args = parse_backtest(&backtest_with(&["--commission-rate", "0.00003"])).unwrap();
        assert_eq!(args.commission.args(), ["--commission-rate", "0.00003"]);
    }

    #[test]
    fn a_backtest_of_a_model_strategy_needs_a_dataset_balance() {
        let err = Children::of(&parse_backtest(&backtest_required()).unwrap())
            .err()
            .unwrap();
        assert!(err.to_string().contains("--dataset-balance"), "{err}");
        let mut argv = backtest_required();
        argv.extend(["--dataset-balance", "10000000 EUR"]);
        let children = Children::of(&parse_backtest(&argv).unwrap()).unwrap();
        assert_eq!(children.balance, Money::from("10000000 EUR"));
    }
}
