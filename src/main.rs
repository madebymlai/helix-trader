//! Command line: one subcommand per process (fill-catalog, backtest, live), since
//! Nautilus allows one node per process. Every subcommand takes the strategy
//! name and its TOML config: the instruments and the warmup come from the
//! config's data requirements.

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, ensure};
use clap::{Args, Parser, Subcommand, ValueEnum};
use helix_trader::{
    backtest::{self, Backtest},
    data::{DatabentoHistory, IbHistory, IcHistory, OkxHistory, fill_catalog},
    live::{IbAccount, Live},
    strategy::{
        DataRequirements, catalog_series,
        ema::{EmaCross, EmaCrossConfig},
    },
};
use jiff::Timestamp;
use nautilus_common::logging::{
    bridge::init_tracing, init_logging, logger::LoggerConfig, writer::FileWriterConfig,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_execution::models::fee::FeeModelAny;
use nautilus_model::{
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
}

/// The commission of a backtest: exactly one of the flags.
#[derive(Debug, Args)]
#[group(required = true, multiple = false)]
struct CommissionArgs {
    /// Commission charged once per order, for example "3 EUR".
    #[arg(long, value_parser = parse_commission)]
    commission: Option<FeeModelAny>,
    /// Commission rate on the value of each fill, with no minimum, for
    /// example 0.0005 (0.05%).
    #[arg(long, value_parser = parse_commission_rate)]
    commission_rate: Option<FeeModelAny>,
}

impl CommissionArgs {
    /// The fee model of the one flag given.
    fn fee_model(self) -> FeeModelAny {
        self.commission
            .or(self.commission_rate)
            .expect("clap requires one commission flag")
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
}

/// The strategies, by command-line name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Strategy {
    Ema,
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
fn parse_commission(s: &str) -> anyhow::Result<FeeModelAny> {
    backtest::commission_per_order(parse_money(s).map_err(anyhow::Error::msg)?)
}

/// A commission rate: a decimal, not negative.
fn parse_commission_rate(s: &str) -> anyhow::Result<FeeModelAny> {
    backtest::commission_rate(s.parse::<Decimal>()?)
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
    }
}

/// Checks everything that needs no gateway, then fills the catalog with the
/// instruments and the series their bars are built from (see
/// `strategy::catalog_series`) from each instrument's source: OKX's
/// archive for OKX instruments (their 1-minute candles), Databento's
/// historical API for GLBX instruments (their MBP-10 book and trades, the
/// cost quoted first), IC Markets' cTrader for IC instruments, IB
/// (connected only if needed) for the others.
async fn run_fill_catalog(args: FillCatalogArgs) -> anyhow::Result<()> {
    let (start, end) = args.run.window()?;
    let requirements = match args.run.strategy {
        Strategy::Ema => load_ema_config(&args.run.config)?.bars.data_requirements(),
    };
    // No node runs in this process, so no node installs Nautilus' logger:
    // install it here, as a node would, so the `log` output of this crate and
    // Nautilus, and the adapter's `tracing` warnings (why a contract did not
    // resolve), reach the console. Configured by NAUTILUS_LOG and RUST_LOG.
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
    if !okx_ids.is_empty() {
        let series = catalog_series(&requirements.of_instruments(&okx_ids).bar_types);
        OkxHistory::new()?
            .fill(&mut catalog, &series, start, end)
            .await?;
    }
    if !glbx_ids.is_empty() {
        let api_key = std::env::var("DATABENTO_API_KEY").context("reading DATABENTO_API_KEY")?;
        DatabentoHistory::new(&api_key)?
            .fill(&catalog, &glbx_ids, start, end)
            .await?;
    }
    if !ic_ids.is_empty() {
        let series = catalog_series(&requirements.of_instruments(&ic_ids).bar_types);
        let ic = IcHistory::connect().await?;
        let filled = ic.fill(&mut catalog, &series, start, end).await;
        ic.disconnect().await;
        filled?;
    }
    if !ib_ids.is_empty() {
        let ib = IbHistory::connect(args.client_id, end, Timestamp::now()).await?;
        let instruments = ib.instruments(&ib_ids).await?;
        let series = catalog_series(&requirements.of_instruments(&ib_ids).bar_types);
        fill_catalog(
            &mut catalog,
            instruments,
            &series,
            start,
            end,
            async |data_type, identifier, from, to| ib.data(data_type, identifier, from, to).await,
        )
        .await?;
    }
    Ok(())
}

/// Builds the strategy from its config, runs it over the window of the
/// catalog and prints the results to stdout.
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
        warmup: requirements.warmup,
        fee_model,
        balance: args.balance,
        leverage: args.leverage,
        venue_oms: backtest::venue_oms_type,
        logging,
    };
    let outcome = match args.run.strategy {
        Strategy::Ema => {
            let config = load_ema_config(&args.run.config)?;
            backtest(config.bars.data_requirements()).run(EmaCross::new(config)?)?
        }
    };
    print!("{}", backtest::report(&outcome));
    Ok(())
}

/// Builds the strategy from its config and runs it live until Ctrl-C.
async fn run_live(args: LiveArgs) -> anyhow::Result<()> {
    let live = Live {
        trader_id: args.trader_id,
        account: args.account,
        client_id: args.client_id,
        leverage: args.leverage,
        logging: logger_config(std::env::var("NAUTILUS_LOG"))?,
        catalog: args.catalog,
    };
    match args.strategy {
        Strategy::Ema => {
            let config = load_ema_config(&args.config)?;
            live.run(&config.bars.data_requirements(), EmaCross::new(config)?)
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
