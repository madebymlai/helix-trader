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
    data::{IbGateway, IbHistory, fill_catalog},
    live::{IbAccount, Live},
    strategy::{
        DataRequirements,
        ema::{EmaCross, EmaCrossConfig},
    },
};
use jiff::{Timestamp, civil::Date, tz::TimeZone};
use nautilus_common::logging::{
    bridge::init_tracing, init_logging, logger::LoggerConfig, writer::FileWriterConfig,
};
use nautilus_core::UUID4;
use nautilus_model::{identifiers::TraderId, types::Money};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Fills the Nautilus catalog with the strategy's history from Interactive Brokers.
    FillCatalog(FillCatalogArgs),
    /// Backtests the strategy over a window of the catalog and prints the results.
    Backtest(BacktestArgs),
    /// Runs the strategy on a live node with Interactive Brokers, after
    /// filling the catalog up to now; stops on Ctrl-C.
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
    /// IB Gateway or TWS host.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    /// IB Gateway or TWS port (4002: paper gateway).
    #[arg(long, default_value_t = 4002)]
    port: u16,
    /// IB client id of this process; not the adapter default 1, so a live
    /// node with the default id can run at the same time.
    #[arg(long, default_value_t = 10)]
    client_id: i32,
}

#[derive(Debug, Args)]
struct BacktestArgs {
    #[command(flatten)]
    run: RunArgs,
    /// Commission charged once per order, for example "3 EUR".
    #[arg(long, value_parser = parse_money)]
    commission: Money,
    /// Starting balance of the account, for example "10000 EUR".
    #[arg(long, value_parser = parse_money)]
    balance: Money,
}

#[derive(Debug, Args)]
struct LiveArgs {
    /// The strategy to run.
    strategy: Strategy,
    /// The strategy's TOML config.
    #[arg(long)]
    config: PathBuf,
    /// The catalog directory, filled up to now at start.
    #[arg(long, default_value = "catalog")]
    catalog: PathBuf,
    /// The IB account traded, for example DU1234567 (paper).
    #[arg(long)]
    account: IbAccount,
    /// IB Gateway or TWS host.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    /// IB Gateway or TWS port (4002: paper gateway); must match the account.
    #[arg(long, default_value_t = 4002)]
    port: u16,
    /// IB client id of the node, the adapter default.
    #[arg(long, default_value_t = 1)]
    client_id: i32,
}

/// The strategies, by command-line name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Strategy {
    Ema,
}

/// An RFC 3339 timestamp, or a date taken as midnight UTC.
fn parse_time(s: &str) -> anyhow::Result<Timestamp> {
    if let Ok(timestamp) = s.parse::<Timestamp>() {
        return Ok(timestamp);
    }
    let date: Date = s
        .parse()
        .with_context(|| format!("{s:?} is neither a date nor an RFC 3339 timestamp"))?;
    Ok(date.to_zoned(TimeZone::UTC)?.timestamp())
}

/// An amount and a currency code, for example "3 EUR". Explicit, since
/// clap would otherwise pick `Money: From<&str>`, which panics.
fn parse_money(s: &str) -> Result<Money, String> {
    s.parse()
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
    match Cli::parse().command {
        Command::FillCatalog(args) => run_fill_catalog(args).await,
        Command::Backtest(args) => run_backtest(args),
        Command::Live(args) => run_live(args).await,
    }
}

/// Checks everything that needs no gateway, then connects to IB, fetches
/// the instruments and fills the catalog with their quotes. No bars: the
/// strategies' bars are built from the quotes.
async fn run_fill_catalog(args: FillCatalogArgs) -> anyhow::Result<()> {
    let (start, end) = args.run.window()?;
    let requirements = match args.run.strategy {
        Strategy::Ema => load_ema_config(&args.run.config)?.data_requirements(),
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
    let gateway = IbGateway {
        host: args.host,
        port: args.port,
        client_id: args.client_id,
    };
    let ib = IbHistory::connect(gateway, end, Timestamp::now()).await?;
    let instruments = ib.instruments(&requirements.instrument_ids()).await?;
    let catalog_dir = &args.run.catalog;
    fs::create_dir_all(catalog_dir)
        .with_context(|| format!("creating the catalog {}", catalog_dir.display()))?;
    let mut catalog = ParquetDataCatalog::new(catalog_dir, None, None, None, None);
    fill_catalog(
        &mut catalog,
        instruments,
        start,
        end,
        async |id, from, to| ib.quotes(id, from, to).await,
    )
    .await
}

/// Builds the strategy from its config, runs it over the window of the
/// catalog and prints the results to stdout.
fn run_backtest(args: BacktestArgs) -> anyhow::Result<()> {
    let (start, end) = args.run.window()?;
    // The node installs Nautilus' logger from its engine config: pass the
    // NAUTILUS_LOG config through, rather than install a logger here.
    let logging = logger_config(std::env::var("NAUTILUS_LOG"))?;
    let backtest = |requirements: DataRequirements| Backtest {
        catalog: args.run.catalog.clone(),
        bar_types: requirements.bar_types,
        start,
        end,
        warmup: requirements.warmup,
        commission: args.commission,
        balance: args.balance,
        logging,
    };
    let result = match args.run.strategy {
        Strategy::Ema => {
            let config = load_ema_config(&args.run.config)?;
            backtest(config.data_requirements()).run(EmaCross::new(config)?)?
        }
    };
    print!("{}", backtest::report(&result));
    Ok(())
}

/// Builds the strategy from its config and runs it live until Ctrl-C.
async fn run_live(args: LiveArgs) -> anyhow::Result<()> {
    let live = Live {
        catalog: args.catalog,
        gateway: IbGateway {
            host: args.host,
            port: args.port,
            client_id: args.client_id,
        },
        account: args.account,
        logging: logger_config(std::env::var("NAUTILUS_LOG"))?,
    };
    match args.strategy {
        Strategy::Ema => {
            let config = load_ema_config(&args.config)?;
            live.run(&config.data_requirements(), EmaCross::new(config)?)
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::identifiers::InstrumentId;

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
            Command::Backtest(args) => args,
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
    fn parses_fill_catalog_with_paper_gateway_defaults() {
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
        assert_eq!(args.host, "127.0.0.1");
        assert_eq!(args.port, 4002);
        assert_eq!(args.client_id, 10);
    }

    #[test]
    fn parses_catalog_and_gateway_overrides() {
        let mut argv = REQUIRED.to_vec();
        argv.extend([
            "--catalog",
            "/data/cat",
            "--host",
            "10.0.0.5",
            "--port",
            "7497",
            "--client-id",
            "42",
        ]);
        let args = parse(&argv).unwrap();
        assert_eq!(args.run.catalog, PathBuf::from("/data/cat"));
        assert_eq!(args.host, "10.0.0.5");
        assert_eq!(args.port, 7497);
        assert_eq!(args.client_id, 42);
    }

    #[test]
    fn rejects_an_unknown_strategy_and_a_bad_date() {
        let mut argv = REQUIRED.to_vec();
        argv[0] = "rsi";
        assert!(parse(&argv).is_err());
        let mut argv = REQUIRED.to_vec();
        argv[4] = "2024-13-01";
        assert!(parse(&argv).is_err());
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
        assert_eq!(args.commission, Money::from("3 EUR"));
        assert_eq!(args.balance, Money::from("10000 EUR"));
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

    fn parse_live(args: &[&str]) -> Result<LiveArgs, clap::Error> {
        parse_command("live", args).map(|command| match command {
            Command::Live(args) => args,
            command => panic!("{command:?}"),
        })
    }

    #[test]
    fn parses_live_with_the_paper_gateway_and_the_adapter_client_id() {
        let args = parse_live(&["ema", "--config", "ema.toml", "--account", "DU1234567"]).unwrap();
        assert_eq!(args.strategy, Strategy::Ema);
        assert_eq!(args.config, PathBuf::from("ema.toml"));
        assert_eq!(args.catalog, PathBuf::from("catalog"));
        assert_eq!(args.account, "DU1234567".parse().unwrap());
        assert_eq!(args.host, "127.0.0.1");
        assert_eq!(args.port, 4002);
        assert_eq!(args.client_id, 1);
    }

    #[test]
    fn live_requires_a_well_formed_account() {
        let err = parse_live(&["ema", "--config", "ema.toml"]).unwrap_err();
        assert!(err.to_string().contains("--account"), "{err}");
        let err = parse_live(&["ema", "--config", "ema.toml", "--account", "IB-DU1"]).unwrap_err();
        assert!(err.to_string().contains("--account"), "{err}");
    }

    #[test]
    fn loads_and_validates_the_ema_config_from_toml() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ema.toml");
        std::fs::write(
            &path,
            r#"
            instrument_ids = ["ESH4.CME", "ESM4.CME"]
            bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" }
            trade_size = "1"
            warmup = "720h"
            session = { time_zone = "America/Chicago", open = "08:30", close = "15:00" }
            "#,
        )
        .unwrap();
        let config = load_ema_config(&path).unwrap();
        assert_eq!(
            config.instrument_ids,
            [
                InstrumentId::from("ESH4.CME"),
                InstrumentId::from("ESM4.CME")
            ]
        );
        assert_eq!(config.warmup, jiff::SignedDuration::from_hours(720));
    }

    #[test]
    fn rejects_an_invalid_ema_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ema.toml");
        std::fs::write(
            &path,
            r#"
            instrument_ids = []
            bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" }
            trade_size = "1"
            warmup = "720h"
            session = { time_zone = "America/Chicago", open = "08:30", close = "15:00" }
            "#,
        )
        .unwrap();
        let err = format!("{:#}", load_ema_config(&path).unwrap_err());
        assert!(err.contains("no instruments"), "{err}");
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
        std::fs::write(&path, "instrument_ids = []").unwrap();
        let err = format!("{:#}", load_ema_config(&path).unwrap_err());
        assert!(err.contains("ema.toml"), "{err}");
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
