//! Command line: one subcommand per process (fill-catalog, backtest, paper), since
//! Nautilus allows one node per process. Every subcommand takes the strategy
//! name and its TOML config, the single source of instruments and bar types.

use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, ensure};
use clap::{Args, Parser, Subcommand, ValueEnum};
use helix_trader::{
    data::{IbGateway, IbHistory, fill_catalog},
    strategy::ema::EmaCrossConfig,
};
use jiff::{Timestamp, civil::Date, tz::TimeZone};
use nautilus_common::logging::{
    bridge::init_tracing, init_logging, logger::LoggerConfig, writer::FileWriterConfig,
};
use nautilus_core::UUID4;
use nautilus_model::{
    data::BarType,
    identifiers::{InstrumentId, TraderId},
};
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
}

#[derive(Debug, Args)]
struct FillCatalogArgs {
    /// The strategy whose instruments and bar types are filled.
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
    /// IB Gateway or TWS host.
    #[arg(long, default_value = "127.0.0.1")]
    host: String,
    /// IB Gateway or TWS port (4002: paper gateway).
    #[arg(long, default_value_t = 4002)]
    port: u16,
    /// IB client id of this process.
    #[arg(long, default_value_t = 1)]
    client_id: i32,
}

impl FillCatalogArgs {
    /// The window `[start, end]`, checked.
    fn window(&self) -> anyhow::Result<(Timestamp, Timestamp)> {
        let (start, end) = (self.start, self.end);
        ensure!(start < end, "start {start} must be before end {end}");
        Ok((start, end))
    }
}

/// The strategies, by command-line name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Strategy {
    Ema,
}

/// What a strategy config says to fill: its instruments and bar types.
#[derive(Debug)]
struct Universe {
    instrument_ids: Vec<InstrumentId>,
    bar_types: Vec<BarType>,
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

/// Reads the config of `strategy` from `path` and returns what it trades.
fn load_universe(strategy: Strategy, path: &Path) -> anyhow::Result<Universe> {
    let read = || -> anyhow::Result<Universe> {
        let text = fs::read_to_string(path)?;
        match strategy {
            Strategy::Ema => {
                let config: EmaCrossConfig = toml::from_str(&text)?;
                config.validate()?;
                Ok(Universe {
                    bar_types: config.bar_types(),
                    instrument_ids: config.instrument_ids,
                })
            }
        }
    };
    read().with_context(|| format!("loading the {strategy:?} config {}", path.display()))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match Cli::parse().command {
        Command::FillCatalog(args) => run_fill_catalog(args).await,
    }
}

/// Checks everything that needs no gateway, then connects to IB, fetches
/// the instruments and fills the catalog.
async fn run_fill_catalog(args: FillCatalogArgs) -> anyhow::Result<()> {
    let (start, end) = args.window()?;
    let universe = load_universe(args.strategy, &args.config)?;
    // No node runs in this process, so no node installs Nautilus' logger:
    // install it here, as a node would, so the `log` output of this crate and
    // Nautilus, and the adapter's `tracing` warnings (why a contract did not
    // resolve), reach the console. Configured by NAUTILUS_LOG and RUST_LOG.
    let _log_guard = init_logging(
        TraderId::default(),
        UUID4::new(),
        LoggerConfig::from_env().unwrap_or_default(),
        FileWriterConfig::default(),
    )?;
    init_tracing()?;
    let gateway = IbGateway {
        host: args.host,
        port: args.port,
        client_id: args.client_id,
    };
    let ib = IbHistory::connect(gateway, &universe.bar_types, end, Timestamp::now()).await?;
    let instruments = ib.instruments(&universe.instrument_ids).await?;
    fs::create_dir_all(&args.catalog)
        .with_context(|| format!("creating the catalog {}", args.catalog.display()))?;
    let mut catalog = ParquetDataCatalog::new(&args.catalog, None, None, None, None);
    fill_catalog(
        &mut catalog,
        instruments,
        &universe.bar_types,
        start,
        end,
        async |bar_type, from, to| ib.bars(bar_type, from, to).await,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<FillCatalogArgs, clap::Error> {
        let argv = ["helix-trader", "fill-catalog"].iter().chain(args);
        Cli::try_parse_from(argv).map(|cli| match cli.command {
            Command::FillCatalog(args) => args,
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

    #[test]
    fn parses_fill_catalog_with_paper_gateway_defaults() {
        let args = parse(&REQUIRED).unwrap();
        assert_eq!(args.strategy, Strategy::Ema);
        assert_eq!(args.config, PathBuf::from("ema.toml"));
        assert_eq!(
            args.start,
            "2024-01-02T00:00:00Z".parse::<Timestamp>().unwrap()
        );
        assert_eq!(
            args.end,
            "2024-03-01T12:30:00Z".parse::<Timestamp>().unwrap()
        );
        assert_eq!(args.catalog, PathBuf::from("catalog"));
        assert_eq!(args.host, "127.0.0.1");
        assert_eq!(args.port, 4002);
        assert_eq!(args.client_id, 1);
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
        assert_eq!(args.catalog, PathBuf::from("/data/cat"));
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
    fn loads_the_ema_universe_from_its_toml_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ema.toml");
        std::fs::write(
            &path,
            r#"
            instrument_ids = ["ESH4.CME", "ESM4.CME"]
            bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }
            trade_size = "1"
            warmup = "720h"
            "#,
        )
        .unwrap();
        let universe = load_universe(Strategy::Ema, &path).unwrap();
        assert_eq!(
            universe.instrument_ids,
            [
                InstrumentId::from("ESH4.CME"),
                InstrumentId::from("ESM4.CME")
            ]
        );
        let bar_types: Vec<String> = universe.bar_types.iter().map(ToString::to_string).collect();
        assert_eq!(
            bar_types,
            [
                "ESH4.CME-1-HOUR-LAST-EXTERNAL",
                "ESM4.CME-1-HOUR-LAST-EXTERNAL"
            ]
        );
    }

    #[test]
    fn rejects_an_invalid_ema_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ema.toml");
        std::fs::write(
            &path,
            r#"
            instrument_ids = []
            bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }
            trade_size = "1"
            warmup = "720h"
            "#,
        )
        .unwrap();
        let err = format!("{:#}", load_universe(Strategy::Ema, &path).unwrap_err());
        assert!(err.contains("no instruments"), "{err}");
    }

    #[test]
    fn rejects_a_start_not_before_the_end() {
        let mut argv = REQUIRED.to_vec();
        argv[4] = "2024-03-01T12:30:00Z";
        let err = parse(&argv).unwrap().window().unwrap_err().to_string();
        assert!(err.contains("before"), "{err}");
        assert!(parse(&REQUIRED).unwrap().window().is_ok());
    }

    #[test]
    fn a_config_error_names_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ema.toml");
        std::fs::write(&path, "instrument_ids = []").unwrap();
        let err = format!("{:#}", load_universe(Strategy::Ema, &path).unwrap_err());
        assert!(err.contains("ema.toml"), "{err}");
        let missing = dir.path().join("missing.toml");
        let err = format!("{:#}", load_universe(Strategy::Ema, &missing).unwrap_err());
        assert!(err.contains("missing.toml"), "{err}");
    }
}
