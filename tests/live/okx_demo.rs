//! Tests end to end on OKX's demo trading (EEA region). A live run on the BTC X-Perp does two
//! things:
//!
//! - It checks the real demo account: the credentials are set and the position mode is net.
//! - It is refused for a catalog that defines the X-Perp at a leverage no account has, a margin of
//!   1 / 1000. The refusal names the account's leverage, and comes before any node is built.
//!
//! The test needs the demo key (`OKX_API_KEY`, `OKX_API_SECRET`, `OKX_API_PASSPHRASE`) and
//! `OKX_ENVIRONMENT=demo` from the environment or `.env`. Any other environment fails the test
//! before any request. Another failing check fails the test with its own message.
//!
//! The test is gated like `ib_gateway.rs`, by `NAUTILUS_OKX_RUN=1` in the shell. It only reads the
//! account. Run it with:
//!
//! ```sh
//! NAUTILUS_OKX_RUN=1 cargo test --test live okx_demo:: -- --nocapture
//! ```

use helix_trader::{
    live::Live,
    strategy::ema::{EmaCross, EmaCrossConfig},
};
use nautilus_common::logging::logger::LoggerConfig;
use nautilus_model::{
    identifiers::{InstrumentId, Symbol, TraderId},
    instruments::{InstrumentAny, stubs::crypto_future_btcusdt},
    types::{Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rust_decimal::Decimal;

/// Returns whether the run is opted in.
///
/// Reads the opt-in from the shell before `.env` is read, so the file cannot opt in. An opted-in
/// run claims the process (`claim_process`).
fn opted_in(test: &str) -> bool {
    let run = std::env::var("NAUTILUS_OKX_RUN").is_ok_and(|value| value == "1");
    if run {
        crate::claim_process(module_path!());
    } else {
        println!("{test}: skipped; set NAUTILUS_OKX_RUN=1 to run it on OKX's demo");
    }
    run
}

/// The EMA crossover on the BTC X-Perp the EEA demo lists.
///
/// The real account's X-Perp is BTC-USD_UM_XPERP-310404. The strategy trades hour bars of its
/// 1-minute candles, and leaves hyphens out of its order ids, which OKX refuses.
fn x_perp_ema_cross() -> EmaCrossConfig {
    EmaCrossConfig::load(
        r#"
        [base]
        strategy_id = "EMA_CROSS-001"
        use_hyphens_in_client_order_ids = false

        [data]
        series = [
            { data = "bars", instrument_id = "BTC-USD_UM_XPERP-310328.OKX", bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }, from = { step = 1, aggregation = "MINUTE", aggregation_source = "EXTERNAL" } },
        ]
        warmup = "30m"

        [indicators]
        fast = { period = 10 }
        slow = { period = 30 }

        [trade]
        instruments = [{ instrument_id = "BTC-USD_UM_XPERP-310328.OKX" }]
        risk = "0.5%"
        "#,
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_okx_run_passes_the_account_checks_then_is_refused_at_another_leverage() {
    if !opted_in("an_okx_run_passes_the_account_checks_then_is_refused_at_another_leverage") {
        return;
    }
    dotenvy::dotenv().ok();
    assert_eq!(
        std::env::var("OKX_ENVIRONMENT").as_deref(),
        Ok("demo"),
        "the OKX end to end test runs on the demo only: set OKX_ENVIRONMENT=demo"
    );
    let config = x_perp_ema_cross();
    let requirements = config.data_requirements();

    // A catalog defines the X-Perp at a margin of 1 / 1000, above OKX's highest leverage, 125, so
    // the run is refused before any node is built.
    let catalog = tempfile::tempdir().unwrap();
    let mut xperp = crypto_future_btcusdt(2, 6, Price::from("0.01"), Quantity::from("0.000001"));
    xperp.id = InstrumentId::from("BTC-USD_UM_XPERP-310328.OKX");
    xperp.raw_symbol = Symbol::from("BTC-USD_UM_XPERP-310328");
    xperp.margin_init = Decimal::ONE / Decimal::from(1000);
    xperp.margin_maint = xperp.margin_init;
    ParquetDataCatalog::new(catalog.path(), None, None, None, None)
        .write_instruments(vec![InstrumentAny::CryptoFuture(xperp)])
        .unwrap();

    // Outside the catalog, as `Live` requires
    let staging = tempfile::tempdir().unwrap();
    let live = Live {
        trader_id: TraderId::from("HELIXTEST-001"),
        venues: crate::venues(&[]),
        logging: LoggerConfig::default(),
        catalog: catalog.path().to_path_buf(),
        recording: crate::recording(staging.path()),
    };

    let strategy = EmaCross::live(config).unwrap();
    let err = Box::pin(live.run(&requirements, &strategy.warmup(), strategy, None))
        .await
        .unwrap_err()
        .to_string();

    // The refusal names the X-Perp, the catalog's leverage (1000) and the account's (at most 125)
    let numbers: Vec<f64> = err
        .split(|c: char| !c.is_ascii_digit() && c != '.')
        .filter_map(|number| number.trim_matches('.').parse().ok())
        .collect();
    assert!(err.contains("BTC-USD_UM_XPERP-310328.OKX"), "{err}");
    assert!(numbers.contains(&1000.0), "{err}");
    assert!(
        numbers
            .iter()
            .any(|leverage| (1.0..=125.0).contains(leverage)),
        "{err}"
    );

    // No node was built, so the staging holds only the trader's instance id, which is kept before
    // the clients are set up.
    let staged: Vec<String> = std::fs::read_dir(staging.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(staged, ["HELIXTEST-001.instance-id"]);
}
