//! End to end on OKX's demo trading (EEA region): a live run on the BTC
//! X-Perp checks the real demo account (the credentials set, net position
//! mode) and is refused for a catalog defining it at a leverage no account
//! has, a margin of 1 / 1000, naming the account's, before any node is
//! built. It needs the demo key (`OKX_API_KEY`,
//! `OKX_API_SECRET`, `OKX_API_PASSPHRASE`) and `OKX_ENVIRONMENT=demo` from
//! the environment or `.env` (any other environment fails the test before
//! any request): another failing check would fail the test with its own
//! message.
//!
//! Gated as `ib_gateway.rs` is: the test returns at once, saying it was
//! skipped, unless `NAUTILUS_OKX_RUN=1` is set in the shell. It only reads
//! the account and places no order. Run it with:
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

/// Whether the run is opted in: from the shell, before `.env` is read, so
/// that the file cannot opt in.
/// An opted-in run claims the process for this module (`claim_process`).
fn opted_in(test: &str) -> bool {
    let run = std::env::var("NAUTILUS_OKX_RUN").is_ok_and(|value| value == "1");
    if run {
        crate::claim_process(module_path!());
    } else {
        println!("{test}: skipped; set NAUTILUS_OKX_RUN=1 to run it on OKX's demo");
    }
    run
}

/// The EMA crossover on the BTC X-Perp the EEA demo lists (the real
/// account's is BTC-USD_UM_XPERP-310404), on hour bars of its 1-minute
/// candles, without hyphens in its order ids, which OKX refuses.
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
    // A catalog of its own defining the X-Perp at a margin of 1 / 1000,
    // above OKX's highest leverage, 125: the run is refused before any node
    // is built, so nothing else reads it.
    let catalog = tempfile::tempdir().unwrap();
    let mut xperp = crypto_future_btcusdt(2, 6, Price::from("0.01"), Quantity::from("0.000001"));
    xperp.id = InstrumentId::from("BTC-USD_UM_XPERP-310328.OKX");
    xperp.raw_symbol = Symbol::from("BTC-USD_UM_XPERP-310328");
    xperp.margin_init = Decimal::ONE / Decimal::from(1000);
    xperp.margin_maint = xperp.margin_init;
    ParquetDataCatalog::new(catalog.path(), None, None, None, None)
        .write_instruments(vec![InstrumentAny::CryptoFuture(xperp)])
        .unwrap();
    // Outside the catalog, as `Live` requires.
    let staging = tempfile::tempdir().unwrap();
    let live = Live {
        trader_id: TraderId::from("HELIXTEST-001"),
        venues: crate::venues(&[]),
        logging: LoggerConfig::default(),
        catalog: catalog.path().to_path_buf(),
        recording: crate::recording(staging.path()),
    };

    let strategy = EmaCross::live(config).unwrap();
    let err = live
        .run(&requirements, &strategy.warmup(), strategy, None)
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("catalog defines it at leverage 1000"), "{err}");
    assert!(err.contains("BTC-USD_UM_XPERP-310328"), "{err}");
}
