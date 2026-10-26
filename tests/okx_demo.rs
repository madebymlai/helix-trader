//! End to end on OKX's demo trading (EEA region): a live run on the BTC
//! X-Perp checks the real demo account (the credentials set, net position
//! mode) and is refused at a leverage no account has, naming the account's,
//! before any node is built. It needs the demo key (`OKX_API_KEY`,
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
//! NAUTILUS_OKX_RUN=1 cargo test --test okx_demo -- --nocapture
//! ```

use helix_trader::{
    live::Live,
    strategy::ema::{EmaCross, EmaCrossConfig},
};
use nautilus_common::logging::logger::LoggerConfig;
use nautilus_model::identifiers::TraderId;
use rust_decimal::Decimal;

/// Whether the run is opted in: from the shell, before `.env` is read, so
/// that the file cannot opt in.
fn opted_in(test: &str) -> bool {
    let run = std::env::var("NAUTILUS_OKX_RUN").is_ok_and(|value| value == "1");
    if !run {
        println!("{test}: skipped; set NAUTILUS_OKX_RUN=1 to run it on OKX's demo");
    }
    run
}

/// The EMA crossover on the BTC X-Perp the EEA demo lists (the real
/// account's is BTC-USD_UM_XPERP-310404), on hour bars of its 1-minute
/// candles, without hyphens in its order ids, which OKX refuses.
fn x_perp_ema_cross() -> EmaCrossConfig {
    let config: EmaCrossConfig = toml::from_str(
        r#"
        instrument_ids = ["BTC-USD_UM_XPERP-310328.OKX"]
        bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }
        composite = { step = 1, aggregation = "MINUTE", aggregation_source = "EXTERNAL" }
        trade_size = "1"
        warmup = "30m"

        [base]
        strategy_id = "EMA_CROSS-001"
        order_id_tag = "001"
        use_hyphens_in_client_order_ids = false
        "#,
    )
    .unwrap();
    config.validate().unwrap();
    config
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
    // Above OKX's highest leverage, 125.
    let leverage = Decimal::from(1000);
    let catalog = tempfile::tempdir().unwrap();
    let config = x_perp_ema_cross();
    let requirements = config.bars.data_requirements();
    let live = Live {
        trader_id: TraderId::from("HELIXTEST-001"),
        catalog: catalog.path().to_path_buf(),
        account: None,
        client_id: 1,
        leverage,
        logging: LoggerConfig::default(),
    };

    let err = live
        .run(&requirements, EmaCross::new(config).unwrap())
        .await
        .unwrap_err()
        .to_string();

    assert!(err.contains("not 1000"), "{err}");
    assert!(err.contains("BTC-USD_UM_XPERP-310328"), "{err}");
}
