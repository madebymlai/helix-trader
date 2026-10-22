//! End to end on OKX's demo trading (EEA region): a live run on the BTC
//! X-Perp passes the start checks against the real demo account (the
//! credentials set, net position mode, the leverage) and is then refused
//! for lack of recorded quotes, before any node is built. It needs the demo
//! key (`OKX_API_KEY`, `OKX_API_SECRET`, `OKX_API_PASSPHRASE`) from the
//! environment or `.env`, and the account's leverage on the X-Perp in
//! `NAUTILUS_OKX_LEVERAGE` (1 if unset): a failing check would fail the
//! test with its own message.
//!
//! Gated as `ib_gateway.rs` is: the test returns at once, saying it was
//! skipped, unless `NAUTILUS_OKX_RUN=1` is set in the shell. It only reads
//! the account and places no order. Run it with:
//!
//! ```sh
//! NAUTILUS_OKX_RUN=1 NAUTILUS_OKX_LEVERAGE=3 cargo test --test okx_demo -- --nocapture
//! ```

use helix_trader::{
    live::Live,
    strategy::ema::{EmaCross, EmaCrossConfig},
};
use nautilus_common::logging::logger::LoggerConfig;
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
/// account's is BTC-USD_UM_XPERP-310404), on one-minute MID bars, without
/// hyphens in its order ids, which OKX refuses.
fn x_perp_ema_cross() -> EmaCrossConfig {
    let config: EmaCrossConfig = toml::from_str(
        r#"
        instrument_ids = ["BTC-USD_UM_XPERP-310328.OKX"]
        bar_spec = { step = 1, aggregation = "MINUTE", price_type = "MID" }
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
async fn an_okx_run_passes_the_account_checks_then_is_refused_for_lack_of_recorded_quotes() {
    if !opted_in("an_okx_run_passes_the_account_checks_then_is_refused_for_lack_of_recorded_quotes")
    {
        return;
    }
    dotenvy::dotenv().ok();
    let leverage: Decimal = std::env::var("NAUTILUS_OKX_LEVERAGE")
        .map_or(Ok(Decimal::ONE), |leverage| leverage.parse())
        .expect("NAUTILUS_OKX_LEVERAGE, a decimal");
    let catalog = tempfile::tempdir().unwrap();
    let config = x_perp_ema_cross();
    let requirements = config.data_requirements();
    let live = Live {
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

    assert!(err.contains("OKX live needs recorded quotes"), "{err}");
}
