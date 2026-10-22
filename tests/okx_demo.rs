//! End to end on OKX's demo trading (EEA region): the live node starts on
//! the BTC X-Perp, checks the account (net position mode, the leverage),
//! starts warming up on live bars and stops cleanly on SIGINT. It needs the
//! demo key (`OKX_API_KEY`, `OKX_API_SECRET`, `OKX_API_PASSPHRASE`) from the
//! environment or `.env`, and the account's leverage on the X-Perp in
//! `NAUTILUS_OKX_LEVERAGE` (1 if unset).
//!
//! Gated as `ib_gateway.rs` is: the test returns at once, saying it was
//! skipped, unless `NAUTILUS_OKX_RUN=1` is set in the shell. It places no
//! order: the EMA needs 30 one-minute bars before its first target, and the
//! run lasts under three minutes. Run it with:
//!
//! ```sh
//! NAUTILUS_OKX_RUN=1 NAUTILUS_OKX_LEVERAGE=3 cargo test --test okx_demo -- --nocapture
//! ```
//!
//! With `nautilus-okx` 0.66.0 the X-Perp gets no quote, hence no bar: the
//! adapter subscribes it on the spread channel, which OKX refuses
//! (nautilus_trader#5337, fixed after 0.66.0). The run still starts,
//! reconciles and stops cleanly.
//!
//! One node per process, so one run per file.

use std::{process::Command, time::Duration};

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

/// Long enough for the first full one-minute bar: the first, partial one
/// is dropped.
const LIVE_RUN: Duration = Duration::from_secs(150);

#[tokio::test(flavor = "multi_thread")]
async fn live_runs_on_the_okx_demo_and_stops_cleanly_on_sigint() {
    if !opted_in("live_runs_on_the_okx_demo_and_stops_cleanly_on_sigint") {
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
    // As Ctrl-C would, once the node runs: its signal handler stops it.
    // The node is not `Send`, so it runs here and the signal is sent from
    // another task.
    let interrupt = tokio::spawn(async {
        tokio::time::sleep(LIVE_RUN).await;
        let status = Command::new("kill")
            .args(["-INT", &std::process::id().to_string()])
            .status()
            .unwrap();
        assert!(status.success());
    });
    let ran = tokio::time::timeout(
        LIVE_RUN + Duration::from_secs(60),
        live.run(&requirements, EmaCross::new(config).unwrap()),
    )
    .await;
    let interrupted = interrupt.is_finished();
    interrupt.abort();
    ran.expect("the node stops within a minute of SIGINT")
        .unwrap();
    assert!(interrupted, "the node stopped before SIGINT");
}
