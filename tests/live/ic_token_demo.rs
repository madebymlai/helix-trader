//! End to end on IC Markets' cTrader demo, the token pair: a forced
//! renewal of the access token on a running session, which goes on
//! receiving quotes and answering requests; then a fresh session
//! authenticates from the token file without renewing. It ROTATES the
//! account's real token pair, in IC's token file, the one `ic::connection`
//! names (`$XDG_STATE_HOME/helix-trader/ic-token.json`, by default
//! `~/.local/state/helix-trader/ic-token.json`): the file keeps the new
//! pair, `.env`'s is spent. It needs what `ic_demo.rs` needs, through the
//! cTrader adapter's hooks (its `test-support` feature).
//!
//! Gated as `ic_demo.rs` is: the test returns at once, saying it was
//! skipped, unless `NAUTILUS_IC_RUN=1` is set in the shell; demo only, no
//! order. Its own module, run alone (`main.rs`), so that the other IC demo
//! tests' sessions do not run on the pair it rotates. Run it with:
//!
//! ```sh
//! NAUTILUS_IC_RUN=1 cargo test --test live ic_token_demo:: -- --nocapture
//! ```

use std::{os::unix::fs::PermissionsExt, sync::Mutex, time::Duration};

use ctrader::{CTraderSession, SpotEvents};
use helix_trader::venues::ic;
use jiff::{SignedDuration, Timestamp};
use nautilus_model::identifiers::InstrumentId;

/// Whether the run is opted in: from the shell, before `.env` is read, so
/// that the file cannot opt in.
/// An opted-in run claims the process for this module (`claim_process`).
fn opted_in(test: &str) -> bool {
    let run = std::env::var("NAUTILUS_IC_RUN").is_ok_and(|value| value == "1");
    if run {
        crate::claim_process(module_path!());
    } else {
        println!("{test}: skipped; set NAUTILUS_IC_RUN=1 to run it on IC's cTrader demo");
    }
    run
}

/// Reads `.env` and refuses any environment but the demo.
fn demo_only() {
    dotenvy::dotenv().ok();
    assert_eq!(
        std::env::var("IC_ENVIRONMENT").as_deref(),
        Ok("demo"),
        "the IC end to end test runs on the demo only: set IC_ENVIRONMENT=demo"
    );
}

const XAUUSD: &str = "XAUUSD.ICMARKETS";
const EURUSD: &str = "EURUSD.ICMARKETS";

/// The log records of the cTrader adapter at info and above, for the test
/// to show: none holds a token.
struct Captured(Mutex<Vec<String>>);

impl log::Log for Captured {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.level() <= log::Level::Info && metadata.target().starts_with("ctrader")
    }

    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            let line = format!("{} {}", record.level(), record.args());
            self.0.lock().unwrap().push(line);
        }
    }

    fn flush(&self) {}
}

static CAPTURED: Captured = Captured(Mutex::new(Vec::new()));

/// Prints the records captured when dropped, the test failing or not.
struct ShowLogs;

impl Drop for ShowLogs {
    fn drop(&mut self) {
        for line in CAPTURED.0.lock().unwrap().iter() {
            println!("log: {line}");
        }
    }
}

/// When the next spot event was stamped.
async fn next_spot_at(spots: &mut SpotEvents, timeout: Duration) -> Timestamp {
    let spot = tokio::time::timeout(timeout, spots.recv())
        .await
        .expect("no spot event in time")
        .expect("the spot events ended");
    Timestamp::from_millisecond(spot.timestamp.unwrap_or_default()).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_demo_session_renews_its_token_and_keeps_receiving_quotes() {
    if !opted_in("the_demo_session_renews_its_token_and_keeps_receiving_quotes") {
        return;
    }
    demo_only();
    let _ = log::set_logger(&CAPTURED);
    log::set_max_level(log::LevelFilter::Info);
    let _logs = ShowLogs;
    let connection = ic::connection().unwrap();
    let credential = &connection.credential;
    let stored = || credential.stored_tokens().unwrap();
    let ids = [InstrumentId::from(XAUUSD), InstrumentId::from(EURUSD)];
    let (session, mut spots) = CTraderSession::connect_with_spots(&connection, &ids)
        .await
        .unwrap();
    next_spot_at(&mut spots, Duration::from_secs(30)).await;
    let before = stored();

    session.renew_token();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while stored().same_access_token(&before) {
        assert!(tokio::time::Instant::now() < deadline, "no renewal in 30 s");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let renewed_at = Timestamp::now();
    let after = loop {
        let at = next_spot_at(&mut spots, Duration::from_secs(60)).await;
        if at > renewed_at {
            break at;
        }
    };
    let instruments = session.instruments(&ids).await;
    session.disconnect().await;

    let renewed = stored();
    let mode = std::fs::metadata(&credential.token_file)
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    let left = renewed.expires_at.unwrap().duration_since(Timestamp::now());
    println!(
        "renewed at {renewed_at}, a spot at {after}; token file {} mode {mode:o}, the \
         access token valid until {:?} ({:.1} days)",
        credential.token_file.display(),
        renewed.expires_at,
        left.as_secs_f64() / 86_400.0
    );
    assert_eq!(instruments.unwrap().len(), ids.len());
    assert_eq!(mode, 0o600);
    assert!(!renewed.access_token.expose_secret().is_empty());
    assert!(!renewed.refresh_token.expose_secret().is_empty());
    assert!(
        renewed.refresh_token.expose_secret() != before.refresh_token.expose_secret(),
        "the refresh token was not replaced"
    );
    assert!(
        SignedDuration::from_hours(24 * 20) < left && left < SignedDuration::from_hours(24 * 40),
        "{left:?}"
    );

    // A fresh session, as the next run: from the file, no renewal.
    let fresh = CTraderSession::connect(&connection).await.unwrap();
    let account = fresh.check_account().await;
    fresh.disconnect().await;

    account.unwrap();
    assert!(stored().same_access_token(&renewed));
}
