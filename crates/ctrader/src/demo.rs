//! The end-to-end test of a session's token renewal on a real cTrader
//! server, for a caller holding a demo account's credential, which gates
//! and runs it (feature `demo-test`): the adapter names no broker, so
//! holds none.

use std::{os::unix::fs::PermissionsExt, time::Duration};

use jiff::{SignedDuration, Timestamp};
use nautilus_model::identifiers::InstrumentId;

use crate::{
    common::messages::ProtoOaSpotEvent,
    config::CTraderConnection,
    session::CTraderSession,
    websocket::{HEARTBEAT_INTERVAL, SpotEvents},
};

/// The log records of the adapter at info and above, for the test to show:
/// none holds a token.
struct Captured(std::sync::Mutex<Vec<String>>);

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

static CAPTURED: Captured = Captured(std::sync::Mutex::new(Vec::new()));

/// Prints the records captured when dropped, the test failing or not.
struct ShowLogs;

impl Drop for ShowLogs {
    fn drop(&mut self) {
        for line in CAPTURED.0.lock().unwrap().iter() {
            println!("log: {line}");
        }
    }
}

async fn next_spot(spots: &mut SpotEvents, timeout: Duration) -> ProtoOaSpotEvent {
    tokio::time::timeout(timeout, spots.recv())
        .await
        .expect("no spot event in time")
        .expect("the spot events ended")
}

/// On the demo account of `connection`, quoting `instrument_ids`: a forced
/// renewal of the access token on a running session, which goes on
/// receiving quotes and answering requests; then a fresh session
/// authenticates from the token file without renewing. It ROTATES the
/// account's real token pair: the file keeps the new one, the credential's
/// is spent. Installs a logger of its own, if none is.
///
/// # Panics
///
/// Panics if a step fails: it is a test.
pub async fn session_renews_its_token_and_keeps_receiving_quotes(
    connection: &CTraderConnection,
    instrument_ids: &[InstrumentId],
) {
    let _ = log::set_logger(&CAPTURED);
    log::set_max_level(log::LevelFilter::Info);
    let _logs = ShowLogs;
    let tokens = connection.credential.tokens();
    let url = connection.environment.url();
    let (session, mut spots) = CTraderSession::connect_to(
        url,
        connection.venue,
        connection.credential.clone(),
        HEARTBEAT_INTERVAL,
    )
    .await
    .unwrap();
    for symbol in session.symbols(instrument_ids).await.unwrap() {
        session.subscribe_spots(symbol.symbol_id).unwrap();
    }
    next_spot(&mut spots, Duration::from_secs(30)).await;
    let before = tokens.read().unwrap();

    session.renew_token();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokens.read().unwrap().same_access_token(&before) {
        assert!(tokio::time::Instant::now() < deadline, "no renewal in 30 s");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let renewed_at = Timestamp::now();
    let after = loop {
        let spot = next_spot(&mut spots, Duration::from_secs(60)).await;
        let at = Timestamp::from_millisecond(spot.timestamp.unwrap_or_default()).unwrap();
        if at > renewed_at {
            break at;
        }
    };
    let instruments = session.instruments(instrument_ids).await;
    session.disconnect().await;

    let renewed = tokens.read().unwrap();
    let mode = std::fs::metadata(tokens.path())
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    let left = renewed.expires_at.unwrap().duration_since(Timestamp::now());
    println!(
        "renewed at {renewed_at}, a spot at {after}; token file {} mode {mode:o}, the \
         access token valid until {:?} ({:.1} days)",
        tokens.path().display(),
        renewed.expires_at,
        left.as_secs_f64() / 86_400.0
    );
    assert_eq!(instruments.unwrap().len(), instrument_ids.len());
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
    let fresh = CTraderSession::connect(connection).await.unwrap();
    let account = fresh.check_account().await;
    fresh.disconnect().await;

    account.unwrap();
    assert!(tokens.read().unwrap().same_access_token(&renewed));
}
