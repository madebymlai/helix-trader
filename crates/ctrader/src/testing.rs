//! A mock cTrader peer for the tests.
//!
//! The peer is a WebSocket server on 127.0.0.1. It records every frame it receives and answers
//! through a script.

use std::{
    collections::HashSet,
    net::TcpListener,
    sync::{Arc, Mutex},
    time::Duration,
};

use jiff::{SignedDuration, Timestamp};
use nautilus_common::testing::wait_until_async;
use nautilus_core::{UnixNanos, string::secret::SecretString, time::get_atomic_clock_realtime};
use nautilus_model::{identifiers::Venue, instruments::Cfd};
use prost::Message as _;
use rust_decimal::Decimal;
use tempfile::TempDir;
use tungstenite::Message;

use super::{
    common::{
        messages::{
            Payload, ProtoHeartbeatEvent, ProtoMessage, ProtoOaAccountAuthReq,
            ProtoOaAccountAuthRes, ProtoOaAccountsTokenInvalidatedEvent, ProtoOaApplicationAuthReq,
            ProtoOaApplicationAuthRes, ProtoOaAssetListRes, ProtoOaErrorRes,
            ProtoOaGetDynamicLeverageByIdReq, ProtoOaGetDynamicLeverageByIdRes,
            ProtoOaGetTickDataReq, ProtoOaGetTickDataRes, ProtoOaRefreshTokenReq,
            ProtoOaRefreshTokenRes, ProtoOaSubscribeSpotsReq, ProtoOaSubscribeSpotsRes,
            ProtoOaSymbolByIdRes, ProtoOaSymbolsListRes, ProtoOaTickData, frame,
        },
        parse,
        token::TokenPair,
    },
    config::{
        CTraderAccountId, CTraderConnection, CTraderCredential, CTraderEnvironment,
        CTraderSettingNames,
    },
    session::ticks::Tick,
};

// A made-up broker, so a real broker's name left in the adapter fails a test
pub(crate) const VENUE: &str = "ACMEFX";
pub(crate) const CLIENT: &str = "ACME-CTRADER";

pub(crate) const NAMES: CTraderSettingNames = CTraderSettingNames {
    environment: "ACME_ENVIRONMENT",
    client_id: "ACME_CLIENT_ID",
    client_secret: "ACME_CLIENT_SECRET",
    access_token: "ACME_ACCESS_TOKEN",
    refresh_token: "ACME_REFRESH_TOKEN",
    account_id: "ACME_ACCOUNT_ID",
    source: "acme.env",
    guide: "the ACME guide",
};

// The `ctidTraderAccountId` the captured responses were scrubbed to
pub(crate) const ACCOUNT_ID: i64 = 1_000_001;

// Responses captured from a broker's cTrader demo and scrubbed (see the `.txtpb` in `test_data/`)
pub(crate) const APPLICATION_AUTH_RES: &[u8] =
    include_bytes!("../test_data/application_auth_res.bin");
pub(crate) const APPLICATION_AUTH_REFUSED: &[u8] =
    include_bytes!("../test_data/application_auth_refused.bin");
pub(crate) const ACCOUNT_AUTH_RES: &[u8] = include_bytes!("../test_data/account_auth_res.bin");
pub(crate) const ACCOUNT_AUTH_TOKEN_INVALID: &[u8] =
    include_bytes!("../test_data/account_auth_token_invalid.bin");
pub(crate) const ASSET_LIST_RES: &[u8] = include_bytes!("../test_data/asset_list_res.bin");
pub(crate) const SYMBOLS_LIST_RES: &[u8] = include_bytes!("../test_data/symbols_list_res.bin");
pub(crate) const SYMBOL_BY_ID_RES: &[u8] = include_bytes!("../test_data/symbol_by_id_res.bin");
pub(crate) const DYNAMIC_LEVERAGE_XAUUSD_RES: &[u8] =
    include_bytes!("../test_data/dynamic_leverage_xauusd_res.bin");
pub(crate) const DYNAMIC_LEVERAGE_EURUSD_RES: &[u8] =
    include_bytes!("../test_data/dynamic_leverage_eurusd_res.bin");
pub(crate) const EXPECTED_MARGIN_EURUSD_RES: &[u8] =
    include_bytes!("../test_data/expected_margin_eurusd_res.bin");
pub(crate) const SYMBOL_BY_ID_UNKNOWN: &[u8] =
    include_bytes!("../test_data/symbol_by_id_unknown.bin");
pub(crate) const TICK_DATA_BID_RES: &[u8] = include_bytes!("../test_data/tick_data_bid_res.bin");
pub(crate) const TICK_DATA_ASK_RES: &[u8] = include_bytes!("../test_data/tick_data_ask_res.bin");
pub(crate) const SUBSCRIBE_SPOTS_RES: &[u8] =
    include_bytes!("../test_data/subscribe_spots_res.bin");
pub(crate) const UNSUBSCRIBE_SPOTS_RES: &[u8] =
    include_bytes!("../test_data/unsubscribe_spots_res.bin");
pub(crate) const SPOT_EVENT_EURUSD_FIRST: &[u8] =
    include_bytes!("../test_data/spot_event_eurusd_first.bin");
pub(crate) const SPOT_EVENT_EURUSD_BID: &[u8] =
    include_bytes!("../test_data/spot_event_eurusd_bid.bin");
pub(crate) const SPOT_EVENT_EURUSD_ASK: &[u8] =
    include_bytes!("../test_data/spot_event_eurusd_ask.bin");
pub(crate) const SPOT_EVENT_XAUUSD_FIRST: &[u8] =
    include_bytes!("../test_data/spot_event_xauusd_first.bin");
pub(crate) const TRADER_RES: &[u8] = include_bytes!("../test_data/trader_res.bin");
pub(crate) const RECONCILE_RES_EMPTY: &[u8] =
    include_bytes!("../test_data/reconcile_res_empty.bin");
pub(crate) const DEAL_LIST_RES_EMPTY: &[u8] =
    include_bytes!("../test_data/deal_list_res_empty.bin");

// Trade events captured on the demo with 1 oz orders of XAUUSD
pub(crate) const EXECUTION_MARKET_ACCEPTED: &[u8] =
    include_bytes!("../test_data/execution_market_accepted.bin");
pub(crate) const EXECUTION_MARKET_FILLED: &[u8] =
    include_bytes!("../test_data/execution_market_filled.bin");
pub(crate) const EXECUTION_PROTECTION_ACCEPTED: &[u8] =
    include_bytes!("../test_data/execution_protection_accepted.bin");
pub(crate) const EXECUTION_PROTECTION_REPLACED: &[u8] =
    include_bytes!("../test_data/execution_protection_replaced.bin");
pub(crate) const EXECUTION_CLOSE_ACCEPTED: &[u8] =
    include_bytes!("../test_data/execution_close_accepted.bin");
pub(crate) const EXECUTION_CLOSE_FILLED: &[u8] =
    include_bytes!("../test_data/execution_close_filled.bin");
pub(crate) const EXECUTION_PROTECTION_CANCELLED: &[u8] =
    include_bytes!("../test_data/execution_protection_cancelled.bin");
pub(crate) const EXECUTION_LIMIT_ACCEPTED: &[u8] =
    include_bytes!("../test_data/execution_limit_accepted.bin");
pub(crate) const EXECUTION_LIMIT_REPLACED: &[u8] =
    include_bytes!("../test_data/execution_limit_replaced.bin");
pub(crate) const EXECUTION_LIMIT_CANCELLED: &[u8] =
    include_bytes!("../test_data/execution_limit_cancelled.bin");
pub(crate) const EXECUTION_MARKET_RANGE_ACCEPTED: &[u8] =
    include_bytes!("../test_data/execution_market_range_accepted.bin");
pub(crate) const EXECUTION_MARKET_RANGE_DEAL_REJECTED: &[u8] =
    include_bytes!("../test_data/execution_market_range_deal_rejected.bin");
pub(crate) const EXECUTION_MARKET_RANGE_BUY_BETTER_ACCEPTED: &[u8] =
    include_bytes!("../test_data/execution_market_range_buy_better_accepted.bin");
pub(crate) const EXECUTION_MARKET_RANGE_BUY_BETTER_FILLED: &[u8] =
    include_bytes!("../test_data/execution_market_range_buy_better_filled.bin");
pub(crate) const EXECUTION_MARKET_RANGE_SELL_BETTER_ACCEPTED: &[u8] =
    include_bytes!("../test_data/execution_market_range_sell_better_accepted.bin");
pub(crate) const EXECUTION_MARKET_RANGE_SELL_BETTER_FILLED: &[u8] =
    include_bytes!("../test_data/execution_market_range_sell_better_filled.bin");
pub(crate) const EXECUTION_MARKET_RANGE_BRACKET_ACCEPTED: &[u8] =
    include_bytes!("../test_data/execution_market_range_bracket_accepted.bin");
pub(crate) const EXECUTION_MARKET_RANGE_BRACKET_FILLED: &[u8] =
    include_bytes!("../test_data/execution_market_range_bracket_filled.bin");
pub(crate) const EXECUTION_MARKET_RANGE_BRACKET_PROTECTION_ACCEPTED: &[u8] =
    include_bytes!("../test_data/execution_market_range_bracket_protection_accepted.bin");
pub(crate) const EXECUTION_STOP_LOSS_ENTRY_FILLED: &[u8] =
    include_bytes!("../test_data/execution_stop_loss_entry_filled.bin");
pub(crate) const EXECUTION_STOP_LOSS_ACCEPTED: &[u8] =
    include_bytes!("../test_data/execution_stop_loss_accepted.bin");
pub(crate) const EXECUTION_STOP_LOSS_FILLED: &[u8] =
    include_bytes!("../test_data/execution_stop_loss_filled.bin");
pub(crate) const ORDER_ERROR_BAD_VOLUME: &[u8] =
    include_bytes!("../test_data/order_error_bad_volume.bin");
pub(crate) const ORDER_ERROR_ORDER_NOT_FOUND: &[u8] =
    include_bytes!("../test_data/order_error_order_not_found.bin");
pub(crate) const ORDER_ERROR_POSITION_NOT_FOUND: &[u8] =
    include_bytes!("../test_data/order_error_position_not_found.bin");

pub(crate) enum Action {
    Send(Vec<u8>),
    Close,
}

pub(crate) fn reply<M: Payload>(request: &ProtoMessage, response: &M) -> Action {
    Action::Send(frame(response, request.client_msg_id.clone()))
}

pub(crate) fn event<M: Payload>(message: &M, client_msg_id: Option<&str>) -> Action {
    Action::Send(frame(message, client_msg_id.map(str::to_owned)))
}

pub(crate) fn spots_answer(request: &ProtoMessage) -> Action {
    let asked = ProtoOaSubscribeSpotsReq::decode(request.payload.as_deref().unwrap()).unwrap();
    let mut answer = ProtoOaSubscribeSpotsRes::decode(SUBSCRIBE_SPOTS_RES).unwrap();
    answer.subscribed_symbol_id = asked.symbol_id;
    reply(request, &answer)
}

/// Answers with the captured bytes as served.
///
/// This includes fields the vendored `.proto` lacks.
pub(crate) fn replay<M: Payload>(request: &ProtoMessage, payload: &[u8]) -> Action {
    Action::Send(
        ProtoMessage {
            payload_type: M::TYPE,
            payload: Some(payload.to_vec()),
            client_msg_id: request.client_msg_id.clone(),
        }
        .encode_to_vec(),
    )
}

pub(crate) fn dynamic_leverage_answer(request: &ProtoMessage) -> Action {
    let asked =
        ProtoOaGetDynamicLeverageByIdReq::decode(request.payload.as_deref().unwrap()).unwrap();
    let captured = match asked.leverage_id {
        52_543 => DYNAMIC_LEVERAGE_XAUUSD_RES,
        52_531 => DYNAMIC_LEVERAGE_EURUSD_RES,
        other => panic!("no leverage {other} captured"),
    };
    replay::<ProtoOaGetDynamicLeverageByIdRes>(request, captured)
}

/// The captured XAUUSD or EURUSD on the made-up venue, at a margin of 5%.
pub(crate) fn instrument(name: &str) -> Cfd {
    let light = ProtoOaSymbolsListRes::decode(SYMBOLS_LIST_RES)
        .unwrap()
        .symbol
        .into_iter()
        .find(|s| s.symbol_name.as_deref() == Some(name))
        .unwrap();
    let details = ProtoOaSymbolByIdRes::decode(SYMBOL_BY_ID_RES)
        .unwrap()
        .symbol
        .into_iter()
        .find(|s| s.symbol_id == light.symbol_id)
        .unwrap();
    let assets = parse::assets_by_id(ProtoOaAssetListRes::decode(ASSET_LIST_RES).unwrap().asset);
    parse::cfd(
        &light,
        &details,
        Venue::from(VENUE),
        &assets,
        Decimal::new(5, 2),
        UnixNanos::default(),
    )
    .unwrap()
}

pub(crate) fn accept_auth(request: &ProtoMessage) -> Option<Vec<Action>> {
    match request.payload_type {
        t if t == ProtoOaApplicationAuthReq::TYPE => {
            Some(vec![replay::<ProtoOaApplicationAuthRes>(
                request,
                APPLICATION_AUTH_RES,
            )])
        }
        t if t == ProtoOaAccountAuthReq::TYPE => Some(vec![replay::<ProtoOaAccountAuthRes>(
            request,
            ACCOUNT_AUTH_RES,
        )]),
        _ => None,
    }
}

pub(crate) type Received = Arc<Mutex<Vec<(usize, ProtoMessage)>>>;

/// Starts the peer.
///
/// `script` answers each frame except heartbeats, which cTrader does not answer.
///
/// Each connection is served on its own thread until the test ends. The transport opens a
/// replacement before it drops the connection it replaces.
pub(crate) fn start(
    script: impl FnMut(usize, &ProtoMessage) -> Vec<Action> + Send + 'static,
) -> (String, Received) {
    start_delayed(|_| Duration::ZERO, script)
}

pub(crate) fn start_delayed(
    handshake: impl Fn(usize) -> Duration + Send + Sync + 'static,
    script: impl FnMut(usize, &ProtoMessage) -> Vec<Action> + Send + 'static,
) -> (String, Received) {
    let handshake = Arc::new(handshake);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let received: Received = Arc::default();
    let log = Arc::clone(&received);
    let script = Arc::new(Mutex::new(script));
    std::thread::spawn(move || {
        for (connection, stream) in listener.incoming().enumerate() {
            let log = Arc::clone(&log);
            let script = Arc::clone(&script);
            let handshake = Arc::clone(&handshake);
            std::thread::spawn(move || {
                std::thread::sleep(handshake(connection));
                let Ok(mut socket) = tungstenite::accept(stream.unwrap()) else {
                    return;
                };
                'frames: while let Ok(message) = socket.read() {
                    let Message::Binary(bytes) = message else {
                        continue;
                    };
                    let request = ProtoMessage::decode(&bytes[..]).unwrap();
                    log.lock().unwrap().push((connection, request.clone()));
                    if request.payload_type == ProtoHeartbeatEvent::TYPE {
                        continue;
                    }
                    let actions = (script.lock().unwrap())(connection, &request);
                    for action in actions {
                        match action {
                            Action::Send(bytes) => {
                                if socket.send(Message::Binary(bytes.into())).is_err() {
                                    break 'frames;
                                }
                            }
                            Action::Close => {
                                let _ = socket.close(None);
                                let _ = socket.flush();
                                break 'frames;
                            }
                        }
                    }
                }
            });
        }
    });
    (url, received)
}

pub(crate) async fn wait_until(
    received: &Received,
    condition: impl Fn(&[(usize, ProtoMessage)]) -> bool,
) -> Vec<(usize, ProtoMessage)> {
    wait_until_async(
        || {
            // Unlocked before `condition`, which may read `received` too
            let frames = received.lock().unwrap().clone();
            let met = condition(&frames);
            async move { met }
        },
        Duration::from_secs(10),
    )
    .await;
    received.lock().unwrap().clone()
}

pub(crate) const ACCESS_TOKEN: &str = "accessTokenValue";
pub(crate) const REFRESH_TOKEN: &str = "refreshTokenValue";

pub(crate) fn now() -> Timestamp {
    Timestamp::from(get_atomic_clock_realtime().get_time_ns())
}

/// Returns a credential and the directory of its token file.
///
/// The file holds a pair valid for 30 days.
pub(crate) fn credential() -> (CTraderCredential, TempDir) {
    credential_expiring(Some(now() + SignedDuration::from_hours(30 * 24)))
}

pub(crate) fn connection() -> (CTraderConnection, TempDir) {
    let (credential, tokens) = credential();
    let connection = CTraderConnection {
        venue: Venue::new(VENUE),
        environment: CTraderEnvironment::Demo,
        credential,
    };
    (connection, tokens)
}

/// Returns a credential like `credential`, with the pair expiring at `expires_at`.
///
/// `None` means the expiry is unknown, as when the credential seeded the file.
pub(crate) fn credential_expiring(expires_at: Option<Timestamp>) -> (CTraderCredential, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let credential = CTraderCredential {
        client_id: SecretString::from("12345_clientIdValue"),
        client_secret: SecretString::from("clientSecretValue"),
        account_id: CTraderAccountId::new(ACCOUNT_ID),
        access_token: SecretString::from(ACCESS_TOKEN),
        refresh_token: SecretString::from(REFRESH_TOKEN),
        token_file: dir.path().join("token.json"),
        names: NAMES,
    };
    let tokens = credential.tokens();
    let seed = tokens.try_load().unwrap().unwrap();
    let stored = TokenPair { expires_at, ..seed };
    tokens.try_lock().unwrap().unwrap().save(&stored).unwrap();
    (credential, dir)
}

pub(crate) fn stored_access_token(credential: &CTraderCredential) -> String {
    credential
        .tokens()
        .read()
        .unwrap()
        .access_token
        .expose_secret()
        .to_owned()
}

/// A synthetic renewal answer with the access token `renewed_access(1)`.
pub(crate) fn renewal_answer(request: &ProtoMessage) -> Action {
    let renewed = ProtoOaRefreshTokenRes {
        access_token: renewed_access(1),
        token_type: "bearer".to_owned(),
        expires_in: EXPIRES_IN,
        refresh_token: "renewedRefresh1".to_owned(),
        ..Default::default()
    };
    reply(request, &renewed)
}

pub(crate) fn renewed_access(renewal: usize) -> String {
    format!("renewedAccess{renewal}")
}

// A renewed access token's lifetime, as cTrader's docs give it
pub(crate) const EXPIRES_IN: i64 = 2_628_000;

/// cTrader's tokens as the docs describe them. The peers on one account share it.
///
/// There is one pair at a time. A renewal replaces both tokens and invalidates the old pair.
#[derive(Debug)]
pub(crate) struct TokenVenue {
    renewals: usize,
    refuse_renewal: bool,
    expired: bool,
    peers: usize,
    authenticated: HashSet<(usize, usize)>,
    /// These sessions are told so with their next request.
    ended: HashSet<(usize, usize)>,
}

pub(crate) type SharedVenue = Arc<Mutex<TokenVenue>>;

impl TokenVenue {
    pub(crate) fn new() -> SharedVenue {
        Arc::new(Mutex::new(Self {
            renewals: 0,
            refuse_renewal: false,
            expired: false,
            peers: 0,
            authenticated: HashSet::new(),
            ended: HashSet::new(),
        }))
    }

    /// Refuses every renewal, as for a revoked refresh token.
    pub(crate) fn refusing() -> SharedVenue {
        let venue = Self::new();
        venue.lock().unwrap().refuse_renewal = true;
        venue
    }

    pub(crate) fn renewals(&self) -> usize {
        self.renewals
    }

    fn access(&self) -> String {
        match self.renewals {
            0 => ACCESS_TOKEN.to_owned(),
            n => renewed_access(n),
        }
    }

    fn refresh(&self) -> String {
        match self.renewals {
            0 => REFRESH_TOKEN.to_owned(),
            n => format!("renewedRefresh{n}"),
        }
    }

    fn invalidate(&mut self) {
        self.ended.extend(self.authenticated.drain());
    }

    pub(crate) fn expire(&mut self) {
        self.expired = true;
        self.invalidate();
    }
}

pub(crate) fn token_refused(request: &ProtoMessage) -> Action {
    let error = ProtoOaErrorRes {
        error_code: "CH_ACCESS_TOKEN_INVALID".to_owned(),
        description: Some("Invalid access token".to_owned()),
        ..Default::default()
    };
    reply(request, &error)
}

pub(crate) fn token_invalidated() -> Action {
    let event = ProtoOaAccountsTokenInvalidatedEvent {
        ctid_trader_account_ids: vec![ACCOUNT_ID],
        reason: Some("Access Token is expired or recalled".to_owned()),
        ..Default::default()
    };
    Action::Send(frame(&event, None))
}

/// A peer that answers an access token it does not hold with the captured
/// `CH_ACCESS_TOKEN_INVALID`.
///
/// A renewal sends the event that ends the renewing session, then the new pair. A session whose
/// token is no longer valid gets the event with its next request. `other` then answers that
/// request.
pub(crate) fn token_peer(
    venue: SharedVenue,
    mut other: impl FnMut(usize, &ProtoMessage) -> Vec<Action> + Send + 'static,
) -> impl FnMut(usize, &ProtoMessage) -> Vec<Action> + Send + 'static {
    let peer = {
        let mut venue = venue.lock().unwrap();
        venue.peers += 1;
        venue.peers
    };
    move |connection, request| {
        let session = (peer, connection);
        let mut venue = venue.lock().unwrap();
        match request.payload_type {
            t if t == ProtoOaApplicationAuthReq::TYPE => vec![replay::<ProtoOaApplicationAuthRes>(
                request,
                APPLICATION_AUTH_RES,
            )],
            t if t == ProtoOaAccountAuthReq::TYPE => {
                let asked =
                    ProtoOaAccountAuthReq::decode(request.payload.as_deref().unwrap()).unwrap();
                if asked.access_token == venue.access() && !venue.expired {
                    venue.authenticated.insert(session);
                    venue.ended.remove(&session);
                    vec![replay::<ProtoOaAccountAuthRes>(request, ACCOUNT_AUTH_RES)]
                } else {
                    vec![replay::<ProtoOaErrorRes>(
                        request,
                        ACCOUNT_AUTH_TOKEN_INVALID,
                    )]
                }
            }
            t if t == ProtoOaRefreshTokenReq::TYPE => {
                let asked =
                    ProtoOaRefreshTokenReq::decode(request.payload.as_deref().unwrap()).unwrap();
                if venue.refuse_renewal || asked.refresh_token != venue.refresh() {
                    return vec![token_refused(request)];
                }
                venue.renewals += 1;
                venue.expired = false;
                venue.invalidate();
                let ended = venue.ended.remove(&session);
                let renewed = ProtoOaRefreshTokenRes {
                    access_token: venue.access(),
                    token_type: "bearer".to_owned(),
                    expires_in: EXPIRES_IN,
                    refresh_token: venue.refresh(),
                    ..Default::default()
                };
                let mut actions: Vec<Action> = ended.then(token_invalidated).into_iter().collect();
                actions.push(reply(request, &renewed));
                actions
            }
            _ => {
                let mut actions = Vec::new();
                if venue.ended.remove(&session) {
                    actions.push(token_invalidated());
                }
                drop(venue);
                actions.extend(other(connection, request));
                actions
            }
        }
    }
}

/// Returns the newest `page_size` of `ticks` over `[from, to)`, newest first, and whether there
/// are more.
///
/// This is as measured on the demo (see `README.md`, "Tick history").
pub(crate) fn venue_page(
    ticks: &[Tick],
    from: i64,
    to: i64,
    page_size: usize,
) -> (Vec<Tick>, bool) {
    let window: Vec<Tick> = ticks
        .iter()
        .rev()
        .filter(|t| from <= t.ms && t.ms < to)
        .copied()
        .collect();
    let more = window.len() > page_size;
    (window.into_iter().take(page_size).collect(), more)
}

/// Answers a tick request with the first tick absolute and the others as differences from the one
/// before (see `venue_page`).
pub(crate) fn tick_answer(
    request: &ProtoOaGetTickDataReq,
    bids: &[Tick],
    asks: &[Tick],
    page_size: usize,
) -> ProtoOaGetTickDataRes {
    let ticks = if request.r#type == 1 { bids } else { asks };
    let (page, has_more) = venue_page(
        ticks,
        request.from_timestamp.unwrap(),
        request.to_timestamp.unwrap(),
        page_size,
    );
    let mut before: Option<Tick> = None;
    let tick_data = page
        .into_iter()
        .map(|tick| {
            let data = match before {
                None => ProtoOaTickData {
                    timestamp: tick.ms,
                    tick: tick.price,
                },
                Some(newer) => ProtoOaTickData {
                    timestamp: tick.ms - newer.ms,
                    tick: tick.price - newer.price,
                },
            };
            before = Some(tick);
            data
        })
        .collect();
    ProtoOaGetTickDataRes {
        ctid_trader_account_id: request.ctid_trader_account_id,
        tick_data,
        has_more,
        ..Default::default()
    }
}
