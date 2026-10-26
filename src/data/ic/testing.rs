//! A mock cTrader peer for the tests: a WebSocket server on 127.0.0.1
//! that records every frame it receives and answers through a script.

use std::{
    collections::HashSet,
    net::TcpListener,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use jiff::{SignedDuration, Timestamp};
use nautilus_core::string::secret::SecretString;
use prost::Message as _;
use tempfile::TempDir;
use tungstenite::Message;

use super::config::{IcAccountId, IcCredential};
use super::messages::{
    Payload, ProtoHeartbeatEvent, ProtoMessage, ProtoOaAccountAuthReq, ProtoOaAccountAuthRes,
    ProtoOaAccountsTokenInvalidatedEvent, ProtoOaApplicationAuthReq, ProtoOaApplicationAuthRes,
    ProtoOaErrorRes, ProtoOaGetTickDataReq, ProtoOaGetTickDataRes, ProtoOaRefreshTokenReq,
    ProtoOaRefreshTokenRes, ProtoOaTickData, frame,
};
use super::ticks::Tick;
use super::token::{TokenPair, TokenStore};

/// The fake account the tests authenticate: the `ctidTraderAccountId` the
/// captured responses were scrubbed to.
pub(super) const ACCOUNT_ID: i64 = 1_000_001;

// Responses captured from IC's cTrader demo, scrubbed: the payloads, as
// served (see the `.txtpb` beside each in `testdata/`).
pub(super) const APPLICATION_AUTH_RES: &[u8] = include_bytes!("testdata/application_auth_res.bin");
pub(super) const APPLICATION_AUTH_REFUSED: &[u8] =
    include_bytes!("testdata/application_auth_refused.bin");
pub(super) const ACCOUNT_AUTH_RES: &[u8] = include_bytes!("testdata/account_auth_res.bin");
pub(super) const ACCOUNT_AUTH_TOKEN_INVALID: &[u8] =
    include_bytes!("testdata/account_auth_token_invalid.bin");
pub(super) const ASSET_LIST_RES: &[u8] = include_bytes!("testdata/asset_list_res.bin");
pub(super) const SYMBOLS_LIST_RES: &[u8] = include_bytes!("testdata/symbols_list_res.bin");
pub(super) const SYMBOL_BY_ID_RES: &[u8] = include_bytes!("testdata/symbol_by_id_res.bin");
pub(super) const SYMBOL_BY_ID_UNKNOWN: &[u8] = include_bytes!("testdata/symbol_by_id_unknown.bin");
pub(super) const TICK_DATA_BID_RES: &[u8] = include_bytes!("testdata/tick_data_bid_res.bin");
pub(super) const TICK_DATA_ASK_RES: &[u8] = include_bytes!("testdata/tick_data_ask_res.bin");
pub(super) const SUBSCRIBE_SPOTS_RES: &[u8] = include_bytes!("testdata/subscribe_spots_res.bin");
pub(super) const UNSUBSCRIBE_SPOTS_RES: &[u8] =
    include_bytes!("testdata/unsubscribe_spots_res.bin");
pub(super) const SPOT_EVENT_EURUSD_FIRST: &[u8] =
    include_bytes!("testdata/spot_event_eurusd_first.bin");
pub(super) const SPOT_EVENT_EURUSD_BID: &[u8] =
    include_bytes!("testdata/spot_event_eurusd_bid.bin");
pub(super) const SPOT_EVENT_EURUSD_ASK: &[u8] =
    include_bytes!("testdata/spot_event_eurusd_ask.bin");
pub(super) const SPOT_EVENT_XAUUSD_FIRST: &[u8] =
    include_bytes!("testdata/spot_event_xauusd_first.bin");
pub(super) const TRADER_RES: &[u8] = include_bytes!("testdata/trader_res.bin");
pub(super) const RECONCILE_RES_EMPTY: &[u8] = include_bytes!("testdata/reconcile_res_empty.bin");
pub(super) const DEAL_LIST_RES_EMPTY: &[u8] = include_bytes!("testdata/deal_list_res_empty.bin");

/// What the peer does after a frame.
pub(super) enum Action {
    /// Sends these bytes as one binary frame.
    Send(Vec<u8>),
    /// Closes this connection.
    Close,
}

/// The answer to `request`: `response` under the request's `clientMsgId`.
pub(super) fn reply<M: Payload>(request: &ProtoMessage, response: &M) -> Action {
    Action::Send(frame(response, request.client_msg_id.clone()))
}

/// The answer to `request`: the captured `payload`, an `M`, under the
/// request's `clientMsgId`. The bytes go as captured, fields the vendored
/// `.proto` lacks included.
pub(super) fn replay<M: Payload>(request: &ProtoMessage, payload: &[u8]) -> Action {
    Action::Send(
        ProtoMessage {
            payload_type: M::TYPE,
            payload: Some(payload.to_vec()),
            client_msg_id: request.client_msg_id.clone(),
        }
        .encode_to_vec(),
    )
}

/// The captured answers of a cTrader that accepts the application and the
/// account `ACCOUNT_ID`, or `None` for a frame that is no auth request.
pub(super) fn accept_auth(request: &ProtoMessage) -> Option<Vec<Action>> {
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

/// The frames the peer received: the connection (0, 1, ...) and the frame.
pub(super) type Received = Arc<Mutex<Vec<(usize, ProtoMessage)>>>;

/// Starts the peer; `script` answers each frame received on a connection,
/// heartbeats aside, which cTrader does not answer. Serves connections one
/// after another, until the test ends.
pub(super) fn start(
    mut script: impl FnMut(usize, &ProtoMessage) -> Vec<Action> + Send + 'static,
) -> (String, Received) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let received: Received = Arc::default();
    let log = Arc::clone(&received);
    std::thread::spawn(move || {
        for (connection, stream) in listener.incoming().enumerate() {
            let Ok(mut socket) = tungstenite::accept(stream.unwrap()) else {
                continue;
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
                for action in script(connection, &request) {
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
        }
    });
    (url, received)
}

/// Waits, up to 10 s, until `condition` holds on the frames received.
pub(super) async fn wait_until(
    received: &Received,
    condition: impl Fn(&[(usize, ProtoMessage)]) -> bool,
) -> Vec<(usize, ProtoMessage)> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let frames = received.lock().unwrap().clone();
        if condition(&frames) {
            return frames;
        }
        assert!(
            Instant::now() < deadline,
            "condition not met; received: {frames:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// The made-up token pair of `credential`, as `.env` would hold it.
pub(super) const ACCESS_TOKEN: &str = "accessTokenValue";
pub(super) const REFRESH_TOKEN: &str = "refreshTokenValue";

/// A credential of made-up values, for account `ACCOUNT_ID`, its token
/// file in the directory returned, holding the pair of `ACCESS_TOKEN`
/// and `REFRESH_TOKEN`, valid for 30 days.
pub(super) fn credential() -> (IcCredential, TempDir) {
    credential_expiring(Some(Timestamp::now() + SignedDuration::from_hours(30 * 24)))
}

/// As `credential`, the access token expiring at `expires_at` (`None`:
/// unknown, as when `.env` seeded it).
pub(super) fn credential_expiring(expires_at: Option<Timestamp>) -> (IcCredential, TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let seed = TokenPair {
        access_token: SecretString::from(ACCESS_TOKEN),
        refresh_token: SecretString::from(REFRESH_TOKEN),
        expires_at: None,
    };
    let tokens = TokenStore::new(dir.path().join("ic-token.json"), seed.clone());
    tokens.load().unwrap();
    let stored = TokenPair { expires_at, ..seed };
    tokens.try_lock().unwrap().unwrap().save(&stored).unwrap();
    let credential = IcCredential {
        client_id: SecretString::from("12345_clientIdValue"),
        client_secret: SecretString::from("clientSecretValue"),
        account_id: IcAccountId::new(ACCOUNT_ID),
        tokens,
    };
    (credential, dir)
}

/// The access token renewals hand out: `ACCESS_TOKEN` then
/// `renewedAccess1`, `renewedAccess2`, ...
pub(super) fn renewed_access(renewal: usize) -> String {
    format!("renewedAccess{renewal}")
}

/// How long a renewed access token lasts, as cTrader's docs give it.
pub(super) const EXPIRES_IN: i64 = 2_628_000;

/// cTrader's tokens as the docs describe them: one pair at a time; a
/// renewal with the current refresh token replaces both, the old pair
/// invalidated. Shared by the peers of sessions on the same account.
#[derive(Debug)]
pub(super) struct TokenVenue {
    renewals: usize,
    refuse_renewal: bool,
    /// Whether the access token expired: refused, until renewed.
    expired: bool,
    /// The peers created on the venue.
    peers: usize,
    /// The connections (peer, connection) whose account session is
    /// authenticated.
    authenticated: HashSet<(usize, usize)>,
    /// The connections whose account session ended, told so with their
    /// next request.
    ended: HashSet<(usize, usize)>,
}

pub(super) type SharedVenue = Arc<Mutex<TokenVenue>>;

impl TokenVenue {
    /// A venue holding the pair of `credential`.
    pub(super) fn new() -> SharedVenue {
        Arc::new(Mutex::new(Self {
            renewals: 0,
            refuse_renewal: false,
            expired: false,
            peers: 0,
            authenticated: HashSet::new(),
            ended: HashSet::new(),
        }))
    }

    /// A venue that refuses every renewal, as for a revoked refresh token.
    pub(super) fn refusing() -> SharedVenue {
        let venue = Self::new();
        venue.lock().unwrap().refuse_renewal = true;
        venue
    }

    pub(super) fn renewals(&self) -> usize {
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

    /// Ends the account session of every connection, the access token
    /// invalidated: it expired, or was renewed.
    fn invalidate(&mut self) {
        self.ended.extend(self.authenticated.drain());
    }

    /// The access token expires: the sessions end, and it is refused
    /// until renewed.
    pub(super) fn expire(&mut self) {
        self.expired = true;
        self.invalidate();
    }
}

/// Synthetic: cTrader's answer to a token it does not hold.
pub(super) fn token_refused(request: &ProtoMessage) -> Action {
    let error = ProtoOaErrorRes {
        error_code: "CH_ACCESS_TOKEN_INVALID".to_owned(),
        description: Some("Invalid access token".to_owned()),
        ..Default::default()
    };
    reply(request, &error)
}

/// Synthetic: the event of an account session cTrader ended because its
/// token was refreshed or expired.
pub(super) fn token_invalidated() -> Action {
    let event = ProtoOaAccountsTokenInvalidatedEvent {
        ctid_trader_account_ids: vec![ACCOUNT_ID],
        reason: Some("Access Token is expired or recalled".to_owned()),
        ..Default::default()
    };
    Action::Send(frame(&event, None))
}

/// A peer on `venue`: it answers the auth and the renewals as `venue`
/// does, the account auth with an access token it does not hold with the
/// captured `CH_ACCESS_TOKEN_INVALID`. A renewal sends the event ending
/// the renewing session, then the new pair; a session whose token is no
/// longer valid gets the event with its next request, which `other` then
/// answers, as any other request.
pub(super) fn token_peer(
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

/// cTrader's answer to a tick request over `[from, to)` on a side whose
/// ticks are `ticks`, oldest first: the newest `page_size` ticks of the
/// window, newest first, and whether the window holds more (as measured
/// on the demo, see `ic/README.md`, "Tick history").
pub(super) fn venue_page(
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

/// cTrader's answer to `request`, a tick request, on a symbol whose BID
/// and ASK ticks are `bids` and `asks`, oldest first, in pages of
/// `page_size` (see `venue_page`): the first tick absolute, the others
/// differences from the one before.
pub(super) fn tick_answer(
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
