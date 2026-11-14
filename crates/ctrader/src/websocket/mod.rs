//! The cTrader Open API connection: an outer client and the handler task
//! that owns Nautilus' `WebSocketClient` (see `handler`).
//!
//! The outer client starts the handler with `SetClient`, waits for the
//! session to authenticate (application, then account, tracked by
//! Nautilus' `AuthTracker`, registered with the transport), then sends
//! requests as commands and awaits
//! each response. The handler sends the binary heartbeat, correlates
//! responses by `clientMsgId` and, after the transport reconnects,
//! authenticates the new session before serving waiting requests. When
//! cTrader ends the client's session, the handler asks the transport for
//! that reconnect (`WebSocketClient::request_reconnect`). Requests in
//! flight at a reconnect fail: their session is gone.
//!
//! The socket's state is Nautilus' to report: given the client's
//! `SocketControl`, the transport tells the node when the socket becomes
//! available and when it is lost (`SocketStateChanged`, endpoint
//! `SOCKET_ENDPOINT`), and the node's reconnect of the endpoint replaces
//! the transport as a drop does, the handler authenticating the new
//! session; a disconnect deregisters it (`live.md`, "Socket transport
//! state").
//!
//! The handler task is held in Nautilus' `TaskSlot`: `begin_shutdown`
//! tells it to close, `disconnect` joins it (`finish_task`), a token
//! renewal out answered and saved first. Time goes through Nautilus'
//! simulation seams (`nautilus_common::live::dst`, and the wall clock
//! through `nautilus_core::time`).
//!
//! Spot subscriptions are the session's too, tracked in Nautilus'
//! `SubscriptionState` (topics `spot:<symbolId>`): the handler marks them,
//! sends them once the session is ready, has cTrader's answers confirm or
//! fail them, sends them all again after each reconnect
//! (`reset_after_reconnect`), and passes their spot events on.
//!
//! Trade requests go out under the `clientMsgId` their caller chose
//! (`trade`), which learns whether the frame was sent; their outcome, as
//! every execution event and order error of the account, comes on the
//! trade events (`trade_events`), with its `clientMsgId`.
//!
//! Historical requests wait for their slot of Nautilus' `RateLimiter`,
//! keyed `HISTORICAL_KEY`: one every 250 ms, without burst, so fewer than
//! cTrader's 5 in any second, even after an idle spell (a burst of 5 then
//! the refill could send 9 within one second). One every 200 ms got
//! `BLOCKED_PAYLOAD_TYPE` ("You are being rate limited") on the demo. The
//! caller waits, before its command goes to the handler, as Nautilus'
//! `HttpClient` does: on the transport's keyed quota, the handler's loop
//! would wait too, and hold the trade requests, the answers and the
//! heartbeat. A slot counts only while the session is authenticated
//! (`AuthTracker`, which the handler invalidates while it authenticates the
//! account again on the same connection): the handler holds the requests
//! meanwhile and sends them together once authenticated, faster than the
//! limit.
//!
//! The transport's own heartbeat is off: cTrader counts its
//! `ProtoHeartbeatEvent`, which the transport cannot send (it sends Ping
//! frames or text). Its dead-peer timeout is on, against the heartbeats
//! cTrader sends (`DEAD_PEER_TIMEOUT_SECS`).

mod handler;

use std::{sync::Arc, time::Duration};

use anyhow::Context;
use nautilus_common::live::dst::time;
use nautilus_live::{
    SocketControl,
    task::{TaskJoinOutcome, TaskSlot, finish_task},
};
use nautilus_network::{
    Message,
    ratelimiter::{RateLimiter, clock::MonotonicClock, quota::Quota},
    websocket::{
        AuthTracker, EpochMessageHandler, SubscriptionState, WebSocketClient, WebSocketConfig,
        split_topic,
    },
};
use tokio::sync::{mpsc, oneshot};
use ustr::Ustr;

use self::handler::{CTraderWsHandler, HandlerCommand, LOCK_RETRY};
use super::{
    common::messages::{
        CTraderError, Payload, ProtoOaExecutionEvent, ProtoOaOrderErrorEvent, ProtoOaSpotEvent,
        is_historical, open,
    },
    config::CTraderCredential,
};

/// cTrader's heartbeat interval.
pub(crate) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// The label of a client's cTrader socket in Nautilus' socket state
/// events and reconnect commands: one socket carries all its session.
pub(crate) const SOCKET_ENDPOINT: &str = "ctrader-open-api";

/// How long a connection may bring no frame at all before the transport
/// takes its peer for dead and reconnects (`heartbeat_timeout_secs`), as it
/// would on a drop: three of the heartbeats cTrader's proxy sends every 30 s
/// on an authenticated connection that is otherwise silent
/// (`ProtoHeartbeatEvent`'s doc: the proxy's sign of a healthy connection
/// when nothing else is sent; the interval, Spotware's support on its
/// forum). Without it a peer gone silent, its TCP connection still open,
/// would hold the session until the requests time out, and the spot events
/// stopped.
const DEAD_PEER_TIMEOUT_SECS: u64 = 90;

/// How long the application and account authentication may take.
const AUTH_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a request waits for its response.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// The rate-limit key of historical requests.
const HISTORICAL_KEY: &str = "historical";

/// One historical request per this period: cTrader allows 5 a second.
const HISTORICAL_PERIOD: Duration = Duration::from_millis(250);

/// How long `disconnect` waits for the handler to close the transport:
/// the answer of a token renewal out first, up to `AUTH_TIMEOUT`.
const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(40);

/// How long `disconnect` waits for the handler once aborted.
const ABORT_TIMEOUT: Duration = Duration::from_secs(2);

/// The channel of the spot subscriptions' topics in their
/// `SubscriptionState`: `spot:<symbolId>`.
const SPOT_CHANNEL: &str = "spot";

/// The delimiter of a topic's channel and symbol.
const TOPIC_DELIMITER: char = ':';

/// The topic of the spot subscription of `symbol_id`.
fn spot_topic(symbol_id: i64) -> String {
    format!("{SPOT_CHANNEL}{TOPIC_DELIMITER}{symbol_id}")
}

/// Whether the spot subscription of `symbol_id` is confirmed or pending.
fn spot_subscribed(subscriptions: &SubscriptionState, symbol_id: i64) -> bool {
    subscriptions.is_subscribed(
        &Ustr::from(SPOT_CHANNEL),
        &Ustr::from(&symbol_id.to_string()),
    )
}

/// The symbol of a topic `spot_topic` made.
fn spot_symbol(topic: &str) -> i64 {
    split_topic(topic, TOPIC_DELIMITER)
        .1
        .and_then(|symbol| symbol.parse().ok())
        .expect("a spot topic holds a symbol id")
}

/// The spot events of the symbols subscribed, as cTrader sends them.
pub type SpotEvents = mpsc::UnboundedReceiver<ProtoOaSpotEvent>;

/// The trade events of the account, as cTrader sends them.
pub(crate) type TradeEvents = mpsc::UnboundedReceiver<TradeEvent>;

/// A trade event: what cTrader sent, and the `clientMsgId` it came under,
/// the trade request's when it answers one.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TradeEvent {
    pub(crate) client_msg_id: Option<String>,
    pub(crate) message: TradeMessage,
}

/// What a trade event holds.
#[derive(Debug, Clone, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "each event goes through the channel once, to the one listener"
)]
pub(crate) enum TradeMessage {
    /// An order accepted, filled, replaced, cancelled, expired or
    /// rejected, or a position's protection changed.
    Execution(ProtoOaExecutionEvent),
    /// An order the server refused.
    OrderError(ProtoOaOrderErrorEvent),
    /// cTrader's error answer to a trade request (`CTraderError::Venue`).
    Refused(CTraderError),
}

/// Whether a trade request was sent: `Ok` once its frame is written, an
/// error if it never was (the session failed, the connection closed, or
/// the frame could not be written).
pub(crate) type TradeSent = oneshot::Receiver<Result<(), CTraderError>>;

/// An authenticated cTrader session on one account.
pub(super) struct CTraderWebSocketClient {
    cmd_tx: mpsc::UnboundedSender<HandlerCommand>,
    /// The handler task, explicitly singular: `disconnect` joins it.
    handler: TaskSlot<()>,
    /// The spot subscriptions, which the handler marks, confirms and fails.
    subscriptions: SubscriptionState,
    /// The session's authentication, which the handler tracks.
    auth: AuthTracker,
    /// The slots of the historical requests.
    historical: RateLimiter<Ustr, MonotonicClock>,
    /// The socket's state publication and reconnect registration, if a
    /// node's client owns the session.
    socket: Option<SocketControl>,
}

impl std::fmt::Debug for CTraderWebSocketClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CTraderWebSocketClient")
            .field("confirmed_spots", &self.subscriptions.len())
            .finish_non_exhaustive()
    }
}

impl CTraderWebSocketClient {
    /// Connects to `url`, then authenticates the application and the
    /// account of `credential` with the token file's pair (seeded from the
    /// credential the first time, renewed first if due), sending the
    /// heartbeat every `heartbeat`; with the spot events of the symbols
    /// `subscribe_spots` subscribes. `socket`, a node's client's, reports
    /// the socket's state and takes the node's reconnect of it.
    ///
    /// # Errors
    ///
    /// Returns an error if the token file cannot be read or written, the
    /// connection fails, or cTrader refuses an authentication or the
    /// renewal (naming its error code and what to do, never a credential)
    /// or does not answer within `AUTH_TIMEOUT`.
    pub(super) async fn connect(
        url: &str,
        credential: CTraderCredential,
        heartbeat: Duration,
        socket: Option<SocketControl>,
    ) -> anyhow::Result<(Self, SpotEvents)> {
        // While another session renews the pair, its new one is awaited.
        let tokens = loop {
            if let Some(tokens) = credential.tokens().try_load()? {
                break tokens;
            }
            time::sleep(LOCK_RETRY).await;
        };
        let (raw_tx, raw_rx) = mpsc::unbounded_channel::<(u64, Message)>();
        let message_handler: EpochMessageHandler = Arc::new(move |epoch, message| {
            // The receiver is gone only once the handler has stopped.
            let _ = raw_tx.send((epoch, message));
        });
        let config = WebSocketConfig::builder()
            .url(url.to_owned())
            .heartbeat_timeout_secs(DEAD_PEER_TIMEOUT_SECS)
            .build()
            .context("the cTrader WebSocket config")?;
        let historical = Quota::with_period(HISTORICAL_PERIOD)
            .context("the historical request period is zero")?;
        let historical =
            RateLimiter::new_with_quota(None, vec![(Ustr::from(HISTORICAL_KEY), historical)]);
        let client = WebSocketClient::epoch_builder()
            .config(config)
            .epoch_handler(message_handler)
            .maybe_state_sink(socket.as_ref().map(SocketControl::sink))
            .connect()
            .await
            .with_context(|| format!("connecting to cTrader at {url}"))?;
        let reconnect = client.reconnect_handle();

        // The transport invalidates the session's authentication when the
        // connection drops and fails it once closed. Its reconnect buffer
        // waits for the authentication; the handler's frames, bound to their
        // connection, never go through it.
        let auth = AuthTracker::new();
        client.set_auth_tracker(auth.clone(), true);
        let subscriptions = SubscriptionState::new(TOPIC_DELIMITER);
        let authenticated = auth.begin();
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        cmd_tx
            .send(HandlerCommand::SetClient(client))
            .ok()
            .context("the cTrader handler is gone")?;
        let (spot_tx, spot_rx) = mpsc::unbounded_channel();
        let handler = CTraderWsHandler::new(
            credential,
            tokens,
            auth.clone(),
            subscriptions.clone(),
            cmd_rx,
            raw_rx,
            spot_tx,
            heartbeat,
        );
        let mut handler_task = TaskSlot::new();
        handler_task
            .spawn(handler.run())
            .map_err(|e| anyhow::anyhow!("starting the cTrader handler: {e}"))?;
        if let Some(socket) = &socket {
            socket.register(move || reconnect.request_reconnect());
        }
        let connected = Self {
            cmd_tx,
            handler: handler_task,
            subscriptions,
            auth: auth.clone(),
            historical,
            socket,
        };
        match auth
            .wait_for_result::<String>(AUTH_TIMEOUT, authenticated)
            .await
        {
            Ok(()) => Ok((connected, spot_rx)),
            Err(reason) => {
                connected.disconnect().await;
                anyhow::bail!("authenticating on cTrader: {reason}")
            }
        }
    }

    /// Sends `request`, a historical one once its slot comes, and awaits
    /// its response.
    ///
    /// # Errors
    ///
    /// Returns cTrader's error response, or an error if the session cannot
    /// serve requests, the connection is lost first, or no response comes
    /// within `REQUEST_TIMEOUT`.
    pub(super) async fn request<Req: Payload, Res: Payload>(
        &self,
        request: &Req,
    ) -> Result<Res, CTraderError> {
        let answered = async {
            if is_historical(Req::TYPE) {
                self.historical_slot().await;
            }
            let (reply, response) = oneshot::channel();
            self.cmd_tx
                .send(HandlerCommand::Request {
                    payload_type: Req::TYPE,
                    payload: request.encode_to_vec(),
                    reply,
                })
                .map_err(|_| CTraderError::ConnectionLost)?;
            response.await.map_err(|_| CTraderError::ConnectionLost)?
        };
        let envelope = time::timeout(REQUEST_TIMEOUT, answered)
            .await
            .map_err(|_| CTraderError::Timeout)??;
        open(&envelope)
    }

    /// Waits for a slot of the historical requests taken while the session
    /// is authenticated; one taken while it authenticates again is spent.
    /// A failed session takes none: the handler answers with its failure.
    async fn historical_slot(&self) {
        let keys = [Ustr::from(HISTORICAL_KEY)];
        while self.auth.wait_for_authenticated(REQUEST_TIMEOUT).await {
            self.historical.await_keys_ready(Some(&keys)).await;
            if self.auth.is_authenticated() {
                return;
            }
        }
    }

    /// Subscribes to the spot events of `symbol_id`, with their
    /// timestamps, for the rest of the session, reconnects included. A
    /// symbol already subscribed is left as it is. cTrader's refusal is
    /// logged and marks the subscription failed, kept for the next
    /// connection: its answer comes after this returns.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection is closed.
    pub(super) fn subscribe_spots(&self, symbol_id: i64) -> Result<(), CTraderError> {
        self.cmd_tx
            .send(HandlerCommand::SubscribeSpots(symbol_id))
            .map_err(|_| CTraderError::ConnectionLost)
    }

    /// Ends the spot subscription of `symbol_id`, if any. cTrader may
    /// still send a few of its spot events.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection is closed.
    pub(super) fn unsubscribe_spots(&self, symbol_id: i64) -> Result<(), CTraderError> {
        self.cmd_tx
            .send(HandlerCommand::UnsubscribeSpots(symbol_id))
            .map_err(|_| CTraderError::ConnectionLost)
    }

    /// Sends `request`, a trade request, under `client_msg_id`, once the
    /// session is ready; its outcome comes as trade events under that id.
    /// The answer says whether it was sent.
    pub(super) fn trade<Req: Payload>(&self, request: &Req, client_msg_id: String) -> TradeSent {
        let (sent, sending) = oneshot::channel();
        let command = HandlerCommand::Trade {
            payload_type: Req::TYPE,
            payload: request.encode_to_vec(),
            client_msg_id,
            sent,
        };
        if let Err(mpsc::error::SendError(HandlerCommand::Trade { sent, .. })) =
            self.cmd_tx.send(command)
        {
            let _ = sent.send(Err(CTraderError::ConnectionLost));
        }
        sending
    }

    /// The trade events of the account from now on: those of the trade
    /// requests, and every other execution event and order error.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection is closed.
    pub(super) fn trade_events(&self) -> Result<TradeEvents, CTraderError> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.cmd_tx
            .send(HandlerCommand::TradeEvents(tx))
            .map_err(|_| CTraderError::ConnectionLost)?;
        Ok(rx)
    }

    /// Renews the access token now, as on schedule, if the session is
    /// ready.
    #[cfg(any(test, feature = "test-support"))]
    pub(super) fn renew_token(&self) {
        let _ = self.cmd_tx.send(HandlerCommand::RenewToken);
    }

    /// Asks the handler to close the connection, once a token renewal out
    /// is answered (and its pair saved) or `AUTH_TIMEOUT` passed; requests
    /// still waiting fail. `disconnect` awaits it.
    pub(super) fn begin_shutdown(&self) {
        // The handler is gone once it has stopped.
        let _ = self.cmd_tx.send(HandlerCommand::Disconnect);
    }

    /// Closes the connection as `begin_shutdown` asks, joins the handler
    /// within `DISCONNECT_TIMEOUT`, then aborts it, and deregisters the
    /// socket from the node's reconnects.
    pub(super) async fn disconnect(mut self) {
        self.begin_shutdown();
        match finish_task(&mut self.handler, DISCONNECT_TIMEOUT, ABORT_TIMEOUT).await {
            None | Some(TaskJoinOutcome::Completed(())) => {}
            Some(TaskJoinOutcome::Aborted) => {
                log::warn!("the cTrader handler did not stop in time: aborted");
            }
            Some(TaskJoinOutcome::Failed(e)) => log::error!("the cTrader handler failed: {e}"),
            Some(TaskJoinOutcome::Incomplete) => {
                log::error!("the cTrader handler did not stop after its abort");
            }
        }
        if let Some(socket) = &self.socket {
            socket.deregister();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use jiff::Timestamp;
    use prost::Message as _;

    use super::*;
    use crate::{
        common::messages::{
            ProtoHeartbeatEvent, ProtoMessage, ProtoOaAccountAuthReq, ProtoOaAccountAuthRes,
            ProtoOaAccountDisconnectEvent, ProtoOaApplicationAuthReq, ProtoOaClientDisconnectEvent,
            ProtoOaErrorRes, ProtoOaGetTickDataReq, ProtoOaGetTickDataRes, ProtoOaQuoteType,
            ProtoOaRefreshTokenReq, ProtoOaRefreshTokenRes, ProtoOaSpotEvent,
            ProtoOaSubscribeSpotsReq, ProtoOaSubscribeSpotsRes, ProtoOaSymbol,
            ProtoOaSymbolByIdReq, ProtoOaSymbolByIdRes, ProtoOaUnsubscribeSpotsReq,
            ProtoOaUnsubscribeSpotsRes,
        },
        common::token::{RENEW_BEFORE, TokenStore},
        testing::{
            self, ACCESS_TOKEN, ACCOUNT_AUTH_TOKEN_INVALID, ACCOUNT_ID, APPLICATION_AUTH_REFUSED,
            Action, EXPIRES_IN, REFRESH_TOKEN, Received, SYMBOL_BY_ID_UNKNOWN, SharedVenue,
            TokenVenue, accept_auth, credential_expiring, renewed_access, replay, reply,
            token_peer,
        },
    };

    const SLOW_HEARTBEAT: Duration = Duration::from_secs(3600);

    /// A session whose spot events nobody reads.
    async fn connect(
        url: &str,
        credential: CTraderCredential,
        heartbeat: Duration,
    ) -> anyhow::Result<CTraderWebSocketClient> {
        let (client, _) = CTraderWebSocketClient::connect(url, credential, heartbeat, None).await?;
        Ok(client)
    }

    fn decode<M: Payload>(frame: &ProtoMessage) -> M {
        assert_eq!(frame.payload_type, M::TYPE);
        M::decode(frame.payload.as_deref().unwrap_or_default()).unwrap()
    }

    /// A cTrader that accepts the auth and answers a symbol request with
    /// the symbols it names.
    fn symbols_peer(connection: usize, request: &ProtoMessage) -> Vec<Action> {
        let _ = connection;
        if let Some(actions) = accept_auth(request) {
            return actions;
        }
        let asked: ProtoOaSymbolByIdReq = decode(request);
        vec![reply(request, &symbols(&asked.symbol_id))]
    }

    fn symbols(ids: &[i64]) -> ProtoOaSymbolByIdRes {
        ProtoOaSymbolByIdRes {
            ctid_trader_account_id: ACCOUNT_ID,
            symbol: ids
                .iter()
                .map(|&symbol_id| ProtoOaSymbol {
                    symbol_id,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn symbol_request(id: i64) -> ProtoOaSymbolByIdReq {
        ProtoOaSymbolByIdReq {
            ctid_trader_account_id: ACCOUNT_ID,
            symbol_id: vec![id],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn the_session_authenticates_the_application_then_the_account() {
        let (url, received) = testing::start(symbols_peer);

        let (credential, _tokens) = testing::credential();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();

        let frames = received.lock().unwrap().clone();
        assert_eq!(frames.len(), 2, "{frames:?}");
        let application: ProtoOaApplicationAuthReq = decode(&frames[0].1);
        assert_eq!(application.client_id, "12345_clientIdValue");
        assert_eq!(application.client_secret, "clientSecretValue");
        let account: ProtoOaAccountAuthReq = decode(&frames[1].1);
        assert_eq!(account.ctid_trader_account_id, ACCOUNT_ID);
        assert_eq!(account.access_token, "accessTokenValue");
        assert_ne!(frames[0].1.client_msg_id, frames[1].1.client_msg_id);
        client.disconnect().await;
    }

    #[tokio::test]
    async fn the_heartbeat_is_a_binary_heartbeat_event() {
        let (url, received) = testing::start(symbols_peer);
        let (credential, _tokens) = testing::credential();
        let client = connect(&url, credential, Duration::from_millis(50))
            .await
            .unwrap();

        let frames = testing::wait_until(&received, |frames| {
            frames
                .iter()
                .filter(|(_, f)| f.payload_type == ProtoHeartbeatEvent::TYPE)
                .count()
                >= 2
        })
        .await;

        let heartbeat = frames
            .iter()
            .map(|(_, f)| f)
            .find(|f| f.payload_type == 51)
            .unwrap();
        assert_eq!(heartbeat.client_msg_id, None);
        assert_eq!(
            decode::<ProtoHeartbeatEvent>(heartbeat),
            ProtoHeartbeatEvent::default()
        );
        client.disconnect().await;
    }

    #[tokio::test]
    async fn each_request_gets_its_own_response_whatever_their_order() {
        let held = Arc::new(Mutex::new(None::<ProtoMessage>));
        let (url, _) = testing::start(move |_, request| {
            if let Some(actions) = accept_auth(request) {
                return actions;
            }
            let mut held = held.lock().unwrap();
            let Some(first) = held.take() else {
                *held = Some(request.clone());
                return vec![];
            };
            let answer =
                |r: &ProtoMessage| reply(r, &symbols(&decode::<ProtoOaSymbolByIdReq>(r).symbol_id));
            vec![answer(request), answer(&first)]
        });
        let (credential, _tokens) = testing::credential();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();

        let (first, second) = (symbol_request(1), symbol_request(2));
        let (one, two) = tokio::join!(
            client.request::<_, ProtoOaSymbolByIdRes>(&first),
            client.request::<_, ProtoOaSymbolByIdRes>(&second),
        );

        assert_eq!(one.unwrap(), symbols(&[1]));
        assert_eq!(two.unwrap(), symbols(&[2]));
        client.disconnect().await;
    }

    /// The error is the fixture `symbol_by_id_unknown`.
    #[tokio::test]
    async fn a_venue_error_answers_its_request() {
        let (url, _) = testing::start(|_, request| {
            accept_auth(request)
                .unwrap_or_else(|| vec![replay::<ProtoOaErrorRes>(request, SYMBOL_BY_ID_UNKNOWN)])
        });
        let (credential, _tokens) = testing::credential();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();

        let error = client
            .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(987_654_321))
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "cTrader error SYMBOL_NOT_FOUND: No symbols with requested ID"
        );
        client.disconnect().await;
    }

    /// The token error is the fixture `account_auth_token_invalid`; the
    /// renewal answer is synthetic, as OpenApiMessages.proto defines it.
    #[tokio::test]
    async fn a_token_refused_even_renewed_fails_naming_the_code_and_the_playground() {
        let (url, received) = testing::start(|_, request| match request.payload_type {
            t if t == ProtoOaAccountAuthReq::TYPE => vec![replay::<ProtoOaErrorRes>(
                request,
                ACCOUNT_AUTH_TOKEN_INVALID,
            )],
            t if t == ProtoOaRefreshTokenReq::TYPE => {
                let renewed = ProtoOaRefreshTokenRes {
                    access_token: "renewedAccessValue".to_owned(),
                    token_type: "bearer".to_owned(),
                    expires_in: EXPIRES_IN,
                    refresh_token: "renewedRefreshValue".to_owned(),
                    ..Default::default()
                };
                vec![reply(request, &renewed)]
            }
            _ => accept_auth(request).unwrap_or_default(),
        });

        let (credential, _tokens) = testing::credential();
        let err = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap_err();

        let err = format!("{err:#} {err:?}");
        assert!(
            err.contains(
                "cTrader refused the account authentication (CH_ACCESS_TOKEN_INVALID) even with \
                 a renewed access token"
            ),
            "{err}"
        );
        assert!(
            err.contains(
                "Playground of the app on openapi.ctrader.com and set ACME_ACCESS_TOKEN and \
                 ACME_REFRESH_TOKEN in acme.env (see the ACME guide)"
            ),
            "{err}"
        );
        for secret in [
            ACCESS_TOKEN,
            REFRESH_TOKEN,
            "renewedAccessValue",
            "renewedRefreshValue",
            "clientSecretValue",
            "12345_clientIdValue",
            "1000001",
        ] {
            assert!(!err.contains(secret), "{err}");
        }
        assert_eq!(
            payload_types(&received, 0),
            [2100, 2102, 2173, 2102],
            "renewed once"
        );
    }

    /// The refusal is the fixture `application_auth_refused`.
    #[tokio::test]
    async fn a_refused_application_fails_before_any_account_auth() {
        let (url, received) = testing::start(|_, request| {
            vec![replay::<ProtoOaErrorRes>(request, APPLICATION_AUTH_REFUSED)]
        });

        let (credential, _tokens) = testing::credential();
        let err = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap_err();

        assert!(
            format!("{err:#}").contains(
                "the application authentication (CH_CLIENT_AUTH_FAILURE): check ACME_CLIENT_ID \
                 and ACME_CLIENT_SECRET"
            ),
            "{err:#}"
        );
        let frames = received.lock().unwrap().clone();
        assert!(
            frames
                .iter()
                .all(|(_, f)| f.payload_type == ProtoOaApplicationAuthReq::TYPE),
            "{frames:?}"
        );
    }

    /// Synthetic: the account auth response of another account.
    #[tokio::test]
    async fn another_account_authenticated_is_refused() {
        let (url, _) = testing::start(|_, request| {
            if request.payload_type == ProtoOaAccountAuthReq::TYPE {
                let other = ProtoOaAccountAuthRes {
                    ctid_trader_account_id: ACCOUNT_ID + 1,
                    ..Default::default()
                };
                return vec![reply(request, &other)];
            }
            accept_auth(request).unwrap_or_default()
        });

        let (credential, _tokens) = testing::credential();
        let err = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap_err();

        assert!(
            format!("{err:#}")
                .contains("cTrader authenticated another account than ACME_ACCOUNT_ID"),
            "{err:#}"
        );
    }

    /// Synthetic: bytes that are no frame, and an event of a payload type
    /// no `.proto` defines.
    #[tokio::test]
    async fn malformed_and_unknown_frames_are_skipped() {
        let (url, _) = testing::start(|_, request| {
            accept_auth(request).unwrap_or_else(|| {
                let unknown = ProtoMessage {
                    payload_type: 9999,
                    payload: Some(vec![1, 2, 3]),
                    client_msg_id: None,
                };
                let asked: ProtoOaSymbolByIdReq = decode(request);
                vec![
                    Action::Send(vec![0xff, 0xff, 0xff]),
                    Action::Send(unknown.encode_to_vec()),
                    reply(request, &symbols(&asked.symbol_id)),
                ]
            })
        });
        let (credential, _tokens) = testing::credential();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();

        let response = client
            .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(7))
            .await;

        assert_eq!(response, Ok(symbols(&[7])));
        client.disconnect().await;
    }

    #[tokio::test]
    async fn after_a_reconnect_the_session_authenticates_again() {
        let (url, received) = testing::start(|connection, request| {
            let actions = accept_auth(request);
            match (connection, actions) {
                (0, Some(mut actions)) if request.payload_type == ProtoOaAccountAuthReq::TYPE => {
                    actions.push(Action::Close);
                    actions
                }
                (_, Some(actions)) => actions,
                (_, None) => symbols_peer(connection, request),
            }
        });
        let (credential, _tokens) = testing::credential();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();
        testing::wait_until(&received, |frames| {
            frames
                .iter()
                .any(|(c, f)| *c == 1 && f.payload_type == ProtoOaAccountAuthReq::TYPE)
        })
        .await;

        let response = client
            .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(3))
            .await;

        assert_eq!(response, Ok(symbols(&[3])));
        let frames = received.lock().unwrap().clone();
        let second: Vec<u32> = frames
            .iter()
            .filter(|(c, f)| *c == 1 && f.payload_type != ProtoHeartbeatEvent::TYPE)
            .map(|(_, f)| f.payload_type)
            .collect();
        assert_eq!(second, [2100, 2102, 2116]);
        client.disconnect().await;
    }

    fn tick_request() -> ProtoOaGetTickDataReq {
        ProtoOaGetTickDataReq {
            ctid_trader_account_id: ACCOUNT_ID,
            symbol_id: 41,
            r#type: ProtoOaQuoteType::Bid as i32,
            from_timestamp: Some(0),
            to_timestamp: Some(1),
            ..Default::default()
        }
    }

    /// How long seven requests asked at once take, at least, held to
    /// cTrader's 5 historical requests a second: six gaps, each wider than
    /// 200 ms (one every 200 ms was refused on the demo, see
    /// `HISTORICAL_PERIOD`), so above 1.2 s, with a margin for the round
    /// trips at that pace.
    const SEVEN_HISTORICAL: Duration = Duration::from_millis(1250);

    /// cTrader allows 5 historical requests a second per connection: seven
    /// tick requests asked at once are answered no sooner than
    /// `SEVEN_HISTORICAL`.
    #[tokio::test]
    async fn tick_requests_reach_ctrader_at_most_five_a_second() {
        let (url, _) = testing::start(|_, request| {
            if let Some(actions) = accept_auth(request) {
                return actions;
            }
            vec![reply(request, &ProtoOaGetTickDataRes::default())]
        });
        let (credential, _tokens) = testing::credential();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();
        let request = tick_request();
        let start = std::time::Instant::now();

        let answers = futures::future::join_all(
            (0..7).map(|_| client.request::<_, ProtoOaGetTickDataRes>(&request)),
        )
        .await;

        let elapsed = start.elapsed();
        assert!(answers.iter().all(Result::is_ok), "{answers:?}");
        assert!(
            elapsed >= SEVEN_HISTORICAL,
            "7 tick requests in {elapsed:?}"
        );
        client.disconnect().await;
    }

    /// Seven symbol requests asked at once are answered sooner than seven
    /// requests at most 5 a second could be.
    #[tokio::test]
    async fn other_requests_are_not_held_to_the_historical_limit() {
        let (url, _) = testing::start(symbols_peer);
        let (credential, _tokens) = testing::credential();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();
        let requests: Vec<_> = (0..7).map(symbol_request).collect();
        let start = std::time::Instant::now();

        let answers = futures::future::join_all(
            requests
                .iter()
                .map(|request| client.request::<_, ProtoOaSymbolByIdRes>(request)),
        )
        .await;

        let elapsed = start.elapsed();
        assert!(answers.iter().all(Result::is_ok), "{answers:?}");
        assert!(
            elapsed < SEVEN_HISTORICAL,
            "7 symbol requests in {elapsed:?}"
        );
        client.disconnect().await;
    }

    /// A renewal holds the tick requests asked meanwhile: once the account
    /// is authenticated again, they still reach cTrader at most five a
    /// second.
    #[tokio::test]
    async fn tick_requests_held_by_a_renewal_reach_ctrader_at_most_five_a_second() {
        let arrivals = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&arrivals);
        let peer = token_peer(TokenVenue::new(), move |_, request| {
            log.lock().unwrap().push(std::time::Instant::now());
            vec![reply(request, &ProtoOaGetTickDataRes::default())]
        });
        let (url, _) = testing::start(slow_renewal(Duration::from_millis(1100), peer));
        let (credential, _tokens) = testing::credential();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();
        let request = tick_request();

        let answers = {
            let mut ticks = std::pin::pin!(futures::future::join_all(
                (0..7).map(|_| client.request::<_, ProtoOaGetTickDataRes>(&request)),
            ));
            // The tick requests are asked first.
            assert!(futures::poll!(ticks.as_mut()).is_pending());
            client.renew_token();
            ticks.await
        };

        assert!(answers.iter().all(Result::is_ok), "{answers:?}");
        let arrivals = arrivals.lock().unwrap().clone();
        assert_eq!(arrivals.len(), 7);
        for six in arrivals.windows(6) {
            let span = six[5] - six[0];
            assert!(
                span >= Duration::from_millis(900),
                "6 tick requests within {span:?}"
            );
        }
        client.disconnect().await;
    }

    /// A tick request waiting for its slot holds back no other frame: a
    /// trade request asked meanwhile reaches cTrader before it.
    #[tokio::test]
    async fn a_trade_request_does_not_wait_behind_a_tick_request_held_at_the_limit() {
        use crate::common::messages::ProtoOaNewOrderReq;
        let (url, received) = testing::start(|_, request| {
            if let Some(actions) = accept_auth(request) {
                return actions;
            }
            if request.payload_type == ProtoOaGetTickDataReq::TYPE {
                return vec![reply(request, &ProtoOaGetTickDataRes::default())];
            }
            Vec::new()
        });
        let (credential, _tokens) = testing::credential();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();
        let request = tick_request();
        let order = ProtoOaNewOrderReq {
            ctid_trader_account_id: ACCOUNT_ID,
            volume: 100,
            ..Default::default()
        };

        let (sent, answers) = {
            let mut ticks = std::pin::pin!(futures::future::join_all(
                (0..2).map(|_| client.request::<_, ProtoOaGetTickDataRes>(&request)),
            ));
            // The tick requests are asked first.
            assert!(futures::poll!(ticks.as_mut()).is_pending());
            let sent = client.trade(&order, "N:O-1".to_owned()).await;
            (sent, ticks.await)
        };

        assert_eq!(sent, Ok(Ok(())));
        assert!(answers.iter().all(Result::is_ok), "{answers:?}");
        let tick = ProtoOaGetTickDataReq::TYPE;
        assert_eq!(
            payload_types(&received, 0)[2..],
            [tick, ProtoOaNewOrderReq::TYPE, tick]
        );
        client.disconnect().await;
    }

    /// A cTrader that, on connection 0, answers the first request after the
    /// auth with the end of the client's session (synthetic, as
    /// OpenApiMessages.proto defines the event), keeping the socket open;
    /// connection 1 serves symbols.
    fn ending_peer(connection: usize, request: &ProtoMessage) -> Vec<Action> {
        if connection > 0 {
            return symbols_peer(connection, request);
        }
        accept_auth(request).unwrap_or_else(|| {
            let event = ProtoOaClientDisconnectEvent {
                reason: Some("Server maintenance".to_owned()),
                ..Default::default()
            };
            vec![Action::Send(crate::common::messages::frame(&event, None))]
        })
    }

    #[tokio::test]
    async fn once_ctrader_ends_the_session_the_client_reconnects_and_authenticates_again() {
        let (url, received) = testing::start(ending_peer);
        let (credential, _tokens) = testing::credential();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();

        let in_flight = client
            .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(1))
            .await;
        let next = client
            .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(2))
            .await;

        assert_eq!(in_flight, Err(CTraderError::ConnectionLost));
        assert_eq!(next, Ok(symbols(&[2])));
        assert_eq!(
            payload_types(&received, 1),
            [
                ProtoOaApplicationAuthReq::TYPE,
                ProtoOaAccountAuthReq::TYPE,
                ProtoOaSymbolByIdReq::TYPE
            ]
        );
        client.disconnect().await;
    }

    /// The order and the answers are synthetic, as OpenApiMessages.proto
    /// defines them; the `clientMsgId` of the order's events is the
    /// request's, as on the demo (2026-10-26).
    #[tokio::test]
    async fn a_trade_request_goes_under_its_id_and_its_answers_come_as_trade_events() {
        use crate::common::messages::{
            ProtoOaExecutionEvent, ProtoOaExecutionType, ProtoOaNewOrderReq, ProtoOaOrderErrorEvent,
        };
        let accepted = ProtoOaExecutionEvent {
            ctid_trader_account_id: ACCOUNT_ID,
            execution_type: ProtoOaExecutionType::OrderAccepted as i32,
            ..Default::default()
        };
        let error = ProtoOaOrderErrorEvent {
            ctid_trader_account_id: ACCOUNT_ID,
            error_code: "TRADING_BAD_VOLUME".to_owned(),
            ..Default::default()
        };
        let (answer, other) = (accepted.clone(), error.clone());
        let (url, received) = testing::start(move |_, request| {
            accept_auth(request).unwrap_or_else(|| {
                vec![
                    reply(request, &answer),
                    Action::Send(crate::common::messages::frame(&other, None)),
                ]
            })
        });
        let (credential, _tokens) = testing::credential();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();
        let mut trades = client.trade_events().unwrap();
        let order = ProtoOaNewOrderReq {
            ctid_trader_account_id: ACCOUNT_ID,
            volume: 100,
            ..Default::default()
        };

        let sent = client.trade(&order, "N:O-1".to_owned()).await;

        assert_eq!(sent, Ok(Ok(())));
        let first = tokio::time::timeout(Duration::from_secs(10), trades.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            first,
            TradeEvent {
                client_msg_id: Some("N:O-1".to_owned()),
                message: TradeMessage::Execution(accepted),
            }
        );
        let frames = received.lock().unwrap().clone();
        let request = frames
            .iter()
            .map(|(_, f)| f)
            .find(|f| f.payload_type == ProtoOaNewOrderReq::TYPE)
            .unwrap();
        assert_eq!(request.client_msg_id.as_deref(), Some("N:O-1"));
        assert_eq!(decode::<ProtoOaNewOrderReq>(request), order);
        let event = tokio::time::timeout(Duration::from_secs(10), trades.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            event,
            TradeEvent {
                client_msg_id: None,
                message: TradeMessage::OrderError(error),
            }
        );
        client.disconnect().await;
    }

    #[tokio::test]
    async fn a_trade_request_once_ctrader_ended_the_session_goes_out_on_the_next_one() {
        let (url, received) = testing::start(ending_peer);
        let (credential, _tokens) = testing::credential();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();
        let _ = client
            .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(1))
            .await;

        let sent = client
            .trade(
                &crate::common::messages::ProtoOaCancelOrderReq::default(),
                "X:O-1".to_owned(),
            )
            .await;

        assert!(matches!(sent, Ok(Ok(()))), "{sent:?}");
        testing::wait_until(&received, |frames| {
            frames.iter().any(|(c, f)| {
                *c == 1
                    && f.payload_type == crate::common::messages::ProtoOaCancelOrderReq::TYPE
                    && f.client_msg_id.as_deref() == Some("X:O-1")
            })
        })
        .await;
        client.disconnect().await;
    }

    /// A trade request asked while the transport reconnects, the session
    /// still ready as far as the handler knows, goes out on the new
    /// connection only once it is authenticated.
    #[tokio::test]
    async fn a_trade_request_during_a_reconnect_goes_out_once_authenticated_again() {
        use std::sync::atomic::{AtomicBool, Ordering};

        use crate::common::messages::ProtoOaNewOrderReq;
        let reconnecting = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&reconnecting);
        let (url, received) = testing::start_delayed(
            move |connection| {
                if connection == 0 {
                    return Duration::ZERO;
                }
                seen.store(true, Ordering::SeqCst);
                Duration::from_millis(1500)
            },
            |connection, request| {
                if let Some(actions) = accept_auth(request) {
                    return actions;
                }
                if connection == 0 {
                    return vec![Action::Close];
                }
                Vec::new()
            },
        );
        let (credential, _tokens) = testing::credential();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();
        let order = ProtoOaNewOrderReq {
            ctid_trader_account_id: ACCOUNT_ID,
            volume: 100,
            ..Default::default()
        };

        let first = symbol_request(1);

        let (lost, sent) = {
            // The first request closes the connection.
            let mut lost = std::pin::pin!(client.request::<_, ProtoOaSymbolByIdRes>(&first));
            assert!(futures::poll!(lost.as_mut()).is_pending());
            tokio::time::timeout(Duration::from_secs(10), async {
                while !reconnecting.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            let sent = client.trade(&order, "N:O-1".to_owned()).await;
            (lost.await, sent)
        };

        assert_eq!(lost, Err(CTraderError::ConnectionLost));
        assert_eq!(sent, Ok(Ok(())));
        testing::wait_until(&received, |frames| {
            frames
                .iter()
                .any(|(c, f)| *c == 1 && f.payload_type == ProtoOaNewOrderReq::TYPE)
        })
        .await;
        assert_eq!(
            payload_types(&received, 1),
            [
                ProtoOaApplicationAuthReq::TYPE,
                ProtoOaAccountAuthReq::TYPE,
                ProtoOaNewOrderReq::TYPE
            ]
        );
        client.disconnect().await;
    }

    /// Synthetic: a spot event of `symbol_id` as OpenApiMessages.proto
    /// defines it, both sides and the time.
    fn spot_event(symbol_id: i64) -> ProtoOaSpotEvent {
        ProtoOaSpotEvent {
            ctid_trader_account_id: ACCOUNT_ID,
            symbol_id,
            bid: Some(412_847_000),
            ask: Some(412_852_000),
            timestamp: Some(1_792_591_204_967),
            ..Default::default()
        }
    }

    /// A cTrader that accepts the auth, answers a spot subscription and
    /// sends a spot event of each symbol, and answers an unsubscription.
    fn spots_peer(connection: usize, request: &ProtoMessage) -> Vec<Action> {
        let _ = connection;
        if let Some(actions) = accept_auth(request) {
            return actions;
        }
        let answered = |account| ProtoOaSubscribeSpotsRes {
            ctid_trader_account_id: account,
            ..Default::default()
        };
        match request.payload_type {
            t if t == ProtoOaSubscribeSpotsReq::TYPE => {
                let asked: ProtoOaSubscribeSpotsReq = decode(request);
                let mut actions = vec![reply(request, &answered(asked.ctid_trader_account_id))];
                actions.extend(asked.symbol_id.iter().map(|&id| {
                    Action::Send(crate::common::messages::frame(&spot_event(id), None))
                }));
                actions
            }
            t if t == ProtoOaUnsubscribeSpotsReq::TYPE => {
                let asked: ProtoOaUnsubscribeSpotsReq = decode(request);
                let res = ProtoOaUnsubscribeSpotsRes {
                    ctid_trader_account_id: asked.ctid_trader_account_id,
                    ..Default::default()
                };
                vec![reply(request, &res)]
            }
            other => panic!("unexpected request {other}"),
        }
    }

    /// The spot subscriptions received on `connection`.
    fn spot_subscriptions(received: &Received, connection: usize) -> Vec<ProtoOaSubscribeSpotsReq> {
        received
            .lock()
            .unwrap()
            .iter()
            .filter(|(c, f)| *c == connection && f.payload_type == ProtoOaSubscribeSpotsReq::TYPE)
            .map(|(_, f)| decode(f))
            .collect()
    }

    async fn next_spot(spots: &mut SpotEvents) -> ProtoOaSpotEvent {
        tokio::time::timeout(Duration::from_secs(10), spots.recv())
            .await
            .expect("no spot event within 10 s")
            .expect("the spot events ended")
    }

    #[tokio::test]
    async fn a_spot_subscription_asks_for_timestamps_and_its_spot_events_come_through() {
        let (url, received) = testing::start(spots_peer);
        let (credential, _tokens) = testing::credential();
        let (client, mut spots) =
            CTraderWebSocketClient::connect(&url, credential, SLOW_HEARTBEAT, None)
                .await
                .unwrap();

        client.subscribe_spots(41).unwrap();

        assert_eq!(next_spot(&mut spots).await, spot_event(41));
        let asked = spot_subscriptions(&received, 0);
        assert_eq!(asked.len(), 1);
        assert_eq!(asked[0].ctid_trader_account_id, ACCOUNT_ID);
        assert_eq!(asked[0].symbol_id, [41]);
        assert_eq!(asked[0].subscribe_to_spot_timestamp, Some(true));
        client.disconnect().await;
    }

    #[tokio::test]
    async fn a_symbol_subscribed_twice_is_asked_for_once() {
        let (url, received) = testing::start(spots_peer);
        let (credential, _tokens) = testing::credential();
        let (client, mut spots) =
            CTraderWebSocketClient::connect(&url, credential, SLOW_HEARTBEAT, None)
                .await
                .unwrap();

        client.subscribe_spots(41).unwrap();
        client.subscribe_spots(41).unwrap();
        client.subscribe_spots(1).unwrap();
        next_spot(&mut spots).await;
        next_spot(&mut spots).await;

        let asked: Vec<Vec<i64>> = spot_subscriptions(&received, 0)
            .into_iter()
            .map(|r| r.symbol_id)
            .collect();
        assert_eq!(asked, [vec![41], vec![1]]);
        client.disconnect().await;
    }

    /// Synthetic, as OpenApiModelMessages.proto defines the code: cTrader
    /// refusing the spot subscription of a symbol it does not list, on the
    /// first connection only, which it closes once it has answered the
    /// subscription of symbol 1.
    #[tokio::test]
    async fn a_refused_spot_subscription_is_sent_again_on_the_next_connection() {
        let (url, received) = testing::start(|connection, request| {
            if request.payload_type != ProtoOaSubscribeSpotsReq::TYPE || connection > 0 {
                return spots_peer(connection, request);
            }
            let asked: ProtoOaSubscribeSpotsReq = decode(request);
            if asked.symbol_id == [41] {
                let error = ProtoOaErrorRes {
                    error_code: "SYMBOL_NOT_FOUND".to_owned(),
                    ..Default::default()
                };
                return vec![reply(request, &error)];
            }
            let mut actions = spots_peer(connection, request);
            actions.push(Action::Close);
            actions
        });
        let (credential, _tokens) = testing::credential();
        let (client, mut spots) =
            CTraderWebSocketClient::connect(&url, credential, SLOW_HEARTBEAT, None)
                .await
                .unwrap();

        client.subscribe_spots(41).unwrap();
        client.subscribe_spots(1).unwrap();
        assert_eq!(next_spot(&mut spots).await, spot_event(1));
        testing::wait_until(&received, |_| !spot_subscriptions(&received, 1).is_empty()).await;

        let again: Vec<Vec<i64>> = spot_subscriptions(&received, 1)
            .into_iter()
            .map(|r| r.symbol_id)
            .collect();
        assert_eq!(again, [vec![1, 41]]);
        client.disconnect().await;
    }

    /// The peer closes the first connection once it has answered `close_on`.
    fn closing_peer(
        close_on: u32,
    ) -> impl FnMut(usize, &ProtoMessage) -> Vec<Action> + Send + 'static {
        move |connection, request| {
            let mut actions = spots_peer(connection, request);
            if connection == 0 && request.payload_type == close_on {
                actions.push(Action::Close);
            }
            actions
        }
    }

    #[tokio::test]
    async fn after_a_reconnect_the_spot_subscriptions_are_sent_again() {
        let (url, received) = testing::start(closing_peer(ProtoOaSubscribeSpotsReq::TYPE));
        let (credential, _tokens) = testing::credential();
        let (client, mut spots) =
            CTraderWebSocketClient::connect(&url, credential, SLOW_HEARTBEAT, None)
                .await
                .unwrap();

        client.subscribe_spots(41).unwrap();

        assert_eq!(next_spot(&mut spots).await, spot_event(41));
        assert_eq!(next_spot(&mut spots).await, spot_event(41));
        let again = spot_subscriptions(&received, 1);
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].symbol_id, [41]);
        assert_eq!(again[0].subscribe_to_spot_timestamp, Some(true));
        let frames = received.lock().unwrap().clone();
        let second: Vec<u32> = frames
            .iter()
            .filter(|(c, f)| *c == 1 && f.payload_type != ProtoHeartbeatEvent::TYPE)
            .map(|(_, f)| f.payload_type)
            .collect();
        assert_eq!(second, [2100, 2102, 2127]);
        client.disconnect().await;
    }

    #[tokio::test]
    async fn an_unsubscribed_symbol_is_not_subscribed_again_after_a_reconnect() {
        let (url, received) = testing::start(closing_peer(ProtoOaUnsubscribeSpotsReq::TYPE));
        let (credential, _tokens) = testing::credential();
        let (client, mut spots) =
            CTraderWebSocketClient::connect(&url, credential, SLOW_HEARTBEAT, None)
                .await
                .unwrap();
        client.subscribe_spots(41).unwrap();
        client.subscribe_spots(1).unwrap();
        next_spot(&mut spots).await;
        next_spot(&mut spots).await;

        client.unsubscribe_spots(41).unwrap();

        assert_eq!(next_spot(&mut spots).await, spot_event(1));
        let unsubscribed: Vec<ProtoOaUnsubscribeSpotsReq> = received
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, f)| f.payload_type == ProtoOaUnsubscribeSpotsReq::TYPE)
            .map(|(_, f)| decode(f))
            .collect();
        assert_eq!(unsubscribed.len(), 1);
        assert_eq!(unsubscribed[0].symbol_id, [41]);
        let again: Vec<Vec<i64>> = spot_subscriptions(&received, 1)
            .into_iter()
            .map(|r| r.symbol_id)
            .collect();
        assert_eq!(again, [vec![1]]);
        client.disconnect().await;
    }

    /// A spot subscription asked while the transport reconnects, the
    /// session still ready as far as the handler knows, goes out on the new
    /// connection with the others, once it is authenticated.
    #[tokio::test]
    async fn a_spot_subscription_during_a_reconnect_goes_out_once_authenticated_again() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let reconnecting = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&reconnecting);
        let (url, received) = testing::start_delayed(
            move |connection| {
                if connection == 0 {
                    return Duration::ZERO;
                }
                seen.store(true, Ordering::SeqCst);
                Duration::from_millis(1500)
            },
            closing_peer(ProtoOaSubscribeSpotsReq::TYPE),
        );
        let (credential, _tokens) = testing::credential();
        let (client, mut spots) =
            CTraderWebSocketClient::connect(&url, credential, SLOW_HEARTBEAT, None)
                .await
                .unwrap();
        client.subscribe_spots(1).unwrap();
        assert_eq!(next_spot(&mut spots).await, spot_event(1));
        tokio::time::timeout(Duration::from_secs(10), async {
            while !reconnecting.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();

        client.subscribe_spots(41).unwrap();

        let mut events = [
            next_spot(&mut spots).await.symbol_id,
            next_spot(&mut spots).await.symbol_id,
        ];
        events.sort_unstable();
        assert_eq!(events, [1, 41]);
        assert_eq!(
            payload_types(&received, 1),
            [
                ProtoOaApplicationAuthReq::TYPE,
                ProtoOaAccountAuthReq::TYPE,
                ProtoOaSubscribeSpotsReq::TYPE
            ]
        );
        let mut again = spot_subscriptions(&received, 1).remove(0).symbol_id;
        again.sort_unstable();
        assert_eq!(again, [1, 41]);
        assert_eq!(spot_subscriptions(&received, 0).len(), 1);
        client.disconnect().await;
    }

    /// The payload types received on `connection`, heartbeats aside.
    fn payload_types(received: &Received, connection: usize) -> Vec<u32> {
        received
            .lock()
            .unwrap()
            .iter()
            .filter(|(c, f)| *c == connection && f.payload_type != ProtoHeartbeatEvent::TYPE)
            .map(|(_, f)| f.payload_type)
            .collect()
    }

    /// The access tokens of the account auths received on `connection`.
    fn account_auths(received: &Received, connection: usize) -> Vec<String> {
        received
            .lock()
            .unwrap()
            .iter()
            .filter(|(c, f)| *c == connection && f.payload_type == ProtoOaAccountAuthReq::TYPE)
            .map(|(_, f)| decode::<ProtoOaAccountAuthReq>(f).access_token)
            .collect()
    }

    /// The refresh tokens of the renewals received.
    fn renewals(received: &Received) -> Vec<String> {
        received
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, f)| f.payload_type == ProtoOaRefreshTokenReq::TYPE)
            .map(|(_, f)| decode::<ProtoOaRefreshTokenReq>(f).refresh_token)
            .collect()
    }

    /// The pair the token file holds: the tokens and the expiry.
    fn stored(tokens: &TokenStore) -> (String, String, Option<Timestamp>) {
        let pair = tokens.read().unwrap();
        (
            pair.access_token.expose_secret().to_owned(),
            pair.refresh_token.expose_secret().to_owned(),
            pair.expires_at,
        )
    }

    /// A peer on `venue` serving symbols and spots.
    /// cTrader keeps a connection's spot subscriptions across a new
    /// account auth on it, and refuses one already held with
    /// `ALREADY_SUBSCRIBED` (demo, 2026-10-26).
    fn venue_peer(
        venue: &SharedVenue,
    ) -> impl FnMut(usize, &ProtoMessage) -> Vec<Action> + Send + 'static {
        let mut held = std::collections::HashSet::<(usize, i64)>::new();
        token_peer(
            Arc::clone(venue),
            move |connection, request| match request.payload_type {
                t if t == ProtoOaSymbolByIdReq::TYPE => symbols_peer(connection, request),
                t if t == ProtoOaSubscribeSpotsReq::TYPE => {
                    let asked: ProtoOaSubscribeSpotsReq = decode(request);
                    if asked
                        .symbol_id
                        .iter()
                        .any(|&id| held.contains(&(connection, id)))
                    {
                        let error = ProtoOaErrorRes {
                            error_code: "ALREADY_SUBSCRIBED".to_owned(),
                            ..Default::default()
                        };
                        return vec![reply(request, &error)];
                    }
                    held.extend(asked.symbol_id.iter().map(|&id| (connection, id)));
                    spots_peer(connection, request)
                }
                t if t == ProtoOaUnsubscribeSpotsReq::TYPE => {
                    let asked: ProtoOaUnsubscribeSpotsReq = decode(request);
                    for id in &asked.symbol_id {
                        held.remove(&(connection, *id));
                    }
                    spots_peer(connection, request)
                }
                _ => spots_peer(connection, request),
            },
        )
    }

    /// The symbols of the spot unsubscriptions received on `connection`.
    fn spot_unsubscriptions(received: &Received, connection: usize) -> Vec<Vec<i64>> {
        received
            .lock()
            .unwrap()
            .iter()
            .filter(|(c, f)| *c == connection && f.payload_type == ProtoOaUnsubscribeSpotsReq::TYPE)
            .map(|(_, f)| decode::<ProtoOaUnsubscribeSpotsReq>(f).symbol_id)
            .collect()
    }

    /// A token file whose access token is due for renewal `after` from now.
    fn due_after(after: Duration) -> Option<Timestamp> {
        Some(Timestamp::now() + RENEW_BEFORE + jiff::SignedDuration::try_from(after).unwrap())
    }

    #[tokio::test]
    async fn a_token_of_unknown_expiry_is_renewed_and_saved_before_the_account_auth() {
        let venue = TokenVenue::new();
        let (url, received) = testing::start(venue_peer(&venue));
        let (credential, _dir) = credential_expiring(None);
        let tokens = credential.tokens();
        let before = Timestamp::now();

        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();

        assert_eq!(payload_types(&received, 0), [2100, 2173, 2102]);
        assert_eq!(renewals(&received), [REFRESH_TOKEN]);
        assert_eq!(account_auths(&received, 0), [renewed_access(1)]);
        let (access, refresh, expires_at) = stored(&tokens);
        assert_eq!(
            (access, refresh),
            (renewed_access(1), "renewedRefresh1".into())
        );
        let lifetime = jiff::SignedDuration::from_secs(EXPIRES_IN);
        let expires_at = expires_at.unwrap();
        assert!(before + lifetime <= expires_at && expires_at <= Timestamp::now() + lifetime);
        client.disconnect().await;
    }

    #[tokio::test]
    async fn a_token_expiring_within_a_day_is_renewed_at_start() {
        let venue = TokenVenue::new();
        let (url, received) = testing::start(venue_peer(&venue));
        let (credential, _dir) = credential_expiring(Some(
            Timestamp::now() + jiff::SignedDuration::from_hours(23),
        ));

        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();

        assert_eq!(payload_types(&received, 0), [2100, 2173, 2102]);
        assert_eq!(account_auths(&received, 0), [renewed_access(1)]);
        client.disconnect().await;
    }

    #[tokio::test]
    async fn a_token_expiring_later_than_a_day_is_not_renewed_at_start() {
        let venue = TokenVenue::new();
        let (url, received) = testing::start(venue_peer(&venue));
        let (credential, _dir) = credential_expiring(Some(
            Timestamp::now() + jiff::SignedDuration::from_hours(25),
        ));

        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();

        assert_eq!(payload_types(&received, 0), [2100, 2102]);
        assert_eq!(account_auths(&received, 0), [ACCESS_TOKEN]);
        client.disconnect().await;
    }

    /// The symbol request is answered, and a spot sent, only once the
    /// account authenticated again with the renewed token: the request was
    /// in flight across the renewal, the subscription kept by cTrader.
    #[tokio::test]
    async fn a_request_in_flight_across_a_renewal_is_answered_and_the_spots_kept() {
        let venue = TokenVenue::new();
        let held = Arc::new(Mutex::new(None::<ProtoMessage>));
        let renewed = Arc::clone(&venue);
        let mut peer = venue_peer(&venue);
        let (url, received) = testing::start(move |connection, request| {
            if request.payload_type == ProtoOaSymbolByIdReq::TYPE
                && renewed.lock().unwrap().renewals() == 0
            {
                *held.lock().unwrap() = Some(request.clone());
                return vec![];
            }
            let mut actions = peer(connection, request);
            let reauthenticated = request.payload_type == ProtoOaAccountAuthReq::TYPE
                && renewed.lock().unwrap().renewals() == 1;
            if let Some(request) = held.lock().unwrap().take_if(|_| reauthenticated) {
                actions.extend(symbols_peer(connection, &request));
                actions.push(Action::Send(crate::common::messages::frame(
                    &spot_event(41),
                    None,
                )));
            }
            actions
        });
        let (credential, _dir) = testing::credential();
        let (client, mut spots) =
            CTraderWebSocketClient::connect(&url, credential, SLOW_HEARTBEAT, None)
                .await
                .unwrap();
        client.subscribe_spots(41).unwrap();
        next_spot(&mut spots).await;
        let request = symbol_request(3);

        let (in_flight, ()) =
            tokio::join!(client.request::<_, ProtoOaSymbolByIdRes>(&request), async {
                testing::wait_until(&received, |frames| {
                    frames
                        .iter()
                        .any(|(_, f)| f.payload_type == ProtoOaSymbolByIdReq::TYPE)
                })
                .await;
                client.renew_token();
            },);

        assert_eq!(in_flight, Ok(symbols(&[3])));
        assert_eq!(next_spot(&mut spots).await, spot_event(41));
        assert_eq!(
            account_auths(&received, 0),
            [ACCESS_TOKEN.to_owned(), renewed_access(1)]
        );
        assert_eq!(
            spot_subscriptions(&received, 0).len(),
            1,
            "subscribed again"
        );
        client.disconnect().await;
    }

    #[tokio::test]
    async fn a_running_session_renews_its_token_on_schedule() {
        let venue = TokenVenue::new();
        let (url, received) = testing::start(venue_peer(&venue));
        let (credential, _dir) = credential_expiring(due_after(Duration::from_secs(1)));
        let tokens = credential.tokens();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();

        testing::wait_until(&received, |_| {
            account_auths(&received, 0).last() == Some(&renewed_access(1))
        })
        .await;

        assert_eq!(
            account_auths(&received, 0),
            [ACCESS_TOKEN.to_owned(), renewed_access(1)]
        );
        assert_eq!(renewals(&received), [REFRESH_TOKEN]);
        assert_eq!(stored(&tokens).0, renewed_access(1));
        client.disconnect().await;
    }

    /// The event comes with the next request, whose answer follows it.
    #[tokio::test]
    async fn an_invalidated_token_is_renewed_and_the_account_authenticated_again() {
        let venue = TokenVenue::new();
        let (url, received) = testing::start(venue_peer(&venue));
        let (credential, _dir) = testing::credential();
        let tokens = credential.tokens();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();
        venue.lock().unwrap().expire();

        let in_flight = client
            .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(1))
            .await;
        testing::wait_until(&received, |_| venue.lock().unwrap().renewals() == 1).await;
        let next = client
            .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(2))
            .await;

        assert_eq!(in_flight, Ok(symbols(&[1])));
        assert_eq!(next, Ok(symbols(&[2])));
        assert_eq!(
            account_auths(&received, 0),
            [
                ACCESS_TOKEN.to_owned(),
                ACCESS_TOKEN.to_owned(),
                renewed_access(1)
            ]
        );
        assert_eq!(stored(&tokens).0, renewed_access(1));
        client.disconnect().await;
    }

    #[tokio::test]
    async fn a_token_refused_as_expired_at_start_is_renewed() {
        let venue = TokenVenue::new();
        venue.lock().unwrap().expire();
        let (url, received) = testing::start(venue_peer(&venue));
        let (credential, _dir) = testing::credential();

        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();

        assert_eq!(payload_types(&received, 0), [2100, 2102, 2173, 2102]);
        assert_eq!(
            account_auths(&received, 0),
            [ACCESS_TOKEN.to_owned(), renewed_access(1)]
        );
        assert_eq!(
            client
                .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(1))
                .await,
            Ok(symbols(&[1]))
        );
        client.disconnect().await;
    }

    /// The refusal is synthetic: an error as OpenApiMessages.proto defines it.
    #[tokio::test]
    async fn a_refused_renewal_fails_naming_the_playground_and_keeps_the_pair() {
        let venue = TokenVenue::refusing();
        let (url, _) = testing::start(venue_peer(&venue));
        let (credential, _dir) = credential_expiring(None);
        let tokens = credential.tokens();

        let err = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap_err();

        let err = format!("{err:#} {err:?}");
        assert!(
            err.contains("cTrader refused to renew the access token (CH_ACCESS_TOKEN_INVALID)"),
            "{err}"
        );
        assert!(
            err.contains(
                "Playground of the app on openapi.ctrader.com and set ACME_ACCESS_TOKEN and \
                 ACME_REFRESH_TOKEN in acme.env (see the ACME guide)"
            ),
            "{err}"
        );
        assert!(
            !err.contains(ACCESS_TOKEN) && !err.contains(REFRESH_TOKEN),
            "{err}"
        );
        assert_eq!(
            stored(&tokens),
            (ACCESS_TOKEN.into(), REFRESH_TOKEN.into(), None)
        );
    }

    /// `peer`, the renewals answered `delay` late: the renewing session
    /// holds the lock that long.
    fn slow_renewal(
        delay: Duration,
        mut peer: impl FnMut(usize, &ProtoMessage) -> Vec<Action> + Send + 'static,
    ) -> impl FnMut(usize, &ProtoMessage) -> Vec<Action> + Send + 'static {
        move |connection, request| {
            if request.payload_type == ProtoOaRefreshTokenReq::TYPE {
                std::thread::sleep(delay);
            }
            peer(connection, request)
        }
    }

    #[tokio::test]
    async fn sessions_starting_together_on_one_token_file_renew_it_once() {
        let venue = TokenVenue::new();
        let slow = Duration::from_millis(300);
        let (one_url, one) = testing::start(slow_renewal(slow, venue_peer(&venue)));
        let (other_url, other) = testing::start(slow_renewal(slow, venue_peer(&venue)));
        let (credential, _dir) = credential_expiring(None);
        let tokens = credential.tokens();

        let (one_client, other_client) = tokio::join!(
            connect(&one_url, credential.clone(), SLOW_HEARTBEAT),
            connect(&other_url, credential, SLOW_HEARTBEAT),
        );

        let (one_client, other_client) = (one_client.unwrap(), other_client.unwrap());
        assert_eq!(venue.lock().unwrap().renewals(), 1);
        assert_eq!(renewals(&one).len() + renewals(&other).len(), 1);
        assert_eq!(account_auths(&one, 0).last(), Some(&renewed_access(1)));
        assert_eq!(account_auths(&other, 0).last(), Some(&renewed_access(1)));
        assert_eq!(stored(&tokens).0, renewed_access(1));
        for client in [&one_client, &other_client] {
            assert_eq!(
                client
                    .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(1))
                    .await,
                Ok(symbols(&[1]))
            );
        }
        one_client.disconnect().await;
        other_client.disconnect().await;
    }

    #[tokio::test]
    async fn running_sessions_due_together_renew_once_and_both_go_on() {
        let venue = TokenVenue::new();
        let slow = Duration::from_millis(300);
        let (one_url, one) = testing::start(slow_renewal(slow, venue_peer(&venue)));
        let (other_url, other) = testing::start(slow_renewal(slow, venue_peer(&venue)));
        let (credential, _dir) = credential_expiring(due_after(Duration::from_millis(500)));
        let one_client = connect(&one_url, credential.clone(), SLOW_HEARTBEAT)
            .await
            .unwrap();
        let other_client = connect(&other_url, credential, SLOW_HEARTBEAT)
            .await
            .unwrap();

        testing::wait_until(&one, |_| {
            [&one, &other]
                .iter()
                .all(|r| account_auths(r, 0).last() == Some(&renewed_access(1)))
        })
        .await;

        assert_eq!(venue.lock().unwrap().renewals(), 1);
        for client in [&one_client, &other_client] {
            assert_eq!(
                client
                    .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(1))
                    .await,
                Ok(symbols(&[1]))
            );
        }
        one_client.disconnect().await;
        other_client.disconnect().await;
    }

    /// One session renews; the other, its token invalidated, takes the
    /// renewed pair from the file instead of spending a refresh token.
    #[tokio::test]
    async fn a_session_whose_token_another_renewed_takes_the_new_pair_from_the_file() {
        let venue = TokenVenue::new();
        let (one_url, one) = testing::start(venue_peer(&venue));
        let (other_url, other) = testing::start(venue_peer(&venue));
        let (credential, _dir) = testing::credential();
        let one_client = connect(&one_url, credential.clone(), SLOW_HEARTBEAT)
            .await
            .unwrap();
        let other_client = connect(&other_url, credential, SLOW_HEARTBEAT)
            .await
            .unwrap();

        one_client.renew_token();
        testing::wait_until(&one, |_| {
            account_auths(&one, 0).last() == Some(&renewed_access(1))
        })
        .await;
        let in_flight = other_client
            .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(1))
            .await;
        let next = other_client
            .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(2))
            .await;

        assert_eq!(in_flight, Ok(symbols(&[1])));
        assert_eq!(next, Ok(symbols(&[2])));
        assert_eq!(venue.lock().unwrap().renewals(), 1);
        assert!(renewals(&other).is_empty());
        assert_eq!(
            account_auths(&other, 0),
            [ACCESS_TOKEN.to_owned(), renewed_access(1)]
        );
        one_client.disconnect().await;
        other_client.disconnect().await;
    }

    /// Synthetic, as OpenApiMessages.proto defines it: the event of the
    /// account session cTrader dropped.
    fn account_dropped() -> Action {
        let event = ProtoOaAccountDisconnectEvent {
            ctid_trader_account_id: ACCOUNT_ID,
            ..Default::default()
        };
        Action::Send(crate::common::messages::frame(&event, None))
    }

    /// Synthetic: the renewal's answer as OpenApiMessages.proto defines it.
    fn renewal_answer() -> ProtoOaRefreshTokenRes {
        ProtoOaRefreshTokenRes {
            access_token: "renewedAccessValue".to_owned(),
            token_type: "bearer".to_owned(),
            expires_in: EXPIRES_IN,
            refresh_token: "renewedRefreshValue".to_owned(),
            ..Default::default()
        }
    }

    /// A cTrader taking any token and serving symbols, whose renewal
    /// answers are `renewal`'s.
    fn renewing_peer(
        mut renewal: impl FnMut(&ProtoMessage) -> Vec<Action> + Send + 'static,
    ) -> impl FnMut(usize, &ProtoMessage) -> Vec<Action> + Send + 'static {
        move |connection, request| {
            if request.payload_type == ProtoOaRefreshTokenReq::TYPE {
                return renewal(request);
            }
            symbols_peer(connection, request)
        }
    }

    /// Waits until the account authenticated with the renewed token.
    async fn reauthenticated(received: &Received) {
        testing::wait_until(received, |_| {
            account_auths(received, 0).last().map(String::as_str) == Some("renewedAccessValue")
        })
        .await;
    }

    #[tokio::test]
    async fn a_renewal_answered_after_ctrader_drops_the_account_session_is_saved_and_used() {
        let (url, received) = testing::start(renewing_peer(|request| {
            vec![account_dropped(), reply(request, &renewal_answer())]
        }));
        let (credential, _dir) = testing::credential();
        let tokens = credential.tokens();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();

        client.renew_token();
        reauthenticated(&received).await;

        assert_eq!(stored(&tokens).0, "renewedAccessValue");
        assert_eq!(
            client
                .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(1))
                .await,
            Ok(symbols(&[1]))
        );
        client.disconnect().await;
    }

    #[tokio::test]
    async fn a_renewal_answer_without_its_client_msg_id_is_saved_and_used() {
        let (url, received) = testing::start(renewing_peer(|_| {
            vec![Action::Send(crate::common::messages::frame(
                &renewal_answer(),
                None,
            ))]
        }));
        let (credential, _dir) = testing::credential();
        let tokens = credential.tokens();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();

        client.renew_token();
        reauthenticated(&received).await;

        assert_eq!(stored(&tokens).0, "renewedAccessValue");
        client.disconnect().await;
    }

    #[tokio::test]
    async fn a_disconnect_waits_for_the_renewal_in_flight_and_saves_its_pair() {
        let (url, received) = testing::start(renewing_peer(|request| {
            std::thread::sleep(Duration::from_millis(300));
            vec![reply(request, &renewal_answer())]
        }));
        let (credential, _dir) = testing::credential();
        let tokens = credential.tokens();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();
        client.renew_token();
        testing::wait_until(&received, |frames| {
            frames
                .iter()
                .any(|(_, f)| f.payload_type == ProtoOaRefreshTokenReq::TYPE)
        })
        .await;

        client.disconnect().await;

        assert_eq!(stored(&tokens).0, "renewedAccessValue");
    }

    #[tokio::test]
    async fn an_account_session_ctrader_drops_is_authenticated_again() {
        let dropped = Arc::new(Mutex::new(false));
        let (url, received) = testing::start(move |connection, request| {
            let mut actions = symbols_peer(connection, request);
            let mut dropped = dropped.lock().unwrap();
            if request.payload_type == ProtoOaSymbolByIdReq::TYPE && !*dropped {
                *dropped = true;
                actions.insert(0, account_dropped());
            }
            actions
        });
        let (credential, _dir) = testing::credential();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();

        let in_flight = client
            .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(1))
            .await;
        testing::wait_until(&received, |_| account_auths(&received, 0).len() == 2).await;
        let next = client
            .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(2))
            .await;

        assert_eq!(in_flight, Ok(symbols(&[1])));
        assert_eq!(next, Ok(symbols(&[2])));
        assert_eq!(account_auths(&received, 0), [ACCESS_TOKEN, ACCESS_TOKEN]);
        client.disconnect().await;
    }

    /// The renewal's answer comes late, so the (un)subscriptions arrive
    /// while the account is not authenticated.
    #[tokio::test]
    async fn spots_changed_during_a_renewal_are_sent_once_authenticated_again() {
        let venue = TokenVenue::new();
        let (url, received) =
            testing::start(slow_renewal(Duration::from_millis(300), venue_peer(&venue)));
        let (credential, _dir) = testing::credential();
        let (client, mut spots) =
            CTraderWebSocketClient::connect(&url, credential, SLOW_HEARTBEAT, None)
                .await
                .unwrap();
        client.subscribe_spots(41).unwrap();
        next_spot(&mut spots).await;

        client.renew_token();
        client.subscribe_spots(1).unwrap();
        client.unsubscribe_spots(41).unwrap();

        assert_eq!(next_spot(&mut spots).await, spot_event(1));
        // The unsubscription follows the subscription whose spot just came.
        testing::wait_until(&received, |_| {
            !spot_unsubscriptions(&received, 0).is_empty()
        })
        .await;
        let subscribed: Vec<Vec<i64>> = spot_subscriptions(&received, 0)
            .into_iter()
            .map(|r| r.symbol_id)
            .collect();
        assert_eq!(subscribed, [vec![41], vec![1]]);
        assert_eq!(spot_unsubscriptions(&received, 0), [vec![41]]);
        assert_eq!(
            account_auths(&received, 0),
            [ACCESS_TOKEN.to_owned(), renewed_access(1)]
        );
        client.disconnect().await;
    }
}
