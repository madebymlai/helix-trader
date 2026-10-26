//! The cTrader Open API connection: an outer client and the handler task
//! that owns Nautilus' `WebSocketClient` (see `handler`).
//!
//! The outer client starts the handler with `SetClient`, waits for the
//! session to authenticate (application, then account, tracked by
//! Nautilus' `AuthTracker`), then sends requests as commands and awaits
//! each response. The handler sends the binary heartbeat, correlates
//! responses by `clientMsgId` and, after the transport reconnects,
//! authenticates the new session before serving waiting requests.
//! Requests in flight at a reconnect fail: their session is gone.
//!
//! Spot subscriptions are the session's too: the handler keeps the symbols
//! subscribed, sends their subscription once the session is ready, and
//! again after each reconnect, and passes their spot events on.
//!
//! Historical requests go through the transport's rate limiter, keyed
//! `HISTORICAL_KEY`: one every 250 ms, without burst, so fewer than
//! cTrader's 5 in any second, even after an idle spell (a burst of 5 then
//! the refill could send 9 within one second). One every 200 ms got
//! `BLOCKED_PAYLOAD_TYPE` ("You are being rate limited") on the demo.
//!
//! The transport's own heartbeat is off: cTrader counts its
//! `ProtoHeartbeatEvent`, which the transport cannot send (it sends Ping
//! frames or text).

mod handler;

use std::{sync::Arc, time::Duration};

use anyhow::Context;
use nautilus_network::{
    Message,
    ratelimiter::quota::Quota,
    websocket::{AuthTracker, MessageHandler, WebSocketClient, WebSocketConfig},
};
use tokio::sync::{mpsc, oneshot};

use self::handler::{HandlerCommand, IcWsHandler};
use super::{
    config::IcCredential,
    messages::{IcError, Payload, ProtoOaSpotEvent, open},
};

/// cTrader's heartbeat interval.
pub(super) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

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

/// The spot events of the symbols subscribed, as cTrader sends them.
pub(super) type SpotEvents = mpsc::UnboundedReceiver<ProtoOaSpotEvent>;

/// An authenticated cTrader session on one account.
#[derive(Debug)]
pub(super) struct IcWebSocketClient {
    cmd_tx: mpsc::UnboundedSender<HandlerCommand>,
    handler: tokio::task::JoinHandle<()>,
}

impl IcWebSocketClient {
    /// Connects to `url`, then authenticates the application and the
    /// account of `credential` with the token file's pair (seeded from
    /// `.env` the first time, renewed first if due), sending the heartbeat
    /// every `heartbeat`; with the spot events of the symbols
    /// `subscribe_spots` subscribes.
    ///
    /// # Errors
    ///
    /// Returns an error if the token file cannot be read or written, the
    /// connection fails, or cTrader refuses an authentication or the
    /// renewal (naming its error code and what to do, never a credential)
    /// or does not answer within `AUTH_TIMEOUT`.
    pub(super) async fn connect(
        url: &str,
        credential: IcCredential,
        heartbeat: Duration,
    ) -> anyhow::Result<(Self, SpotEvents)> {
        let store = credential.tokens.clone();
        let tokens = tokio::task::spawn_blocking(move || store.load())
            .await
            .context("loading IC's token file")??;
        let (raw_tx, raw_rx) = mpsc::unbounded_channel::<Message>();
        let message_handler: MessageHandler = Arc::new(move |message| {
            // The receiver is gone only once the handler has stopped.
            let _ = raw_tx.send(message);
        });
        let config = WebSocketConfig::builder()
            .url(url.to_owned())
            .build()
            .context("the cTrader WebSocket config")?;
        let historical = Quota::with_period(HISTORICAL_PERIOD)
            .context("the historical request period is zero")?;
        let client = WebSocketClient::builder()
            .config(config)
            .message_handler(message_handler)
            .keyed_quotas(vec![(HISTORICAL_KEY.to_owned(), historical)])
            .connect()
            .await
            .with_context(|| format!("connecting to cTrader at {url}"))?;

        let auth = AuthTracker::new();
        let authenticated = auth.begin();
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
        cmd_tx
            .send(HandlerCommand::SetClient(client))
            .ok()
            .context("the cTrader handler is gone")?;
        let (spot_tx, spot_rx) = mpsc::unbounded_channel();
        let handler = IcWsHandler::new(
            credential,
            tokens,
            auth.clone(),
            cmd_rx,
            raw_rx,
            spot_tx,
            heartbeat,
        );
        let connected = Self {
            cmd_tx,
            handler: tokio::spawn(handler.run()),
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

    /// Sends `request` and awaits its response.
    ///
    /// # Errors
    ///
    /// Returns cTrader's error response, or an error if the session cannot
    /// serve requests, the connection is lost first, or no response comes
    /// within `REQUEST_TIMEOUT`.
    pub(super) async fn request<Req: Payload, Res: Payload>(
        &self,
        request: &Req,
    ) -> Result<Res, IcError> {
        let (reply, response) = oneshot::channel();
        self.cmd_tx
            .send(HandlerCommand::Request {
                payload_type: Req::TYPE,
                payload: request.encode_to_vec(),
                reply,
            })
            .map_err(|_| IcError::ConnectionLost)?;
        let envelope = tokio::time::timeout(REQUEST_TIMEOUT, response)
            .await
            .map_err(|_| IcError::Timeout)?
            .map_err(|_| IcError::ConnectionLost)??;
        open(&envelope)
    }

    /// Subscribes to the spot events of `symbol_id`, with their
    /// timestamps, for the rest of the session, reconnects included. A
    /// symbol already subscribed is left as it is. cTrader's refusal is
    /// logged: its answer comes after this returns.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection is closed.
    pub(super) fn subscribe_spots(&self, symbol_id: i64) -> Result<(), IcError> {
        self.cmd_tx
            .send(HandlerCommand::SubscribeSpots(symbol_id))
            .map_err(|_| IcError::ConnectionLost)
    }

    /// Ends the spot subscription of `symbol_id`, if any. cTrader may
    /// still send a few of its spot events.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection is closed.
    pub(super) fn unsubscribe_spots(&self, symbol_id: i64) -> Result<(), IcError> {
        self.cmd_tx
            .send(HandlerCommand::UnsubscribeSpots(symbol_id))
            .map_err(|_| IcError::ConnectionLost)
    }

    /// Renews the access token now, as on schedule, if the session is
    /// ready.
    #[cfg(test)]
    pub(super) fn renew_token(&self) {
        let _ = self.cmd_tx.send(HandlerCommand::RenewToken);
    }

    /// Closes the connection, once a token renewal out is answered (and
    /// its pair saved) or `AUTH_TIMEOUT` passed; requests still waiting
    /// fail.
    pub(super) async fn disconnect(self) {
        let _ = self.cmd_tx.send(HandlerCommand::Disconnect);
        let mut handler = self.handler;
        if tokio::time::timeout(DISCONNECT_TIMEOUT, &mut handler)
            .await
            .is_err()
        {
            log::warn!("the cTrader handler did not stop in time: aborted");
            handler.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use jiff::Timestamp;
    use prost::Message as _;

    use super::*;
    use crate::data::ic::{
        messages::{
            ProtoHeartbeatEvent, ProtoMessage, ProtoOaAccountAuthReq, ProtoOaAccountAuthRes,
            ProtoOaAccountDisconnectEvent, ProtoOaApplicationAuthReq, ProtoOaClientDisconnectEvent,
            ProtoOaErrorRes, ProtoOaGetTickDataReq, ProtoOaGetTickDataRes, ProtoOaQuoteType,
            ProtoOaRefreshTokenReq, ProtoOaRefreshTokenRes, ProtoOaSpotEvent,
            ProtoOaSubscribeSpotsReq, ProtoOaSubscribeSpotsRes, ProtoOaSymbol,
            ProtoOaSymbolByIdReq, ProtoOaSymbolByIdRes, ProtoOaUnsubscribeSpotsReq,
            ProtoOaUnsubscribeSpotsRes,
        },
        testing::{
            self, ACCESS_TOKEN, ACCOUNT_AUTH_TOKEN_INVALID, ACCOUNT_ID, APPLICATION_AUTH_REFUSED,
            Action, EXPIRES_IN, REFRESH_TOKEN, Received, SYMBOL_BY_ID_UNKNOWN, SharedVenue,
            TokenVenue, accept_auth, credential_expiring, renewed_access, replay, reply,
            token_peer,
        },
        token::{RENEW_BEFORE, TokenStore},
    };

    const SLOW_HEARTBEAT: Duration = Duration::from_secs(3600);

    /// A session whose spot events nobody reads.
    async fn connect(
        url: &str,
        credential: IcCredential,
        heartbeat: Duration,
    ) -> anyhow::Result<IcWebSocketClient> {
        let (client, _) = IcWebSocketClient::connect(url, credential, heartbeat).await?;
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
        assert!(err.contains("Playground"), "{err}");
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
                "the application authentication (CH_CLIENT_AUTH_FAILURE): check IC_CLIENT_ID \
                 and IC_CLIENT_SECRET"
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
            format!("{err:#}").contains("cTrader authenticated another account than IC_ACCOUNT_ID"),
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

    /// cTrader allows 5 historical requests a second per connection: in
    /// any second, at most 5 tick requests reach it, however many wait.
    #[tokio::test]
    async fn tick_requests_reach_ctrader_at_most_five_a_second() {
        let arrivals = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&arrivals);
        let (url, _) = testing::start(move |_, request| {
            if let Some(actions) = accept_auth(request) {
                return actions;
            }
            let at = std::time::Instant::now();
            if request.payload_type == ProtoOaGetTickDataReq::TYPE {
                log.lock().unwrap().push(at);
                return vec![reply(request, &ProtoOaGetTickDataRes::default())];
            }
            symbols_peer(0, request)
        });
        let (credential, _tokens) = testing::credential();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();
        let request = tick_request();

        let answers = futures::future::join_all(
            (0..7).map(|_| client.request::<_, ProtoOaGetTickDataRes>(&request)),
        )
        .await;

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

    #[tokio::test]
    async fn other_requests_are_not_held_to_the_historical_limit() {
        let (url, _) = testing::start(symbols_peer);
        let (credential, _tokens) = testing::credential();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();
        let start = std::time::Instant::now();

        for id in 0..7 {
            client
                .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(id))
                .await
                .unwrap();
        }

        assert!(
            start.elapsed() < Duration::from_millis(900),
            "{:?}",
            start.elapsed()
        );
        client.disconnect().await;
    }

    /// Synthetic: the event as OpenApiMessages.proto defines it.
    #[tokio::test]
    async fn requests_fail_once_ctrader_ends_the_session() {
        let (url, _) = testing::start(|_, request| {
            accept_auth(request).unwrap_or_else(|| {
                let event = ProtoOaClientDisconnectEvent {
                    reason: Some("Server maintenance".to_owned()),
                    ..Default::default()
                };
                vec![Action::Send(crate::data::ic::messages::frame(&event, None))]
            })
        });
        let (credential, _tokens) = testing::credential();
        let client = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap();

        let in_flight = client
            .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(1))
            .await;
        let next = client
            .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(2))
            .await;

        let ended = IcError::Session(
            "cTrader ended the session (PROTO_OA_CLIENT_DISCONNECT_EVENT)".to_owned(),
        );
        assert_eq!(in_flight, Err(ended.clone()));
        assert_eq!(next, Err(ended));
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
                    Action::Send(crate::data::ic::messages::frame(&spot_event(id), None))
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
        let (client, mut spots) = IcWebSocketClient::connect(&url, credential, SLOW_HEARTBEAT)
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
        let (client, mut spots) = IcWebSocketClient::connect(&url, credential, SLOW_HEARTBEAT)
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
        let (client, mut spots) = IcWebSocketClient::connect(&url, credential, SLOW_HEARTBEAT)
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
        let (client, mut spots) = IcWebSocketClient::connect(&url, credential, SLOW_HEARTBEAT)
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
        let tokens = credential.tokens.clone();
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
    async fn a_running_session_renews_on_schedule_keeping_spots_and_requests_in_flight() {
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
                actions.push(Action::Send(crate::data::ic::messages::frame(
                    &spot_event(41),
                    None,
                )));
            }
            actions
        });
        let (credential, _dir) = credential_expiring(due_after(Duration::from_secs(1)));
        let tokens = credential.tokens.clone();
        let (client, mut spots) = IcWebSocketClient::connect(&url, credential, SLOW_HEARTBEAT)
            .await
            .unwrap();
        client.subscribe_spots(41).unwrap();
        next_spot(&mut spots).await;

        let in_flight = client
            .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(3))
            .await;

        assert_eq!(in_flight, Ok(symbols(&[3])));
        assert_eq!(next_spot(&mut spots).await, spot_event(41));
        assert_eq!(renewals(&received), [REFRESH_TOKEN]);
        assert_eq!(
            account_auths(&received, 0),
            [ACCESS_TOKEN.to_owned(), renewed_access(1)]
        );
        assert_eq!(
            spot_subscriptions(&received, 0).len(),
            1,
            "subscribed again"
        );
        assert_eq!(stored(&tokens).0, renewed_access(1));
        assert_eq!(
            client
                .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(4))
                .await,
            Ok(symbols(&[4]))
        );
        client.disconnect().await;
    }

    /// The event comes with the next request, whose answer follows it.
    #[tokio::test]
    async fn an_invalidated_token_is_renewed_and_the_account_authenticated_again() {
        let venue = TokenVenue::new();
        let (url, received) = testing::start(venue_peer(&venue));
        let (credential, _dir) = testing::credential();
        let tokens = credential.tokens.clone();
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
        let tokens = credential.tokens.clone();

        let err = connect(&url, credential, SLOW_HEARTBEAT).await.unwrap_err();

        let err = format!("{err:#} {err:?}");
        assert!(
            err.contains("cTrader refused to renew the access token (CH_ACCESS_TOKEN_INVALID)"),
            "{err}"
        );
        assert!(err.contains("Playground"), "{err}");
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
        let tokens = credential.tokens.clone();

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
        Action::Send(crate::data::ic::messages::frame(&event, None))
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
        let tokens = credential.tokens.clone();
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
            vec![Action::Send(crate::data::ic::messages::frame(
                &renewal_answer(),
                None,
            ))]
        }));
        let (credential, _dir) = testing::credential();
        let tokens = credential.tokens.clone();
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
        let tokens = credential.tokens.clone();
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
        let (client, mut spots) = IcWebSocketClient::connect(&url, credential, SLOW_HEARTBEAT)
            .await
            .unwrap();
        client.subscribe_spots(41).unwrap();
        next_spot(&mut spots).await;

        client.renew_token();
        client.subscribe_spots(1).unwrap();
        client.unsubscribe_spots(41).unwrap();

        assert_eq!(next_spot(&mut spots).await, spot_event(1));
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
