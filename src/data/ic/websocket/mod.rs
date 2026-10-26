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

/// How long `disconnect` waits for the handler to close the transport.
const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(10);

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
    /// account of `credential`, sending the heartbeat every `heartbeat`;
    /// with the spot events of the symbols `subscribe_spots` subscribes.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails, or cTrader refuses an
    /// authentication (naming its error code and what to check, never a
    /// credential) or does not answer within `AUTH_TIMEOUT`.
    pub(super) async fn connect(
        url: &str,
        credential: IcCredential,
        heartbeat: Duration,
    ) -> anyhow::Result<(Self, SpotEvents)> {
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
        let handler =
            IcWsHandler::new(credential, auth.clone(), cmd_rx, raw_rx, spot_tx, heartbeat);
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

    /// Closes the connection; requests still waiting fail.
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

    use prost::Message as _;

    use super::*;
    use crate::data::ic::{
        messages::{
            ProtoHeartbeatEvent, ProtoMessage, ProtoOaAccountAuthReq, ProtoOaAccountAuthRes,
            ProtoOaAccountsTokenInvalidatedEvent, ProtoOaApplicationAuthReq, ProtoOaErrorRes,
            ProtoOaGetTickDataReq, ProtoOaGetTickDataRes, ProtoOaQuoteType, ProtoOaSpotEvent,
            ProtoOaSubscribeSpotsReq, ProtoOaSubscribeSpotsRes, ProtoOaSymbol,
            ProtoOaSymbolByIdReq, ProtoOaSymbolByIdRes, ProtoOaUnsubscribeSpotsReq,
            ProtoOaUnsubscribeSpotsRes,
        },
        testing::{
            self, ACCOUNT_AUTH_TOKEN_INVALID, ACCOUNT_ID, APPLICATION_AUTH_REFUSED, Action,
            Received, SYMBOL_BY_ID_UNKNOWN, accept_auth, credential, replay, reply,
        },
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

        let client = connect(&url, credential(), SLOW_HEARTBEAT).await.unwrap();

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
        let client = connect(&url, credential(), Duration::from_millis(50))
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
        let client = connect(&url, credential(), SLOW_HEARTBEAT).await.unwrap();

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
        let client = connect(&url, credential(), SLOW_HEARTBEAT).await.unwrap();

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

    /// The token error is the fixture `account_auth_token_invalid`.
    #[tokio::test]
    async fn a_rejected_token_fails_the_connection_naming_the_code_never_the_token() {
        let (url, _) = testing::start(|_, request| {
            if request.payload_type == ProtoOaAccountAuthReq::TYPE {
                return vec![replay::<ProtoOaErrorRes>(
                    request,
                    ACCOUNT_AUTH_TOKEN_INVALID,
                )];
            }
            accept_auth(request).unwrap_or_default()
        });

        let err = connect(&url, credential(), SLOW_HEARTBEAT)
            .await
            .unwrap_err();

        let err = format!("{err:#} {err:?}");
        assert!(
            err.contains(
                "cTrader refused the account authentication (CH_ACCESS_TOKEN_INVALID): the \
                 access token is invalid or expired: renew IC_ACCESS_TOKEN"
            ),
            "{err}"
        );
        for secret in [
            "accessTokenValue",
            "clientSecretValue",
            "12345_clientIdValue",
            "1000001",
        ] {
            assert!(!err.contains(secret), "{err}");
        }
    }

    /// The refusal is the fixture `application_auth_refused`.
    #[tokio::test]
    async fn a_refused_application_fails_before_any_account_auth() {
        let (url, received) = testing::start(|_, request| {
            vec![replay::<ProtoOaErrorRes>(request, APPLICATION_AUTH_REFUSED)]
        });

        let err = connect(&url, credential(), SLOW_HEARTBEAT)
            .await
            .unwrap_err();

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

        let err = connect(&url, credential(), SLOW_HEARTBEAT)
            .await
            .unwrap_err();

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
        let client = connect(&url, credential(), SLOW_HEARTBEAT).await.unwrap();

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
        let client = connect(&url, credential(), SLOW_HEARTBEAT).await.unwrap();
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
        let client = connect(&url, credential(), SLOW_HEARTBEAT).await.unwrap();
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
        let client = connect(&url, credential(), SLOW_HEARTBEAT).await.unwrap();
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
                let event = ProtoOaAccountsTokenInvalidatedEvent {
                    ctid_trader_account_ids: vec![ACCOUNT_ID],
                    reason: Some("Access Token is expired or recalled".to_owned()),
                    ..Default::default()
                };
                vec![Action::Send(crate::data::ic::messages::frame(&event, None))]
            })
        });
        let client = connect(&url, credential(), SLOW_HEARTBEAT).await.unwrap();

        let in_flight = client
            .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(1))
            .await;
        let next = client
            .request::<_, ProtoOaSymbolByIdRes>(&symbol_request(2))
            .await;

        let ended = IcError::Session(
            "cTrader ended the session (PROTO_OA_ACCOUNTS_TOKEN_INVALIDATED_EVENT)".to_owned(),
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
        let (client, mut spots) = IcWebSocketClient::connect(&url, credential(), SLOW_HEARTBEAT)
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
        let (client, mut spots) = IcWebSocketClient::connect(&url, credential(), SLOW_HEARTBEAT)
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
        let (client, mut spots) = IcWebSocketClient::connect(&url, credential(), SLOW_HEARTBEAT)
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
        let (client, mut spots) = IcWebSocketClient::connect(&url, credential(), SLOW_HEARTBEAT)
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
}
