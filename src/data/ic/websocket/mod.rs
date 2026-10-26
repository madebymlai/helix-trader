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
//! The transport's own heartbeat is off: cTrader counts its
//! `ProtoHeartbeatEvent`, which the transport cannot send (it sends Ping
//! frames or text).

mod handler;

use std::{sync::Arc, time::Duration};

use anyhow::Context;
use nautilus_network::{
    Message,
    websocket::{AuthTracker, MessageHandler, WebSocketClient, WebSocketConfig},
};
use tokio::sync::{mpsc, oneshot};

use self::handler::{HandlerCommand, IcWsHandler};
use super::{
    config::IcCredential,
    messages::{IcError, Payload, open},
};

/// cTrader's heartbeat interval.
pub(super) const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

/// How long the application and account authentication may take.
const AUTH_TIMEOUT: Duration = Duration::from_secs(30);

/// How long a request waits for its response.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);

/// How long `disconnect` waits for the handler to close the transport.
const DISCONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// An authenticated cTrader session on one account.
#[derive(Debug)]
pub(super) struct IcWebSocketClient {
    cmd_tx: mpsc::UnboundedSender<HandlerCommand>,
    handler: tokio::task::JoinHandle<()>,
}

impl IcWebSocketClient {
    /// Connects to `url`, then authenticates the application and the
    /// account of `credential`, sending the heartbeat every `heartbeat`.
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
    ) -> anyhow::Result<Self> {
        let (raw_tx, raw_rx) = mpsc::unbounded_channel::<Message>();
        let message_handler: MessageHandler = Arc::new(move |message| {
            // The receiver is gone only once the handler has stopped.
            let _ = raw_tx.send(message);
        });
        let config = WebSocketConfig::builder()
            .url(url.to_owned())
            .build()
            .context("the cTrader WebSocket config")?;
        let client = WebSocketClient::builder()
            .config(config)
            .message_handler(message_handler)
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
        let handler = IcWsHandler::new(credential, auth.clone(), cmd_rx, raw_rx, heartbeat);
        let connected = Self {
            cmd_tx,
            handler: tokio::spawn(handler.run()),
        };
        match auth
            .wait_for_result::<String>(AUTH_TIMEOUT, authenticated)
            .await
        {
            Ok(()) => Ok(connected),
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
            ProtoOaSymbol, ProtoOaSymbolByIdReq, ProtoOaSymbolByIdRes,
        },
        testing::{
            self, ACCOUNT_AUTH_TOKEN_INVALID, ACCOUNT_ID, APPLICATION_AUTH_REFUSED, Action,
            SYMBOL_BY_ID_UNKNOWN, accept_auth, credential, replay, reply,
        },
    };

    const SLOW_HEARTBEAT: Duration = Duration::from_secs(3600);

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

        let client = IcWebSocketClient::connect(&url, credential(), SLOW_HEARTBEAT)
            .await
            .unwrap();

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
        let client = IcWebSocketClient::connect(&url, credential(), Duration::from_millis(50))
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
        let client = IcWebSocketClient::connect(&url, credential(), SLOW_HEARTBEAT)
            .await
            .unwrap();

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
        let client = IcWebSocketClient::connect(&url, credential(), SLOW_HEARTBEAT)
            .await
            .unwrap();

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

        let err = IcWebSocketClient::connect(&url, credential(), SLOW_HEARTBEAT)
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

        let err = IcWebSocketClient::connect(&url, credential(), SLOW_HEARTBEAT)
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

        let err = IcWebSocketClient::connect(&url, credential(), SLOW_HEARTBEAT)
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
        let client = IcWebSocketClient::connect(&url, credential(), SLOW_HEARTBEAT)
            .await
            .unwrap();

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
        let client = IcWebSocketClient::connect(&url, credential(), SLOW_HEARTBEAT)
            .await
            .unwrap();
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
        let client = IcWebSocketClient::connect(&url, credential(), SLOW_HEARTBEAT)
            .await
            .unwrap();

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
}
