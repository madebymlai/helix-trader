//! The handler task: the one owner of the `WebSocketClient`. It
//! authenticates each connection, sends the heartbeat, tags requests with
//! a `clientMsgId` and routes each response back to its caller. It keeps
//! the spot subscriptions, sends them again on each new session, and
//! passes spot events on.

use std::{
    collections::{BTreeSet, HashMap},
    time::Duration,
};

use nautilus_network::{
    Message, RECONNECTED,
    websocket::{AuthTracker, WebSocketClient},
};
use prost::Message as _;
use tokio::sync::{mpsc, oneshot};
use ustr::Ustr;
use zeroize::Zeroize;

use super::super::{
    config::IcCredential,
    messages::{
        IcError, Payload, ProtoHeartbeatEvent, ProtoMessage, ProtoOaAccountAuthReq,
        ProtoOaAccountAuthRes, ProtoOaApplicationAuthReq, ProtoOaApplicationAuthRes,
        ProtoOaPayloadType, ProtoOaSpotEvent, ProtoOaSubscribeSpotsReq, ProtoOaSubscribeSpotsRes,
        ProtoOaUnsubscribeSpotsReq, ProtoOaUnsubscribeSpotsRes, frame, is_historical, open,
        payload_name, secret_frame,
    },
};
use super::HISTORICAL_KEY;

/// Where a response goes: the caller awaiting it.
pub(super) type Reply = oneshot::Sender<Result<ProtoMessage, IcError>>;

/// Commands from the outer client to the handler.
pub(super) enum HandlerCommand {
    /// The connected client; the first command, queued before the command
    /// sender is shared, so no request can overtake it.
    SetClient(WebSocketClient),
    /// A request: its payload type and encoded message.
    Request {
        payload_type: u32,
        payload: Vec<u8>,
        reply: Reply,
    },
    /// Spot events of a symbol, from now on.
    SubscribeSpots(i64),
    /// No more spot events of a symbol.
    UnsubscribeSpots(i64),
    Disconnect,
}

/// Where the session stands. Requests wait while it authenticates.
enum Session {
    /// No client yet.
    Unconnected,
    /// The application auth request with this `clientMsgId` is out.
    ApplicationAuth(String),
    /// The account auth request with this `clientMsgId` is out.
    AccountAuth(String),
    Ready,
    /// Authentication failed, or cTrader ended the session.
    Failed(String),
}

/// Who awaits a response.
enum Pending {
    /// A caller of the outer client.
    Caller(Reply),
    /// The handler's own spot (un)subscription of these symbols, whose
    /// outcome is logged.
    Spots {
        subscribe: bool,
        symbol_ids: Vec<i64>,
    },
}

/// A request waiting for the session to be ready.
struct Queued {
    payload_type: u32,
    payload: Vec<u8>,
    reply: Reply,
}

pub(super) struct IcWsHandler {
    client: Option<WebSocketClient>,
    credential: IcCredential,
    auth: AuthTracker,
    cmd_rx: mpsc::UnboundedReceiver<HandlerCommand>,
    raw_rx: mpsc::UnboundedReceiver<Message>,
    spot_tx: mpsc::UnboundedSender<ProtoOaSpotEvent>,
    heartbeat: Duration,
    session: Session,
    next_id: u64,
    pending: HashMap<String, Pending>,
    queued: Vec<Queued>,
    /// The symbols whose spot events the session subscribes to.
    spots: BTreeSet<i64>,
}

impl IcWsHandler {
    pub(super) fn new(
        credential: IcCredential,
        auth: AuthTracker,
        cmd_rx: mpsc::UnboundedReceiver<HandlerCommand>,
        raw_rx: mpsc::UnboundedReceiver<Message>,
        spot_tx: mpsc::UnboundedSender<ProtoOaSpotEvent>,
        heartbeat: Duration,
    ) -> Self {
        Self {
            client: None,
            credential,
            auth,
            cmd_rx,
            raw_rx,
            spot_tx,
            heartbeat,
            session: Session::Unconnected,
            next_id: 0,
            pending: HashMap::new(),
            queued: Vec::new(),
            spots: BTreeSet::new(),
        }
    }

    /// Runs until disconnected, the outer client dropped, or the
    /// transport closed for good.
    pub(super) async fn run(mut self) {
        let start = tokio::time::Instant::now() + self.heartbeat;
        let mut heartbeat = tokio::time::interval_at(start, self.heartbeat);
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                command = self.cmd_rx.recv() => match command {
                    Some(HandlerCommand::SetClient(client)) => {
                        self.client = Some(client);
                        self.authenticate().await;
                    }
                    Some(HandlerCommand::Request { payload_type, payload, reply }) => {
                        self.request(Queued { payload_type, payload, reply }).await;
                    }
                    Some(HandlerCommand::SubscribeSpots(symbol_id)) => {
                        if self.spots.insert(symbol_id) && matches!(self.session, Session::Ready) {
                            self.send_spots(true, vec![symbol_id]).await;
                        }
                    }
                    Some(HandlerCommand::UnsubscribeSpots(symbol_id)) => {
                        if self.spots.remove(&symbol_id) && matches!(self.session, Session::Ready) {
                            self.send_spots(false, vec![symbol_id]).await;
                        }
                    }
                    Some(HandlerCommand::Disconnect) | None => break,
                },
                message = self.raw_rx.recv() => match message {
                    Some(message) => self.receive(message).await,
                    None => {
                        log::debug!("cTrader transport closed");
                        break;
                    }
                },
                _ = heartbeat.tick() => self.send_heartbeat().await,
            }
        }
        self.close().await;
    }

    async fn close(&mut self) {
        if let Some(client) = self.client.take() {
            client.disconnect().await;
        }
        self.auth.fail("the cTrader connection is closed");
        self.fail_pending(&IcError::ConnectionLost);
        for queued in self.queued.drain(..) {
            let _ = queued.reply.send(Err(IcError::ConnectionLost));
        }
    }

    fn next_id(&mut self) -> String {
        self.next_id += 1;
        self.next_id.to_string()
    }

    /// Sends one frame, first waiting for the rate limit of `keys`. No
    /// frame content is logged: auth frames hold the secret and the token.
    /// The transport takes the frame; its copies are out of the adapter's
    /// reach.
    async fn send(&self, frame: Vec<u8>, keys: Option<&[Ustr]>) -> Result<(), IcError> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| IcError::Transport("no connection".to_owned()))?;
        client
            .send_bytes(frame, keys)
            .await
            .map_err(|e| IcError::Transport(e.to_string()))
    }

    /// cTrader's keepalive: a `ProtoHeartbeatEvent` frame, every 10 s
    /// (help.ctrader.com/open-api, "Establish a connection").
    async fn send_heartbeat(&self) {
        if self.client.is_none() {
            return;
        }
        if let Err(e) = self
            .send(frame(&ProtoHeartbeatEvent::default(), None), None)
            .await
        {
            log::warn!("cTrader heartbeat not sent: {e}");
        }
    }

    /// Starts the session: application auth, then account auth on its
    /// response (see `receive`).
    async fn authenticate(&mut self) {
        let id = self.next_id();
        let mut request = ProtoOaApplicationAuthReq {
            client_id: self.credential.client_id.expose_secret().to_owned(),
            client_secret: self.credential.client_secret.expose_secret().to_owned(),
            ..Default::default()
        };
        let frame = secret_frame(&request, Some(id.clone()));
        request.client_id.zeroize();
        request.client_secret.zeroize();
        match self.send(frame, None).await {
            Ok(()) => self.session = Session::ApplicationAuth(id),
            Err(e) => self.fail_session(format!("sending the application auth: {e}")),
        }
    }

    async fn authenticate_account(&mut self) {
        let id = self.next_id();
        let mut request = ProtoOaAccountAuthReq {
            ctid_trader_account_id: self.credential.account_id.get(),
            access_token: self.credential.access_token.expose_secret().to_owned(),
            ..Default::default()
        };
        let frame = secret_frame(&request, Some(id.clone()));
        request.access_token.zeroize();
        match self.send(frame, None).await {
            Ok(()) => self.session = Session::AccountAuth(id),
            Err(e) => self.fail_session(format!("sending the account auth: {e}")),
        }
    }

    async fn ready(&mut self) {
        log::info!("cTrader session authenticated");
        self.session = Session::Ready;
        self.auth.succeed();
        // A new session holds no subscription: send them all again.
        if !self.spots.is_empty() {
            let symbol_ids = self.spots.iter().copied().collect();
            self.send_spots(true, symbol_ids).await;
        }
        for queued in std::mem::take(&mut self.queued) {
            self.dispatch(queued).await;
        }
    }

    /// Ends the session: the requests in flight and waiting fail, and so
    /// will the next, until a reconnect authenticates again.
    fn fail_session(&mut self, reason: String) {
        log::error!("cTrader session failed: {reason}");
        self.auth.fail(reason.clone());
        self.fail_pending(&IcError::Session(reason.clone()));
        for queued in self.queued.drain(..) {
            let _ = queued.reply.send(Err(IcError::Session(reason.clone())));
        }
        self.session = Session::Failed(reason);
    }

    /// Fails the callers awaiting a response. The handler's own spot
    /// requests are dropped: the next session sends them again.
    fn fail_pending(&mut self, error: &IcError) {
        for (_, pending) in self.pending.drain() {
            if let Pending::Caller(reply) = pending {
                let _ = reply.send(Err(error.clone()));
            }
        }
    }

    /// Subscribes to the spot events of `symbol_ids`, with their
    /// timestamps, or unsubscribes from them; the outcome is logged.
    async fn send_spots(&mut self, subscribe: bool, symbol_ids: Vec<i64>) {
        let id = self.next_id();
        let account = self.credential.account_id.get();
        let frame = if subscribe {
            let request = ProtoOaSubscribeSpotsReq {
                ctid_trader_account_id: account,
                symbol_id: symbol_ids.clone(),
                subscribe_to_spot_timestamp: Some(true),
                ..Default::default()
            };
            frame(&request, Some(id.clone()))
        } else {
            let request = ProtoOaUnsubscribeSpotsReq {
                ctid_trader_account_id: account,
                symbol_id: symbol_ids.clone(),
                ..Default::default()
            };
            frame(&request, Some(id.clone()))
        };
        match self.send(frame, None).await {
            Ok(()) => {
                self.pending.insert(
                    id,
                    Pending::Spots {
                        subscribe,
                        symbol_ids,
                    },
                );
            }
            Err(e) => log::error!("cTrader spot subscription of {symbol_ids:?} not sent: {e}"),
        }
    }

    async fn request(&mut self, request: Queued) {
        match &self.session {
            Session::Ready => self.dispatch(request).await,
            Session::Failed(reason) => {
                let _ = request.reply.send(Err(IcError::Session(reason.clone())));
            }
            Session::Unconnected | Session::ApplicationAuth(_) | Session::AccountAuth(_) => {
                self.queued.push(request);
            }
        }
    }

    async fn dispatch(&mut self, request: Queued) {
        let id = self.next_id();
        let historical = [Ustr::from(HISTORICAL_KEY)];
        let keys = is_historical(request.payload_type).then_some(historical.as_slice());
        let frame = ProtoMessage {
            payload_type: request.payload_type,
            payload: Some(request.payload),
            client_msg_id: Some(id.clone()),
        }
        .encode_to_vec();
        match self.send(frame, keys).await {
            Ok(()) => {
                // Callers that timed out left their replies closed.
                self.pending.retain(|_, pending| match pending {
                    Pending::Caller(reply) => !reply.is_closed(),
                    Pending::Spots { .. } => true,
                });
                self.pending.insert(id, Pending::Caller(request.reply));
            }
            Err(e) => {
                let _ = request.reply.send(Err(e));
            }
        }
    }

    async fn receive(&mut self, message: Message) {
        match message {
            Message::Binary(bytes) => match ProtoMessage::decode(&bytes[..]) {
                Ok(envelope) => self.route(envelope).await,
                Err(e) => log::warn!("malformed cTrader frame ({} bytes): {e}", bytes.len()),
            },
            Message::Text(text) if &text[..] == RECONNECTED.as_bytes() => {
                self.reconnected().await;
            }
            Message::Text(text) => {
                log::warn!("unexpected cTrader text frame ({} bytes)", text.len());
            }
            Message::Ping(_) | Message::Pong(_) | Message::Close(_) => {}
        }
    }

    /// The transport replaced the connection: a new cTrader session, so
    /// the responses still pending are lost, and it authenticates again
    /// before serving the requests that wait.
    async fn reconnected(&mut self) {
        log::warn!("cTrader reconnected: authenticating again");
        self.fail_pending(&IcError::ConnectionLost);
        drop(self.auth.begin());
        self.authenticate().await;
    }

    async fn route(&mut self, envelope: ProtoMessage) {
        if envelope.payload_type == ProtoHeartbeatEvent::TYPE {
            return;
        }
        let id = envelope.client_msg_id.as_deref();
        match &self.session {
            Session::ApplicationAuth(auth_id) if id == Some(auth_id.as_str()) => {
                match open::<ProtoOaApplicationAuthRes>(&envelope) {
                    Ok(_) => self.authenticate_account().await,
                    Err(e) => self.fail_session(auth_failure("application", &e)),
                }
                return;
            }
            Session::AccountAuth(auth_id) if id == Some(auth_id.as_str()) => {
                match open::<ProtoOaAccountAuthRes>(&envelope) {
                    Ok(res) if res.ctid_trader_account_id == self.credential.account_id.get() => {
                        self.ready().await;
                    }
                    Ok(_) => self.fail_session(
                        "cTrader authenticated another account than IC_ACCOUNT_ID".to_owned(),
                    ),
                    Err(e) => self.fail_session(auth_failure("account", &e)),
                }
                return;
            }
            _ => {}
        }
        match id.and_then(|id| self.pending.remove(id)) {
            Some(Pending::Caller(reply)) => {
                let _ = reply.send(Ok(envelope));
            }
            Some(Pending::Spots {
                subscribe,
                symbol_ids,
            }) => log_spots(subscribe, &symbol_ids, &envelope),
            None => self.event(&envelope),
        }
    }

    /// A message no request awaits: an event, or a late response.
    fn event(&mut self, envelope: &ProtoMessage) {
        let ended = [
            ProtoOaPayloadType::ProtoOaAccountsTokenInvalidatedEvent,
            ProtoOaPayloadType::ProtoOaAccountDisconnectEvent,
            ProtoOaPayloadType::ProtoOaClientDisconnectEvent,
        ];
        if envelope.payload_type == ProtoOaSpotEvent::TYPE {
            let payload = envelope.payload.as_deref().unwrap_or_default();
            match ProtoOaSpotEvent::decode(payload) {
                // The receiver is gone once the client stopped listening.
                Ok(spot) => {
                    let _ = self.spot_tx.send(spot);
                }
                Err(e) => log::warn!("malformed cTrader spot event: {e}"),
            }
            return;
        }
        let name = payload_name(envelope.payload_type);
        if ended.iter().any(|t| *t as u32 == envelope.payload_type) {
            self.fail_session(format!("cTrader ended the session ({name})"));
        } else {
            log::debug!("cTrader {name} not awaited");
        }
    }
}

/// Logs cTrader's answer to the handler's spot (un)subscription of
/// `symbol_ids`.
fn log_spots(subscribe: bool, symbol_ids: &[i64], envelope: &ProtoMessage) {
    let outcome = if subscribe {
        open::<ProtoOaSubscribeSpotsRes>(envelope).map(|_| ())
    } else {
        open::<ProtoOaUnsubscribeSpotsRes>(envelope).map(|_| ())
    };
    let action = if subscribe {
        "subscription"
    } else {
        "unsubscription"
    };
    match outcome {
        Ok(()) => log::debug!("cTrader spot {action} of symbols {symbol_ids:?} done"),
        Err(e) => log::error!("cTrader refused the spot {action} of symbols {symbol_ids:?}: {e}"),
    }
}

/// The error of a refused authentication, with what to check. Only the
/// code: the description is not shown, so nothing cTrader echoes from the
/// request can reach a log.
fn auth_failure(stage: &str, error: &IcError) -> String {
    let IcError::Venue { code, .. } = error else {
        return format!("the {stage} authentication failed: {error}");
    };
    let hint = match code.as_str() {
        "CH_CLIENT_AUTH_FAILURE" | "CH_OA_CLIENT_NOT_FOUND" => {
            "check IC_CLIENT_ID and IC_CLIENT_SECRET"
        }
        "CH_ACCESS_TOKEN_INVALID" | "OA_AUTH_TOKEN_EXPIRED" => {
            "the access token is invalid or expired: renew IC_ACCESS_TOKEN"
        }
        "CH_CTID_TRADER_ACCOUNT_NOT_FOUND" | "ACCOUNT_NOT_AUTHORIZED" | "RET_NO_SUCH_LOGIN" => {
            "check IC_ACCOUNT_ID, and IC_ENVIRONMENT: an account authenticates only on its own \
             environment"
        }
        _ => "see src/data/ic/README.md, \"Credentials\"",
    };
    format!("cTrader refused the {stage} authentication ({code}): {hint}")
}
