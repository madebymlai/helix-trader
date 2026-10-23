//! The handler task: the one owner of the `WebSocketClient`. It
//! authenticates each connection, sends the heartbeat, tags requests with
//! a `clientMsgId` and routes each response back to its caller.

use std::{collections::HashMap, time::Duration};

use nautilus_network::{
    Message, RECONNECTED,
    websocket::{AuthTracker, WebSocketClient},
};
use prost::Message as _;
use tokio::sync::{mpsc, oneshot};
use zeroize::Zeroize;

use super::super::{
    config::IcCredential,
    messages::{
        IcError, Payload, ProtoHeartbeatEvent, ProtoMessage, ProtoOaAccountAuthReq,
        ProtoOaAccountAuthRes, ProtoOaApplicationAuthReq, ProtoOaApplicationAuthRes,
        ProtoOaPayloadType, frame, open, payload_name, secret_frame,
    },
};

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
    heartbeat: Duration,
    session: Session,
    next_id: u64,
    pending: HashMap<String, Reply>,
    queued: Vec<Queued>,
}

impl IcWsHandler {
    pub(super) fn new(
        credential: IcCredential,
        auth: AuthTracker,
        cmd_rx: mpsc::UnboundedReceiver<HandlerCommand>,
        raw_rx: mpsc::UnboundedReceiver<Message>,
        heartbeat: Duration,
    ) -> Self {
        Self {
            client: None,
            credential,
            auth,
            cmd_rx,
            raw_rx,
            heartbeat,
            session: Session::Unconnected,
            next_id: 0,
            pending: HashMap::new(),
            queued: Vec::new(),
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

    /// Sends one frame. No frame content is logged: auth frames hold the
    /// secret and the token. The transport takes the frame; its copies are
    /// out of the adapter's reach.
    async fn send(&self, frame: Vec<u8>) -> Result<(), IcError> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| IcError::Transport("no connection".to_owned()))?;
        client
            .send_bytes(frame, None)
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
            .send(frame(&ProtoHeartbeatEvent::default(), None))
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
        match self.send(frame).await {
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
        match self.send(frame).await {
            Ok(()) => self.session = Session::AccountAuth(id),
            Err(e) => self.fail_session(format!("sending the account auth: {e}")),
        }
    }

    async fn ready(&mut self) {
        log::info!("cTrader session authenticated");
        self.session = Session::Ready;
        self.auth.succeed();
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

    fn fail_pending(&mut self, error: &IcError) {
        for (_, reply) in self.pending.drain() {
            let _ = reply.send(Err(error.clone()));
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
        let frame = ProtoMessage {
            payload_type: request.payload_type,
            payload: Some(request.payload),
            client_msg_id: Some(id.clone()),
        }
        .encode_to_vec();
        match self.send(frame).await {
            Ok(()) => {
                // Callers that timed out left their replies closed.
                self.pending.retain(|_, reply| !reply.is_closed());
                self.pending.insert(id, request.reply);
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
        if let Some(reply) = id.and_then(|id| self.pending.remove(id)) {
            let _ = reply.send(Ok(envelope));
            return;
        }
        self.event(&envelope);
    }

    /// A message no request awaits: an event, or a late response.
    fn event(&mut self, envelope: &ProtoMessage) {
        let ended = [
            ProtoOaPayloadType::ProtoOaAccountsTokenInvalidatedEvent,
            ProtoOaPayloadType::ProtoOaAccountDisconnectEvent,
            ProtoOaPayloadType::ProtoOaClientDisconnectEvent,
        ];
        let name = payload_name(envelope.payload_type);
        if ended.iter().any(|t| *t as u32 == envelope.payload_type) {
            self.fail_session(format!("cTrader ended the session ({name})"));
        } else {
            log::debug!("cTrader {name} not awaited");
        }
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
        _ => "see the data README, \"IC Markets specifics\"",
    };
    format!("cTrader refused the {stage} authentication ({code}): {hint}")
}
