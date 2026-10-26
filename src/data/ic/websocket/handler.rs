//! The handler task: the one owner of the `WebSocketClient`. It
//! authenticates each connection, sends the heartbeat, tags requests with
//! a `clientMsgId` and routes each response back to its caller. It keeps
//! the spot subscriptions, sends them again on each new connection, and
//! passes spot events on.
//!
//! It renews the access token (see `token`): at start when the token is
//! due (its expiry unknown or within `RENEW_BEFORE`), on schedule while
//! the session runs, and when cTrader refuses the token at the account
//! auth. The renewal goes under the token file's lock: the file first,
//! since another session may have renewed the pair already (then the
//! handler takes that pair), else `ProtoOARefreshTokenReq`. Its answer
//! holds the only copy of the new pair, the old one spent: it is written
//! to the file before anything else, whatever the session went through
//! meanwhile (the answer is known by its `clientMsgId` or its payload
//! type), and a disconnect waits for it, up to `AUTH_TIMEOUT`. Then the
//! account is authenticated again on the open connection. When cTrader
//! ends the account session (`ProtoOAAccountsTokenInvalidatedEvent` or
//! `ProtoOAAccountDisconnectEvent`: the token was renewed, by this session
//! or another, or expired), the account is authenticated again with the
//! file's pair, renewed first if refused; during a renewal or an auth such
//! an event is the old token's, and ignored. Meanwhile new requests wait;
//! those in flight keep awaiting their response; the spot subscriptions,
//! which cTrader keeps on the connection, are not sent again, only those
//! changed meanwhile.

use std::{
    collections::{BTreeSet, HashMap},
    time::Duration,
};

use jiff::Timestamp;
use nautilus_core::string::secret::SecretString;
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
        ProtoOaAccountAuthRes, ProtoOaAccountDisconnectEvent, ProtoOaAccountsTokenInvalidatedEvent,
        ProtoOaApplicationAuthReq, ProtoOaApplicationAuthRes, ProtoOaClientDisconnectEvent,
        ProtoOaRefreshTokenReq, ProtoOaRefreshTokenRes, ProtoOaSpotEvent, ProtoOaSubscribeSpotsReq,
        ProtoOaSubscribeSpotsRes, ProtoOaUnsubscribeSpotsReq, ProtoOaUnsubscribeSpotsRes, frame,
        is_historical, open, payload_name, secret_frame,
    },
    token::{TokenLock, TokenPair},
};
use super::{AUTH_TIMEOUT, HISTORICAL_KEY};

/// How often a renewal tries the token file's lock while another session
/// holds it.
const LOCK_RETRY: Duration = Duration::from_millis(100);

/// cTrader's error codes of an access token it does not take.
const TOKEN_REFUSED: [&str; 2] = ["CH_ACCESS_TOKEN_INVALID", "OA_AUTH_TOKEN_EXPIRED"];

/// Where to get a new pair once cTrader refuses to renew it.
const PLAYGROUND: &str = "generate a new token pair in the Playground of the app on \
                          openapi.ctrader.com and set IC_ACCESS_TOKEN and IC_REFRESH_TOKEN in \
                          .env (see src/data/ic/README.md, \"Credentials\")";

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
    /// Renews the access token now, as on schedule.
    #[cfg(test)]
    RenewToken,
    Disconnect,
}

/// Where the session stands. Requests wait while it authenticates.
enum Session {
    /// No client yet.
    Unconnected,
    /// The application auth request with this `clientMsgId` is out.
    ApplicationAuth(String),
    /// A renewal waits for the token file's lock, which another session
    /// holds.
    TokenLock,
    /// The renewal request is out (see `IcWsHandler::renewal`).
    Renewal,
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

/// A renewal request out, under the token file's lock.
struct Renewal {
    client_msg_id: String,
    lock: TokenLock,
    /// Until when its answer is awaited.
    deadline: tokio::time::Instant,
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
    /// The token pair the account authenticates with.
    tokens: TokenPair,
    /// The renewal awaiting its answer: kept whatever happens to the
    /// session, until answered, the connection replaced or closed.
    renewal: Option<Renewal>,
    /// Whether the session renewed the pair since it was last ready: a
    /// token refused even then is not renewed again.
    renewed: bool,
    /// When to act without a message: renew on schedule (ready), try the
    /// lock again, or give up on an unanswered renewal.
    wake: Option<tokio::time::Instant>,
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
    /// The symbols the connection holds subscribed: cTrader keeps them
    /// across a new account auth on the same connection, answering
    /// `ALREADY_SUBSCRIBED` to a subscription sent again (demo,
    /// 2026-10-26); a new connection holds none.
    connection_spots: BTreeSet<i64>,
}

impl IcWsHandler {
    pub(super) fn new(
        credential: IcCredential,
        tokens: TokenPair,
        auth: AuthTracker,
        cmd_rx: mpsc::UnboundedReceiver<HandlerCommand>,
        raw_rx: mpsc::UnboundedReceiver<Message>,
        spot_tx: mpsc::UnboundedSender<ProtoOaSpotEvent>,
        heartbeat: Duration,
    ) -> Self {
        Self {
            client: None,
            credential,
            tokens,
            renewal: None,
            renewed: false,
            wake: None,
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
            connection_spots: BTreeSet::new(),
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
                    #[cfg(test)]
                    Some(HandlerCommand::RenewToken) => {
                        if matches!(self.session, Session::Ready) {
                            self.renew().await;
                        }
                    }
                    Some(HandlerCommand::Disconnect) | None => {
                        self.finish_renewal().await;
                        break;
                    }
                },
                message = self.raw_rx.recv() => match message {
                    Some(message) => self.receive(message).await,
                    None => {
                        log::debug!("cTrader transport closed");
                        break;
                    }
                },
                _ = heartbeat.tick() => self.send_heartbeat().await,
                () = sleep_until(self.wake), if self.wake.is_some() => {
                    self.wake = None;
                    self.woken().await;
                }
            }
        }
        self.close().await;
    }

    /// Before closing: a renewal out gets until its deadline for its
    /// answer, which is saved.
    async fn finish_renewal(&mut self) {
        while let Some(deadline) = self.renewal.as_ref().map(|r| r.deadline) {
            match tokio::time::timeout_at(deadline, self.raw_rx.recv()).await {
                Ok(Some(message)) => self.receive(message).await,
                Ok(None) | Err(_) => break,
            }
        }
    }

    /// Gives up the renewal out, if any: its answer can no longer come.
    fn abandon_renewal(&mut self, why: &str) {
        if self.renewal.take().is_some() {
            log::error!(
                "the cTrader access token renewal got no answer ({why}): its new pair is lost \
                 if cTrader made it; if the next run is refused, {PLAYGROUND}"
            );
        }
    }

    async fn close(&mut self) {
        self.abandon_renewal("the connection closed");
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
    /// response (see `receive`), the token renewed first if due.
    async fn authenticate(&mut self) {
        // A new connection: whatever the last one was renewing, or held
        // subscribed, is over.
        self.renewed = false;
        self.wake = None;
        self.connection_spots.clear();
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

    /// The application is authenticated: the account next, with the
    /// file's pair (another session may have renewed it), renewed first if
    /// due.
    async fn application_ready(&mut self) {
        self.adopt_stored();
        let due = self
            .tokens
            .renew_at()
            .is_none_or(|at| at <= Timestamp::now());
        if due {
            self.renew().await;
        } else {
            self.authenticate_account().await;
        }
    }

    /// Takes the file's pair if it is another than the session's, which
    /// another session renewed; whether it did. The session keeps its
    /// pair if the file cannot be read.
    fn adopt_stored(&mut self) -> bool {
        match self.credential.tokens.read() {
            Ok(stored) if !stored.same_access_token(&self.tokens) => {
                log::info!("cTrader token pair renewed by another session: taken from the file");
                self.tokens = stored;
                true
            }
            Ok(_) => false,
            Err(e) => {
                log::warn!("{e:#}: the session keeps its token pair");
                false
            }
        }
    }

    /// Renews the access token under the token file's lock, or takes the
    /// pair another session renewed meanwhile; while another session holds
    /// the lock, tries again every `LOCK_RETRY`.
    async fn renew(&mut self) {
        let lock = match self.credential.tokens.try_lock() {
            Ok(Some(lock)) => lock,
            Ok(None) => {
                self.session = Session::TokenLock;
                self.wake = Some(tokio::time::Instant::now() + LOCK_RETRY);
                return;
            }
            Err(e) => {
                self.fail_session(format!("renewing the access token: {e:#}"));
                return;
            }
        };
        if self.adopt_stored() {
            drop(lock);
            self.authenticate_account().await;
            return;
        }
        let id = self.next_id();
        let mut request = ProtoOaRefreshTokenReq {
            refresh_token: self.tokens.refresh_token.expose_secret().to_owned(),
            ..Default::default()
        };
        let frame = secret_frame(&request, Some(id.clone()));
        request.refresh_token.zeroize();
        match self.send(frame, None).await {
            Ok(()) => {
                log::info!("renewing the cTrader access token");
                let deadline = tokio::time::Instant::now() + AUTH_TIMEOUT;
                self.session = Session::Renewal;
                self.renewal = Some(Renewal {
                    client_msg_id: id,
                    lock,
                    deadline,
                });
                self.wake = Some(deadline);
            }
            Err(e) => self.fail_session(format!("sending the token renewal: {e}")),
        }
    }

    /// Whether `envelope` answers the renewal out: by its `clientMsgId`,
    /// or, whatever its id, as the renewal's answer message.
    fn answers_renewal(&self, envelope: &ProtoMessage) -> bool {
        self.renewal.as_ref().is_some_and(|renewal| {
            envelope.client_msg_id.as_deref() == Some(renewal.client_msg_id.as_str())
                || envelope.payload_type == ProtoOaRefreshTokenRes::TYPE
        })
    }

    /// cTrader's answer to the renewal out: the new pair, written to the
    /// token file before anything else, then the account auth with it.
    async fn renewal_answered(&mut self, envelope: &ProtoMessage) {
        let Some(Renewal { lock, .. }) = self.renewal.take() else {
            return;
        };
        let mut renewed = match open::<ProtoOaRefreshTokenRes>(envelope) {
            Ok(renewed) => renewed,
            Err(e) => {
                drop(lock);
                self.fail_session(renewal_failure(&e));
                return;
            }
        };
        let tokens = TokenPair::renewed(
            SecretString::from(std::mem::take(&mut renewed.access_token)),
            SecretString::from(std::mem::take(&mut renewed.refresh_token)),
            renewed.expires_in,
            Timestamp::now(),
        );
        match lock.save(&tokens) {
            Ok(()) => log::info!(
                "cTrader access token renewed, valid until {}; saved to {}",
                tokens
                    .expires_at
                    .map_or_else(String::new, |t| t.to_string()),
                self.credential.tokens.path().display()
            ),
            // The old pair is spent: this session goes on with the new one,
            // which no later run will find.
            Err(e) => log::error!(
                "cTrader access token renewed but not saved ({e:#}): this session goes on \
                 with it; once it ends, {PLAYGROUND}"
            ),
        }
        drop(lock);
        self.tokens = tokens;
        self.renewed = true;
        self.authenticate_account().await;
    }

    async fn authenticate_account(&mut self) {
        self.wake = None;
        let id = self.next_id();
        let mut request = ProtoOaAccountAuthReq {
            ctid_trader_account_id: self.credential.account_id.get(),
            access_token: self.tokens.access_token.expose_secret().to_owned(),
            ..Default::default()
        };
        let frame = secret_frame(&request, Some(id.clone()));
        request.access_token.zeroize();
        match self.send(frame, None).await {
            Ok(()) => self.session = Session::AccountAuth(id),
            Err(e) => self.fail_session(format!("sending the account auth: {e}")),
        }
    }

    /// Acts on the time `wake` set.
    async fn woken(&mut self) {
        match self.session {
            Session::Ready | Session::TokenLock => self.renew().await,
            // The renewal stays out: a late answer is still saved.
            Session::Renewal => self.fail_session(format!(
                "cTrader did not answer the access token renewal within {AUTH_TIMEOUT:?}: if \
                 the next run is refused, {PLAYGROUND}"
            )),
            Session::Unconnected
            | Session::ApplicationAuth(_)
            | Session::AccountAuth(_)
            | Session::Failed(_) => {}
        }
    }

    async fn ready(&mut self) {
        log::info!("cTrader session authenticated");
        self.session = Session::Ready;
        self.renewed = false;
        self.wake = self.tokens.renew_at().map(|at| {
            let wait = at
                .duration_since(Timestamp::now())
                .max(jiff::SignedDuration::ZERO);
            tokio::time::Instant::now() + wait.unsigned_abs()
        });
        self.auth.succeed();
        // The connection's subscriptions brought in line with the session's:
        // all of them on a new connection, those changed meanwhile after a
        // new account auth on the same one.
        let subscribe: Vec<i64> = self
            .spots
            .difference(&self.connection_spots)
            .copied()
            .collect();
        if !subscribe.is_empty() {
            self.send_spots(true, subscribe).await;
        }
        let unsubscribe: Vec<i64> = self
            .connection_spots
            .difference(&self.spots)
            .copied()
            .collect();
        if !unsubscribe.is_empty() {
            self.send_spots(false, unsubscribe).await;
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
        self.wake = None;
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
                for symbol_id in &symbol_ids {
                    if subscribe {
                        self.connection_spots.insert(*symbol_id);
                    } else {
                        self.connection_spots.remove(symbol_id);
                    }
                }
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
            Session::Unconnected
            | Session::ApplicationAuth(_)
            | Session::TokenLock
            | Session::Renewal
            | Session::AccountAuth(_) => {
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
        self.abandon_renewal("the connection was replaced");
        self.fail_pending(&IcError::ConnectionLost);
        drop(self.auth.begin());
        self.authenticate().await;
    }

    async fn route(&mut self, envelope: ProtoMessage) {
        if envelope.payload_type == ProtoHeartbeatEvent::TYPE {
            return;
        }
        if self.answers_renewal(&envelope) {
            self.renewal_answered(&envelope).await;
            return;
        }
        let id = envelope.client_msg_id.as_deref();
        match &self.session {
            Session::ApplicationAuth(auth_id) if id == Some(auth_id.as_str()) => {
                match open::<ProtoOaApplicationAuthRes>(&envelope) {
                    Ok(_) => self.application_ready().await,
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
                    Err(e) if is_token_refused(&e) && !self.renewed => {
                        log::warn!("cTrader refused the access token: renewing it");
                        self.renew().await;
                    }
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
            None => self.event(&envelope).await,
        }
    }

    /// cTrader ended the account session (`name`), its token renewed or
    /// expired: the account authenticates again, with the file's pair
    /// (renewed if refused). During a renewal or an auth the event is the
    /// old token's, already replaced.
    async fn account_ended(&mut self, name: &str) {
        if !matches!(self.session, Session::Ready) {
            log::info!("cTrader ended the previous account session ({name})");
            return;
        }
        log::warn!("cTrader ended the account session ({name}): authenticating it again");
        self.adopt_stored();
        self.authenticate_account().await;
    }

    /// A message no request awaits: an event, or a late response.
    async fn event(&mut self, envelope: &ProtoMessage) {
        if [
            ProtoOaAccountsTokenInvalidatedEvent::TYPE,
            ProtoOaAccountDisconnectEvent::TYPE,
        ]
        .contains(&envelope.payload_type)
        {
            self.account_ended(&payload_name(envelope.payload_type))
                .await;
            return;
        }
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
        if envelope.payload_type == ProtoOaClientDisconnectEvent::TYPE {
            self.fail_session(format!("cTrader ended the session ({name})"));
        } else {
            log::debug!("cTrader {name} not awaited");
        }
    }
}

/// Waits until `at`, or forever without one.
async fn sleep_until(at: Option<tokio::time::Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Whether `error` is cTrader refusing the access token.
fn is_token_refused(error: &IcError) -> bool {
    matches!(error, IcError::Venue { code, .. } if TOKEN_REFUSED.contains(&code.as_str()))
}

/// The error of a refused renewal: the code only, as for an auth.
fn renewal_failure(error: &IcError) -> String {
    match error {
        IcError::Venue { code, .. } => {
            format!("cTrader refused to renew the access token ({code}): {PLAYGROUND}")
        }
        other => format!("renewing the access token: {other}"),
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
            return format!(
                "cTrader refused the {stage} authentication ({code}) even with a renewed \
                 access token: {PLAYGROUND}"
            );
        }
        "CH_CTID_TRADER_ACCOUNT_NOT_FOUND" | "ACCOUNT_NOT_AUTHORIZED" | "RET_NO_SUCH_LOGIN" => {
            "check IC_ACCOUNT_ID, and IC_ENVIRONMENT: an account authenticates only on its own \
             environment"
        }
        _ => "see src/data/ic/README.md, \"Credentials\"",
    };
    format!("cTrader refused the {stage} authentication ({code}): {hint}")
}
