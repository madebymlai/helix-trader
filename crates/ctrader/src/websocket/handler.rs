//! The handler task: the one owner of the `WebSocketClient`. It authenticates each connection,
//! sends the heartbeat, tags requests with a `clientMsgId` and routes each response back to its
//! caller. It tracks the spot subscriptions in Nautilus' `SubscriptionState` (confirmed or failed
//! by cTrader's answer) and sends them all again on each new connection.
//!
//! It renews the access token (see `token`) at start when due (expiry unknown or within
//! `RENEW_BEFORE`), on schedule, and when cTrader refuses the token at the account auth. The
//! renewal goes under the token file's lock: the file first, since another session may have
//! renewed the pair, else `ProtoOARefreshTokenReq`. Its answer holds the only copy of the new
//! pair, the old one spent, so it is written to the file before anything else, whatever the
//! session went through meanwhile (the answer is known by its `clientMsgId` or payload type), and a
//! disconnect waits for it, up to `AUTH_TIMEOUT`. Then the account is authenticated again on the
//! open connection.
//!
//! When cTrader ends the account session (`ProtoOAAccountsTokenInvalidatedEvent` or
//! `ProtoOAAccountDisconnectEvent`: the token was renewed by this session or another, or expired),
//! the account is authenticated again with the file's pair, renewed first if refused. During a
//! renewal or an auth such an event is the old token's, and ignored. Meanwhile new requests wait
//! and those in flight keep awaiting their response. The spot subscriptions, which cTrader keeps on
//! the connection, are not sent again: (un)subscriptions asked meanwhile wait.
//!
//! Every frame but the heartbeat is bound to the connection the session authenticates on
//! (`WebSocketClient::send_bytes_on_connection`, the epoch as its `RECONNECTED` carries it): the
//! transport writes it there or not at all, never on the next connection, which it would reach
//! before its authentication. A frame refused because that connection is gone or going (refused at
//! once while the transport reconnects, as Nautilus' OKX handler does) was never sent: a request
//! or trade request waits for the new session, a spot change waits in the subscription state, and
//! the auth frames and a renewal wait for the new connection. The heartbeat goes to the current
//! connection. A frame whose write timed out or failed after it began may have reached cTrader,
//! and counts as sent.
//!
//! Trade requests go out under the `clientMsgId` their caller chose, who learns only whether the
//! frame was sent; their outcome comes as trade events (`TradeEvents`). A trade request still
//! waiting when the session fails or the connection closes was never sent, and says so.

use std::collections::{BTreeMap, HashMap};

use jiff::Timestamp;
use nautilus_common::live::dst::time::{self, Duration, Instant, MissedTickBehavior};
use nautilus_core::{string::secret::SecretString, time::get_atomic_clock_realtime};
use nautilus_network::{
    Message, RECONNECTED,
    error::SendError,
    websocket::{AuthTracker, SubscriptionState, WebSocketClient},
};
use prost::Message as _;
use zeroize::Zeroize;

use super::{
    AUTH_TIMEOUT, HEARTBEAT_INTERVAL, TradeEvent, TradeMessage, spot_subscribed, spot_symbol,
    spot_topic,
};
use crate::{
    common::{
        messages::{
            CTraderError, Payload, ProtoErrorRes, ProtoHeartbeatEvent, ProtoMessage,
            ProtoOaAccountAuthReq, ProtoOaAccountAuthRes, ProtoOaAccountDisconnectEvent,
            ProtoOaAccountsTokenInvalidatedEvent, ProtoOaApplicationAuthReq,
            ProtoOaApplicationAuthRes, ProtoOaClientDisconnectEvent, ProtoOaErrorRes,
            ProtoOaExecutionEvent, ProtoOaOrderErrorEvent, ProtoOaRefreshTokenReq,
            ProtoOaRefreshTokenRes, ProtoOaSpotEvent, ProtoOaSubscribeSpotsReq,
            ProtoOaSubscribeSpotsRes, ProtoOaUnsubscribeSpotsReq, ProtoOaUnsubscribeSpotsRes,
            frame, open, payload_name, secret_frame,
        },
        token::{TokenLock, TokenPair, TokenStore},
    },
    config::{CTraderCredential, CTraderSettingNames},
};

pub(super) const LOCK_RETRY: Duration = Duration::from_millis(100);

const TOKEN_REFUSED: [&str; 2] = ["CH_ACCESS_TOKEN_INVALID", "OA_AUTH_TOKEN_EXPIRED"];

fn playground(names: &CTraderSettingNames) -> String {
    format!(
        "generate a new token pair in the Playground of the app on openapi.ctrader.com and \
         set {} and {} in {} (see {})",
        names.access_token, names.refresh_token, names.source, names.guide
    )
}

pub(super) type Reply = tokio::sync::oneshot::Sender<Result<ProtoMessage, CTraderError>>;

/// `Ok` once a trade request's frame is written or may have been, an error if it never was.
pub(super) type Sent = tokio::sync::oneshot::Sender<Result<(), CTraderError>>;

pub(super) enum HandlerCommand {
    /// Queued before the command sender is shared, so no request overtakes it.
    SetClient(WebSocketClient),
    Request {
        payload_type: u32,
        payload: Vec<u8>,
        reply: Reply,
    },
    Trade {
        payload_type: u32,
        payload: Vec<u8>,
        client_msg_id: String,
        sent: Sent,
    },
    TradeEvents(tokio::sync::mpsc::UnboundedSender<TradeEvent>),
    SubscribeSpots(i64),
    UnsubscribeSpots(i64),
    #[cfg(any(test, feature = "test-support"))]
    RenewToken,
    Disconnect,
}

enum Session {
    /// No client yet, or no connection to authenticate on until the next `RECONNECTED`.
    Unconnected,
    ApplicationAuth(String),
    /// Another session holds the token file's lock.
    TokenLock,
    Renewal,
    AccountAuth(String),
    Ready,
    /// Authentication failed, or cTrader ended the session.
    Failed(String),
}

enum Pending {
    Caller(Reply),
    Spots {
        subscribe: bool,
        symbol_ids: Vec<i64>,
    },
    Trade,
}

struct Renewal {
    client_msg_id: String,
    lock: TokenLock,
    deadline: Instant,
}

struct Queued {
    payload_type: u32,
    payload: Vec<u8>,
    awaiting: Awaiting,
}

enum Awaiting {
    Caller(Reply),
    Trade { client_msg_id: String, sent: Sent },
}

impl Awaiting {
    fn fail(self, e: CTraderError) {
        match self {
            Self::Caller(reply) => {
                let _ = reply.send(Err(e));
            }
            Self::Trade { sent, .. } => {
                let _ = sent.send(Err(e));
            }
        }
    }
}

pub(super) struct CTraderWsHandler {
    client: Option<WebSocketClient>,
    connection: u64,
    credential: CTraderCredential,
    token_file: TokenStore,
    tokens: TokenPair,
    /// Kept until answered, the connection replaced or closed.
    renewal: Option<Renewal>,
    /// Whether the pair was renewed since the session was last ready: a token refused even then is
    /// not renewed again.
    renewed: bool,
    wake: Option<Instant>,
    /// Invalidated on a new account auth on the same connection too, so historical requests take
    /// no slots meanwhile.
    auth: AuthTracker,
    cmd_rx: tokio::sync::mpsc::UnboundedReceiver<HandlerCommand>,
    raw_rx: tokio::sync::mpsc::UnboundedReceiver<(u64, Message)>,
    spot_tx: tokio::sync::mpsc::UnboundedSender<ProtoOaSpotEvent>,
    trade_tx: Option<tokio::sync::mpsc::UnboundedSender<TradeEvent>>,
    session: Session,
    next_id: u64,
    pending: HashMap<String, Pending>,
    queued: Vec<Queued>,
    subscriptions: SubscriptionState,
    /// Whether the connection is new and holds no subscriptions: changes go to `subscriptions`
    /// only, and all are sent once the session is ready. Else changes asked meanwhile wait in
    /// `spot_changes`, as cTrader keeps a connection's subscriptions across a new account auth,
    /// answering `ALREADY_SUBSCRIBED` to one sent again (demo, 2026-10-26).
    replay: bool,
    /// The last (un)subscription of each symbol asked during an account auth: `true` subscribes.
    spot_changes: BTreeMap<i64, bool>,
}

impl CTraderWsHandler {
    pub(super) fn new(
        credential: CTraderCredential,
        tokens: TokenPair,
        auth: AuthTracker,
        subscriptions: SubscriptionState,
        cmd_rx: tokio::sync::mpsc::UnboundedReceiver<HandlerCommand>,
        raw_rx: tokio::sync::mpsc::UnboundedReceiver<(u64, Message)>,
        spot_tx: tokio::sync::mpsc::UnboundedSender<ProtoOaSpotEvent>,
    ) -> Self {
        Self {
            client: None,
            connection: 0,
            token_file: credential.tokens(),
            credential,
            tokens,
            renewal: None,
            renewed: false,
            wake: None,
            auth,
            cmd_rx,
            raw_rx,
            spot_tx,
            trade_tx: None,
            session: Session::Unconnected,
            next_id: 0,
            pending: HashMap::new(),
            queued: Vec::new(),
            subscriptions,
            replay: true,
            spot_changes: BTreeMap::new(),
        }
    }

    pub(super) async fn run(mut self) {
        let start = Instant::now() + HEARTBEAT_INTERVAL;
        let mut heartbeat = time::interval_at(start, HEARTBEAT_INTERVAL);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                command = self.cmd_rx.recv() => match command {
                    Some(HandlerCommand::SetClient(client)) => {
                        self.connection = client.connection_epoch();
                        self.client = Some(client);
                        self.authenticate().await;
                    }
                    Some(HandlerCommand::Request { payload_type, payload, reply }) => {
                        let awaiting = Awaiting::Caller(reply);
                        self.request(Queued { payload_type, payload, awaiting }).await;
                    }
                    Some(HandlerCommand::Trade { payload_type, payload, client_msg_id, sent }) => {
                        let awaiting = Awaiting::Trade { client_msg_id, sent };
                        self.request(Queued { payload_type, payload, awaiting }).await;
                    }
                    Some(HandlerCommand::TradeEvents(listener)) => {
                        self.trade_tx = Some(listener);
                    }
                    Some(HandlerCommand::SubscribeSpots(symbol_id)) => {
                        self.change_spots(symbol_id, true).await;
                    }
                    Some(HandlerCommand::UnsubscribeSpots(symbol_id)) => {
                        self.change_spots(symbol_id, false).await;
                    }
                    #[cfg(any(test, feature = "test-support"))]
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
                    Some((epoch, message)) => self.receive(epoch, message).await,
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

    /// A renewal in flight gets until its deadline for its answer, which is saved.
    async fn finish_renewal(&mut self) {
        while let Some(deadline) = self.renewal.as_ref().map(|r| r.deadline) {
            let left = deadline.saturating_duration_since(Instant::now());
            match time::timeout(left, self.raw_rx.recv()).await {
                Ok(Some((epoch, message))) => self.receive(epoch, message).await,
                Ok(None) | Err(_) => break,
            }
        }
    }

    fn abandon_renewal(&mut self, why: &str) {
        if self.renewal.take().is_some() {
            log::error!(
                "the cTrader access token renewal got no answer ({why}): its new pair is lost \
                 if cTrader made it; if the next run is refused, {}",
                playground(&self.credential.names)
            );
        }
    }

    async fn close(&mut self) {
        self.abandon_renewal("the connection closed");
        if let Some(client) = self.client.take() {
            // Fails the authentication, which the transport tracks
            client.disconnect().await;
        }
        self.fail_pending(&CTraderError::ConnectionLost);
        for queued in self.queued.drain(..) {
            queued.awaiting.fail(CTraderError::ConnectionLost);
        }
    }

    fn next_id(&mut self) -> String {
        self.next_id += 1;
        self.next_id.to_string()
    }

    async fn send(&self, frame: Vec<u8>) -> Result<(), SendError> {
        self.send_on(frame, self.connection).await
    }

    /// Sends one frame on the connection of epoch `connection`, or not at all. No frame content is
    /// logged: auth frames hold the secret and the token.
    async fn send_on(&self, frame: Vec<u8>, connection: u64) -> Result<(), SendError> {
        let client = self.client.as_ref().ok_or(SendError::Closed)?;

        // Refused at once rather than held in the transport until a connection is up, which is not
        // the frame's.
        if client.connection_mode().is_reconnect() {
            return Err(SendError::ConnectionChanged);
        }
        client
            .send_bytes_on_connection(frame, None, connection)
            .await
            .or_else(failed_send)
    }

    /// A `ProtoHeartbeatEvent` every 10 s (help.ctrader.com/open-api, "Establish a connection"),
    /// skipped while reconnecting as Nautilus skips a pong (`WebSocketClient::send_pong`).
    async fn send_heartbeat(&self) {
        let Some(client) = self.client.as_ref().filter(|c| c.is_active()) else {
            return;
        };
        let heartbeat = frame(&ProtoHeartbeatEvent::default(), None);
        match self.send_on(heartbeat, client.connection_epoch()).await {
            Ok(()) | Err(SendError::ConnectionChanged) => {}
            Err(e) => log::warn!("cTrader heartbeat not sent: {e}"),
        }
    }

    fn await_connection(&mut self, sending: &str) {
        log::debug!("cTrader {sending} not sent: the connection changed");
        self.wake = None;
        self.session = Session::Unconnected;
    }

    async fn authenticate(&mut self) {
        // A new connection: the last one's renewal is over and it holds no subscription, so the
        // changes waiting go to the state, all sent once the session is ready.
        self.renewed = false;
        self.wake = None;
        self.replay = true;
        for (symbol_id, subscribe) in std::mem::take(&mut self.spot_changes) {
            self.mark_spots(symbol_id, subscribe);
        }
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
            Err(SendError::ConnectionChanged) => self.await_connection("application auth"),
            Err(e) => {
                self.fail_session(format!("sending the application auth: {e}"));
            }
        }
    }

    async fn application_ready(&mut self) {
        self.adopt_stored();
        let due = self.tokens.renew_at().is_none_or(|at| at <= wall_clock());
        if due {
            self.renew().await;
        } else {
            self.authenticate_account().await;
        }
    }

    /// Takes the file's pair if another session renewed it. Keeps the session's pair if the file
    /// cannot be read.
    fn adopt_stored(&mut self) -> bool {
        match self.token_file.read() {
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

    /// Retries every `LOCK_RETRY` while another session holds the lock.
    async fn renew(&mut self) {
        self.auth.invalidate();
        let lock = match self.token_file.try_lock() {
            Ok(Some(lock)) => lock,
            Ok(None) => {
                self.session = Session::TokenLock;
                self.wake = Some(Instant::now() + LOCK_RETRY);
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
        match self.send(frame).await {
            Ok(()) => {
                log::info!("renewing the cTrader access token");
                let deadline = Instant::now() + AUTH_TIMEOUT;
                self.session = Session::Renewal;
                self.renewal = Some(Renewal {
                    client_msg_id: id,
                    lock,
                    deadline,
                });
                self.wake = Some(deadline);
            }

            // The new connection's application auth renews the token if due
            Err(SendError::ConnectionChanged) => self.await_connection("token renewal"),
            Err(e) => self.fail_session(format!("sending the token renewal: {e}")),
        }
    }

    /// By its `clientMsgId` or, whatever its id, as the renewal's answer message.
    fn answers_renewal(&self, envelope: &ProtoMessage) -> bool {
        self.renewal.as_ref().is_some_and(|renewal| {
            envelope.client_msg_id.as_deref() == Some(renewal.client_msg_id.as_str())
                || envelope.payload_type == ProtoOaRefreshTokenRes::TYPE
        })
    }

    async fn renewal_answered(&mut self, envelope: &ProtoMessage) {
        let Some(Renewal { lock, .. }) = self.renewal.take() else {
            return;
        };
        let mut renewed = match open::<ProtoOaRefreshTokenRes>(envelope) {
            Ok(renewed) => renewed,
            Err(e) => {
                drop(lock);
                self.fail_session(renewal_failure(&e, &self.credential.names));
                return;
            }
        };
        let tokens = TokenPair::renewed(
            SecretString::from(std::mem::take(&mut renewed.access_token)),
            SecretString::from(std::mem::take(&mut renewed.refresh_token)),
            renewed.expires_in,
            wall_clock(),
        );
        match lock.save(&tokens) {
            Ok(()) => log::info!(
                "cTrader access token renewed, valid until {}; saved to {}",
                tokens
                    .expires_at
                    .map_or_else(String::new, |t| t.to_string()),
                self.token_file.path().display()
            ),

            // The old pair is spent: this session goes on with the new one, which no later run
            // finds.
            Err(e) => log::error!(
                "cTrader access token renewed but not saved ({e:#}): this session goes on \
                 with it; once it ends, {}",
                playground(&self.credential.names)
            ),
        }
        drop(lock);
        self.tokens = tokens;
        self.renewed = true;
        self.authenticate_account().await;
    }

    async fn authenticate_account(&mut self) {
        self.auth.invalidate();
        self.wake = None;
        let id = self.next_id();
        let mut request = ProtoOaAccountAuthReq {
            ctid_trader_account_id: self.credential.account_id.get(),
            access_token: self.tokens.access_token.expose_secret().to_owned(),
            ..Default::default()
        };
        let frame = secret_frame(&request, Some(id.clone()));
        request.access_token.zeroize();
        match self.send(frame).await {
            Ok(()) => self.session = Session::AccountAuth(id),
            Err(SendError::ConnectionChanged) => self.await_connection("account auth"),
            Err(e) => self.fail_session(format!("sending the account auth: {e}")),
        }
    }

    async fn woken(&mut self) {
        match self.session {
            Session::Ready | Session::TokenLock => self.renew().await,

            // The renewal stays out: a late answer is still saved
            Session::Renewal => self.fail_session(format!(
                "cTrader did not answer the access token renewal within {AUTH_TIMEOUT:?}: if \
                 the next run is refused, {}",
                playground(&self.credential.names)
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
                .duration_since(wall_clock())
                .max(jiff::SignedDuration::ZERO);
            Instant::now() + wait.unsigned_abs()
        });
        self.auth.succeed();

        // All subscriptions on a new connection, those changed meanwhile after a new account auth
        // on the same one.
        let mut subscribe = Vec::new();
        let mut unsubscribe = Vec::new();
        if std::mem::take(&mut self.replay) {
            subscribe = self
                .subscriptions
                .reset_after_reconnect()
                .iter()
                .map(|topic| spot_symbol(topic))
                .collect();
        }
        for (symbol_id, subscribe_it) in std::mem::take(&mut self.spot_changes) {
            if self.mark_spots(symbol_id, subscribe_it) {
                if subscribe_it {
                    subscribe.push(symbol_id);
                } else {
                    unsubscribe.push(symbol_id);
                }
            }
        }
        if !subscribe.is_empty() {
            self.send_spots(true, subscribe).await;
        }
        if !unsubscribe.is_empty() {
            self.send_spots(false, unsubscribe).await;
        }
        for queued in std::mem::take(&mut self.queued) {
            self.dispatch(queued).await;
        }
    }

    fn fail_session(&mut self, reason: String) {
        log::error!("cTrader session failed: {reason}");
        self.auth.fail(reason.clone());
        self.fail_pending(&CTraderError::Session(reason.clone()));
        for queued in self.queued.drain(..) {
            queued.awaiting.fail(CTraderError::Session(reason.clone()));
        }
        self.wake = None;
        self.session = Session::Failed(reason);
    }

    /// The handler's own spot requests are dropped, the next connection sending them again, and so
    /// are the trade requests, their outcome unknown until reconciliation.
    fn fail_pending(&mut self, error: &CTraderError) {
        for (_, pending) in self.pending.drain() {
            if let Pending::Caller(reply) = pending {
                let _ = reply.send(Err(error.clone()));
            }
        }
    }

    async fn change_spots(&mut self, symbol_id: i64, subscribe: bool) {
        if matches!(self.session, Session::Ready) {
            if self.mark_spots(symbol_id, subscribe) {
                self.send_spots(subscribe, vec![symbol_id]).await;
            }
        } else if self.replay {
            self.mark_spots(symbol_id, subscribe);
        } else {
            self.spot_changes.insert(symbol_id, subscribe);
        }
    }

    /// Returns whether it changes the state: a subscription of a symbol not yet wanted, an
    /// unsubscription of one subscribed or pending.
    fn mark_spots(&self, symbol_id: i64, subscribe: bool) -> bool {
        let topic = spot_topic(symbol_id);
        if subscribe {
            return self.subscriptions.try_mark_subscribe(&topic);
        }
        let subscribed = spot_subscribed(&self.subscriptions, symbol_id);
        if subscribed {
            self.subscriptions.mark_unsubscribe(&topic);
        }
        subscribed
    }

    /// One refused because the connection changed stays marked in the state, which the new session
    /// sends (answers: see `spots_answered`).
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
        match self.send(frame).await {
            Ok(()) => {
                self.pending.insert(
                    id,
                    Pending::Spots {
                        subscribe,
                        symbol_ids,
                    },
                );
            }
            Err(SendError::ConnectionChanged) => {
                log::debug!(
                    "cTrader spot change of {symbol_ids:?} not sent: the connection changed"
                );
            }
            Err(e) => {
                log::error!("cTrader spot subscription of {symbol_ids:?} not sent: {e}");
                if subscribe {
                    for symbol_id in &symbol_ids {
                        self.subscriptions.mark_failure(&spot_topic(*symbol_id));
                    }
                }
            }
        }
    }

    async fn request(&mut self, request: Queued) {
        match &self.session {
            Session::Ready => self.dispatch(request).await,
            Session::Failed(reason) => {
                request.awaiting.fail(CTraderError::Session(reason.clone()));
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
        let id = match &request.awaiting {
            Awaiting::Caller(_) => self.next_id(),
            Awaiting::Trade { client_msg_id, .. } => client_msg_id.clone(),
        };
        let frame = ProtoMessage {
            payload_type: request.payload_type,
            payload: Some(request.payload.clone()),
            client_msg_id: Some(id.clone()),
        }
        .encode_to_vec();
        match self.send(frame).await {
            Ok(()) => {}
            Err(SendError::ConnectionChanged) => {
                log::debug!(
                    "cTrader {} held for the new session",
                    payload_name(request.payload_type)
                );
                self.queued.push(request);
                return;
            }
            Err(e) => {
                request
                    .awaiting
                    .fail(CTraderError::Transport(e.to_string()));
                return;
            }
        }

        // Callers that timed out left their replies closed
        self.pending.retain(|_, pending| match pending {
            Pending::Caller(reply) => !reply.is_closed(),
            Pending::Spots { .. } | Pending::Trade => true,
        });
        match request.awaiting {
            Awaiting::Caller(reply) => {
                self.pending.insert(id, Pending::Caller(reply));
            }
            Awaiting::Trade { sent, .. } => {
                self.pending.insert(id, Pending::Trade);
                let _ = sent.send(Ok(()));
            }
        }
    }

    async fn receive(&mut self, epoch: u64, message: Message) {
        match message {
            Message::Binary(bytes) => match ProtoMessage::decode(&bytes[..]) {
                Ok(envelope) => self.route(envelope).await,
                Err(e) => log::warn!("malformed cTrader frame ({} bytes): {e}", bytes.len()),
            },
            Message::Text(text) if &text[..] == RECONNECTED.as_bytes() => {
                self.reconnected(epoch).await;
            }
            Message::Text(text) => {
                log::warn!("unexpected cTrader text frame ({} bytes)", text.len());
            }
            Message::Ping(_) | Message::Pong(_) | Message::Close(_) => {}
        }
    }

    /// A new connection is a new cTrader session: pending responses are lost.
    async fn reconnected(&mut self, epoch: u64) {
        log::warn!("cTrader reconnected: authenticating again");
        self.connection = epoch;
        self.abandon_renewal("the connection was replaced");
        self.fail_pending(&CTraderError::ConnectionLost);
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
                    Err(e) => {
                        self.fail_session(auth_failure("application", &e, &self.credential.names));
                    }
                }
                return;
            }
            Session::AccountAuth(auth_id) if id == Some(auth_id.as_str()) => {
                match open::<ProtoOaAccountAuthRes>(&envelope) {
                    Ok(res) if res.ctid_trader_account_id == self.credential.account_id.get() => {
                        self.ready().await;
                    }
                    Ok(_) => self.fail_session(format!(
                        "cTrader authenticated another account than {}",
                        self.credential.names.account_id
                    )),
                    Err(e) if is_token_refused(&e) && !self.renewed => {
                        log::warn!("cTrader refused the access token: renewing it");
                        self.renew().await;
                    }
                    Err(e) => {
                        self.fail_session(auth_failure("account", &e, &self.credential.names));
                    }
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
            }) => self.spots_answered(subscribe, &symbol_ids, &envelope),
            Some(Pending::Trade) => self.trade_answered(&envelope),
            None => self.event(&envelope).await,
        }
    }

    fn trade_answered(&self, envelope: &ProtoMessage) {
        let message = match envelope.payload_type {
            t if t == ProtoOaExecutionEvent::TYPE => {
                open::<ProtoOaExecutionEvent>(envelope).map(TradeMessage::Execution)
            }
            t if t == ProtoOaOrderErrorEvent::TYPE => {
                open::<ProtoOaOrderErrorEvent>(envelope).map(TradeMessage::OrderError)
            }
            t if t == ProtoOaErrorRes::TYPE => {
                open::<ProtoOaErrorRes>(envelope).map(|e| TradeMessage::Refused {
                    code: e.error_code,
                    description: e.description,
                })
            }
            t if t == ProtoErrorRes::TYPE => {
                open::<ProtoErrorRes>(envelope).map(|e| TradeMessage::Refused {
                    code: e.error_code,
                    description: e.description,
                })
            }
            other => Err(CTraderError::Protocol(format!(
                "unexpected {}",
                payload_name(other)
            ))),
        };
        match message {
            Ok(message) => self.pass_trade(envelope, message),
            Err(e) => log::warn!("cTrader's answer to a trade request: {e}"),
        }
    }

    fn pass_trade(&self, envelope: &ProtoMessage, message: TradeMessage) {
        let Some(listener) = &self.trade_tx else {
            log::debug!(
                "cTrader {} dropped: no trade listener",
                payload_name(envelope.payload_type)
            );
            return;
        };

        // The receiver is gone once the client stopped listening
        let _ = listener.send(TradeEvent {
            client_msg_id: envelope.client_msg_id.clone(),
            message,
        });
    }

    /// A subscription's answer confirms the symbols it lists in `subscribedSymbolId`, which the
    /// demo filled with each symbol asked on 14 November 2026. Any other symbol, and a refused
    /// unsubscription's, is marked failed (kept for the next connection) and logged.
    fn spots_answered(&self, subscribe: bool, symbol_ids: &[i64], envelope: &ProtoMessage) {
        let outcome = if subscribe {
            open::<ProtoOaSubscribeSpotsRes>(envelope).map(|res| res.subscribed_symbol_id)
        } else {
            open::<ProtoOaUnsubscribeSpotsRes>(envelope).map(|_| symbol_ids.to_vec())
        };
        let action = if subscribe {
            "subscription"
        } else {
            "unsubscription"
        };
        match outcome {
            Ok(answered) => {
                let (done, missing): (Vec<i64>, Vec<i64>) =
                    symbol_ids.iter().partition(|id| answered.contains(id));
                for symbol_id in &done {
                    let topic = spot_topic(*symbol_id);
                    if subscribe {
                        self.subscriptions.confirm_subscribe(&topic);
                    } else {
                        self.subscriptions.confirm_unsubscribe(&topic);
                    }
                }
                for symbol_id in &missing {
                    self.subscriptions.mark_failure(&spot_topic(*symbol_id));
                }
                if !done.is_empty() {
                    log::debug!("cTrader spot {action} of symbols {done:?} done");
                }
                if !missing.is_empty() {
                    log::error!(
                        "cTrader answered the spot subscription of symbols {symbol_ids:?} \
                         without subscribing {missing:?}"
                    );
                }
            }
            Err(e) => {
                if subscribe {
                    for symbol_id in symbol_ids {
                        self.subscriptions.mark_failure(&spot_topic(*symbol_id));
                    }
                }
                log::error!("cTrader refused the spot {action} of symbols {symbol_ids:?}: {e}");
            }
        }
    }

    async fn account_ended(&mut self, name: &str) {
        if !matches!(self.session, Session::Ready) {
            log::info!("cTrader ended the previous account session ({name})");
            return;
        }
        log::warn!("cTrader ended the account session ({name}): authenticating it again");
        self.adopt_stored();
        self.authenticate_account().await;
    }

    /// Asks the transport to replace the connection (see `reconnected`), or fails the session if
    /// it takes no reconnect (closing).
    fn client_ended(&mut self, name: &str) {
        if self
            .client
            .as_ref()
            .is_some_and(WebSocketClient::request_reconnect)
        {
            log::warn!("cTrader ended the session ({name}): reconnecting");
            self.auth.invalidate();
            self.wake = None;
            self.session = Session::Unconnected;
        } else {
            self.fail_session(format!("cTrader ended the session ({name})"));
        }
    }

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
        if envelope.payload_type == ProtoOaExecutionEvent::TYPE {
            match open::<ProtoOaExecutionEvent>(envelope) {
                Ok(event) => self.pass_trade(envelope, TradeMessage::Execution(event)),
                Err(e) => log::warn!("cTrader execution event: {e}"),
            }
            return;
        }
        if envelope.payload_type == ProtoOaOrderErrorEvent::TYPE {
            match open::<ProtoOaOrderErrorEvent>(envelope) {
                Ok(event) => self.pass_trade(envelope, TradeMessage::OrderError(event)),
                Err(e) => log::warn!("cTrader order error event: {e}"),
            }
            return;
        }
        if envelope.payload_type == ProtoOaSpotEvent::TYPE {
            let payload = envelope.payload.as_deref().unwrap_or_default();
            match ProtoOaSpotEvent::decode(payload) {
                // The receiver is gone once the client stopped listening
                Ok(spot) => {
                    let _ = self.spot_tx.send(spot);
                }
                Err(e) => log::warn!("malformed cTrader spot event: {e}"),
            }
            return;
        }
        let name = payload_name(envelope.payload_type);
        if envelope.payload_type == ProtoOaClientDisconnectEvent::TYPE {
            self.client_ended(&name);
        } else {
            log::debug!("cTrader {name} not awaited");
        }
    }
}

fn wall_clock() -> Timestamp {
    Timestamp::from(get_atomic_clock_realtime().get_time_ns())
}

async fn sleep_until(at: Option<Instant>) {
    match at {
        Some(at) => time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

fn is_token_refused(error: &CTraderError) -> bool {
    matches!(error, CTraderError::Venue { code, .. } if TOKEN_REFUSED.contains(&code.as_str()))
}

fn renewal_failure(error: &CTraderError, names: &CTraderSettingNames) -> String {
    match error {
        CTraderError::Venue { code, .. } => format!(
            "cTrader refused to renew the access token ({code}): {}",
            playground(names)
        ),
        other => format!("renewing the access token: {other}"),
    }
}

/// A frame whose send failed with `e` counts as sent if its write may have begun, its outcome left
/// to cTrader's answer or reconciliation; else it is unsent.
///
/// A write that timed out (`WriteTimeout`) or failed (`BrokenPipe`) may have begun, and
/// `BrokenPipe` also stands for the writer gone, which is no proof that the frame was never sent.
fn failed_send(e: SendError) -> Result<(), SendError> {
    match e {
        SendError::WriteTimeout | SendError::BrokenPipe(_) => {
            log::warn!("cTrader frame write failed ({e}): cTrader may have it");
            Ok(())
        }
        SendError::ConnectionChanged
        | SendError::InvalidInput(_)
        | SendError::BufferFull
        | SendError::Closed
        | SendError::Timeout => Err(e),
    }
}

/// Shows only the code, so nothing cTrader echoes from the request reaches a log.
fn auth_failure(stage: &str, error: &CTraderError, names: &CTraderSettingNames) -> String {
    let CTraderError::Venue { code, .. } = error else {
        return format!("the {stage} authentication failed: {error}");
    };
    let hint = match code.as_str() {
        "CH_CLIENT_AUTH_FAILURE" | "CH_OA_CLIENT_NOT_FOUND" => {
            format!("check {} and {}", names.client_id, names.client_secret)
        }
        "CH_ACCESS_TOKEN_INVALID" | "OA_AUTH_TOKEN_EXPIRED" => {
            return format!(
                "cTrader refused the {stage} authentication ({code}) even with a renewed \
                 access token: {}",
                playground(names)
            );
        }
        "CH_CTID_TRADER_ACCOUNT_NOT_FOUND" | "ACCOUNT_NOT_AUTHORIZED" | "RET_NO_SUCH_LOGIN" => {
            format!(
                "check {}, and {}: an account authenticates only on its own environment",
                names.account_id, names.environment
            )
        }
        _ => format!("see {}", names.guide),
    };
    format!("cTrader refused the {stage} authentication ({code}): {hint}")
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    /// Nautilus' adapter guide leaves such an outcome unknown, never a rejection.
    #[rstest]
    #[case::broken_pipe(SendError::BrokenPipe("connection reset".to_owned()))]
    #[case::write_timeout(SendError::WriteTimeout)]
    fn a_frame_whose_write_failed_after_it_began_counts_as_sent(#[case] failed: SendError) {
        assert!(failed_send(failed).is_ok());
    }

    #[rstest]
    #[case::invalid_input(SendError::InvalidInput("empty".to_owned()))]
    #[case::buffer_full(SendError::BufferFull)]
    #[case::closed(SendError::Closed)]
    #[case::timeout(SendError::Timeout)]
    #[case::connection_changed(SendError::ConnectionChanged)]
    fn a_frame_refused_before_its_write_is_unsent(#[case] refused: SendError) {
        let unsent = failed_send(refused.clone());
        assert!(
            unsent
                .as_ref()
                .is_err_and(|e| std::mem::discriminant(e) == std::mem::discriminant(&refused)),
            "{refused:?}: {unsent:?}"
        );
    }
}
