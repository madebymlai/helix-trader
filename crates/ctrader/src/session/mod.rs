//! The cTrader session on the broker's account, which history, the data
//! client's live quotes and the execution client share: the instruments of
//! the broker's forex and metals and their tick history, merged into quotes
//! (`ticks`), the spot subscriptions, and the account, its open positions,
//! orders and deals, and its trade requests, over the connection
//! (`websocket`).

use std::collections::HashMap;

use anyhow::{Context, ensure};
use jiff::Timestamp;
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_live::SocketControl;
use nautilus_model::{
    data::QuoteTick,
    identifiers::{InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
};

use self::ticks::{SideTicks, Tick};
use super::{
    common::{
        account::CTraderAccount,
        messages::{
            self, CTraderError, ProtoOaAssetListReq, ProtoOaAssetListRes, ProtoOaDeal,
            ProtoOaDealListByPositionIdReq, ProtoOaDealListByPositionIdRes, ProtoOaDealListReq,
            ProtoOaDealListRes, ProtoOaDynamicLeverage, ProtoOaGetDynamicLeverageByIdReq,
            ProtoOaGetDynamicLeverageByIdRes, ProtoOaGetTickDataReq, ProtoOaGetTickDataRes,
            ProtoOaOrder, ProtoOaOrderDetailsReq, ProtoOaOrderDetailsRes, ProtoOaOrderListReq,
            ProtoOaOrderListRes, ProtoOaQuoteType, ProtoOaReconcileReq, ProtoOaReconcileRes,
            ProtoOaSymbolByIdReq, ProtoOaSymbolByIdRes, ProtoOaSymbolsListReq,
            ProtoOaSymbolsListRes, ProtoOaTraderReq, ProtoOaTraderRes, TradeRequest,
        },
        parse,
    },
    config::{CTraderAccountId, CTraderConnection, CTraderCredential, CTraderSettingNames},
    websocket::{CTraderWebSocketClient, HEARTBEAT_INTERVAL, SpotEvents, TradeEvents, TradeSent},
};

pub(crate) mod ticks;

/// A cTrader session on the broker's account: history, the spot
/// subscriptions of the data client, and the account, open positions and
/// deals of the execution client.
#[derive(Debug)]
pub struct CTraderSession {
    client: CTraderWebSocketClient,
    venue: Venue,
    account_id: CTraderAccountId,
    names: CTraderSettingNames,
}

impl CTraderConnection {
    /// Checks the account, on a session of its own closed again: hedging
    /// and the credential's (see `CTraderSession::check_account`). Run
    /// before a node is built, so a wrong account fails the start with its
    /// reason.
    ///
    /// # Errors
    ///
    /// Returns an error if cTrader cannot be reached or refuses the
    /// credential, or if the account does not suit (see
    /// `CTraderSession::check_account`).
    pub async fn check_account(&self) -> anyhow::Result<()> {
        let session = CTraderSession::connect(self).await?;
        let checked = session.check_account().await;
        session.disconnect().await;
        checked
    }
}

impl CTraderSession {
    /// Connects to the environment of `connection` and authenticates its
    /// credential, with the token file's pair (the credential's pair seeds
    /// it), renewed when due; none is ever printed.
    ///
    /// # Errors
    ///
    /// Returns an error if cTrader cannot be reached, or if it refuses the
    /// credential (naming its error code).
    pub async fn connect(connection: &CTraderConnection) -> anyhow::Result<Self> {
        let (venue, environment) = (connection.venue, connection.environment);
        let (session, _) = Self::connect_to(
            environment.url(),
            venue,
            connection.credential.clone(),
            HEARTBEAT_INTERVAL,
            None,
        )
        .await?;
        log::info!("connected to {venue} on cTrader's {environment:?} server");
        Ok(session)
    }

    /// Connects to `url` as `credential`, at the broker of `venue`, with
    /// the spot events of the symbols `subscribe_spots` subscribes; its
    /// socket under `socket`, a node's client's (see `websocket`).
    pub(crate) async fn connect_to(
        url: &str,
        venue: Venue,
        credential: CTraderCredential,
        heartbeat: std::time::Duration,
        socket: Option<SocketControl>,
    ) -> anyhow::Result<(Self, SpotEvents)> {
        let (account_id, names) = (credential.account_id, credential.names);
        let (client, spots) =
            CTraderWebSocketClient::connect(url, credential, heartbeat, socket).await?;
        let session = Self {
            client,
            venue,
            account_id,
            names,
        };
        Ok((session, spots))
    }

    /// The venue of the broker's instruments.
    pub(crate) fn venue(&self) -> Venue {
        self.venue
    }

    /// Checks the account: hedging and the credential's (see `account`).
    ///
    /// # Errors
    ///
    /// Returns an error if a request fails, or the account is another one,
    /// is not hedging or has a deposit asset Nautilus does not know.
    pub async fn check_account(&self) -> anyhow::Result<()> {
        self.account().await.map(|_| ())
    }

    /// The account, from the trader and the assets, checked (see
    /// `CTraderAccount::check`).
    pub(crate) async fn account(&self) -> anyhow::Result<CTraderAccount> {
        let (venue, account) = (self.venue, self.account_id.get());
        let (trader, assets) = tokio::try_join!(
            async {
                self.client
                    .request::<_, ProtoOaTraderRes>(&ProtoOaTraderReq {
                        ctid_trader_account_id: account,
                        ..Default::default()
                    })
                    .await
                    .with_context(|| format!("requesting the {venue} account"))
            },
            async {
                self.client
                    .request::<_, ProtoOaAssetListRes>(&ProtoOaAssetListReq {
                        ctid_trader_account_id: account,
                        ..Default::default()
                    })
                    .await
                    .with_context(|| format!("requesting the {venue} assets"))
            },
        )?;
        CTraderAccount::check(
            &trader.trader,
            venue,
            self.account_id,
            &self.names,
            &parse::assets_by_id(assets.asset),
        )
    }

    /// The open positions and pending orders, their stop loss and take
    /// profit in the positions' fields.
    pub(crate) async fn reconcile(&self) -> anyhow::Result<ProtoOaReconcileRes> {
        self.client
            .request(&ProtoOaReconcileReq {
                ctid_trader_account_id: self.account_id.get(),
                return_protection_orders: Some(false),
                ..Default::default()
            })
            .await
            .with_context(|| format!("requesting the {} open positions and orders", self.venue))
    }

    /// The deals created over `[from_ms, to_ms]`, each once, oldest first;
    /// none when the window is empty. cTrader's deal lists filter on a
    /// deal's creation (or its last update, the same on every deal
    /// measured), bounds included, which can come a millisecond before its
    /// execution (demo, 2026-11-12): a deal executed at `from_ms` may be
    /// left out. A window cTrader answers only in part (`hasMore`) is asked
    /// again in halves, which needs no knowledge of the order or size of
    /// its pages (neither is documented).
    ///
    /// # Errors
    ///
    /// Returns an error if a request fails, or a single millisecond holds
    /// more deals than a page.
    pub(crate) async fn deals(&self, from_ms: i64, to_ms: i64) -> anyhow::Result<Vec<ProtoOaDeal>> {
        // cTrader refuses a window not ending after its start
        // (`INCORRECT_BOUNDARIES`, demo, 2026-10-26); it holds no deal.
        if from_ms >= to_ms {
            return Ok(Vec::new());
        }
        let mut windows = vec![(from_ms, to_ms)];
        let mut deals: HashMap<i64, ProtoOaDeal> = HashMap::new();
        while let Some((from, to)) = windows.pop() {
            let page: ProtoOaDealListRes = self
                .client
                .request(&ProtoOaDealListReq {
                    ctid_trader_account_id: self.account_id.get(),
                    from_timestamp: Some(from),
                    to_timestamp: Some(to),
                    ..Default::default()
                })
                .await
                .with_context(|| {
                    format!("requesting the {} deals over [{from}, {to}] ms", self.venue)
                })?;
            if page.has_more {
                // The halves overlap by a millisecond: whether cTrader's
                // bounds are inclusive or not, no deal falls between them.
                ensure!(
                    to - from > 2,
                    "{} holds more deals over [{from}, {to}] ms than one answer carries",
                    self.venue
                );
                let middle = from + (to - from) / 2;
                windows.push((from, middle + 1));
                windows.push((middle, to));
            }
            deals.extend(page.deal.into_iter().map(|d| (d.deal_id, d)));
        }
        let mut deals: Vec<ProtoOaDeal> = deals.into_values().collect();
        deals.sort_by_key(|d| (d.execution_timestamp, d.deal_id));
        Ok(deals)
    }

    /// The deals of the account's position `position_id` created by
    /// `to_ms`, oldest first, as `deals` reads them; none if the account
    /// holds no such position (`POSITION_NOT_FOUND`, demo, 2026-11-12).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails, or the position holds more
    /// deals created by then than one answer carries.
    pub(crate) async fn position_deals(
        &self,
        position_id: i64,
        to_ms: i64,
    ) -> anyhow::Result<Vec<ProtoOaDeal>> {
        let asked = self
            .client
            .request(&ProtoOaDealListByPositionIdReq {
                ctid_trader_account_id: self.account_id.get(),
                position_id,
                to_timestamp: Some(to_ms),
                ..Default::default()
            })
            .await;
        let page: ProtoOaDealListByPositionIdRes = match asked {
            Ok(page) => page,
            Err(CTraderError::Venue { code, .. }) if code == "POSITION_NOT_FOUND" => {
                return Ok(Vec::new());
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!(
                        "requesting the {} deals of position {position_id} by {to_ms} ms",
                        self.venue
                    )
                });
            }
        };
        ensure!(
            !page.has_more,
            "{} holds more deals of position {position_id} by {to_ms} ms than one answer carries",
            self.venue
        );
        let mut deals = page.deal;
        deals.sort_by_key(|d| (d.execution_timestamp, d.deal_id));
        Ok(deals)
    }

    /// Sends `request`, a trade request of the account, under
    /// `client_msg_id`; its outcome comes as trade events under that id (see
    /// `trade_events`). The answer says whether it was sent.
    pub(crate) fn trade<Req: TradeRequest>(
        &self,
        mut request: Req,
        client_msg_id: String,
    ) -> TradeSent {
        request.set_account(self.account_id.get());
        self.client.trade(&request, client_msg_id)
    }

    /// The account's trade events from now on (see
    /// `CTraderWebSocketClient::trade_events`).
    ///
    /// # Errors
    ///
    /// Returns an error if the session is closed.
    pub(crate) fn trade_events(&self) -> anyhow::Result<TradeEvents> {
        Ok(self.client.trade_events()?)
    }

    /// The order `order_id` and the deals that filled it, open or not.
    ///
    /// # Errors
    ///
    /// Returns cTrader's error for an order it does not hold
    /// (`ORDER_NOT_FOUND`), or an error if the request fails.
    pub(crate) async fn order_details(
        &self,
        order_id: i64,
    ) -> Result<ProtoOaOrderDetailsRes, messages::CTraderError> {
        self.client
            .request(&ProtoOaOrderDetailsReq {
                ctid_trader_account_id: self.account_id.get(),
                order_id,
                ..Default::default()
            })
            .await
    }

    /// The orders updated over `[from_ms, to_ms]`, open or not, each once,
    /// as `deals` reads deals: a window answered only in part is asked
    /// again in halves.
    ///
    /// # Errors
    ///
    /// Returns an error if a request fails, or a single millisecond holds
    /// more orders than a page.
    pub(crate) async fn orders(
        &self,
        from_ms: i64,
        to_ms: i64,
    ) -> anyhow::Result<Vec<ProtoOaOrder>> {
        if from_ms >= to_ms {
            return Ok(Vec::new());
        }
        let mut windows = vec![(from_ms, to_ms)];
        let mut orders: HashMap<i64, ProtoOaOrder> = HashMap::new();
        while let Some((from, to)) = windows.pop() {
            let page: ProtoOaOrderListRes = self
                .client
                .request(&ProtoOaOrderListReq {
                    ctid_trader_account_id: self.account_id.get(),
                    from_timestamp: Some(from),
                    to_timestamp: Some(to),
                    ..Default::default()
                })
                .await
                .with_context(|| {
                    format!(
                        "requesting the {} orders over [{from}, {to}] ms",
                        self.venue
                    )
                })?;
            if page.has_more {
                ensure!(
                    to - from > 2,
                    "{} holds more orders over [{from}, {to}] ms than one answer carries",
                    self.venue
                );
                let middle = from + (to - from) / 2;
                windows.push((from, middle + 1));
                windows.push((middle, to));
            }
            orders.extend(page.order.into_iter().map(|o| (o.order_id, o)));
        }
        let mut orders: Vec<ProtoOaOrder> = orders.into_values().collect();
        orders.sort_by_key(|o| o.order_id);
        Ok(orders)
    }

    /// The instruments of `ids`, from cTrader's current definitions: the
    /// assets, the symbol list (each id's symbol name to its `symbolId` on
    /// this server), the details of those symbols, and the leverage the
    /// broker applies to each, from the account's and the symbol's dynamic
    /// leverage, which sets its margins (see `parse`).
    ///
    /// # Errors
    ///
    /// Returns an error if an id is not on the session's venue (before any
    /// request), if a request fails, if the broker lists no symbol of that
    /// name, or if a symbol's assets are not both currencies Nautilus knows.
    pub async fn instruments(&self, ids: &[InstrumentId]) -> anyhow::Result<Vec<InstrumentAny>> {
        let symbols = self.symbols(ids).await?;
        Ok(symbols.into_iter().map(|s| s.instrument).collect())
    }

    /// The symbols of `ids`, as `instruments`.
    pub(crate) async fn symbols(&self, ids: &[InstrumentId]) -> anyhow::Result<Vec<CTraderSymbol>> {
        let venue = self.venue;
        for &id in ids {
            parse::ensure_on(id, venue)?;
        }
        let account = self.account_id.get();
        let assets: ProtoOaAssetListRes = self
            .client
            .request(&ProtoOaAssetListReq {
                ctid_trader_account_id: account,
                ..Default::default()
            })
            .await
            .with_context(|| format!("requesting the {venue} assets"))?;
        let list: ProtoOaSymbolsListRes = self
            .client
            .request(&ProtoOaSymbolsListReq {
                ctid_trader_account_id: account,
                ..Default::default()
            })
            .await
            .with_context(|| format!("requesting the {venue} symbols"))?;
        let listed: HashMap<&str, _> = list
            .symbol
            .iter()
            .filter_map(|s| Some((s.symbol_name.as_deref()?, s)))
            .collect();
        let light = ids
            .iter()
            .map(|&id| {
                listed
                    .get(parse::symbol_name(id))
                    .copied()
                    .with_context(|| {
                        format!(
                            "{venue} lists no symbol {}: an instrument id on {venue} is the \
                             symbol's name in cTrader, such as XAUUSD.{venue}",
                            parse::symbol_name(id)
                        )
                    })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let details: ProtoOaSymbolByIdRes = self
            .client
            .request(&ProtoOaSymbolByIdReq {
                ctid_trader_account_id: account,
                symbol_id: light.iter().map(|s| s.symbol_id).collect(),
                ..Default::default()
            })
            .await
            .with_context(|| format!("requesting the details of the {venue} symbols"))?;
        let details: HashMap<i64, _> = details.symbol.iter().map(|s| (s.symbol_id, s)).collect();
        let account_leverage = self
            .client
            .request::<_, ProtoOaTraderRes>(&ProtoOaTraderReq {
                ctid_trader_account_id: account,
                ..Default::default()
            })
            .await
            .with_context(|| format!("requesting the {venue} account"))?
            .trader
            .leverage_in_cents;
        let mut leverages: HashMap<i64, ProtoOaDynamicLeverage> = HashMap::new();
        for leverage_id in light
            .iter()
            .filter_map(|s| details.get(&s.symbol_id)?.leverage_id)
        {
            if leverages.contains_key(&leverage_id) {
                continue;
            }
            let answer: ProtoOaGetDynamicLeverageByIdRes = self
                .client
                .request(&ProtoOaGetDynamicLeverageByIdReq {
                    ctid_trader_account_id: account,
                    leverage_id,
                    ..Default::default()
                })
                .await
                .with_context(|| format!("requesting the {venue} leverage {leverage_id}"))?;
            leverages.insert(leverage_id, answer.leverage);
        }
        let assets = parse::assets_by_id(assets.asset);
        let ts_init = get_atomic_clock_realtime().get_time_ns();
        light
            .into_iter()
            .map(|light| {
                let name = light.symbol_name.as_deref().unwrap_or_default();
                let symbol = details
                    .get(&light.symbol_id)
                    .with_context(|| format!("{venue} returned no details of {name}"))?;
                let symbol_leverage = symbol.leverage_id.and_then(|id| leverages.get(&id));
                let margin = parse::margin(name, venue, account_leverage, symbol_leverage)?;
                let cfd = parse::cfd(light, symbol, venue, &assets, margin, ts_init)?;
                Ok(CTraderSymbol {
                    instrument: InstrumentAny::Cfd(cfd),
                    symbol_id: light.symbol_id,
                })
            })
            .collect()
    }

    /// The quotes of `symbol` stamped in `[from, to]`, merged from its BID
    /// and ASK ticks of those milliseconds and the last tick of each before
    /// them (see `ticks::merge`), the two sides fetched side by side. A
    /// failed tick request cuts its side short: the quotes then start where
    /// both sides' ticks do, with the failure. Logs the counts.
    ///
    /// # Errors
    ///
    /// Returns an error if the window is out of range, or as `ticks::merge`.
    pub(crate) async fn quote_ticks(
        &self,
        symbol: &CTraderSymbol,
        from: Timestamp,
        to: Timestamp,
    ) -> anyhow::Result<FetchedQuotes> {
        // [from, to] in ns holds the ticks of [from_ms, to_ms) in ms.
        let ms = |ns: i128| i64::try_from(ns.div_euclid(1_000_000)).context("a time out of range");
        let from_ms = ms(from.as_nanosecond() + 999_999)?;
        let to_ms = ms(to.as_nanosecond())? + 1;
        if from_ms >= to_ms {
            return Ok(FetchedQuotes::default());
        }
        let ((bid, bid_failure), (ask, ask_failure)) = tokio::join!(
            self.side_ticks(symbol, ProtoOaQuoteType::Bid, from_ms, to_ms),
            self.side_ticks(symbol, ProtoOaQuoteType::Ask, from_ms, to_ms),
        );
        let merged = ticks::merge(&symbol.instrument, &bid, &ask)?;
        let id = symbol.instrument.id();
        log::info!(
            "{id}: {} quotes over [{from}, {to}] from {} BID and {} ASK ticks; {} ticks before \
             both sides were known, {} crossed quotes (ask below bid) dropped",
            merged.quotes.len(),
            bid.ticks.len(),
            ask.ticks.len(),
            merged.one_sided,
            merged.crossed,
        );
        let failure = bid_failure.or(ask_failure);
        if let Some(e) = &failure {
            log::warn!(
                "{id}: the quotes start at {} ms, not {from_ms} ms: {e:#}",
                bid.from_ms.max(ask.from_ms)
            );
        }
        Ok(FetchedQuotes {
            quotes: merged.quotes,
            failure,
        })
    }

    /// The `side` ticks of `symbol` over `[from_ms, to_ms)` and its last
    /// tick before them, and the failure that cut them short, if any.
    async fn side_ticks(
        &self,
        symbol: &CTraderSymbol,
        side: ProtoOaQuoteType,
        from_ms: i64,
        to_ms: i64,
    ) -> (SideTicks, Option<anyhow::Error>) {
        // A closure returning the future, not an async closure: the latter
        // makes the future of a quote request not `Send`.
        let page = move |from, to| self.tick_page(symbol, side, from, to);
        let before = match ticks::last_before(from_ms, page).await {
            Ok(before) => before,
            Err(e) => {
                let nothing = SideTicks {
                    before: None,
                    ticks: Vec::new(),
                    from_ms: to_ms,
                };
                return (nothing, Some(e));
            }
        };
        let pages = ticks::side_ticks(from_ms, to_ms, page).await;
        let side = SideTicks {
            // Known only at the window's start.
            before: before.filter(|_| pages.from_ms == from_ms),
            ticks: pages.ticks,
            from_ms: pages.from_ms,
        };
        (side, pages.failure)
    }

    /// One tick request: the newest page of `side` ticks of `symbol` over
    /// `[from_ms, to_ms)`, newest first, and whether there are more.
    async fn tick_page(
        &self,
        symbol: &CTraderSymbol,
        side: ProtoOaQuoteType,
        from_ms: i64,
        to_ms: i64,
    ) -> anyhow::Result<(Vec<Tick>, bool)> {
        let response: ProtoOaGetTickDataRes = self
            .client
            .request(&ProtoOaGetTickDataReq {
                ctid_trader_account_id: self.account_id.get(),
                symbol_id: symbol.symbol_id,
                r#type: side as i32,
                from_timestamp: Some(from_ms),
                to_timestamp: Some(to_ms),
                ..Default::default()
            })
            .await
            .with_context(|| {
                format!(
                    "requesting the {} ticks of {} over [{from_ms}, {to_ms}) ms",
                    side.as_str_name(),
                    symbol.instrument.id()
                )
            })?;
        Ok((ticks::decode(&response.tick_data)?, response.has_more))
    }

    /// Subscribes to the spot events of `symbol_id` for the rest of the
    /// session (see `CTraderWebSocketClient::subscribe_spots`).
    pub(crate) fn subscribe_spots(&self, symbol_id: i64) -> anyhow::Result<()> {
        Ok(self.client.subscribe_spots(symbol_id)?)
    }

    /// Ends the spot subscription of `symbol_id`.
    pub(crate) fn unsubscribe_spots(&self, symbol_id: i64) -> anyhow::Result<()> {
        Ok(self.client.unsubscribe_spots(symbol_id)?)
    }

    /// Renews the access token now, as on schedule: the token file holds
    /// the new pair once cTrader answers.
    #[cfg(any(test, feature = "test-support"))]
    pub fn renew_token(&self) {
        self.client.renew_token();
    }

    /// Connects as `connect` does and subscribes to the spot events of
    /// `instrument_ids`, which come as cTrader sends them.
    ///
    /// # Errors
    ///
    /// As `connect`, and as `instruments` for `instrument_ids`.
    #[cfg(feature = "test-support")]
    pub async fn connect_with_spots(
        connection: &CTraderConnection,
        instrument_ids: &[InstrumentId],
    ) -> anyhow::Result<(Self, SpotEvents)> {
        Self::connect_with_spots_to(connection.environment.url(), connection, instrument_ids).await
    }

    /// As `connect_with_spots`, to `url`.
    #[cfg(any(test, feature = "test-support"))]
    async fn connect_with_spots_to(
        url: &str,
        connection: &CTraderConnection,
        instrument_ids: &[InstrumentId],
    ) -> anyhow::Result<(Self, SpotEvents)> {
        let (session, spots) = Self::connect_to(
            url,
            connection.venue,
            connection.credential.clone(),
            HEARTBEAT_INTERVAL,
            None,
        )
        .await?;
        for symbol in session.symbols(instrument_ids).await? {
            session.subscribe_spots(symbol.symbol_id)?;
        }
        Ok((session, spots))
    }

    /// Asks the session to close, as `disconnect` does, without waiting:
    /// a token renewal out is still answered and saved, if `disconnect`
    /// then awaits it.
    pub(crate) fn begin_shutdown(&self) {
        self.client.begin_shutdown();
    }

    /// Closes the session.
    pub async fn disconnect(self) {
        self.client.disconnect().await;
    }
}

/// The quotes `CTraderSession::quote_ticks` got over a window, and the
/// failure that cut them short, if any: then they start later than the
/// window.
#[derive(Debug, Default)]
pub(crate) struct FetchedQuotes {
    pub(crate) quotes: Vec<QuoteTick>,
    pub(crate) failure: Option<anyhow::Error>,
}

/// An instrument of the broker and its cTrader `symbolId` on this server.
#[derive(Debug, Clone)]
pub(crate) struct CTraderSymbol {
    pub(crate) instrument: InstrumentAny,
    pub(crate) symbol_id: i64,
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use jiff::SignedDuration;
    use nautilus_common::{
        cache::Cache,
        clock::{Clock, VirtualClock},
        messages::execution::{SubmitOrder, TradingCommand},
        msgbus::{self, MessagingSwitchboard, stubs::get_typed_into_message_saving_handler},
    };
    use nautilus_core::{UUID4, UnixNanos};
    use nautilus_model::{
        accounts::{AccountAny, MarginAccount},
        enums::{AccountType, OrderSide, OrderType},
        events::{AccountState, OrderEventAny},
        identifiers::AccountId,
        instruments::{Cfd, Instrument},
        orders::{Order, OrderTestBuilder},
        types::{AccountBalance, Currency, Money, Price, Quantity},
    };
    use nautilus_portfolio::Portfolio;
    use nautilus_risk::engine::{RiskEngine, config::RiskEngineConfig};
    use prost::Message as _;
    use rust_decimal::Decimal;

    use super::{
        messages::{
            Payload, ProtoMessage, ProtoOaRefreshTokenReq, ProtoOaSpotEvent,
            ProtoOaSubscribeSpotsReq, ProtoOaSubscribeSpotsRes,
        },
        ticks::Tick,
        *,
    };
    use crate::testing::{
        self, ACCOUNT_ID, ASSET_LIST_RES, Action, Received, SPOT_EVENT_EURUSD_FIRST,
        SPOT_EVENT_XAUUSD_FIRST, SUBSCRIBE_SPOTS_RES, SYMBOL_BY_ID_RES, SYMBOLS_LIST_RES,
        TRADER_RES, VENUE, accept_auth, replay, reply, tick_answer,
    };

    /// A cTrader serving the captured assets, symbols and details.
    fn fixtures_peer(connection: usize, request: &ProtoMessage) -> Vec<Action> {
        ticks_peer(vec![], vec![], 10_000)(connection, request)
    }

    /// As `fixtures_peer`, with the BID and ASK ticks of every symbol,
    /// oldest first, served in pages of `page_size` (see `tick_answer`).
    fn ticks_peer(
        bids: Vec<Tick>,
        asks: Vec<Tick>,
        page_size: usize,
    ) -> impl FnMut(usize, &ProtoMessage) -> Vec<Action> + Send + 'static {
        move |_, request| peer_answer(request, &bids, &asks, page_size)
    }

    fn peer_answer(
        request: &ProtoMessage,
        bids: &[Tick],
        asks: &[Tick],
        page_size: usize,
    ) -> Vec<Action> {
        if let Some(actions) = accept_auth(request) {
            return actions;
        }
        let response = match request.payload_type {
            t if t == ProtoOaGetTickDataReq::TYPE => {
                let asked =
                    ProtoOaGetTickDataReq::decode(request.payload.as_deref().unwrap()).unwrap();
                reply(request, &tick_answer(&asked, bids, asks, page_size))
            }
            t if t == ProtoOaAssetListReq::TYPE => {
                replay::<ProtoOaAssetListRes>(request, ASSET_LIST_RES)
            }
            t if t == ProtoOaSymbolsListReq::TYPE => {
                replay::<ProtoOaSymbolsListRes>(request, SYMBOLS_LIST_RES)
            }
            t if t == ProtoOaSymbolByIdReq::TYPE => {
                replay::<ProtoOaSymbolByIdRes>(request, SYMBOL_BY_ID_RES)
            }
            t if t == ProtoOaTraderReq::TYPE => replay::<ProtoOaTraderRes>(request, TRADER_RES),
            t if t == ProtoOaGetDynamicLeverageByIdReq::TYPE => {
                testing::dynamic_leverage_answer(request)
            }
            other => panic!("unexpected request {other}"),
        };
        vec![response]
    }

    async fn connected() -> (CTraderSession, Received) {
        connected_to(fixtures_peer).await
    }

    async fn connected_to(
        peer: impl FnMut(usize, &ProtoMessage) -> Vec<Action> + Send + 'static,
    ) -> (CTraderSession, Received) {
        let (url, received) = testing::start(peer);
        let (credential, _tokens) = testing::credential();
        let (session, _) = CTraderSession::connect_to(
            &url,
            Venue::new(VENUE),
            credential,
            HEARTBEAT_INTERVAL,
            None,
        )
        .await
        .unwrap();
        (session, received)
    }

    /// The payload types received after the two auth requests.
    fn requests(received: &Received) -> Vec<ProtoMessage> {
        received
            .lock()
            .unwrap()
            .iter()
            .skip(2)
            .map(|(_, f)| f.clone())
            .collect()
    }

    #[tokio::test]
    async fn instruments_come_from_the_assets_the_symbol_list_and_the_details() {
        let (session, received) = connected().await;
        let ids = [
            InstrumentId::from("XAUUSD.ACMEFX"),
            InstrumentId::from("EURUSD.ACMEFX"),
        ];

        let instruments = session.instruments(&ids).await.unwrap();

        let [InstrumentAny::Cfd(xau), InstrumentAny::Cfd(eur)] = instruments.as_slice() else {
            panic!("{instruments:?}");
        };
        assert_eq!((xau.id(), eur.id()), (ids[0], ids[1]));
        assert_eq!(xau.base_currency, Some(Currency::from("XAU")));
        assert_eq!(xau.lot_size, Some(Quantity::from(100)));
        assert_eq!(eur.price_precision, 5);
        let requests = requests(&received);
        let mut types: Vec<u32> = requests.iter().map(|f| f.payload_type).collect();
        types.sort_unstable();
        assert_eq!(types, [2112, 2114, 2116, 2121, 2177, 2177]);
        let details = requests
            .iter()
            .find(|f| f.payload_type == ProtoOaSymbolByIdReq::TYPE)
            .unwrap();
        let details = ProtoOaSymbolByIdReq::decode(details.payload.as_deref().unwrap()).unwrap();
        assert_eq!(details.ctid_trader_account_id, ACCOUNT_ID);
        assert_eq!(details.symbol_id, [41, 1]);
        let mut leverages: Vec<(i64, i64)> = requests
            .iter()
            .filter(|f| f.payload_type == ProtoOaGetDynamicLeverageByIdReq::TYPE)
            .map(|f| {
                let asked = ProtoOaGetDynamicLeverageByIdReq::decode(f.payload.as_deref().unwrap())
                    .unwrap();
                (asked.ctid_trader_account_id, asked.leverage_id)
            })
            .collect();
        leverages.sort_unstable();
        assert_eq!(leverages, [(ACCOUNT_ID, 52_531), (ACCOUNT_ID, 52_543)]);
        session.disconnect().await;
    }

    /// The demo's leverages: the account 1:30, gold 1:200 and EUR/USD
    /// 1:1000; the broker applies the account's to both.
    #[tokio::test]
    async fn instruments_carry_the_broker_s_margin_at_the_account_s_leverage() {
        let (session, _) = connected().await;
        let ids = [
            InstrumentId::from("XAUUSD.ACMEFX"),
            InstrumentId::from("EURUSD.ACMEFX"),
        ];

        let instruments = session.instruments(&ids).await.unwrap();

        let [gold, euro_dollar] = instruments.as_slice() else {
            panic!("{instruments:?}");
        };
        let at_1_30 = Decimal::ONE / Decimal::from(30);
        for instrument in [gold, euro_dollar] {
            assert_eq!(instrument.margin_init(), at_1_30);
            assert_eq!(instrument.margin_maint(), at_1_30);
        }
        session.disconnect().await;
    }

    /// A cTrader serving the fixtures and `trader` as the account.
    fn trader_peer(
        trader: ProtoOaTraderRes,
    ) -> impl FnMut(usize, &ProtoMessage) -> Vec<Action> + Send + 'static {
        move |connection, request| {
            if request.payload_type == ProtoOaTraderReq::TYPE {
                return vec![reply(request, &trader)];
            }
            fixtures_peer(connection, request)
        }
    }

    /// The captured trader: the demo account, hedged, in EUR.
    fn captured_trader() -> ProtoOaTraderRes {
        ProtoOaTraderRes::decode(TRADER_RES).unwrap()
    }

    #[tokio::test]
    async fn the_captured_hedged_account_passes_the_check() {
        let (session, received) = connected_to(trader_peer(captured_trader())).await;

        session.check_account().await.unwrap();

        let asked = requests(&received)
            .into_iter()
            .find(|f| f.payload_type == ProtoOaTraderReq::TYPE)
            .unwrap();
        let asked = ProtoOaTraderReq::decode(asked.payload.as_deref().unwrap()).unwrap();
        assert_eq!(asked.ctid_trader_account_id, ACCOUNT_ID);
        session.disconnect().await;
    }

    /// Synthetic: the captured trader made a netting account.
    #[tokio::test]
    async fn a_netting_account_is_refused_naming_its_type() {
        let mut trader = captured_trader();
        trader
            .trader
            .set_account_type(messages::ProtoOaAccountType::Netted);
        let (session, _) = connected_to(trader_peer(trader)).await;

        let err = session.check_account().await.unwrap_err().to_string();

        assert!(err.contains("NETTED, not HEDGED"), "{err}");
        session.disconnect().await;
    }

    /// Synthetic: the captured trader of another account.
    #[tokio::test]
    async fn the_trader_of_another_account_is_refused() {
        let mut trader = captured_trader();
        trader.trader.ctid_trader_account_id = ACCOUNT_ID + 1;
        let (session, _) = connected_to(trader_peer(trader)).await;

        let err = session.check_account().await.unwrap_err().to_string();

        assert!(
            err.contains("another account than ACME_ACCOUNT_ID"),
            "{err}"
        );
        session.disconnect().await;
    }

    #[tokio::test]
    async fn a_symbol_the_broker_does_not_list_is_named() {
        let (session, _) = connected().await;

        let err = session
            .instruments(&[InstrumentId::from("GBPUSD.ACMEFX")])
            .await
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "ACMEFX lists no symbol GBPUSD: an instrument id on ACMEFX is the symbol's name in \
             cTrader, such as XAUUSD.ACMEFX"
        );
        session.disconnect().await;
    }

    #[tokio::test]
    async fn an_id_of_another_venue_is_refused_before_any_request() {
        let (session, received) = connected().await;

        let err = session
            .instruments(&[InstrumentId::from("EUR/USD.IDEALPRO")])
            .await
            .unwrap_err();

        assert_eq!(err.to_string(), "EUR/USD.IDEALPRO is not on ACMEFX");
        assert_eq!(requests(&received), []);
        session.disconnect().await;
    }

    /// As `fixtures_peer`, answering a spot subscription with the captured
    /// answer, then each symbol's captured first spot event.
    fn spots_peer(connection: usize, request: &ProtoMessage) -> Vec<Action> {
        if request.payload_type != ProtoOaSubscribeSpotsReq::TYPE {
            return fixtures_peer(connection, request);
        }
        let asked = ProtoOaSubscribeSpotsReq::decode(request.payload.as_deref().unwrap()).unwrap();
        let mut actions = vec![replay::<ProtoOaSubscribeSpotsRes>(
            request,
            SUBSCRIBE_SPOTS_RES,
        )];
        actions.extend(asked.symbol_id.iter().map(|&symbol_id| {
            let captured = match symbol_id {
                41 => SPOT_EVENT_XAUUSD_FIRST,
                1 => SPOT_EVENT_EURUSD_FIRST,
                other => panic!("no spot event of symbol {other} captured"),
            };
            Action::Send(
                ProtoMessage {
                    payload_type: ProtoOaSpotEvent::TYPE,
                    payload: Some(captured.to_vec()),
                    client_msg_id: None,
                }
                .encode_to_vec(),
            )
        }));
        actions
    }

    #[tokio::test]
    async fn a_session_connected_with_spots_gets_its_instruments_spot_events_as_sent() {
        let (url, _) = testing::start(spots_peer);
        let (connection, _tokens) = testing::connection();
        let ids = [
            InstrumentId::from("XAUUSD.ACMEFX"),
            InstrumentId::from("EURUSD.ACMEFX"),
        ];

        let (session, mut spots) = CTraderSession::connect_with_spots_to(&url, &connection, &ids)
            .await
            .unwrap();

        let mut stamped = Vec::new();
        for _ in &ids {
            let spot = tokio::time::timeout(std::time::Duration::from_secs(10), spots.recv())
                .await
                .expect("no spot event in time")
                .unwrap();
            stamped.push((spot.symbol_id, spot.timestamp));
        }
        stamped.sort_unstable();
        // The symbol ids and times of `testdata/spot_event_*_first.txtpb`.
        assert_eq!(
            stamped,
            [(1, Some(1_793_017_476_361)), (41, Some(1_793_017_475_790))]
        );
        session.disconnect().await;
    }

    /// The renewal goes out on `renew_token`; `disconnect` awaits its
    /// answer and saves the pair.
    #[tokio::test]
    async fn a_forced_renewal_leaves_the_renewed_pair_in_the_token_file() {
        let (url, _) = testing::start(|connection, request| {
            if request.payload_type == ProtoOaRefreshTokenReq::TYPE {
                return vec![testing::renewal_answer(request)];
            }
            fixtures_peer(connection, request)
        });
        let (credential, _tokens) = testing::credential();
        let (session, _) = CTraderSession::connect_to(
            &url,
            Venue::new(VENUE),
            credential.clone(),
            HEARTBEAT_INTERVAL,
            None,
        )
        .await
        .unwrap();
        let before = credential.stored_tokens().unwrap();

        let asked_at = Timestamp::now();
        session.renew_token();
        session.disconnect().await;
        let answered_by = Timestamp::now();

        let renewed = credential.stored_tokens().unwrap();
        assert_eq!(before.access_token.expose_secret(), testing::ACCESS_TOKEN);
        assert_eq!(renewed.access_token.expose_secret(), "renewedAccess1");
        assert_eq!(renewed.refresh_token.expose_secret(), "renewedRefresh1");
        let lifetime = SignedDuration::from_secs(testing::EXPIRES_IN);
        let expires_at = renewed.expires_at.unwrap();
        assert!(
            asked_at + lifetime <= expires_at && expires_at <= answered_by + lifetime,
            "{expires_at}"
        );
    }

    /// What Nautilus' risk engine makes of a market buy of 1 oz of gold,
    /// `xau` here, on a margin account of 1,000 EUR, at the demo's first
    /// captured spots (`testdata/spot_event_*_first`): XAUUSD 4017.96 /
    /// 4018.01, EURUSD 1.11964 / 1.11965 (the account is in EUR, gold in
    /// USD). The order's value, about 3,590 EUR, is above the balance; the
    /// broker's margin at 1:30, about 120 EUR, is below it. The order events
    /// the engine sends back, and whether it passed the order on.
    fn risk_check_of_one_ounce(xau: Cfd, eur: InstrumentAny) -> (Vec<OrderEventAny>, usize) {
        let (events, saved_events) = get_typed_into_message_saving_handler::<OrderEventAny>(None);
        msgbus::register_order_event_endpoint(MessagingSwitchboard::exec_engine_process(), events);
        let (commands, saved_commands) =
            get_typed_into_message_saving_handler::<TradingCommand>(None);
        msgbus::register_trading_command_endpoint(
            MessagingSwitchboard::exec_engine_queue_execute(),
            commands,
        );
        let gold = xau.id;
        let mut cache = Cache::default();
        let quote = |id: InstrumentId, bid: &str, ask: &str| {
            QuoteTick::new(
                id,
                Price::from(bid),
                Price::from(ask),
                Quantity::from(10_000),
                Quantity::from(10_000),
                UnixNanos::default(),
                UnixNanos::default(),
            )
        };
        cache
            .add_quote(quote(xau.id, "4017.96", "4018.01"))
            .unwrap();
        cache
            .add_quote(quote(eur.id(), "1.11964", "1.11965"))
            .unwrap();
        cache.add_instrument(InstrumentAny::Cfd(xau)).unwrap();
        cache.add_instrument(eur).unwrap();
        let account = AccountState::new(
            AccountId::from("ACMEFX-DEMO"),
            AccountType::Margin,
            vec![AccountBalance::new(
                Money::from("1000 EUR"),
                Money::from("0 EUR"),
                Money::from("1000 EUR"),
            )],
            vec![],
            true,
            UUID4::new(),
            UnixNanos::default(),
            UnixNanos::default(),
            Some(Currency::EUR()),
        );
        cache
            .add_account(AccountAny::Margin(MarginAccount::new(account, true)))
            .unwrap();
        let order = OrderTestBuilder::new(OrderType::Market)
            .instrument_id(gold)
            .side(OrderSide::Buy)
            .quantity(Quantity::from(1))
            .build();
        cache.add_order(order.clone(), None, None, false).unwrap();
        let cache = Rc::new(RefCell::new(cache));
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        let portfolio = Portfolio::new(
            Rc::clone(&clock) as Rc<RefCell<dyn Clock>>,
            Rc::clone(&cache),
            None,
        );
        let mut engine = RiskEngine::new(RiskEngineConfig::default(), portfolio, clock, cache);

        engine.execute(TradingCommand::SubmitOrder(SubmitOrder::from_order(
            &order,
            order.trader_id(),
            None,
            None,
            UUID4::new(),
            UnixNanos::default(),
        )));

        (
            saved_events.get_messages(),
            saved_commands.get_messages().len(),
        )
    }

    /// XAUUSD and EURUSD as the live clients load them from the captured
    /// details and leverages.
    async fn gold_and_euro_dollar() -> (Cfd, InstrumentAny) {
        let (session, _) = connected().await;
        let mut instruments = session
            .instruments(&[
                InstrumentId::from("XAUUSD.ACMEFX"),
                InstrumentId::from("EURUSD.ACMEFX"),
            ])
            .await
            .unwrap();
        session.disconnect().await;
        let eur = instruments.pop().unwrap();
        let Some(InstrumentAny::Cfd(xau)) = instruments.pop() else {
            panic!("no gold")
        };
        (xau, eur)
    }

    #[tokio::test]
    async fn an_ounce_of_gold_worth_more_than_the_balance_passes_on_the_broker_s_margin() {
        let (xau, eur) = gold_and_euro_dollar().await;

        let (events, sent) = risk_check_of_one_ounce(xau, eur);

        assert!(events.is_empty(), "{events:?}");
        assert_eq!(sent, 1);
    }

    /// The same order on gold carrying margins of 1, as before the broker's
    /// margin: denied, so the check above turns on the margin.
    #[tokio::test]
    async fn an_ounce_of_gold_at_margins_of_one_is_denied() {
        let (mut xau, eur) = gold_and_euro_dollar().await;
        xau.margin_init = Decimal::ONE;
        xau.margin_maint = Decimal::ONE;

        let (events, sent) = risk_check_of_one_ounce(xau, eur);

        let [OrderEventAny::Denied(denied)] = events.as_slice() else {
            panic!("{events:?}")
        };
        assert!(
            denied
                .reason
                .starts_with("INITIAL_MARGIN_EXCEEDS_FREE_BALANCE"),
            "{denied:?}"
        );
        assert_eq!(sent, 0);
    }
}
