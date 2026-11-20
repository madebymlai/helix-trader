//! The cTrader session on the broker's account. History, the data client and the execution client
//! share it.
//!
//! It serves these over `websocket`:
//! - Instruments.
//! - Tick history, merged into quotes (`ticks`).
//! - Spot subscriptions.
//! - The account's positions, orders and deals.
//! - Trade requests.

use std::collections::HashMap;

use anyhow::Context;
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
    websocket::{CTraderWebSocketClient, SpotEvents, TradeEvents, TradeSent},
};

pub(crate) mod ticks;

/// A cTrader session on the broker's account.
#[derive(Debug)]
pub struct CTraderSession {
    client: CTraderWebSocketClient,
    venue: Venue,
    account_id: CTraderAccountId,
    names: CTraderSettingNames,
}

impl CTraderConnection {
    /// Checks the account on its own session (see `CTraderSession::check_account`).
    ///
    /// A wrong account then fails the start before a node is built.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Reaching cTrader fails.
    /// - It refuses the credential.
    /// - The account does not suit (see `CTraderSession::check_account`).
    pub async fn check_account(&self) -> anyhow::Result<()> {
        let session = CTraderSession::connect(self).await?;
        let checked = session.check_account().await;
        session.disconnect().await;
        checked
    }
}

impl CTraderSession {
    /// Connects to the environment of `connection` and authenticates its credential.
    ///
    /// The session uses the token pair from the token file. The credential's pair seeds that file.
    /// The pair is renewed when due.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Reaching cTrader fails.
    /// - It refuses the credential, naming its error code.
    pub async fn connect(connection: &CTraderConnection) -> anyhow::Result<Self> {
        let (venue, environment) = (connection.venue, connection.environment);
        let (session, _) = Self::connect_to(
            environment.url(),
            venue,
            connection.credential.clone(),
            None,
        )
        .await?;
        log::info!("connected to {venue} on cTrader's {environment:?} server");
        Ok(session)
    }

    pub(crate) async fn connect_to(
        url: &str,
        venue: Venue,
        credential: CTraderCredential,
        socket: Option<SocketControl>,
    ) -> anyhow::Result<(Self, SpotEvents)> {
        let (account_id, names) = (credential.account_id, credential.names);
        let (client, spots) = CTraderWebSocketClient::connect(url, credential, socket).await?;
        let session = Self {
            client,
            venue,
            account_id,
            names,
        };
        Ok((session, spots))
    }

    pub(crate) fn venue(&self) -> Venue {
        self.venue
    }

    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A request fails.
    /// - The account is another one.
    /// - The account is not hedging.
    /// - Its deposit asset is one Nautilus does not know.
    pub async fn check_account(&self) -> anyhow::Result<()> {
        self.account().await.map(|_| ())
    }

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

    /// Returns the deals created over `[from_ms, to_ms]`, oldest first, each once.
    ///
    /// cTrader filters deal lists on creation time, bounds included. (It may be the last update
    /// instead; the two were the same on every deal measured.) Creation can come a millisecond
    /// before execution (demo, 2026-11-12), so a deal executed at `from_ms` may be left out.
    ///
    /// The order and size of pages are undocumented. A window answered only in part (`hasMore`) is
    /// asked again in halves.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A request fails.
    /// - A single millisecond holds more deals than a page.
    pub(crate) async fn deals(&self, from_ms: i64, to_ms: i64) -> anyhow::Result<Vec<ProtoOaDeal>> {
        // cTrader refuses a window that does not end after its start (`INCORRECT_BOUNDARIES`,
        // demo, 2026-10-26).
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
                // The halves overlap by a millisecond, so no deal falls between them
                anyhow::ensure!(
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

    /// Returns `None` if the account holds no such position (`POSITION_NOT_FOUND`, demo,
    /// 2026-11-12).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - The position holds more deals created by then than one answer carries.
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
        anyhow::ensure!(
            !page.has_more,
            "{} holds more deals of position {position_id} by {to_ms} ms than one answer carries",
            self.venue
        );
        let mut deals = page.deal;
        deals.sort_by_key(|d| (d.execution_timestamp, d.deal_id));
        Ok(deals)
    }

    pub(crate) fn trade<Req: TradeRequest>(
        &self,
        mut request: Req,
        client_msg_id: String,
    ) -> TradeSent {
        request.set_account(self.account_id.get());
        self.client.trade(&request, client_msg_id)
    }

    /// # Errors
    ///
    /// Returns an error if the session is closed.
    pub(crate) fn trade_events(&self) -> anyhow::Result<TradeEvents> {
        Ok(self.client.trade_events()?)
    }

    /// # Errors
    ///
    /// Returns an error if:
    /// - The order is not held by cTrader (`ORDER_NOT_FOUND`).
    /// - The request fails.
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

    /// Returns the orders over a window, as `deals` does.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A request fails.
    /// - A single millisecond holds more orders than a page.
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
                anyhow::ensure!(
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

    /// Returns the instruments of `ids`, from cTrader's current definitions.
    ///
    /// The broker's leverage comes from the account's and the symbol's dynamic leverage. It sets
    /// the margins (see `parse`).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - An id is not on the session's venue, before any request.
    /// - A request fails.
    /// - The broker lists no symbol of that name.
    /// - A symbol's assets are not both currencies Nautilus knows.
    pub async fn instruments(&self, ids: &[InstrumentId]) -> anyhow::Result<Vec<InstrumentAny>> {
        let symbols = self.symbols(ids).await?;
        Ok(symbols.into_iter().map(|s| s.instrument).collect())
    }

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

    /// Returns the quotes of `symbol` stamped in `[from, to]`, merged from its BID and ASK ticks
    /// (see `ticks::merge`).
    ///
    /// A failed tick request cuts its side short. The quotes then start where the ticks of both
    /// sides do, and the result carries the failure.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The window is out of range.
    /// - The `ticks::merge` call fails.
    pub(crate) async fn quote_ticks(
        &self,
        symbol: &CTraderSymbol,
        from: Timestamp,
        to: Timestamp,
    ) -> anyhow::Result<FetchedQuotes> {
        // The ticks of [from_ms, to_ms) in ms are those in [from, to] in ns
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

    async fn side_ticks(
        &self,
        symbol: &CTraderSymbol,
        side: ProtoOaQuoteType,
        from_ms: i64,
        to_ms: i64,
    ) -> (SideTicks, Option<anyhow::Error>) {
        // Not an async closure, which would make the quote request's future not `Send`
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
            // Known only at the window's start
            before: before.filter(|_| pages.from_ms == from_ms),
            ticks: pages.ticks,
            from_ms: pages.from_ms,
        };
        (side, pages.failure)
    }

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

    pub(crate) fn subscribe_spots(&self, symbol_id: i64) -> anyhow::Result<()> {
        Ok(self.client.subscribe_spots(symbol_id)?)
    }

    pub(crate) fn unsubscribe_spots(&self, symbol_id: i64) -> anyhow::Result<()> {
        Ok(self.client.unsubscribe_spots(symbol_id)?)
    }

    /// Renews the access token now.
    #[cfg(any(test, feature = "test-support"))]
    pub fn renew_token(&self) {
        self.client.renew_token();
    }

    /// Connects as `connect` does and subscribes to the spot events of `instrument_ids`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The `connect` call fails.
    /// - The `instruments` call fails for `instrument_ids`.
    #[cfg(feature = "test-support")]
    pub async fn connect_with_spots(
        connection: &CTraderConnection,
        instrument_ids: &[InstrumentId],
    ) -> anyhow::Result<(Self, SpotEvents)> {
        Self::connect_with_spots_to(connection.environment.url(), connection, instrument_ids).await
    }

    #[cfg(any(test, feature = "test-support"))]
    async fn connect_with_spots_to(
        url: &str,
        connection: &CTraderConnection,
        instrument_ids: &[InstrumentId],
    ) -> anyhow::Result<(Self, SpotEvents)> {
        let (session, spots) =
            Self::connect_to(url, connection.venue, connection.credential.clone(), None).await?;

        for symbol in session.symbols(instrument_ids).await? {
            session.subscribe_spots(symbol.symbol_id)?;
        }
        Ok((session, spots))
    }

    /// Asks the session to close without waiting.
    ///
    /// A renewal in flight is still saved if `disconnect` awaits it.
    pub(crate) fn begin_shutdown(&self) {
        self.client.begin_shutdown();
    }

    /// Closes the session.
    pub async fn disconnect(self) {
        self.client.disconnect().await;
    }
}

/// Quotes fetched for a window, with the failure that cut them short, if any.
///
/// Such a failure makes the quotes start later than the window.
#[derive(Debug, Default)]
pub(crate) struct FetchedQuotes {
    pub(crate) quotes: Vec<QuoteTick>,
    pub(crate) failure: Option<anyhow::Error>,
}

#[derive(Debug, Clone)]
pub(crate) struct CTraderSymbol {
    pub(crate) instrument: InstrumentAny,
    pub(crate) symbol_id: i64,
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

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
        messages::{Payload, ProtoMessage, ProtoOaSpotEvent, ProtoOaSubscribeSpotsReq},
        ticks::Tick,
        *,
    };
    use crate::testing::{
        self, ACCOUNT_ID, ASSET_LIST_RES, Action, Received, SPOT_EVENT_EURUSD_FIRST,
        SPOT_EVENT_XAUUSD_FIRST, SYMBOL_BY_ID_RES, SYMBOLS_LIST_RES, TRADER_RES, VENUE,
        accept_auth, replay, reply, spots_answer, tick_answer,
    };

    fn fixtures_peer(connection: usize, request: &ProtoMessage) -> Vec<Action> {
        ticks_peer(vec![], vec![], 10_000)(connection, request)
    }

    /// Acts as `fixtures_peer` and serves the ticks of every symbol in pages of `page_size`.
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
        let (session, _) = CTraderSession::connect_to(&url, Venue::new(VENUE), credential, None)
            .await
            .unwrap();
        (session, received)
    }

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

    /// Demo leverages are 1:30 for the account, 1:200 for gold and 1:1000 for EUR/USD. The broker
    /// applies the account's.
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

        let err = err.to_string();
        assert!(err.contains("GBPUSD"), "{err}");
        assert!(err.contains("ACMEFX"), "{err}");
        session.disconnect().await;
    }

    #[tokio::test]
    async fn an_id_of_another_venue_is_refused_before_any_request() {
        let (session, received) = connected().await;

        let err = session
            .instruments(&[InstrumentId::from("EUR/USD.IDEALPRO")])
            .await
            .unwrap_err();

        let err = err.to_string();
        assert!(err.contains("EUR/USD.IDEALPRO"), "{err}");
        assert!(err.contains("ACMEFX"), "{err}");
        assert_eq!(requests(&received), []);
        session.disconnect().await;
    }

    fn spots_peer(connection: usize, request: &ProtoMessage) -> Vec<Action> {
        if request.payload_type != ProtoOaSubscribeSpotsReq::TYPE {
            return fixtures_peer(connection, request);
        }
        let asked = ProtoOaSubscribeSpotsReq::decode(request.payload.as_deref().unwrap()).unwrap();
        let mut actions = vec![spots_answer(request)];
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

        assert_eq!(
            stamped,
            [(1, Some(1_793_017_476_361)), (41, Some(1_793_017_475_790))]
        );
        session.disconnect().await;
    }

    /// The order's value, about 3,590 EUR, is above the balance. The margin at 1:30, about 120 EUR,
    /// is below it.
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
