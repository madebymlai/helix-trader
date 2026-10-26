//! IC Markets history, over Spotware's cTrader Open API: the instruments
//! of IC's forex and metals, filled into the catalog.
//!
//! Nautilus has no cTrader adapter, so this module is one, laid out as
//! Nautilus' adapters are (`docs/developer_guide/adapters.md`): Spotware's
//! protobuf messages (`messages`, from the vendored `proto/`), the
//! environment and credential (`config`), the connection (`websocket`, an
//! outer client and its handler task) and the parsing of symbols into
//! instruments (`parse`), and the tick history merged into quotes
//! (`ticks`). IC's instruments are on venue `ICMARKETS`
//! (`XAUUSD.ICMARKETS`).

use std::{collections::HashMap, str::FromStr};

use anyhow::{Context, ensure};
use jiff::{SignedDuration, Timestamp};
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    data::{Data, NautilusDataType},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

use self::{
    config::{IcAccountId, IcCredential, IcEnvironment},
    messages::{
        ProtoOaAssetListReq, ProtoOaAssetListRes, ProtoOaGetTickDataReq, ProtoOaGetTickDataRes,
        ProtoOaQuoteType, ProtoOaSymbolByIdReq, ProtoOaSymbolByIdRes, ProtoOaSymbolsListReq,
        ProtoOaSymbolsListRes,
    },
    ticks::{SideTicks, Tick},
    websocket::{HEARTBEAT_INTERVAL, IcWebSocketClient},
};
use super::fill_catalog;

mod config;
mod messages;
mod parse;
#[cfg(test)]
mod testing;
mod ticks;
mod websocket;

/// A cTrader session on IC's account, for history.
#[derive(Debug)]
pub struct IcHistory {
    client: IcWebSocketClient,
    account_id: IcAccountId,
}

impl IcHistory {
    /// Connects to the environment `IC_ENVIRONMENT` names and
    /// authenticates the credential `IC_CLIENT_ID`, `IC_CLIENT_SECRET`,
    /// `IC_ACCESS_TOKEN` and `IC_ACCOUNT_ID` from the environment (or
    /// `.env`); none is ever printed.
    ///
    /// # Errors
    ///
    /// Returns an error if the environment or a credential variable is
    /// missing or invalid (before connecting), if cTrader cannot be
    /// reached, or if it refuses the credential (naming its error code).
    pub async fn connect() -> anyhow::Result<Self> {
        let var = |name: &str| std::env::var(name).ok();
        let environment = IcEnvironment::from_vars(var)?;
        let credential = IcCredential::from_vars(var)?;
        let history = Self::connect_to(environment.url(), credential, HEARTBEAT_INTERVAL).await?;
        log::info!("connected to IC's cTrader {environment:?} server");
        Ok(history)
    }

    async fn connect_to(
        url: &str,
        credential: IcCredential,
        heartbeat: std::time::Duration,
    ) -> anyhow::Result<Self> {
        let account_id = credential.account_id;
        let client = IcWebSocketClient::connect(url, credential, heartbeat).await?;
        Ok(Self { client, account_id })
    }

    /// Whether `id` is IC's, so its history comes from here.
    #[must_use]
    pub fn serves(id: InstrumentId) -> bool {
        parse::is_ic(id)
    }

    /// The instruments of `ids`, from cTrader's current definitions: the
    /// assets, the symbol list (each id's symbol name to its `symbolId` on
    /// this server) and the details of those symbols (see `parse`).
    ///
    /// # Errors
    ///
    /// Returns an error if an id is not IC's (before any request), if a
    /// request fails, if IC lists no symbol of that name, or if a symbol is
    /// no currency pair.
    pub async fn instruments(&self, ids: &[InstrumentId]) -> anyhow::Result<Vec<InstrumentAny>> {
        let symbols = self.symbols(ids).await?;
        Ok(symbols.into_iter().map(|s| s.instrument).collect())
    }

    /// The symbols of `ids`, as `instruments`.
    async fn symbols(&self, ids: &[InstrumentId]) -> anyhow::Result<Vec<IcSymbol>> {
        for &id in ids {
            parse::ensure_ic(id)?;
        }
        let account = self.account_id.get();
        let assets: ProtoOaAssetListRes = self
            .client
            .request(&ProtoOaAssetListReq {
                ctid_trader_account_id: account,
                ..Default::default()
            })
            .await
            .context("requesting IC's assets")?;
        let list: ProtoOaSymbolsListRes = self
            .client
            .request(&ProtoOaSymbolsListReq {
                ctid_trader_account_id: account,
                ..Default::default()
            })
            .await
            .context("requesting IC's symbols")?;
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
                            "IC lists no symbol {}: an IC id is the symbol's name in cTrader \
                         on {}, such as XAUUSD.{}",
                            parse::symbol_name(id),
                            parse::IC_VENUE,
                            parse::IC_VENUE
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
            .context("requesting the details of IC's symbols")?;
        let details: HashMap<i64, _> = details.symbol.iter().map(|s| (s.symbol_id, s)).collect();
        let assets = parse::assets_by_id(assets.asset);
        let ts_init = get_atomic_clock_realtime().get_time_ns();
        light
            .into_iter()
            .map(|light| {
                let name = light.symbol_name.as_deref().unwrap_or_default();
                let symbol = details
                    .get(&light.symbol_id)
                    .with_context(|| format!("IC returned no details of {name}"))?;
                let pair = parse::currency_pair(light, symbol, &assets, ts_init)?;
                Ok(IcSymbol {
                    instrument: InstrumentAny::CurrencyPair(pair),
                    symbol_id: light.symbol_id,
                })
            })
            .collect()
    }

    /// Fills `catalog` with the instruments of `series`, each the quotes
    /// of an IC instrument, and over `[start, end]` with the quotes
    /// themselves, merged from cTrader's BID and ASK ticks (see `quotes`),
    /// a week at a time (see `fill_quotes`).
    ///
    /// # Errors
    ///
    /// Returns an error if a series is not the quotes of an IC instrument
    /// (before any request), as `instruments`, as `quotes` or as
    /// `fill_catalog`: a `start` before the stored quotes, or a first
    /// window without quotes, naming the series and the window.
    pub async fn fill(
        &self,
        catalog: &mut ParquetDataCatalog,
        series: &[(NautilusDataType, String)],
        start: Timestamp,
        end: Timestamp,
    ) -> anyhow::Result<()> {
        let ids = series
            .iter()
            .map(|(data_type, identifier)| quotes_of(data_type, identifier))
            .collect::<anyhow::Result<Vec<_>>>()?;
        for symbol in self.symbols(&ids).await? {
            fill_quotes(catalog, &symbol.instrument, start, end, async |from, to| {
                self.quotes(&symbol, from, to).await
            })
            .await?;
        }
        Ok(())
    }

    /// The quotes of `symbol` stamped in `[from, to]`, merged from its BID
    /// and ASK ticks of those milliseconds and the last tick of each before
    /// them (see `ticks::merge`), the two sides fetched side by side.
    /// Logs the counts.
    async fn quotes(
        &self,
        symbol: &IcSymbol,
        from: Timestamp,
        to: Timestamp,
    ) -> anyhow::Result<Vec<Data>> {
        // [from, to] in ns holds the ticks of [from_ms, to_ms) in ms.
        let ms = |ns: i128| i64::try_from(ns.div_euclid(1_000_000)).context("a time out of range");
        let from_ms = ms(from.as_nanosecond() + 999_999)?;
        let to_ms = ms(to.as_nanosecond())? + 1;
        if from_ms >= to_ms {
            return Ok(Vec::new());
        }
        let (bid, ask) = tokio::try_join!(
            self.side_ticks(symbol, ProtoOaQuoteType::Bid, from_ms, to_ms),
            self.side_ticks(symbol, ProtoOaQuoteType::Ask, from_ms, to_ms),
        )?;
        let merged = ticks::merge(&symbol.instrument, &bid, &ask)?;
        log::info!(
            "{}: {} quotes over [{from}, {to}] from {} BID and {} ASK ticks; {} ticks before \
             both sides were known, {} crossed quotes (ask below bid) dropped",
            symbol.instrument.id(),
            merged.quotes.len(),
            bid.ticks.len(),
            ask.ticks.len(),
            merged.one_sided,
            merged.crossed,
        );
        Ok(merged.quotes.into_iter().map(Data::Quote).collect())
    }

    /// The `side` ticks of `symbol` over `[from_ms, to_ms)` and its last
    /// tick before them.
    async fn side_ticks(
        &self,
        symbol: &IcSymbol,
        side: ProtoOaQuoteType,
        from_ms: i64,
        to_ms: i64,
    ) -> anyhow::Result<SideTicks> {
        let page = async |from, to| self.tick_page(symbol, side, from, to).await;
        Ok(SideTicks {
            before: ticks::last_before(from_ms, page).await?,
            ticks: ticks::side_ticks(from_ms, to_ms, page).await?,
        })
    }

    /// One tick request: the newest page of `side` ticks of `symbol` over
    /// `[from_ms, to_ms)`, newest first, and whether there are more.
    async fn tick_page(
        &self,
        symbol: &IcSymbol,
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

    /// Closes the session.
    pub async fn disconnect(self) {
        self.client.disconnect().await;
    }
}

/// An IC instrument and its cTrader `symbolId` on this server.
struct IcSymbol {
    instrument: InstrumentAny,
    symbol_id: i64,
}

/// How much of a fill `fill_quotes` writes at once: a week of XAUUSD is
/// about 0.7 M BID and 0.7 M ASK ticks (`ic/README.md`, "Tick history").
const FILL_PERIOD: SignedDuration = SignedDuration::from_hours(7 * 24);

/// Fills `catalog` with `instrument` and its quotes over `[start, end]`
/// through `fetch`, one `fill_catalog` call per period of `FILL_PERIOD`
/// from where the stored quotes end (or `start`), the last period taking
/// the remainder, so that each period but a short whole window spans a
/// week, which always holds ticks. Each call resumes where the catalog
/// stopped, so a period without quotes after the first (a market closed)
/// writes nothing, and the next asks for it again.
///
/// # Errors
///
/// As `fill_catalog`.
async fn fill_quotes(
    catalog: &mut ParquetDataCatalog,
    instrument: &InstrumentAny,
    start: Timestamp,
    end: Timestamp,
    mut fetch: impl AsyncFnMut(Timestamp, Timestamp) -> anyhow::Result<Vec<Data>>,
) -> anyhow::Result<()> {
    let identifier = instrument.id().to_string();
    let last = catalog
        .query_last_timestamp(&NautilusDataType::QuoteTick.into(), Some(&identifier))
        .with_context(|| format!("reading the stored range of the quotes of {identifier}"))?;
    // `fill_catalog` resumes 1 ns after the stored end.
    let begin = last.map_or(start, |last| {
        Timestamp::from(UnixNanos::from(last)) + SignedDuration::from_nanos(1)
    });
    let series = [(NautilusDataType::QuoteTick, identifier)];
    let mut period_end = begin;
    loop {
        period_end = if end.duration_since(period_end) < FILL_PERIOD * 2 {
            end
        } else {
            period_end + FILL_PERIOD
        };
        fill_catalog(
            catalog,
            vec![instrument.clone()],
            &series,
            start,
            period_end,
            async |_, _, from, to| fetch(from, to).await,
        )
        .await?;
        if period_end >= end {
            return Ok(());
        }
    }
}

/// The IC instrument whose quotes the series of `data_type` and
/// `identifier` is; an error for any other series.
fn quotes_of(data_type: &NautilusDataType, identifier: &str) -> anyhow::Result<InstrumentId> {
    ensure!(
        *data_type == NautilusDataType::QuoteTick,
        "IC serves quotes only, not the {data_type:?} of {identifier}"
    );
    let id = InstrumentId::from_str(identifier)
        .with_context(|| format!("{identifier} is no instrument id"))?;
    parse::ensure_ic(id)?;
    Ok(id)
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        instruments::Instrument,
        types::{Currency, Quantity},
    };
    use prost::Message as _;

    use super::{
        messages::{Payload, ProtoMessage},
        testing::{
            self, ACCOUNT_ID, ASSET_LIST_RES, Action, Received, SYMBOL_BY_ID_RES, SYMBOLS_LIST_RES,
            accept_auth, credential, replay, reply, tick_answer,
        },
        ticks::{Tick, WEEK_MS},
        *,
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
            other => panic!("unexpected request {other}"),
        };
        vec![response]
    }

    async fn connected() -> (IcHistory, Received) {
        connected_to(fixtures_peer).await
    }

    async fn connected_to(
        peer: impl FnMut(usize, &ProtoMessage) -> Vec<Action> + Send + 'static,
    ) -> (IcHistory, Received) {
        let (url, received) = testing::start(peer);
        let history = IcHistory::connect_to(&url, credential(), HEARTBEAT_INTERVAL)
            .await
            .unwrap();
        (history, received)
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
        let (history, received) = connected().await;
        let ids = [
            InstrumentId::from("XAUUSD.ICMARKETS"),
            InstrumentId::from("EURUSD.ICMARKETS"),
        ];

        let instruments = history.instruments(&ids).await.unwrap();

        let [
            InstrumentAny::CurrencyPair(xau),
            InstrumentAny::CurrencyPair(eur),
        ] = instruments.as_slice()
        else {
            panic!("{instruments:?}");
        };
        assert_eq!((xau.id(), eur.id()), (ids[0], ids[1]));
        assert_eq!(xau.base_currency, Currency::from("XAU"));
        assert_eq!(xau.lot_size, Some(Quantity::from(100)));
        assert_eq!(eur.price_precision, 5);
        let requests = requests(&received);
        let types: Vec<u32> = requests.iter().map(|f| f.payload_type).collect();
        assert_eq!(types, [2112, 2114, 2116]);
        let details =
            ProtoOaSymbolByIdReq::decode(requests[2].payload.as_deref().unwrap()).unwrap();
        assert_eq!(details.ctid_trader_account_id, ACCOUNT_ID);
        assert_eq!(details.symbol_id, [41, 1]);
        history.disconnect().await;
    }

    #[tokio::test]
    async fn a_symbol_ic_does_not_list_is_named() {
        let (history, _) = connected().await;

        let err = history
            .instruments(&[InstrumentId::from("GBPUSD.ICMARKETS")])
            .await
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "IC lists no symbol GBPUSD: an IC id is the symbol's name in cTrader on ICMARKETS, \
             such as XAUUSD.ICMARKETS"
        );
        history.disconnect().await;
    }

    #[tokio::test]
    async fn an_id_of_another_venue_is_refused_before_any_request() {
        let (history, received) = connected().await;

        let err = history
            .instruments(&[InstrumentId::from("EUR/USD.IDEALPRO")])
            .await
            .unwrap_err();

        assert_eq!(err.to_string(), "EUR/USD.IDEALPRO is not on ICMARKETS");
        assert_eq!(requests(&received), []);
        history.disconnect().await;
    }

    const XAUUSD: &str = "XAUUSD.ICMARKETS";

    /// cTrader's XAUUSD symbol id on the server the fixtures come from.
    const XAUUSD_SYMBOL_ID: i64 = 41;

    fn ts(at: &str) -> Timestamp {
        at.parse().unwrap()
    }

    fn ms(at: &str) -> i64 {
        ts(at).as_millisecond()
    }

    fn tick(ms: i64, price: i64) -> Tick {
        Tick { ms, price }
    }

    fn quotes_series() -> [(NautilusDataType, String); 1] {
        [(NautilusDataType::QuoteTick, XAUUSD.to_owned())]
    }

    /// The stored quotes of XAUUSD: (ms, bid, ask).
    fn stored(catalog: &mut ParquetDataCatalog) -> Vec<(i64, String, String)> {
        catalog
            .quote_ticks(Some(vec![XAUUSD.to_owned()]), None, None)
            .unwrap()
            .iter()
            .map(|q| {
                (
                    i64::try_from(q.ts_init.as_u64() / 1_000_000).unwrap(),
                    q.bid_price.to_string(),
                    q.ask_price.to_string(),
                )
            })
            .collect()
    }

    fn quote(ms: i64, bid: &str, ask: &str) -> (i64, String, String) {
        (ms, bid.to_owned(), ask.to_owned())
    }

    fn intervals(catalog: &ParquetDataCatalog) -> Vec<(u64, u64)> {
        catalog
            .get_intervals(&NautilusDataType::QuoteTick.into(), Some(XAUUSD))
            .unwrap()
    }

    fn ns(at: Timestamp) -> u64 {
        UnixNanos::from(at).as_u64()
    }

    /// The tick requests received: (type, from, to).
    fn tick_requests(received: &Received) -> Vec<(i32, i64, i64)> {
        requests(received)
            .iter()
            .filter(|f| f.payload_type == ProtoOaGetTickDataReq::TYPE)
            .map(|f| {
                let r = ProtoOaGetTickDataReq::decode(f.payload.as_deref().unwrap()).unwrap();
                assert_eq!(
                    (r.ctid_trader_account_id, r.symbol_id),
                    (ACCOUNT_ID, XAUUSD_SYMBOL_ID)
                );
                (r.r#type, r.from_timestamp.unwrap(), r.to_timestamp.unwrap())
            })
            .collect()
    }

    fn sorted<T: Ord>(mut items: Vec<T>) -> Vec<T> {
        items.sort();
        items
    }

    #[tokio::test]
    async fn a_fill_stores_the_quotes_merged_from_the_bid_and_ask_ticks_of_its_window() {
        let (start, end) = ("2026-10-01T00:00:00Z", "2026-10-01T01:00:00Z");
        let s = ms(start);
        let bids = vec![
            tick(s - 60_000, 400_000_000),
            tick(s + 1_000, 400_010_000),
            tick(s + 3_000, 400_020_000),
            tick(ms(end) + 5_000, 401_000_000),
        ];
        let asks = vec![
            tick(s - 30_000, 400_050_000),
            tick(s + 2_000, 400_060_000),
            tick(s + 3_000, 400_070_000),
        ];
        let (history, _) = connected_to(ticks_peer(bids, asks, 2)).await;
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);

        history
            .fill(&mut catalog, &quotes_series(), ts(start), ts(end))
            .await
            .unwrap();

        // The bid and ask before the window seed its first quotes; the bid
        // after it is not stored.
        assert_eq!(
            stored(&mut catalog),
            [
                quote(s + 1_000, "4000.10", "4000.50"),
                quote(s + 2_000, "4000.10", "4000.60"),
                quote(s + 3_000, "4000.20", "4000.70"),
            ]
        );
        assert_eq!(intervals(&catalog), [(ns(ts(start)), ns(ts(end)))]);
        let stored = catalog.instruments(None, None, None).unwrap();
        let ids: Vec<InstrumentId> = stored.iter().map(Instrument::id).collect();
        assert_eq!(ids, [InstrumentId::from(XAUUSD)]);
        history.disconnect().await;
    }

    #[tokio::test]
    async fn each_side_is_asked_for_its_last_tick_before_the_window_and_the_window_to_1_ms_past_its_end()
     {
        let (start, end) = ("2026-10-01T00:00:00Z", "2026-10-01T01:00:00Z");
        let (s, e) = (ms(start), ms(end));
        let (history, received) = connected().await;
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);

        let _ = history
            .fill(&mut catalog, &quotes_series(), ts(start), ts(end))
            .await;

        assert_eq!(
            sorted(tick_requests(&received)),
            [
                (1, s - WEEK_MS, s),
                (1, s, e + 1),
                (2, s - WEEK_MS, s),
                (2, s, e + 1),
            ]
        );
        history.disconnect().await;
    }

    #[tokio::test]
    async fn a_resumed_fill_asks_from_1_ms_after_the_stored_end_and_stores_no_quote_twice() {
        let (start, middle, end) = (
            "2026-10-01T00:00:00Z",
            "2026-10-01T01:00:00Z",
            "2026-10-01T02:00:00Z",
        );
        let (s, m, e) = (ms(start), ms(middle), ms(end));
        let bids = vec![
            tick(s, 400_000_000),
            tick(m, 400_010_000),
            tick(m + 1, 400_020_000),
        ];
        let asks = vec![tick(s, 400_050_000)];
        let (history, received) = connected_to(ticks_peer(bids, asks, 10_000)).await;
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        history
            .fill(&mut catalog, &quotes_series(), ts(start), ts(middle))
            .await
            .unwrap();
        let first = tick_requests(&received).len();

        history
            .fill(&mut catalog, &quotes_series(), ts(start), ts(end))
            .await
            .unwrap();

        assert_eq!(
            sorted(tick_requests(&received)[first..].to_vec()),
            [
                (1, m + 1 - WEEK_MS, m + 1),
                (1, m + 1, e + 1),
                (2, m + 1 - WEEK_MS, m + 1),
                (2, m + 1, e + 1),
            ]
        );
        assert_eq!(
            stored(&mut catalog),
            [
                quote(s, "4000.00", "4000.50"),
                quote(m, "4000.10", "4000.50"),
                quote(m + 1, "4000.20", "4000.50"),
            ]
        );
        history.disconnect().await;
    }

    #[tokio::test]
    async fn a_start_before_the_stored_quotes_is_refused_before_any_tick_request() {
        let (start, end) = ("2026-10-01T00:00:00Z", "2026-10-01T01:00:00Z");
        let bids = vec![tick(ms(start) + 1, 400_000_000)];
        let asks = vec![tick(ms(start) + 1, 400_050_000)];
        let (history, received) = connected_to(ticks_peer(bids, asks, 10_000)).await;
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        history
            .fill(&mut catalog, &quotes_series(), ts(start), ts(end))
            .await
            .unwrap();
        let first = tick_requests(&received).len();

        let err = history
            .fill(
                &mut catalog,
                &quotes_series(),
                ts("2026-09-30T00:00:00Z"),
                ts(end),
            )
            .await
            .unwrap_err();

        assert!(format!("{err:#}").contains("backwards"), "{err:#}");
        assert_eq!(tick_requests(&received).len(), first);
        history.disconnect().await;
    }

    #[tokio::test]
    async fn a_first_window_without_ticks_is_an_error_naming_the_series_and_the_window() {
        let (start, end) = ("2014-01-01T00:00:00Z", "2014-01-02T00:00:00Z");
        let (history, _) = connected().await;
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);

        let err = history
            .fill(&mut catalog, &quotes_series(), ts(start), ts(end))
            .await
            .unwrap_err();

        let err = format!("{err:#}");
        assert!(err.contains("quotes of XAUUSD.ICMARKETS"), "{err}");
        assert!(err.contains(start) && err.contains(end), "{err}");
        history.disconnect().await;
    }

    #[tokio::test]
    async fn a_week_without_ticks_is_no_error_and_a_long_fill_is_stored_a_week_at_a_time() {
        let start = ts("2026-10-01T00:00:00Z");
        let week = jiff::SignedDuration::from_hours(7 * 24);
        let end = start + week * 3;
        let in_week = |n: i32| (start + week * n).as_millisecond() + 60_000;
        let bids = vec![tick(in_week(0), 400_000_000), tick(in_week(2), 400_010_000)];
        let asks = vec![tick(in_week(0), 400_050_000)];
        let (history, _) = connected_to(ticks_peer(bids, asks, 10_000)).await;
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);

        history
            .fill(&mut catalog, &quotes_series(), start, end)
            .await
            .unwrap();

        assert_eq!(
            stored(&mut catalog),
            [
                quote(in_week(0), "4000.00", "4000.50"),
                quote(in_week(2), "4000.10", "4000.50"),
            ]
        );
        let first_week = ns(start + week);
        assert_eq!(
            intervals(&catalog),
            [(ns(start), first_week), (first_week + 1, ns(end))]
        );
        history.disconnect().await;
    }

    #[tokio::test]
    async fn a_series_other_than_quotes_is_refused_before_any_request() {
        let (history, received) = connected().await;
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let bars = "XAUUSD.ICMARKETS-1-MINUTE-BID-EXTERNAL".to_owned();
        let start: Timestamp = "2026-10-01T00:00:00Z".parse().unwrap();

        let err = history
            .fill(&mut catalog, &[(NautilusDataType::Bar, bars)], start, start)
            .await
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "IC serves quotes only, not the Bar of XAUUSD.ICMARKETS-1-MINUTE-BID-EXTERNAL"
        );
        assert_eq!(requests(&received), []);
        history.disconnect().await;
    }
}
