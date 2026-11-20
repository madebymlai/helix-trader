//! Interactive Brokers: the account, the gateway (see `gateway`) and the data client config its
//! history and its live clients share, instruments and history for `fill_catalog`, which asks for
//! series of any data type: IB serves its own time bars, its quotes and its trades (see `serves`,
//! `IbHistory::data`), and the live node's clients (see `live`), whose data client streams its
//! quotes, trades and time bars (`streams`); the client id each command connects with, and live's
//! account, are its flags (`IbHistoryArgs`, `live::IbLiveArgs`). See `README.md`.
//!
//! Temporary: Nautilus writes none of IB's answers back, as IB's data client answers without a
//! `DataResponseEnvelope`, so IB's fill (`IbHistory::fill`) fetches and writes them itself. Once
//! nautilus_trader#5472 is fixed, IB fills through the node as LSE and IC do (see
//! `catalog::NodeFill`), and `fill` and `data` go: helix-trader#4
//! (<https://github.com/madebymlai/helix-trader/issues/4>).
//!
//! Uses `HistoricalInteractiveBrokersClient` (`nautilus_interactive_brokers::historical`), which
//! needs no node: on the connection to IB Gateway or TWS this module makes for it (`connected`), it
//! returns `Vec<InstrumentAny>` from `request_instruments` and `Vec<Bar>` from `request_bars`. The
//! quotes and trades of IB's historical ticks are paged here, on the same connection, with the
//! requests and the parsers Nautilus' IB clients use, failing where Nautilus' `request_ticks` ends
//! short (`IbHistory::ticks`). The data is stored as IB gives it.

use std::{
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant},
};

use anyhow::Context;
use clap::Args;
use ibapi::{
    Client, SUBSCRIPTION_LAG_CODE,
    contracts::{Contract, Symbol},
    market_data::{
        IgnoreSize, TradingHours,
        historical::{TickBidAsk, TickDecoder, TickLast, TickSubscription},
    },
    prelude::StreamExt,
    subscriptions::SubscriptionItem,
};
use jiff::{RoundMode, SignedDuration, Timestamp, TimestampRound, ToSpan, Unit, tz::TimeZone};
use nautilus_core::UnixNanos;
use nautilus_interactive_brokers::{
    common::{
        enums::IbHistoricalTickType,
        futures::parse_futures_symbol,
        shared_client::{self, SharedClientHandle},
        symbology::Symbology,
    },
    config::{InteractiveBrokersDataClientConfig, MarketDataType},
    data::{
        convert::{
            IbPriceConverter, bar_type_to_ib_bar_size, ib_timestamp_to_unix_nanos,
            jiff_to_ib_datetime,
        },
        parse::{parse_quote_tick, parse_trade_tick},
    },
    historical::HistoricalInteractiveBrokersClient,
    providers::instruments::InteractiveBrokersInstrumentProvider,
};
use nautilus_model::{
    data::{Bar, BarSpecification, BarType, Data, NautilusDataType},
    enums::{BarAggregation, PriceType},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

use crate::{
    catalog::{fill_slices, resumed},
    strategy::requirements::{CatalogSeries, SeriesSlice},
};

mod gateway;
pub use gateway::IbAccount;
use gateway::IbGateway;
mod live;
pub(in crate::venues) use live::BOOK;
#[cfg(test)]
pub(in crate::venues) use live::offline_clients;
pub(crate) use live::{IbLiveArgs, clients, streams};

/// IB's settings of a catalog fill, fill-catalog's flags: the client id its history connects with
/// (see `IbHistory::connect`).
#[derive(Debug, Args)]
pub(crate) struct IbHistoryArgs {
    /// IB client id of this process; not the adapter default 1, so a live node with the default id
    /// can run at the same time.
    #[arg(long, default_value_t = 10)]
    pub(super) client_id: i32,
}

/// A connection to IB for history: instruments, bars and ticks, no node.
pub struct IbHistory {
    client: HistoricalInteractiveBrokersClient,
    /// The connection the client asks IB on, which the ticks' pages are asked on too (`TickPages`).
    ib: SharedClientHandle,
    /// The instrument provider the client defines instruments with, which the ticks' pages take
    /// their contracts and prices from.
    provider: Arc<InteractiveBrokersInstrumentProvider>,
    requests: HistoryRequests,
    symbology: Symbology,
    /// The requests of small bars sent so far, which IB paces.
    small_bars: Mutex<SmallBarPacing>,
}

/// The settings every history request passes, taken from the data client config so history and live
/// agree on the session.
#[derive(Debug, Clone, Copy)]
struct HistoryRequests {
    use_rth: bool,
    timeout_secs: u64,
}

impl HistoryRequests {
    fn of(config: &InteractiveBrokersDataClientConfig) -> Self {
        Self {
            use_rth: config.use_regular_trading_hours,
            timeout_secs: config.request_timeout,
        }
    }
}

impl IbHistory {
    /// Connects with `client_id` to the paper gateway, started or reused (see `IbGateway::paper`),
    /// once IB can serve closed bars over a window ending at `end`: checked before the gateway
    /// starts.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `end` is within a minute of `now`.
    /// - The gateway does not start and log in.
    /// - The connection fails.
    pub async fn connect(client_id: i32, end: Timestamp, now: Timestamp) -> anyhow::Result<Self> {
        ensure_bars_closed(end, now)?;
        let config = data_client_config(IbGateway::paper(client_id).await?);
        let requests = HistoryRequests::of(&config);
        let symbology = Symbology::new(config.instrument_provider.symbology_method);
        let address = format!("{}:{}", config.host, config.port);
        let (ib, provider) = connected(&config)
            .await
            .with_context(|| format!("connecting to IB at {address}"))?;
        let client =
            HistoricalInteractiveBrokersClient::new(Arc::clone(ib.as_arc()), Arc::clone(&provider));
        Ok(Self {
            client,
            ib,
            provider,
            requests,
            symbology,
            small_bars: Mutex::default(),
        })
    }

    /// The latest end of a window IB can serve closed bars over at `now`: a minute before `now`,
    /// the 1-minute bar in progress until then.
    ///
    /// # Errors
    ///
    /// Returns an error if that end is out of range.
    pub fn latest_end(now: Timestamp) -> anyhow::Result<Timestamp> {
        Ok(now.checked_sub(SignedDuration::from_mins(1))?)
    }

    /// The instruments of `ids`, as IB defines them. A future IB does not resolve by id (an expired
    /// one) is asked for again as an explicit contract with `includeExpired`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - Any id is not returned (IB answers an unknown or expired id with nothing).
    pub async fn instruments(&self, ids: &[InstrumentId]) -> anyhow::Result<Vec<InstrumentAny>> {
        let mut instruments = self
            .client
            .request_instruments(Some(ids.to_vec()), None)
            .await
            .context("requesting instruments from IB")?;
        let expired: Vec<Contract> = missing_ids(ids, &instruments)
            .into_iter()
            .filter_map(|id| expired_future_contract(self.symbology, id))
            .collect();
        if !expired.is_empty() {
            instruments.extend(
                self.client
                    .request_instruments(None, Some(expired))
                    .await
                    .context("requesting expired futures from IB")?,
            );
        }
        ensure_all_loaded(ids, &instruments)?;
        Ok(instruments)
    }

    /// Fills `catalog` with `slices`, IB series each on a contract over its slice
    /// (`DataRequirements::slices`, as the node fill takes them), which IB serves (`serves`) and
    /// holds (`check_held`, checked before the fill connects): the instruments of their contracts,
    /// then each slice through `fill_slices`, fetched by `data`. Temporary until
    /// nautilus_trader#5472 is fixed; then IB fills through the node.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A slice is not of a series IB serves (before any request).
    /// - `instruments` or `fill_slices` fails.
    pub async fn fill(
        &self,
        catalog: &mut ParquetDataCatalog,
        slices: &[SeriesSlice],
    ) -> anyhow::Result<()> {
        let mut ids: Vec<InstrumentId> = Vec::new();

        for slice in slices {
            serves(&slice.series)?;
            let id = slice.series.instrument_id();
            if !ids.contains(&id) {
                ids.push(id);
            }
        }
        let instruments = self.instruments(&ids).await?;
        fill_slices(catalog, &instruments, slices, async |series, from, to| {
            self.data(series, from, to).await
        })
        .await
    }

    /// The resolved `series` over `[start, end]`, for `fill_catalog`: its bars (see `bars`) or the
    /// instrument's quotes or trades (see `ticks`).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - IB's history does not serve the series (see `serves`).
    /// - `bars` or `ticks` fails.
    pub async fn data(
        &self,
        series: &CatalogSeries,
        start: Timestamp,
        end: Timestamp,
    ) -> anyhow::Result<Vec<Data>> {
        match served(series)? {
            Served::Bars(bar_type) => {
                let bars = self.bars(bar_type, start, end).await?;
                Ok(bars.into_iter().map(Data::Bar).collect())
            }
            Served::Ticks(tick_type, instrument_id) => {
                self.ticks(tick_type, instrument_id, start, end).await
            }
        }
    }

    /// The bars of `bar_type` over `[start, end]`, sorted by `ts_init` and unique, with the
    /// regular-hours setting of the live data client. Walks back from `end` one request at a time
    /// (see `walk_back`), each over the duration IB takes for the bars' size (`request_duration`),
    /// a request of bars of 30 seconds or less sent once IB's pacing of them takes it
    /// (`SmallBarPacing`).
    async fn bars(
        &self,
        bar_type: BarType,
        start: Timestamp,
        end: Timestamp,
    ) -> anyhow::Result<Vec<Bar>> {
        let spec = bar_type.spec();
        let spec_string = spec.to_string();
        let duration = request_duration(spec);
        walk_back(start, end, async |cursor| {
            if spec.aggregation == BarAggregation::Second {
                self.paced(SmallBarRequest {
                    bar_type,
                    end: cursor,
                    duration,
                })
                .await;
            }
            self.client
                .request_bars(
                    vec![&spec_string],
                    cursor,
                    None,
                    Some(duration),
                    None,
                    Some(vec![bar_type.instrument_id()]),
                    self.requests.use_rth,
                    self.requests.timeout_secs,
                )
                .await
                .with_context(|| format!("requesting {duration} of bars ending at {cursor}"))
        })
        .await
    }

    /// Waits until IB's pacing of small bars takes `request` (`SmallBarPacing::wait`), then counts
    /// it sent.
    async fn paced(&self, request: SmallBarRequest) {
        let pacing = || {
            self.small_bars
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
        };
        loop {
            let wait = pacing().wait(Instant::now(), &request);
            if wait.is_zero() {
                break;
            }
            tokio::time::sleep(wait).await;
        }
        pacing().sent(Instant::now(), request);
    }

    /// The `tick_type` ticks of `instrument_id` over `[start, end]`, IB's historical BID_ASK ticks
    /// as quotes or its TRADES ticks as trades, sorted by `ts_event`, with the regular-hours
    /// setting of the live data client: paged back from `end` (`page_back`), each page asked as
    /// Nautilus' IB clients ask it (`TickPages::page`), on the contract and at the prices of the
    /// instrument's definition, fetched first if the provider lacks it, as Nautilus'
    /// `request_ticks` does. Nautilus' `request_ticks` is not used: it logs and skips an error in a
    /// page, which then reads as a short or an empty page, the latter ending the paging; it ends
    /// the paging at a second holding a full page too, answering the ticks so far with no error,
    /// which the writer would store as the whole window; and it loses the rest of each full page's
    /// earliest second, which IB cuts short (see `page_back`).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The definition cannot be had.
    /// - A page is refused, is not answered whole within the request timeout or ends with an error.
    /// - A second holds a full page (see `page_back`).
    ///
    /// The error names the instrument and the window, and IB's error.
    async fn ticks(
        &self,
        tick_type: IbHistoricalTickType,
        instrument_id: InstrumentId,
        start: Timestamp,
        end: Timestamp,
    ) -> anyhow::Result<Vec<Data>> {
        let paged = async {
            let client = self.ib.as_arc().as_ref();
            if self.provider.find(&instrument_id).is_none() {
                self.provider
                    .fetch_contract_details(client, instrument_id, false, None)
                    .await?;
            }
            let instrument = self
                .provider
                .find(&instrument_id)
                .with_context(|| format!("IB defines no instrument {instrument_id}"))?;
            let contract = self
                .provider
                .resolve_contract_for_instrument_async(client, instrument_id)
                .await?;
            let pages = TickPages {
                client,
                contract: &contract,
                instrument_id,
                prices: IbPriceConverter::for_instrument(
                    &instrument,
                    self.provider.get_price_magnifier(&instrument_id),
                ),
                size_precision: instrument.size_precision(),
                trading_hours: if self.requests.use_rth {
                    TradingHours::Regular
                } else {
                    TradingHours::Extended
                },
                timeout: Duration::from_secs(self.requests.timeout_secs),
            };
            page_back(
                start,
                end,
                TICKS_PER_PAGE as usize,
                |(ts_event, _): &PagedTick| *ts_event,
                async |cursor| pages.page(tick_type, cursor).await,
            )
            .await
        };
        let paged = paged.await.with_context(|| {
            format!(
                "requesting the {} ticks of {instrument_id} over [{start}, {end}]",
                tick_type.as_str()
            )
        })?;
        Ok(paged.into_iter().filter_map(|(_, tick)| tick).collect())
    }
}

/// A tick of a page of IB's historical ticks: its time (`ts_event`), and the tick Nautilus' parsers
/// make of it, none of one Nautilus' IB clients drop.
type PagedTick = (UnixNanos, Option<Data>);

/// How many ticks a request of IB's historical ticks asks, IB's most: IB answers a few more, its
/// earliest second cut short (see `page_back`).
const TICKS_PER_PAGE: i32 = 1000;

/// What the pages of an instrument's historical ticks are asked on: the connection, the
/// instrument's contract, the prices and size precision of its definition, the session and the
/// request timeout.
struct TickPages<'a> {
    client: &'a Client,
    contract: &'a Contract,
    instrument_id: InstrumentId,
    prices: IbPriceConverter,
    size_precision: u8,
    trading_hours: TradingHours,
    timeout: Duration,
}

impl TickPages<'_> {
    /// The page of `tick_type` ticks up to `cursor`, asked as Nautilus' IB clients ask it
    /// (`historical_ticks` of `TICKS_PER_PAGE`, ending at `cursor`, in the session, BID_ASK with
    /// sizes or TRADES), read whole within the request timeout: its items in the order IB sent
    /// them, each a tick, made of IB's by Nautilus' public parsers (`parse_quote_tick`,
    /// `parse_trade_tick`), or the error the page ended with (see `drained`).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request is refused.
    /// - The request is not answered whole within the timeout.
    async fn page(
        &self,
        tick_type: IbHistoricalTickType,
        cursor: Timestamp,
    ) -> anyhow::Result<Vec<anyhow::Result<PagedTick>>> {
        let request = async {
            let request = self
                .client
                .historical_ticks(self.contract, TICKS_PER_PAGE)
                .ending(jiff_to_ib_datetime(&cursor))
                .trading_hours(self.trading_hours);
            anyhow::Ok(match tick_type {
                IbHistoricalTickType::BidAsk => {
                    let page = request.bid_ask(IgnoreSize::No).await?;
                    drained(page, |tick: TickBidAsk| self.quote(&tick)).await
                }
                IbHistoricalTickType::Trades => {
                    let page = request.trade().await?;
                    drained(page, |tick: TickLast| self.trade(&tick)).await
                }
            })
        };
        tokio::time::timeout(self.timeout, request)
            .await
            .with_context(|| format!("no whole answer within {:?}", self.timeout))?
    }

    /// The quote of `tick`, at its prices and sizes, as Nautilus' live IB client makes it; none,
    /// logged, if Nautilus' parser refuses it.
    fn quote(&self, tick: &TickBidAsk) -> PagedTick {
        let ts_event = ib_timestamp_to_unix_nanos(&tick.timestamp);
        let quote = parse_quote_tick(
            self.instrument_id,
            Some(tick.price_bid),
            Some(tick.price_ask),
            tick.size_bid,
            tick.size_ask,
            self.prices,
            self.size_precision,
            ts_event,
            ts_event,
        );
        (ts_event, self.kept("quote", quote.map(Data::Quote)))
    }

    /// The trade of `tick`, as Nautilus' live IB client makes it, its id Nautilus' of its time,
    /// price and size; none, logged, for a tick of no size, which Nautilus' IB clients skip, or one
    /// Nautilus' parser refuses.
    fn trade(&self, tick: &TickLast) -> PagedTick {
        let ts_event = ib_timestamp_to_unix_nanos(&tick.timestamp);
        let Some(size) = tick.size else {
            log::warn!(
                "Dropping IB's trade tick of {} at {ts_event}: no size",
                self.instrument_id
            );
            return (ts_event, None);
        };
        let trade = parse_trade_tick(
            self.instrument_id,
            tick.price,
            size,
            self.prices,
            self.size_precision,
            ts_event,
            ts_event,
            None,
        );
        (ts_event, self.kept("trade", trade.map(Data::Trade)))
    }

    /// `parsed`, a tick of `kind` Nautilus' parser made, if it did; none, logged, as Nautilus' IB
    /// clients drop it, if it refused it.
    fn kept(&self, kind: &str, parsed: anyhow::Result<Data>) -> Option<Data> {
        parsed
            .inspect_err(|e| {
                log::warn!("Dropping IB's {kind} tick of {}: {e:#}", self.instrument_id);
            })
            .ok()
    }
}

/// The items of `page`, a subscription to a page of IB's historical ticks, in the order IB sent
/// them, each tick made by `tick`, until the page ends: whole, or with an error, its last item,
/// IB's or ibapi's notice that it dropped frames of the page (`SUBSCRIPTION_LAG_CODE`), ticks lost.
/// Other notices, IB's warnings and data advisories, are logged.
async fn drained<T: TickDecoder<T> + Send + Unpin>(
    mut page: TickSubscription<T>,
    tick: impl Fn(T) -> PagedTick,
) -> Vec<anyhow::Result<PagedTick>> {
    let mut items = Vec::new();
    while let Some(item) = page.next().await {
        match item {
            Ok(SubscriptionItem::Data(data)) => items.push(Ok(tick(data))),
            Ok(SubscriptionItem::Notice(notice)) if notice.code == SUBSCRIPTION_LAG_CODE => {
                items.push(Err(anyhow::anyhow!("ticks lost: {}", notice.message)));
                break;
            }
            Ok(SubscriptionItem::Notice(notice)) => {
                log::info!(
                    "IB's notice on a page of ticks: {} {}",
                    notice.code,
                    notice.message
                );
            }
            Err(e) => {
                items.push(Err(e.into()));
                break;
            }
        }
    }
    items
}

/// The connection to IB on `config` and an instrument provider initialized on it, as Nautilus'
/// `HistoricalInteractiveBrokersClient::connect` makes them, held here so that the ticks' pages are
/// asked on the connection and with the definitions the client's other requests use. `connect`
/// checks the config first (crate-private): the provider's config, which the provider checks
/// itself, and the idle timeout of live subscriptions, which history has none of.
///
/// # Errors
///
/// Returns an error if:
/// - The provider's config is refused.
/// - The connection fails.
/// - The provider cannot be initialized.
async fn connected(
    config: &InteractiveBrokersDataClientConfig,
) -> anyhow::Result<(
    SharedClientHandle,
    Arc<InteractiveBrokersInstrumentProvider>,
)> {
    let provider = Arc::new(InteractiveBrokersInstrumentProvider::new(
        config.instrument_provider.clone(),
    )?);
    let ib = shared_client::get_or_connect(
        &config.host,
        config.port,
        config.client_id,
        config.connection_timeout,
        config.max_reconnect_attempts,
    )
    .await?;
    if config.market_data_type != MarketDataType::Realtime {
        ib.switch_market_data_type(config.market_data_type.into())
            .await?;
    }
    provider
        .initialize_with_client(ib.as_arc().as_ref())
        .await?;
    Ok((ib, provider))
}

/// What IB's history is asked for a series it serves (see `served`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Served {
    /// The bars of a bar type, IB's own (`request_bars`).
    Bars(BarType),
    /// An instrument's historical ticks of a type (`IbHistory::ticks`): its BID_ASK ticks, its
    /// quotes, or its TRADES ticks, its trades.
    Ticks(IbHistoricalTickType, InstrumentId),
}

/// Checks that IB's history serves `source`, the series of an instrument of a venue whose history
/// is IB's (see `served`).
///
/// # Errors
///
/// Returns an error if IB's history does not serve `source`, naming it and saying why.
pub(crate) fn serves(source: &CatalogSeries) -> anyhow::Result<()> {
    served(source).map(drop)
}

/// What IB's history is asked for `series`, as Nautilus' IB clients serve it: IB's own (`EXTERNAL`)
/// time bars of a size IB has (Nautilus' `bar_type_to_ib_bar_size`) but a month, which Nautilus' IB
/// history client reads no bars of (its `request_bars` parses seconds, minutes, hours, days and
/// weeks alone), priced at one of `BAR_PRICES`, and an instrument's quotes and trades, IB's
/// historical ticks (the requests of the historical client's `request_ticks`, the live client's
/// `request_quotes` and `request_trades`). Nautilus' IB clients request no book history.
///
/// # Errors
///
/// Returns an error if IB's history does not serve the series, naming it and saying why.
fn served(series: &CatalogSeries) -> anyhow::Result<Served> {
    match series {
        CatalogSeries::Bars(bar_type) => {
            let spec = bar_type.spec();
            anyhow::ensure!(
                bar_type.is_externally_aggregated()
                    && BAR_PRICES.contains(&spec.price_type)
                    && bar_type_to_ib_bar_size(bar_type).is_ok()
                    && spec.aggregation != BarAggregation::Month,
                "IB's history serves its own (EXTERNAL) time bars of a size IB has and \
                 Nautilus' IB history client reads, 1, 5, 15 or 30 seconds, 1, 2, 3, 5, 10, \
                 15, 20 or 30 minutes, 1, 2, 3, 4 or 8 hours, a day or a week, priced LAST, \
                 BID, ASK or MID, not {bar_type}"
            );
            anyhow::ensure!(
                !(bar_type.spec().price_type == PriceType::Last
                    && bar_type.instrument_id().venue.as_str() == FOREX_VENUE),
                "IB has no TRADES bars of a forex pair, so its history serves no {bar_type}: \
                 read its BID, ASK or MID bars"
            );
            Ok(Served::Bars(*bar_type))
        }
        CatalogSeries::Ticks(tick) if tick.data == NautilusDataType::QuoteTick => Ok(
            Served::Ticks(IbHistoricalTickType::BidAsk, tick.instrument_id),
        ),
        CatalogSeries::Ticks(tick) if tick.data == NautilusDataType::TradeTick => {
            let instrument_id = tick.instrument_id;
            anyhow::ensure!(
                !CURRENCY_PAIR_VENUES.contains(&instrument_id.venue.as_str()),
                "Nautilus' IB client serves no trades of a currency pair, so IB's history \
                 serves no trades of {instrument_id}: read its quotes"
            );
            Ok(Served::Ticks(IbHistoricalTickType::Trades, instrument_id))
        }
        CatalogSeries::Ticks(other) => {
            anyhow::bail!("IB's history serves bars, quotes and trades, not the {other}")
        }
    }
}

/// IB's forex venue, whose pairs IB has no TRADES of, bars or ticks.
const FOREX_VENUE: &str = "IDEALPRO";

/// The venues of the instruments Nautilus' IB client defines as currency pairs, IB's forex (CASH)
/// and crypto (CRYPTO) contracts (`providers/parse.rs`, `parse_forex_contract` and
/// `parse_crypto_contract`, at the pin): its `request_trades` refuses a currency pair's trades by
/// logging an error and answering nothing (`data/core.rs`, `request_trades`), so a warmup asking
/// for them would never end, and its `subscribe_trades` by logging an error and streaming nothing
/// (`data/core.rs:1000-1006`), so a run subscribing to them would hear none. A venue is known
/// before any definition is, as `serves` and `streams` are asked: IB routes crypto to PAXOS and
/// ZEROHASH (`common/symbology.rs`).
const CURRENCY_PAIR_VENUES: [&str; 3] = [FOREX_VENUE, "PAXOS", "ZEROHASH"];

/// The prices of the bars IB's history serves and its live data streams, as Nautilus' IB clients
/// ask IB for them (`price_type_to_ib_what_to_show`, and `price_type_to_ib_realtime_what_to_show`
/// for IB's real-time bars): LAST, IB's TRADES bars, the only ones with a volume, BID and ASK, IB's
/// BID and ASK bars, the history of an IDEALPRO pair, which has no TRADES, and MID, IB's MIDPOINT
/// bars. The adapter asks TRADES for any other price, so it serves and streams none.
const BAR_PRICES: [PriceType; 4] = [
    PriceType::Last,
    PriceType::Bid,
    PriceType::Ask,
    PriceType::Mid,
];

/// The IB duration of one request of bars of `spec`, a size IB has: the longest IB's step-size
/// table admits the size in (TWS API, "Historical Data Limitations", "Step Sizes"), so that IB
/// serves it and a request holds a few thousand bars at most: `1800 S` of 1-second bars, `3600 S`
/// of 5-second bars, `14400 S` of 15-second bars, `28800 S` of 30-second bars, a day of 1-minute
/// bars, two of 2-minute bars, a week of 3- to 20-minute bars, a month of 30-minute and hour bars
/// and a year of daily and weekly bars. A day of 1-minute bars is asked as `86400 S`: `D` counts
/// whole trading days, so a `1 D` request ending in a session's first minute answers that one bar,
/// and the walk back would end there (see `walk_back`); the longer durations end there with a
/// session or more before it.
fn request_duration(spec: BarSpecification) -> &'static str {
    use BarAggregation as A;
    match (spec.aggregation, spec.step.get()) {
        (A::Second, 1) => "1800 S",
        (A::Second, 5) => "3600 S",
        (A::Second, 15) => "14400 S",
        (A::Second, _) => "28800 S",
        (A::Minute, 1) => "86400 S",
        (A::Minute, 2) => "2 D",
        (A::Minute, 3..=20) => "1 W",
        (A::Minute | A::Hour, _) => "1 M",
        _ => "1 Y",
    }
}

/// A request of bars of 30 seconds or less, which IB paces (`SmallBarPacing`): its bar type, naming
/// the contract, its exchange and the data IB shows (its price), and its end and duration, the rest
/// of what makes two requests identical.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SmallBarRequest {
    bar_type: BarType,
    end: Timestamp,
    duration: &'static str,
}

/// IB's hard pacing of the history of bars of 30 seconds or less (TWS API, "Historical Data
/// Limitations", "Pacing Violations for Small Bars"), which Nautilus' IB clients do not pace: no
/// request identical to one sent in the last 15 seconds, fewer than six of one contract, exchange
/// and data shown within two seconds, and no more than 60 within ten minutes. IB counts a BID_ASK
/// request twice; Nautilus asks IB for the BID and the ASK of bars apart, never BID_ASK, so each
/// counts once. The requests sent in the last ten minutes, oldest first.
#[derive(Debug, Default)]
struct SmallBarPacing {
    sent: Vec<(Instant, SmallBarRequest)>,
}

/// How long IB remembers a request, refusing an identical one.
const IDENTICAL_WINDOW: Duration = Duration::from_secs(15);
/// The window of IB's count of the requests of one contract, exchange and data shown, and how many
/// it takes within one.
const SAME_SERIES_WINDOW: Duration = Duration::from_secs(2);
const SAME_SERIES_MOST: usize = 5;
/// The window of IB's count of every request, and how many it takes within one.
const ALL_WINDOW: Duration = Duration::from_secs(600);
const ALL_MOST: usize = 60;

impl SmallBarPacing {
    /// How long `request` waits at `now` before IB takes it, by the requests sent: until 15 seconds
    /// after an identical one, until two seconds after the fifth latest of its contract and price
    /// within two seconds, and until ten minutes after the sixtieth latest within ten minutes; none
    /// if no rule holds it.
    fn wait(&self, now: Instant, request: &SmallBarRequest) -> Duration {
        let after = |at: Instant, window: Duration| (at + window).saturating_duration_since(now);
        let within = |window: Duration, of: &dyn Fn(&SmallBarRequest) -> bool| -> Vec<Instant> {
            self.sent
                .iter()
                .filter(|(at, sent)| now.saturating_duration_since(*at) < window && of(sent))
                .map(|(at, _)| *at)
                .collect()
        };

        // Holds until the earliest of `sent` leaves `window`, if `most` are within it: after it,
        // `most - 1` are, and the request.
        let full = |sent: Vec<Instant>, window: Duration, most: usize| {
            sent.len()
                .checked_sub(most)
                .map_or(Duration::ZERO, |first| after(sent[first], window))
        };
        let identical = full(
            within(IDENTICAL_WINDOW, &|sent| sent == request),
            IDENTICAL_WINDOW,
            1,
        );
        let same_series = full(
            within(SAME_SERIES_WINDOW, &|sent| {
                sent.bar_type.instrument_id() == request.bar_type.instrument_id()
                    && sent.bar_type.spec().price_type == request.bar_type.spec().price_type
            }),
            SAME_SERIES_WINDOW,
            SAME_SERIES_MOST,
        );
        let all = full(within(ALL_WINDOW, &|_| true), ALL_WINDOW, ALL_MOST);
        identical.max(same_series).max(all)
    }

    /// Counts `request` sent at `at`, forgetting those IB no longer counts.
    fn sent(&mut self, at: Instant, request: SmallBarRequest) {
        self.sent
            .retain(|(sent, _)| at.saturating_duration_since(*sent) < ALL_WINDOW);
        self.sent.push((at, request));
    }
}

/// Checks that IB's history holds what a fill of `slices` into `catalog` asks of it at `now`: IB
/// holds no bars of 30 seconds or less older than six months (TWS API, "Historical Data
/// Limitations"), so the writer asks a slice of them from within six months of `now`, rather than
/// storing its older windows empty. The writer asks a slice from where it resumes it
/// (`catalog::resumed`): from its start if the catalog stores none of its series, else 1 ns after
/// their stored end, and nothing if that is past the slice, so a fill run again over a window whose
/// bars are stored is held as far as it asks IB for more.
///
/// # Errors
///
/// Returns an error if:
/// - A slice of bars would be asked from earlier than the oldest start IB holds, naming the bars
///   and that start.
/// - `catalog::resumed` fails.
pub(crate) fn check_held(
    catalog: &ParquetDataCatalog,
    slices: &[SeriesSlice],
    now: Timestamp,
) -> anyhow::Result<()> {
    let oldest = now
        .to_zoned(TimeZone::UTC)
        .checked_sub(6.months())?
        .timestamp();

    for slice in slices {
        let CatalogSeries::Bars(bar_type) = slice.series else {
            continue;
        };

        if bar_type.spec().aggregation == BarAggregation::Second
            && let (Some(from), _) = resumed(catalog, &slice.series, slice.from, slice.to)?
        {
            anyhow::ensure!(
                from >= oldest,
                "IB holds no bars of 30 seconds or less older than six months: {bar_type}, \
                 asked from {from}, reaches before {oldest}"
            );
        }
    }
    Ok(())
}

/// Fails if `end` is later than `IbHistory::latest_end(now)`.
///
/// IB answers a window ending within one bar of now with the bar still in progress; stored, it
/// would never be replaced.
fn ensure_bars_closed(end: Timestamp, now: Timestamp) -> anyhow::Result<()> {
    let latest = IbHistory::latest_end(now)?;
    anyhow::ensure!(
        end <= latest,
        "end {end} is within a minute of now: IB would return the minute still in progress; \
         use an end at or before {latest}"
    );
    Ok(())
}

/// The ticks over `[start, end]`, paged back from `end`: `fetch` sends one request ending at a
/// cursor and answers IB's page, its items in the order IB sent them, each a tick or the error IB
/// ended the page with. IB stamps a tick to the second and reads an end to the second, leaving out
/// the ticks of the second it reads, so the first cursor is the second after `end`'s. A page of
/// `page_size` ticks or more ends where IB's count ran out, its earliest second cut short (probed
/// on the paper gateway on 19 November 2026: a page's earliest second held 1 tick of EUR/USD's 7, 1
/// and 54 there), so its ticks of that second are dropped and the next cursor is the second after
/// it, asking it whole. A shorter page holds all IB has up to its cursor in the session, so it is
/// kept whole and the next cursor is its earliest second. The paging ends on an empty page, IB
/// holding nothing earlier, or once a page reaches back to `start`. Nautilus' IB clients end each
/// next page at the earliest second instead (`extend_historical_tick_batch` in `data/convert.rs`),
/// losing the rest of a cut second. Returns the ticks in `[start, end]`, oldest first, those of one
/// time in IB's order.
///
/// # Errors
///
/// Returns an error if:
/// - A request fails (`fetch`'s error) or a page does (an item's), the ticks paged so far dropped.
/// - A full page reaches back no further than the page before, naming the second: that second alone
///   holds a full page, which IB cannot page past, so the ticks before it cannot be asked.
async fn page_back<T>(
    start: Timestamp,
    end: Timestamp,
    page_size: usize,
    ts_event: impl Fn(&T) -> UnixNanos,
    mut fetch: impl AsyncFnMut(Timestamp) -> anyhow::Result<Vec<anyhow::Result<T>>>,
) -> anyhow::Result<Vec<T>> {
    let second = SignedDuration::from_secs(1);
    let mut ticks: Vec<T> = Vec::new();
    let mut cursor = second_of(end)? + second;
    let mut cut: Option<Timestamp> = None;

    loop {
        let page = fetch(cursor)
            .await
            .with_context(|| format!("requesting the ticks before {cursor}"))?
            .into_iter()
            .collect::<anyhow::Result<Vec<T>>>()
            .with_context(|| format!("reading the ticks before {cursor}"))?;
        let Some(earliest) = page.iter().map(&ts_event).min() else {
            break;
        };
        let earliest = second_of(Timestamp::from(earliest))?;

        if page.len() < page_size {
            ticks.extend(page);

            if earliest <= start {
                break;
            }
            cursor = earliest;
            cut = None;
        } else {
            anyhow::ensure!(
                cut.is_none_or(|cut| earliest < cut),
                "the second of {earliest} holds a full page of ticks, which IB cannot page \
                 past: the ticks before it cannot be asked"
            );
            let whole = UnixNanos::from(earliest + second);
            ticks.extend(page.into_iter().filter(|tick| ts_event(tick) >= whole));

            if earliest < start {
                break;
            }
            cursor = earliest + second;
            cut = Some(earliest);
        }
    }
    let window = UnixNanos::from(start)..=UnixNanos::from(end);
    ticks.retain(|tick| window.contains(&ts_event(tick)));

    // Pages come latest first, each oldest first
    ticks.sort_by_key(&ts_event);
    Ok(ticks)
}

/// The whole second `at` falls in.
///
/// # Errors
///
/// Returns an error if `at` cannot be truncated, out of range.
fn second_of(at: Timestamp) -> anyhow::Result<Timestamp> {
    Ok(at.round(
        TimestampRound::new()
            .smallest(Unit::Second)
            .mode(RoundMode::Trunc),
    )?)
}

/// The bars over `[start, end]`, fetched by walking back from `end`: `fetch` gets one request
/// ending at a cursor, first `end`, then the `ts_init` of the oldest bar so far. Stops once a bar
/// at or before `start` arrived, on an empty answer, or when an answer brings nothing older than
/// its cursor. Returns the bars sorted by `ts_init`, unique.
///
/// One request per cursor, each ending where the previous answer starts, so no hole opens between
/// requests, and each one IB answers within the request timeout: a request by start over the whole
/// window, which the adapter sends as one request between its bounds (nautilus_trader#5621), took
/// 1,164 s for a year of minute bars, against 60 s (measured 2026-11-08). Temporary, as IB's fill,
/// the one thing it serves: it goes with it (helix-trader issue #4).
async fn walk_back(
    start: Timestamp,
    end: Timestamp,
    mut fetch: impl AsyncFnMut(Timestamp) -> anyhow::Result<Vec<Bar>>,
) -> anyhow::Result<Vec<Bar>> {
    let stamp = |bar: &Bar| Timestamp::from(bar.ts_init);
    let mut bars = Vec::new();
    let mut cursor = end;
    loop {
        let answer = fetch(cursor).await?;
        let Some(oldest) = answer.iter().map(stamp).min() else {
            break;
        };
        bars.extend(answer);
        if oldest <= start || oldest >= cursor {
            break;
        }
        cursor = oldest;
    }
    bars.retain(|bar| (start..=end).contains(&stamp(bar)));
    bars.sort_by_key(|bar| bar.ts_init);
    bars.dedup_by_key(|bar| bar.ts_init);
    Ok(bars)
}

/// The IB data client config, the one the live data client uses.
///
/// The full session, not regular trading hours: IB's first regular-hours bar of a day is partial
/// (13:30-14:00 UTC for ES) but the adapter stamps every bar at open + bar size, so that bar would
/// be stamped half an hour after its close. Adapter defaults otherwise: `Simplified` symbology (ids
/// like `ESM24.CME`). Never sets `convert_exchange_to_mic_venue`: it makes the venue of a CME
/// contract change between processes.
fn data_client_config(gateway: IbGateway) -> InteractiveBrokersDataClientConfig {
    InteractiveBrokersDataClientConfig {
        host: gateway.host,
        port: gateway.port,
        client_id: gateway.client_id,
        use_regular_trading_hours: false,
        ..Default::default()
    }
}

/// The ids of `requested` missing from `loaded`.
fn missing_ids(requested: &[InstrumentId], loaded: &[InstrumentAny]) -> Vec<InstrumentId> {
    requested
        .iter()
        .filter(|id| !loaded.iter().any(|instrument| instrument.id() == **id))
        .copied()
        .collect()
}

/// The IB contract of the future `id` with `includeExpired`, the form IB resolves an expired future
/// by; `None` if `id` is not a future. IB also needs the root symbol (`ES` of `ESU5`), which
/// `Symbology::contract` leaves empty.
fn expired_future_contract(symbology: Symbology, id: InstrumentId) -> Option<Contract> {
    let mut contract = symbology.contract(id, None).ok()?;
    let root = parse_futures_symbol(&contract.local_symbol)?.root;
    contract.symbol = Symbol(root.to_string());
    contract.include_expired = true;
    Some(contract)
}

/// Fails naming every id of `requested` missing from `loaded`.
fn ensure_all_loaded(requested: &[InstrumentId], loaded: &[InstrumentAny]) -> anyhow::Result<()> {
    let missing: Vec<String> = missing_ids(requested, loaded)
        .iter()
        .map(ToString::to_string)
        .collect();
    anyhow::ensure!(
        missing.is_empty(),
        "IB returned no instrument for {} (unknown or expired contract)",
        missing.join(", ")
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use nautilus_model::instruments::stubs::futures_contract_es;
    use rstest::rstest;

    use super::*;

    fn es() -> InstrumentAny {
        InstrumentAny::FuturesContract(futures_contract_es(None, None))
    }

    /// A gateway for the configs; nothing listens there.
    fn closed_gateway() -> IbGateway {
        IbGateway {
            host: "127.0.0.1".to_string(),
            port: 1,
            client_id: 1,
        }
    }

    /// Starting the gateway would fail without credentials or Docker, or start a container: the end
    /// is checked first.
    #[tokio::test]
    async fn connect_rejects_an_end_within_a_minute_of_now_before_starting_the_gateway() {
        let now = at("2024-06-10T12:00:00Z");
        let end = at("2024-06-10T11:59:30Z");
        let Err(e) = IbHistory::connect(1, end, now).await else {
            panic!("connected");
        };
        let err = e.to_string();
        assert!(
            err.contains("2024-06-10T11:59:30Z") && err.contains("2024-06-10T11:59:00Z"),
            "{err}"
        );
    }

    fn at(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    const HOUR_NS: u64 = 3_600_000_000_000;

    /// The hour `h` after the epoch.
    fn hour(h: u64) -> Timestamp {
        Timestamp::from(UnixNanos::from(h * HOUR_NS))
    }

    /// A 1-HOUR bar of ESM24 closing at hour `h`.
    fn hourly(h: u64) -> Bar {
        use nautilus_model::types::{Price, Quantity};
        let p = Price::from("5000.00");
        let ts = UnixNanos::from(h * HOUR_NS);
        let bt = bar_type("ESM24.CME-1-HOUR-LAST-EXTERNAL");
        Bar::new(bt, p, p, p, p, Quantity::from(1), ts, ts)
    }

    fn hours(bars: &[Bar]) -> Vec<u64> {
        bars.iter()
            .map(|bar| bar.ts_init.as_u64() / HOUR_NS)
            .collect()
    }

    /// A source holding a bar at each hour of `held` that answers a request ending at `cursor` with
    /// the latest `per_answer` bars at or before it, oldest first, as IB does; records each cursor.
    fn source(
        held: std::ops::RangeInclusive<u64>,
        per_answer: usize,
        cursors: &mut Vec<Timestamp>,
    ) -> impl AsyncFnMut(Timestamp) -> anyhow::Result<Vec<Bar>> {
        async move |cursor: Timestamp| {
            cursors.push(cursor);
            let mut answer: Vec<Bar> = held
                .clone()
                .map(hourly)
                .filter(|bar| Timestamp::from(bar.ts_init) <= cursor)
                .collect();
            answer.drain(..answer.len().saturating_sub(per_answer));
            Ok(answer)
        }
    }

    /// A tick of the pager's tests: when it was made, and a name telling ticks of one second apart.
    type Tick = (UnixNanos, &'static str);

    /// `time`, `HH:MM:SS` or finer, on 10 November 2026.
    fn on_the_10th(time: &str) -> Timestamp {
        at(&format!("2026-11-10T{time}Z"))
    }

    /// A tick named `name` made at `time` on the 10th.
    fn tick_at(time: &str, name: &'static str) -> Tick {
        (UnixNanos::from(on_the_10th(time)), name)
    }

    /// Answers the pages' requests with `pages` in turn, each a request's answer, recording each
    /// cursor; a request past the last answer is an error.
    fn pages(
        pages: Vec<anyhow::Result<Vec<anyhow::Result<Tick>>>>,
        cursors: &mut Vec<Timestamp>,
    ) -> impl AsyncFnMut(Timestamp) -> anyhow::Result<Vec<anyhow::Result<Tick>>> {
        let mut pages = pages.into_iter();
        async move |cursor: Timestamp| {
            cursors.push(cursor);
            pages.next().context("no page left")?
        }
    }

    /// Pages the ticks of `[10:00:00, 10:00:10]` on the 10th through `fetch`, a page of three ticks
    /// or more being full.
    async fn paged(
        fetch: impl AsyncFnMut(Timestamp) -> anyhow::Result<Vec<anyhow::Result<Tick>>>,
    ) -> anyhow::Result<Vec<&'static str>> {
        let ticks = page_back(
            on_the_10th("10:00:00"),
            on_the_10th("10:00:10"),
            3,
            |tick: &Tick| tick.0,
            fetch,
        )
        .await?;
        Ok(ticks.into_iter().map(|(_, name)| name).collect())
    }

    /// Worked by hand, IB leaving out the second a request ends at and cutting a full page's
    /// earliest second short, as the paper gateway does: the first page ends at 10:00:11, holding
    /// 10:00:10's d, and only c2 of 10:00:07, which is dropped; the next ends at 10:00:08 and holds
    /// 10:00:07 whole, c1 and c2, and a2 of 10:00:05, cut, dropped; the next ends at 10:00:06,
    /// holding a1 and a2 of 10:00:05 and z of 10:00:00, cut; the last, ending at 10:00:01, holds
    /// 10:00:00 whole, y and z, and x of 09:59:59, before the window. The ticks of the window,
    /// oldest first, each once.
    #[tokio::test]
    async fn ticks_are_paged_back_to_the_windows_start_each_once() {
        let mut cursors = Vec::new();
        let answers = vec![
            Ok(vec![
                Ok(tick_at("10:00:07", "c2")),
                Ok(tick_at("10:00:08", "b")),
                Ok(tick_at("10:00:10", "d")),
            ]),
            Ok(vec![
                Ok(tick_at("10:00:05", "a2")),
                Ok(tick_at("10:00:07", "c1")),
                Ok(tick_at("10:00:07", "c2")),
            ]),
            Ok(vec![
                Ok(tick_at("10:00:00", "z")),
                Ok(tick_at("10:00:05", "a1")),
                Ok(tick_at("10:00:05", "a2")),
            ]),
            Ok(vec![
                Ok(tick_at("09:59:59", "x")),
                Ok(tick_at("10:00:00", "y")),
                Ok(tick_at("10:00:00", "z")),
            ]),
        ];

        let ticks = paged(pages(answers, &mut cursors)).await.unwrap();

        assert_eq!(ticks, ["y", "z", "a1", "a2", "c1", "c2", "b", "d"]);
        assert_eq!(
            cursors,
            [
                on_the_10th("10:00:11"),
                on_the_10th("10:00:08"),
                on_the_10th("10:00:06"),
                on_the_10th("10:00:01"),
            ]
        );
    }

    /// A page shorter than a full one holds all IB has up to its cursor in its session, its
    /// earliest second whole: kept, and the next page ends at that second, before it.
    #[tokio::test]
    async fn a_short_page_is_kept_whole_and_the_next_ends_at_its_earliest_second() {
        let mut cursors = Vec::new();
        let answers = vec![
            Ok(vec![
                Ok(tick_at("10:00:08", "b")),
                Ok(tick_at("10:00:09", "c")),
            ]),
            Ok(vec![Ok(tick_at("10:00:00", "a"))]),
        ];

        let ticks = paged(pages(answers, &mut cursors)).await.unwrap();

        assert_eq!(ticks, ["a", "b", "c"]);
        assert_eq!(cursors, [on_the_10th("10:00:11"), on_the_10th("10:00:08")]);
    }

    /// An empty page with no error: IB holds nothing earlier, and the ticks paged so far are the
    /// window's.
    #[tokio::test]
    async fn an_empty_page_ends_the_paging() {
        let mut cursors = Vec::new();
        let answers = vec![
            Ok(vec![
                Ok(tick_at("10:00:09", "a")),
                Ok(tick_at("10:00:10", "b")),
            ]),
            Ok(vec![]),
        ];

        let ticks = paged(pages(answers, &mut cursors)).await.unwrap();

        assert_eq!(ticks, ["a", "b"]);
        assert_eq!(cursors, [on_the_10th("10:00:11"), on_the_10th("10:00:09")]);
    }

    /// The second page fails: IB ends it with an error after one tick, or its request fails, sent
    /// or timed out. The paging fails with the error, rather than answering the ticks it got.
    #[rstest]
    #[case::ib_ending_a_page_with_an_error(
        Ok(vec![
            Ok(tick_at("10:00:05", "c")),
            Err(anyhow::anyhow!("162: Historical Market Data Service error")),
        ]),
        "Historical Market Data Service error"
    )]
    #[case::a_failed_request(Err(anyhow::anyhow!("not connected")), "not connected")]
    #[tokio::test]
    async fn an_error_ending_a_page_fails_the_paging(
        #[case] second: anyhow::Result<Vec<anyhow::Result<Tick>>>,
        #[case] error: &str,
    ) {
        let mut cursors = Vec::new();
        let answers = vec![
            Ok(vec![
                Ok(tick_at("10:00:09", "a")),
                Ok(tick_at("10:00:10", "b")),
            ]),
            second,
        ];

        let err = paged(pages(answers, &mut cursors)).await.unwrap_err();

        assert!(format!("{err:#}").contains(error), "{err:#}");
    }

    /// The first page is full, its 10:00:08 cut; the page ending at 10:00:09, asking that second
    /// whole, holds only ticks of 10:00:08 and is full too: that second holds a full page, which IB
    /// cannot page past, so the ticks before it cannot be asked and the paging fails, naming the
    /// second.
    #[tokio::test]
    async fn a_second_holding_a_full_page_fails_the_paging_naming_it() {
        let mut cursors = Vec::new();
        let answers = vec![
            Ok(vec![
                Ok(tick_at("10:00:08", "a")),
                Ok(tick_at("10:00:09", "b")),
                Ok(tick_at("10:00:10", "c")),
            ]),
            Ok(vec![
                Ok(tick_at("10:00:08", "a")),
                Ok(tick_at("10:00:08", "d")),
                Ok(tick_at("10:00:08", "e")),
            ]),
        ];

        let err = paged(pages(answers, &mut cursors)).await.unwrap_err();

        assert!(
            format!("{err:#}").contains("2026-11-10T10:00:08Z"),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn walks_back_from_end_with_each_request_ending_at_the_oldest_bar_so_far() {
        let mut cursors = Vec::new();
        let bars = walk_back(hour(5), hour(25), source(1..=30, 8, &mut cursors))
            .await
            .unwrap();
        assert_eq!(hours(&bars), (5..=25).collect::<Vec<_>>());
        assert_eq!(cursors, [hour(25), hour(18), hour(11)]);
    }

    /// Answers the requests with `answers` in turn, recording each cursor; a request past the last
    /// answer is an error.
    fn canned(
        answers: Vec<Vec<u64>>,
        cursors: &mut Vec<Timestamp>,
    ) -> impl AsyncFnMut(Timestamp) -> anyhow::Result<Vec<Bar>> {
        let mut answers = answers.into_iter();
        async move |cursor: Timestamp| {
            cursors.push(cursor);
            let answer = answers.next().context("no answer left")?;
            Ok(answer.into_iter().map(hourly).collect())
        }
    }

    #[tokio::test]
    async fn stops_once_a_bar_at_start_arrived() {
        let mut cursors = Vec::new();
        let bars = walk_back(hour(17), hour(25), source(1..=30, 5, &mut cursors))
            .await
            .unwrap();
        assert_eq!(hours(&bars), (17..=25).collect::<Vec<_>>());
        assert_eq!(cursors, [hour(25), hour(21)]);
    }

    /// A walk from hour 5 to hour 25 over canned answers: it asks again from the oldest bar so far
    /// until an answer is empty, brings nothing older than its cursor, or reaches the start, and
    /// keeps the bars from start to end, sorted and unique.
    #[rstest]
    #[case::stopping_on_an_empty_answer(
        vec![(20..=25).collect(), vec![]],
        (20..=25).collect(),
        vec![hour(25), hour(20)]
    )]
    #[case::empty_on_an_empty_first_answer(vec![vec![]], vec![], vec![hour(25)])]
    #[case::stopping_when_an_answer_brings_nothing_older_than_its_cursor(
        vec![(20..=25).collect(), (20..=25).collect(), vec![]],
        (20..=25).collect(),
        vec![hour(25), hour(20)]
    )]
    #[case::keeping_only_bars_from_start_to_end(
        vec![(3..=27).collect()],
        (5..=25).collect(),
        vec![hour(25)]
    )]
    #[case::sorting_the_bars_and_keeping_each_once(
        vec![vec![25, 23, 24, 22], vec![20, 22, 21, 21], vec![4]],
        (20..=25).collect(),
        vec![hour(25), hour(22), hour(20)]
    )]
    #[tokio::test]
    async fn walks_back_over_its_answers_to_the_bars_from_start_to_end(
        #[case] answers: Vec<Vec<u64>>,
        #[case] walked: Vec<u64>,
        #[case] asked: Vec<Timestamp>,
    ) {
        let mut cursors = Vec::new();
        let bars = walk_back(hour(5), hour(25), canned(answers, &mut cursors))
            .await
            .unwrap();
        assert_eq!(hours(&bars), walked);
        assert_eq!(cursors, asked);
    }

    #[tokio::test]
    async fn a_failed_request_fails_the_walk() {
        let mut cursors = Vec::new();
        let answers = vec![(20..=25).collect()];
        let err = walk_back(hour(5), hour(25), canned(answers, &mut cursors))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no answer left"), "{err:#}");
    }

    #[rstest]
    fn history_requests_use_the_regular_hours_setting_of_the_data_client_config() {
        let config = data_client_config(closed_gateway());
        let requests = HistoryRequests::of(&config);
        assert!(!requests.use_rth);
        assert_eq!(requests.timeout_secs, config.request_timeout);
        let regular = InteractiveBrokersDataClientConfig {
            use_regular_trading_hours: true,
            request_timeout: 7,
            ..config
        };
        let requests = HistoryRequests::of(&regular);
        assert!(requests.use_rth);
        assert_eq!(requests.timeout_secs, 7);
    }

    fn test_series(kind: NautilusDataType, identifier: &str) -> CatalogSeries {
        if kind == NautilusDataType::Bar {
            CatalogSeries::Bars(bar_type(identifier))
        } else {
            CatalogSeries::Ticks(crate::strategy::requirements::TickSeries {
                data: kind,
                instrument_id: InstrumentId::from(identifier),
            })
        }
    }

    /// IB's own 1-minute TRADES bars; an IDEALPRO pair, which IB serves no TRADES bars of, has its
    /// own BID and ASK bars, of any size IB has, and IB serves MIDPOINT bars too.
    #[rstest]
    #[case::an_etf_s_last("SXR8.IBIS-1-MINUTE-LAST-EXTERNAL")]
    #[case::a_future_s_last("ESZ26.CME-1-MINUTE-LAST-EXTERNAL")]
    #[case::a_pair_s_bid("EUR/USD.IDEALPRO-1-MINUTE-BID-EXTERNAL")]
    #[case::a_pair_s_ask("EUR/USD.IDEALPRO-1-MINUTE-ASK-EXTERNAL")]
    #[case::a_pair_s_hourly_bid("EUR/USD.IDEALPRO-1-HOUR-BID-EXTERNAL")]
    #[case::a_pair_s_5_minute_ask("EUR/USD.IDEALPRO-5-MINUTE-ASK-EXTERNAL")]
    #[case::a_pair_s_1_second_bid("EUR/USD.IDEALPRO-1-SECOND-BID-EXTERNAL")]
    #[case::a_pair_s_mid("EUR/USD.IDEALPRO-1-MINUTE-MID-EXTERNAL")]
    #[case::an_etf_s_mid("SXR8.IBIS-1-MINUTE-MID-EXTERNAL")]
    #[case::an_etf_s_daily_bid("SXR8.IBIS-1-DAY-BID-EXTERNAL")]
    fn ib_serves_its_own_last_bid_ask_and_mid_bars_of_any_size_it_has(#[case] identifier: &str) {
        let served = served(&CatalogSeries::Bars(bar_type(identifier))).unwrap();
        assert_eq!(served, Served::Bars(bar_type(identifier)));
    }

    /// IB's historical ticks: BID_ASK for quotes, TRADES for trades, of the instrument the series
    /// names.
    #[rstest]
    fn ib_serves_quotes_and_trades_as_its_historical_ticks() {
        let sxr8 = InstrumentId::from("SXR8.IBIS");
        assert_eq!(
            served(&test_series(NautilusDataType::QuoteTick, "SXR8.IBIS")).unwrap(),
            Served::Ticks(IbHistoricalTickType::BidAsk, sxr8)
        );
        assert_eq!(
            served(&test_series(NautilusDataType::TradeTick, "SXR8.IBIS")).unwrap(),
            Served::Ticks(IbHistoricalTickType::Trades, sxr8)
        );
    }

    /// The trades of an instrument Nautilus' IB client defines as a currency pair, a forex pair of
    /// IDEALPRO's or a crypto pair of PAXOS's or ZEROHASH's, which its `request_trades` refuses
    /// without an answer, and the TRADES bars of an IDEALPRO pair, which IB has none of.
    #[rstest]
    #[case::a_forex_pair_s_trades(NautilusDataType::TradeTick, "EUR/USD.IDEALPRO")]
    #[case::a_paxos_crypto_pair_s_trades(NautilusDataType::TradeTick, "BTC/USD.PAXOS")]
    #[case::a_zerohash_crypto_pair_s_trades(NautilusDataType::TradeTick, "BTC/USD.ZEROHASH")]
    #[case::a_forex_pair_s_trades_bars(
        NautilusDataType::Bar,
        "EUR/USD.IDEALPRO-1-MINUTE-LAST-EXTERNAL"
    )]
    fn ib_serves_no_trades_of_a_currency_pair_and_no_trades_bars_of_a_forex_pair(
        #[case] data_type: NautilusDataType,
        #[case] identifier: &str,
    ) {
        let err = served(&test_series(data_type, identifier)).unwrap_err();
        assert!(err.to_string().contains(identifier), "{err}");
    }

    /// A crypto pair's trade-price bars are served (IB's AGGTRADES), as are a currency pair's
    /// quotes and other instruments' trades.
    #[rstest]
    #[case::a_crypto_pair_s_trades_bars(
        NautilusDataType::Bar,
        "BTC/USD.PAXOS-1-MINUTE-LAST-EXTERNAL"
    )]
    #[case::a_forex_pair_s_quotes(NautilusDataType::QuoteTick, "EUR/USD.IDEALPRO")]
    #[case::a_crypto_pair_s_quotes(NautilusDataType::QuoteTick, "BTC/USD.ZEROHASH")]
    #[case::a_future_s_trades(NautilusDataType::TradeTick, "ESZ26.CME")]
    fn ib_serves_a_currency_pair_s_quotes_and_trade_priced_crypto_bars_and_other_trades(
        #[case] data_type: NautilusDataType,
        #[case] identifier: &str,
    ) {
        served(&test_series(data_type, identifier)).unwrap();
    }

    /// A price IB's adapter would ask TRADES for, a size IB has not, monthly bars, which Nautilus'
    /// IB history client parses none of, bars Nautilus builds, and books, which Nautilus' IB
    /// clients request none of.
    #[rstest]
    #[case::a_mark_price(NautilusDataType::Bar, "EUR/USD.IDEALPRO-1-MINUTE-MARK-EXTERNAL")]
    #[case::four_minutes(NautilusDataType::Bar, "SXR8.IBIS-4-MINUTE-LAST-EXTERNAL")]
    #[case::two_seconds(NautilusDataType::Bar, "SXR8.IBIS-2-SECOND-LAST-EXTERNAL")]
    #[case::ten_seconds(NautilusDataType::Bar, "SXR8.IBIS-10-SECOND-LAST-EXTERNAL")]
    #[case::six_hours(NautilusDataType::Bar, "SXR8.IBIS-6-HOUR-LAST-EXTERNAL")]
    #[case::a_month(NautilusDataType::Bar, "SXR8.IBIS-1-MONTH-LAST-EXTERNAL")]
    #[case::internal_last_bars(NautilusDataType::Bar, "SXR8.IBIS-1-MINUTE-LAST-INTERNAL")]
    #[case::internal_bid_bars(NautilusDataType::Bar, "EUR/USD.IDEALPRO-1-MINUTE-BID-INTERNAL")]
    #[case::internal_ask_bars(NautilusDataType::Bar, "EUR/USD.IDEALPRO-1-MINUTE-ASK-INTERNAL")]
    #[case::book_deltas(NautilusDataType::OrderBookDelta, "SXR8.IBIS")]
    #[case::book_depth(NautilusDataType::OrderBookDepth, "SXR8.IBIS")]
    fn ib_serves_no_other_series(#[case] data_type: NautilusDataType, #[case] identifier: &str) {
        let err = served(&test_series(data_type, identifier)).unwrap_err();
        assert!(err.to_string().contains(identifier), "{err}");
    }

    /// IB's step-size table ("Historical Data Limitations"): the longest duration whose row admits
    /// each size IB has, a day of 1-minute bars in seconds.
    #[rstest]
    #[case("1-SECOND", "1800 S")]
    #[case("5-SECOND", "3600 S")]
    #[case("15-SECOND", "14400 S")]
    #[case("30-SECOND", "28800 S")]
    #[case("1-MINUTE", "86400 S")]
    #[case("2-MINUTE", "2 D")]
    #[case("3-MINUTE", "1 W")]
    #[case("5-MINUTE", "1 W")]
    #[case("10-MINUTE", "1 W")]
    #[case("15-MINUTE", "1 W")]
    #[case("20-MINUTE", "1 W")]
    #[case("30-MINUTE", "1 M")]
    #[case("1-HOUR", "1 M")]
    #[case("2-HOUR", "1 M")]
    #[case("3-HOUR", "1 M")]
    #[case("4-HOUR", "1 M")]
    #[case("8-HOUR", "1 M")]
    #[case("1-DAY", "1 Y")]
    #[case("1-WEEK", "1 Y")]
    fn each_bar_size_is_asked_over_the_longest_duration_ibs_step_sizes_admit_it_in(
        #[case] bars: &str,
        #[case] duration: &str,
    ) {
        let spec = bar_type(&format!("SXR8.IBIS-{bars}-LAST-EXTERNAL")).spec();
        assert_eq!(request_duration(spec), duration);
    }

    /// A request of `bars`, 1-second bars of SXR8 priced `price`, ending at second `end` of the
    /// epoch.
    fn small(price: &str, end: i64) -> SmallBarRequest {
        SmallBarRequest {
            bar_type: bar_type(&format!("SXR8.IBIS-1-SECOND-{price}-EXTERNAL")),
            end: Timestamp::from_second(end).unwrap(),
            duration: "1800 S",
        }
    }

    /// `requests`, each sent at its offset from `start`.
    fn pacing_of(start: Instant, requests: &[(u64, SmallBarRequest)]) -> SmallBarPacing {
        let mut pacing = SmallBarPacing::default();
        for &(at, request) in requests {
            pacing.sent(start + Duration::from_millis(at), request);
        }
        pacing
    }

    /// Worked by hand: a request identical to one sent at 0 waits until 15 s, at 10 s 5 s more; one
    /// of another end does not.
    #[rstest]
    fn an_identical_small_bar_request_waits_15_seconds_after_the_first() {
        let start = Instant::now();
        let pacing = pacing_of(start, &[(0, small("LAST", 3600))]);
        let at_10_s = start + Duration::from_secs(10);

        assert_eq!(
            pacing.wait(at_10_s, &small("LAST", 3600)),
            Duration::from_secs(5)
        );
        assert_eq!(pacing.wait(at_10_s, &small("LAST", 1800)), Duration::ZERO);
        assert_eq!(
            pacing.wait(start + Duration::from_secs(15), &small("LAST", 3600)),
            Duration::ZERO
        );
    }

    /// Worked by hand: five requests of SXR8's LAST bars sent at 0, 100, 200, 300 and 400 ms; a
    /// sixth within two seconds of the first would be IB's sixth, so at 500 ms it waits until the
    /// first is 2 s old, 1.5 s; one of SXR8's BID bars, another data shown, does not wait.
    #[rstest]
    fn a_sixth_small_bar_request_of_a_series_within_two_seconds_waits() {
        let start = Instant::now();
        let sent: Vec<(u64, SmallBarRequest)> = (0..5)
            .map(|i| (i * 100, small("LAST", 3600 - i64::try_from(i).unwrap())))
            .collect();
        let pacing = pacing_of(start, &sent);
        let at_500_ms = start + Duration::from_millis(500);

        assert_eq!(
            pacing.wait(at_500_ms, &small("LAST", 100)),
            Duration::from_millis(1500)
        );
        assert_eq!(pacing.wait(at_500_ms, &small("BID", 100)), Duration::ZERO);
    }

    /// Worked by hand: 60 requests, of two series apart, one every 3 s from 0 to 177 s; the 61st
    /// within ten minutes of the first, at 180 s, waits until the first is ten minutes old, 420 s;
    /// at 600 s the first is out of the count and the 61st goes.
    #[rstest]
    fn a_61st_small_bar_request_within_ten_minutes_waits() {
        let start = Instant::now();
        let sent: Vec<(u64, SmallBarRequest)> = (0..60)
            .map(|i| {
                let price = if i % 2 == 0 { "BID" } else { "ASK" };
                (i * 3000, small(price, 3600 - i64::try_from(i).unwrap()))
            })
            .collect();
        let pacing = pacing_of(start, &sent);

        assert_eq!(
            pacing.wait(start + Duration::from_secs(180), &small("LAST", 100)),
            Duration::from_secs(420)
        );
        assert_eq!(
            pacing.wait(start + Duration::from_secs(600), &small("LAST", 100)),
            Duration::ZERO
        );
    }

    /// The slices of `series`, each over `[from, to]` on its instrument.
    fn slices_of(series: &[CatalogSeries], from: &str) -> Vec<SeriesSlice> {
        series
            .iter()
            .map(|series| SeriesSlice {
                series: series.clone(),
                from: at(from),
                to: at("2026-11-14T00:00:00Z"),
            })
            .collect()
    }

    /// On 15 November 2026 IB holds 30-second bars from 15 May on, six months before: a fill of
    /// them from 16 May is held, one from 14 May is refused, naming the bars and the start IB
    /// holds; 1-minute bars and quotes from 14 May are held, IB keeping them longer.
    #[rstest]
    fn small_bars_older_than_six_months_are_refused_naming_the_oldest_ib_holds() {
        let now = at("2026-11-15T12:00:00Z");
        let small_bars = "SXR8.IBIS-30-SECOND-LAST-EXTERNAL";
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let held = |from| {
            check_held(
                &catalog,
                &slices_of(&[CatalogSeries::Bars(BarType::from(small_bars))], from),
                now,
            )
        };

        held("2026-05-16T00:00:00Z").unwrap();
        let err = held("2026-05-14T00:00:00Z").unwrap_err().to_string();
        assert!(
            err.contains(small_bars) && err.contains("2026-05-15T12:00:00Z"),
            "{err}"
        );
        check_held(
            &catalog,
            &slices_of(
                &[
                    CatalogSeries::Bars(BarType::from("SXR8.IBIS-1-MINUTE-LAST-EXTERNAL")),
                    CatalogSeries::Ticks(crate::strategy::requirements::TickSeries {
                        data: NautilusDataType::QuoteTick,
                        instrument_id: InstrumentId::from("SXR8.IBIS"),
                    }),
                ],
                "2026-05-14T00:00:00Z",
            ),
            now,
        )
        .unwrap();
    }

    /// Stores 5-second LAST bars of SXR8 at `from` and `to` in `catalog`, over `[from, to]`.
    fn store_small_bars(catalog: &ParquetDataCatalog, from: &str, to: &str) {
        use nautilus_model::types::{Price, Quantity};
        let price = Price::from("100.00");
        let bars: Vec<Bar> = [from, to]
            .map(|time| {
                let ts = UnixNanos::from(at(time));
                Bar::new(
                    bar_type("SXR8.IBIS-5-SECOND-LAST-EXTERNAL"),
                    price,
                    price,
                    price,
                    price,
                    Quantity::from(1),
                    ts,
                    ts,
                )
            })
            .to_vec();
        catalog
            .write_to_parquet(&bars, Some(at(from).into()), Some(at(to).into()), None)
            .unwrap();
    }

    /// A fill of stored bars asks IB from 1 ns after their stored end, the writer's resume
    /// (`catalog::fill_catalog`), whatever its slice's start. On 30 November 2026 IB holds small
    /// bars from 30 May on: 5-second bars stored over 20 May to 20 June are filled again from 20
    /// June, held, though the slice starts on 20 May; stored over 1 April to 1 May, they would be
    /// filled again from 1 May, refused, naming them, though the slice starts on 15 June.
    #[rstest]
    fn small_bars_stored_are_held_from_where_the_writer_resumes_them() {
        let now = at("2026-11-30T12:00:00Z");
        let small_bars = "SXR8.IBIS-5-SECOND-LAST-EXTERNAL";
        let held = |stored: [&str; 2], from: &str| {
            let dir = tempfile::tempdir().unwrap();
            let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
            store_small_bars(&catalog, stored[0], stored[1]);
            check_held(
                &catalog,
                &slices_of(&[CatalogSeries::Bars(BarType::from(small_bars))], from),
                now,
            )
        };

        held(
            ["2026-05-20T00:00:00Z", "2026-06-20T00:00:00Z"],
            "2026-05-20T00:00:00Z",
        )
        .unwrap();
        let err = held(
            ["2026-04-01T00:00:00Z", "2026-05-01T00:00:00Z"],
            "2026-06-15T00:00:00Z",
        )
        .unwrap_err()
        .to_string();
        assert!(
            err.contains(small_bars) && err.contains("2026-05-30T12:00:00Z"),
            "{err}"
        );
    }

    /// A fill over a window whose small bars the catalog stores to its end asks IB nothing, the
    /// writer resuming past the window (`catalog::resumed`): 5-second bars stored over 1 April to 1
    /// May 2026, filled again over the same window on 30 November, are held, though the window is
    /// all before 30 May, the oldest IB holds.
    #[rstest]
    fn small_bars_stored_over_the_whole_window_are_held_whenever() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let (from, to) = ("2026-04-01T00:00:00Z", "2026-05-01T00:00:00Z");
        store_small_bars(&catalog, from, to);
        let slice = SeriesSlice {
            series: CatalogSeries::Bars(BarType::from("SXR8.IBIS-5-SECOND-LAST-EXTERNAL")),
            from: at(from),
            to: at(to),
        };

        check_held(&catalog, &[slice], at("2026-11-30T12:00:00Z")).unwrap();
    }

    #[rstest]
    fn the_latest_end_is_a_minute_before_now() {
        let now = at("2024-06-10T12:00:00Z");
        assert_eq!(
            IbHistory::latest_end(now).unwrap(),
            at("2024-06-10T11:59:00Z")
        );
    }

    #[rstest]
    fn accepts_an_end_at_least_a_minute_before_now() {
        let now = at("2024-06-10T12:00:00Z");
        ensure_bars_closed(at("2024-06-10T11:59:00Z"), now).unwrap();
        ensure_bars_closed(at("2024-06-01T00:00:00Z"), now).unwrap();
    }

    #[rstest]
    fn rejects_an_end_within_a_minute_of_now() {
        let now = at("2024-06-10T12:00:00Z");
        let err = ensure_bars_closed(at("2024-06-10T11:59:00.000000001Z"), now)
            .unwrap_err()
            .to_string();
        assert!(err.contains("2024-06-10T11:59:00.000000001Z"), "{err}");
        assert!(err.contains("2024-06-10T11:59:00Z"), "{err}");
    }

    #[rstest]
    fn accepts_instruments_when_every_requested_id_loaded() {
        let requested = [InstrumentId::from("ESZ21.GLBX")];
        ensure_all_loaded(&requested, &[es()]).unwrap();
    }

    #[rstest]
    fn names_the_requested_ids_ib_did_not_return() {
        let requested = [
            InstrumentId::from("ESH24.CME"),
            InstrumentId::from("ESZ21.GLBX"),
            InstrumentId::from("ESM24.CME"),
        ];
        let err = ensure_all_loaded(&requested, &[es()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("ESH24.CME, ESM24.CME"), "{err}");
        assert!(!err.contains("ESZ21.GLBX"), "{err}");
    }

    fn bar_type(s: &str) -> BarType {
        s.parse().unwrap()
    }

    fn symbology() -> Symbology {
        Symbology::new(
            data_client_config(closed_gateway())
                .instrument_provider
                .symbology_method,
        )
    }

    #[rstest]
    fn an_expired_future_is_asked_for_as_an_explicit_contract() {
        let contract =
            expired_future_contract(symbology(), InstrumentId::from("ESU25.CME")).unwrap();
        assert_eq!(contract.symbol.to_string(), "ES");
        assert_eq!(contract.local_symbol, "ESU5");
        assert!(contract.include_expired);
    }

    #[rstest]
    #[case::a_stock("AAPL.SMART")]
    #[case::a_forex_pair("EUR/USD.IDEALPRO")]
    #[case::a_stock_whose_symbol_reads_like_a_future("ABCZ5.SMART")]
    fn only_futures_are_asked_for_as_expired_contracts(#[case] id: &str) {
        assert!(expired_future_contract(symbology(), InstrumentId::from(id)).is_none());
    }
}
