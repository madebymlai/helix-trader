//! Interactive Brokers: the account, the gateway (see `gateway`) and the clients.
//!
//! History and live share one data client config. History fills the catalog for `fill_catalog`
//! (`serves`, `IbHistory::data`). The live node uses the clients (`live`, `streams`). See
//! `README.md`.
//!
//! Temporary: IB's data client answers without a `DataResponseEnvelope`, so Nautilus writes none
//! of IB's answers back, and `IbHistory::fill` fetches and writes them itself. Once
//! nautilus_trader#5472 is fixed, IB fills through the node as LSE and IC do (see
//! `catalog::NodeFill`), and `fill` and `data` go: helix-trader#4
//! (<https://github.com/madebymlai/helix-trader/issues/4>).
//!
//! History uses `HistoricalInteractiveBrokersClient`, which needs no node, on the connection that
//! `connected` makes.

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

/// IB's settings of a catalog fill.
#[derive(Debug, Args)]
pub(crate) struct IbHistoryArgs {
    /// IB client id of this process, not the adapter default 1, so a live node with the default id
    /// can run at the same time.
    #[arg(long, default_value_t = 10)]
    pub(super) client_id: i32,
}

/// A connection to IB for history, with no node.
pub struct IbHistory {
    client: HistoricalInteractiveBrokersClient,
    ib: SharedClientHandle,
    provider: Arc<InteractiveBrokersInstrumentProvider>,
    requests: HistoryRequests,
    symbology: Symbology,
    small_bars: Mutex<SmallBarPacing>,
}

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
    /// Connects with `client_id` to the paper gateway.
    ///
    /// Before it starts, checks that IB can serve closed bars over a window ending at `end`.
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

    /// Returns the latest end IB can serve closed bars over at `now`, a minute before it.
    ///
    /// # Errors
    ///
    /// Returns an error if that end is out of range.
    pub fn latest_end(now: Timestamp) -> anyhow::Result<Timestamp> {
        Ok(now.checked_sub(SignedDuration::from_mins(1))?)
    }

    /// Returns the instruments of `ids`.
    ///
    /// IB does not resolve an expired future by id, so it is asked again as an explicit contract
    /// with `includeExpired`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The request fails.
    /// - Any id is not returned. IB answers an unknown or expired id with nothing.
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

    /// Fills `catalog` with `slices` of series IB serves (`serves`) and holds (`check_held`).
    ///
    /// Temporary until nautilus_trader#5472 is fixed. Then IB fills through the node.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A slice is not of a series IB serves (checked before any request).
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

    /// Returns the `series` over `[start, end]`.
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

    /// Bars of 30 seconds or less are paced (`SmallBarPacing`).
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

    /// Nautilus' `request_ticks` is not used because it:
    /// - Skips a page's error. That reads as a short or empty page, and an empty page ends the
    ///   paging.
    /// - Ends the paging at a second that holds a full page with no error. The writer would store
    ///   that as the whole window.
    /// - Loses the rest of each full page's cut second (see `page_back`).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The definition cannot be had.
    /// - A page is refused, is not answered whole within the request timeout or ends with an error.
    /// - A second holds a full page (see `page_back`).
    ///
    /// The error names the instrument, the window and IB's error.
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

/// A tick's `ts_event`, and the tick Nautilus' parsers make from it.
///
/// The tick is `None` if Nautilus' IB clients drop it.
type PagedTick = (UnixNanos, Option<Data>);

/// The most ticks IB returns per request.
const TICKS_PER_PAGE: i32 = 1000;

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
    /// Returns the page of ticks up to `cursor`, asked as Nautilus' IB clients ask it.
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

    /// Returns `None` and logs a warning for a tick with no size, which Nautilus' IB clients skip.
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

    fn kept(&self, kind: &str, parsed: anyhow::Result<Data>) -> Option<Data> {
        parsed
            .inspect_err(|e| {
                log::warn!("Dropping IB's {kind} tick of {}: {e:#}", self.instrument_id);
            })
            .ok()
    }
}

/// Returns the items of `page`.
///
/// The items end with an error if the page has one. The notice that frames were dropped
/// (`SUBSCRIPTION_LAG_CODE`) counts as an error, because ticks were lost. Other notices are
/// logged.
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

/// Returns the connection and provider that Nautilus' `HistoricalInteractiveBrokersClient::connect`
/// makes.
///
/// They are held here, so the tick pages use the client's own.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Served {
    Bars(BarType),
    /// BID_ASK ticks as quotes, TRADES ticks as trades.
    Ticks(IbHistoricalTickType, InstrumentId),
}

/// # Errors
///
/// Returns an error if IB's history does not serve `source`, naming it and saying why.
pub(crate) fn serves(source: &CatalogSeries) -> anyhow::Result<()> {
    served(source).map(drop)
}

/// IB's history serves:
/// - IB's own (`EXTERNAL`) time bars, priced at one of `BAR_PRICES`. The size must be one IB has
///   (`bar_type_to_ib_bar_size`), and not a month, which `request_bars` does not parse.
/// - Quotes and trades, as historical ticks.
///
/// Nautilus' IB clients request no book history.
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

const FOREX_VENUE: &str = "IDEALPRO";

/// The venues of currency pairs: forex (CASH) and crypto (CRYPTO) contracts (`providers/parse.rs`).
///
/// Nautilus' `request_trades` logs an error and answers nothing for them (`data/core.rs`), so a
/// warmup that asks would never end. `subscribe_trades` streams nothing for them
/// (`data/core.rs:1000-1006`).
///
/// A venue is the test because `serves` and `streams` run before any definition is known. IB
/// routes crypto to PAXOS and ZEROHASH (`common/symbology.rs`).
const CURRENCY_PAIR_VENUES: [&str; 3] = [FOREX_VENUE, "PAXOS", "ZEROHASH"];

/// The bar prices served and streamed (`price_type_to_ib_what_to_show`).
///
/// LAST is IB's TRADES bars, and MID is its MIDPOINT bars. The adapter asks TRADES for any other
/// price.
const BAR_PRICES: [PriceType; 4] = [
    PriceType::Last,
    PriceType::Bid,
    PriceType::Ask,
    PriceType::Mid,
];

/// Returns the longest duration that IB's step-size table admits (TWS API, "Historical Data
/// Limitations", "Step Sizes").
///
/// A day is `86400 S` because `D` counts whole trading days. A `1 D` request ending in a session's
/// first minute answers one bar and ends the walk back.
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SmallBarRequest {
    bar_type: BarType,
    end: Timestamp,
    duration: &'static str,
}

/// IB paces bars of 30 seconds or less (TWS API, "Historical Data Limitations", "Pacing Violations
/// for Small Bars"). Nautilus' IB clients do not pace them.
///
/// The rules are:
/// - No request identical to one sent in the last 15 seconds.
/// - Fewer than six requests for one contract, exchange and price within two seconds.
/// - No more than 60 requests within ten minutes.
///
/// IB counts a BID_ASK request twice. Nautilus asks for BID and ASK bars apart, so each counts
/// once.
#[derive(Debug, Default)]
struct SmallBarPacing {
    sent: Vec<(Instant, SmallBarRequest)>,
}

const IDENTICAL_WINDOW: Duration = Duration::from_secs(15);
const SAME_SERIES_WINDOW: Duration = Duration::from_secs(2);
const SAME_SERIES_MOST: usize = 5;
const ALL_WINDOW: Duration = Duration::from_secs(600);
const ALL_MOST: usize = 60;

impl SmallBarPacing {
    fn wait(&self, now: Instant, request: &SmallBarRequest) -> Duration {
        let after = |at: Instant, window: Duration| (at + window).saturating_duration_since(now);
        let within = |window: Duration, of: &dyn Fn(&SmallBarRequest) -> bool| -> Vec<Instant> {
            self.sent
                .iter()
                .filter(|(at, sent)| now.saturating_duration_since(*at) < window && of(sent))
                .map(|(at, _)| *at)
                .collect()
        };

        // Holds until the earliest of `sent` leaves `window`, if `most` are in it
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

    fn sent(&mut self, at: Instant, request: SmallBarRequest) {
        self.sent
            .retain(|(sent, _)| at.saturating_duration_since(*sent) < ALL_WINDOW);
        self.sent.push((at, request));
    }
}

/// Checks that IB holds what a fill of `slices` asks at `now`.
///
/// IB holds no bars of 30 seconds or less older than six months (TWS API, "Historical Data
/// Limitations"). A slice is asked from where the writer resumes it (`catalog::resumed`).
///
/// # Errors
///
/// Returns an error if:
/// - A slice of bars would be asked from before the oldest start IB holds.
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

// A window ending within one bar of now gets the bar still in progress, which once stored would
// never be replaced.
fn ensure_bars_closed(end: Timestamp, now: Timestamp) -> anyhow::Result<()> {
    let latest = IbHistory::latest_end(now)?;
    anyhow::ensure!(
        end <= latest,
        "end {end} is within a minute of now: IB would return the minute still in progress; \
         use an end at or before {latest}"
    );
    Ok(())
}

/// Returns the ticks over `[start, end]`, paged back from `end`.
///
/// `fetch` answers the page ending at a cursor. The paging follows these rules:
/// - IB leaves out the end's second, so the first cursor is the second after it.
/// - A page of `page_size` ticks or more has its earliest second cut short (probed on the paper
///   gateway on 19 November 2026: 1 tick of EUR/USD's 7, 1 and 54). That second is dropped, and
///   the next cursor is the second after it.
/// - A shorter page is kept whole, and the next cursor is its earliest second.
///
/// Nautilus' IB clients end each next page at the earliest second (`extend_historical_tick_batch`,
/// `data/convert.rs`), so they lose the rest of a cut second.
///
/// # Errors
///
/// Returns an error if:
/// - A request fails (`fetch`'s error) or a page fails (an item's error). The ticks paged so far
///   are dropped.
/// - A full page reaches back no further than the page before. IB cannot page past that second,
///   and the error names it.
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

/// # Errors
///
/// Returns an error if `at` is out of range and cannot be truncated.
fn second_of(at: Timestamp) -> anyhow::Result<Timestamp> {
    Ok(at.round(
        TimestampRound::new()
            .smallest(Unit::Second)
            .mode(RoundMode::Trunc),
    )?)
}

/// Returns the bars over `[start, end]`, walking back from `end`.
///
/// `fetch` gets a request ending at a cursor. The cursor is `end` first, then the oldest `ts_init`
/// so far. The walk stops at:
/// - A bar at or before `start`.
/// - An empty answer.
/// - An answer with nothing older than its cursor.
///
/// The adapter sends one request over the whole window as one request between its bounds
/// (nautilus_trader#5621). That took 1,164 s for a year of minute bars, against 60 s (measured
/// 2026-11-08). Temporary, as IB's fill: it goes with it (helix-trader issue #4).
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

// Uses the full session, not regular trading hours, because IB's first regular-hours bar of a day
// is partial (13:30-14:00 UTC for ES) but the adapter stamps it at open + bar size, half an hour
// after its close, and `convert_exchange_to_mic_venue` is never set, because it changes a CME
// contract's venue between processes.
fn data_client_config(gateway: IbGateway) -> InteractiveBrokersDataClientConfig {
    InteractiveBrokersDataClientConfig {
        host: gateway.host,
        port: gateway.port,
        client_id: gateway.client_id,
        use_regular_trading_hours: false,
        ..Default::default()
    }
}

fn missing_ids(requested: &[InstrumentId], loaded: &[InstrumentAny]) -> Vec<InstrumentId> {
    requested
        .iter()
        .filter(|id| !loaded.iter().any(|instrument| instrument.id() == **id))
        .copied()
        .collect()
}

/// Returns the contract of the future `id` with `includeExpired`.
///
/// IB resolves an expired future in this form, with the root symbol (`ES` of `ESU5`) that
/// `Symbology::contract` leaves empty.
fn expired_future_contract(symbology: Symbology, id: InstrumentId) -> Option<Contract> {
    let mut contract = symbology.contract(id, None).ok()?;
    let root = parse_futures_symbol(&contract.local_symbol)?.root;
    contract.symbol = Symbol(root.to_string());
    contract.include_expired = true;
    Some(contract)
}

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

    fn closed_gateway() -> IbGateway {
        IbGateway {
            host: "127.0.0.1".to_string(),
            port: 1,
            client_id: 1,
        }
    }

    /// Starting the gateway would fail without credentials or Docker, or start a container.
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

    fn hour(h: u64) -> Timestamp {
        Timestamp::from(UnixNanos::from(h * HOUR_NS))
    }

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

    type Tick = (UnixNanos, &'static str);

    fn on_the_10th(time: &str) -> Timestamp {
        at(&format!("2026-11-10T{time}Z"))
    }

    fn tick_at(time: &str, name: &'static str) -> Tick {
        (UnixNanos::from(on_the_10th(time)), name)
    }

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

    /// Nautilus' `request_trades` refuses a currency pair's trades without an answer, and IB has
    /// no TRADES bars of an IDEALPRO pair.
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

    fn small(price: &str, end: i64) -> SmallBarRequest {
        SmallBarRequest {
            bar_type: bar_type(&format!("SXR8.IBIS-1-SECOND-{price}-EXTERNAL")),
            end: Timestamp::from_second(end).unwrap(),
            duration: "1800 S",
        }
    }

    fn pacing_of(start: Instant, requests: &[(u64, SmallBarRequest)]) -> SmallBarPacing {
        let mut pacing = SmallBarPacing::default();
        for &(at, request) in requests {
            pacing.sent(start + Duration::from_millis(at), request);
        }
        pacing
    }

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
