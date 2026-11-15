//! Interactive Brokers: the account, the gateway (see `gateway`) and the
//! data client config its history and its live clients share, instruments
//! and history for `fill_catalog`, which asks for series of any data type:
//! IB serves its own time bars, its quotes and its trades (see `serves`,
//! `IbHistory::data`), and the live node's clients (see `live`), and the
//! tag linking a bracket's exits in an OCA group (`bracket_exits_tag`); the
//! client id each command connects with, and live's account, are its flags
//! (`IbHistoryArgs`, `live::IbLiveArgs`). See `README.md`.
//!
//! IB's fill (`IbHistory::fill`) is temporary: IB's data client answers
//! without a `DataResponseEnvelope`, so Nautilus writes none of its answers
//! back. Once nautilus_trader#5472 is fixed, IB fills through the node as
//! LSE and IC do (see `catalog::NodeFill`), and `fill` and `data` go:
//! helix-trader issue #4 (<https://github.com/madebymlai/helix-trader/issues/4>).
//!
//! Uses `HistoricalInteractiveBrokersClient`
//! (`nautilus_interactive_brokers::historical`), which needs no node: it
//! connects to IB Gateway or TWS, returns `Vec<InstrumentAny>` from
//! `request_instruments`, `Vec<Bar>` from `request_bars` and the quotes or
//! trades of IB's historical ticks from `request_ticks`. The data is
//! stored as IB gives it.

use std::{
    str::FromStr,
    sync::{Mutex, PoisonError},
    time::{Duration, Instant},
};

use anyhow::{Context, bail, ensure};
use clap::Args;
use ibapi::{
    contracts::{Contract, Symbol},
    orders::OcaType,
};
use jiff::{SignedDuration, Timestamp, ToSpan, tz::TimeZone};
use nautilus_interactive_brokers::{
    common::{enums::IbHistoricalTickType, futures::parse_futures_symbol, symbology::Symbology},
    config::InteractiveBrokersDataClientConfig,
    data::convert::bar_type_to_ib_bar_size,
    historical::HistoricalInteractiveBrokersClient,
};
use nautilus_model::{
    data::{Bar, BarSpecification, BarType, Data, NautilusDataType},
    enums::{BarAggregation, PriceType},
    identifiers::{ClientOrderId, InstrumentId},
    instruments::{Instrument, InstrumentAny},
};

use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use ustr::Ustr;

use crate::{
    catalog::fill_slices,
    strategy::requirements::{CatalogSeries, SeriesSlice},
};

mod gateway;
pub use gateway::IbAccount;
use gateway::IbGateway;
mod live;
#[cfg(test)]
pub(in crate::venues) use live::offline_clients;
pub(crate) use live::{IbLiveArgs, clients};

/// The tag linking a bracket's exits in one IB OCA group named `entry`,
/// its entry's client order id, in the form Nautilus' IB adapter reads
/// (`IBOrderTags:` and a JSON object of `ibapi` order fields:
/// `docs/integrations/interactive_brokers.md`, "Commands and order
/// attributes"; `execution/transform/tags.rs`), which sends a list's
/// exits in no group of its own (`transform.rs`,
/// `test_order_list_id_does_not_set_oca_group`). IB then acts on a fill of
/// one exit itself, the node up or not.
///
/// The group's type is IB's 2, reduce with block: a fill of one exit
/// reduces the other by it, cancelling it once nothing is left, as
/// Nautilus' one-updates-the-other bracket exits do, and the adapter
/// reads the group back as one (`execution/parse.rs`,
/// `link_order_contingencies`); with block, IB routes one exit at a time,
/// so both cannot fill, which reducing without block (3) allows. The name
/// is unique to the bracket, as IB takes a name once used for no other
/// group, and stays on the exits' tags in the cache across restarts.
pub(crate) fn bracket_exits_tag(entry: ClientOrderId) -> Ustr {
    let fields = serde_json::json!({
        "ocaGroup": entry.as_str(),
        "ocaType": i32::from(OcaType::ReduceWithBlock),
    });
    Ustr::from(&format!("IBOrderTags:{fields}"))
}

/// IB's settings of a catalog fill, fill-catalog's flags: the client id its
/// history connects with (see `IbHistory::connect`).
#[derive(Debug, Args)]
pub(crate) struct IbHistoryArgs {
    /// IB client id of this process; not the adapter default 1, so a live
    /// node with the default id can run at the same time.
    #[arg(long, default_value_t = 10)]
    pub(super) client_id: i32,
}

/// A connection to IB for history: instruments, bars and ticks, no node.
pub struct IbHistory {
    client: HistoricalInteractiveBrokersClient,
    requests: HistoryRequests,
    symbology: Symbology,
    /// The requests of small bars sent so far, which IB paces.
    small_bars: Mutex<SmallBarPacing>,
}

/// The settings every history request passes, taken from the data client
/// config so history and live agree on the session.
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
    /// Connects with `client_id` to the paper gateway, started or reused
    /// (see `IbGateway::paper`), once IB can serve closed bars over a
    /// window ending at `end`: checked before the gateway starts.
    ///
    /// # Errors
    ///
    /// Returns an error if `end` is within a minute of `now`, if the
    /// gateway does not start and log in, or if the connection fails.
    pub async fn connect(client_id: i32, end: Timestamp, now: Timestamp) -> anyhow::Result<Self> {
        ensure_bars_closed(end, now)?;
        let config = data_client_config(IbGateway::paper(client_id).await?);
        let requests = HistoryRequests::of(&config);
        let symbology = Symbology::new(config.instrument_provider.symbology_method);
        let address = format!("{}:{}", config.host, config.port);
        let client = HistoricalInteractiveBrokersClient::connect(config)
            .await
            .with_context(|| format!("connecting to IB at {address}"))?;
        Ok(Self {
            client,
            requests,
            symbology,
            small_bars: Mutex::default(),
        })
    }

    /// The latest end of a window IB can serve closed bars over at `now`:
    /// a minute before `now`, the 1-minute bar in progress until then.
    ///
    /// # Errors
    ///
    /// Returns an error if that end is out of range.
    pub fn latest_end(now: Timestamp) -> anyhow::Result<Timestamp> {
        Ok(now.checked_sub(SignedDuration::from_mins(1))?)
    }

    /// The instruments of `ids`, as IB defines them. A future IB does not
    /// resolve by id (an expired one) is asked for again as an explicit
    /// contract with `includeExpired`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or any id is not returned
    /// (IB answers an unknown or expired id with nothing).
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

    /// Fills `catalog` with `slices`, IB series each on a contract over its
    /// slice (`DataRequirements::slices`, as the node fill takes them),
    /// which IB serves (`serves`) and holds (`check_held`, checked before
    /// the fill connects): the instruments of their contracts, then each
    /// slice through `fill_slices`, fetched by `data`. Temporary until
    /// nautilus_trader#5472 is fixed; then IB fills through the node.
    ///
    /// # Errors
    ///
    /// Returns an error if a slice is not of a series IB serves (before any
    /// request), or as `instruments` and `fill_slices`.
    pub async fn fill(
        &self,
        catalog: &mut ParquetDataCatalog,
        slices: &[SeriesSlice],
    ) -> anyhow::Result<()> {
        let mut ids: Vec<InstrumentId> = Vec::new();
        for SeriesSlice {
            series: (data_type, identifier),
            slice,
        } in slices
        {
            served(data_type, identifier)?;
            if !ids.contains(&slice.instrument_id) {
                ids.push(slice.instrument_id);
            }
        }
        let instruments = self.instruments(&ids).await?;
        fill_slices(
            catalog,
            &instruments,
            slices,
            async |data_type, identifier, from, to| {
                self.data(data_type, identifier, from, to).await
            },
        )
        .await
    }

    /// The series of `data_type` and `identifier` over `[start, end]`, for
    /// `fill_catalog`, as IB gives it: the bars of the bar type
    /// `identifier` names (see `bars`), or the quotes or trades of the
    /// instrument it names (see `ticks`).
    ///
    /// # Errors
    ///
    /// Returns an error if IB's history does not serve the series (see
    /// `serves`), or as `bars` and `ticks`.
    pub async fn data(
        &self,
        data_type: &NautilusDataType,
        identifier: &str,
        start: Timestamp,
        end: Timestamp,
    ) -> anyhow::Result<Vec<Data>> {
        match served(data_type, identifier)? {
            Served::Bars(bar_type) => {
                let bars = self.bars(bar_type, start, end).await?;
                Ok(bars.into_iter().map(Data::Bar).collect())
            }
            Served::Ticks(tick_type, instrument_id) => {
                self.ticks(tick_type, instrument_id, start, end).await
            }
        }
    }

    /// The bars of `bar_type` over `[start, end]`, sorted by `ts_init` and
    /// unique, with the regular-hours setting of the live data client.
    /// Walks back from `end` one request at a time (see `walk_back`), each
    /// over the duration IB takes for the bars' size (`request_duration`),
    /// a request of bars of 30 seconds or less sent once IB's pacing of
    /// them takes it (`SmallBarPacing`).
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

    /// Waits until IB's pacing of small bars takes `request`
    /// (`SmallBarPacing::wait`), then counts it sent.
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

    /// The `tick_type` ticks of `instrument_id` over `[start, end]`, IB's
    /// historical BID_ASK ticks as quotes or its TRADES ticks as trades,
    /// sorted by `ts_event`, with the regular-hours setting of the live
    /// data client: Nautilus' `request_ticks`, which pages back from `end`
    /// to `start`, 1,000 ticks a request, a trading session at most, and
    /// keeps the latest ticks up to a limit, here one that bounds nothing.
    async fn ticks(
        &self,
        tick_type: IbHistoricalTickType,
        instrument_id: InstrumentId,
        start: Timestamp,
        end: Timestamp,
    ) -> anyhow::Result<Vec<Data>> {
        self.client
            .request_ticks(
                tick_type,
                start,
                end,
                None,
                Some(vec![instrument_id]),
                self.requests.use_rth,
                self.requests.timeout_secs,
                usize::MAX,
            )
            .await
            .with_context(|| {
                format!(
                    "requesting the {} ticks of {instrument_id} over [{start}, {end}]",
                    tick_type.as_str()
                )
            })
    }
}

/// What IB's history is asked for a series it serves (see `served`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Served {
    /// The bars of a bar type, IB's own (`request_bars`).
    Bars(BarType),
    /// An instrument's historical ticks of a type (`request_ticks`): its
    /// BID_ASK ticks, its quotes, or its TRADES ticks, its trades.
    Ticks(IbHistoricalTickType, InstrumentId),
}

/// Checks that IB's history serves `source`, the series of an instrument
/// of a venue whose history is IB's (see `served`).
///
/// # Errors
///
/// Returns an error naming `source` and saying why if not.
pub(crate) fn serves(source: &CatalogSeries) -> anyhow::Result<()> {
    let (data_type, identifier) = source.key();
    served(&data_type, &identifier).map(drop)
}

/// What IB's history is asked for the series of `data_type` and
/// `identifier`, as Nautilus' IB clients serve it: IB's own (`EXTERNAL`)
/// time bars of a size IB has (Nautilus' `bar_type_to_ib_bar_size`),
/// priced at one of `SERVED_PRICES`, and an instrument's quotes and trades,
/// IB's historical ticks (the historical client's `request_ticks`, the
/// live client's `request_quotes` and `request_trades`). Nautilus' IB
/// clients request no book history.
///
/// # Errors
///
/// Returns an error naming the series and saying why if IB's history does
/// not serve it.
fn served(data_type: &NautilusDataType, identifier: &str) -> anyhow::Result<Served> {
    let instrument = || {
        InstrumentId::from_str(identifier)
            .with_context(|| format!("the {data_type} of {identifier}, no instrument id"))
    };
    match data_type {
        NautilusDataType::Bar => {
            let bar_type = BarType::from_str(identifier)?;
            ensure!(
                bar_type.is_externally_aggregated()
                    && SERVED_PRICES.contains(&bar_type.spec().price_type)
                    && bar_type_to_ib_bar_size(&bar_type).is_ok(),
                "IB's history serves its own (EXTERNAL) time bars of a size IB has, 1, 5, 10, \
                 15 or 30 seconds, 1, 2, 3, 5, 10, 15, 20 or 30 minutes, 1, 2, 3, 4 or 8 hours, \
                 a day, a week or a month, priced LAST, BID, ASK or MID, not {identifier}"
            );
            Ok(Served::Bars(bar_type))
        }
        NautilusDataType::QuoteTick => {
            Ok(Served::Ticks(IbHistoricalTickType::BidAsk, instrument()?))
        }
        NautilusDataType::TradeTick => {
            Ok(Served::Ticks(IbHistoricalTickType::Trades, instrument()?))
        }
        other => {
            bail!("IB's history serves bars, quotes and trades, not the {other} of {identifier}")
        }
    }
}

/// The prices of the bars IB's history serves, as Nautilus' IB clients ask
/// IB for them (`price_type_to_ib_what_to_show`): LAST, IB's TRADES bars,
/// the only ones with a volume, BID and ASK, IB's BID and ASK bars, the
/// history of an IDEALPRO pair, which has no TRADES, and MID, IB's MIDPOINT
/// bars. The adapter asks TRADES for any other price, so it serves none.
const SERVED_PRICES: [PriceType; 4] = [
    PriceType::Last,
    PriceType::Bid,
    PriceType::Ask,
    PriceType::Mid,
];

/// The IB duration of one request of bars of `spec`, a size IB has: the
/// longest IB's step-size table admits the size in (TWS API, "Historical
/// Data Limitations", "Step Sizes"), so that IB serves it and a request
/// holds a few thousand bars at most: `1800 S` of 1-second bars, `3600 S`
/// of 5-second bars, `14400 S` of 10- and 15-second bars, `28800 S` of
/// 30-second bars, a day of 1-minute bars, two of 2-minute bars, a week of
/// 3- to 20-minute bars, a month of 30-minute and hour bars and a year of
/// daily, weekly and monthly bars. A day of 1-minute bars is asked as
/// `86400 S`: `D` counts whole trading days, so a `1 D` request ending in
/// a session's first minute answers that one bar, and the walk back would
/// end there (see `walk_back`); the longer durations end there with a
/// session or more before it.
fn request_duration(spec: BarSpecification) -> &'static str {
    use BarAggregation as A;
    match (spec.aggregation, spec.step.get()) {
        (A::Second, 1) => "1800 S",
        (A::Second, 2..=5) => "3600 S",
        (A::Second, 6..=15) => "14400 S",
        (A::Second, _) => "28800 S",
        (A::Minute, 1) => "86400 S",
        (A::Minute, 2) => "2 D",
        (A::Minute, 3..=20) => "1 W",
        (A::Minute | A::Hour, _) => "1 M",
        _ => "1 Y",
    }
}

/// A request of bars of 30 seconds or less, which IB paces
/// (`SmallBarPacing`): its bar type, naming the contract, its exchange and
/// the data IB shows (its price), and its end and duration, the rest of
/// what makes two requests identical.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SmallBarRequest {
    bar_type: BarType,
    end: Timestamp,
    duration: &'static str,
}

/// IB's hard pacing of the history of bars of 30 seconds or less (TWS API,
/// "Historical Data Limitations", "Pacing Violations for Small Bars"),
/// which Nautilus' IB clients do not pace: no request identical to one
/// sent in the last 15 seconds, fewer than six of one contract, exchange
/// and data shown within two seconds, and no more than 60 within ten
/// minutes. IB counts a BID_ASK request twice; Nautilus asks IB for the
/// BID and the ASK of bars apart, never BID_ASK, so each counts once. The
/// requests sent in the last ten minutes, oldest first.
#[derive(Debug, Default)]
struct SmallBarPacing {
    sent: Vec<(Instant, SmallBarRequest)>,
}

/// How long IB remembers a request, refusing an identical one.
const IDENTICAL_WINDOW: Duration = Duration::from_secs(15);
/// The window of IB's count of the requests of one contract, exchange and
/// data shown, and how many it takes within one.
const SAME_SERIES_WINDOW: Duration = Duration::from_secs(2);
const SAME_SERIES_MOST: usize = 5;
/// The window of IB's count of every request, and how many it takes within
/// one.
const ALL_WINDOW: Duration = Duration::from_secs(600);
const ALL_MOST: usize = 60;

impl SmallBarPacing {
    /// How long `request` waits at `now` before IB takes it, by the
    /// requests sent: until 15 seconds after an identical one, until two
    /// seconds after the fifth latest of its contract and price within two
    /// seconds, and until ten minutes after the sixtieth latest within ten
    /// minutes; none if no rule holds it.
    fn wait(&self, now: Instant, request: &SmallBarRequest) -> Duration {
        let after = |at: Instant, window: Duration| (at + window).saturating_duration_since(now);
        let within = |window: Duration, of: &dyn Fn(&SmallBarRequest) -> bool| -> Vec<Instant> {
            self.sent
                .iter()
                .filter(|(at, sent)| now.saturating_duration_since(*at) < window && of(sent))
                .map(|(at, _)| *at)
                .collect()
        };
        // Holds until the earliest of `sent` leaves `window`, if `most`
        // are within it: after it, `most - 1` are, and the request.
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

/// Checks that IB's history holds `slices` at `now`: IB holds no bars of
/// 30 seconds or less older than six months (TWS API, "Historical Data
/// Limitations"), so a slice of them starts within six months of `now`,
/// rather than the fill storing its older windows empty.
///
/// # Errors
///
/// Returns an error naming the bars and the oldest start IB holds if a
/// slice of them starts earlier, or naming a slice of bars keyed by no bar
/// type.
pub(crate) fn check_held(slices: &[SeriesSlice], now: Timestamp) -> anyhow::Result<()> {
    let oldest = now
        .to_zoned(TimeZone::UTC)
        .checked_sub(6.months())?
        .timestamp();
    for SeriesSlice {
        series: (data_type, identifier),
        slice,
    } in slices
    {
        if *data_type != NautilusDataType::Bar {
            continue;
        }
        let bar_type = BarType::from_str(identifier)?;
        if bar_type.spec().aggregation == BarAggregation::Second {
            ensure!(
                slice.from >= oldest,
                "IB holds no bars of 30 seconds or less older than six months: {bar_type} from \
                 {} reaches before {oldest}",
                slice.from
            );
        }
    }
    Ok(())
}

/// Fails if `end` is later than `IbHistory::latest_end(now)`.
///
/// IB answers a window ending within one bar of now with the bar still in
/// progress; stored, it would never be replaced.
fn ensure_bars_closed(end: Timestamp, now: Timestamp) -> anyhow::Result<()> {
    let latest = IbHistory::latest_end(now)?;
    ensure!(
        end <= latest,
        "end {end} is within a minute of now: IB would return the minute still in progress; \
         use an end at or before {latest}"
    );
    Ok(())
}

/// The bars over `[start, end]`, fetched by walking back from `end`:
/// `fetch` gets one request ending at a cursor, first `end`, then the
/// `ts_init` of the oldest bar so far. Stops once a bar at or before
/// `start` arrived, on an empty answer, or when an answer brings nothing
/// older than its cursor. Returns the bars sorted by `ts_init`, unique.
///
/// One request per cursor, each ending where the previous answer starts,
/// so no hole opens between requests, and each one IB answers within the
/// request timeout: a request by start over the whole window, which the
/// adapter sends as one request between its bounds
/// (nautilus_trader#5621), took 1,164 s for a year of minute bars, against
/// 60 s (measured 2026-11-08). Temporary, as IB's fill, the one thing it
/// serves: it goes with it (helix-trader issue #4).
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
/// The full session, not regular trading hours: IB's first regular-hours
/// bar of a day is partial (13:30-14:00 UTC for ES) but the adapter stamps
/// every bar at open + bar size, so that bar would be stamped half an hour
/// after its close. Adapter defaults otherwise: `Simplified` symbology (ids
/// like `ESM24.CME`). Never sets
/// `convert_exchange_to_mic_venue`: it makes the venue of a CME contract
/// change between processes.
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

/// The IB contract of the future `id` with `includeExpired`, the form IB
/// resolves an expired future by; `None` if `id` is not a future. IB also
/// needs the root symbol (`ES` of `ESU5`), which `Symbology::contract`
/// leaves empty.
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
    ensure!(
        missing.is_empty(),
        "IB returned no instrument for {} (unknown or expired contract)",
        missing.join(", ")
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_core::UnixNanos;
    use nautilus_model::instruments::stubs::futures_contract_es;

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

    /// Starting the gateway would fail without credentials or Docker, or
    /// start a container: the end is checked first.
    #[tokio::test]
    async fn connect_rejects_an_end_within_a_minute_of_now_before_starting_the_gateway() {
        let now = at("2024-06-10T12:00:00Z");
        let end = at("2024-06-10T11:59:30Z");
        let Err(err) = IbHistory::connect(1, end, now).await else {
            panic!("connected");
        };
        assert!(
            err.to_string().contains("within a minute of now"),
            "{err:#}"
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

    /// A source holding a bar at each hour of `held` that answers a
    /// request ending at `cursor` with the latest `per_answer` bars at or
    /// before it, oldest first, as IB does; records each cursor.
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

    #[tokio::test]
    async fn walks_back_from_end_with_each_request_ending_at_the_oldest_bar_so_far() {
        let mut cursors = Vec::new();
        let bars = walk_back(hour(5), hour(25), source(1..=30, 8, &mut cursors))
            .await
            .unwrap();
        assert_eq!(hours(&bars), (5..=25).collect::<Vec<_>>());
        assert_eq!(cursors, [hour(25), hour(18), hour(11)]);
    }

    /// Answers the requests with `answers` in turn, recording each cursor;
    /// a request past the last answer is an error.
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

    #[tokio::test]
    async fn stops_on_an_empty_answer() {
        let mut cursors = Vec::new();
        let answers = vec![(20..=25).collect(), vec![]];
        let bars = walk_back(hour(5), hour(25), canned(answers, &mut cursors))
            .await
            .unwrap();
        assert_eq!(hours(&bars), (20..=25).collect::<Vec<_>>());
        assert_eq!(cursors, [hour(25), hour(20)]);
    }

    #[tokio::test]
    async fn an_empty_first_answer_is_empty() {
        let mut cursors = Vec::new();
        let bars = walk_back(hour(5), hour(25), canned(vec![vec![]], &mut cursors))
            .await
            .unwrap();
        assert!(bars.is_empty());
        assert_eq!(cursors, [hour(25)]);
    }

    #[tokio::test]
    async fn stops_when_an_answer_brings_nothing_older_than_its_cursor() {
        let mut cursors = Vec::new();
        let answers = vec![(20..=25).collect(), (20..=25).collect(), vec![]];
        let bars = walk_back(hour(5), hour(25), canned(answers, &mut cursors))
            .await
            .unwrap();
        assert_eq!(hours(&bars), (20..=25).collect::<Vec<_>>());
        assert_eq!(cursors, [hour(25), hour(20)]);
    }

    #[tokio::test]
    async fn keeps_only_bars_from_start_to_end() {
        let mut cursors = Vec::new();
        let answers = vec![(3..=27).collect()];
        let bars = walk_back(hour(5), hour(25), canned(answers, &mut cursors))
            .await
            .unwrap();
        assert_eq!(hours(&bars), (5..=25).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn returns_the_bars_sorted_and_unique() {
        let mut cursors = Vec::new();
        let answers = vec![vec![25, 23, 24, 22], vec![20, 22, 21, 21], vec![4]];
        let bars = walk_back(hour(5), hour(25), canned(answers, &mut cursors))
            .await
            .unwrap();
        assert_eq!(hours(&bars), (20..=25).collect::<Vec<_>>());
        assert_eq!(cursors, [hour(25), hour(22), hour(20)]);
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

    #[test]
    fn history_requests_use_the_regular_hours_setting_of_the_data_client_config() {
        let config = data_client_config(closed_gateway());
        assert!(!HistoryRequests::of(&config).use_rth);
        let regular = InteractiveBrokersDataClientConfig {
            use_regular_trading_hours: true,
            ..config
        };
        assert!(HistoryRequests::of(&regular).use_rth);
    }

    #[test]
    fn ib_serves_its_own_one_minute_trades_bars() {
        for identifier in [
            "SXR8.IBIS-1-MINUTE-LAST-EXTERNAL",
            "ESZ26.CME-1-MINUTE-LAST-EXTERNAL",
        ] {
            let served = served(&NautilusDataType::Bar, identifier).unwrap();
            assert_eq!(served, Served::Bars(bar_type(identifier)));
            // IB's bar spec string, as `bars` requests it.
            assert_eq!(bar_type(identifier).spec().to_string(), "1-MINUTE-LAST");
        }
    }

    /// An IDEALPRO pair, which IB serves no TRADES bars of, has its own
    /// BID and ASK bars, of any size IB has, and IB serves MIDPOINT bars
    /// too.
    #[test]
    fn ib_serves_its_own_bid_ask_and_mid_bars_of_any_size_it_has() {
        for identifier in [
            "EUR/USD.IDEALPRO-1-MINUTE-BID-EXTERNAL",
            "EUR/USD.IDEALPRO-1-MINUTE-ASK-EXTERNAL",
            "EUR/USD.IDEALPRO-1-HOUR-BID-EXTERNAL",
            "EUR/USD.IDEALPRO-5-MINUTE-ASK-EXTERNAL",
            "EUR/USD.IDEALPRO-1-SECOND-BID-EXTERNAL",
            "EUR/USD.IDEALPRO-1-MINUTE-MID-EXTERNAL",
            "SXR8.IBIS-1-MINUTE-MID-EXTERNAL",
            "SXR8.IBIS-1-DAY-BID-EXTERNAL",
        ] {
            let served = served(&NautilusDataType::Bar, identifier).unwrap();
            assert_eq!(served, Served::Bars(bar_type(identifier)));
        }
    }

    /// IB's historical ticks: BID_ASK for quotes, TRADES for trades, of the
    /// instrument the series names.
    #[test]
    fn ib_serves_quotes_and_trades_as_its_historical_ticks() {
        let sxr8 = InstrumentId::from("SXR8.IBIS");
        assert_eq!(
            served(&NautilusDataType::QuoteTick, "SXR8.IBIS").unwrap(),
            Served::Ticks(IbHistoricalTickType::BidAsk, sxr8)
        );
        assert_eq!(
            served(&NautilusDataType::TradeTick, "SXR8.IBIS").unwrap(),
            Served::Ticks(IbHistoricalTickType::Trades, sxr8)
        );
    }

    /// A price IB's adapter would ask TRADES for, a size IB has not,
    /// bars Nautilus builds, and books and custom data, which Nautilus' IB
    /// clients request none of.
    #[test]
    fn ib_serves_no_other_series() {
        for (data_type, identifier) in [
            (
                NautilusDataType::Bar,
                "EUR/USD.IDEALPRO-1-MINUTE-MARK-EXTERNAL",
            ),
            (NautilusDataType::Bar, "SXR8.IBIS-4-MINUTE-LAST-EXTERNAL"),
            (NautilusDataType::Bar, "SXR8.IBIS-2-SECOND-LAST-EXTERNAL"),
            (NautilusDataType::Bar, "SXR8.IBIS-6-HOUR-LAST-EXTERNAL"),
            (NautilusDataType::Bar, "SXR8.IBIS-1-MINUTE-LAST-INTERNAL"),
            (
                NautilusDataType::Bar,
                "EUR/USD.IDEALPRO-1-MINUTE-BID-INTERNAL",
            ),
            (
                NautilusDataType::Bar,
                "EUR/USD.IDEALPRO-1-MINUTE-ASK-INTERNAL",
            ),
            (NautilusDataType::OrderBookDelta, "SXR8.IBIS"),
            (NautilusDataType::OrderBookDepth, "SXR8.IBIS"),
            (
                NautilusDataType::Custom {
                    type_name: "Imbalance".to_string(),
                },
                "SXR8.IBIS",
            ),
        ] {
            let err = served(&data_type, identifier).unwrap_err();
            assert!(err.to_string().contains(identifier), "{err}");
        }
    }

    /// IB's step-size table ("Historical Data Limitations"): the longest
    /// duration whose row admits each size IB has, a day of 1-minute bars
    /// in seconds.
    #[test]
    fn each_bar_size_is_asked_over_the_longest_duration_ibs_step_sizes_admit_it_in() {
        for (bars, duration) in [
            ("1-SECOND", "1800 S"),
            ("5-SECOND", "3600 S"),
            ("10-SECOND", "14400 S"),
            ("15-SECOND", "14400 S"),
            ("30-SECOND", "28800 S"),
            ("1-MINUTE", "86400 S"),
            ("2-MINUTE", "2 D"),
            ("3-MINUTE", "1 W"),
            ("5-MINUTE", "1 W"),
            ("10-MINUTE", "1 W"),
            ("15-MINUTE", "1 W"),
            ("20-MINUTE", "1 W"),
            ("30-MINUTE", "1 M"),
            ("1-HOUR", "1 M"),
            ("2-HOUR", "1 M"),
            ("3-HOUR", "1 M"),
            ("4-HOUR", "1 M"),
            ("8-HOUR", "1 M"),
            ("1-DAY", "1 Y"),
            ("1-WEEK", "1 Y"),
            ("1-MONTH", "1 Y"),
        ] {
            let spec = bar_type(&format!("SXR8.IBIS-{bars}-LAST-EXTERNAL")).spec();
            assert_eq!(request_duration(spec), duration, "{bars}");
        }
    }

    /// A request of `bars`, 1-second bars of SXR8 priced `price`, ending at
    /// second `end` of the epoch.
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

    /// Worked by hand: a request identical to one sent at 0 waits until
    /// 15 s, at 10 s 5 s more; one of another end does not.
    #[test]
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

    /// Worked by hand: five requests of SXR8's LAST bars sent at 0, 100,
    /// 200, 300 and 400 ms; a sixth within two seconds of the first would
    /// be IB's sixth, so at 500 ms it waits until the first is 2 s old,
    /// 1.5 s; one of SXR8's BID bars, another data shown, does not wait.
    #[test]
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

    /// Worked by hand: 60 requests, of two series apart, one every 3 s from
    /// 0 to 177 s; the 61st within ten minutes of the first, at 180 s,
    /// waits until the first is ten minutes old, 420 s; at 600 s the first
    /// is out of the count and the 61st goes.
    #[test]
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
    fn slices_of(series: &[(NautilusDataType, &str, &str)], from: &str) -> Vec<SeriesSlice> {
        series
            .iter()
            .map(|(data, identifier, instrument)| SeriesSlice {
                series: (data.clone(), (*identifier).to_string()),
                slice: crate::rolls::Slice {
                    instrument_id: InstrumentId::from(*instrument),
                    from: at(from),
                    to: at("2026-11-14T00:00:00Z"),
                },
            })
            .collect()
    }

    /// On 15 November 2026 IB holds 30-second bars from 15 May on, six
    /// months before: a fill of them from 16 May is held, one from 14 May
    /// is refused, naming the bars and the start IB holds; 1-minute bars
    /// and quotes from 14 May are held, IB keeping them longer.
    #[test]
    fn small_bars_older_than_six_months_are_refused_naming_the_oldest_ib_holds() {
        let now = at("2026-11-15T12:00:00Z");
        let small_bars = "SXR8.IBIS-30-SECOND-LAST-EXTERNAL";
        let held = |from| {
            check_held(
                &slices_of(&[(NautilusDataType::Bar, small_bars, "SXR8.IBIS")], from),
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
            &slices_of(
                &[
                    (
                        NautilusDataType::Bar,
                        "SXR8.IBIS-1-MINUTE-LAST-EXTERNAL",
                        "SXR8.IBIS",
                    ),
                    (NautilusDataType::QuoteTick, "SXR8.IBIS", "SXR8.IBIS"),
                ],
                "2026-05-14T00:00:00Z",
            ),
            now,
        )
        .unwrap();
    }

    #[test]
    fn the_latest_end_is_a_minute_before_now() {
        let now = at("2024-06-10T12:00:00Z");
        assert_eq!(
            IbHistory::latest_end(now).unwrap(),
            at("2024-06-10T11:59:00Z")
        );
    }

    #[test]
    fn accepts_an_end_at_least_a_minute_before_now() {
        let now = at("2024-06-10T12:00:00Z");
        ensure_bars_closed(at("2024-06-10T11:59:00Z"), now).unwrap();
        ensure_bars_closed(at("2024-06-01T00:00:00Z"), now).unwrap();
    }

    #[test]
    fn rejects_an_end_within_a_minute_of_now() {
        let now = at("2024-06-10T12:00:00Z");
        let err = ensure_bars_closed(at("2024-06-10T11:59:00.000000001Z"), now)
            .unwrap_err()
            .to_string();
        assert!(err.contains("2024-06-10T11:59:00.000000001Z"), "{err}");
        assert!(err.contains("2024-06-10T11:59:00Z"), "{err}");
    }

    #[test]
    fn accepts_instruments_when_every_requested_id_loaded() {
        let requested = [InstrumentId::from("ESZ21.GLBX")];
        ensure_all_loaded(&requested, &[es()]).unwrap();
    }

    #[test]
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

    #[test]
    fn an_expired_future_is_asked_for_as_an_explicit_contract() {
        let contract =
            expired_future_contract(symbology(), InstrumentId::from("ESU25.CME")).unwrap();
        assert_eq!(contract.symbol.to_string(), "ES");
        assert_eq!(contract.local_symbol, "ESU5");
        assert!(contract.include_expired);
    }

    #[test]
    fn only_futures_are_asked_for_as_expired_contracts() {
        assert!(expired_future_contract(symbology(), InstrumentId::from("AAPL.SMART")).is_none());
        assert!(
            expired_future_contract(symbology(), InstrumentId::from("EUR/USD.IDEALPRO")).is_none()
        );
        // A stock whose symbol reads like a future.
        assert!(expired_future_contract(symbology(), InstrumentId::from("ABCZ5.SMART")).is_none());
    }
}
