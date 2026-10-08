//! Interactive Brokers history: instruments, bars and quotes for
//! `fill_catalog`.
//!
//! Uses `HistoricalInteractiveBrokersClient`
//! (`nautilus_interactive_brokers::historical`), which needs no node: it
//! connects to IB Gateway or TWS, returns `Vec<InstrumentAny>` from
//! `request_instruments` and `Vec<Bar>` from `request_bars`. IB has no
//! usable quote history, so quotes are made here from BID and ASK bars.
//! Another source is another file next to this one.

use std::{collections::HashMap, time::Duration};

use anyhow::{Context, ensure};
use ibapi::contracts::{Contract, Symbol};
use jiff::Timestamp;
use nautilus_core::{UnixNanos, nanos::DurationNanos};
use nautilus_interactive_brokers::{
    common::parse::instrument_id_to_ib_contract, config::InteractiveBrokersDataClientConfig,
    historical::HistoricalInteractiveBrokersClient,
};
use nautilus_model::{
    data::{Bar, BarSpecification, BarType, QuoteTick},
    enums::{AggregationSource, BarAggregation, PriceType},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
    types::Quantity,
};

/// Where IB Gateway or TWS listens, and the client id of this process.
#[derive(Debug, Clone)]
pub struct IbGateway {
    pub host: String,
    pub port: u16,
    pub client_id: i32,
}

/// A connection to IB for history: instruments, bars and quotes, no node.
pub struct IbHistory {
    client: HistoricalInteractiveBrokersClient,
    requests: BarRequests,
}

/// The settings every bar request passes, taken from the data client
/// config so history and live agree on the session.
#[derive(Debug, Clone, Copy)]
struct BarRequests {
    use_rth: bool,
    timeout_secs: u64,
}

impl BarRequests {
    fn of(config: &InteractiveBrokersDataClientConfig) -> Self {
        Self {
            use_rth: config.use_regular_trading_hours,
            timeout_secs: config.request_timeout,
        }
    }
}

impl IbHistory {
    /// Connects to `gateway`, once IB can serve every bar type of
    /// `bar_types` over a window ending at `end`: checked before any
    /// connection attempt.
    ///
    /// # Errors
    ///
    /// Returns an error if a bar type is not one IB serves, if `end` is
    /// within the longest bar of `now`, or if the connection fails.
    pub async fn connect(
        gateway: IbGateway,
        bar_types: &[BarType],
        end: Timestamp,
        now: Timestamp,
    ) -> anyhow::Result<Self> {
        for bar_type in bar_types {
            bar_spec(bar_type)?;
        }
        ensure_bars_closed(bar_types, end, now)?;
        let config = data_client_config(gateway);
        let requests = BarRequests::of(&config);
        let address = format!("{}:{}", config.host, config.port);
        let client = HistoricalInteractiveBrokersClient::connect(config)
            .await
            .with_context(|| format!("connecting to IB at {address}"))?;
        Ok(Self { client, requests })
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
            .filter_map(expired_future_contract)
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

    /// The bars of `bar_type` over `(start, end]`, sorted by `ts_init` and
    /// unique, with the regular-hours setting of the live data client.
    /// Walks back from `end` one request at a time, each of the duration
    /// for the bar size (see `walk_back` and `request_duration`).
    ///
    /// # Errors
    ///
    /// Returns an error if the bar type is not one IB serves, or if any
    /// request fails or times out.
    pub async fn bars(
        &self,
        bar_type: BarType,
        start: Timestamp,
        end: Timestamp,
    ) -> anyhow::Result<Vec<Bar>> {
        let spec = bar_spec(&bar_type)?;
        let duration = request_duration(&bar_type);
        walk_back(start, end, async |cursor| {
            self.client
                .request_bars(
                    vec![&spec],
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

    /// The quotes of `instrument_id` over `(start, end]`, one per bar of
    /// `spec`'s cadence (its price type is ignored), sorted by `ts_init`.
    /// IB has no usable quote history: each is made from the BID and ASK
    /// bars of the same close, fetched like `bars` (see `quotes_from_bars`),
    /// and stamped 1 ns before it (see `quoted_bars_window`). Logs how many
    /// bars it dropped.
    ///
    /// # Errors
    ///
    /// Returns an error if the cadence is not one IB serves, if any request
    /// fails or times out, or if a BID and an ASK bar differ in precision.
    pub async fn quotes(
        &self,
        instrument_id: InstrumentId,
        spec: BarSpecification,
        start: Timestamp,
        end: Timestamp,
    ) -> anyhow::Result<Vec<QuoteTick>> {
        let priced = |price_type| {
            let spec = BarSpecification { price_type, ..spec };
            BarType::new(instrument_id, spec, AggregationSource::External)
        };
        let (from, to) = quoted_bars_window(start, end);
        let bids = self.bars(priced(PriceType::Bid), from, to).await?;
        let asks = self.bars(priced(PriceType::Ask), from, to).await?;
        let made = quotes_from_bars(&bids, &asks)?;
        log::info!(
            "{instrument_id}: {} quotes from {} BID and {} ASK bars; dropped {} bars \
             without the other side and {} crossed pairs (ask below bid)",
            made.quotes.len(),
            bids.len(),
            asks.len(),
            made.unpaired,
            made.crossed,
        );
        Ok(made.quotes)
    }
}

/// How long before its bars' close a quote is stamped: it then sorts
/// before a bar of the same close, so a strategy deciding on that bar
/// fills on its quote (see the backtest README, "Fills on quotes"). The
/// close quote is the last of its bar, so no look-ahead.
const QUOTE_LEAD: DurationNanos = DurationNanos::new(1);

/// The window of the bars whose quotes fall in `(start, end]`: those
/// closing over `(start, end]` shifted by `QUOTE_LEAD`, so a quote stamped
/// at `end` is fetched and one at `start` is not.
fn quoted_bars_window(start: Timestamp, end: Timestamp) -> (Timestamp, Timestamp) {
    let lead = Duration::from_nanos(QUOTE_LEAD.as_u64());
    (start + lead, end + lead)
}

/// The nominal size of both sides of a quote made from bars: IB gives
/// none, and this one never limits an order.
const QUOTE_SIZE: f64 = 1_000_000.0;

/// The quotes made from BID and ASK bars, and what was dropped.
#[derive(Debug)]
struct MadeQuotes {
    /// Sorted by `ts_init`.
    quotes: Vec<QuoteTick>,
    /// Bars without a bar of the other side at the same close.
    unpaired: usize,
    /// Pairs whose ask is below the bid.
    crossed: usize,
}

/// One quote per BID and ASK bar pair of the same close (`ts_init`), from
/// `bids` and `asks` sorted by `ts_init` and unique: bid = the BID close,
/// ask = the ASK close, at the bars' price precision (the instrument's),
/// size `QUOTE_SIZE` at the bars' size precision on both sides, and
/// `ts_event = ts_init` = `QUOTE_LEAD` before the close.
///
/// A bar without the other side, and a crossed pair (ask below bid,
/// mostly in pre-opening auctions), is dropped and counted, not an error:
/// the hour then fills on the previous quote. A locked pair (ask = bid) is
/// kept.
///
/// # Errors
///
/// Returns an error if a BID and an ASK bar of a pair differ in price or
/// size precision.
fn quotes_from_bars(bids: &[Bar], asks: &[Bar]) -> anyhow::Result<MadeQuotes> {
    let asks_by_close: HashMap<UnixNanos, &Bar> =
        asks.iter().map(|ask| (ask.ts_init, ask)).collect();
    let mut made = MadeQuotes {
        quotes: Vec::new(),
        unpaired: 0,
        crossed: 0,
    };
    for bid in bids {
        let Some(ask) = asks_by_close.get(&bid.ts_init) else {
            made.unpaired += 1;
            continue;
        };
        if ask.close < bid.close {
            made.crossed += 1;
            continue;
        }
        let stamp = bid.ts_init - QUOTE_LEAD;
        let quote = QuoteTick::new_checked(
            bid.bar_type.instrument_id(),
            bid.close,
            ask.close,
            Quantity::new(QUOTE_SIZE, bid.volume.precision),
            Quantity::new(QUOTE_SIZE, ask.volume.precision),
            stamp,
            stamp,
        )
        .with_context(|| format!("pairing the BID and ASK bars closing at {}", bid.ts_init))?;
        made.quotes.push(quote);
    }
    made.unpaired += asks.len() - (made.quotes.len() + made.crossed);
    Ok(made)
}

/// The bars over `(start, end]`, fetched by walking back from `end`:
/// `fetch` gets one request ending at a cursor, first `end`, then the
/// `ts_init` of the oldest bar so far. Stops once a bar at or before
/// `start` arrived, on an empty answer, or when an answer brings nothing
/// older than its cursor. Returns the bars sorted by `ts_init`, unique.
///
/// One request per cursor, each ending where the previous answer starts,
/// so no hole opens between requests: the adapter's own split of a window
/// into a days segment and a seconds remainder loses bars, because IB
/// counts days in trading sessions.
async fn walk_back(
    start: Timestamp,
    end: Timestamp,
    mut fetch: impl AsyncFnMut(Timestamp) -> anyhow::Result<Vec<Bar>>,
) -> anyhow::Result<Vec<Bar>> {
    let nanos = |bar: &Bar| i128::from(bar.ts_init.as_u64());
    let mut bars = Vec::new();
    let mut cursor = end;
    loop {
        let answer = fetch(cursor).await?;
        let Some(oldest) = answer.iter().map(nanos).min() else {
            break;
        };
        bars.extend(answer);
        if oldest <= start.as_nanosecond() || oldest >= cursor.as_nanosecond() {
            break;
        }
        cursor = Timestamp::from_nanosecond(oldest)?;
    }
    bars.retain(|bar| start.as_nanosecond() < nanos(bar) && nanos(bar) <= end.as_nanosecond());
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
/// like `ESM4.CME`). Never sets
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

/// The IB bar spec string of `bar_type`, for example `"1-HOUR-LAST"`.
///
/// Only `EXTERNAL` time bars at a size IB serves, priced on last, bid, ask
/// or mid. The historical client does not parse `MONTH`. `WEEK` is refused
/// too: the adapter stamps a weekly bar at its open, and the catalog needs
/// `ts_init` on the close.
fn bar_spec(bar_type: &BarType) -> anyhow::Result<String> {
    let spec = bar_type.spec();
    let step = spec.step.get();
    let size_served = match spec.aggregation {
        BarAggregation::Second => matches!(step, 1 | 5 | 15 | 30),
        BarAggregation::Minute => matches!(step, 1 | 2 | 3 | 5 | 10 | 15 | 20 | 30),
        BarAggregation::Hour => matches!(step, 1 | 2 | 3 | 4 | 8),
        BarAggregation::Day => step == 1,
        _ => false,
    };
    let price_served = matches!(
        spec.price_type,
        PriceType::Last | PriceType::Bid | PriceType::Ask | PriceType::Mid
    );
    ensure!(
        size_served && price_served && bar_type.aggregation_source() == AggregationSource::External,
        "IB cannot serve bar type {bar_type}: only EXTERNAL time bars of \
         1/5/15/30 SECOND, 1/2/3/5/10/15/20/30 MINUTE, 1/2/3/4/8 HOUR or 1 DAY, \
         priced LAST, BID, ASK or MID (no WEEK: the adapter stamps weekly bars \
         at the open, not the close)"
    );
    Ok(format!("{step}-{}-{}", spec.aggregation, spec.price_type))
}

/// The IB duration of one bar request of `bar_type`: within IB's maximum
/// duration per bar size, and long enough that a year of 1-HOUR bars takes
/// about 13 requests, well under IB's pacing limit of 60 requests in ten
/// minutes. `D` counts whole trading days: a `1 D` request ending in a
/// session's first minute answers that one bar, so 1-MINUTE bars use
/// `86400 S`, the 1,440 bars before the end, across sessions (IB's
/// documented alternative). Takes bar types `bar_spec` accepted.
fn request_duration(bar_type: &BarType) -> &'static str {
    let spec = bar_type.spec();
    match (spec.aggregation, spec.step.get()) {
        (BarAggregation::Second, 1) => "1800 S",
        (BarAggregation::Second, 5) => "3600 S",
        (BarAggregation::Second, 15) => "14400 S",
        (BarAggregation::Second, _) => "28800 S",
        (BarAggregation::Minute, 1) => "86400 S",
        (BarAggregation::Minute, 2) => "2 D",
        (BarAggregation::Minute, 3..=10) => "5 D",
        (BarAggregation::Minute, 15 | 20) => "10 D",
        (BarAggregation::Minute | BarAggregation::Hour, _) => "20 D",
        _ => "1 Y",
    }
}

/// Fails if `end` is later than `now` minus the longest bar of `bar_types`.
///
/// IB answers a window ending within one bar of now with the bar still in
/// progress; stored, it would never be replaced. Takes bar types `bar_spec`
/// accepted.
fn ensure_bars_closed(bar_types: &[BarType], end: Timestamp, now: Timestamp) -> anyhow::Result<()> {
    let longest = bar_types
        .iter()
        .map(|bar_type| bar_type.spec().timedelta())
        .max()
        .unwrap_or_default();
    let latest = now.checked_sub(longest)?;
    ensure!(
        end <= latest,
        "end {end} is within one bar of now: IB would return the bar still in progress; \
         use an end at or before {latest}"
    );
    Ok(())
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
/// needs the root symbol (`ES` of `ESU5`), which the adapter's conversion
/// leaves empty.
fn expired_future_contract(id: InstrumentId) -> Option<Contract> {
    let mut contract = instrument_id_to_ib_contract(id, None).ok()?;
    let root = future_root(&contract.local_symbol)?;
    contract.symbol = Symbol(root.to_string());
    contract.include_expired = true;
    Some(contract)
}

/// The root of a future's local symbol: `ES` of `ESU5`, `ESU25` or
/// `ESU2025` (the year forms the adapter accepts).
fn future_root(local_symbol: &str) -> Option<&str> {
    let without_year = local_symbol.trim_end_matches(|c: char| c.is_ascii_digit());
    let year_len = local_symbol.len() - without_year.len();
    let root = without_year.strip_suffix(|c: char| "FGHJKMNQUVXZ".contains(c))?;
    (matches!(year_len, 1 | 2 | 4) && !root.is_empty()).then_some(root)
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
    use ibapi::contracts::SecurityType;
    use nautilus_model::instruments::stubs::futures_contract_es;

    fn es() -> InstrumentAny {
        InstrumentAny::FuturesContract(futures_contract_es(None, None))
    }

    /// Nothing listens there: a connection attempt would fail with a
    /// connection error, not the error asserted.
    fn closed_gateway() -> IbGateway {
        IbGateway {
            host: "127.0.0.1".to_string(),
            port: 1,
            client_id: 1,
        }
    }

    #[tokio::test]
    async fn connect_rejects_a_bar_type_ib_cannot_serve_before_connecting() {
        let bar_types = [
            bar_type("ESM4.CME-1-HOUR-LAST-EXTERNAL"),
            bar_type("ESM4.CME-1-MONTH-LAST-EXTERNAL"),
        ];
        let now = at("2024-06-10T12:00:00Z");
        let Err(err) = IbHistory::connect(
            closed_gateway(),
            &bar_types,
            at("2024-01-01T00:00:00Z"),
            now,
        )
        .await
        else {
            panic!("connected");
        };
        assert!(
            err.to_string()
                .contains("IB cannot serve bar type ESM4.CME-1-MONTH-LAST-EXTERNAL"),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn connect_rejects_an_end_within_one_bar_of_now_before_connecting() {
        let bar_types = [bar_type("ESM4.CME-1-HOUR-LAST-EXTERNAL")];
        let now = at("2024-06-10T12:00:00Z");
        let Err(err) = IbHistory::connect(closed_gateway(), &bar_types, now, now).await else {
            panic!("connected");
        };
        assert!(err.to_string().contains("within one bar of now"), "{err:#}");
    }

    fn at(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    const HOUR_NS: u64 = 3_600_000_000_000;

    /// The hour `h` after the epoch.
    fn hour(h: u64) -> Timestamp {
        Timestamp::from_nanosecond(i128::from(h * HOUR_NS)).unwrap()
    }

    /// A 1-HOUR bar of ESM4 closing at hour `h`.
    fn hourly(h: u64) -> Bar {
        use nautilus_core::UnixNanos;
        use nautilus_model::types::{Price, Quantity};
        let p = Price::from("5000.00");
        let ts = UnixNanos::from(h * HOUR_NS);
        let bt = bar_type("ESM4.CME-1-HOUR-LAST-EXTERNAL");
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
                .filter(|bar| i128::from(bar.ts_init.as_u64()) <= cursor.as_nanosecond())
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
        assert_eq!(hours(&bars), (6..=25).collect::<Vec<_>>());
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
        assert_eq!(hours(&bars), (18..=25).collect::<Vec<_>>());
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
    async fn keeps_only_bars_after_start_and_up_to_end() {
        let mut cursors = Vec::new();
        let answers = vec![(3..=27).collect()];
        let bars = walk_back(hour(5), hour(25), canned(answers, &mut cursors))
            .await
            .unwrap();
        assert_eq!(hours(&bars), (6..=25).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn returns_the_bars_sorted_and_unique() {
        let mut cursors = Vec::new();
        let answers = vec![vec![25, 23, 24, 22], vec![20, 22, 21, 21], vec![5]];
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
    fn the_data_client_config_uses_the_full_session() {
        assert!(!data_client_config(closed_gateway()).use_regular_trading_hours);
    }

    #[test]
    fn bar_requests_use_the_regular_hours_setting_of_the_data_client_config() {
        let config = data_client_config(closed_gateway());
        assert!(!BarRequests::of(&config).use_rth);
        let regular = InteractiveBrokersDataClientConfig {
            use_regular_trading_hours: true,
            ..config
        };
        assert!(BarRequests::of(&regular).use_rth);
    }

    #[test]
    fn accepts_an_end_at_least_the_longest_bar_before_now() {
        let bar_types = [
            bar_type("ESM4.CME-1-HOUR-LAST-EXTERNAL"),
            bar_type("ESM4.CME-1-DAY-LAST-EXTERNAL"),
        ];
        let now = at("2024-06-10T12:00:00Z");
        ensure_bars_closed(&bar_types, at("2024-06-09T12:00:00Z"), now).unwrap();
        ensure_bars_closed(&bar_types, at("2024-06-01T00:00:00Z"), now).unwrap();
    }

    #[test]
    fn rejects_an_end_within_the_longest_bar_of_now() {
        let bar_types = [
            bar_type("ESM4.CME-1-HOUR-LAST-EXTERNAL"),
            bar_type("ESM4.CME-1-DAY-LAST-EXTERNAL"),
        ];
        let now = at("2024-06-10T12:00:00Z");
        let err = ensure_bars_closed(&bar_types, at("2024-06-09T12:00:01Z"), now)
            .unwrap_err()
            .to_string();
        assert!(err.contains("2024-06-09T12:00:01Z"), "{err}");
        assert!(err.contains("2024-06-09T12:00:00Z"), "{err}");
    }

    #[test]
    fn accepts_instruments_when_every_requested_id_loaded() {
        let requested = [InstrumentId::from("ESZ21.GLBX")];
        ensure_all_loaded(&requested, &[es()]).unwrap();
    }

    #[test]
    fn names_the_requested_ids_ib_did_not_return() {
        let requested = [
            InstrumentId::from("ESH4.CME"),
            InstrumentId::from("ESZ21.GLBX"),
            InstrumentId::from("ESM4.CME"),
        ];
        let err = ensure_all_loaded(&requested, &[es()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("ESH4.CME, ESM4.CME"), "{err}");
        assert!(!err.contains("ESZ21.GLBX"), "{err}");
    }

    fn bar_type(s: &str) -> BarType {
        s.parse().unwrap()
    }

    #[test]
    fn maps_a_bar_type_to_the_ib_bar_spec_string() {
        let spec = bar_spec(&bar_type("ESM4.CME-1-HOUR-LAST-EXTERNAL")).unwrap();
        assert_eq!(spec, "1-HOUR-LAST");
    }

    #[test]
    fn maps_every_bar_size_ib_serves() {
        for spec in [
            "1-SECOND-LAST",
            "5-SECOND-BID",
            "15-SECOND-ASK",
            "30-SECOND-MID",
            "1-MINUTE-LAST",
            "2-MINUTE-LAST",
            "3-MINUTE-LAST",
            "5-MINUTE-LAST",
            "10-MINUTE-LAST",
            "15-MINUTE-LAST",
            "20-MINUTE-LAST",
            "30-MINUTE-LAST",
            "1-HOUR-LAST",
            "2-HOUR-LAST",
            "3-HOUR-LAST",
            "4-HOUR-LAST",
            "8-HOUR-LAST",
            "1-DAY-LAST",
        ] {
            let bt = bar_type(&format!("ESM4.CME-{spec}-EXTERNAL"));
            assert_eq!(bar_spec(&bt).unwrap(), spec);
        }
    }

    #[test]
    fn the_duration_of_one_request_depends_on_the_bar_size() {
        for (spec, duration) in [
            ("1-SECOND-LAST", "1800 S"),
            ("5-SECOND-BID", "3600 S"),
            ("15-SECOND-LAST", "14400 S"),
            ("30-SECOND-LAST", "28800 S"),
            ("1-MINUTE-LAST", "86400 S"),
            ("2-MINUTE-LAST", "2 D"),
            ("3-MINUTE-LAST", "5 D"),
            ("5-MINUTE-LAST", "5 D"),
            ("10-MINUTE-LAST", "5 D"),
            ("15-MINUTE-LAST", "10 D"),
            ("20-MINUTE-LAST", "10 D"),
            ("30-MINUTE-LAST", "20 D"),
            ("1-HOUR-LAST", "20 D"),
            ("2-HOUR-LAST", "20 D"),
            ("3-HOUR-LAST", "20 D"),
            ("4-HOUR-LAST", "20 D"),
            ("8-HOUR-LAST", "20 D"),
            ("1-DAY-LAST", "1 Y"),
        ] {
            let bt = bar_type(&format!("ESM4.CME-{spec}-EXTERNAL"));
            assert_eq!(request_duration(&bt), duration, "{spec}");
        }
    }

    #[test]
    fn rejects_a_bar_type_ib_cannot_serve() {
        for bt in [
            "ESM4.CME-10-SECOND-LAST-EXTERNAL",
            "ESM4.CME-4-MINUTE-LAST-EXTERNAL",
            "ESM4.CME-12-MINUTE-LAST-EXTERNAL",
            "ESM4.CME-6-HOUR-LAST-EXTERNAL",
            "ESM4.CME-2-DAY-LAST-EXTERNAL",
            "ESM4.CME-1-WEEK-LAST-EXTERNAL",
            "ESM4.CME-1-MONTH-LAST-EXTERNAL",
            "ESM4.CME-100-TICK-LAST-EXTERNAL",
            "ESM4.CME-1-HOUR-MARK-EXTERNAL",
            "ESM4.CME-1-HOUR-LAST-INTERNAL",
        ] {
            let err = bar_spec(&bar_type(bt)).unwrap_err();
            assert!(err.to_string().contains(bt), "{bt}: {err}");
        }
    }

    #[test]
    fn an_expired_future_is_asked_for_as_an_explicit_contract() {
        let contract = expired_future_contract(InstrumentId::from("ESU5.CME")).unwrap();
        assert_eq!(contract.security_type, SecurityType::Future);
        assert_eq!(contract.symbol.to_string(), "ES");
        assert_eq!(contract.local_symbol, "ESU5");
        assert_eq!(contract.exchange.to_string(), "CME");
        assert_eq!(contract.currency.to_string(), "USD");
        assert!(contract.include_expired);
    }

    #[test]
    fn only_futures_are_asked_for_as_expired_contracts() {
        assert!(expired_future_contract(InstrumentId::from("AAPL.SMART")).is_none());
        assert!(expired_future_contract(InstrumentId::from("EUR/USD.IDEALPRO")).is_none());
        // A stock whose symbol reads like a future.
        assert!(expired_future_contract(InstrumentId::from("ABCZ5.SMART")).is_none());
    }

    #[test]
    fn the_root_of_a_future_drops_month_code_and_year() {
        assert_eq!(future_root("ESU5"), Some("ES"));
        assert_eq!(future_root("ESU25"), Some("ES"));
        assert_eq!(future_root("ZNH6"), Some("ZN"));
        assert_eq!(future_root("ESU2025"), Some("ES"));
        assert_eq!(future_root("ESU123"), None);
        assert_eq!(future_root("ES"), None);
        assert_eq!(future_root("U5"), None);
        assert_eq!(future_root("ESA5"), None);
    }

    /// A 1-HOUR bar of SXR8 priced on `side`, closing at `close`
    /// (nanoseconds) at `price`, with volume 0 at `size_precision`, as IB
    /// gives BID and ASK bars.
    fn side_bar(side: &str, close: u64, price: &str, size_precision: u8) -> Bar {
        use nautilus_model::types::Price;
        let p = Price::from(price);
        let ts = UnixNanos::from(close);
        let bt = bar_type(&format!("SXR8.IBIS-1-HOUR-{side}-EXTERNAL"));
        Bar::new(bt, p, p, p, p, Quantity::new(0.0, size_precision), ts, ts)
    }

    fn bid(close: u64, price: &str) -> Bar {
        side_bar("BID", close, price, 0)
    }

    fn ask(close: u64, price: &str) -> Bar {
        side_bar("ASK", close, price, 0)
    }

    /// The stamp, bid and ask of each quote.
    fn quoted(made: &MadeQuotes) -> Vec<(u64, String, String)> {
        made.quotes
            .iter()
            .map(|q| {
                (
                    q.ts_init.as_u64(),
                    q.bid_price.to_string(),
                    q.ask_price.to_string(),
                )
            })
            .collect()
    }

    #[test]
    fn a_bid_and_ask_pair_makes_a_quote_at_their_closes() {
        let made = quotes_from_bars(
            &[bid(HOUR_NS, "709.4400"), bid(2 * HOUR_NS, "710.2200")],
            &[ask(HOUR_NS, "709.4800"), ask(2 * HOUR_NS, "710.2900")],
        )
        .unwrap();
        assert_eq!(
            quoted(&made),
            [
                (HOUR_NS - 1, "709.4400".into(), "709.4800".into()),
                (2 * HOUR_NS - 1, "710.2200".into(), "710.2900".into()),
            ]
        );
        let quote = made.quotes[0];
        assert_eq!(quote.instrument_id, InstrumentId::from("SXR8.IBIS"));
        assert_eq!(quote.bid_price.precision, 4);
        assert_eq!(quote.ask_price.precision, 4);
        assert_eq!((made.unpaired, made.crossed), (0, 0));
    }

    #[test]
    fn a_quote_is_stamped_one_nanosecond_before_its_bars_close() {
        let made = quotes_from_bars(&[bid(HOUR_NS, "709.44")], &[ask(HOUR_NS, "709.48")]).unwrap();
        let quote = made.quotes[0];
        assert_eq!(quote.ts_event.as_u64(), HOUR_NS - 1);
        assert_eq!(quote.ts_init.as_u64(), HOUR_NS - 1);
    }

    #[test]
    fn both_sides_have_the_nominal_size_at_the_bars_size_precision() {
        for precision in [0, 2] {
            let made = quotes_from_bars(
                &[side_bar("BID", HOUR_NS, "709.44", precision)],
                &[side_bar("ASK", HOUR_NS, "709.48", precision)],
            )
            .unwrap();
            let quote = made.quotes[0];
            for size in [quote.bid_size, quote.ask_size] {
                assert_eq!(size, Quantity::new(1_000_000.0, precision), "{precision}");
                assert_eq!(size.precision, precision);
            }
        }
    }

    #[test]
    fn a_bar_without_the_other_side_is_dropped_and_counted() {
        let made = quotes_from_bars(
            &[bid(HOUR_NS, "709.44"), bid(2 * HOUR_NS, "710.22")],
            &[
                ask(2 * HOUR_NS, "710.29"),
                ask(3 * HOUR_NS, "711.00"),
                ask(4 * HOUR_NS, "712.00"),
            ],
        )
        .unwrap();
        assert_eq!(
            quoted(&made),
            [(2 * HOUR_NS - 1, "710.22".into(), "710.29".into())]
        );
        assert_eq!((made.unpaired, made.crossed), (3, 0));
    }

    #[test]
    fn a_crossed_pair_is_dropped_and_counted_and_a_locked_one_kept() {
        let made = quotes_from_bars(
            &[bid(HOUR_NS, "712.58"), bid(2 * HOUR_NS, "712.48")],
            &[ask(HOUR_NS, "712.28"), ask(2 * HOUR_NS, "712.48")],
        )
        .unwrap();
        assert_eq!(
            quoted(&made),
            [(2 * HOUR_NS - 1, "712.48".into(), "712.48".into())]
        );
        assert_eq!((made.unpaired, made.crossed), (0, 1));
    }

    #[test]
    fn no_bars_make_no_quotes() {
        let made = quotes_from_bars(&[], &[]).unwrap();
        assert!(made.quotes.is_empty());
        assert_eq!((made.unpaired, made.crossed), (0, 0));
    }

    #[test]
    fn a_pair_of_different_precisions_is_an_error() {
        let err =
            quotes_from_bars(&[bid(HOUR_NS, "709.44")], &[ask(HOUR_NS, "709.4800")]).unwrap_err();
        assert!(format!("{err:#}").contains("precision"), "{err:#}");
    }

    #[test]
    fn the_bars_of_quotes_over_a_window_close_one_nanosecond_later() {
        let (from, to) = quoted_bars_window(hour(5), hour(25));
        assert_eq!(from.as_nanosecond(), hour(5).as_nanosecond() + 1);
        assert_eq!(to.as_nanosecond(), hour(25).as_nanosecond() + 1);
    }

    #[test]
    fn missing_ids_are_those_not_loaded() {
        let requested = [
            InstrumentId::from("ESZ21.GLBX"),
            InstrumentId::from("ESU5.CME"),
        ];
        assert_eq!(
            missing_ids(&requested, &[es()]),
            [InstrumentId::from("ESU5.CME")]
        );
    }
}
