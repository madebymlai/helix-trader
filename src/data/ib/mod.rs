//! Interactive Brokers connection and history: the account, the gateway
//! (see `gateway`), the data client config, and instruments and bars for
//! `fill_catalog`, which asks for series of any data type: IB serves its
//! 1-MINUTE bars only (see `IbHistory::data`).
//!
//! Uses `HistoricalInteractiveBrokersClient`
//! (`nautilus_interactive_brokers::historical`), which needs no node: it
//! connects to IB Gateway or TWS, returns `Vec<InstrumentAny>` from
//! `request_instruments` and `Vec<Bar>` from `request_bars`. The bars are
//! stored as IB gives them. Another source is another file next to this
//! one.

use std::str::FromStr;

use anyhow::{Context, ensure};
use ibapi::contracts::{Contract, Symbol};
use jiff::Timestamp;
use nautilus_interactive_brokers::{
    common::symbology::Symbology, config::InteractiveBrokersDataClientConfig,
    historical::HistoricalInteractiveBrokersClient,
};
use nautilus_model::{
    data::{Bar, BarSpecification, BarType, Data, NautilusDataType},
    enums::{BarAggregation, PriceType},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};

mod gateway;
pub use gateway::IbAccount;
pub(crate) use gateway::IbGateway;

/// A connection to IB for history: instruments and bars, no node.
pub struct IbHistory {
    client: HistoricalInteractiveBrokersClient,
    requests: BarRequests,
    symbology: Symbology,
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
        let requests = BarRequests::of(&config);
        let symbology = Symbology::new(config.instrument_provider.symbology_method);
        let address = format!("{}:{}", config.host, config.port);
        let client = HistoricalInteractiveBrokersClient::connect(config)
            .await
            .with_context(|| format!("connecting to IB at {address}"))?;
        Ok(Self {
            client,
            requests,
            symbology,
        })
    }

    /// The latest end of a window IB can serve closed bars over at `now`:
    /// one bar before `now`, the bar still in progress until then (see
    /// `served_spec`).
    ///
    /// # Errors
    ///
    /// Returns an error if that end is out of range.
    pub fn latest_end(now: Timestamp) -> anyhow::Result<Timestamp> {
        Ok(now.checked_sub(served_spec(PriceType::Bid).timedelta())?)
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

    /// The series of `data_type` and `identifier` over `[start, end]`, for
    /// `fill_catalog`: the bars of the bar type `identifier` names, as IB
    /// gives them (see `bars`).
    ///
    /// # Errors
    ///
    /// Returns an error if the series is not one of IB's 1-MINUTE bars (see
    /// `served_bar_type`), or as `bars`.
    pub async fn data(
        &self,
        data_type: &NautilusDataType,
        identifier: &str,
        start: Timestamp,
        end: Timestamp,
    ) -> anyhow::Result<Vec<Data>> {
        let bar_type = served_bar_type(data_type, identifier)?;
        let bars = self.bars(bar_type, start, end).await?;
        Ok(bars.into_iter().map(Data::Bar).collect())
    }

    /// The bars of `bar_type` over `[start, end]`, sorted by `ts_init` and
    /// unique, with the regular-hours setting of the live data client.
    /// Walks back from `end` one request at a time, each of
    /// `BARS_PER_REQUEST` (see `walk_back`).
    async fn bars(
        &self,
        bar_type: BarType,
        start: Timestamp,
        end: Timestamp,
    ) -> anyhow::Result<Vec<Bar>> {
        let spec = bar_type.spec().to_string();
        let duration = BARS_PER_REQUEST;
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
}

/// The bar type of the series of `data_type` and `identifier`, which must
/// be bars IB serves: its own (`EXTERNAL`) 1-MINUTE bars (see
/// `served_spec`), of any price type.
fn served_bar_type(data_type: &NautilusDataType, identifier: &str) -> anyhow::Result<BarType> {
    ensure!(
        *data_type == NautilusDataType::Bar,
        "IB's history serves bars, not {data_type} ({identifier})"
    );
    let bar_type = BarType::from_str(identifier)?;
    let spec = bar_type.spec();
    ensure!(
        bar_type.is_externally_aggregated() && spec == served_spec(spec.price_type),
        "IB's history serves its own 1-MINUTE bars, not {identifier}"
    );
    Ok(bar_type)
}

/// The spec of the bars IB's history serves, priced on `price_type`:
/// 1-MINUTE, the finest bars IB paces softly. Its string (`1-MINUTE-BID`)
/// is IB's bar spec string.
fn served_spec(price_type: PriceType) -> BarSpecification {
    BarSpecification::new(1, BarAggregation::Minute, price_type)
}

/// The IB duration of one bar request: within IB's maximum duration for
/// 1-MINUTE bars. `D` counts whole trading days: a `1 D` request ending in
/// a session's first minute answers that one bar, so `86400 S`, the 1,440
/// bars before the end, across sessions (IB's documented alternative).
const BARS_PER_REQUEST: &str = "86400 S";

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
/// so no hole opens between requests: the adapter's own split of a window
/// into a days segment and a seconds remainder loses bars, because IB
/// counts days in trading sessions.
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
/// like `ESM4.CME`). Never sets
/// `convert_exchange_to_mic_venue`: it makes the venue of a CME contract
/// change between processes.
pub(crate) fn data_client_config(gateway: IbGateway) -> InteractiveBrokersDataClientConfig {
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

    /// A 1-HOUR bar of ESM4 closing at hour `h`.
    fn hourly(h: u64) -> Bar {
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
    fn ib_serves_its_own_one_minute_bars_of_any_price_type() {
        for (identifier, spec) in [
            ("SXR8.IBIS-1-MINUTE-MID-EXTERNAL", "1-MINUTE-MID"),
            ("SXR8.IBIS-1-MINUTE-BID-EXTERNAL", "1-MINUTE-BID"),
            ("SXR8.IBIS-1-MINUTE-ASK-EXTERNAL", "1-MINUTE-ASK"),
            ("ESZ6.CME-1-MINUTE-LAST-EXTERNAL", "1-MINUTE-LAST"),
        ] {
            let served = served_bar_type(&NautilusDataType::Bar, identifier).unwrap();
            assert_eq!(served, bar_type(identifier));
            // IB's bar spec string, as `bars` requests it.
            assert_eq!(served.spec().to_string(), spec);
        }
    }

    #[test]
    fn ib_serves_no_other_series() {
        for (data_type, identifier) in [
            (NautilusDataType::QuoteTick, "SXR8.IBIS"),
            (NautilusDataType::Bar, "SXR8.IBIS-1-HOUR-MID-EXTERNAL"),
            (NautilusDataType::Bar, "SXR8.IBIS-5-MINUTE-MID-EXTERNAL"),
            (NautilusDataType::Bar, "SXR8.IBIS-1-MINUTE-MID-INTERNAL"),
        ] {
            let err = served_bar_type(&data_type, identifier).unwrap_err();
            assert!(err.to_string().contains(identifier), "{err}");
        }
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
            expired_future_contract(symbology(), InstrumentId::from("ESU5.CME")).unwrap();
        assert_eq!(contract.security_type, SecurityType::Future);
        assert_eq!(contract.symbol.to_string(), "ES");
        assert_eq!(contract.local_symbol, "ESU5");
        assert_eq!(contract.exchange.to_string(), "CME");
        assert_eq!(contract.currency.to_string(), "USD");
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
}
