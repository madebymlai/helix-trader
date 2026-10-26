//! IB's 1-minute bars on the live node, end to end on the Docker paper
//! gateway, with a catalog already holding the instrument's minute bars up
//! to `CATALOG_BEHIND` before now, as fill-catalog leaves it: the warmup of
//! a composite bar type is the catalog's bars, then IB's answer to the
//! source bars' `request_bars` for the rest, the live minute bars come
//! from `subscribe_bars`, and the two meet with no gap and no duplicate:
//! the warmup request ends at now, so the data engine drops the minute
//! still in progress from IB's answer, and the live stream starts with it,
//! not at the catalog's end (no replay: the subscription's `start_ns` is
//! now). Prints how long after its close each live minute bar arrived,
//! which `SOURCE_BAR_DELAY_MICROS` in `strategy::warmup` must exceed.
//!
//! Gated as `ib_gateway.rs`: it returns at once, saying it was skipped,
//! unless `NAUTILUS_IB_RUN=1` is set in the shell. It places no order. It
//! needs SXR8.IBIS quoted on IB (Xetra's full session, 07:30-22:00
//! Frankfurt time, on a trading day), or the instrument
//! `NAUTILUS_IB_INSTRUMENT` names, trading then, and takes about five
//! minutes:
//!
//! ```sh
//! NAUTILUS_IB_RUN=1 cargo test --test ib_minute_bars -- --nocapture
//! ```
//!
//! The node's cache in Redis is under the test's trader, `HELIXTEST-046`,
//! whose records the test deletes once the node has stopped. Client id 46
//! for the node, 47 for filling the catalog.

mod common;

use std::{cell::RefCell, process::Command, rc::Rc, time::Duration};

use helix_trader::{
    data::{IbAccount, IbHistory, fill_catalog},
    live::Live,
    strategy::{DataRequirements, catalog_series, warmup},
};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::DataActor, logging::logger::LoggerConfig, messages::data::DataResponseEnvelope,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_model::{
    data::{Bar, BarType},
    identifiers::{StrategyId, TraderId},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rust_decimal::Decimal;

/// The hour bars of the instrument, built from IB's minute bars.
fn composite() -> BarType {
    let instrument =
        std::env::var("NAUTILUS_IB_INSTRUMENT").unwrap_or_else(|_| "SXR8.IBIS".to_string());
    BarType::from(format!("{instrument}-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL").as_str())
}
const TRADER: &str = "HELIXTEST-046";
const WARMUP: SignedDuration = SignedDuration::from_hours(2);
/// How far before now the catalog's minute bars end.
const CATALOG_BEHIND: SignedDuration = SignedDuration::from_mins(30);
/// The live minute bars the probe waits for before it stops the node.
const LIVE_MINUTES: usize = 4;

/// What the probe got: the minute bars of the warmup response, then each
/// live minute bar with the time it arrived.
#[derive(Debug, Default)]
struct Record {
    /// When the warmup was requested.
    requested_at: UnixNanos,
    warmup: Vec<Bar>,
    /// How the warmup ended: `Ok` complete, or the error.
    warmup_end: Option<Result<(), String>>,
    live: Vec<(Bar, UnixNanos)>,
}

/// Warms up on the composite hour bars through `strategy::warmup`, as a
/// strategy does, and subscribes to their source minute bars too, to see
/// each one; stops the node after `LIVE_MINUTES` of them.
#[derive(Debug)]
struct Probe {
    core: StrategyCore,
    warmup: UUID4,
    record: Rc<RefCell<Record>>,
}

nautilus_strategy!(Probe);

impl DataActor for Probe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let composite = composite();
        self.record.borrow_mut().requested_at = self.clock().timestamp_ns();
        warmup::start(self, &[composite], WARMUP, self.warmup)?;
        self.subscribe_bars(composite.composite(), None, None);
        Ok(())
    }

    fn on_data_response(&mut self, response: &DataResponseEnvelope) -> anyhow::Result<()> {
        let end = match warmup::finished(self.warmup, response) {
            Ok(false) => return Ok(()),
            Ok(true) => Ok(()),
            Err(e) => Err(e.to_string()),
        };
        self.record.borrow_mut().warmup_end = Some(end);
        Ok(())
    }

    fn on_historical_bars(&mut self, bars: &[Bar]) -> anyhow::Result<()> {
        let now = self.clock().timestamp_ns();
        self.record.borrow_mut().warmup.extend_from_slice(bars);
        println!(
            "PROBE warmup response at {now}: {} minute bars, {} to {}",
            bars.len(),
            bars.first().map(|bar| bar.ts_event).unwrap_or_default(),
            bars.last().map(|bar| bar.ts_event).unwrap_or_default(),
        );
        Ok(())
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        let now = self.clock().timestamp_ns();
        println!(
            "PROBE live {} closing {} arrived {now}, {:.3} s after its close",
            bar.bar_type,
            bar.ts_event,
            (now.as_i64() - bar.ts_event.as_i64()) as f64 / 1e9,
        );
        if bar.bar_type.is_externally_aggregated() {
            let mut record = self.record.borrow_mut();
            record.live.push((*bar, now));
            if record.live.len() == LIVE_MINUTES {
                interrupt();
            }
        }
        Ok(())
    }
}

/// Stops the node as Ctrl-C would: its signal handler stops it.
fn interrupt() {
    let pid = std::process::id().to_string();
    let status = Command::new("kill").args(["-INT", &pid]).status();
    assert!(status.is_ok_and(|status| status.success()));
}

const MINUTE_NS: i64 = 60_000_000_000;

#[tokio::test(flavor = "multi_thread")]
async fn the_live_minute_bars_follow_the_warmup_with_no_gap_and_no_duplicate() {
    if !std::env::var("NAUTILUS_IB_RUN").is_ok_and(|value| value == "1") {
        println!("skipped; set NAUTILUS_IB_RUN=1 to run it against IB");
        return;
    }
    dotenvy::dotenv().ok();
    let account: IbAccount = std::env::var("NAUTILUS_IB_ACCOUNT_ID")
        .expect("NAUTILUS_IB_ACCOUNT_ID, a paper account")
        .parse()
        .unwrap();
    assert!(account.to_string().starts_with('D'), "not a paper account");
    let record = Rc::new(RefCell::new(Record::default()));
    let probe = Probe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("PROBE-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
        warmup: UUID4::new(),
        record: Rc::clone(&record),
    };
    let requirements = DataRequirements {
        bar_types: vec![composite()],
        warmup: WARMUP,
    };
    // The catalog: the instrument and its minute bars over
    // [now - WARMUP, now - CATALOG_BEHIND], as fill-catalog stores them.
    let catalog = tempfile::tempdir().unwrap();
    let now = Timestamp::now();
    let catalog_end = now - CATALOG_BEHIND;
    {
        let ib = IbHistory::connect(47, catalog_end, now).await.unwrap();
        let composite = composite();
        let instruments = ib.instruments(&[composite.instrument_id()]).await.unwrap();
        let mut parquet = ParquetDataCatalog::new(catalog.path(), None, None, None, None);
        fill_catalog(
            &mut parquet,
            instruments,
            &catalog_series(&[composite]),
            now - WARMUP,
            catalog_end,
            async |data_type, identifier, from, to| ib.data(data_type, identifier, from, to).await,
        )
        .await
        .unwrap();
    }
    let live = Live {
        trader_id: TraderId::from(TRADER),
        account: Some(account),
        client_id: 46,
        leverage: Decimal::ONE,
        logging: LoggerConfig::default(),
        catalog: catalog.path().to_path_buf(),
    };
    let started = Timestamp::now();
    let ran = tokio::time::timeout(Duration::from_secs(600), live.run(&requirements, probe)).await;
    common::cache::delete_trader(TRADER).await;
    ran.expect("the probe stops the node within 10 minutes")
        .unwrap();

    let record = record.borrow();
    let stamps = |bars: &mut dyn Iterator<Item = &Bar>| -> Vec<i64> {
        bars.map(|bar| bar.ts_event.as_i64()).collect()
    };
    let warmup = stamps(&mut record.warmup.iter());
    let live = stamps(&mut record.live.iter().map(|(bar, _)| bar));
    assert!(warmup.len() > 1, "no warmup bars");
    assert!(
        warmup.windows(2).all(|pair| pair[0] < pair[1]),
        "the warmup bars are not sorted and unique"
    );
    assert!(
        warmup[0] >= (started - WARMUP).as_nanosecond() as i64 - MINUTE_NS,
        "the warmup starts before its window"
    );
    assert_eq!(
        record.warmup_end,
        Some(Ok(())),
        "the warmup did not complete"
    );
    // Both the catalog and IB served it: bars on either side of the
    // catalog's end, and the largest step between two bars.
    let catalog_end = catalog_end.as_nanosecond() as i64;
    let from_catalog = warmup.iter().filter(|&&ts| ts <= catalog_end).count();
    let largest_step = warmup.windows(2).map(|pair| pair[1] - pair[0]).max();
    println!(
        "PROBE warmup: {} bars, {from_catalog} up to the catalog's end {}, \
         the largest step {largest_step:?} ns",
        warmup.len(),
        UnixNanos::from(catalog_end as u64),
    );
    assert!(from_catalog > 0, "no warmup bar from the catalog");
    assert!(from_catalog < warmup.len(), "no warmup bar from IB");
    assert_eq!(live.len(), LIVE_MINUTES);
    assert!(
        live.windows(2).all(|pair| pair[1] - pair[0] == MINUTE_NS),
        "the live minute bars are not one a minute: {live:?}"
    );
    let last_warmup = warmup[warmup.len() - 1];
    println!(
        "PROBE hand-off: warmup requested {}, its last bar closes {}; \
         the first live bar closes {}",
        record.requested_at,
        UnixNanos::from(last_warmup as u64),
        UnixNanos::from(live[0] as u64),
    );
    assert!(
        last_warmup <= record.requested_at.as_i64(),
        "the warmup holds the minute in progress"
    );
    // No replay: IB sends no bar from the catalog's end as live.
    assert!(
        live[0] > record.requested_at.as_i64(),
        "a live bar closing {} before the start: replayed",
        live[0]
    );
    assert_eq!(
        live[0] - last_warmup,
        MINUTE_NS,
        "a gap or a duplicate at the hand-off"
    );
    for (bar, arrived) in &record.live {
        let latency = arrived.as_i64() - bar.ts_event.as_i64();
        assert!(
            (0..5_000_000_000).contains(&latency),
            "the minute closing {} arrived {latency} ns after its close",
            bar.ts_event
        );
    }
}
