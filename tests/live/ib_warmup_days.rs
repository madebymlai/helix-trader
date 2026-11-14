//! A warmup of days on IB, end to end on the Docker paper gateway, with an
//! empty catalog, so that IB answers all of it: the warmup asks in windows
//! under a day, each of which IB's client asks as one request between its
//! bounds, in seconds (see `strategy::warmup`'s `WINDOW`), and gets every
//! minute bar IB's history holds over them, as read a page at a time, and
//! builds each hour bar from them once.
//!
//! Gated as `ib_gateway.rs`: it returns at once, saying it was skipped,
//! unless `NAUTILUS_IB_RUN=1` is set in the shell. It places no order. It
//! tells only within a session of the instrument, SXR8.IBIS or the one
//! `NAUTILUS_IB_INSTRUMENT` names, on a day of no weekend in the 72 hours:
//!
//! ```sh
//! NAUTILUS_IB_RUN=1 cargo test --test live ib_warmup_days:: -- --nocapture
//! ```
//!
//! Client id 46 for the node, 47 for the reference.

use crate::common;

use std::{cell::RefCell, collections::BTreeSet, rc::Rc, time::Duration};

use helix_trader::{
    live::Live,
    strategy::{DataRequirements, warmup::Warmup},
    venues::ib::{IbAccount, IbHistory},
};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, indicators::SharedActorIndicator},
    logging::logger::LoggerConfig,
    messages::data::DataResponseEnvelope,
};
use nautilus_model::{
    data::{Bar, BarType, Data, NautilusDataType},
    identifiers::{StrategyId, TraderId},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};

const TRADER: &str = "HELIXTEST-046";
const WARMUP: SignedDuration = SignedDuration::from_hours(72);

fn composite() -> BarType {
    let instrument =
        std::env::var("NAUTILUS_IB_INSTRUMENT").unwrap_or_else(|_| "SXR8.IBIS".to_string());
    BarType::from(format!("{instrument}-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL").as_str())
}

#[derive(Debug, Default)]
struct Record {
    /// When the warmup was started.
    started: Option<Timestamp>,
    /// The stamps of the minute bars of the responses.
    minutes: Vec<u64>,
    failure: Option<String>,
}

/// Warms up as a strategy does and stops the node when the warmup ends.
#[derive(Debug)]
struct Probe {
    core: StrategyCore,
    warmup: Rc<Warmup>,
    record: Rc<RefCell<Record>>,
}

nautilus_strategy!(Probe);

impl DataActor for Probe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.record.borrow_mut().started = Some(self.clock().utc_now());
        Rc::clone(&self.warmup).start(self)
    }

    fn on_historical_bars(&mut self, bars: &[Bar]) -> anyhow::Result<()> {
        let stamps = bars.iter().map(|bar| bar.ts_event.as_u64());
        self.record.borrow_mut().minutes.extend(stamps);
        Ok(())
    }

    fn on_data_response(&mut self, response: &DataResponseEnvelope) -> anyhow::Result<()> {
        if let Err(e) = Rc::clone(&self.warmup).on_data_response(self, response) {
            self.record.borrow_mut().failure = Some(e.to_string());
        }
        if !self.warmup.is_warming() {
            self.shutdown_system(Some("the warmup ended".to_string()));
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_warmup_of_days_holds_every_minute_bar_of_ibs_history_and_builds_each_hour_bar_once() {
    if !std::env::var("NAUTILUS_IB_RUN").is_ok_and(|value| value == "1") {
        println!("skipped; set NAUTILUS_IB_RUN=1 to run it against IB");
        return;
    }
    crate::claim_process(module_path!());
    dotenvy::dotenv().ok();
    let account: IbAccount = std::env::var("NAUTILUS_IB_ACCOUNT_ID")
        .expect("NAUTILUS_IB_ACCOUNT_ID, a paper account")
        .parse()
        .unwrap();
    assert!(account.to_string().starts_with('D'), "not a paper account");
    let requirements = DataRequirements {
        bar_types: vec![composite()],
        ticks: vec![],
        custom: vec![],
        warmup: WARMUP,
    };
    let fed = Rc::new(crate::FedBars::default());
    let warmup =
        Rc::new(Warmup::live(&requirements).feeding(Rc::clone(&fed) as SharedActorIndicator));
    let record = Rc::new(RefCell::new(Record::default()));
    let probe = Probe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("PROBE-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
        warmup: Rc::clone(&warmup),
        record: Rc::clone(&record),
    };
    // An empty catalog of its own: IB answers the whole warmup.
    let catalog = tempfile::tempdir().unwrap();
    // Outside the catalog, as `Live` requires.
    let staging = tempfile::tempdir().unwrap();
    let live = Live {
        trader_id: TraderId::from(TRADER),
        venues: crate::venues(&["--account", &account.to_string(), "--client-id", "46"]),
        logging: LoggerConfig::default(),
        catalog: catalog.path().to_path_buf(),
        recording: crate::recording(staging.path()),
    };
    let ran = tokio::time::timeout(
        Duration::from_secs(300),
        live.run(&requirements, &warmup, probe, None),
    )
    .await;
    common::cache::delete_trader(TRADER).await;
    ran.expect("the probe stops the node within 5 minutes")
        .unwrap();

    assert_eq!(record.borrow().failure, None);
    assert!(!warmup.is_warming(), "the warmup did not complete");
    // The minute in progress and the one before it are not IB's history yet.
    let started = record.borrow().started.expect("a start");
    let (from, to) = (started - WARMUP, started - SignedDuration::from_mins(2));
    let source = composite().composite();
    let ib = IbHistory::connect(47, to, Timestamp::now()).await.unwrap();
    let reference: BTreeSet<u64> = ib
        .data(&NautilusDataType::Bar, &source.to_string(), from, to)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|datum| match datum {
            Data::Bar(bar) => Some(bar.ts_event.as_u64()),
            _ => None,
        })
        .collect();
    let bounds =
        u64::try_from(from.as_nanosecond()).unwrap()..=u64::try_from(to.as_nanosecond()).unwrap();
    let got: BTreeSet<u64> = record
        .borrow()
        .minutes
        .iter()
        .copied()
        .filter(|stamp| bounds.contains(stamp))
        .collect();
    let missing = reference.difference(&got).count();
    println!(
        "PROBE [{from}, {to}]: {} minute bars of IB's history, {} in the warmup, {missing} missing",
        reference.len(),
        got.len(),
    );
    assert!(!reference.is_empty(), "no history of {source}");
    assert_eq!(missing, 0, "minute bars missing from the warmup");
    let hours = fed.bars.borrow();
    println!("PROBE {} hour bars handed over", hours.len());
    assert!(!hours.is_empty(), "no hour bar handed over");
    assert!(
        hours.windows(2).all(|w| w[0].ts_event < w[1].ts_event),
        "an hour bar handed over twice or out of order"
    );
}
