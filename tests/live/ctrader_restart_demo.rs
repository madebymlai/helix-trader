//! A cTrader bracket over a restart of the node, end to end on a broker's
//! demo: what Nautilus' reconciliation makes of the broker's reports when
//! the node comes back, in particular whether every fill it applies is one
//! of the broker's deals (its deal id the trade id, its commission the
//! broker's) and no order status waits on a fill that never comes, as the
//! execution client requires (`requires_authoritative_fills`).
//!
//! Each node runs in a child process of this test binary (`phase`, one
//! node per process) under the test's own trader, so its cache in Redis is
//! apart from `live`'s; the test deletes it at the end. The probe sends one
//! bracket of 1 oz of gold, a market buy with its stop loss and take
//! profit, and stops the node once the position holds both, leaving it
//! open; a later node reads it back. After each test the broker's
//! execution client reads the account: no position, no order open.
//!
//! It needs what `ctrader_orders_demo.rs` needs: the gate, gold's market
//! open, the account flat in gold. Its tests trade the same account, so
//! they run one at a time:
//!
//! ```sh
//! NAUTILUS_CTRADER_RUN=pepperstone cargo test --test live ctrader_restart_demo:: -- --test-threads=1 --nocapture
//! ```

use std::{cell::RefCell, path::PathBuf, process::Command, rc::Rc, time::Duration};

use ctrader::{CTraderExecutionClientConfig, CTraderExecutionClientFactory};
use helix_trader::{
    live::Live,
    strategy::{DataRequirements, warmup::Warmup},
};
use jiff::SignedDuration;
use nautilus_common::{
    actor::DataActor, cache::Cache, clock::VirtualClock, factories::ExecutionClientFactory,
    live::runner::replace_exec_event_sender, logging::logger::LoggerConfig,
    messages::ExecutionEvent,
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    accounts::AccountAny,
    data::{BarType, QuoteTick},
    enums::{OrderSide, OrderStatus},
    events::{OrderEventAny, OrderSubmitted},
    identifiers::{ClientOrderId, InstrumentId, StrategyId, TraderId, VenueOrderId},
    orders::{Order, OrderAny},
    position::Position,
    reports::PositionStatusReport,
    types::{Price, Quantity},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    common::cache::delete_trader,
    demo_broker::{DemoBroker, opted_in},
};

const PHASE: &str = "HELIX_CTRADER_RESTART_PHASE";
const STATE: &str = "HELIX_CTRADER_RESTART_STATE";
const TRADER: &str = "HELIX_CTRADER_RESTART_TRADER";

/// How long `Observe` lets the node run before it looks: past startup
/// reconciliation, three open-order checks and a position check
/// (`live::node`'s intervals, 10 s and 30 s).
const SETTLE: Duration = Duration::from_secs(40);

/// What a probe does on its node.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
enum Step {
    /// On a flat account, one bracket: a market buy of 1 oz, its stop loss
    /// `distance` below the bid, its take profit `distance` above the ask.
    /// Once the entry filled and the position holds both, saves the orders
    /// and the entry's fills, and stops, the position left open. With
    /// `reduce`, the bracket is of 2 oz, and once the position holds both
    /// protections it is reduced by 1 oz, by a sell naming it; the save
    /// waits for the position and both legs to be of 1 oz.
    Open { distance: f64, reduce: bool },
    /// On a flat account, a market buy of 1 oz, saved as the entry; once
    /// it is submitted, stops the process as a crash would, before the
    /// broker's answer is processed: the order is in flight in the cache.
    Crash,
    /// Lets the node settle (`SETTLE`), then saves what it holds: the open
    /// positions, the live orders, and each of the last step's orders.
    Observe,
    /// Closes every position, then cancels every order left, and stops once
    /// flat, or `SETTLE` after its cancels with the orders left noted.
    Flatten,
}

/// What a step leaves for the next, through `STATE`.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Saved {
    position_id: String,
    entry: String,
    stop_loss: String,
    take_profit: String,
    /// `Open`: the entry's fills.
    entry_fills: Vec<Fill>,
    /// `Observe`: the open positions, the live orders, and `Open`'s orders
    /// as the node holds them.
    positions_open: Vec<String>,
    live_orders: Vec<(String, OrderStatus)>,
    orders: Vec<Held>,
}

/// An order as the node holds it.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Held {
    id: String,
    status: Option<OrderStatus>,
    quantity: Option<String>,
    filled: Option<String>,
    fills: Vec<Fill>,
}

/// A fill of an order, as its event holds it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Fill {
    trade_id: String,
    quantity: String,
    price: String,
    commission: Option<(f64, String)>,
}

impl Fill {
    /// Whether it is one of the broker's deals: cTrader's deal ids are
    /// numbers, and every deal of gold is charged.
    fn is_a_deal(&self, currency: &str) -> bool {
        self.trade_id.parse::<i64>().is_ok()
            && self
                .commission
                .as_ref()
                .is_some_and(|(amount, code)| *amount > 0.0 && code == currency)
    }
}

/// One step, in a child process: a live node with the probe, stopped by
/// the probe. Runs only when the parent test names a step.
#[tokio::test(flavor = "multi_thread")]
async fn phase() {
    let Ok(step) = std::env::var(PHASE) else {
        return;
    };
    let step: Step = serde_json::from_str(&step).unwrap();
    let broker = opted_in("phase", module_path!()).expect("a phase runs opted in");
    let requirements = DataRequirements {
        bar_types: [broker.gold(), broker.eurusd()]
            .map(|id| BarType::from(format!("{id}-1-MINUTE-MID-INTERNAL").as_str()))
            .to_vec(),
        ticks: vec![],
        warmup: SignedDuration::from_mins(2),
    };
    // An empty catalog of its own: the venue answers the whole warmup.
    let catalog = tempfile::tempdir().unwrap();
    // Outside the catalog, as `Live` requires.
    let staging = tempfile::tempdir().unwrap();
    let live = Live {
        trader_id: TraderId::from(std::env::var(TRADER).unwrap().as_str()),
        venues: crate::venues(&[]),
        leverage: Decimal::ONE,
        logging: LoggerConfig::default(),
        catalog: catalog.path().to_path_buf(),
        recording: crate::recording(staging.path()),
    };
    let probe = Probe::new(step, broker, PathBuf::from(std::env::var(STATE).unwrap()));
    let ran = tokio::time::timeout(
        Duration::from_secs(300),
        live.run(&requirements, &Warmup::live(&requirements), probe, None),
    )
    .await;
    ran.expect("the node stops within 5 minutes").unwrap();
    let report: Report =
        serde_json::from_str(&std::fs::read_to_string(report_path()).unwrap()).unwrap();
    println!("PROBE {step:?}: {report:#?}");
    assert!(report.failures.is_empty(), "{:#?}", report.failures);
    assert!(report.done, "the probe did not finish");
}

/// A bracket held over a restart comes back as it was: the entry filled by
/// its deal alone, both protections open on the same position.
#[tokio::test(flavor = "multi_thread")]
async fn a_bracket_held_over_a_restart_comes_back_as_it_was() {
    let Some(broker) = opted_in(
        "a_bracket_held_over_a_restart_comes_back_as_it_was",
        module_path!(),
    ) else {
        return;
    };
    let run = Run::new("HELIXTEST-103");
    run.phase(Step::Open {
        distance: 30.0,
        reduce: false,
    });
    let opened = run.saved();
    run.phase(Step::Observe);
    let observed = run.saved();
    run.phase(Step::Flatten);
    assert_eq!(
        open_at_broker(broker).await,
        (0, 0),
        "the account is not flat"
    );

    println!("opened: {opened:#?}\nobserved: {observed:#?}");
    assert_eq!(
        observed.positions_open,
        std::slice::from_ref(&opened.position_id)
    );
    let held = |id: &str| observed.orders.iter().find(|o| o.id == id).unwrap().clone();
    let entry = held(&opened.entry);
    assert_eq!(entry.status, Some(OrderStatus::Filled), "{entry:?}");
    assert_eq!(entry.fills, opened.entry_fills, "the entry's fills changed");
    for leg in [&opened.stop_loss, &opened.take_profit] {
        let leg = held(leg);
        assert_eq!(leg.status, Some(OrderStatus::Accepted), "{leg:?}");
        assert!(leg.fills.is_empty(), "{leg:?}");
    }
}

/// A position closed by its stop loss or take profit while the node was
/// down: the restarted node fills that leg by the broker's closing deal,
/// cancels the other, and holds nothing open.
#[tokio::test(flavor = "multi_thread")]
async fn a_bracket_leg_filled_while_the_node_was_down_fills_by_its_deal() {
    let Some(broker) = opted_in(
        "a_bracket_leg_filled_while_the_node_was_down_fills_by_its_deal",
        module_path!(),
    ) else {
        return;
    };
    let run = Run::new("HELIXTEST-104");
    run.phase(Step::Open {
        distance: 2.0,
        reduce: false,
    });
    let opened = run.saved();
    let closed = closed_at_broker(broker, Duration::from_secs(15 * 60)).await;
    run.phase(Step::Observe);
    let observed = run.saved();
    run.phase(Step::Flatten);
    assert_eq!(
        open_at_broker(broker).await,
        (0, 0),
        "the account is not flat"
    );
    let closed = closed.expect(
        "the position was closed when first read after the node stopped: a leg may have filled \
         while the node was up, so the run does not count; run it again",
    );
    assert!(closed, "neither protection was hit within 15 minutes");

    println!("opened: {opened:#?}\nobserved: {observed:#?}");
    assert!(observed.positions_open.is_empty(), "{observed:?}");
    assert!(observed.live_orders.is_empty(), "{observed:?}");
    let held = |id: &str| observed.orders.iter().find(|o| o.id == id).unwrap().clone();
    let entry = held(&opened.entry);
    assert_eq!(entry.status, Some(OrderStatus::Filled), "{entry:?}");
    assert_eq!(entry.fills, opened.entry_fills, "the entry's fills changed");
    let legs = [held(&opened.stop_loss), held(&opened.take_profit)];
    let filled: Vec<&Held> = legs
        .iter()
        .filter(|l| l.status == Some(OrderStatus::Filled))
        .collect();
    let [filled] = filled.as_slice() else {
        panic!("not one leg filled: {legs:?}");
    };
    let [fill] = filled.fills.as_slice() else {
        panic!("not one fill of the leg: {filled:?}");
    };
    assert!(
        fill.is_a_deal(broker.commission_currency()),
        "the leg's fill is not a deal of the broker's: {fill:?}"
    );
    let other = legs.iter().find(|l| l.id != filled.id).unwrap();
    assert_eq!(other.status, Some(OrderStatus::Canceled), "{other:?}");
}

/// A bracket of 2 oz reduced by 1 oz, then closed by one of its legs
/// while the node was down: the broker's leg reports fill what is left,
/// 1 oz, as its deal does, and the restarted node fills that leg by the
/// deal, its filled quantity the deal's, and cancels the other.
#[tokio::test(flavor = "multi_thread")]
async fn a_leg_hit_after_a_partial_close_fills_what_is_left() {
    let Some(broker) = opted_in(
        "a_leg_hit_after_a_partial_close_fills_what_is_left",
        module_path!(),
    ) else {
        return;
    };
    let run = Run::new("HELIXTEST-107");
    run.phase(Step::Open {
        distance: 2.0,
        reduce: true,
    });
    let opened = run.saved();
    let closed = closed_at_broker(broker, Duration::from_secs(15 * 60)).await;
    let at_broker = legs_at_broker(broker, &opened.position_id).await;
    run.phase(Step::Observe);
    let observed = run.saved();
    run.phase(Step::Flatten);
    assert_eq!(
        open_at_broker(broker).await,
        (0, 0),
        "the account is not flat"
    );
    let closed = closed.expect(
        "the position was closed when first read after the node stopped: a leg may have filled \
         while the node was up, so the run does not count; run it again",
    );
    assert!(closed, "neither protection was hit within 15 minutes");

    println!("opened: {opened:#?}\nat the broker: {at_broker:#?}\nobserved: {observed:#?}");
    for leg in &at_broker {
        assert_eq!(leg.filled, leg.fills, "{leg:?}");
    }
    assert!(observed.positions_open.is_empty(), "{observed:?}");
    assert!(observed.live_orders.is_empty(), "{observed:?}");
    let held = |id: &str| observed.orders.iter().find(|o| o.id == id).unwrap().clone();
    let legs = [held(&opened.stop_loss), held(&opened.take_profit)];
    let filled: Vec<&Held> = legs
        .iter()
        .filter(|l| l.status == Some(OrderStatus::Filled))
        .collect();
    let [filled] = filled.as_slice() else {
        panic!("not one leg filled: {legs:?}");
    };
    let [fill] = filled.fills.as_slice() else {
        panic!("not one fill of the leg: {filled:?}");
    };
    assert!(
        fill.is_a_deal(broker.commission_currency()),
        "the leg's fill is not a deal of the broker's: {fill:?}"
    );
    assert_eq!(fill.quantity, "1", "{fill:?}");
    assert_eq!(filled.filled.as_deref(), Some("1"), "{filled:?}");
    let other = legs.iter().find(|l| l.id != filled.id).unwrap();
    assert_eq!(other.status, Some(OrderStatus::Canceled), "{other:?}");
}

/// A market order in flight in the cache when the node died, filled at the
/// broker meanwhile: the restarted node fills it by the broker's deal, not
/// by a fill inferred from the order's status, and holds its position.
#[tokio::test(flavor = "multi_thread")]
async fn a_market_order_in_flight_when_the_node_died_fills_by_its_deal() {
    let Some(broker) = opted_in(
        "a_market_order_in_flight_when_the_node_died_fills_by_its_deal",
        module_path!(),
    ) else {
        return;
    };
    let run = Run::new("HELIXTEST-106");
    run.crash();
    let crashed = run.saved();
    run.phase(Step::Observe);
    let observed = run.saved();
    run.phase(Step::Flatten);
    assert_eq!(
        open_at_broker(broker).await,
        (0, 0),
        "the account is not flat"
    );

    println!("crashed: {crashed:#?}\nobserved: {observed:#?}");
    assert_eq!(observed.positions_open.len(), 1, "{observed:?}");
    assert!(observed.live_orders.is_empty(), "{observed:?}");
    let [entry] = observed.orders.as_slice() else {
        panic!("not the one order: {observed:?}");
    };
    assert_eq!(entry.id, crashed.entry);
    assert_eq!(entry.status, Some(OrderStatus::Filled), "{entry:?}");
    let [fill] = entry.fills.as_slice() else {
        panic!("not one fill of the order: {entry:?}");
    };
    assert!(
        fill.is_a_deal(broker.commission_currency()),
        "the order's fill is not a deal of the broker's: {fill:?}"
    );
}

/// The steps of one test: a trader of its own and the file its steps
/// share. Dropping it deletes the trader's records in Redis.
struct Run {
    trader: &'static str,
    state: tempfile::NamedTempFile,
}

impl Run {
    fn new(trader: &'static str) -> Self {
        Self {
            trader,
            state: tempfile::NamedTempFile::new().unwrap(),
        }
    }

    fn command(&self, step: Step) -> Command {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "ctrader_restart_demo::phase", "--nocapture"])
            .env(PHASE, serde_json::to_string(&step).unwrap())
            .env(STATE, self.state.path())
            .env(TRADER, self.trader);
        command
    }

    /// Runs `step` in a child process, and panics if it fails.
    fn phase(&self, step: Step) {
        let status = self.command(step).status().unwrap();
        assert!(status.success(), "step {step:?} failed");
    }

    /// Runs `Crash`, which aborts its process.
    fn crash(&self) {
        let status = self.command(Step::Crash).status().unwrap();
        assert!(!status.success(), "the crash step exited cleanly");
    }

    /// What the last step left in `STATE`.
    fn saved(&self) -> Saved {
        serde_json::from_str(&std::fs::read_to_string(self.state.path()).unwrap()).unwrap()
    }
}

impl Drop for Run {
    fn drop(&mut self) {
        let deleted = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(delete_trader(self.trader))
        });
        println!("deleted the Redis records of {}: {deleted:?}", self.trader);
    }
}

/// The broker's execution client on the demo account, connected, as the
/// node's is.
async fn connected(broker: &DemoBroker) -> Box<dyn nautilus_common::clients::ExecutionClient> {
    let (tx, mut events) = tokio::sync::mpsc::unbounded_channel();
    replace_exec_event_sender(tx);
    let cache = Rc::new(RefCell::new(Cache::default()));
    let config = CTraderExecutionClientConfig::new(broker.connection(), vec![broker.gold()]);
    let factory = CTraderExecutionClientFactory::new(&config);
    let mut client = factory
        .create(
            TraderId::from("HELIXTEST-105"),
            factory.name(),
            &config,
            cache.clone().into(),
            Rc::new(RefCell::new(VirtualClock::new())),
        )
        .unwrap();
    client.start().unwrap();
    let register = async {
        let event = tokio::time::timeout(Duration::from_secs(30), events.recv())
            .await
            .expect("no account state within 30 s")
            .expect("the execution events ended");
        let ExecutionEvent::Account(state) = event else {
            panic!("not an account state: {event:?}");
        };
        cache
            .borrow_mut()
            .add_account(AccountAny::from_events(std::slice::from_ref(&state)).unwrap())
            .unwrap();
    };
    let (connected, ()) = tokio::join!(client.connect(), register);
    connected.unwrap();
    client
}

/// The open positions and orders the broker's execution client reports on
/// the demo account, as the node's startup reconciliation reads them: the
/// mass status also holds the orders closed over its window.
async fn open_at_broker(broker: &DemoBroker) -> (usize, usize) {
    let mut client = connected(broker).await;
    let mass_status = client.generate_mass_status(Some(60)).await;
    client.disconnect().await.unwrap();
    let mass_status = mass_status.unwrap().unwrap();
    (
        mass_status
            .position_reports()
            .values()
            .flatten()
            .filter(|r| !r.is_flat())
            .count(),
        mass_status
            .order_reports()
            .values()
            .filter(|r| !r.order_status.is_closed())
            .count(),
    )
}

/// Whether the broker holds no open position, read at once, then every 5 s
/// for up to `wait`: the bracket's position closed by one of its
/// protections. Called once the node is down, its process gone: `None` if
/// the first read finds the position closed already, as it may have closed
/// while the node was up.
async fn closed_at_broker(broker: &DemoBroker, wait: Duration) -> Option<bool> {
    let mut client = connected(broker).await;
    let deadline = tokio::time::Instant::now() + wait;
    let flat = async || {
        let mass_status = client.generate_mass_status(Some(1)).await.unwrap().unwrap();
        mass_status
            .position_reports()
            .values()
            .flatten()
            .all(PositionStatusReport::is_flat)
    };
    let closed = if flat().await {
        None
    } else {
        let mut closed = false;
        while !closed && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_secs(5)).await;
            closed = flat().await;
        }
        Some(closed)
    };
    client.disconnect().await.unwrap();
    println!("closed at the broker: {closed:?}");
    closed
}

/// A leg of a position as the broker's execution client reports it in a
/// mass status: its venue order id, status, quantity and filled quantity,
/// and the quantity its fills add up to.
#[derive(Debug)]
#[expect(dead_code, reason = "printed")]
struct LegAtBroker {
    venue_order_id: String,
    status: OrderStatus,
    quantity: Quantity,
    filled: Quantity,
    fills: Quantity,
}

/// The legs of `position` the broker's execution client reports, closed,
/// in a mass status over the last hour.
async fn legs_at_broker(broker: &DemoBroker, position: &str) -> Vec<LegAtBroker> {
    let mut client = connected(broker).await;
    let mass_status = client.generate_mass_status(Some(60)).await;
    client.disconnect().await.unwrap();
    let mass_status = mass_status.unwrap().unwrap();
    let fills = mass_status.fill_reports();
    ["SL", "TP"]
        .map(|leg| VenueOrderId::from(format!("{position}-{leg}").as_str()))
        .iter()
        .filter_map(|id| {
            let report = mass_status.order_reports().get(id)?.clone();
            let filled_by_fills = fills
                .get(id)
                .into_iter()
                .flatten()
                .fold(Quantity::zero(report.filled_qty.precision), |sum, f| {
                    sum + f.last_qty
                });
            Some(LegAtBroker {
                venue_order_id: id.to_string(),
                status: report.order_status,
                quantity: report.quantity,
                filled: report.filled_qty,
                fills: filled_by_fills,
            })
        })
        .collect()
}

/// What the probe saw, written next to `STATE` for the child's test.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Report {
    lines: Vec<String>,
    failures: Vec<String>,
    done: bool,
}

fn report_path() -> PathBuf {
    PathBuf::from(format!("{}.report", std::env::var(STATE).unwrap()))
}

#[derive(Debug)]
struct Probe {
    core: StrategyCore,
    gold: InstrumentId,
    eurusd: InstrumentId,
    step: Step,
    state: PathBuf,
    /// The bracket's entry, stop loss and take profit, once sent.
    bracket: Option<[ClientOrderId; 3]>,
    rate_known: bool,
    first_quote: Option<UnixNanos>,
    /// `Crash`: whether the buy went out; `Open` with `reduce`: whether
    /// the reducing sell did.
    sent: bool,
    /// `Flatten`: whether the positions were closed, and when the orders
    /// left were cancelled.
    closing: bool,
    cancelled_at: Option<UnixNanos>,
    report: Report,
}

impl Probe {
    fn new(step: Step, broker: &DemoBroker, state: PathBuf) -> Self {
        Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from("CTRADER_RESTART-001")),
                order_id_tag: Some("001".to_string()),
                ..Default::default()
            }),
            gold: broker.gold(),
            eurusd: broker.eurusd(),
            step,
            state,
            bracket: None,
            rate_known: false,
            first_quote: None,
            sent: false,
            closing: false,
            cancelled_at: None,
            report: Report::default(),
        }
    }

    fn note(&mut self, line: String) {
        println!("PROBE {line}");
        self.report.lines.push(line);
    }

    fn fail(&mut self, why: String) {
        println!("PROBE FAILURE {why}");
        self.report.failures.push(why);
    }

    fn open_positions(&self) -> Vec<Position> {
        self.cache()
            .positions_open(None, Some(&self.gold), None, None, None)
            .iter()
            .map(|position| (*position).clone())
            .collect()
    }

    /// The open and in-flight orders of gold, any strategy.
    fn live_orders(&self) -> Vec<OrderAny> {
        let cache = self.cache();
        let id = Some(&self.gold);
        let mut orders: Vec<OrderAny> = cache
            .orders_open(None, id, None, None, None)
            .iter()
            .map(|order| (*order).clone())
            .collect();
        orders.extend(
            cache
                .orders_inflight(None, id, None, None, None)
                .iter()
                .map(|order| (*order).clone()),
        );
        orders
    }

    fn held(&self, id: &str) -> Held {
        let order = self.cache().order(&ClientOrderId::from(id));
        Held {
            id: id.to_string(),
            status: order.as_ref().map(OrderAny::status),
            quantity: order.as_ref().map(|o| o.quantity().to_string()),
            filled: order.as_ref().map(|o| o.filled_qty().to_string()),
            fills: order.as_ref().map(fills).unwrap_or_default(),
        }
    }

    fn note_state(&mut self, when: &str) {
        let positions = self.open_positions();
        let orders = self.live_orders();
        self.note(format!(
            "{when}: {} positions open, {} orders open or in flight",
            positions.len(),
            orders.len()
        ));
        for position in positions {
            self.note(format!(
                "  position {} {:?} qty={} orders={:?}",
                position.id,
                position.side,
                position.quantity,
                position.client_order_ids(),
            ));
        }
        for order in orders {
            self.note(format!(
                "  order {} {:?} {:?} {} status={:?} venue_order_id={:?}",
                order.client_order_id(),
                order.order_type(),
                order.order_side(),
                order.quantity(),
                order.status(),
                order.venue_order_id(),
            ));
        }
    }

    fn save(&self, saved: &Saved) -> anyhow::Result<()> {
        Ok(std::fs::write(&self.state, serde_json::to_string(saved)?)?)
    }

    fn saved(&self) -> anyhow::Result<Saved> {
        Ok(serde_json::from_str(&std::fs::read_to_string(
            &self.state,
        )?)?)
    }

    fn finish(&mut self) -> anyhow::Result<()> {
        self.report.done = true;
        std::fs::write(report_path(), serde_json::to_string(&self.report)?)?;
        interrupt();
        Ok(())
    }

    /// `Open`: the bracket on a flat account; then, once the position
    /// holds both protections, the save and the stop.
    fn open(&mut self, quote: &QuoteTick, distance: f64, reduce: bool) -> anyhow::Result<()> {
        let Some([entry, stop_loss, take_profit]) = self.bracket else {
            if !self.open_positions().is_empty() || !self.live_orders().is_empty() {
                self.note_state("not flat");
                self.fail("gold not flat at the start".to_string());
                return self.finish();
            }
            let orders = self
                .order()
                .bracket()
                .instrument_id(self.gold)
                .order_side(OrderSide::Buy)
                .quantity(Quantity::from(if reduce { 2 } else { 1 }))
                .sl_trigger_price(Price::new(quote.bid_price.as_f64() - distance, 2))
                .tp_price(Price::new(quote.ask_price.as_f64() + distance, 2))
                .call();
            let ids: Vec<ClientOrderId> = orders.iter().map(OrderAny::client_order_id).collect();
            self.bracket = Some(ids.try_into().expect("a bracket is three orders"));
            self.note(format!(
                "bracket {:?} at bid {} ask {}",
                self.bracket, quote.bid_price, quote.ask_price
            ));
            return self.submit_order_list(orders, None, None, None);
        };
        let statuses = [entry, stop_loss, take_profit]
            .map(|id| self.cache().order(&id).map(|order| order.status()));
        let [entry_status, legs @ ..] = statuses;
        let ended = |status: &Option<OrderStatus>| {
            matches!(
                status,
                Some(
                    OrderStatus::Canceled
                        | OrderStatus::Rejected
                        | OrderStatus::Denied
                        | OrderStatus::Expired
                )
            )
        };
        if ended(&entry_status)
            || legs
                .iter()
                .any(|leg| ended(leg) || *leg == Some(OrderStatus::Filled))
        {
            self.note_state("open failed");
            self.fail(format!("the bracket is {statuses:?}"));
            return self.finish();
        }
        if statuses
            != [
                Some(OrderStatus::Filled),
                Some(OrderStatus::Accepted),
                Some(OrderStatus::Accepted),
            ]
        {
            return Ok(());
        }
        let [position] = self
            .open_positions()
            .try_into()
            .map_err(|positions| anyhow::anyhow!("one position expected: {positions:?}"))?;
        if reduce {
            if !self.sent {
                self.sent = true;
                self.note_state("reducing");
                let order = self.order().market(
                    self.gold,
                    OrderSide::Sell,
                    Quantity::from(1),
                    None,
                    Some(true),
                    None,
                    None,
                    None,
                    None,
                    None,
                );
                return self.submit_order(order, Some(position.id), None, None);
            }
            let one = Quantity::from(1);
            let legs = [stop_loss, take_profit]
                .map(|id| self.cache().order(&id).map(|order| order.quantity()));
            if position.quantity != one || legs != [Some(one), Some(one)] {
                return Ok(());
            }
        }
        self.note_state("open");
        let entry_fills = self.held(entry.as_str()).fills;
        self.save(&Saved {
            position_id: position.id.to_string(),
            entry: entry.to_string(),
            stop_loss: stop_loss.to_string(),
            take_profit: take_profit.to_string(),
            entry_fills,
            ..Saved::default()
        })?;
        self.finish()
    }

    /// `Observe`: what the node holds, once settled.
    fn observe(&mut self) -> anyhow::Result<()> {
        self.note_state("observed");
        let left = self.saved()?;
        let orders: Vec<Held> = [&left.entry, &left.stop_loss, &left.take_profit]
            .into_iter()
            .filter(|id| !id.is_empty())
            .map(|id| self.held(id))
            .collect();
        for order in &orders {
            self.note(format!("  {order:?}"));
        }
        self.save(&Saved {
            positions_open: self
                .open_positions()
                .iter()
                .map(|p| p.id.to_string())
                .collect(),
            live_orders: self
                .live_orders()
                .iter()
                .map(|o| (o.client_order_id().to_string(), o.status()))
                .collect(),
            orders,
            ..left
        })?;
        self.finish()
    }

    /// `Crash`: the buy on a flat account (see `on_order_submitted`).
    fn crash(&mut self) -> anyhow::Result<()> {
        if !self.open_positions().is_empty() || !self.live_orders().is_empty() {
            self.note_state("not flat");
            self.fail("gold not flat at the start".to_string());
            return self.finish();
        }
        self.sent = true;
        let order = self.order().market(
            self.gold,
            OrderSide::Buy,
            Quantity::from(1),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        let id = order.client_order_id();
        self.save(&Saved {
            entry: id.to_string(),
            ..Saved::default()
        })?;
        self.note(format!("submitting {id}, a market buy"));
        self.submit_order(order, None, None, None)
    }

    /// `Flatten`: closes the positions; once none is open, cancels the
    /// orders left; stops once flat, or `SETTLE` after the cancels, noting
    /// the orders left.
    fn flatten(&mut self, now: UnixNanos) -> anyhow::Result<()> {
        let positions = self.open_positions();
        let orders = self.live_orders();
        if positions.is_empty() && orders.is_empty() {
            self.note_state("flat");
            return self.finish();
        }
        if !positions.is_empty() {
            if !self.closing {
                self.closing = true;
                self.note_state("closing");
                for position in &positions {
                    self.close_position(position, None, None, None, None, None, None)?;
                }
            }
            return Ok(());
        }
        let Some(cancelled_at) = self.cancelled_at else {
            self.cancelled_at = Some(now);
            self.note_state("cancelling");
            for order in orders {
                self.cancel_order(order.client_order_id(), None, None)?;
            }
            return Ok(());
        };
        if now.as_u64().saturating_sub(cancelled_at.as_u64()) >= u64::try_from(SETTLE.as_nanos())? {
            self.note_state("orders left after their cancels");
            return self.finish();
        }
        Ok(())
    }
}

/// The fills of `order`, from its events.
fn fills(order: &OrderAny) -> Vec<Fill> {
    order
        .events()
        .into_iter()
        .filter_map(|event| match event {
            OrderEventAny::Filled(fill) => Some(Fill {
                trade_id: fill.trade_id.to_string(),
                quantity: fill.last_qty.to_string(),
                price: fill.last_px.to_string(),
                commission: fill
                    .commission
                    .map(|c| (c.as_f64(), c.currency.code.to_string())),
            }),
            _ => None,
        })
        .collect()
}

/// Stops the node as Ctrl-C would: its signal handler stops it.
fn interrupt() {
    let pid = std::process::id().to_string();
    let status = Command::new("kill").args(["-INT", &pid]).status();
    assert!(status.is_ok_and(|status| status.success()));
}

nautilus_strategy!(Probe, {
    /// In `Crash`, aborts once the order is submitted, after long enough
    /// for Redis to receive it (its writer runs on another thread) but with
    /// the node thread held, so the broker's answer is not processed.
    fn on_order_submitted(&mut self, event: OrderSubmitted) {
        if self.step == Step::Crash {
            std::thread::sleep(Duration::from_millis(500));
            println!("PROBE aborting with {} submitted", event.client_order_id);
            std::process::abort();
        }
    }
});

impl DataActor for Probe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.note_state("start");
        // EUR/USD for the risk engine on an account in EUR (see
        // `ctrader_orders_demo.rs`).
        self.subscribe_quotes(self.eurusd, None, None);
        self.subscribe_quotes(self.gold, None, None);
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        if quote.instrument_id == self.eurusd {
            self.rate_known = true;
            return Ok(());
        }
        if self.report.done || !self.rate_known {
            return Ok(());
        }
        match self.step {
            Step::Open { distance, reduce } => self.open(quote, distance, reduce),
            Step::Crash if !self.sent => self.crash(),
            Step::Crash => Ok(()),
            Step::Observe => {
                let first = *self.first_quote.get_or_insert(quote.ts_init);
                let settle = u64::try_from(SETTLE.as_nanos())?;
                if quote.ts_init.as_u64().saturating_sub(first.as_u64()) < settle {
                    return Ok(());
                }
                self.observe()
            }
            Step::Flatten => self.flatten(quote.ts_init),
        }
    }
}
