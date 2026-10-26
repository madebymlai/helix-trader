//! The live node's cache in Redis across restarts, end to end on the IB
//! paper account: what comes back from the cache, and what Nautilus'
//! reconciliation makes of a cache that disagrees with IB. It places
//! orders, so it runs only with both `NAUTILUS_IB_RUN=1` and
//! `NAUTILUS_IB_LIVE_ORDERS=1` set in the shell, on a paper account
//! (`NAUTILUS_IB_ACCOUNT_ID`, `D...`), with Docker for the gateway and
//! Redis, EXS1.IBIS flat and Xetra open:
//!
//! ```sh
//! NAUTILUS_IB_RUN=1 NAUTILUS_IB_LIVE_ORDERS=1 cargo test --test live ib_restart:: -- --test-threads=1 --nocapture
//! ```
//!
//! Each node runs in a child process of this test binary (`phase`, one
//! node per process) under the test's own trader, so its records in Redis
//! are apart from `live`'s; the test deletes them at the end. The probe
//! leaves its `oms_type` unset: on IB it nets. After each test, IB itself
//! is read: EXS1 flat, no open order. Client ids 44 (nodes) and 45 (the
//! checks and the order sent behind the node's back).

use std::{path::PathBuf, process::Command, time::Duration};

use crate::common::cache::delete_trader;
use helix_trader::{data::IbAccount, live::Live, strategy::DataRequirements};
use ibapi::{Client, accounts::PositionUpdate, contracts::Contract, orders::Orders};
use jiff::SignedDuration;
use nautilus_common::{actor::DataActor, logging::logger::LoggerConfig};
use nautilus_model::{
    data::{BarType, QuoteTick},
    enums::{OrderSide, OrderStatus},
    events::OrderSubmitted,
    identifiers::{ClientOrderId, InstrumentId, StrategyId, TraderId},
    instruments::Instrument,
    orders::{Order, OrderAny},
    position::Position,
    types::{Price, Quantity},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

const CLIENT_ID: i32 = 44;
const INSTRUMENT: &str = "EXS1.IBIS";
const SYMBOL: &str = "EXS1";
const PHASE: &str = "HELIX_RESTART_PHASE";
const STATE: &str = "HELIX_RESTART_STATE";
const TRADER: &str = "HELIX_RESTART_TRADER";

/// What a probe does on its node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
enum Step {
    /// On a flat instrument, buys 1 share at market, then once it fills 1
    /// more: two overlapping trades, one netted position of 2. Writes the
    /// position and its orders to `STATE`.
    Open,
    /// Checks the position `Open` left came back as it was, from its own
    /// orders, alone; then closes it.
    Restart,
    /// On a flat instrument, rests a buy limit about 10% under the bid
    /// and, once the order is submitted to IB, stops the process as a crash
    /// would, before IB's answer is processed: the order is in flight in the
    /// cache. Its price is a whole euro, on Xetra's grid, if `on_grid`, so
    /// IB takes it; else a whole euro and 0.0001, IB's tick but not
    /// Xetra's, so IB rejects it (error 110). Writes the order to `STATE`.
    Crash { on_grid: bool },
    /// Records what the node starts with, after reconciliation, in
    /// `STATE`, and stops: no order.
    Observe,
    /// Cancels every order of the instrument and closes every position,
    /// then stops once flat.
    Flatten,
}

/// What a step leaves for the next, through `STATE`.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Saved {
    position_id: Option<String>,
    quantity: Option<String>,
    avg_px_open: Option<Decimal>,
    orders: Vec<String>,
    /// `Observe`: the open positions of the instrument, its open or
    /// in-flight orders, and the status of each of `orders`.
    open_positions: usize,
    live_orders: Vec<(String, OrderStatus)>,
    statuses: Vec<(String, Option<OrderStatus>)>,
}

/// One step, in a child process: a live node with the probe, stopped by
/// the probe. Runs only when the parent test names a step.
#[tokio::test(flavor = "multi_thread")]
async fn phase() {
    let Ok(step) = std::env::var(PHASE) else {
        return;
    };
    let step: Step = serde_json::from_str(&step).unwrap();
    dotenvy::dotenv().ok();
    let requirements = DataRequirements {
        bar_types: vec![BarType::from(
            format!("{INSTRUMENT}-1-MINUTE-MID-INTERNAL").as_str(),
        )],
        warmup: SignedDuration::from_mins(10),
    };
    // An empty catalog of its own: the venue answers the whole warmup.
    let catalog = tempfile::tempdir().unwrap();
    let live = Live {
        trader_id: TraderId::from(std::env::var(TRADER).unwrap().as_str()),
        account: Some(account()),
        client_id: CLIENT_ID,
        leverage: Decimal::ONE,
        logging: LoggerConfig::default(),
        catalog: catalog.path().to_path_buf(),
    };
    let probe = Probe::new(step, PathBuf::from(std::env::var(STATE).unwrap()));
    let ran = tokio::time::timeout(Duration::from_secs(300), live.run(&requirements, probe)).await;
    ran.expect("the node stops within 5 minutes").unwrap();
    let report: Report =
        serde_json::from_str(&std::fs::read_to_string(report_path()).unwrap()).unwrap();
    println!("PROBE {step:?}: {report:#?}");
    assert!(report.failures.is_empty(), "{:#?}", report.failures);
    assert!(report.done, "the probe did not finish");
}

/// A position held over a restart comes back from the cache in Redis as
/// it was, from the probe's own two orders, and no other position appears.
#[tokio::test(flavor = "multi_thread")]
async fn an_open_position_comes_back_from_the_cache_after_a_restart() {
    let Some(account) = opted_in() else { return };
    let run = Run::new("HELIXTEST-044");
    run.phase(Step::Open);
    run.phase(Step::Restart);
    assert_flat(&account).await;
}

/// A position closed behind the node's back, while it was down: the cache
/// still has it open, IB has none.
#[tokio::test(flavor = "multi_thread")]
async fn a_position_closed_while_the_node_was_down() {
    let Some(account) = opted_in() else { return };
    let run = Run::new("HELIXTEST-045");
    run.phase(Step::Open);
    sell_at_market(&account, 2.0).await;
    run.phase(Step::Observe);
    // Reconciliation takes IB's sell as an external order claimed by the
    // strategy, and its fill closes the cached position.
    let observed = run.saved();
    assert_eq!(observed.open_positions, 0, "{observed:?}");
    assert!(observed.live_orders.is_empty(), "{observed:?}");
    run.phase(Step::Flatten);
    assert_flat(&account).await;
}

/// An order in flight in the cache when the node died, working at IB.
#[tokio::test(flavor = "multi_thread")]
async fn an_order_in_flight_when_the_node_died_that_ib_took() {
    let Some(account) = opted_in() else { return };
    let run = Run::new("HELIXTEST-046");
    run.crash(true);
    run.phase(Step::Observe);
    // Startup reconciliation finds it in IB's open orders: Accepted.
    let observed = run.saved();
    assert_eq!(
        observed.statuses,
        [(observed.orders[0].clone(), Some(OrderStatus::Accepted))],
        "{observed:?}"
    );
    run.phase(Step::Flatten);
    assert_flat(&account).await;
}

/// An order in flight in the cache when the node died, which IB rejected
/// meanwhile: IB lists it nowhere, not even among its open orders.
#[tokio::test(flavor = "multi_thread")]
async fn an_order_in_flight_when_the_node_died_that_ib_rejected() {
    let Some(account) = opted_in() else { return };
    let run = Run::new("HELIXTEST-047");
    run.crash(false);
    run.phase(Step::Observe);
    // Nautilus leaves it Submitted: no report of IB's names it, the
    // in-flight check covers only the orders sent in the current run, and
    // the open-order check resolves an order missing at the venue only in
    // full-history mode (`open_check_open_only = false`), not in Nautilus'
    // default open-only mode (concepts/execution/reconciliation.md).
    let observed = run.saved();
    assert_eq!(
        observed.statuses,
        [(observed.orders[0].clone(), Some(OrderStatus::Submitted))],
        "{observed:?}"
    );
    assert_flat(&account).await;
}

/// IB's state in EXS1, read directly: a check to run by hand after a
/// failed run, with `NAUTILUS_IB_RUN=1` and `--ignored`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "run by hand with --ignored"]
async fn ib_state_in_exs1() {
    if std::env::var("NAUTILUS_IB_RUN").is_err() {
        return;
    }
    dotenvy::dotenv().ok();
    let client = Client::connect("127.0.0.1:4002", CLIENT_ID + 1)
        .await
        .unwrap();
    let (position, orders) = ib_state(&client, &account()).await;
    println!("IB in {SYMBOL}: position {position}, {orders} open orders");
}

/// Whether the test is opted in, from the shell, before `.env` is read;
/// then the paper account. An opted-in test claims the process for this
/// module (`claim_process`).
fn opted_in() -> Option<IbAccount> {
    let set = |name| std::env::var(name).is_ok_and(|value| value == "1");
    if !(set("NAUTILUS_IB_RUN") && set("NAUTILUS_IB_LIVE_ORDERS")) {
        println!("skipped; set NAUTILUS_IB_RUN=1 and NAUTILUS_IB_LIVE_ORDERS=1 to place orders");
        return None;
    }
    crate::claim_process(module_path!());
    dotenvy::dotenv().ok();
    Some(account())
}

fn account() -> IbAccount {
    let account: IbAccount = std::env::var("NAUTILUS_IB_ACCOUNT_ID")
        .expect("NAUTILUS_IB_ACCOUNT_ID, a paper account")
        .parse()
        .unwrap();
    assert!(
        account.to_string().starts_with('D'),
        "{account} is not a paper account"
    );
    account
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
            .args(["--exact", "ib_restart::phase", "--nocapture"])
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
    fn crash(&self, on_grid: bool) {
        let status = self.command(Step::Crash { on_grid }).status().unwrap();
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

/// Sells `quantity` EXS1 at market in `account`, behind the node's back,
/// and waits for IB to report the position flat.
async fn sell_at_market(account: &IbAccount, quantity: f64) {
    let client = Client::connect("127.0.0.1:4002", CLIENT_ID + 1)
        .await
        .unwrap();
    // Routed SMART with Xetra as primary exchange, as Nautilus' adapter
    // routes it.
    let contract = Contract::stock(SYMBOL)
        .on_exchange("SMART")
        .primary("IBIS")
        .in_currency("EUR")
        .build();
    let mut updates = client.order_update_stream().await.unwrap();
    let order_id = client
        .order(&contract)
        .sell(quantity)
        .market()
        .account(account.to_string())
        .submit()
        .await
        .unwrap();
    println!("sold {quantity} {SYMBOL} at market behind the node's back: IB order {order_id}");
    for update in updates.collect_for(Duration::from_secs(10)).await {
        println!("IB order update: {update:?}");
    }
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_secs(2)).await;
        if ib_state(&client, account).await.0 == 0.0 {
            return;
        }
    }
    panic!("{SYMBOL} not flat after the sell");
}

/// IB's position in EXS1 of `account` and its open EXS1 orders.
async fn ib_state(client: &Client, account: &IbAccount) -> (f64, usize) {
    let mut subscription = client.positions().await.unwrap();
    let updates = subscription
        .collect_until(Duration::from_secs(30), |updates| {
            matches!(updates.last(), Some(PositionUpdate::PositionEnd))
        })
        .await;
    assert!(
        matches!(updates.last(), Some(PositionUpdate::PositionEnd)),
        "no end of positions from IB: {updates:?}"
    );
    let position = updates
        .iter()
        .filter_map(|update| match update {
            PositionUpdate::Position(position)
                if position.account == account.to_string()
                    && position.contract.symbol == SYMBOL =>
            {
                Some(position.position)
            }
            _ => None,
        })
        .sum();
    let orders = client
        .all_open_orders()
        .await
        .unwrap()
        .collect_for(Duration::from_secs(10))
        .await;
    let open = orders
        .iter()
        .filter(|order| matches!(order, Orders::OrderData(data) if data.contract.symbol == SYMBOL))
        .count();
    (position, open)
}

/// Reads IB: EXS1 flat in `account`, no open EXS1 order.
async fn assert_flat(account: &IbAccount) {
    let client = Client::connect("127.0.0.1:4002", CLIENT_ID + 1)
        .await
        .unwrap();
    let (position, orders) = ib_state(&client, account).await;
    println!("IB in {SYMBOL}: position {position}, {orders} open orders");
    assert_eq!(position, 0.0, "{SYMBOL} left open on {account}");
    assert_eq!(orders, 0, "{SYMBOL} orders left open on {account}");
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
    instrument_id: InstrumentId,
    step: Step,
    state: PathBuf,
    entries: Vec<ClientOrderId>,
    acted: bool,
    report: Report,
}

impl Probe {
    fn new(step: Step, state: PathBuf) -> Self {
        Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from("RESTART_PROBE-001")),
                order_id_tag: Some("001".to_string()),
                ..Default::default()
            }),
            instrument_id: InstrumentId::from(INSTRUMENT),
            step,
            state,
            entries: vec![],
            acted: false,
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

    /// The open positions of the instrument, any strategy, oldest first.
    fn open_positions(&self) -> Vec<Position> {
        let cache = self.cache();
        let mut positions: Vec<Position> = cache
            .positions_open(None, Some(&self.instrument_id), None, None, None)
            .iter()
            .map(|position| (*position).clone())
            .collect();
        positions.sort_by_key(|position| position.ts_opened);
        positions
    }

    /// The open and in-flight orders of the instrument, any strategy.
    fn live_orders(&self) -> Vec<OrderAny> {
        let cache = self.cache();
        let id = Some(&self.instrument_id);
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

    fn status(&self, order: &ClientOrderId) -> Option<OrderStatus> {
        self.cache().order(order).map(|order| order.status())
    }

    fn note_state(&mut self, when: &str) {
        let positions = self.open_positions();
        let orders = self.live_orders();
        let net = self.portfolio().net_position(&self.instrument_id);
        self.note(format!(
            "{when}: net {net}, {} positions open, {} orders open or in flight",
            positions.len(),
            orders.len()
        ));
        for position in positions {
            self.note(format!(
                "  position {} strategy={} {:?} qty={} avg_px_open={} orders={:?}",
                position.id,
                position.strategy_id,
                position.side,
                position.quantity,
                position.avg_px_open,
                position.client_order_ids(),
            ));
        }
        for order in orders {
            self.note(format!(
                "  order {} strategy={} {:?} {:?} {} status={:?}",
                order.client_order_id(),
                order.strategy_id(),
                order.order_type(),
                order.order_side(),
                order.quantity(),
                order.status(),
            ));
        }
    }

    /// Whether the instrument is flat in the cache, no order live.
    fn is_flat(&self) -> bool {
        self.open_positions().is_empty()
            && self.live_orders().is_empty()
            && self.portfolio().net_position(&self.instrument_id) == Decimal::ZERO
    }

    fn buy_at_market(&mut self) -> anyhow::Result<()> {
        let order = self.order().market(
            self.instrument_id,
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
        self.entries.push(order.client_order_id());
        self.submit_order(order, None, None, None)
    }

    fn save(&self, saved: &Saved) -> anyhow::Result<()> {
        Ok(std::fs::write(&self.state, serde_json::to_string(saved)?)?)
    }

    fn finish(&mut self) -> anyhow::Result<()> {
        self.report.done = true;
        std::fs::write(report_path(), serde_json::to_string(&self.report)?)?;
        interrupt();
        Ok(())
    }

    /// `Open`: the first buy on a flat instrument, the second once the
    /// first filled, then the netted position saved.
    fn open(&mut self) -> anyhow::Result<()> {
        match self.entries.as_slice() {
            [] => {
                if !self.is_flat() {
                    self.note_state("not flat");
                    self.fail(format!("{INSTRUMENT} not flat at the start"));
                    return self.finish();
                }
                self.buy_at_market()
            }
            [first] if self.status(first) == Some(OrderStatus::Filled) => self.buy_at_market(),
            [_, second] if self.status(second) == Some(OrderStatus::Filled) => {
                let [position] = self
                    .open_positions()
                    .try_into()
                    .map_err(|positions| anyhow::anyhow!("one position expected: {positions:?}"))?;
                if position.quantity != Quantity::from(2) {
                    return Ok(());
                }
                self.note_state("two trades in one position");
                self.save(&Saved {
                    position_id: Some(position.id.to_string()),
                    quantity: Some(position.quantity.to_string()),
                    avg_px_open: Some(position.avg_px_open),
                    orders: self.entries.iter().map(ToString::to_string).collect(),
                    ..Saved::default()
                })?;
                self.finish()
            }
            _ => Ok(()),
        }
    }

    /// `Restart`: the check, once.
    fn check_restart(&mut self) -> anyhow::Result<()> {
        self.note_state("after the restart");
        let saved: Saved = serde_json::from_str(&std::fs::read_to_string(&self.state)?)?;
        self.note(format!("open left {saved:?}"));
        let positions = self.open_positions();
        let [position] = positions.as_slice() else {
            self.fail(format!("{} positions open, one expected", positions.len()));
            return Ok(());
        };
        if Some(position.id.to_string()) != saved.position_id
            || Some(position.quantity.to_string()) != saved.quantity
            || Some(position.avg_px_open) != saved.avg_px_open
        {
            self.fail(format!("{position:?} is not the position open left"));
        }
        let orders: Vec<String> = position
            .client_order_ids()
            .iter()
            .map(ToString::to_string)
            .collect();
        if orders != saved.orders {
            self.fail(format!(
                "the position's orders {orders:?}, not open's {:?}",
                saved.orders
            ));
        }
        Ok(())
    }

    /// `Crash`: the buy limit (see `on_order_submitted`).
    fn crash(&mut self, quote: &QuoteTick, on_grid: bool) -> anyhow::Result<()> {
        if !self.is_flat() {
            self.fail(format!("{INSTRUMENT} not flat at the start"));
            return self.finish();
        }
        let instrument = self
            .cache()
            .instrument(&self.instrument_id)
            .ok_or_else(|| anyhow::anyhow!("{INSTRUMENT} not in cache"))?;
        let price = (quote.bid_price.as_f64() * 0.9).floor() + if on_grid { 0.0 } else { 0.0001 };
        let price: Price = instrument.make_price(price);
        let order = self.order().limit(
            self.instrument_id,
            OrderSide::Buy,
            Quantity::from(1),
            price,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        let id = order.client_order_id();
        self.entries.push(id);
        self.save(&Saved {
            orders: vec![id.to_string()],
            ..Saved::default()
        })?;
        self.note(format!("submitting {id}, a buy limit at {price}"));
        self.submit_order(order, None, None, None)
    }

    /// `Observe`: records the instrument's open positions and
    /// live orders, and the status of the orders the last step left.
    fn observe(&mut self) -> anyhow::Result<()> {
        self.note_state("observed");
        let left: Saved = serde_json::from_str(&std::fs::read_to_string(&self.state)?)?;
        let statuses = left
            .orders
            .iter()
            .map(|id| (id.clone(), self.status(&ClientOrderId::from(id.as_str()))))
            .collect();
        self.save(&Saved {
            open_positions: self.open_positions().len(),
            live_orders: self
                .live_orders()
                .iter()
                .map(|order| (order.client_order_id().to_string(), order.status()))
                .collect(),
            statuses,
            orders: left.orders,
            ..Saved::default()
        })?;
        self.finish()
    }

    /// `Flatten`: cancels the orders and closes the positions once, then
    /// stops once flat.
    fn flatten(&mut self) -> anyhow::Result<()> {
        if self.is_flat() {
            self.note_state("flat");
            return self.finish();
        }
        if self.acted {
            return Ok(());
        }
        self.acted = true;
        self.note_state("flattening");
        for order in self.live_orders() {
            self.cancel_order(order.client_order_id(), None, None)?;
        }
        for position in self.open_positions() {
            // IB denies reduce-only orders.
            self.close_position(&position, None, None, None, Some(false), None, None)?;
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

nautilus_strategy!(Probe, {
    /// In `Crash`, aborts once the order is submitted, after long enough
    /// for Redis to receive it (its writer runs on another thread) but
    /// with the node thread held, so IB's acknowledgement is not processed.
    fn on_order_submitted(&mut self, event: OrderSubmitted) {
        if matches!(self.step, Step::Crash { .. }) {
            std::thread::sleep(Duration::from_millis(500));
            let status = self.status(&event.client_order_id);
            println!("PROBE aborting with {} {status:?}", event.client_order_id);
            std::process::abort();
        }
    }
});

impl DataActor for Probe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.note_state("start");
        // Market orders need a quote: the risk engine denies them without.
        self.subscribe_quotes(self.instrument_id, None, None);
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        if self.report.done {
            return Ok(());
        }
        match self.step {
            Step::Open => self.open(),
            Step::Restart if !self.acted => {
                self.check_restart()?;
                self.acted = false;
                self.step = Step::Flatten;
                self.flatten()
            }
            Step::Restart => Ok(()),
            Step::Crash { on_grid } if self.entries.is_empty() => self.crash(quote, on_grid),
            Step::Crash { .. } => Ok(()),
            Step::Observe => self.observe(),
            Step::Flatten => self.flatten(),
        }
    }
}
