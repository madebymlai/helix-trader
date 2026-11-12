//! End to end on a cTrader broker's demo, with orders: `Live` on XAU/USD
//! and EUR/USD with the broker's clients, a strategy sending, once it has a quote
//! of each, one bracket of 3 oz of gold (`GOLD_OUNCES`)
//! (a market buy, its stop loss 30.00 below the bid, its take profit 30.00
//! above the ask), closing the position it opens at once by its id
//! (`close_position`), and recording every order event. The node stops
//! once the position is closed; then the broker's execution client reads
//! the account: no position, no order open.
//!
//! EUR/USD's quotes are there for Nautilus' risk engine: on an account in
//! EUR, as IC's demo is, it converts an order's USD margin at the cache's
//! EUR/USD, and denies the order without one (`VALIDATION_FAILED: Account
//! currency conversion failed: No exchange rate from USD to EUR`, demo,
//! 2026-10-26). An account in USD does not need them.
//!
//! It needs what `ctrader_live_demo.rs` needs, and gold's market open.
//! Gated as `ctrader_demo.rs` is. One node per process, hence its own
//! module, run alone (`main.rs`). The node keeps its cache in Redis under
//! the test's trader, `HELIXTEST-102`, whose records the test deletes. Run
//! it with:
//!
//! ```sh
//! NAUTILUS_CTRADER_RUN=pepperstone cargo test --test live ctrader_orders_demo:: -- --nocapture
//! ```

use crate::common;

use std::{
    cell::RefCell,
    process::Command,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

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
use nautilus_model::{
    accounts::AccountAny,
    data::{BarType, QuoteTick},
    enums::{OrderSide, PositionSide},
    events::{
        OrderAccepted, OrderCanceled, OrderDenied, OrderFilled, OrderRejected, PositionClosed,
        PositionOpened,
    },
    identifiers::{InstrumentId, StrategyId, TraderId},
    types::{Price, Quantity},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};
use rust_decimal::Decimal;

use crate::demo_broker::{DemoBroker, GOLD_OUNCES, opted_in};

const TRADER: &str = "HELIXTEST-102";

/// What the strategy saw, in order: each order event as `(client order id,
/// what)`, and the fills.
#[derive(Debug, Default)]
struct Record {
    events: Vec<(String, String)>,
    fills: Vec<OrderFilled>,
    bracket: Option<(Price, Price)>,
    opened: Option<PositionOpened>,
    closed: Option<PositionClosed>,
}

/// Sends one bracket on gold's first quote after a EUR/USD quote, and
/// closes the position it opens.
#[derive(Debug)]
struct BracketOnce {
    core: StrategyCore,
    gold: InstrumentId,
    eurusd: InstrumentId,
    record: Arc<Mutex<Record>>,
    done: Arc<AtomicBool>,
    rate_known: bool,
    sent: bool,
}

impl BracketOnce {
    fn note(&self, id: impl ToString, what: impl Into<String>) {
        let what = what.into();
        println!("order event: {} {what}", id.to_string());
        self.record
            .lock()
            .unwrap()
            .events
            .push((id.to_string(), what));
    }
}

nautilus_strategy!(BracketOnce, {
    fn on_order_accepted(&mut self, event: OrderAccepted) {
        self.note(
            event.client_order_id,
            format!("accepted {}", event.venue_order_id),
        );
    }

    fn on_order_filled(&mut self, event: &OrderFilled) {
        self.note(
            event.client_order_id,
            format!(
                "filled {} {} at {}, position {:?}, commission {:?}",
                event.order_side,
                event.last_qty,
                event.last_px,
                event.position_id,
                event.commission
            ),
        );
        self.record.lock().unwrap().fills.push(event.clone());
    }

    fn on_order_canceled(&mut self, event: &OrderCanceled) {
        self.note(event.client_order_id, "canceled");
    }

    fn on_order_rejected(&mut self, event: OrderRejected) {
        self.note(event.client_order_id, format!("rejected: {}", event.reason));
        self.done.store(true, Ordering::SeqCst);
    }

    fn on_order_denied(&mut self, event: OrderDenied) {
        self.note(event.client_order_id, format!("denied: {}", event.reason));
        self.done.store(true, Ordering::SeqCst);
    }

    fn on_position_opened(&mut self, event: PositionOpened) {
        let position_id = event.position_id;
        self.record.lock().unwrap().opened = Some(event);
        let position = self.cache().position(&position_id);
        let Some(position) = position else {
            self.note(position_id, "opened but not in the cache");
            return;
        };
        if let Err(e) = self.close_position(&position, None, None, None, None, None, None) {
            self.note(position_id, format!("close failed: {e}"));
            self.done.store(true, Ordering::SeqCst);
        }
    }

    fn on_position_closed(&mut self, event: PositionClosed) {
        self.record.lock().unwrap().closed = Some(event);
        self.done.store(true, Ordering::SeqCst);
    }
});

impl DataActor for BracketOnce {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.subscribe_quotes(self.eurusd, None, None);
        self.subscribe_quotes(self.gold, None, None);
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        if quote.instrument_id == self.eurusd {
            self.rate_known = true;
            return Ok(());
        }
        if self.sent || !self.rate_known {
            return Ok(());
        }
        self.sent = true;
        let stop_loss = Price::new(quote.bid_price.as_f64() - 30.0, 2);
        let take_profit = Price::new(quote.ask_price.as_f64() + 30.0, 2);
        self.record.lock().unwrap().bracket = Some((stop_loss, take_profit));
        let orders = self
            .order()
            .bracket()
            .instrument_id(quote.instrument_id)
            .order_side(OrderSide::Buy)
            .quantity(Quantity::from(GOLD_OUNCES))
            .sl_trigger_price(stop_loss)
            .tp_price(take_profit)
            .call();
        self.submit_order_list(orders, None, None, None)
    }

    /// Leaves nothing open, whatever happened.
    fn on_stop(&mut self) -> anyhow::Result<()> {
        let id = self.gold;
        self.cancel_all_orders(id, None, None, true, None)?;
        self.close_all_positions(id, None, None, None, None, None, None, None)
    }
}

/// How long the bracket gets, from the node's start, to open and close.
const DEADLINE: Duration = Duration::from_secs(180);

#[tokio::test(flavor = "multi_thread")]
async fn a_minimal_bracket_fills_closes_at_once_and_leaves_the_demo_account_flat() {
    let Some(broker) = opted_in(
        "a_minimal_bracket_fills_closes_at_once_and_leaves_the_demo_account_flat",
        module_path!(),
    ) else {
        return;
    };
    let bar_types = [broker.gold(), broker.eurusd()]
        .map(|id| BarType::from(format!("{id}-1-MINUTE-MID-INTERNAL").as_str()));
    let requirements = DataRequirements {
        bar_types: bar_types.to_vec(),
        ticks: vec![],
        warmup: SignedDuration::from_mins(5),
    };
    let record = Arc::new(Mutex::new(Record::default()));
    let done = Arc::new(AtomicBool::new(false));
    let strategy = BracketOnce {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("BRACKET-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
        gold: broker.gold(),
        eurusd: broker.eurusd(),
        record: Arc::clone(&record),
        done: Arc::clone(&done),
        rate_known: false,
        sent: false,
    };
    // An empty catalog of its own: the venue answers the whole warmup.
    let catalog = tempfile::tempdir().unwrap();
    // Outside the catalog, as `Live` requires.
    let staging = tempfile::tempdir().unwrap();
    let live = Live {
        trader_id: TraderId::from(TRADER),
        venues: crate::venues(&[]),
        leverage: Decimal::ONE,
        logging: LoggerConfig::default(),
        catalog: catalog.path().to_path_buf(),
        recording: crate::recording(staging.path()),
    };
    // As Ctrl-C would, once the position closed or the deadline passed.
    let finished = Arc::clone(&done);
    let interrupt = tokio::spawn(async move {
        let deadline = tokio::time::Instant::now() + DEADLINE;
        while !finished.load(Ordering::SeqCst) && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        // The last events out first.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let status = Command::new("kill")
            .args(["-INT", &std::process::id().to_string()])
            .status()
            .unwrap();
        assert!(status.success());
    });

    let ran = tokio::time::timeout(
        DEADLINE + Duration::from_secs(90),
        live.run(&requirements, &Warmup::live(&requirements), strategy, None),
    )
    .await;

    interrupt.abort();
    common::cache::delete_trader(TRADER).await;
    let (positions, orders) = open_at_broker(broker).await;
    ran.expect("the node stops within 90 s of SIGINT").unwrap();
    println!("after the run: {positions} open positions, {orders} open orders");
    assert_eq!((positions, orders), (0, 0), "the demo account is not flat");

    let record = record.lock().unwrap();
    let (stop_loss, take_profit) = record.bracket.expect("no quote, no bracket");
    let opened = record.opened.as_ref().expect("no position opened");
    let closed = record.closed.as_ref().expect("the position did not close");
    println!(
        "bracket: stop loss {stop_loss}, take profit {take_profit}; position {} opened at {}, \
         closed at {}, realized {:?}",
        opened.position_id,
        opened.avg_px_open,
        closed.avg_px_close.unwrap_or_default(),
        closed.realized_pnl
    );
    assert_eq!(opened.side, PositionSide::Long);
    assert_eq!(opened.quantity, Quantity::from(GOLD_OUNCES));
    assert_eq!(closed.position_id, opened.position_id);
    // The cTrader position id is the Nautilus position id.
    assert!(
        opened.position_id.as_str().parse::<i64>().is_ok(),
        "{}",
        opened.position_id
    );
    let accepted = |suffix: &str| {
        record
            .events
            .iter()
            .any(|(_, what)| what == &format!("accepted {}-{suffix}", opened.position_id))
    };
    assert!(accepted("SL"), "{:?}", record.events);
    assert!(accepted("TP"), "{:?}", record.events);
    let canceled = record
        .events
        .iter()
        .filter(|(_, what)| what == "canceled")
        .count();
    assert_eq!(
        canceled, 2,
        "the legs cancelled with the position: {:?}",
        record.events
    );
    let [entry, close] = record.fills.as_slice() else {
        panic!("not two fills: {:?}", record.fills);
    };
    for fill in [entry, close] {
        assert_eq!(fill.position_id, Some(opened.position_id));
        assert_eq!(fill.last_qty, Quantity::from(GOLD_OUNCES));
        let commission = fill.commission.expect("no commission");
        assert_eq!(
            commission.currency.code.as_str(),
            broker.commission_currency()
        );
        let expected = broker.gold_commission(fill.last_px.as_f64());
        println!(
            "{} fill at {}: commission {commission}, expected {expected:?}",
            fill.order_side, fill.last_px
        );
        assert!(
            expected.contains(&commission.as_f64()),
            "{commission} outside {expected:?}"
        );
    }
    assert_eq!(entry.order_side, OrderSide::Buy);
    assert_eq!(close.order_side, OrderSide::Sell);
}

/// The open positions and orders the execution client of `broker` reports
/// on the demo account, as the node's startup reconciliation reads them:
/// the mass status also holds the orders closed over its window.
async fn open_at_broker(broker: &DemoBroker) -> (usize, usize) {
    let (tx, mut events) = tokio::sync::mpsc::unbounded_channel();
    replace_exec_event_sender(tx);
    let cache = Rc::new(RefCell::new(Cache::default()));
    let config = CTraderExecutionClientConfig::new(broker.connection(), vec![broker.gold()]);
    let factory = CTraderExecutionClientFactory::new(&config);
    let mut client = factory
        .create(
            TraderId::from(TRADER),
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
