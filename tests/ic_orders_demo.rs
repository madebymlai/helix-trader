//! End to end on IC Markets' cTrader demo, with orders: `Live` on XAU/USD
//! and EUR/USD with IC's clients, a strategy sending, once it has a quote
//! of each, one bracket of gold's minimum, 1 oz
//! (a market buy, its stop loss 30.00 below the bid, its take profit 30.00
//! above the ask), closing the position it opens at once by its id
//! (`close_position`), and recording every order event. The node stops
//! once the position is closed; then IC's execution client reads the
//! account: no position, no order open.
//!
//! EUR/USD's quotes are there for Nautilus' risk engine: on the account in
//! EUR it converts an order's USD margin at the cache's EUR/USD, and
//! denies the order without one (`VALIDATION_FAILED: Account currency
//! conversion failed: No exchange rate from USD to EUR`, demo,
//! 2026-10-26).
//!
//! It needs what `ic_live_demo.rs` needs, and gold's market open; it runs on
//! the demo only (`IC_ENVIRONMENT=demo`, any other fails it before
//! connecting). Gated as `ic_demo.rs` is: the test returns at once, saying
//! it was skipped, unless `NAUTILUS_IC_RUN=1` is set in the shell. One
//! node per process, hence its own file. The node keeps its cache in Redis
//! under the test's trader, `HELIXTEST-102`, whose records the test deletes.
//! Run it with:
//!
//! ```sh
//! NAUTILUS_IC_RUN=1 cargo test --test ic_orders_demo -- --nocapture
//! ```

mod common;

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

use helix_trader::{
    data::IcConnection,
    live::{IcExecutionClientConfig, IcExecutionClientFactory, Live},
    strategy::DataRequirements,
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

/// Whether the run is opted in: from the shell, before `.env` is read, so
/// that the file cannot opt in.
fn opted_in(test: &str) -> bool {
    let run = std::env::var("NAUTILUS_IC_RUN").is_ok_and(|value| value == "1");
    if !run {
        println!("{test}: skipped; set NAUTILUS_IC_RUN=1 to run it on IC's cTrader demo");
    }
    run
}

/// Reads `.env` and refuses any environment but the demo.
fn demo_only() {
    dotenvy::dotenv().ok();
    assert_eq!(
        std::env::var("IC_ENVIRONMENT").as_deref(),
        Ok("demo"),
        "the IC end to end test runs on the demo only: set IC_ENVIRONMENT=demo"
    );
}

const XAUUSD: &str = "XAUUSD.ICMARKETS";
const EURUSD: &str = "EURUSD.ICMARKETS";
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
        self.subscribe_quotes(InstrumentId::from(EURUSD), None, None);
        self.subscribe_quotes(InstrumentId::from(XAUUSD), None, None);
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        if quote.instrument_id == InstrumentId::from(EURUSD) {
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
            .quantity(Quantity::from(1))
            .sl_trigger_price(stop_loss)
            .tp_price(take_profit)
            .call();
        self.submit_order_list(orders, None, None, None)
    }

    /// Leaves nothing open, whatever happened.
    fn on_stop(&mut self) -> anyhow::Result<()> {
        let id = InstrumentId::from(XAUUSD);
        self.cancel_all_orders(id, None, None, true, None)?;
        self.close_all_positions(id, None, None, None, None, None, None, None)
    }
}

/// How long the bracket gets, from the node's start, to open and close.
const DEADLINE: Duration = Duration::from_secs(180);

#[tokio::test(flavor = "multi_thread")]
async fn a_minimal_bracket_fills_closes_at_once_and_leaves_the_demo_account_flat() {
    if !opted_in("a_minimal_bracket_fills_closes_at_once_and_leaves_the_demo_account_flat") {
        return;
    }
    demo_only();
    let bar_types =
        [XAUUSD, EURUSD].map(|id| BarType::from(format!("{id}-1-MINUTE-MID-INTERNAL").as_str()));
    let requirements = DataRequirements {
        bar_types: bar_types.to_vec(),
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
        record: Arc::clone(&record),
        done: Arc::clone(&done),
        rate_known: false,
        sent: false,
    };
    // An empty catalog of its own: the venue answers the whole warmup.
    let catalog = tempfile::tempdir().unwrap();
    let live = Live {
        trader_id: TraderId::from(TRADER),
        account: None,
        client_id: 1,
        leverage: Decimal::ONE,
        logging: LoggerConfig::default(),
        catalog: catalog.path().to_path_buf(),
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
        live.run(&requirements, strategy),
    )
    .await;

    interrupt.abort();
    common::cache::delete_trader(TRADER).await;
    let (positions, orders) = open_at_ic().await;
    ran.expect("the node stops within 90 s of SIGINT").unwrap();
    println!("IC after the run: {positions} open positions, {orders} open orders");
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
    assert_eq!(opened.quantity, Quantity::from(1));
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
        assert_eq!(fill.last_qty, Quantity::from(1));
        // IC's Raw charge: 30 USD per million per side, in EUR on this
        // account: 1 oz of gold, about 4,000 USD, is charged 0.10 to 0.12 EUR.
        let commission = fill.commission.expect("no commission");
        assert_eq!(commission.currency.code.as_str(), "EUR");
        let notional_usd = fill.last_px.as_f64();
        let usd = notional_usd * 30.0 / 1_000_000.0;
        println!(
            "{} fill at {}: commission {commission}, 30 per million is {usd:.4} USD",
            fill.order_side, fill.last_px
        );
        assert!(
            (0.75 * usd..=usd).contains(&commission.as_f64()),
            "{commission} for {usd} USD"
        );
    }
    assert_eq!(entry.order_side, OrderSide::Buy);
    assert_eq!(close.order_side, OrderSide::Sell);
}

/// The open positions and orders IC's execution client reports on the
/// demo account, as the node's startup reconciliation reads them.
async fn open_at_ic() -> (usize, usize) {
    let (tx, mut events) = tokio::sync::mpsc::unbounded_channel();
    replace_exec_event_sender(tx);
    let cache = Rc::new(RefCell::new(Cache::default()));
    let config = IcExecutionClientConfig {
        connection: IcConnection::from_env().unwrap(),
        instrument_ids: vec![InstrumentId::from(XAUUSD)],
    };
    let mut client = IcExecutionClientFactory
        .create(
            TraderId::from(TRADER),
            "ICMARKETS",
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
        mass_status.position_reports().len(),
        mass_status.order_reports().len(),
    )
}
