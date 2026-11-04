//! End to end on IC Markets' cTrader demo, a trailing stop: `Live` on
//! XAU/USD and EUR/USD with IC's clients, a strategy sending, once it has a
//! quote of each, one bracket of gold's minimum, 1 oz, from Nautilus'
//! bracket factory: a market buy, its stop loss a trailing stop market 0.50
//! behind (`sl_order_type`, from the bid less 0.50), its take profit 30.00
//! above the ask. It records every order event and gold's quotes, and the
//! node stops once the trailing stop loss has closed the position, or after
//! ten minutes; then IC's execution client reads the account: no position,
//! no order open. It prints each move of the stop with the bid that made
//! it, the measure of cTrader's trailing rule.
//!
//! It needs what `ic_orders_demo.rs` needs; it runs on the demo only
//! (`IC_ENVIRONMENT=demo`, any other fails it before connecting). Gated as
//! `ic_demo.rs` is: the test returns at once, saying it was skipped, unless
//! `NAUTILUS_IC_RUN=1` is set in the shell. One node per process, hence its
//! own module, run alone (`main.rs`). The node keeps its cache in Redis
//! under the test's trader, `HELIXTEST-103`, whose records the test
//! deletes. Run it with:
//!
//! ```sh
//! NAUTILUS_IC_RUN=1 cargo test --test live ic_trailing_demo:: -- --nocapture
//! ```

use crate::common;

use std::{
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use helix_trader::{
    live::Live,
    strategy::{DataRequirements, warmup::Warmup},
};
use jiff::SignedDuration;
use nautilus_common::{actor::DataActor, logging::logger::LoggerConfig};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{BarType, QuoteTick},
    enums::{OrderSide, OrderType, PositionSide, TrailingOffsetType},
    events::{
        OrderAccepted, OrderCanceled, OrderDenied, OrderFilled, OrderRejected, OrderUpdated,
        PositionClosed, PositionOpened,
    },
    identifiers::{ClientOrderId, InstrumentId, StrategyId, TraderId},
    orders::Order,
    types::{Price, Quantity},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{Strategy, StrategyConfig, StrategyCore},
};
use rust_decimal::Decimal;

/// Whether the run is opted in: from the shell, before `.env` is read, so
/// that the file cannot opt in.
/// An opted-in run claims the process for this module (`claim_process`).
fn opted_in(test: &str) -> bool {
    let run = std::env::var("NAUTILUS_IC_RUN").is_ok_and(|value| value == "1");
    if run {
        crate::claim_process(module_path!());
    } else {
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
const TRADER: &str = "HELIXTEST-103";

/// How far behind the stop trails: 0.50, by price.
const OFFSET: Decimal = Decimal::from_parts(50, 0, 0, false, 2);

/// What the strategy saw, in order: each order event as `(client order id,
/// what)`, the fills, the stop loss's updates and gold's quotes.
#[derive(Debug, Default)]
struct Record {
    events: Vec<(String, String)>,
    fills: Vec<OrderFilled>,
    stop_loss: Option<ClientOrderId>,
    updates: Vec<OrderUpdated>,
    quotes: Vec<QuoteTick>,
    opened: Option<PositionOpened>,
    closed: Option<PositionClosed>,
}

/// Sends one bracket with a trailing stop loss on gold's first quote after
/// a EUR/USD quote, and lets the stop close the position.
#[derive(Debug)]
struct TrailOnce {
    core: StrategyCore,
    record: Arc<Mutex<Record>>,
    done: Arc<AtomicBool>,
    rate_known: bool,
    sent: bool,
}

impl TrailOnce {
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

nautilus_strategy!(TrailOnce, {
    fn on_order_accepted(&mut self, event: OrderAccepted) {
        self.note(
            event.client_order_id,
            format!("accepted {}", event.venue_order_id),
        );
    }

    fn on_order_updated(&mut self, event: OrderUpdated) {
        self.note(
            event.client_order_id,
            format!(
                "updated: trigger {:?} at {}",
                event.trigger_price, event.ts_event
            ),
        );
        self.record.lock().unwrap().updates.push(event);
    }

    fn on_order_filled(&mut self, event: &OrderFilled) {
        self.note(
            event.client_order_id,
            format!(
                "filled {} {} {} at {}, position {:?}",
                event.order_type,
                event.order_side,
                event.last_qty,
                event.last_px,
                event.position_id
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
        self.record.lock().unwrap().opened = Some(event);
    }

    fn on_position_closed(&mut self, event: PositionClosed) {
        self.record.lock().unwrap().closed = Some(event);
        self.done.store(true, Ordering::SeqCst);
    }
});

impl DataActor for TrailOnce {
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
        if self.sent {
            self.record.lock().unwrap().quotes.push(*quote);
            return Ok(());
        }
        if !self.rate_known {
            return Ok(());
        }
        self.sent = true;
        let offset: f64 = OFFSET.try_into().unwrap();
        let orders = self
            .order()
            .bracket()
            .instrument_id(quote.instrument_id)
            .order_side(OrderSide::Buy)
            .quantity(Quantity::from(1))
            .sl_order_type(OrderType::TrailingStopMarket)
            .sl_trigger_price(Price::new(quote.bid_price.as_f64() - offset, 2))
            .sl_trailing_offset(OFFSET)
            .sl_trailing_offset_type(TrailingOffsetType::Price)
            .tp_price(Price::new(quote.ask_price.as_f64() + 30.0, 2))
            .call();
        let stop_loss = orders
            .iter()
            .find(|o| o.order_type() == OrderType::TrailingStopMarket)
            .map(|o| o.client_order_id());
        {
            let mut record = self.record.lock().unwrap();
            record.stop_loss = stop_loss;
            record.quotes.push(*quote);
        }
        self.submit_order_list(orders, None, None, None)
    }

    /// Leaves nothing open, whatever happened.
    fn on_stop(&mut self) -> anyhow::Result<()> {
        let id = InstrumentId::from(XAUUSD);
        self.cancel_all_orders(id, None, None, true, None)?;
        self.close_all_positions(id, None, None, None, None, None, None, None)
    }
}

/// How long the trailing stop gets, from the node's start, to close the
/// position.
const DEADLINE: Duration = Duration::from_secs(600);

/// The last bid of `quotes` at or before `ts`, cTrader's time of a move.
fn bid_at(quotes: &[QuoteTick], ts: UnixNanos) -> Option<Price> {
    quotes
        .iter()
        .take_while(|q| q.ts_event <= ts)
        .last()
        .map(|q| q.bid_price)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_trailing_stop_loss_follows_the_bid_and_closes_the_position_on_the_demo() {
    if !opted_in("a_trailing_stop_loss_follows_the_bid_and_closes_the_position_on_the_demo") {
        return;
    }
    demo_only();
    let bar_types =
        [XAUUSD, EURUSD].map(|id| BarType::from(format!("{id}-1-MINUTE-MID-INTERNAL").as_str()));
    let requirements = DataRequirements {
        bar_types: bar_types.to_vec(),
        ticks: vec![],
        warmup: SignedDuration::from_mins(5),
    };
    let record = Arc::new(Mutex::new(Record::default()));
    let done = Arc::new(AtomicBool::new(false));
    let strategy = TrailOnce {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("TRAIL-001")),
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
    let (positions, orders) = crate::ic_orders_demo::open_at_ic().await;
    ran.expect("the node stops within 90 s of SIGINT").unwrap();
    println!("IC after the run: {positions} open positions, {orders} open orders");
    assert_eq!((positions, orders), (0, 0), "the demo account is not flat");

    let record = record.lock().unwrap();
    let stop_loss = record.stop_loss.expect("no quote, no bracket");
    let opened = record.opened.as_ref().expect("no position opened");
    let closed = record.closed.as_ref().expect("the position did not close");
    assert_eq!(opened.side, PositionSide::Long);
    let venue = format!("accepted {}-SL", opened.position_id);
    assert!(
        record
            .events
            .iter()
            .any(|(id, what)| id == stop_loss.as_str() && *what == venue),
        "{:?}",
        record.events
    );
    let [entry, stop] = record.fills.as_slice() else {
        panic!("not two fills: {:?}", record.fills);
    };
    assert_eq!(entry.order_side, OrderSide::Buy);
    assert_eq!(stop.client_order_id, stop_loss, "closed by another order");
    assert_eq!(stop.order_type, OrderType::TrailingStopMarket);
    assert_eq!(stop.position_id, Some(opened.position_id));
    assert_eq!(closed.position_id, opened.position_id);

    let moves: Vec<&OrderUpdated> = record
        .updates
        .iter()
        .filter(|u| u.client_order_id == stop_loss)
        .collect();
    println!(
        "entry filled at {}; the stop set at {:?}, offset {OFFSET}; hit at {}",
        entry.last_px,
        moves.first().and_then(|u| u.trigger_price),
        stop.last_px
    );
    for update in &moves {
        let trigger = update.trigger_price.unwrap();
        let bid = bid_at(&record.quotes, update.ts_event);
        println!(
            "stop {trigger} at {}: bid then {bid:?}, bid less stop {:?}",
            update.ts_event,
            bid.map(|b| b.as_decimal() - trigger.as_decimal())
        );
    }
    let triggers: Vec<Price> = moves.iter().filter_map(|u| u.trigger_price).collect();
    assert!(
        triggers.len() >= 2,
        "the stop never moved after it was set: {triggers:?}"
    );
    assert!(
        triggers.windows(2).all(|w| w[0] <= w[1]),
        "the stop loosened: {triggers:?}"
    );
}
