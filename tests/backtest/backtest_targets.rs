//! Targets from orders (`model::targets`): a probe decides every hour on
//! one feature, the last mid or close, and opens at each decision a
//! triple-barrier bracket and a horizon trade on each side, on a hedging
//! venue, through `Backtest::run_dataset`; it records the target rows
//! published for it. On quotes and on minute bars, with barriers and
//! paths chosen so that each target is worked out by hand: a long ended by
//! its stop and a short by its take profit, a barrier hit at the very
//! nanosecond of the time limit (inside the window), barriers left alone
//! (closed by time), a bar touching both barriers (the venue replays its
//! high before its low), the last decision unsettled at the end of data,
//! barriers in basis points and exits in average true ranges of hourly
//! bars, the cadence's, which the targets subscribe to themselves, no
//! order before the ATR is initialized. Orders the risk engine denies
//! drop their decisions, unsettled, before any time alert could. Over two
//! days of a random walk the targets are the same with the execution
//! engine purging closed orders and positions as without, while the purge
//! keeps the cache to the last minutes' orders. One node per thread, so
//! one run per test, or per thread of a test.

use std::{cell::RefCell, collections::BTreeSet, path::PathBuf, rc::Rc, thread};

use crate::common::ic;
use helix_trader::{
    backtest::{Backtest, commission_rate},
    model::{
        decide::{self, DecisionRule, FeatureRow},
        features::FeatureSet,
        targets::{
            AtrConfig, Barrier, Distance, Exit, Outcome, Outcomes, TargetRow, Targets,
            TargetsConfig,
        },
    },
    strategy::TickSeries,
};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::DataActor,
    logging::logger::LoggerConfig,
    msgbus::{self, ShareableMessageHandler, switchboard::get_custom_topic},
    timer::TimeEvent,
};
use nautilus_core::UnixNanos;
use nautilus_indicators::average::sma::SimpleMovingAverage;
use nautilus_model::{
    data::{Bar, BarType, CustomData, NautilusDataType, QuoteTick},
    enums::{OmsType, PriceType},
    events::{OrderDenied, OrderFilled, OrderRejected, PositionClosed},
    identifiers::{InstrumentId, StrategyId, Symbol},
    instruments::{CryptoPerpetual, InstrumentAny},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rust_decimal::Decimal;

const STRATEGY: &str = "TARGETS-001";

/// What the probe decides on: the mid of the last quote of an instrument,
/// or the close of the last bar of a bar type.
#[derive(Debug, Clone, Copy)]
enum Source {
    Quotes(InstrumentId),
    Bars(BarType),
}

/// What the probe recorded.
#[derive(Debug, Default)]
struct Record {
    /// The target rows published for it, in order.
    rows: Vec<TargetRow>,
    /// The closed orders in the cache when it stopped.
    closed_orders: usize,
    /// The decisions it opened the targets of.
    decisions: usize,
    /// The decisions still open when it stopped.
    open_decisions: usize,
    /// When each of its orders filled, in order.
    fills: Vec<UnixNanos>,
}

/// Decides every hour on its one feature and opens the targets of each
/// decision; records the rows published for it.
#[derive(Debug)]
struct TargetProbe {
    core: StrategyCore,
    source: Source,
    every: SignedDuration,
    features: Rc<FeatureSet>,
    targets: Rc<Targets>,
    record: Rc<RefCell<Record>>,
}

impl TargetProbe {
    /// The probe deciding every hour on `source`, opening `targets`.
    fn new(source: Source, targets: TargetsConfig) -> (Self, Rc<RefCell<Record>>) {
        Self::every(source, targets, SignedDuration::from_hours(1))
    }

    /// The probe deciding every `every` on `source`, opening `targets`,
    /// whose ATR, if any, is on bars of `every`.
    fn every(
        source: Source,
        targets: TargetsConfig,
        every: SignedDuration,
    ) -> (Self, Rc<RefCell<Record>>) {
        let features = match source {
            Source::Quotes(id) => FeatureSet::builder().quotes(
                id,
                "mid",
                SimpleMovingAverage::new(1, Some(PriceType::Mid)),
            ),
            Source::Bars(bar_type) => {
                FeatureSet::builder().bars(bar_type, "close", SimpleMovingAverage::new(1, None))
            }
        }
        .build()
        .unwrap();
        let record = Rc::new(RefCell::new(Record::default()));
        let probe = Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from(STRATEGY)),
                order_id_tag: Some("001".to_string()),
                ..Default::default()
            }),
            source,
            every,
            features,
            targets: Targets::new(targets, DecisionRule::Every(every)).unwrap(),
            record: Rc::clone(&record),
        };
        (probe, record)
    }
}

nautilus_strategy!(TargetProbe, {
    fn on_order_filled(&mut self, event: &OrderFilled) {
        self.record.borrow_mut().fills.push(event.ts_event);
        self.targets.on_order_filled(event);
    }

    fn on_order_denied(&mut self, event: OrderDenied) {
        self.targets.on_order_denied(&event);
    }

    fn on_order_rejected(&mut self, event: OrderRejected) {
        self.targets.on_order_rejected(&event);
    }

    fn on_position_closed(&mut self, event: PositionClosed) {
        Rc::clone(&self.targets).on_position_closed(self, &event);
    }
});

impl DataActor for TargetProbe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        let features = Rc::clone(&self.features);
        features.register(self);
        Rc::clone(&self.targets).start(self);
        let record = Rc::clone(&self.record);
        let handler = ShareableMessageHandler::from_typed(move |data: &CustomData| {
            let row = data.data.as_any().downcast_ref::<TargetRow>().unwrap();
            record.borrow_mut().rows.push(row.clone());
        });
        let topic = get_custom_topic(&TargetRow::data_type(StrategyId::from(STRATEGY)));
        msgbus::subscribe_any(topic.into(), handler, None);
        match self.source {
            Source::Quotes(id) => self.subscribe_quotes(id, None, None),
            Source::Bars(bar_type) => self.subscribe_bars(bar_type, None, None),
        }
        decide::start(self, DecisionRule::Every(self.every), SignedDuration::ZERO)
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        if decide::is_decision(event) {
            let instrument_id = match self.source {
                Source::Quotes(id) => id,
                Source::Bars(bar_type) => bar_type.instrument_id(),
            };
            let rule = DecisionRule::Every(self.every);
            if let Some(row) = decide::decide(self, &self.features, rule, instrument_id) {
                self.record.borrow_mut().decisions += 1;
                Rc::clone(&self.targets).open(self, row)?;
            }
            return Ok(());
        }
        Rc::clone(&self.targets).on_time_event(self, event)
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        self.record.borrow_mut().closed_orders = self
            .cache()
            .orders_closed(None, None, None, None, None)
            .len();
        self.record.borrow_mut().open_decisions = self.targets.open_decisions();
        self.targets.on_stop();
        Ok(())
    }
}

/// The targets of every test: a bracket `bps` basis points of the
/// decision's price away on both sides, closed 45 minutes after its
/// decision, and a horizon of 30 minutes, of `quantity` of
/// `instrument_id`.
fn targets(instrument_id: &str, quantity: &str, bps: Decimal) -> TargetsConfig {
    TargetsConfig::builder()
        .instrument_id(InstrumentId::from(instrument_id))
        .quantity(Quantity::from(quantity))
        .barrier(Barrier {
            take_profit: Distance::BasisPoints(bps),
            stop: Distance::BasisPoints(bps),
            limit: SignedDuration::from_mins(45),
            atr: None,
        })
        .horizon(SignedDuration::from_mins(30))
        .build()
}

/// The decision at `hh_mm` and `features`: it fires 10 ms after its time.
fn decision(at: Timestamp, features: f64) -> FeatureRow {
    let time = UnixNanos::from(at + SignedDuration::from_millis(10));
    FeatureRow {
        strategy_id: StrategyId::from(STRATEGY),
        features: vec![features],
        ts_event: time,
        ts_init: time,
    }
}

fn outcome(pnl: &str, exit: Exit) -> Outcome {
    Outcome {
        pnl: Money::from(pnl),
        exit,
    }
}

fn outcomes(long: Outcome, short: Outcome) -> Outcomes {
    Outcomes { long, short }
}

/// The row of `decision`, settled `settled` after its time.
fn row(
    decision: FeatureRow,
    barrier: Outcomes,
    horizon: Outcomes,
    settled: SignedDuration,
) -> TargetRow {
    let settled_at = UnixNanos::from(Timestamp::from(decision.ts_event) + settled);
    TargetRow {
        decision,
        barrier: Some(barrier),
        horizon: Some(horizon),
        settled_at,
    }
}

// ---------------------------------------------------------------- quotes

/// IC's gold every 5 s over `[08:00, 13:00)`, 4000.00/4000.20 but where
/// `overrides` says, and the quotes of `extra` besides.
fn gold_quotes(
    overrides: impl Fn(Timestamp) -> Option<(&'static str, &'static str)>,
    extra: &[(Timestamp, &str, &str)],
) -> Vec<QuoteTick> {
    let mut quotes = vec![];
    let mut time = ic::at("08:00:00");
    while time < ic::at("13:00:00") {
        let (bid, ask) = overrides(time).unwrap_or(("4000.00", "4000.20"));
        quotes.push(ic::quote(time, bid, ask));
        time += SignedDuration::from_secs(5);
    }
    quotes.extend(extra.iter().map(|&(t, bid, ask)| ic::quote(t, bid, ask)));
    quotes.sort_by_key(|quote| quote.ts_event);
    quotes
}

/// A run over `[08:30, 12:30]` of IC's gold quotes alone, on IC's hedging
/// venue, 30 USD per million traded, 10M USD.
fn gold_backtest(catalog: PathBuf) -> Backtest {
    Backtest {
        catalog,
        bar_types: vec![],
        ticks: vec![TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: InstrumentId::from(ic::INSTRUMENT_ID),
        }],
        start: ic::at("08:30:00"),
        end: ic::at("12:30:00"),
        warmup: SignedDuration::ZERO,
        fee_model: commission_rate(Decimal::new(3, 5)).unwrap(),
        balance: Money::from("10000000 USD"),
        leverage: Decimal::ONE,
        venue_oms: |_| OmsType::Hedging,
        logging: LoggerConfig::default(),
    }
}

/// The quotes of the hand-worked targets: a dip at 09:20, a spike at
/// 10:45:00.010, the time limit of the 10:00 decision, and a rise of 0.90
/// from 11:20 on.
fn hand_worked_quotes() -> Vec<QuoteTick> {
    gold_quotes(
        |t| match t {
            t if t == ic::at("09:20:00") => Some(("3996.80", "3997.00")),
            t if t >= ic::at("11:20:00") => Some(("4001.00", "4001.20")),
            _ => None,
        },
        &[(ic::at("10:45:00.010"), "4003.20", "4003.40")],
    )
}

/// One ounce, barriers 7.5 bps from the mid of 4000.10, 3.000075, on the
/// nearest cent: a long enters at the ask, 4000.20, its take profit a sell
/// limit at 4003.10 and its stop a sell stop at 3997.10, both on the bid;
/// a short enters at the bid,
/// 4000.00, and exits on the ask. Each fill pays 30 USD per million of its
/// value, 0.12 USD at these prices.
#[test]
fn targets_on_quotes_are_the_outcomes_worked_out_by_hand() {
    let (from, to) = (ic::at("08:00:00"), ic::at("13:00:00"));
    let catalog = ic::catalog_of(hand_worked_quotes(), from, to);
    let source = Source::Quotes(InstrumentId::from(ic::INSTRUMENT_ID));
    let (probe, record) =
        TargetProbe::new(source, targets(ic::INSTRUMENT_ID, "1", Decimal::new(75, 1)));

    gold_backtest(catalog.path().to_path_buf())
        .run_dataset(probe)
        .unwrap();

    let at = |hh_mm: &str| ic::at(&format!("{hh_mm}:00"));
    let flat = || {
        // 4000.20 to 4000.00 and back: 0.20 lost and two fees.
        outcomes(
            outcome("-0.44 USD", Exit::Time),
            outcome("-0.44 USD", Exit::Time),
        )
    };
    let expected = [
        // The dip at 09:20, 3996.80/3997.00: the long's stop fills at the
        // bid, 3996.80 (-3.40 - 0.24); the short's take profit at its limit,
        // 3997.10 (+2.90 - 0.24). The horizon closes on the flat quote.
        row(
            decision(at("09:00"), 4000.10),
            outcomes(
                outcome("-3.64 USD", Exit::Stop),
                outcome("2.66 USD", Exit::TakeProfit),
            ),
            flat(),
            SignedDuration::from_mins(30),
        ),
        // The spike at the bracket's time limit, at the same nanosecond as
        // its alert: inside the window, the long's take profit fills at
        // 4003.10, the short's stop at the ask, 4003.40.
        row(
            decision(at("10:00"), 4000.10),
            outcomes(
                outcome("2.66 USD", Exit::TakeProfit),
                outcome("-3.64 USD", Exit::Stop),
            ),
            flat(),
            SignedDuration::from_mins(45),
        ),
        // No barrier reached: both closed by time, on 4001.00/4001.20, the
        // long +0.80, the short -1.20, less the fees.
        row(
            decision(at("11:00"), 4000.10),
            outcomes(
                outcome("0.56 USD", Exit::Time),
                outcome("-1.44 USD", Exit::Time),
            ),
            outcomes(
                outcome("0.56 USD", Exit::Time),
                outcome("-1.44 USD", Exit::Time),
            ),
            SignedDuration::from_mins(45),
        ),
        // The 12:00 decision's positions are open at the end, 12:30:
        // unsettled, no row.
    ];
    assert_eq!(record.borrow().rows, expected);
}

/// Every order exceeds the instrument's largest, 10,000 oz: the risk
/// engine denies each, and the denial drops its decision, unsettled. The
/// run ends at 09:20, before the 09:00 decision's horizon and time limit,
/// whose alerts would drop it too: only the denial can have.
#[test]
fn denied_orders_leave_their_decisions_unsettled() {
    let (from, to) = (ic::at("08:00:00"), ic::at("13:00:00"));
    let catalog = ic::catalog_of(hand_worked_quotes(), from, to);
    let source = Source::Quotes(InstrumentId::from(ic::INSTRUMENT_ID));
    let (probe, record) = TargetProbe::new(
        source,
        targets(ic::INSTRUMENT_ID, "20000", Decimal::new(75, 1)),
    );
    let backtest = Backtest {
        end: ic::at("09:20:00"),
        ..gold_backtest(catalog.path().to_path_buf())
    };

    backtest.run_dataset(probe).unwrap();

    assert_eq!(record.borrow().decisions, 1);
    assert_eq!(record.borrow().rows, []);
    assert_eq!(record.borrow().open_decisions, 0);
}

// ------------------------------------------------------------------ bars

const BTC: &str = "BTC-USDT-SWAP.OKX";
const BTC_MINUTE: &str = "BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL";

/// BTC-USDT-SWAP as OKX defines it: price tick 0.1, lot 0.01 contracts,
/// 0.01 BTC a contract, with initial and maintenance margins of 1.
fn btc() -> InstrumentAny {
    InstrumentAny::CryptoPerpetual(
        CryptoPerpetual::builder()
            .instrument_id(InstrumentId::from(BTC))
            .raw_symbol(Symbol::from("BTC-USDT-SWAP"))
            .base_currency(Currency::BTC())
            .quote_currency(Currency::USDT())
            .settlement_currency(Currency::USDT())
            .is_inverse(false)
            .price_precision(1)
            .size_precision(2)
            .price_increment(Price::from("0.1"))
            .size_increment(Quantity::from("0.01"))
            .multiplier(Quantity::from("0.01"))
            .lot_size(Quantity::from("0.01"))
            .min_quantity(Quantity::from("0.01"))
            .margin_init(Decimal::ONE)
            .margin_maint(Decimal::ONE)
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .unwrap(),
    )
}

/// The minute bar opening at `open`, stamped as fill-catalog stores OKX's
/// candles (`ts_event` at the open, `ts_init` at the close): flat at
/// 100,000, but the minute opening 09:10 reaching 100,150 and then
/// 99,850, and the one opening 10:20 falling to 99,850.
fn btc_bar(open: Timestamp) -> Bar {
    let (high, low) = match open {
        t if t == ic::at("09:10:00") => (100_150.0, 99_850.0),
        t if t == ic::at("10:20:00") => (100_000.0, 99_850.0),
        _ => (100_000.0, 100_000.0),
    };
    let price = |value: f64| Price::new(value, 1);
    Bar::new(
        BarType::from(BTC_MINUTE),
        price(100_000.0),
        price(high),
        price(low),
        price(100_000.0),
        Quantity::from("500.00"),
        UnixNanos::from(open),
        UnixNanos::from(open + SignedDuration::from_mins(1)),
    )
}

/// Minute bars of BTC-USDT-SWAP over `[08:00, 13:00)`, each `bar` of its
/// open, in a catalog with the instrument.
fn btc_catalog(bar: impl Fn(Timestamp) -> Bar) -> tempfile::TempDir {
    let (from, to) = (ic::at("08:00:00"), ic::at("13:00:00"));
    let mut bars = vec![];
    let mut open = from;
    while open < to {
        bars.push(bar(open));
        open += SignedDuration::from_mins(1);
    }
    let dir = tempfile::tempdir().unwrap();
    let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    catalog.write_instruments(vec![btc()]).unwrap();
    catalog
        .write_to_parquet(&bars, Some(from.into()), Some(to.into()), None)
        .unwrap();
    dir
}

/// A run over `[08:30, 12:30]` of `catalog`'s BTC minute bars alone, on a
/// hedging venue, OKX's 0.05% of each fill, 10M USDT.
fn btc_backtest(catalog: &tempfile::TempDir) -> Backtest {
    Backtest {
        catalog: catalog.path().to_path_buf(),
        bar_types: vec![BarType::from(BTC_MINUTE)],
        ticks: vec![],
        fee_model: commission_rate(Decimal::new(5, 4)).unwrap(),
        balance: Money::from("10000000 USDT"),
        ..gold_backtest(PathBuf::new())
    }
}

/// One contract (0.01 BTC), barriers 10 bps, 100, from the close of
/// 100,000, on minute bars alone: the venue fills a market order at the
/// last close, a take profit at its limit and a stop at its trigger, and
/// replays each bar's open, high, low and close in that order. Each fill
/// pays OKX's 0.05% of its value: 0.5 USDT at 100,000, 0.5005 at 100,100,
/// 0.4995 at 99,900.
#[test]
fn targets_on_minute_bars_are_the_outcomes_worked_out_by_hand() {
    let catalog = btc_catalog(btc_bar);
    let source = Source::Bars(BarType::from(BTC_MINUTE));
    let (probe, record) = TargetProbe::new(source, targets(BTC, "1.00", Decimal::from(10)));

    btc_backtest(&catalog).run_dataset(probe).unwrap();

    let at = |hh_mm: &str| ic::at(&format!("{hh_mm}:00"));
    let flat = || {
        outcomes(
            outcome("-1 USDT", Exit::Time),
            outcome("-1 USDT", Exit::Time),
        )
    };
    let expected = [
        // The bar touching both barriers: its high comes first, so the
        // long's take profit fills at 100,100 (+1 less 1.0005 of fees, a
        // loss) and the short's stop at its trigger, 100,100 (-1 - 1.0005).
        row(
            decision(at("09:00"), 100_000.0),
            outcomes(
                outcome("-0.0005 USDT", Exit::TakeProfit),
                outcome("-2.0005 USDT", Exit::Stop),
            ),
            flat(),
            SignedDuration::from_mins(30),
        ),
        // The fall: the long's stop at its trigger, 99,900 (-1 - 0.9995),
        // the short's take profit at its limit (+1 - 0.9995).
        row(
            decision(at("10:00"), 100_000.0),
            outcomes(
                outcome("-1.9995 USDT", Exit::Stop),
                outcome("0.0005 USDT", Exit::TakeProfit),
            ),
            flat(),
            SignedDuration::from_mins(30),
        ),
        row(
            decision(at("11:00"), 100_000.0),
            flat(),
            flat(),
            SignedDuration::from_mins(45),
        ),
    ];
    let record = record.borrow();
    assert_eq!(record.rows, expected);
    // A take profit can lose net of fees: its sign is the P&L's.
    let barrier = record.rows[0].barrier.unwrap();
    assert_eq!((barrier.long.sign(), barrier.short.sign()), (-1, -1));
    assert_eq!(record.rows[1].barrier.unwrap().short.sign(), 1);
}

// ------------------------------------------------------------------- atr

/// IC's gold as `gold_quotes`, 4000.10 its mid, ranging in the hours
/// before 10:00: from 08:30, the run's start, to 09:00 between 4000.00
/// (08:40) and 4000.30 (08:50), to 10:00 between 3999.90 (09:20) and
/// 4000.40 (09:40); and a rise to 4000.90/4001.10 at 10:20.
fn ranging_quotes() -> Vec<QuoteTick> {
    gold_quotes(
        |t| match t {
            t if t == ic::at("08:40:00") => Some(("3999.90", "4000.10")),
            t if t == ic::at("08:50:00") => Some(("4000.20", "4000.40")),
            t if t == ic::at("09:20:00") => Some(("3999.80", "4000.00")),
            t if t == ic::at("09:40:00") => Some(("4000.30", "4000.50")),
            t if t == ic::at("10:20:00") => Some(("4000.90", "4001.10")),
            _ => None,
        },
        &[],
    )
}

/// One ounce of gold, deciding every hour, until 10:50: a take profit 2
/// and a stop 1.5 average true ranges away, the ATR of 2 hourly bars of
/// the mids, which the targets subscribe to and the probe does not. The
/// bar closing 09:00, from the run's start, ranges 0.30; the one closing
/// 10:00 from 3999.90 to 4000.40 past the close before, 4000.10, 0.50:
/// an ATR of 0.40, the exits 0.80 and 0.60 from 4000.10. The ATR has one
/// bar at 09:00: that decision opens no target. Each fill pays 30 USD per
/// million of its value, 0.12 USD at these prices.
#[test]
fn targets_an_atr_of_cadence_bars_away_are_the_outcomes_worked_out_by_hand() {
    let (from, to) = (ic::at("08:00:00"), ic::at("13:00:00"));
    let catalog = ic::catalog_of(ranging_quotes(), from, to);
    let backtest = Backtest {
        end: ic::at("10:50:00"),
        ..gold_backtest(catalog.path().to_path_buf())
    };
    let config = TargetsConfig::builder()
        .instrument_id(InstrumentId::from(ic::INSTRUMENT_ID))
        .quantity(Quantity::from(1))
        .barrier(Barrier {
            take_profit: Distance::Atr(Decimal::from(2)),
            stop: Distance::Atr(Decimal::new(15, 1)),
            limit: SignedDuration::from_mins(45),
            atr: Some(AtrConfig { period: 2 }),
        })
        .horizon(SignedDuration::from_mins(30))
        .build();
    let source = Source::Quotes(InstrumentId::from(ic::INSTRUMENT_ID));
    let (probe, record) = TargetProbe::new(source, config);

    backtest.run_dataset(probe).unwrap();

    let expected = [
        // The long enters at 4000.20, its take profit a sell limit at
        // 4000.90, its stop at 3999.50; the short at 4000.00, its take
        // profit at 3999.30, its stop a buy stop at 4000.70. The rise at
        // 10:20 fills the long's take profit at its limit (+0.70 - 0.24)
        // and the short's stop at the ask, 4001.10 (-1.10 - 0.24). The
        // horizon closes both on the flat quote at 10:30.
        row(
            decision(ic::at("10:00:00"), 4000.10),
            outcomes(
                outcome("0.46 USD", Exit::TakeProfit),
                outcome("-1.34 USD", Exit::Stop),
            ),
            outcomes(
                outcome("-0.44 USD", Exit::Time),
                outcome("-0.44 USD", Exit::Time),
            ),
            SignedDuration::from_mins(30),
        ),
    ];
    let record = record.borrow();
    assert_eq!(record.rows, expected);
    // The 09:00 decision sent no order: the first fill is the 10:00 one's.
    assert_eq!(
        record.fills.first(),
        Some(&UnixNanos::from(ic::at("10:00:00.010")))
    );
}

// ----------------------------------------------------------------- purge

/// A deterministic random walk in [0, 1).
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> f64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// The run of the probe deciding every 15 minutes on `catalog`'s random
/// walk from `start` to `end`, a dataset run or a trading one, and what
/// it recorded: on a thread of its own, one node per thread.
fn walk_run(
    catalog: PathBuf,
    start: Timestamp,
    end: Timestamp,
    dataset: bool,
) -> (Vec<TargetRow>, usize) {
    thread::spawn(move || {
        let backtest = Backtest {
            start,
            end,
            ..gold_backtest(catalog)
        };
        let config = TargetsConfig::builder()
            .instrument_id(InstrumentId::from(ic::INSTRUMENT_ID))
            .quantity(Quantity::from(1))
            .barrier(Barrier {
                take_profit: Distance::BasisPoints(Decimal::new(75, 1)),
                stop: Distance::BasisPoints(Decimal::new(75, 1)),
                limit: SignedDuration::from_hours(2),
                atr: None,
            })
            .horizon(SignedDuration::from_hours(1))
            .build();
        let source = Source::Quotes(InstrumentId::from(ic::INSTRUMENT_ID));
        let (probe, record) = TargetProbe::every(source, config, SignedDuration::from_mins(15));
        if dataset {
            backtest.run_dataset(probe).unwrap();
        } else {
            backtest.run(probe).unwrap();
        }
        let record = record.take();
        (record.rows, record.closed_orders)
    })
    .join()
    .unwrap()
}

/// Two days of IC's gold, a quote every 5 s, the bid a random walk of up
/// to 0.30 a step, the ask 0.20 above.
#[test]
fn targets_are_the_same_with_the_cache_purged_and_without() {
    let (from, to) = (
        ic::at("00:00:00"),
        ic::at("00:00:00") + SignedDuration::from_hours(48),
    );
    let mut walk = Lcg(11);
    let mut bid = 4000.0_f64;
    let mut quotes = vec![];
    let mut time = from;
    while time < to {
        bid = ((bid + (walk.next() - 0.5) * 0.6) * 100.0).round() / 100.0;
        let (b, a) = (format!("{bid:.2}"), format!("{:.2}", bid + 0.2));
        quotes.push(ic::quote(time, &b, &a));
        time += SignedDuration::from_secs(5);
    }
    let catalog = ic::catalog_of(quotes, from, to);
    let (start, end) = (from + SignedDuration::from_mins(30), to);

    let (purged, closed_purged) = walk_run(catalog.path().to_path_buf(), start, end, true);
    let (kept, closed_kept) = walk_run(catalog.path().to_path_buf(), start, end, false);

    // Every decision whose time limit, two hours on, passes before the end
    // settles, none denied on 10M USD; a later one settles if its barriers
    // are hit before the end.
    let settled: BTreeSet<UnixNanos> = purged.iter().map(|row| row.decision.ts_event).collect();
    let mut due = 0;
    let mut decision = start + SignedDuration::from_millis(10);
    while decision + SignedDuration::from_hours(2) < end {
        assert!(settled.contains(&decision.into()), "{decision} unsettled");
        due += 1;
        decision += SignedDuration::from_mins(15);
    }
    assert_eq!(purged, kept);
    // Unpurged, the cache holds each closed order, four or more a target;
    // purged, only those closed in the last minutes.
    assert!(closed_kept >= 4 * 2 * due, "{closed_kept} kept");
    assert!(closed_purged < 50, "{closed_purged} left after purging");
}
