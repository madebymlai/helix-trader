//! Targets from orders (`model::targets`): a probe decides every hour on
//! one feature, the last mid, trade or close, and opens at each decision
//! its target on each side, a bracket or a time-only trade, on a hedging
//! venue, through `Backtest::run_dataset`; it records the target rows
//! published for it. On quotes, trades and minute bars, with exits and
//! paths chosen so that each target is worked out by hand: a long ended by
//! its stop and a short by its take profit, an exit hit at the very
//! nanosecond of the time limit (inside the window), exits left alone
//! (closed by time), a bar touching both exits (the venue replays its
//! high before its low), the last decision unsettled at the end of data,
//! a bracket and a time-only target on an inverse instrument refused, each
//! side's payoff features worked out by hand on quotes, on bars and on
//! trades alone, exits in basis points and in average true ranges of hourly
//! bars, the cadence's, which the targets subscribe to themselves, built
//! from the quotes, from the trades alone and from minute bars alone, a
//! time-only target's unit the worth of such an ATR on quotes and on minute
//! bars, no order before the ATR is initialized; deciding on LAST bars the run
//! builds from the trades alone is deciding on the trades. An order the
//! risk engine denies fails the run with its reason and stops it. A
//! stop from a mid on a half tick settles at its exact worth, and one in the
//! market the venue rejects, without a row. The probe opens its targets at
//! its run's commission: at one per quantity, a backtest of it charges each
//! fill what the commission says it costs, and a dataset run settles the
//! rows the backtest does, net of that cost.
//! Over two days of a random walk the targets are the same
//! with the execution engine purging closed orders and positions as
//! without, while the purge keeps the cache to the last minutes' orders.
//! One node per thread, so one run per test, or per thread of a test.

use std::{cell::RefCell, collections::BTreeSet, path::PathBuf, rc::Rc, thread};

use crate::common::{
    features::{SmaArgs, tables},
    ic,
};
use helix_trader::{
    backtest::{Backtest, Commission},
    model::{
        dataset::DatasetVenue,
        decide::{self, DecisionRule, FeatureRow},
        features::FeatureSet,
        handle::SettledRows,
        targets::{
            BracketSpec, DEFAULT_ATR, Distance, Exit, Outcome, Outcomes, Target, TargetConfig,
            TargetRow, Targets,
        },
    },
    strategy::{AtrArgs, DataRequirements, Declared, TickSeries, orders, risk::Risk},
};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, indicators::ActorIndicator},
    logging::logger::LoggerConfig,
    msgbus::{self, ShareableMessageHandler, switchboard::get_custom_topic},
    timer::TimeEvent,
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, CustomData, NautilusDataType, QuoteTick, TradeTick},
    enums::{AggressorSide, OmsType},
    events::{OrderCanceled, OrderDenied, OrderFilled, OrderRejected, PositionClosed},
    identifiers::{InstrumentId, StrategyId, Symbol, TradeId},
    instruments::{CryptoPerpetual, InstrumentAny, stubs::btcusd_bybit},
    types::{Currency, Money, Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rust_decimal::Decimal;

const STRATEGY: &str = "TARGETS-001";

/// The mid of the last quote, Nautilus' simple average of one,
/// `[indicators.mid]`.
const MID: Declared<SmaArgs> = Declared::new("mid");
/// The price of the last trade, `[indicators.last]`.
const LAST: Declared<SmaArgs> = Declared::new("last");
/// The close of the last bar, `[indicators.close]`.
const CLOSE: Declared<SmaArgs> = Declared::new("close");

/// What the probe decides on: the mid of the last quote of an instrument,
/// the price of its last trade, or the close of the last bar of a bar
/// type.
#[derive(Debug, Clone, Copy)]
enum Source {
    Quotes(InstrumentId),
    Trades(InstrumentId),
    Bars(BarType),
}

impl Source {
    /// What a strategy deciding on it reads, as the run replays it.
    fn requirements(self) -> DataRequirements {
        let ticks = |data, instrument_id| DataRequirements {
            bar_types: vec![],
            ticks: vec![TickSeries {
                data,
                instrument_id,
            }],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        };
        match self {
            Self::Quotes(id) => ticks(NautilusDataType::QuoteTick, id),
            Self::Trades(id) => ticks(NautilusDataType::TradeTick, id),
            Self::Bars(bar_type) => DataRequirements {
                bar_types: vec![bar_type],
                ticks: vec![],
                custom: vec![],
                warmup: SignedDuration::ZERO,
            },
        }
    }

    fn instrument_id(self) -> InstrumentId {
        match self {
            Self::Quotes(id) | Self::Trades(id) => id,
            Self::Bars(bar_type) => bar_type.instrument_id(),
        }
    }
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
    /// When each of its orders filled, in order.
    fills: Vec<UnixNanos>,
    /// The client order id of each of its orders filled, in order.
    filled: Vec<String>,
    /// The commission of each fill, in order.
    commissions: Vec<Money>,
    /// The client order id of each of its orders the venue rejected, in order.
    rejected: Vec<String>,
    /// The denial that failed the run, if any (`Targets::on_order_denied`).
    denial: Option<String>,
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
    /// What the targets fail the run with: a denial.
    settled: SettledRows,
    record: Rc<RefCell<Record>>,
}

impl TargetProbe {
    /// The probe deciding every hour on `source`, opening `targets` at the
    /// commission and the reference stake of the balance of `backtest`, the
    /// run it runs in.
    fn new(
        source: Source,
        targets: TargetConfig,
        backtest: &Backtest,
    ) -> (Self, Rc<RefCell<Record>>) {
        Self::every(source, targets, SignedDuration::from_hours(1), backtest)
    }

    /// The probe deciding every `every` on `source`, opening `targets` at
    /// the commission of `backtest`, whose ATR, if any, is on bars of
    /// `every`.
    fn every(
        source: Source,
        targets: TargetConfig,
        every: SignedDuration,
        backtest: &Backtest,
    ) -> (Self, Rc<RefCell<Record>>) {
        let tables = tables(
            r#"
            mid = { period = 1, price_type = "MID" }
            last = { period = 1 }
            close = { period = 1 }
            "#,
        );
        let features = match source {
            Source::Quotes(id) => FeatureSet::builder().quotes(id, MID, &tables),
            Source::Trades(id) => FeatureSet::builder().trades(id, LAST, &tables),
            Source::Bars(bar_type) => FeatureSet::builder().bars(bar_type, CLOSE, &tables),
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
            targets: Targets::new(
                targets,
                DecisionRule::Every(every),
                &source.requirements(),
                &DatasetVenue {
                    commission: backtest.commission,
                    balance: backtest.balance,
                },
            )
            .unwrap(),
            settled: SettledRows::default(),
            record: Rc::clone(&record),
        };
        (probe, record)
    }
}

nautilus_strategy!(TargetProbe, {
    fn on_order_filled(&mut self, event: &OrderFilled) {
        let mut record = self.record.borrow_mut();
        record.fills.push(event.ts_event);
        record.filled.push(event.client_order_id.to_string());
        record.commissions.extend(event.commission);
    }

    fn on_order_canceled(&mut self, event: &OrderCanceled) {
        self.targets.on_order_canceled(self, event);
    }

    fn on_order_denied(&mut self, event: OrderDenied) {
        let settled = self.settled.clone();
        Rc::clone(&self.targets).on_order_denied(self, &event, &settled);
    }

    fn on_order_rejected(&mut self, event: OrderRejected) {
        self.record
            .borrow_mut()
            .rejected
            .push(event.client_order_id.to_string());
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
        Rc::clone(&self.targets).start(self)?;
        let record = Rc::clone(&self.record);
        let handler = ShareableMessageHandler::from_typed(move |data: &CustomData| {
            let row = data.data.as_any().downcast_ref::<TargetRow>().unwrap();
            record.borrow_mut().rows.push(row.clone());
        });
        let topic = get_custom_topic(&TargetRow::data_type(StrategyId::from(STRATEGY)));
        msgbus::subscribe_any(topic.into(), handler, None);
        match self.source {
            Source::Quotes(id) => self.subscribe_quotes(id, None, None),
            Source::Trades(id) => self.subscribe_trades(id, None, None),
            Source::Bars(bar_type) => self.subscribe_bars(bar_type, None, None),
        }
        decide::start(self, DecisionRule::Every(self.every), SignedDuration::ZERO)
    }

    /// Hands the features its quotes and trades: the set is registered with
    /// Nautilus for its bars alone, a model strategy's warmup handing it its
    /// ticks.
    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        self.features.handle_quote(quote)
    }

    fn on_trade(&mut self, trade: &TradeTick) -> anyhow::Result<()> {
        self.features.handle_trade(trade)
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        if decide::is_decision(event) {
            let instrument_id = self.source.instrument_id();
            let rule = DecisionRule::Every(self.every);
            if let Some(row) = decide::decide(self, &self.features, rule, instrument_id) {
                self.record.borrow_mut().decisions += 1;
                Rc::clone(&self.targets).open(self, row)?;
            }
            return Ok(());
        }
        orders::on_time_event(self, event)?;
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        let mut record = self.record.borrow_mut();
        record.closed_orders = self
            .cache()
            .orders_closed(None, None, None, None, None)
            .len();
        record.denial = self.settled.take().err().map(|e| format!("{e:#}"));
        drop(record);
        self.targets.on_stop();
        Ok(())
    }
}

/// The risk at which the reference stake is `money` of the runs' balance of
/// ten million, USD or USDT: a target whose one R `money` is worth on one
/// unit, and less than on two, trades one unit, as the tests work out.
fn risking(money: &str) -> Risk {
    let fraction = Decimal::from_str_exact(money).unwrap() / Decimal::from(10_000_000);
    format!("{}%", (fraction * Decimal::ONE_HUNDRED).normalize())
        .parse()
        .unwrap()
}

/// `spec` on `instrument_id` at `risk`.
fn trading(spec: Target, instrument_id: &str, risk: Risk) -> TargetConfig {
    TargetConfig::builder()
        .instrument_id(InstrumentId::from(instrument_id))
        .risk(risk)
        .spec(spec)
        .build()
}

/// A bracket `bps` basis points of the decision's price away on both
/// sides, closed 45 minutes after its decision, on `instrument_id` at
/// `risk`.
fn bracket(instrument_id: &str, risk: Risk, bps: Decimal) -> TargetConfig {
    let spec = Target::builder()
        .bracket(BracketSpec {
            take_profit: Distance::BasisPoints(bps),
            stop: Distance::BasisPoints(bps),
            atr: None,
        })
        .limit(SignedDuration::from_mins(45))
        .build();
    trading(spec, instrument_id, risk)
}

/// A trade with no exits, closed 30 minutes after its decision, on
/// `instrument_id` at `risk`, its one R the average true range of 2 hourly
/// bars, the cadence's, which the targets subscribe to.
fn time_only(instrument_id: &str, risk: Risk) -> TargetConfig {
    let spec = Target::builder()
        .limit(SignedDuration::from_mins(30))
        .atr(AtrArgs {
            period: 2,
            ..DEFAULT_ATR
        })
        .build();
    trading(spec, instrument_id, risk)
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

/// The outcome of a position closed by `exit` with `pnl`, net of
/// `commission`, of a unit of 1 until `of_unit` gives it its target's.
fn outcome(pnl: &str, commission: &str, exit: Exit) -> Outcome {
    let pnl = Money::from(pnl);
    Outcome {
        pnl,
        commission: Money::from(commission),
        exit,
        unit: Money::new(1.0, pnl.currency),
    }
}

fn outcomes(long: Outcome, short: Outcome) -> Outcomes {
    Outcomes { long, short }
}

/// The outcomes `long` and `short`, each side's unit worth `unit` in its
/// P&L's currency: a bracket's stop's worth, a time-only target's ATR's.
fn of_unit(long: Outcome, short: Outcome, unit: f64) -> Outcomes {
    let unit = |outcome: Outcome| {
        let unit = Decimal::try_from(unit).unwrap();
        Money::from_decimal_native(unit, outcome.pnl.currency).unwrap()
    };
    Outcomes {
        long: Outcome {
            unit: unit(long),
            ..long
        },
        short: Outcome {
            unit: unit(short),
            ..short
        },
    }
}

/// A target row as the tests compare it: its decision, its outcomes and
/// when it settled; its payoff features are compared apart, to 1e-12
/// (`assert_payoff`).
type Settled = (FeatureRow, Outcomes, UnixNanos);

/// The row of `decision`, its target's `outcomes`, settled at `settled_at`.
fn row(decision: FeatureRow, outcomes: Outcomes, settled_at: Timestamp) -> Settled {
    (decision, outcomes, UnixNanos::from(settled_at))
}

/// `rows` as the tests compare them (`Settled`).
fn settled(rows: &[TargetRow]) -> Vec<Settled> {
    rows.iter()
        .map(|row| (row.decision.clone(), row.outcomes, row.settled_at))
        .collect()
}

/// Asserts that each of `rows` has the payoff features `long` and `short`
/// on its sides, each within 1e-12, as many: a bracket's take profit's R,
/// stop's nominal loss and entry cost, a time-only target's entry cost.
fn assert_payoff(rows: &[TargetRow], long: &[f64], short: &[f64]) {
    assert!(!rows.is_empty());
    for row in rows {
        for (got, want) in [(&row.payoff.long, long), (&row.payoff.short, short)] {
            assert_eq!(got.len(), want.len(), "{got:?}, not {want:?}");
            for (g, w) in got.iter().zip(want) {
                assert!((g - w).abs() < 1e-12, "{got:?}, not {want:?}");
            }
        }
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
        custom: vec![],
        start: ic::at("08:30:00"),
        end: ic::at("12:30:00"),
        commission: Commission::Rate(Decimal::new(3, 5)),
        balance: Money::from("10000000 USD"),
        venue_oms: |_| OmsType::Hedging,
        logging: LoggerConfig::default(),
    }
}

/// The quotes of the hand-worked targets: a dip at 09:20, a spike at
/// 10:45:00.010, the time limit of the 10:00 decision, and a rise of 0.90
/// from 11:20 on.
pub(crate) fn hand_worked_quotes() -> Vec<QuoteTick> {
    gold_quotes(
        |t| match t {
            t if t == ic::at("09:20:00") => Some(("3996.80", "3997.00")),
            t if t >= ic::at("11:20:00") => Some(("4001.00", "4001.20")),
            _ => None,
        },
        &[(ic::at("10:45:00.010"), "4003.20", "4003.40")],
    )
}

/// One ounce, exits 7.5 bps from the mid of 4000.10, 3.000075, on the
/// nearest cent: a long enters at the ask, 4000.20, its take profit a sell
/// limit at 4003.10 and its stop a sell stop at 3997.10, both on the bid;
/// a short enters at the bid,
/// 4000.00, and exits on the ask. Each stop, 3.00 away on one ounce, is worth
/// 3 USD. Each fill pays 30 USD per million of its value, 0.12 USD at these
/// prices. At each decision, on 4000.00/4000.20, the long's take profit would
/// net 2.90 − 0.24 and its stop lose 3.10 + 0.24, the short's the same, and
/// either's entry costs the spread and a fee, 0.20 + 0.12: in R, over 3 USD,
/// its payoff features. A take profit realises its R.
#[test]
fn targets_on_quotes_are_the_outcomes_worked_out_by_hand() {
    let (from, to) = (ic::at("08:00:00"), ic::at("13:00:00"));
    let catalog = ic::catalog_of(hand_worked_quotes(), from, to);
    let source = Source::Quotes(InstrumentId::from(ic::INSTRUMENT_ID));
    let backtest = gold_backtest(catalog.path().to_path_buf());
    let (probe, record) = TargetProbe::new(
        source,
        bracket(ic::INSTRUMENT_ID, risking("4"), Decimal::new(75, 1)),
        &backtest,
    );

    backtest.run_dataset(probe).unwrap();

    let at = |hh_mm: &str| ic::at(&format!("{hh_mm}:00"));
    let expected = [
        // The dip at 09:20, 3996.80/3997.00: the long's stop fills at the
        // bid, 3996.80 (-3.40 - 0.24); the short's take profit at its limit,
        // 3997.10 (+2.90 - 0.24). Both on that quote, which settles the row.
        row(
            decision(at("09:00"), 4000.10),
            of_unit(
                outcome("-3.64 USD", "0.24 USD", Exit::Stop),
                outcome("2.66 USD", "0.24 USD", Exit::TakeProfit),
                3.0,
            ),
            ic::at("09:20:00"),
        ),
        // The spike at the bracket's time limit, at the same nanosecond as
        // its alert: inside the window, the long's take profit fills at
        // 4003.10, the short's stop at the ask, 4003.40.
        row(
            decision(at("10:00"), 4000.10),
            of_unit(
                outcome("2.66 USD", "0.24 USD", Exit::TakeProfit),
                outcome("-3.64 USD", "0.24 USD", Exit::Stop),
                3.0,
            ),
            ic::at("10:45:00.010"),
        ),
        // No exit reached: both closed by time, on 4001.00/4001.20, the
        // long +0.80, the short -1.20, less the fees.
        row(
            decision(at("11:00"), 4000.10),
            of_unit(
                outcome("0.56 USD", "0.24 USD", Exit::Time),
                outcome("-1.44 USD", "0.24 USD", Exit::Time),
                3.0,
            ),
            ic::at("11:45:00.010"),
        ),
        // The 12:00 decision's positions are open at the end, 12:30:
        // unsettled, no row.
    ];
    let record = record.borrow();
    assert_eq!(settled(&record.rows), expected);
    let side = [2.66 / 3.0, 3.34 / 3.0, 0.32 / 3.0];
    assert_payoff(&record.rows, &side, &side);
    let take_profit = record.rows[1].outcomes.long;
    assert!((take_profit.pnl.as_f64() / take_profit.unit.as_f64() - side[0]).abs() < 1e-12);
}

/// Pepperstone's commission on gold, 3.5 USD per lot of 100 oz: 0.035 USD
/// an ounce.
fn per_ounce() -> Commission {
    Commission::PerQuantity(Money::from_str_exact("0.035 USD").unwrap())
}

/// The hand-worked targets' probe at `commission`, over `[08:30, 12:30]`,
/// in a dataset run or a backtest, and what it recorded: on a thread of its
/// own, one node per thread.
fn hand_worked_run(commission: Commission, dataset: bool) -> Record {
    thread::spawn(move || {
        let (from, to) = (ic::at("08:00:00"), ic::at("13:00:00"));
        let catalog = ic::catalog_of(hand_worked_quotes(), from, to);
        let backtest = Backtest {
            commission,
            ..gold_backtest(catalog.path().to_path_buf())
        };
        let source = Source::Quotes(InstrumentId::from(ic::INSTRUMENT_ID));
        let (probe, record) = TargetProbe::new(
            source,
            bracket(ic::INSTRUMENT_ID, risking("4"), Decimal::new(75, 1)),
            &backtest,
        );
        if dataset {
            backtest.run_dataset(probe).unwrap();
        } else {
            backtest.run(probe).unwrap();
        }
        record.take()
    })
    .join()
    .unwrap()
}

/// The hand-worked targets at Pepperstone's commission: a fill of an ounce
/// costs 0.035 USD, to the cent 0.04 USD, at any price, so each side's
/// trade 0.08 USD. A backtest of the probe charges each of its 14 fills that
/// cost, the entries and exits of the three decisions settling and the
/// entries of the 12:00 one, and a dataset run of it settles the rows the
/// backtest does, net of it: the long stopped -3.48 USD (-3.40 less 0.08),
/// taking profit 2.82 (2.90 less 0.08) and closed by time 0.72 (0.80 less
/// 0.08), the short the reverse but closed by time -1.28 (-1.20 less 0.08).
/// Each side's payoff features are of that cost: a take profit netting 2.90 −
/// 0.08, a stop losing 3.10 + 0.08 and an entry costing 0.20 + 0.04, over the
/// stop's 3 USD.
#[test]
fn a_backtest_and_a_dataset_run_charge_a_commission_per_quantity_alike() {
    let cost = per_ounce()
        .cost(Quantity::from(1), Price::from("4000.20"), &ic::instrument())
        .unwrap();

    let backtest = hand_worked_run(per_ounce(), false);
    let dataset = hand_worked_run(per_ounce(), true);

    assert_eq!(cost, Money::from("0.04 USD"));
    assert_eq!(backtest.commissions, [cost; 14]);
    assert_eq!(dataset.commissions, [cost; 14]);
    let at = |hh_mm: &str| ic::at(&format!("{hh_mm}:00"));
    let expected = [
        row(
            decision(at("09:00"), 4000.10),
            of_unit(
                outcome("-3.48 USD", "0.08 USD", Exit::Stop),
                outcome("2.82 USD", "0.08 USD", Exit::TakeProfit),
                3.0,
            ),
            ic::at("09:20:00"),
        ),
        row(
            decision(at("10:00"), 4000.10),
            of_unit(
                outcome("2.82 USD", "0.08 USD", Exit::TakeProfit),
                outcome("-3.48 USD", "0.08 USD", Exit::Stop),
                3.0,
            ),
            ic::at("10:45:00.010"),
        ),
        row(
            decision(at("11:00"), 4000.10),
            of_unit(
                outcome("0.72 USD", "0.08 USD", Exit::Time),
                outcome("-1.28 USD", "0.08 USD", Exit::Time),
                3.0,
            ),
            ic::at("11:45:00.010"),
        ),
    ];
    assert_eq!(settled(&dataset.rows), expected);
    assert_eq!(settled(&backtest.rows), expected);
    let side = [2.82 / 3.0, 3.18 / 3.0, 0.24 / 3.0];
    assert_payoff(&dataset.rows, &side, &side);
    assert_payoff(&backtest.rows, &side, &side);
}

/// A run to 09:50, one decision settling, of IC's gold quoted at
/// 4000.00/4000.01 throughout: the mid, 4000.005, on a half tick.
fn half_tick_run() -> (tempfile::TempDir, Backtest) {
    let (from, to) = (ic::at("08:00:00"), ic::at("13:00:00"));
    let quotes = gold_quotes(|_| Some(("4000.00", "4000.01")), &[]);
    let catalog = ic::catalog_of(quotes, from, to);
    let backtest = Backtest {
        end: ic::at("09:50:00"),
        ..gold_backtest(catalog.path().to_path_buf())
    };
    (catalog, backtest)
}

/// One ounce, exits 7.5 bps of the mid of 4000.005, 3.00000375, on the nearest
/// cent: the long's take profit 4003.01 and stop 3997.00, the short's the
/// reverse, each stop 3.005 away and worth 3.005 USD, not the cents around it.
/// No exit is reached: each side crosses the cent of the spread and closes by
/// time, -0.01, less two fees of 0.12 USD.
#[test]
fn a_stop_from_a_mid_on_a_half_tick_settles_at_its_exact_worth() {
    let (_catalog, backtest) = half_tick_run();
    let source = Source::Quotes(InstrumentId::from(ic::INSTRUMENT_ID));
    let (probe, record) = TargetProbe::new(
        source,
        bracket(ic::INSTRUMENT_ID, risking("4"), Decimal::new(75, 1)),
        &backtest,
    );

    backtest.run_dataset(probe).unwrap();

    let expected = [row(
        decision(ic::at("09:00:00"), 4000.005),
        of_unit(
            outcome("-0.25 USD", "0.24 USD", Exit::Time),
            outcome("-0.25 USD", "0.24 USD", Exit::Time),
            3.005,
        ),
        ic::at("09:45:00.010"),
    )];
    assert_eq!(settled(&record.borrow().rows), expected);
}

/// One ounce, exits a hundredth of a basis point of the mid of 4000.005, on the
/// cents either side: each stop is half a cent from the mid and on the touch,
/// in the market, so the venue rejects it, and the decision stores no row.
#[test]
fn a_stop_half_a_tick_from_a_mid_on_a_half_tick_is_rejected_and_stores_no_row() {
    let (_catalog, backtest) = half_tick_run();
    let source = Source::Quotes(InstrumentId::from(ic::INSTRUMENT_ID));
    let (probe, record) = TargetProbe::new(
        source,
        bracket(ic::INSTRUMENT_ID, risking("4"), Decimal::new(1, 2)),
        &backtest,
    );

    backtest.run_dataset(probe).unwrap();

    let record = record.borrow();
    assert_eq!(record.decisions, 1);
    assert_eq!(record.rejected, ["D0-L-SL", "D0-S-SL"]);
    assert_eq!(record.rows, []);
}

/// One ounce, no exits, on the same quotes: each side enters as a bracket
/// does, its position closed 30 minutes after its send on the quote then,
/// each fill paying 0.12 USD. The dip at 09:20 and the spike at 10:45 pass
/// by it. Its unit is the worth of the ATR of 2 hourly bars of the mids on
/// one ounce: the bar closing 09:00, from the run's start, ranges nought;
/// the one closing 10:00 from 3996.90 (09:20) to 4000.10, 3.20, an ATR of
/// 1.60, Wilder's seeded by the mean of the two; the one closing 11:00 from
/// 4000.10 to 4003.30 (10:45), 3.20, an ATR of (1.60 + 3.20) / 2 = 2.40. The
/// ATR has one bar at 09:00: that decision opens no target and takes no
/// number. Each side's entry costs the spread and a fee, 0.20 + 0.12 USD,
/// over its unit.
#[test]
fn time_only_targets_on_quotes_are_the_outcomes_worked_out_by_hand() {
    let (from, to) = (ic::at("08:00:00"), ic::at("13:00:00"));
    let catalog = ic::catalog_of(hand_worked_quotes(), from, to);
    let source = Source::Quotes(InstrumentId::from(ic::INSTRUMENT_ID));
    let backtest = gold_backtest(catalog.path().to_path_buf());
    let (probe, record) = TargetProbe::new(
        source,
        time_only(ic::INSTRUMENT_ID, risking("3")),
        &backtest,
    );

    backtest.run_dataset(probe).unwrap();

    let at = |hh_mm: &str| ic::at(&format!("{hh_mm}:00"));
    let expected = [
        // 4000.20 to 4000.00 and back: 0.20 lost and two fees.
        row(
            decision(at("10:00"), 4000.10),
            of_unit(
                outcome("-0.44 USD", "0.24 USD", Exit::Time),
                outcome("-0.44 USD", "0.24 USD", Exit::Time),
                1.6,
            ),
            ic::at("10:30:00.010"),
        ),
        // Closed on 4001.00/4001.20, the long +0.80, the short -1.20, less
        // the fees.
        row(
            decision(at("11:00"), 4000.10),
            of_unit(
                outcome("0.56 USD", "0.24 USD", Exit::Time),
                outcome("-1.44 USD", "0.24 USD", Exit::Time),
                2.4,
            ),
            ic::at("11:30:00.010"),
        ),
        // The 12:00 decision closes 10 ms after the end, 12:30: unsettled,
        // no row.
    ];
    let record = record.borrow();
    assert_eq!(settled(&record.rows), expected);
    for (row, unit) in record.rows.iter().zip([1.6, 2.4]) {
        assert_payoff(std::slice::from_ref(row), &[0.32 / unit], &[0.32 / unit]);
    }
    assert!(record.filled[0].starts_with("D0-"), "{:?}", record.filled);
}

/// Each order's id is its decision's number, its side and its leg, so the
/// outcome of a position reads from its orders alone: over the first
/// decision to 09:25, the entries, and on the dip at 09:20 the long's stop
/// and the short's take profit.
#[test]
fn a_targets_orders_are_named_by_their_decision_side_and_leg() {
    let (from, to) = (ic::at("08:00:00"), ic::at("13:00:00"));
    let catalog = ic::catalog_of(hand_worked_quotes(), from, to);
    let source = Source::Quotes(InstrumentId::from(ic::INSTRUMENT_ID));
    let backtest = Backtest {
        end: ic::at("09:25:00"),
        ..gold_backtest(catalog.path().to_path_buf())
    };
    let (probe, record) = TargetProbe::new(
        source,
        bracket(ic::INSTRUMENT_ID, risking("4"), Decimal::new(75, 1)),
        &backtest,
    );

    backtest.run_dataset(probe).unwrap();

    let mut filled = record.borrow().filled.clone();
    filled.sort();
    assert_eq!(filled, ["D0-L-E", "D0-L-SL", "D0-S-E", "D0-S-TP"]);
}

/// Half of a balance of 100,000 USD, 50,000 USD, over a stop of 3.00 buys
/// 16,666 oz, which Nautilus' sizer caps at gold's largest order, 10,000
/// oz: 40M USD of gold a side, whose margin at IC's 1 / 30, 1.33M USD, an
/// account of 100,000 USD could not carry. A dataset run's account locks no
/// margin, so nothing is denied: each decision settles as the hand-worked
/// targets do, each side's unit its stop's worth on 10,000 oz, 30,000 USD.
#[test]
fn a_dataset_run_locks_no_margin_so_a_size_beyond_its_balance_settles() {
    let (from, to) = (ic::at("08:00:00"), ic::at("13:00:00"));
    let catalog = ic::catalog_of(hand_worked_quotes(), from, to);
    let source = Source::Quotes(InstrumentId::from(ic::INSTRUMENT_ID));
    let backtest = Backtest {
        balance: Money::from("100000 USD"),
        ..gold_backtest(catalog.path().to_path_buf())
    };
    let (probe, record) = TargetProbe::new(
        source,
        bracket(
            ic::INSTRUMENT_ID,
            "50%".parse().unwrap(),
            Decimal::new(75, 1),
        ),
        &backtest,
    );

    backtest.run_dataset(probe).unwrap();

    let record = record.borrow();
    assert_eq!(record.denial, None);
    let units: Vec<(UnixNanos, Money, Money)> = record
        .rows
        .iter()
        .map(|row| {
            (
                row.settled_at,
                row.outcomes.long.unit,
                row.outcomes.short.unit,
            )
        })
        .collect();
    let unit = Money::from("30000 USD");
    assert_eq!(
        units,
        [
            (UnixNanos::from(ic::at("09:20:00")), unit, unit),
            (UnixNanos::from(ic::at("10:45:00.010")), unit, unit),
            (UnixNanos::from(ic::at("11:45:00.010")), unit, unit),
        ]
    );
}

/// A commission of 4M USD an order on the 10M USD balance: the 09:00
/// decision's four fills, its entries and exits, cost 16M USD, more than
/// the balance, which an account of the run's P&L would leave at -6M USD,
/// below any margin. A dataset run's account is frozen, its balance whole
/// whatever its trades lose, so every later decision opens and settles as
/// the hand-worked targets do.
#[test]
fn a_dataset_runs_account_keeps_its_balance_whatever_its_trades_lose() {
    let (from, to) = (ic::at("08:00:00"), ic::at("13:00:00"));
    let catalog = ic::catalog_of(hand_worked_quotes(), from, to);
    let source = Source::Quotes(InstrumentId::from(ic::INSTRUMENT_ID));
    let backtest = Backtest {
        commission: Commission::PerOrder(Money::from("4000000 USD")),
        ..gold_backtest(catalog.path().to_path_buf())
    };
    let (probe, record) = TargetProbe::new(
        source,
        bracket(ic::INSTRUMENT_ID, risking("4"), Decimal::new(75, 1)),
        &backtest,
    );

    backtest.run_dataset(probe).unwrap();

    let record = record.borrow();
    assert_eq!(record.denial, None);
    let settled_at: Vec<UnixNanos> = record.rows.iter().map(|row| row.settled_at).collect();
    assert_eq!(
        settled_at,
        ["09:20:00", "10:45:00.010", "11:45:00.010"].map(|t| UnixNanos::from(ic::at(t)))
    );
}

/// IC's gold defined with a minimum of 5 oz: the hand-worked targets'
/// one ounce is below it, and Nautilus' risk engine, its checks kept in a
/// dataset run, denies the first entry, which fails the run with its reason
/// and stops it there, at the 09:00 decision, rather than leave the
/// decision without its row: no row, and no later decision.
#[test]
fn a_size_below_the_instruments_minimum_fails_the_dataset_run_and_stops_it() {
    let InstrumentAny::CurrencyPair(mut gold) = ic::instrument() else {
        unreachable!("IC's gold is a currency pair")
    };
    gold.min_quantity = Some(Quantity::from(5));
    let (from, to) = (ic::at("08:00:00"), ic::at("13:00:00"));
    let catalog = ic::catalog_defining(
        InstrumentAny::CurrencyPair(gold),
        hand_worked_quotes(),
        from,
        to,
    );
    let source = Source::Quotes(InstrumentId::from(ic::INSTRUMENT_ID));
    let backtest = gold_backtest(catalog.path().to_path_buf());
    let (probe, record) = TargetProbe::new(
        source,
        bracket(ic::INSTRUMENT_ID, risking("4"), Decimal::new(75, 1)),
        &backtest,
    );

    backtest.run_dataset(probe).unwrap();

    let record = record.borrow();
    let err = record.denial.clone().unwrap_or_default();
    assert!(
        err.contains("D0-L-E") && err.contains("QUANTITY_BELOW_MINIMUM"),
        "{err}"
    );
    assert_eq!(record.decisions, 1);
    assert_eq!(record.rows, []);
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
        commission: Commission::Rate(Decimal::new(5, 4)),
        balance: Money::from("10000000 USDT"),
        ..gold_backtest(PathBuf::new())
    }
}

/// One contract (0.01 BTC), exits 10 bps, 100, from the close of
/// 100,000, on minute bars alone: the venue fills a market order at the
/// last close, a take profit at its limit and a stop at its trigger, and
/// replays each bar's open, high, low and close in that order. Each stop,
/// 100 away on 0.01 BTC, is worth 1 USDT. Each fill pays OKX's 0.05% of its
/// value: 0.5 USDT at 100,000, 0.5005 at 100,100, 0.4995 at 99,900. With no
/// quote, both sides enter at the close, no spread: the long's take profit
/// would net 1 − 0.5 − 0.5005 and its stop lose 1 + 0.5 + 0.4995, the
/// short's take profit net 1 − 0.5 − 0.4995 and its stop lose 1 + 0.5 +
/// 0.5005, each entry costing its fee alone, 0.5, all over 1 USDT: the
/// payoffs the bars realise.
#[test]
fn targets_on_minute_bars_are_the_outcomes_worked_out_by_hand() {
    let catalog = btc_catalog(btc_bar);
    let source = Source::Bars(BarType::from(BTC_MINUTE));
    let backtest = btc_backtest(&catalog);
    let (probe, record) = TargetProbe::new(
        source,
        bracket(BTC, risking("1"), Decimal::from(10)),
        &backtest,
    );

    backtest.run_dataset(probe).unwrap();

    let at = |hh_mm: &str| ic::at(&format!("{hh_mm}:00"));
    let flat = || {
        outcomes(
            outcome("-1 USDT", "1 USDT", Exit::Time),
            outcome("-1 USDT", "1 USDT", Exit::Time),
        )
    };
    let expected = [
        // The bar touching both exits: its high comes first, so the
        // long's take profit fills at 100,100 (+1 less 1.0005 of fees, a
        // loss) and the short's stop at its trigger, 100,100 (-1 - 1.0005).
        // It opens at 09:10 and is replayed at its close.
        row(
            decision(at("09:00"), 100_000.0),
            of_unit(
                outcome("-0.0005 USDT", "1.0005 USDT", Exit::TakeProfit),
                outcome("-2.0005 USDT", "1.0005 USDT", Exit::Stop),
                1.0,
            ),
            ic::at("09:11:00"),
        ),
        // The fall: the long's stop at its trigger, 99,900 (-1 - 0.9995),
        // the short's take profit at its limit (+1 - 0.9995). The bar opens
        // at 10:20 and is replayed at its close.
        row(
            decision(at("10:00"), 100_000.0),
            of_unit(
                outcome("-1.9995 USDT", "0.9995 USDT", Exit::Stop),
                outcome("0.0005 USDT", "0.9995 USDT", Exit::TakeProfit),
                1.0,
            ),
            ic::at("10:21:00"),
        ),
        row(
            decision(at("11:00"), 100_000.0),
            of_unit(flat().long, flat().short, 1.0),
            ic::at("11:45:00.010"),
        ),
    ];
    let record = record.borrow();
    assert_eq!(settled(&record.rows), expected);
    assert_payoff(
        &record.rows,
        &[-0.0005, 1.9995, 0.5],
        &[0.0005, 2.0005, 0.5],
    );
    // A take profit can lose net of fees: its sign is the P&L's.
    let outcomes = record.rows[0].outcomes;
    assert_eq!((outcomes.long.sign(), outcomes.short.sign()), (-1, -1));
    assert_eq!(record.rows[1].outcomes.short.sign(), 1);
}

/// One contract, no exits, on the same bars: each side enters at the last
/// close, 100,000, and closes 30 minutes after its send at the last close,
/// 100,000 still, the bars touching 100,150 and 99,850 passing by it: its
/// two fees of 0.5 USDT lost. Its unit is the worth of the ATR of 2 of
/// Nautilus' hourly bars built from the minute bars,
/// `BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL`: the hour
/// closing 09:00, from the run's start, ranges nought; the one closing 10:00,
/// from 99,850 to 100,150 (09:10), 300, an ATR of 150, Wilder's seeded by the
/// mean of the two; the one closing 11:00, from 99,850 (10:20) to 100,000,
/// 150, an ATR of (150 + 150) / 2 = 150; on 0.01 BTC, the contract's
/// multiplier, 1.5 USDT. The ATR has one bar at 09:00: that decision opens
/// no target and takes no number.
#[test]
fn time_only_targets_on_minute_bars_are_the_outcomes_worked_out_by_hand() {
    let catalog = btc_catalog(btc_bar);
    let source = Source::Bars(BarType::from(BTC_MINUTE));
    let backtest = btc_backtest(&catalog);
    let (probe, record) = TargetProbe::new(source, time_only(BTC, risking("1.5")), &backtest);

    backtest.run_dataset(probe).unwrap();

    let at = |hh_mm: &str| ic::at(&format!("{hh_mm}:00"));
    let flat = || {
        of_unit(
            outcome("-1 USDT", "1 USDT", Exit::Time),
            outcome("-1 USDT", "1 USDT", Exit::Time),
            1.5,
        )
    };
    let expected = [
        row(
            decision(at("10:00"), 100_000.0),
            flat(),
            ic::at("10:30:00.010"),
        ),
        row(
            decision(at("11:00"), 100_000.0),
            flat(),
            ic::at("11:30:00.010"),
        ),
    ];
    let record = record.borrow();
    assert_eq!(settled(&record.rows), expected);
    // On bars, no spread: each entry costs its fee alone, over the unit.
    assert_payoff(&record.rows, &[0.5 / 1.5], &[0.5 / 1.5]);
    assert_eq!(
        record.fills.first(),
        Some(&UnixNanos::from(ic::at("10:00:00.010")))
    );
}

// --------------------------------------------------------------- inverse

const INVERSE: &str = "BTCUSD.BYBIT";
const INVERSE_MINUTE: &str = "BTCUSD.BYBIT-1-MINUTE-LAST-EXTERNAL";

/// Minute bars of Bybit's inverse BTCUSD over `[08:00, 13:00)`, flat at 100,000
/// and of 100,000 contracts, so that the venue fills 1000 at the close, in a
/// catalog with the instrument; and a run over `[08:30, 12:30]` of them alone,
/// on a hedging venue, 0.05% of each fill, 10 BTC.
fn inverse_run() -> (tempfile::TempDir, Backtest) {
    let (from, to) = (ic::at("08:00:00"), ic::at("13:00:00"));
    let mut bars = vec![];
    let mut open = from;
    while open < to {
        let flat = btc_bar(from);
        bars.push(Bar {
            bar_type: BarType::from(INVERSE_MINUTE),
            volume: Quantity::from(100_000),
            ts_event: UnixNanos::from(open),
            ts_init: UnixNanos::from(open + SignedDuration::from_mins(1)),
            ..flat
        });
        open += SignedDuration::from_mins(1);
    }
    let dir = tempfile::tempdir().unwrap();
    let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    catalog
        .write_instruments(vec![InstrumentAny::CryptoPerpetual(btcusd_bybit())])
        .unwrap();
    catalog
        .write_to_parquet(&bars, Some(from.into()), Some(to.into()), None)
        .unwrap();
    let backtest = Backtest {
        catalog: dir.path().to_path_buf(),
        bar_types: vec![BarType::from(INVERSE_MINUTE)],
        ticks: vec![],
        commission: Commission::Rate(Decimal::new(5, 4)),
        balance: Money::from("10 BTC"),
        ..gold_backtest(PathBuf::new())
    };
    (dir, backtest)
}

/// Refused as the targets start, the run ends with the refusal before any
/// decision, where an error at a decision would only be logged by Nautilus and
/// leave an empty dataset. A time-only target is refused as a bracket is: its
/// unit, its ATR's worth, is no multiple of a distance in price on an inverse
/// instrument, as a stop's is not.
#[test]
fn a_target_on_an_inverse_instrument_fails_the_dataset_run() {
    for target in [
        bracket(INVERSE, risking("1"), Decimal::from(10)),
        time_only(INVERSE, risking("1")),
    ] {
        let spec = target.spec;
        // One node per thread.
        let (err, decisions) = thread::spawn(move || {
            let (_catalog, backtest) = inverse_run();
            let source = Source::Bars(BarType::from(INVERSE_MINUTE));
            let (probe, record) = TargetProbe::new(source, target, &backtest);
            let err = backtest.run_dataset(probe).unwrap_err();
            (format!("{err:#}"), record.borrow().decisions)
        })
        .join()
        .unwrap();

        assert!(err.contains("inverse"), "{spec:?}: {err}");
        assert_eq!(decisions, 0, "{spec:?}");
    }
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
/// an ATR of 0.40, the exits 0.80 and 0.60 from 4000.10, each stop worth 0.60
/// USD on one ounce. The ATR has one bar at 09:00: that decision opens no
/// target and takes no number. Each fill pays 30 USD per million of its value,
/// 0.12 USD at these prices.
#[test]
fn targets_an_atr_of_cadence_bars_away_are_the_outcomes_worked_out_by_hand() {
    let (from, to) = (ic::at("08:00:00"), ic::at("13:00:00"));
    let catalog = ic::catalog_of(ranging_quotes(), from, to);
    let backtest = Backtest {
        end: ic::at("10:50:00"),
        ..gold_backtest(catalog.path().to_path_buf())
    };
    let spec = Target {
        bracket: Some(BracketSpec {
            take_profit: Distance::Atr(Decimal::from(2)),
            stop: Distance::Atr(Decimal::new(15, 1)),
            atr: Some(AtrArgs {
                period: 2,
                ..DEFAULT_ATR
            }),
        }),
        limit: SignedDuration::from_mins(45),
        atr: None,
    };
    let config = trading(spec, ic::INSTRUMENT_ID, risking("1"));
    let source = Source::Quotes(InstrumentId::from(ic::INSTRUMENT_ID));
    let (probe, record) = TargetProbe::new(source, config, &backtest);

    backtest.run_dataset(probe).unwrap();

    let expected = [
        // The long enters at 4000.20, its take profit a sell limit at
        // 4000.90, its stop at 3999.50; the short at 4000.00, its take
        // profit at 3999.30, its stop a buy stop at 4000.70. The rise at
        // 10:20 fills the long's take profit at its limit (+0.70 - 0.24)
        // and the short's stop at the ask, 4001.10 (-1.10 - 0.24), on that
        // quote, which settles the row.
        row(
            decision(ic::at("10:00:00"), 4000.10),
            of_unit(
                outcome("0.46 USD", "0.24 USD", Exit::TakeProfit),
                outcome("-1.34 USD", "0.24 USD", Exit::Stop),
                0.6,
            ),
            ic::at("10:20:00"),
        ),
    ];
    let record = record.borrow();
    assert_eq!(settled(&record.rows), expected);
    // The 09:00 decision sent no order: the first fill is the 10:00 one's,
    // the first decision numbered.
    assert_eq!(
        record.fills.first(),
        Some(&UnixNanos::from(ic::at("10:00:00.010")))
    );
    assert!(record.filled[0].starts_with("D0-"), "{:?}", record.filled);
}

/// The ATR of `targets_an_atr_of_cadence_bars_away_are_the_outcomes_worked_out_by_hand`,
/// 0.40 at 10:00, with a stop of a hundredth of it, 0.004 from 4000.10:
/// on gold's cent ticks the stop rounds onto the decision's price, no
/// distance, so the decision opens no target, as one on a cold ATR: no
/// row and no order.
#[test]
fn a_stop_rounding_onto_the_decisions_price_opens_no_target() {
    let (from, to) = (ic::at("08:00:00"), ic::at("13:00:00"));
    let catalog = ic::catalog_of(ranging_quotes(), from, to);
    let backtest = Backtest {
        end: ic::at("10:50:00"),
        ..gold_backtest(catalog.path().to_path_buf())
    };
    let spec = Target {
        bracket: Some(BracketSpec {
            take_profit: Distance::Atr(Decimal::from(2)),
            stop: Distance::Atr(Decimal::new(1, 2)),
            atr: Some(AtrArgs {
                period: 2,
                ..DEFAULT_ATR
            }),
        }),
        limit: SignedDuration::from_mins(45),
        atr: None,
    };
    let config = trading(spec, ic::INSTRUMENT_ID, risking("1"));
    let source = Source::Quotes(InstrumentId::from(ic::INSTRUMENT_ID));
    let (probe, record) = TargetProbe::new(source, config, &backtest);

    backtest.run_dataset(probe).unwrap();

    let record = record.borrow();
    assert_eq!(record.rows, []);
    assert!(record.fills.is_empty(), "{:?}", record.fills);
}

/// The bracket of the ATR tests on BTC: a take profit 2 and a stop 1.5
/// average true ranges of 2 hourly bars away, closed 45 minutes after its
/// decision, of one contract.
fn btc_atr_targets() -> TargetConfig {
    let spec = Target {
        bracket: Some(BracketSpec {
            take_profit: Distance::Atr(Decimal::from(2)),
            stop: Distance::Atr(Decimal::new(15, 1)),
            atr: Some(AtrArgs {
                period: 2,
                ..DEFAULT_ATR
            }),
        }),
        limit: SignedDuration::from_mins(45),
        atr: None,
    };
    trading(spec, BTC, risking("0.825"))
}

/// A trade of one contract of BTC-USDT-SWAP at `time` and `price`, of no
/// aggressor: the venue's bid and ask both move to it.
fn btc_trade(time: Timestamp, price: f64) -> TradeTick {
    TradeTick::new(
        InstrumentId::from(BTC),
        Price::new(price, 1),
        Quantity::from("1.00"),
        AggressorSide::NoAggressor,
        TradeId::from(time.as_millisecond().to_string().as_str()),
        time.into(),
        time.into(),
    )
}

/// BTC-USDT-SWAP trading every 5 s over `[08:00, 13:00)` at 100,000, but
/// for single trades ranging in the hours before 10:00: to 09:00 between
/// 99,990 (08:40) and 100,020 (08:50), to 10:00 between 99,970 (09:20)
/// and 100,050 (09:40); and a rise to 100,120 at 10:20. In a catalog
/// with the instrument.
fn btc_trades_catalog() -> tempfile::TempDir {
    let (from, to) = (ic::at("08:00:00"), ic::at("13:00:00"));
    let mut trades = vec![];
    let mut time = from;
    while time < to {
        let price = match time {
            t if t == ic::at("08:40:00") => 99_990.0,
            t if t == ic::at("08:50:00") => 100_020.0,
            t if t == ic::at("09:20:00") => 99_970.0,
            t if t == ic::at("09:40:00") => 100_050.0,
            t if t == ic::at("10:20:00") => 100_120.0,
            _ => 100_000.0,
        };
        trades.push(btc_trade(time, price));
        time += SignedDuration::from_secs(5);
    }
    let dir = tempfile::tempdir().unwrap();
    let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    catalog.write_instruments(vec![btc()]).unwrap();
    catalog
        .write_to_parquet(&trades, Some(from.into()), Some(to.into()), None)
        .unwrap();
    dir
}

/// On BTC's trades alone, no quote, until 10:50: the ATR's bars are the
/// last prices of the trades at the cadence,
/// `BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL`. The bar closing 09:00, from
/// the run's start, ranges 30; the one closing 10:00 from 99,970 to
/// 100,050 past the close before, 100,000, 80: an ATR of 55, Wilder's
/// average of 30 and 80, the exits 110 and 82.5 from the last trade,
/// 100,000, each stop worth 0.825 USDT on 0.01 BTC. The ATR has one bar at
/// 09:00: that decision opens no target. Each fill pays OKX's 0.05% of its
/// value, 0.5 USDT at 100,000.
#[test]
fn targets_an_atr_of_cadence_bars_of_trades_away_are_the_outcomes_worked_out_by_hand() {
    let catalog = btc_trades_catalog();
    let backtest = Backtest {
        bar_types: vec![],
        ticks: vec![TickSeries {
            data: NautilusDataType::TradeTick,
            instrument_id: InstrumentId::from(BTC),
        }],
        end: ic::at("10:50:00"),
        ..btc_backtest(&catalog)
    };
    let source = Source::Trades(InstrumentId::from(BTC));
    let (probe, record) = TargetProbe::new(source, btc_atr_targets(), &backtest);

    backtest.run_dataset(probe).unwrap();

    let record = record.borrow();
    assert_eq!(settled(&record.rows), btc_trades_atr_rows());
    // With no quote both sides enter at the last trade, 100,000, no spread:
    // the long's take profit, 110 away, would net 1.10 − 0.5 − 0.50055, as it
    // did, and its stop, 82.5 away, lose 0.825 + 0.5 + 0.4995875; the short's
    // take profit would net 1.10 − 0.5 − 0.49945 and its stop lose 0.825 + 0.5
    // + 0.5004125; each entry costs its fee alone, 0.5; all over 0.825 USDT.
    assert_payoff(
        &record.rows,
        &[0.09945 / 0.825, 1.8245875 / 0.825, 0.5 / 0.825],
        &[0.10055 / 0.825, 1.8254125 / 0.825, 0.5 / 0.825],
    );
    // The 09:00 decision sent no order: the first fill is the 10:00 one's.
    assert_eq!(
        record.fills.first(),
        Some(&UnixNanos::from(ic::at("10:00:00.010")))
    );
}

/// The probe deciding on the close of BTC's hourly LAST bars, which the
/// run builds from its trades alone (the strategy reads no trade): each
/// bar closes on the hour's last trade, 100,000 at 09:59:55, so the
/// decisions, their ATR and their targets are those of the probe deciding
/// on the trades themselves.
#[test]
fn targets_on_last_bars_of_trades_are_those_on_the_trades() {
    let catalog = btc_trades_catalog();
    let hour_last = BarType::from("BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL");
    let backtest = Backtest {
        bar_types: vec![hour_last],
        ticks: vec![],
        end: ic::at("10:50:00"),
        ..btc_backtest(&catalog)
    };
    let (probe, record) = TargetProbe::new(Source::Bars(hour_last), btc_atr_targets(), &backtest);

    backtest.run_dataset(probe).unwrap();

    assert_eq!(settled(&record.borrow().rows), btc_trades_atr_rows());
}

/// The rows of the probe on BTC's trades, an ATR of 55 at 10:00.
fn btc_trades_atr_rows() -> [Settled; 1] {
    [
        // Both enter at the last trade, 100,000: the long's take profit a
        // sell limit at 100,110, its stop at 99,917.5; the short's take
        // profit at 99,890, its stop a buy stop at 100,082.5. The trade at
        // 100,120 at 10:20 fills the long's take profit at its limit
        // (+1.10 - 0.5 - 0.50055) and the short's stop at the trade,
        // 100,120 (-1.20 - 0.5 - 0.5006), which settles the row.
        row(
            decision(ic::at("10:00:00"), 100_000.0),
            of_unit(
                outcome("0.09945 USDT", "1.00055 USDT", Exit::TakeProfit),
                outcome("-2.2006 USDT", "1.0006 USDT", Exit::Stop),
                0.825,
            ),
            ic::at("10:20:00"),
        ),
    ]
}

/// BTC's minute bars as `btc_bar`, flat at 100,000, but for the minutes
/// ranging in the hours before 10:00: the one opening 08:40 from 99,990
/// to 100,020, the one opening 09:20 down to 99,970 and the hour's last,
/// opening 09:59 and stamped at 10:00, up to 100,050; and the one opening
/// 10:20 rising to 100,120.
fn ranging_btc_bar(open: Timestamp) -> Bar {
    let (high, low) = match open {
        t if t == ic::at("08:40:00") => (100_020.0, 99_990.0),
        t if t == ic::at("09:20:00") => (100_000.0, 99_970.0),
        t if t == ic::at("09:59:00") => (100_050.0, 100_000.0),
        t if t == ic::at("10:20:00") => (100_120.0, 100_000.0),
        _ => (100_000.0, 100_000.0),
    };
    Bar {
        high: Price::new(high, 1),
        low: Price::new(low, 1),
        ..btc_bar(open)
    }
}

/// On BTC's minute bars alone, until 10:50: the ATR's bars are Nautilus'
/// composite bars of the cadence built from them,
/// `BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL`; the probe
/// reads the minute bars alone. The hour closing 09:00, from the run's
/// start, ranges 30; the one closing 10:00 from 99,970 to 100,050 past
/// the close before, 100,000, 80, its high in the minute replayed at
/// 10:00, before the timer closing the hour: an ATR of 55, the exits 110
/// and 82.5 from the last close, 100,000, each stop worth 0.825 USDT on 0.01
/// BTC. The ATR has one bar at 09:00: that decision opens no target. Each fill
/// pays OKX's 0.05% of its value, 0.5 USDT at 100,000.
#[test]
fn targets_an_atr_of_cadence_bars_of_source_bars_away_are_the_outcomes_worked_out_by_hand() {
    let catalog = btc_catalog(ranging_btc_bar);
    let backtest = Backtest {
        end: ic::at("10:50:00"),
        ..btc_backtest(&catalog)
    };
    let source = Source::Bars(BarType::from(BTC_MINUTE));
    let (probe, record) = TargetProbe::new(source, btc_atr_targets(), &backtest);

    backtest.run_dataset(probe).unwrap();

    let expected = [
        // Both enter at the last close, 100,000, the exits as on trades.
        // The minute opening 10:20 replays its high, 100,120, first: the
        // long's take profit fills at its limit, 100,110 (+1.10 - 0.5 -
        // 0.50055), the short's stop at its trigger, 100,082.5 (-0.825 -
        // 0.5 - 0.5004125), the minute replayed at its close, 10:21.
        row(
            decision(ic::at("10:00:00"), 100_000.0),
            of_unit(
                outcome("0.09945 USDT", "1.00055 USDT", Exit::TakeProfit),
                outcome("-1.8254125 USDT", "1.0004125 USDT", Exit::Stop),
                0.825,
            ),
            ic::at("10:21:00"),
        ),
    ];
    let record = record.borrow();
    assert_eq!(settled(&record.rows), expected);
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
        let spec = Target::builder()
            .bracket(BracketSpec {
                take_profit: Distance::BasisPoints(Decimal::new(75, 1)),
                stop: Distance::BasisPoints(Decimal::new(75, 1)),
                atr: None,
            })
            .limit(SignedDuration::from_hours(2))
            .build();
        let config = trading(spec, ic::INSTRUMENT_ID, risking("4"));
        let source = Source::Quotes(InstrumentId::from(ic::INSTRUMENT_ID));
        let (probe, record) =
            TargetProbe::every(source, config, SignedDuration::from_mins(15), &backtest);
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
    // settles, none denied on 10M USD; a later one settles if its exits
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
    // Unpurged, the cache holds each closed order, three or more on each
    // side of a decision: its entry and its two exits, filled or cancelled,
    // and any close at its time; purged, only those closed in the last
    // minutes.
    assert!(closed_kept >= 3 * 2 * due, "{closed_kept} kept");
    assert!(closed_purged < 50, "{closed_purged} left after purging");
}
