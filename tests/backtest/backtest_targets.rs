//! Tests targets from orders (`model::targets`).
//!
//! A probe decides every hour on one feature. It opens its target on each side, as a bracket or a
//! time-only trade, on a hedging venue through `Backtest::run_dataset`, and records the target rows
//! published. The runs use quotes, trades and minute bars. Exits and paths are chosen so that each
//! target is worked out by hand.
//!
//! - The probe subscribes to the ATR's hourly bars as a model handle does
//!   (`Targets::atr_bar_type`, `strategy::subscriptions`).
//! - Deciding on LAST bars built from the trades alone is deciding on the trades.
//! - A denied order fails the run with its reason.
//! - A dataset run settles the rows a backtest does, net of the run's commission.

use std::{cell::RefCell, collections::BTreeSet, path::PathBuf, rc::Rc, thread};

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
    strategy::{
        AtrArgs, Declared, Series, orders,
        requirements::{DataRequirements, TickSeries},
        risk::Risk,
        subscriptions::Subscriptions,
        warmup::Warmup,
    },
};
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{
        DataActor,
        indicators::{ActorIndicator, SharedActorIndicator},
    },
    logging::logger::LoggerConfig,
    msgbus::{self, ShareableMessageHandler, switchboard::get_custom_topic},
    timer::TimeEvent,
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{Bar, BarType, CustomData, NautilusDataType, QuoteTick, TradeTick},
    enums::AggressorSide,
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
use rstest::rstest;
use rust_decimal::Decimal;

use crate::common::{
    features::{SmaArgs, tables},
    ic,
};

const STRATEGY: &str = "TARGETS-001";

const MID: Declared<SmaArgs> = Declared::new("mid");
const LAST: Declared<SmaArgs> = Declared::new("last");
const CLOSE: Declared<SmaArgs> = Declared::new("close");

#[derive(Debug, Clone, Copy)]
enum Source {
    Quotes(InstrumentId),
    Trades(InstrumentId),
    Bars(BarType),
}

impl Source {
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

#[derive(Debug, Default)]
struct Record {
    rows: Vec<TargetRow>,
    closed_orders: usize,
    decisions: usize,
    fills: Vec<UnixNanos>,
    filled: Vec<String>,
    commissions: Vec<Money>,
    rejected: Vec<String>,
    /// Records the denial that failed the run (`Targets::on_order_denied`).
    denial: Option<String>,
}

#[derive(Debug)]
struct TargetProbe {
    core: StrategyCore,
    source: Source,
    every: SignedDuration,
    features: Rc<FeatureSet>,
    targets: Rc<Targets>,
    warmup: Warmup,
    settled: SettledRows,
    record: Rc<RefCell<Record>>,
}

impl TargetProbe {
    fn new(
        source: Source,
        targets: TargetConfig,
        backtest: &Backtest,
    ) -> (Self, Rc<RefCell<Record>>) {
        Self::every(source, targets, SignedDuration::from_hours(1), backtest)
    }

    /// Builds the probe that decides every `every`, with its ATR, if any, on bars of `every`.
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
            Source::Quotes(id) => {
                FeatureSet::builder().feature(&[Series::quotes(id)], MID, &tables)
            }
            Source::Trades(id) => {
                FeatureSet::builder().feature(&[Series::trades(id)], LAST, &tables)
            }
            Source::Bars(bar_type) => {
                FeatureSet::builder().feature(&[Series::Bars(bar_type)], CLOSE, &tables)
            }
        }
        .build()
        .unwrap();
        let record = Rc::new(RefCell::new(Record::default()));
        let targets = Targets::new(
            targets,
            DecisionRule::Every(every),
            &source.requirements(),
            &DatasetVenue {
                commission: backtest.commission,
                balance: backtest.balance,
                latency: backtest.latency,
            },
        )
        .unwrap();
        let warmup = targets.warmed(Warmup::replayed(&source.requirements()).feeding(
            &features.bar_types(),
            Rc::clone(&features) as SharedActorIndicator,
        ));
        let probe = Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from(STRATEGY)),
                order_id_tag: Some("001".to_string()),
                ..Default::default()
            }),
            source,
            every,
            features,
            targets,
            warmup,
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
        let now = self.clock().utc_now();
        Subscriptions::new(
            self.targets.atr_bar_type().into_iter().collect(),
            vec![],
            vec![],
        )
        .start(self, now);
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

    /// Hands quotes and trades to the features, and bars to the warmup.
    ///
    /// The features and the targets' ATR are registered with Nautilus for nothing.
    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        self.features.handle_quote(quote)
    }

    fn on_trade(&mut self, trade: &TradeTick) -> anyhow::Result<()> {
        self.features.handle_trade(trade)
    }

    fn on_bar(&mut self, bar: &Bar) -> anyhow::Result<()> {
        self.warmup.on_bar(bar)
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

/// Returns the risk at which the reference stake is `money` of the runs' balance of 10M.
///
/// A target whose one R is worth `money` on one unit, and less on two, trades one unit.
fn risking(money: &str) -> Risk {
    let fraction = Decimal::from_str_exact(money).unwrap() / Decimal::from(10_000_000);
    format!("{}%", (fraction * Decimal::ONE_HUNDRED).normalize())
        .parse()
        .unwrap()
}

fn trading(spec: Target, instrument_id: &str, risk: Risk) -> TargetConfig {
    TargetConfig::builder()
        .instrument_id(InstrumentId::from(instrument_id))
        .risk(risk)
        .spec(spec)
        .build()
}

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

/// Builds the decision at `at`, which fires 10 ms after.
fn decision(at: Timestamp, features: f64) -> FeatureRow {
    let time = UnixNanos::from(at + SignedDuration::from_millis(10));
    FeatureRow {
        strategy_id: StrategyId::from(STRATEGY),
        features: vec![features],
        ts_event: time,
        ts_init: time,
    }
}

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

/// A target row as the tests compare it, to 1e-12 (`assert_payoff`).
///
/// The payoff features are kept apart.
type Settled = (FeatureRow, Outcomes, UnixNanos);

fn row(decision: FeatureRow, outcomes: Outcomes, settled_at: Timestamp) -> Settled {
    (decision, outcomes, UnixNanos::from(settled_at))
}

fn settled(rows: &[TargetRow]) -> Vec<Settled> {
    rows.iter()
        .map(|row| (row.decision.clone(), row.outcomes, row.settled_at))
        .collect()
}

/// Asserts that each row has the payoff features `long` and `short`, each within 1e-12.
///
/// - A bracket has the take profit's R, the stop's nominal loss and the entry cost.
/// - A time-only target has the entry cost.
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
        latency: None,
        pairs: vec![],
        balance: Money::from("10000000 USD"),
        logging: LoggerConfig::default(),
    }
}

/// Returns quotes with a dip at 09:20, a spike at 10:45:00.010 and a rise of 0.90 from 11:20.
///
/// The spike is the time limit of the 10:00 decision.
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

/// The probe trades one ounce with exits 7.5 bps (3.000075) from the mid of 4000.10, on the
/// nearest cent.
///
/// - A long enters at the ask, 4000.20. Its take profit is a sell limit at 4003.10 and its stop is
///   a sell stop at 3997.10.
/// - A short enters at the bid, 4000.00, and exits on the ask.
/// - Each stop is 3.00 away and worth 3 USD. Each fill costs 0.12 (30 USD per million).
///
/// On 4000.00/4000.20, either side's take profit nets 2.90 - 0.24, its stop loses 3.10 + 0.24 and
/// its entry costs 0.20 + 0.12. All are over 3 USD: the payoff features.
#[rstest]
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
        // The dip at 09:20 is 3996.80/3997.00, so the long's stop fills at the bid, 3996.80
        // (-3.40 - 0.24), and the short's take profit fills at its limit, 3997.10 (+2.90 - 0.24),
        // which settles the row.
        row(
            decision(at("09:00"), 4000.10),
            of_unit(
                outcome("-3.64 USD", "0.24 USD", Exit::Stop),
                outcome("2.66 USD", "0.24 USD", Exit::TakeProfit),
                3.0,
            ),
            ic::at("09:20:00"),
        ),
        // The spike at the time limit's nanosecond is inside the window, so the long's take profit
        // fills at 4003.10 and the short's stop fills at the ask, 4003.40.
        row(
            decision(at("10:00"), 4000.10),
            of_unit(
                outcome("2.66 USD", "0.24 USD", Exit::TakeProfit),
                outcome("-3.64 USD", "0.24 USD", Exit::Stop),
                3.0,
            ),
            ic::at("10:45:00.010"),
        ),
        // No exit is reached, so time closes both on 4001.00/4001.20, long +0.80 and short -1.20,
        // less fees.
        row(
            decision(at("11:00"), 4000.10),
            of_unit(
                outcome("0.56 USD", "0.24 USD", Exit::Time),
                outcome("-1.44 USD", "0.24 USD", Exit::Time),
                3.0,
            ),
            ic::at("11:45:00.010"),
        ),
        // The 12:00 decision's positions are open at the end, 12:30: unsettled, no row
    ];
    let record = record.borrow();
    assert_eq!(settled(&record.rows), expected);
    let side = [2.66 / 3.0, 3.34 / 3.0, 0.32 / 3.0];
    assert_payoff(&record.rows, &side, &side);
    let take_profit = record.rows[1].outcomes.long;
    assert!((take_profit.pnl.as_f64() / take_profit.unit.as_f64() - side[0]).abs() < 1e-12);
}

/// Returns Pepperstone's commission on gold: 3.5 USD per lot of 100 oz, or 0.035 USD an ounce.
fn per_ounce() -> Commission {
    Commission::PerQuantity(Money::from_str_exact("0.035 USD").unwrap())
}

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

/// A fill of an ounce costs 0.035 USD, or 0.04 to the cent, so each side's trade costs 0.08.
///
/// The backtest charges each of its 14 fills that cost. The dataset run settles the same rows net
/// of it:
/// - The long: stopped at -3.48 (-3.40 - 0.08), take profit at 2.82, closed by time at 0.72.
/// - The short: closed by time at -1.28.
///
/// Payoff features: take profit 2.90 - 0.08, stop 3.10 + 0.08, entry 0.20 + 0.04, all over 3 USD.
#[rstest]
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

/// Builds a run to 09:50 in which one decision settles.
///
/// Quotes are 4000.00/4000.01 throughout, so the mid, 4000.005, is on a half tick.
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

/// The probe trades one ounce with exits 7.5 bps (3.00000375) from the mid of 4000.005, on the
/// nearest cent.
///
/// The long's take profit is 4003.01 and its stop is 3997.00. The short's are the reverse. Each
/// stop is 3.005 away and worth 3.005 USD, not the cents around it. No exit is reached, so each
/// side closes by time at -0.01, the spread's cent, less two fees of 0.12.
#[rstest]
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

/// The probe trades one ounce with exits 0.01 bp from the mid of 4000.005.
///
/// Each stop is half a cent from the mid, on the touch, in the market. The venue rejects it and the
/// decision stores no row.
#[rstest]
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

/// The probe trades one ounce with no exits and closes each side 30 minutes after its send.
///
/// Each side enters as a bracket does. Each fill pays 0.12 USD. The unit is the worth of the ATR of
/// 2 hourly bars of the mids. 09:00 ranges 0. 10:00 ranges 3.20, from 3996.90 (09:20) to 4000.10,
/// so the ATR is 1.60, Wilder's seeded by the mean of the two. 11:00 ranges 3.20, from 4000.10 to
/// 4003.30 (10:45), so the ATR is (1.60 + 3.20) / 2 = 2.40.
///
/// The ATR has one bar at 09:00, so that decision opens no target. An entry costs 0.20 + 0.12 USD,
/// over the unit.
#[rstest]
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
        // 4000.20 to 4000.00 and back: 0.20 lost and two fees
        row(
            decision(at("10:00"), 4000.10),
            of_unit(
                outcome("-0.44 USD", "0.24 USD", Exit::Time),
                outcome("-0.44 USD", "0.24 USD", Exit::Time),
                1.6,
            ),
            ic::at("10:30:00.010"),
        ),
        row(
            decision(at("11:00"), 4000.10),
            of_unit(
                outcome("0.56 USD", "0.24 USD", Exit::Time),
                outcome("-1.44 USD", "0.24 USD", Exit::Time),
                2.4,
            ),
            ic::at("11:30:00.010"),
        ),
    ];
    let record = record.borrow();
    assert_eq!(settled(&record.rows), expected);
    for (row, unit) in record.rows.iter().zip([1.6, 2.4]) {
        assert_payoff(std::slice::from_ref(row), &[0.32 / unit], &[0.32 / unit]);
    }
    assert!(record.filled[0].starts_with("D0-"), "{:?}", record.filled);
}

/// Names each order by its decision's number, its side and its leg.
#[rstest]
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

/// Half of 100,000 USD, 50,000, over a stop of 3.00 buys 16,666 oz.
///
/// Nautilus' sizer caps that at gold's largest order, 10,000 oz. Margin at IC's 1 / 30 would be
/// 1.33M USD, beyond the account. A dataset run's account locks no margin, so nothing is denied.
/// Each unit is its stop's worth on 10,000 oz, 30,000 USD.
#[rstest]
fn a_dataset_run_locks_no_margin_so_a_size_beyond_its_balance_settles() {
    let (from, to) = (ic::at("08:00:00"), ic::at("13:00:00"));
    let catalog = ic::catalog_of(hand_worked_quotes(), from, to);
    let source = Source::Quotes(InstrumentId::from(ic::INSTRUMENT_ID));
    let backtest = Backtest {
        pairs: vec![],
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

/// The commission is 4M USD an order, so the 09:00 decision's four fills cost 16M USD, more than
/// the 10M USD balance.
///
/// A dataset run's account is frozen. Its balance stays whole, so every later decision settles as
/// the hand-worked targets do.
#[rstest]
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

/// The minimum is 5 oz, so the one ounce is below it.
///
/// Nautilus' risk engine keeps its checks in a dataset run and denies the first entry. The run
/// fails with the reason at the 09:00 decision, with no row and no later decision.
#[rstest]
fn a_size_below_the_instruments_minimum_fails_the_dataset_run_and_stops_it() {
    let InstrumentAny::Cfd(mut gold) = ic::instrument() else {
        unreachable!("IC's gold is a CFD")
    };
    gold.min_quantity = Some(Quantity::from(5));
    let (from, to) = (ic::at("08:00:00"), ic::at("13:00:00"));
    let catalog = ic::catalog_defining(InstrumentAny::Cfd(gold), hand_worked_quotes(), from, to);
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

const BTC: &str = "BTC-USDT-SWAP.OKX";
const BTC_MINUTE: &str = "BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL";

/// Returns BTC-USDT-SWAP as OKX defines it: price tick 0.1, lot 0.01 contracts, 0.01 BTC a
/// contract, margins of 1.
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

/// Builds the minute bar opening at `open`.
///
/// The bar is stamped as fill-catalog stores OKX's candles: `ts_event` at the open and `ts_init`
/// at the close.
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

fn btc_backtest(catalog: &tempfile::TempDir) -> Backtest {
    Backtest {
        catalog: catalog.path().to_path_buf(),
        bar_types: vec![BarType::from(BTC_MINUTE)],
        ticks: vec![],
        commission: Commission::Rate(Decimal::new(5, 4)),
        pairs: vec![],
        balance: Money::from("10000000 USDT"),
        ..gold_backtest(PathBuf::new())
    }
}

/// The probe trades one contract (0.01 BTC) with exits 10 bps (100) from the close of 100,000, on
/// minute bars alone.
///
/// The venue fills a market order at the last close, a take profit at its limit and a stop at its
/// trigger. It replays each bar's open, high, low and close in that order. Each stop is worth 1
/// USDT. Each fill costs OKX's 0.05%: 0.5 at 100,000, 0.5005 at 100,100 and 0.4995 at 99,900.
///
/// With no quote, both sides enter at the close, with no spread. All are over 1 USDT:
/// - The long's take profit nets 1 - 0.5 - 0.5005, and its stop loses 1 + 0.5 + 0.4995.
/// - The short's take profit nets 1 - 0.5 - 0.4995, and its stop loses 1 + 0.5 + 0.5005.
/// - An entry costs 0.5.
#[rstest]
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
        // The bar touching both exits replays its high first, so the long's take profit fills at
        // 100,100 (+1 - 1.0005 of fees, a loss) and the short's stop fills at its trigger
        // (-1 - 1.0005).
        row(
            decision(at("09:00"), 100_000.0),
            of_unit(
                outcome("-0.0005 USDT", "1.0005 USDT", Exit::TakeProfit),
                outcome("-2.0005 USDT", "1.0005 USDT", Exit::Stop),
                1.0,
            ),
            ic::at("09:11:00"),
        ),
        // The fall fills the long's stop at 99,900 (-1 - 0.9995) and the short's take profit at its
        // limit (+1 - 0.9995).
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

    // A take profit can lose net of fees, so its sign is the P&L's
    let outcomes = record.rows[0].outcomes;
    assert_eq!((outcomes.long.sign(), outcomes.short.sign()), (-1, -1));
    assert_eq!(record.rows[1].outcomes.short.sign(), 1);
}

/// The probe trades one contract with no exits. Each side enters at the last close, 100,000, and
/// closes 30 minutes later at 100,000 still, so it loses two fees of 0.5 USDT.
///
/// The unit is the worth of the ATR of 2 of Nautilus' hourly bars built from the minute bars
/// (`BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL`). 09:00 ranges 0. 10:00 ranges 300,
/// from 99,850 to 100,150 (09:10). 11:00 ranges 150, from 99,850 (10:20) to 100,000. The ATR is
/// 150 at both, and on 0.01 BTC the unit is 1.5 USDT.
#[rstest]
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

    // On bars, no spread, so each entry costs its fee alone, over the unit
    assert_payoff(&record.rows, &[0.5 / 1.5], &[0.5 / 1.5]);
    assert_eq!(
        record.fills.first(),
        Some(&UnixNanos::from(ic::at("10:00:00.010")))
    );
}

const INVERSE: &str = "BTCUSD.BYBIT";
const INVERSE_MINUTE: &str = "BTCUSD.BYBIT-1-MINUTE-LAST-EXTERNAL";

/// Builds flat minute bars of Bybit's inverse BTCUSD, and a run on them alone.
///
/// The bars are 100,000 contracts, so the venue fills 1000 at the close. The run is on a hedging
/// venue, with 0.05% of each fill and 10 BTC.
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
        pairs: vec![],
        balance: Money::from("10 BTC"),
        ..gold_backtest(PathBuf::new())
    };
    (dir, backtest)
}

/// Refuses a target on an inverse instrument as the targets start, before any decision.
///
/// An error at a decision would only be logged by Nautilus and leave an empty dataset. The unit of
/// a time-only target is the worth of its ATR. On an inverse instrument that is no multiple of a
/// distance in price, and neither is a stop's.
#[rstest]
#[case::bracket(bracket(INVERSE, risking("1"), Decimal::from(10)))]
#[case::time_only(time_only(INVERSE, risking("1")))]
fn a_target_on_an_inverse_instrument_fails_the_dataset_run(#[case] target: TargetConfig) {
    let (err, decisions) = thread::spawn(move || {
        let (_catalog, backtest) = inverse_run();
        let source = Source::Bars(BarType::from(INVERSE_MINUTE));
        let (probe, record) = TargetProbe::new(source, target, &backtest);
        let err = backtest.run_dataset(probe).unwrap_err();
        (format!("{err:#}"), record.borrow().decisions)
    })
    .join()
    .unwrap();

    assert!(err.contains(INVERSE), "{err}");
    assert_eq!(decisions, 0);
}

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

/// The probe trades one ounce and decides every hour until 10:50.
///
/// The take profit is 2 average true ranges away and the stop is 1.5. The ATR is of 2 hourly bars
/// of the mids, which the targets subscribe to and the probe does not. 09:00 ranges 0.30. 10:00
/// ranges 0.50, from 3999.90 to 4000.40. The ATR is 0.40, so the exits are 0.80 and 0.60 from
/// 4000.10.
///
/// Each stop is worth 0.60 USD.
#[rstest]
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
            atr: Some(
                AtrArgs {
                    period: 2,
                    ..DEFAULT_ATR
                }
                .into(),
            ),
        }),
        limit: SignedDuration::from_mins(45),
        atr: None,
    };
    let config = trading(spec, ic::INSTRUMENT_ID, risking("1"));
    let source = Source::Quotes(InstrumentId::from(ic::INSTRUMENT_ID));
    let (probe, record) = TargetProbe::new(source, config, &backtest);

    backtest.run_dataset(probe).unwrap();

    let expected = [
        // The rise at 10:20 fills the long's take profit at its limit, 4000.90 (+0.70 - 0.24),
        // and the short's stop at the ask, 4001.10 (-1.10 - 0.24), which settles the row.
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

    // The 09:00 decision sent no order, so the first fill is the first numbered decision's
    assert_eq!(
        record.fills.first(),
        Some(&UnixNanos::from(ic::at("10:00:00.010")))
    );
    assert!(record.filled[0].starts_with("D0-"), "{:?}", record.filled);
}

/// A stop rounding onto the decision's price opens no target: no row, no order.
///
/// The ATR is that of `targets_an_atr_of_cadence_bars_away_are_the_outcomes_worked_out_by_hand`,
/// 0.40 at 10:00. The stop is a hundredth of it, 0.004 from 4000.10. On gold's cent ticks it rounds
/// onto the decision's price.
#[rstest]
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
            atr: Some(
                AtrArgs {
                    period: 2,
                    ..DEFAULT_ATR
                }
                .into(),
            ),
        }),
        limit: SignedDuration::from_mins(45),
        atr: None,
    };
    let config = trading(spec, ic::INSTRUMENT_ID, risking("1"));
    let source = Source::Quotes(InstrumentId::from(ic::INSTRUMENT_ID));
    let (probe, record) = TargetProbe::new(source, config, &backtest);

    backtest.run_dataset(probe).unwrap();

    let record = record.borrow();
    assert_eq!(record.decisions, 2);
    assert_eq!(record.rows, []);
    assert!(record.fills.is_empty(), "{:?}", record.fills);
}

fn btc_atr_targets() -> TargetConfig {
    let spec = Target {
        bracket: Some(BracketSpec {
            take_profit: Distance::Atr(Decimal::from(2)),
            stop: Distance::Atr(Decimal::new(15, 1)),
            atr: Some(
                AtrArgs {
                    period: 2,
                    ..DEFAULT_ATR
                }
                .into(),
            ),
        }),
        limit: SignedDuration::from_mins(45),
        atr: None,
    };
    trading(spec, BTC, risking("0.825"))
}

/// Builds a trade with no aggressor, so the venue's bid and ask both move to the price.
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

/// The run uses BTC's trades alone, with no quote, until 10:50.
///
/// The ATR's bars are the last prices of the trades at the cadence
/// (`BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL`). 09:00 ranges 30. 10:00 ranges 80, from 99,970 to
/// 100,050. The ATR is 55, Wilder's average of 30 and 80.
///
/// The exits are 110 and 82.5 from the last trade. Each stop is worth 0.825 USDT.
#[rstest]
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

    // No quote, so both sides enter at the last trade, 100,000. The long's take profit nets
    // 1.10 - 0.5 - 0.50055 and its stop loses 0.825 + 0.5 + 0.4995875, the short's take profit
    // nets 1.10 - 0.5 - 0.49945 and its stop loses 0.825 + 0.5 + 0.5004125, and an entry costs
    // 0.5, all over 0.825 USDT.
    assert_payoff(
        &record.rows,
        &[0.09945 / 0.825, 1.8245875 / 0.825, 0.5 / 0.825],
        &[0.10055 / 0.825, 1.8254125 / 0.825, 0.5 / 0.825],
    );

    // The 09:00 decision sent no order, so the first fill is the 10:00 one's
    assert_eq!(
        record.fills.first(),
        Some(&UnixNanos::from(ic::at("10:00:00.010")))
    );
}

/// Decides on the close of BTC's hourly LAST bars.
///
/// The run builds those bars from its trades alone. Each bar closes on the hour's last trade, so
/// the decisions, ATR and targets are those of deciding on the trades.
#[rstest]
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

fn btc_trades_atr_rows() -> [Settled; 1] {
    [
        // The trade at 100,120 fills the long's take profit at 100,110 (+1.10 - 0.5 - 0.50055) and
        // the short's stop at 100,120 (-1.20 - 0.5 - 0.5006), which settles the row.
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

/// The run uses BTC's minute bars alone, until 10:50.
///
/// The ATR's bars are Nautilus' composite bars of the cadence built from them
/// (`BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL`). 09:00 ranges 30. 10:00 ranges 80,
/// from 99,970 to 100,050, its high in the minute replayed at 10:00, before the timer closing the
/// hour. The ATR is 55.
///
/// The exits are 110 and 82.5 from the last close.
#[rstest]
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
        // The minute opening 10:20 replays its high first, so the long's take profit fills at its
        // limit, 100,110, and the short's stop fills at its trigger, 100,082.5, at the minute's
        // close, 10:21.
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

#[rstest]
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

    // Every decision settles if its time limit, two hours on, passes before the end, with none
    // denied on 10M USD, and a later one settles if its exits are hit before the end.
    let settled: BTreeSet<UnixNanos> = purged.iter().map(|row| row.decision.ts_event).collect();
    let mut due = 0;
    let mut decision = start + SignedDuration::from_millis(10);
    while decision + SignedDuration::from_hours(2) < end {
        assert!(settled.contains(&decision.into()), "{decision} unsettled");
        due += 1;
        decision += SignedDuration::from_mins(15);
    }
    assert_eq!(purged, kept);

    // Unpurged, the cache holds every closed order, three or more a side of a decision, but
    // purged, it holds only those closed in the last minutes.
    assert!(closed_kept >= 3 * 2 * due, "{closed_kept} kept");
    assert!(closed_purged < 50, "{closed_purged} left after purging");
}
