//! Training at a time (`model::train`) on a fixture catalog with an edge:
//! the dataset probe (`common::dataset`) replays the days the window lacks
//! through `Backtest::run_dataset`, and the fake learner fits each side on
//! the rows. IC's gold rises 4.00 over each even hour and falls back over
//! each odd one (`edge_quotes`), and the probe's one feature, `cheap`,
//! marks where the hour starts: 1 at the bottom, 0 at the top. The fake's
//! prediction is the feature, calibrated increasing for the long, which
//! wins every even hour, and decreasing for the short, which wins every
//! odd one: each side takes the trades it wins, and earns more per trade
//! than taking every trade. One node per thread: a test training twice
//! trains the first time on a thread of its own.

use std::{path::Path, rc::Rc};

use crate::common::{
    dataset::{STRATEGY, replay_on, requirements, rule, targets, venue},
    ic,
};
use helix_trader::model::{
    config::{Margins, ModelConfig},
    dataset,
    features::{Feature, FeatureSet},
    holdout::{Holdout, Refusal},
    learner::LearnerConfig,
    targets::{Label, Side},
    train::{Kept, Outcome, Trained, Training},
};
use jiff::{SignedDuration, Timestamp};
use nautilus_indicators::indicator::Indicator;
use nautilus_model::{
    data::QuoteTick,
    identifiers::{InstrumentId, StrategyId},
};
use tempfile::TempDir;

/// The price of gold at the bottom of each even hour, and the rise over
/// it.
const BOTTOM: f64 = 4000.0;
const RISE: f64 = 4.0;

/// IC's gold's quotes over `[from, to)`, one every 45 s, the ask 0.20
/// above the bid: over each even hour of the day the bid rises from
/// 4000.00 by 0.05 a quote, to 4003.95, and over each odd hour falls from
/// 4004.00 by as much, to 4000.05.
fn edge_quotes(from: Timestamp, to: Timestamp) -> Vec<QuoteTick> {
    let mut quotes = vec![];
    let mut time = from;
    while time < to {
        let seconds = time.as_second();
        let step = (seconds % 3600 / 45) as f64 * 0.05;
        let bid = if seconds / 3600 % 2 == 0 {
            BOTTOM + step
        } else {
            BOTTOM + RISE - step
        };
        quotes.push(ic::quote(
            time,
            &format!("{bid:.2}"),
            &format!("{:.2}", bid + 0.2),
        ));
        time += SignedDuration::from_secs(45);
    }
    quotes
}

/// Where the last mid is between the top of the hours, 4004.10, at 0, and
/// their bottom, 4000.10, at 1, clamped.
#[derive(Default)]
struct Cheap {
    mid: Option<f64>,
}

impl Indicator for Cheap {
    fn name(&self) -> String {
        "Cheap".to_string()
    }

    fn has_inputs(&self) -> bool {
        self.mid.is_some()
    }

    fn initialized(&self) -> bool {
        self.mid.is_some()
    }

    fn handle_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        self.mid = Some((quote.bid_price.as_f64() + quote.ask_price.as_f64()) / 2.0);
        Ok(())
    }

    fn reset(&mut self) {
        self.mid = None;
    }
}

impl Feature for Cheap {
    fn value(&self) -> f64 {
        let mid = self.mid.unwrap_or(f64::NAN);
        ((BOTTOM + RISE + 0.1 - mid) / RISE).clamp(0.0, 1.0)
    }
}

fn features() -> Rc<FeatureSet> {
    FeatureSet::builder()
        .quotes(
            InstrumentId::from(ic::INSTRUMENT_ID),
            "cheap",
            Cheap::default(),
        )
        .build()
        .unwrap()
}

/// The probe's decisions on `cheap`, learning whether each side of the
/// bracket gains, on a window of 46 hours, the last 12 held out and the 12
/// before them calibrated on; each side traded from an expected net P&L of
/// nought, published from `min_pnl` USD per trade over 5 trades.
fn config(min_pnl: f64) -> ModelConfig {
    ModelConfig::builder()
        .decision(rule())
        .targets(targets())
        .label(Label::Barrier)
        .retrain(SignedDuration::from_hours(24))
        .window(SignedDuration::from_hours(46))
        .holdout(SignedDuration::from_hours(12))
        .margins(Margins {
            long: Some(0.0),
            short: Some(0.0),
        })
        .min_pnl(min_pnl)
        .min_trades(5)
        .learner(LearnerConfig::Fake {})
        .build()
}

fn training(catalog: &TempDir, models: &Path, min_pnl: f64) -> Training {
    Training::new(
        catalog.path(),
        models,
        StrategyId::from(STRATEGY),
        &features(),
        config(min_pnl),
        &requirements(),
        &venue(),
    )
    .unwrap()
}

/// Quotes from 23:00 of the 20th of October 2026 to 03:00 of the 23rd.
fn catalog() -> TempDir {
    let (from, to) = (
        ic::at("00:00:00") - SignedDuration::from_hours(1),
        ic::at("00:00:00") + SignedDuration::from_hours(51),
    );
    ic::catalog_of(edge_quotes(from, to), from, to)
}

/// Midnight of the 23rd: the window starts at 02:00 of the 21st.
fn t() -> Timestamp {
    ic::at("00:00:00") + SignedDuration::from_hours(48)
}

fn close(actual: Option<f64>, expected: f64) {
    let actual = actual.unwrap();
    assert!((actual - expected).abs() < 1e-9, "{actual} != {expected}");
}

/// The holdout of a published `outcome`.
fn published(outcome: Option<Outcome>) -> Holdout {
    match outcome {
        Some(Outcome::Published {
            valid_from,
            holdout,
        }) if valid_from == t() => holdout,
        outcome => panic!("{outcome:?}"),
    }
}

/// Worked by hand. A decision at the bottom, mid 4000.10, sets each exit
/// 3.00 away (7.5 bps, on the cent): the long enters at the ask, 4000.20,
/// and its take profit, a sell limit at 4003.10, fills when the bid
/// reaches it, +2.90 less two fees of 0.12, 2.66 USD; the short enters at
/// the bid, 4000.00, and its stop at 4003.10 fills when the ask reaches
/// it, -3.10 less the fees, -3.34. At the top, mid 4004.10, the reverse.
/// Held out, the decisions of 12:00 to 22:00 of the 22nd (that of 23:00
/// settles at the horizon after the train): six at the bottom, five at
/// the top. The long takes the six it wins, 2.66 per trade, every trade
/// (6 x 2.66 - 5 x 3.34) / 11; the short the five it wins, 2.66, every
/// trade (5 x 2.66 - 6 x 3.34) / 11. At 1.5 times the commission of 0.24
/// each trade makes 0.12 less. Every row ranked right: AUC 1.
#[test]
fn train_at_t_publishes_each_side_earning_on_the_edge_whose_probes_reload_to_identical_predictions()
{
    let catalog = catalog();
    let models = tempfile::tempdir().unwrap();
    let training = training(&catalog, models.path(), 1.0);

    let trained = training.train_at(t(), |from, to| replay_on(&catalog, features(), from, to));

    let Trained { long, short, meta } = trained;
    assert_eq!(meta, None);
    for (holdout, taken, every, every_stressed) in [
        (
            published(long),
            6,
            (6.0 * 2.66 - 5.0 * 3.34) / 11.0,
            (6.0 * 2.54 - 5.0 * 3.46) / 11.0,
        ),
        (
            published(short),
            5,
            (5.0 * 2.66 - 6.0 * 3.34) / 11.0,
            (5.0 * 2.54 - 6.0 * 3.46) / 11.0,
        ),
    ] {
        let earnings = holdout.earnings;
        assert_eq!(
            (earnings.taken.count, earnings.every.count),
            (taken, 11),
            "{holdout:?}"
        );
        close(earnings.taken.expectancy, 2.66);
        close(earnings.taken.win_rate, 1.0);
        close(earnings.every.expectancy, every);
        close(earnings.taken_stressed.expectancy, 2.54);
        close(earnings.every_stressed.expectancy, every_stressed);
        assert_eq!(holdout.auc, Some(1.0));
        assert!(holdout.log_loss < holdout.baseline_log_loss, "{holdout:?}");
    }
    let fingerprint =
        dataset::fingerprint(&features(), rule(), &targets(), &requirements(), &venue())
            .unwrap()
            .to_string();
    for side in Side::BOTH {
        let stored = training.store(side).unwrap().read(t()).unwrap();
        let model = stored
            .load(features().names(), &fingerprint)
            .unwrap_or_else(|e| panic!("{side:?}: {e:#}"));
        assert!(!stored.manifest.probes.is_empty());
        assert!(stored.manifest.payoff.is_some(), "{side:?}");
        for probe in &stored.manifest.probes {
            assert_eq!(
                model.predict(&probe.row).unwrap().to_bits(),
                probe.prediction.to_bits(),
                "{probe:?}"
            );
        }
    }
}

/// An hour later the holdout is the decisions of 13:00 to 23:00: each
/// side takes the trades it wins, 2.66 USD each, short of a minimum of 3.
#[test]
fn a_minimum_above_what_the_holdout_earns_keeps_each_current_model() {
    let catalog = catalog();
    let models = tempfile::tempdir().unwrap();
    let first = std::thread::scope(|scope| {
        scope
            .spawn(|| {
                training(&catalog, models.path(), 1.0)
                    .train_at(t(), |from, to| replay_on(&catalog, features(), from, to))
            })
            .join()
            .unwrap()
    });
    assert!(first.published(), "{first:?}");
    // The minimums key the store: the stricter one holds the models as if
    // it had published them.
    let published = training(&catalog, models.path(), 1.0);
    let stricter = training(&catalog, models.path(), 3.0);
    for side in Side::BOTH {
        let current = published.store(side).unwrap().read(t()).unwrap();
        stricter.store(side).unwrap().publish(&current).unwrap();
    }

    let trained = stricter.train_at(t() + SignedDuration::from_hours(1), |from, to| {
        replay_on(&catalog, features(), from, to)
    });

    for outcome in [trained.long, trained.short] {
        let Some(Outcome::Kept(Kept::Refused { why, .. })) = outcome else {
            panic!("{outcome:?}");
        };
        let Refusal::MinPnl { pnl, min_pnl } = why else {
            panic!("{why:?}");
        };
        close(Some(pnl), 2.66);
        assert_eq!(min_pnl, 3.0);
    }
    for side in Side::BOTH {
        assert_eq!(stricter.store(side).unwrap().valid_froms().unwrap(), [t()]);
    }
}
