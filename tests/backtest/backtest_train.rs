//! Training at a time (`model::train`) on a fixture catalog with an edge:
//! the dataset probe (`common::dataset`) replays the days the window lacks
//! through `Backtest::run_dataset`, and LightGBM fits each side's realised
//! R on the rows. IC's gold rises 4.00 over each even hour and falls back
//! over each odd one (`edge_quotes`), and the probe's one feature, `cheap`,
//! marks where the hour starts: 1 at the bottom, 0 at the top. Each side
//! learns its R from the feature, the long's high at the bottom, where it
//! wins, and the short's at the top, and their pair calibrated together
//! expects each to win where it does: each row picks the side that wins it,
//! and the trades picked earn more per trade than taking every trade. One
//! node per thread: a test training twice trains the first time on a thread
//! of its own.

use std::{path::Path, rc::Rc};

use crate::common::{
    dataset::{blueprint, replay_on, rule, target, venue},
    features::tables,
    ic,
};
use helix_trader::{
    model::{
        config::{Margins, ModelConfig},
        features::{Feature, FeatureSet, side_names},
        holdout::{Holdout, Refusal},
        learner::{LearnerConfig, LightGbmConfig},
        store::{ModelCalibration, ModelStore},
        targets::Side,
        train::{Kept, Outcome, Trained, Training},
    },
    strategy::{Declared, IndicatorArgs, risk::R},
};
use jiff::{SignedDuration, Timestamp};
use nautilus_indicators::indicator::Indicator;
use nautilus_model::{data::QuoteTick, identifiers::InstrumentId};
use tempfile::TempDir;

/// The price of gold at the bottom of each even hour, and the rise over
/// it.
const BOTTOM: f64 = 4000.0;
const RISE: f64 = 4.0;

/// The worth of each trade's stop, 3.00 from its decision's mid, on an
/// ounce: one R, in USD.
const UNIT: f64 = 3.0;

/// The least expectancy the models publish from, which the
/// trades picked clear.
const MINIMUM: R = R::new(0.5);

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

/// The arguments of `Cheap`: none.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CheapArgs {}

impl IndicatorArgs for CheapArgs {
    type Indicator = Cheap;

    fn build(self) -> anyhow::Result<Cheap> {
        Ok(Cheap::default())
    }
}

/// `Cheap`, `[indicators.cheap]`.
const CHEAP: Declared<CheapArgs> = Declared::new("cheap");

fn features() -> Rc<FeatureSet> {
    FeatureSet::builder()
        .quotes(
            InstrumentId::from(ic::INSTRUMENT_ID),
            CHEAP,
            &tables("cheap = {}"),
        )
        .build()
        .unwrap()
}

/// The probe's decisions on `cheap`, learning the R of each side of the
/// bracket by LightGBM, on leaves of 20 rows, on a window of 70 hours, the
/// last 12 held out and the 12 before them calibrated on, so that the 46
/// hours fitted on split; each side traded from an expected net R of
/// nought, published from `min_expectancy` per trade over 5 trades.
fn config(min_expectancy: R) -> ModelConfig {
    ModelConfig::builder()
        .decision(rule())
        .target(target())
        .retrain(SignedDuration::from_hours(24))
        .window(SignedDuration::from_hours(70))
        .holdout(SignedDuration::from_hours(12))
        .margins(Margins {
            long: Some(R::new(0.0)),
            short: Some(R::new(0.0)),
        })
        .min_expectancy(min_expectancy)
        .min_trades(5)
        .learner(LearnerConfig::LightGbm(
            LightGbmConfig::builder().min_data_in_leaf(20).build(),
        ))
        .build()
}

fn training(catalog: &TempDir, models: &Path, min_expectancy: R) -> Training {
    blueprint(features(), config(min_expectancy))
        .training(catalog.path(), models, &venue())
        .unwrap()
}

/// The store under `models` of the `side` models of a training of
/// `min_expectancy`, as `training` keys it.
fn store(models: &Path, min_expectancy: R, side: Side) -> ModelStore {
    blueprint(features(), config(min_expectancy))
        .model_set(models, &venue())
        .unwrap()
        .side(side)
        .unwrap()
        .store()
        .clone()
}

/// Quotes from 23:00 of the 19th of October 2026 to 03:00 of the 23rd.
fn catalog() -> TempDir {
    let (from, to) = (
        ic::at("00:00:00") - SignedDuration::from_hours(25),
        ic::at("00:00:00") + SignedDuration::from_hours(51),
    );
    ic::catalog_of(edge_quotes(from, to), from, to)
}

/// Midnight of the 23rd: the window starts at 02:00 of the 20th.
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
        }) if valid_from == t() => *holdout,
        outcome => panic!("{outcome:?}"),
    }
}

/// Worked by hand. A decision at the bottom, mid 4000.10, sets each exit
/// 3.00 away (7.5 bps, on the cent), a stop worth 3.00 USD on the ounce,
/// the trade's unit, one R (`UNIT`): the long enters at the ask, 4000.20,
/// and its take profit, a sell limit at 4003.10, fills when the bid
/// reaches it, +2.90 less two fees of 0.12, 2.66 USD; the short enters at
/// the bid, 4000.00, and its stop at 4003.10 fills when the ask reaches
/// it, -3.10 less the fees, -3.34. At the top, mid 4004.10, the reverse.
/// Those fills are 58 and 62 quotes of 45 s after the hour starts, each
/// row settling at 46:30 past it. Held out, the decisions of 12:00 to 23:00
/// of the 22nd, the last settled at 23:46:30, before the train: six at the
/// bottom, six at the top. Each row picks the side that wins it, the long at
/// the bottom and the short at the top: 12 trades of 2.66 / 3 R, beside every
/// trade, a win and a loss of each row, (12 x 2.66 - 12 x 3.34) / 24 / 3 R.
/// Both sides hold those earnings. At 1.5 times the commission of 0.24 each
/// trade makes 0.12 / 3 R less. Each side's expected R is nearer its R than its training mean is:
/// a skill above nought. Both manifests hold the pair's one calibration,
/// naming it, and each side's model, reloaded by its learner, predicts its
/// probes to the bit.
#[test]
fn train_at_t_publishes_each_side_earning_on_the_edge_whose_probes_reload_to_identical_predictions()
{
    let catalog = catalog();
    let models = tempfile::tempdir().unwrap();
    let training = training(&catalog, models.path(), MINIMUM);

    let trained = training.train_at(t(), |from, to| replay_on(&catalog, features(), from, to));

    let Trained { long, short, meta } = trained;
    assert_eq!(meta, None);
    for holdout in [published(long), published(short)] {
        let earnings = holdout.earnings;
        assert_eq!(
            (earnings.taken.count, earnings.every.count),
            (12, 24),
            "{holdout:?}"
        );
        close(earnings.taken.expectancy, 2.66 / UNIT);
        close(earnings.taken.win_rate, 1.0);
        close(
            earnings.every.expectancy,
            (12.0 * 2.66 - 12.0 * 3.34) / 24.0 / UNIT,
        );
        close(earnings.taken_stressed.expectancy, 2.54 / UNIT);
        close(
            earnings.every_stressed.expectancy,
            (12.0 * 2.54 - 12.0 * 3.46) / 24.0 / UNIT,
        );
        assert!(
            holdout.score.skill.is_some_and(|skill| skill > 0.0),
            "{holdout:?}"
        );
    }
    let fingerprint = blueprint(features(), config(MINIMUM))
        .fingerprint(&venue())
        .unwrap()
        .to_string();
    // A side's model reads the strategy's features, then the bracket's payoff
    // features, the names the decision checks it against.
    let names = side_names(features().names(), &target().payoff_names());
    let [long, short] =
        Side::BOTH.map(|side| store(models.path(), MINIMUM, side).read(t()).unwrap());
    assert_eq!(long.manifest.calibration, short.manifest.calibration);
    let ModelCalibration::Pair(calibration) = &long.manifest.calibration else {
        panic!("{:?}", long.manifest.calibration);
    };
    assert_eq!(
        (calibration.valid_from, &calibration.sides[..]),
        (t(), &Side::BOTH[..])
    );
    for (side, stored) in Side::BOTH.into_iter().zip([long, short]) {
        assert_eq!(
            (&stored.manifest.feature_names, &stored.manifest.fingerprint),
            (&names, &fingerprint),
            "{side:?}"
        );
        let model = stored
            .manifest
            .learner
            .load(&stored.model)
            .unwrap_or_else(|e| panic!("{side:?}: {e:#}"));
        assert!(!stored.manifest.probes.is_empty());
        for probe in &stored.manifest.probes {
            assert_eq!(
                model.predict(&probe.row).unwrap().to_bits(),
                probe.prediction.to_bits(),
                "{probe:?}"
            );
        }
    }
}

/// An hour later the holdout is the decisions of 13:00 to 00:00: each
/// row picks the side that wins it, 2.66 / 3 R each, short of a minimum of
/// 1R, however far above it their money is.
#[test]
fn a_minimum_above_what_the_holdout_earns_keeps_each_current_model() {
    let catalog = catalog();
    let models = tempfile::tempdir().unwrap();
    let first = std::thread::scope(|scope| {
        scope
            .spawn(|| {
                training(&catalog, models.path(), MINIMUM)
                    .train_at(t(), |from, to| replay_on(&catalog, features(), from, to))
            })
            .join()
            .unwrap()
    });
    for outcome in [first.long, first.short] {
        published(outcome);
    }
    // The minimums key the store: the stricter one holds the models as if
    // it had published them.
    let stricter = training(&catalog, models.path(), R::new(1.0));
    for side in Side::BOTH {
        let current = store(models.path(), MINIMUM, side).read(t()).unwrap();
        store(models.path(), R::new(1.0), side)
            .publish(&current)
            .unwrap();
    }

    let trained = stricter.train_at(t() + SignedDuration::from_hours(1), |from, to| {
        replay_on(&catalog, features(), from, to)
    });

    for outcome in [trained.long, trained.short] {
        let Some(Outcome::Kept(Kept::Refused { why, .. })) = outcome else {
            panic!("{outcome:?}");
        };
        let Refusal::MinExpectancy {
            expectancy,
            min_expectancy,
        } = why
        else {
            panic!("{why:?}");
        };
        close(Some(expectancy), 2.66 / UNIT);
        assert_eq!(min_expectancy, R::new(1.0));
    }
    for side in Side::BOTH {
        assert_eq!(
            store(models.path(), R::new(1.0), side)
                .valid_froms()
                .unwrap(),
            [t()]
        );
    }
}
