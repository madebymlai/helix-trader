//! Tests training at a time (`model::train`) on a fixture catalog with an edge.
//!
//! The dataset probe (`common::dataset`) replays the days the window lacks through
//! `Backtest::run_dataset`. LightGBM fits each side's realized R on the rows.
//!
//! IC's gold rises 4.00 over each even hour and falls back over each odd one
//! (`ic::edge_quotes`). The probe's one feature, `cheap`, is 1 at the bottom of the hour and 0 at
//! the top. So the long learns a high R at the bottom and the short at the top. The two sides are
//! calibrated together on the calibration slice and published. Each model reloads to its probes.

use std::{path::Path, rc::Rc};

use helix_trader::{
    model::{
        config::{Margins, ModelConfig},
        features::{Feature, FeatureSet, side_names},
        learner::{LearnerConfig, LightGbmConfig},
        store::StoredModelSet,
        targets::Side,
        train::{Outcome, Training},
    },
    strategy::{Declared, IndicatorArgs, Series, risk::R},
};
use jiff::{SignedDuration, Timestamp};
use nautilus_indicators::indicator::Indicator;
use nautilus_model::{data::QuoteTick, identifiers::InstrumentId};
use rstest::rstest;
use tempfile::TempDir;

use crate::common::{
    dataset::{blueprint, replay_on, rule, target, venue},
    features::tables,
    ic,
};

const BOTTOM: f64 = ic::EDGE_BOTTOM;
const RISE: f64 = ic::EDGE_RISE;

/// Computes `cheap` from the last mid, clamped.
///
/// It is 0 at the top of the hours (4004.10) and 1 at their bottom (4000.10).
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

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CheapArgs {}

impl IndicatorArgs for CheapArgs {
    type Indicator = Cheap;

    fn build(self) -> anyhow::Result<Cheap> {
        Ok(Cheap::default())
    }
}

const CHEAP: Declared<CheapArgs> = Declared::new("cheap");

fn features() -> Rc<FeatureSet> {
    FeatureSet::builder()
        .feature(
            &[Series::quotes(InstrumentId::from(ic::INSTRUMENT_ID))],
            CHEAP,
            &tables("cheap = {}"),
        )
        .build()
        .unwrap()
}

/// Builds the LightGBM config: leaves of 20 rows and a window of 70 hours.
///
/// The last 12 hours are for calibration, so 58 are fitted on split. Each slice has at least 5
/// rows. Each side trades from an expected net R of 0.
fn config() -> ModelConfig {
    ModelConfig::builder()
        .decision(rule())
        .target(target())
        .retrain(SignedDuration::from_hours(24))
        .dataset(venue())
        .window(SignedDuration::from_hours(70))
        .calibration(SignedDuration::from_hours(12))
        .margins(Margins {
            long: Some(R::new(0.0)),
            short: Some(R::new(0.0)),
        })
        .min_rows(5)
        .learner(LearnerConfig::LightGbm(
            LightGbmConfig::builder().min_data_in_leaf(20).build(),
        ))
        .build()
}

fn training(catalog: &TempDir, models: &Path) -> Training {
    blueprint(features(), config())
        .training(catalog.path(), models)
        .unwrap()
}

fn model_set(models: &Path, valid_from: Timestamp) -> StoredModelSet {
    blueprint(features(), config())
        .model_store(models)
        .unwrap()
        .read(valid_from)
        .unwrap()
}

fn catalog() -> TempDir {
    let (from, to) = (
        ic::at("00:00:00") - SignedDuration::from_hours(25),
        ic::at("00:00:00") + SignedDuration::from_hours(51),
    );
    ic::catalog_of(ic::edge_quotes(from, to), from, to)
}

/// Returns midnight of the 23rd, so the window starts at 02:00 of the 20th.
fn t() -> Timestamp {
    ic::at("00:00:00") + SignedDuration::from_hours(48)
}

/// Publishes each side at t in the model set valid from then, with the pair's calibration.
///
/// Each learner reloads its model. The model predicts its probes, rows of the calibration slice,
/// to the bit.
#[rstest]
fn train_at_t_publishes_each_side_whose_probes_reload_to_identical_predictions() {
    let catalog = catalog();
    let models = tempfile::tempdir().unwrap();
    let training = training(&catalog, models.path());

    let trained = training.train_at(t(), |from, to| replay_on(&catalog, features(), from, to));

    assert_eq!(
        trained.outcome,
        Outcome::Published {
            model_set: blueprint(features(), config())
                .model_store(models.path())
                .unwrap()
                .directory()
                .join(t().to_string())
        }
    );
    let fingerprint = blueprint(features(), config())
        .fingerprint()
        .unwrap()
        .to_string();

    // A side's model reads the strategy's features, then the bracket's payoff features, and the
    // decision checks it against these names.
    let names = side_names(features().names(), &target().payoff_names());
    let model_set = model_set(models.path(), t());
    assert_eq!(model_set.valid_from, t());
    assert!(model_set.calibration.side_call.is_some());
    assert_eq!(model_set.meta, None);
    for (side, stored) in Side::BOTH.into_iter().zip(model_set.sides) {
        assert_eq!(stored.manifest.side, Some(side));
        assert_eq!(stored.manifest.calibration, None, "{side:?}");
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
