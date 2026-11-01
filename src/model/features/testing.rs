//! The feature of the harness' tests: the last mid of an instrument's
//! quotes, declared and set in `[indicators]` as a strategy's features are.

use nautilus_model::identifiers::InstrumentId;

use super::{FeatureSet, FeatureSetBuilder};
use crate::strategy::{Declared, EmaArgs, IndicatorsConfig};

/// The last mid of a quote, Nautilus' exponential average of one,
/// `[indicators.mid]`.
pub const MID: Declared<EmaArgs> = Declared::new("mid");

/// `[indicators.mid]`, as `[indicators]` holds it.
pub fn tables() -> IndicatorsConfig {
    toml::from_str(r#"mid = { period = 1, price_type = "MID" }"#).unwrap()
}

/// The features of one, `mid`, the last mid of `instrument_id`, warm once
/// a quote came.
pub fn last_mid(instrument_id: InstrumentId) -> FeatureSetBuilder {
    FeatureSet::builder().quotes(instrument_id, MID, &tables())
}
