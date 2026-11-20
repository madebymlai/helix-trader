//! Defines `LseRow`, the custom data type for every LSE row that is not a price.
//!
//! It is made with Nautilus' custom data macros (`docs/concepts/custom_data.md`).

use nautilus_core::UnixNanos;
use nautilus_model::custom_data;
use nautilus_serialization::arrow_custom_data;
use serde_json::{Map, Value};

// The metadata keys of the `LseRow` data type (`data::served::row_source`)
pub(crate) const SOURCE: &str = "source";
pub(crate) const SYMBOL: &str = "symbol";

/// A row that LSE serves and that is not a price.
///
/// It holds the source (`bond_yields`), the symbol (`US10Y`) and the other fields as LSE sends
/// them. Both `ts_event` and `ts_init` are stamped once the row's values are known.
#[arrow_custom_data]
#[custom_data]
pub struct LseRow {
    pub source: String,
    pub symbol: String,
    /// Every field except the symbol and the selecting time, as JSON keyed by LSE's column names.
    /// Dates and times are in ISO 8601.
    #[custom_data_field(serde)]
    pub fields: Map<String, Value>,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

impl LseRow {
    /// Registers the type with Nautilus, so that a catalog writes and reads its rows by type name.
    ///
    /// Calling it again has no effect.
    pub fn register() {
        nautilus_serialization::ensure_custom_data_registered::<Self>();
    }
}
