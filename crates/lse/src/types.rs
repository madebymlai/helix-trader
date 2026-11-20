//! The custom data type of every LSE row that is not a price (`LseRow`), made with Nautilus' custom
//! data macros (`docs/concepts/custom_data.md`).

use nautilus_core::UnixNanos;
use nautilus_model::custom_data;
use nautilus_serialization::arrow_custom_data;
use serde_json::{Map, Value};

// The metadata keys of an `LseRow` data type (`data::served::row_source`)
pub(crate) const SOURCE: &str = "source";
pub(crate) const SYMBOL: &str = "symbol";

/// A row LSE serves that is not a price: its source (`bond_yields`), its symbol (`US10Y`) and the
/// row's other fields as LSE sends them.
///
/// Both `ts_event` and `ts_init` are stamped when its values are known.
#[arrow_custom_data]
#[custom_data]
pub struct LseRow {
    pub source: String,
    pub symbol: String,
    /// The row's fields but its symbol and selecting time, by LSE's column names, as JSON, with
    /// dates and times in ISO 8601.
    #[custom_data_field(serde)]
    pub fields: Map<String, Value>,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

impl LseRow {
    /// Registers the type with Nautilus, so that a catalog writes and reads its rows by type name.
    /// Idempotent.
    pub fn register() {
        nautilus_serialization::ensure_custom_data_registered::<Self>();
    }
}
