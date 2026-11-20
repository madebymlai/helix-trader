//! The custom data type of every LSE row that is not a price (`LseRow`), made with Nautilus' custom
//! data macros, as Nautilus' adapters make theirs (`docs/concepts/custom_data.md`).

use nautilus_core::UnixNanos;
use nautilus_model::custom_data;
use nautilus_serialization::arrow_custom_data;
use serde_json::{Map, Value};

/// The metadata keys of an `LseRow` data type: what the data client asks LSE for
/// (`data::served::row_source`).
pub(crate) const SOURCE: &str = "source";
pub(crate) const SYMBOL: &str = "symbol";

/// A row LSE serves that is not a price: its source, the collection, macro series or factors it is
/// of (`bond_yields`), its symbol (`US10Y`), and the row's other fields as LSE sends them.
///
/// It is stamped when its values are known, both `ts_event` and `ts_init`.
#[arrow_custom_data]
#[custom_data]
pub struct LseRow {
    pub source: String,
    pub symbol: String,
    /// The row's fields but its symbol and the time LSE selects it by, by LSE's column names, as
    /// JSON: numbers, strings, `null`, and dates and times in ISO 8601.
    #[custom_data_field(serde)]
    pub fields: Map<String, Value>,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

impl LseRow {
    /// Registers the type with Nautilus, so that its rows are written to and read from a catalog,
    /// which decodes custom data by its type name. Idempotent.
    pub fn register() {
        nautilus_serialization::ensure_custom_data_registered::<Self>();
    }
}
