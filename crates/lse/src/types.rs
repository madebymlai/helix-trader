//! The custom data type of every LSE row that is not a price (`LseRow`),
//! made with Nautilus' custom data macros, as Nautilus' adapters make
//! theirs (`docs/concepts/custom_data.md`).

use nautilus_core::{Params, UnixNanos};
use nautilus_model::{
    custom_data,
    data::{CustomDataTrait, DataType},
};
use nautilus_serialization::arrow_custom_data;
use serde_json::{Map, Value, json};

/// The metadata keys of an `LseRow` data type: what the data client asks
/// LSE for.
pub(crate) const SOURCE: &str = "source";
pub(crate) const SYMBOL: &str = "symbol";

/// A row LSE serves that is not a price: its source, the collection, macro
/// series or factors it is of (`bond_yields`), its symbol (`US10Y`),
/// stamped when its values are known, both `ts_event` and `ts_init`, and
/// the row's other fields as LSE sends them.
#[arrow_custom_data]
#[custom_data]
pub struct LseRow {
    pub source: String,
    pub symbol: String,
    /// The row's fields but its symbol and the time LSE selects it by, by
    /// LSE's column names, as JSON: numbers, strings, `null`, and dates and
    /// times in ISO 8601.
    #[custom_data_field(serde)]
    pub fields: Map<String, Value>,
    pub ts_event: UnixNanos,
    pub ts_init: UnixNanos,
}

impl LseRow {
    /// The data type of the rows of `symbol` in LSE's `source`, as the
    /// data client is asked for them: `source` and `symbol` in its
    /// metadata, and keyed in a catalog by the identifier
    /// `<symbol>-<SOURCE>` (`US10Y-BOND_YIELDS`), LSE's symbol, a hyphen
    /// and its source in capitals, as an `LSEDGE` series' symbol names its
    /// market.
    #[must_use]
    pub fn data_type(source: &str, symbol: &str) -> DataType {
        let metadata = Params::new(
            [
                (SOURCE.to_string(), json!(source)),
                (SYMBOL.to_string(), json!(symbol)),
            ]
            .into(),
        );
        let identifier = format!("{symbol}-{}", source.to_uppercase());
        DataType::new(Self::type_name_static(), Some(metadata), Some(identifier))
    }

    /// Registers the type with Nautilus, so that its rows are written to
    /// and read from a catalog, which decodes custom data by its type
    /// name. Idempotent.
    pub fn register() {
        nautilus_serialization::ensure_custom_data_registered::<Self>();
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn the_rows_of_a_symbol_of_a_source_are_asked_by_both_and_keyed_by_symbol_and_source() {
        let data_type = LseRow::data_type("bond_yields", "US10Y");

        assert_eq!(data_type.type_name(), LseRow::type_name_static());
        let metadata = data_type.metadata().expect("metadata");
        assert_eq!(
            (metadata.get_str("source"), metadata.get_str("symbol")),
            (Some("bond_yields"), Some("US10Y"))
        );
        assert_eq!(data_type.identifier(), Some("US10Y-BOND_YIELDS"));
    }
}
