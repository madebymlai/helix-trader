//! Fills the catalog with the history of the given instruments and bar types.
//!
//! Source-agnostic, no node: the bars come from a fetch function the caller
//! passes, backed by an adapter's historical client (`ib.rs`). Per bar type
//! it resumes after the last stored bar (`query_last_timestamp`), checks the
//! response, sorts it by `ts_init` and writes it with `write_instruments`
//! and `write_to_parquet` on a `ParquetDataCatalog` (`nautilus_persistence`).
//! Nautilus does not persist requested data on its own.
