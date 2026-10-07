//! Ingest actor: fills the catalog with the history of the given instruments.
//!
//! A Nautilus `DataActor` (`nautilus_common::actor`) run on a live node with
//! a data client only. It calls `request_instruments` and `request_bars`,
//! which reach any adapter through the Nautilus `DataClient` trait, and
//! writes the responses from `on_instrument` and `on_historical_bars`
//! through `CatalogWriter` (`write_instruments`, `write_data`) on a
//! `ParquetDataCatalog` (`nautilus_persistence`). Nautilus does not persist
//! requested data on its own.
