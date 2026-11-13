//! Nautilus' data tester on LSE: a live node with the LSE data client and
//! Nautilus' `DataTester`, run against LSE itself, for the cases of
//! Nautilus' data testing spec (`docs/developer_guide/spec_data_testing.md`)
//! the adapter supports: TC-D01, the venue's instruments, which the node's
//! catalog answers, LSE publishing no definitions; TC-D31, historical
//! trades, of gold futures (gold spot's fails with LSE's refusal, a
//! commodity, of which LSE stores no trades); and TC-D41, historical bars,
//! gold spot's 1-minute candles. The adapter streams nothing yet, so every
//! subscription case is skipped.
//!
//! LSE publishes no definitions: the tester writes the crate's test
//! definitions of the two series into a temporary catalog the node reads,
//! as one written by hand would be. The window is a past hour, as LSE's
//! history ends at the previous session's close. Edit the constants below
//! to change them. The node runs until interrupted (Ctrl+C).
//!
//! Run with: `cargo run -p lse --features test-support --example lse-data-tester`
//!
//! Required credential environment variables:
//! - `LSE_API_KEY`, from the environment or `.env`

use lse::{
    LseDataClientConfig, LseDataClientFactory,
    testing::{GOLD_FUTURES, GOLD_SPOT, gold_futures, gold_spot},
};
use nautilus_common::{enums::Environment, factories::DataClientFactory};
use nautilus_live::{builder::LiveNodeBuilder, config::LiveNodeConfig};
use nautilus_model::{
    data::BarType,
    identifiers::{ClientId, InstrumentId, TraderId},
};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, config::DataCatalogConfig,
};
use nautilus_testkit::testers::{DataTester, DataTesterConfig};

const TRADER_ID: &str = "TESTER-001";
const NODE_NAME: &str = "LSE-TESTER-001";
/// Gold spot's bars, a series LSE serves only as candles.
const BAR_TYPE: &str = "XAU/USD-COMMODITIES.LSEDGE-1-MINUTE-LAST-EXTERNAL";
/// An hour of Thursday 12 November 2026.
const REQUEST_START: &str = "2026-11-12T10:00:00Z";
const REQUEST_END: &str = "2026-11-12T11:00:00Z";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let dir = tempfile::tempdir()?;
    ParquetDataCatalog::new(dir.path(), None, None, None, None)
        .write_instruments(vec![gold_futures(), gold_spot()])?;
    let catalog_path = dir.path().to_str().ok_or("a catalog path in UTF-8")?;

    let config = LiveNodeConfig {
        environment: Environment::Live,
        trader_id: TraderId::from(TRADER_ID),
        load_state: false,
        save_state: false,
        catalogs: vec![DataCatalogConfig::new(catalog_path.to_owned(), None, None)],
        ..Default::default()
    };
    let factory = LseDataClientFactory::new();
    let client_id = ClientId::from(factory.name());
    let mut node = LiveNodeBuilder::from_config(config)?
        .with_name(NODE_NAME)
        .with_delay_post_stop_secs(2)
        .add_data_client(
            None,
            Box::new(factory),
            Box::new(LseDataClientConfig::default()),
        )?
        .build()?;

    let tester_config = DataTesterConfig::builder()
        .client_id(client_id)
        .instrument_ids(vec![
            InstrumentId::from(GOLD_FUTURES),
            InstrumentId::from(GOLD_SPOT),
        ])
        .bar_types(vec![BarType::from(BAR_TYPE)])
        .request_instruments(true)
        .request_trades(true)
        .request_bars(true)
        .request_start(REQUEST_START.parse()?)
        .request_end(REQUEST_END.parse()?)
        .build()?;

    node.add_actor(DataTester::new(tester_config))?;
    node.run().await?;

    Ok(())
}
