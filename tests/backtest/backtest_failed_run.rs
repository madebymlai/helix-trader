//! A run that fails once its node exists disposes the node all the same,
//! as one that completes does, so that the next node of the thread starts
//! alone: Nautilus runs nodes one after another when each is disposed
//! before the next starts (`concepts/architecture.md`, "Processes and
//! threads"), and disposing clears the thread's message bus.

use nautilus_common::{actor::DataActor, msgbus::get_message_bus};
use nautilus_model::identifiers::StrategyId;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};

use crate::common::trades;

/// A strategy refusing to start, which fails the run once the node is
/// built and the strategy added.
#[derive(Debug)]
struct Refusing {
    core: StrategyCore,
}

impl Refusing {
    fn new() -> Self {
        Self {
            core: StrategyCore::new(StrategyConfig {
                strategy_id: Some(StrategyId::from("REFUSING-001")),
                order_id_tag: Some("001".to_string()),
                ..Default::default()
            }),
        }
    }
}

nautilus_strategy!(Refusing);

impl DataActor for Refusing {
    fn on_start(&mut self) -> anyhow::Result<()> {
        anyhow::bail!("refusing to start")
    }
}

/// The thread's message bus holds no endpoint: no node's engines are left
/// on it.
fn assert_no_node_left() {
    let bus = get_message_bus();
    let endpoints = bus.borrow().endpoints().join(", ");
    assert!(endpoints.is_empty(), "endpoints left: {endpoints}");
}

#[test]
fn a_run_failing_at_the_start_of_its_strategy_disposes_its_node() {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);
    let backtest = trades::backtest(catalog.path(), trades::start(), &levels, "0 EUR");

    let err = backtest.run(Refusing::new()).unwrap_err();

    assert!(format!("{err:#}").contains("refusing to start"), "{err:#}");
    assert_no_node_left();
}

#[test]
fn a_dataset_run_failing_at_the_start_of_its_strategy_disposes_its_node() {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);
    let backtest = trades::backtest(catalog.path(), trades::start(), &levels, "0 EUR");

    let err = backtest.run_dataset(Refusing::new()).unwrap_err();

    assert!(format!("{err:#}").contains("refusing to start"), "{err:#}");
    assert_no_node_left();
}
