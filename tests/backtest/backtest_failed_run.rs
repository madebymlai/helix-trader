//! A run that fails once its node exists still disposes the node, so the next node of the thread
//! starts alone: Nautilus runs nodes one after another when each is disposed first
//! (`concepts/architecture.md`, "Processes and threads"), and disposing clears the thread's message
//! bus.

use helix_trader::backtest::Backtest;
use nautilus_common::{actor::DataActor, msgbus::get_message_bus};
use nautilus_model::identifiers::StrategyId;
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rstest::rstest;

use crate::common::trades;

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

fn assert_no_node_left() {
    let bus = get_message_bus();
    let endpoints = bus.borrow().endpoints().join(", ");
    assert!(endpoints.is_empty(), "endpoints left: {endpoints}");
}

type Run = fn(&Backtest, Refusing) -> anyhow::Result<()>;

#[rstest]
#[case::run(|backtest: &Backtest, strategy| backtest.run(strategy).map(drop))]
#[case::dataset_run(|backtest: &Backtest, strategy| backtest.run_dataset(strategy))]
fn a_run_failing_at_the_start_of_its_strategy_disposes_its_node(#[case] run: Run) {
    let levels = trades::TIME_EXIT;
    let catalog = trades::catalog(&levels);
    let backtest = trades::backtest(catalog.path(), trades::start(), &levels, "0 EUR");

    let err = run(&backtest, Refusing::new()).unwrap_err();

    assert!(format!("{err:#}").contains("refusing to start"), "{err:#}");
    assert_no_node_left();
}
