//! The EMA crossover over the wave (`backtest_ema_cross_free.rs`) on a
//! hedging venue, as IC's: each crossing closes the open position by its
//! id before opening the other side, so it sends the orders and makes the
//! PnL of the netting venue, never holding opposite positions. One node
//! per process, so one run per file.

mod common;

use common::wave;
use helix_trader::backtest::Backtest;
use nautilus_model::enums::OmsType;

#[test]
fn on_a_hedging_venue_the_ema_crossover_closes_before_it_turns_as_on_a_netting_one() {
    let catalog = wave::catalog();
    let backtest = Backtest {
        oms_type: OmsType::Hedging,
        ..wave::backtest(catalog.path(), "0 EUR")
    };

    let result = backtest.run(wave::ema_cross()).unwrap().result;

    assert_eq!(result.total_orders, wave::EMA_ORDERS);
    assert_eq!(
        result.stats_pnls["EUR"]["PnL (total)"],
        wave::EMA_PNL_WITHOUT_COMMISSION
    );
}
