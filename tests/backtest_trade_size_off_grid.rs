//! The EMA crossover over the wave with a trade size finer than the
//! instrument's whole shares: 10.5 rounds to the nearest even share count,
//! so it trades as 10 shares do, and the half share left over is too small
//! to order. One node per process, so one run per file.

mod common;

use common::wave;
use helix_trader::strategy::ema::{EmaCross, EmaCrossConfig};
use nautilus_model::{
    data::BarSpecification,
    enums::{BarAggregation, PriceType},
    identifiers::InstrumentId,
    types::Quantity,
};

#[test]
fn a_trade_size_off_the_share_grid_trades_the_rounded_size_and_the_run_completes() {
    let config = EmaCrossConfig::builder()
        .instrument_ids(vec![InstrumentId::from(common::INSTRUMENT_ID)])
        .bar_spec(BarSpecification::new(
            1,
            BarAggregation::Hour,
            PriceType::Mid,
        ))
        .fast_period(5)
        .slow_period(15)
        .trade_size(Quantity::from("10.5"))
        .warmup(wave::WARMUP)
        .session(common::session())
        .build();
    let catalog = wave::catalog();
    let result = wave::backtest(catalog.path(), "3 EUR")
        .run(EmaCross::new(config).unwrap())
        .unwrap()
        .result;

    // The orders and the balance of 10 shares (backtest_ema_cross.rs).
    assert_eq!(result.total_orders, wave::EMA_ORDERS);
    assert_eq!(common::balance(&result), "9754.00 EUR");
}
