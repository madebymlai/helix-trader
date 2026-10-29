//! The backtests: one test binary, its modules running the real strategy or
//! a recorder through `Backtest::run` on fixture catalogs (`common`), a node
//! per test. The tests run in parallel, each on its own thread: Nautilus' message
//! bus and registries are thread-local, and its logger, process-wide, is
//! installed by the first node and shared by the others, its clock with
//! them: a log line may carry another test's simulated time
//! (`docs/ARCHITECTURE.md`, "Nautilus v2 constraints"). Run one module with:
//!
//! ```sh
//! cargo test --test backtest backtest_warmup::
//! ```

#[path = "../common/mod.rs"]
mod common;

mod backtest_bars;
mod backtest_bars_on_close;
mod backtest_book_deltas_and_custom_data;
mod backtest_book_depth;
mod backtest_commission_per_order;
mod backtest_commission_rate;
mod backtest_composite_bars;
mod backtest_composite_internal_source;
mod backtest_composite_warmup;
mod backtest_continuous_bars;
mod backtest_coverage;
mod backtest_dataset;
mod backtest_ema_entries;
mod backtest_ema_exits;
mod backtest_ema_overlap;
mod backtest_ema_read_only_instrument;
mod backtest_ema_time_exit;
mod backtest_features;
mod backtest_hedging_venue;
mod backtest_ib_minute_bars;
mod backtest_ic_hedging;
mod backtest_ic_margin;
mod backtest_ic_quotes;
mod backtest_inputs;
mod backtest_learned;
mod backtest_long_warmup;
mod backtest_max_drawdown;
mod backtest_model;
mod backtest_netting_venue;
mod backtest_okx_swap;
mod backtest_okx_swap_funding;
mod backtest_okx_xperp;
mod backtest_okx_xperp_funding;
mod backtest_quote_gap;
mod backtest_retrain;
mod backtest_rolls;
mod backtest_session;
mod backtest_start_on_close;
mod backtest_stop;
mod backtest_targets;
mod backtest_ticks;
mod backtest_trade_size_off_grid;
mod backtest_train;
mod backtest_two_venues;
mod backtest_warmup;
mod backtest_without_session;
