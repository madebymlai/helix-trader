//! The backtests: one test binary, its modules running the real strategy or
//! a recorder through `Backtest::run` on fixture catalogs (`common`), a node
//! per test. The tests run in parallel, each on its own thread, so their
//! nodes run at once, which Nautilus does not support: its message bus and
//! registries are thread-local, one per test, but its logger, installed by
//! the first node, with the logging mode and clock each backtest switches,
//! its logging worker and its Tokio runtime are the process's, shared by
//! every node: a log line may carry another test's simulated time
//! (`docs/ARCHITECTURE.md`, "Process constraints"). Run one module with:
//!
//! ```sh
//! cargo test --test backtest backtest_warmup::
//! ```

#[path = "../common/mod.rs"]
mod common;

mod backtest_bars;
mod backtest_bars_on_close;
mod backtest_book_deltas_and_custom_data;
mod backtest_catalog_gaps;
mod backtest_commission_rate;
mod backtest_composite_bars;
mod backtest_continuous_bars;
mod backtest_dataset;
mod backtest_ema_entries;
mod backtest_ema_exits;
mod backtest_ema_overlap;
mod backtest_ema_time_exit;
mod backtest_failed_run;
mod backtest_features;
mod backtest_golden_rows;
mod backtest_ic_hedging;
mod backtest_ic_margin;
mod backtest_ic_quotes;
mod backtest_inputs;
mod backtest_last_bars;
mod backtest_max_drawdown;
mod backtest_meta;
mod backtest_model;
mod backtest_model_ema;
mod backtest_netting_venue;
mod backtest_okx_swap;
mod backtest_okx_swap_funding;
mod backtest_quote_gap;
mod backtest_relative_catalog;
mod backtest_retrain;
mod backtest_rolls;
mod backtest_root_ticks;
mod backtest_session;
mod backtest_targets;
mod backtest_ticks;
mod backtest_trade_size_off_grid;
mod backtest_traded_quotes;
mod backtest_train;
mod backtest_two_venues;
mod backtest_warmup;
mod backtest_without_session;
