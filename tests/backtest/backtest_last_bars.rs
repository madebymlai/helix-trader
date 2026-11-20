//! The EMA crossover on hour LAST bars, on a catalog holding the instrument's trades and no quote.
//! Nautilus builds the bars from the trades, and the venue fills on them (a trade with no aggressor
//! moves both bid and ask to its price). The trades come one a minute at the mid of
//! `common::trades`' quotes, so each bar closes where its MID bar did.

use helix_trader::{
    backtest::Backtest,
    strategy::{
        DataConfig, Series,
        ema::{EmaCross, EmaCrossConfig},
    },
};
use jiff::Timestamp;
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{BarSpecification, BarType, TradeTick},
    enums::{AggregationSource, AggressorSide, BarAggregation, PriceType},
    identifiers::{InstrumentId, TradeId},
    types::{Price, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rstest::rstest;
use tempfile::TempDir;

use crate::common::{self, INSTRUMENT_ID, trades};

fn trade_ticks(levels: &[f64]) -> Vec<TradeTick> {
    trades::quotes(levels)
        .iter()
        .map(|quote| {
            let price = (quote.bid_price.as_f64() + quote.ask_price.as_f64()) / 2.0;
            TradeTick::new(
                quote.instrument_id,
                Price::new(price, 2),
                Quantity::from(100),
                AggressorSide::NoAggressor,
                TradeId::from(quote.ts_event.as_u64().to_string().as_str()),
                quote.ts_event,
                quote.ts_init,
            )
        })
        .collect()
}

fn trades_catalog(levels: &[f64]) -> TempDir {
    let dir = common::catalog_without_quotes();
    let catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
    let (from, to): (Timestamp, Timestamp) = (trades::at("09:00"), trades::end(levels));
    catalog
        .write_to_parquet(
            &trade_ticks(levels),
            Some(UnixNanos::from(from)),
            Some(UnixNanos::from(to)),
            None,
        )
        .unwrap();
    dir
}

fn last_bars() -> BarType {
    BarType::new(
        InstrumentId::from(INSTRUMENT_ID),
        BarSpecification::new(1, BarAggregation::Hour, PriceType::Last),
        AggregationSource::Internal,
    )
}

fn ema_on_last_bars() -> EmaCross {
    let config = common::ema_config(1, 2, Some(common::session()));
    let config = EmaCrossConfig {
        data: DataConfig {
            series: vec![Series::Bars(last_bars())],
            ..config.data
        },
        ..config
    };
    EmaCross::new(config).unwrap()
}

/// Runs `trades::TIME_EXIT` on trades.
///
/// The closes 610.60 (11:00) and 600.60 (12:00) warm up the crossover. Then:
///
/// - 13:00 at 620.60, above 605.60: buys 10 at the last trade (on quotes, the ask 620.65), with
///   stop loss 617.50 and take profit 626.81.
/// - 17:00, 4 hours after the entry: the time alert sells 10 at 624.90 (on quotes, the bid 624.85).
///   The trades from 624.01 to 624.90 touch neither exit.
///
/// The run gains 10 x 4.30 = 43.00, ending at 10043.00 EUR after 4 orders.
#[rstest]
fn the_ema_trades_on_last_bars_built_from_trades_alone() {
    let levels = trades::TIME_EXIT;
    let catalog = trades_catalog(&levels);
    let backtest = Backtest {
        bar_types: vec![last_bars()],
        ..trades::backtest(catalog.path(), trades::start(), &levels, "0 EUR")
    };

    let result = backtest.run(ema_on_last_bars()).unwrap().result;

    assert_eq!(result.total_orders, trades::TIME_EXIT_ORDERS);
    assert_eq!(common::balance(&result), "10043.00 EUR");
}
