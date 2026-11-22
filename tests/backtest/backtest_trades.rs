//! Tests the positions a backtest writes to its trades file (`backtest::Trades`), on the run of
//! `backtest_ema_entries`: three trades of the EMA crossover, one after another, on SXR8 at
//! IBIS, a netting venue, so the three are cycles of one position ID.
//!
//! - 13:00 buys 10 at 620.65, one R 3.10 from the close 620.60 to the stop loss 617.50, 31.00 EUR
//!   on 10. The 13:01 quote, stamped a nanosecond before the minute as each quote of the fixture
//!   is (`common::quote`), bids 599.96 and the stop loss sells there: -206.90.
//! - 14:00 sells 10 at 600.55, one R 3.00 from 600.60 to 603.60, 30.00 EUR. The 14:01 quote asks
//!   610.06 and the stop loss buys there: -95.10.
//! - 15:00 buys 10 at 610.65, one R 3.05 from 610.60 to 607.55, 30.50 EUR. It is open at 16:00,
//!   the end, its realized P&L the commission of nought.
//!
//! Each trade is sent on the latest quote, the one its bar closed on, which bids and asks the half
//! spread 0.05 either side of the close: 620.55 and 620.65, 600.55 and 600.65, 610.55 and 610.65.
//! Each trade is sized at 0.32% of the equity then over its one R, 10 shares each time. Nautilus
//! archives each closed cycle as a snapshot before the next fill reuses the ID, under the ID and a
//! UUID, so the file holds the two snapshots and the open position. No decision sent them, so they
//! have no expected R.

use std::{path::Path, process::Command};

use arrow::{
    array::{Array, Float64Array, StringArray, TimestampNanosecondArray},
    record_batch::RecordBatch,
};
use helix_trader::backtest::Trades;
use jiff::{SignedDuration, Timestamp};
use nautilus_common::live::block_on_nautilus_with;
use nautilus_persistence::backend::parquet::io::{
    create_object_store_from_path, read_parquet_from_object_store,
};
use object_store::path::Path as ObjectPath;
use rstest::rstest;

use crate::common::trades;

const LEVELS: [f64; 4] = [620.0, 600.0, 610.0, 611.0];

fn strings(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
    let column = batch.column_by_name(name).unwrap();
    let column = column.as_any().downcast_ref::<StringArray>().unwrap();
    column
        .iter()
        .map(|value| value.map(str::to_string))
        .collect()
}

fn numbers(batch: &RecordBatch, name: &str) -> Vec<Option<f64>> {
    let column = batch.column_by_name(name).unwrap();
    let column = column.as_any().downcast_ref::<Float64Array>().unwrap();
    column.iter().collect()
}

fn times(batch: &RecordBatch, name: &str) -> Vec<Option<Timestamp>> {
    let column = batch.column_by_name(name).unwrap();
    let column = column
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    column
        .iter()
        .map(|ns| ns.map(|ns| Timestamp::from_nanosecond(i128::from(ns)).unwrap()))
        .collect()
}

fn run() -> (Trades, f64) {
    let catalog = trades::catalog(&LEVELS);
    let outcome = trades::backtest(catalog.path(), trades::start(), &LEVELS, "0 EUR")
        .run(trades::ema_cross())
        .unwrap();
    let total = outcome.result.stats_pnls["EUR"]["PnL (total)"];
    (outcome.trades, total)
}

#[rstest]
fn each_cycle_of_the_position_is_a_row_with_the_r_its_trade_was_sent_at() {
    let (trades, total) = run();

    let batch = trades.batch();
    let some = |values: &[&str]| -> Vec<Option<String>> {
        values
            .iter()
            .map(|value| Some((*value).to_string()))
            .collect()
    };
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(strings(batch, "entry"), some(&["BUY", "SELL", "BUY"]));
    assert_eq!(strings(batch, "side"), some(&["FLAT", "FLAT", "LONG"]));
    assert_eq!(
        times(batch, "ts_opened"),
        ["13:00", "14:00", "15:00"].map(|at| Some(trades::at(at)))
    );
    let quoted = |at: &str| Some(trades::at(at) - SignedDuration::from_nanos(1));
    assert_eq!(
        times(batch, "ts_closed"),
        [quoted("13:01"), quoted("14:01"), None]
    );
    assert_eq!(
        numbers(batch, "avg_px_open"),
        [Some(620.65), Some(600.55), Some(610.65)]
    );
    assert_eq!(
        numbers(batch, "avg_px_close"),
        [Some(599.96), Some(610.06), None]
    );
    assert_eq!(numbers(batch, "peak_qty"), [Some(10.0); 3]);
    assert_eq!(
        numbers(batch, "realized_pnl_amount"),
        [Some(-206.9), Some(-95.1), Some(0.0)]
    );
    assert_eq!(strings(batch, "realized_pnl_currency"), some(&["EUR"; 3]));
    assert_eq!(
        numbers(batch, "r_amount"),
        [Some(31.0), Some(30.0), Some(30.5)]
    );
    assert_eq!(strings(batch, "r_currency"), some(&["EUR"; 3]));
    assert_eq!(numbers(batch, "expected_r"), [None; 3]);
    assert_eq!(
        numbers(batch, "entry_bid"),
        [Some(620.55), Some(600.55), Some(610.55)]
    );
    assert_eq!(
        numbers(batch, "entry_ask"),
        [Some(620.65), Some(600.65), Some(610.65)]
    );
    let ids = strings(batch, "position_id");
    let open = ids[2].clone().unwrap();
    assert!(
        ids[..2]
            .iter()
            .all(|id| id.as_ref().unwrap().starts_with(&format!("{open}-"))),
        "{ids:?}"
    );
    let summed: f64 = numbers(batch, "realized_pnl_amount")
        .into_iter()
        .flatten()
        .sum();
    assert!((summed - total).abs() < 1e-9, "{summed} against {total}");
}

/// The command writes the file through Nautilus' Parquet writer, which Nautilus' reader reads
/// back, and names it after the report.
#[rstest]
fn a_backtest_given_a_trades_file_writes_its_positions_there_and_says_so() {
    let catalog = trades::catalog(&LEVELS);
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("ema.toml");
    std::fs::write(&config, trades::ema_toml("")).unwrap();
    let path = dir.path().join("out").join("trades.parquet");

    let output = Command::new(env!("CARGO_BIN_EXE_helix-trader"))
        .args(["backtest", "ema", "--config"])
        .arg(&config)
        .arg("--catalog")
        .arg(catalog.path())
        .args(["--start", &trades::start().to_string()])
        .args(["--end", &trades::end(&LEVELS).to_string()])
        .args([
            "--balance",
            "10000 EUR",
            "--commission",
            "0 EUR",
            "--trades",
        ])
        .arg(&path)
        .output()
        .unwrap();

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains(&format!(
            "Trades: 3 positions written to {}",
            path.display()
        )),
        "{stdout}"
    );
    let batch = read(&path);
    assert_eq!(
        numbers(&batch, "realized_pnl_amount"),
        [Some(-206.9), Some(-95.1), Some(0.0)]
    );
    assert_eq!(
        numbers(&batch, "r_amount"),
        [Some(31.0), Some(30.0), Some(30.5)]
    );
}

fn read(path: &Path) -> RecordBatch {
    let dir = path.parent().unwrap().to_str().unwrap().to_string();
    let name = path.file_name().unwrap().to_str().unwrap().to_string();
    let (batches, _) = block_on_nautilus_with(move || async move {
        let (store, _, _) = create_object_store_from_path(&dir, None)?;
        read_parquet_from_object_store(store, &ObjectPath::from(name)).await
    })
    .unwrap();
    let [batch] = <[RecordBatch; 1]>::try_from(batches).unwrap();
    batch
}
