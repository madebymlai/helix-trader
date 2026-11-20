//! Tests that on simulated IC Markets the EMA's second cross opens a short beside the still-open
//! long. Each closes on its own 4-hour hold alert (`common::trades`). The moves are too small to
//! reach a stop loss (50 bps) or take profit (100 bps).
//!
//! The 32 USD risk budget over a stop about 3.10 USD away sizes each at 10 ounces.
//!
//! - 13:00 at 610.60: cross up, buy 10 at 610.65.
//! - 14:00 at 608.60: cross down, sell 10 at 608.55.
//! - 17:00: the long's hold sells at the bid 609.55, for -11.00.
//! - 18:00: the short's hold buys at the ask 607.65, for +9.00.
//!
//! On a netting venue the sell at 14:00 would have closed the long there.

use helix_trader::{
    backtest::{Backtest, Commission},
    strategy::{Series, ema::EmaCross},
};
use nautilus_model::{data::BarType, identifiers::InstrumentId, types::Money};
use rstest::rstest;

use crate::common::{self, ic, trades};

const LEVELS: [f64; 7] = [610.0, 608.0, 608.0, 608.0, 609.0, 607.0, 608.0];

#[rstest]
fn on_a_hedging_venue_overlapping_trades_are_two_positions_each_closed_by_its_hold() {
    let instrument_id = InstrumentId::from(ic::INSTRUMENT_ID);
    let mut quotes = trades::quotes(&LEVELS);
    for quote in &mut quotes {
        quote.instrument_id = instrument_id;
    }
    let end = trades::end(&LEVELS);
    let catalog = ic::catalog_of(quotes, trades::at("09:00"), end);
    let backtest = Backtest {
        start: trades::start(),
        end,
        balance: Money::from("10000 USD"),
        commission: Commission::PerOrder(Money::from("0 USD")),
        ..ic::backtest(&catalog)
    };
    let mut config = common::ema_config(1, 2, Some(common::session()));
    config.data.series = vec![Series::Bars(BarType::from(ic::HOUR_BARS))];
    config.trade.instruments[0].instrument_id = instrument_id;

    let result = backtest.run(EmaCross::new(config).unwrap()).unwrap().result;

    // Two brackets of 3 orders, two closes
    assert_eq!(result.total_orders, 8);
    assert_eq!(result.total_positions, 2);
    let pnl = &result.stats_pnls["USD"];
    assert_eq!(pnl["Max Loser"], -11.0);
    assert_eq!(pnl["Max Winner"], 9.0);
    assert_eq!(
        result.summary["account.ICMARKETS.balance.USD.total"],
        "9998.00 USD"
    );
}
