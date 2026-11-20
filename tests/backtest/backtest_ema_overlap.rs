//! On simulated IC Markets the EMA's second cross opens a short beside its still-open long, each
//! closing on its own 4-hour hold alert (`common::trades`). The moves are too small to reach a
//! stop loss (50 bps) or take profit (100 bps).

use helix_trader::{
    backtest::{Backtest, Commission},
    strategy::{Series, ema::EmaCross},
};
use nautilus_model::{data::BarType, identifiers::InstrumentId, types::Money};
use rstest::rstest;

use crate::common::{self, ic, trades};

/// With 10,000 USD and no fees the risk budget is 32 USD: a stop about 3.10 USD from entry sizes
/// the long at 10 ounces, and the short still rounds down to 10 after the long's unrealized loss.
/// - 13:00 at 610.60 > 605.60: cross up, buy 10 at 610.65, stop loss 607.55, take profit 616.71
///   (610.60 less 0.5%, plus 1%, to the cent); slow EMA 608.93.
/// - 14:00 at 608.60 < 608.93: cross down, sell 10 at 608.55, stop loss 611.64, take profit 602.51
///   (608.60 plus 0.5%, less 1%); slow EMA 608.71.
/// - 15:00 and 16:00 at 608.60 < slow EMA (608.71, 608.64): no cross; 17:00 to 19:00 are out of the
///   session.
/// - Bids 13:01 to 17:00, 607.96 to 609.55, and asks 14:01 to 18:00, 607.06 to 609.65, touch no
///   exit.
/// - 17:00, 4 hours after the buy: the long sells 10 at the bid, 609.00 + 0.60 − 0.05 = 609.55:
///   −11.00.
/// - 18:00, 4 hours after the sell: the short buys 10 at the ask, 607.00 + 0.60 + 0.05 = 607.65:
///   +9.00.
///
/// On a netting venue the sell at 14:00 would have closed the long there.
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
