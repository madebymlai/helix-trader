//! What the client serves of the past (`check_served`), as a fill or a
//! warmup asks before it requests, the twin of what it streams
//! (`streams::streams`).

use std::str::FromStr;

use anyhow::{Context, bail, ensure};
use nautilus_model::{
    data::{BarType, NautilusDataType},
    identifiers::InstrumentId,
};

use crate::{
    common::{
        consts::{LSEDGE_VENUE, LseExchange, OPRA_VENUE},
        symbol::LseSymbol,
    },
    http::query::CandleWidth,
};

/// Checks that the client serves the history of `series`, a Nautilus data
/// type and its catalog identifier, an instrument id, or a bar type for
/// bars, as its requests check what they are asked before LSE is: the
/// quotes, trades, book depths and book deltas of an instrument of a venue
/// it serves, `LSEDGE`, `GLBX`, `XNAS` or `OPRA` (`request_quotes`,
/// `request_trades`, `request_book_depth`, `request_book_deltas`), but the
/// trades of an `LSEDGE` index, which has none, its ticks being quotes; and
/// the bars of LSE's candles (`CandleWidth::of`) of an `LSEDGE` series or
/// an `OPRA` contract (`request_bars`). What LSE refuses once asked is its
/// own answer, not checked here: the quotes of an `LSEDGE` series but an
/// index's and of an `OPRA` contract, and the book of either.
///
/// # Errors
///
/// Returns an error naming the series and saying why if the client does
/// not serve it.
pub fn check_served(series: &(NautilusDataType, String)) -> anyhow::Result<()> {
    let (data, identifier) = series;
    if *data == NautilusDataType::Bar {
        let bar_type = BarType::from_str(identifier)
            .with_context(|| format!("the bars {identifier}, no bar type"))?;
        CandleWidth::of(bar_type)?;
        let venue = bar_type.instrument_id().venue;
        ensure!(
            venue == *LSEDGE_VENUE || venue == *OPRA_VENUE,
            "{bar_type}: the client asks LSE for the candles of LSEDGE series and OPRA \
             contracts alone"
        );
        return Ok(());
    }
    let id = InstrumentId::from_str(identifier)
        .with_context(|| format!("the {data} of {identifier}, no instrument id"))?;
    let venue = id.venue;
    ensure!(
        venue == *LSEDGE_VENUE || venue == *OPRA_VENUE || LseExchange::of_venue(venue).is_some(),
        "LSE serves nothing of {id}: {venue} is none of its venues, {LSEDGE}, GLBX, XNAS and \
         OPRA",
        LSEDGE = *LSEDGE_VENUE
    );
    match data {
        NautilusDataType::TradeTick
            if venue == *LSEDGE_VENUE && LseSymbol::from_instrument_id(id)?.is_index() =>
        {
            bail!("{id} is an index, which has no trades: its ticks are quotes, its candles bars")
        }
        NautilusDataType::QuoteTick
        | NautilusDataType::TradeTick
        | NautilusDataType::OrderBookDepth
        | NautilusDataType::OrderBookDelta => Ok(()),
        other => bail!(
            "LSE's history serves no {other} of {id}: an instrument's quotes, trades, book \
             depths and book deltas, and its candles as bars"
        ),
    }
}
