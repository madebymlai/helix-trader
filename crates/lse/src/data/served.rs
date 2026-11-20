//! What the client serves of the past, checked before a fill or warmup requests it: an instrument's
//! series (`serves`) and custom data rows (`serves_custom`). They are the twins of
//! `streams::streams` and `streams::streams_custom`.

use std::str::FromStr;

use anyhow::Context;
use nautilus_model::{
    data::{BarType, CustomDataTrait, DataType, NautilusDataType},
    identifiers::InstrumentId,
};

use crate::{
    common::{
        consts::{LSEDGE_VENUE, LseExchange, OPRA_VENUE},
        symbol::LseSymbol,
    },
    http::{query::CandleWidth, rows::RowSource},
    types::{LseRow, SOURCE, SYMBOL},
};

/// Checks that the client serves the history of `series`, a Nautilus data type and its catalog
/// identifier (an instrument id, or a bar type for bars).
///
/// It checks what a request checks before LSE is asked:
/// - Quotes, trades, book depths and book deltas of an `LSEDGE`, `GLBX`, `XNAS` or `OPRA`
///   instrument, except the trades of an `LSEDGE` index, whose ticks are quotes.
/// - Bars, from LSE's candles (`CandleWidth::of`), of an `LSEDGE` series or `OPRA` contract.
///
/// Custom data is judged apart (`serves_custom`). What LSE itself refuses is not checked here:
/// the quotes of an `LSEDGE` index or an `OPRA` contract, and the book of either.
///
/// # Errors
///
/// Returns an error if the client does not serve the series, naming it and saying why.
pub fn serves(series: &(NautilusDataType, String)) -> anyhow::Result<()> {
    let (data, identifier) = series;
    if *data == NautilusDataType::Bar {
        let bar_type = BarType::from_str(identifier)
            .with_context(|| format!("the bars {identifier}, no bar type"))?;
        CandleWidth::of(bar_type)?;
        let venue = bar_type.instrument_id().venue;
        anyhow::ensure!(
            venue == *LSEDGE_VENUE || venue == *OPRA_VENUE,
            "{bar_type}: the client asks LSE for the candles of LSEDGE series and OPRA \
             contracts alone"
        );
        return Ok(());
    }
    let id = InstrumentId::from_str(identifier)
        .with_context(|| format!("the {data} of {identifier}, no instrument id"))?;
    let venue = id.venue;
    anyhow::ensure!(
        venue == *LSEDGE_VENUE || venue == *OPRA_VENUE || LseExchange::of_venue(venue).is_some(),
        "LSE serves nothing of {id}: {venue} is none of its venues, {LSEDGE}, GLBX, XNAS and \
         OPRA",
        LSEDGE = *LSEDGE_VENUE
    );
    match data {
        NautilusDataType::TradeTick
            if venue == *LSEDGE_VENUE && LseSymbol::from_instrument_id(id)?.is_index() =>
        {
            anyhow::bail!(
                "{id} is an index, which has no trades: its ticks are quotes, its candles bars"
            )
        }
        NautilusDataType::QuoteTick
        | NautilusDataType::TradeTick
        | NautilusDataType::OrderBookDepth
        | NautilusDataType::OrderBookDelta => Ok(()),
        other => anyhow::bail!(
            "LSE's history serves no {other} of {id}: an instrument's quotes, trades, book \
             depths and book deltas, and its candles as bars"
        ),
    }
}

/// Checks that the client serves `data_type`, custom data of no instrument, as its request checks
/// it before LSE is asked (`row_source`).
///
/// It serves LSE's rows of a source and a symbol, under the identifier the data type names, which
/// keys them in a catalog.
///
/// # Errors
///
/// Returns an error if the client does not serve the data type, naming it and saying why (see
/// `row_source`).
pub fn serves_custom(data_type: &DataType) -> anyhow::Result<()> {
    row_source(data_type).map(drop)
}

/// Returns the rows `data_type` asks for: an `LseRow` data type whose metadata names a `source` and
/// a `symbol` string, and no other key (see `RowSource::of`).
///
/// The identifier is the caller's: the rows are stored under the data type as asked.
///
/// # Errors
///
/// Returns an error if the data type, which it names, is not `LseRow`'s, if its metadata names no
/// source or symbol, a key not a string, or another key, or if `RowSource::of` fails.
pub(super) fn row_source(data_type: &DataType) -> anyhow::Result<RowSource> {
    let rows = LseRow::type_name_static();
    anyhow::ensure!(
        data_type.type_name() == rows,
        "LSE serves no custom data {}: only its rows, {rows}",
        data_type.type_name()
    );
    let named = |key| {
        data_type
            .metadata()
            .and_then(|metadata| metadata.get_str(key))
            .with_context(|| format!("{data_type:?} names no {key}, a string, in its metadata"))
    };
    let (source, symbol) = (named(SOURCE)?, named(SYMBOL)?);

    if let Some(other) = data_type.metadata().and_then(|metadata| {
        metadata
            .borrowed_entries()
            .map(|(key, _)| key)
            .find(|key| ![SOURCE, SYMBOL].contains(&key.as_str()))
    }) {
        anyhow::bail!(
            "{data_type:?} names {other} in its metadata: LSE's rows are asked by a {SOURCE} \
             and a {SYMBOL} alone"
        );
    }
    RowSource::of(source, symbol)
}

#[cfg(test)]
mod tests {
    use nautilus_core::Params;
    use nautilus_model::data::DataType;
    use rstest::rstest;
    use serde_json::json;

    use super::*;

    /// Under the identifier `RATES`, which no metadata derives.
    fn custom(type_name: &str, metadata: &[(&str, &str)]) -> DataType {
        let metadata = metadata
            .iter()
            .map(|&(key, value)| (key.to_string(), json!(value)))
            .collect();
        DataType::new(type_name, Some(Params::new(metadata)), Some("RATES".into()))
    }

    #[rstest]
    #[case::a_collection_s("bond_yields", "US10Y")]
    #[case::the_economic_calendar_s("economic_calendar", "US.Core Inflation Rate YoY")]
    #[case::macro_releases("macro_release", "US.Core Inflation Rate YoY")]
    #[case::macro_yields("macro_yield", "US10Y")]
    #[case::an_instrument_s_dividends("factors_dividends", "AAPL-US_STOCKS")]
    fn lse_s_rows_of_a_source_and_a_symbol_are_served(#[case] source: &str, #[case] symbol: &str) {
        serves_custom(&custom("LseRow", &[("source", source), ("symbol", symbol)])).unwrap();
    }

    #[rstest]
    #[case::another_type("Imbalance", &[("source", "bond_yields"), ("symbol", "US10Y")], "Imbalance")]
    #[case::no_source("LseRow", &[("symbol", "US10Y")], "source")]
    #[case::no_symbol("LseRow", &[("source", "bond_yields")], "symbol")]
    #[case::another_key(
        "LseRow",
        &[("source", "bond_yields"), ("symbol", "US10Y"), ("market", "bonds")],
        "market"
    )]
    #[case::the_options_chain("LseRow", &[("source", "options_chain"), ("symbol", "AAPL")], "options_chain")]
    #[case::macro_observations(
        "LseRow",
        &[("source", "macro_observation"), ("symbol", "US.core_inflation_rate")],
        "macro_release"
    )]
    #[case::a_source_unknown("LseRow", &[("source", "no_such_collection"), ("symbol", "US10Y")], "no_such_collection")]
    #[case::factors_of_no_market("LseRow", &[("source", "factors_splits"), ("symbol", "AAPL")], "AAPL")]
    fn custom_data_the_client_does_not_ask_lse_for_is_refused_naming_why(
        #[case] type_name: &str,
        #[case] metadata: &[(&str, &str)],
        #[case] why: &str,
    ) {
        let err = serves_custom(&custom(type_name, metadata))
            .unwrap_err()
            .to_string();

        assert!(err.contains(why), "{err}");
    }
    /// The client asks LSE for no other data, funding say: each is refused naming data and series.
    #[rstest]
    #[case::of_an_lsedge_series("GOLD-FUTURES.LSEDGE")]
    #[case::of_an_lsedge_index("SPX-INDICES.LSEDGE")]
    #[case::of_a_cme_globex_contract("GCZ26.GLBX")]
    #[case::of_a_nasdaq_stock("NVDA.XNAS")]
    #[case::of_an_option_contract("AAPL  261120C00355000.OPRA")]
    fn other_data_of_an_instrument_is_refused_naming_the_data_and_the_series(
        #[case] identifier: &str,
    ) {
        let funding = NautilusDataType::FundingRateUpdate;

        let err = serves(&(funding, identifier.to_string()))
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("FundingRateUpdate") && err.contains(identifier),
            "{err}"
        );
    }
}
