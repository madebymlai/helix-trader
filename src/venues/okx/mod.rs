//! Provides OKX history and the live node's clients. Nautilus' OKX data client fills the history
//! through the node (see `catalog::NodeFill`). The live clients are in `live`. Both key an
//! instrument by its contract (`Contract`). See `README.md`.
//!
//! The node asks `OKXDataClient` for the bars and funding the catalog lacks (`requests`). It does
//! not ask for the definitions, because of their margins (see `README.md`). `OkxHistory` writes
//! them from OKX's public REST API through `OKXRawHttpClient` and `parse_instrument_any`. Their
//! margins are 1 / the account leverage (see `instrument_from`, `account`).

use anyhow::Context;
use jiff::SignedDuration;
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    data::{BarType, NautilusDataType},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_okx::{
    common::{
        consts::OKX_VENUE,
        enums::OKXInstrumentType,
        models::OKXInstrument,
        parse::{
            bar_spec_as_okx_channel, extract_inst_family, is_xperp_rule_type,
            okx_instrument_type_from_symbol, parse_instrument_any,
        },
    },
    config::OKXDataClientConfig,
    factories::OKXDataClientFactory,
    http::{client::OKXHttpClient, query::GetInstrumentsParams},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rust_decimal::Decimal;

use self::account::Account;
use super::{HistorySource, Row};
use crate::{
    catalog::{FillData, FillRequest, NodeClient, missing},
    strategy::requirements::{CatalogSeries, SeriesSlice, TickSeries},
};

mod account;
mod live;
pub(in crate::venues) use live::BOOK;
pub(crate) use live::{clients, streams};

/// Two weeks, which is 336 hourly funding times and fits the client's one page of 400.
///
/// The oldest update of each window comes without its interval, which a backtest does not settle,
/// so the windows are long.
const FUNDING_PERIOD: SignedDuration = SignedDuration::from_hours(14 * 24);

/// One day: 1,440 minute bars, or 15 of the client's history pages of 100.
const DAY: SignedDuration = SignedDuration::from_hours(24);

/// Reads instrument definitions from OKX's public REST API, through the adapter's keyless client.
#[derive(Debug, Default)]
pub struct OkxHistory {
    client: OKXHttpClient,
}

impl OkxHistory {
    /// Writes into `catalog` the definition of each `slices` instrument that it lacks.
    ///
    /// Each definition is as OKX defines it now, with the account's current leverage. The account
    /// is read only if a definition is lacking.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - An instrument is not an OKX swap or X-Perp.
    /// - OKX does not define an instrument.
    /// - The account cannot be read (environment or key missing, OKX unreachable or refusing it)
    ///   or lists no leverage.
    /// - The catalog cannot be read or written.
    pub async fn write_definitions(
        &self,
        catalog: &ParquetDataCatalog,
        slices: &[SeriesSlice],
    ) -> anyhow::Result<()> {
        let mut account: Option<Account> = None;
        write_definitions(catalog, slices, async |id| {
            if account.is_none() {
                account = Some(Account::of_environment().context(
                    "an OKX definition holds the account's margin, read from the leverage it \
                     trades the instrument at",
                )?);
            }
            let leverage = account.as_ref().expect("read above").leverage(id).await?;
            self.instrument(id, leverage).await
        })
        .await
    }

    async fn instrument(
        &self,
        id: InstrumentId,
        leverage: Decimal,
    ) -> anyhow::Result<InstrumentAny> {
        let contract = Contract::of(id)?;
        let raw = self
            .client
            .raw_client()
            .get_instruments(GetInstrumentsParams {
                inst_type: contract.inst_type,
                inst_id: Some(id.symbol.to_string()),
                ..Default::default()
            })
            .await
            .with_context(|| format!("requesting the instrument {id} from OKX"))?;
        let [raw] = raw.as_slice() else {
            anyhow::bail!("OKX returned {} instruments for {id}, not one", raw.len());
        };
        instrument_from(raw, leverage, get_atomic_clock_realtime().get_time_ns())
    }
}

async fn write_definitions(
    catalog: &ParquetDataCatalog,
    slices: &[SeriesSlice],
    mut instrument: impl AsyncFnMut(InstrumentId) -> anyhow::Result<InstrumentAny>,
) -> anyhow::Result<()> {
    let mut ids: Vec<InstrumentId> = Vec::new();

    for slice in slices {
        let id = slice.series.instrument_id();
        if !ids.contains(&id) {
            ids.push(id);
        }
    }
    let names: Vec<String> = ids.iter().map(ToString::to_string).collect();
    let stored: Vec<InstrumentId> = catalog
        .instruments(Some(&names), None, None)
        .context("reading stored instruments")?
        .iter()
        .map(Instrument::id)
        .collect();
    let mut new = Vec::new();
    for id in ids.into_iter().filter(|id| !stored.contains(id)) {
        new.push(instrument(id).await?);
    }
    catalog
        .write_instruments(new)
        .context("writing instruments")?;
    Ok(())
}

/// Checks that the OKX data client serves the history of `source`.
///
/// The client serves trades (`request_trades`) and candles (`request_bars`). Candles are EXTERNAL
/// bars of the LAST price, at a width `bar_spec_as_okx_channel` accepts. The client asks for
/// trade-price candles whatever price a bar type names. It requests no quotes or book
/// (`DataClient` defaults refuse them).
///
/// # Errors
///
/// Returns an error if `source` is not served.
pub(super) fn serves(source: &CatalogSeries) -> anyhow::Result<()> {
    match source {
        CatalogSeries::Bars(bar_type) => anyhow::ensure!(
            is_candle(bar_type),
            "OKX's history serves its candles, EXTERNAL bars of the LAST price 1, 3, 5, 15 or 30 \
             minutes, 1, 2, 4, 6 or 12 hours, 1, 2, 3 or 5 days, a week or 1, 3, 6 or 12 months \
             wide, or 1 second, and its trades, not {bar_type}"
        ),
        CatalogSeries::Ticks(TickSeries {
            data: NautilusDataType::TradeTick,
            ..
        }) => {}
        CatalogSeries::Ticks(ticks) => {
            anyhow::bail!("OKX's history serves its trades and its candles, not the {ticks}")
        }
    }
    Ok(())
}

/// Returns whether `bar_type` is EXTERNAL with a spec `bar_spec_as_okx_channel` accepts.
///
/// `request_bars` and `subscribe_bars` serve such bars alike.
fn is_candle(bar_type: &BarType) -> bool {
    bar_type.is_externally_aggregated() && bar_spec_as_okx_channel(bar_type.spec()).is_ok()
}

/// Returns what `catalog` lacks for `slices` and for their instruments' funding.
///
/// Funding comes first. The definitions are left out (see the module doc).
///
/// # Errors
///
/// Returns an error if `catalog::missing` fails.
pub fn requests(
    catalog: &ParquetDataCatalog,
    slices: &[SeriesSlice],
) -> anyhow::Result<Vec<FillRequest>> {
    let funding = slices.iter().map(|of| SeriesSlice {
        series: CatalogSeries::Ticks(TickSeries {
            data: NautilusDataType::FundingRateUpdate,
            instrument_id: of.series.instrument_id(),
        }),
        from: of.from,
        to: of.to,
    });
    let slices = SeriesSlice::merged(funding.chain(slices.iter().cloned()));
    Ok(missing(catalog, &slices)?
        .into_iter()
        .filter(|request| !matches!(request.data, FillData::Instrument(_)))
        .collect())
}

/// Returns the window of one request for `data` (see `NodeFill::add`).
#[must_use]
pub fn period(data: FillData) -> SignedDuration {
    match data {
        FillData::Funding(_) => FUNDING_PERIOD,
        _ => DAY,
    }
}

/// Returns the node's data client, which uses OKX's public API with no key.
///
/// # Errors
///
/// Returns an error if an id is not an OKX swap or X-Perp.
pub fn node_client(instrument_ids: &[InstrumentId]) -> anyhow::Result<NodeClient> {
    let (instrument_types, families) = selection(instrument_ids)?;
    Ok(NodeClient {
        factory: Box::new(OKXDataClientFactory::new()),
        config: Box::new(OKXDataClientConfig {
            instrument_types,
            instrument_families: Some(families),
            ..Default::default()
        }),
    })
}

fn selection(
    instrument_ids: &[InstrumentId],
) -> anyhow::Result<(Vec<OKXInstrumentType>, Vec<String>)> {
    let mut instrument_types = Vec::new();
    let mut families = Vec::new();
    for &id in instrument_ids {
        let contract = Contract::of(id)?;
        if !instrument_types.contains(&contract.inst_type) {
            instrument_types.push(contract.inst_type);
        }
        let family = contract.family.to_string();
        if !families.contains(&family) {
            families.push(family);
        }
    }
    Ok((instrument_types, families))
}

/// Returns the margin as a fraction of notional: 1 / `leverage`.
///
/// # Errors
///
/// Returns an error if `leverage` is not positive.
fn margin_at(leverage: Decimal) -> anyhow::Result<Decimal> {
    anyhow::ensure!(
        leverage > Decimal::ZERO,
        "an OKX leverage must be positive, was {leverage}"
    );
    Ok(Decimal::ONE / leverage)
}

/// Refuses a dated future, since only a future with rule type `xperp` is served.
///
/// OKX locks notional / leverage. That is Nautilus' default margin model (notional / leverage x the
/// instrument's margin) at a backtest leverage of 1. The adapter would leave the margins at 0,
/// which locks nothing.
fn instrument_from(
    raw: &OKXInstrument,
    leverage: Decimal,
    ts_init: UnixNanos,
) -> anyhow::Result<InstrumentAny> {
    let id = raw.inst_id;
    if raw.inst_type == OKXInstrumentType::Futures {
        anyhow::ensure!(
            is_xperp_rule_type(&raw.rule_type),
            "{id} is a dated future (rule type {:?}); of OKX's futures only X-Perps are served",
            raw.rule_type
        );
    }
    let margin = margin_at(leverage)?;
    parse_instrument_any(raw, Some(margin), Some(margin), ts_init)?
        .with_context(|| format!("{id} is not an instrument Nautilus supports"))
}

/// An OKX swap (`BTC-USDT-SWAP.OKX`) or future (`BTC-USD_UM_XPERP-310404.OKX`).
#[derive(Debug, Clone, Copy)]
struct Contract {
    inst_type: OKXInstrumentType,
    /// `BTC-USDT` or `BTC-USD_UM_XPERP`.
    family: &'static str,
}

impl Contract {
    fn of(id: InstrumentId) -> anyhow::Result<Self> {
        anyhow::ensure!(
            Row::of(id.venue).history == HistorySource::Okx,
            "{id} is not on {}",
            *OKX_VENUE
        );
        let symbol = id.symbol.inner().as_str();
        let inst_type = okx_instrument_type_from_symbol(symbol);
        anyhow::ensure!(
            matches!(
                inst_type,
                OKXInstrumentType::Swap | OKXInstrumentType::Futures
            ),
            "{id} is not an OKX swap (BTC-USDT-SWAP.OKX) or X-Perp \
             (BTC-USD_UM_XPERP-310404.OKX)"
        );
        let family = extract_inst_family(symbol)
            .with_context(|| format!("{id} has no instrument family"))?;
        Ok(Self {
            inst_type,
            family: family.as_str(),
        })
    }
}

#[cfg(test)]
mod tests {
    use jiff::Timestamp;
    use nautilus_model::data::{BarType, FundingRateUpdate};
    use rstest::rstest;

    use super::*;
    use crate::strategy::requirements::DataRequirements;

    const INSTRUMENT: &str = include_str!("test_data/BTC-USDT-SWAP-instrument.json");

    fn raw_instrument() -> OKXInstrument {
        serde_json::from_str(INSTRUMENT).unwrap()
    }

    /// Returns BTC-USDT-SWAP.OKX as OKX defined it on 2026-10-21, at leverage 10.
    fn btc_instrument() -> InstrumentAny {
        instrument_from(&raw_instrument(), Decimal::TEN, UnixNanos::default()).unwrap()
    }

    const XPERP_INSTRUMENT: &str =
        include_str!("test_data/BTC-USD_UM_XPERP-310404-instrument.json");

    fn raw_xperp() -> OKXInstrument {
        serde_json::from_str(XPERP_INSTRUMENT).unwrap()
    }

    /// Returns BTC-USD_UM_XPERP-310404.OKX as OKX defined it on 2026-10-22, at leverage 25.
    fn xperp_instrument() -> InstrumentAny {
        instrument_from(&raw_xperp(), Decimal::from(25), UnixNanos::default()).unwrap()
    }

    #[rstest]
    #[case::a_swap_a_crypto_perpetual(btc_instrument(), "CryptoPerpetual", (1, 2), Decimal::new(1, 1))]
    #[case::an_x_perp_a_crypto_future(xperp_instrument(), "CryptoFuture", (1, 0), Decimal::new(4, 2))]
    fn an_okx_swap_or_x_perp_is_a_crypto_instrument_with_the_account_s_margins(
        #[case] instrument: InstrumentAny,
        #[case] kind: &str,
        #[case] precisions: (u8, u8),
        #[case] margin: Decimal,
    ) {
        let of_kind = match &instrument {
            InstrumentAny::CryptoPerpetual(_) => "CryptoPerpetual",
            InstrumentAny::CryptoFuture(_) => "CryptoFuture",
            _ => "another kind",
        };
        assert_eq!(of_kind, kind);
        assert_eq!(
            (instrument.price_precision(), instrument.size_precision()),
            precisions
        );
        assert_eq!(
            (instrument.margin_init(), instrument.margin_maint()),
            (margin, margin)
        );
    }

    #[rstest]
    #[case::nought(Decimal::ZERO, "0")]
    #[case::negative(Decimal::NEGATIVE_ONE, "-1")]
    fn a_leverage_not_positive_is_refused_naming_it(
        #[case] leverage: Decimal,
        #[case] named: &str,
    ) {
        let err = instrument_from(&raw_instrument(), leverage, UnixNanos::default())
            .unwrap_err()
            .to_string();
        assert!(err.contains(&format!("was {named}")), "{err}");
    }

    #[rstest]
    fn a_dated_future_is_refused_naming_it_and_its_rule_type() {
        let mut dated = raw_xperp();
        dated.rule_type = "normal".to_string();
        let err = instrument_from(&dated, Decimal::TEN, UnixNanos::default())
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("BTC-USD_UM_XPERP-310404") && err.contains("\"normal\""),
            "{err}"
        );
    }

    #[rstest]
    fn the_type_and_family_of_an_okx_swap_or_future_key_its_clients() {
        let swap = Contract::of(btc()).unwrap();
        assert_eq!(
            (swap.inst_type, swap.family),
            (OKXInstrumentType::Swap, "BTC-USDT")
        );
        let future = Contract::of(xperp()).unwrap();
        assert_eq!(
            (future.inst_type, future.family),
            (OKXInstrumentType::Futures, "BTC-USD_UM_XPERP")
        );
    }

    #[rstest]
    #[case::a_spot_pair("BTC-USDT.OKX")]
    #[case::an_option("BTC-USD-261225-100000-C.OKX")]
    #[case::a_swap_of_another_venue("BTC-USDT-SWAP.BINANCE")]
    fn an_id_not_of_an_okx_swap_or_future_has_no_contract_naming_it(#[case] id: &str) {
        let err = Contract::of(InstrumentId::from(id))
            .unwrap_err()
            .to_string();
        assert!(err.contains(id), "{err}");
    }

    fn btc() -> InstrumentId {
        InstrumentId::from("BTC-USDT-SWAP.OKX")
    }

    fn xperp() -> InstrumentId {
        InstrumentId::from("BTC-USD_UM_XPERP-310404.OKX")
    }

    const BTC_MINUTES: &str = "BTC-USDT-SWAP.OKX-1-MINUTE-LAST-EXTERNAL";

    fn at(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    fn new_catalog(dir: &tempfile::TempDir) -> ParquetDataCatalog {
        ParquetDataCatalog::new(dir.path(), None, None, None, None)
    }

    fn fill(data: FillData, from: &str, to: &str) -> FillRequest {
        FillRequest {
            data,
            from: at(from),
            to: at(to),
        }
    }

    fn btc_slices(from: &str, to: &str) -> Vec<SeriesSlice> {
        DataRequirements {
            bar_types: vec![
                BarType::from("BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"),
                BarType::from("BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@5-MINUTE-EXTERNAL"),
            ],
            ticks: vec![],
            custom: vec![],
            warmup: SignedDuration::ZERO,
        }
        .slices(at(from), at(to))
    }

    #[rstest]
    fn an_instrument_s_funding_is_asked_once_before_its_bars_and_its_definition_not_at_all() {
        let dir = tempfile::tempdir().unwrap();
        let (from, to) = ("2026-10-01T00:00:00Z", "2026-10-08T00:00:00Z");

        let asked = requests(&new_catalog(&dir), &btc_slices(from, to)).unwrap();

        assert_eq!(
            asked,
            [
                fill(FillData::Funding(btc()), from, to),
                fill(FillData::Bars(BarType::from(BTC_MINUTES)), from, to),
                fill(
                    FillData::Bars(BarType::from("BTC-USDT-SWAP.OKX-5-MINUTE-LAST-EXTERNAL")),
                    from,
                    to
                ),
            ]
        );
    }

    /// Funding stored through October 4 does not cover either bar series. The next funding request
    /// still starts on October 5, not October 1.
    #[rstest]
    fn stored_funding_shortens_only_the_funding_request() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = new_catalog(&dir);
        let (from, to) = ("2026-10-01T00:00:00Z", "2026-10-08T00:00:00Z");
        let update = at("2026-10-03T08:00:00Z").into();
        catalog
            .write_to_parquet(
                &[FundingRateUpdate::new(
                    btc(),
                    Decimal::new(1, 4),
                    Some(480),
                    None,
                    update,
                    update,
                )],
                Some(at(from).into()),
                Some(at("2026-10-04T23:59:59.999999999Z").into()),
                None,
            )
            .unwrap();

        let asked = requests(&catalog, &btc_slices(from, to)).unwrap();

        assert_eq!(
            asked,
            [
                fill(FillData::Funding(btc()), "2026-10-05T00:00:00Z", to),
                fill(FillData::Bars(BarType::from(BTC_MINUTES)), from, to),
                fill(
                    FillData::Bars(BarType::from("BTC-USDT-SWAP.OKX-5-MINUTE-LAST-EXTERNAL")),
                    from,
                    to
                ),
            ]
        );
    }

    #[rstest]
    fn funding_is_asked_in_two_weeks_and_bars_in_days() {
        assert_eq!(
            period(FillData::Funding(btc())),
            SignedDuration::from_hours(336)
        );
        assert_eq!(
            period(FillData::Bars(BarType::from(BTC_MINUTES))),
            SignedDuration::from_hours(24)
        );
    }

    #[tokio::test]
    async fn each_definition_the_catalog_lacks_is_asked_once_and_written() {
        let dir = tempfile::tempdir().unwrap();
        let catalog = new_catalog(&dir);
        catalog.write_instruments(vec![btc_instrument()]).unwrap();
        let xperp_minutes = "BTC-USD_UM_XPERP-310404.OKX-1-MINUTE-LAST-EXTERNAL";
        let slices: Vec<SeriesSlice> = btc_slices("2026-10-01T00:00:00Z", "2026-10-08T00:00:00Z")
            .into_iter()
            .chain(
                DataRequirements {
                    bar_types: vec![
                        BarType::from(
                            "BTC-USD_UM_XPERP-310404.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL",
                        ),
                        BarType::from(
                            "BTC-USD_UM_XPERP-310404.OKX-1-HOUR-LAST-INTERNAL@5-MINUTE-EXTERNAL",
                        ),
                    ],
                    ticks: vec![],
                    custom: vec![],
                    warmup: SignedDuration::ZERO,
                }
                .slices(at("2026-10-01T00:00:00Z"), at("2026-10-08T00:00:00Z")),
            )
            .collect();
        assert!(
            slices
                .iter()
                .any(|s| s.series == CatalogSeries::Bars(BarType::from(xperp_minutes)))
        );
        let mut asked = Vec::new();

        write_definitions(&catalog, &slices, async |id| {
            asked.push(id);
            Ok(xperp_instrument())
        })
        .await
        .unwrap();

        assert_eq!(asked, [xperp()]);
        let mut stored: Vec<String> = catalog
            .instruments(None, None, None)
            .unwrap()
            .iter()
            .map(|i| i.id().to_string())
            .collect();
        stored.sort();
        assert_eq!(stored, [btc().to_string(), xperp().to_string()]);
    }
}
