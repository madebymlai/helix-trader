//! OKX: its history, filled through the node (see `catalog::NodeFill`) by
//! Nautilus' OKX data client, and the live node's clients (see `live`),
//! both keying an instrument by its contract (`Contract`). See `README.md`.
//!
//! The node asks Nautilus' `OKXDataClient` for the bars of OKX swaps and
//! X-Perps and for their funding over what the catalog lacks of them
//! (`requests`), and the data engine writes back the windows the client
//! answers in full. The client's answer to a definition request is not
//! written back, so `OkxHistory` writes the definitions, from OKX's public
//! REST API through the adapter's `OKXRawHttpClient` and
//! `parse_instrument_any`, with margins of 1 (see `instrument_from`).

use anyhow::{Context, bail, ensure};
use jiff::SignedDuration;
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};
use nautilus_live::config::RoutingConfig;
use nautilus_model::{
    data::NautilusDataType,
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_okx::{
    common::{
        consts::OKX_VENUE,
        enums::OKXInstrumentType,
        models::OKXInstrument,
        parse::{
            extract_inst_family, is_xperp_rule_type, okx_instrument_type_from_symbol,
            parse_instrument_any,
        },
    },
    config::OKXDataClientConfig,
    factories::OKXDataClientFactory,
    http::{client::OKXHttpClient, query::GetInstrumentsParams},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;
use rust_decimal::Decimal;

use super::HistorySource;
use crate::{
    catalog::{FillData, FillRequest, NodeClient, missing},
    strategy::SeriesSlice,
};

mod live;
pub(crate) use live::clients;

/// The window one funding request holds at most: two weeks, 336 funding
/// times at an hourly interval, within the one page of 400 the client
/// reads. The oldest update of each window comes without the interval
/// before it, which a backtest does not settle, so the windows are long.
const FUNDING_PERIOD: SignedDuration = SignedDuration::from_hours(14 * 24);

/// The window one request of bars holds at most: a UTC day, 1,440 minute
/// bars, 15 of the client's history pages of 100.
const DAY: SignedDuration = SignedDuration::from_hours(24);

/// OKX's public REST API, through the adapter's default client (no key),
/// for the definitions of OKX instruments.
#[derive(Debug, Default)]
pub struct OkxHistory {
    client: OKXHttpClient,
}

impl OkxHistory {
    /// Writes into `catalog` the definition of each instrument of `slices`
    /// it lacks, as OKX's API defines it now (see `instrument_from`).
    ///
    /// # Errors
    ///
    /// Returns an error if an instrument is not an OKX swap or X-Perp, if
    /// OKX does not define it, or if the catalog cannot be read or written.
    pub async fn write_definitions(
        &self,
        catalog: &mut ParquetDataCatalog,
        slices: &[SeriesSlice],
    ) -> anyhow::Result<()> {
        write_definitions(catalog, slices, async |id| self.instrument(id).await).await
    }

    /// The instrument of `id`, an OKX swap or X-Perp (see
    /// `instrument_from`).
    async fn instrument(&self, id: InstrumentId) -> anyhow::Result<InstrumentAny> {
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
            bail!("OKX returned {} instruments for {id}, not one", raw.len());
        };
        instrument_from(raw, get_atomic_clock_realtime().get_time_ns())
    }
}

/// Writes into `catalog` the definition of each instrument of `slices` it
/// lacks, once, as `instrument` gives it.
async fn write_definitions(
    catalog: &mut ParquetDataCatalog,
    slices: &[SeriesSlice],
    mut instrument: impl AsyncFnMut(InstrumentId) -> anyhow::Result<InstrumentAny>,
) -> anyhow::Result<()> {
    let mut ids: Vec<InstrumentId> = Vec::new();
    for SeriesSlice { slice, .. } in slices {
        if !ids.contains(&slice.instrument_id) {
            ids.push(slice.instrument_id);
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

/// What `catalog` lacks of `slices`, series of OKX instruments
/// (`DataRequirements::slices`), and of the funding of each slice's
/// instrument over the slice, the funding first (see `catalog::missing`),
/// without the definitions, which `OkxHistory::write_definitions` writes.
///
/// # Errors
///
/// As `catalog::missing`.
pub fn requests(
    catalog: &ParquetDataCatalog,
    slices: &[SeriesSlice],
) -> anyhow::Result<Vec<FillRequest>> {
    let funding = slices.iter().map(|of| SeriesSlice {
        series: (
            NautilusDataType::FundingRateUpdate,
            of.slice.instrument_id.to_string(),
        ),
        slice: of.slice,
    });
    let slices = SeriesSlice::merged(funding.chain(slices.iter().cloned()));
    Ok(missing(catalog, &slices)?
        .into_iter()
        .filter(|request| request.data != FillData::Instrument)
        .collect())
}

/// The window one request of `data` holds at most (see `NodeFill::add`):
/// `FUNDING_PERIOD` of funding, a UTC day of bars.
#[must_use]
pub fn period(data: FillData) -> SignedDuration {
    match data {
        FillData::Funding => FUNDING_PERIOD,
        _ => DAY,
    }
}

/// The node's data client: Nautilus' OKX data client loading the instrument
/// types and families of `instrument_ids` (see `selection`), on OKX's
/// public API with no key. OKX is one venue, so Nautilus routes its
/// requests to it without a route of ours.
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
        routing: RoutingConfig::default(),
    })
}

/// The instrument types and families of `instrument_ids`, OKX swaps or
/// X-Perps, each once in the order they first come: what Nautilus' OKX
/// clients load (see `Contract`).
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

/// The instrument of `raw`, an OKX swap or X-Perp (a future whose rule
/// type is `xperp`), through the adapter's `parse_instrument_any`, with
/// initial and maintenance margins of 1; a dated future is refused. OKX
/// locks notional / leverage on a position, which is Nautilus' default
/// margin model (notional / leverage x the instrument's margin) with
/// margins of 1 and the account's leverage. The adapter would leave them at
/// 0, which locks nothing.
fn instrument_from(raw: &OKXInstrument, ts_init: UnixNanos) -> anyhow::Result<InstrumentAny> {
    let id = raw.inst_id;
    if raw.inst_type == OKXInstrumentType::Futures {
        ensure!(
            is_xperp_rule_type(&raw.rule_type),
            "{id} is a dated future (rule type {:?}); of OKX's futures only X-Perps are served",
            raw.rule_type
        );
    }
    parse_instrument_any(raw, Some(Decimal::ONE), Some(Decimal::ONE), ts_init)?
        .with_context(|| format!("{id} is not an instrument Nautilus supports"))
}

/// An OKX swap (`BTC-USDT-SWAP.OKX`) or future
/// (`BTC-USD_UM_XPERP-310404.OKX`), keyed in OKX's API and in the clients'
/// configs by its instrument type and family.
#[derive(Debug, Clone, Copy)]
struct Contract {
    /// `Swap` or `Futures`, from the symbol (the adapter's
    /// `okx_instrument_type_from_symbol`).
    inst_type: OKXInstrumentType,
    /// `BTC-USDT`, `BTC-USD_UM_XPERP`, from the symbol (the adapter's
    /// `extract_inst_family`).
    family: &'static str,
}

impl Contract {
    /// The contract of `id`; an error if it is not an OKX swap or future.
    fn of(id: InstrumentId) -> anyhow::Result<Self> {
        ensure!(
            HistorySource::of(id) == HistorySource::Okx,
            "{id} is not on {}",
            *OKX_VENUE
        );
        let symbol = id.symbol.inner().as_str();
        let inst_type = okx_instrument_type_from_symbol(symbol);
        ensure!(
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
    use nautilus_model::data::BarType;

    use super::*;
    use crate::strategy::DataRequirements;

    const INSTRUMENT: &str = include_str!("testdata/BTC-USDT-SWAP-instrument.json");

    fn raw_instrument() -> OKXInstrument {
        serde_json::from_str(INSTRUMENT).unwrap()
    }

    /// BTC-USDT-SWAP.OKX as OKX's public API defined it on 2026-10-21.
    fn btc_instrument() -> InstrumentAny {
        instrument_from(&raw_instrument(), UnixNanos::default()).unwrap()
    }

    const XPERP_INSTRUMENT: &str = include_str!("testdata/BTC-USD_UM_XPERP-310404-instrument.json");

    fn raw_xperp() -> OKXInstrument {
        serde_json::from_str(XPERP_INSTRUMENT).unwrap()
    }

    /// BTC-USD_UM_XPERP-310404.OKX as OKX's public API defined it on
    /// 2026-10-22.
    fn xperp_instrument() -> InstrumentAny {
        instrument_from(&raw_xperp(), UnixNanos::default()).unwrap()
    }

    #[test]
    fn an_okx_swap_is_a_crypto_perpetual_with_margins_of_one() {
        let InstrumentAny::CryptoPerpetual(btc) = btc_instrument() else {
            panic!("not a CryptoPerpetual");
        };
        assert_eq!((btc.price_precision, btc.size_precision), (1, 2));
        assert_eq!(
            (btc.margin_init, btc.margin_maint),
            (Decimal::ONE, Decimal::ONE)
        );
    }

    #[test]
    fn an_okx_x_perp_is_a_crypto_future_with_margins_of_one() {
        let InstrumentAny::CryptoFuture(btc) = xperp_instrument() else {
            panic!("not a CryptoFuture");
        };
        assert_eq!((btc.price_precision, btc.size_precision), (1, 0));
        assert_eq!(
            (btc.margin_init, btc.margin_maint),
            (Decimal::ONE, Decimal::ONE)
        );
    }

    #[test]
    fn a_dated_future_is_refused() {
        let mut dated = raw_xperp();
        dated.rule_type = "normal".to_string();
        let err = instrument_from(&dated, UnixNanos::default())
            .unwrap_err()
            .to_string();
        assert!(err.contains("X-Perp"), "{err}");
    }

    #[test]
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
        for id in [
            "BTC-USDT.OKX",
            "BTC-USD-261225-100000-C.OKX",
            "BTC-USDT-SWAP.BINANCE",
        ] {
            assert!(Contract::of(InstrumentId::from(id)).is_err(), "{id}");
        }
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

    fn fill(data: FillData, id: InstrumentId, from: &str, to: &str) -> FillRequest {
        FillRequest {
            data,
            instrument_id: id,
            from: at(from),
            to: at(to),
        }
    }

    /// The slices of strategy bars on BTC built from its 1-minute and
    /// 5-minute bars over `[from, to]`.
    fn btc_slices(from: &str, to: &str) -> Vec<SeriesSlice> {
        DataRequirements {
            bar_types: vec![
                BarType::from("BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@1-MINUTE-EXTERNAL"),
                BarType::from("BTC-USDT-SWAP.OKX-1-HOUR-LAST-INTERNAL@5-MINUTE-EXTERNAL"),
            ],
            ticks: vec![],
            warmup: SignedDuration::ZERO,
        }
        .slices(at(from), at(to))
    }

    #[test]
    fn an_instrument_s_funding_is_asked_once_before_its_bars_and_its_definition_not_at_all() {
        let dir = tempfile::tempdir().unwrap();
        let (from, to) = ("2026-10-01T00:00:00Z", "2026-10-08T00:00:00Z");

        let asked = requests(&new_catalog(&dir), &btc_slices(from, to)).unwrap();

        assert_eq!(
            asked,
            [
                fill(FillData::Funding, btc(), from, to),
                fill(FillData::Bars(BarType::from(BTC_MINUTES)), btc(), from, to),
                fill(
                    FillData::Bars(BarType::from("BTC-USDT-SWAP.OKX-5-MINUTE-LAST-EXTERNAL")),
                    btc(),
                    from,
                    to
                ),
            ]
        );
    }

    #[test]
    fn funding_is_asked_in_two_weeks_and_bars_in_days() {
        assert_eq!(period(FillData::Funding), SignedDuration::from_hours(336));
        assert_eq!(
            period(FillData::Bars(BarType::from(BTC_MINUTES))),
            SignedDuration::from_hours(24)
        );
    }

    #[tokio::test]
    async fn each_definition_the_catalog_lacks_is_asked_once_and_written() {
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = new_catalog(&dir);
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
                    warmup: SignedDuration::ZERO,
                }
                .slices(at("2026-10-01T00:00:00Z"), at("2026-10-08T00:00:00Z")),
            )
            .collect();
        assert!(slices.iter().any(|s| s.series.1 == xperp_minutes));
        let mut asked = Vec::new();

        write_definitions(&mut catalog, &slices, async |id| {
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
