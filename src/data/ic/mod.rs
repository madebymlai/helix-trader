//! IC Markets history, over Spotware's cTrader Open API: the instruments
//! of IC's forex and metals, filled into the catalog.
//!
//! Nautilus has no cTrader adapter, so this module is one, laid out as
//! Nautilus' adapters are (`docs/developer_guide/adapters.md`): Spotware's
//! protobuf messages (`messages`, from the vendored `proto/`), the
//! environment and credential (`config`), the connection (`websocket`, an
//! outer client and its handler task) and the parsing of symbols into
//! instruments (`parse`). IC's instruments are on venue `ICMARKETS`
//! (`XAUUSD.ICMARKETS`).
//!
//! The quotes of IC's instruments are not served yet: a fill stores the
//! instruments, then stops with an error naming the first quote series.

use std::{collections::HashMap, str::FromStr};

use anyhow::{Context, bail, ensure};
use jiff::Timestamp;
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::{
    data::{Data, NautilusDataType},
    identifiers::InstrumentId,
    instruments::InstrumentAny,
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

use self::{
    config::{IcAccountId, IcCredential, IcEnvironment},
    messages::{
        ProtoOaAssetListReq, ProtoOaAssetListRes, ProtoOaSymbolByIdReq, ProtoOaSymbolByIdRes,
        ProtoOaSymbolsListReq, ProtoOaSymbolsListRes,
    },
    websocket::{HEARTBEAT_INTERVAL, IcWebSocketClient},
};
use super::fill_catalog;

mod config;
mod messages;
mod parse;
#[cfg(test)]
mod testing;
mod websocket;

/// A cTrader session on IC's account, for history.
#[derive(Debug)]
pub struct IcHistory {
    client: IcWebSocketClient,
    account_id: IcAccountId,
}

impl IcHistory {
    /// Connects to the environment `IC_ENVIRONMENT` names and
    /// authenticates the credential `IC_CLIENT_ID`, `IC_CLIENT_SECRET`,
    /// `IC_ACCESS_TOKEN` and `IC_ACCOUNT_ID` from the environment (or
    /// `.env`); none is ever printed.
    ///
    /// # Errors
    ///
    /// Returns an error if the environment or a credential variable is
    /// missing or invalid (before connecting), if cTrader cannot be
    /// reached, or if it refuses the credential (naming its error code).
    pub async fn connect() -> anyhow::Result<Self> {
        let var = |name: &str| std::env::var(name).ok();
        let environment = IcEnvironment::from_vars(var)?;
        let credential = IcCredential::from_vars(var)?;
        let history = Self::connect_to(environment.url(), credential, HEARTBEAT_INTERVAL).await?;
        log::info!("connected to IC's cTrader {environment:?} server");
        Ok(history)
    }

    async fn connect_to(
        url: &str,
        credential: IcCredential,
        heartbeat: std::time::Duration,
    ) -> anyhow::Result<Self> {
        let account_id = credential.account_id;
        let client = IcWebSocketClient::connect(url, credential, heartbeat).await?;
        Ok(Self { client, account_id })
    }

    /// Whether `id` is IC's, so its history comes from here.
    #[must_use]
    pub fn serves(id: InstrumentId) -> bool {
        parse::is_ic(id)
    }

    /// The instruments of `ids`, from cTrader's current definitions: the
    /// assets, the symbol list (each id's symbol name to its `symbolId` on
    /// this server) and the details of those symbols (see `parse`).
    ///
    /// # Errors
    ///
    /// Returns an error if an id is not IC's (before any request), if a
    /// request fails, if IC lists no symbol of that name, or if a symbol is
    /// no currency pair.
    pub async fn instruments(&self, ids: &[InstrumentId]) -> anyhow::Result<Vec<InstrumentAny>> {
        for &id in ids {
            parse::ensure_ic(id)?;
        }
        let account = self.account_id.get();
        let assets: ProtoOaAssetListRes = self
            .client
            .request(&ProtoOaAssetListReq {
                ctid_trader_account_id: account,
                ..Default::default()
            })
            .await
            .context("requesting IC's assets")?;
        let list: ProtoOaSymbolsListRes = self
            .client
            .request(&ProtoOaSymbolsListReq {
                ctid_trader_account_id: account,
                ..Default::default()
            })
            .await
            .context("requesting IC's symbols")?;
        let listed: HashMap<&str, _> = list
            .symbol
            .iter()
            .filter_map(|s| Some((s.symbol_name.as_deref()?, s)))
            .collect();
        let light = ids
            .iter()
            .map(|&id| {
                listed
                    .get(parse::symbol_name(id))
                    .copied()
                    .with_context(|| {
                        format!(
                            "IC lists no symbol {}: an IC id is the symbol's name in cTrader \
                         on {}, such as XAUUSD.{}",
                            parse::symbol_name(id),
                            parse::IC_VENUE,
                            parse::IC_VENUE
                        )
                    })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let details: ProtoOaSymbolByIdRes = self
            .client
            .request(&ProtoOaSymbolByIdReq {
                ctid_trader_account_id: account,
                symbol_id: light.iter().map(|s| s.symbol_id).collect(),
                ..Default::default()
            })
            .await
            .context("requesting the details of IC's symbols")?;
        let details: HashMap<i64, _> = details.symbol.iter().map(|s| (s.symbol_id, s)).collect();
        let assets = parse::assets_by_id(assets.asset);
        let ts_init = get_atomic_clock_realtime().get_time_ns();
        light
            .into_iter()
            .map(|light| {
                let name = light.symbol_name.as_deref().unwrap_or_default();
                let symbol = details
                    .get(&light.symbol_id)
                    .with_context(|| format!("IC returned no details of {name}"))?;
                parse::currency_pair(light, symbol, &assets, ts_init)
                    .map(InstrumentAny::CurrencyPair)
            })
            .collect()
    }

    /// Fills `catalog` with the instruments of `series`, each the quotes
    /// of an IC instrument, then over `[start, end]` with the quotes
    /// themselves, through `fill_catalog`.
    ///
    /// # Errors
    ///
    /// Returns an error if a series is not the quotes of an IC instrument
    /// (before any request), as `instruments`, or as `fill_catalog`. IC's
    /// quotes are not served yet: once the instruments are stored, the
    /// first quote series is an error.
    pub async fn fill(
        &self,
        catalog: &mut ParquetDataCatalog,
        series: &[(NautilusDataType, String)],
        start: Timestamp,
        end: Timestamp,
    ) -> anyhow::Result<()> {
        let ids = series
            .iter()
            .map(|(data_type, identifier)| quotes_of(data_type, identifier))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let instruments = self.instruments(&ids).await?;
        fill_catalog(
            catalog,
            instruments,
            series,
            start,
            end,
            async |_: &NautilusDataType, identifier: &str, _, _| -> anyhow::Result<Vec<Data>> {
                bail!("IC quote history is not served yet: {identifier}'s instrument is stored, its quotes are not")
            },
        )
        .await
    }

    /// Closes the session.
    pub async fn disconnect(self) {
        self.client.disconnect().await;
    }
}

/// The IC instrument whose quotes the series of `data_type` and
/// `identifier` is; an error for any other series.
fn quotes_of(data_type: &NautilusDataType, identifier: &str) -> anyhow::Result<InstrumentId> {
    ensure!(
        *data_type == NautilusDataType::QuoteTick,
        "IC serves quotes only, not the {data_type:?} of {identifier}"
    );
    let id = InstrumentId::from_str(identifier)
        .with_context(|| format!("{identifier} is no instrument id"))?;
    parse::ensure_ic(id)?;
    Ok(id)
}

#[cfg(test)]
mod tests {
    use nautilus_model::{
        instruments::Instrument,
        types::{Currency, Quantity},
    };
    use prost::Message as _;

    use super::{
        messages::{Payload, ProtoMessage},
        testing::{self, ACCOUNT_ID, Action, Received, accept_auth, credential},
        *,
    };

    /// A cTrader serving the fixtures' assets, symbols and details.
    fn fixtures_peer(_: usize, request: &ProtoMessage) -> Vec<Action> {
        if let Some(actions) = accept_auth(request) {
            return actions;
        }
        let fixture: &[u8] = match request.payload_type {
            t if t == ProtoOaAssetListReq::TYPE => include_bytes!("testdata/asset_list_res.bin"),
            t if t == ProtoOaSymbolsListReq::TYPE => {
                include_bytes!("testdata/symbols_list_res.bin")
            }
            t if t == ProtoOaSymbolByIdReq::TYPE => include_bytes!("testdata/symbol_by_id_res.bin"),
            other => panic!("unexpected request {other}"),
        };
        let response = ProtoMessage {
            payload_type: request.payload_type + 1,
            payload: Some(fixture.to_vec()),
            client_msg_id: request.client_msg_id.clone(),
        };
        vec![Action::Send(response.encode_to_vec())]
    }

    async fn connected() -> (IcHistory, Received) {
        let (url, received) = testing::start(fixtures_peer);
        let history = IcHistory::connect_to(&url, credential(), HEARTBEAT_INTERVAL)
            .await
            .unwrap();
        (history, received)
    }

    /// The payload types received after the two auth requests.
    fn requests(received: &Received) -> Vec<ProtoMessage> {
        received
            .lock()
            .unwrap()
            .iter()
            .skip(2)
            .map(|(_, f)| f.clone())
            .collect()
    }

    #[tokio::test]
    async fn instruments_come_from_the_assets_the_symbol_list_and_the_details() {
        let (history, received) = connected().await;
        let ids = [
            InstrumentId::from("XAUUSD.ICMARKETS"),
            InstrumentId::from("EURUSD.ICMARKETS"),
        ];

        let instruments = history.instruments(&ids).await.unwrap();

        let [
            InstrumentAny::CurrencyPair(xau),
            InstrumentAny::CurrencyPair(eur),
        ] = instruments.as_slice()
        else {
            panic!("{instruments:?}");
        };
        assert_eq!((xau.id(), eur.id()), (ids[0], ids[1]));
        assert_eq!(xau.base_currency, Currency::from("XAU"));
        assert_eq!(xau.lot_size, Some(Quantity::from(100)));
        assert_eq!(eur.price_precision, 5);
        let requests = requests(&received);
        let types: Vec<u32> = requests.iter().map(|f| f.payload_type).collect();
        assert_eq!(types, [2112, 2114, 2116]);
        let details =
            ProtoOaSymbolByIdReq::decode(requests[2].payload.as_deref().unwrap()).unwrap();
        assert_eq!(details.ctid_trader_account_id, ACCOUNT_ID);
        assert_eq!(details.symbol_id, [41, 1]);
        history.disconnect().await;
    }

    #[tokio::test]
    async fn a_symbol_ic_does_not_list_is_named() {
        let (history, _) = connected().await;

        let err = history
            .instruments(&[InstrumentId::from("GBPUSD.ICMARKETS")])
            .await
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "IC lists no symbol GBPUSD: an IC id is the symbol's name in cTrader on ICMARKETS, \
             such as XAUUSD.ICMARKETS"
        );
        history.disconnect().await;
    }

    #[tokio::test]
    async fn an_id_of_another_venue_is_refused_before_any_request() {
        let (history, received) = connected().await;

        let err = history
            .instruments(&[InstrumentId::from("EUR/USD.IDEALPRO")])
            .await
            .unwrap_err();

        assert_eq!(err.to_string(), "EUR/USD.IDEALPRO is not on ICMARKETS");
        assert_eq!(requests(&received), []);
        history.disconnect().await;
    }

    #[tokio::test]
    async fn a_fill_stores_the_instruments_then_names_the_quotes_not_served() {
        let (history, _) = connected().await;
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let series = [(NautilusDataType::QuoteTick, "XAUUSD.ICMARKETS".to_owned())];
        let start: Timestamp = "2026-10-01T00:00:00Z".parse().unwrap();
        let end: Timestamp = "2026-10-02T00:00:00Z".parse().unwrap();

        let err = history
            .fill(&mut catalog, &series, start, end)
            .await
            .unwrap_err();

        assert!(
            format!("{err:#}").contains(
                "IC quote history is not served yet: XAUUSD.ICMARKETS's instrument is stored, \
                 its quotes are not"
            ),
            "{err:#}"
        );
        let stored = catalog.instruments(None, None, None).unwrap();
        let ids: Vec<InstrumentId> = stored.iter().map(Instrument::id).collect();
        assert_eq!(ids, [InstrumentId::from("XAUUSD.ICMARKETS")]);
        history.disconnect().await;
    }

    #[tokio::test]
    async fn a_series_other_than_quotes_is_refused_before_any_request() {
        let (history, received) = connected().await;
        let dir = tempfile::tempdir().unwrap();
        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let bars = "XAUUSD.ICMARKETS-1-MINUTE-BID-EXTERNAL".to_owned();
        let start: Timestamp = "2026-10-01T00:00:00Z".parse().unwrap();

        let err = history
            .fill(&mut catalog, &[(NautilusDataType::Bar, bars)], start, start)
            .await
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "IC serves quotes only, not the Bar of XAUUSD.ICMARKETS-1-MINUTE-BID-EXTERNAL"
        );
        assert_eq!(requests(&received), []);
        history.disconnect().await;
    }
}
