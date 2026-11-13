//! LSE history, filled through the node (see `catalog::NodeFill`) by the LSE
//! adapter's data client (`lse`), whose answers the data engine writes back
//! to the catalog: the trades of `LSEDGE` instruments, London Strategic
//! Edge's own derived series, each an index instrument named after its LSE
//! market (`GOLD-FUTURES.LSEDGE`: GOLD in `futures`), from LSE's tick
//! slices; and the trades and quotes of `GLBX` contracts, CME Globex's
//! Level 1, under the ids Databento gives them (`GCZ26.GLBX`). Each
//! contract's definition is its row's definitions source's: `LSEDGE`'s the
//! catalog's alone, written there by hand, LSE publishing none, the data
//! engine answering a definition request from the catalog before asking the
//! client; `GLBX`'s Databento's, asked of Nautilus' Databento client. The
//! client converts LSE's prices at the definition's precision. See
//! `README.md`.

use ::lse::{LseDataClientConfig, LseDataClientFactory};
use jiff::SignedDuration;
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

use super::{DefinitionsSource, Row, databento::DatabentoHistory};
use crate::{
    catalog::{FillData, NodeClient, NodeFill, missing},
    strategy::SeriesSlice,
};

/// The window of one request of a series: a day, as Databento's. The
/// client asks LSE a UTC day an export whatever the window.
const DAY: SignedDuration = SignedDuration::from_hours(24);

/// Adds to `node` the fill of what the catalog lacks of `slices`, series
/// LSE gives the history of (see `catalog::missing`): first each contract's
/// definition, asked of its row's definitions source, Databento's client,
/// which `databento` makes only if Databento has a definition to give,
/// checked against its dataset's end, for the contracts Databento defines,
/// and LSE's for the others, the data engine answering them from the
/// catalog before the client; then the series, asked of the LSE client
/// `config` configures, its keys read only when the node creates it, so
/// only if something is missing. The data engine puts each definition in
/// the cache, where the client reads the precision of its prices; a
/// definition neither source gives leaves the series' request to fail
/// naming the instrument.
///
/// # Errors
///
/// Returns an error if a series is not one the node fills (see `missing`),
/// if the catalog cannot be read, or as `databento` and
/// `DatabentoHistory::check`.
pub(super) async fn add_fill(
    node: &mut NodeFill,
    catalog: &ParquetDataCatalog,
    slices: &[SeriesSlice],
    config: LseDataClientConfig,
    databento: impl FnOnce() -> anyhow::Result<DatabentoHistory>,
) -> anyhow::Result<()> {
    let (definitions, requests): (Vec<_>, Vec<_>) =
        missing(catalog, slices)?.into_iter().partition(|request| {
            request.data == FillData::Instrument
                && Row::of(request.instrument_id.venue).definitions == DefinitionsSource::Databento
        });
    if !definitions.is_empty() {
        let databento = databento()?;
        databento.check(&definitions).await?;
        node.add(databento.node_client(), &definitions, |data| {
            unreachable!("Databento's check passes definitions alone, not {data}")
        });
    }
    node.add(node_client(config), &requests, period);
    Ok(())
}

/// The window one request of any series holds at most (see
/// `NodeFill::add`): a day.
fn period(_data: FillData) -> SignedDuration {
    DAY
}

/// The node's data client: the LSE adapter's, on no venue, so asked only
/// by the requests naming it (see `NodeFill::add`).
fn node_client(config: LseDataClientConfig) -> NodeClient {
    NodeClient {
        factory: Box::new(LseDataClientFactory::new()),
        config: Box::new(config),
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, path::Path};

    use ::lse::testing::{
        ALLOWANCE_EXHAUSTED, API_KEY, CME_API_KEY, DATA_UNAVAILABLE_TODAY, DATE_NOT_TRADING, GCZ26,
        GCZ26_QUOTES, GCZ26_TRADES, GOLD_FUTURES, GOLD_TRADES, MockLse, PAIRS_GCZ6, PLAN_REQUIRED,
        ROWS_EXHAUSTED, Reply, SYMBOL_UNKNOWN, gold_futures,
    };
    use jiff::Timestamp;
    use nautilus_core::{UnixNanos, string::secret::SecretString};
    use nautilus_model::{
        data::{NautilusDataType, TradeTick},
        enums::AggressorSide,
        identifiers::{InstrumentId, TradeId},
        instruments::InstrumentAny,
        types::{Price, Quantity},
    };

    use super::*;
    use crate::{
        strategy::{DataRequirements, TickSeries},
        venues::databento::testing::MockDatabento,
    };

    /// LSE lists no such series.
    const NOSUCH: &str = "NOSUCH-FUTURES.LSEDGE";

    fn at(time: &str) -> Timestamp {
        format!("2026-10-21T{time}Z").parse().unwrap()
    }

    /// `GOLD_FUTURES`'s definition, as `id`'s.
    fn defined_as(id: &str) -> InstrumentAny {
        let InstrumentAny::IndexInstrument(mut index) = gold_futures() else {
            unreachable!("an index");
        };
        index.id = InstrumentId::from(id);
        InstrumentAny::IndexInstrument(index)
    }

    /// fill-catalog's fill of the trades of `id` over the captured second,
    /// `[10:00:00, 10:00:01)`, into the catalog in `dir`, from `lse`, the
    /// catalog holding `definitions`.
    fn fill(
        dir: &Path,
        lse: &MockLse,
        id: &str,
        definitions: Vec<InstrumentAny>,
    ) -> anyhow::Result<()> {
        fill_over(
            dir,
            lse,
            id,
            definitions,
            at("10:00:00"),
            at("10:00:00.999999999"),
        )
    }

    /// fill-catalog's fill of the trades of `id` over `[from, to]`, as
    /// `fill`'s.
    fn fill_over(
        dir: &Path,
        lse: &MockLse,
        id: &str,
        definitions: Vec<InstrumentAny>,
        from: Timestamp,
        to: Timestamp,
    ) -> anyhow::Result<()> {
        fill_series(
            dir,
            lse,
            None,
            &[(NautilusDataType::TradeTick, id)],
            definitions,
            (from, to),
        )
    }

    /// fill-catalog's fill of `series`, each a data type of an instrument,
    /// over `[from, to]`, into the catalog in `dir`, from `lse` and
    /// `databento`, Databento's mock peer, the catalog holding
    /// `definitions`. With no peer of Databento's, Databento is never asked.
    fn fill_series(
        dir: &Path,
        lse: &MockLse,
        databento: Option<&MockDatabento>,
        series: &[(NautilusDataType, &str)],
        definitions: Vec<InstrumentAny>,
        (from, to): (Timestamp, Timestamp),
    ) -> anyhow::Result<()> {
        let catalog = ParquetDataCatalog::new(dir, None, None, None, None);
        if !definitions.is_empty() {
            catalog.write_instruments(definitions).unwrap();
        }
        let requirements = DataRequirements {
            bar_types: vec![],
            ticks: series
                .iter()
                .map(|(data, id)| TickSeries {
                    data: data.clone(),
                    instrument_id: InstrumentId::from(*id),
                })
                .collect(),
            warmup: SignedDuration::ZERO,
        };
        let slices = requirements.slices(from, to);
        let config = LseDataClientConfig::builder()
            .api_key(SecretString::from(API_KEY))
            .cme_api_key(SecretString::from(CME_API_KEY))
            .base_url_http(lse.url().to_string())
            .build();
        let databento = || {
            let peer = databento.expect("Databento asked for a definition");
            DatabentoHistory::with_base_url(&format!("db-{}", "0".repeat(29)), peer.url())
        };
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let mut node = NodeFill::new();
                add_fill(&mut node, &catalog, &slices, config, databento).await?;
                node.run(dir).await
            })
    }

    /// The spans LSE was asked for, in order.
    fn asked(lse: &MockLse) -> Vec<(String, String)> {
        lse.calls()
            .iter()
            .map(|call| {
                (
                    call.param("from").unwrap().to_string(),
                    call.param("to").unwrap().to_string(),
                )
            })
            .collect()
    }

    fn stored_trades(dir: &Path) -> Vec<TradeTick> {
        ParquetDataCatalog::new(dir, None, None, None, None)
            .trade_ticks(Some(vec![GOLD_FUTURES.to_string()]), None, None)
            .unwrap()
    }

    /// The captured second, as pyarrow reads LSE's file.
    #[test]
    fn the_trades_lse_exports_are_written_back_to_the_catalog() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);

        fill(dir.path(), &lse, GOLD_FUTURES, vec![gold_futures()]).unwrap();

        let stored = stored_trades(dir.path());
        let rows: Vec<_> = stored
            .iter()
            .map(|t| (t.ts_event, t.price, t.size, t.aggressor_side))
            .collect();
        let row = |time, price, size| {
            (
                UnixNanos::from(at(time)),
                Price::from(price),
                Quantity::from(size),
                AggressorSide::NoAggressor,
            )
        };
        assert_eq!(
            rows,
            [
                row("10:00:00.619501", "4136.4", 1),
                row("10:00:00.619501", "4136.4", 1),
                row("10:00:00.690600", "4136.3", 1),
                row("10:00:00.690600", "4136.2", 2),
                row("10:00:00.690600", "4136.1", 2),
                row("10:00:00.901218", "4136.2", 1),
            ]
        );
        let ids: HashSet<_> = stored.iter().map(|t| t.trade_id).collect();
        assert_eq!(ids.len(), stored.len());
        let [export] = &lse.calls()[..] else {
            panic!("{:?}", lse.calls())
        };
        assert_eq!(
            (export.param("from"), export.param("to")),
            (Some("2026-10-21T10:00:00Z"), Some("2026-10-21T10:00:01Z"))
        );
    }

    #[test]
    fn lse_s_symbol_unknown_fails_the_fill_naming_the_request() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![Reply::refusal(404, SYMBOL_UNKNOWN)]);

        let err = fill(dir.path(), &lse, NOSUCH, vec![defined_as(NOSUCH)])
            .unwrap_err()
            .to_string();

        assert!(err.contains(&format!("the trades of {NOSUCH}")), "{err}");
        assert!(err.contains("symbol-unknown"), "{err}");
    }

    /// LSE is never asked: its prices could not be read.
    #[test]
    fn a_fill_of_an_lsedge_instrument_with_no_definition_fails_naming_it() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![Reply::file(GOLD_TRADES)]);

        let err = fill(dir.path(), &lse, GOLD_FUTURES, vec![])
            .unwrap_err()
            .to_string();

        assert!(
            err.contains(&format!("{GOLD_FUTURES} is not in the cache")),
            "{err}"
        );
        assert!(lse.calls().is_empty(), "{:?}", lse.calls());
        assert_eq!(stored_trades(dir.path()), []);
    }

    #[test]
    fn lse_s_refusal_fails_the_fill_with_its_code_message_and_required_plan() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![Reply::refusal(403, PLAN_REQUIRED)]);

        let err = fill(dir.path(), &lse, GOLD_FUTURES, vec![gold_futures()])
            .unwrap_err()
            .to_string();

        for part in [
            "plan-required",
            "This feed requires a subscription",
            "CME Group Lite",
        ] {
            assert!(err.contains(part), "{part}: {err}");
        }
    }
    /// The 21st is filled and the 22nd refused for a quota, which ends the
    /// fill with LSE's message. The next run asks for the 22nd alone, of
    /// which LSE holds no row as on a date with no session, and the run
    /// after asks for nothing.
    #[test]
    fn a_quota_stops_the_fill_with_lse_s_message_and_the_next_run_asks_only_what_is_missing() {
        let day = |time: &str| -> Timestamp { time.parse().unwrap() };
        let (from, to) = (
            day("2026-10-21T00:00:00Z"),
            day("2026-10-22T23:59:59.999999999Z"),
        );
        for quota in [ROWS_EXHAUSTED, ALLOWANCE_EXHAUSTED] {
            let dir = tempfile::tempdir().unwrap();
            let refusing = MockLse::start(vec![
                Reply::file(GOLD_TRADES),
                // The first window ends at the first instant of the 22nd.
                Reply::refusal(404, DATE_NOT_TRADING),
                Reply::refusal(429, quota),
            ]);

            let err = fill_over(
                dir.path(),
                &refusing,
                GOLD_FUTURES,
                vec![gold_futures()],
                from,
                to,
            )
            .unwrap_err()
            .to_string();

            let refusal: serde_json::Value = serde_json::from_str(quota).unwrap();
            let message = refusal["message"].as_str().unwrap();
            assert!(err.contains(message), "{message}: {err}");
            assert_eq!(stored_trades(dir.path()).len(), 6);

            let resuming = MockLse::start(vec![Reply::refusal(404, DATE_NOT_TRADING)]);
            fill_over(dir.path(), &resuming, GOLD_FUTURES, vec![], from, to).unwrap();

            assert_eq!(
                asked(&resuming),
                [(
                    "2026-10-22T00:00:00.000000001Z".to_string(),
                    "2026-10-23T00:00:00Z".to_string()
                )]
            );

            let done = MockLse::start(vec![]);
            fill_over(dir.path(), &done, GOLD_FUTURES, vec![], from, to).unwrap();

            assert!(done.calls().is_empty(), "{:?}", done.calls());
        }
    }

    /// Trades stored until 20 hours ago, then a window until now, which
    /// reaches into the current UTC day, LSE storing a day the morning after
    /// it. The fill ends, but the window is not recorded as stored beside
    /// the stored trades, so that the next run asks for it again.
    #[test]
    fn a_window_lse_has_not_loaded_is_left_missing_for_the_next_run() {
        let dir = tempfile::tempdir().unwrap();
        let now = Timestamp::now();
        let from = now - SignedDuration::from_hours(72);
        let stored_to = now - SignedDuration::from_hours(20);
        let trade = TradeTick::new(
            InstrumentId::from(GOLD_FUTURES),
            Price::from("4136.4"),
            Quantity::from(1),
            AggressorSide::NoAggressor,
            TradeId::new("1"),
            UnixNanos::from(from),
            UnixNanos::from(from),
        );
        ParquetDataCatalog::new(dir.path(), None, None, None, None)
            .write_to_parquet(
                &[trade],
                Some(from.into()),
                Some((stored_to - SignedDuration::from_nanos(1)).into()),
                None,
            )
            .unwrap();
        // The day before's part, if any, and the current day's.
        let not_loaded = || vec![Reply::refusal(404, DATA_UNAVAILABLE_TODAY); 2];
        let first = MockLse::start(not_loaded());

        fill_over(
            dir.path(),
            &first,
            GOLD_FUTURES,
            vec![gold_futures()],
            from,
            now,
        )
        .unwrap();

        let again = MockLse::start(not_loaded());
        fill_over(dir.path(), &again, GOLD_FUTURES, vec![], from, now).unwrap();
        let asked = asked(&again);
        assert_eq!(
            asked.first().map(|(from, _)| from.as_str()),
            Some(stored_to.to_string().as_str()),
            "{asked:?}"
        );
    }

    /// GCZ6 of 2026's trades and quotes over the captured second: its
    /// definition is Databento's, asked of Databento's client, which the
    /// mock Databento peer answers with Databento's records of GCZ6, and its
    /// trades and quotes LSE's, asked of LSE's client, which looks the
    /// contract up in LSE's catalogue and asks for its listing's rows with
    /// the CME key. Neither client claims a venue: each request reaches the
    /// client it names. The definition is the one Databento's client wrote
    /// into this repository's catalog before.
    #[test]
    fn a_glbx_fill_takes_its_definition_from_databento_and_its_trades_and_quotes_from_lse() {
        let dir = tempfile::tempdir().unwrap();
        let lse = MockLse::start(vec![
            Reply::answer(PAIRS_GCZ6),
            Reply::file(GCZ26_TRADES),
            Reply::file(GCZ26_QUOTES),
        ]);
        let databento = MockDatabento::start();
        let second: (Timestamp, Timestamp) = (
            "2026-11-12T14:30:00Z".parse().unwrap(),
            "2026-11-12T14:30:00.999999999Z".parse().unwrap(),
        );

        fill_series(
            dir.path(),
            &lse,
            Some(&databento),
            &[
                (NautilusDataType::TradeTick, GCZ26),
                (NautilusDataType::QuoteTick, GCZ26),
            ],
            vec![],
            second,
        )
        .unwrap();

        let mut catalog = ParquetDataCatalog::new(dir.path(), None, None, None, None);
        let [InstrumentAny::FuturesContract(definition)] = &catalog
            .instruments(Some(&[GCZ26.to_string()]), None, None)
            .unwrap()[..]
        else {
            panic!("not one futures contract");
        };
        assert_eq!(
            (
                definition.raw_symbol.as_str(),
                definition.price_precision,
                definition.price_increment,
                definition.multiplier,
                Timestamp::from(definition.expiration_ns),
            ),
            (
                "GCZ6",
                2,
                Price::from("0.10"),
                Quantity::from(100),
                "2026-12-29T18:30:00Z".parse().unwrap(),
            )
        );
        let trades = catalog
            .trade_ticks(Some(vec![GCZ26.to_string()]), None, None)
            .unwrap();
        assert_eq!(trades.len(), 38);
        assert_eq!(
            (trades[0].ts_event, trades[0].price),
            (
                UnixNanos::from(1_794_493_800_001_418_191),
                Price::from("3908.10")
            )
        );
        let quotes = catalog
            .quote_ticks(Some(vec![GCZ26.to_string()]), None, None)
            .unwrap();
        assert_eq!(quotes.len(), 636);
        let asked_of_databento: Vec<_> = databento
            .calls()
            .iter()
            .map(|call| {
                (
                    call.path.clone(),
                    call.param("schema").map(ToString::to_string),
                )
            })
            .collect();
        assert_eq!(
            asked_of_databento,
            [
                ("/v0/metadata.get_dataset_range".to_string(), None),
                (
                    "/v0/timeseries.get_range".to_string(),
                    Some("definition".to_string())
                ),
            ]
        );
        let asked_of_lse: Vec<_> = lse
            .calls()
            .iter()
            .map(|call| {
                (
                    call.path.clone(),
                    call.param("pair_id")
                        .or(call.param("symbol"))
                        .map(ToString::to_string),
                    call.param("data").map(ToString::to_string),
                    call.api_key.clone(),
                )
            })
            .collect();
        let call = |path: &str, instrument: &str, data: Option<&str>| {
            (
                path.to_string(),
                Some(instrument.to_string()),
                data.map(ToString::to_string),
                Some(CME_API_KEY.to_string()),
            )
        };
        assert_eq!(
            asked_of_lse,
            [
                call("/v3/pairs", "GCZ6", None),
                call("/v3/export", "4000000002", Some("trades")),
                call("/v3/export", "4000000002", Some("quotes")),
            ]
        );
    }
}
