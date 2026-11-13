//! Mock LSE peers for the tests, the adapter's and its callers': an HTTP
//! server on 127.0.0.1 answering each call with the next reply of a
//! script, and a socket (`MockLseSocket`) answering each frame as a script
//! says, both replaying payloads captured from LSE (`test_data/`, see its
//! `README.md`) and recording what they receive. Built for the crate's
//! tests and, with the `test-support` feature, for a caller's.

use std::{
    io::{BufRead, BufReader, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{AssetClass, OptionKind},
    identifiers::{InstrumentId, Symbol},
    instruments::{FuturesContract, IndexInstrument, InstrumentAny, OptionContract},
    types::{Currency, Price, Quantity},
};
use nautilus_network::http::Url;
use sha2::{Digest, Sha256};

pub use self::socket::{MockLseSocket, SocketAction, data_ws_answer, socket_answer};

mod socket;

/// The trade slice of LSE's gold futures series (`GOLD` in `futures`) over
/// `[2026-10-21T10:00:00Z, 2026-10-21T10:00:01Z)`, as LSE exported it:
/// six trades.
pub const GOLD_TRADES: &[u8] = include_bytes!("../test_data/export_gold_futures_tick.parquet");
/// LSE's catalogue of its reference collections (`/v3/reference`).
pub const REFERENCE_CATALOGUE: &str = include_str!("../test_data/reference_catalogue.json");
/// The rows of the US 10-year yield (`US10Y`) in LSE's daily bond yields
/// (`bond_yields`) from 9 to 12 November 2026, as LSE exported them: three
/// rows, of the 9th, 10th and 12th.
pub const US10Y_BOND_YIELDS: &[u8] =
    include_bytes!("../test_data/reference_bond_yields_us10y.parquet");
/// The 4-hour rows of `US10Y` in LSE's `bond_yields_four_hour` of 10
/// November 2026, as LSE exported them: six rows, stamped at their open,
/// 00:00 to 20:00.
pub const US10Y_BOND_YIELDS_FOUR_HOURS: &[u8] =
    include_bytes!("../test_data/reference_bond_yields_four_hour_us10y.parquet");
/// The rows of `US10Y` in LSE's other interval collections of 10 November
/// 2026, as LSE exported them, each stamped at its interval's open: at 1,
/// 5, 15 and 30 minutes, an hour (`bond_yields_hourly`) and LSE's hourly
/// bond yields (`bond_yields_intraday`).
pub const US10Y_BOND_YIELDS_MINUTE: &[u8] =
    include_bytes!("../test_data/reference_bond_yields_minute_us10y.parquet");
pub const US10Y_BOND_YIELDS_FIVE_MINUTES: &[u8] =
    include_bytes!("../test_data/reference_bond_yields_five_minute_us10y.parquet");
pub const US10Y_BOND_YIELDS_FIFTEEN_MINUTES: &[u8] =
    include_bytes!("../test_data/reference_bond_yields_fifteen_minute_us10y.parquet");
pub const US10Y_BOND_YIELDS_THIRTY_MINUTES: &[u8] =
    include_bytes!("../test_data/reference_bond_yields_thirty_minute_us10y.parquet");
pub const US10Y_BOND_YIELDS_HOURLY: &[u8] =
    include_bytes!("../test_data/reference_bond_yields_hourly_us10y.parquet");
pub const US10Y_BOND_YIELDS_INTRADAY: &[u8] =
    include_bytes!("../test_data/reference_bond_yields_intraday_us10y.parquet");
/// A series of each of LSE's collections of a day's market values, a row
/// a field, as LSE exported them on 13 November 2026: the Australian
/// 10-year bond (`AUGB10Y` in `sovereign_yields`) of the 10th and 11th,
/// Erste Group's 2.96% bond (`AT016322245` in `corporate_bonds`), an
/// offshore RMB credit index (`CNH_NONFIN_IG_0_1` in `credit_indices`) and
/// AUD/USD's 1-month at-the-money implied volatility (`AUDUSD_1M_ATM` in
/// `fx_derivatives`) of the 9th to the 11th.
pub const AUGB10Y_SOVEREIGN_YIELDS: &[u8] =
    include_bytes!("../test_data/reference_sovereign_yields_augb10y.parquet");
pub const AT016322245_CORPORATE_BONDS: &[u8] =
    include_bytes!("../test_data/reference_corporate_bonds_at016322245.parquet");
pub const CNH_CREDIT_INDICES: &[u8] =
    include_bytes!("../test_data/reference_credit_indices_cnh_nonfin_ig_0_1.parquet");
pub const AUDUSD_FX_DERIVATIVES: &[u8] =
    include_bytes!("../test_data/reference_fx_derivatives_audusd_1m_atm.parquet");
/// Atlassian's (`TEAM`) insider trade filed on 13 October 2026, as LSE
/// exported it.
pub const TEAM_INSIDER_TRADES: &[u8] =
    include_bytes!("../test_data/reference_insider_trades_team.parquet");
/// AAPL's earnings calendar of 2026, its dividends of 2026 and its stock
/// splits since 2014, as LSE exported them.
pub const AAPL_EARNINGS_CALENDAR: &[u8] =
    include_bytes!("../test_data/reference_earnings_calendar_aapl.parquet");
pub const AAPL_DIVIDENDS: &[u8] = include_bytes!("../test_data/reference_dividends_aapl.parquet");
pub const AAPL_STOCK_SPLITS: &[u8] =
    include_bytes!("../test_data/reference_stock_splits_aapl.parquet");
/// The snapshot of a company (`AHMA`) in LSE's company profiles and stock
/// fundamentals, as LSE exported it: one row each, updated on 13 October
/// 2026 at 05:08:01 UTC.
pub const AHMA_COMPANY_PROFILES: &[u8] =
    include_bytes!("../test_data/reference_company_profiles_ahma.parquet");
pub const AHMA_STOCK_FUNDAMENTALS: &[u8] =
    include_bytes!("../test_data/reference_stock_fundamentals_ahma.parquet");
/// The whole series of US core inflation's releases in LSE's economic
/// calendar (`US.Core Inflation Rate YoY`), as LSE exported it: 14 rows,
/// from 18 December 2025 to 10 December 2026, the past ones of 11
/// February and 10 November 2026 with no actual.
pub const US_CORE_CPI_CALENDAR: &[u8] =
    include_bytes!("../test_data/reference_economic_calendar_us_core_cpi.parquet");
/// The whole series of the 10-year T-note's Commitments of Traders (`ZN` in
/// `cot`), as LSE exported it: 614 reports, of 6 January 2015 to 6
/// October 2026, those since 28 April 2026 with no release date.
pub const ZN_COT: &[u8] = include_bytes!("../test_data/reference_cot_zn.parquet");
/// AAPL's financial reports of its fiscal periods ending 28 June 2025 to
/// 27 June 2026, and its trailing twelve months' metrics and ratios of 7
/// October 2026, as LSE exported them: 38 rows.
pub const AAPL_FINANCIAL_REPORTS: &[u8] =
    include_bytes!("../test_data/reference_financial_reports_aapl.parquet");
/// The whole series of US core inflation's releases in LSE's macro series
/// (`/v3/macro/export`, `kind=release`), and US10Y's yields of 9 to 12
/// November 2026 (`kind=yield`), as LSE exported them.
pub const US_CORE_CPI_RELEASES: &[u8] =
    include_bytes!("../test_data/macro_release_us_core_cpi.parquet");
pub const US10Y_YIELDS: &[u8] = include_bytes!("../test_data/macro_yield_us10y.parquet");
/// LSE's factors (`/v3/factors`) of AAPL in `us_stocks`, its five splits
/// and 93 dividends, and of GOLD in `futures`, no split and no dividends.
pub const AAPL_FACTORS: &str = include_str!("../test_data/factors_aapl_us_stocks.json");
pub const GOLD_FACTORS: &str = include_str!("../test_data/factors_gold_futures.json");
/// The 1-minute candles of LSE's gold spot series (`XAU/USD` in
/// `commodities`) over `[2026-10-21T10:00:00Z, 2026-10-21T10:05:00Z)`, as LSE
/// exported them: five candles, stamped at their open, of no volume.
pub const GOLD_SPOT_MINUTES: &[u8] =
    include_bytes!("../test_data/export_xau_usd_commodities_1m.parquet");
/// The 4-hour candles of `GOLD` in `futures` over
/// `[2026-10-21T00:00:00Z, 2026-10-21T12:00:00Z)`, as LSE exported them:
/// three candles, stamped at their open.
pub const GOLD_FUTURES_4H: &[u8] = include_bytes!("../test_data/export_gold_futures_4h.parquet");
/// A series of each other derived market, as LSE exported it on 13
/// November 2026 (`test_data/README.md`): the trade slices of `AAPL` in
/// `us_stocks` over `[2026-11-12T15:00:00Z, 15:00:01Z)`, 54 trades, of
/// `VOD` in `uk_stocks` over `[10:00:00Z, 10:01:00Z)`, 9 trades, of `RY`
/// in `ca_stocks` over `[15:00:00Z, 15:01:00Z)`, 23 trades, and of
/// `BTCUSDT` in `crypto` over `[10:00:00Z, 10:00:01Z)`, 36 trades; and
/// the 1-minute candles of `EUR_USD` in `forex` over `[10:00Z, 10:05Z)`
/// and of `SPX` in `indices` over `[15:00Z, 15:05Z)`, five each, stamped
/// at their open, of no volume.
pub const AAPL_TRADES: &[u8] = include_bytes!("../test_data/export_aapl_us_stocks_tick.parquet");
pub const VOD_TRADES: &[u8] = include_bytes!("../test_data/export_vod_uk_stocks_tick.parquet");
pub const RY_TRADES: &[u8] = include_bytes!("../test_data/export_ry_ca_stocks_tick.parquet");
pub const BTCUSDT_TRADES: &[u8] = include_bytes!("../test_data/export_btcusdt_crypto_tick.parquet");
pub const EUR_USD_MINUTES: &[u8] = include_bytes!("../test_data/export_eur_usd_forex_1m.parquet");
pub const SPX_MINUTES: &[u8] = include_bytes!("../test_data/export_spx_indices_1m.parquet");
/// The tick slice of `SPX` in `indices` over `[2026-11-12T15:00:00Z,
/// 15:00:05Z)`, as LSE exported it on 13 November 2026: three of the
/// index's values, each of kind `trade` and size 0.
pub const SPX_TICKS: &[u8] = include_bytes!("../test_data/export_spx_indices_tick.parquet");
/// LSE's refusals, captured with the free key, but the calls a minute and
/// the quotas, which are LSE's documented answers, and the export too wide,
/// captured with the CME key (`test_data/README.md`).
pub const SYMBOL_UNKNOWN: &str = include_str!("../test_data/refusal_symbol_unknown.json");
pub const PLAN_REQUIRED: &str = include_str!("../test_data/refusal_plan_required.json");
pub const DATA_UNAVAILABLE: &str = include_str!("../test_data/refusal_data_unavailable.json");
pub const DATE_NOT_TRADING: &str = include_str!("../test_data/refusal_date_not_trading.json");
pub const REFERENCE_DATA_UNAVAILABLE: &str =
    include_str!("../test_data/refusal_reference_data_unavailable.json");
pub const CALLS_EXHAUSTED: &str = include_str!("../test_data/refusal_calls_exhausted.json");
pub const ROWS_EXHAUSTED: &str = include_str!("../test_data/refusal_rows_exhausted.json");
pub const ALLOWANCE_EXHAUSTED: &str = include_str!("../test_data/refusal_allowance_exhausted.json");
/// LSE's refusal of a day of GCZ6's quotes, past the rows one export may
/// read.
pub const EXPORT_TOO_WIDE: &str = include_str!("../test_data/refusal_export_too_wide.json");
/// LSE's answer for a day it had not loaded yet, the current one.
pub const DATA_UNAVAILABLE_TODAY: &str =
    include_str!("../test_data/refusal_data_unavailable_today.json");
/// LSE's answer for the tick slice of a market storing no trades, `forex`.
pub const TICKS_NOT_STORED: &str = include_str!("../test_data/refusal_data_unavailable_tick.json");
/// LSE's refusal of a derived market's quotes, a 400: it serves them as
/// timeframes alone.
pub const DATA_UNAVAILABLE_QUOTES: &str =
    include_str!("../test_data/refusal_data_unavailable_quotes.json");
/// LSE's refusal of CME rows older than the CME Group Lite+ plan's twelve
/// years of Level 1, captured with that plan's key.
pub const HISTORY_LIMIT: &str = include_str!("../test_data/refusal_history_limit.json");

/// LSE's catalogue of CME Globex (`/v3/pairs?market=cme&symbol=…`), captured
/// with the CME Group Lite+ key: the listings of each exchange name. ESZ6
/// and GCZ6 name the contracts of 2016 and of 2026; GCZ4 those of 2014 and
/// of 2024, the second named by its symbol alone; NGZ6 the contract of
/// 2016 alone, natural gas naming 2026's NGZ26; LSE lists no ESZ26.
pub const PAIRS_ESZ6: &str = include_str!("../test_data/pairs_cme_esz6.json");
pub const PAIRS_GCZ6: &str = include_str!("../test_data/pairs_cme_gcz6.json");
pub const PAIRS_GCZ4: &str = include_str!("../test_data/pairs_cme_gcz4.json");
pub const PAIRS_NGZ6: &str = include_str!("../test_data/pairs_cme_ngz6.json");
pub const PAIRS_NGZ26: &str = include_str!("../test_data/pairs_cme_ngz26.json");
pub const PAIRS_ESZ26: &str = include_str!("../test_data/pairs_cme_esz26.json");

/// The pair id LSE gives GCZ6 of 2026, `GCZ26.GLBX`.
pub const GCZ26_PAIR_ID: u64 = 4_000_000_002;
/// The trades and the quotes of GCZ6 of 2026 over
/// `[2026-11-12T14:30:00Z, 2026-11-12T14:30:01Z)`, as LSE exported them by
/// that pair id to the CME key: 38 trades and 636 quotes.
pub const GCZ26_TRADES: &[u8] = include_bytes!("../test_data/export_gcz6_trades.parquet");
pub const GCZ26_QUOTES: &[u8] = include_bytes!("../test_data/export_gcz6_quotes.parquet");
/// A book and an orders export of GCZ6 of 2026 over the same second, as
/// LSE's docs record the two files, built, not captured: the keys hold no
/// Level 2 or 3 (`test_data/README.md`). Four rows of the book, ten levels
/// a side but the last, of eight asks; five orders, one of no action.
pub const GCZ26_BOOK: &[u8] = include_bytes!("../test_data/export_gcz6_book.parquet");
pub const GCZ26_ORDERS: &[u8] = include_bytes!("../test_data/export_gcz6_orders.parquet");
/// LSE's refusals of GCZ6's book and orders to the CME Group Lite+ key,
/// which holds Level 1 alone, captured with that key.
pub const LEVEL_REQUIRED_BOOK: &str = include_str!("../test_data/refusal_level_required_book.json");
pub const LEVEL_REQUIRED_ORDERS: &str =
    include_str!("../test_data/refusal_level_required_orders.json");

/// The CME contract of `GCZ26_TRADES`.
pub const GCZ26: &str = "GCZ26.GLBX";

/// The trade slice of AAPL's 355 call of 20 November 2026 (`AAPL
/// 261120C00355000` in `us_options`) over `[2026-11-12T14:30:00Z,
/// 2026-11-12T14:31:00Z)`, and its 1-minute candles over
/// `[2026-11-12T14:30:00Z, 2026-11-12T14:35:00Z)`, as LSE exported them to
/// the free key: 23 trades, and four candles, opening at 14:30 to 14:33,
/// none at 14:34, which had no trade.
pub const AAPL_CALL_TRADES: &[u8] = include_bytes!("../test_data/export_aapl_call_tick.parquet");
pub const AAPL_CALL_MINUTES: &[u8] = include_bytes!("../test_data/export_aapl_call_1m.parquet");

/// The US option contract of `AAPL_CALL_TRADES`, under the id Databento
/// gives it: its OSI symbol under `OPRA`.
pub const AAPL_CALL: &str = "AAPL  261120C00355000.OPRA";

/// The CME key the tests give the adapter, which the peer records.
pub const CME_API_KEY: &str = "lse-cme-test-key-0123456789";

/// LSE's data-ws frames, captured with the free key on 13 November 2026
/// (`test_data/README.md`): its welcome; its authenticated frame, trimmed
/// to the futures it lists; the subscription and unsubscription of
/// `GOLD.F`, the gold futures series, and its first three ticks; the
/// answer to a ping; and its refusals of a 17th symbol on the free key's
/// 16, of a second connection on the key's one, and of a key it does not
/// know.
pub const WS_WELCOME: &str = include_str!("../test_data/ws_welcome.json");
pub const WS_AUTHENTICATED_FUTURES: &str =
    include_str!("../test_data/ws_authenticated_futures.json");
pub const WS_SUBSCRIBED_GOLD: &str = include_str!("../test_data/ws_subscribed_gold.json");
pub const WS_UNSUBSCRIBED_GOLD: &str = include_str!("../test_data/ws_unsubscribed_gold.json");
pub const WS_TICKS_GOLD: &str = include_str!("../test_data/ws_ticks_gold.jsonl");
pub const WS_PONG: &str = include_str!("../test_data/ws_pong.json");
pub const WS_LIMIT_REACHED: &str = include_str!("../test_data/ws_error_limit_reached.json");
pub const WS_TOO_MANY_CONNECTIONS: &str =
    include_str!("../test_data/ws_error_too_many_connections.json");
pub const WS_INVALID_KEY: &str = include_str!("../test_data/ws_error_invalid_key.json");

/// LSE's sockets' frames, captured with the free key from 22:09 to 22:16
/// UTC on 13 November 2026 (`test_data/README.md`): each socket's
/// authenticated frame, its list trimmed, and the first tick, LSE's last,
/// after the subscription of a series of each market the tests stream
/// there. data-ws's list is trimmed to `GOLD.F`, `AAPL`, `BRK.B`,
/// `EUR/USD`, `XAU/USD`, `SPX.IDX` and `BTC/USD`, which it lists and of
/// which it sent only a tick of 8 November, crypto streaming on london-ws
/// since; london-ws's to its UK stocks and crypto; tsx-ws's to its
/// Canadian stocks, `NVDA.TO` alone, although it streamed `RY.TO` too.
pub const WS_AUTHENTICATED_DATA: &str = include_str!("../test_data/ws_authenticated_data.json");
pub const WS_TICKS_DATA: &str = include_str!("../test_data/ws_ticks_data.jsonl");
pub const WS_AUTHENTICATED_LONDON: &str = include_str!("../test_data/ws_authenticated_london.json");
pub const WS_TICKS_LONDON: &str = include_str!("../test_data/ws_ticks_london.jsonl");
pub const WS_AUTHENTICATED_TSX: &str = include_str!("../test_data/ws_authenticated_tsx.json");
pub const WS_TICKS_TSX: &str = include_str!("../test_data/ws_ticks_tsx.jsonl");

/// The export id LSE sends with a file (`x-export-id`), the one it gave
/// `GOLD_TRADES`; the peer sends it with every file.
pub const EXPORT_ID: &str = "15f88a2dca3f401d90c7acae13236f86";

/// The series of `GOLD_TRADES` and `GOLD_FUTURES_4H`.
pub const GOLD_FUTURES: &str = "GOLD-FUTURES.LSEDGE";

/// The series of `GOLD_SPOT_MINUTES`.
pub const GOLD_SPOT: &str = "XAU/USD-COMMODITIES.LSEDGE";

/// The key the tests give the adapter, which the peer records.
pub const API_KEY: &str = "lse-test-key-0123456789";

/// A definition of `GOLD_FUTURES` as one would be written by hand: an
/// index in USD, its prices to a tenth and its sizes whole lots, as LSE
/// sends them, stamped at 1970 so that it stands for any time.
#[must_use]
pub fn gold_futures() -> InstrumentAny {
    InstrumentAny::IndexInstrument(
        IndexInstrument::builder()
            .instrument_id(InstrumentId::from(GOLD_FUTURES))
            .raw_symbol(Symbol::from("GOLD"))
            .currency(Currency::USD())
            .price_precision(1)
            .size_precision(0)
            .price_increment(Price::from("0.1"))
            .size_increment(Quantity::from(1))
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .expect("a valid index"),
    )
}

/// A definition of `GOLD_SPOT` as one would be written by hand: an index in
/// USD, its prices to a cent, as LSE sends them, and its sizes whole,
/// stamped at 1970 so that it stands for any time.
#[must_use]
pub fn gold_spot() -> InstrumentAny {
    InstrumentAny::IndexInstrument(
        IndexInstrument::builder()
            .instrument_id(InstrumentId::from(GOLD_SPOT))
            .raw_symbol(Symbol::from("XAU/USD"))
            .currency(Currency::USD())
            .price_precision(2)
            .size_precision(0)
            .price_increment(Price::from("0.01"))
            .size_increment(Quantity::from(1))
            .ts_event(UnixNanos::default())
            .ts_init(UnixNanos::default())
            .build()
            .expect("a valid index"),
    )
}

/// GCZ6 of 2026 as Databento defines it, `GCZ26.GLBX` (as Nautilus' Databento
/// client wrote it into a catalog): COMEX gold, 100 troy ounces, priced in
/// USD to two decimals in ticks of a tenth.
#[must_use]
pub fn gcz26() -> InstrumentAny {
    let at = |time: &str| UnixNanos::from(time.parse::<jiff::Timestamp>().expect("a time"));
    InstrumentAny::FuturesContract(
        FuturesContract::builder()
            .instrument_id(InstrumentId::from(GCZ26))
            .raw_symbol(Symbol::from("GCZ6"))
            .asset_class(AssetClass::Commodity)
            .exchange("XCEC".into())
            .underlying("GC".into())
            .activation_ns(at("2020-12-30T22:30:00Z"))
            .expiration_ns(at("2026-12-29T18:30:00Z"))
            .currency(Currency::USD())
            .price_precision(2)
            .price_increment(Price::from("0.10"))
            .multiplier(Quantity::from(100))
            .lot_size(Quantity::from(1))
            .min_quantity(Quantity::from(1))
            .ts_event(at("2026-07-31T00:00:00Z"))
            .ts_init(at("2026-07-31T00:00:00Z"))
            .build()
            .expect("a valid futures contract"),
    )
}

/// AAPL's 355 call of 20 November 2026 as Nautilus' Databento decoder
/// gives Databento's definition of it of 12 November 2026
/// (`decode_option_contract`), `AAPL_CALL`: an equity option in USD,
/// priced to a cent, Databento giving no tick, expiring at 16:00 New York,
/// its multiplier and lot size 1, Databento giving neither.
#[must_use]
pub fn aapl_call() -> InstrumentAny {
    let at = |time: &str| UnixNanos::from(time.parse::<jiff::Timestamp>().expect("a time"));
    InstrumentAny::OptionContract(
        OptionContract::builder()
            .instrument_id(InstrumentId::from(AAPL_CALL))
            .raw_symbol(Symbol::from("AAPL  261120C00355000"))
            .asset_class(AssetClass::Equity)
            .exchange("OPRA".into())
            .underlying("AAPL".into())
            .option_kind(OptionKind::Call)
            .strike_price(Price::from("355.00"))
            .currency(Currency::USD())
            .activation_ns(UnixNanos::default())
            .expiration_ns(at("2026-11-20T21:00:00Z"))
            .price_precision(2)
            .price_increment(Price::from("0.01"))
            .multiplier(Quantity::from(1))
            .lot_size(Quantity::from(1))
            .ts_event(UnixNanos::from(1_794_483_001_860_225_968))
            .ts_init(UnixNanos::from(1_794_483_001_860_225_968))
            .build()
            .expect("a valid option contract"),
    )
}

/// An answer of the peer.
#[derive(Debug, Clone)]
pub struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    delay: Duration,
    /// The bytes of the body sent before the connection is closed, if not
    /// all of it.
    cut_at: Option<usize>,
}

impl Reply {
    /// An export's file, as LSE sends one: 200, its SHA-256 and LSE's id
    /// of the export beside it.
    #[must_use]
    pub fn file(bytes: &[u8]) -> Self {
        Self::file_named_by(bytes, Sha256::digest(bytes).as_slice())
    }

    /// A file whose download is cut halfway: its headers, then half of it,
    /// then the connection closed.
    #[must_use]
    pub fn cut_file(bytes: &[u8]) -> Self {
        let mut reply = Self::file(bytes);
        reply.cut_at = Some(bytes.len() / 2);
        reply
    }

    /// A file sent whole beside the SHA-256 of other bytes.
    #[must_use]
    pub fn corrupt_file(bytes: &[u8]) -> Self {
        Self::file_named_by(bytes, Sha256::digest(b"other bytes").as_slice())
    }

    fn file_named_by(bytes: &[u8], sha: &[u8]) -> Self {
        let sha = sha.iter().map(|byte| format!("{byte:02x}")).collect();
        Self {
            status: 200,
            headers: vec![
                (
                    "Content-Type".into(),
                    "application/vnd.apache.parquet".into(),
                ),
                ("x-checksum-sha256".into(), sha),
                ("x-export-id".into(), EXPORT_ID.into()),
            ],
            body: bytes.to_vec(),
            delay: Duration::ZERO,
            cut_at: None,
        }
    }

    /// A JSON answer of the catalogue, as LSE sends one: 200.
    #[must_use]
    pub fn json(body: &str) -> Self {
        Self {
            status: 200,
            headers: vec![("Content-Type".into(), "application/json".into())],
            body: body.as_bytes().to_vec(),
            delay: Duration::ZERO,
            cut_at: None,
        }
    }

    /// A refusal: `status` and LSE's JSON `body`.
    #[must_use]
    pub fn refusal(status: u16, body: &str) -> Self {
        Self {
            status,
            headers: vec![("Content-Type".into(), "application/json".into())],
            body: body.as_bytes().to_vec(),
            delay: Duration::ZERO,
            cut_at: None,
        }
    }

    /// The reply with a `Retry-After` of `seconds`.
    #[must_use]
    pub fn retry_after(mut self, seconds: u64) -> Self {
        self.headers
            .push(("Retry-After".into(), seconds.to_string()));
        self
    }

    /// The reply sent `delay` after the call came.
    #[must_use]
    pub fn after(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    /// The reply with the status `status`.
    #[must_use]
    pub fn with_status(mut self, status: u16) -> Self {
        self.status = status;
        self
    }

    /// The reply without the file's SHA-256.
    #[must_use]
    pub fn without_checksum(mut self) -> Self {
        self.headers
            .retain(|(name, _)| !name.eq_ignore_ascii_case("x-checksum-sha256"));
        self
    }
}

/// A call the peer received.
#[derive(Debug, Clone)]
pub struct Call {
    /// The path, `/v3/export`, `/v3/reference`, `/v3/reference/export`.
    pub path: String,
    /// The query's parameters, decoded, in order.
    pub query: Vec<(String, String)>,
    /// The key the call carried.
    pub api_key: Option<String>,
    /// When the call came, and when its answer started.
    pub received: Instant,
    pub answered: Instant,
}

impl Call {
    /// The query parameter `name`.
    #[must_use]
    pub fn param(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// The peer: its address and the calls it received.
#[derive(Debug)]
pub struct MockLse {
    url: String,
    calls: Arc<Mutex<Vec<Call>>>,
}

impl MockLse {
    /// Starts a peer answering its calls with `script` in order, each on
    /// its own thread, and with a 500 once the script is done.
    ///
    /// # Panics
    ///
    /// Panics if no port of 127.0.0.1 is free.
    #[must_use]
    pub fn start(script: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a free port");
        let url = format!("http://{}", listener.local_addr().expect("an address"));
        let calls: Arc<Mutex<Vec<Call>>> = Arc::default();
        let log = Arc::clone(&calls);
        let script = Arc::new(Mutex::new(script.into_iter()));
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                let log = Arc::clone(&log);
                let script = Arc::clone(&script);
                std::thread::spawn(move || serve(stream, &log, &script));
            }
        });
        Self { url, calls }
    }

    /// The peer's address, the adapter's base URL.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// The calls answered so far, in the order they came.
    #[must_use]
    pub fn calls(&self) -> Vec<Call> {
        let mut calls = self.calls.lock().expect("not poisoned").clone();
        calls.sort_by_key(|call| call.received);
        calls
    }
}

/// Answers the calls of one connection, until it closes.
fn serve(stream: TcpStream, log: &Mutex<Vec<Call>>, script: &Mutex<std::vec::IntoIter<Reply>>) {
    let mut reader = BufReader::new(stream.try_clone().expect("a stream"));
    let mut writer = stream;
    loop {
        let mut request_line = String::new();
        if reader.read_line(&mut request_line).unwrap_or(0) == 0 {
            return;
        }
        let received = Instant::now();
        let mut api_key = None;
        loop {
            let mut header = String::new();
            if reader.read_line(&mut header).unwrap_or(0) == 0 {
                return;
            }
            let header = header.trim_end();
            if header.is_empty() {
                break;
            }
            if let Some((name, value)) = header.split_once(':')
                && name.eq_ignore_ascii_case("x-api-key")
            {
                api_key = Some(value.trim().to_string());
            }
        }
        let target = request_line.split_whitespace().nth(1).unwrap_or("/");
        let url = Url::parse(&format!("http://lse{target}")).expect("a request target");
        let reply = script
            .lock()
            .expect("not poisoned")
            .next()
            .unwrap_or(Reply {
                status: 500,
                headers: vec![],
                body: b"no reply left in the mock's script".to_vec(),
                delay: Duration::ZERO,
                cut_at: None,
            });
        std::thread::sleep(reply.delay);
        // Logged before the answer goes, so that a caller reading the calls
        // once it has the answer finds this one.
        log.lock().expect("not poisoned").push(Call {
            path: url.path().to_string(),
            query: url.query_pairs().into_owned().collect(),
            api_key,
            received,
            answered: Instant::now(),
        });
        let mut head = format!(
            "HTTP/1.1 {} {}\r\nContent-Length: {}\r\n",
            reply.status,
            reason(reply.status),
            reply.body.len()
        );
        for (name, value) in &reply.headers {
            head.push_str(&format!("{name}: {value}\r\n"));
        }
        head.push_str("\r\n");
        let body = &reply.body[..reply.cut_at.unwrap_or(reply.body.len())];
        let sent = writer
            .write_all(head.as_bytes())
            .and_then(|()| writer.write_all(body))
            .and_then(|()| writer.flush());
        if sent.is_err() || reply.cut_at.is_some() {
            let _ = writer.shutdown(Shutdown::Both);
            return;
        }
    }
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        202 => "Accepted",
        403 => "Forbidden",
        404 => "Not Found",
        413 => "Payload Too Large",
        429 => "Too Many Requests",
        503 => "Service Unavailable",
        _ => "Status",
    }
}
