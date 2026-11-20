//! Mock LSE peers for the tests of the adapter and its callers.
//!
//! There are two peers. One is an HTTP server on 127.0.0.1 that answers each call with the next
//! reply of a script. The other is a socket (`MockLseSocket`) that answers each frame as a script
//! says.
//!
//! Both replay payloads captured from LSE (`test_data/`, see its `README.md`) and record what they
//! receive. The `test-support` feature exposes them to a caller's tests.

use std::{
    io::{BufRead, BufReader, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use nautilus_core::UnixNanos;
use nautilus_model::{
    enums::{AssetClass, ExerciseStyle, OptionKind, SettlementType},
    identifiers::{InstrumentId, Symbol},
    instruments::{Equity, FuturesContract, IndexInstrument, InstrumentAny, OptionContract},
    types::{Currency, Price, Quantity},
};
use nautilus_network::http::Url;
use sha2::{Digest, Sha256};

pub use self::{
    exchange::{
        CME_ESZ6_DOCUMENTED, CME_GCZ6_GCG7, CME_GCZ6_JSON, CME_GCZ6_L2_DOCUMENTED,
        CME_GCZ6_L3_DOCUMENTED, CME_KEY_UNKNOWN, CME_LEVEL_REQUIRED, CME_SYMBOL_UNAVAILABLE, ESZ26,
        MockExchangeStream, NASDAQ_KEY_UNKNOWN, NASDAQ_NVDA_L1_DOCUMENTED,
        NASDAQ_NVDA_L2_DOCUMENTED, NASDAQ_NVDA_L3_DOCUMENTED, StreamAction, StreamFrame, dbn_frame,
        dbn_frames, frames, held, sent,
    },
    socket::{MockLseSocket, SocketAction, data_ws_answer, socket_answer},
};

mod exchange;
mod socket;

/// `GOLD` in `futures`, tick slice over `[2026-10-21T10:00:00Z, 10:00:01Z)`: six trades.
pub const GOLD_TRADES: &[u8] = include_bytes!("../test_data/export_gold_futures_tick.parquet");
/// LSE's catalogue of its reference collections (`/v3/reference`).
pub const REFERENCE_CATALOGUE: &str = include_str!("../test_data/reference_catalogue.json");
/// `US10Y` in `bond_yields`, 9 to 12 November 2026.
pub const US10Y_BOND_YIELDS: &[u8] =
    include_bytes!("../test_data/reference_bond_yields_us10y.parquet");
/// `US10Y` in `bond_yields_four_hour`, 10 November 2026: six rows, 00:00 to 20:00.
pub const US10Y_BOND_YIELDS_FOUR_HOURS: &[u8] =
    include_bytes!("../test_data/reference_bond_yields_four_hour_us10y.parquet");
/// `US10Y` in the other interval collections, 10 November 2026. Each row is stamped at its
/// interval's open.
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
/// A series of each collection of a day's market values, a row a field.
pub const AUGB10Y_SOVEREIGN_YIELDS: &[u8] =
    include_bytes!("../test_data/reference_sovereign_yields_augb10y.parquet");
pub const AT016322245_CORPORATE_BONDS: &[u8] =
    include_bytes!("../test_data/reference_corporate_bonds_at016322245.parquet");
pub const CNH_CREDIT_INDICES: &[u8] =
    include_bytes!("../test_data/reference_credit_indices_cnh_nonfin_ig_0_1.parquet");
pub const AUDUSD_FX_DERIVATIVES: &[u8] =
    include_bytes!("../test_data/reference_fx_derivatives_audusd_1m_atm.parquet");
/// Atlassian's (`TEAM`) insider trade filed on 13 October 2026.
pub const TEAM_INSIDER_TRADES: &[u8] =
    include_bytes!("../test_data/reference_insider_trades_team.parquet");
/// AAPL's earnings calendar and dividends of 2026, and its stock splits since 2014.
pub const AAPL_EARNINGS_CALENDAR: &[u8] =
    include_bytes!("../test_data/reference_earnings_calendar_aapl.parquet");
pub const AAPL_DIVIDENDS: &[u8] = include_bytes!("../test_data/reference_dividends_aapl.parquet");
pub const AAPL_STOCK_SPLITS: &[u8] =
    include_bytes!("../test_data/reference_stock_splits_aapl.parquet");
/// `AHMA`'s snapshot, updated on 13 October 2026 at 05:08:01 UTC.
pub const AHMA_COMPANY_PROFILES: &[u8] =
    include_bytes!("../test_data/reference_company_profiles_ahma.parquet");
pub const AHMA_STOCK_FUNDAMENTALS: &[u8] =
    include_bytes!("../test_data/reference_stock_fundamentals_ahma.parquet");
/// The whole series of `US.Core Inflation Rate YoY`: 14 rows. The past releases of 11 February
/// and 10 November 2026 have no actual.
pub const US_CORE_CPI_CALENDAR: &[u8] =
    include_bytes!("../test_data/reference_economic_calendar_us_core_cpi.parquet");
/// The whole series of `ZN` in `cot`: 614 reports. Those since 28 April 2026 have no release
/// date.
pub const ZN_COT: &[u8] = include_bytes!("../test_data/reference_cot_zn.parquet");
/// AAPL's 38 financial rows: statements, derived rows and trailing twelve months.
pub const AAPL_FINANCIAL_REPORTS: &[u8] =
    include_bytes!("../test_data/reference_financial_reports_aapl.parquet");
/// US core inflation's releases and `US10Y`'s yields of 9 to 12 November 2026.
pub const US_CORE_CPI_RELEASES: &[u8] =
    include_bytes!("../test_data/macro_release_us_core_cpi.parquet");
pub const US10Y_YIELDS: &[u8] = include_bytes!("../test_data/macro_yield_us10y.parquet");
/// The factors of AAPL in `us_stocks` (five splits, 93 dividends) and of GOLD in `futures` (none).
pub const AAPL_FACTORS: &str = include_str!("../test_data/factors_aapl_us_stocks.json");
pub const GOLD_FACTORS: &str = include_str!("../test_data/factors_gold_futures.json");
/// `XAU/USD` in `commodities`, 1-minute candles: five, of no volume.
pub const GOLD_SPOT_MINUTES: &[u8] =
    include_bytes!("../test_data/export_xau_usd_commodities_1m.parquet");
/// `GOLD` in `futures`, 4-hour candles: three.
pub const GOLD_FUTURES_4H: &[u8] = include_bytes!("../test_data/export_gold_futures_4h.parquet");
/// A series of each other derived market: trade slices of `AAPL`, `VOD`, `RY` and `BTCUSDT`, and
/// 1-minute candles of `EUR_USD` and `SPX`.
pub const AAPL_TRADES: &[u8] = include_bytes!("../test_data/export_aapl_us_stocks_tick.parquet");
pub const VOD_TRADES: &[u8] = include_bytes!("../test_data/export_vod_uk_stocks_tick.parquet");
pub const RY_TRADES: &[u8] = include_bytes!("../test_data/export_ry_ca_stocks_tick.parquet");
pub const BTCUSDT_TRADES: &[u8] = include_bytes!("../test_data/export_btcusdt_crypto_tick.parquet");
pub const EUR_USD_MINUTES: &[u8] = include_bytes!("../test_data/export_eur_usd_forex_1m.parquet");
pub const SPX_MINUTES: &[u8] = include_bytes!("../test_data/export_spx_indices_1m.parquet");
/// `SPX` in `indices`, tick slice: three values of size 0.
pub const SPX_TICKS: &[u8] = include_bytes!("../test_data/export_spx_indices_tick.parquet");
/// LSE's refusals. The calls-a-minute and quota answers are LSE's documented ones, not captured.
pub const SYMBOL_UNKNOWN: &str = include_str!("../test_data/refusal_symbol_unknown.json");
pub const PLAN_REQUIRED: &str = include_str!("../test_data/refusal_plan_required.json");
pub const DATA_UNAVAILABLE: &str = include_str!("../test_data/refusal_data_unavailable.json");
pub const DATE_NOT_TRADING: &str = include_str!("../test_data/refusal_date_not_trading.json");
pub const REFERENCE_DATA_UNAVAILABLE: &str =
    include_str!("../test_data/refusal_reference_data_unavailable.json");
pub const CALLS_EXHAUSTED: &str = include_str!("../test_data/refusal_calls_exhausted.json");
pub const ROWS_EXHAUSTED: &str = include_str!("../test_data/refusal_rows_exhausted.json");
pub const ALLOWANCE_EXHAUSTED: &str = include_str!("../test_data/refusal_allowance_exhausted.json");
/// A day of GCZ6's quotes, past the rows one export may read.
pub const EXPORT_TOO_WIDE: &str = include_str!("../test_data/refusal_export_too_wide.json");
/// The current day, not loaded yet.
pub const DATA_UNAVAILABLE_TODAY: &str =
    include_str!("../test_data/refusal_data_unavailable_today.json");
/// The tick slice of `forex`, which stores no trades.
pub const TICKS_NOT_STORED: &str = include_str!("../test_data/refusal_data_unavailable_tick.json");
/// A 400 for a derived market's quotes: LSE serves them as timeframes alone.
pub const DATA_UNAVAILABLE_QUOTES: &str =
    include_str!("../test_data/refusal_data_unavailable_quotes.json");
/// CME rows older than CME Group Lite+'s twelve years of Level 1.
pub const HISTORY_LIMIT: &str = include_str!("../test_data/refusal_history_limit.json");

/// LSE's catalogue of CME Globex (`/v3/pairs?market=cme&symbol=…`).
///
/// ESZ6 and GCZ6 name the contracts of 2016 and 2026. GCZ4 names 2014's and 2024's, the second by
/// its symbol alone. NGZ6 names 2016's alone, because 2026's is NGZ26. LSE lists no ESZ26.
pub const PAIRS_ESZ6: &str = include_str!("../test_data/pairs_cme_esz6.json");
pub const PAIRS_GCZ6: &str = include_str!("../test_data/pairs_cme_gcz6.json");
pub const PAIRS_GCZ4: &str = include_str!("../test_data/pairs_cme_gcz4.json");
pub const PAIRS_NGZ6: &str = include_str!("../test_data/pairs_cme_ngz6.json");
pub const PAIRS_NGZ26: &str = include_str!("../test_data/pairs_cme_ngz26.json");
pub const PAIRS_ESZ26: &str = include_str!("../test_data/pairs_cme_esz26.json");
/// LSE's catalogue of GCG7, the contracts of 2017 and 2027.
///
/// GCG7 is the next GC contract after GCZ6 on the CME stream (`CME_GCZ6_GCG7`).
pub const PAIRS_GCG7: &str = include_str!("../test_data/pairs_cme_gcg7.json");

/// The pair id LSE gives GCZ6 of 2026, `GCZ26.GLBX`.
pub const GCZ26_PAIR_ID: u64 = 4_000_000_002;
/// The trades and quotes of GCZ6 of 2026 by pair id.
pub const GCZ26_TRADES: &[u8] = include_bytes!("../test_data/export_gcz6_trades.parquet");
pub const GCZ26_QUOTES: &[u8] = include_bytes!("../test_data/export_gcz6_quotes.parquet");
/// GCZ6's book and orders, built and not captured (`test_data/README.md`). The book has four rows
/// with ten levels a side, except the last row, which has eight asks. The orders have five rows,
/// one with no action.
pub const GCZ26_BOOK: &[u8] = include_bytes!("../test_data/export_gcz6_book.parquet");
pub const GCZ26_ORDERS: &[u8] = include_bytes!("../test_data/export_gcz6_orders.parquet");
/// LSE's refusals of GCZ6's book and orders to the CME Group Lite+ key, Level 1 alone.
pub const LEVEL_REQUIRED_BOOK: &str = include_str!("../test_data/refusal_level_required_book.json");
pub const LEVEL_REQUIRED_ORDERS: &str =
    include_str!("../test_data/refusal_level_required_orders.json");

/// The CME contract of `GCZ26_TRADES`.
pub const GCZ26: &str = "GCZ26.GLBX";

/// AAPL's 355 call of 20 November 2026 (`AAPL 261120C00355000` in `us_options`): trades, and
/// 1-minute candles from 14:30 to 14:33. There is none at 14:34, for want of a trade.
pub const AAPL_CALL_TRADES: &[u8] = include_bytes!("../test_data/export_aapl_call_tick.parquet");
pub const AAPL_CALL_MINUTES: &[u8] = include_bytes!("../test_data/export_aapl_call_1m.parquet");

/// The contract of `AAPL_CALL_TRADES`: its OSI symbol under `OPRA`.
pub const AAPL_CALL: &str = "AAPL  261120C00355000.OPRA";

/// The CME key the tests give the adapter.
pub const CME_API_KEY: &str = "lse-cme-test-key-0123456789";

/// LSE's catalogue of Nasdaq TotalView (`/v3/pairs?market=nasdaq`).
///
/// It holds NVDA's listing, 2000008377 (also its number in `us_stocks`), and the 21 listings for
/// `q=BRK`, among them `BRK.B` and `BRKH` twice, delisted on 18 December 2024 and listed now.
pub const PAIRS_NASDAQ_NVDA: &str = include_str!("../test_data/pairs_nasdaq_nvda.json");
pub const PAIRS_NASDAQ_BRK: &str = include_str!("../test_data/pairs_nasdaq_brk.json");

/// LSE's `403 plan-required` (Nasdaq Lite) for Nasdaq TotalView's rows.
pub const PLAN_REQUIRED_NASDAQ: &str =
    include_str!("../test_data/refusal_plan_required_nasdaq.json");

/// NVDA's trades, quotes, book and orders, built and not captured, as no helix key holds a Nasdaq
/// plan (`test_data/README.md`).
///
/// They hold four trades, two of them alike, four quotes, and a book and orders shaped as
/// `GCZ26_BOOK` and `GCZ26_ORDERS`.
pub const NVDA_TRADES: &[u8] = include_bytes!("../test_data/export_nvda_trades.parquet");
pub const NVDA_QUOTES: &[u8] = include_bytes!("../test_data/export_nvda_quotes.parquet");
pub const NVDA_BOOK: &[u8] = include_bytes!("../test_data/export_nvda_book.parquet");
pub const NVDA_ORDERS: &[u8] = include_bytes!("../test_data/export_nvda_orders.parquet");

/// The stock of `NVDA_TRADES`: its ticker under `XNAS`.
pub const NVDA: &str = "NVDA.XNAS";

/// The Nasdaq key the tests give the adapter.
pub const NASDAQ_API_KEY: &str = "lse-nasdaq-test-key-0123456789";

/// LSE's data-ws frames (`test_data/README.md`).
///
/// They are:
/// - The welcome.
/// - The authenticated frame, trimmed to the futures.
/// - `GOLD.F`'s subscription, unsubscription and first three ticks.
/// - A pong.
/// - The refusals of a 17th symbol on the key's 16, of a second connection and of an unknown key.
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

/// LSE's socket frames (`test_data/README.md`).
///
/// Each socket has its authenticated frame with the list trimmed, and the first tick (LSE's last)
/// after subscribing a series of each market streamed there. data-ws sent only a tick of 8
/// November for crypto, which has streamed on london-ws since.
pub const WS_AUTHENTICATED_DATA: &str = include_str!("../test_data/ws_authenticated_data.json");
pub const WS_TICKS_DATA: &str = include_str!("../test_data/ws_ticks_data.jsonl");
pub const WS_AUTHENTICATED_LONDON: &str = include_str!("../test_data/ws_authenticated_london.json");
pub const WS_TICKS_LONDON: &str = include_str!("../test_data/ws_ticks_london.jsonl");
pub const WS_AUTHENTICATED_TSX: &str = include_str!("../test_data/ws_authenticated_tsx.json");
pub const WS_TICKS_TSX: &str = include_str!("../test_data/ws_ticks_tsx.jsonl");

/// LSE's data-ws frames of an option chain (`test_data/README.md`).
///
/// They are the acknowledgements of AAPL's chain and the refusal of `NOSUCHX`, which lists no
/// options. They were captured with US options closed. The five option ticks are built on the tick
/// that LSE's WebSocket documentation gives, from trades of AAPL's chain.
pub const WS_OPTIONS_SUBSCRIBED_AAPL: &str =
    include_str!("../test_data/ws_options_subscribed_aapl.json");
pub const WS_OPTIONS_UNSUBSCRIBED_AAPL: &str =
    include_str!("../test_data/ws_options_unsubscribed_aapl.json");
pub const WS_INVALID_UNDERLYING: &str =
    include_str!("../test_data/ws_error_invalid_underlying.json");
pub const WS_TICKS_AAPL_OPTIONS: &str =
    include_str!("../test_data/ws_ticks_aapl_options_documented.jsonl");

/// AAPL's 325 put of 17 September 2027, the last trade of `WS_TICKS_AAPL_OPTIONS`.
pub const AAPL_PUT_2027: &str = "AAPL  270917P00325000.OPRA";

/// The `x-export-id` LSE gave `GOLD_TRADES`, sent with every file.
pub const EXPORT_ID: &str = "15f88a2dca3f401d90c7acae13236f86";

/// The series of `GOLD_TRADES` and `GOLD_FUTURES_4H`.
pub const GOLD_FUTURES: &str = "GOLD-FUTURES.LSEDGE";

/// The series of `GOLD_SPOT_MINUTES`.
pub const GOLD_SPOT: &str = "XAU/USD-COMMODITIES.LSEDGE";

/// The key the tests give the adapter.
pub const API_KEY: &str = "lse-test-key-0123456789";

/// A hand-written definition of `GOLD_FUTURES`, stamped at 1970 so that it stands for any time.
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

/// A hand-written definition of `GOLD_SPOT`, stamped at 1970 so that it stands for any time.
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

/// GCZ6 of 2026 as Databento defines it, `GCZ26.GLBX`, as Nautilus' Databento client wrote it
/// into a catalog.
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

/// ESZ6 of 2026, `ESZ26.GLBX`, the contract of `CME_ESZ6_DOCUMENTED`.
///
/// It was listed on 17 September 2021 as `PAIRS_ESZ6` lists it. Trading ends on the third Friday
/// of December 2026 at 09:30 New York.
#[must_use]
pub fn esz26() -> InstrumentAny {
    let at = |time: &str| UnixNanos::from(time.parse::<jiff::Timestamp>().expect("a time"));
    InstrumentAny::FuturesContract(
        FuturesContract::builder()
            .instrument_id(InstrumentId::from(ESZ26))
            .raw_symbol(Symbol::from("ESZ6"))
            .asset_class(AssetClass::Index)
            .exchange("XCME".into())
            .underlying("ES".into())
            .activation_ns(at("2021-09-17T00:00:00Z"))
            .expiration_ns(at("2026-12-18T14:30:00Z"))
            .currency(Currency::USD())
            .price_precision(2)
            .price_increment(Price::from("0.25"))
            .multiplier(Quantity::from(50))
            .lot_size(Quantity::from(1))
            .min_quantity(Quantity::from(1))
            .ts_event(at("2021-09-17T00:00:00Z"))
            .ts_init(at("2021-09-17T00:00:00Z"))
            .build()
            .expect("a valid futures contract"),
    )
}

/// NVDA as Nautilus' Databento decoder (`decode_equity`) gives it from Databento's last definition
/// of 12 November 2026 in `XNAS.ITCH`. It is stamped when Databento received the definition.
#[must_use]
pub fn nvda() -> InstrumentAny {
    InstrumentAny::Equity(
        Equity::builder()
            .instrument_id(InstrumentId::from(NVDA))
            .raw_symbol(Symbol::from("NVDA"))
            .currency(Currency::USD())
            .price_precision(2)
            .price_increment(Price::from("0.01"))
            .lot_size(Quantity::from(100))
            .ts_event(UnixNanos::from(1_794_470_692_980_137_640))
            .ts_init(UnixNanos::from(1_794_470_692_980_137_640))
            .build()
            .expect("a valid equity"),
    )
}

/// `AAPL_CALL` as Nautilus' Databento decoder (`decode_option_contract`) gives it from Databento's
/// definition. It expires at 16:00 New York. Databento gives no root, multiplier or lot size, so
/// Nautilus takes the root, a multiplier of 100 and a lot size of 1.
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
            .multiplier(Quantity::from(100))
            .lot_size(Quantity::from(1))
            .exercise_style(ExerciseStyle::American)
            .settlement_type(SettlementType::Physical)
            .ts_event(UnixNanos::from(1_794_483_001_860_225_968))
            .ts_init(UnixNanos::from(1_794_483_001_860_225_968))
            .build()
            .expect("a valid option contract"),
    )
}

/// `AAPL_PUT_2027`, defined as `aapl_call` is.
#[must_use]
pub fn aapl_put_2027() -> InstrumentAny {
    let InstrumentAny::OptionContract(mut put) = aapl_call() else {
        unreachable!("an option contract")
    };
    put.id = InstrumentId::from(AAPL_PUT_2027);
    put.raw_symbol = Symbol::from("AAPL  270917P00325000");
    put.option_kind = OptionKind::Put;
    put.strike_price = Price::from("325.00");
    put.expiration_ns = UnixNanos::from(
        "2027-09-17T20:00:00Z"
            .parse::<jiff::Timestamp>()
            .expect("a time"),
    );
    InstrumentAny::OptionContract(put)
}

/// An answer of the peer.
#[derive(Debug, Clone)]
pub struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    delay: Duration,
    // The bytes of the body sent before the connection closes, if not all
    cut_at: Option<usize>,
}

impl Reply {
    /// An export's file, as LSE sends one: 200 with its SHA-256 and export id.
    #[must_use]
    pub fn file(bytes: &[u8]) -> Self {
        Self::file_named_by(bytes, Sha256::digest(bytes).as_slice())
    }

    /// A file cut halfway: its headers, half of it, then the connection closed.
    #[must_use]
    pub fn cut_file(bytes: &[u8]) -> Self {
        let mut reply = Self::file(bytes);
        reply.cut_at = Some(bytes.len() / 2);
        reply
    }

    /// A file sent whole with the SHA-256 of other bytes.
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

    /// A 200 with a JSON `body`, as LSE sends its catalogue.
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
    /// The path, such as `/v3/export`.
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
    /// Starts a peer that answers its calls with `script` in order, each on its own thread. Once
    /// the script is done, it answers with a 500.
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

    /// The peer's address.
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

        // Logged before the answer goes, so that a caller holding the answer finds this call.
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
