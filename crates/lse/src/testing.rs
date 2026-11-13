//! A mock LSE peer for the tests, the adapter's and its callers': an HTTP
//! server on 127.0.0.1 answering each call with the next reply of a
//! script, replaying payloads captured from LSE (`test_data/`, see its
//! `README.md`), and recording each call it receives. Built for the
//! crate's tests and, with the `test-support` feature, for a caller's.

use std::{
    io::{BufRead, BufReader, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use nautilus_core::UnixNanos;
use nautilus_model::{
    identifiers::{InstrumentId, Symbol},
    instruments::{IndexInstrument, InstrumentAny},
    types::{Currency, Price, Quantity},
};
use nautilus_network::http::Url;
use sha2::{Digest, Sha256};

/// The trade slice of LSE's gold futures series (`GOLD` in `futures`) over
/// `[2026-10-21T10:00:00Z, 2026-10-21T10:00:01Z)`, as LSE exported it:
/// six trades.
pub const GOLD_TRADES: &[u8] = include_bytes!("../test_data/export_gold_futures_tick.parquet");
/// LSE's refusals, captured with the free key, but the calls a minute and
/// the quotas, which are LSE's documented answers, and the export too wide,
/// captured with the CME key (`test_data/README.md`).
pub const SYMBOL_UNKNOWN: &str = include_str!("../test_data/refusal_symbol_unknown.json");
pub const PLAN_REQUIRED: &str = include_str!("../test_data/refusal_plan_required.json");
pub const DATA_UNAVAILABLE: &str = include_str!("../test_data/refusal_data_unavailable.json");
pub const DATE_NOT_TRADING: &str = include_str!("../test_data/refusal_date_not_trading.json");
pub const CALLS_EXHAUSTED: &str = include_str!("../test_data/refusal_calls_exhausted.json");
pub const ROWS_EXHAUSTED: &str = include_str!("../test_data/refusal_rows_exhausted.json");
pub const ALLOWANCE_EXHAUSTED: &str =
    include_str!("../test_data/refusal_allowance_exhausted.json");
/// LSE's refusal of a day of GCZ6's quotes, past the rows one export may
/// read.
pub const EXPORT_TOO_WIDE: &str = include_str!("../test_data/refusal_export_too_wide.json");
/// LSE's answer for a day it had not loaded yet, the current one.
pub const DATA_UNAVAILABLE_TODAY: &str =
    include_str!("../test_data/refusal_data_unavailable_today.json");
/// LSE's answer for the tick slice of a market storing no trades, `forex`.
pub const TICKS_NOT_STORED: &str = include_str!("../test_data/refusal_data_unavailable_tick.json");

/// The export id LSE sends with a file (`x-export-id`), the one it gave
/// `GOLD_TRADES`; the peer sends it with every file.
pub const EXPORT_ID: &str = "15f88a2dca3f401d90c7acae13236f86";

/// The series of `GOLD_TRADES`.
pub const GOLD_FUTURES: &str = "GOLD-FUTURES.LSEDGE";

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
    /// The path, `/v3/export`.
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
