//! A mock Databento historical API: an HTTP server on 127.0.0.1 answering the two calls a
//! definitions fill makes with Databento's captured answer for the call's dataset (`test_data/`),
//! whatever else they ask, and recording each call.
//!
//! - `metadata.get_dataset_range`: `GLBX.MDP3` captured 2026-11-13 (end
//!   2026-11-13T09:31:29.885311Z), `OPRA.PILLAR` captured 2026-11-13 (end 2026-11-13T14:30:00Z),
//!   `XNAS.ITCH` captured 2026-11-14
//!   (end 2026-11-13T05:00:00Z).
//! - `timeseries.get_range`, definition records of 12 November 2026 (`schema=definition`,
//!   `stype_in=raw_symbol`, zstd-compressed DBN, as Databento's client asks), each DBN version 1 as
//!   sent:
//!   - `GLBX.MDP3`: GCZ6 (`symbols=GCZ6`), captured 2026-11-13 for $0.0000008.
//!   - `OPRA.PILLAR`: AAPL's 355 call of 20 November 2026 (`symbols=AAPL 261120C00355000`),
//!     captured 2026-11-13 for $0.0000017, the cost `metadata.get_cost` gave before.
//!   - `XNAS.ITCH`: NVDA's three records (`symbols=NVDA`), captured 2026-11-14 for $0.000016093,
//!     the cost `metadata.get_cost` gave before. Nautilus' client answers with the last, in round
//!     lots of 100.

use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
};

use nautilus_network::http::Url;

/// Each dataset's range and definition: `GCZ26.GLBX`, `AAPL 261120C00355000.OPRA`, `NVDA.XNAS`.
const DATASETS: [(&str, &[u8], &[u8]); 3] = [
    (
        "GLBX.MDP3",
        include_bytes!("test_data/dataset_range_glbx.json"),
        include_bytes!("test_data/definition_gcz6.dbn.zst"),
    ),
    (
        "OPRA.PILLAR",
        include_bytes!("test_data/dataset_range_opra.json"),
        include_bytes!("test_data/definition_aapl_call.dbn.zst"),
    ),
    (
        "XNAS.ITCH",
        include_bytes!("test_data/dataset_range_xnas.json"),
        include_bytes!("test_data/definition_nvda.dbn.zst"),
    ),
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DatabentoCall {
    pub(crate) path: String,
    pub(crate) params: Vec<(String, String)>,
}

impl DatabentoCall {
    pub(crate) fn param(&self, name: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

#[derive(Debug)]
pub(crate) struct MockDatabento {
    url: String,
    calls: Arc<Mutex<Vec<DatabentoCall>>>,
}

impl MockDatabento {
    /// Starts the peer, answering each call on its own thread.
    pub(crate) fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a free port");
        let url = format!("http://{}/", listener.local_addr().expect("an address"));
        let calls: Arc<Mutex<Vec<DatabentoCall>>> = Arc::default();
        let log = Arc::clone(&calls);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                let log = Arc::clone(&log);
                std::thread::spawn(move || serve(stream, &log));
            }
        });
        Self { url, calls }
    }

    /// The historical API's base URL.
    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    pub(crate) fn calls(&self) -> Vec<DatabentoCall> {
        self.calls.lock().expect("not poisoned").clone()
    }
}

fn serve(stream: TcpStream, log: &Mutex<Vec<DatabentoCall>>) {
    let mut reader = BufReader::new(stream.try_clone().expect("a stream"));
    let mut writer = stream;
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).unwrap_or(0) == 0 {
        return;
    }
    let mut length = 0;
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
            && name.eq_ignore_ascii_case("content-length")
        {
            length = value.trim().parse().expect("a length");
        }
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body).expect("the body");
    let target = request_line.split_whitespace().nth(1).unwrap_or("/");
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let params = [query.as_bytes(), &body]
        .into_iter()
        .flat_map(|encoded| {
            Url::parse(&format!(
                "http://databento/?{}",
                String::from_utf8_lossy(encoded)
            ))
            .expect("parameters")
            .query_pairs()
            .into_owned()
            .collect::<Vec<_>>()
        })
        .collect();
    let call = DatabentoCall {
        path: path.to_string(),
        params,
    };
    let dataset = DATASETS
        .iter()
        .find(|(dataset, ..)| call.param("dataset") == Some(dataset));
    let (content_type, answer) = match dataset {
        Some((_, range, _)) if path.ends_with("metadata.get_dataset_range") => {
            ("application/json", *range)
        }
        Some((_, _, definition)) if path.ends_with("timeseries.get_range") => {
            ("application/octet-stream", *definition)
        }
        _ => ("text/plain", &b"no such call or dataset in the mock"[..]),
    };
    log.lock().expect("not poisoned").push(call);
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        answer.len()
    );
    let _ = writer
        .write_all(head.as_bytes())
        .and_then(|()| writer.write_all(answer))
        .and_then(|()| writer.flush());
}
