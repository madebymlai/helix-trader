//! A mock Databento historical API for the tests: an HTTP server on
//! 127.0.0.1 answering the two calls a definitions fill makes with what
//! Databento answered (`testdata/`), whatever they ask, and recording each
//! call it receives.
//!
//! - `metadata.get_dataset_range`: the range of `GLBX.MDP3`, captured on
//!   2026-11-13 (its end 2026-11-13T09:31:29.885311Z).
//! - `timeseries.get_range`: GCZ6's definition records of 12 November 2026
//!   (`schema=definition`, `symbols=GCZ6`, `stype_in=raw_symbol`, DBN
//!   compressed with zstd, as Databento's client asks), captured on
//!   2026-11-13 for $0.0000008.

use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{Arc, Mutex},
};

/// Databento's range of `GLBX.MDP3`.
const DATASET_RANGE: &[u8] = include_bytes!("testdata/dataset_range_glbx.json");
/// Databento's definition of GCZ6 of 2026, `GCZ26.GLBX`.
const DEFINITION_GCZ6: &[u8] = include_bytes!("testdata/definition_gcz6.dbn.zst");

/// A call the peer received: its path and its parameters, of its query or
/// its form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DatabentoCall {
    pub(crate) path: String,
    pub(crate) params: Vec<(String, String)>,
}

impl DatabentoCall {
    /// The parameter `name`.
    pub(crate) fn param(&self, name: &str) -> Option<&str> {
        self.params
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }
}

/// The peer: its address and the calls it received.
#[derive(Debug)]
pub(crate) struct MockDatabento {
    url: String,
    calls: Arc<Mutex<Vec<DatabentoCall>>>,
}

impl MockDatabento {
    /// Starts the peer, answering each call on a thread of its own.
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

    /// The peer's address, the historical API's base URL.
    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    /// The calls answered so far, in the order they came.
    pub(crate) fn calls(&self) -> Vec<DatabentoCall> {
        self.calls.lock().expect("not poisoned").clone()
    }
}

/// Answers the one call of a connection, then closes it.
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
            nautilus_network::http::Url::parse(&format!(
                "http://databento/?{}",
                String::from_utf8_lossy(encoded)
            ))
            .expect("parameters")
            .query_pairs()
            .into_owned()
            .collect::<Vec<_>>()
        })
        .collect();
    let (content_type, answer) = if path.ends_with("metadata.get_dataset_range") {
        ("application/json", DATASET_RANGE)
    } else if path.ends_with("timeseries.get_range") {
        ("application/octet-stream", DEFINITION_GCZ6)
    } else {
        ("text/plain", &b"no such call in the mock"[..])
    };
    log.lock().expect("not poisoned").push(DatabentoCall {
        path: path.to_string(),
        params,
    });
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        answer.len()
    );
    let _ = writer
        .write_all(head.as_bytes())
        .and_then(|()| writer.write_all(answer))
        .and_then(|()| writer.flush());
}
