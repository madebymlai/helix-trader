//! LSE's CME Globex stream for the tests: its frames, captured with the CME
//! Group Lite+ key or as LSE's documentation gives them (`test_data/`, see
//! its `README.md`), and a mock of the stream (`MockCmeStream`): a WebSocket
//! server on 127.0.0.1 that takes a connection only with the CME key in its
//! `X-API-Key` header, as LSE does, answers its subscription with what a
//! script says, and records each subscription and key it receives.

use std::{
    ffi::c_char,
    net::TcpListener,
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

use dbn::{BidAskPair, FlagSet, Mbp1Msg, RecordHeader, RecordRef, rtype};
use nautilus_common::testing::wait_until_async;
use serde_json::Value;
use tungstenite::{
    Message,
    handshake::server::{ErrorResponse, Request, Response},
    http::StatusCode,
    protocol::{CloseFrame, frame::coding::CloseCode},
};

use super::CME_API_KEY;

/// LSE's CME stream at Level 1 in the `dbn` encoding, captured on 13
/// November 2026 after CME's Friday close: one connection subscribing GCZ6
/// and GCG7, its `hello`, then each contract's opening, `snapshot_begin`,
/// its best bid and offer at the close as a binary frame, and
/// `snapshot_end`; nothing more came, CME being closed. One frame a line:
/// a text frame as LSE sent it, a binary frame in hex.
pub const CME_GCZ6_GCG7: &str = include_str!("../../test_data/cme_stream_gcz6_gcg7_l1_dbn.txt");

/// GCZ6's opening of `CME_GCZ6_GCG7` asked in the `json` encoding a minute
/// before: the same record, as JSON.
pub const CME_GCZ6_JSON: &str = include_str!("../../test_data/cme_stream_gcz6_l1_json.txt");

/// The first six messages of a Level 1 subscription to ESZ6 in the `json`
/// encoding, as LSE's WebSocket documentation gives them ("What you
/// receive", read 13 November 2026): `hello`, ESZ6's opening with its best
/// bid and offer, then a change of the best bid and offer and a trade.
pub const CME_ESZ6_DOCUMENTED: &str =
    include_str!("../../test_data/cme_stream_esz6_l1_json_documented.txt");

/// LSE's answer to a stream connection with no CME Group key, HTTP 401, as
/// its documentation and a capture give it.
pub const CME_KEY_UNKNOWN: &str = include_str!("../../test_data/cme_stream_key_unknown.json");

/// LSE's close of a subscription naming a contract the stream does not
/// carry, captured: its code and reason, as JSON.
pub const CME_SYMBOL_UNAVAILABLE: &str =
    include_str!("../../test_data/cme_close_symbol_unavailable.json");

/// ESZ6 of 2026 under the id Databento gives it, the contract of
/// `CME_ESZ6_DOCUMENTED`.
pub const ESZ26: &str = "ESZ26.GLBX";

/// A frame of the stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CmeFrame {
    Text(String),
    Binary(Vec<u8>),
}

/// The frames of a captured session, one a line: a JSON text frame, or a
/// binary frame in hex.
///
/// # Panics
///
/// Panics on a line that is neither.
#[must_use]
pub fn frames(captured: &str) -> Vec<CmeFrame> {
    captured
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            if line.starts_with('{') {
                CmeFrame::Text(line.to_string())
            } else {
                CmeFrame::Binary(
                    (0..line.len())
                        .step_by(2)
                        .map(|at| u8::from_str_radix(&line[at..at + 2], 16).expect("hex"))
                        .collect(),
                )
            }
        })
        .collect()
}

/// The frames of a session LSE gives in the `json` encoding as LSE sends
/// them in the `dbn` encoding, which its documentation describes: the
/// `hello` of the `dbn` encoding, as captured (`CME_GCZ6_GCG7`'s, of the
/// same epoch as the documented one); the snapshot markers as they are,
/// JSON text in both encodings; and each record as its binary frame (see
/// `dbn_frame`).
///
/// # Panics
///
/// Panics on a message that is not JSON, or a record `dbn_frame` cannot
/// write.
#[must_use]
pub fn dbn_frames(json_encoded: &str) -> Vec<CmeFrame> {
    let hello = frames(CME_GCZ6_GCG7)
        .into_iter()
        .next()
        .expect("the captured hello");
    json_encoded
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            let message: Value = serde_json::from_str(line).expect("JSON");
            match message["type"].as_str() {
                Some("hello") => hello.clone(),
                Some("snapshot_book" | "mbp1") => CmeFrame::Binary(dbn_frame(line)),
                _ => CmeFrame::Text(line.to_string()),
            }
        })
        .collect()
}

/// The binary frame LSE sends in the `dbn` encoding for a record `message`
/// of the `json` encoding (`snapshot_book` or `mbp1`), as LSE's
/// documentation describes it: a 24-byte little-endian header, `L3DB`,
/// version 1, a kind byte, 1 in a snapshot and 2 live, two unused bytes,
/// the message's sequence in 8 bytes, the contract's number in 4 and a
/// record count in 4, then the record as Databento's `Mbp1Msg`.
///
/// # Panics
///
/// Panics if `message` is not such a record.
#[must_use]
pub fn dbn_frame(message: &str) -> Vec<u8> {
    let message: Value = serde_json::from_str(message).expect("JSON");
    let kind: u8 = match message["type"].as_str() {
        Some("snapshot_book") => 1,
        Some("mbp1") => 2,
        other => panic!("{other:?} is no record message"),
    };
    let record = &message["record"];
    let level = &record["levels"][0];
    fn whole(value: &Value) -> u64 {
        value.as_u64().expect("a whole number")
    }
    fn text_number(value: &Value) -> &str {
        value.as_str().expect("a number as a string")
    }
    fn price(value: &Value) -> i64 {
        text_number(value).parse().expect("a price")
    }
    fn time(value: &Value) -> u64 {
        text_number(value).parse().expect("a time")
    }
    fn char_of(value: &Value) -> c_char {
        value.as_str().expect("a character").as_bytes()[0] as c_char
    }
    let instrument_id = u32::try_from(whole(&record["instrument_id"])).expect("u32");
    let mbp1 = Mbp1Msg {
        hd: RecordHeader::new::<Mbp1Msg>(
            rtype::MBP_1,
            u16::try_from(whole(&record["publisher_id"])).expect("u16"),
            instrument_id,
            time(&record["ts_event"]),
        ),
        price: price(&record["price"]),
        size: u32::try_from(whole(&record["size"])).expect("u32"),
        action: char_of(&record["action"]),
        side: char_of(&record["side"]),
        flags: FlagSet::new(u8::try_from(whole(&record["flags"])).expect("u8")),
        depth: u8::try_from(whole(&record["depth"])).expect("u8"),
        ts_recv: time(&record["ts_recv"]),
        ts_in_delta: i32::try_from(record["ts_in_delta"].as_i64().expect("i32")).expect("i32"),
        sequence: u32::try_from(whole(&record["sequence"])).expect("u32"),
        levels: [BidAskPair {
            bid_px: price(&level["bid_px"]),
            ask_px: price(&level["ask_px"]),
            bid_sz: u32::try_from(whole(&level["bid_sz"])).expect("u32"),
            ask_sz: u32::try_from(whole(&level["ask_sz"])).expect("u32"),
            bid_ct: u32::try_from(whole(&level["bid_ct"])).expect("u32"),
            ask_ct: u32::try_from(whole(&level["ask_ct"])).expect("u32"),
        }],
    };
    let mut frame = Vec::with_capacity(24 + size_of::<Mbp1Msg>());
    frame.extend_from_slice(b"L3DB");
    frame.extend_from_slice(&[1, kind, 0, 0]);
    frame.extend_from_slice(&whole(&message["sequence"]).to_le_bytes());
    frame.extend_from_slice(&instrument_id.to_le_bytes());
    frame.extend_from_slice(&1_u32.to_le_bytes());
    frame.extend_from_slice(RecordRef::from(&mbp1).as_ref());
    frame
}

/// What the mock stream does after a connection's subscription.
#[derive(Debug, Clone)]
pub enum CmeAction {
    /// Sends this frame.
    Send(CmeFrame),
    /// Closes the connection with this code and reason.
    Close(u16, String),
    /// Sends nothing more until the test releases the stream
    /// (`MockCmeStream::release`).
    Hold,
}

/// Whether the test released the stream, and its signal.
type Released = Arc<(Mutex<bool>, Condvar)>;

/// The frames a connection received: the connection (0, 1, ... in the
/// order they came) and the frame.
type Received = Arc<Mutex<Vec<(usize, Value)>>>;

/// The mock stream: its address, the frames it received and the key of
/// each connection that offered one.
#[derive(Debug)]
pub struct MockCmeStream {
    url: String,
    received: Received,
    keys: Arc<Mutex<Vec<Option<String>>>>,
    released: Released,
}

impl MockCmeStream {
    /// Starts a stream answering each connection's subscription with
    /// `script` of the connection's number and the subscription, each
    /// connection on its own thread. A connection without `CME_API_KEY` in
    /// its `X-API-Key` header is refused with LSE's 401 (`CME_KEY_UNKNOWN`),
    /// as LSE refuses a key that is not a CME Group key.
    ///
    /// # Panics
    ///
    /// Panics if no port of 127.0.0.1 is free.
    #[must_use]
    pub fn start(script: impl FnMut(usize, &Value) -> Vec<CmeAction> + Send + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a free port");
        let url = format!(
            "ws://{}/v1/stream",
            listener.local_addr().expect("an address")
        );
        let received: Received = Arc::default();
        let keys: Arc<Mutex<Vec<Option<String>>>> = Arc::default();
        let released: Released = Arc::default();
        let (log, key_log, release) = (
            Arc::clone(&received),
            Arc::clone(&keys),
            Arc::clone(&released),
        );
        let script = Arc::new(Mutex::new(script));
        std::thread::spawn(move || {
            let mut connection = 0;
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                let (log, key_log, script, release) = (
                    Arc::clone(&log),
                    Arc::clone(&key_log),
                    Arc::clone(&script),
                    Arc::clone(&release),
                );
                #[expect(
                    clippy::result_large_err,
                    reason = "tungstenite's handshake callback refuses with its HTTP response"
                )]
                let accepted =
                    tungstenite::accept_hdr(stream, |request: &Request, response: Response| {
                        let key = request
                            .headers()
                            .get("x-api-key")
                            .and_then(|key| key.to_str().ok())
                            .map(ToString::to_string);
                        let known = key.as_deref() == Some(CME_API_KEY);
                        key_log.lock().expect("not poisoned").push(key);
                        if known {
                            Ok(response)
                        } else {
                            let mut refusal = ErrorResponse::new(Some(CME_KEY_UNKNOWN.to_string()));
                            *refusal.status_mut() = StatusCode::UNAUTHORIZED;
                            Err(refusal)
                        }
                    });
                let Ok(mut socket) = accepted else {
                    continue;
                };
                let this = connection;
                connection += 1;
                std::thread::spawn(move || {
                    let mut answered = false;
                    while let Ok(message) = socket.read() {
                        let Message::Text(text) = message else {
                            continue;
                        };
                        let Ok(frame) = serde_json::from_str::<Value>(&text) else {
                            continue;
                        };
                        log.lock()
                            .expect("not poisoned")
                            .push((this, frame.clone()));
                        if answered {
                            continue;
                        }
                        answered = true;
                        let actions = (script.lock().expect("not poisoned"))(this, &frame);
                        for action in actions {
                            let sent = match action {
                                CmeAction::Send(CmeFrame::Text(text)) => {
                                    socket.send(Message::text(text))
                                }
                                CmeAction::Send(CmeFrame::Binary(bytes)) => {
                                    socket.send(Message::binary(bytes))
                                }
                                CmeAction::Hold => {
                                    let (released, signal) = &*release;
                                    let guard = released.lock().expect("not poisoned");
                                    drop(
                                        signal
                                            .wait_while(guard, |released| !*released)
                                            .expect("not poisoned"),
                                    );
                                    Ok(())
                                }
                                CmeAction::Close(code, reason) => {
                                    let _ = socket.close(Some(CloseFrame {
                                        code: CloseCode::from(code),
                                        reason: reason.into(),
                                    }));
                                    let _ = socket.flush();
                                    // Drains until the client answers the close.
                                    while socket.read().is_ok() {}
                                    return;
                                }
                            };
                            if sent.is_err() {
                                return;
                            }
                        }
                    }
                });
            }
        });
        Self {
            url,
            received,
            keys,
            released,
        }
    }

    /// Lets every connection held (`CmeAction::Hold`) go on.
    pub fn release(&self) {
        let (released, signal) = &*self.released;
        *released.lock().expect("not poisoned") = true;
        signal.notify_all();
    }

    /// The stream's address, the adapter's CME stream address.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }

    /// The frames received so far, in the order they came.
    #[must_use]
    pub fn received(&self) -> Vec<(usize, Value)> {
        self.received.lock().expect("not poisoned").clone()
    }

    /// The key each connection offered, in the order they came.
    #[must_use]
    pub fn keys(&self) -> Vec<Option<String>> {
        self.keys.lock().expect("not poisoned").clone()
    }

    /// Waits, up to 10 s, until `condition` holds on the frames received
    /// (`received`), and returns them.
    ///
    /// # Panics
    ///
    /// Panics if it does not hold within 10 s.
    pub async fn wait_until(
        &self,
        condition: impl Fn(&[(usize, Value)]) -> bool,
    ) -> Vec<(usize, Value)> {
        wait_until_async(
            || {
                let met = condition(&self.received());
                async move { met }
            },
            Duration::from_secs(10),
        )
        .await;
        self.received()
    }
}

/// `frames` sent, as a script's answer.
#[must_use]
pub fn sent(frames: Vec<CmeFrame>) -> Vec<CmeAction> {
    frames.into_iter().map(CmeAction::Send).collect()
}

/// `frames` sent, held after the first `opened` until the test releases
/// the stream, as a script's answer: a connection's opening, then its
/// live records once the test is ready for them.
#[must_use]
pub fn held(frames: Vec<CmeFrame>, opened: usize) -> Vec<CmeAction> {
    let mut actions = sent(frames);
    actions.insert(opened, CmeAction::Hold);
    actions
}

#[cfg(test)]
mod tests {
    use super::*;

    /// LSE's documentation describes the `dbn` encoding of a record; the
    /// same opening of GCZ6, captured in both encodings, holds it to the
    /// byte.
    #[test]
    fn a_record_of_the_json_encoding_is_the_binary_frame_lse_sends_in_the_dbn_encoding() {
        let snapshot_book = CME_GCZ6_JSON
            .lines()
            .find(|line| line.contains("\"snapshot_book\""))
            .unwrap();
        let captured = frames(CME_GCZ6_GCG7)
            .into_iter()
            .find_map(|frame| match frame {
                CmeFrame::Binary(bytes) => Some(bytes),
                CmeFrame::Text(_) => None,
            })
            .unwrap();

        assert_eq!(dbn_frame(snapshot_book), captured);
    }
}
