//! The messages of LSE's CME Globex stream (LSE's WebSocket documentation,
//! "The CME Group stream"): the one subscription a connection sends, the
//! text frames LSE answers with (`hello`, then each contract's
//! `snapshot_begin` and `snapshot_end`), and the binary frame of a record
//! in the `dbn` encoding, LSE's 24-byte header then the record in
//! Databento's DBN format.

use anyhow::{Context, bail, ensure};
use dbn::{
    Mbp1Msg,
    decode::{DecodeRecord, dbn::RecordDecoder},
};
use serde::{Deserialize, Serialize};

/// LSE's first bytes of a record frame.
const MAGIC: &[u8; 4] = b"L3DB";

/// The version of LSE's record frame.
const VERSION: u8 = 1;

/// The length of LSE's header before the records of a frame.
const HEADER_LEN: usize = 24;

/// The kind byte of a record frame: the contract's opening, or live.
const SNAPSHOT: u8 = 1;
const LIVE: u8 = 2;

/// The one message a connection sends, within five seconds of opening: the
/// contracts by their names at LSE, at Level 1, every trade and the best
/// bid and offer (`mbp1` records), in the `dbn` encoding.
#[derive(Debug, Serialize)]
pub(crate) struct CmeSubscription<'a> {
    op: &'static str,
    symbols: &'a [String],
    level: u8,
    encoding: &'static str,
}

impl<'a> CmeSubscription<'a> {
    /// The Level 1 subscription of `symbols`, each named once.
    pub(crate) fn level_1(symbols: &'a [String]) -> Self {
        Self {
            op: "subscribe",
            symbols,
            level: 1,
            encoding: "dbn",
        }
    }

    pub(crate) fn to_json(&self) -> String {
        serde_json::to_string(self).expect("a subscription serializes")
    }
}

/// A text frame of the stream. Its other fields, and other types, are not
/// read.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum CmeText {
    /// First on a connection: the feed's `epoch`, which every later message
    /// repeats; a new one after the feed's reset, its contracts' sequences
    /// starting again.
    Hello { epoch: u64 },
    /// A contract opens: its name, the stream's number for it, and the
    /// sequence of its opening record, which its live records continue.
    SnapshotBegin {
        symbol: String,
        instrument_id: u32,
        sequence: u64,
    },
    /// The contract is open.
    SnapshotEnd { symbol: String },
}

/// A binary frame of the stream: one Level 1 record of a contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CmeRecordFrame {
    /// Whether it is the contract's opening, its last best bid and offer,
    /// rather than a live record.
    pub(crate) snapshot: bool,
    /// The contract's sequence of the record, one more than the last.
    pub(crate) sequence: u64,
    /// The stream's number of the contract.
    pub(crate) instrument_id: u32,
    pub(crate) record: Mbp1Msg,
}

impl CmeRecordFrame {
    /// The record `frame` holds: LSE's header, `L3DB`, version 1, the kind,
    /// 1 in a snapshot and 2 live, two unused bytes, the sequence, the
    /// contract's number and the count of records, little-endian, then the
    /// record, a Level 1 one (`Mbp1Msg`), as each of the stream's messages
    /// holds one.
    ///
    /// # Errors
    ///
    /// Returns an error if `frame` is not such a frame of one `Mbp1Msg`.
    pub(crate) fn parse(frame: &[u8]) -> anyhow::Result<Self> {
        ensure!(
            frame.len() >= HEADER_LEN,
            "{} bytes, short of LSE's {HEADER_LEN}-byte header",
            frame.len()
        );
        let (header, records) = frame.split_at(HEADER_LEN);
        ensure!(header[..4] == *MAGIC, "not an L3DB frame");
        ensure!(header[4] == VERSION, "L3DB version {}", header[4]);
        let snapshot = match header[5] {
            SNAPSHOT => true,
            LIVE => false,
            kind => bail!("a frame of kind {kind}, neither a snapshot (1) nor live (2)"),
        };
        let sequence = u64::from_le_bytes(header[8..16].try_into().expect("8 bytes"));
        let instrument_id = u32::from_le_bytes(header[16..20].try_into().expect("4 bytes"));
        let count = u32::from_le_bytes(header[20..24].try_into().expect("4 bytes"));
        ensure!(
            count == 1 && records.len() == size_of::<Mbp1Msg>(),
            "{count} records in {} bytes, not one Level 1 record of {}",
            records.len(),
            size_of::<Mbp1Msg>()
        );
        let record = RecordDecoder::new(records)
            .decode_record::<Mbp1Msg>()
            .context("a Level 1 record")?
            .context("no record")?
            .clone();
        Ok(Self {
            snapshot,
            sequence,
            instrument_id,
            record,
        })
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use super::*;
    use crate::testing::{CME_GCZ6_GCG7, CmeFrame, frames};

    fn binary(frame: &CmeFrame) -> Option<&[u8]> {
        match frame {
            CmeFrame::Binary(bytes) => Some(bytes),
            CmeFrame::Text(_) => None,
        }
    }

    /// GCZ6's opening, as LSE sent the same record in the `json` encoding
    /// (`test_data/cme_stream_gcz6_l1_json.txt`): its best bid and offer at
    /// CME's Friday close, no price of its own, no side.
    #[rstest]
    fn a_frame_is_lse_s_header_then_the_contract_s_level_1_record() {
        let captured = frames(CME_GCZ6_GCG7);
        let frame = CmeRecordFrame::parse(binary(&captured[2]).unwrap()).unwrap();

        let record = &frame.record;
        let level = &record.levels[0];
        assert_eq!(
            (frame.snapshot, frame.sequence, frame.instrument_id),
            (true, 1794, 1_496_506_229)
        );
        assert_eq!(
            (
                record.hd.instrument_id,
                record.hd.ts_event,
                record.ts_recv,
                record.price,
                record.size,
                record.action as u8,
                record.side as u8,
            ),
            (
                1_496_506_229,
                1_794_607_200_251_734_000,
                1_794_607_200_687_603_000,
                i64::MAX,
                0,
                b'M',
                b'N',
            )
        );
        assert_eq!(
            (level.bid_px, level.bid_sz, level.ask_px, level.ask_sz),
            (3_870_000_000_000, 2, 3_897_500_000_000, 10)
        );
    }

    /// GCG7's opening, beside GCZ6's on one connection.
    #[rstest]
    fn each_frame_names_its_contract_and_its_sequence() {
        let captured = frames(CME_GCZ6_GCG7);
        let frame = CmeRecordFrame::parse(binary(&captured[5]).unwrap()).unwrap();

        assert_eq!(
            (frame.snapshot, frame.sequence, frame.instrument_id),
            (true, 58, 3_648_859_809)
        );
    }

    /// Synthetic: the captured frame cut, of another kind, or of a header
    /// other than LSE's.
    #[rstest]
    #[case::cut(|frame: &mut Vec<u8>| frame.truncate(60))]
    #[case::header_only(|frame: &mut Vec<u8>| frame.truncate(24))]
    #[case::kind(|frame: &mut Vec<u8>| frame[5] = 3)]
    #[case::magic(|frame: &mut Vec<u8>| frame[0] = b'X')]
    #[case::count(|frame: &mut Vec<u8>| frame[20] = 2)]
    fn a_frame_not_of_one_level_1_record_is_refused(#[case] mangle: fn(&mut Vec<u8>)) {
        let captured = frames(CME_GCZ6_GCG7);
        let mut frame = binary(&captured[2]).unwrap().to_vec();
        mangle(&mut frame);

        assert!(CmeRecordFrame::parse(&frame).is_err());
    }

    #[rstest]
    fn the_text_frames_are_read_by_their_type() {
        let captured = frames(CME_GCZ6_GCG7);
        let texts: Vec<CmeText> = captured
            .iter()
            .filter_map(|frame| match frame {
                CmeFrame::Text(text) => Some(serde_json::from_str(text).unwrap()),
                CmeFrame::Binary(_) => None,
            })
            .collect();

        assert_eq!(
            texts,
            [
                CmeText::Hello { epoch: 2 },
                CmeText::SnapshotBegin {
                    symbol: "GCZ6".into(),
                    instrument_id: 1_496_506_229,
                    sequence: 1794
                },
                CmeText::SnapshotEnd {
                    symbol: "GCZ6".into()
                },
                CmeText::SnapshotBegin {
                    symbol: "GCG7".into(),
                    instrument_id: 3_648_859_809,
                    sequence: 58
                },
                CmeText::SnapshotEnd {
                    symbol: "GCG7".into()
                },
            ]
        );
    }

    /// LSE's documented subscription, asking the `dbn` encoding.
    #[rstest]
    fn the_subscription_names_the_contracts_at_level_1_in_dbn() {
        let symbols = ["GCZ6".to_string(), "GCG7".to_string()];

        let sent: serde_json::Value =
            serde_json::from_str(&CmeSubscription::level_1(&symbols).to_json()).unwrap();

        assert_eq!(
            sent,
            json!({"op": "subscribe", "symbols": ["GCZ6", "GCG7"], "level": 1, "encoding": "dbn"})
        );
    }
}
