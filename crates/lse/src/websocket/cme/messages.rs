//! The messages of LSE's CME Globex stream (LSE's WebSocket documentation,
//! "The CME Group stream"): the one subscription a connection sends, at
//! one level, the text frames LSE answers with (`hello`, then each
//! contract's `snapshot_begin` and `snapshot_end`), and the binary frame of
//! a record in the `dbn` encoding, LSE's 24-byte header then the record in
//! Databento's DBN format, of the level's schema.

use std::fmt;

use anyhow::{Context, bail, ensure};
use dbn::{
    HasRType, MboMsg, Mbp1Msg, Mbp10Msg,
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

/// A level of LSE's CME stream, which a connection's subscription names:
/// its records are Databento's of one schema ("to change the contracts or
/// the level, open another connection").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum CmeLevel {
    /// Level 1: every trade and the best bid and offer (`mbp1`), each
    /// contract opening with its last best bid and offer.
    Mbp1,
    /// Level 2: the ten best levels a side after each change of one
    /// (`mbp10`), each contract opening with its last ten levels.
    Mbp10,
    /// Level 3: every order event (`mbo`), with no opening book.
    Mbo,
}

impl CmeLevel {
    /// LSE's number of the level.
    const fn number(self) -> u8 {
        match self {
            Self::Mbp1 => 1,
            Self::Mbp10 => 2,
            Self::Mbo => 3,
        }
    }

    /// The length of a record of the level.
    const fn record_len(self) -> usize {
        match self {
            Self::Mbp1 => size_of::<Mbp1Msg>(),
            Self::Mbp10 => size_of::<Mbp10Msg>(),
            Self::Mbo => size_of::<MboMsg>(),
        }
    }
}

impl fmt::Display for CmeLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Level {}", self.number())
    }
}

/// The one message a connection sends, within five seconds of opening: the
/// contracts by their names at LSE, at one level, in the `dbn` encoding.
#[derive(Debug, Serialize)]
pub(crate) struct CmeSubscription<'a> {
    op: &'static str,
    symbols: &'a [String],
    level: u8,
    encoding: &'static str,
}

impl<'a> CmeSubscription<'a> {
    /// The subscription of `symbols`, each named once, at `level`.
    pub(crate) fn new(level: CmeLevel, symbols: &'a [String]) -> Self {
        Self {
            op: "subscribe",
            symbols,
            level: level.number(),
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
    /// sequence its live records continue: its opening record's, if the
    /// level opens with one.
    SnapshotBegin {
        symbol: String,
        instrument_id: u32,
        sequence: u64,
    },
    /// The contract is open.
    SnapshotEnd { symbol: String },
}

/// A record of the stream, Databento's of its connection's level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CmeRecord {
    Mbp1(Mbp1Msg),
    Mbp10(Box<Mbp10Msg>),
    Mbo(MboMsg),
}

/// A binary frame of the stream: one record of a contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CmeRecordFrame {
    /// Whether it is the contract's opening, its last best bid and offer or
    /// ten levels, rather than a live record.
    pub(crate) snapshot: bool,
    /// The contract's sequence of the record, one more than the last.
    pub(crate) sequence: u64,
    /// The stream's number of the contract.
    pub(crate) instrument_id: u32,
    pub(crate) record: CmeRecord,
}

impl CmeRecordFrame {
    /// The record `frame` holds, of a connection at `level`: LSE's header,
    /// `L3DB`, version 1, the kind, 1 in a snapshot and 2 live, two unused
    /// bytes, the sequence, the contract's number and the count of records,
    /// little-endian, then the record, of the level's schema (`Mbp1Msg`,
    /// `Mbp10Msg` or `MboMsg`), as each of the stream's messages holds one.
    ///
    /// # Errors
    ///
    /// Returns an error if `frame` is not such a frame of one record of
    /// `level`.
    pub(crate) fn parse(frame: &[u8], level: CmeLevel) -> anyhow::Result<Self> {
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
            count == 1 && records.len() == level.record_len(),
            "{count} records in {} bytes, not one {level} record of {}",
            records.len(),
            level.record_len()
        );
        let record = match level {
            CmeLevel::Mbp1 => CmeRecord::Mbp1(decode::<Mbp1Msg>(records, level)?),
            CmeLevel::Mbp10 => CmeRecord::Mbp10(Box::new(decode::<Mbp10Msg>(records, level)?)),
            CmeLevel::Mbo => CmeRecord::Mbo(decode::<MboMsg>(records, level)?),
        };
        Ok(Self {
            snapshot,
            sequence,
            instrument_id,
            record,
        })
    }
}

/// The one record of type `R` in `records`, a record of `level`.
fn decode<R: HasRType + Clone>(records: &[u8], level: CmeLevel) -> anyhow::Result<R> {
    Ok(RecordDecoder::new(records)
        .decode_record::<R>()
        .with_context(|| format!("a {level} record"))?
        .context("no record")?
        .clone())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use serde_json::json;

    use super::*;
    use crate::testing::{
        CME_GCZ6_GCG7, CME_GCZ6_L2_DOCUMENTED, CME_GCZ6_L3_DOCUMENTED, CmeFrame, dbn_frames, frames,
    };

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
        let frame = CmeRecordFrame::parse(binary(&captured[2]).unwrap(), CmeLevel::Mbp1).unwrap();

        let CmeRecord::Mbp1(record) = &frame.record else {
            panic!("not a Level 1 record: {:?}", frame.record)
        };
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
        let frame = CmeRecordFrame::parse(binary(&captured[5]).unwrap(), CmeLevel::Mbp1).unwrap();

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

        assert!(CmeRecordFrame::parse(&frame, CmeLevel::Mbp1).is_err());
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

    /// LSE's documented subscription, asking the `dbn` encoding, at each
    /// level a connection may carry.
    #[rstest]
    #[case::level_1(CmeLevel::Mbp1, 1)]
    #[case::level_2(CmeLevel::Mbp10, 2)]
    #[case::level_3(CmeLevel::Mbo, 3)]
    fn the_subscription_names_the_contracts_at_its_level_in_dbn(
        #[case] level: CmeLevel,
        #[case] number: u8,
    ) {
        let symbols = ["GCZ6".to_string(), "GCG7".to_string()];

        let sent: serde_json::Value =
            serde_json::from_str(&CmeSubscription::new(level, &symbols).to_json()).unwrap();

        assert_eq!(
            sent,
            json!({"op": "subscribe", "symbols": ["GCZ6", "GCG7"], "level": number, "encoding": "dbn"})
        );
    }

    /// GCZ6's opening at Level 2 (`test_data/README.md`): its last ten
    /// levels a side, best first, with each level's count of orders.
    #[rstest]
    fn a_level_2_frame_is_lse_s_header_then_the_contract_s_ten_levels_a_side() {
        let session = dbn_frames(CME_GCZ6_L2_DOCUMENTED);

        let frame = CmeRecordFrame::parse(binary(&session[2]).unwrap(), CmeLevel::Mbp10).unwrap();

        let CmeRecord::Mbp10(record) = &frame.record else {
            panic!("not a Level 2 record: {:?}", frame.record)
        };
        assert_eq!(
            (frame.snapshot, frame.sequence, frame.instrument_id),
            (true, 1001, 1_496_506_229)
        );
        let (best, tenth) = (&record.levels[0], &record.levels[9]);
        assert_eq!(
            (
                best.bid_px,
                best.bid_sz,
                best.bid_ct,
                best.ask_px,
                best.ask_sz,
                best.ask_ct
            ),
            (3_908_100_000_000, 2, 1, 3_908_300_000_000, 9, 4)
        );
        assert_eq!(
            (
                tenth.bid_px,
                tenth.bid_sz,
                tenth.bid_ct,
                tenth.ask_px,
                tenth.ask_sz,
                tenth.ask_ct
            ),
            (3_907_200_000_000, 8, 6, 3_909_200_000_000, 7, 4)
        );
    }

    /// GCZ6's first order event at Level 3: an order of 1 added to the bid
    /// at 3908.10, not the last of its exchange message.
    #[rstest]
    fn a_level_3_frame_is_lse_s_header_then_one_order_event() {
        let session = dbn_frames(CME_GCZ6_L3_DOCUMENTED);

        let frame = CmeRecordFrame::parse(binary(&session[3]).unwrap(), CmeLevel::Mbo).unwrap();

        let CmeRecord::Mbo(record) = &frame.record else {
            panic!("not a Level 3 record: {:?}", frame.record)
        };
        assert_eq!(
            (frame.snapshot, frame.sequence, frame.instrument_id),
            (false, 7312, 1_496_506_229)
        );
        assert_eq!(
            (
                record.order_id,
                record.price,
                record.size,
                record.action as u8,
                record.side as u8,
                record.flags.raw()
            ),
            (6_411_893_001, 3_908_100_000_000, 1, b'A', b'B', 0)
        );
    }

    /// A connection carries one level: a record of another is no record of
    /// it.
    #[rstest]
    #[case::level_1_at_level_2(CME_GCZ6_GCG7, 2, CmeLevel::Mbp10)]
    #[case::level_2_at_level_1(CME_GCZ6_L2_DOCUMENTED, 2, CmeLevel::Mbp1)]
    #[case::level_3_at_level_2(CME_GCZ6_L3_DOCUMENTED, 3, CmeLevel::Mbp10)]
    fn a_frame_of_another_level_s_record_is_refused(
        #[case] session: &str,
        #[case] at: usize,
        #[case] level: CmeLevel,
    ) {
        let session = if session == CME_GCZ6_GCG7 {
            frames(session)
        } else {
            dbn_frames(session)
        };

        assert!(CmeRecordFrame::parse(binary(&session[at]).unwrap(), level).is_err());
    }

    /// At Level 3 a contract opens with no record: `snapshot_begin`, of a
    /// count of 0, then `snapshot_end`.
    #[rstest]
    fn a_level_3_opening_is_its_markers_alone() {
        let texts: Vec<CmeText> = dbn_frames(CME_GCZ6_L3_DOCUMENTED)
            .iter()
            .take(3)
            .map(|frame| match frame {
                CmeFrame::Text(text) => serde_json::from_str(text).unwrap(),
                CmeFrame::Binary(_) => panic!("a record in the opening"),
            })
            .collect();

        assert_eq!(
            texts,
            [
                CmeText::Hello { epoch: 2 },
                CmeText::SnapshotBegin {
                    symbol: "GCZ6".into(),
                    instrument_id: 1_496_506_229,
                    sequence: 7311
                },
                CmeText::SnapshotEnd {
                    symbol: "GCZ6".into()
                },
            ]
        );
    }
}
