//! The messages of LSE's exchange streams, CME Globex's and Nasdaq
//! TotalView's, which are one protocol (LSE's WebSocket documentation, "The
//! CME Group stream": the Nasdaq stream "sends the same messages for
//! stocks"): the one subscription a connection sends, at
//! one level, the text frames LSE answers with (`hello`, then each
//! contract's `snapshot_begin` and `snapshot_end`), and the binary frame of
//! a record in the `dbn` encoding, LSE's 24-byte header then the record in
//! Databento's DBN format, of the level's schema.

use std::fmt::Display;

use anyhow::Context;
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

/// A level of an LSE exchange stream, which a connection's subscription
/// names: its records are Databento's of one schema ("to change the
/// contracts or the level, open another connection").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExchangeLevel {
    /// Level 1: every trade and the best bid and offer (`mbp1`), each
    /// contract opening with its last best bid and offer.
    Mbp1,
    /// Level 2: the ten best levels a side after each change of one
    /// (`mbp10`), each contract opening with its last ten levels.
    Mbp10,
    /// Level 3: every order event (`mbo`), with no opening book.
    Mbo,
}

impl ExchangeLevel {
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

impl Display for ExchangeLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Level {}", self.number())
    }
}

/// The one message a connection sends, within five seconds of opening: the
/// contracts by their names at LSE, at one level, in the `dbn` encoding.
#[derive(Debug, Serialize)]
pub struct ExchangeSubscription<'a> {
    op: &'static str,
    symbols: &'a [String],
    level: u8,
    encoding: &'static str,
}

impl<'a> ExchangeSubscription<'a> {
    /// The subscription of `symbols`, each named once, at `level`.
    pub fn new(level: ExchangeLevel, symbols: &'a [String]) -> Self {
        Self {
            op: "subscribe",
            symbols,
            level: level.number(),
            encoding: "dbn",
        }
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("a subscription serializes")
    }
}

/// A text frame of the stream. Its other fields, and other types, are not
/// read.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExchangeText {
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
pub enum ExchangeRecord {
    Mbp1(Mbp1Msg),
    Mbp10(Box<Mbp10Msg>),
    Mbo(MboMsg),
}

/// A binary frame of the stream: one record of a contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeRecordFrame {
    /// Whether it is the contract's opening, its last best bid and offer or
    /// ten levels, rather than a live record.
    pub snapshot: bool,
    /// The contract's sequence of the record, one more than the last.
    pub sequence: u64,
    /// The stream's number of the contract.
    pub instrument_id: u32,
    pub record: ExchangeRecord,
}

impl ExchangeRecordFrame {
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
    pub fn parse(frame: &[u8], level: ExchangeLevel) -> anyhow::Result<Self> {
        anyhow::ensure!(
            frame.len() >= HEADER_LEN,
            "{} bytes, short of LSE's {HEADER_LEN}-byte header",
            frame.len()
        );
        let (header, records) = frame.split_at(HEADER_LEN);
        anyhow::ensure!(header[..4] == *MAGIC, "not an L3DB frame");
        anyhow::ensure!(header[4] == VERSION, "L3DB version {}", header[4]);
        let snapshot = match header[5] {
            SNAPSHOT => true,
            LIVE => false,
            kind => anyhow::bail!("a frame of kind {kind}, neither a snapshot (1) nor live (2)"),
        };
        let sequence = u64::from_le_bytes(header[8..16].try_into().expect("8 bytes"));
        let instrument_id = u32::from_le_bytes(header[16..20].try_into().expect("4 bytes"));
        let count = u32::from_le_bytes(header[20..24].try_into().expect("4 bytes"));
        anyhow::ensure!(
            count == 1 && records.len() == level.record_len(),
            "{count} records in {} bytes, not one {level} record of {}",
            records.len(),
            level.record_len()
        );
        let record = match level {
            ExchangeLevel::Mbp1 => ExchangeRecord::Mbp1(decode::<Mbp1Msg>(records, level)?),
            ExchangeLevel::Mbp10 => {
                ExchangeRecord::Mbp10(Box::new(decode::<Mbp10Msg>(records, level)?))
            }
            ExchangeLevel::Mbo => ExchangeRecord::Mbo(decode::<MboMsg>(records, level)?),
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
fn decode<R: HasRType + Clone>(records: &[u8], level: ExchangeLevel) -> anyhow::Result<R> {
    Ok(RecordDecoder::new(records)
        .decode_record::<R>()
        .with_context(|| format!("a {level} record"))?
        .context("no record")?
        .clone())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::testing::{
        CME_GCZ6_GCG7, CME_GCZ6_L2_DOCUMENTED, CME_GCZ6_L3_DOCUMENTED, StreamFrame, dbn_frames,
        frames,
    };

    fn binary(frame: &StreamFrame) -> Option<&[u8]> {
        match frame {
            StreamFrame::Binary(bytes) => Some(bytes),
            StreamFrame::Text(_) => None,
        }
    }

    /// GCZ6's opening, as LSE sent the same record in the `json` encoding
    /// (`test_data/cme_stream_gcz6_l1_json.txt`): its best bid and offer at
    /// CME's Friday close, no price of its own, no side.
    #[rstest]
    fn a_frame_is_lse_s_header_then_the_contract_s_level_1_record() {
        let captured = frames(CME_GCZ6_GCG7);
        let frame =
            ExchangeRecordFrame::parse(binary(&captured[2]).unwrap(), ExchangeLevel::Mbp1).unwrap();

        let ExchangeRecord::Mbp1(record) = &frame.record else {
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

    /// The words of `reason`, its numbers among them.
    fn words(reason: &str) -> Vec<&str> {
        reason
            .split(|c: char| !c.is_ascii_alphanumeric())
            .filter(|word| !word.is_empty())
            .collect()
    }

    /// Synthetic: the captured frame cut, of another kind, or of a header
    /// other than LSE's. A Level 1 record, Databento's `Mbp1Msg`, is 80
    /// bytes after the 24 of LSE's header: cut at 60, 36 remain, and none
    /// of a header alone; the header's magic is `L3DB`, its kind at byte 5
    /// and its count of records at byte 20, each named as the frame holds
    /// it.
    #[rstest]
    #[case::cut(|frame: &mut Vec<u8>| frame.truncate(60), &["36", "80"])]
    #[case::header_only(|frame: &mut Vec<u8>| frame.truncate(24), &["0", "80"])]
    #[case::kind(|frame: &mut Vec<u8>| frame[5] = 3, &["3"])]
    #[case::magic(|frame: &mut Vec<u8>| frame[0] = b'X', &["L3DB"])]
    #[case::count(|frame: &mut Vec<u8>| frame[20] = 2, &["2", "80"])]
    fn a_frame_not_of_one_level_1_record_is_refused(
        #[case] mangle: fn(&mut Vec<u8>),
        #[case] facts: &[&str],
    ) {
        let captured = frames(CME_GCZ6_GCG7);
        let mut frame = binary(&captured[2]).unwrap().to_vec();
        mangle(&mut frame);

        let err = ExchangeRecordFrame::parse(&frame, ExchangeLevel::Mbp1)
            .unwrap_err()
            .to_string();

        for fact in facts {
            assert!(words(&err).contains(fact), "{fact}: {err}");
        }
    }

    #[rstest]
    fn the_text_frames_are_read_by_their_type() {
        let captured = frames(CME_GCZ6_GCG7);
        let texts: Vec<ExchangeText> = captured
            .iter()
            .filter_map(|frame| match frame {
                StreamFrame::Text(text) => Some(serde_json::from_str(text).unwrap()),
                StreamFrame::Binary(_) => None,
            })
            .collect();

        assert_eq!(
            texts,
            [
                ExchangeText::Hello { epoch: 2 },
                ExchangeText::SnapshotBegin {
                    symbol: "GCZ6".into(),
                    instrument_id: 1_496_506_229,
                    sequence: 1794
                },
                ExchangeText::SnapshotEnd {
                    symbol: "GCZ6".into()
                },
                ExchangeText::SnapshotBegin {
                    symbol: "GCG7".into(),
                    instrument_id: 3_648_859_809,
                    sequence: 58
                },
                ExchangeText::SnapshotEnd {
                    symbol: "GCG7".into()
                },
            ]
        );
    }

    /// GCZ6's opening at Level 2 (`test_data/README.md`): its last ten
    /// levels a side, best first, with each level's count of orders.
    #[rstest]
    fn a_level_2_frame_is_lse_s_header_then_the_contract_s_ten_levels_a_side() {
        let session = dbn_frames(CME_GCZ6_L2_DOCUMENTED);

        let frame =
            ExchangeRecordFrame::parse(binary(&session[2]).unwrap(), ExchangeLevel::Mbp10).unwrap();

        let ExchangeRecord::Mbp10(record) = &frame.record else {
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

        let frame =
            ExchangeRecordFrame::parse(binary(&session[3]).unwrap(), ExchangeLevel::Mbo).unwrap();

        let ExchangeRecord::Mbo(record) = &frame.record else {
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
    /// it, refused naming its one record and the level's length of one,
    /// Databento's `Mbp1Msg` 80 bytes, `Mbp10Msg` 368 and `MboMsg` 56. The
    /// one record is looked for beside the level's name, which holds a 1 at
    /// Level 1.
    #[rstest]
    #[case::level_1_at_level_2(CME_GCZ6_GCG7, 2, ExchangeLevel::Mbp10, "368")]
    #[case::level_2_at_level_1(CME_GCZ6_L2_DOCUMENTED, 2, ExchangeLevel::Mbp1, "80")]
    #[case::level_3_at_level_2(CME_GCZ6_L3_DOCUMENTED, 3, ExchangeLevel::Mbp10, "368")]
    fn a_frame_of_another_level_s_record_is_refused(
        #[case] session: &str,
        #[case] at: usize,
        #[case] level: ExchangeLevel,
        #[case] level_len: &str,
    ) {
        let session = if session == CME_GCZ6_GCG7 {
            frames(session)
        } else {
            dbn_frames(session)
        };

        let err = ExchangeRecordFrame::parse(binary(&session[at]).unwrap(), level)
            .unwrap_err()
            .to_string();

        let beside = err.replace(&level.to_string(), "");
        for fact in ["1", level_len] {
            assert!(words(&beside).contains(&fact), "{fact}: {err}");
        }
    }
}
