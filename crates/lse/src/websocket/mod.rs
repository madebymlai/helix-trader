//! LSE's sockets: the frames of the protocol four of them share
//! (`messages`), the client of one socket (`client`), and the parsing of a
//! tick into Nautilus data (`parse`); and LSE's exchange streams, CME
//! Globex's and Nasdaq TotalView's, sockets of a protocol of their own
//! (`exchange`).

pub(crate) mod client;
pub(crate) mod exchange;
pub(crate) mod messages;
pub(crate) mod parse;
