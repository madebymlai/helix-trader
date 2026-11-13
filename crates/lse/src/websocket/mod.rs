//! LSE's sockets: the frames of the protocol four of them share
//! (`messages`), the client of one socket (`client`), and the parsing of a
//! tick into Nautilus data (`parse`); and LSE's CME Globex stream, a socket
//! of its own protocol (`cme`).

pub(crate) mod client;
pub(crate) mod cme;
pub(crate) mod messages;
pub(crate) mod parse;
