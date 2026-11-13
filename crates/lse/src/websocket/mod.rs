//! LSE's sockets: the frames of the protocol four of them share
//! (`messages`), the client of one socket (`client`), and the parsing of a
//! tick into Nautilus data (`parse`).

pub(crate) mod client;
pub(crate) mod messages;
pub(crate) mod parse;
