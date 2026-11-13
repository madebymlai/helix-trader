//! LSE's CME Globex stream, a socket of its own protocol: its messages
//! (`messages`) and the client of one connection (`client`).

pub(crate) mod client;
pub(crate) mod messages;
