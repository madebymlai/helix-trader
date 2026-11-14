//! LSE's exchange streams, CME Globex's and Nasdaq TotalView's, sockets of
//! one protocol of their own: its messages (`messages`) and the client of
//! one connection (`client`).

pub(crate) mod client;
pub(crate) mod messages;
