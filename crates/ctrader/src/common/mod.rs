//! What the rest of the adapter shares: Spotware's protobuf messages
//! (`messages`, generated from the vendored `proto/`), the parsing of
//! cTrader symbols into instruments (`parse`), the account check
//! (`account`) and the token pair with the token file that keeps it
//! renewed (`token`).

pub(crate) mod account;
pub(crate) mod messages;
pub(crate) mod parse;
pub(crate) mod token;
