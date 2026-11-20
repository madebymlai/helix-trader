//! What the adapter shares. `messages` holds the generated protobuf types, `parse` turns symbols
//! into instruments, `account` checks the account, and `token` holds the token pair and the file
//! that renews it.

pub(crate) mod account;
pub(crate) mod messages;
pub(crate) mod parse;
pub(crate) mod token;
