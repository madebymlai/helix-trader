//! What the adapter shares: the generated protobuf `messages`, `parse` of symbols into
//! instruments, the `account` check and the `token` pair with its renewing file.

pub(crate) mod account;
pub(crate) mod messages;
pub(crate) mod parse;
pub(crate) mod token;
