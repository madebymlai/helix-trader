//! What the HTTP client and the data client share: the venue, the names
//! and addresses of LSE (`consts`), the key (`credential`) and the symbol
//! boundary (`symbol`).

pub(crate) mod consts;
pub(crate) mod credential;
pub(crate) mod symbol;
