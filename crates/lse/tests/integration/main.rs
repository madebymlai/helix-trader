//! Tests of the adapter's public boundaries, against the mock LSE peers of `lse::testing`, which
//! the crate's dev-dependency on itself enables (`test-support`). Pure protocol logic is tested in
//! `src`.

mod data_client;
mod data_tester;
mod exchange_stream_client;
mod exchange_streams;
mod http_client;
mod http_export;
mod http_listing;
mod websocket_client;
