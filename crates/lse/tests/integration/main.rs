//! The adapter's tests of its public boundaries, as Nautilus' adapter
//! guide places them ("Testing": "Typed HTTP and WebSocket behavior through
//! mock servers, event dispatch, lifecycle, and retries"), each against the
//! mock LSE peers and their captured payloads of `lse::testing`, which the
//! crate's dev-dependency on itself enables (`test-support`). Pure protocol
//! logic is tested in `src`.

mod data_client;
mod data_tester;
mod exchange_stream_client;
mod exchange_streams;
mod http_client;
mod http_export;
mod http_listing;
mod websocket_client;
