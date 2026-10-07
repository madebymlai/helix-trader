//! Interactive Brokers history: instruments and bars for `fill_catalog`.
//!
//! Uses `HistoricalInteractiveBrokersClient`
//! (`nautilus_interactive_brokers::historical`), which needs no node: it
//! connects to IB Gateway or TWS, returns `Vec<InstrumentAny>` from
//! `request_instruments` and `Vec<Bar>` from `request_bars`. Another source
//! is another file next to this one.
