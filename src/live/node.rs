//! Live node: builds and runs it.
//!
//! Uses `LiveNode::builder` (`nautilus_live`) with the data and execution
//! clients of the chosen adapter, adds the strategy (`add_strategy`) or the
//! ingest actor (`add_actor`), and calls `run`, which stops on SIGINT or
//! SIGTERM.
