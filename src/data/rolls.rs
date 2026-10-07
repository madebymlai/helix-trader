//! Futures roll dates for continuous futures.
//!
//! Nautilus stitches contracts but does not find the rolls. This module
//! detects them from the catalog's contract bars and renders them as the
//! `continuous_future_transitions` params that `DataActor::subscribe_bars`
//! takes on an `INTERNAL` bar type. Pure computation, no I/O.
