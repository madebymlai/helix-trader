//! Defines the event filter of the `model_cross` strategy: a cross of a fast over a slow
//! `SimpleMovingAverage` of the closes of one bar type, built from `[indicators.cross]`.
//!
//! A simple average reads its last `period` closes alone, so once the slow average is warm the
//! cross after a bar is the same whatever bar the replay started at, up to the rounding of the
//! averages' running sums. An exponential average remembers its start, so a dataset day and a
//! backtest started on different bars would disagree on a cross whose averages are closer than
//! what is left of that start.

use nautilus_core::UnixNanos;
use nautilus_indicators::{
    average::sma::SimpleMovingAverage,
    indicator::{Indicator, MovingAverage},
};
use nautilus_model::data::Bar;
use serde::{Deserialize, Serialize};

use crate::{
    model::features::EventFilter,
    strategy::{IndicatorArgs, SmaArgs},
};

/// A cross of the fast average over the slow, either way, as an edge event.
///
/// The fast average is above the slow after a bar when its value is greater. A cross is a bar
/// after which that differs from the bar before. The filter keeps the `ts_init` of its last cross,
/// and a decision is an event when that lies after the previous grid point and not after the
/// decision, so a cross between two grid points makes the next decision. It is initialized once
/// both averages are.
#[derive(Debug)]
pub(super) struct Cross {
    fast: SimpleMovingAverage,
    slow: SimpleMovingAverage,
    /// Whether the fast average was above the slow after the last bar, `None` while cold.
    above: Option<bool>,
    crossed_at: Option<UnixNanos>,
}

impl Indicator for Cross {
    fn name(&self) -> String {
        "Cross".to_string()
    }

    fn has_inputs(&self) -> bool {
        self.fast.has_inputs()
    }

    fn initialized(&self) -> bool {
        self.fast.initialized() && self.slow.initialized()
    }

    fn handle_bar(&mut self, bar: &Bar) {
        self.fast.handle_bar(bar);
        self.slow.handle_bar(bar);

        if !self.initialized() {
            return;
        }
        let above = self.fast.value() > self.slow.value();
        if self.above.is_some_and(|was| was != above) {
            self.crossed_at = Some(bar.ts_init);
        }
        self.above = Some(above);
    }

    fn reset(&mut self) {
        self.fast.reset();
        self.slow.reset();
        self.above = None;
        self.crossed_at = None;
    }
}

impl EventFilter for Cross {
    fn is_event(&self, previous: UnixNanos, now: UnixNanos) -> bool {
        self.crossed_at
            .is_some_and(|crossed_at| previous < crossed_at && crossed_at <= now)
    }
}

/// The arguments of `Cross`: those of its fast and slow averages, each a Nautilus
/// `SimpleMovingAverage` (`SmaArgs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CrossArgs {
    pub fast: SmaArgs,
    pub slow: SmaArgs,
}

impl IndicatorArgs for CrossArgs {
    type Indicator = Cross;

    /// # Errors
    ///
    /// Returns an error if the fast period is not below the slow, or if Nautilus refuses an
    /// average.
    fn build(self) -> anyhow::Result<Cross> {
        anyhow::ensure!(
            self.fast.period < self.slow.period,
            "the fast period {} must be below the slow period {}",
            self.fast.period,
            self.slow.period
        );
        Ok(Cross {
            fast: self.fast.build()?,
            slow: self.slow.build()?,
            above: None,
            crossed_at: None,
        })
    }
}
