//! Defines the event filter of the `model_cross` strategy: a cross of a fast over a slow
//! `SimpleMovingAverage` of the closes of one bar type, built from `[indicators.cross]`.
//!
//! A simple average reads its last `period` closes alone, so once the slow average is warm the
//! cross after a bar is the same whatever bar the replay started at. An exponential average
//! remembers its start, so a dataset day and a backtest started on different bars would disagree
//! on a cross whose averages are closer than what is left of that start.
//!
//! Nautilus' `SimpleMovingAverage` keeps a running sum, reseeded from its closes every 16 periods
//! of updates, so its value carries a rounding that depends on where the replay started: averages
//! of equal closes come out a few units of their last place apart. The fast average is therefore
//! above the slow only by more than half the smallest gap two averages that differ can have:
//! - Closes are multiples of the increment `10^-precision` of the bar's close, so averages of
//!   `fast` and `slow` closes that differ do so by at least that increment over the least common
//!   multiple of the periods, a thirtieth of it for periods of 10 and 30.
//! - Between reseeds a sum takes at most `32 * period` additions and subtractions, each rounding
//!   it by half a unit of its last place, so an average strays by at most some `16 * period` units
//!   of the last place of a close, about `1e-13` of the close for a period of 30.
//!
//! The half gap lies above that rounding, `lcm` being the least common multiple of the periods,
//! while a close is below about `1 / (32 * (fast + slow) * lcm * f64::EPSILON)` increments: some
//! `1e11` for periods of 10 and 30, against gold's mid of some `1e6`.

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
/// The fast average is above the slow after a bar when its value is greater by more than half the
/// smallest gap the averages can have, so equal averages are not. A cross is a bar after which
/// that differs from the bar before. The filter keeps the `ts_init` of its last cross, and a
/// decision is an event when that lies after the previous grid point and not after the decision,
/// so a cross between two grid points makes the next decision. It is initialized once both
/// averages are.
#[derive(Debug)]
pub(super) struct Cross {
    fast: SimpleMovingAverage,
    slow: SimpleMovingAverage,
    /// The least common multiple of the averages' periods: the smallest gap between averages of
    /// closes that differ is the price increment over it.
    periods_lcm: f64,
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
        let above = self.is_above(bar.close.precision);
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

impl Cross {
    /// Whether the fast average lies above the slow by more than half their smallest gap, on
    /// closes of `precision` decimals.
    fn is_above(&self, precision: u8) -> bool {
        let increment = 10f64.powi(-i32::from(precision));
        self.fast.value() - self.slow.value() > increment / self.periods_lcm / 2.0
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
            periods_lcm: lcm(self.fast.period, self.slow.period) as f64,
            above: None,
            crossed_at: None,
        })
    }
}

/// The least common multiple of `a` and `b`, by Euclid's greatest common divisor.
fn lcm(a: usize, b: usize) -> usize {
    let (mut x, mut y) = (a, b);
    while y != 0 {
        (x, y) = (y, x % y);
    }
    a / x * b
}
