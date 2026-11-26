//! Keeps the times of an edge event filter's events (`EventFilter`): the `tEvents` of López de
//! Prado's event-based sampling (*Advances in Financial Machine Learning*).

use nautilus_core::UnixNanos;

/// The `ts_init` of each event an edge filter marked that a window still to be asked may hold.
///
/// The filter records each event as it reads it (`record`), and asks the window of each decision
/// (`in_window`). Live, data stamped after a decision's scheduled time can be read before its late
/// callback runs, so events of the next window can be recorded before this one is asked. Each is
/// kept, never replaced by a later one, until a window after it is asked.
///
/// Windows are asked in grid order, so `previous` never moves back. An event at or before the
/// `previous` asked lies in no window still to come, and asking drops it: the times held are those
/// after the last `previous` asked.
#[derive(Debug, Default)]
pub struct EventTimes {
    times: Vec<UnixNanos>,
}

impl EventTimes {
    /// Records an event at `ts`, the `ts_init` of the data that marked it.
    pub fn record(&mut self, ts: UnixNanos) {
        self.times.push(ts);
    }

    /// Returns whether an event lies in the window `(previous, now]`, after dropping the events at
    /// or before `previous`. `previous` is never before that of the window asked last.
    pub fn in_window(&mut self, previous: UnixNanos, now: UnixNanos) -> bool {
        self.times.retain(|&t| t > previous);
        self.times.iter().any(|&t| t <= now)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    /// Events at 10, 20 and 30, asked over the window from 20: those at 10 and at 20 lie in no
    /// window still to come.
    #[rstest]
    fn asking_a_window_drops_the_events_at_or_before_its_start() {
        let mut events = EventTimes::default();
        for ts in [10, 20, 30] {
            events.record(UnixNanos::from(ts));
        }

        let _ = events.in_window(UnixNanos::from(20), UnixNanos::from(40));

        assert_eq!(events.times, [UnixNanos::from(30)]);
    }
}
