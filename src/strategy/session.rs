use anyhow::ensure;
use jiff::{SignedDuration, Timestamp, civil::Time, tz::TimeZone};
use serde::Deserialize;

/// The hours the strategy trades, every day from `open` to `close` local
/// time, so daylight saving moves them in UTC.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Session {
    /// An IANA time zone, such as `Europe/Berlin`.
    #[serde(with = "jiff::fmt::serde::tz::required")]
    pub time_zone: TimeZone,
    pub open: Time,
    pub close: Time,
}

impl Session {
    /// # Errors
    ///
    /// Returns an error if the session does not open before it closes.
    pub fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.open < self.close,
            "session open {} must be before its close {}",
            self.open,
            self.close
        );
        Ok(())
    }

    /// Whether the span `[end - length, end]` lies inside the session of
    /// the local day of `end`.
    pub(super) fn contains(&self, end: Timestamp, length: SignedDuration) -> bool {
        let local = |ts: Timestamp| self.time_zone.to_datetime(ts);
        let (start, end) = (local(end - length), local(end));
        start.date() == end.date() && self.open <= start.time() && end.time() <= self.close
    }
}

#[cfg(test)]
mod tests {
    use jiff::civil::{DateTime, time};

    use super::*;

    fn xetra() -> Session {
        Session {
            time_zone: TimeZone::get("Europe/Berlin").unwrap(),
            open: time(9, 0, 0, 0),
            close: time(17, 30, 0, 0),
        }
    }

    const HOUR: SignedDuration = SignedDuration::from_hours(1);

    /// Whether an hourly bar ending at `local` Frankfurt time counts.
    fn counts(local: &str) -> bool {
        let session = xetra();
        let local: DateTime = local.parse().unwrap();
        let end = local.to_zoned(session.time_zone.clone()).unwrap();
        session.contains(end.timestamp(), HOUR)
    }

    /// Whether an hourly bar ending at the `utc` timestamp counts.
    fn counts_utc(utc: &str) -> bool {
        xetra().contains(utc.parse().unwrap(), HOUR)
    }

    #[test]
    fn a_bar_inside_counts() {
        assert!(counts("2026-01-07T12:00"));
    }

    #[test]
    fn bars_outside_do_not_count() {
        assert!(!counts("2026-01-07T07:00"));
        assert!(!counts("2026-01-07T20:00"));
    }

    #[test]
    fn a_bar_starting_exactly_at_the_open_counts() {
        assert!(counts("2026-01-07T10:00"));
    }

    #[test]
    fn a_bar_straddling_the_open_does_not_count() {
        assert!(!counts("2026-01-07T09:30"));
        assert!(!counts("2026-01-07T09:59:59"));
    }

    #[test]
    fn a_bar_ending_exactly_at_the_close_counts() {
        assert!(counts("2026-01-07T17:30"));
    }

    #[test]
    fn a_bar_straddling_the_close_does_not_count() {
        assert!(!counts("2026-01-07T17:30:01"));
        assert!(!counts("2026-01-07T18:00"));
    }

    #[test]
    fn a_bar_spanning_midnight_does_not_count() {
        // 23:30 to 00:30: each end alone is past the open or before the close.
        assert!(!counts("2026-01-08T00:30"));
    }

    #[test]
    fn the_session_follows_daylight_saving_in_march() {
        // Friday in CET (UTC+1), Monday in CEST (UTC+2).
        assert!(!counts_utc("2026-03-27T08:00Z"));
        assert!(counts_utc("2026-03-27T16:00Z"));
        assert!(counts_utc("2026-03-30T08:00Z"));
        assert!(!counts_utc("2026-03-30T16:00Z"));
    }

    #[test]
    fn the_session_follows_daylight_saving_in_october() {
        // Friday in CEST (UTC+2), Monday in CET (UTC+1).
        assert!(counts_utc("2026-10-23T08:00Z"));
        assert!(!counts_utc("2026-10-23T16:00Z"));
        assert!(!counts_utc("2026-10-26T08:00Z"));
        assert!(counts_utc("2026-10-26T16:00Z"));
    }

    #[test]
    fn validates_open_before_close() {
        xetra().validate().unwrap();
        let mut session = xetra();
        session.close = session.open;
        assert!(session.validate().is_err());
        session.close = time(8, 0, 0, 0);
        assert!(session.validate().is_err());
    }
}
