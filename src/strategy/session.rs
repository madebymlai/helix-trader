use jiff::{SignedDuration, Timestamp, civil::Time, tz::TimeZone};
use serde::Deserialize;

/// The hours the strategy trades, every day from `open` to `close` local time, so daylight saving
/// moves them in UTC.
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
        anyhow::ensure!(
            self.open < self.close,
            "session open {} must be before its close {}",
            self.open,
            self.close
        );
        Ok(())
    }

    /// Whether the span `[end - length, end]` lies inside the session of the local day of `end`.
    pub(super) fn contains(&self, end: Timestamp, length: SignedDuration) -> bool {
        let local = |ts: Timestamp| self.time_zone.to_datetime(ts);
        let (start, end) = (local(end - length), local(end));
        start.date() == end.date() && self.open <= start.time() && end.time() <= self.close
    }
}

#[cfg(test)]
mod tests {
    use jiff::civil::{DateTime, time};
    use rstest::rstest;

    use super::*;

    fn xetra() -> Session {
        Session {
            time_zone: TimeZone::get("Europe/Berlin").unwrap(),
            open: time(9, 0, 0, 0),
            close: time(17, 30, 0, 0),
        }
    }

    const HOUR: SignedDuration = SignedDuration::from_hours(1);

    fn counts(local: &str) -> bool {
        let session = xetra();
        let local: DateTime = local.parse().unwrap();
        let end = local.to_zoned(session.time_zone.clone()).unwrap();
        session.contains(end.timestamp(), HOUR)
    }

    fn counts_utc(utc: &str) -> bool {
        xetra().contains(utc.parse().unwrap(), HOUR)
    }

    /// An hourly bar counts when its whole hour lies inside 09:00 to 17:30 on one day.
    #[rstest]
    #[case::inside("2026-01-07T12:00", true)]
    #[case::before_the_open("2026-01-07T07:00", false)]
    #[case::after_the_close("2026-01-07T20:00", false)]
    #[case::starting_exactly_at_the_open("2026-01-07T10:00", true)]
    #[case::straddling_the_open("2026-01-07T09:30", false)]
    #[case::straddling_the_open_by_a_second("2026-01-07T09:59:59", false)]
    #[case::ending_exactly_at_the_close("2026-01-07T17:30", true)]
    #[case::straddling_the_close_by_a_second("2026-01-07T17:30:01", false)]
    #[case::straddling_the_close("2026-01-07T18:00", false)]
    // 23:30 to 00:30, each end alone past the open or before the close
    #[case::spanning_midnight("2026-01-08T00:30", false)]
    fn a_bar_counts_when_it_lies_inside_the_session(#[case] local: &str, #[case] expected: bool) {
        assert_eq!(counts(local), expected);
    }

    /// The session stays 09:00 to 17:30 in Frankfurt as daylight saving moves it in UTC.
    #[rstest]
    // Friday in CET (UTC+1), Monday in CEST (UTC+2)
    #[case::march_friday_morning("2026-03-27T08:00Z", false)]
    #[case::march_friday_afternoon("2026-03-27T16:00Z", true)]
    #[case::march_monday_morning("2026-03-30T08:00Z", true)]
    #[case::march_monday_afternoon("2026-03-30T16:00Z", false)]
    // Friday in CEST (UTC+2), Monday in CET (UTC+1)
    #[case::october_friday_morning("2026-10-23T08:00Z", true)]
    #[case::october_friday_afternoon("2026-10-23T16:00Z", false)]
    #[case::october_monday_morning("2026-10-26T08:00Z", false)]
    #[case::october_monday_afternoon("2026-10-26T16:00Z", true)]
    fn the_session_follows_daylight_saving(#[case] utc: &str, #[case] expected: bool) {
        assert_eq!(counts_utc(utc), expected);
    }

    /// A session closing at or before its open is refused, naming both.
    #[rstest]
    #[case::closing_after_the_open(time(17, 30, 0, 0), None)]
    #[case::closing_at_the_open(time(9, 0, 0, 0), Some("09:00"))]
    #[case::closing_before_the_open(time(8, 0, 0, 0), Some("08:00"))]
    fn a_session_not_opening_before_its_close_is_refused_naming_both(
        #[case] close: Time,
        #[case] refused_naming: Option<&str>,
    ) {
        let result = Session { close, ..xetra() }.validate();

        match refused_naming {
            None => result.unwrap(),
            Some(close) => {
                let message = result.unwrap_err().to_string();
                assert!(
                    message.contains("09:00") && message.contains(close),
                    "{message}"
                );
            }
        }
    }
}
