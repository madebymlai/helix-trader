//! The roll calendar: per product, its liquid contract months and the day
//! and time the front contract rolls, read from `calendar.toml`, embedded
//! at build time.

use std::sync::LazyLock;

use anyhow::ensure;
use jiff::{Timestamp, civil, tz::TimeZone};
use nautilus_model::identifiers::{InstrumentId, Symbol};
use serde::Deserialize;

const CALENDAR: &str = include_str!("calendar.toml");

/// The embedded calendar, parsed once. A test parses the same file, so a
/// broken calendar fails there first.
static PRODUCTS: LazyLock<Vec<Product>> =
    LazyLock::new(|| parse(CALENDAR).expect("the embedded roll calendar parses"));

/// The product whose root is `root`, if any.
pub(super) fn product(root: InstrumentId) -> Option<&'static Product> {
    PRODUCTS.iter().find(|product| product.root == root)
}

/// The products of a calendar in TOML.
///
/// # Errors
///
/// Returns an error if the TOML is not a calendar, if a product lists no
/// months, or if two products share a root.
fn parse(toml: &str) -> anyhow::Result<Vec<Product>> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Calendar {
        product: Vec<Product>,
    }
    let Calendar { product: products } = toml::from_str(toml)?;
    for (i, product) in products.iter().enumerate() {
        ensure!(
            !product.months.is_empty(),
            "{} lists no months",
            product.root
        );
        ensure!(
            products[..i].iter().all(|other| other.root != product.root),
            "{} is listed twice",
            product.root
        );
    }
    Ok(products)
}

/// A rolled futures product.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Product {
    /// The root id: the product's symbol on the data venue.
    root: InstrumentId,
    /// The liquid contract months, those the front contract rolls through.
    months: Vec<MonthCode>,
    /// The day the front contract rolls.
    roll: Roll,
    /// The time of day of the roll, UTC.
    time: civil::Time,
}

/// The day a product's front contract rolls to the next liquid one.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Roll {
    /// The last weekday of the month before the front contract's delivery
    /// month (COMEX metals' first notice day, holidays aside).
    LastBusinessDayBeforeDeliveryMonth,
}

/// A futures month code, January (F) to December (Z).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
enum MonthCode {
    F,
    G,
    H,
    J,
    K,
    M,
    N,
    Q,
    U,
    V,
    X,
    Z,
}

impl MonthCode {
    /// The month, 1 to 12.
    fn month(self) -> i8 {
        self as i8 + 1
    }
}

/// A dated contract of a product: its delivery year and month.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Contract {
    year: i16,
    month: MonthCode,
}

impl Product {
    /// The front contract at `time`: the first liquid contract not yet
    /// rolled out of, a roll at `time` exactly included.
    pub(super) fn front_at(&self, time: Timestamp) -> Contract {
        let year = time.to_zoned(TimeZone::UTC).year();
        // A contract's roll falls at most a month before its delivery, so
        // the front at `time` delivers within the year after `time`'s.
        (year..=year + 1)
            .flat_map(|year| {
                self.months
                    .iter()
                    .map(move |&month| Contract { year, month })
            })
            .find(|&contract| self.roll_out_of(contract) > time)
            .expect("a product has liquid months")
    }

    /// When the front rolls out of `contract` into the next liquid one.
    pub(super) fn roll_out_of(&self, contract: Contract) -> Timestamp {
        let Roll::LastBusinessDayBeforeDeliveryMonth = self.roll;
        let mut day = civil::date(contract.year, contract.month.month(), 1)
            .yesterday()
            .expect("a contract delivers within jiff's dates");
        while matches!(
            day.weekday(),
            civil::Weekday::Saturday | civil::Weekday::Sunday
        ) {
            day = day
                .yesterday()
                .expect("a contract delivers within jiff's dates");
        }
        day.to_datetime(self.time)
            .to_zoned(TimeZone::UTC)
            .expect("a contract delivers within jiff's dates")
            .timestamp()
    }

    /// The liquid contract after `contract`.
    pub(super) fn next(&self, contract: Contract) -> Contract {
        self.months
            .iter()
            .map(|&month| Contract {
                year: contract.year,
                month,
            })
            .find(|&next| next > contract)
            .unwrap_or(Contract {
                year: contract.year + 1,
                month: self.months[0],
            })
    }

    /// The instrument id of `contract`: the root's symbol, the month code
    /// and the year's last digit, on the root's venue, as Databento's raw
    /// symbols name them (`GCZ6.GLBX`).
    pub(super) fn instrument_id(&self, contract: Contract) -> InstrumentId {
        let symbol = format!(
            "{}{:?}{}",
            self.root.symbol,
            contract.month,
            contract.year.rem_euclid(10)
        );
        InstrumentId::new(Symbol::new(symbol), self.root.venue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_embedded_calendar_parses() {
        let products = parse(CALENDAR).unwrap();
        assert!(
            products
                .iter()
                .any(|product| product.root == InstrumentId::from("GC.GLBX"))
        );
    }

    #[test]
    fn rejects_a_root_listed_twice() {
        let product = r#"
            [[product]]
            root = "GC.GLBX"
            months = ["G", "J", "M", "Q", "Z"]
            roll = "last-business-day-before-delivery-month"
            time = "00:00"
        "#;
        let err = parse(&product.repeat(2)).unwrap_err();
        assert!(err.to_string().contains("GC.GLBX"), "{err}");
    }

    #[test]
    fn rejects_a_product_without_months() {
        let err = parse(
            r#"
            [[product]]
            root = "GC.GLBX"
            months = []
            roll = "last-business-day-before-delivery-month"
            time = "00:00"
            "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("no months"), "{err}");
    }

    #[test]
    fn rejects_an_unknown_month_code() {
        assert!(
            parse(
                r#"
                [[product]]
                root = "GC.GLBX"
                months = ["A"]
                roll = "last-business-day-before-delivery-month"
                time = "00:00"
                "#,
            )
            .is_err()
        );
    }
}
