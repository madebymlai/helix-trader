//! The roll calendar: per product, how its contracts are named, its liquid contract months and the
//! day and time the front contract rolls, read from `calendar.toml`, embedded at build time.

use std::sync::LazyLock;

use anyhow::Context;
use jiff::{Timestamp, civil, tz::TimeZone};
use nautilus_model::identifiers::{InstrumentId, Symbol};
use serde::Deserialize;

const CALENDAR: &str = include_str!("calendar.toml");

static PRODUCTS: LazyLock<Vec<Product>> =
    LazyLock::new(|| parse(CALENDAR).expect("the embedded roll calendar parses"));

pub(super) fn product(root: InstrumentId) -> Option<&'static Product> {
    PRODUCTS.iter().find(|product| product.root == root)
}

pub(super) fn root_of(contract: InstrumentId) -> Option<InstrumentId> {
    PRODUCTS
        .iter()
        .find(|product| product.rolls_through(contract))
        .map(|product| product.root)
}

/// The products of a calendar in TOML.
///
/// # Errors
///
/// Returns an error if:
/// - The TOML is not a calendar (a product without `days_before`, or with a negative one, is not).
/// - A product's contract template does not parse.
/// - A product lists no months.
/// - Two products share a root.
fn parse(toml: &str) -> anyhow::Result<Vec<Product>> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Calendar {
        product: Vec<Product>,
    }
    let Calendar { product: products } = toml::from_str(toml)?;
    for (i, product) in products.iter().enumerate() {
        anyhow::ensure!(
            !product.months.is_empty(),
            "{} lists no months",
            product.root
        );
        anyhow::ensure!(
            products[..i].iter().all(|other| other.root != product.root),
            "{} is listed twice",
            product.root
        );
    }
    Ok(products)
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Product {
    root: InstrumentId,
    contract: ContractTemplate,
    months: Vec<MonthCode>,
    roll: Roll,
    /// The weekdays before the rule day the front rolls, so that it rolls before the volume leaves.
    days_before: u32,
    /// The time of day of the roll, UTC.
    time: civil::Time,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum Roll {
    /// The last weekday of the month before the delivery month (COMEX metals' first notice day,
    /// holidays aside).
    LastBusinessDayBeforeDeliveryMonth,
}

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
    fn month(self) -> i8 {
        self as i8 + 1
    }
}

/// A contract's symbol as literal text and placeholders: `{month}` the month code, `{y}` the
/// year's last digit, `{yy}` its last two, `{yyyy}` the year (`GC{month}{y}` is `GCZ6`). It names a
/// month and a year, or two contracts would share a symbol.
#[derive(Debug, Clone, Deserialize)]
#[serde(try_from = "String")]
struct ContractTemplate(Vec<Part>);

#[derive(Debug, Clone)]
enum Part {
    Text(String),
    Month,
    Y,
    Yy,
    Yyyy,
}

impl TryFrom<String> for ContractTemplate {
    type Error = anyhow::Error;

    fn try_from(template: String) -> anyhow::Result<Self> {
        let mut parts = Vec::new();
        let mut rest = template.as_str();
        while let Some(open) = rest.find('{') {
            if open > 0 {
                parts.push(Part::Text(rest[..open].to_owned()));
            }
            let (name, after) = rest[open + 1..]
                .split_once('}')
                .with_context(|| format!("contract {template:?} has an unclosed brace"))?;
            parts.push(match name {
                "month" => Part::Month,
                "y" => Part::Y,
                "yy" => Part::Yy,
                "yyyy" => Part::Yyyy,
                _ => anyhow::bail!("contract {template:?} has an unknown placeholder {{{name}}}"),
            });
            rest = after;
        }
        if !rest.is_empty() {
            parts.push(Part::Text(rest.to_owned()));
        }
        anyhow::ensure!(
            parts.iter().any(|part| matches!(part, Part::Month)),
            "contract {template:?} has no {{month}}"
        );
        anyhow::ensure!(
            parts
                .iter()
                .any(|part| matches!(part, Part::Y | Part::Yy | Part::Yyyy)),
            "contract {template:?} has no year: {{y}}, {{yy}} or {{yyyy}}"
        );
        Ok(Self(parts))
    }
}

impl ContractTemplate {
    fn fill(&self, contract: Contract) -> Symbol {
        let symbol: String = self
            .0
            .iter()
            .map(|part| match part {
                Part::Text(text) => text.clone(),
                Part::Month => format!("{:?}", contract.month),
                Part::Y => contract.year.rem_euclid(10).to_string(),
                Part::Yy => format!("{:02}", contract.year.rem_euclid(100)),
                Part::Yyyy => format!("{:04}", contract.year),
            })
            .collect();
        Symbol::new(symbol)
    }

    /// The month code in `symbol`, if the template fills it so: each placeholder as wide as `fill`
    /// writes it.
    fn month_of<'a>(&self, symbol: &'a str) -> Option<&'a str> {
        let mut month = None;
        let mut rest = symbol;
        for part in &self.0 {
            let width = match part {
                Part::Text(text) => {
                    rest = rest.strip_prefix(text.as_str())?;
                    continue;
                }
                Part::Month | Part::Y => 1,
                Part::Yy => 2,
                Part::Yyyy => 4,
            };
            let (filled, after) = rest.split_at_checked(width)?;
            match part {
                Part::Month if filled.chars().all(|c| c.is_ascii_uppercase()) => {
                    month = Some(filled);
                }
                Part::Y | Part::Yy | Part::Yyyy if filled.chars().all(|c| c.is_ascii_digit()) => {}
                _ => return None,
            }
            rest = after;
        }
        month.filter(|_| rest.is_empty())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Contract {
    year: i16,
    month: MonthCode,
}

impl Product {
    /// The front contract at `time`: the first liquid contract not yet rolled out of, a roll at
    /// `time` included.
    pub(super) fn front_at(&self, time: Timestamp) -> Contract {
        let year = time.to_zoned(TimeZone::UTC).year();

        // A roll falls weeks before delivery, so the front is in `time`'s year or the next
        (year..=year + 1)
            .flat_map(|year| {
                self.months
                    .iter()
                    .map(move |&month| Contract { year, month })
            })
            .find(|&contract| self.roll_out_of(contract) > time)
            .expect("a product has liquid months")
    }

    /// When the front rolls out of `contract`: `days_before` business days before the rule day, at
    /// the product's time.
    pub(super) fn roll_out_of(&self, contract: Contract) -> Timestamp {
        let Roll::LastBusinessDayBeforeDeliveryMonth = self.roll;
        let rule_day = weekday_before(civil::date(contract.year, contract.month.month(), 1));
        let day = (0..self.days_before).fold(rule_day, |day, _| weekday_before(day));
        day.to_datetime(self.time)
            .to_zoned(TimeZone::UTC)
            .expect("a contract delivers within jiff's dates")
            .timestamp()
    }

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

    pub(super) fn previous(&self, contract: Contract) -> Contract {
        self.months
            .iter()
            .rev()
            .map(|&month| Contract {
                year: contract.year,
                month,
            })
            .find(|&previous| previous < contract)
            .unwrap_or(Contract {
                year: contract.year - 1,
                month: self.months[self.months.len() - 1],
            })
    }

    /// The instrument id of `contract` on the root's venue (`GCZ26.GLBX`).
    pub(super) fn instrument_id(&self, contract: Contract) -> InstrumentId {
        InstrumentId::new(self.contract.fill(contract), self.root.venue)
    }

    /// Whether the front rolls through `contract`, of any year: on the root's venue, with a liquid
    /// month (`GCZ26.GLBX` of GC's, not `GCV26.GLBX`).
    fn rolls_through(&self, contract: InstrumentId) -> bool {
        contract.venue == self.root.venue
            && self
                .contract
                .month_of(contract.symbol.as_str())
                .is_some_and(|code| self.months.iter().any(|month| format!("{month:?}") == code))
    }
}

fn weekday_before(date: civil::Date) -> civil::Date {
    let mut day = date;
    loop {
        day = day
            .yesterday()
            .expect("a contract delivers within jiff's dates");
        if !matches!(
            day.weekday(),
            civil::Weekday::Saturday | civil::Weekday::Sunday
        ) {
            return day;
        }
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::identifiers::Venue;
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn rejects_a_root_listed_twice() {
        let product = r#"
            [[product]]
            root = "GC.GLBX"
            contract = "GC{month}{y}"
            months = ["G", "J", "M", "Q", "Z"]
            roll = "last-business-day-before-delivery-month"
            days_before = 2
            time = "00:00"
        "#;
        let err = parse(&product.repeat(2)).unwrap_err();
        assert!(err.to_string().contains("GC.GLBX"), "{err}");
    }

    #[rstest]
    fn rejects_a_product_without_months() {
        let err = parse(
            r#"
            [[product]]
            root = "GC.GLBX"
            contract = "GC{month}{y}"
            months = []
            roll = "last-business-day-before-delivery-month"
            days_before = 2
            time = "00:00"
            "#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("GC.GLBX"), "{err}");
    }

    fn gold_rolling(days_before: &str) -> Product {
        let products = parse(&format!(
            r#"
            [[product]]
            root = "GC.GLBX"
            contract = "GC{{month}}{{y}}"
            months = ["G", "J", "M", "Q", "Z"]
            roll = "last-business-day-before-delivery-month"
            days_before = {days_before}
            time = "00:00"
            "#
        ))
        .unwrap();
        products.into_iter().next().unwrap()
    }

    fn roll_out_of(gold: &Product, year: i16, month: MonthCode) -> Timestamp {
        gold.roll_out_of(Contract { year, month })
    }

    /// The last weekday of July 2026 is Fri the 31st. Of March 2026 it is Tue the 31st, and three
    /// weekdays before it are Mon the 30th, Fri the 27th and Thu the 26th.
    #[rstest]
    #[case::with_no_days_before("0", MonthCode::Q, "2026-07-31T00:00Z")]
    #[case::days_before_counted_in_weekdays_across_a_weekend(
        "3",
        MonthCode::J,
        "2026-03-26T00:00Z"
    )]
    fn rolls_on_the_rule_day_less_its_days_before(
        #[case] days_before: &str,
        #[case] month: MonthCode,
        #[case] roll: &str,
    ) {
        let gold = gold_rolling(days_before);
        assert_eq!(
            roll_out_of(&gold, 2026, month),
            roll.parse::<Timestamp>().unwrap()
        );
    }

    fn gold_named(contract: &str) -> anyhow::Result<Product> {
        let products = parse(&format!(
            r#"
            [[product]]
            root = "GC.GLBX"
            contract = "{contract}"
            months = ["G", "J", "M", "Q", "Z"]
            roll = "last-business-day-before-delivery-month"
            days_before = 2
            time = "00:00"
            "#
        ))?;
        Ok(products.into_iter().next().unwrap())
    }

    fn december(year: i16) -> Contract {
        Contract {
            year,
            month: MonthCode::Z,
        }
    }

    #[rstest]
    #[case::by_month_code_and_the_years_last_digit("GC{month}{y}", 2026, "GCZ6")]
    #[case::by_the_years_last_digit_into_a_new_decade("GC{month}{y}", 2030, "GCZ0")]
    #[case::by_the_two_digit_year_with_literal_text("GC.{month}{yy}", 2026, "GC.Z26")]
    #[case::by_the_two_digit_year_padded("GC.{month}{yy}", 2007, "GC.Z07")]
    #[case::by_the_four_digit_year_in_any_order("GC-{yyyy}-{month}", 2026, "GC-2026-Z")]
    fn names_a_contract_by_its_template(
        #[case] template: &str,
        #[case] year: i16,
        #[case] symbol: &str,
    ) {
        let gold = gold_named(template).unwrap();
        assert_eq!(
            gold.instrument_id(december(year)),
            InstrumentId::new(Symbol::new(symbol), Venue::new("GLBX"))
        );
    }

    #[rstest]
    #[case::an_unknown_placeholder("GC{month}{yyy}")]
    #[case::an_unclosed_brace("GC{month}{y")]
    #[case::a_contract_without_a_month("GC{y}")]
    #[case::a_contract_without_a_year("GC{month}")]
    fn rejects_a_template_naming_it(#[case] template: &str) {
        let err = ContractTemplate::try_from(template.to_string()).unwrap_err();
        assert!(err.to_string().contains(template), "{err}");
    }
}
