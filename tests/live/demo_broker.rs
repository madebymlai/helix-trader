//! The cTrader brokers the demo modules (`ctrader_*`) run on, and what each broker's demo account
//! is expected to hold. The checks are the same on each broker. The venue, margins, sizes and costs
//! are the broker's.
//!
//! The gate is `NAUTILUS_CTRADER_RUN=ic` or `pepperstone` in the shell. Without it, a test returns
//! at once, saying it was skipped.

use std::ops::RangeInclusive;

use ctrader::CTraderConnection;
use helix_trader::venues::{ic, pepperstone};
use nautilus_model::{
    identifiers::{AccountId, InstrumentId},
    types::Quantity,
};
use rust_decimal::Decimal;

#[derive(Debug)]
pub(crate) struct DemoBroker {
    name: &'static str,
    venue: &'static str,
    environment_var: &'static str,
    connection: fn() -> anyhow::Result<CTraderConnection>,

    /// The leverage of the margin on gold and on EUR/USD.
    ///
    /// It is the lower of the account's leverage and the symbol's first tier.
    gold_leverage: u32,
    eurusd_leverage: u32,

    /// The ounces of gold's synthetic quote size, which is its maximum order.
    gold_quote_size: u32,

    commission_currency: &'static str,
    gold_commission: fn(f64) -> RangeInclusive<f64>,
}

/// The ounces of gold the orders test trades.
///
/// At Pepperstone's 0.035 USD an ounce, 3 oz is a commission of 0.105 USD, a half cent, so the
/// charge shows how the account rounds one.
pub(crate) const GOLD_OUNCES: u32 = 3;

const BROKERS: [DemoBroker; 2] = [
    // Measured on the demo account (2026-10-26): the account's 1:30 is under the symbols' own 1:200
    // and 1:1000, and IC's Raw charge is 30 USD per million per side, in EUR on this account, so
    // 1 oz of gold, about 4,000 USD, is charged 0.10 to 0.12 EUR, and the test's ounces as many
    // times.
    DemoBroker {
        name: "ic",
        venue: ic::VENUE,
        environment_var: "IC_ENVIRONMENT",
        connection: ic::connection,
        gold_leverage: 30,
        eurusd_leverage: 30,
        gold_quote_size: 10_000,
        commission_currency: "EUR",
        gold_commission: |price| {
            let usd = f64::from(GOLD_OUNCES) * price * 30.0 / 1_000_000.0;
            0.75 * usd..=usd
        },
    },
    // Measured on the 'Pepperstone - Europe' demo account (2026-11-04): the account's 1:30 is
    // over gold's own 1:20 and under EUR/USD's 1:30, an order is 5,000 oz at most, and gold is
    // charged 3.5 USD per lot per side (a lot is 100 oz), in the account's USD, in cents.
    DemoBroker {
        name: "pepperstone",
        venue: pepperstone::VENUE,
        environment_var: "PEPPERSTONE_ENVIRONMENT",
        connection: pepperstone::connection,
        gold_leverage: 20,
        eurusd_leverage: 30,
        gold_quote_size: 5_000,
        commission_currency: "USD",
        gold_commission: |_| 0.10..=0.11,
    },
];

/// Returns the broker a run names, if it names one.
///
/// It reads the shell before `.env`, so that the file cannot opt in, and claims the process for
/// `module` (`claim_process`). Then it reads `.env` and refuses any environment but the broker's
/// demo, before anything connects.
///
/// # Panics
///
/// Panics if:
/// - The run names no known broker.
/// - Another module has claimed the process.
/// - The broker's environment is not the demo.
pub(crate) fn opted_in(test: &str, module: &'static str) -> Option<&'static DemoBroker> {
    let Ok(name) = std::env::var("NAUTILUS_CTRADER_RUN") else {
        println!(
            "{test}: skipped; set NAUTILUS_CTRADER_RUN to `ic` or `pepperstone` to run it on \
             that broker's cTrader demo"
        );
        return None;
    };
    let broker = BROKERS
        .iter()
        .find(|broker| broker.name == name)
        .unwrap_or_else(|| panic!("NAUTILUS_CTRADER_RUN={name}: not `ic` or `pepperstone`"));
    crate::claim_process(module);
    dotenvy::dotenv().ok();
    assert_eq!(
        std::env::var(broker.environment_var).as_deref(),
        Ok("demo"),
        "the end to end tests run on the demo only: set {}=demo",
        broker.environment_var
    );
    Some(broker)
}

impl DemoBroker {
    pub(crate) fn connection(&self) -> CTraderConnection {
        (self.connection)().unwrap()
    }

    pub(crate) fn gold(&self) -> InstrumentId {
        InstrumentId::from(format!("XAUUSD.{}", self.venue).as_str())
    }

    pub(crate) fn eurusd(&self) -> InstrumentId {
        InstrumentId::from(format!("EURUSD.{}", self.venue).as_str())
    }

    pub(crate) fn account_id(&self) -> AccountId {
        AccountId::from(format!("{}-DEMO", self.venue).as_str())
    }

    pub(crate) fn gold_margin(&self) -> Decimal {
        Decimal::ONE / Decimal::from(self.gold_leverage)
    }

    pub(crate) fn eurusd_margin(&self) -> Decimal {
        Decimal::ONE / Decimal::from(self.eurusd_leverage)
    }

    pub(crate) fn gold_quote_size(&self) -> Quantity {
        Quantity::from(self.gold_quote_size)
    }

    pub(crate) fn commission_currency(&self) -> &'static str {
        self.commission_currency
    }

    pub(crate) fn gold_commission(&self, price: f64) -> RangeInclusive<f64> {
        (self.gold_commission)(price)
    }
}
