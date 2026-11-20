//! The broker's trading account, as cTrader states it (`ProtoOATrader`).
//!
//! The adapter trades a hedging account only. Each cTrader position is one Nautilus position, and
//! its `positionId` is the venue position id. The adapter refuses a netting or spread betting
//! account, and any account other than the credential's. Money is an integer scaled by
//! `moneyDigits`, in the deposit asset.

use std::collections::HashMap;

use anyhow::Context;
use nautilus_model::{
    identifiers::Venue,
    types::{AccountBalance, Currency, MarginBalance, Money},
};
use rust_decimal::Decimal;

use super::messages::{ProtoOaAccountType, ProtoOaAsset, ProtoOaPosition, ProtoOaTrader};
use crate::config::{CTraderAccountId, CTraderSettingNames};

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CTraderAccount {
    pub(crate) venue: Venue,
    pub(crate) currency: Currency,
    /// Deposits and realized PnL, excluding open positions.
    pub(crate) balance: Decimal,
}

impl CTraderAccount {
    /// Returns the account `trader` states, if it is `account_id` and hedging.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The trader is another account.
    /// - The account is not hedging.
    /// - The deposit asset is not a currency Nautilus knows.
    pub(crate) fn check(
        trader: &ProtoOaTrader,
        venue: Venue,
        account_id: CTraderAccountId,
        names: &CTraderSettingNames,
        assets: &HashMap<i64, ProtoOaAsset>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            trader.ctid_trader_account_id == account_id.get(),
            "cTrader returned the trader of another account than {}",
            names.account_id
        );
        let account_type = trader.account_type();
        anyhow::ensure!(
            account_type == ProtoOaAccountType::Hedged,
            "the {venue} account is {}, not HEDGED: the adapter trades a hedging account only, \
             each cTrader position one Nautilus position (a netting account is refused)",
            account_type.as_str_name()
        );
        let asset = assets.get(&trader.deposit_asset_id).with_context(|| {
            format!(
                "the {venue} account's deposit asset {} is not listed",
                trader.deposit_asset_id
            )
        })?;
        let currency = asset.name.parse().map_err(|_| {
            anyhow::anyhow!(
                "the {venue} account's deposit asset {} is no currency Nautilus knows",
                asset.name
            )
        })?;
        Ok(Self {
            venue,
            currency,
            balance: money(trader.balance, trader.money_digits)?,
        })
    }

    /// Returns the account state with `positions` open.
    ///
    /// The total is the balance, and the locked amount is the margin of the positions. The margin
    /// is account-wide, and counts as both initial and maintenance.
    ///
    /// # Errors
    ///
    /// Returns an error if an amount is out of `Money`'s range.
    pub(crate) fn state(
        &self,
        positions: &[ProtoOaPosition],
    ) -> anyhow::Result<(Vec<AccountBalance>, Vec<MarginBalance>)> {
        let mut used = Decimal::ZERO;
        for position in positions {
            let margin = i64::try_from(position.used_margin.unwrap_or_default())
                .context("a position's used margin is out of range")?;
            used += money(margin, position.money_digits)?;
        }
        let balance = AccountBalance::from_total_and_locked(self.balance, used, self.currency)
            .map_err(|e| anyhow::anyhow!("the {} account balance: {e}", self.venue))?;
        let margin = Money::from_decimal(used, self.currency)
            .map_err(|e| anyhow::anyhow!("the {} account margin: {e}", self.venue))?;
        Ok((
            vec![balance],
            vec![MarginBalance::new(margin, margin, None)],
        ))
    }
}

/// Returns a cTrader amount: `value` scaled by 10^`digits`.
///
/// `digits` is `moneyDigits`. It is 2 if absent, as on the demo.
fn money(value: i64, digits: Option<u32>) -> anyhow::Result<Decimal> {
    let digits = digits.unwrap_or(2);
    anyhow::ensure!(digits <= 28, "moneyDigits {digits} out of range");
    Ok(Decimal::new(value, digits))
}
