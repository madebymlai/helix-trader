//! The broker's trading account as cTrader states it (`ProtoOATrader`):
//! checked for the adapter, and its balance and margin as Nautilus'
//! account state.
//!
//! The adapter trades a hedging account only: each cTrader position is one
//! Nautilus position, its `positionId` the venue position id. A netting or
//! spread betting account, or another account than the credential's, is
//! refused. Money is an integer scaled by `moneyDigits`, in the deposit
//! asset (`depositAssetId`).

use std::collections::HashMap;

use anyhow::{Context, ensure};
use nautilus_model::{
    identifiers::Venue,
    types::{AccountBalance, Currency, MarginBalance, Money},
};
use rust_decimal::Decimal;

use super::messages::{ProtoOaAccountType, ProtoOaAsset, ProtoOaPosition, ProtoOaTrader};
use crate::config::{CTraderAccountId, CTraderSettingNames};

/// The account, checked: hedging, the configured one, in a currency
/// Nautilus knows.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CTraderAccount {
    /// The venue of the broker holding it, which errors name.
    pub(crate) venue: Venue,
    /// The deposit currency, the account's base currency.
    pub(crate) currency: Currency,
    /// The balance: deposits and realized PnL, without open positions.
    pub(crate) balance: Decimal,
}

impl CTraderAccount {
    /// The account `trader` states, at the broker of `venue`, if it is
    /// `account_id`'s, which `names` names, and hedging; `assets` name its
    /// deposit asset.
    ///
    /// # Errors
    ///
    /// Returns an error if the trader is another account, the account is
    /// not hedging, or its deposit asset is no currency Nautilus knows.
    pub(crate) fn check(
        trader: &ProtoOaTrader,
        venue: Venue,
        account_id: CTraderAccountId,
        names: &CTraderSettingNames,
        assets: &HashMap<i64, ProtoOaAsset>,
    ) -> anyhow::Result<Self> {
        ensure!(
            trader.ctid_trader_account_id == account_id.get(),
            "cTrader returned the trader of another account than {}",
            names.account_id
        );
        let account_type = trader.account_type();
        ensure!(
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

    /// The account state with `positions` open: one balance, the total the
    /// account's balance, locked the margin the positions use, free the
    /// rest; and that margin, account-wide, as initial and maintenance.
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

/// A cTrader amount: `value` scaled by 10^`digits` (`moneyDigits`, 2 if
/// absent, as on the demo's account).
fn money(value: i64, digits: Option<u32>) -> anyhow::Result<Decimal> {
    let digits = digits.unwrap_or(2);
    ensure!(digits <= 28, "moneyDigits {digits} out of range");
    Ok(Decimal::new(value, digits))
}
