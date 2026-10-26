//! IC's trading account as cTrader states it (`ProtoOATrader`): checked
//! for the adapter, and its balance and margin as Nautilus' account state.
//!
//! The adapter trades a hedging account only: each cTrader position is one
//! Nautilus position, its `positionId` the venue position id. A netting or
//! spread betting account, or another account than `IC_ACCOUNT_ID`, is
//! refused. Money is an integer scaled by `moneyDigits`, in the deposit
//! asset (`depositAssetId`).

use std::collections::HashMap;

use anyhow::{Context, ensure};
use nautilus_model::types::{AccountBalance, Currency, MarginBalance, Money};
use rust_decimal::Decimal;

use super::{
    config::IcAccountId,
    messages::{ProtoOaAccountType, ProtoOaAsset, ProtoOaPosition, ProtoOaTrader},
};

/// The account, checked: hedging, the configured one, in a currency
/// Nautilus knows.
#[derive(Debug, Clone, PartialEq)]
pub(super) struct IcAccount {
    /// The deposit currency, the account's base currency.
    pub(super) currency: Currency,
    /// The balance: deposits and realized PnL, without open positions.
    pub(super) balance: Decimal,
}

impl IcAccount {
    /// The account `trader` states, if it is `account_id`'s and hedging;
    /// `assets` name its deposit asset.
    ///
    /// # Errors
    ///
    /// Returns an error if the trader is another account, the account is
    /// not hedging, or its deposit asset is no currency Nautilus knows.
    pub(super) fn check(
        trader: &ProtoOaTrader,
        account_id: IcAccountId,
        assets: &HashMap<i64, ProtoOaAsset>,
    ) -> anyhow::Result<Self> {
        ensure!(
            trader.ctid_trader_account_id == account_id.get(),
            "cTrader returned the trader of another account than IC_ACCOUNT_ID"
        );
        let account_type = trader.account_type();
        ensure!(
            account_type == ProtoOaAccountType::Hedged,
            "the IC account is {}, not HEDGED: the adapter trades a hedging account only, each \
             cTrader position one Nautilus position (IC's default; a netting account is refused)",
            account_type.as_str_name()
        );
        let asset = assets.get(&trader.deposit_asset_id).with_context(|| {
            format!(
                "the IC account's deposit asset {} is not listed",
                trader.deposit_asset_id
            )
        })?;
        let currency = asset.name.parse().map_err(|_| {
            anyhow::anyhow!(
                "the IC account's deposit asset {} is no currency Nautilus knows",
                asset.name
            )
        })?;
        Ok(Self {
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
    pub(super) fn state(
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
            .map_err(|e| anyhow::anyhow!("the IC account balance: {e}"))?;
        let margin = Money::from_decimal(used, self.currency)
            .map_err(|e| anyhow::anyhow!("the IC account margin: {e}"))?;
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
