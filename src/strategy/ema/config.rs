use anyhow::ensure;
use jiff::SignedDuration;
use nautilus_model::{
    identifiers::{InstrumentId, StrategyId},
    types::Quantity,
};
use nautilus_trading::strategy::StrategyConfig;
use serde::Deserialize;

use super::bracket::Bps;
use crate::strategy::BarsConfig;

/// Config of the EMA crossover strategy, loaded from TOML.
#[derive(Debug, Clone, Deserialize, bon::Builder)]
#[serde(deny_unknown_fields)]
pub struct EmaCrossConfig {
    #[serde(default = "default_base")]
    #[builder(default = default_base())]
    pub base: StrategyConfig,
    /// The bars read, their warmup and their session: those of every
    /// instrument in `instrument_ids`.
    #[serde(flatten)]
    pub bars: BarsConfig,
    /// The one instrument traded, among `instrument_ids`; the others are
    /// read only.
    pub trade_instrument_id: InstrumentId,
    #[serde(default = "default_fast_period")]
    #[builder(default = default_fast_period())]
    pub fast_period: usize,
    #[serde(default = "default_slow_period")]
    #[builder(default = default_slow_period())]
    pub slow_period: usize,
    /// The size of each trade.
    pub trade_size: Quantity,
    /// The stop loss of each trade, this far from the close of the bar it
    /// was decided on, against the trade.
    pub stop_loss_bps: Bps,
    /// The take profit of each trade, this far from the close of the bar
    /// it was decided on, in the trade's favour.
    pub take_profit_bps: Bps,
    /// How long a trade is held after its entry fills, unless its stop
    /// loss or take profit ends it first, such as `"1h"`.
    pub hold: SignedDuration,
}

impl EmaCrossConfig {
    /// # Errors
    ///
    /// Returns an error if the bars are inconsistent (see
    /// `BarsConfig::validate`), the traded instrument not among those read,
    /// the periods out of order, the size, the distances or the hold not
    /// positive, or the stop loss of a long at or below zero.
    pub fn validate(&self) -> anyhow::Result<()> {
        self.bars.validate()?;
        ensure!(
            self.bars.instrument_ids.contains(&self.trade_instrument_id),
            "trade instrument {} is not among the instrument ids",
            self.trade_instrument_id
        );
        ensure!(
            self.fast_period > 0 && self.fast_period < self.slow_period,
            "fast period must be positive and below the slow period"
        );
        ensure!(self.trade_size.is_positive(), "trade size must be positive");
        ensure!(
            self.stop_loss_bps.is_positive() && self.stop_loss_bps < Bps::WHOLE,
            "stop loss must be above 0 and below 10000 bps"
        );
        ensure!(
            self.take_profit_bps.is_positive(),
            "take profit must be positive"
        );
        ensure!(self.hold.is_positive(), "hold must be positive");
        Ok(())
    }
}

/// The strategy's id and order tag. `oms_type` is left unset: Nautilus
/// keeps the strategy's positions as the execution client of the venue
/// does, netting or hedging, in a backtest as live.
fn default_base() -> StrategyConfig {
    StrategyConfig {
        strategy_id: Some(StrategyId::from("EMA_CROSS-001")),
        order_id_tag: Some("001".to_string()),
        ..Default::default()
    }
}

const fn default_fast_period() -> usize {
    10
}

const fn default_slow_period() -> usize {
    30
}

#[cfg(test)]
mod tests {
    use nautilus_model::data::BarType;
    use rust_decimal::Decimal;

    use super::*;

    const TOML: &str = r#"
        instrument_ids = ["ESH4.CME", "ESM4.CME"]
        trade_instrument_id = "ESM4.CME"
        bar_spec = { step = 1, aggregation = "HOUR", price_type = "MID" }
        trade_size = "1"
        stop_loss_bps = 50
        take_profit_bps = "75.5"
        hold = "1h"
        warmup = "720h"
        session = { time_zone = "America/Chicago", open = "08:30", close = "15:00" }
    "#;

    #[test]
    fn loads_from_toml_with_defaults() {
        let config: EmaCrossConfig = toml::from_str(TOML).unwrap();
        config.validate().unwrap();
        assert_eq!(config.fast_period, 10);
        assert_eq!(config.slow_period, 30);
        assert_eq!(config.trade_size, Quantity::from(1));
        assert_eq!(config.trade_instrument_id, InstrumentId::from("ESM4.CME"));
        assert_eq!(config.stop_loss_bps, Bps::from(Decimal::from(50)));
        assert_eq!(config.take_profit_bps, Bps::from(Decimal::new(755, 1)));
        assert_eq!(config.hold, SignedDuration::from_hours(1));
    }

    #[test]
    fn loads_the_bars_from_the_top_of_the_toml() {
        let config: EmaCrossConfig = toml::from_str(TOML).unwrap();
        assert_eq!(
            config.bars.data_requirements().bar_types,
            [
                BarType::from("ESH4.CME-1-HOUR-MID-INTERNAL"),
                BarType::from("ESM4.CME-1-HOUR-MID-INTERNAL")
            ]
        );
        assert_eq!(config.bars.session.unwrap().open.to_string(), "08:30:00");
    }

    #[test]
    fn leaves_the_oms_to_the_venue() {
        let config: EmaCrossConfig = toml::from_str(TOML).unwrap();
        assert_eq!(config.base.oms_type, None);
    }

    #[test]
    fn rejects_a_misspelt_session_at_load() {
        let toml = TOML.replace("session =", "sesion =");
        let err = toml::from_str::<EmaCrossConfig>(&toml).unwrap_err();
        assert!(err.to_string().contains("sesion"), "{err}");
    }

    #[test]
    fn rejects_a_misspelt_period_at_load() {
        let toml = format!("{TOML}\nfast_perod = 5");
        let err = toml::from_str::<EmaCrossConfig>(&toml).unwrap_err();
        assert!(err.to_string().contains("fast_perod"), "{err}");
    }

    #[test]
    fn rejects_an_unknown_time_zone_at_load() {
        let toml = TOML.replace("America/Chicago", "America/Gotham");
        let err = toml::from_str::<EmaCrossConfig>(&toml).unwrap_err();
        assert!(err.to_string().contains("America/Gotham"), "{err}");
    }

    #[test]
    fn rejects_inconsistent_bars() {
        let toml = TOML.replace("ESM4.CME", "ESM4.EUREX");
        let config: EmaCrossConfig = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("venue"), "{err}");
    }

    #[test]
    fn rejects_fast_period_not_below_slow() {
        let toml = format!("{TOML}\nfast_period = 30");
        let config: EmaCrossConfig = toml::from_str(&toml).unwrap();
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_a_non_positive_trade_size() {
        let toml = TOML.replace("trade_size = \"1\"", "trade_size = \"0\"");
        let config: EmaCrossConfig = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("trade size"), "{err}");
    }

    #[test]
    fn rejects_a_trade_instrument_not_read() {
        let toml = TOML.replace(
            "trade_instrument_id = \"ESM4.CME\"",
            "trade_instrument_id = \"ESU4.CME\"",
        );
        let config: EmaCrossConfig = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("ESU4.CME"), "{err}");
    }

    #[test]
    fn rejects_a_missing_trade_instrument_at_load() {
        let toml = TOML.replace("trade_instrument_id = \"ESM4.CME\"", "");
        let err = toml::from_str::<EmaCrossConfig>(&toml).unwrap_err();
        assert!(err.to_string().contains("trade_instrument_id"), "{err}");
    }

    #[test]
    fn rejects_non_positive_exits() {
        for (from, to) in [
            ("stop_loss_bps = 50", "stop_loss_bps = 0"),
            ("take_profit_bps = \"75.5\"", "take_profit_bps = -1"),
            ("hold = \"1h\"", "hold = \"0s\""),
        ] {
            let config: EmaCrossConfig = toml::from_str(&TOML.replace(from, to)).unwrap();
            assert!(config.validate().is_err(), "{to}");
        }
    }

    #[test]
    fn rejects_a_stop_loss_of_the_whole_price() {
        let toml = TOML.replace("stop_loss_bps = 50", "stop_loss_bps = 10000");
        let config: EmaCrossConfig = toml::from_str(&toml).unwrap();
        let err = config.validate().unwrap_err().to_string();
        assert!(err.contains("stop loss"), "{err}");
    }
}
