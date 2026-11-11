//! The features of the harness' tests: the last mid of an instrument's
//! quotes, and the same over the bracket's width, declared and set in
//! `[indicators]` as a strategy's features are.

use std::rc::Rc;

use nautilus_indicators::indicator::Indicator;
use nautilus_model::{data::QuoteTick, identifiers::InstrumentId};

use super::{Feature, FeatureSet, FeatureSetBuilder, OverBracket};
use crate::{
    model::targets::BracketWidth,
    strategy::{Declared, EmaArgs, IndicatorArgs, IndicatorsConfig},
};

/// The last mid of a quote, Nautilus' exponential average of one,
/// `[indicators.mid]`.
pub const MID: Declared<EmaArgs> = Declared::new("mid");

/// `[indicators.mid]`, as `[indicators]` holds it.
pub fn tables() -> IndicatorsConfig {
    toml::from_str(r#"mid = { period = 1, price_type = "MID" }"#).unwrap()
}

/// The features of one, `mid`, the last mid of `instrument_id`, warm once
/// a quote came.
pub fn last_mid(instrument_id: InstrumentId) -> FeatureSetBuilder {
    FeatureSet::builder().quotes(instrument_id, MID, &tables())
}

/// The last mid of a quote over the bracket's width there: initialized once
/// a quote came and the width has a length.
pub struct MidOverWidth {
    mid: Option<f64>,
    width: Option<Rc<BracketWidth>>,
}

impl Indicator for MidOverWidth {
    fn name(&self) -> String {
        "MidOverWidth".to_string()
    }

    fn has_inputs(&self) -> bool {
        self.mid.is_some()
    }

    fn initialized(&self) -> bool {
        self.mid.is_some() && self.width.as_ref().is_some_and(|width| width.initialized())
    }

    fn handle_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        self.mid = Some((quote.bid_price.as_f64() + quote.ask_price.as_f64()) / 2.0);
        Ok(())
    }

    fn reset(&mut self) {
        self.mid = None;
    }
}

impl Feature for MidOverWidth {
    fn value(&self) -> f64 {
        let mid = self.mid.expect("a mid");
        let width = self.width.as_ref().expect("a width");
        mid / width.length(mid).expect("a length")
    }
}

impl OverBracket for MidOverWidth {
    fn over(&mut self, width: Rc<BracketWidth>) {
        self.width = Some(width);
    }
}

/// The arguments of `MidOverWidth`: none.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MidOverWidthArgs {}

impl IndicatorArgs for MidOverWidthArgs {
    type Indicator = MidOverWidth;

    fn build(self) -> anyhow::Result<MidOverWidth> {
        Ok(MidOverWidth {
            mid: None,
            width: None,
        })
    }
}

/// `[indicators.over]`.
pub const OVER: Declared<MidOverWidthArgs> = Declared::new("over");

/// The features of one, `over`, the last mid of `instrument_id` over the
/// bracket's `width` there.
pub fn mid_over(instrument_id: InstrumentId, width: &Rc<BracketWidth>) -> FeatureSetBuilder {
    let tables: IndicatorsConfig = toml::from_str("over = {}").unwrap();
    FeatureSet::builder().over_bracket(instrument_id, OVER, &tables, width)
}
