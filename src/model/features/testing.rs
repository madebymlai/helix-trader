//! The features of the harness' tests: the last mid of an instrument's
//! quotes, the same over the bracket's width, and a count of an
//! instrument's book depth and deltas, declared and set in `[indicators]`
//! as a strategy's features are.

use std::rc::Rc;

use nautilus_indicators::indicator::Indicator;
use nautilus_model::{
    data::{OrderBookDeltas, OrderBookDepth, QuoteTick},
    identifiers::InstrumentId,
};

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

/// A count of the depth and deltas a feature gets, `[indicators.book]`.
pub const BOOK: Declared<BookCountArgs> = Declared::new("book");

/// The features of two, the count of `instrument_id`'s depth snapshots,
/// then of its batches of deltas, each warm once one came.
pub fn book_counts(instrument_id: InstrumentId) -> FeatureSetBuilder {
    let tables: IndicatorsConfig = toml::from_str("book = {}").unwrap();
    FeatureSet::builder()
        .depth(instrument_id, BOOK, &tables)
        .deltas(instrument_id, BOOK, &tables)
}

/// A feature counting the depth and deltas it gets, initialized by the
/// first, its value the count.
pub struct BookCount {
    count: usize,
}

impl Indicator for BookCount {
    fn name(&self) -> String {
        "BookCount".to_string()
    }

    fn has_inputs(&self) -> bool {
        self.count > 0
    }

    fn initialized(&self) -> bool {
        self.count > 0
    }

    fn handle_depth(&mut self, _depth: &OrderBookDepth) {
        self.count += 1;
    }

    fn handle_deltas(&mut self, _deltas: &OrderBookDeltas) {
        self.count += 1;
    }

    fn reset(&mut self) {
        self.count = 0;
    }
}

impl Feature for BookCount {
    fn value(&self) -> f64 {
        self.count as f64
    }
}

/// The arguments of `BookCount`: none.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BookCountArgs {}

impl IndicatorArgs for BookCountArgs {
    type Indicator = BookCount;

    fn build(self) -> anyhow::Result<BookCount> {
        Ok(BookCount { count: 0 })
    }
}
