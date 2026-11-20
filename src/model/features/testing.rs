//! Features for the tests: the last mid of an instrument's quotes, the same over the bracket's
//! width, and counts of book depth and deltas and of custom points, declared in `[indicators]`.

use std::rc::Rc;

use nautilus_indicators::indicator::Indicator;
use nautilus_model::{
    data::{CustomData, OrderBookDeltas, OrderBookDepth, QuoteTick},
    identifiers::InstrumentId,
};

use super::{CustomHandler, Feature, FeatureSet, FeatureSetBuilder, OverBracket};
use crate::{
    model::targets::BracketWidth,
    strategy::{
        Declared, EmaArgs, IndicatorArgs, IndicatorsConfig, Series, requirements::CustomSeries,
    },
};

/// The last mid of a quote, an exponential average of period 1.
pub(crate) const MID: Declared<EmaArgs> = Declared::new("mid");

pub(crate) fn tables() -> IndicatorsConfig {
    toml::from_str(r#"mid = { period = 1, price_type = "MID" }"#).unwrap()
}

/// The feature `mid`, warm once a quote came.
pub(crate) fn last_mid(instrument_id: InstrumentId) -> FeatureSetBuilder {
    FeatureSet::builder().feature(&[Series::quotes(instrument_id)], MID, &tables())
}

/// The last mid of a quote over the bracket's width there, initialized once a quote came and the
/// width has a length. Reading a width never handed, it panics.
pub(crate) struct MidOverWidth {
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
        let width = self
            .width
            .as_ref()
            .expect("a width, handed as the set is built");
        self.mid.is_some() && width.initialized()
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
        let width = self
            .width
            .as_ref()
            .expect("a width, handed as the set is built");
        mid / width.length(mid).expect("a length")
    }

    fn over_bracket(&mut self) -> Option<&mut dyn OverBracket> {
        Some(self)
    }
}

impl OverBracket for MidOverWidth {
    fn over(&mut self, width: Rc<BracketWidth>) {
        self.width = Some(width);
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MidOverWidthArgs {}

impl IndicatorArgs for MidOverWidthArgs {
    type Indicator = MidOverWidth;

    fn build(self) -> anyhow::Result<MidOverWidth> {
        Ok(MidOverWidth {
            mid: None,
            width: None,
        })
    }
}

pub(crate) const OVER: Declared<MidOverWidthArgs> = Declared::new("over");

/// The feature `over`, the last mid over the bracket's `width`.
pub(crate) fn mid_over(instrument_id: InstrumentId, width: &Rc<BracketWidth>) -> FeatureSetBuilder {
    let tables: IndicatorsConfig = toml::from_str("over = {}").unwrap();
    FeatureSet::builder()
        .bracket(width)
        .feature(&[Series::quotes(instrument_id)], OVER, &tables)
}

pub(crate) const BOOK: Declared<BookCountArgs> = Declared::new("book");

/// Two features: the count of depth snapshots, then of batches of deltas, each warm once one came.
pub(crate) fn book_counts(instrument_id: InstrumentId) -> FeatureSetBuilder {
    let tables: IndicatorsConfig = toml::from_str("book = {}").unwrap();
    FeatureSet::builder()
        .feature(&[Series::depth(instrument_id)], BOOK, &tables)
        .feature(&[Series::deltas(instrument_id)], BOOK, &tables)
}

/// Counts the depth and deltas it gets, initialized by the first.
pub(crate) struct BookCount {
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

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BookCountArgs {}

impl IndicatorArgs for BookCountArgs {
    type Indicator = BookCount;

    fn build(self) -> anyhow::Result<BookCount> {
        Ok(BookCount { count: 0 })
    }
}

pub(crate) const CUSTOM: Declared<CustomCountArgs> = Declared::new("custom");

/// The count of the points of `series`, warm once one came.
pub(crate) fn custom_count(series: &CustomSeries) -> FeatureSetBuilder {
    let tables: IndicatorsConfig = toml::from_str("custom = {}").unwrap();
    FeatureSet::builder().feature(&[Series::Custom(series.clone())], CUSTOM, &tables)
}

/// Counts the custom data it gets, initialized by the first.
pub(crate) struct CustomCount {
    count: usize,
}

impl Indicator for CustomCount {
    fn name(&self) -> String {
        "CustomCount".to_string()
    }

    fn has_inputs(&self) -> bool {
        self.count > 0
    }

    fn initialized(&self) -> bool {
        self.count > 0
    }

    fn reset(&mut self) {
        self.count = 0;
    }
}

impl Feature for CustomCount {
    fn value(&self) -> f64 {
        self.count as f64
    }

    fn custom_handler(&mut self) -> Option<&mut dyn CustomHandler> {
        Some(self)
    }
}

impl CustomHandler for CustomCount {
    fn handle_custom(&mut self, _data: &CustomData) -> anyhow::Result<()> {
        self.count += 1;
        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CustomCountArgs {}

impl IndicatorArgs for CustomCountArgs {
    type Indicator = CustomCount;

    fn build(self) -> anyhow::Result<CustomCount> {
        Ok(CustomCount { count: 0 })
    }
}
