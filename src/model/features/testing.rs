//! Declares features for the tests in `[indicators]`.

use std::rc::Rc;

use nautilus_core::{UnixNanos, datetime::NANOSECONDS_IN_MINUTE};
use nautilus_indicators::indicator::Indicator;
use nautilus_model::{
    data::{CustomData, OrderBookDeltas, OrderBookDepth, QuoteTick, TradeTick},
    enums::PriceType,
    identifiers::InstrumentId,
};

use super::{
    CustomHandler, EventFilter, EventTimes, Feature, FeatureSet, FeatureSetBuilder, OverBracket,
};
use crate::{
    model::targets::BracketWidth,
    strategy::{
        Declared, EmaArgs, IndicatorArgs, IndicatorsConfig, Series, requirements::CustomSeries,
    },
};

pub(crate) const MID: Declared<EmaArgs> = Declared::new("mid");

pub(crate) fn tables() -> IndicatorsConfig {
    toml::from_str(r#"mid = { period = 1, price_type = "MID" }"#).unwrap()
}

pub(crate) fn last_mid(instrument_id: InstrumentId) -> FeatureSetBuilder {
    FeatureSet::builder().feature(&[Series::quotes(instrument_id)], MID, &tables())
}

/// The last mid of a quote over the bracket's width there. Reading a width never handed panics.
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

/// An event at every decision once initialized, which reads the width.
impl EventFilter for MidOverWidth {
    fn is_event(&mut self, _previous: UnixNanos, _now: UnixNanos) -> bool {
        self.initialized()
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

pub(crate) fn mid_over(instrument_id: InstrumentId, width: &Rc<BracketWidth>) -> FeatureSetBuilder {
    let tables: IndicatorsConfig = toml::from_str("over = {}").unwrap();
    FeatureSet::builder()
        .bracket(width)
        .feature(&[Series::quotes(instrument_id)], OVER, &tables)
}

pub(crate) const BOOK: Declared<BookCountArgs> = Declared::new("book");

/// Two features, each warm once one came: the count of depth snapshots, then of delta batches.
pub(crate) fn book_counts(instrument_id: InstrumentId) -> FeatureSetBuilder {
    let tables: IndicatorsConfig = toml::from_str("book = {}").unwrap();
    FeatureSet::builder()
        .feature(&[Series::depth(instrument_id)], BOOK, &tables)
        .feature(&[Series::deltas(instrument_id)], BOOK, &tables)
}

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

pub(crate) fn custom_count(series: &CustomSeries) -> FeatureSetBuilder {
    let tables: IndicatorsConfig = toml::from_str("custom = {}").unwrap();
    FeatureSet::builder().feature(&[Series::Custom(series.clone())], CUSTOM, &tables)
}

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

/// An event at every decision once a point has come.
impl EventFilter for CustomCount {
    fn is_event(&mut self, _previous: UnixNanos, _now: UnixNanos) -> bool {
        true
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

pub(crate) const RISE: Declared<RiseArgs> = Declared::new("rise");

/// An edge event filter: an event is a quote's mid, or a trade's price, more than `step` above the
/// price before it, each recorded in its `EventTimes`. It is initialized once a price has come.
pub(crate) struct Rise {
    step: f64,
    last: Option<f64>,
    rises: EventTimes,
}

impl Rise {
    fn read(&mut self, price: f64, ts_init: UnixNanos) {
        if self.last.is_some_and(|last| price > last + self.step) {
            self.rises.record(ts_init);
        }
        self.last = Some(price);
    }
}

impl Indicator for Rise {
    fn name(&self) -> String {
        "Rise".to_string()
    }

    fn has_inputs(&self) -> bool {
        self.last.is_some()
    }

    fn initialized(&self) -> bool {
        self.last.is_some()
    }

    fn handle_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        self.read(quote.extract_price(PriceType::Mid)?.as_f64(), quote.ts_init);
        Ok(())
    }

    fn handle_trade(&mut self, trade: &TradeTick) {
        self.read(trade.price.as_f64(), trade.ts_init);
    }

    fn reset(&mut self) {
        self.last = None;
        self.rises = EventTimes::default();
    }
}

/// Its value is the last price, so it can be declared as a feature too.
impl Feature for Rise {
    fn value(&self) -> f64 {
        self.last.expect("a price, once initialized")
    }
}

impl EventFilter for Rise {
    fn is_event(&mut self, previous: UnixNanos, now: UnixNanos) -> bool {
        self.rises.in_window(previous, now)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RiseArgs {
    step: f64,
}

impl IndicatorArgs for RiseArgs {
    type Indicator = Rise;

    fn build(self) -> anyhow::Result<Rise> {
        Ok(Rise {
            step: self.step,
            last: None,
            rises: EventTimes::default(),
        })
    }
}

pub(crate) const FIRST_HALF_HOUR: Declared<FirstHalfHourArgs> = Declared::new("first_half_hour");

/// An event filter of time alone: every decision in the first half of an hour is an event.
pub(crate) struct FirstHalfHour;

impl Indicator for FirstHalfHour {
    fn name(&self) -> String {
        "FirstHalfHour".to_string()
    }

    fn has_inputs(&self) -> bool {
        false
    }

    fn initialized(&self) -> bool {
        true
    }

    fn reset(&mut self) {}
}

impl EventFilter for FirstHalfHour {
    fn is_event(&mut self, _previous: UnixNanos, now: UnixNanos) -> bool {
        now.as_u64() / NANOSECONDS_IN_MINUTE % 60 < 30
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FirstHalfHourArgs {}

impl IndicatorArgs for FirstHalfHourArgs {
    type Indicator = FirstHalfHour;

    fn build(self) -> anyhow::Result<FirstHalfHour> {
        Ok(FirstHalfHour)
    }
}
