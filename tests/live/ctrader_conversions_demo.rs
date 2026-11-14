//! End to end on a cTrader broker's demo: a strategy converting through the
//! broker's EUR/USD (`[trade] conversions`) runs on `Live` reading gold's
//! minute bars, and the node's cache holds the pair's quotes after the
//! start, with no warmup of them: the broker's clients load the pair, which
//! the strategy reads as its quotes alone, and the strategy subscribes to
//! them from its start, as every strategy does through its roller
//! (`rolls::Roller`) of its data requirements' ticks. Its rates then convert
//! euros into dollars. It needs what `ctrader_demo.rs` needs, and the
//! forex market open.
//!
//! Gated as `ctrader_demo.rs` is. The strategy is a recorder wired as the
//! EMA crossover is, a warmup of its bars and a roller of its ticks, its
//! requirements and rates those of an EMA config listing the pair; it sends
//! no order, and the broker's execution client reconciles the account. One
//! node per process, hence its own module, run alone (`main.rs`). The node
//! keeps its cache in Redis (`live::redis`) under the test's trader,
//! `HELIXTEST-108`, whose records the test deletes once the node has stopped
//! cleanly. Run it with:
//!
//! ```sh
//! NAUTILUS_CTRADER_RUN=ic cargo test --test live ctrader_conversions_demo:: -- --nocapture
//! ```

use crate::common;

use std::{cell::RefCell, process::Command, rc::Rc, time::Duration};

use helix_trader::{
    live::Live,
    rolls::Roller,
    strategy::{conversions::Rates, ema::EmaCrossConfig, warmup::Warmup},
};
use nautilus_common::{
    actor::{DataActor, DataActorNative},
    logging::logger::LoggerConfig,
    messages::data::DataResponseEnvelope,
};
use nautilus_core::UnixNanos;
use nautilus_model::{
    data::QuoteTick,
    identifiers::{InstrumentId, StrategyId, TraderId},
    types::{Currency, Money},
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};

use crate::demo_broker::opted_in;

const TRADER: &str = "HELIXTEST-108";

/// What the recorder got.
#[derive(Debug, Default)]
struct Record {
    /// When it started.
    started: Option<UnixNanos>,
    /// Why the warmup failed, if it did.
    warmup_failure: Option<String>,
    /// The instruments of the quotes of its warmup.
    warmup_quoted: Vec<InstrumentId>,
    /// The pair's live quotes, as the strategy got them.
    pair_quotes: Vec<QuoteTick>,
    /// At its stop: the pair's latest quote in the cache, and 1,000 EUR in
    /// USD at the rates.
    cached: Option<QuoteTick>,
    converted: Option<Money>,
}

/// Warms up its bars through a `Warmup` and subscribes to its ticks through
/// a `Roller`, as the EMA crossover does, records the pair's quotes, and
/// at its stop reads the cache. It sends no order.
#[derive(Debug)]
struct Recorder {
    core: StrategyCore,
    warmup: Rc<Warmup>,
    roller: Roller,
    pair: InstrumentId,
    rates: Rates,
    record: Rc<RefCell<Record>>,
}

nautilus_strategy!(Recorder);

impl DataActor for Recorder {
    fn on_start(&mut self) -> anyhow::Result<()> {
        self.record.borrow_mut().started = Some(self.clock().timestamp_ns());
        Rc::clone(&self.warmup).start(self)?;
        self.roller.clone().start(self)
    }

    fn on_data_response(&mut self, response: &DataResponseEnvelope) -> anyhow::Result<()> {
        if let Err(e) = Rc::clone(&self.warmup).on_data_response(self, response) {
            self.record.borrow_mut().warmup_failure = Some(e.to_string());
        }
        Ok(())
    }

    fn on_historical_quotes(&mut self, quotes: &[QuoteTick]) -> anyhow::Result<()> {
        let mut record = self.record.borrow_mut();
        for quote in quotes {
            if !record.warmup_quoted.contains(&quote.instrument_id) {
                record.warmup_quoted.push(quote.instrument_id);
            }
        }
        Ok(())
    }

    fn on_quote(&mut self, quote: &QuoteTick) -> anyhow::Result<()> {
        if quote.instrument_id == self.pair {
            self.record.borrow_mut().pair_quotes.push(*quote);
        }
        Ok(())
    }

    fn on_stop(&mut self) -> anyhow::Result<()> {
        let cache = self.cache_ref();
        let cached = cache.quote(&self.pair).copied();
        let converted = self
            .rates
            .convert(&cache, [Money::from("1000 EUR")], Currency::USD())?;
        let mut record = self.record.borrow_mut();
        record.cached = cached;
        record.converted = converted;
        Ok(())
    }
}

/// Long enough for the node's start, the warmup and a minute of quotes.
const LIVE_RUN: Duration = Duration::from_secs(90);

#[tokio::test(flavor = "multi_thread")]
async fn a_live_run_caches_its_conversion_pairs_quotes_after_its_start() {
    let Some(broker) = opted_in(
        "a_live_run_caches_its_conversion_pairs_quotes_after_its_start",
        module_path!(),
    ) else {
        return;
    };
    let (gold, eurusd) = (broker.gold(), broker.eurusd());
    // Slow periods the run never warms, so the config stands for a
    // strategy that decides nothing here.
    let config = EmaCrossConfig::load(&format!(
        r#"
        [data]
        series = [
            {{ data = "bars", instrument_id = "{gold}", bar_spec = {{ step = 1, aggregation = "MINUTE", price_type = "MID" }} }},
        ]
        warmup = "10m"

        [indicators]
        fast = {{ period = 100 }}
        slow = {{ period = 200 }}

        [trade]
        instruments = [{{ instrument_id = "{gold}" }}]
        risk = "0.5%"
        conversions = ["{eurusd}"]
        "#
    ))
    .unwrap();
    let requirements = config.data_requirements();
    let warmup = Rc::new(Warmup::live(&requirements));
    let record = Rc::new(RefCell::new(Record::default()));
    let recorder = Recorder {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("RECORDER-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
        warmup: Rc::clone(&warmup),
        roller: Roller::live(requirements.ticks.clone()),
        pair: eurusd,
        rates: config.rates(),
        record: Rc::clone(&record),
    };
    // An empty catalog of its own: the broker answers the whole warmup.
    let catalog = tempfile::tempdir().unwrap();
    // Outside the catalog, as `Live` requires.
    let staging = tempfile::tempdir().unwrap();
    let live = Live {
        trader_id: TraderId::from(TRADER),
        venues: crate::venues(&[]),
        logging: LoggerConfig::default(),
        catalog: catalog.path().to_path_buf(),
        recording: crate::recording(staging.path()),
    };
    // As Ctrl-C would, once the node runs: its signal handler stops it.
    let interrupt = tokio::spawn(async {
        tokio::time::sleep(LIVE_RUN).await;
        let status = Command::new("kill")
            .args(["-INT", &std::process::id().to_string()])
            .status()
            .unwrap();
        assert!(status.success());
    });

    let ran = tokio::time::timeout(
        LIVE_RUN + Duration::from_secs(60),
        live.run(&requirements, &warmup, recorder, None),
    )
    .await;

    let interrupted = interrupt.is_finished();
    interrupt.abort();
    ran.expect("the node stops within a minute of SIGINT")
        .unwrap();
    assert!(interrupted, "the node stopped before SIGINT");
    common::cache::delete_trader(TRADER).await;
    let record = record.borrow();
    println!(
        "warmup quotes of {:?}; {} live quotes of {eurusd}, first {:?}; cached {:?}; \
         1,000 EUR is {:?}",
        record.warmup_quoted,
        record.pair_quotes.len(),
        record.pair_quotes.first(),
        record.cached,
        record.converted,
    );
    assert_eq!(record.warmup_failure, None);
    // The pair has no warmup: the gold's quotes alone, which its bars are
    // built from, are asked.
    assert!(
        !record.warmup_quoted.contains(&eurusd),
        "{:?}",
        record.warmup_quoted
    );
    // Subscribed from the start: its quotes reach the strategy, and the
    // cache holds one stamped after the start.
    assert!(!record.pair_quotes.is_empty(), "no quote of {eurusd}");
    let started = record.started.unwrap();
    let cached = record.cached.expect("no quote of the pair in the cache");
    assert!(cached.ts_init > started, "{cached:?} before {started}");
    // A rate, so a trade is sized: euros reach dollars through the pair.
    let converted = record.converted.expect("1,000 EUR not converted into USD");
    assert_eq!(converted.currency, Currency::USD());
    assert!(converted.as_decimal() > rust_decimal::Decimal::ZERO);
}
