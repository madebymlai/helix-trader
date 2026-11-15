//! A live start on IB of a strategy converting through IDEALPRO's
//! EUR/USD, end to end on the Docker paper gateway, with an empty catalog,
//! so that IB answers the whole warmup: the pair's 1-minute BID and ASK
//! bars, which the strategy's data requirements carry, are requested as
//! IB serves them (see `strategy::warmup`), the warmup completes, and
//! Nautilus' rate of EUR in USD through them is in the cache at its end,
//! as a trade sized at the start reads it.
//!
//! Gated as `ib_gateway.rs`: it returns at once, saying it was skipped,
//! unless `NAUTILUS_IB_RUN=1` is set in the shell. It places no order. Its
//! warmup reaches back 72 hours, so that it holds the pair's last bars on a
//! weekend too:
//!
//! ```sh
//! NAUTILUS_IB_RUN=1 cargo test --test live ib_pair_warmup:: -- --nocapture
//! ```
//!
//! Client id 50.

use crate::common;

use std::{cell::RefCell, rc::Rc, time::Duration};

use helix_trader::{
    live::Live,
    strategy::{ema::EmaCrossConfig, warmup::Warmup},
    venues::ib::IbAccount,
};
use nautilus_common::{
    actor::DataActor, logging::logger::LoggerConfig, messages::data::DataResponseEnvelope,
};
use nautilus_model::{
    data::{Bar, BarType},
    enums::PriceType,
    identifiers::{StrategyId, TraderId, Venue},
    types::Currency,
};
use nautilus_trading::{
    nautilus_strategy,
    strategy::{StrategyConfig, StrategyCore},
};
use rust_decimal::Decimal;

const TRADER: &str = "HELIXTEST-050";

/// The EMA crossover on SXR8.IBIS hour bars of IB's minute bars,
/// converting through IDEALPRO's EUR/USD, warmed up over 72 hours.
fn converting_ema_cross() -> EmaCrossConfig {
    EmaCrossConfig::load(
        r#"
        [data]
        series = [
            { data = "bars", instrument_id = "SXR8.IBIS", bar_spec = { step = 1, aggregation = "HOUR", price_type = "LAST" }, from = { step = 1, aggregation = "MINUTE", aggregation_source = "EXTERNAL" } },
        ]
        warmup = "72h"

        [indicators]
        fast = { period = 10 }
        slow = { period = 30 }

        [trade]
        instruments = [{ instrument_id = "SXR8.IBIS" }]
        risk = "0.5%"
        conversions = ["EUR/USD.IDEALPRO"]
        "#,
    )
    .unwrap()
}

#[derive(Debug, Default)]
struct Record {
    /// The bar types of the bars of the responses, each once.
    answered: Vec<BarType>,
    /// The rate of EUR in USD through IDEALPRO's pairs when the warmup
    /// ended.
    rate: Option<Decimal>,
    failure: Option<String>,
}

/// Warms up as a strategy does, reads the rate when the warmup ends, and
/// stops the node.
#[derive(Debug)]
struct Probe {
    core: StrategyCore,
    warmup: Rc<Warmup>,
    record: Rc<RefCell<Record>>,
}

nautilus_strategy!(Probe);

impl DataActor for Probe {
    fn on_start(&mut self) -> anyhow::Result<()> {
        Rc::clone(&self.warmup).start(self)
    }

    fn on_historical_bars(&mut self, bars: &[Bar]) -> anyhow::Result<()> {
        let mut record = self.record.borrow_mut();
        for bar in bars {
            if !record.answered.contains(&bar.bar_type) {
                record.answered.push(bar.bar_type);
            }
        }
        Ok(())
    }

    fn on_data_response(&mut self, response: &DataResponseEnvelope) -> anyhow::Result<()> {
        if let Err(e) = Rc::clone(&self.warmup).on_data_response(self, response) {
            self.record.borrow_mut().failure = Some(e.to_string());
        }
        if !self.warmup.is_warming() {
            let rate = self.cache().get_xrate(
                Venue::from("IDEALPRO"),
                Currency::EUR(),
                Currency::USD(),
                PriceType::Mid,
            );
            self.record.borrow_mut().rate = rate;
            self.shutdown_system(Some("the warmup ended".to_string()));
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_live_start_warms_up_an_idealpro_pairs_bid_and_ask_bars_and_has_its_rate() {
    if !std::env::var("NAUTILUS_IB_RUN").is_ok_and(|value| value == "1") {
        println!("skipped; set NAUTILUS_IB_RUN=1 to run it against IB");
        return;
    }
    crate::claim_process(module_path!());
    dotenvy::dotenv().ok();
    let account: IbAccount = std::env::var("NAUTILUS_IB_ACCOUNT_ID")
        .expect("NAUTILUS_IB_ACCOUNT_ID, a paper account")
        .parse()
        .unwrap();
    assert!(account.to_string().starts_with('D'), "not a paper account");
    let requirements = converting_ema_cross().data_requirements();
    let pair_bars = [
        BarType::from("EUR/USD.IDEALPRO-1-MINUTE-BID-EXTERNAL"),
        BarType::from("EUR/USD.IDEALPRO-1-MINUTE-ASK-EXTERNAL"),
    ];
    assert!(
        pair_bars
            .iter()
            .all(|bar_type| requirements.bar_types.contains(bar_type)),
        "{:?}",
        requirements.bar_types
    );
    let warmup = Rc::new(Warmup::live(&requirements));
    let record = Rc::new(RefCell::new(Record::default()));
    let probe = Probe {
        core: StrategyCore::new(StrategyConfig {
            strategy_id: Some(StrategyId::from("PROBE-001")),
            order_id_tag: Some("001".to_string()),
            ..Default::default()
        }),
        warmup: Rc::clone(&warmup),
        record: Rc::clone(&record),
    };
    // An empty catalog of its own: IB answers the whole warmup.
    let catalog = tempfile::tempdir().unwrap();
    // Outside the catalog, as `Live` requires.
    let staging = tempfile::tempdir().unwrap();
    let live = Live {
        trader_id: TraderId::from(TRADER),
        venues: crate::venues(&["--account", &account.to_string(), "--client-id", "50"]),
        logging: LoggerConfig::default(),
        catalog: catalog.path().to_path_buf(),
        recording: crate::recording(staging.path()),
    };
    let ran = tokio::time::timeout(
        Duration::from_secs(300),
        // The probe trades nothing: it claims no instrument.
        live.run(&requirements, &warmup, probe, None),
    )
    .await;
    common::cache::delete_trader(TRADER).await;
    ran.expect("the probe stops the node within 5 minutes")
        .unwrap();

    let record = record.borrow();
    println!("answered: {:?}, rate: {:?}", record.answered, record.rate);
    assert_eq!(record.failure, None);
    assert!(!warmup.is_warming(), "the warmup did not complete");
    assert!(
        pair_bars
            .iter()
            .all(|bar_type| record.answered.contains(bar_type)),
        "{:?}",
        record.answered
    );
    let rate = record
        .rate
        .expect("a rate of EUR in USD at the warmup's end");
    assert!(
        (Decimal::new(5, 1)..Decimal::new(2, 0)).contains(&rate),
        "{rate} USD per EUR"
    );
}
