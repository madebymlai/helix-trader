//! End to end on LSE: a node streams the trades of AAPL's options of the
//! week, `OPRA` contracts, for a minute through the LSE adapter's data
//! client on data-ws, routed OPRA as the live node routes it, all of them
//! through one subscription of AAPL's chain. The contracts are the calls
//! and puts of the week's Friday expiry, the first on or after the run's
//! UTC day, struck at 200 to 500 by 5, a band holding AAPL's listed
//! strikes near the money (AAPL at about 355 in November 2026); a strike
//! not listed simply never trades. Their definitions, which Databento gives
//! in a live run, filled into the catalog, are written into the node's
//! cache by hand as Nautilus' Databento decoder gives Databento's: priced
//! to a cent, a multiplier of 1. It needs the LSE key, `LSE_API_KEY`, from
//! the environment or `.env`; the free key streams US options, on one
//! connection a key, so no other live run may hold data-ws meanwhile.
//!
//! A chain sends no last prices, only its contracts' next trades, so run it
//! while US options trade, 14:30 to 21:00 UTC on a trading day; at any
//! other time no trade comes and it fails saying so.
//!
//! Gated as `lse_history.rs` is: the test returns at once, saying it was
//! skipped, unless `NAUTILUS_LSE_RUN=1` is set in the shell. Run it with:
//!
//! ```sh
//! NAUTILUS_LSE_RUN=1 cargo test --test live lse_option_trades:: -- --nocapture
//! ```

use std::{cell::RefCell, collections::BTreeMap, rc::Rc, time::Duration};

use jiff::{Timestamp, ToSpan, civil::Weekday, tz::TimeZone};
use nautilus_common::{
    actor::{DataActor, DataActorConfig, DataActorCore},
    enums::Environment,
    nautilus_actor,
};
use nautilus_core::UnixNanos;
use nautilus_live::{
    builder::LiveNodeBuilder,
    config::{LiveNodeConfig, RoutingConfig},
};
use nautilus_model::{
    data::TradeTick,
    enums::{AggressorSide, AssetClass, OptionKind},
    identifiers::{InstrumentId, Symbol, TraderId},
    instruments::{Instrument, InstrumentAny, OptionContract},
    types::{Currency, Price, Quantity},
};

/// How long the trades stream.
const STREAMED: Duration = Duration::from_secs(60);

/// Whether the run is opted in: from the shell, before `.env` is read, so
/// that the file cannot opt in.
/// An opted-in run claims the process for this module (`claim_process`).
fn opted_in(test: &str) -> bool {
    let run = std::env::var("NAUTILUS_LSE_RUN").is_ok_and(|value| value == "1");
    if run {
        crate::claim_process(module_path!());
    } else {
        println!("{test}: skipped; set NAUTILUS_LSE_RUN=1 to run it on LSE");
    }
    run
}

/// AAPL's calls and puts expiring on the week's Friday, the first on or
/// after `now`'s UTC day, struck at 200 to 500 by 5, defined as Nautilus'
/// Databento decoder defines an OPRA contract (`decode_option_contract`):
/// an equity option in USD, priced to a cent, expiring at 16:00 New York,
/// its multiplier and lot size 1, Databento giving neither.
fn contracts_of_the_week(now: Timestamp) -> Vec<InstrumentAny> {
    let today = now.to_zoned(TimeZone::UTC).date();
    let friday = (0..7)
        .map(|days| today + days.days())
        .find(|day| day.weekday() == Weekday::Friday)
        .expect("a Friday in a week");
    let expiration = friday
        .at(16, 0, 0, 0)
        .in_tz("America/New_York")
        .expect("New York's time zone")
        .timestamp();
    let mut contracts = Vec::new();
    for strike in (200..=500).step_by(5) {
        for (kind, letter) in [(OptionKind::Call, 'C'), (OptionKind::Put, 'P')] {
            let osi = format!(
                "AAPL  {}{letter}{:08}",
                friday.strftime("%y%m%d"),
                strike * 1000
            );
            contracts.push(InstrumentAny::OptionContract(
                OptionContract::builder()
                    .instrument_id(InstrumentId::from(format!("{osi}.OPRA").as_str()))
                    .raw_symbol(Symbol::from(osi.as_str()))
                    .asset_class(AssetClass::Equity)
                    .exchange("OPRA".into())
                    .underlying("AAPL".into())
                    .option_kind(kind)
                    .strike_price(Price::new(f64::from(strike), 2))
                    .currency(Currency::USD())
                    .activation_ns(UnixNanos::default())
                    .expiration_ns(UnixNanos::from(expiration))
                    .price_precision(2)
                    .price_increment(Price::from("0.01"))
                    .multiplier(Quantity::from(1))
                    .lot_size(Quantity::from(1))
                    .ts_event(UnixNanos::from(now))
                    .ts_init(UnixNanos::from(now))
                    .build()
                    .unwrap(),
            ));
        }
    }
    contracts
}

/// Subscribes to the trades of each contract, recording each.
#[derive(Debug)]
struct Trades {
    core: DataActorCore,
    contracts: Vec<InstrumentId>,
    trades: Rc<RefCell<Vec<TradeTick>>>,
}

nautilus_actor!(Trades);

impl DataActor for Trades {
    fn on_start(&mut self) -> anyhow::Result<()> {
        for &contract in &self.contracts.clone() {
            self.subscribe_trades(contract, None, None);
        }
        Ok(())
    }

    fn on_trade(&mut self, trade: &TradeTick) -> anyhow::Result<()> {
        self.trades.borrow_mut().push(*trade);
        Ok(())
    }
}

#[tokio::test(flavor = "current_thread")]
async fn the_trades_of_aapl_s_options_of_the_week_stream_from_lse_for_a_minute() {
    if !opted_in("the_trades_of_aapl_s_options_of_the_week_stream_from_lse_for_a_minute") {
        return;
    }
    dotenvy::dotenv().ok();
    let started = Timestamp::now();
    let definitions = contracts_of_the_week(started);
    let contracts: Vec<InstrumentId> = definitions.iter().map(Instrument::id).collect();
    let config = LiveNodeConfig {
        environment: Environment::Live,
        trader_id: TraderId::from("LSE-OPTIONS-001"),
        load_state: false,
        save_state: false,
        ..Default::default()
    };
    let lse = lse::LseDataClientConfig::builder()
        .stream_ids(contracts.clone())
        .build();
    let mut node = LiveNodeBuilder::from_config(config)
        .unwrap()
        .with_name("LSE-OPTIONS")
        .with_delay_post_stop_secs(1)
        .add_data_client_with_routing(
            None,
            Box::new(lse::LseDataClientFactory::new()),
            Box::new(lse),
            RoutingConfig {
                default: false,
                venues: Some(vec!["OPRA".to_string()]),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    for definition in definitions {
        node.kernel()
            .cache()
            .borrow_mut()
            .add_instrument(definition)
            .unwrap();
    }
    let trades: Rc<RefCell<Vec<TradeTick>>> = Rc::default();
    node.add_actor(Trades {
        core: DataActorCore::new(DataActorConfig::default()),
        contracts: contracts.clone(),
        trades: Rc::clone(&trades),
    })
    .unwrap();
    let handle = node.handle();

    let (ran, ()) = tokio::join!(node.run(), async {
        tokio::time::sleep(STREAMED).await;
        handle.stop();
    });
    ran.unwrap();
    node.dispose();

    let trades = trades.borrow();
    let mut of_contract: BTreeMap<String, usize> = BTreeMap::new();
    for trade in trades.iter() {
        *of_contract
            .entry(trade.instrument_id.to_string())
            .or_default() += 1;
    }
    println!(
        "AAPL's options of the week: {} trades of {} contracts in {STREAMED:?}: {of_contract:?}",
        trades.len(),
        of_contract.len()
    );
    let first = trades.first().unwrap_or_else(|| {
        panic!(
            "no trade of AAPL's options of the week in {STREAMED:?}: a chain sends its \
             contracts' next trades alone, so run this while US options trade, 14:30 to 21:00 \
             UTC on a trading day"
        )
    });
    println!("first: {first:?}");
    let (from, to) = (
        UnixNanos::from(started - 1.minute()),
        UnixNanos::from(Timestamp::now() + 1.minute()),
    );
    assert!(
        trades
            .iter()
            .all(|trade| contracts.contains(&trade.instrument_id)
                && trade.price.precision == 2
                && trade.size.is_positive()
                && trade.aggressor_side == AggressorSide::NoAggressor
                && (from..=to).contains(&trade.ts_event)),
        "a trade of a contract not subscribed, off the cent, of no size, with a side or not \
         of the minute streamed"
    );
}
