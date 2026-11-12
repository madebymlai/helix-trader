//! End to end on a cTrader broker's demo: the session authenticates the
//! app and the demo account, XAU/USD and EUR/USD load as currency pairs at
//! the broker's margins, a day of XAU/USD's quotes fills a temporary
//! catalog through the node (the broker's data client), in one fill and
//! resumed, and the execution client reads the account: its state and its
//! startup reconciliation. It needs the login's `CTRADER_*` credential, and
//! the broker's account and environment, its demo (any other environment
//! fails the test before connecting), from the environment or `.env`.
//!
//! Gated by `demo_broker::opted_in`: the test returns at once, saying it
//! was skipped, unless `NAUTILUS_CTRADER_RUN` names the broker in the
//! shell. It only reads and places no order. Run it with:
//!
//! ```sh
//! NAUTILUS_CTRADER_RUN=pepperstone cargo test --test live ctrader_demo:: -- --nocapture
//! ```

use std::{cell::RefCell, rc::Rc, time::Duration};

use clap::{Args, Command, FromArgMatches};
use ctrader::{CTraderExecutionClientConfig, CTraderExecutionClientFactory, CTraderSession};
use helix_trader::{
    strategy::{DataRequirements, TickSeries},
    venues::{HistoryVenueArgs, fill_history},
};
use jiff::Timestamp;
use nautilus_common::{
    cache::Cache, clock::VirtualClock, factories::ExecutionClientFactory,
    live::runner::replace_exec_event_sender, messages::ExecutionEvent,
};
use nautilus_model::{
    accounts::AccountAny,
    data::{NautilusDataType, QuoteTick},
    enums::{AccountType, OmsType},
    identifiers::{InstrumentId, TraderId},
    instruments::InstrumentAny,
    types::{Currency, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

use crate::demo_broker::opted_in;

#[tokio::test(flavor = "multi_thread")]
async fn the_demo_account_loads_gold_and_euro_dollar_as_currency_pairs() {
    let Some(broker) = opted_in(
        "the_demo_account_loads_gold_and_euro_dollar_as_currency_pairs",
        module_path!(),
    ) else {
        return;
    };
    let ids = [broker.gold(), broker.eurusd()];
    let session = CTraderSession::connect(&broker.connection()).await.unwrap();

    let instruments = session.instruments(&ids).await;
    session.disconnect().await;

    let instruments = instruments.unwrap();
    let [
        InstrumentAny::CurrencyPair(xau),
        InstrumentAny::CurrencyPair(eur),
    ] = instruments.as_slice()
    else {
        panic!("not two currency pairs: {instruments:?}");
    };
    println!("{xau:?}\n{eur:?}");
    assert_eq!(xau.base_currency, Currency::from("XAU"));
    assert_eq!(xau.quote_currency, Currency::from("USD"));
    assert_eq!(xau.lot_size, Some(Quantity::from(100)));
    assert_eq!(eur.base_currency, Currency::from("EUR"));
    assert_eq!(eur.quote_currency, Currency::from("USD"));
    assert_eq!(eur.lot_size, Some(Quantity::from(100_000)));
    assert_eq!(eur.price_precision, 5);
    for (pair, margin) in [(xau, broker.gold_margin()), (eur, broker.eurusd_margin())] {
        assert_eq!((pair.margin_init, pair.margin_maint), (margin, margin));
    }
}

/// The stored quotes of `instrument_id`.
fn stored(catalog: &mut ParquetDataCatalog, instrument_id: InstrumentId) -> Vec<QuoteTick> {
    catalog
        .quote_ticks(Some(vec![instrument_id.to_string()]), None, None)
        .unwrap()
}

/// Fills the catalog in `dir` with the quotes of `instrument_id` over
/// `[start, end]` through the node, as fill-catalog does (`fill_history`):
/// the data client of its broker on the connection of the environment.
pub(crate) async fn fill_quotes_through_node(
    dir: &std::path::Path,
    instrument_id: InstrumentId,
    start: Timestamp,
    end: Timestamp,
) -> anyhow::Result<()> {
    let requirements = DataRequirements {
        bar_types: vec![],
        ticks: vec![TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id,
        }],
        warmup: jiff::SignedDuration::ZERO,
    };
    let matches = HistoryVenueArgs::augment_args(Command::new("fill-catalog"))
        .try_get_matches_from(["fill-catalog"])?;
    let venues = HistoryVenueArgs::from_arg_matches(&matches)?;
    fill_history(&requirements, start, end, &venues, dir).await
}

/// A day of XAU/USD: Wednesday 21 October 2026 UTC, split for the resumed
/// fill at a time between two milliseconds.
#[tokio::test(flavor = "current_thread")]
async fn a_day_of_gold_quotes_fills_the_same_in_one_fill_and_resumed() {
    let Some(broker) = opted_in(
        "a_day_of_gold_quotes_fills_the_same_in_one_fill_and_resumed",
        module_path!(),
    ) else {
        return;
    };
    let start: Timestamp = "2026-10-21T00:00:00Z".parse().unwrap();
    let middle: Timestamp = "2026-10-21T12:00:00.000000500Z".parse().unwrap();
    let end: Timestamp = "2026-10-22T00:00:00Z".parse().unwrap();
    let gold = broker.gold();
    let (one_dir, resumed_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());

    fill_quotes_through_node(one_dir.path(), gold, start, end)
        .await
        .unwrap();
    fill_quotes_through_node(resumed_dir.path(), gold, start, middle)
        .await
        .unwrap();
    fill_quotes_through_node(resumed_dir.path(), gold, start, end)
        .await
        .unwrap();

    let mut one = ParquetDataCatalog::new(one_dir.path(), None, None, None, None);
    let mut resumed = ParquetDataCatalog::new(resumed_dir.path(), None, None, None, None);
    let quotes = stored(&mut one, gold);
    println!(
        "{} quotes, first {:?}, last {:?}",
        quotes.len(),
        quotes.first(),
        quotes.last()
    );
    assert!(quotes.len() > 10_000, "{} quotes", quotes.len());
    assert!(
        quotes.windows(2).all(|w| w[0].ts_init < w[1].ts_init),
        "not one quote per time, in order"
    );
    assert!(quotes.iter().all(|q| q.bid_price <= q.ask_price));
    assert!(
        quotes
            .iter()
            .all(|q| { q.ts_event == q.ts_init && q.ts_init.as_u64() % 1_000_000 == 0 })
    );
    assert!(
        quotes
            .iter()
            .all(|q| q.bid_size == broker.gold_quote_size() && q.ask_size == q.bid_size)
    );
    let interval = one
        .get_intervals(
            &NautilusDataType::QuoteTick.into(),
            Some(gold.to_string().as_str()),
        )
        .unwrap();
    assert_eq!(
        interval,
        [(
            start.as_nanosecond().try_into().unwrap(),
            end.as_nanosecond().try_into().unwrap()
        )]
    );
    assert!(
        stored(&mut resumed, gold) == quotes,
        "the resumed fill stored other quotes"
    );
}

/// The execution client on the demo account, as the node drives it:
/// started, connected (the node registering the account state it
/// publishes), then the startup mass status. Read only: no order.
#[tokio::test(flavor = "current_thread")]
async fn the_execution_client_reads_the_hedged_demo_account_and_reconciles_it() {
    let Some(broker) = opted_in(
        "the_execution_client_reads_the_hedged_demo_account_and_reconciles_it",
        module_path!(),
    ) else {
        return;
    };
    let (tx, mut events) = tokio::sync::mpsc::unbounded_channel();
    replace_exec_event_sender(tx);
    let cache = Rc::new(RefCell::new(Cache::default()));
    let config = CTraderExecutionClientConfig::new(
        broker.connection(),
        vec![broker.gold(), broker.eurusd()],
    );
    let factory = CTraderExecutionClientFactory::new(&config);
    let mut client = factory
        .create(
            TraderId::from("HELIX-001"),
            factory.name(),
            &config,
            cache.clone().into(),
            Rc::new(RefCell::new(VirtualClock::new())),
        )
        .unwrap();
    client.start().unwrap();

    let register = async {
        let event = tokio::time::timeout(Duration::from_secs(30), events.recv())
            .await
            .expect("no account state within 30 s")
            .unwrap();
        let ExecutionEvent::Account(state) = event else {
            panic!("not an account state: {event:?}");
        };
        cache
            .borrow_mut()
            .add_account(AccountAny::from_events(std::slice::from_ref(&state)).unwrap())
            .unwrap();
        state
    };
    let (connected, state) = tokio::join!(client.connect(), register);
    connected.unwrap();
    // A week's lookback, so the deal list is asked over a real window.
    let mass_status = client.generate_mass_status(Some(7 * 24 * 60)).await;
    client.disconnect().await.unwrap();

    let mass_status = mass_status.unwrap().unwrap();
    println!(
        "account: {:?} base {:?}, balances {:?}, margins {:?}",
        state.account_type, state.base_currency, state.balances, state.margins
    );
    println!(
        "mass status: {} positions, {} orders, {} fill groups, window from {:?}, complete {}",
        mass_status.position_reports().len(),
        mass_status.order_reports().len(),
        mass_status.fill_reports().len(),
        mass_status.lookback_start(),
        mass_status.reports_complete()
    );
    assert_eq!(client.oms_type(), OmsType::Hedging);
    assert_eq!(state.account_id, broker.account_id());
    assert_eq!(state.account_type, AccountType::Margin);
    let currency = state.base_currency.expect("no base currency");
    let [balance] = state.balances.as_slice() else {
        panic!("not one balance: {:?}", state.balances);
    };
    assert_eq!(balance.currency, currency);
    assert!(balance.total.as_f64() > 0.0, "{balance:?}");
    assert!(mass_status.reports_complete());
    // The demo account holds nothing open: each position or order there
    // would be one report here, beside the orders closed over the window.
    assert!(mass_status.position_reports().is_empty());
    assert!(
        mass_status
            .order_reports()
            .values()
            .all(|r| r.order_status.is_closed()),
        "{:?}",
        mass_status.order_reports()
    );
}
