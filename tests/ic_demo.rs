//! End to end on IC Markets' cTrader demo: the session authenticates the
//! app and the demo account, XAU/USD and EUR/USD load as currency pairs,
//! and a day of XAU/USD's quotes fills a temporary catalog, in one fill
//! and resumed. It needs the app's `IC_CLIENT_ID` and `IC_CLIENT_SECRET`, the
//! demo account's `IC_ACCESS_TOKEN` and `IC_ACCOUNT_ID`, and
//! `IC_ENVIRONMENT=demo`, from the environment or `.env` (any other
//! environment fails the test before connecting).
//!
//! Gated as `okx_demo.rs` is: the test returns at once, saying it was
//! skipped, unless `NAUTILUS_IC_RUN=1` is set in the shell. It only reads
//! and places no order. Run it with:
//!
//! ```sh
//! NAUTILUS_IC_RUN=1 cargo test --test ic_demo -- --nocapture
//! ```

use helix_trader::data::IcHistory;
use jiff::Timestamp;
use nautilus_model::{
    data::{NautilusDataType, QuoteTick},
    identifiers::InstrumentId,
    instruments::InstrumentAny,
    types::{Currency, Quantity},
};
use nautilus_persistence::backend::parquet::catalog::ParquetDataCatalog;

/// Whether the run is opted in: from the shell, before `.env` is read, so
/// that the file cannot opt in.
fn opted_in(test: &str) -> bool {
    let run = std::env::var("NAUTILUS_IC_RUN").is_ok_and(|value| value == "1");
    if !run {
        println!("{test}: skipped; set NAUTILUS_IC_RUN=1 to run it on IC's cTrader demo");
    }
    run
}

/// Reads `.env` and refuses any environment but the demo.
fn demo_only() {
    dotenvy::dotenv().ok();
    assert_eq!(
        std::env::var("IC_ENVIRONMENT").as_deref(),
        Ok("demo"),
        "the IC end to end test runs on the demo only: set IC_ENVIRONMENT=demo"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_demo_account_loads_gold_and_euro_dollar_as_currency_pairs() {
    if !opted_in("the_demo_account_loads_gold_and_euro_dollar_as_currency_pairs") {
        return;
    }
    demo_only();
    let ids = [
        InstrumentId::from("XAUUSD.ICMARKETS"),
        InstrumentId::from("EURUSD.ICMARKETS"),
    ];
    let history = IcHistory::connect().await.unwrap();

    let instruments = history.instruments(&ids).await;
    history.disconnect().await;

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
}

const XAUUSD: &str = "XAUUSD.ICMARKETS";

/// The stored quotes of XAUUSD.
fn stored(catalog: &mut ParquetDataCatalog) -> Vec<QuoteTick> {
    catalog
        .quote_ticks(Some(vec![XAUUSD.to_owned()]), None, None)
        .unwrap()
}

/// A day of XAU/USD: Wednesday 21 October 2026 UTC, split for the resumed
/// fill at a time between two milliseconds.
#[tokio::test(flavor = "multi_thread")]
async fn a_day_of_gold_quotes_fills_the_same_in_one_fill_and_resumed() {
    if !opted_in("a_day_of_gold_quotes_fills_the_same_in_one_fill_and_resumed") {
        return;
    }
    demo_only();
    let start: Timestamp = "2026-10-21T00:00:00Z".parse().unwrap();
    let middle: Timestamp = "2026-10-21T12:00:00.000000500Z".parse().unwrap();
    let end: Timestamp = "2026-10-22T00:00:00Z".parse().unwrap();
    let series = [(NautilusDataType::QuoteTick, XAUUSD.to_owned())];
    let (one, resumed) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let mut one = ParquetDataCatalog::new(one.path(), None, None, None, None);
    let mut resumed = ParquetDataCatalog::new(resumed.path(), None, None, None, None);
    let history = IcHistory::connect().await.unwrap();

    let filled = async {
        history.fill(&mut one, &series, start, end).await?;
        history.fill(&mut resumed, &series, start, middle).await?;
        history.fill(&mut resumed, &series, start, end).await
    }
    .await;
    history.disconnect().await;

    filled.unwrap();
    let quotes = stored(&mut one);
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
            .all(|q| q.bid_size == Quantity::from(10_000) && q.ask_size == q.bid_size)
    );
    let interval = one
        .get_intervals(&NautilusDataType::QuoteTick.into(), Some(XAUUSD))
        .unwrap();
    assert_eq!(
        interval,
        [(
            start.as_nanosecond().try_into().unwrap(),
            end.as_nanosecond().try_into().unwrap()
        )]
    );
    assert!(
        stored(&mut resumed) == quotes,
        "the resumed fill stored other quotes"
    );
}
