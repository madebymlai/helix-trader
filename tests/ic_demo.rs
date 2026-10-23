//! End to end on IC Markets' cTrader demo: the session authenticates the
//! app and the demo account, and XAU/USD and EUR/USD load as currency
//! pairs. It needs the app's `IC_CLIENT_ID` and `IC_CLIENT_SECRET`, the
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
use nautilus_model::{
    identifiers::InstrumentId,
    instruments::InstrumentAny,
    types::{Currency, Quantity},
};

/// Whether the run is opted in: from the shell, before `.env` is read, so
/// that the file cannot opt in.
fn opted_in(test: &str) -> bool {
    let run = std::env::var("NAUTILUS_IC_RUN").is_ok_and(|value| value == "1");
    if !run {
        println!("{test}: skipped; set NAUTILUS_IC_RUN=1 to run it on IC's cTrader demo");
    }
    run
}

#[tokio::test(flavor = "multi_thread")]
async fn the_demo_account_loads_gold_and_euro_dollar_as_currency_pairs() {
    if !opted_in("the_demo_account_loads_gold_and_euro_dollar_as_currency_pairs") {
        return;
    }
    dotenvy::dotenv().ok();
    assert_eq!(
        std::env::var("IC_ENVIRONMENT").as_deref(),
        Ok("demo"),
        "the IC end to end test runs on the demo only: set IC_ENVIRONMENT=demo"
    );
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
