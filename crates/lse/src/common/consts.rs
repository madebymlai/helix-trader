//! LSE's names and addresses, and the adapter's.

use std::{num::NonZeroU32, sync::LazyLock};

use nautilus_model::identifiers::Venue;

/// The venue of LSE's own derived series, one instrument each: `LSEDGE`,
/// not `LSE`, which is IB's London Stock Exchange.
pub static LSEDGE_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new("LSEDGE"));

/// The variable holding the LSE key, for LSE's own markets.
pub const LSE_API_KEY: &str = "LSE_API_KEY";

/// The variable holding a CME Group plan's key, for CME Globex.
pub const LSE_CME_API_KEY: &str = "LSE_CME_API_KEY";

/// The name of the data client, and of its factory.
pub(crate) const LSE_CLIENT: &str = "LSE";

/// LSE's REST API, every call a GET under `/v3`.
pub(crate) const LSE_HTTP_BASE_URL: &str = "https://download.londonstrategicedge.com";

/// The calls a minute of LSE's free plan, the fewest of any plan (LSE's
/// docs, "Calls a minute").
pub(crate) const FREE_CALLS_PER_MINUTE: NonZeroU32 = NonZeroU32::new(60).expect("not zero");

/// The calls a minute of CME Group Lite, the fewest of the CME Group plans
/// (LSE's docs, "Calls a minute": Lite, Lite+, Standard, Standard+ and
/// Ultimate allow 200, 300, 500, 1,000 and 5,000).
pub(crate) const CME_LITE_CALLS_PER_MINUTE: NonZeroU32 = NonZeroU32::new(200).expect("not zero");

/// LSE's name of CME Globex, its market's folder.
pub(crate) const CME_MARKET: &str = "cme";

/// LSE's exchange markets: real listings, which take the ids Databento
/// gives them, not `LSEDGE` ones. `opra` is a second name for
/// `us_options`. Every other market is derived.
pub(crate) const EXCHANGE_MARKETS: &[&str] = &["cme", "nasdaq", "us_options", "opra"];
