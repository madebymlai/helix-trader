# PAXG spot trades

[Data index](README.md) · [Gold scalp study: PAXG](../research/thesis/gold-scalp-feature-theses.md#paxg-and-xaut)

## Information

Binance spot PAXG/USDT aggregate trades provide price, quantity, time and a maker-side flag from which taker side can be derived with the exchange's convention. [Paxos](https://www.paxos.com/paxgold) says each PAXG is backed by one fine troy ounce of allocated gold. A token trade is nevertheless a crypto exchange execution in USDT, not an executable IC quote or a direct observation of physical-gold demand. Trades show executed flow, not standing depth, cancellations or the identities and motives of traders. Historical books are not held. Availability and the distinction between accepted PAXG and excluded perps and XAUT are owned by the study.

## Relations

**Token premium against its own normal:** `10,000 × log(PAXG_USDT × USDT_USD / IC_mid_USD)` is a USD-adjusted token-versus-broker premium in basis points, when an eligible USDT/USD rate is actually available. A deviation from a trailing normal describes unusual relative pricing, potentially token-specific demand or dislocation, not necessarily subsequent gold direction. Without the conversion, the raw premium also contains the USDT/USD rate; explicitly label it unadjusted rather than treating USDT as USD. Mint/redemption frictions, venue liquidity, stale last trades and broker spreads also affect it. This relation is a state hypothesis, not a lead-lag trade.

**Flow against flow:** compare PAXG signed taker quantity over a completed window with [GC trade flow](gc-book-trades.md), after converting contracts to ounces and defining the sign consistently. Agreement or divergence describes how executions differ across two venues; it is not a direct measure of the same traders or a causal explanation. Missing signs, small PAXG trade counts and different venues' liquidity can dominate the comparison. GC history limits their common period.

**Activity against activity:** compare PAXG traded ounces or notional to its own trailing normal, and the same for GC; their difference or ratio describes relative activity rather than a shared absolute scale. A quiet PAXG window is a market observation only if feed coverage is known; an outage is missing, not zero. Token activity could condition a risk model, but adds information only if it improves the broker-only baseline in the study's screen.

## Timing and comparability

The last trade is not a mid-price. Retain its age and exclude it when stale; a quiet market may leave no eligible price or signed flow. Join all sources by actual availability and compare intervals that end before the decision. USDT/USD needs its own venue, clock and staleness policy. Normalize with past data only. Binance's [public-data documentation](https://github.com/binance/binance-public-data) says spot archive timestamps are in microseconds from 2025-01-01 and its aggregate-trade field says whether the buyer was the maker; map `true` to seller-initiated and `false` to buyer-initiated. Verify older timestamp units and live conventions before joining; historical/live equivalence is unmeasured. Token-market prices during an IC closure cannot be compared to a stale IC quote as a live premium.

## Evidence

Paxos's product description supports the token's gold-backing definition, not its predictive value. The [study's PAXG entry](../research/thesis/gold-scalp-feature-theses.md#paxg-and-xaut) records sampled activity and proposed relations; no PAXG model result is claimed. The project's [crypto-perpetual study](../research/thesis/gold-perps.md) concerns a different instrument and cannot establish a PAXG result.
