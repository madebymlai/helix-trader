# Broker XAUUSD quotes

[Data index](README.md) · [Gold scalp study: broker XAUUSD](../research/thesis/gold-scalp-feature-theses.md#broker-xauusd-quotes-ic-or-pepperstone)

## Information

Each bid/ask update describes the broker's displayed quote, not an exchange trade, traded size, participant identity or the whole gold market. The midpoint `m = (bid + ask)/2` is a reference for price changes; the bid and ask are the relevant sides for modeled execution. The spread `10,000 × (ask − bid)/m` is in basis points. Quote-update counts describe the feed's updates, not traded volume or independent orders. IC and Pepperstone are accepted; history, account and live-feed caveats are in the study.

## Relations

**Movement against its own recent scale:** a trailing return or high–low range divided by trailing volatility describes how unusual a move is relative to that broker's recent movement. This is a state or risk input; it does not establish a reversal or trend edge. A volatility denominator near zero and windows with insufficient valid quotes make the ratio undefined. A return and its normalizer share input history, so they do not provide independent sources of information.

**Spread against movement:** spread in basis points divided by a trailing volatility estimate at the same horizon describes execution friction relative to typical movement. A large ratio can make an otherwise accurate forecast untradeable. Volatility describes movement, not the cost of a specific fill; spread alone omits commission, slippage and market impact.

**Quote activity against its own history:** short-window update count relative to its trailing normal describes feed activity. It may condition volatility or liquidity hypotheses, but changes in aggregation, vendor throttling or reconnection can look like market activity. On the same broker, compare this to spread and realized volatility rather than calling it traded flow. A within-feed normal reduces level differences but does not establish history/live parity.

**Downside against upside variation:** for eligible intrawindow returns `r`, sum `r²` separately for negative and nonnegative returns. Their difference divided by their sum describes the recent asymmetry of squared movement; it is undefined when the sum is zero. It might condition risk, but a recent downside-heavy path does not itself predict future direction. Sampling frequency and quote noise affect both sums.

**Broker against broker:** compare simultaneous mids and spreads only when both feeds are fresh. Their difference describes broker-specific pricing and possible execution friction; it does not prove a tradable arbitrage, a common order book or which broker is right. Each broker's feature must be tested against that broker's own quotes and execution, as required by the study.

## Timing and comparability

Use bid and ask that belong to a valid quote at or before the decision; flag crossed, stale or missing quotes. Calculate trailing features on completed observations, and preserve gaps and warm-up as missing. History and live update frequency differ for IC, and Pepperstone live has not been checked; the study owns those measurements. A last quote can remain unchanged during a quiet market, so quote age and feed health are distinct questions. A zero update count over a verified continuous interval differs from an outage.

## Evidence

Our [gold ML study](../research/thesis/gold-ml.md) found realized-volatility and tick-rate features useful for movement-related labels but no tested direction trade cleared its cost bar; its results are specific to the stated samples and fills. The [volatility study](../research/thesis/gold-volatility.md) compares information groups against a realized-variance baseline; its improvements are not independent confirmation of every representation above. [Broker comparisons](../research/brokers/brokers.md) describe the feeds and spreads. Neither result establishes a new feature's incremental value.
