# Gold data for feature engineering

This directory describes what the data accepted for the [gold scalp study](../research/thesis/gold-scalp-feature-theses.md) can reveal, and relations between it and other accepted data. It is a research reference, not a list of approved features or a trading strategy. The study owns source decisions, current access and feature theses; the [ML guide](../research/ml-guide/README.md) explains feature quality and validation. For a new data kind, use [TEMPLATE.md](TEMPLATE.md).

A relation is a hypothesis about an information set, not evidence of a predictive edge. Our context is IC XAUUSD at minute horizons. The study excludes event, carry, trend and clock-based edges, and lead-lag as a source of direction. Market activity can vary by session and affect data availability; that is not itself a clock-based edge. Missing values stay missing.

## Data kinds

- [Broker XAUUSD quotes](broker-xauusd.md): price, spread, quote activity and the execution baseline.
- [Other broker symbols](broker-other-symbols.md): cross-market state measured against gold.
- [GC book and trades](gc-book-trades.md): liquidity, aggressive flow, price impact and replenishment.
- [OG trades](og-trades.md): option prices and trade activity in relation to the underlying and realized risk.
- [OG open interest and settlement](og-open-interest-settlement.md): a delayed strike and expiry map, not signed dealer positions.
- [PAXG trades](paxg-trades.md): token activity and its relation to gold, with USDT contamination.
- [LSE backdrop](lse-backdrop.md): volatility, dollar and rates context, subject to source and publication timing.

Availability is not ownership: several accepted sources are not held or wired live. OG top-of-book is held as a later upgrade, not an accepted current input; excluded data are not covered here. Consult the study for the current decision rather than treating this index as an access catalogue.
