# Other broker symbols

[Data index](README.md) · [Gold scalp study: other broker symbols](../research/thesis/gold-scalp-feature-theses.md#other-broker-symbols-ic-or-pepperstone)

## Information

The same broker can supply bid/ask quotes on silver, other gold currency crosses, FX, equity indices, energy and crypto. These are broker quotes, not traded volumes or consolidated market books. They can describe related market states alongside IC XAUUSD, but coverage varies by symbol and historical/live availability must be checked individually in the study. EUR/USD and a few other FX pairs are held; most accepted symbols are not yet held.

## Relations

**Common versus gold-specific movement:** compare gold returns with contemporaneous returns in a related market, or use the residual from a hedge coefficient fitted on past data. It describes how much of the observed gold move is not explained by that chosen market and horizon; it does not establish a profitable residual or a reason for it to revert. A coefficient estimated using the whole sample leaks future relationships. A changing coefficient can instead describe state, with substantial estimation noise.

**Currency consistency and decomposition:** XAU/EUR × EUR/USD implies XAU/USD when quote directions and units are correct. In synchronized log returns, the identity is `r_XAUUSD = r_XAUEUR + r_EURUSD`: the two legs and their covariance describe how gold's USD movement decomposes into non-USD gold and currency movement. Broker crosses may be mechanically synthesized from the same inputs, so an apparent relationship may add no independent information. Comparing the implied and direct prices describes cross-rate consistency and pricing friction, not an instruction to trade arbitrage or use lead-lag for direction. Crossed markets, stale legs and bid/ask spreads can create apparent discrepancies.

**Relative risk:** compare gold's trailing realized volatility with another market's own volatility normalized on a consistent horizon, or compare each with its own past normal. This describes whether gold is unusually active relative to a broader market. Because the assets have different inherent volatility scales, the raw ratio can be hard to interpret without a past-only reference. Correlation or co-movement may change during stress and is not evidence of causation.

## Timing and comparability

Align fresh quotes by actual receipt, not future-nearest timestamps; the slower leg limits availability. Use consistent quote sides and units for conversions, and preserve gaps and warm-up. Indices and CFDs may have different trading hours, update throttling, spread rules and composition. A cross-market comparison after one leg closes can be a staleness artifact. Live symbol access, history and demo/live parity differ by provider; check the study instead of assuming they match.

## Evidence

The [Chicago Fed analysis](https://www.chicagofed.org/publications/chicago-fed-letter/2021/464) finds relationships between gold and broader macro variables at annual, quarterly and daily frequencies, not five-minute predictive evidence. Our [EUR/USD directional lead study](../research/thesis/gold-dollar.md) failed in its tested setting. A same-window cross-market state is a different, unproven use, and a gold/silver ratio does not by itself establish a reversion edge; see [gold feature data](../research/gold/gold-feature-data.md).
