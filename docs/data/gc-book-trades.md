# GC book and trades

[Data index](README.md) · [Gold scalp study: GC](../research/thesis/gold-scalp-feature-theses.md#gc-book-and-trades)

## Information

GC is COMEX gold futures. The held Databento history has market-by-price depth up to ten levels and trades with aggressor side where provided. Each level aggregates displayed quantity and order count at a price; it is not individual orders, hidden liquidity, participant identity or the entire spot market. MBP-10 is a bounded near-book view, not the shape of all liquidity. Whether a trade print represents one complete aggressing order or one of its fills must be established from the feed before calling it a sweep.

The study owns contract coverage, roll exclusions and live access. In particular, the held historical data and IB's live interface differ: history's exchange timing, sequencing and aggressor field cannot be assumed to exist on IB live. No live parity is established.

## Relations

**Flow against price response:** over a completed window, compare signed aggressive lots `V` with the GC mid-price change `Δp`. A trailing fitted slope `k` gives residual `Δp − kV`: movement unusually small for the flow may describe absorption; unusually large movement may describe fragile liquidity. This is not identification of a passive participant or an aggressor's eventual loss. A persistent one-sided flow can coexist with little price movement for many reasons, including liquidity response, and flow and price are contemporaneous. The [study's absorption thesis](../research/thesis/gold-scalp-feature-theses.md#absorption-on-gc) is pending, not validated by that relation.

**Flow against displayed depth:** compare signed traded volume or best-quote order-flow imbalance with near-book lots over the same completed interval. The ratio describes flow relative to displayed liquidity; use a strictly positive, well-defined depth and a consistent unit. Trade imbalance includes executions only, while order-flow imbalance also accounts for changes in displayed limit orders and cancellations; they are different quantities. Thin depth can exaggerate ratios, and hidden and outside-ten-level liquidity are unobserved. A state linked to recent movement is not automatically useful beyond IC's own volatility.

**Depth or spread against its normal:** displayed near-book quantities, spread, or changes in depth relative to a trailing reference can describe liquidity conditions. Compare them with IC spread and realized volatility as risk or execution states. Depth is both affected by and potentially informative about market volatility; a correlation cannot establish direction or a causal mechanism. The ten-level boundary, rolls and differing feed coverage can change the measurement.

**Replenishment after consumption:** compare pre-trade displayed size with later displayed size at the same defined side and price or distance from mid. Time to a specified recovery describes displayed book resilience; a cap makes unresolved events censored, not genuinely refilled. A price move, cancellation and replacement, missing book update or multiple overlapping trades can imitate replenishment. [Refill speed](../research/thesis/gold-scalp-feature-theses.md#refill-speed-on-gc) is a proposed risk feature; its event count and live timing are unresolved.

## Timing and comparability

Reconstruct book state in sequence, respect resets and gaps, and mark intervals with incomplete state missing. Separate exchange event time from feed and local receipt time; no decision may use information received later. Define whether trade and book updates at equal timestamps are sequenced; otherwise a supposed pre-trade book can contain the trade's effects. Normalize on past eligible data and keep warm-up missing. Classifying historical and live aggressor side differently changes the feature. Cross-venue comparisons also need fresh quotes, correct contract and a roll rule; asynchronous quotes can manufacture a lead.

## Evidence

[Cont, Kukanov and Stoikov](https://academic.oup.com/jfec/article-abstract/12/1/47/816163) report a contemporaneous relation between best-quote order-flow imbalance, price change and depth in 50 US stocks (abstract read). It is not an out-of-sample forecast of GC or IC. Our [delay test](../research/thesis/gold-microstructure-delay.md) found the strong undelayed short-horizon direction result largely disappeared when GC inputs were delayed; the tested cells and period do not rule out every future hypothesis. In our [volatility study](../research/thesis/gold-volatility.md), GC depth and trade-intensity groups added to a broker-variance baseline in June 2026, but direction-free brackets lost under the tested fills and costs. The [touch study](../research/thesis/gold-touch-features.md) owns its separate result. [Databento MBP-10](https://databento.com/docs/schemas/mbp-10) documents the historical schema, not live IB equivalence.
