# LSE market backdrop

[Data index](README.md) · [Gold scalp study: LSE data](../research/thesis/gold-scalp-feature-theses.md#lse-data)

## Information

The accepted LSE data group supplies a possible market backdrop beyond broker gold: dollar and equity-volatility indices, yields and other instruments listed in the study. These measure different markets, not gold-specific order flow. LSE history and live availability differ by series; upstream provenance and delivery of the reference collections are not established. IB or FRED can backfill some series but are different providers and sometimes different instruments or frequencies. The study's exclusions still apply: COT is out despite appearing in the LSE catalogue, and GVZ has its own out decision.

## Relations

**Dollar state against gold:** compare completed dollar-index changes or volatility with gold's concurrent changes or volatility, with a past-only rolling correlation or residual as a state. The relation describes co-movement and diversification of shocks, not which asset will lead the next move. DXY and EUR/USD are not equivalent: EUR/USD is one currency pair rather than the dollar index basket. Their changing sign, asynchronous sessions and proxy substitution matter.

**Rates against gold:** a change in real yield or a past-fitted gold sensitivity to yield changes can describe a broad opportunity-cost backdrop. Nominal yield, real yield and breakeven inflation are different quantities; matching maturity and units are required. Subtracting a slow daily real-yield observation from an intraday nominal yield without a clock and maturity convention does not create a contemporaneous inflation measure. A slow state repeated on many minute rows is still one underlying observation, not new independent information each minute.

**Equity risk against gold activity:** a fresh equity-volatility index or index movement may condition gold's own realized volatility. VIX is equity-option implied volatility, not gold implied volatility, and it is not evidence of the direction of the next gold move. Compare it to a baseline containing gold's own realized variance; otherwise it may only proxy the volatility already visible on IC.

## Timing and comparability

As-of join by actual delivery time, not the reference date or bar opening stamp; enforce a per-series freshness limit and keep unavailable data missing. Market sessions differ, as do live feed and historical coverage. A daily FRED backfill is not a historical intraday quote, and a yield observation may be revised. Preserve source, vintage, units and maturity; a splice across providers or from an index to a proxy changes the feature. Actual live permissions and update timing must be checked before any live claim.

## Evidence

The [Chicago Fed's gold study](https://www.chicagofed.org/publications/chicago-fed-letter/2021/464), read in full, reports associations of gold with real rates and inflation using annual, quarterly and daily data; its daily regression is contemporaneous and has low explanatory power. It cautions that association does not establish causality. It is not evidence for five-minute prediction. Our [EUR/USD–gold test](../research/thesis/gold-dollar.md) addresses a directional lead and failed under its own conditions; that does not validate a backdrop interaction. Source-specific coverage and accepted backfills remain in the study.
