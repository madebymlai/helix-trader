# OG open interest and settlement

[Data index](README.md) · [Gold scalp study: OG statistics](../research/thesis/gold-scalp-feature-theses.md#og-open-interest-and-settlement-price)

## Information

For each OG strike and expiry, open interest counts outstanding contracts; settlement is an official daily price, not necessarily a transaction at that price. These are slow observations of a cross-section, not intraday positioning updates. Open interest counts both sides of each contract: it does not identify which side dealers or customers hold, the hedge already in place, or the net position across venues. [Databento's statistics schema](https://databento.com/docs/schemas/statistics) distinguishes settlement flags, the reference time and received time; the generic schema lists volatility and delta types, but its GLBX coverage table does not include them. They must be calculated or sourced separately for OG.

## Relations

**Strike concentration against current price:** published open interest at each strike can be grouped by distance from the current underlying and expiry. This describes where outstanding contracts are concentrated as price moves; it does not reveal dealer sign or prove a support/resistance level. Use the contract's matching underlying futures, definitions and multiplier; strike clustering can reflect old positions or expiry selection. Expiry and exercise can change the map without new trading demand.

**Gamma-weighted open interest against movement:** a model can estimate each contract's gamma from settlement-derived implied volatility, current matching futures price, remaining maturity and contract terms, then weight by open interest. This is a sensitivity proxy: it says where options *could* require more delta adjustment, not that dealers actually hold a known net gamma. A signed aggregate additionally assumes who is long and short; test alternative signs and model assumptions rather than presenting a call/put convention as observed inventory. A change in this proxy can condition a future-volatility hypothesis, not prove a direction.

**Settlement-implied volatility against realized-risk forecast:** derive IV using the applicable option model and matching underlying at the settlement's time, then compare it in common annualized volatility units with a past-only forecast over the same maturity. This describes option-price risk relative to a historical forecast; it mixes expected variance, risk premium, model assumptions and stale settlement. CME's [option settlement procedures](https://cmegroupclientsite.atlassian.net/wiki/spaces/EPICSANDBOX/pages/457412876/CME+CBOT+NYMEX+COMEX+Daily+Option+Settlement+Procedures) allow model-based settlement for some contracts, so a settlement can reflect the exchange's procedure rather than a fresh trade. It is distinct from a direct [OG trade-based IV](og-trades.md) and does not become intraday IV merely by repricing with today's gold price.

## Timing and comparability

`ts_ref` identifies the trading period, not when the value was known. Use the publication/receipt time of each observation and distinguish preliminary and final settlement. [CME's settlement documentation](https://www.cmegroup.com/trading/about-settlements.html) says preliminary and final pre-clearing option files contain the previous trading day's open interest, whereas the final post-clearing COMEX file contains the latest; its precise availability time remains unchecked. Do not backdate a revised daily value or use a current file as if its final contents were available earlier. Require valid strike/expiry definitions and a matching futures contract; keep stale, missing, implausible or model-unsolvable values missing. OI and settlement history are accepted but not held or wired, so acquisition and actual publish-time history remain prerequisites.

## Evidence

The [study's dealer-gamma feature thesis](../research/thesis/gold-scalp-feature-theses.md#dealer-gamma) is a pending hypothesis, not a screened result. Its assumed dealer sign is unverified for gold. [Databento's schema](https://databento.com/docs/schemas/statistics) documents what the fields mean; [CME's open-interest introduction](https://www.cmegroup.com/education/courses/introduction-to-futures/open-interest.html) distinguishes open interest from traded volume. Neither source establishes five-minute IC predictability.
