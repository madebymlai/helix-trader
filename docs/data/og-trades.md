# OG option trades

[Data index](README.md) · [Gold scalp study: OG trades](../research/thesis/gold-scalp-feature-theses.md#og-trades)

## Information

OG prints give traded option price and quantity, strike, expiry, call/put and aggressor side where supplied, plus contract definitions. They are transactions, not a continuously quoted option surface, the full order book, participant identities or open interest. A buyer-initiated trade is not necessarily a new long position, and seller initiation does not establish a dealer's inventory. London and US coverage, historical/live gaps and accepted scope are in the study; top-of-book history is a later upgrade, not an available substitute.

## Relations

**Implied volatility against a forecast:** invert the price of an eligible near-the-money option using its strike, expiry, the matching GC futures price at the option trade, rate and an explicitly chosen pricing model. Compare the resulting annualized IV with a forecast of realized volatility over the *same maturity*, in the same units. This measures option price relative to a past-data risk forecast; it is a risk-state hypothesis for shorter horizons, not a direct five-minute forecast. The difference includes volatility risk premium, model error and transaction-price noise. Annualized variance and annualized volatility are not interchangeable: take the square root of a variance forecast before subtracting it from IV.

**Trade activity against its normal:** executed volume or number of prints in a defined strike/expiry region, relative to its own trailing normal, describes unusual participation. Compare with GC or IC volatility as a state, not as proof of informed trading. Instrument selection, time to expiry and the fact that several individual fills can form one underlying order affect counts; lack of a print can mean illiquidity or missing feed.

**Surface relationships:** same-expiry ATM versus out-of-the-money IV and near versus farther expiry describe the shape of option prices. They require enough fresh, comparable contracts and a consistent strike/expiry rule. A trade-derived skew is particularly sensitive to which strikes happened to trade; the study dropped skew under the available data. Describe this as a limitation of the source, not a new accepted feature.

## Timing and comparability

An option trade's IV needs the underlying *at that trade*, not the underlying at the later decision. Use only trades and underlying observations actually received before the decision, with separate age limits. Keep no-trade, invalid-price and unsolvable-IV cases missing; do not silently carry stale trades forward. Side-of-spread execution, changing strikes and expiry rolls can create apparent IV moves. Black-76 is a modeling approximation; exercise terms and input choices can matter. With only trades, a reliable contemporaneous bid/ask spread cannot be inferred. The accepted London/US coverage is partial, and live availability is not yet established.

## Evidence

[Neely (2004)](https://files.stlouisfed.org/files/htdocs/wp/2003/2003-018.pdf), gold futures options and spot returns over 1987–1998, finds statistical incremental information in econometric volatility forecasts beyond IV, but no consistent improvement in its delta-hedging exercise. This addresses option-expiry volatility and a particular hedging task, not IC's next five minutes; the paper notes possible look-ahead from full-sample parameters in one model. The project's [OG skew study](../research/thesis/og-skew.md) reports the trade-derived change's limitations; [our GVZ study](../research/thesis/gvz-minute.md) concerns GLD implied volatility, not OG prints. The study owns acceptance and screening decisions.
