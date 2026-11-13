# Databento history

`fill-catalog` fills the history of `GLBX` instruments from Databento's `GLBX.MDP3` dataset through the [node fill](../../catalog/README.md); [IB](../ib/README.md) trades them live (see the [routing table](../mod.rs)). Set `DATABENTO_API_KEY` in the environment or `.env`. The declared series can include MBP-10 depth, MBO deltas, trades or quotes; custom historical data is refused.

Before starting the node, the fill checks the dataset's available end. The dataset lags real time, so an end beyond its available range is refused.

Requests are split into day-long windows relative to the request start, not aligned to UTC midnight. The client collects a full response before returning it, and catalog writing copies data again; a busy day of depth can require many gigabytes of memory. Smaller windows would reduce peak memory but increase request latency. See [shared fill semantics](../../catalog/README.md) for coverage and resume.

Continuous symbols are stored under the continuous ID; dated contracts are stored under their own IDs. Rolled roots fill their dated contracts over their respective slices, not the root itself. Backtest and live IDs must be chosen deliberately: Databento's default venue differs from IB's; see [instrument identity](../../catalog/README.md).

Offline tests cover request preparation and the node-fill seam in `src/venues/databento` and `tests/node_fill`.
