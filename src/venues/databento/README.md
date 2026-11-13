# Databento definitions

`fill-catalog` fills the definitions of `GLBX` instruments from Databento's `GLBX.MDP3` dataset through the [node fill](../../catalog/README.md), each request naming Databento's client, Nautilus' data engine writing each back to the catalog; their history is [LSE](../lse/README.md)'s, and [IB](../ib/README.md) serves their live data and trades them (see the [routing table](../mod.rs)). A contract's definition is asked in LSE's fill of its series, before them, over the span of what the catalog lacks of them; once the catalog holds it, Nautilus answers later requests from it and Databento is not asked. Set `DATABENTO_API_KEY` in the environment or `.env`; it is read, and the dataset's end checked, whenever a `GLBX` series lacks something.

Before starting the node, the fill checks the dataset's available end. The dataset lags real time, so an end beyond its available range is refused.

Dated contracts are stored under their own IDs, in Nautilus' two-digit year (`GCZ26.GLBX`), Databento's default and LSE's; rolled roots fill their dated contracts over their respective slices, not the root itself. Backtest and live IDs must be chosen deliberately: Databento's default venue differs from IB's; see [instrument identity](../../catalog/README.md).

Offline tests cover the dataset's end and the requests Databento takes in `src/venues/databento`; a `GLBX` fill against mock Databento and LSE peers, Databento's peer replaying its captured definition of GCZ6 (`testdata/`, see `testing.rs`), is in `src/venues/lse`.
