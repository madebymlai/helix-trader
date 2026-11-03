# Data and catalog fills

`fill-catalog` loads the instruments and source series required by the strategy into a Nautilus Parquet catalog. Strategy requirements and bar authoring belong to [strategy](../strategy/README.md); contract transitions belong to [rolls](../rolls/README.md). The backtest consumes the catalog as described in [backtest](../backtest/README.md).

The instrument venue selects the source, in the [routing table](../venues/mod.rs): [OKX](../venues/okx/README.md) for `OKX`, [Databento](../venues/databento/README.md) for `GLBX`, [IC Markets](ic/README.md) for `ICMARKETS`, and [IB](../venues/ib/README.md) for other venues. A run may use several sources. Definitions and the source series, not the internally aggregated strategy bars, are stored.

## Fill paths

Databento and IC answer historical requests through a Nautilus node. The node requests missing intervals sequentially and writes completed responses back. It waits for each answer with no deadline, as Nautilus requests have none: both clients answer every request, a failed one with its failure, and a client that never answered would leave the fill waiting until interrupted. IB and OKX use the direct `fill_catalog` writer: their clients return payloads without the completed response envelope required for node writeback (nautilus_trader#5472 and #5473). A failed request stops the fill; previously written data remains.

Node requests are cut into source-sized, epoch-aligned inclusive windows, ending 1 ns before the next. A definition precedes its series. The node verifies coverage for windows that returned rows because Nautilus can log a writeback failure without reporting it to the requester (nautilus_trader#5474). An empty answer may record coverage when adjacent to stored data; an isolated empty window records nothing and is requested again. A wholly empty first fill is not an error.

The direct writer resumes each series 1 ns after its stored end, even if the requested start is later, to avoid a gap. It refuses a start before the stored range: use a new catalog to extend backwards. An empty first response is an error; an empty resumed response writes no coverage and is requested again. Data is sorted and deduplicated by `ts_init` before writing. Windows are inclusive, and overlapping catalog intervals are invalid.

**Coverage is not completeness.** A nonempty answer can mark the entire requested window covered even if the source had only part of it. Check source history depth and availability before relying on a catalog; a rerun will not necessarily retrieve missing observations inside covered windows.

## Instrument identity

The source, catalog, strategy and live broker must agree on instrument IDs. IB and Databento use different default venues for the same contract; matching them requires explicit venue configuration. Changing source or venue requires matching config IDs and a refilled catalog. Dated futures symbols omit the decade, so the same ID can represent contracts ten years apart; a run's cache cannot hold both definitions. Use separate catalogs per decade or accept that rewriting symbols breaks the live-broker match.

## Tests

Catalog fill tests are in `src/data` and `tests/node_fill`; venue-specific test gates are in the linked venue READMEs. They do not establish completeness of a remote source.
