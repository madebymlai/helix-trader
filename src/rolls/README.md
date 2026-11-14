# Futures rolls

The calendar maps futures roots to dated contracts. Nautilus' data sources and venues use dated contracts; its continuous bars take a caller-supplied transition table. A root is an instrument listed in `calendar.toml`; every other instrument, including a dated contract or a vendor's continuous symbol, is plain and maps to itself. No source serves a vendor's continuous symbol now: Databento's `GC.v.0.GLBX` filled from Databento until LSE took `GLBX`'s history, and LSE, which serves dated contracts alone, refuses it, so read the root, `GC.GLBX`, whose contracts the calendar rolls. Configs need no roll flag.

## Boundaries and subscriptions

A transition table covers `[start, end]`: transitions are after start and at or before end. A roll exactly at start is already done. At a transition the post contract is in front; catalog slices include both ends, with the pre slice ending one nanosecond before the transition. A contract declared both directly and through a root has its slices merged to avoid duplicate replay. Roll prices are not part of the table.

The strategy's `Roller` subscribes tick series on the front contract and changes subscriptions at each roll. It alerts at the last nanosecond of the pre slice: backtest data at a timestamp precedes timers at that timestamp, so an alert at the transition would miss the first post-contract tick. The strategy receives the transition and must reset state such as indicators. Shared subscriptions remain when another instrument still needs the pre contract. A live roller requests the next post contract in advance; replay registers its contracts up front. Bars use Nautilus' continuous-bar machinery instead of the roller. See [strategy authoring](../strategy/README.md) and [live operations](../live/README.md).

## Continuous bars

Root bars must be internal LAST bars built from trades, not composite bars. Transitions are priced using the pre contract's last trade and post contract's first trade in their respective slices, not simultaneous prices. The run fails if a transition lacks either trade, or if the span has no transition; in the latter case declare the dated contract instead. Live refuses root bars because a future roll's prices are not known at subscription time.

Choose adjustment deliberately: backward spread and ratio modes anchor on the last contract, so earlier price levels incorporate later roll prices (lookahead); forward modes anchor on the first and use past rolls. At a transition, the post-contract trade stamped exactly then reaches no bar because the subscription-switch timer fires after data at that timestamp. A mid-bar roll leaves the pre-roll portion at its old adjustment.

## Calendar

`calendar.toml` is embedded at build time. Each product declares the root, contract symbol template, liquid month codes, roll rule, business days before that rule and UTC roll time. The template supports `{month}`, `{y}`, `{yy}` and `{yyyy}`. The current roll rule counts back from the last business day before the delivery month; see the calendar for product-specific values.

Business days mean weekdays, not exchange holidays. A holiday on the rule day or a counted day can shift the actual roll closer to the rule day than intended. Check exchange calendars and liquidity before using or extending a product's schedule; the deterministic rule is not a guarantee of a volume-based roll. Calendar parsing validates product entries and the unit tests parse the embedded file.
