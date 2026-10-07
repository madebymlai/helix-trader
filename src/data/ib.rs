//! Interactive Brokers history: instruments and bars for `fill_catalog`.
//!
//! Uses `HistoricalInteractiveBrokersClient`
//! (`nautilus_interactive_brokers::historical`), which needs no node: it
//! connects to IB Gateway or TWS, returns `Vec<InstrumentAny>` from
//! `request_instruments` and `Vec<Bar>` from `request_bars`. Another source
//! is another file next to this one.

use anyhow::{Context, ensure};
use jiff::Timestamp;
use nautilus_interactive_brokers::{
    config::InteractiveBrokersDataClientConfig, historical::HistoricalInteractiveBrokersClient,
};
use nautilus_model::{
    data::{Bar, BarType},
    enums::{AggregationSource, BarAggregation, PriceType},
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};

/// Where IB Gateway or TWS listens, and the client id of this process.
#[derive(Debug, Clone)]
pub struct IbGateway {
    pub host: String,
    pub port: u16,
    pub client_id: i32,
}

/// A connection to IB for history: instruments and bars, no node.
pub struct IbHistory {
    client: HistoricalInteractiveBrokersClient,
    use_rth: bool,
    timeout_secs: u64,
}

impl IbHistory {
    /// Connects to `gateway`, once IB can serve every bar type of
    /// `bar_types` over a window ending at `end`: checked before any
    /// connection attempt.
    ///
    /// # Errors
    ///
    /// Returns an error if a bar type is not one IB serves, if `end` is
    /// within the longest bar of `now`, or if the connection fails.
    pub async fn connect(
        gateway: IbGateway,
        bar_types: &[BarType],
        end: Timestamp,
        now: Timestamp,
    ) -> anyhow::Result<Self> {
        for bar_type in bar_types {
            bar_spec(bar_type)?;
        }
        ensure_bars_closed(bar_types, end, now)?;
        let config = data_client_config(gateway);
        let use_rth = config.use_regular_trading_hours;
        let timeout_secs = config.request_timeout;
        let address = format!("{}:{}", config.host, config.port);
        let client = HistoricalInteractiveBrokersClient::connect(config)
            .await
            .with_context(|| format!("connecting to IB at {address}"))?;
        Ok(Self {
            client,
            use_rth,
            timeout_secs,
        })
    }

    /// The instruments of `ids`, as IB defines them.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or any id is not returned
    /// (IB answers an unknown or expired id with nothing).
    pub async fn instruments(&self, ids: &[InstrumentId]) -> anyhow::Result<Vec<InstrumentAny>> {
        let instruments = self
            .client
            .request_instruments(Some(ids.to_vec()), None)
            .await
            .context("requesting instruments from IB")?;
        ensure_all_loaded(ids, &instruments)?;
        Ok(instruments)
    }

    /// The bars of `bar_type` over `[start, end]`, with the regular-hours
    /// setting of the live data client.
    ///
    /// # Errors
    ///
    /// Returns an error if the bar type is not one IB serves, or if any
    /// request fails or times out.
    pub async fn bars(
        &self,
        bar_type: BarType,
        start: Timestamp,
        end: Timestamp,
    ) -> anyhow::Result<Vec<Bar>> {
        let spec = bar_spec(&bar_type)?;
        self.client
            .request_bars(
                vec![&spec],
                end,
                Some(start),
                None,
                None,
                Some(vec![bar_type.instrument_id()]),
                self.use_rth,
                self.timeout_secs,
            )
            .await
    }
}

/// The IB data client config, the one the live data client uses.
///
/// Adapter defaults otherwise: `Simplified` symbology (ids like
/// `ESM4.CME`), regular trading hours only. Never sets
/// `convert_exchange_to_mic_venue`: it makes the venue of a CME contract
/// change between processes.
fn data_client_config(gateway: IbGateway) -> InteractiveBrokersDataClientConfig {
    InteractiveBrokersDataClientConfig {
        host: gateway.host,
        port: gateway.port,
        client_id: gateway.client_id,
        ..Default::default()
    }
}

/// The IB bar spec string of `bar_type`, for example `"1-HOUR-LAST"`.
///
/// Only `EXTERNAL` time bars at a size IB serves, priced on last, bid, ask
/// or mid. The historical client does not parse `MONTH`.
fn bar_spec(bar_type: &BarType) -> anyhow::Result<String> {
    let spec = bar_type.spec();
    let step = spec.step.get();
    let size_served = match spec.aggregation {
        BarAggregation::Second => matches!(step, 1 | 5 | 15 | 30),
        BarAggregation::Minute => matches!(step, 1 | 2 | 3 | 5 | 10 | 15 | 20 | 30),
        BarAggregation::Hour => matches!(step, 1 | 2 | 3 | 4 | 8),
        BarAggregation::Day | BarAggregation::Week => step == 1,
        _ => false,
    };
    let price_served = matches!(
        spec.price_type,
        PriceType::Last | PriceType::Bid | PriceType::Ask | PriceType::Mid
    );
    ensure!(
        size_served && price_served && bar_type.aggregation_source() == AggregationSource::External,
        "IB cannot serve bar type {bar_type}: only EXTERNAL time bars of \
         1/5/15/30 SECOND, 1/2/3/5/10/15/20/30 MINUTE, 1/2/3/4/8 HOUR, 1 DAY or 1 WEEK, \
         priced LAST, BID, ASK or MID"
    );
    Ok(format!("{step}-{}-{}", spec.aggregation, spec.price_type))
}

/// Fails if `end` is later than `now` minus the longest bar of `bar_types`.
///
/// IB answers a window ending within one bar of now with the bar still in
/// progress; stored, it would never be replaced. Takes bar types `bar_spec`
/// accepted.
fn ensure_bars_closed(bar_types: &[BarType], end: Timestamp, now: Timestamp) -> anyhow::Result<()> {
    let longest = bar_types
        .iter()
        .map(|bar_type| bar_type.spec().timedelta())
        .max()
        .unwrap_or_default();
    let latest = now.checked_sub(longest)?;
    ensure!(
        end <= latest,
        "end {end} is within one bar of now: IB would return the bar still in progress; \
         use an end at or before {latest}"
    );
    Ok(())
}

/// Fails naming every id of `requested` missing from `loaded`.
fn ensure_all_loaded(requested: &[InstrumentId], loaded: &[InstrumentAny]) -> anyhow::Result<()> {
    let missing: Vec<String> = requested
        .iter()
        .filter(|id| !loaded.iter().any(|instrument| instrument.id() == **id))
        .map(ToString::to_string)
        .collect();
    ensure!(
        missing.is_empty(),
        "IB returned no instrument for {} (unknown or expired contract)",
        missing.join(", ")
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nautilus_model::instruments::stubs::futures_contract_es;

    fn es() -> InstrumentAny {
        InstrumentAny::FuturesContract(futures_contract_es(None, None))
    }

    /// Nothing listens there: a connection attempt would fail with a
    /// connection error, not the error asserted.
    fn closed_gateway() -> IbGateway {
        IbGateway {
            host: "127.0.0.1".to_string(),
            port: 1,
            client_id: 1,
        }
    }

    #[tokio::test]
    async fn connect_rejects_a_bar_type_ib_cannot_serve_before_connecting() {
        let bar_types = [
            bar_type("ESM4.CME-1-HOUR-LAST-EXTERNAL"),
            bar_type("ESM4.CME-1-MONTH-LAST-EXTERNAL"),
        ];
        let now = at("2024-06-10T12:00:00Z");
        let Err(err) = IbHistory::connect(
            closed_gateway(),
            &bar_types,
            at("2024-01-01T00:00:00Z"),
            now,
        )
        .await
        else {
            panic!("connected");
        };
        assert!(
            err.to_string()
                .contains("IB cannot serve bar type ESM4.CME-1-MONTH-LAST-EXTERNAL"),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn connect_rejects_an_end_within_one_bar_of_now_before_connecting() {
        let bar_types = [bar_type("ESM4.CME-1-HOUR-LAST-EXTERNAL")];
        let now = at("2024-06-10T12:00:00Z");
        let Err(err) = IbHistory::connect(closed_gateway(), &bar_types, now, now).await else {
            panic!("connected");
        };
        assert!(err.to_string().contains("within one bar of now"), "{err:#}");
    }

    fn at(s: &str) -> Timestamp {
        s.parse().unwrap()
    }

    #[test]
    fn accepts_an_end_at_least_the_longest_bar_before_now() {
        let bar_types = [
            bar_type("ESM4.CME-1-HOUR-LAST-EXTERNAL"),
            bar_type("ESM4.CME-1-DAY-LAST-EXTERNAL"),
        ];
        let now = at("2024-06-10T12:00:00Z");
        ensure_bars_closed(&bar_types, at("2024-06-09T12:00:00Z"), now).unwrap();
        ensure_bars_closed(&bar_types, at("2024-06-01T00:00:00Z"), now).unwrap();
    }

    #[test]
    fn rejects_an_end_within_the_longest_bar_of_now() {
        let bar_types = [
            bar_type("ESM4.CME-1-HOUR-LAST-EXTERNAL"),
            bar_type("ESM4.CME-1-DAY-LAST-EXTERNAL"),
        ];
        let now = at("2024-06-10T12:00:00Z");
        let err = ensure_bars_closed(&bar_types, at("2024-06-09T12:00:01Z"), now)
            .unwrap_err()
            .to_string();
        assert!(err.contains("2024-06-09T12:00:01Z"), "{err}");
        assert!(err.contains("2024-06-09T12:00:00Z"), "{err}");
    }

    #[test]
    fn accepts_instruments_when_every_requested_id_loaded() {
        let requested = [InstrumentId::from("ESZ21.GLBX")];
        ensure_all_loaded(&requested, &[es()]).unwrap();
    }

    #[test]
    fn names_the_requested_ids_ib_did_not_return() {
        let requested = [
            InstrumentId::from("ESH4.CME"),
            InstrumentId::from("ESZ21.GLBX"),
            InstrumentId::from("ESM4.CME"),
        ];
        let err = ensure_all_loaded(&requested, &[es()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("ESH4.CME, ESM4.CME"), "{err}");
        assert!(!err.contains("ESZ21.GLBX"), "{err}");
    }

    fn bar_type(s: &str) -> BarType {
        s.parse().unwrap()
    }

    #[test]
    fn maps_a_bar_type_to_the_ib_bar_spec_string() {
        let spec = bar_spec(&bar_type("ESM4.CME-1-HOUR-LAST-EXTERNAL")).unwrap();
        assert_eq!(spec, "1-HOUR-LAST");
    }

    #[test]
    fn maps_every_bar_size_ib_serves() {
        for spec in [
            "1-SECOND-LAST",
            "5-SECOND-BID",
            "15-SECOND-ASK",
            "30-SECOND-MID",
            "1-MINUTE-LAST",
            "2-MINUTE-LAST",
            "3-MINUTE-LAST",
            "5-MINUTE-LAST",
            "10-MINUTE-LAST",
            "15-MINUTE-LAST",
            "20-MINUTE-LAST",
            "30-MINUTE-LAST",
            "1-HOUR-LAST",
            "2-HOUR-LAST",
            "3-HOUR-LAST",
            "4-HOUR-LAST",
            "8-HOUR-LAST",
            "1-DAY-LAST",
            "1-WEEK-LAST",
        ] {
            let bt = bar_type(&format!("ESM4.CME-{spec}-EXTERNAL"));
            assert_eq!(bar_spec(&bt).unwrap(), spec);
        }
    }

    #[test]
    fn rejects_a_bar_type_ib_cannot_serve() {
        for bt in [
            "ESM4.CME-10-SECOND-LAST-EXTERNAL",
            "ESM4.CME-4-MINUTE-LAST-EXTERNAL",
            "ESM4.CME-12-MINUTE-LAST-EXTERNAL",
            "ESM4.CME-6-HOUR-LAST-EXTERNAL",
            "ESM4.CME-2-DAY-LAST-EXTERNAL",
            "ESM4.CME-1-MONTH-LAST-EXTERNAL",
            "ESM4.CME-100-TICK-LAST-EXTERNAL",
            "ESM4.CME-1-HOUR-MARK-EXTERNAL",
            "ESM4.CME-1-HOUR-LAST-INTERNAL",
        ] {
            let err = bar_spec(&bar_type(bt)).unwrap_err();
            assert!(err.to_string().contains(bt), "{bt}: {err}");
        }
    }
}
