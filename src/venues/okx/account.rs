//! Reads the OKX account with its key, and changes nothing. The adapter reads the position mode.
//! Our signed `GET /api/v5/account/leverage-info` reads the leverage, which the adapter does not.
//! The key is the adapter's (`OKX_API_KEY`, `OKX_API_SECRET`, `OKX_API_PASSPHRASE`). It works on
//! the environment `OKX_ENVIRONMENT` names, `demo` or `live`. We read that variable ourselves,
//! because the adapter takes the environment only as a config field.

use std::collections::HashMap;

use anyhow::Context;
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::identifiers::InstrumentId;
use nautilus_network::http::{HttpClient, HttpRedirectPolicy};
use nautilus_okx::{
    OKXPositionMode,
    common::{
        credential::Credential,
        enums::{OKXEnvironment, OKXMarginMode, OKXRegion},
    },
    config::OKXExecutionClientConfig,
    http::OKXHttpClient,
};
use rust_decimal::Decimal;
use serde::Deserialize;

pub(super) const ENVIRONMENT_VAR: &str = "OKX_ENVIRONMENT";

/// The region for an account opened on my.okx.com, which trades X-Perps.
pub(super) const REGION: OKXRegion = OKXRegion::Eea;

/// Cross margin: the whole account balance backs every position.
pub(super) const MARGIN_MODE: OKXMarginMode = OKXMarginMode::Cross;

/// Returns the OKX environment `value` names, which must be exactly `demo` or `live`, as in
/// Nautilus' serde.
pub(super) fn environment_of(value: Option<&str>) -> anyhow::Result<OKXEnvironment> {
    let refused = || {
        anyhow::anyhow!(
            "{ENVIRONMENT_VAR} must be `demo` or `live` (got {value:?}): set it \
             in .env to the environment of the OKX key, `demo` for a demo \
             trading key, `live` for a real one (see .env.example)"
        )
    };
    let value = value.ok_or_else(refused)?;
    serde_json::from_value(serde_json::Value::from(value)).map_err(|_| refused())
}

pub(super) struct Account {
    okx: OKXHttpClient,
    /// Our client, which reads the leverage.
    http: HttpClient,
    base_url: String,
    credential: Credential,
}

impl Account {
    /// Connects to the account in `REGION`, on the environment `OKX_ENVIRONMENT` names.
    ///
    /// # Errors
    ///
    /// Returns an error if `environment_of` or `connect` fails.
    pub(super) fn of_environment() -> anyhow::Result<Self> {
        let environment = environment_of(std::env::var(ENVIRONMENT_VAR).ok().as_deref())?;
        Self::connect(&OKXExecutionClientConfig {
            environment,
            region: REGION,
            margin_mode: Some(MARGIN_MODE),
            ..Default::default()
        })
    }

    pub(super) fn connect(exec: &OKXExecutionClientConfig) -> anyhow::Result<Self> {
        let credential = Credential::resolve(None, None, None).context(
            "OKX credentials missing: set OKX_API_KEY, OKX_API_SECRET and \
             OKX_API_PASSPHRASE (see .env.example)",
        )?;
        let base_url = exec.http_base_url();
        let okx = OKXHttpClient::with_credentials(
            None,
            None,
            None,
            Some(base_url.clone()),
            exec.http_timeout_secs,
            exec.max_retries,
            exec.retry_delay_initial_ms,
            exec.retry_delay_max_ms,
            exec.environment,
            None,
        )
        .context("building the OKX HTTP client")?;

        // Temporary: the adapter does not read the leverage, so we read it through OKX's bare API,
        // until Nautilus' OKX client reads it: helix-trader#21.
        let mut headers = HashMap::new();
        if exec.environment == OKXEnvironment::Demo {
            headers.insert("x-simulated-trading".to_string(), "1".to_string());
        }
        let http = HttpClient::builder()
            .headers(headers)
            .redirect_policy(HttpRedirectPolicy::Reject)
            .timeout_secs(exec.http_timeout_secs)
            .build()
            .context("building the OKX account client")?;
        Ok(Self {
            okx,
            http,
            base_url,
            credential,
        })
    }

    pub(super) async fn position_mode(&self) -> anyhow::Result<OKXPositionMode> {
        let configs = self
            .okx
            .raw_client()
            .get_account_configuration()
            .await
            .context("reading the OKX account configuration")?;
        let config = configs
            .first()
            .context("OKX returned no account configuration")?;
        Ok(config.position_mode)
    }

    pub(super) async fn leverage(&self, id: InstrumentId) -> anyhow::Result<Decimal> {
        let mode = serde_json::to_value(MARGIN_MODE)?;
        let mode = mode.as_str().context("a margin mode is a string")?;
        let path = format!(
            "/api/v5/account/leverage-info?instId={}&mgnMode={mode}",
            id.symbol
        );

        // Same format as the adapter signs: ISO 8601 to the millisecond
        let timestamp = format!(
            "{:.3}",
            get_atomic_clock_realtime().get_time_ns().to_datetime_utc()
        );
        let signature = self.credential.sign(&timestamp, "GET", &path, "");
        let headers = HashMap::from([
            (
                "OK-ACCESS-KEY".to_string(),
                self.credential.api_key().to_string(),
            ),
            (
                "OK-ACCESS-PASSPHRASE".to_string(),
                self.credential.api_passphrase().to_string(),
            ),
            ("OK-ACCESS-TIMESTAMP".to_string(), timestamp),
            ("OK-ACCESS-SIGN".to_string(), signature),
        ]);
        let response = self
            .http
            .get(
                format!("{}{path}", self.base_url),
                None,
                Some(headers),
                None,
                None,
            )
            .await
            .with_context(|| format!("asking OKX the leverage of {id}"))?;
        anyhow::ensure!(
            response.status.is_success(),
            "OKX answered HTTP {} for the leverage of {id}",
            response.status.as_u16()
        );
        leverage_of(id, &response.body)
    }
}

#[derive(Debug, Deserialize)]
struct LeverageAnswer {
    code: String,
    msg: String,
    data: Vec<LeverageInfo>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LeverageInfo {
    inst_id: String,
    lever: Decimal,
}

/// Returns the leverage of `id` in `body`.
///
/// Fails if OKX refused, listed another instrument or none, or the rows disagree.
fn leverage_of(id: InstrumentId, body: &[u8]) -> anyhow::Result<Decimal> {
    let answer: LeverageAnswer =
        serde_json::from_slice(body).with_context(|| format!("parsing OKX's leverage of {id}"))?;
    anyhow::ensure!(
        answer.code == "0",
        "OKX refused the leverage of {id}: code {}, {}",
        answer.code,
        answer.msg
    );
    let symbol = id.symbol.as_str();
    let mut levers = answer.data.iter().map(|info| {
        anyhow::ensure!(
            info.inst_id == symbol,
            "OKX answered the leverage of {} for {id}",
            info.inst_id
        );
        Ok(info.lever)
    });
    let lever = levers
        .next()
        .with_context(|| format!("OKX listed no leverage for {id}"))??;

    for other in levers {
        let other = other?;
        anyhow::ensure!(
            other == lever,
            "OKX lists leverages {lever} and {other} for {id}"
        );
    }
    Ok(lever)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn xperp() -> InstrumentId {
        InstrumentId::from("BTC-USD_UM_XPERP-310328.OKX")
    }

    #[rstest]
    fn the_environment_is_demo_or_live_by_its_lowercase_name() {
        assert_eq!(environment_of(Some("demo")).unwrap(), OKXEnvironment::Demo);
        assert_eq!(environment_of(Some("live")).unwrap(), OKXEnvironment::Live);
    }

    #[rstest]
    #[case::missing(None)]
    #[case::empty(Some(""))]
    #[case::capitalized(Some("Demo"))]
    #[case::another_name(Some("real"))]
    #[case::with_a_space(Some(" demo"))]
    fn a_missing_empty_or_other_environment_is_refused_naming_the_variable_and_values(
        #[case] value: Option<&str>,
    ) {
        let err = environment_of(value).unwrap_err().to_string();
        assert!(err.contains("OKX_ENVIRONMENT"), "{err}");
        assert!(err.contains("`demo` or `live`"), "{err}");
    }

    #[rstest]
    fn the_leverage_is_read_from_okxs_answer() {
        let body = br#"{"code":"0","msg":"","data":[{"ccy":"","instId":"BTC-USD_UM_XPERP-310328","lever":"3","mgnMode":"cross","posSide":"net"}]}"#;
        assert_eq!(leverage_of(xperp(), body).unwrap(), Decimal::from(3));
    }

    #[rstest]
    #[case::refused(
        br#"{"code":"51001","msg":"Instrument ID doesn't exist","data":[]}"#,
        "51001"
    )]
    #[case::empty(br#"{"code":"0","msg":"","data":[]}"#, "BTC-USD_UM_XPERP-310328.OKX")]
    #[case::foreign(
        br#"{"code":"0","msg":"","data":[{"ccy":"","instId":"BTC-USDT-SWAP","lever":"3","mgnMode":"cross","posSide":"net"}]}"#,
        "BTC-USDT-SWAP"
    )]
    #[case::split(
        br#"{"code":"0","msg":"","data":[{"ccy":"","instId":"BTC-USD_UM_XPERP-310328","lever":"3","mgnMode":"cross","posSide":"net"},{"ccy":"","instId":"BTC-USD_UM_XPERP-310328","lever":"5","mgnMode":"cross","posSide":"net"}]}"#,
        "3 and 5"
    )]
    fn a_refused_empty_or_foreign_leverage_answer_is_an_error(
        #[case] body: &[u8],
        #[case] named: &str,
    ) {
        let err = leverage_of(xperp(), body).unwrap_err().to_string();
        assert!(err.contains(named), "{err}");
    }
}
