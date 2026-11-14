//! What a trade request's failure proves, as Nautilus' `CommandFailure`:
//! each wire condition one class, as the adapter guide names them
//! (`developer_guide/adapters.md`, "Order command outcome policy" and
//! "Naming the evidence classes"). The dispatch emits each class's events
//! (`Dispatch::fail`).
//!
//! - **Not sent** (`NotSent`): the session never wrote the request's
//!   frame: it had failed, the connection closed while the request waited,
//!   or the transport refused the frame before its write began (see
//!   `websocket::handler`); or the request could not be made.
//! - **Unknown** (`Ambiguous`): the session ended without saying what
//!   became of the frame (its handler gone, maybe mid-write); cTrader
//!   answered with a code that does not say the request was not carried
//!   out (`UNKNOWN_OUTCOME`); or an error answer is not cTrader's. A frame
//!   whose write began and failed counts as sent (the handler's
//!   `failed_send`): its outcome is unknown too, and like any sent request
//!   it waits for its events.
//! - **Rejected** (`VenueRejected`): cTrader refused the request, by an
//!   order error event or an error answer of any other code.

use nautilus_live::execution::failure::CommandFailure;
use nautilus_model::identifiers::Venue;
use tokio::sync::oneshot::error::RecvError;

use crate::common::messages::CTraderError;

/// The codes of cTrader's refusals that do not say the request was not
/// carried out (Spotware's `ProtoErrorCode` and `ProtoOAErrorCode`): a
/// rate limit, the trading server unreachable or under maintenance, an
/// error cTrader does not name. Nautilus' guide leaves such an outcome
/// unknown, as its Bybit and Binance adapters do their rate limits and
/// server errors.
const UNKNOWN_OUTCOME: [&str; 6] = [
    // "Generic error."
    "UNKNOWN_ERROR",
    // "Connection to Server is lost or not supported."
    "CANT_ROUTE_REQUEST",
    // "Message is blocked by server or rate limit is reached."
    "BLOCKED_PAYLOAD_TYPE",
    // "Request frequency is reached."
    "REQUEST_FREQUENCY_EXCEEDED",
    // "Trading service is not available."
    "CH_SERVER_NOT_REACHABLE",
    // "Server is under maintenance."
    "SERVER_IS_UNDER_MAINTENANCE",
];

/// The failure, if any, of a trade request to `venue` whose sending ended
/// as `sending` (its `TradeSent`): none once its frame is written, or its
/// write began; `NotSent` if the session says it never was; `Ambiguous` if
/// the session is gone without a word.
pub(super) fn sending(
    sending: Result<Result<(), CTraderError>, RecvError>,
    venue: Venue,
) -> Result<(), CommandFailure> {
    match sending {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(CommandFailure::not_sent(format!(
            "not sent to {venue}: {e}"
        ))),
        Err(_) => Err(CommandFailure::ambiguous(format!(
            "the {venue} session ended without saying whether it sent the request"
        ))),
    }
}

/// cTrader's refusal of a trade request, by its `code` and `description`
/// (an order error event's, or an error answer's); its reason, the code
/// and the description.
pub(super) fn refusal(code: &str, description: Option<&str>) -> CommandFailure {
    let reason = match description.filter(|d| !d.is_empty()) {
        Some(description) => format!("{code}: {description}"),
        None => code.to_owned(),
    };
    if UNKNOWN_OUTCOME.contains(&code) {
        CommandFailure::Ambiguous(reason)
    } else {
        CommandFailure::VenueRejected(reason)
    }
}

/// The error answering a trade request: cTrader's refusal (see `refusal`),
/// else no answer of cTrader's.
pub(super) fn answer(error: &CTraderError) -> CommandFailure {
    match error {
        CTraderError::Venue { code, description } => refusal(code, description.as_deref()),
        other => CommandFailure::ambiguous(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::oneshot;

    use super::*;

    const VENUE: &str = "ACMEFX";

    /// What became of a request's frame, `sent` by the session, or not
    /// at all if `None`: the session's sender gone.
    async fn sent(sent: Option<Result<(), CTraderError>>) -> Result<(), CommandFailure> {
        let (tx, rx) = oneshot::channel();
        if let Some(sent) = sent {
            tx.send(sent).unwrap();
        } else {
            drop(tx);
        }
        sending(rx.await, Venue::from(VENUE))
    }

    #[tokio::test]
    async fn a_request_whose_frame_is_written_has_not_failed() {
        assert_eq!(sent(Some(Ok(()))).await, Ok(()));
    }

    /// The session answers an error only for a frame it never wrote (see
    /// `websocket::handler`).
    #[tokio::test]
    async fn a_request_the_session_says_it_never_sent_is_not_sent() {
        for error in [
            CTraderError::ConnectionLost,
            CTraderError::Session("the account auth failed".to_owned()),
            CTraderError::Transport("send failed: client closed or disconnecting".to_owned()),
        ] {
            let failure = sent(Some(Err(error.clone()))).await;
            assert!(
                matches!(&failure, Err(CommandFailure::NotSent(reason)) if reason.contains(VENUE)),
                "{error:?}: {failure:?}"
            );
        }
    }

    /// Its handler aborted, maybe mid-write: cTrader may have the frame.
    #[tokio::test]
    async fn a_request_whose_session_went_without_a_word_is_ambiguous() {
        assert!(matches!(
            sent(None).await,
            Err(CommandFailure::Ambiguous(_))
        ));
    }

    /// The refusals the demo answered (`testdata/order_error_*`), and the
    /// deal timeout Spotware documents as "rejected".
    #[test]
    fn a_refusal_ctrader_explains_is_a_venue_rejection() {
        for code in [
            "TRADING_BAD_VOLUME",
            "ORDER_NOT_FOUND",
            "POSITION_NOT_FOUND",
            "NOT_ENOUGH_MONEY",
            "MARKET_CLOSED",
            "TIMEOUT_ERROR",
        ] {
            assert_eq!(
                refusal(code, Some("as cTrader put it")),
                CommandFailure::VenueRejected(format!("{code}: as cTrader put it")),
                "{code}"
            );
        }
        assert_eq!(
            refusal("TRADING_BAD_VOLUME", None),
            CommandFailure::VenueRejected("TRADING_BAD_VOLUME".to_owned())
        );
    }

    #[test]
    fn a_rate_limit_or_an_unavailable_server_leaves_the_outcome_unknown() {
        for code in [
            "UNKNOWN_ERROR",
            "CANT_ROUTE_REQUEST",
            "BLOCKED_PAYLOAD_TYPE",
            "REQUEST_FREQUENCY_EXCEEDED",
            "CH_SERVER_NOT_REACHABLE",
            "SERVER_IS_UNDER_MAINTENANCE",
        ] {
            assert!(
                matches!(refusal(code, None), CommandFailure::Ambiguous(_)),
                "{code}"
            );
        }
    }

    #[test]
    fn an_error_answer_is_classified_by_its_code() {
        let refused = CTraderError::Venue {
            code: "POSITION_NOT_FOUND".to_owned(),
            description: Some("Position not found with id 690004734".to_owned()),
        };
        assert_eq!(
            answer(&refused),
            CommandFailure::VenueRejected(
                "POSITION_NOT_FOUND: Position not found with id 690004734".to_owned()
            )
        );
        let limited = CTraderError::Venue {
            code: "REQUEST_FREQUENCY_EXCEEDED".to_owned(),
            description: None,
        };
        assert!(matches!(answer(&limited), CommandFailure::Ambiguous(_)));
    }

    #[test]
    fn an_answer_that_is_not_ctraders_refusal_is_ambiguous() {
        for error in [
            CTraderError::Protocol("unexpected ProtoOASpotEvent".to_owned()),
            CTraderError::Timeout,
            CTraderError::ConnectionLost,
        ] {
            assert!(
                matches!(answer(&error), CommandFailure::Ambiguous(_)),
                "{error:?}"
            );
        }
    }
}
