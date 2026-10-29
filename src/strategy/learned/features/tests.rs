use nautilus_common::actor::indicators::ActorIndicator;
use nautilus_model::data::Bar;

use super::*;

const GOLD: &str = "XAUUSD.ICMARKETS-1-MINUTE-MID-INTERNAL";
const HOUR: SignedDuration = SignedDuration::from_hours(1);

/// A dataset build replays the strategy's warmup before its first day.
#[test]
fn the_features_declare_the_strategys_warmup() {
    let warmup = SignedDuration::from_hours(3);

    assert_eq!(
        features(&[BarType::from(GOLD)], warmup).unwrap().warmup(),
        warmup
    );
}

#[test]
fn each_bar_type_has_a_fast_and_a_slow_average_in_order() {
    let silver = "XAGUSD.ICMARKETS-1-MINUTE-MID-INTERNAL";

    let features = features(&[BarType::from(GOLD), BarType::from(silver)], HOUR).unwrap();

    assert_eq!(
        features.names(),
        [
            format!("{GOLD}:ema_10"),
            format!("{GOLD}:ema_30"),
            format!("{silver}:ema_10"),
            format!("{silver}:ema_30"),
        ]
    );
}

/// Flat closes: each average is the close once warm, the slow after
/// 30 bars.
#[test]
fn the_features_are_warm_after_the_slow_period() {
    let features = features(&[BarType::from(GOLD)], HOUR).unwrap();
    let bar = |close: &str| {
        Bar::new(
            BarType::from(GOLD),
            close.into(),
            close.into(),
            close.into(),
            close.into(),
            1.into(),
            0.into(),
            0.into(),
        )
    };

    for _ in 0..29 {
        features.handle_bar(&bar("4000.00")).unwrap();
    }
    assert_eq!(features.values(), None);
    features.handle_bar(&bar("4000.00")).unwrap();

    assert_eq!(features.values(), Some(vec![4000.0, 4000.0]));
}

#[test]
fn no_bar_type_is_no_feature_set() {
    assert!(features(&[], HOUR).is_err());
}

/// The fingerprint hashes this module's code: its tests live in a
/// file of their own, so editing one changes no fingerprint.
#[test]
fn the_fingerprinted_code_holds_no_test() {
    assert!(!include_str!("../features.rs").contains("#[test]"));
}
