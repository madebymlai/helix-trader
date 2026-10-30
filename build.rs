//! Compiles Spotware's cTrader Open API messages into Rust types (prost),
//! for the IC Markets adapter (`src/data/ic/`).
//!
//! The four `.proto` files in `src/data/ic/proto/` are Spotware's, copied
//! unchanged from github.com/spotware/openapi-proto-messages at commit
//! 3fd8bddfbe0cfc2ecfda079623dc4e498af11e66 (2025-11-13), MIT-licensed
//! (`LICENSE` there). They declare no package, so the types land in one
//! file, `ctrader_open_api.rs`. Needs `protoc` (or `PROTOC` naming it).
//!
//! Also sets the `gold_scalp` cfg when `src/strategy/gold_scalp/` exists: that
//! strategy is kept out of the repository, and a checkout without it builds.
//!
//! And names the source of Nautilus' indicators, as `Cargo.lock` pins it, in
//! `NAUTILUS_INDICATORS_SOURCE`: the feature fingerprint covers the shared
//! indicator code through it (`model::features`).

use std::{
    fs,
    io::{Error, Result},
    path::Path,
};

const PROTO_DIR: &str = "src/data/ic/proto";

const GOLD_SCALP_DIR: &str = "src/strategy/gold_scalp";

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed={PROTO_DIR}");
    println!("cargo:rerun-if-changed=src/strategy");
    println!("cargo:rustc-check-cfg=cfg(gold_scalp)");
    if Path::new(GOLD_SCALP_DIR).is_dir() {
        println!("cargo:rustc-cfg=gold_scalp");
    }
    println!("cargo:rerun-if-changed=Cargo.lock");
    let lock = fs::read_to_string("Cargo.lock")?;
    let source = locked_source(&lock, "nautilus-indicators")?;
    println!("cargo:rustc-env=NAUTILUS_INDICATORS_SOURCE={source}");
    prost_build::Config::new()
        .default_package_filename("ctrader_open_api")
        .compile_protos(
            &[
                format!("{PROTO_DIR}/OpenApiMessages.proto"),
                format!("{PROTO_DIR}/OpenApiCommonMessages.proto"),
            ],
            &[PROTO_DIR],
        )
}

/// The `source` of the package `name` in the lock file `lock`: for a git
/// dependency, its repository and the commit it is pinned to.
fn locked_source<'a>(lock: &'a str, name: &str) -> Result<&'a str> {
    let package = format!("name = \"{name}\"");
    lock.split("[[package]]")
        .find(|block| block.lines().any(|line| line == package))
        .and_then(|block| {
            block
                .lines()
                .find_map(|line| line.strip_prefix("source = "))
        })
        .map(|source| source.trim_matches('"'))
        .ok_or_else(|| Error::other(format!("no source of {name} in Cargo.lock")))
}
