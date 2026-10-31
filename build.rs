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

use std::{io::Result, path::Path};

const PROTO_DIR: &str = "src/data/ic/proto";

const GOLD_SCALP_DIR: &str = "src/strategy/gold_scalp";

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed={PROTO_DIR}");
    println!("cargo:rerun-if-changed=src/strategy");
    println!("cargo:rustc-check-cfg=cfg(gold_scalp)");
    if Path::new(GOLD_SCALP_DIR).is_dir() {
        println!("cargo:rustc-cfg=gold_scalp");
    }
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
