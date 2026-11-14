//! Compiles Spotware's cTrader Open API messages into Rust types (prost).
//!
//! The four `.proto` files in `proto/` are Spotware's, copied unchanged
//! from github.com/spotware/openapi-proto-messages at commit
//! a9676f27f40b60569fb8a510aeadedba901b74e9 (2026-11-04, release 100),
//! MIT-licensed (`LICENSE` there). They declare no package, so the types
//! land in one file, `ctrader_open_api.rs`. Needs `protoc` (or `PROTOC`
//! naming it).

use std::io::Result;

const PROTO_DIR: &str = "proto";

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed={PROTO_DIR}");
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
