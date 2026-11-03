//! Sets the `gold_scalp` cfg when `src/strategy/gold_scalp/` exists: that
//! strategy is kept out of the repository, and a checkout without it builds.

use std::path::Path;

const GOLD_SCALP_DIR: &str = "src/strategy/gold_scalp";

fn main() {
    println!("cargo:rerun-if-changed=src/strategy");
    println!("cargo:rustc-check-cfg=cfg(gold_scalp)");
    if Path::new(GOLD_SCALP_DIR).is_dir() {
        println!("cargo:rustc-cfg=gold_scalp");
    }
}
