//! Sets the cfg of each private strategy whose folder exists, each apart
//! from the others: those strategies are kept out of the repository, and a
//! checkout without them builds.

use std::path::Path;

/// Each private strategy's cfg and folder.
const PRIVATE: [(&str, &str); 2] = [
    ("gold_scalp", "src/strategy/gold_scalp"),
    ("gold_scalp_v2", "src/strategy/gold_scalp_v2"),
];

fn main() {
    println!("cargo:rerun-if-changed=src/strategy");
    for (cfg, dir) in PRIVATE {
        println!("cargo:rustc-check-cfg=cfg({cfg})");
        if Path::new(dir).is_dir() {
            println!("cargo:rustc-cfg={cfg}");
        }
    }
}
