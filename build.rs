//! Finds the strategies: each folder of `src/strategy` with a `mod.rs` is
//! one, named by its folder. Writes `strategies.rs` to `OUT_DIR`, which
//! `strategy/mod.rs` includes: a module per strategy, and `StrategyName`,
//! the command line's names, each handing its strategy to a
//! `strategy::Visitor` through the folder's `visit`. No file names a
//! strategy, so a folder kept out of the repository builds where it exists
//! and a checkout without it builds without it.

use std::{
    env,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
};

fn main() {
    let root =
        Path::new(&env::var("CARGO_MANIFEST_DIR").expect("set by Cargo")).join("src/strategy");
    println!("cargo:rerun-if-changed={}", root.display());
    let strategies = strategies(&root);
    let out = Path::new(&env::var("OUT_DIR").expect("set by Cargo")).join("strategies.rs");
    fs::write(&out, source(&strategies))
        .unwrap_or_else(|e| panic!("writing {}: {e}", out.display()));
}

/// A strategy's folder name, its module and command-line name, and its
/// `mod.rs`.
struct Found {
    name: String,
    module: PathBuf,
}

/// The strategies under `root`, by name: its folders holding a `mod.rs`.
/// A folder holding none is a module's (`orders/` beside `orders.rs`).
///
/// # Panics
///
/// Panics if a strategy's folder name is not a lowercase identifier: it
/// names a module and a command-line value.
fn strategies(root: &Path) -> Vec<Found> {
    let entries = fs::read_dir(root).unwrap_or_else(|e| panic!("reading {}: {e}", root.display()));
    let mut found: Vec<Found> = entries
        .map(|entry| entry.expect("a readable entry").path())
        .filter(|dir| dir.join("mod.rs").is_file())
        .map(|dir| {
            let name = dir
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| is_lowercase_identifier(name))
                .unwrap_or_else(|| {
                    panic!(
                        "the strategy folder {} must be named by a lowercase identifier \
                         ([a-z][a-z0-9_]*): it names its module and its command-line name",
                        dir.display()
                    )
                })
                .to_owned();
            Found {
                name,
                module: dir.join("mod.rs"),
            }
        })
        .collect();
    found.sort_by(|a, b| a.name.cmp(&b.name));
    found
}

fn is_lowercase_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|c| c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// `model_ema` as `ModelEma`: a variant of `StrategyName`.
fn variant(name: &str) -> String {
    name.split('_')
        .flat_map(|word| {
            let mut chars = word.chars();
            chars
                .next()
                .map(|first| first.to_ascii_uppercase())
                .into_iter()
                .chain(chars)
        })
        .collect()
}

/// The source `strategy/mod.rs` includes.
fn source(strategies: &[Found]) -> String {
    let mut src = String::new();
    for found in strategies {
        let path = found.module.to_str().expect("a UTF-8 path");
        writeln!(src, "#[path = {path:?}]\npub mod {};", found.name).unwrap();
    }
    src.push_str(
        "\n/// The strategies, by command-line name: each folder of `src/strategy` \
         holding a `mod.rs`, by its folder's name.\n\
         #[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]\n\
         pub enum StrategyName {\n",
    );
    for found in strategies {
        writeln!(
            src,
            "    #[value(name = {:?})]\n    {},",
            found.name,
            variant(&found.name)
        )
        .unwrap();
    }
    src.push_str(
        "}\n\nimpl StrategyName {\n    \
         /// Hands the strategy to `visitor`, as its folder's `visit` does.\n    \
         pub fn visit<V: Visitor>(self, visitor: V) -> V::Output {\n        \
         match self {\n",
    );
    for found in strategies {
        writeln!(
            src,
            "            Self::{} => {}::visit(visitor),",
            variant(&found.name),
            found.name
        )
        .unwrap();
    }
    src.push_str("        }\n    }\n}\n");
    src
}
