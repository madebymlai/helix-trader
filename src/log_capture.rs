//! Test support: captures the log records of a test that runs itself alone in a process of
//! its own (`alone`, then `capture`), for a test whose contract is what is logged. Nautilus
//! has no log capture: its tests each install a `log::Log` of their own, as this one does.

use std::{process::Command, sync::Mutex};

struct Captured(Mutex<Vec<String>>);

impl log::Log for Captured {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        let line = format!("{} {}", record.level(), record.args());
        self.0.lock().unwrap().push(line);
    }

    fn flush(&self) {}
}

static CAPTURED: Captured = Captured(Mutex::new(Vec::new()));

/// Set in the process a test runs itself in (`alone`).
const ALONE: &str = "HELIX_TRADER_TEST_ALONE";

/// Whether this is the process in which the test `name` of the module `module` (its
/// `module_path!()`) runs itself alone, so that the logger it installs (`capture`) is the
/// process's: the test binary's own process holds Nautilus' logger, which the first node of a
/// test installs, and which a logger installed before would make it refuse. Otherwise runs the
/// test so, and checks that it ran and passed.
pub(crate) fn alone(module: &str, name: &str) -> bool {
    if std::env::var_os(ALONE).is_some() {
        return true;
    }
    // The test's path in the binary: its module's, the crate left out
    let (_, module) = module.split_once("::").unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            format!("{module}::{name}").as_str(),
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(ALONE, "1")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && stdout.contains("1 passed"),
        "{stdout}{}",
        String::from_utf8_lossy(&output.stderr)
    );
    false
}

/// Installs the process's logger, which keeps every record at `level` or above, from a test
/// run `alone`.
///
/// # Panics
///
/// Panics if the process has a logger already.
pub(crate) fn capture(level: log::LevelFilter) {
    log::set_logger(&CAPTURED).unwrap();
    log::set_max_level(level);
}

/// The records kept so far, each as its level and message: `WARN the message`.
pub(crate) fn lines() -> Vec<String> {
    CAPTURED.0.lock().unwrap().clone()
}
