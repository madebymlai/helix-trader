//! Test support that captures the log records of a test run alone in its own process (`alone`,
//! then `capture`). Nautilus has no such support: its tests each install a `log::Log`.

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

const ALONE: &str = "HELIX_TRADER_TEST_ALONE";

/// Returns whether this process is the one running test `name` in `module` alone.
///
/// If not, runs the test alone in a new process.
///
/// The test binary's own process holds Nautilus' logger, which refuses a logger installed before
/// it.
pub(crate) fn alone(module: &str, name: &str) -> bool {
    if std::env::var_os(ALONE).is_some() {
        return true;
    }
    // The test's path in the binary, without the crate
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

/// Installs the process's logger. It keeps records at `level` or above.
///
/// # Panics
///
/// Panics if the process already has a logger.
pub(crate) fn capture(level: log::LevelFilter) {
    log::set_logger(&CAPTURED).unwrap();
    log::set_max_level(level);
}

/// Returns the records kept so far, each as `WARN the message`.
pub(crate) fn lines() -> Vec<String> {
    CAPTURED.0.lock().unwrap().clone()
}
