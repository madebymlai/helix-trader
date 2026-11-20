//! Reports the binary's `train`, with one block per retrain time. The figures describe the run,
//! not performance, so training and evaluation stay apart and an operator can size a train against
//! its interval. The out-of-sample evidence for a train is a backtest (`walk`).
//!
//! A block names the train's time and its outcome. The outcome is the model set published (its
//! directory) or the failure and its reason. The block then lists the figures reached, whether
//! the train failed or not (`train::Reached`):
//! - The wall time, CPU time (user and system) and peak memory of the train process.
//! - The dataset days read, built or cached.
//! - The rows in the fit and calibration slices of the sides, the out-of-fold pair and the meta,
//!   beside each level's `min_rows`.
//!
//! The process figures are read once at the end of a train (`Meter::lap`). Wall time comes from
//! Nautilus' real-time clock. CPU time and peak memory come from `getrusage` (`RUSAGE_SELF`),
//! which Nautilus does not read. They exist on Unix alone, and off Unix they are reported as not
//! measured. A block's CPU and wall times run from the end of the previous block, or for the first
//! block from the process start. The peak memory is the process's high-water mark, which no block
//! resets.

use std::{fmt::Display, time::Duration};

use jiff::Timestamp;
use nautilus_core::{UnixNanos, time::get_atomic_clock_realtime};

use super::train::{Reached, Slices, Trained};

/// Bytes in a mebibyte, the unit the peak memory is reported in.
const MIB: f64 = 1024.0 * 1024.0;

/// The process figures for the trains of one train process, with one lap per train.
#[derive(Debug)]
pub struct Meter {
    since: UnixNanos,
    cpu: Cpu,
}

/// What a train took: its wall time, plus the process's CPU time over it and its peak memory where
/// `getrusage` reads them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Usage {
    wall: Duration,
    process: Option<Process>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Cpu {
    user: Duration,
    system: Duration,
}

/// The CPU time of a train and the peak memory of the process, in bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Process {
    cpu: Cpu,
    peak_memory: u64,
}

/// The block for the train at `at`, which did `trained` and took `usage`.
#[derive(Debug)]
pub struct Report<'a> {
    pub at: Timestamp,
    pub trained: &'a Trained,
    pub usage: Usage,
}

impl Meter {
    /// Starts a meter now. The first lap's CPU time counts from the process start.
    #[must_use]
    pub fn start() -> Self {
        Self {
            since: get_atomic_clock_realtime().get_time_ns(),
            cpu: Cpu::default(),
        }
    }

    /// Returns what the process took since the last lap, or the meter's start, and its peak memory
    /// so far.
    pub fn lap(&mut self) -> Usage {
        let now = get_atomic_clock_realtime().get_time_ns();
        let wall = Duration::from_nanos(now.as_u64().saturating_sub(self.since.as_u64()));
        self.since = now;
        let process = resources().map(|(cpu, peak_memory)| {
            let lap = Cpu {
                user: cpu.user.saturating_sub(self.cpu.user),
                system: cpu.system.saturating_sub(self.cpu.system),
            };
            self.cpu = cpu;
            Process {
                cpu: lap,
                peak_memory,
            }
        });
        Usage { wall, process }
    }
}

/// Returns the process's CPU time and peak memory in bytes, from `getrusage`.
///
/// `ru_maxrss` is in KiB on Linux and the BSDs, and in bytes on macOS.
#[cfg(unix)]
#[allow(unsafe_code)]
fn resources() -> Option<(Cpu, u64)> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // SAFETY: the pointer is valid for an `rusage`, which `getrusage`
    // writes whole, failing only on an invalid `who`.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return None;
    }
    // SAFETY: `getrusage` succeeded, so wrote the whole `rusage`.
    let usage = unsafe { usage.assume_init() };
    let time = |time: libc::timeval| {
        Duration::new(
            u64::try_from(time.tv_sec).unwrap_or_default(),
            u32::try_from(time.tv_usec).unwrap_or_default() * 1000,
        )
    };
    let unit = if cfg!(target_os = "macos") { 1 } else { 1024 };
    Some((
        Cpu {
            user: time(usage.ru_utime),
            system: time(usage.ru_stime),
        },
        u64::try_from(usage.ru_maxrss).unwrap_or_default() * unit,
    ))
}

#[cfg(not(unix))]
fn resources() -> Option<(Cpu, u64)> {
    None
}

impl Display for Report<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Reached {
            days,
            sides,
            out_of_fold,
            meta,
        } = self.trained.reached;
        writeln!(f, "the train at {}: {}", self.at, self.trained.outcome)?;
        writeln!(f, "  Wall time: {}", seconds(self.usage.wall))?;
        match self.usage.process {
            Some(Process { cpu, peak_memory }) => {
                writeln!(
                    f,
                    "  CPU time: {} user, {} system",
                    seconds(cpu.user),
                    seconds(cpu.system)
                )?;
                #[allow(clippy::cast_precision_loss, reason = "a figure to read")]
                let mib = peak_memory as f64 / MIB;
                writeln!(f, "  Peak memory: {mib:.1} MiB")?;
            }
            None => {
                writeln!(f, "  CPU time: not measured off Unix")?;
                writeln!(f, "  Peak memory: not measured off Unix")?;
            }
        }

        if let Some(days) = days {
            writeln!(
                f,
                "  Dataset days: {} built, {} cached",
                days.built, days.cached
            )?;
        }

        for (name, slices) in [
            ("sides", sides),
            ("out-of-fold pair", out_of_fold),
            ("meta", meta),
        ] {
            if let Some(Slices {
                fit,
                calibration,
                min_rows,
            }) = slices
            {
                writeln!(
                    f,
                    "  Rows of the {name}: {fit} fit, {calibration} calibration, min_rows {min_rows}"
                )?;
            }
        }
        Ok(())
    }
}

fn seconds(duration: Duration) -> String {
    format!("{:.3} s", duration.as_secs_f64())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use rstest::rstest;

    use super::*;
    use crate::model::{
        dataset::DaysRead,
        train::{Failure, Outcome},
    };

    fn at() -> Timestamp {
        "2026-10-23T00:00:00Z".parse().unwrap()
    }

    /// 2.5 s of wall time, 1.25 s of user CPU time, 0.125 s of system CPU time and 150 MiB at the
    /// peak.
    fn usage() -> Usage {
        Usage {
            wall: Duration::from_millis(2500),
            process: Some(Process {
                cpu: Cpu {
                    user: Duration::from_millis(1250),
                    system: Duration::from_millis(125),
                },
                peak_memory: 150 * 1024 * 1024,
            }),
        }
    }

    fn slices(fit: usize, calibration: usize, min_rows: usize) -> Slices {
        Slices {
            fit,
            calibration,
            min_rows,
        }
    }

    fn report(trained: &Trained, usage: Usage) -> String {
        Report {
            at: at(),
            trained,
            usage,
        }
        .to_string()
    }

    #[rstest]
    fn a_published_model_set_reports_its_directory_and_every_figure() {
        let trained = Trained {
            outcome: Outcome::Published {
                model_set: PathBuf::from("models/key/2026-10-23T00:00:00Z"),
            },
            reached: Reached {
                days: Some(DaysRead {
                    built: 1,
                    cached: 2,
                }),
                sides: Some(slices(34, 12, 1)),
                out_of_fold: Some(slices(33, 11, 1)),
                meta: Some(slices(18, 6, 2)),
            },
        };

        assert_eq!(
            report(&trained, usage()),
            "the train at 2026-10-23T00:00:00Z: published the model set \
             models/key/2026-10-23T00:00:00Z\n\
             \x20 Wall time: 2.500 s\n\
             \x20 CPU time: 1.250 s user, 0.125 s system\n\
             \x20 Peak memory: 150.0 MiB\n\
             \x20 Dataset days: 1 built, 2 cached\n\
             \x20 Rows of the sides: 34 fit, 12 calibration, min_rows 1\n\
             \x20 Rows of the out-of-fold pair: 33 fit, 11 calibration, min_rows 1\n\
             \x20 Rows of the meta: 18 fit, 6 calibration, min_rows 2\n"
        );
    }

    /// A failure at the out-of-fold pair reports its reason and the figures reached, with no sides
    /// or meta.
    #[rstest]
    fn a_failed_train_reports_its_reason_and_the_figures_it_reached() {
        let trained = Trained {
            outcome: Outcome::Failed(Failure::Error("0 rows in the fit slice".to_string())),
            reached: Reached {
                days: Some(DaysRead {
                    built: 3,
                    cached: 0,
                }),
                out_of_fold: Some(slices(0, 12, 1)),
                ..Reached::default()
            },
        };

        assert_eq!(
            report(&trained, usage()),
            "the train at 2026-10-23T00:00:00Z: failed: 0 rows in the fit slice\n\
             \x20 Wall time: 2.500 s\n\
             \x20 CPU time: 1.250 s user, 0.125 s system\n\
             \x20 Peak memory: 150.0 MiB\n\
             \x20 Dataset days: 3 built, 0 cached\n\
             \x20 Rows of the out-of-fold pair: 0 fit, 12 calibration, min_rows 1\n"
        );
    }

    /// When already published, no day or row is read. Off Unix, there is no CPU time or peak
    /// memory.
    #[rstest]
    fn a_train_of_a_model_set_already_published_reports_its_times_alone() {
        let trained = Trained::from(Outcome::AlreadyPublished);
        let unmeasured = Usage {
            process: None,
            ..usage()
        };

        assert_eq!(
            report(&trained, unmeasured),
            "the train at 2026-10-23T00:00:00Z: already published\n\
             \x20 Wall time: 2.500 s\n\
             \x20 CPU time: not measured off Unix\n\
             \x20 Peak memory: not measured off Unix\n"
        );
    }
}
