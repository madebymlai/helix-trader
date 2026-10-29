//! Live retraining: how a live node starts a train at a time, and keeps at
//! most one running per strategy.
//!
//! A `Launcher` starts a train at `t` (`train::Training::train_at`) off the
//! node's thread and hands back a `Running`, which the node polls without
//! waiting: in production a child process, the binary's own `train`
//! subcommand with `--at t` (`Launcher::process`), since a train replays
//! the dataset days it lacks through a Nautilus node of its own and a
//! process holds one node; in tests a thread of this process
//! (`Launcher::in_process`). The child's exit status says what it did:
//! success is a published model, a failure the current one kept, the
//! reason the error it printed last (`train` exits with an error when it
//! keeps the model).
//!
//! `Retraining` is a live model handle's side of it: a Nautilus timer
//! named after the strategy (`RETRAIN-<strategy id>`), started on an
//! epoch-aligned time, fires at every multiple of the retrain interval
//! since the epoch, `delay` late, the time the data the node received up to
//! then takes to reach the catalog; each firing launches the train at its
//! grid time, unless one is still running: that one is skipped, with a
//! warning. The handle polls it at each decision: a finished retrain is
//! logged, as info when it published, as an error with its reason when it
//! kept the current model, and the next may start. The model a retrain
//! publishes is the handle's from its next decision, which lists the store
//! again (`handle`).
//!
//! A child still running when the node stops runs on to its end: its model
//! is published, written whole, for the next start.

use std::{
    cell::RefCell,
    collections::VecDeque,
    ffi::OsString,
    fmt,
    io::{BufRead, BufReader, Write},
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        Arc,
        mpsc::{self, Receiver, TryRecvError},
    },
};

use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, DataActorNative},
    timer::TimeEvent,
};
use nautilus_core::{DurationNanos, UnixNanos};
use nautilus_model::identifiers::StrategyId;

use super::train::Outcome;

/// The most lines of a child's standard error kept for the reason it
/// failed: the error it printed last is at their end.
const STDERR_TAIL: usize = 32;

/// Starts trains at a time, off the node's thread.
#[derive(Clone)]
pub struct Launcher {
    train: Arc<dyn Fn(Timestamp) -> Retrained + Send + Sync>,
}

/// A train started at `at`, running or finished.
#[derive(Debug)]
pub struct Running {
    at: Timestamp,
    finished: Receiver<Retrained>,
}

/// What a finished train did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Retrained {
    /// It published a model, valid from its time.
    Published,
    /// It kept the current model; why, or why it failed.
    Kept(String),
}

impl Launcher {
    /// Trains in a child process: `program` run with `args`, then `--at`
    /// and the time. Its standard output is the node's, its standard error
    /// too, line by line, its last lines kept for the reason it failed.
    #[must_use]
    pub fn process(program: PathBuf, args: Vec<OsString>) -> Self {
        Self {
            train: Arc::new(move |at| {
                let mut command = Command::new(&program);
                command
                    .args(&args)
                    .arg("--at")
                    .arg(at.to_string())
                    .stdin(Stdio::null())
                    .stderr(Stdio::piped());
                child(&mut command)
            }),
        }
    }

    /// Trains on a thread of this process, by `train`: for tests, which
    /// hold no node on that thread or replay none.
    #[must_use]
    pub fn in_process(train: impl Fn(Timestamp) -> Outcome + Send + Sync + 'static) -> Self {
        Self {
            train: Arc::new(move |at| match train(at) {
                Outcome::Published { .. } => Retrained::Published,
                kept @ Outcome::Kept(_) => Retrained::Kept(kept.to_string()),
            }),
        }
    }

    /// Starts the train at `at`.
    #[must_use]
    pub fn launch(&self, at: Timestamp) -> Running {
        let (sender, finished) = mpsc::channel();
        let train = Arc::clone(&self.train);
        std::thread::spawn(move || {
            // The receiver gone, nobody waits for the outcome.
            let _ = sender.send(train(at));
        });
        Running { at, finished }
    }
}

/// Runs the child of `command`, its standard error piped, to its end.
fn child(command: &mut Command) -> Retrained {
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(e) => return Retrained::Kept(format!("the train could not start: {e}")),
    };
    let mut tail = VecDeque::with_capacity(STDERR_TAIL);
    if let Some(stderr) = child.stderr.take() {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            // Passed on as it comes; a closed standard error loses it.
            let _ = writeln!(std::io::stderr(), "{line}");
            if tail.len() == STDERR_TAIL {
                tail.pop_front();
            }
            tail.push_back(line);
        }
    }
    match child.wait() {
        Ok(status) if status.success() => Retrained::Published,
        Ok(status) => Retrained::Kept(failure(tail.make_contiguous(), &status.to_string())),
        Err(e) => Retrained::Kept(format!("waiting for the train: {e}")),
    }
}

/// Why a child failed, from the last lines of its standard error, `tail`,
/// and its exit `status`: the error it printed last (`Error: ` and what
/// follows), else the status.
fn failure(tail: &[String], status: &str) -> String {
    match tail.iter().rposition(|line| line.starts_with("Error: ")) {
        Some(start) => tail[start..]
            .iter()
            .map(|line| line.trim())
            .filter(|line| !line.is_empty())
            .collect::<Vec<_>>()
            .join(" ")
            .trim_start_matches("Error: ")
            .to_string(),
        None => format!("the train failed ({status})"),
    }
}

impl Running {
    /// The time trained at.
    #[must_use]
    pub const fn at(&self) -> Timestamp {
        self.at
    }

    /// What the train did, once it finished; `None` while it runs. Asked
    /// again after it answered, a train is kept, having no outcome left.
    #[must_use]
    pub fn finished(&self) -> Option<Retrained> {
        match self.finished.try_recv() {
            Ok(retrained) => Some(retrained),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => Some(ended()),
        }
    }

    /// Waits for the train to finish: what it did.
    #[must_use]
    pub fn wait(self) -> Retrained {
        self.finished.recv().unwrap_or_else(|_| ended())
    }
}

/// A train whose thread ended without an outcome: it panicked.
fn ended() -> Retrained {
    Retrained::Kept("the train ended without an outcome".to_string())
}

/// A live handle's retrains: at most one running, launched by a timer
/// keyed by the strategy.
#[derive(Debug)]
pub(super) struct Retraining {
    timer: String,
    every: SignedDuration,
    delay: SignedDuration,
    launcher: Launcher,
    running: RefCell<Option<Running>>,
}

impl Retraining {
    /// The retrains of `strategy_id`, `every` interval, each launched
    /// `delay` after its grid time by `launcher`.
    pub(super) fn new(
        strategy_id: StrategyId,
        every: SignedDuration,
        delay: SignedDuration,
        launcher: Launcher,
    ) -> Self {
        Self {
            timer: timer(strategy_id),
            every,
            delay,
            launcher,
            running: RefCell::default(),
        }
    }

    /// Starts the retrain timer on `actor`, on the last grid time not
    /// after now, `delay` late, so it first fires on the next one.
    ///
    /// # Errors
    ///
    /// Returns an error if Nautilus refuses the timer.
    pub(super) fn start<A: DataActor + DataActorNative>(&self, actor: &A) -> anyhow::Result<()> {
        let clock = actor.clock();
        let start = last_grid_time(clock.timestamp_ns(), self.every, self.delay)?;
        clock.set_timer_ns(
            &self.timer,
            DurationNanos::try_from(self.every)?,
            Some(start),
            None,
            None,
            Some(true),
            Some(false),
        )
    }

    /// Launches the retrain at the grid time of `event`, if it is the
    /// retrain timer's, unless one is running: then skipped, with a
    /// warning.
    pub(super) fn on_time_event(&self, event: &TimeEvent) {
        if event.name != self.timer.as_str() {
            return;
        }
        let at = Timestamp::from(event.ts_event) - self.delay;
        self.poll();
        let mut running = self.running.borrow_mut();
        if let Some(running) = running.as_ref() {
            log::warn!(
                "the retrain at {at} skipped: the retrain at {} is still running",
                running.at()
            );
            return;
        }
        log::info!("retraining at {at}");
        *running = Some(self.launcher.launch(at));
    }

    /// Logs the running retrain if it finished, and makes way for the next.
    pub(super) fn poll(&self) {
        let finished = self.running.borrow().as_ref().and_then(|running| {
            running
                .finished()
                .map(|retrained| (running.at(), retrained))
        });
        let Some((at, retrained)) = finished else {
            return;
        };
        *self.running.borrow_mut() = None;
        match retrained {
            Retrained::Published => {
                log::info!("the retrain at {at} published a model valid from {at}")
            }
            Retrained::Kept(why) => log::error!("the retrain at {at}: {why}"),
        }
    }
}

/// The name of the retrain timer of `strategy_id`.
fn timer(strategy_id: StrategyId) -> String {
    format!("RETRAIN-{strategy_id}")
}

/// The last multiple of `every` since the epoch, plus `delay`, not after
/// `now`.
fn last_grid_time(
    now: UnixNanos,
    every: SignedDuration,
    delay: SignedDuration,
) -> anyhow::Result<UnixNanos> {
    let (every, delay) = (every.as_nanos(), delay.as_nanos());
    let start = (i128::from(now.as_u64()) - delay).div_euclid(every) * every + delay;
    Ok(UnixNanos::from(u64::try_from(start)?))
}

impl fmt::Debug for Launcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Launcher").finish_non_exhaustive()
    }
}

impl fmt::Display for Retrained {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Published => write!(f, "published a model"),
            Self::Kept(why) => write!(f, "{why}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::model::train::Kept;

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    fn published(at: Timestamp) -> Outcome {
        Outcome::Published {
            valid_from: at,
            score: 0.6,
        }
    }

    /// A child of `sh` running `script`, `--at` and the time its `$1` and
    /// `$2`.
    fn shell(script: &str) -> Launcher {
        Launcher::process(
            PathBuf::from("sh"),
            vec!["-c".into(), script.into(), "sh".into()],
        )
    }

    #[test]
    fn an_in_process_train_publishing_is_published() {
        let launcher = Launcher::in_process(published);

        assert_eq!(
            launcher.launch(at("2026-10-22T00:00:00Z")).wait(),
            Retrained::Published
        );
    }

    #[test]
    fn an_in_process_train_keeping_the_model_is_kept_with_its_reason() {
        let launcher = Launcher::in_process(|_| {
            Outcome::Kept(Kept::BelowMinScore {
                score: 0.48,
                min_score: 0.55,
            })
        });

        let Retrained::Kept(why) = launcher.launch(at("2026-10-22T00:00:00Z")).wait() else {
            panic!("published");
        };
        assert!(why.contains("0.48") && why.contains("0.55"), "{why}");
    }

    #[test]
    fn a_panicking_in_process_train_is_kept() {
        let launcher = Launcher::in_process(|_| panic!("the train panicked"));

        assert!(matches!(
            launcher.launch(at("2026-10-22T00:00:00Z")).wait(),
            Retrained::Kept(_)
        ));
    }

    #[test]
    fn a_child_is_told_the_time_and_succeeding_publishes() {
        let launcher = shell(r#"[ "$1" = --at ] && [ "$2" = 2026-10-22T00:00:00Z ]"#);

        assert_eq!(
            launcher.launch(at("2026-10-22T00:00:00Z")).wait(),
            Retrained::Published
        );
    }

    #[test]
    fn a_failing_child_is_kept_with_the_error_it_printed_last() {
        let launcher = shell(
            "echo 'ERROR a log line of the replay' >&2; \
             echo 'Error: kept the current model: holdout AUC 0.48 below the minimum 0.55' >&2; \
             exit 1",
        );

        assert_eq!(
            launcher.launch(at("2026-10-22T00:00:00Z")).wait(),
            Retrained::Kept(
                "kept the current model: holdout AUC 0.48 below the minimum 0.55".to_string()
            )
        );
    }

    #[test]
    fn a_failing_child_printing_no_error_is_kept_with_its_status() {
        let launcher = shell("exit 3");

        let Retrained::Kept(why) = launcher.launch(at("2026-10-22T00:00:00Z")).wait() else {
            panic!("published");
        };
        assert!(why.contains('3'), "{why}");
    }

    #[test]
    fn a_child_that_cannot_start_is_kept() {
        let launcher = Launcher::process(PathBuf::from("/nonexistent/helix-trader"), vec![]);

        let Retrained::Kept(why) = launcher.launch(at("2026-10-22T00:00:00Z")).wait() else {
            panic!("published");
        };
        assert!(why.contains("could not start"), "{why}");
    }

    /// The event of the retrain timer of `strategy` at `time`.
    fn fired(strategy: &str, time: &str) -> TimeEvent {
        let ns = UnixNanos::from(at(time));
        TimeEvent::new(
            timer(StrategyId::from(strategy)).as_str().into(),
            nautilus_core::UUID4::new(),
            ns,
            ns,
        )
    }

    /// The retrains of `LIVE-001`, daily, launched on time by `launcher`.
    fn daily(launcher: Launcher) -> Retraining {
        Retraining::new(
            StrategyId::from("LIVE-001"),
            SignedDuration::from_hours(24),
            SignedDuration::ZERO,
            launcher,
        )
    }

    #[test]
    fn a_retrain_due_while_one_runs_is_skipped() {
        let (release, released) = mpsc::channel::<()>();
        let released = Mutex::new(released);
        let (finish, finished) = mpsc::channel();
        let retraining = daily(Launcher::in_process(move |at| {
            released.lock().unwrap().recv().unwrap();
            finish.send(at).unwrap();
            published(at)
        }));

        retraining.on_time_event(&fired("LIVE-001", "2026-10-22T00:00:00Z"));
        retraining.on_time_event(&fired("LIVE-001", "2026-10-23T00:00:00Z"));
        release.send(()).unwrap();
        release.send(()).unwrap();

        assert_eq!(finished.recv().unwrap(), at("2026-10-22T00:00:00Z"));
        // Released twice, a second train would send its time before its
        // launcher is dropped.
        drop(retraining);
        assert_eq!(finished.recv(), Err(mpsc::RecvError));
    }

    #[test]
    fn a_finished_retrain_makes_way_for_the_next() {
        let (finish, finished) = mpsc::channel();
        let retraining = daily(Launcher::in_process(move |at| {
            finish.send(at).unwrap();
            published(at)
        }));
        retraining.on_time_event(&fired("LIVE-001", "2026-10-22T00:00:00Z"));
        assert_eq!(finished.recv().unwrap(), at("2026-10-22T00:00:00Z"));
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);

        // The first's outcome reaches the handle just after it finishes:
        // the second fires until it is launched.
        let second = loop {
            assert!(std::time::Instant::now() < deadline, "never launched");
            retraining.on_time_event(&fired("LIVE-001", "2026-10-23T00:00:00Z"));
            if let Ok(at) = finished.recv_timeout(std::time::Duration::from_millis(10)) {
                break at;
            }
        };

        assert_eq!(second, at("2026-10-23T00:00:00Z"));
    }

    #[test]
    fn another_strategys_retrain_timer_launches_nothing() {
        let (finish, finished) = mpsc::channel();
        let retraining = daily(Launcher::in_process(move |at| {
            finish.send(at).unwrap();
            published(at)
        }));

        retraining.on_time_event(&fired("LIVE-002", "2026-10-22T00:00:00Z"));
        drop(retraining);

        assert_eq!(finished.recv(), Err(mpsc::RecvError));
    }

    #[test]
    fn a_retrain_fired_late_trains_at_its_grid_time() {
        let (finish, finished) = mpsc::channel();
        let retraining = Retraining::new(
            StrategyId::from("LIVE-001"),
            SignedDuration::from_hours(24),
            SignedDuration::from_mins(6),
            Launcher::in_process(move |at| {
                finish.send(at).unwrap();
                published(at)
            }),
        );

        retraining.on_time_event(&fired("LIVE-001", "2026-10-22T00:06:00Z"));

        assert_eq!(finished.recv().unwrap(), at("2026-10-22T00:00:00Z"));
    }

    #[test]
    fn the_grid_time_is_the_last_multiple_since_the_epoch_plus_the_delay() {
        let last = |now: &str| {
            Timestamp::from(
                last_grid_time(
                    UnixNanos::from(at(now)),
                    SignedDuration::from_hours(24),
                    SignedDuration::from_mins(6),
                )
                .unwrap(),
            )
        };

        assert_eq!(last("2026-10-22T13:00:00Z"), at("2026-10-22T00:06:00Z"));
        assert_eq!(last("2026-10-22T00:06:00Z"), at("2026-10-22T00:06:00Z"));
        assert_eq!(last("2026-10-22T00:05:59Z"), at("2026-10-21T00:06:00Z"));
    }
}
