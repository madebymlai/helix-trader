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
//! success is a train that kept no model, each published or its current
//! one in use; a failure, a current model kept, the reason the error it
//! printed last (`train` exits with an error when it keeps a model).
//!
//! `Retraining` is a live model handle's side of it: a Nautilus timer
//! named after the strategy (`schedule::timer`), started on an
//! epoch-aligned time (`schedule::start`), fires at every multiple of the
//! retrain interval since the epoch, and, never retrained, is not started,
//! so no train is launched; each firing launches a train at once,
//! at the last time the catalog holds for every series the strategy reads
//! (the earliest of their last stored timestamps, Nautilus'
//! `ParquetDataCatalog::query_last_timestamp`), never past the grid time:
//! live, the data up to the last daily promotion (`live::Recording`). A
//! firing is skipped, with a warning, if the catalog holds nothing of a
//! series read, if that time is not past the previous retrain's, the
//! catalog having gained nothing since, or if a retrain is still running.
//! The handle polls it at each decision: a finished retrain is
//! logged, as info when it succeeded, as an error with its reason when it
//! kept a current model, and the next may start. The model a retrain
//! publishes is the handle's from its next decision, which lists the store
//! again (`handle`).
//!
//! A child still running when the node stops runs on to its end: its model
//! is published, written whole, for the next start.

use std::{
    cell::{Cell, RefCell},
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

use anyhow::Context;
use jiff::Timestamp;
use nautilus_common::{
    actor::{DataActor, DataActorNative},
    timer::TimeEvent,
};
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::StrategyId;
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, catalog::types::data_path_prefix,
};

use super::{
    schedule::{self, Retrain},
    train::Trained,
};
use crate::strategy::DataRequirements;

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
    /// It kept no model: each model it trained was published, valid from
    /// its time, or its current one is in use.
    Succeeded,
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
    pub fn in_process(train: impl Fn(Timestamp) -> Trained + Send + Sync + 'static) -> Self {
        Self {
            train: Arc::new(move |at| {
                let trained = train(at);
                if trained.succeeded() {
                    Retrained::Succeeded
                } else {
                    Retrained::Kept(trained.to_string())
                }
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
        Ok(status) if status.success() => Retrained::Succeeded,
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
    retrain: Retrain,
    catalog: PathBuf,
    requirements: DataRequirements,
    launcher: Launcher,
    running: RefCell<Option<Running>>,
    /// The time the last retrain launched trains at.
    previous: Cell<Option<Timestamp>>,
}

impl Retraining {
    /// The retrains of `strategy_id`, retrained as `retrain` says, of a
    /// strategy of `requirements` reading the catalog at `catalog`,
    /// launched by `launcher`.
    pub(super) fn new(
        strategy_id: StrategyId,
        retrain: Retrain,
        catalog: PathBuf,
        requirements: DataRequirements,
        launcher: Launcher,
    ) -> Self {
        Self {
            timer: schedule::timer(strategy_id),
            retrain,
            catalog,
            requirements,
            launcher,
            running: RefCell::default(),
            previous: Cell::default(),
        }
    }

    /// Starts the retrain timer on `actor` (`schedule::start`): retrained,
    /// on the last grid time not after now, so it first fires on the next
    /// one; never retrained, none, so no retrain is launched.
    ///
    /// # Errors
    ///
    /// Returns an error if Nautilus refuses the timer.
    pub(super) fn start<A: DataActor + DataActorNative>(&self, actor: &A) -> anyhow::Result<()> {
        schedule::start(actor, &self.timer, self.retrain)
    }

    /// Launches the retrain due at the grid time of `event`, if it is the
    /// retrain timer's, at the catalog's end then (`stored_until`). Skipped,
    /// with a warning, if the catalog holds nothing of a series read, if
    /// its end is not past the previous retrain's time, or if one is
    /// running.
    pub(super) fn on_time_event(&self, event: &TimeEvent) {
        if event.name != self.timer.as_str() {
            return;
        }
        let due = Timestamp::from(event.ts_event);
        let at = match self.stored_until(due) {
            Ok(at) => at,
            Err(e) => {
                log::warn!("the retrain at {due} skipped: {e:#}");
                return;
            }
        };
        if let Some(previous) = self.previous.get()
            && at <= previous
        {
            log::warn!(
                "the retrain at {due} skipped: the catalog holds nothing past {previous}, \
                 the previous retrain's time"
            );
            return;
        }
        self.poll();
        let mut running = self.running.borrow_mut();
        if let Some(running) = running.as_ref() {
            log::warn!(
                "the retrain at {due} skipped: the retrain at {} is still running",
                running.at()
            );
            return;
        }
        log::info!("retraining at {at}, the catalog's end, for the retrain due at {due}");
        self.previous.set(Some(at));
        *running = Some(self.launcher.launch(at));
    }

    /// The time a retrain due at `due` trains at: the last time the
    /// catalog holds for every series read at `due`, the earliest of their
    /// last stored timestamps, or `due` if that is past it.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be read, or holds nothing of
    /// a series read.
    fn stored_until(&self, due: Timestamp) -> anyhow::Result<Timestamp> {
        let catalog = ParquetDataCatalog::new(&self.catalog, None, None, None, None);
        let mut until = due;
        for slice in self.requirements.slices(due, due) {
            let (data_type, identifier) = &slice.series;
            let last = catalog
                .query_last_timestamp(&data_type.clone().into(), Some(identifier))
                .with_context(|| {
                    format!(
                        "reading the stored range of the {} of {identifier}",
                        data_path_prefix(data_type)
                    )
                })?
                .with_context(|| {
                    format!(
                        "the catalog holds no {} of {identifier}",
                        data_path_prefix(data_type)
                    )
                })?;
            until = until.min(Timestamp::from(UnixNanos::from(last)));
        }
        Ok(until)
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
            Retrained::Succeeded => {
                log::info!("the retrain at {at} succeeded, its models published or in use")
            }
            Retrained::Kept(why) => log::error!("the retrain at {at}: {why}"),
        }
    }
}

impl fmt::Debug for Launcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Launcher").finish_non_exhaustive()
    }
}

impl fmt::Display for Retrained {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Succeeded => write!(f, "succeeded"),
            Self::Kept(why) => write!(f, "{why}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{path::Path, sync::Mutex};

    use jiff::SignedDuration;
    use nautilus_model::{
        data::{NautilusDataType, QuoteTick},
        identifiers::InstrumentId,
        types::{Price, Quantity},
    };

    use super::*;
    use crate::{
        model::{
            holdout::{Earnings, Holdout},
            train::Outcome,
        },
        strategy::TickSeries,
    };

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    /// A holdout of no trade.
    fn holdout() -> Holdout {
        Holdout {
            earnings: Earnings::default(),
            log_loss: 0.6,
            baseline_log_loss: 0.69,
            auc: Some(0.6),
        }
    }

    fn published(at: Timestamp) -> Trained {
        Outcome::Published {
            valid_from: at,
            holdout: holdout(),
        }
        .into()
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
    fn a_panicking_in_process_train_is_kept() {
        let launcher = Launcher::in_process(|_| panic!("the train panicked"));

        assert!(matches!(
            launcher.launch(at("2026-10-22T00:00:00Z")).wait(),
            Retrained::Kept(_)
        ));
    }

    /// A train finding its model published at its time keeps it in use:
    /// it kept no model, and succeeded.
    #[test]
    fn an_in_process_train_whose_model_is_in_use_succeeds() {
        let launcher = Launcher::in_process(|at| {
            Outcome::InUse {
                valid_from: at,
                retrain: Retrain::Every(SignedDuration::from_hours(24)),
            }
            .into()
        });

        assert_eq!(
            launcher.launch(at("2026-10-22T00:00:00Z")).wait(),
            Retrained::Succeeded
        );
    }

    #[test]
    fn a_child_is_told_the_time_and_its_success_is_the_retrains() {
        let launcher = shell(r#"[ "$1" = --at ] && [ "$2" = 2026-10-22T00:00:00Z ]"#);

        assert_eq!(
            launcher.launch(at("2026-10-22T00:00:00Z")).wait(),
            Retrained::Succeeded
        );
    }

    #[test]
    fn a_failing_child_is_kept_with_the_error_it_printed_last() {
        let launcher = shell(
            "echo 'ERROR a log line of the replay' >&2; \
             echo 'Error: kept the current model: holdout net P&L per trade taken 0.48 below the minimum 0.55' >&2; \
             exit 1",
        );

        assert_eq!(
            launcher.launch(at("2026-10-22T00:00:00Z")).wait(),
            Retrained::Kept(
                "kept the current model: holdout net P&L per trade taken 0.48 below the minimum 0.55".to_string()
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
            schedule::timer(StrategyId::from(strategy)).as_str().into(),
            nautilus_core::UUID4::new(),
            ns,
            ns,
        )
    }

    const AUD: &str = "AUD/USD.SIM";
    const EUR: &str = "EUR/USD.SIM";

    /// Writes a quote of each of `quotes`, an instrument and its time, to
    /// the catalog at `catalog`.
    fn stored(catalog: &Path, quotes: &[(&str, &str)]) {
        let catalog = ParquetDataCatalog::new(catalog, None, None, None, None);
        for (instrument_id, time) in quotes {
            let ts = UnixNanos::from(at(time));
            let quote = QuoteTick::new(
                InstrumentId::from(*instrument_id),
                Price::from("0.69990"),
                Price::from("0.70010"),
                Quantity::from(1),
                Quantity::from(1),
                ts,
                ts,
            );
            catalog
                .write_to_parquet(&[quote], None, None, None)
                .unwrap();
        }
    }

    /// The retrains of `LIVE-001`, daily, of a strategy reading the quotes
    /// of AUD/USD and EUR/USD from the catalog at `catalog`, launched by
    /// `launcher`.
    fn daily_on(catalog: &Path, launcher: Launcher) -> Retraining {
        let quotes = |instrument_id| TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: InstrumentId::from(instrument_id),
        };
        Retraining::new(
            StrategyId::from("LIVE-001"),
            Retrain::Every(SignedDuration::from_hours(24)),
            catalog.to_path_buf(),
            DataRequirements {
                bar_types: vec![],
                ticks: vec![quotes(AUD), quotes(EUR)],
                warmup: SignedDuration::ZERO,
            },
            launcher,
        )
    }

    /// `daily_on` a catalog holding both series well past every retrain
    /// of the tests, kept in `dir`.
    fn daily(dir: &tempfile::TempDir, launcher: Launcher) -> Retraining {
        stored(
            dir.path(),
            &[(AUD, "2026-12-01T00:00:00Z"), (EUR, "2026-12-01T00:00:00Z")],
        );
        daily_on(dir.path(), launcher)
    }

    /// A launcher handing each time it trains at to `finish`.
    fn sending(finish: mpsc::Sender<Timestamp>) -> Launcher {
        Launcher::in_process(move |at| {
            finish.send(at).unwrap();
            published(at)
        })
    }

    #[test]
    fn a_retrain_due_while_one_runs_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let (release, released) = mpsc::channel::<()>();
        let released = Mutex::new(released);
        let (finish, finished) = mpsc::channel();
        let retraining = daily(
            &dir,
            Launcher::in_process(move |at| {
                released.lock().unwrap().recv().unwrap();
                finish.send(at).unwrap();
                published(at)
            }),
        );

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

    /// Polls `retraining` until the outcome of the train it runs reached
    /// it, making way for the next. A train's outcome is sent just after
    /// the train returns, off this thread, so it may come after the time
    /// the train sent; the handle polls at each decision.
    fn polled_to_its_end(retraining: &Retraining) {
        while retraining.running.borrow().is_some() {
            retraining.poll();
            std::thread::yield_now();
        }
    }

    /// Fires the retrain timer of `retraining` at `time` until a train
    /// sends its time to `finished`: that time. The outcome of a train
    /// reaches the handle just after the train sent its time, so a retrain
    /// fired right after may be skipped as running. Panics after a minute,
    /// so that a retrain never launched fails rather than hangs.
    fn fire_until_launched(
        retraining: &Retraining,
        finished: &Receiver<Timestamp>,
        time: &str,
    ) -> Timestamp {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        loop {
            retraining.on_time_event(&fired("LIVE-001", time));
            if let Ok(at) = finished.recv_timeout(std::time::Duration::from_millis(10)) {
                return at;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "no retrain launched at {time} within a minute of firing"
            );
        }
    }

    #[test]
    fn a_finished_retrain_makes_way_for_the_next() {
        let dir = tempfile::tempdir().unwrap();
        let (finish, finished) = mpsc::channel();
        let retraining = daily(&dir, sending(finish));
        retraining.on_time_event(&fired("LIVE-001", "2026-10-22T00:00:00Z"));
        assert_eq!(finished.recv().unwrap(), at("2026-10-22T00:00:00Z"));

        let second = fire_until_launched(&retraining, &finished, "2026-10-23T00:00:00Z");

        assert_eq!(second, at("2026-10-23T00:00:00Z"));
    }

    #[test]
    fn another_strategys_retrain_timer_launches_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let (finish, finished) = mpsc::channel();
        let retraining = daily(&dir, sending(finish));

        retraining.on_time_event(&fired("LIVE-002", "2026-10-22T00:00:00Z"));
        drop(retraining);

        assert_eq!(finished.recv(), Err(mpsc::RecvError));
    }

    #[test]
    fn a_retrain_trains_at_its_time_when_the_catalog_holds_past_it() {
        let dir = tempfile::tempdir().unwrap();
        let (finish, finished) = mpsc::channel();
        let retraining = daily(&dir, sending(finish));

        retraining.on_time_event(&fired("LIVE-001", "2026-10-22T00:00:00Z"));

        assert_eq!(finished.recv().unwrap(), at("2026-10-22T00:00:00Z"));
    }

    #[test]
    fn a_retrain_trains_at_the_catalogs_end_when_it_ends_before_its_time() {
        let dir = tempfile::tempdir().unwrap();
        stored(
            dir.path(),
            &[(AUD, "2026-10-21T13:00:00Z"), (EUR, "2026-10-21T13:00:00Z")],
        );
        let (finish, finished) = mpsc::channel();
        let retraining = daily_on(dir.path(), sending(finish));

        retraining.on_time_event(&fired("LIVE-001", "2026-10-22T00:00:00Z"));

        assert_eq!(finished.recv().unwrap(), at("2026-10-21T13:00:00Z"));
    }

    #[test]
    fn the_catalogs_end_is_the_earliest_last_time_of_the_series_read() {
        let dir = tempfile::tempdir().unwrap();
        stored(
            dir.path(),
            &[(AUD, "2026-10-21T13:00:00Z"), (EUR, "2026-10-21T09:30:00Z")],
        );
        let (finish, finished) = mpsc::channel();
        let retraining = daily_on(dir.path(), sending(finish));

        retraining.on_time_event(&fired("LIVE-001", "2026-10-22T00:00:00Z"));

        assert_eq!(finished.recv().unwrap(), at("2026-10-21T09:30:00Z"));
    }

    #[test]
    fn a_retrain_is_skipped_when_the_catalog_holds_nothing_of_a_series_read() {
        let dir = tempfile::tempdir().unwrap();
        stored(dir.path(), &[(AUD, "2026-10-21T13:00:00Z")]);
        let (finish, finished) = mpsc::channel();
        let retraining = daily_on(dir.path(), sending(finish));

        retraining.on_time_event(&fired("LIVE-001", "2026-10-22T00:00:00Z"));
        drop(retraining);

        assert_eq!(finished.recv(), Err(mpsc::RecvError));
    }

    #[test]
    fn a_retrain_is_skipped_when_the_catalog_gained_nothing_since_the_previous() {
        let dir = tempfile::tempdir().unwrap();
        stored(
            dir.path(),
            &[(AUD, "2026-10-21T13:00:00Z"), (EUR, "2026-10-21T13:00:00Z")],
        );
        let (finish, finished) = mpsc::channel();
        let retraining = daily_on(dir.path(), sending(finish));
        retraining.on_time_event(&fired("LIVE-001", "2026-10-22T00:00:00Z"));
        assert_eq!(finished.recv().unwrap(), at("2026-10-21T13:00:00Z"));
        polled_to_its_end(&retraining);

        retraining.on_time_event(&fired("LIVE-001", "2026-10-23T00:00:00Z"));
        drop(retraining);

        assert_eq!(finished.recv(), Err(mpsc::RecvError));
    }

    #[test]
    fn a_retrain_trains_at_what_the_catalog_gained_since_the_previous() {
        let dir = tempfile::tempdir().unwrap();
        stored(
            dir.path(),
            &[(AUD, "2026-10-21T13:00:00Z"), (EUR, "2026-10-21T13:00:00Z")],
        );
        let (finish, finished) = mpsc::channel();
        let retraining = daily_on(dir.path(), sending(finish));
        retraining.on_time_event(&fired("LIVE-001", "2026-10-22T00:00:00Z"));
        assert_eq!(finished.recv().unwrap(), at("2026-10-21T13:00:00Z"));
        stored(
            dir.path(),
            &[(AUD, "2026-10-22T13:00:00Z"), (EUR, "2026-10-22T13:00:00Z")],
        );

        let second = fire_until_launched(&retraining, &finished, "2026-10-23T00:00:00Z");

        assert_eq!(second, at("2026-10-22T13:00:00Z"));
    }
}
