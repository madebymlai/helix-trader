//! Live retraining: how a live node starts a train at a time, keeping at most one running per
//! strategy.
//!
//! A `Launcher` starts a train at `t` (`train::Training::train_at`) off the node's thread and hands
//! back a `Running`:
//! - In production, a child process running the binary's `train --at t` (`Launcher::process`): a
//!   train replays the dataset days it lacks through a Nautilus node of its own, and Nautilus runs
//!   no second node in a process while the live node runs.
//! - In tests, a thread of this process (`Launcher::in_process`).
//!
//! A backtest holds no node while it trains, so it trains in its own process and starts no
//! launcher.
//!
//! The child's exit status says what it did. A failure published nothing, and its reason is the
//! error the child printed last. Each line the child prints is a log record of the node's process.
//!
//! `RetrainActor` is a live node's retrains of one strategy, named `RETRAIN-` and the strategy's
//! id, as Nautilus refuses two actors of one id. Its timer fires at every multiple of the retrain
//! interval since the epoch (`schedule::start`). At its start the actor also takes the latest grid
//! time not after now, if the store holds no model set of it. A strategy never retrained has no
//! actor.
//!
//! A retrain at `t` trains as `train --at t` does, so that live and a backtest of the same span
//! share every model set. It waits until Nautilus' `ParquetDataCatalog::query_last_timestamp`
//! returns an end at or after `t` for each bar source and tick series, even if the last row is
//! earlier; gaps in the window are not checked. The catalog is checked again every minute on a
//! second timer, and the previous model set decides meanwhile.
//!
//! A retrain falling due while another waits takes its place, the waiting one skipped with a
//! warning. One falling due while the previous retrain runs is skipped with a warning. A retrain
//! logs its outcome as it ends: info on success, an error with its reason otherwise. A published
//! model set is the strategy's from its next decision (`handle`). A skipped retrain time is
//! trained later, if a backtest needs it, by `train` over its span.
//!
//! A child still running when the node stops runs on to its end and publishes its model set.

use std::{
    collections::VecDeque,
    ffi::OsString,
    fmt::{Debug, Display},
    io::{BufRead, BufReader},
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        Arc,
        mpsc::{self, Receiver, TryRecvError},
    },
};

use anyhow::Context;
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, DataActorConfig, DataActorCore},
    nautilus_actor,
    timer::TimeEvent,
};
use nautilus_core::{DurationNanos, UnixNanos};
use nautilus_model::identifiers::{ActorId, StrategyId};
use nautilus_persistence::{
    backend::parquet::catalog::ParquetDataCatalog, catalog::types::data_path_prefix,
};

#[cfg(test)]
use super::train::Trained;
use super::{
    schedule::{self, Retrain},
    store::ModelStore,
};
use crate::strategy::requirements::DataRequirements;

/// The most lines of a child's standard error kept for the reason it failed.
const STDERR_TAIL: usize = 32;

/// The variable of Nautilus' logger spec (`LoggerConfig::from_spec`).
const LOGGING: &str = "NAUTILUS_LOG";

const TIMER: &str = "RETRAIN";

/// The name of the timer, on the retrain actor's clock, that checks again whether the catalog holds
/// the data of a waiting retrain.
const RECHECK: &str = "RETRAIN-RECHECK";

/// How often a waiting retrain checks the catalog again: a check lists the files of each series
/// read, and the data comes with the live recording's daily promotion (`live::Recording`), at no
/// set time.
const RECHECK_EVERY: SignedDuration = SignedDuration::from_mins(1);

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
    /// Its model set was published, valid from its time, or already published.
    Succeeded,
    /// It failed, publishing nothing, the current model set deciding on; why.
    Failed(String),
}

impl Launcher {
    /// Trains in a child process: `program` run with `args`, then `--at` and the time. Each line of
    /// its output is a log record of this process (`child`); its logger, set by `NAUTILUS_LOG`,
    /// writes plain lines and no file.
    #[must_use]
    pub fn process(program: PathBuf, args: Vec<OsString>) -> Self {
        let mut logging = std::env::var_os(LOGGING).unwrap_or_default();
        logging.push(";is_colored=false;fileout=off");
        Self {
            train: Arc::new(move |at| {
                let mut command = Command::new(&program);
                command
                    .args(&args)
                    .arg("--at")
                    .arg(at.to_string())
                    .env(LOGGING, &logging)
                    .stdin(Stdio::null())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped());
                child(&mut command)
            }),
        }
    }

    /// Trains on a thread of this process, by `train`: for tests, which hold no node on that thread
    /// or replay none.
    #[cfg(test)]
    #[must_use]
    pub fn in_process(train: impl Fn(Timestamp) -> Trained + Send + Sync + 'static) -> Self {
        Self {
            train: Arc::new(move |at| {
                let trained = train(at);
                if trained.succeeded() {
                    Retrained::Succeeded
                } else {
                    Retrained::Failed(trained.outcome.to_string())
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
            // The receiver gone, nobody waits for the outcome
            let _ = sender.send(train(at));
        });
        Running { at, finished }
    }

    fn logged(self) -> Self {
        let train = self.train;
        Self {
            train: Arc::new(move |at| {
                let retrained = train(at);
                match &retrained {
                    Retrained::Succeeded => {
                        log::info!(
                            "the retrain at {at} succeeded, its model set published or already published"
                        );
                    }
                    Retrained::Failed(why) => log::error!("the retrain at {at}: {why}"),
                }
                retrained
            }),
        }
    }
}

/// Runs `command`, its output piped, to its end, logging each line of standard output as info and
/// of standard error as a warning. The last standard error lines are kept for the failure reason.
fn child(command: &mut Command) -> Retrained {
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(e) => return Retrained::Failed(format!("the train could not start: {e}")),
    };

    // Read beside its standard error, so that neither pipe fills while the other is read
    let stdout = child.stdout.take().map(|stdout| {
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                log::info!("{line}");
            }
        })
    });
    let mut tail = VecDeque::with_capacity(STDERR_TAIL);
    if let Some(stderr) = child.stderr.take() {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            log::warn!("{line}");

            if tail.len() == STDERR_TAIL {
                tail.pop_front();
            }
            tail.push_back(line);
        }
    }

    // A reader that panicked logged what it read before
    if let Some(stdout) = stdout {
        let _ = stdout.join();
    }

    match child.wait() {
        Ok(status) if status.success() => Retrained::Succeeded,
        Ok(status) => Retrained::Failed(failure(tail.make_contiguous(), &status.to_string())),
        Err(e) => Retrained::Failed(format!("waiting for the train: {e}")),
    }
}

/// Returns why a child failed: the error it printed last in `tail`, its last standard error lines
/// (`Error: ` and what follows), else its exit `status`.
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

    /// Returns what the train did, once it finished; `None` while it runs. Asked again after it
    /// answered, a train failed, having no outcome left.
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
    Retrained::Failed("the train ended without an outcome".to_string())
}

/// A live node's retrains of a strategy, a Nautilus actor: at most one running.
#[derive(Debug)]
pub struct RetrainActor {
    core: DataActorCore,
    every: SignedDuration,
    catalog: PathBuf,
    requirements: DataRequirements,
    models: ModelStore,
    launcher: Launcher,
    running: Option<Running>,
    /// The retrain time due whose data the catalog lacks, checked again at each `RECHECK`.
    waiting: Option<Timestamp>,
}

impl RetrainActor {
    /// Returns the retrains of `strategy_id` as `retrain` says, none for a strategy never
    /// retrained.
    #[must_use]
    pub fn new(
        strategy_id: StrategyId,
        retrain: Retrain,
        catalog: PathBuf,
        requirements: DataRequirements,
        models: ModelStore,
        launcher: Launcher,
    ) -> Option<Self> {
        let Retrain::Every(every) = retrain else {
            return None;
        };
        let actor_id = ActorId::from(format!("RETRAIN-{strategy_id}").as_str());
        Some(Self {
            core: DataActorCore::new(DataActorConfig {
                actor_id: Some(actor_id),
                ..Default::default()
            }),
            every,
            catalog,
            requirements,
            models,
            launcher: launcher.logged(),
            running: None,
            waiting: None,
        })
    }

    /// Handles the retrain at `at` falling due: skipped if the previous still runs, else it takes
    /// the place of a waiting one (`check`).
    fn due(&mut self, at: Timestamp) -> anyhow::Result<()> {
        if let Some(running) = &self.running
            && running.finished().is_none()
        {
            log::warn!(
                "the retrain at {at} skipped: the retrain at {} is still running",
                running.at()
            );
            return Ok(());
        }

        if let Some(waiting) = self.waiting {
            log::warn!(
                "the retrain at {waiting} skipped: the retrain at {at} is due before the catalog \
                 held the data to {waiting}"
            );
        }
        self.check(at)
    }

    /// Launches the retrain for `at` when `lacking` finds no series behind the cutoff, else
    /// `RECHECK` retries it. The actor logs the wait once and warns on each catalog read error.
    fn check(&mut self, at: Timestamp) -> anyhow::Result<()> {
        match self.lacking(at) {
            Ok(None) => {
                if self.waiting.take().is_some() {
                    self.clock().cancel_timer(RECHECK);
                }
                log::info!("retraining at {at}: every series read is stored to it");
                self.running = Some(self.launcher.launch(at));
                return Ok(());
            }
            Ok(Some(lacking)) => {
                if self.waiting != Some(at) {
                    log::info!(
                        "the retrain at {at} waits for the data: {lacking}; checked again every \
                         {RECHECK_EVERY:#}"
                    );
                }
            }
            Err(e) => log::warn!("the retrain at {at} waits: {e:#}"),
        }

        if self.waiting.replace(at).is_none() {
            self.clock().set_timer_ns(
                RECHECK,
                DurationNanos::try_from(RECHECK_EVERY)?,
                None,
                None,
                None,
                None,
                Some(false),
            )?;
        }
        Ok(())
    }

    /// Returns the first bar source or tick series in `DataRequirements::slices(at, at)` with no
    /// catalog interval or one ending before `at`, with the end if known; `None` if all reach `at`.
    ///
    /// # Errors
    ///
    /// Returns an error if the catalog cannot be read.
    fn lacking(&self, at: Timestamp) -> anyhow::Result<Option<String>> {
        let catalog = ParquetDataCatalog::new(&self.catalog, None, None, None, None);

        for slice in self.requirements.slices(at, at) {
            let (data_type, identifier) = &slice.series.key();
            let series = format!("the {} of {identifier}", data_path_prefix(data_type));
            let last = catalog
                .query_last_timestamp(&data_type.clone().into(), Some(identifier))
                .with_context(|| format!("reading the stored range of {series}"))?
                .map(|last| Timestamp::from(UnixNanos::from(last)));
            match last {
                Some(last) if last >= at => {}
                Some(last) => return Ok(Some(format!("{series} are stored to {last}"))),
                None => return Ok(Some(format!("the catalog holds none of {series}"))),
            }
        }
        Ok(None)
    }
}

impl DataActor for RetrainActor {
    /// Starts the retrain timer (`schedule::start`), and falls the latest retrain time due now if
    /// the store holds no model set of it, as when the node was down then.
    fn on_start(&mut self) -> anyhow::Result<()> {
        schedule::start(&*self, TIMER, self.every)?;
        let latest = Timestamp::from(schedule::last_grid_time(
            self.clock().timestamp_ns(),
            self.every,
        )?);
        let model_sets = self
            .models
            .model_sets()
            .context("listing the model sets, of which the latest retrain time's is trained")?;
        if model_sets.contains(&latest) {
            return Ok(());
        }
        self.due(latest)
    }

    /// Does nothing at stop: a running child runs on to its end and publishes, and Nautilus cancels
    /// the timers at dispose. Nautilus' own `on_stop` would warn at every stop.
    fn on_stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        match event.name.as_str() {
            TIMER => self.due(Timestamp::from(event.ts_event)),
            RECHECK => match self.waiting {
                Some(at) => self.check(at),
                None => Ok(()),
            },
            name => anyhow::bail!("the retrain actor has no timer {name}"),
        }
    }
}

nautilus_actor!(RetrainActor);

impl Debug for Launcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(Launcher)).finish_non_exhaustive()
    }
}

impl Display for Retrained {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Succeeded => write!(f, "succeeded"),
            Self::Failed(why) => write!(f, "{why}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cell::RefCell,
        path::Path,
        rc::Rc,
        sync::Mutex,
        time::{Duration, Instant},
    };

    use nautilus_common::{
        actor::registry::deregister_actor,
        cache::Cache,
        clock::VirtualClock,
        component::{Component, deregister_component, register_component_actor, start_component},
    };
    use nautilus_model::{
        data::{NautilusDataType, QuoteTick},
        identifiers::{InstrumentId, TraderId},
        types::{Price, Quantity},
    };
    use rstest::rstest;
    use ustr::Ustr;

    use super::*;
    use crate::{
        log_capture,
        model::{
            deciding::testing::{baseline, store},
            train::{Failure, Outcome},
        },
        strategy::requirements::TickSeries,
    };

    fn at(time: &str) -> Timestamp {
        time.parse().unwrap()
    }

    fn published(at: Timestamp) -> Trained {
        Outcome::Published {
            model_set: PathBuf::from("models").join(at.to_string()),
        }
        .into()
    }

    /// A child of `sh` running `script`, `--at` and the time its `$1` and `$2`.
    fn shell(script: &str) -> Launcher {
        Launcher::process(
            PathBuf::from("sh"),
            vec!["-c".into(), script.into(), "sh".into()],
        )
    }

    #[rstest]
    fn a_panicking_in_process_train_fails() {
        let launcher = Launcher::in_process(|_| panic!("the train panicked"));

        let Retrained::Failed(why) = launcher.launch(at("2026-10-22T00:00:00Z")).wait() else {
            panic!("a panicking train succeeded");
        };
        assert!(why.contains("without an outcome"), "{why}");
    }

    #[rstest]
    fn a_child_is_told_the_time_and_its_success_is_the_retrains() {
        let launcher = shell(r#"[ "$1" = --at ] && [ "$2" = 2026-10-22T00:00:00Z ]"#);

        assert_eq!(
            launcher.launch(at("2026-10-22T00:00:00Z")).wait(),
            Retrained::Succeeded
        );
    }

    #[rstest]
    fn a_failing_child_fails_with_the_error_it_printed_last() {
        let launcher = shell(
            "echo 'ERROR a log line of the replay' >&2; \
             echo 'Error: failed: 34 rows in the fit slice, fewer than min_rows 35' >&2; \
             exit 1",
        );

        assert_eq!(
            launcher.launch(at("2026-10-22T00:00:00Z")).wait(),
            Retrained::Failed(
                "failed: 34 rows in the fit slice, fewer than min_rows 35".to_string()
            )
        );
    }

    #[rstest]
    fn a_failing_child_printing_no_error_fails_with_its_status() {
        let launcher = shell("exit 3");

        let Retrained::Failed(why) = launcher.launch(at("2026-10-22T00:00:00Z")).wait() else {
            panic!("published");
        };
        assert!(why.contains('3'), "{why}");
    }

    #[rstest]
    fn a_child_that_cannot_start_fails() {
        let launcher = Launcher::process(PathBuf::from("/nonexistent/helix-trader"), vec![]);

        let Retrained::Failed(why) = launcher.launch(at("2026-10-22T00:00:00Z")).wait() else {
            panic!("published");
        };
        assert!(why.contains("could not start"), "{why}");
    }

    fn fired(time: &str) -> TimeEvent {
        let ns = UnixNanos::from(at(time));
        TimeEvent::new(TIMER.into(), nautilus_core::UUID4::new(), ns, ns)
    }

    const AUD: &str = "AUD/USD.SIM";
    const EUR: &str = "EUR/USD.SIM";

    fn catalog(dir: &Path) -> PathBuf {
        dir.join("catalog")
    }

    fn stored(dir: &Path, quotes: &[(&str, &str)]) {
        std::fs::create_dir_all(catalog(dir)).unwrap();
        let catalog = ParquetDataCatalog::new(&catalog(dir), None, None, None, None);

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

    fn published_at(dir: &Path, valid_from: &str) {
        store(&dir.join("models"))
            .publish(&baseline(valid_from, 6))
            .unwrap();
    }

    fn retrains(dir: &Path, retrain: Retrain, launcher: Launcher) -> Option<RetrainActor> {
        let quotes = |instrument_id| TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: InstrumentId::from(instrument_id),
        };
        RetrainActor::new(
            StrategyId::from("LIVE-001"),
            retrain,
            catalog(dir),
            DataRequirements {
                bar_types: vec![],
                ticks: vec![quotes(AUD), quotes(EUR)],
                custom: vec![],
                warmup: SignedDuration::ZERO,
            },
            store(&dir.join("models")),
            launcher,
        )
    }

    fn daily_on(dir: &Path, launcher: Launcher) -> RetrainActor {
        retrains(
            dir,
            Retrain::Every(SignedDuration::from_hours(24)),
            launcher,
        )
        .unwrap()
    }

    /// `daily_on` a catalog holding both series well past every retrain of the tests.
    fn daily(dir: &tempfile::TempDir, launcher: Launcher) -> RetrainActor {
        stored(
            dir.path(),
            &[(AUD, "2026-12-01T00:00:00Z"), (EUR, "2026-12-01T00:00:00Z")],
        );
        daily_on(dir.path(), launcher)
    }

    /// The next time sent to `finished`, within a minute, so that a train never launched fails.
    fn trained(finished: &Receiver<Timestamp>) -> Timestamp {
        finished
            .recv_timeout(Duration::from_secs(60))
            .expect("no train within a minute")
    }

    fn sending(finish: mpsc::Sender<Timestamp>) -> Launcher {
        Launcher::in_process(move |at| {
            finish.send(at).unwrap();
            published(at)
        })
    }

    fn failing(finish: mpsc::Sender<Timestamp>) -> Launcher {
        Launcher::in_process(move |at| {
            finish.send(at).unwrap();
            Outcome::Failed(Failure::Error(
                "34 rows in the fit slice, fewer than min_rows 35".to_string(),
            ))
            .into()
        })
    }

    #[rstest]
    fn a_strategy_never_retrained_has_no_retrain_actor_and_a_retrained_one_has_one() {
        let dir = tempfile::tempdir().unwrap();
        let launcher = || Launcher::in_process(|_| unreachable!());

        let never = retrains(dir.path(), Retrain::Never, launcher());
        let daily = retrains(
            dir.path(),
            Retrain::Every(SignedDuration::from_hours(24)),
            launcher(),
        );

        assert!(never.is_none());
        assert!(daily.is_some());
    }

    /// Nautilus refuses two actors of one id, so each strategy's actor is named after it.
    #[rstest]
    fn a_strategys_retrain_actor_is_named_after_it() {
        let dir = tempfile::tempdir().unwrap();

        let actor = daily_on(dir.path(), Launcher::in_process(|_| unreachable!()));

        assert_eq!(actor.actor_id(), ActorId::from("RETRAIN-LIVE-001"));
    }

    /// Adds `actor` at `now` as `Trader::add_actor` does, on a clock of its own whose default
    /// handler hands the actor its time events, then starts it.
    fn added(mut actor: RetrainActor, now: &str) -> (Ustr, Rc<RefCell<VirtualClock>>) {
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        clock
            .borrow_mut()
            .advance_time(UnixNanos::from(at(now)), true);
        Component::register(
            &mut actor,
            TraderId::from("HELIX-001"),
            clock.clone(),
            Rc::new(RefCell::new(Cache::default())),
        )
        .unwrap();
        let id = actor.actor_id().inner();
        register_component_actor(actor);
        start_component(&id).unwrap();
        (id, clock)
    }

    fn advanced(clock: &Rc<RefCell<VirtualClock>>, time: &str) {
        let events = clock
            .borrow_mut()
            .advance_time(UnixNanos::from(at(time)), true);
        let handlers = clock.borrow().match_handlers(events);
        for handler in handlers {
            handler.callback.call(handler.event);
        }
    }

    /// Releases the actor `id` as the trader does at dispose, ending the wait for a train never
    /// launched.
    fn released(id: &Ustr) {
        deregister_component(id);
        deregister_actor(id);
    }

    /// With the model set of midnight stored nothing trains at start, and the clock past the next
    /// midnight launches its train.
    #[rstest]
    fn an_added_retrain_actor_trains_at_the_grid_time_its_clock_fires() {
        let dir = tempfile::tempdir().unwrap();
        published_at(dir.path(), "2026-10-22T00:00:00Z");
        let (finish, finished) = mpsc::channel();
        let (id, clock) = added(daily(&dir, sending(finish)), "2026-10-22T11:30:00Z");

        advanced(&clock, "2026-10-23T00:30:00Z");
        released(&id);

        assert_eq!(finished.recv(), Ok(at("2026-10-23T00:00:00Z")));
    }

    /// With no model set of midnight the node was down then, and the actor trains it at its start.
    #[rstest]
    #[case::of_an_empty_store(&[])]
    #[case::of_a_store_holding_the_day_before(&["2026-10-21T00:00:00Z"])]
    fn a_retrain_actor_started_after_a_retrain_time_without_its_model_set_trains_it_at_start(
        #[case] model_sets: &[&str],
    ) {
        let dir = tempfile::tempdir().unwrap();
        for valid_from in model_sets {
            published_at(dir.path(), valid_from);
        }
        let (finish, finished) = mpsc::channel();

        let (id, _clock) = added(daily(&dir, sending(finish)), "2026-10-22T11:30:00Z");
        let first = trained(&finished);
        released(&id);

        assert_eq!(first, at("2026-10-22T00:00:00Z"));
        assert_eq!(finished.recv(), Err(mpsc::RecvError));
    }

    /// AUD/USD stored to 01:00 on the 23rd, EUR/USD as `eur`: the retrain due at midnight on the
    /// 23rd waits, trained at no re-check to 00:30.
    #[rstest]
    #[case::of_a_series_stored_before_it(&[(EUR, "2026-10-22T21:00:00Z")])]
    #[case::of_a_series_not_stored(&[])]
    fn a_retrain_due_before_every_series_read_is_stored_at_or_after_it_waits(
        #[case] eur: &[(&str, &str)],
    ) {
        let dir = tempfile::tempdir().unwrap();
        stored(dir.path(), &[(AUD, "2026-10-23T01:00:00Z")]);
        stored(dir.path(), eur);
        published_at(dir.path(), "2026-10-22T00:00:00Z");
        let (finish, finished) = mpsc::channel();
        let (id, clock) = added(
            daily_on(dir.path(), sending(finish)),
            "2026-10-22T11:30:00Z",
        );

        advanced(&clock, "2026-10-23T00:30:00Z");
        released(&id);

        assert_eq!(finished.recv(), Err(mpsc::RecvError));
    }

    /// Once `eur` is stored, the re-check at 00:31 trains at midnight, not at either series' last
    /// point.
    #[rstest]
    #[case::at_it("2026-10-23T00:00:00Z")]
    #[case::after_it("2026-10-23T00:20:00Z")]
    fn a_waiting_retrain_trains_at_its_time_once_every_series_read_is_stored_at_or_after_it(
        #[case] eur: &str,
    ) {
        let dir = tempfile::tempdir().unwrap();
        stored(
            dir.path(),
            &[(AUD, "2026-10-23T01:00:00Z"), (EUR, "2026-10-22T21:00:00Z")],
        );
        published_at(dir.path(), "2026-10-22T00:00:00Z");
        let (finish, finished) = mpsc::channel();
        let (id, clock) = added(
            daily_on(dir.path(), sending(finish)),
            "2026-10-22T11:30:00Z",
        );
        advanced(&clock, "2026-10-23T00:30:00Z");
        stored(dir.path(), &[(EUR, eur)]);

        advanced(&clock, "2026-10-23T00:31:00Z");
        let first = trained(&finished);
        released(&id);

        assert_eq!(first, at("2026-10-23T00:00:00Z"));
    }

    /// The 23rd's retrain waits; the 24th's takes its place, skipped; with EUR/USD stored to 01:00
    /// on the 24th, the re-check at 01:31 trains at midnight on the 24th alone.
    #[rstest]
    fn a_retrain_due_while_one_waits_for_its_data_takes_its_place() {
        let dir = tempfile::tempdir().unwrap();
        stored(
            dir.path(),
            &[(AUD, "2026-10-25T00:00:00Z"), (EUR, "2026-10-22T21:00:00Z")],
        );
        published_at(dir.path(), "2026-10-22T00:00:00Z");
        let (finish, finished) = mpsc::channel();
        let (id, clock) = added(
            daily_on(dir.path(), sending(finish)),
            "2026-10-22T11:30:00Z",
        );
        advanced(&clock, "2026-10-23T00:30:00Z");
        advanced(&clock, "2026-10-24T00:30:00Z");
        stored(dir.path(), &[(EUR, "2026-10-24T01:00:00Z")]);

        advanced(&clock, "2026-10-24T01:31:00Z");
        let first = trained(&finished);
        released(&id);

        assert_eq!(first, at("2026-10-24T00:00:00Z"));
        assert_eq!(finished.recv(), Err(mpsc::RecvError));
    }

    #[rstest]
    fn a_retrain_due_while_one_runs_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let (release, released) = mpsc::channel::<()>();
        let released = Mutex::new(released);
        let (finish, finished) = mpsc::channel();
        let mut actor = daily(
            &dir,
            Launcher::in_process(move |at| {
                released.lock().unwrap().recv().unwrap();
                finish.send(at).unwrap();
                published(at)
            }),
        );

        actor.on_time_event(&fired("2026-10-22T00:00:00Z")).unwrap();
        actor.on_time_event(&fired("2026-10-23T00:00:00Z")).unwrap();
        release.send(()).unwrap();
        release.send(()).unwrap();

        assert_eq!(trained(&finished), at("2026-10-22T00:00:00Z"));

        // Released twice, a second train would send its time before its launcher is dropped
        drop(actor);
        assert_eq!(finished.recv(), Err(mpsc::RecvError));
    }

    /// Waits until the train `actor` runs finished, its outcome following the time it sent. Panics
    /// after a minute.
    fn waited_to_its_end(actor: &RetrainActor) {
        let deadline = Instant::now() + Duration::from_secs(60);

        while actor
            .running
            .as_ref()
            .is_some_and(|running| running.finished().is_none())
        {
            assert!(
                Instant::now() < deadline,
                "the train is still running a minute on"
            );
            std::thread::yield_now();
        }
    }

    /// Fires the retrain timer at `time` until a train sends its time: that time. The outcome
    /// follows the time, so a retrain fired at once may be skipped as running. Panics after a
    /// minute.
    fn fire_until_launched(
        actor: &mut RetrainActor,
        finished: &Receiver<Timestamp>,
        time: &str,
    ) -> Timestamp {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            actor.on_time_event(&fired(time)).unwrap();
            if let Ok(at) = finished.recv_timeout(Duration::from_millis(10)) {
                return at;
            }
            assert!(
                Instant::now() < deadline,
                "no retrain launched at {time} within a minute of firing"
            );
        }
    }

    #[rstest]
    #[case::published(sending)]
    #[case::failed(failing)]
    fn a_finished_retrain_makes_way_for_the_next(
        #[case] launcher: fn(mpsc::Sender<Timestamp>) -> Launcher,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let (finish, finished) = mpsc::channel();
        let mut actor = daily(&dir, launcher(finish));
        actor.on_time_event(&fired("2026-10-22T00:00:00Z")).unwrap();
        assert_eq!(trained(&finished), at("2026-10-22T00:00:00Z"));

        let second = fire_until_launched(&mut actor, &finished, "2026-10-23T00:00:00Z");

        assert_eq!(second, at("2026-10-23T00:00:00Z"));
    }

    fn logged(lines: &[String], level: &str, facts: &[&str]) -> bool {
        lines.iter().any(|line| {
            line.split_once(' ').is_some_and(|(logged_at, message)| {
                logged_at == level && facts.iter().all(|fact| message.contains(fact))
            })
        })
    }

    #[rstest]
    fn a_finished_retrain_logs_its_outcome_as_it_ends() {
        if !log_capture::alone(
            module_path!(),
            "a_finished_retrain_logs_its_outcome_as_it_ends",
        ) {
            return;
        }
        log_capture::capture(log::LevelFilter::Info);
        let dir = tempfile::tempdir().unwrap();
        let mut succeeding = daily(&dir, Launcher::in_process(published));
        let mut refused = daily_on(
            dir.path(),
            shell(
                "echo 'Error: failed: 34 rows in the fit slice, fewer than min_rows 35' >&2; \
                 exit 1",
            ),
        );

        succeeding
            .on_time_event(&fired("2026-10-22T00:00:00Z"))
            .unwrap();
        refused
            .on_time_event(&fired("2026-10-22T00:00:00Z"))
            .unwrap();

        // Each train logs its outcome before it hands it over
        waited_to_its_end(&succeeding);
        waited_to_its_end(&refused);
        let lines = log_capture::lines();

        assert!(
            logged(&lines, "INFO", &["2026-10-22T00:00:00Z", "succeeded"]),
            "{lines:#?}"
        );
        assert!(
            logged(
                &lines,
                "ERROR",
                &[
                    "2026-10-22T00:00:00Z",
                    "failed: 34 rows in the fit slice, fewer than \
                     min_rows 35",
                ]
            ),
            "{lines:#?}"
        );
    }

    /// Output lines are logged, standard output as info and standard error as a warning, plain and
    /// with no file as `NAUTILUS_LOG` sets, which the child echoes.
    #[rstest]
    fn the_output_lines_of_a_child_are_logged_through_the_logger() {
        if !log_capture::alone(
            module_path!(),
            "the_output_lines_of_a_child_are_logged_through_the_logger",
        ) {
            return;
        }
        log_capture::capture(log::LevelFilter::Info);
        let launcher = shell(
            "echo 'a line of the replay'; \
             echo 'a warning of the replay' >&2; \
             echo \"logging: $NAUTILUS_LOG\"",
        );

        let retrained = launcher.launch(at("2026-10-22T00:00:00Z")).wait();
        let lines = log_capture::lines();

        assert_eq!(retrained, Retrained::Succeeded);
        assert!(
            logged(&lines, "INFO", &["a line of the replay"]),
            "{lines:#?}"
        );
        assert!(
            logged(&lines, "WARN", &["a warning of the replay"]),
            "{lines:#?}"
        );
        assert!(
            logged(
                &lines,
                "INFO",
                &["logging: ", "is_colored=false", "fileout=off"]
            ),
            "{lines:#?}"
        );
    }
}
