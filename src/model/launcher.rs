//! Live retraining: how a live node starts a train at a time, and keeps at
//! most one running per strategy.
//!
//! A `Launcher` starts a train at `t` (`train::Training::train_at`) off the
//! node's thread and hands back a `Running`, which the retrain actor asks
//! without waiting: in production a child process, the binary's own
//! `train` subcommand with `--at t` (`Launcher::process`), since a train
//! replays the dataset days it lacks through a Nautilus node of its own,
//! and Nautilus runs no second node in a process while the live node
//! runs; in tests a thread of this process (`Launcher::in_process`). A
//! backtest, holding no node while it trains, trains in its own process
//! and starts no launcher (`backtest_on_models`, in `main.rs`). The
//! child's exit status says what it did: success is a train that kept no
//! model, each published or its current one in use; a failure, a current
//! model kept, the reason the error it printed last (`train` exits with an
//! error when it keeps a model).
//!
//! `RetrainActor` is a live node's retrains of one strategy, a Nautilus
//! actor of its own beside the strategy, named after it (`RETRAIN-` and
//! the strategy's id), so that a node holds one per strategy retrained,
//! Nautilus refusing two actors of one id. Its timer, on the actor's own
//! clock, set at the actor's start on an epoch-aligned time
//! (`schedule::start`), fires at every multiple of the retrain interval
//! since the epoch; a strategy never retrained has no actor
//! (`RetrainActor::new`), so no timer is set and no train launched. Each
//! firing launches a train at once, at the last time the catalog holds for
//! every series the strategy reads (the earliest of their last stored
//! timestamps, Nautilus' `ParquetDataCatalog::query_last_timestamp`),
//! never past the grid time: live, the data up to the last daily
//! promotion (`live::Recording`). A firing is skipped, with a warning, if
//! the catalog holds nothing of a series read, if that time is not past
//! the previous retrain's, the catalog having gained nothing since, or if
//! the previous retrain is still running. A retrain logs what it did as it
//! ends, on its own thread, waiting for no decision nor firing: as info
//! when it succeeded, as an error with its reason when it kept a current
//! model. The model a retrain publishes
//! is the strategy's from its next decision, at which its model handle
//! lists the store again (`handle`).
//!
//! A child still running when the node stops runs on to its end: its model
//! is published, written whole, for the next start.

use std::{
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
use jiff::{SignedDuration, Timestamp};
use nautilus_common::{
    actor::{DataActor, DataActorConfig, DataActorCore},
    nautilus_actor,
    timer::TimeEvent,
};
use nautilus_core::UnixNanos;
use nautilus_model::identifiers::{ActorId, StrategyId};
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

/// The name of the retrain timer, on the retrain actor's own clock.
const TIMER: &str = "RETRAIN";

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

    /// This launcher, each of its trains logging what it did as it ends,
    /// on its own thread: as info when it succeeded, as an error with its
    /// reason when it kept a current model.
    fn logged(self) -> Self {
        let train = self.train;
        Self {
            train: Arc::new(move |at| {
                let retrained = train(at);
                match &retrained {
                    Retrained::Succeeded => {
                        log::info!("the retrain at {at} succeeded, its models published or in use");
                    }
                    Retrained::Kept(why) => log::error!("the retrain at {at}: {why}"),
                }
                retrained
            }),
        }
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

/// A live node's retrains of a strategy, a Nautilus actor: at most one
/// running, launched by the timer on its own clock.
#[derive(Debug)]
pub struct RetrainActor {
    core: DataActorCore,
    /// The retrain interval.
    every: SignedDuration,
    catalog: PathBuf,
    requirements: DataRequirements,
    /// Launches trains that log what they did as they end
    /// (`Launcher::logged`).
    launcher: Launcher,
    running: Option<Running>,
    /// The time the last retrain launched trains at.
    previous: Option<Timestamp>,
}

impl RetrainActor {
    /// The retrains of `strategy_id`, retrained as `retrain` says, of a
    /// strategy of `requirements` reading the catalog at `catalog`,
    /// launched by `launcher`; none never retrained, so that no timer is
    /// set and no train launched.
    #[must_use]
    pub fn new(
        strategy_id: StrategyId,
        retrain: Retrain,
        catalog: PathBuf,
        requirements: DataRequirements,
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
            launcher: launcher.logged(),
            running: None,
            previous: None,
        })
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
}

impl DataActor for RetrainActor {
    /// Starts the retrain timer on the actor's clock (`schedule::start`),
    /// on the last grid time not after now, so it first fires on the next
    /// one.
    fn on_start(&mut self) -> anyhow::Result<()> {
        schedule::start(&*self, TIMER, self.every)
    }

    /// Nothing to do at stop: a running child runs on to its end and
    /// publishes, and Nautilus cancels the timer when the trader releases
    /// the actor at dispose. Nautilus' own `on_stop` would warn at every
    /// stop.
    fn on_stop(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Launches the retrain due at the grid time of `event`, the retrain
    /// timer's, the one timer of the actor's clock, at the catalog's end
    /// then (`stored_until`). Skipped, with a warning, if the catalog holds
    /// nothing of a series read, if its end is not past the previous
    /// retrain's time, or if the previous retrain is still running.
    fn on_time_event(&mut self, event: &TimeEvent) -> anyhow::Result<()> {
        let due = Timestamp::from(event.ts_event);
        let at = match self.stored_until(due) {
            Ok(at) => at,
            Err(e) => {
                log::warn!("the retrain at {due} skipped: {e:#}");
                return Ok(());
            }
        };
        if let Some(previous) = self.previous
            && at <= previous
        {
            log::warn!(
                "the retrain at {due} skipped: the catalog holds nothing past {previous}, \
                 the previous retrain's time"
            );
            return Ok(());
        }
        if let Some(running) = &self.running
            && running.finished().is_none()
        {
            log::warn!(
                "the retrain at {due} skipped: the retrain at {} is still running",
                running.at()
            );
            return Ok(());
        }
        log::info!("retraining at {at}, the catalog's end, for the retrain due at {due}");
        self.previous = Some(at);
        self.running = Some(self.launcher.launch(at));
        Ok(())
    }
}

nautilus_actor!(RetrainActor);

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
        clock::{Clock, VirtualClock},
        component::{Component, deregister_component, register_component_actor, start_component},
        timer::TimeEventCallback,
    };
    use nautilus_model::{
        data::{NautilusDataType, QuoteTick},
        identifiers::{InstrumentId, TraderId},
        types::{Price, Quantity},
    };
    use ustr::Ustr;

    use super::*;
    use crate::{
        model::{
            holdout::{Earnings, Holdout, Refusal},
            score::ExpectedScore,
            train::{Kept, Outcome},
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
            score: ExpectedScore {
                rows: 2,
                skill: Some(0.1),
                ..ExpectedScore::default()
            },
        }
    }

    fn published(at: Timestamp) -> Trained {
        Outcome::Published {
            valid_from: at,
            holdout: Box::new(holdout()),
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

    /// The event of the retrain timer at `time`.
    fn fired(time: &str) -> TimeEvent {
        let ns = UnixNanos::from(at(time));
        TimeEvent::new(TIMER.into(), nautilus_core::UUID4::new(), ns, ns)
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

    /// The retrains of `LIVE-001`, retrained as `retrain` says, of a
    /// strategy reading the quotes of AUD/USD and EUR/USD from the catalog
    /// at `catalog`, launched by `launcher`.
    fn retrains(catalog: &Path, retrain: Retrain, launcher: Launcher) -> Option<RetrainActor> {
        let quotes = |instrument_id| TickSeries {
            data: NautilusDataType::QuoteTick,
            instrument_id: InstrumentId::from(instrument_id),
        };
        RetrainActor::new(
            StrategyId::from("LIVE-001"),
            retrain,
            catalog.to_path_buf(),
            DataRequirements {
                bar_types: vec![],
                ticks: vec![quotes(AUD), quotes(EUR)],
                warmup: SignedDuration::ZERO,
            },
            launcher,
        )
    }

    /// The `retrains`, daily.
    fn daily_on(catalog: &Path, launcher: Launcher) -> RetrainActor {
        retrains(
            catalog,
            Retrain::Every(SignedDuration::from_hours(24)),
            launcher,
        )
        .unwrap()
    }

    /// `daily_on` a catalog holding both series well past every retrain
    /// of the tests, kept in `dir`.
    fn daily(dir: &tempfile::TempDir, launcher: Launcher) -> RetrainActor {
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

    /// A launcher handing each time it trains at to `finish`, each train
    /// keeping the current model: the model fitted is refused, its holdout
    /// net P&L per trade below the minimum.
    fn keeping(finish: mpsc::Sender<Timestamp>) -> Launcher {
        Launcher::in_process(move |at| {
            finish.send(at).unwrap();
            Outcome::Kept(Kept::Refused {
                why: Refusal::MinPnl {
                    pnl: 0.25,
                    min_pnl: 0.5,
                },
                holdout: Box::new(holdout()),
            })
            .into()
        })
    }

    #[test]
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

    /// Nautilus refuses two actors of one id: a node retraining two
    /// strategies holds the actor of each, named after it.
    #[test]
    fn a_strategys_retrain_actor_is_named_after_it() {
        let dir = tempfile::tempdir().unwrap();

        let actor = daily_on(dir.path(), Launcher::in_process(|_| unreachable!()));

        assert_eq!(actor.actor_id(), ActorId::from("RETRAIN-LIVE-001"));
    }

    /// Registers `actor` without a node at `now`, as the handle's tests
    /// register their strategy: its clock.
    fn registered(actor: &mut RetrainActor, now: &str) -> Rc<RefCell<VirtualClock>> {
        let clock = Rc::new(RefCell::new(VirtualClock::new()));
        clock
            .borrow_mut()
            .advance_time(UnixNanos::from(at(now)), true);
        clock
            .borrow_mut()
            .register_default_handler(TimeEventCallback::from(|_: TimeEvent| {}));
        actor
            .core
            .register(
                TraderId::from("HELIX-001"),
                clock.clone(),
                Rc::new(RefCell::new(Cache::default())),
            )
            .unwrap();
        clock
    }

    /// Started at 11:30 on 2026-10-22, a daily retrain's timer, on the
    /// actor's own clock, first fires at the next midnight, 2026-10-23, a
    /// whole number of days since the epoch.
    #[test]
    fn the_retrain_timer_is_the_actors_on_the_grid() {
        let dir = tempfile::tempdir().unwrap();
        let mut actor = daily_on(dir.path(), Launcher::in_process(|_| unreachable!()));
        let clock = registered(&mut actor, "2026-10-22T11:30:00Z");

        DataActor::on_start(&mut actor).unwrap();

        let next = clock.borrow().next_time_ns(TIMER).unwrap();
        assert_eq!(Timestamp::from(next), at("2026-10-23T00:00:00Z"));
    }

    /// Adds `actor` at `now` as a node's trader adds an actor
    /// (`Trader::add_actor`): registered on a clock of its own, whose
    /// default handler hands the actor its time events, and kept in
    /// Nautilus' registries; then starts it as the trader does. Its id and
    /// its clock.
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

    /// A daily retrain added and started at 11:30 on 2026-10-22 as a node
    /// adds and starts an actor, on a catalog holding both series past
    /// every retrain: its clock advanced past the next midnight, the event
    /// of the retrain timer there, run through the handler Nautilus matches
    /// it with, launches a train at 2026-10-23T00:00. The actor is released
    /// then, as the trader releases it at dispose, its launcher with it, so
    /// that a train never launched ends the wait.
    #[test]
    fn an_added_retrain_actor_trains_at_the_grid_time_its_clock_fires() {
        let dir = tempfile::tempdir().unwrap();
        let (finish, finished) = mpsc::channel();
        let (id, clock) = added(daily(&dir, sending(finish)), "2026-10-22T11:30:00Z");

        let events = clock
            .borrow_mut()
            .advance_time(UnixNanos::from(at("2026-10-23T00:30:00Z")), true);
        let handlers = clock.borrow().match_handlers(events);
        for handler in handlers {
            handler.callback.call(handler.event);
        }
        deregister_component(&id);
        deregister_actor(&id);

        assert_eq!(finished.recv(), Ok(at("2026-10-23T00:00:00Z")));
    }

    #[test]
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

        assert_eq!(finished.recv().unwrap(), at("2026-10-22T00:00:00Z"));
        // Released twice, a second train would send its time before its
        // launcher is dropped.
        drop(actor);
        assert_eq!(finished.recv(), Err(mpsc::RecvError));
    }

    /// Waits until the train `actor` runs finished, so that its next
    /// firing is not skipped as running. A train's outcome is sent just
    /// after the train returns, off this thread, so it may come after the
    /// time the train sent.
    fn waited_to_its_end(actor: &RetrainActor) {
        while actor
            .running
            .as_ref()
            .is_some_and(|running| running.finished().is_none())
        {
            std::thread::yield_now();
        }
    }

    /// Fires the retrain timer of `actor` at `time` until a train sends
    /// its time to `finished`: that time. The outcome of a train is sent
    /// just after the train sent its time, so a retrain fired right after
    /// may be skipped as running. Panics after a minute, so that a retrain
    /// never launched fails rather than hangs.
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

    #[test]
    fn a_finished_retrain_makes_way_for_the_next() {
        let dir = tempfile::tempdir().unwrap();
        let (finish, finished) = mpsc::channel();
        let mut actor = daily(&dir, sending(finish));
        actor.on_time_event(&fired("2026-10-22T00:00:00Z")).unwrap();
        assert_eq!(finished.recv().unwrap(), at("2026-10-22T00:00:00Z"));

        let second = fire_until_launched(&mut actor, &finished, "2026-10-23T00:00:00Z");

        assert_eq!(second, at("2026-10-23T00:00:00Z"));
    }

    /// A retrain that kept the current model, which the strategy trades
    /// on still, leaves the next retrain free to launch, as one that
    /// published does.
    #[test]
    fn a_retrain_keeping_the_model_makes_way_for_the_next() {
        let dir = tempfile::tempdir().unwrap();
        let (finish, finished) = mpsc::channel();
        let mut actor = daily(&dir, keeping(finish));
        actor.on_time_event(&fired("2026-10-22T00:00:00Z")).unwrap();
        assert_eq!(finished.recv().unwrap(), at("2026-10-22T00:00:00Z"));

        let second = fire_until_launched(&mut actor, &finished, "2026-10-23T00:00:00Z");

        assert_eq!(second, at("2026-10-23T00:00:00Z"));
    }

    #[test]
    fn a_retrain_trains_at_its_time_when_the_catalog_holds_past_it() {
        let dir = tempfile::tempdir().unwrap();
        let (finish, finished) = mpsc::channel();
        let mut actor = daily(&dir, sending(finish));

        actor.on_time_event(&fired("2026-10-22T00:00:00Z")).unwrap();

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
        let mut actor = daily_on(dir.path(), sending(finish));

        actor.on_time_event(&fired("2026-10-22T00:00:00Z")).unwrap();

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
        let mut actor = daily_on(dir.path(), sending(finish));

        actor.on_time_event(&fired("2026-10-22T00:00:00Z")).unwrap();

        assert_eq!(finished.recv().unwrap(), at("2026-10-21T09:30:00Z"));
    }

    #[test]
    fn a_retrain_is_skipped_when_the_catalog_holds_nothing_of_a_series_read() {
        let dir = tempfile::tempdir().unwrap();
        stored(dir.path(), &[(AUD, "2026-10-21T13:00:00Z")]);
        let (finish, finished) = mpsc::channel();
        let mut actor = daily_on(dir.path(), sending(finish));

        actor.on_time_event(&fired("2026-10-22T00:00:00Z")).unwrap();
        drop(actor);

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
        let mut actor = daily_on(dir.path(), sending(finish));
        actor.on_time_event(&fired("2026-10-22T00:00:00Z")).unwrap();
        assert_eq!(finished.recv().unwrap(), at("2026-10-21T13:00:00Z"));
        waited_to_its_end(&actor);

        actor.on_time_event(&fired("2026-10-23T00:00:00Z")).unwrap();
        drop(actor);

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
        let mut actor = daily_on(dir.path(), sending(finish));
        actor.on_time_event(&fired("2026-10-22T00:00:00Z")).unwrap();
        assert_eq!(finished.recv().unwrap(), at("2026-10-21T13:00:00Z"));
        stored(
            dir.path(),
            &[(AUD, "2026-10-22T13:00:00Z"), (EUR, "2026-10-22T13:00:00Z")],
        );

        let second = fire_until_launched(&mut actor, &finished, "2026-10-23T00:00:00Z");

        assert_eq!(second, at("2026-10-22T13:00:00Z"));
    }

    /// Every log record of the process, its level and its message.
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

    /// Whether this is the process the test `name` of this module runs
    /// itself in, alone, so that the logger it installs is the process's:
    /// the test binary's own process holds Nautilus' logger, which the
    /// first node of a test installs, and which a logger installed before
    /// would make it refuse. Else runs it so, and checks that it ran and
    /// passed.
    fn alone(name: &str) -> bool {
        if std::env::var_os(ALONE).is_some() {
            return true;
        }
        // The test's path in the binary: its module's, the crate left out.
        let (_, module) = module_path!().split_once("::").unwrap();
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

    /// Two retrain actors, each fired once at midnight and then left
    /// alone, no decision nor firing after: the train that published logs
    /// its success as info, and the child that kept the current model,
    /// printing why last (`Error: ` and the reason), logs that reason as an
    /// error, each as its train ends.
    #[test]
    fn a_finished_retrain_logs_its_outcome_as_it_ends() {
        if !alone("a_finished_retrain_logs_its_outcome_as_it_ends") {
            return;
        }
        log::set_logger(&CAPTURED).unwrap();
        log::set_max_level(log::LevelFilter::Info);
        let dir = tempfile::tempdir().unwrap();
        let mut succeeding = daily(&dir, Launcher::in_process(published));
        let mut refused = daily_on(
            dir.path(),
            shell(
                "echo 'Error: kept the current model: holdout net P&L per trade taken 0.48 below the minimum 0.55' >&2; \
                 exit 1",
            ),
        );

        succeeding
            .on_time_event(&fired("2026-10-22T00:00:00Z"))
            .unwrap();
        refused
            .on_time_event(&fired("2026-10-22T00:00:00Z"))
            .unwrap();

        let outcomes = [
            "INFO the retrain at 2026-10-22T00:00:00Z succeeded, its models published or in use",
            "ERROR the retrain at 2026-10-22T00:00:00Z: kept the current model: holdout net P&L \
             per trade taken 0.48 below the minimum 0.55",
        ];
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let logged = CAPTURED.0.lock().unwrap().clone();
            if outcomes
                .iter()
                .all(|outcome| logged.iter().any(|line| line == outcome))
            {
                break;
            }
            assert!(Instant::now() < deadline, "{logged:#?}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
