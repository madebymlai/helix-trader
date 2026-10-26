//! The account's token pair and the file that keeps the latest one.
//!
//! cTrader's access token lasts about 30 days; the refresh token renews it
//! (`ProtoOARefreshTokenReq`), and each renewal replaces both, the old
//! pair invalidated (help.ctrader.com/open-api, "App and account
//! authentication"). So the latest pair lives in a token file outside the
//! repository, readable by the user only (mode 0600, its directory 0700),
//! where every session finds it: `$XDG_STATE_HOME/helix-trader/ic-token.json`,
//! by default `~/.local/state/helix-trader/ic-token.json` (the XDG base
//! directory convention for user state).
//!
//! The first pair comes from `IC_ACCESS_TOKEN` and `IC_REFRESH_TOKEN` in
//! `.env`, which seed the file once; `.env` is never written. The file
//! keeps a SHA-256 fingerprint of the pair that seeded it: when `.env`
//! holds another pair (a new one from the Playground), it seeds the file
//! again.
//!
//! Sessions of several processes (a fill and a live node) share the file.
//! A renewal holds an advisory lock (`flock`, on `ic-token.json.lock`, a
//! file never replaced) from before its request until the new pair is
//! written, so two sessions never spend the same refresh token; the file
//! is replaced atomically (a temporary file renamed over it), so a reader
//! sees a whole pair, the latest writer's.

use std::{
    fmt::Write as _,
    fs::{self, File, OpenOptions, TryLockError},
    io::Write as _,
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

use anyhow::{Context, bail};
use jiff::{SignedDuration, Timestamp};
use nautilus_core::string::secret::SecretString;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use zeroize::Zeroize;

/// How long before the access token expires a session renews it.
pub(super) const RENEW_BEFORE: SignedDuration = SignedDuration::from_hours(24);

/// The token file under the user's state directory.
const RELATIVE_PATH: &str = "helix-trader/ic-token.json";

/// An access token, the refresh token that renews it, and when the access
/// token expires (unknown for a pair seeded from `.env`).
#[derive(Debug, Clone)]
pub(super) struct TokenPair {
    pub(super) access_token: SecretString,
    pub(super) refresh_token: SecretString,
    pub(super) expires_at: Option<Timestamp>,
}

impl TokenPair {
    /// The pair a renewal at `now` returned, its access token valid for
    /// `expires_in` seconds.
    pub(super) fn renewed(
        access_token: SecretString,
        refresh_token: SecretString,
        expires_in: i64,
        now: Timestamp,
    ) -> Self {
        Self {
            access_token,
            refresh_token,
            expires_at: Some(now + SignedDuration::from_secs(expires_in)),
        }
    }

    /// When to renew it: `RENEW_BEFORE` its expiry, or at once when the
    /// expiry is unknown.
    pub(super) fn renew_at(&self) -> Option<Timestamp> {
        self.expires_at.map(|at| at - RENEW_BEFORE)
    }

    /// Whether it is this pair's access token that `other` holds.
    pub(super) fn same_access_token(&self, other: &Self) -> bool {
        self.access_token.expose_secret() == other.access_token.expose_secret()
    }

    /// The fingerprint of the pair: SHA-256 of both tokens, in hex.
    fn fingerprint(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.access_token.expose_secret());
        hasher.update(b"\n");
        hasher.update(self.refresh_token.expose_secret());
        hasher
            .finalize()
            .iter()
            .fold(String::new(), |mut hex, byte| {
                let _ = write!(hex, "{byte:02x}");
                hex
            })
    }
}

/// The token file, and the pair from `.env` that seeds it.
#[derive(Debug, Clone)]
pub(super) struct TokenStore {
    path: PathBuf,
    seed: TokenPair,
}

/// The file's content.
#[derive(Serialize, Deserialize)]
struct Stored {
    /// The fingerprint of the `.env` pair that seeded the file.
    seed: String,
    access_token: String,
    refresh_token: String,
    expires_at: Option<Timestamp>,
}

impl Drop for Stored {
    fn drop(&mut self) {
        self.access_token.zeroize();
        self.refresh_token.zeroize();
    }
}

impl TokenStore {
    /// The store at `path`, seeded with `seed`, the pair from `.env`.
    pub(super) fn new(path: PathBuf, seed: TokenPair) -> Self {
        Self { path, seed }
    }

    /// The token file's path under the user's state directory: under
    /// `XDG_STATE_HOME` if set to an absolute path (the XDG base directory
    /// specification ignores a relative one), else under
    /// `$HOME/.local/state`, through `var`.
    ///
    /// # Errors
    ///
    /// Returns an error if neither is set to an absolute path.
    pub(super) fn path_from_vars(var: impl Fn(&str) -> Option<String>) -> anyhow::Result<PathBuf> {
        let absolute = |name| var(name).map(PathBuf::from).filter(|p| p.is_absolute());
        if let Some(state) = absolute("XDG_STATE_HOME") {
            return Ok(state.join(RELATIVE_PATH));
        }
        match absolute("HOME") {
            Some(home) => Ok(home.join(".local/state").join(RELATIVE_PATH)),
            None => bail!(
                "no place for IC's token file: set HOME, or XDG_STATE_HOME, to an absolute path"
            ),
        }
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    /// The latest pair: the file's, or the `.env` pair, written to the
    /// file first, when there is no file or `.env` holds another pair than
    /// the one that seeded it. Blocks while another session renews.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read, parsed or written;
    /// never a token.
    pub(super) fn load(&self) -> anyhow::Result<TokenPair> {
        let lock = self.lock(true)?.expect("a blocking lock is taken");
        let fingerprint = self.seed.fingerprint();
        match self.stored()? {
            Some(stored) if stored.seed == fingerprint => Ok(pair_of(&stored)),
            _ => {
                lock.save(&self.seed)?;
                Ok(self.seed.clone())
            }
        }
    }

    /// The pair the file holds now.
    ///
    /// # Errors
    ///
    /// Returns an error if there is no file, or it cannot be read or
    /// parsed; never a token.
    pub(super) fn read(&self) -> anyhow::Result<TokenPair> {
        match self.stored()? {
            Some(stored) => Ok(pair_of(&stored)),
            None => bail!("IC's token file {} is gone", self.path.display()),
        }
    }

    /// The renewal lock, or `None` while another session holds it.
    ///
    /// # Errors
    ///
    /// Returns an error if the lock file cannot be opened or locked.
    pub(super) fn try_lock(&self) -> anyhow::Result<Option<TokenLock>> {
        self.lock(false)
    }

    fn lock(&self, block: bool) -> anyhow::Result<Option<TokenLock>> {
        let directory = self
            .path
            .parent()
            .context("the token file has no directory")?;
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(directory)
            .with_context(|| format!("creating {}", directory.display()))?;
        let mut lock_path = self.path.clone().into_os_string();
        lock_path.push(".lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o600)
            .open(&lock_path)
            .with_context(|| format!("opening {}", Path::new(&lock_path).display()))?;
        let locked = if block {
            file.lock().map(|()| true)
        } else {
            match file.try_lock() {
                Ok(()) => Ok(true),
                Err(TryLockError::WouldBlock) => Ok(false),
                Err(TryLockError::Error(e)) => Err(e),
            }
        };
        let locked =
            locked.with_context(|| format!("locking {}", Path::new(&lock_path).display()))?;
        Ok(locked.then(|| TokenLock {
            _file: file,
            path: self.path.clone(),
            seed: self.seed.fingerprint(),
        }))
    }

    fn stored(&self) -> anyhow::Result<Option<Stored>> {
        let mut text = match fs::read_to_string(&self.path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("reading {}", self.path.display())),
        };
        let parsed = serde_json::from_str::<Stored>(&text);
        text.zeroize();
        // serde's message may quote the content: only where it failed.
        parsed.map(Some).map_err(|e| {
            anyhow::anyhow!(
                "{} is not an IC token file (line {}, column {}): delete it to seed it again \
                 from IC_ACCESS_TOKEN and IC_REFRESH_TOKEN",
                self.path.display(),
                e.line(),
                e.column()
            )
        })
    }
}

fn pair_of(stored: &Stored) -> TokenPair {
    TokenPair {
        access_token: SecretString::from(stored.access_token.as_str()),
        refresh_token: SecretString::from(stored.refresh_token.as_str()),
        expires_at: stored.expires_at,
    }
}

/// The renewal lock on the token file, held until dropped.
#[derive(Debug)]
pub(super) struct TokenLock {
    _file: File,
    path: PathBuf,
    seed: String,
}

impl TokenLock {
    /// Replaces the file's pair with `pair`, atomically, mode 0600.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be written; never a token.
    pub(super) fn save(&self, pair: &TokenPair) -> anyhow::Result<()> {
        let directory = self
            .path
            .parent()
            .context("the token file has no directory")?;
        let stored = Stored {
            seed: self.seed.clone(),
            access_token: pair.access_token.expose_secret().to_owned(),
            refresh_token: pair.refresh_token.expose_secret().to_owned(),
            expires_at: pair.expires_at,
        };
        let mut json = serde_json::to_vec(&stored).context("encoding the token file")?;
        let written = (|| {
            let mut file = tempfile::NamedTempFile::new_in(directory)?;
            file.as_file()
                .set_permissions(fs::Permissions::from_mode(0o600))?;
            file.write_all(&json)?;
            file.as_file().sync_all()?;
            file.persist(&self.path)?;
            File::open(directory)?.sync_all()
        })();
        json.zeroize();
        written.with_context(|| format!("writing {}", self.path.display()))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    };

    use nautilus_core::string::secret::REDACTED;

    use super::*;

    fn pair(access: &str, refresh: &str, expires_at: Option<&str>) -> TokenPair {
        TokenPair {
            access_token: SecretString::from(access),
            refresh_token: SecretString::from(refresh),
            expires_at: expires_at.map(|t| t.parse().unwrap()),
        }
    }

    fn seed() -> TokenPair {
        pair("envAccessToken", "envRefreshToken", None)
    }

    fn store_in(dir: &Path, seed: TokenPair) -> TokenStore {
        TokenStore::new(dir.join("state").join(RELATIVE_PATH), seed)
    }

    /// The tokens and expiry of `pair`, to compare pairs.
    fn exposed(pair: &TokenPair) -> (String, String, Option<Timestamp>) {
        (
            pair.access_token.expose_secret().to_owned(),
            pair.refresh_token.expose_secret().to_owned(),
            pair.expires_at,
        )
    }

    fn mode(path: &Path) -> u32 {
        fs::metadata(path).unwrap().permissions().mode() & 0o777
    }

    #[test]
    fn the_first_load_seeds_the_file_from_the_env_pair_expiry_unknown() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(dir.path(), seed());

        let loaded = store.load().unwrap();

        assert_eq!(
            exposed(&loaded),
            ("envAccessToken".into(), "envRefreshToken".into(), None)
        );
        assert_eq!(exposed(&store.read().unwrap()), exposed(&loaded));
    }

    #[test]
    fn the_file_is_the_user_s_only() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(dir.path(), seed());

        store.load().unwrap();

        assert_eq!(mode(store.path()), 0o600);
        assert_eq!(mode(store.path().parent().unwrap()), 0o700);
    }

    #[test]
    fn a_renewed_pair_is_what_the_next_load_finds() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(dir.path(), seed());
        store.load().unwrap();
        let renewed = pair("newAccess", "newRefresh", Some("2026-11-25T10:00:00Z"));

        store.try_lock().unwrap().unwrap().save(&renewed).unwrap();

        let next_run = store_in(dir.path(), seed());
        assert_eq!(exposed(&next_run.load().unwrap()), exposed(&renewed));
        assert_eq!(mode(store.path()), 0o600);
    }

    #[test]
    fn another_env_pair_seeds_the_file_again() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(dir.path(), seed());
        store.load().unwrap();
        let renewed = pair("newAccess", "newRefresh", Some("2026-11-25T10:00:00Z"));
        store.try_lock().unwrap().unwrap().save(&renewed).unwrap();
        let from_playground = pair("playgroundAccess", "playgroundRefresh", None);

        let loaded = store_in(dir.path(), from_playground.clone())
            .load()
            .unwrap();

        assert_eq!(exposed(&loaded), exposed(&from_playground));
    }

    #[test]
    fn the_lock_is_taken_by_one_session_at_a_time() {
        let dir = tempfile::tempdir().unwrap();
        let (one, other) = (store_in(dir.path(), seed()), store_in(dir.path(), seed()));

        let held = one.try_lock().unwrap();

        assert!(held.is_some());
        assert!(other.try_lock().unwrap().is_none());
        drop(held);
        assert!(other.try_lock().unwrap().is_some());
    }

    /// Two writers replace the pair over and over while a reader reads:
    /// every read is one whole pair, and the file ends with the last one
    /// written.
    #[test]
    fn concurrent_writers_never_leave_a_partial_pair_and_the_later_wins() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(dir.path(), pair("seedaccess", "seedrefresh", None));
        store.load().unwrap();
        let done = Arc::new(AtomicBool::new(false));
        let reader = {
            let (store, done) = (store.clone(), Arc::clone(&done));
            std::thread::spawn(move || {
                let mut reads = 0;
                while !done.load(Ordering::Relaxed) {
                    let (access, refresh, _) = exposed(&store.read().unwrap());
                    assert_eq!(access.replace("access", "refresh"), refresh);
                    reads += 1;
                }
                reads
            })
        };
        let writers: Vec<_> = ["a", "b"]
            .into_iter()
            .map(|writer| {
                let store = store.clone();
                std::thread::spawn(move || {
                    for n in 0..100 {
                        let lock = loop {
                            if let Some(lock) = store.try_lock().unwrap() {
                                break lock;
                            }
                        };
                        let renewed = pair(
                            &format!("{writer}{n}access"),
                            &format!("{writer}{n}refresh"),
                            Some("2026-11-25T10:00:00Z"),
                        );
                        lock.save(&renewed).unwrap();
                    }
                })
            })
            .collect();
        for writer in writers {
            writer.join().unwrap();
        }
        done.store(true, Ordering::Relaxed);

        assert!(reader.join().unwrap() > 0);
        let (access, _, _) = exposed(&store.read().unwrap());
        assert!(["a99access", "b99access"].contains(&access.as_str()));
    }

    #[test]
    fn a_file_not_a_token_file_is_an_error_naming_it_not_its_content() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(dir.path(), seed());
        store.load().unwrap();
        fs::write(store.path(), r#"{"access_token": 7, "x": "leakedValue"}"#).unwrap();

        let err = format!("{:#}", store.read().unwrap_err());

        assert!(err.contains("is not an IC token file"), "{err}");
        assert!(err.contains(&store.path().display().to_string()), "{err}");
        assert!(!err.contains("leakedValue"), "{err}");
    }

    #[test]
    fn a_renewed_pair_expires_after_its_lifetime_and_renews_a_day_before() {
        let now: Timestamp = "2026-10-26T12:00:00Z".parse().unwrap();

        let renewed = TokenPair::renewed("a".into(), "r".into(), 2_628_000, now);

        assert_eq!(
            renewed.expires_at,
            Some("2026-11-25T22:00:00Z".parse().unwrap())
        );
        assert_eq!(
            renewed.renew_at(),
            Some("2026-11-24T22:00:00Z".parse().unwrap())
        );
        assert_eq!(seed().renew_at(), None);
    }

    #[test]
    fn debug_shows_no_token() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(dir.path(), seed());

        let debug = format!("{store:?} {:?}", store.load().unwrap());

        assert!(!debug.contains("envAccessToken"), "{debug}");
        assert!(!debug.contains("envRefreshToken"), "{debug}");
        assert!(debug.contains(REDACTED), "{debug}");
    }

    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |name| map.get(name).cloned()
    }

    #[test]
    fn the_file_is_under_the_xdg_state_home() {
        let path = TokenStore::path_from_vars(vars(&[
            ("XDG_STATE_HOME", "/home/u/state"),
            ("HOME", "/home/u"),
        ]))
        .unwrap();

        assert_eq!(path, Path::new("/home/u/state/helix-trader/ic-token.json"));
    }

    #[test]
    fn without_an_absolute_xdg_state_home_the_file_is_under_local_state() {
        for pairs in [
            vec![("HOME", "/home/u")],
            vec![("XDG_STATE_HOME", "state"), ("HOME", "/home/u")],
        ] {
            let path = TokenStore::path_from_vars(vars(&pairs)).unwrap();

            assert_eq!(
                path,
                Path::new("/home/u/.local/state/helix-trader/ic-token.json")
            );
        }
    }

    #[test]
    fn without_a_home_there_is_no_token_file() {
        let err = TokenStore::path_from_vars(vars(&[])).unwrap_err();

        assert!(err.to_string().contains("set HOME"), "{err}");
    }
}
