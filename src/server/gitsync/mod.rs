//! TODO: Add overall documentation
use crate::errors::Result;
use crate::server::encryption::{Cryptor, Sealed, Unsealed};
use crate::server::{
    AddVersionResult, GetVersionResult, HistorySegment, Server, Snapshot, SnapshotUrgency,
    VersionId,
};
use crate::Error;
use async_trait::async_trait;
use glob::glob;
use log::info;
use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use serde_with::{base64::Base64, serde_as};
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::Command;
use uuid::Uuid;

/// A version record stored in a version file on disk.
#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
struct Version {
    #[serde(with = "uuid::serde::simple")]
    version_id: VersionId,
    #[serde(with = "uuid::serde::simple")]
    parent_version_id: VersionId,
    #[serde_as(as = "Base64")]
    history_segment: HistorySegment,
}

/// The snapshot record stored in the `snapshot` file on disk.
///
/// It is overwritten when a newer snapshot is added
#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
struct SnapshotFile {
    #[serde(with = "uuid::serde::simple")]
    version_id: VersionId,
    #[serde_as(as = "Base64")]
    payload: Vec<u8>,
}

/// Repository metadata.
#[serde_as]
#[derive(Serialize, Deserialize, Debug)]
struct Meta {
    #[serde(with = "uuid::serde::simple")]
    latest_version: VersionId,
    #[serde_as(as = "Base64")]
    salt: Vec<u8>,
}

/// A [`Server`] backed by a local git repository.
///
///
/// When `local_only` is `false` the server pushes to and pulls from `remote` on the `branch`
/// branch after each write. Conflict resolution is handled as: commit locally,
/// attempt push, and on rejection pull-and-reset before returning.
pub(crate) struct GitSyncServer {
    meta: Meta,
    local_path: PathBuf,
    branch: String,
    remote: String,
    local_only: bool,
    cryptor: Cryptor,
}

/// Run a git command in a given directory, returning an error if it exits non-zero.
fn git_cmd(dir: &Path, args: &[&str]) -> Result<()> {
    let status = Command::new("git").args(args).current_dir(dir).status()?;
    if !status.success() {
        return Err(Error::Server(format!(
            "git {} failed with status {}",
            args.join(" "),
            status
        )));
    }
    Ok(())
}

impl GitSyncServer {
    /// Create or re-open a git-backed sync server.
    ///
    /// If `local_path` does not yet exist it is created. If it exists but is not a git
    /// repository:
    /// - In `local_only` mode a new repo is initialised there.
    /// - In remote mode the repo is cloned from `remote`.
    pub(crate) fn new(
        local_path: PathBuf,
        branch: String,
        remote: String,
        local_only: bool,
        encryption_secret: Vec<u8>,
    ) -> Result<GitSyncServer> {
        let meta = Self::init_repo(&local_path, &branch, &remote, local_only)?;
        let cryptor = Cryptor::new(&meta.salt, &encryption_secret.into())?;
        let server = GitSyncServer {
            meta,
            local_path,
            branch,
            remote,
            local_only,
            cryptor,
        };
        Ok(server)
    }

    /// Initialise or open the git repository and return the current [`Meta`].
    ///
    /// Creates the directory, initialises or clones the repo, switches to `branch`, and
    /// creates the `meta` file on first run.
    fn init_repo(local_path: &Path, branch: &str, remote: &str, local_only: bool) -> Result<Meta> {
        // Create the local directory if needed.
        fs::create_dir_all(local_path)?;

        // Check if path is already a git repo.
        let is_repo = Command::new("git")
            .arg("rev-parse")
            .current_dir(local_path)
            .output()?
            .status
            .success();

        // Create one if not.
        if !is_repo {
            if local_only {
                info!("Creating new repo at {:?}", local_path);
                git_cmd(local_path, &["init"])?;
            } else {
                info!("Cloning repo from {:?} to {:?}", remote, local_path);
                let parent = local_path
                    .parent()
                    .ok_or_else(|| Error::Server("local_path has no parent".into()))?;
                let dir_name = local_path
                    .file_name()
                    .ok_or_else(|| Error::Server("local_path has no file name".into()))?
                    .to_str()
                    .ok_or_else(|| Error::Server("local_path is not valid UTF-8".into()))?;
                git_cmd(parent, &["clone", remote, dir_name])?;
            }
            // Set identity so commits work in environments without a global git config.
            git_cmd(local_path, &["config", "user.email", "taskchampion@local"])?;
            git_cmd(local_path, &["config", "user.name", "taskchampion"])?;
        }

        // Switch to the requested branch.  Try checking out an existing branch first,
        // only create a new one if that fails.
        info!("Switching branch to {:?}", branch);
        let checkout_ok = Command::new("git")
            .args(["checkout", branch])
            .current_dir(local_path)
            .stderr(std::process::Stdio::null())
            .status()?
            .success();
        if !checkout_ok {
            // For a brand-new repo `git checkout -b` also fails, so use
            // `git symbolic-ref` to point HEAD at the desired branch without needing a commit.
            let has_commits = Command::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(local_path)
                .stderr(std::process::Stdio::null())
                .status()?
                .success();
            if has_commits {
                git_cmd(local_path, &["checkout", "-b", branch])?;
            } else {
                git_cmd(
                    local_path,
                    &["symbolic-ref", "HEAD", &format!("refs/heads/{}", branch)],
                )?;
            }
        }

        // Check for meta file, create and commit if missing.
        let meta_path = local_path.join("meta");
        let meta = match File::open(&meta_path) {
            Ok(mut file) => {
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                serde_json::from_str(&contents)?
            }
            Err(_) => {
                let m = Meta {
                    latest_version: Uuid::nil(),
                    salt: Cryptor::gen_salt()?,
                };
                let f = File::create_new(&meta_path)?;
                serde_json::to_writer(f, &m)?;
                git_cmd(local_path, &["add", "meta"])?;
                git_cmd(local_path, &["commit", "-m", "init taskchampion repo"])?;
                m
            }
        };

        // Remove any untracked files left behind by interrupted writes.
        git_cmd(
            local_path,
            &["clean", "-f", "--", "v-*", "snapshot", "meta"],
        )?;

        Ok(meta)
    }

    /// Stage the given paths and create a commit.
    fn stage_and_commit(&self, paths: &[&Path], message: &str) -> Result<()> {
        for path in paths {
            let path_str = path
                .to_str()
                .ok_or_else(|| Error::Server("path is not valid UTF-8".into()))?;
            git_cmd(&self.local_path, &["add", path_str])?;
        }
        git_cmd(&self.local_path, &["commit", "-m", message])?;
        Ok(())
    }

    /// Read the meta file from disk and update self.meta.
    fn read_meta(&mut self) -> Result<()> {
        let meta_path = self.local_path.join("meta");
        let file = File::open(&meta_path)?;
        let reader = BufReader::new(file);
        self.meta = serde_json::from_reader(reader)?;
        Ok(())
    }

    /// Serialise self.meta to the meta file and return its path.
    fn write_meta(&self) -> Result<PathBuf> {
        let meta_path = self.local_path.join("meta");
        let f = File::create(&meta_path)?;
        serde_json::to_writer(f, &self.meta)?;
        Ok(meta_path)
    }

    /// Fetch and fast-forward to the remote branch. No-op in local-only mode.
    /// If the remote branch does not yet exist (e.g. fresh bare repo), this is also a no-op.
    fn pull(&self) -> Result<()> {
        if self.local_only {
            return Ok(());
        }
        // Check whether the remote branch exists before fetching. A bare repo with no commits
        // has no refs yet, and `git fetch origin <branch>` would fail in that case.
        let has_remote_branch = Command::new("git")
            .args([
                "ls-remote",
                "--exit-code",
                "--heads",
                &self.remote,
                &self.branch,
            ])
            .current_dir(&self.local_path)
            .stderr(std::process::Stdio::null())
            .status()?
            .success();
        if !has_remote_branch {
            return Ok(());
        }
        git_cmd(&self.local_path, &["fetch", &self.remote, &self.branch])?;
        git_cmd(&self.local_path, &["reset", "--hard", "FETCH_HEAD"])?;
        // Remove any untracked files left behind by interrupted writes.
        git_cmd(
            &self.local_path,
            &["clean", "-f", "--", "v-*", "snapshot", "meta"],
        )?;
        Ok(())
    }

    /// Push to the remote branch. Returns `true` on success, `false` if the push is rejected
    /// Always returns `true` in local-only mode.
    fn push(&self) -> Result<bool> {
        if self.local_only {
            return Ok(true);
        }
        let output = Command::new("git")
            .args(["push", &self.remote, &self.branch])
            .current_dir(&self.local_path)
            .output()?;
        if !output.status.success() {
            log::debug!(
                "git push failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        Ok(output.status.success())
    }

    /// Encrypt and write a version file named `v-{parent_version_id}-{version_id}`.
    ///
    /// Returns the path of the newly written file.
    fn add_version_by_parent_version_id(&self, version: &Version) -> Result<PathBuf> {
        let unsealed = Unsealed {
            version_id: version.version_id,
            payload: version.history_segment.clone(),
        };
        let sealed = self.cryptor.seal(unsealed)?;
        let filename = format!(
            "v-{}-{}",
            version.parent_version_id.simple(),
            version.version_id.simple()
        );
        let path = self.local_path.join(&filename);
        std::fs::write(&path, Vec::<u8>::from(sealed))?;
        Ok(path)
    }

    /// Find, read, and decrypt the version file whose parent matches `version`.
    fn get_version_by_parent_version_id(&self, version: &VersionId) -> Result<Option<Version>> {
        // glob to find file.
        // v-PARENT-CHILD
        let pattern = format!(
            "{}/v-{}-*",
            self.local_path
                .to_str()
                .ok_or_else(|| Error::Server("path is not valid UTF-8".into()))?,
            version.simple()
        );

        for entry in glob(&pattern).map_err(|e| Error::Server(format!("{:?}", e)))? {
            let path = match entry {
                Ok(p) => p,
                Err(e) => {
                    log::warn!("glob entry error: {e}");
                    continue;
                }
            };
            let filename = match path.to_str() {
                Some(f) => f,
                None => {
                    log::warn!("non-UTF-8 path, skipping");
                    continue;
                }
            };
            let (_, version_id_str) = match filename.rsplit_once('-') {
                Some(parts) => parts,
                None => {
                    log::warn!("unexpected filename format: {filename}");
                    continue;
                }
            };
            let version_id = match Uuid::parse_str(version_id_str) {
                Ok(id) => id,
                Err(e) => {
                    log::warn!("bad version id in {filename}: {e}");
                    continue;
                }
            };
            // Real errors past this point
            let mut buf = Vec::new();
            File::open(&path)?.read_to_end(&mut buf)?;
            let sealed = Sealed {
                version_id,
                payload: buf,
            };
            let unsealed = self.cryptor.unseal(sealed)?;
            return Ok(Some(Version {
                version_id: unsealed.version_id,
                parent_version_id: *version,
                history_segment: unsealed.payload,
            }));
        }
        Ok(None)
    }

    /// Return the appropriate [`SnapshotUrgency`] based on the number of uncommitted version files.
    ///
    /// Returns `None` if a snapshot already exists (cleanup will have removed covered files,
    /// so a non-zero count reflects only post-snapshot versions). Otherwise urgency rises
    /// with the file count.
    fn snapshot_urgency(&self) -> SnapshotUrgency {
        // If a snapshot already exists, cleanup has (or will) remove covered version files.
        // Don't request another snapshot until enough new versions accumulate.
        if self.local_path.join("snapshot").exists() {
            let pattern = format!("{}/v-*", self.local_path.display());
            let count = glob(&pattern).map(|g| g.count()).unwrap_or(0);
            return match count {
                0..=50 => SnapshotUrgency::None,
                51..=100 => SnapshotUrgency::Low,
                _ => SnapshotUrgency::High,
            };
        }
        let pattern = format!("{}/v-*", self.local_path.display());
        let count = glob(&pattern).map(|g| g.count()).unwrap_or(0);
        // TODO: Performance test get_version_by_parent_version_id to help determine
        // what a reasonable number is.
        match count {
            0..=50 => SnapshotUrgency::None,
            51..=100 => SnapshotUrgency::Low,
            _ => SnapshotUrgency::High,
        }
    }

    /// Cleans up the repository by removing version files covered by the current snapshot.
    ///
    /// Reads the snapshot to determine which version it covers, then walks backward
    /// through the version chain from that version. All version files on that chain
    /// (i.e. all history that is now redundant given the snapshot) are deleted,
    /// committed, and pushed. If the push is rejected, we reset to the remote state
    /// and will retry on the next snapshot.
    ///
    /// This is a best-effort operation: if called with no snapshot present it is a no-op.
    fn cleanup(&self) -> Result<()> {
        // Read snapshot metadata. The version_id is stored unencrypted in the JSON wrapper,
        // so no decryption is needed here.
        let snapshot_path = self.local_path.join("snapshot");
        let snapshot_version: Uuid = if let Ok(file) = File::open(&snapshot_path) {
            let s: SnapshotFile = serde_json::from_reader(BufReader::new(file))?;
            s.version_id
        } else {
            return Ok(());
        };

        // Parse all v-* filenames into a child → (parent, path) map.
        let pattern = format!(
            "{}/v-*",
            self.local_path
                .to_str()
                .ok_or_else(|| Error::Server("repo path is not valid UTF-8".into()))?
        );
        let mut versions: HashMap<Uuid, (Uuid, PathBuf)> = HashMap::new();
        for entry in glob(&pattern).map_err(|e| Error::Server(format!("{e:?}")))? {
            let path = match entry {
                Ok(p) => p,
                Err(e) => {
                    log::warn!("cleanup: glob error: {e}");
                    continue;
                }
            };
            let Some(filename) = path.file_name().and_then(|f| f.to_str()) else {
                continue;
            };
            // Filename format: v-{parent_simple}-{child_simple}
            let Some(stem) = filename.strip_prefix("v-") else {
                continue;
            };
            let Some((parent_str, child_str)) = stem.split_once('-') else {
                continue;
            };
            let (Ok(parent_id), Ok(child_id)) =
                (Uuid::parse_str(parent_str), Uuid::parse_str(child_str))
            else {
                continue;
            };
            versions.insert(child_id, (parent_id, path));
        }

        if versions.is_empty() {
            return Ok(());
        }

        // Walk the chain backward from snapshot_version to find all versions whose
        // history is now captured by the snapshot.
        let mut covered: HashSet<Uuid> = HashSet::new();
        let mut current = snapshot_version;
        for _ in 0..=versions.len() {
            if !covered.insert(current) {
                log::warn!("cleanup: version cycle detected, aborting");
                return Ok(());
            }
            match versions.get(&current) {
                Some((parent, _)) => current = *parent,
                None => break,
            }
        }

        // Remove each covered version file from the git index and working tree.
        let mut any_removed = false;
        for (child_id, (_, path)) in &versions {
            if covered.contains(child_id) {
                let name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| Error::Server("version file path is not valid UTF-8".into()))?;
                git_cmd(&self.local_path, &["rm", name])?;
                any_removed = true;
            }
        }

        if !any_removed {
            return Ok(());
        }

        git_cmd(
            &self.local_path,
            &["commit", "-m", "cleanup: remove version files covered by snapshot"],
        )?;

        // Best-effort push. A rejected push just means another replica will clean up
        // later (or we will on the next snapshot). Reset to remote state so we are in sync.
        if !self.push()? {
            log::info!("cleanup: push rejected, resetting to remote state");
            self.pull()?;
        }

        Ok(())
    }
}

#[async_trait(?Send)]
impl Server for GitSyncServer {
    async fn add_version(
        &mut self,
        parent_version_id: VersionId,
        history_segment: HistorySegment,
    ) -> Result<(AddVersionResult, SnapshotUrgency)> {
        // Accept any parent when the repo is empty (latest == NIL).
        // Otherwise check if parent is in latest. If it isn't, pull recheck.
        if self.meta.latest_version != Uuid::nil() && parent_version_id != self.meta.latest_version
        {
            self.pull()?;
            self.read_meta()?;
            if parent_version_id != self.meta.latest_version {
                return Ok((
                    AddVersionResult::ExpectedParentVersion(self.meta.latest_version),
                    SnapshotUrgency::None,
                ));
            }
        }

        // Create the new version and write it to file.
        let version_id = Uuid::new_v4();
        let version = Version {
            version_id,
            parent_version_id,
            history_segment,
        };
        let version_path = self.add_version_by_parent_version_id(&version)?;
        self.meta.latest_version = version_id;
        let meta_path = self.write_meta()?;

        // Commit and push, reverting if push fails.
        self.stage_and_commit(&[&version_path, &meta_path], "add version")?;

        if !self.push()? {
            // Push was rejected (non-fast-forward). Undo the commit and re-read remote state.
            git_cmd(&self.local_path, &["reset", "HEAD~1", "--soft"])?;
            std::fs::remove_file(&version_path)?;
            self.pull()?;
            self.read_meta()?;
            return Ok((
                AddVersionResult::ExpectedParentVersion(self.meta.latest_version),
                SnapshotUrgency::None,
            ));
        }

        Ok((AddVersionResult::Ok(version_id), self.snapshot_urgency()))
    }

    async fn get_child_version(
        &mut self,
        parent_version_id: VersionId,
    ) -> Result<GetVersionResult> {
        let version = match self.get_version_by_parent_version_id(&parent_version_id)? {
            Some(v) => Some(v),
            None => {
                self.pull()?;
                self.get_version_by_parent_version_id(&parent_version_id)?
            }
        };
        match version {
            Some(v) => Ok(GetVersionResult::Version {
                version_id: v.version_id,
                parent_version_id: v.parent_version_id,
                history_segment: v.history_segment,
            }),
            None => Ok(GetVersionResult::NoSuchVersion),
        }
    }

    async fn add_snapshot(&mut self, version_id: VersionId, snapshot: Snapshot) -> Result<()> {
        self.pull()?;
        // Write the snapshot to a file.
        let unsealed = Unsealed {
            version_id,
            payload: snapshot,
        };
        let sealed = self.cryptor.seal(unsealed)?;
        let snapshot_file = SnapshotFile {
            version_id,
            payload: Vec::<u8>::from(sealed),
        };
        let snapshot_path = self.local_path.join("snapshot");
        let f = File::create(&snapshot_path)?;
        serde_json::to_writer(f, &snapshot_file)?;

        // Commit and push, reverting if push fails.
        self.stage_and_commit(&[&snapshot_path], "add snapshot")?;

        if !self.push()? {
            // Push was rejected.
            git_cmd(&self.local_path, &["reset", "HEAD~1", "--soft"])?;
            std::fs::remove_file(&snapshot_path)?;
            self.pull()?;
            self.read_meta()?;
            return Err(Error::Server("Couldn't push to remote.".into()));
        }

        // Cleanup: delete all version files covered by this snapshot, then commit and push.
        self.cleanup()?;
        Ok(())
    }

    async fn get_snapshot(&mut self) -> Result<Option<(VersionId, Snapshot)>> {
        self.pull()?;

        let snapshot_path = self.local_path.join("snapshot");
        if let Ok(file) = File::open(&snapshot_path) {
            let s: SnapshotFile = serde_json::from_reader(BufReader::new(file))?;
            let unsealed = self.cryptor.unseal(Sealed {
                version_id: s.version_id,
                payload: s.payload,
            })?;
            Ok(Some((unsealed.version_id, unsealed.payload)))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::server::NIL_VERSION_ID;

    use super::*;
    use tempfile::TempDir;

    fn make_server(dir: &Path) -> Result<GitSyncServer> {
        GitSyncServer::new(
            dir.to_path_buf(),
            "main".into(),
            "".into(),
            true,
            b"test-secret".to_vec(),
        )
    }

    /// Create a bare repo to act as a remote, then clone it into `clone_dir`.
    /// Returns a GitSyncServer backed by the clone, with the bare repo as its remote.
    fn make_server_with_remote(bare_dir: &Path, clone_dir: &Path) -> Result<GitSyncServer> {
        // Initialise the bare remote.
        git_cmd(
            bare_dir.parent().unwrap(),
            &[
                "init",
                "--bare",
                bare_dir.file_name().unwrap().to_str().unwrap(),
            ],
        )?;
        let bare_url = bare_dir.to_str().unwrap();
        GitSyncServer::new(
            clone_dir.to_path_buf(),
            "main".into(),
            bare_url.into(),
            false,
            b"test-secret".to_vec(),
        )
    }

    #[test]
    fn test_init_creates_repo_and_meta() -> Result<()> {
        let tmp = TempDir::new()?;
        let server = make_server(tmp.path())?;
        assert!(tmp.path().join("meta").exists());
        assert_eq!(server.meta.latest_version, Uuid::nil());
        Ok(())
    }

    #[test]
    fn test_init_idempotent() -> Result<()> {
        let tmp = TempDir::new()?;
        let s1 = make_server(tmp.path())?;
        // second call should succeed and load the same meta (same salt)
        let s2 = make_server(tmp.path())?;
        assert_eq!(s1.meta.salt, s2.meta.salt);
        Ok(())
    }

    #[test]
    fn test_read_write_meta_roundtrip() -> Result<()> {
        let tmp = TempDir::new()?;
        let mut server = make_server(tmp.path())?;

        let new_id = Uuid::new_v4();
        server.meta.latest_version = new_id;
        server.write_meta()?;

        server.meta.latest_version = Uuid::nil(); // clobber in-memory value
        server.read_meta()?;
        assert_eq!(server.meta.latest_version, new_id);
        Ok(())
    }

    #[test]
    fn test_push_and_pull() -> Result<()> {
        let tmp = TempDir::new()?;
        let bare = tmp.path().join("bare");
        let clone1 = tmp.path().join("clone1");
        let clone2 = tmp.path().join("clone2");

        // Set up first clone (initialises remote with the meta commit).
        let server1 = make_server_with_remote(&bare, &clone1)?;
        server1.push()?;

        // Clone a second copy directly via git.
        let bare_url = bare.to_str().unwrap();
        git_cmd(tmp.path(), &["clone", bare_url, "clone2"])?;
        git_cmd(&clone2, &["config", "user.email", "taskchampion@local"])?;
        git_cmd(&clone2, &["config", "user.name", "taskchampion"])?;

        // Write a new file in clone1 and push it.
        let new_file = clone1.join("testfile");
        std::fs::write(&new_file, b"hello")?;
        server1.stage_and_commit(&[&new_file], "add testfile")?;
        assert!(server1.push()?);

        // Build a server for clone2 and pull
        // It should see the new file.
        let bare_url_str: String = bare_url.into();
        let server2 = GitSyncServer::new(
            clone2.clone(),
            "main".into(),
            bare_url_str,
            false,
            b"test-secret".to_vec(),
        )?;
        server2.pull()?;
        assert!(clone2.join("testfile").exists());
        Ok(())
    }

    #[tokio::test]
    async fn test_add_zero_base() -> Result<()> {
        let tmp = TempDir::new()?;
        let mut server = make_server(tmp.path())?;
        let history = b"1234".to_vec();
        match server.add_version(NIL_VERSION_ID, history.clone()).await?.0 {
            AddVersionResult::ExpectedParentVersion(_) => {
                panic!("should have accepted the version")
            }
            AddVersionResult::Ok(version_id) => {
                // Verify the version file exists on disk.
                let filename = format!("v-{}-{}", NIL_VERSION_ID.simple(), version_id.simple());
                assert!(
                    tmp.path().join(&filename).exists(),
                    "version file missing: {filename}"
                );
                // Verify meta was updated.
                assert_eq!(server.meta.latest_version, version_id);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_add_nonzero_base() -> Result<()> {
        let tmp = TempDir::new()?;
        let mut server = make_server(tmp.path())?;
        let history = b"1234".to_vec();
        let parent_version_id = Uuid::new_v4();

        // OK because latest == NIL (repo is empty).
        match server.add_version(parent_version_id, history).await?.0 {
            AddVersionResult::ExpectedParentVersion(_) => {
                panic!("should have accepted the version")
            }
            AddVersionResult::Ok(version_id) => {
                assert_eq!(server.meta.latest_version, version_id);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_add_nonzero_base_forbidden() -> Result<()> {
        let tmp = TempDir::new()?;
        let mut server = make_server(tmp.path())?;
        let history = b"1234".to_vec();
        let parent_version_id = Uuid::new_v4();

        // Add a first version.
        if let (AddVersionResult::ExpectedParentVersion(_), _) = server
            .add_version(parent_version_id, history.clone())
            .await?
        {
            panic!("should have accepted the first version");
        }

        // Try to add another with the same (now stale) parent, should be rejected.
        match server.add_version(parent_version_id, history).await?.0 {
            AddVersionResult::Ok(_) => panic!("should not have accepted the version"),
            AddVersionResult::ExpectedParentVersion(expected) => {
                assert_eq!(expected, server.meta.latest_version);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_add_version_conflict_with_remote() -> Result<()> {
        let tmp = TempDir::new()?;
        let bare = tmp.path().join("bare");
        let clone1 = tmp.path().join("clone1");
        let clone2 = tmp.path().join("clone2");

        let mut server1 = make_server_with_remote(&bare, &clone1)?;
        server1.push()?;

        let bare_url = bare.to_str().unwrap();
        git_cmd(tmp.path(), &["clone", bare_url, "clone2"])?;
        git_cmd(&clone2, &["config", "user.email", "taskchampion@local"])?;
        git_cmd(&clone2, &["config", "user.name", "taskchampion"])?;
        let mut server2 = GitSyncServer::new(
            clone2,
            "main".into(),
            bare_url.into(),
            false,
            b"test-secret".to_vec(),
        )?;

        // server1 adds a version from NIL parent and pushes successfully.
        let (result1, _) = server1.add_version(NIL_VERSION_ID, b"v1".to_vec()).await?;
        let v1_id = match result1 {
            AddVersionResult::Ok(id) => id,
            AddVersionResult::ExpectedParentVersion(_) => panic!("server1 add failed"),
        };

        // server2 also tries to add from NIL, should fail with ExpectedParentVersion pointing at v1.
        let (result2, _) = server2.add_version(NIL_VERSION_ID, b"v2".to_vec()).await?;
        match result2 {
            AddVersionResult::Ok(_) => panic!("server2 should have been rejected"),
            AddVersionResult::ExpectedParentVersion(expected) => {
                assert_eq!(expected, v1_id);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn get_child_version() -> Result<()> {
        let tmp = TempDir::new()?;
        let mut server = make_server(tmp.path())?;
        let history = b"1234".to_vec();
        let parent_version_id = Uuid::new_v4();

        // Version doesn't exist yet
        assert_eq!(
            server.get_child_version(parent_version_id).await?,
            GetVersionResult::NoSuchVersion
        );

        // Add a first version.
        let (rst, _) = server
            .add_version(parent_version_id, history.clone())
            .await?;

        let AddVersionResult::Ok(version_id) = rst else {
            panic!("Couldn't add version");
        };
        // Now we should be able to get the version.
        match server.get_child_version(parent_version_id).await? {
            GetVersionResult::Version {
                version_id: v_id,
                parent_version_id: p_id,
                history_segment: h_seg,
            } => {
                assert_eq!(v_id, version_id);
                assert_eq!(p_id, parent_version_id);
                assert_eq!(h_seg, history);
            }
            GetVersionResult::NoSuchVersion => panic!("Failed to read version"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_get_child_version_from_remote() -> Result<()> {
        let tmp = TempDir::new()?;
        let bare = tmp.path().join("bare");
        let clone1 = tmp.path().join("clone1");
        let clone2 = tmp.path().join("clone2");

        let mut server1 = make_server_with_remote(&bare, &clone1)?;
        let bare_url = bare.to_str().unwrap();

        // server1 adds a version and pushes it.
        let (result, _) = server1
            .add_version(NIL_VERSION_ID, b"history".to_vec())
            .await?;
        let AddVersionResult::Ok(version_id) = result else {
            panic!("server1 add_version failed");
        };

        let mut server2 = GitSyncServer::new(
            clone2,
            "main".into(),
            bare_url.into(),
            false,
            b"test-secret".to_vec(),
        )?;

        // get_child_version should pull and find the version.
        match server2.get_child_version(NIL_VERSION_ID).await? {
            GetVersionResult::Version {
                version_id: v_id,
                history_segment,
                ..
            } => {
                assert_eq!(v_id, version_id);
                assert_eq!(history_segment, b"history".to_vec());
            }
            GetVersionResult::NoSuchVersion => panic!("should have found the version after pull"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_empty() -> Result<()> {
        let tmp = TempDir::new()?;
        let mut server = make_server(tmp.path())?;
        assert_eq!(server.get_snapshot().await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_roundtrip() -> Result<()> {
        let tmp = TempDir::new()?;
        let mut server = make_server(tmp.path())?;

        let version_id = Uuid::new_v4();
        let data = b"my snapshot data".to_vec();
        server.add_snapshot(version_id, data.clone()).await?;

        let result = server.get_snapshot().await?;
        assert!(result.is_some(), "expected a snapshot");
        let (got_id, got_data) = result.unwrap();
        assert_eq!(got_id, version_id);
        assert_eq!(got_data, data);
        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_from_remote() -> Result<()> {
        let tmp = TempDir::new()?;
        let bare = tmp.path().join("bare");
        let clone1 = tmp.path().join("clone1");
        let clone2 = tmp.path().join("clone2");

        let mut server1 = make_server_with_remote(&bare, &clone1)?;
        let bare_url = bare.to_str().unwrap();
        // server1 stores a snapshot and pushes it.
        let version_id = Uuid::new_v4();
        let data = b"snapshot payload".to_vec();
        server1.add_snapshot(version_id, data.clone()).await?;
        // server2 should pull and find it.
        let mut server2 = GitSyncServer::new(
            clone2.clone(),
            "main".into(),
            bare_url.into(),
            false,
            b"test-secret".to_vec(),
        )?;
        let result = server2.get_snapshot().await?;
        assert!(result.is_some(), "expected snapshot from remote");
        let (got_id, got_data) = result.unwrap();
        assert_eq!(got_id, version_id);
        assert_eq!(got_data, data);
        Ok(())
    }

    /// A second `add_snapshot` should overwrite the first: only the latest snapshot is
    /// returned by `get_snapshot`.
    #[tokio::test]
    async fn test_snapshot_overwrite() -> Result<()> {
        let tmp = TempDir::new()?;
        let mut server = make_server(tmp.path())?;

        let v1 = Uuid::new_v4();
        server.add_snapshot(v1, b"first snapshot".to_vec()).await?;

        let v2 = Uuid::new_v4();
        server.add_snapshot(v2, b"second snapshot".to_vec()).await?;

        let result = server.get_snapshot().await?;
        assert!(result.is_some());
        let (got_id, got_data) = result.unwrap();
        assert_eq!(got_id, v2, "expected the second snapshot's version_id");
        assert_eq!(got_data, b"second snapshot".to_vec());
        Ok(())
    }

    /// `add_snapshot` push-failure rollback: install a pre-receive hook on the bare repo that
    /// rejects all pushes (while still allowing fetches), forcing a push rejection.
    /// After the Err, the working tree must be clean and the snapshot file must be absent.
    #[tokio::test]
    async fn test_snapshot_push_rejected_rollback() -> Result<()> {
        let tmp = TempDir::new()?;
        let bare = tmp.path().join("bare");
        let clone1 = tmp.path().join("clone1");

        let mut server = make_server_with_remote(&bare, &clone1)?;

        // Add a version so the remote branch exists (required for pull's ls-remote check).
        server.add_version(NIL_VERSION_ID, b"v1".to_vec()).await?;

        // Install a pre-receive hook that rejects all pushes.
        // Fetches still work because hooks only run on the receive side.
        let hooks_dir = bare.join("hooks");
        fs::create_dir_all(&hooks_dir)?;
        let hook = hooks_dir.join("pre-receive");
        std::fs::write(&hook, b"#!/bin/sh\nexit 1\n")?;
        // Make the hook executable.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&hook, std::fs::Permissions::from_mode(0o755))?;
        }

        // add_snapshot should pull successfully then fail to push (hook rejects it).
        let v1 = Uuid::new_v4();
        let result = server.add_snapshot(v1, b"snap".to_vec()).await;
        assert!(
            result.is_err(),
            "expected Err when push is rejected by hook"
        );

        // The snapshot file must not remain in the working tree.
        assert!(
            !server.local_path.join("snapshot").exists(),
            "snapshot file should be removed after rollback"
        );

        // The git index must be clean (no staged changes left over).
        let status = Command::new("git")
            .args(["status", "--porcelain"])
            .current_dir(&server.local_path)
            .output()?;
        assert!(
            status.stdout.is_empty(),
            "git index should be clean after rollback, got: {}",
            String::from_utf8_lossy(&status.stdout)
        );

        Ok(())
    }

    /// After `add_snapshot`, all version files covered by the snapshot should be deleted
    /// (cleanup is called automatically by `add_snapshot`).
    #[tokio::test]
    async fn test_cleanup_removes_version_files() -> Result<()> {
        let tmp = TempDir::new()?;
        let mut server = make_server(tmp.path())?;

        // Add a chain of three versions.
        let (r1, _) = server.add_version(NIL_VERSION_ID, b"v1".to_vec()).await?;
        let AddVersionResult::Ok(v1) = r1 else {
            panic!("add_version 1 failed");
        };
        let (r2, _) = server.add_version(v1, b"v2".to_vec()).await?;
        let AddVersionResult::Ok(v2) = r2 else {
            panic!("add_version 2 failed");
        };
        let (r3, _) = server.add_version(v2, b"v3".to_vec()).await?;
        let AddVersionResult::Ok(v3) = r3 else {
            panic!("add_version 3 failed");
        };
        // Confirm they exist.
        assert!(tmp.path().join(format!("v-{}-{}", NIL_VERSION_ID.simple(), v1.simple())).exists());
        assert!(tmp.path().join(format!("v-{}-{}", v1.simple(), v2.simple())).exists());
        assert!(tmp.path().join(format!("v-{}-{}", v2.simple(), v3.simple())).exists());

        // Snapshot at v3; cleanup should remove all three version files.
        server.add_snapshot(v3, b"full state".to_vec()).await?;

        assert!(!tmp.path().join(format!("v-{}-{}", NIL_VERSION_ID.simple(), v1.simple())).exists(), "v1 file should be gone");
        assert!(!tmp.path().join(format!("v-{}-{}", v1.simple(), v2.simple())).exists(), "v2 file should be gone");
        assert!(!tmp.path().join(format!("v-{}-{}", v2.simple(), v3.simple())).exists(), "v3 file should be gone");
        // The snapshot file itself must still exist.
        assert!(tmp.path().join("snapshot").exists(), "snapshot file should remain");
        Ok(())
    }
}
