use crate::errors::Result;
use crate::server::encryption::Cryptor;
use crate::server::{
    AddVersionResult, GetVersionResult, HistorySegment, Server, Snapshot, SnapshotUrgency,
    VersionId,
};
use crate::Error;
use async_trait::async_trait;
use log::info;
use serde::{Deserialize, Serialize};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use uuid::Uuid;
#[derive(Serialize, Deserialize, Debug)]
struct Version {
    version_id: VersionId,
    parent_version_id: VersionId,
    history_segment: HistorySegment,
}

/// The meta file holds the UUID of the most recent Version and the salt.
#[derive(Serialize, Deserialize, Debug)]
struct Meta {
    #[serde(with = "uuid::serde::simple")]
    latest: VersionId,
    salt: String,  // hex-encoded
}

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
    pub(crate) fn new(
        local_path: PathBuf,
        branch: String,
        remote: String,
        local_only: bool,
        encryption_secret: Vec<u8>,
    ) -> Result<GitSyncServer> {
        let meta = Self::init_repo(&local_path, &branch, &remote, local_only)?;
        let salt_bytes = hex::decode(&meta.salt)
            .map_err(|e| Error::Server(format!("invalid salt in meta file: {e}")))?;
        let cryptor = Cryptor::new(&salt_bytes, &encryption_secret.into())?;
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
            // For a brand-new repo (no commits) `git checkout -b` also fails, so use
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
                    latest: Uuid::nil(),
                    salt: hex::encode(Cryptor::gen_salt()?),
                };
                let f = File::create_new(&meta_path)?;
                serde_json::to_writer(f, &m)?;
                git_cmd(local_path, &["add", "meta"])?;
                git_cmd(local_path, &["commit", "-m", "init taskchampion repo"])?;
                m
            }
        };

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
        let mut file = File::open(&meta_path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        self.meta = serde_json::from_str(&contents)?;
        Ok(())
    }

    /// Serialise self.meta to the meta file and return its path.
    fn write_meta(&self) -> Result<PathBuf> {
        let meta_path = self.local_path.join("meta");
        let f = File::create(&meta_path)?;
        serde_json::to_writer(f, &self.meta)?;
        Ok(meta_path)
    }
}

#[async_trait(?Send)]
impl Server for GitSyncServer {
    async fn add_version(
        &mut self,
        parent_version_id: VersionId,
        history_segment: HistorySegment,
    ) -> Result<(AddVersionResult, SnapshotUrgency)> {
        todo!();
        // check the parent_version_id for linearity
        // if parent_version_id != self.meta.latest {
        //     // Pull and try again if remote exists and not local_only

        //     // Else fail
        //     return Ok((
        //         AddVersionResult::ExpectedParentVersion(self.meta.latest),
        //         SnapshotUrgency::None,
        //     ));
        // }

        // // invent a new ID for this version
        // let version_id = Uuid::new_v4();
        // let version_path = self.add_version_by_parent_version_id(Version {
        //     version_id,
        //     parent_version_id,
        //     history_segment,
        // })?;
        // let meta = self.set_latest_version_id(version_id)?;
        // git add and commit version_path and meta
        // if remote mode:
        //   push
        //   if push fails
        //     revert local commit
        //     re-read latest
        //     return Ok((
        //         AddVersionResult::ExpectedParentVersion(self.meta.latest),
        //         SnapshotUrgency::None,
        //     ));
        // Ok((AddVersionResult::Ok(version_id), SnapshotUrgency::None))
    }

    async fn get_child_version(
        &mut self,
        parent_version_id: VersionId,
    ) -> Result<GetVersionResult> {
        todo!();
        // if let Some(version) = self.get_version_by_parent_version_id(parent_version_id)? {
        //     Ok(GetVersionResult::Version {
        //         version_id: version.version_id,
        //         parent_version_id: version.parent_version_id,
        //         history_segment: version.history_segment,
        //     })
        // } else {
        //     Ok(GetVersionResult::NoSuchVersion)
        // }
    }

    async fn add_snapshot(&mut self, _version_id: VersionId, _snapshot: Snapshot) -> Result<()> {
        todo!();
    }

    async fn get_snapshot(&mut self) -> Result<Option<(VersionId, Snapshot)>> {
        todo!();
    }
}

#[cfg(test)]
mod test {
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

    #[test]
    fn test_init_creates_repo_and_meta() -> Result<()> {
        let tmp = TempDir::new()?;
        let server = make_server(tmp.path())?;
        assert!(tmp.path().join("meta").exists());
        assert_eq!(server.meta.latest, Uuid::nil());
        eprintln!("tmp dir: {}", tmp.path().display());
        std::mem::forget(tmp);
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
        server.meta.latest = new_id;
        server.write_meta()?;

        server.meta.latest = Uuid::nil(); // clobber in-memory value
        server.read_meta()?;
        assert_eq!(server.meta.latest, new_id);
        Ok(())
    }

    #[test]
    fn test_stage_and_commit() -> Result<()> {
        let tmp = TempDir::new()?;
        let server = make_server(tmp.path())?;

        // Write a new file, stage and commit it.
        let new_file = tmp.path().join("testfile");
        std::fs::write(&new_file, b"hello, taskchampion")?;
        server.stage_and_commit(&[&new_file], "test commit")?;

        // Verify the commit exists, git show HEAD succeeds only if there is a HEAD commit.
        git_cmd(tmp.path(), &["show", "HEAD"])?;
        eprintln!("tmp dir: {}", tmp.path().display());
        std::mem::forget(tmp);
        Ok(())
    }

    // #[tokio::test]
    // async fn test_empty() -> Result<()> { ... }

    // #[tokio::test]
    // async fn test_add_zero_base() -> Result<()> { ... }

    // #[tokio::test]
    // async fn test_add_nonzero_base() -> Result<()> { ... }

    // #[tokio::test]
    // async fn test_add_nonzero_base_forbidden() -> Result<()> { ... }

    // #[tokio::test]
    // async fn test_snapshot() -> Result<()> { ... }

    // #[tokio::test]
    // async fn test_conflict_with_local_remote() -> Result<()> { ... }
}
