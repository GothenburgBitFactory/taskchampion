use anyhow::anyhow;

#[cfg(feature = "storage-sqlite")]
use super::sqlite::SqliteStorage;
use super::{inmemory::InMemoryStorage, Storage};
use crate::errors::Result;
use crate::{errors, Replica};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessMode {
    ReadOnly,
    ReadWrite,
}

/// The configuration required for a replica's storage.
#[non_exhaustive]
pub enum StorageConfig {
    /// Store the data on disk.  This is the common choice.
    #[cfg(feature = "storage-sqlite")]
    OnDisk {
        /// Path containing the task DB.
        taskdb_dir: PathBuf,

        /// Create the DB if it does not already exist. This will occur
        /// even if access_mode is `ReadOnly`.
        create_if_missing: bool,

        /// Access mode for this database.
        access_mode: AccessMode,
    },
    /// Store the data in memory.  This is only useful for testing.
    InMemory,
}

#[cfg(feature = "storage-sqlite")]
impl TryFrom<StorageConfig> for SqliteStorage {
    type Error = errors::Error;

    fn try_from(config: StorageConfig) -> Result<Self> {
        match config {
            #[cfg(feature = "storage-sqlite")]
            StorageConfig::OnDisk {
                taskdb_dir,
                create_if_missing,
                access_mode,
            } => SqliteStorage::new(taskdb_dir, access_mode, create_if_missing),
            StorageConfig::InMemory => Err(errors::Error::Other(anyhow!(
                "Cannot create SqliteStorage from InMemory config"
            ))),
        }
    }
}

impl TryFrom<StorageConfig> for InMemoryStorage {
    type Error = errors::Error;

    fn try_from(config: StorageConfig) -> Result<Self> {
        match config {
            StorageConfig::InMemory => Ok(InMemoryStorage::new()),
            #[cfg(feature = "storage-sqlite")]
            StorageConfig::OnDisk { .. } => Err(errors::Error::Other(anyhow!(
                "Cannot create InMemoryStorage from OnDisk config"
            ))),
        }
    }
}

impl<S> TryFrom<StorageConfig> for Replica<S>
where
    S: Storage + TryFrom<StorageConfig>,
    errors::Error: From<S::Error>,
{
    type Error = errors::Error;

    fn try_from(config: StorageConfig) -> Result<Self> {
        let storage = config.try_into()?;
        Ok(Replica::new(storage))
    }
}
