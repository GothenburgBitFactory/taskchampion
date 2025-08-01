use super::{inmemory::InMemoryStorage, Storage};
use crate::errors::Result;
use crate::storage::async_storage::adapter::BlockingStorageAdapter;
#[cfg(feature = "storage-sqlite")]
use crate::storage::sqlite::AsyncSqliteStorage;
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

impl StorageConfig {
    pub fn into_storage(self) -> Result<Box<dyn Storage>> {
        Ok(match self {
            #[cfg(feature = "storage-sqlite")]
            StorageConfig::OnDisk {
                taskdb_dir,
                create_if_missing,
                access_mode,
            } => {
                let rt = tokio::runtime::Runtime::new()?;
                let async_storage = rt.block_on(AsyncSqliteStorage::new(
                    taskdb_dir,
                    access_mode,
                    create_if_missing,
                ))?;
                Box::new(BlockingStorageAdapter::new(Box::new(async_storage))?)
            }
            StorageConfig::InMemory => Box::new(BlockingStorageAdapter::new(Box::new(
                InMemoryStorage::new(),
            ))?),
        })
    }
}
