use crate::errors::Result;
use crate::storage::config::AccessMode;
use crate::storage::send_wrapper::Wrapper;
use crate::storage::sqlite::inner::SqliteStorageInner;
use crate::storage::{Storage, StorageTxn};
use async_trait::async_trait;
use rusqlite::types::FromSql;
use rusqlite::ToSql;
use std::path::Path;
use uuid::Uuid;

mod inner;
mod schema;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum SqliteError {
    #[error("SQLite transaction already committted")]
    TransactionAlreadyCommitted,
    #[error("Task storage was opened in read-only mode")]
    ReadOnlyStorage,
}

/// Newtype to allow implementing `FromSql` for foreign `uuid::Uuid`
pub(crate) struct StoredUuid(pub(crate) Uuid);

/// Conversion from Uuid stored as a string (rusqlite's uuid feature stores as binary blob)
impl FromSql for StoredUuid {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let u = Uuid::parse_str(value.as_str()?)
            .map_err(|_| rusqlite::types::FromSqlError::InvalidType)?;
        Ok(StoredUuid(u))
    }
}

/// Store Uuid as string in database
impl ToSql for StoredUuid {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        let s = self.0.to_string();
        Ok(s.into())
    }
}

/// SqliteStorage stores task data in a file on disk.
#[derive(Clone)]
pub struct SqliteStorage(Wrapper);

impl SqliteStorage {
    pub async fn new<P: AsRef<Path>>(
        path: P,
        access_mode: AccessMode,
        create_if_missing: bool,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        Ok(Self(
            Wrapper::new(async move || {
                let inner = SqliteStorageInner::new(&path, access_mode, create_if_missing)?;
                Ok(inner)
            })
            .await?,
        ))
    }
}

#[async_trait]
impl Storage for SqliteStorage {
    async fn txn<'a>(&'a mut self) -> Result<Box<dyn StorageTxn + Send + 'a>> {
        Ok(self.0.txn().await?)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::errors::Error;
    use crate::storage::config::AccessMode;
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;

    async fn storage() -> Result<SqliteStorage> {
        let tmp_dir = TempDir::new()?;
        SqliteStorage::new(tmp_dir.path(), AccessMode::ReadWrite, true).await
    }

    crate::storage::test::storage_tests!(storage().await?);

    #[tokio::test]
    async fn test_implicit_rollback() -> Result<()> {
        let mut storage = storage().await?;
        let uuid = Uuid::new_v4();

        // Begin a transaction, create a task, but do not commit.
        // The transaction will go out of scope, triggering Drop.
        {
            let mut txn = storage.txn().await?;
            assert!(txn.create_task(uuid).await?);
            // txn is dropped here, which should trigger a rollback message.
        }

        // Begin a new transaction and verify the task does not exist.
        let mut txn = storage.txn().await?;
        let task = txn.get_task(uuid).await?;
        assert_eq!(task, None, "Task should not exist after implicit rollback");

        Ok(())
    }

    #[tokio::test]
    async fn test_init_failure() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let file_path = tmp_dir.path().join("a_file");
        std::fs::write(&file_path, "I am a file, not a directory")?;

        // Try to create the storage inside a path that is a file, not a directory.
        // This should cause SqliteStorage::new to fail inside the actor thread.
        let result = SqliteStorage::new(&file_path, AccessMode::ReadWrite, true).await;
        assert!(
            result.is_err(),
            "Initialization should fail for an invalid path"
        );

        // Check for the expected error message propagated from the actor thread.
        if let Err(Error::Database(msg)) = result {
            assert!(
                msg.contains("Cannot create directory"),
                "Error message should indicate a directory creation problem"
            );
        } else {
            panic!("Expected a Database error");
        }

        Ok(())
    }
}
