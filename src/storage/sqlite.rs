use crate::errors::{Error, Result};
use crate::operation::Operation;
use crate::storage::async_storage::AsyncStorage;
use crate::storage::config::AccessMode;
use crate::storage::{TaskMap, VersionId, DEFAULT_BASE_VERSION};
use anyhow::Context;
use async_trait::async_trait;
use rusqlite::types::{FromSql, ToSql};
use rusqlite::{params, OpenFlags, OptionalExtension};
use std::path::Path;
use tokio_rusqlite::Connection;
use uuid::Uuid;

mod schema;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum SqliteError {
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

/// Wraps [`TaskMap`] (type alias for HashMap) so we can implement rusqlite conversion traits for it
struct StoredTaskMap(TaskMap);

/// Parses TaskMap stored as JSON in string column
impl FromSql for StoredTaskMap {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let o: TaskMap = serde_json::from_str(value.as_str()?)
            .map_err(|_| rusqlite::types::FromSqlError::InvalidType)?;
        Ok(StoredTaskMap(o))
    }
}

/// Stores TaskMap in string column
impl ToSql for StoredTaskMap {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        let s = serde_json::to_string(&self.0)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
        Ok(s.into())
    }
}

/// Stores [`Operation`] in SQLite
impl FromSql for Operation {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let o: Operation = serde_json::from_str(value.as_str()?)
            .map_err(|_| rusqlite::types::FromSqlError::InvalidType)?;
        Ok(o)
    }
}

/// Parses Operation stored as JSON in string column
impl ToSql for Operation {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        let s = serde_json::to_string(&self)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
        Ok(s.into())
    }
}

/// AsyncSqliteStorage is an on-disk storage backed by SQLite3.
pub(super) struct AsyncSqliteStorage {
    con: Connection,
    access_mode: AccessMode,
}

impl AsyncSqliteStorage {
    pub(super) async fn new<P: AsRef<Path>>(
        directory: P,
        access_mode: AccessMode,
        create_if_missing: bool,
    ) -> Result<AsyncSqliteStorage> {
        let directory = directory.as_ref();
        if create_if_missing {
            // Ensure parent folder exists
            std::fs::create_dir_all(directory).map_err(|e| {
                Error::Database(format!("Cannot create directory {directory:?}: {e}"))
            })?;
        }

        // Open (or create) database
        let db_file = directory.join("taskchampion.sqlite3");
        let mut flags = OpenFlags::default();

        // Determine the mode in which to open the DB itself, using read-write mode
        // for a non-existent DB to allow opening an empty DB in read-only mode.
        let mut open_access_mode = access_mode;
        if create_if_missing && access_mode == AccessMode::ReadOnly && !db_file.exists() {
            open_access_mode = AccessMode::ReadWrite;
        }

        // default contains SQLITE_OPEN_CREATE, so remove it if we are not to
        // create a DB when missing.
        if !create_if_missing {
            flags.remove(OpenFlags::SQLITE_OPEN_CREATE);
        }

        if open_access_mode == AccessMode::ReadOnly {
            flags.remove(OpenFlags::SQLITE_OPEN_READ_WRITE);
            flags.insert(OpenFlags::SQLITE_OPEN_READ_ONLY);
            // SQLite does not allow create when opening read-only
            flags.remove(OpenFlags::SQLITE_OPEN_CREATE);
        }

        let mut con = Connection::open_with_flags(db_file, flags).await?;

        // Initialize database
        con.call(|c| {
            c.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))
                .context("Setting journal_mode=WAL")
                .map_err(Error::from)?;
            Ok(())
        })
        .await?;

        // Await the async upgrade function in the async context of `new`
        if open_access_mode == AccessMode::ReadWrite {
            schema::upgrade_db(&mut con).await?;
        }

        Ok(Self { access_mode, con })
    }

    fn check_write_access(&self) -> Result<()> {
        if self.access_mode != AccessMode::ReadWrite {
            Err(SqliteError::ReadOnlyStorage.into())
        } else {
            Ok(())
        }
    }
}

/// Helper to get the largest index + 1 from the working set.
fn get_next_working_set_number(c: &rusqlite::Connection) -> Result<usize> {
    let next_id: Option<usize> = c
        .query_row(
            "SELECT COALESCE(MAX(id), 0) + 1 FROM working_set",
            [],
            |r| r.get(0),
        )
        .optional()
        .context("Getting highest working set ID")?;
    Ok(next_id.unwrap_or(0))
}

#[async_trait]
impl AsyncStorage for AsyncSqliteStorage {
    async fn get_task(&mut self, uuid: Uuid) -> Result<Option<TaskMap>> {
        let result: Option<StoredTaskMap> = self
            .con
            .call(move |c| {
                c.query_row(
                    "SELECT data FROM tasks WHERE uuid = ? LIMIT 1",
                    [&StoredUuid(uuid)],
                    |r| r.get(0),
                )
                .optional()
                .map_err(tokio_rusqlite::Error::from)
            })
            .await?;

        // Get task from "stored" wrapper
        Ok(result.map(|t| t.0))
    }

    async fn get_pending_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        self.con
            .call(|c| {
                let mut q = c.prepare(
                    "SELECT tasks.* FROM tasks JOIN working_set ON tasks.uuid = working_set.uuid",
                )?;
                let rows = q.query_map([], |r| {
                    let uuid: StoredUuid = r.get("uuid")?;
                    let data: StoredTaskMap = r.get("data")?;
                    Ok((uuid.0, data.0))
                })?;

                let mut res = Vec::new();
                for row in rows {
                    res.push(row?)
                }

                Ok(res)
            })
            .await
            .map_err(Error::from)
    }

    async fn create_task(&mut self, uuid: Uuid) -> Result<bool> {
        self.check_write_access()?;
        self.con
            .call(move |c| {
                let count: usize = c.query_row(
                    "SELECT count(uuid) FROM tasks WHERE uuid = ?",
                    [&StoredUuid(uuid)],
                    |x| x.get(0),
                )?;
                if count > 0 {
                    return Ok(false);
                }

                let data = TaskMap::default();
                c.execute(
                    "INSERT INTO tasks (uuid, data) VALUES (?, ?)",
                    params![&StoredUuid(uuid), &StoredTaskMap(data)],
                )?;
                Ok(true)
            })
            .await
            .context("Create task query")
            .map_err(Error::from)
    }

    async fn set_task(&mut self, uuid: Uuid, task: TaskMap) -> Result<()> {
        self.check_write_access()?;
        self.con
            .call(move |c| {
                c.execute(
                    "INSERT OR REPLACE INTO tasks (uuid, data) VALUES (?, ?)",
                    params![&StoredUuid(uuid), &StoredTaskMap(task)],
                )?;
                Ok(())
            })
            .await
            .context("Update task query")
            .map_err(Error::from)
    }

    async fn delete_task(&mut self, uuid: Uuid) -> Result<bool> {
        self.check_write_access()?;
        self.con
            .call(move |c| {
                let changed = c.execute("DELETE FROM tasks WHERE uuid = ?", [&StoredUuid(uuid)])?;
                Ok(changed > 0)
            })
            .await
            .context("Delete task query")
            .map_err(Error::from)
    }

    async fn all_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        self.con
            .call(|c| {
                let mut q = c.prepare("SELECT uuid, data FROM tasks")?;
                let rows = q.query_map([], |r| {
                    let uuid: StoredUuid = r.get("uuid")?;
                    let data: StoredTaskMap = r.get("data")?;
                    Ok((uuid.0, data.0))
                })?;

                let mut ret = vec![];
                for r in rows {
                    ret.push(r?);
                }
                Ok(ret)
            })
            .await
            .map_err(Error::from)
    }

    async fn all_task_uuids(&mut self) -> Result<Vec<Uuid>> {
        self.con
            .call(|c| {
                let mut q = c.prepare("SELECT uuid FROM tasks")?;
                let rows = q.query_map([], |r| {
                    let uuid: StoredUuid = r.get("uuid")?;
                    Ok(uuid.0)
                })?;

                let mut ret = vec![];
                for r in rows {
                    ret.push(r?);
                }
                Ok(ret)
            })
            .await
            .map_err(Error::from)
    }

    async fn base_version(&mut self) -> Result<VersionId> {
        self.con
            .call(|c| {
                let version: Option<StoredUuid> = c
                    .query_row(
                        "SELECT value FROM sync_meta WHERE key = 'base_version'",
                        [],
                        |r| r.get("value"),
                    )
                    .optional()?;
                Ok(version.map(|u| u.0).unwrap_or(DEFAULT_BASE_VERSION))
            })
            .await
            .map_err(Error::from)
    }

    async fn set_base_version(&mut self, version: VersionId) -> Result<()> {
        self.check_write_access()?;
        self.con
            .call(move |c| {
                c.execute(
                    "INSERT OR REPLACE INTO sync_meta (key, value) VALUES (?, ?)",
                    params!["base_version", &StoredUuid(version)],
                )?;
                Ok(())
            })
            .await
            .context("Set base version")
            .map_err(Error::from)
    }

    async fn get_task_operations(&mut self, uuid: Uuid) -> Result<Vec<Operation>> {
        self.con
            .call(move |c| {
                let mut q =
                    c.prepare("SELECT data FROM operations where uuid=? ORDER BY id ASC")?;
                let rows = q.query_map([&StoredUuid(uuid)], |r| {
                    let data: Operation = r.get("data")?;
                    Ok(data)
                })?;

                let mut ret = vec![];
                for r in rows {
                    ret.push(r?);
                }
                Ok(ret)
            })
            .await
            .map_err(Error::from)
    }

    async fn unsynced_operations(&mut self) -> Result<Vec<Operation>> {
        self.con
            .call(|c| {
                let mut q =
                    c.prepare("SELECT data FROM operations WHERE NOT synced ORDER BY id ASC")?;
                let rows = q.query_map([], |r| {
                    let data: Operation = r.get("data")?;
                    Ok(data)
                })?;

                let mut ret = vec![];
                for r in rows {
                    ret.push(r?);
                }
                Ok(ret)
            })
            .await
            .map_err(Error::from)
    }

    async fn num_unsynced_operations(&mut self) -> Result<usize> {
        self.con
            .call(|c| {
                let count: usize = c.query_row(
                    "SELECT count(*) FROM operations WHERE NOT synced",
                    [],
                    |x| x.get(0),
                )?;
                Ok(count)
            })
            .await
            .map_err(Error::from)
    }

    async fn add_operation(&mut self, op: Operation) -> Result<()> {
        self.check_write_access()?;
        self.con
            .call(move |c| {
                c.execute("INSERT INTO operations (data) VALUES (?)", params![&op])?;
                Ok(())
            })
            .await
            .context("Add operation query")
            .map_err(Error::from)
    }

    async fn remove_operation(&mut self, op: Operation) -> Result<()> {
        self.check_write_access()?;
        self.con
            .call(move |c| {
                let last: Option<(u32, Operation)> = c
                    .query_row(
                        "SELECT id, data FROM operations WHERE NOT synced ORDER BY id DESC LIMIT 1",
                        [],
                        |x| Ok((x.get(0)?, x.get(1)?)),
                    )
                    .optional()?;

                // If there is a "last" operation, and it matches the given operation,
                // remove it.
                if let Some((last_id, last_op)) = last {
                    if last_op == op {
                        c.execute("DELETE FROM operations where id = ?", [last_id])
                            .context("Removing operation")
                            .map_err(Error::from)?;
                        return Ok(());
                    }
                }

                Err(tokio_rusqlite::Error::Other(
                    "Last operation does not match -- cannot remove".into(),
                ))
            })
            .await
            .map_err(Error::from)
    }

    async fn sync_complete(&mut self) -> Result<()> {
        self.check_write_access()?;
        self.con.call(|c| {
        c.execute(
            "UPDATE operations SET synced = true WHERE synced = false",
            [],
        ).context("Marking operations as synced")
        .map_err(Error::from)?;

        // Delete all operations for non-existent (usually, deleted) tasks.
        c.execute(
            r#"DELETE from operations
            WHERE uuid IN (
                SELECT operations.uuid FROM operations LEFT JOIN tasks ON operations.uuid = tasks.uuid WHERE tasks.uuid IS NULL
            )"#,
            [],
        ).context("Deleting orphaned operations")
        .map_err(Error::from)?;

        Ok(())}).await.map_err(Error::from)
    }

    async fn get_working_set(&mut self) -> Result<Vec<Option<Uuid>>> {
        self.con
            .call(|c| {
                let mut q = c.prepare("SELECT id, uuid FROM working_set ORDER BY id ASC")?;
                let rows = q
                    .query_map([], |r| {
                        let id: usize = r.get("id")?;
                        let uuid: StoredUuid = r.get("uuid")?;
                        Ok((id, uuid.0))
                    })
                    .context("Get working set query")
                    .map_err(Error::from)?;

                let rows: Vec<std::result::Result<(usize, Uuid), _>> = rows.collect();
                let mut res = Vec::with_capacity(rows.len());
                for _ in 0..get_next_working_set_number(c)
                    .context("Getting working set number")
                    .map_err(Error::from)?
                {
                    res.push(None);
                }

                for r in rows {
                    let (id, uuid) = r?;
                    res[id] = Some(uuid);
                }

                Ok(res)
            })
            .await
            .map_err(Error::from)
    }

    async fn add_to_working_set(&mut self, uuid: Uuid) -> Result<usize> {
        self.check_write_access()?;
        self.con
            .call(move |c| {
                let next_working_id = get_next_working_set_number(c)?;

                c.execute(
                    "INSERT INTO working_set (id, uuid) VALUES (?, ?)",
                    params![next_working_id, &StoredUuid(uuid)],
                )?;

                Ok(next_working_id)
            })
            .await
            .context("Create task query")
            .map_err(Error::from)
    }

    async fn set_working_set_item(&mut self, index: usize, uuid: Option<Uuid>) -> Result<()> {
        self.check_write_access()?;
        self.con
            .call(move |c| {
                match uuid {
                    // Add or override item
                    Some(uuid) => c.execute(
                        "INSERT OR REPLACE INTO working_set (id, uuid) VALUES (?, ?)",
                        params![index, &StoredUuid(uuid)],
                    ),
                    // Setting to None removes the row from database
                    None => c.execute("DELETE FROM working_set WHERE id = ?", [index]),
                }?;
                Ok(())
            })
            .await
            .context("Create task query")
            .map_err(Error::from)
    }

    async fn clear_working_set(&mut self) -> Result<()> {
        self.check_write_access()?;
        self.con
            .call(|c| {
                c.execute("DELETE FROM working_set", [])?;
                Ok(())
            })
            .await
            .context("Clear working set query")
            .map_err(Error::from)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::{self, taskmap_with};
    use chrono::Utc;
    use pretty_assertions::assert_eq;
    use rstest::rstest;
    use rusqlite::TransactionBehavior;
    use std::time::Duration;
    use tempfile::TempDir;

    async fn storage() -> Result<AsyncSqliteStorage> {
        let tmp_dir = TempDir::new()?;
        AsyncSqliteStorage::new(tmp_dir.path(), AccessMode::ReadWrite, true).await
    }

    crate::storage::test::async_storage_tests!(storage().await?);

    /// Manually create a 0_8_0 db, as based on a dump from an actual (test) user.
    /// This is used to test in-place upgrading.
    async fn create_0_8_0_db(path: &Path) -> Result<()> {
        let db_file = path.join("taskchampion.sqlite3");
        let con = Connection::open(db_file).await?;

        con.call(|c| {
            c.query_row("PRAGMA journal_mode=WAL", [], |_row| Ok(()))
                .context("Setting journal_mode=WAL")
                .map_err(Error::from)?;
            let queries = vec![
                r#"CREATE TABLE operations (id INTEGER PRIMARY KEY AUTOINCREMENT, data STRING);"#,
                r#"INSERT INTO operations VALUES(1,'"UndoPoint"');"#,
                r#"INSERT INTO operations VALUES(2,
                    '{"Create":{"uuid":"e2956511-fd47-4e40-926a-52616229c2fa"}}');"#,
                r#"INSERT INTO operations VALUES(3,
                    '{"Update":{"uuid":"e2956511-fd47-4e40-926a-52616229c2fa",
                    "property":"description",
                    "old_value":null,
                    "value":"one",
                    "timestamp":"2024-08-25T19:06:11.840482523Z"}}');"#,
                r#"INSERT INTO operations VALUES(4,
                    '{"Update":{"uuid":"e2956511-fd47-4e40-926a-52616229c2fa",
                    "property":"entry",
                    "old_value":null,
                    "value":"1724612771",
                    "timestamp":"2024-08-25T19:06:11.840497662Z"}}');"#,
                r#"INSERT INTO operations VALUES(5,
                    '{"Update":{"uuid":"e2956511-fd47-4e40-926a-52616229c2fa",
                    "property":"modified",
                    "old_value":null,
                    "value":"1724612771",
                    "timestamp":"2024-08-25T19:06:11.840498973Z"}}');"#,
                r#"INSERT INTO operations VALUES(6,
                    '{"Update":{"uuid":"e2956511-fd47-4e40-926a-52616229c2fa",
                    "property":"status",
                    "old_value":null,
                    "value":"pending",
                    "timestamp":"2024-08-25T19:06:11.840505346Z"}}');"#,
                r#"INSERT INTO operations VALUES(7,'"UndoPoint"');"#,
                r#"INSERT INTO operations VALUES(8,
                    '{"Create":{"uuid":"1d125b41-ee1d-49a7-9319-0506dee414f8"}}');"#,
                r#"INSERT INTO operations VALUES(9,
                    '{"Update":{"uuid":"1d125b41-ee1d-49a7-9319-0506dee414f8",
                    "property":"dep_e2956511-fd47-4e40-926a-52616229c2fa",
                    "old_value":null,
                    "value":"x",
                    "timestamp":"2024-08-25T19:06:15.880952492Z"}}');"#,
                r#"INSERT INTO operations VALUES(10,
                    '{"Update":{"uuid":"1d125b41-ee1d-49a7-9319-0506dee414f8",
                    "property":"depends",
                    "old_value":null,
                    "value":"e2956511-fd47-4e40-926a-52616229c2fa",
                    "timestamp":"2024-08-25T19:06:15.880969429Z"}}');"#,
                r#"INSERT INTO operations VALUES(11,
                    '{"Update":{"uuid":"1d125b41-ee1d-49a7-9319-0506dee414f8",
                    "property":"description",
                    "old_value":null,
                    "value":"two",
                    "timestamp":"2024-08-25T19:06:15.880970972Z"}}');"#,
                r#"INSERT INTO operations VALUES(12,
                    '{"Update":{"uuid":"1d125b41-ee1d-49a7-9319-0506dee414f8",
                    "property":"entry",
                    "old_value":null,
                    "value":"1724612775",
                    "timestamp":"2024-08-25T19:06:15.880974948Z"}}');"#,
                r#"INSERT INTO operations VALUES(13,
                    '{"Update":{"uuid":"1d125b41-ee1d-49a7-9319-0506dee414f8",
                    "property":"modified",
                    "old_value":null,
                    "value":"1724612775",
                    "timestamp":"2024-08-25T19:06:15.880976160Z"}}');"#,
                r#"INSERT INTO operations VALUES(14,
                    '{"Update":{"uuid":"1d125b41-ee1d-49a7-9319-0506dee414f8",
                    "property":"status",
                    "old_value":null,
                    "value":"pending",
                    "timestamp":"2024-08-25T19:06:15.880977255Z"}}');"#,
                r#"CREATE TABLE sync_meta (key STRING PRIMARY KEY, value STRING);"#,
                r#"CREATE TABLE tasks (uuid STRING PRIMARY KEY, data STRING);"#,
                r#"INSERT INTO tasks VALUES('e2956511-fd47-4e40-926a-52616229c2fa',
                    '{"status":"pending",
                    "entry":"1724612771",
                    "modified":"1724612771",
                    "description":"one"}');"#,
                r#"INSERT INTO tasks VALUES('1d125b41-ee1d-49a7-9319-0506dee414f8',
                    '{"modified":"1724612775",
                    "status":"pending",
                    "description":"two",
                    "dep_e2956511-fd47-4e40-926a-52616229c2fa":"x",
                    "entry":"1724612775",
                    "depends":"e2956511-fd47-4e40-926a-52616229c2fa"}');"#,
                r#"CREATE TABLE working_set (id INTEGER PRIMARY KEY, uuid STRING);"#,
                r#"INSERT INTO working_set VALUES(1,'e2956511-fd47-4e40-926a-52616229c2fa');"#,
                r#"INSERT INTO working_set VALUES(2,'1d125b41-ee1d-49a7-9319-0506dee414f8');"#,
                r#"DELETE FROM sqlite_sequence;"#,
                r#"INSERT INTO sqlite_sequence VALUES('operations',14);"#,
            ];
            for q in queries {
                c.execute(q, [])
                    .with_context(|| format!("executing {q}"))
                    .map_err(Error::from)?;
            }
            Ok(())
        })
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_empty_dir() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let non_existant = tmp_dir.path().join("subdir");
        let mut storage =
            AsyncSqliteStorage::new(non_existant.clone(), AccessMode::ReadWrite, true).await?;
        let uuid = Uuid::new_v4();
        assert!(storage.create_task(uuid).await?);
        let task = storage.get_task(uuid).await?;
        assert_eq!(task, Some(taskmap_with(vec![])));

        // Re-open the DB.
        let mut storage =
            AsyncSqliteStorage::new(non_existant, AccessMode::ReadWrite, true).await?;
        let task = storage.get_task(uuid).await?;
        assert_eq!(task, Some(taskmap_with(vec![])));

        Ok(())
    }

    /// Test upgrading from a TaskChampion-0.8.0 database, ensuring that some basic task data
    /// remains intact from that version. This provides a basic coverage test of all schema
    /// upgrade functions.
    #[tokio::test]
    async fn test_0_8_0_db() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        create_0_8_0_db(tmp_dir.path()).await?;
        let mut storage =
            AsyncSqliteStorage::new(tmp_dir.path(), AccessMode::ReadWrite, true).await?;
        assert_eq!(
            schema::get_db_version(&mut storage.con).await?,
            storage::sqlite::schema::LATEST_VERSION,
        );
        let one = Uuid::parse_str("e2956511-fd47-4e40-926a-52616229c2fa").unwrap();
        let two = Uuid::parse_str("1d125b41-ee1d-49a7-9319-0506dee414f8").unwrap();

        let mut task_one = storage.get_task(one).await?.unwrap();
        assert_eq!(task_one.get("description").unwrap(), "one");

        let task_two = storage.get_task(two).await?.unwrap();
        assert_eq!(task_two.get("description").unwrap(), "two");

        let ops = storage.unsynced_operations().await?;
        assert_eq!(ops.len(), 14);
        assert_eq!(ops[0], Operation::UndoPoint);

        task_one.insert("description".into(), "updated".into());
        storage.set_task(one, task_one).await?;
        storage
            .add_operation(Operation::Update {
                uuid: one,
                property: "description".into(),
                old_value: Some("one".into()),
                value: Some("updated".into()),
                timestamp: Utc::now(),
            })
            .await?;

        // Read back the modification.
        let task_one = storage.get_task(one).await?.unwrap();
        assert_eq!(task_one.get("description").unwrap(), "updated");
        let ops = storage.unsynced_operations().await?;
        assert_eq!(ops.len(), 15);

        // Check the UUID fields on the operations directly in the DB.
        storage
            .con
            .call(|c| {
                let t = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
                let mut q = t.prepare("SELECT data, uuid FROM operations ORDER BY id ASC")?;
                let mut num_ops = 0;
                for row in q
                    .query_map([], |r| {
                        let uuid: Option<StoredUuid> = r.get("uuid")?;
                        let operation: Operation = r.get("data")?;
                        Ok((uuid.map(|su| su.0), operation))
                    })
                    .context("Get all operations")
                    .map_err(Error::from)?
                {
                    let (uuid, operation) = row?;
                    assert_eq!(uuid, operation.get_uuid());
                    num_ops += 1;
                }
                assert_eq!(num_ops, 15);
                Ok(())
            })
            .await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_access() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let db_path = tmp_dir.path().to_path_buf();

        // Initialize the DB once, as schema modifications are not isolated by transactions.
        AsyncSqliteStorage::new(db_path.clone(), AccessMode::ReadWrite, true).await?;

        let path1 = db_path.clone();
        let h1 = tokio::spawn(async move {
            let mut storage = AsyncSqliteStorage::new(path1, AccessMode::ReadWrite, false)
                .await
                .unwrap();
            let u = Uuid::new_v4();
            storage.set_base_version(u).await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
        });

        let path2 = db_path.clone();
        let h2 = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let mut storage = AsyncSqliteStorage::new(path2, AccessMode::ReadWrite, false)
                .await
                .unwrap();
            let u = Uuid::new_v4();
            storage.set_base_version(u).await.unwrap();
        });

        h1.await.map_err(Error::from)?;
        h2.await.map_err(Error::from)?;

        Ok(())
    }

    /// Verify that mutating methods fail in the read-only access mode, but read methods succeed,
    /// both with and without the database having been created before being opened.
    #[rstest]
    #[case::create_non_existent(false, true)]
    #[case::create_exists(true, true)]
    #[case::exists_dont_create(true, false)]
    #[tokio::test]
    async fn test_read_only(#[case] exists: bool, #[case] create: bool) -> Result<()> {
        let tmp_dir = TempDir::new()?;
        // If the DB should already exist, create it.
        if exists {
            AsyncSqliteStorage::new(tmp_dir.path(), AccessMode::ReadWrite, true).await?;
        }
        let mut storage =
            AsyncSqliteStorage::new(tmp_dir.path(), AccessMode::ReadOnly, create).await?;

        fn is_read_only_err<T: std::fmt::Debug>(res: Result<T>) -> bool {
            &res.unwrap_err().to_string() == "Task storage was opened in read-only mode"
        }

        let taskmap = TaskMap::new();
        let op = Operation::UndoPoint;

        // Mutating things fail.
        assert!(is_read_only_err(storage.create_task(Uuid::new_v4()).await));
        assert!(is_read_only_err(
            storage.set_task(Uuid::new_v4(), taskmap).await
        ));
        assert!(is_read_only_err(storage.delete_task(Uuid::new_v4()).await));
        assert!(is_read_only_err(
            storage.set_base_version(Uuid::new_v4()).await
        ));
        assert!(is_read_only_err(storage.add_operation(op.clone()).await));
        assert!(is_read_only_err(storage.remove_operation(op).await));
        assert!(is_read_only_err(storage.sync_complete().await));
        assert!(is_read_only_err(
            storage.add_to_working_set(Uuid::new_v4()).await
        ));
        assert!(is_read_only_err(
            storage.set_working_set_item(1, None).await
        ));
        assert!(is_read_only_err(storage.clear_working_set().await));

        // Read-only things succeed.
        assert_eq!(storage.get_task(Uuid::new_v4()).await?, None);
        assert_eq!(storage.get_pending_tasks().await?.len(), 0);
        assert_eq!(storage.all_tasks().await?.len(), 0);
        assert_eq!(storage.base_version().await?, Uuid::nil());
        assert_eq!(storage.get_task_operations(Uuid::new_v4()).await?.len(), 0);
        assert_eq!(storage.unsynced_operations().await?.len(), 0);
        assert_eq!(storage.num_unsynced_operations().await?, 0);
        assert_eq!(storage.get_working_set().await?.len(), 1);

        Ok(())
    }
}
