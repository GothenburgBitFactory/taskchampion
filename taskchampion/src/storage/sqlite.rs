use crate::errors::{Error, Result};
use crate::operation::Operation;
use crate::storage::{Storage, StorageTxn, TaskMap, VersionId, DEFAULT_BASE_VERSION};
use anyhow::Context;
use rusqlite::types::{FromSql, ToSql};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use std::path::Path;
use uuid::Uuid;

mod schema;

#[derive(Debug, thiserror::Error)]
pub enum SqliteError {
    #[error("SQLite transaction already committted")]
    TransactionAlreadyCommitted,
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

/// SqliteStorage is an on-disk storage backed by SQLite3.
pub struct SqliteStorage {
    con: Connection,
}

impl SqliteStorage {
    pub fn new<P: AsRef<Path>>(directory: P, create_if_missing: bool) -> Result<SqliteStorage> {
        if create_if_missing {
            // Ensure parent folder exists
            std::fs::create_dir_all(&directory)?;
        }

        // Open (or create) database
        let db_file = directory.as_ref().join("taskchampion.sqlite3");
        let mut flags = OpenFlags::default();
        // default contains SQLITE_OPEN_CREATE, so remove it if we are not to
        // create a DB when missing.
        if !create_if_missing {
            flags.remove(OpenFlags::SQLITE_OPEN_CREATE);
        }
        let mut con = Connection::open_with_flags(db_file, flags)?;

        // Initialize database
        con.query_row("PRAGMA journal_mode=WAL", [], |_row| Ok(()))
            .context("Setting journal_mode=WAL")?;

        schema::upgrade_db(&mut con)?;

        Ok(Self { con })
    }
}

struct Txn<'t> {
    txn: Option<rusqlite::Transaction<'t>>,
}

impl<'t> Txn<'t> {
    fn get_txn(&self) -> std::result::Result<&rusqlite::Transaction<'t>, SqliteError> {
        self.txn
            .as_ref()
            .ok_or(SqliteError::TransactionAlreadyCommitted)
    }

    fn get_next_working_set_number(&self) -> Result<usize> {
        let t = self.get_txn()?;
        let next_id: Option<usize> = t
            .query_row(
                "SELECT COALESCE(MAX(id), 0) + 1 FROM working_set",
                [],
                |r| r.get(0),
            )
            .optional()
            .context("Getting highest working set ID")?;

        Ok(next_id.unwrap_or(0))
    }
}

impl Storage for SqliteStorage {
    fn txn<'a>(&'a mut self) -> Result<Box<dyn StorageTxn + 'a>> {
        let txn = self
            .con
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        Ok(Box::new(Txn { txn: Some(txn) }))
    }
}

impl<'t> StorageTxn for Txn<'t> {
    fn get_task(&mut self, uuid: Uuid) -> Result<Option<TaskMap>> {
        let t = self.get_txn()?;
        let result: Option<StoredTaskMap> = t
            .query_row(
                "SELECT data FROM tasks WHERE uuid = ? LIMIT 1",
                [&StoredUuid(uuid)],
                |r| r.get("data"),
            )
            .optional()?;

        // Get task from "stored" wrapper
        Ok(result.map(|t| t.0))
    }

    fn get_pending_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        let t = self.get_txn()?;

        let mut q = t.prepare(
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
    }

    fn create_task(&mut self, uuid: Uuid) -> Result<bool> {
        let t = self.get_txn()?;
        let count: usize = t.query_row(
            "SELECT count(uuid) FROM tasks WHERE uuid = ?",
            [&StoredUuid(uuid)],
            |x| x.get(0),
        )?;
        if count > 0 {
            return Ok(false);
        }

        let data = TaskMap::default();
        t.execute(
            "INSERT INTO tasks (uuid, data) VALUES (?, ?)",
            params![&StoredUuid(uuid), &StoredTaskMap(data)],
        )
        .context("Create task query")?;
        Ok(true)
    }

    fn set_task(&mut self, uuid: Uuid, task: TaskMap) -> Result<()> {
        let t = self.get_txn()?;
        t.execute(
            "INSERT OR REPLACE INTO tasks (uuid, data) VALUES (?, ?)",
            params![&StoredUuid(uuid), &StoredTaskMap(task)],
        )
        .context("Update task query")?;
        Ok(())
    }

    fn delete_task(&mut self, uuid: Uuid) -> Result<bool> {
        let t = self.get_txn()?;
        let changed = t
            .execute("DELETE FROM tasks WHERE uuid = ?", [&StoredUuid(uuid)])
            .context("Delete task query")?;
        Ok(changed > 0)
    }

    fn all_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        let t = self.get_txn()?;

        let mut q = t.prepare("SELECT uuid, data FROM tasks")?;
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
    }

    fn all_task_uuids(&mut self) -> Result<Vec<Uuid>> {
        let t = self.get_txn()?;

        let mut q = t.prepare("SELECT uuid FROM tasks")?;
        let rows = q.query_map([], |r| {
            let uuid: StoredUuid = r.get("uuid")?;
            Ok(uuid.0)
        })?;

        let mut ret = vec![];
        for r in rows {
            ret.push(r?);
        }
        Ok(ret)
    }

    fn base_version(&mut self) -> Result<VersionId> {
        let t = self.get_txn()?;

        let version: Option<StoredUuid> = t
            .query_row(
                "SELECT value FROM sync_meta WHERE key = 'base_version'",
                [],
                |r| r.get("value"),
            )
            .optional()?;
        Ok(version.map(|u| u.0).unwrap_or(DEFAULT_BASE_VERSION))
    }

    fn set_base_version(&mut self, version: VersionId) -> Result<()> {
        let t = self.get_txn()?;
        t.execute(
            "INSERT OR REPLACE INTO sync_meta (key, value) VALUES (?, ?)",
            params!["base_version", &StoredUuid(version)],
        )
        .context("Set base version")?;
        Ok(())
    }

    fn get_task_operations(&mut self, uuid: Uuid) -> Result<Vec<Operation>> {
        let t = self.get_txn()?;

        let mut q = t.prepare("SELECT data FROM operations where uuid=? ORDER BY id ASC")?;
        let rows = q.query_map([&StoredUuid(uuid)], |r| {
            let data: Operation = r.get("data")?;
            Ok(data)
        })?;

        let mut ret = vec![];
        for r in rows {
            ret.push(r?);
        }
        Ok(ret)
    }

    fn unsynced_operations(&mut self) -> Result<Vec<Operation>> {
        let t = self.get_txn()?;

        let mut q = t.prepare("SELECT data FROM operations WHERE NOT synced ORDER BY id ASC")?;
        let rows = q.query_map([], |r| {
            let data: Operation = r.get("data")?;
            Ok(data)
        })?;

        let mut ret = vec![];
        for r in rows {
            ret.push(r?);
        }
        Ok(ret)
    }

    fn num_unsynced_operations(&mut self) -> Result<usize> {
        let t = self.get_txn()?;
        let count: usize = t.query_row(
            "SELECT count(*) FROM operations WHERE NOT synced",
            [],
            |x| x.get(0),
        )?;
        Ok(count)
    }

    fn add_operation(&mut self, op: Operation) -> Result<()> {
        let t = self.get_txn()?;

        t.execute("INSERT INTO operations (data) VALUES (?)", params![&op])
            .context("Add operation query")?;
        Ok(())
    }

    fn remove_operation(&mut self, op: Operation) -> Result<()> {
        let t = self.get_txn()?;
        let last: Option<(u32, Operation)> = t
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
                t.execute("DELETE FROM operations where id = ?", [last_id])
                    .context("Removing operation")?;
                return Ok(());
            }
        }

        Err(Error::Database(
            "Last operation does not match -- cannot remove".to_string(),
        ))
    }

    fn sync_complete(&mut self) -> Result<()> {
        let t = self.get_txn()?;
        t.execute(
            "UPDATE operations SET synced = true WHERE synced = false",
            [],
        )
        .context("Marking operations as synced")?;

        // Delete all operations for non-existent (usually, deleted) tasks.
        t.execute(
            r#"DELETE from operations
            WHERE uuid IN (
                SELECT operations.uuid FROM operations LEFT JOIN tasks ON operations.uuid = tasks.uuid WHERE tasks.uuid IS NULL
            )"#,
            [],
        )
        .context("Deleting orphaned operations")?;

        Ok(())
    }

    fn get_working_set(&mut self) -> Result<Vec<Option<Uuid>>> {
        let t = self.get_txn()?;

        let mut q = t.prepare("SELECT id, uuid FROM working_set ORDER BY id ASC")?;
        let rows = q
            .query_map([], |r| {
                let id: usize = r.get("id")?;
                let uuid: StoredUuid = r.get("uuid")?;
                Ok((id, uuid.0))
            })
            .context("Get working set query")?;

        let rows: Vec<std::result::Result<(usize, Uuid), _>> = rows.collect();
        let mut res = Vec::with_capacity(rows.len());
        for _ in 0..self
            .get_next_working_set_number()
            .context("Getting working set number")?
        {
            res.push(None);
        }
        for r in rows {
            let (id, uuid) = r?;
            res[id] = Some(uuid);
        }

        Ok(res)
    }

    fn add_to_working_set(&mut self, uuid: Uuid) -> Result<usize> {
        let t = self.get_txn()?;

        let next_working_id = self.get_next_working_set_number()?;

        t.execute(
            "INSERT INTO working_set (id, uuid) VALUES (?, ?)",
            params![next_working_id, &StoredUuid(uuid)],
        )
        .context("Create task query")?;

        Ok(next_working_id)
    }

    fn set_working_set_item(&mut self, index: usize, uuid: Option<Uuid>) -> Result<()> {
        let t = self.get_txn()?;
        match uuid {
            // Add or override item
            Some(uuid) => t.execute(
                "INSERT OR REPLACE INTO working_set (id, uuid) VALUES (?, ?)",
                params![index, &StoredUuid(uuid)],
            ),
            // Setting to None removes the row from database
            None => t.execute("DELETE FROM working_set WHERE id = ?", [index]),
        }
        .context("Set working set item query")?;
        Ok(())
    }

    fn clear_working_set(&mut self) -> Result<()> {
        let t = self.get_txn()?;
        t.execute("DELETE FROM working_set", [])
            .context("Clear working set query")?;
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        let t = self
            .txn
            .take()
            .ok_or(SqliteError::TransactionAlreadyCommitted)?;
        t.commit().context("Committing transaction")?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::taskmap_with;
    use chrono::Utc;
    use pretty_assertions::assert_eq;
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;

    fn storage() -> Result<SqliteStorage> {
        let tmp_dir = TempDir::new()?;
        SqliteStorage::new(tmp_dir.path(), true)
    }

    crate::storage::test::storage_tests!(storage()?);

    /// Manually create a 0_8_0 db, as based on a dump from an actual (test) user.
    /// This is used to test in-place upgrading.
    fn create_0_8_0_db(path: &Path) -> Result<()> {
        let db_file = path.join("taskchampion.sqlite3");
        let con = Connection::open(db_file)?;

        con.query_row("PRAGMA journal_mode=WAL", [], |_row| Ok(()))
            .context("Setting journal_mode=WAL")?;
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
            con.execute(q, [])
                .with_context(|| format!("executing {}", q))?;
        }
        Ok(())
    }

    #[test]
    fn test_empty_dir() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let non_existant = tmp_dir.path().join("subdir");
        let mut storage = SqliteStorage::new(non_existant.clone(), true)?;
        let uuid = Uuid::new_v4();
        {
            let mut txn = storage.txn()?;
            assert!(txn.create_task(uuid)?);
            txn.commit()?;
        }
        {
            let mut txn = storage.txn()?;
            let task = txn.get_task(uuid)?;
            assert_eq!(task, Some(taskmap_with(vec![])));
        }

        // Re-open the DB.
        let mut storage = SqliteStorage::new(non_existant, true)?;
        {
            let mut txn = storage.txn()?;
            let task = txn.get_task(uuid)?;
            assert_eq!(task, Some(taskmap_with(vec![])));
        }
        Ok(())
    }

    /// Test upgrading from a TaskChampion-0.8.0 database, ensuring that some basic task data
    /// remains intact from that version. This provides a basic coverage test of all schema
    /// upgrade functions.
    #[test]
    fn test_0_8_0_db() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        create_0_8_0_db(tmp_dir.path())?;
        let mut storage = SqliteStorage::new(tmp_dir.path(), true)?;
        assert_eq!(
            schema::get_db_version(&mut storage.con)?,
            schema::LATEST_VERSION,
        );
        let one = Uuid::parse_str("e2956511-fd47-4e40-926a-52616229c2fa").unwrap();
        let two = Uuid::parse_str("1d125b41-ee1d-49a7-9319-0506dee414f8").unwrap();
        {
            let mut txn = storage.txn()?;

            let mut task_one = txn.get_task(one)?.unwrap();
            assert_eq!(task_one.get("description").unwrap(), "one");

            let task_two = txn.get_task(two)?.unwrap();
            assert_eq!(task_two.get("description").unwrap(), "two");

            let ops = txn.unsynced_operations()?;
            assert_eq!(ops.len(), 14);
            assert_eq!(ops[0], Operation::UndoPoint);

            task_one.insert("description".into(), "updated".into());
            txn.set_task(one, task_one)?;
            txn.add_operation(Operation::Update {
                uuid: one,
                property: "description".into(),
                old_value: Some("one".into()),
                value: Some("updated".into()),
                timestamp: Utc::now(),
            })?;
            txn.commit()?;
        }

        // Read back the modification.
        {
            let mut txn = storage.txn()?;
            let task_one = txn.get_task(one)?.unwrap();
            assert_eq!(task_one.get("description").unwrap(), "updated");
            let ops = txn.unsynced_operations()?;
            assert_eq!(ops.len(), 15);
        }

        // Check the UUID fields on the operations directly in the DB.
        {
            let t = storage
                .con
                .transaction_with_behavior(TransactionBehavior::Immediate)?;
            let mut q = t.prepare("SELECT data, uuid FROM operations ORDER BY id ASC")?;
            let mut num_ops = 0;
            for row in q
                .query_map([], |r| {
                    let uuid: Option<StoredUuid> = r.get("uuid")?;
                    let operation: Operation = r.get("data")?;
                    Ok((uuid.map(|su| su.0), operation))
                })
                .context("Get all operations")?
            {
                let (uuid, operation) = row?;
                assert_eq!(uuid, operation.get_uuid());
                num_ops += 1;
            }
            assert_eq!(num_ops, 15);
        }

        Ok(())
    }

    #[test]
    fn test_concurrent_access() -> Result<()> {
        let tmp_dir = TempDir::new()?;

        // Initialize the DB once, as schema modifications are not isolated by transactions.
        SqliteStorage::new(tmp_dir.path(), true).unwrap();

        thread::scope(|scope| {
            // First thread begins a transaction, writes immediately, waits 100ms, and commits it.
            scope.spawn(|| {
                let mut storage = SqliteStorage::new(tmp_dir.path(), true).unwrap();
                let u = Uuid::new_v4();
                let mut txn = storage.txn().unwrap();
                txn.set_base_version(u).unwrap();
                thread::sleep(Duration::from_millis(100));
                txn.commit().unwrap();
            });
            // Second thread waits 50ms, and begins a transaction. This
            // should wait for the first to complete, but the regression would be a SQLITE_BUSY
            // failure.
            scope.spawn(|| {
                thread::sleep(Duration::from_millis(50));
                let mut storage = SqliteStorage::new(tmp_dir.path(), true).unwrap();
                let u = Uuid::new_v4();
                let mut txn = storage.txn().unwrap();
                txn.set_base_version(u).unwrap();
                txn.commit().unwrap();
            });
        });
        Ok(())
    }
}
