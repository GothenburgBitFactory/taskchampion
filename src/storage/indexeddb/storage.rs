use super::schema;
use crate::errors::{Error, Result};
use crate::operation::Operation;
use crate::storage::send_wrapper::{WrappedStorage, WrappedStorageTxn, Wrapper};
use crate::storage::{Storage, StorageTxn, TaskMap, VersionId, DEFAULT_BASE_VERSION};
use async_trait::async_trait;
use idb::{CursorDirection, DatabaseEvent, Query, Transaction, TransactionMode};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use uuid::Uuid;
use wasm_bindgen::JsValue;

struct Inner {
    db: idb::Database,
}

#[async_trait(?Send)]
impl WrappedStorage for Inner {
    async fn txn<'a>(&'a mut self) -> Result<Box<dyn WrappedStorageTxn + 'a>> {
        Ok(Box::new(InnerTxn(Some(self.db.transaction(
            &[
                schema::TASKS,
                schema::OPERATIONS,
                schema::SYNC_META,
                schema::WORKING_SET,
            ],
            TransactionMode::ReadWrite,
        )?))))
    }
}

/// Return a new error stating the data is invalid.
fn invalid() -> Error {
    Error::Database("invalid data in IndexedDB".into())
}

/// Wrap the given error stating the data is invalid.
fn invalid_err(e: impl std::error::Error) -> Error {
    Error::Database(format!("invalid data in IndexedDB: {e}"))
}

/// Return a new error indicating this transaction has been committed.
fn already_committed() -> Error {
    Error::Database("transaction is already committed".into())
}

/// Return a new error indicating the operation was not found.
fn operation_not_found() -> Error {
    Error::Database("operation not found".into())
}

/// Return a new error indicating the operation has already been synced.
fn operation_synced() -> Error {
    Error::Database("operation has been synced".into())
}

/// Convert a JsValue to a Uuid.
fn js2uuid(js: JsValue) -> Result<Uuid> {
    let json = js.as_string().ok_or_else(invalid)?;
    let uuid = Uuid::parse_str(json.as_str()).map_err(invalid_err)?;
    Ok(uuid)
}

/// Convert a UUID to a JsValue.
fn uuid2js(uuid: Uuid) -> Result<JsValue> {
    let string = uuid.to_string();
    let js = JsValue::from_str(string.as_str());
    Ok(js)
}

/// Convert a JsValue to a Task.
fn js2task(js: JsValue) -> Result<TaskMap> {
    Ok(serde_wasm_bindgen::from_value(js)?)
}

/// Convert a Task to a JsValue.
fn task2js(task: TaskMap) -> Result<JsValue> {
    Ok(serde_wasm_bindgen::to_value(&task)?)
}

fn unsynced_is_zero(v: &u8) -> bool {
    *v == 0
}

/// Operations are stored with a separate UUID (for IndexedDB indexing) and
/// a flag indicating whether they have been synced.
#[derive(Serialize, Deserialize)]
struct StoredOperation {
    uuid: Option<Uuid>,
    operation: Operation,

    // IndexedDB does not support indexing by a boolean value, so
    // we store a number here instead. Since we want to index un-synced
    // operations, this field is only present on such entries.
    #[serde(skip_serializing_if = "unsynced_is_zero")]
    #[serde(default)]
    unsynced: u8,
}

/// Convert a JsValue to an Operation.
fn js2op(js: JsValue) -> Result<Operation> {
    Ok(serde_wasm_bindgen::from_value::<StoredOperation>(js)?.operation)
}

/// Convert an Operation to a JsValue.
fn op2js(operation: Operation, unsynced: bool) -> Result<JsValue> {
    let operation = StoredOperation {
        uuid: operation.get_uuid(),
        operation,
        unsynced: unsynced as u8,
    };
    Ok(serde_wasm_bindgen::to_value(&operation)?)
}

struct InnerTxn(Option<Transaction>);

impl InnerTxn {
    fn idb_txn(&self) -> Result<&idb::Transaction> {
        if let Some(transaction) = &self.0 {
            Ok(transaction)
        } else {
            Err(already_committed())
        }
    }

    async fn get_next_working_set_number(&self) -> Result<usize> {
        let working_set = self.idb_txn()?.object_store(schema::WORKING_SET)?;
        let mut max: usize = 0;
        let mut maybe_cursor = working_set.open_key_cursor(None, None)?.await?;
        while let Some(cursor) = maybe_cursor {
            let i = cursor.key()?.as_f64().ok_or_else(invalid)? as usize;
            if i > max {
                max = i;
            }
            maybe_cursor = cursor.next(None)?.await?;
        }
        Ok(max + 1)
    }
}

#[async_trait(?Send)]
impl WrappedStorageTxn for InnerTxn {
    async fn get_task(&mut self, uuid: Uuid) -> Result<Option<TaskMap>> {
        let tasks = self.idb_txn()?.object_store(schema::TASKS)?;
        if let Some(task) = tasks.get(Query::Key(uuid2js(uuid)?))?.await? {
            Ok(Some(js2task(task)?))
        } else {
            Ok(None)
        }
    }

    async fn get_pending_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        let tasks = self.idb_txn()?.object_store(schema::TASKS)?;
        let working_set = self.idb_txn()?.object_store(schema::WORKING_SET)?;
        let mut maybe_cursor = working_set
            .open_cursor(None, Some(CursorDirection::Prev))?
            .await?;
        let mut res = Vec::new();
        while let Some(cursor) = maybe_cursor {
            let jsuuid = cursor.value()?;
            let uuid = js2uuid(jsuuid.clone())?;
            if let Some(task) = tasks.get(Query::Key(jsuuid))?.await? {
                res.push((uuid, js2task(task)?));
            }
            maybe_cursor = cursor.next(None)?.await?;
        }
        Ok(res)
    }

    async fn create_task(&mut self, uuid: Uuid) -> Result<bool> {
        let tasks = self.idb_txn()?.object_store(schema::TASKS)?;
        match tasks
            .add(&task2js(HashMap::new())?, Some(&uuid2js(uuid)?))?
            .await
        {
            Ok(_) => Ok(true),

            Err(idb::Error::DomException(e)) if e.name() == "ConstraintError" => {
                // IndexedDB returns ConstraintError when the entry already exists,
                // which `create_task` indicates by returning false.
                Ok(false)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn set_task(&mut self, uuid: Uuid, task: TaskMap) -> Result<()> {
        let tasks = self.idb_txn()?.object_store(schema::TASKS)?;
        tasks.put(&task2js(task)?, Some(&uuid2js(uuid)?))?.await?;
        Ok(())
    }

    async fn delete_task(&mut self, uuid: Uuid) -> Result<bool> {
        let tasks = self.idb_txn()?.object_store(schema::TASKS)?;
        if tasks.get_key(Query::Key(uuid2js(uuid)?))?.await?.is_some() {
            tasks.delete(Query::Key(uuid2js(uuid)?))?.await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn all_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        let tasks = self.idb_txn()?.object_store(schema::TASKS)?;
        let mut res = Vec::new();
        let mut maybe_cursor = tasks.open_cursor(None, None)?.await?;
        while let Some(cursor) = maybe_cursor {
            res.push((js2uuid(cursor.key()?)?, js2task(cursor.value()?)?));
            maybe_cursor = cursor.next(None)?.await?;
        }
        Ok(res)
    }

    async fn all_task_uuids(&mut self) -> Result<Vec<Uuid>> {
        let tasks = self.idb_txn()?.object_store(schema::TASKS)?;
        let mut res = Vec::new();
        let mut maybe_cursor = tasks.open_key_cursor(None, None)?.await?;
        while let Some(cursor) = maybe_cursor {
            res.push(js2uuid(cursor.key()?)?);
            maybe_cursor = cursor.next(None)?.await?;
        }
        Ok(res)
    }

    async fn base_version(&mut self) -> Result<VersionId> {
        let sync_meta = self.idb_txn()?.object_store(schema::SYNC_META)?;
        let base_version: JsValue = "base_version".into();
        if let Some(version) = sync_meta.get(Query::Key(base_version))?.await? {
            Ok(js2uuid(version)?)
        } else {
            Ok(DEFAULT_BASE_VERSION)
        }
    }

    async fn set_base_version(&mut self, version: VersionId) -> Result<()> {
        let sync_meta = self.idb_txn()?.object_store(schema::SYNC_META)?;
        let base_version: JsValue = "base_version".into();
        sync_meta
            .put(&uuid2js(version)?, Some(&base_version))?
            .await?;
        Ok(())
    }

    async fn get_task_operations(&mut self, uuid: Uuid) -> Result<Vec<Operation>> {
        let ops_by_uuid = self
            .idb_txn()?
            .object_store(schema::OPERATIONS)?
            .index(schema::OPERATIONS_BY_UUID)?;
        let mut res = Vec::new();
        let mut maybe_cursor = ops_by_uuid
            .open_cursor(Some(Query::Key(uuid2js(uuid)?)), None)?
            .await?;
        while let Some(cursor) = maybe_cursor {
            res.push(js2op(cursor.value()?)?);
            maybe_cursor = cursor.next(None)?.await?;
        }
        Ok(res)
    }

    async fn unsynced_operations(&mut self) -> Result<Vec<Operation>> {
        let ops_by_synced = self
            .idb_txn()?
            .object_store(schema::OPERATIONS)?
            .index(schema::OPERATIONS_BY_UNSYNCED)?;
        let mut res = Vec::new();
        // Using no query here returns only values with the `unsynced` property set.
        let mut maybe_cursor = ops_by_synced.open_cursor(None, None)?.await?;
        while let Some(cursor) = maybe_cursor {
            res.push(js2op(cursor.value()?)?);
            maybe_cursor = cursor.next(None)?.await?;
        }
        Ok(res)
    }

    async fn num_unsynced_operations(&mut self) -> Result<usize> {
        let ops_by_synced = self
            .idb_txn()?
            .object_store(schema::OPERATIONS)?
            .index(schema::OPERATIONS_BY_UNSYNCED)?;
        let mut count = 0;
        // Using no query here returns only values with the `unsynced` property set.
        let mut maybe_cursor = ops_by_synced.open_cursor(None, None)?.await?;
        while let Some(cursor) = maybe_cursor {
            count += 1;
            maybe_cursor = cursor.next(None)?.await?;
        }
        Ok(count)
    }

    async fn add_operation(&mut self, op: Operation) -> Result<()> {
        let operations = self.idb_txn()?.object_store(schema::OPERATIONS)?;
        operations.add(&op2js(op, true)?, None)?.await?;
        Ok(())
    }

    async fn remove_operation(&mut self, op: Operation) -> Result<()> {
        // Iterate operations in reverse, to get the highest index.
        let operations = self.idb_txn()?.object_store(schema::OPERATIONS)?;
        let maybe_cursor = operations
            .open_cursor(None, Some(CursorDirection::Prev))?
            .await?;
        let Some(cursor) = maybe_cursor else {
            return Err(operation_not_found());
        };

        let found_op = serde_wasm_bindgen::from_value::<StoredOperation>(cursor.value()?)?;
        if found_op.unsynced == 0 {
            return Err(operation_synced());
        }
        if found_op.operation != op {
            return Err(operation_not_found());
        }
        cursor.delete()?.await?;
        Ok(())
    }

    async fn sync_complete(&mut self) -> Result<()> {
        let ops = self.idb_txn()?.object_store(schema::OPERATIONS)?;
        let ops_by_synced = ops.index(schema::OPERATIONS_BY_UNSYNCED)?;

        // Update all operations to indicate they are sync'd. Using no query here returns only
        // values with the `unsynced` property set.
        let mut maybe_cursor = ops_by_synced.open_cursor(None, None)?.await?;
        while let Some(cursor) = maybe_cursor {
            let op = js2op(cursor.value()?)?;
            cursor.update(&op2js(op, false)?)?.await?;
            maybe_cursor = cursor.next(None)?.await?;
        }

        // Now delete all operations for which no task exists (usually deleted tasks).
        let task_uuids: HashSet<Uuid, std::hash::RandomState> =
            HashSet::from_iter(self.all_task_uuids().await?.drain(..));
        let mut maybe_cursor = ops.open_cursor(None, None)?.await?;
        while let Some(cursor) = maybe_cursor {
            let stored_op = serde_wasm_bindgen::from_value::<StoredOperation>(cursor.value()?)?;
            if let Some(uuid) = stored_op.uuid {
                if !task_uuids.contains(&uuid) {
                    cursor.delete()?.await?;
                }
            }
            maybe_cursor = cursor.next(None)?.await?;
        }

        Ok(())
    }

    async fn get_working_set(&mut self) -> Result<Vec<Option<Uuid>>> {
        let working_set = self.idb_txn()?.object_store(schema::WORKING_SET)?;
        let mut res = vec![None];
        let mut maybe_cursor = working_set
            .open_cursor(None, Some(CursorDirection::Prev))?
            .await?;
        while let Some(cursor) = maybe_cursor {
            let id = cursor.key()?.as_f64().ok_or_else(invalid)? as usize;
            let uuid = js2uuid(cursor.value()?)?;
            res.resize_with(res.len().max(id + 1), Default::default);
            res[id] = Some(uuid);
            maybe_cursor = cursor.next(None)?.await?;
        }
        Ok(res)
    }

    async fn add_to_working_set(&mut self, uuid: Uuid) -> Result<usize> {
        let next_working_set_id = self.get_next_working_set_number().await?;
        let working_set = self.idb_txn()?.object_store(schema::WORKING_SET)?;
        working_set
            .add(&uuid2js(uuid)?, Some(&next_working_set_id.into()))?
            .await?;
        Ok(next_working_set_id)
    }

    async fn set_working_set_item(&mut self, id: usize, uuid: Option<Uuid>) -> Result<()> {
        let working_set = self.idb_txn()?.object_store(schema::WORKING_SET)?;
        if let Some(uuid) = uuid {
            working_set.put(&uuid2js(uuid)?, Some(&id.into()))?.await?;
        } else {
            working_set.delete(Query::Key(id.into()))?.await?;
        }
        Ok(())
    }

    async fn clear_working_set(&mut self) -> Result<()> {
        let working_set = self.idb_txn()?.object_store(schema::WORKING_SET)?;
        working_set.clear()?.await?;
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        self.0.take().ok_or_else(already_committed)?.commit()?;
        Ok(())
    }
}

impl Drop for InnerTxn {
    fn drop(&mut self) {
        if let Some(txn) = self.0.take() {
            // Make an attempt to abort the transaction, but without any recourse
            // if it fails.
            let _ = txn.abort();
        }
    }
}

/// IndexedDbStorage uses the [IndexedDB
/// API](https://developer.mozilla.org/en-US/docs/Web/API/IndexedDB_API) to store tasks, and is
/// suitable for use when running in a browser.
///
/// Although TaskChampion can initialize itself by synchronizing to a server, it is recommended to
/// use the
/// [`StorageManager.persist()`](https://developer.mozilla.org/en-US/docs/Web/API/StorageManager/persist)
/// method to ensure the browser does not discard the database under storage pressure.
///
/// WARNING: Do not read or write the database except via this storage implementation. Later
/// versions of this library may change the schema incompatibly. pub struct
/// IndexedDbStorage(Wrapper);
pub struct IndexedDbStorage(Wrapper);

impl IndexedDbStorage {
    /// Create a new IndexedDbStorage, using the given name for the database.
    pub async fn new(db_name: impl Into<String>) -> Result<IndexedDbStorage> {
        let db_name = db_name.into();
        Ok(IndexedDbStorage(
            Wrapper::new(async move || {
                let factory = idb::Factory::new()?;
                let mut open_request = factory.open(&db_name, Some(schema::DB_VERSION))?;
                open_request.on_upgrade_needed(|event| {
                    // It's unclear what to do with errors here. Abort the transaction?? The
                    // callback is 'static so we can't capture with &mut some local variable
                    // or anything of the sort. For now, `unwrap()`, which the browser will
                    // handle as a JS exception.
                    let old_version = event.old_version().unwrap();
                    let db = event.database().unwrap();
                    schema::upgrade(db, old_version).unwrap()
                });
                let db = open_request.await?;

                Ok(Inner { db })
            })
            .await?,
        ))
    }
}

#[async_trait]
impl Storage for IndexedDbStorage {
    async fn txn<'a>(&'a mut self) -> Result<Box<dyn StorageTxn + Send + 'a>> {
        Ok(self.0.txn().await?)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use wasm_bindgen_test::*;

    wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

    async fn storage() -> IndexedDbStorage {
        // Use a random name for the DB, as tests run in parallel in the same
        // browser profile and would otherwise interfere.
        IndexedDbStorage::new(Uuid::new_v4().to_string().as_str())
            .await
            .expect("IndexedDB setup failed")
    }

    crate::storage::test::storage_tests_wasm!(storage().await);
}
