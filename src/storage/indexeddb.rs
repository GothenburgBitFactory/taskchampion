use crate::errors::{Error, Result};
use crate::operation::Operation;
use crate::storage::{Storage, StorageTxn, TaskMap, VersionId, DEFAULT_BASE_VERSION};
use async_trait::async_trait;
use indexed_db_futures::cursor::CursorDirection;
use indexed_db_futures::database::Database;
use indexed_db_futures::prelude::*;
use indexed_db_futures::transaction::{Transaction, TransactionMode};
use indexed_db_futures::KeyRange;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use uuid::Uuid;
use wasm_bindgen::JsValue;

// --- Schema Definition ---
const DB_NAME: &str = "taskchampion-db";
const DB_VERSION: u32 = 1;

// Object store names
const TASKS_STORE: &str = "tasks";
const OPERATIONS_STORE: &str = "operations";
const WORKING_SET_STORE: &str = "working_set";
const SYNC_META_STORE: &str = "sync_meta";

// Index names for the operations store
const OP_UUID_INDEX: &str = "uuid";
const OP_SYNCED_INDEX: &str = "synced";

#[derive(Serialize, Deserialize, Debug, Clone)]
struct StoredOperation {
    op: Operation,
    uuid: Option<String>,
    synced: u8,
}

pub struct IndexedDbStorage {
    db: Database,
}

impl IndexedDbStorage {
    pub async fn new() -> Result<Self> {
        let db = Database::open(DB_NAME)
            .with_version(DB_VERSION)
            .with_on_upgrade_needed(|_evt, db| {
                if !db.object_store_names().any(|name| name == TASKS_STORE) {
                    db.create_object_store(TASKS_STORE).build()?;
                }
                if !db
                    .object_store_names()
                    .any(|name| name == WORKING_SET_STORE)
                {
                    db.create_object_store(WORKING_SET_STORE).build()?;
                }
                if !db.object_store_names().any(|name| name == SYNC_META_STORE) {
                    db.create_object_store(SYNC_META_STORE).build()?;
                }
                if !db.object_store_names().any(|name| name == OPERATIONS_STORE) {
                    let store = db
                        .create_object_store(OPERATIONS_STORE)
                        .with_auto_increment(true)
                        .build()?;
                    store
                        .create_index(OP_UUID_INDEX, "uuid".into())
                        .with_multi_entry(false)
                        .build()?;
                    store
                        .create_index(OP_SYNCED_INDEX, "synced".into())
                        .with_multi_entry(false)
                        .build()?;
                }
                Ok(())
            })
            .await
            .map_err(Error::from)?;
        Ok(IndexedDbStorage { db })
    }
}

fn to_js_value<T: Serialize + ?Sized>(value: &T) -> Result<JsValue> {
    serde_wasm_bindgen::to_value(value)
        .map_err(|e| Error::Database(format!("Serialization error: {e}")))
}

fn from_js_value<T: for<'de> Deserialize<'de>>(value: JsValue) -> Result<T> {
    serde_wasm_bindgen::from_value(value)
        .map_err(|e| Error::Database(format!("Deserialization error: {e}")))
}

#[async_trait(?Send)]
impl Storage for IndexedDbStorage {
    async fn txn<'a>(&'a mut self) -> Result<Box<dyn StorageTxn + 'a>> {
        let txn = self
            .db
            .transaction([
                TASKS_STORE,
                OPERATIONS_STORE,
                WORKING_SET_STORE,
                SYNC_META_STORE,
            ])
            .with_mode(TransactionMode::Readwrite)
            .build()
            .map_err(Error::from)?;

        Ok(Box::new(Txn { txn: Some(txn) }))
    }
}

pub struct Txn<'a> {
    txn: Option<Transaction<'a>>,
}

impl<'a> Txn<'a> {
    fn get_txn(&self) -> Result<&Transaction<'a>> {
        self.txn
            .as_ref()
            .ok_or_else(|| Error::Database("Transaction already used".to_string()))
    }
}

#[async_trait(?Send)]
impl<'a> StorageTxn for Txn<'a> {
    async fn get_task(&mut self, uuid: Uuid) -> Result<Option<TaskMap>> {
        let store = self.get_txn()?.object_store(TASKS_STORE)?;
        store
            .get(uuid.to_string())
            .serde()?
            .await
            .map_err(Error::from)
    }

    async fn get_pending_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        let working_set_uuids: HashSet<Uuid> = self
            .get_working_set()
            .await?
            .into_iter()
            .flatten()
            .collect();

        let mut pending_tasks = Vec::new();
        for uuid in working_set_uuids {
            if let Some(task_map) = self.get_task(uuid).await? {
                pending_tasks.push((uuid, task_map));
            }
        }
        Ok(pending_tasks)
    }

    async fn create_task(&mut self, uuid: Uuid) -> Result<bool> {
        let store = self.get_txn()?.object_store(TASKS_STORE)?;
        let key = uuid.to_string();

        let existing_task: Option<TaskMap> =
            store.get(key.clone()).serde()?.await.map_err(Error::from)?;
        if existing_task.is_some() {
            return Ok(false);
        }

        let task = TaskMap::new();
        let value = to_js_value(&task)?;
        store
            .add(&value)
            .with_key(key)
            .primitive()?
            .await
            .map_err(Error::from)?;
        Ok(true)
    }

    async fn set_task(&mut self, uuid: Uuid, task: TaskMap) -> Result<()> {
        let store = self.get_txn()?.object_store(TASKS_STORE)?;
        let key = uuid.to_string();
        let value = to_js_value(&task)?;
        store
            .put(&value)
            .with_key(key)
            .primitive()?
            .await
            .map_err(Error::from)?;
        Ok(())
    }

    async fn delete_task(&mut self, uuid: Uuid) -> Result<bool> {
        let store = self.get_txn()?.object_store(TASKS_STORE)?;
        let key = uuid.to_string();
        let existing_task: Option<TaskMap> =
            store.get(key.clone()).serde()?.await.map_err(Error::from)?;
        if existing_task.is_some() {
            store.delete(key).await.map_err(Error::from)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn all_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        let store = self.get_txn()?.object_store(TASKS_STORE)?;
        let Some(mut cursor) = store.open_cursor().await.map_err(Error::from)? else {
            return Ok(Vec::new());
        };

        let mut tasks = Vec::new();
        while let Some(value_js) = cursor.next_record().await.map_err(Error::from)? {
            let key_js = cursor
                .key()?
                .ok_or_else(|| Error::Database("Cursor had a value but no key".to_string()))?;
            let uuid_str: String = from_js_value(key_js)?;
            let uuid = Uuid::parse_str(&uuid_str)?;
            let task: TaskMap = from_js_value(value_js)?;
            tasks.push((uuid, task));
        }
        Ok(tasks)
    }

    async fn all_task_uuids(&mut self) -> Result<Vec<Uuid>> {
        let store = self.get_txn()?.object_store(TASKS_STORE)?;
        let keys_js = store.get_all_keys().await.map_err(Error::from)?;
        let mut uuids = Vec::new();
        for key_js_res in keys_js {
            let uuid_str: String = from_js_value(key_js_res?)?;
            uuids.push(Uuid::parse_str(&uuid_str)?);
        }
        Ok(uuids)
    }

    async fn base_version(&mut self) -> Result<VersionId> {
        let store = self.get_txn()?.object_store(SYNC_META_STORE)?;
        let key = "base_version";
        let value: Option<String> = store.get(key).serde()?.await.map_err(Error::from)?;
        match value {
            Some(uuid_str) => Ok(Uuid::parse_str(&uuid_str)?),
            None => Ok(DEFAULT_BASE_VERSION),
        }
    }

    async fn set_base_version(&mut self, version_id: VersionId) -> Result<()> {
        let store = self.get_txn()?.object_store(SYNC_META_STORE)?;
        let key = "base_version";
        let value = to_js_value(&version_id.to_string())?;
        store
            .put(&value)
            .with_key(key)
            .primitive()?
            .await
            .map_err(Error::from)?;
        Ok(())
    }

    async fn get_task_operations(&mut self, uuid: Uuid) -> Result<Vec<Operation>> {
        let store = self.get_txn()?.object_store(OPERATIONS_STORE)?;
        let index = store.index(OP_UUID_INDEX)?;
        let key_range: KeyRange<JsValue> = KeyRange::Only(uuid.to_string().into());
        // FIX: Added explicit type annotation (turbofish) to with_query
        let Some(mut cursor) = index
            .open_cursor()
            .with_query::<JsValue, _>(key_range)
            .await
            .map_err(Error::from)?
        else {
            return Ok(Vec::new());
        };

        let mut ops = Vec::new();
        while let Some(value) = cursor.next_record().await.map_err(Error::from)? {
            let stored_op: StoredOperation = from_js_value(value)?;
            ops.push(stored_op.op);
        }
        Ok(ops)
    }

    async fn unsynced_operations(&mut self) -> Result<Vec<Operation>> {
        let store = self.get_txn()?.object_store(OPERATIONS_STORE)?;
        let index = store.index(OP_SYNCED_INDEX)?;
        let key_range: KeyRange<JsValue> = KeyRange::Only(JsValue::from(0_u8));
        // FIX: Added explicit type annotation (turbofish) to with_query
        let Some(mut cursor) = index
            .open_cursor()
            .with_query::<JsValue, _>(key_range)
            .await
            .map_err(Error::from)?
        else {
            return Ok(Vec::new());
        };

        let mut ops = Vec::new();
        while let Some(value) = cursor.next_record().await.map_err(Error::from)? {
            let stored_op: StoredOperation = from_js_value(value)?;
            ops.push(stored_op.op);
        }
        Ok(ops)
    }

    async fn num_unsynced_operations(&mut self) -> Result<usize> {
        let store = self.get_txn()?.object_store(OPERATIONS_STORE)?;
        let index = store.index(OP_SYNCED_INDEX)?;
        let key_range: KeyRange<JsValue> = KeyRange::Only(JsValue::from(0_u8));
        // FIX: Added explicit type annotation (turbofish) to with_query
        let count = index
            .count()
            .with_query::<JsValue, _>(key_range)
            .await
            .map_err(Error::from)?;
        Ok(count as usize)
    }

    async fn add_operation(&mut self, op: Operation) -> Result<()> {
        let store = self.get_txn()?.object_store(OPERATIONS_STORE)?;
        let stored_op = StoredOperation {
            uuid: op.get_uuid().map(|u| u.to_string()),
            op,
            synced: 0,
        };
        let value = to_js_value(&stored_op)?;
        store.add(&value).primitive()?.await.map_err(Error::from)?;
        Ok(())
    }

    async fn remove_operation(&mut self, op_to_remove: Operation) -> Result<()> {
        let store = self.get_txn()?.object_store(OPERATIONS_STORE)?;
        let index = store.index(OP_SYNCED_INDEX)?;
        let key_range: KeyRange<JsValue> = KeyRange::Only(JsValue::from(0_u8));
        let Some(mut cursor) = index
            .open_cursor()
            .with_query::<JsValue, _>(key_range)
            .with_direction(CursorDirection::Prev)
            .await
            .map_err(Error::from)?
        else {
            return Err(Error::Database(
                "No unsynced operations to remove".to_string(),
            ));
        };

        if let Some(value) = cursor.next_record().await.map_err(Error::from)? {
            let last_stored_op: StoredOperation = from_js_value(value)?;
            if last_stored_op.op == op_to_remove {
                cursor.delete()?.await.map_err(Error::from)?;
                return Ok(());
            }
        }

        Err(Error::Database(
            "Last operation does not match -- cannot remove".to_string(),
        ))
    }

    async fn sync_complete(&mut self) -> Result<()> {
        let store = self.get_txn()?.object_store(OPERATIONS_STORE)?;
        let index = store.index(OP_SYNCED_INDEX)?;
        let key_range: KeyRange<JsValue> = KeyRange::Only(JsValue::from(0_u8));

        if let Some(mut cursor) = index
            .open_cursor()
            .with_query::<JsValue, _>(key_range)
            .await
            .map_err(Error::from)?
        {
            while let Some(value) = cursor.next_record().await.map_err(Error::from)? {
                let mut stored_op: StoredOperation = from_js_value(value)?;
                stored_op.synced = 1;
                let updated_value = to_js_value(&stored_op)?;
                cursor.update(&updated_value).await.map_err(Error::from)?;
            }
        }

        let task_uuids: HashSet<Uuid> = self.all_task_uuids().await?.into_iter().collect();
        let ops_store = self.get_txn()?.object_store(OPERATIONS_STORE)?;

        let mut keys_to_delete = Vec::new();
        if let Some(mut cursor) = ops_store.open_cursor().await.map_err(Error::from)? {
            while let Some(value) = cursor.next_record().await.map_err(Error::from)? {
                let stored_op: StoredOperation = from_js_value(value)?;
                if let Some(uuid) = stored_op.op.get_uuid() {
                    if !task_uuids.contains(&uuid) {
                        let key = cursor.primary_key::<JsValue>()?.ok_or_else(|| {
                            Error::Database("Cursor had value but no primary key".to_string())
                        })?;
                        keys_to_delete.push(key);
                    }
                }
            }
        }

        for key in keys_to_delete.iter() {
            ops_store.delete(key.clone()).await.map_err(Error::from)?;
        }

        Ok(())
    }

    async fn get_working_set(&mut self) -> Result<Vec<Option<Uuid>>> {
        let store = self.get_txn()?.object_store(WORKING_SET_STORE)?;
        let Some(mut cursor) = store.open_cursor().await? else {
            return Ok(vec![None]);
        };

        let mut items = BTreeMap::new();
        while let Some(value) = cursor.next_record().await? {
            // We expect a key to exist if a value exists.
            let key = cursor
                .key()?
                .expect("Cursor value existed, but key did not");

            let id: usize = from_js_value(key)?;
            let uuid_str: String = from_js_value(value)?;
            let uuid = Uuid::parse_str(&uuid_str)?;
            items.insert(id, uuid);
        }

        if items.is_empty() {
            return Ok(vec![None]);
        }

        let max_id = *items.keys().next_back().unwrap_or(&0);
        let mut working_set = vec![None; max_id + 1];

        for (id, uuid) in items {
            if id < working_set.len() {
                working_set[id] = Some(uuid);
            }
        }

        Ok(working_set)
    }

    async fn add_to_working_set(&mut self, uuid: Uuid) -> Result<usize> {
        let store = self.get_txn()?.object_store(WORKING_SET_STORE)?;
        let next_id = if let Some(cursor) = store
            .open_key_cursor()
            .with_direction(CursorDirection::Prev)
            .await
            .map_err(Error::from)?
        {
            if let Some(key) = cursor.primary_key()? {
                from_js_value::<usize>(key)? + 1
            } else {
                1
            }
        } else {
            1
        };
        let key = to_js_value(&next_id)?;
        let value = to_js_value(&uuid.to_string())?;
        store
            .put(&value)
            .with_key(key)
            .primitive()?
            .await
            .map_err(Error::from)?;
        Ok(next_id)
    }

    async fn set_working_set_item(&mut self, index: usize, uuid: Option<Uuid>) -> Result<()> {
        let store = self.get_txn()?.object_store(WORKING_SET_STORE)?;
        let key = to_js_value(&index)?;
        match uuid {
            Some(uuid) => {
                let value = to_js_value(&uuid.to_string())?;
                store
                    .put(&value)
                    .with_key(key)
                    .primitive()?
                    .await
                    .map_err(Error::from)?;
            }
            None => {
                store.delete(key).await.map_err(Error::from)?;
            }
        }
        Ok(())
    }

    async fn clear_working_set(&mut self) -> Result<()> {
        let store = self.get_txn()?.object_store(WORKING_SET_STORE)?;
        store.clear()?.await.map_err(Error::from)?;
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        if let Some(txn) = self.txn.take() {
            txn.commit().await.map_err(Error::from)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use wasm_bindgen_test::*;

    wasm_bindgen_test_configure!(run_in_browser);

    /// Asynchronously creates a fresh storage backend for each test.
    async fn storage() -> Result<IndexedDbStorage> {
        let storage = IndexedDbStorage::new().await?;
        let txn = storage
            .db
            .transaction([
                TASKS_STORE,
                OPERATIONS_STORE,
                WORKING_SET_STORE,
                SYNC_META_STORE,
            ])
            .with_mode(TransactionMode::Readwrite)
            .build()?;

        txn.object_store(TASKS_STORE)?.clear()?.await?;
        txn.object_store(OPERATIONS_STORE)?.clear()?.await?;
        txn.object_store(WORKING_SET_STORE)?.clear()?.await?;
        txn.object_store(SYNC_META_STORE)?.clear()?.await?;
        txn.commit().await?;

        Ok(storage)
    }

    crate::storage::test::storage_tests!(storage().await?);
}
