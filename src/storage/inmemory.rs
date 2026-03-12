#![allow(clippy::new_without_default)]

use crate::errors::{Error, Result};
use crate::operation::Operation;
use crate::storage::{Storage, StorageTxn, SyncPoint, TaskMap, VersionId, DEFAULT_BASE_VERSION};
use async_trait::async_trait;
use std::any::Any;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(PartialEq, Debug, Clone)]
struct Data {
    next_op_id: i64,
    tasks: HashMap<Uuid, TaskMap>,
    base_version: VersionId,
    operations: Vec<(i64, bool, Operation)>,
    working_set: Vec<Option<Uuid>>,
}

pub(crate) struct InMemorySyncPoint {
    max_op_id: Option<i64>,
    unsynced: Vec<Operation>,
    synced_to_add: Vec<Operation>,
}

impl SyncPoint for InMemorySyncPoint {
    fn operations(&self) -> &[Operation] {
        &self.unsynced
    }

    fn add_synced_operation(&mut self, op: Operation) {
        self.synced_to_add.push(op);
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any + Send> {
        self
    }
}

struct Txn<'t> {
    storage: &'t mut InMemoryStorage,
    new_data: Option<Data>,
}

impl Txn<'_> {
    fn mut_data_ref(&mut self) -> &mut Data {
        if self.new_data.is_none() {
            self.new_data = Some(self.storage.data.clone());
        }
        if let Some(ref mut data) = self.new_data {
            data
        } else {
            unreachable!();
        }
    }

    fn data_ref(&mut self) -> &Data {
        if let Some(ref data) = self.new_data {
            data
        } else {
            &self.storage.data
        }
    }

    // Remove any "None" items from the end of the working set.
    fn normalize_working_set(&mut self) {
        let working_set = &mut self.mut_data_ref().working_set;
        while let Some(None) = &working_set[1..].last() {
            working_set.pop();
        }
    }
}

#[async_trait]
impl StorageTxn for Txn<'_> {
    async fn get_task(&mut self, uuid: Uuid) -> Result<Option<TaskMap>> {
        match self.data_ref().tasks.get(&uuid) {
            None => Ok(None),
            Some(t) => Ok(Some(t.clone())),
        }
    }

    async fn get_pending_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        let res = self
            .get_working_set()
            .await?
            .iter()
            .filter_map(|uuid| {
                // Since uuid is wrapped in an Option and get(&inner_uuid)
                // also returns an Option, the resulting type will be
                // Option<Option<(Uuid, TaskMap)>>. To turn that into
                // an Option<(Uuid, TaskMap)>, flatten is called
                uuid.map(|inner_uuid| {
                    self.data_ref()
                        .tasks
                        .get(&inner_uuid)
                        .map(|taskmap| (inner_uuid, taskmap.clone()))
                })
                .flatten()
            })
            .collect::<Vec<_>>();

        Ok(res)
    }

    async fn create_task(&mut self, uuid: Uuid) -> Result<bool> {
        if let ent @ Entry::Vacant(_) = self.mut_data_ref().tasks.entry(uuid) {
            ent.or_insert_with(TaskMap::new);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn set_task(&mut self, uuid: Uuid, task: TaskMap) -> Result<()> {
        self.mut_data_ref().tasks.insert(uuid, task);
        Ok(())
    }

    async fn delete_task(&mut self, uuid: Uuid) -> Result<bool> {
        Ok(self.mut_data_ref().tasks.remove(&uuid).is_some())
    }

    async fn all_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        Ok(self
            .data_ref()
            .tasks
            .iter()
            .map(|(u, t)| (*u, t.clone()))
            .collect())
    }

    async fn all_task_uuids(&mut self) -> Result<Vec<Uuid>> {
        Ok(self.data_ref().tasks.keys().copied().collect())
    }

    async fn base_version(&mut self) -> Result<VersionId> {
        Ok(self.data_ref().base_version)
    }

    async fn set_base_version(&mut self, version: VersionId) -> Result<()> {
        self.mut_data_ref().base_version = version;
        Ok(())
    }

    async fn get_task_operations(&mut self, uuid: Uuid) -> Result<Vec<Operation>> {
        Ok(self
            .data_ref()
            .operations
            .iter()
            .filter(|(_, _, op)| op.get_uuid() == Some(uuid))
            .map(|(_, _, op)| op.clone())
            .collect())
    }

    async fn unsynced_operations(&mut self) -> Result<Vec<Operation>> {
        Ok(self
            .data_ref()
            .operations
            .iter()
            .filter(|(_, synced, _)| !synced)
            .map(|(_, _, op)| op.clone())
            .collect())
    }

    async fn num_unsynced_operations(&mut self) -> Result<usize> {
        Ok(self
            .data_ref()
            .operations
            .iter()
            .filter(|(_, synced, _)| !synced)
            .count())
    }

    async fn add_operation(&mut self, op: Operation) -> Result<()> {
        let data = self.mut_data_ref();
        let id = data.next_op_id;
        data.next_op_id += 1;
        data.operations.push((id, false, op));
        Ok(())
    }

    async fn remove_operation(&mut self, op: Operation) -> Result<()> {
        if let Some((_, synced, last_op)) = self.data_ref().operations.last() {
            if *synced {
                return Err(Error::Database(
                    "Last operation has been synced -- cannot remove".to_string(),
                ));
            }
            if last_op == &op {
                self.mut_data_ref().operations.pop();
                return Ok(());
            }
        }
        Err(Error::Database(
            "Last operation does not match -- cannot remove".to_string(),
        ))
    }

    async fn get_sync_point(&mut self) -> Result<Box<dyn SyncPoint>> {
        let data = self.data_ref();
        let mut unsynced = Vec::new();
        let mut max_op_id: Option<i64> = None;
        for (id, synced, op) in &data.operations {
            if !synced {
                max_op_id = Some(max_op_id.map_or(*id, |m: i64| m.max(*id)));
                unsynced.push(op.clone());
            }
        }
        Ok(Box::new(InMemorySyncPoint {
            max_op_id,
            unsynced,
            synced_to_add: Vec::new(),
        }))
    }

    async fn sync_complete(&mut self, sync_point: Box<dyn SyncPoint>) -> Result<bool> {
        let sp = sync_point
            .into_any()
            .downcast::<InMemorySyncPoint>()
            .expect("wrong SyncPoint type for InMemoryStorage");
        let data = self.mut_data_ref();

        // Check if sync point is still valid (undo removes from the end,
        // so if max_op_id is gone, undo happened).
        let valid = match sp.max_op_id {
            Some(max_id) => data.operations.iter().any(|(id, _, _)| *id == max_id),
            None => true, // no ops were captured, nothing to invalidate
        };

        // Insert transformed server ops as synced (always — pulled versions
        // are already applied to task data regardless of undo).
        for op in sp.synced_to_add {
            let id = data.next_op_id;
            data.next_op_id += 1;
            data.operations.push((id, true, op));
        }

        // Only mark original ops as synced if the sync point is still valid.
        if valid {
            if let Some(max_id) = sp.max_op_id {
                for (id, synced, _) in data.operations.iter_mut() {
                    if *id <= max_id {
                        *synced = true;
                    }
                }
            }
        }

        // Drop operations which no longer have a corresponding task.
        data.operations.retain(|(_, _, op)| {
            if let Some(uuid) = op.get_uuid() {
                data.tasks.contains_key(&uuid)
            } else {
                true
            }
        });

        Ok(valid)
    }

    async fn get_working_set(&mut self) -> Result<Vec<Option<Uuid>>> {
        Ok(self.data_ref().working_set.clone())
    }

    async fn add_to_working_set(&mut self, uuid: Uuid) -> Result<usize> {
        let working_set = &mut self.mut_data_ref().working_set;
        working_set.push(Some(uuid));
        Ok(working_set.len())
    }

    async fn set_working_set_item(&mut self, index: usize, uuid: Option<Uuid>) -> Result<()> {
        let working_set = &mut self.mut_data_ref().working_set;
        if index >= working_set.len() {
            return Err(Error::Database(format!(
                "Index {index} is not in the working set"
            )));
        }
        working_set[index] = uuid;

        self.normalize_working_set();
        Ok(())
    }

    async fn clear_working_set(&mut self) -> Result<()> {
        self.mut_data_ref().working_set = vec![None];
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        // copy the new_data back into storage to commit the transaction
        if let Some(data) = self.new_data.take() {
            self.storage.data = data;
        }
        Ok(())
    }
}

/// InMemoryStorage is a simple in-memory task storage implementation.  It is not useful for
/// production data, but is useful for testing purposes.
#[derive(PartialEq, Debug, Clone)]
pub struct InMemoryStorage {
    data: Data,
}

impl InMemoryStorage {
    pub fn new() -> InMemoryStorage {
        InMemoryStorage {
            data: Data {
                next_op_id: 0,
                tasks: HashMap::new(),
                base_version: DEFAULT_BASE_VERSION,
                operations: vec![],
                working_set: vec![None],
            },
        }
    }
}

#[async_trait]
impl Storage for InMemoryStorage {
    async fn txn<'a>(&'a mut self) -> Result<Box<dyn StorageTxn + Send + 'a>> {
        Ok(Box::new(Txn {
            storage: self,
            new_data: None,
        }))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    async fn storage() -> InMemoryStorage {
        InMemoryStorage::new()
    }

    crate::storage::test::storage_tests!(storage().await);
}
