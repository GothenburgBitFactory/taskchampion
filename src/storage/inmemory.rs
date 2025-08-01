#![allow(clippy::new_without_default)]

use crate::errors::{Error, Result};
use crate::operation::Operation;
use crate::storage::async_storage::AsyncStorage;
use crate::storage::{TaskMap, VersionId, DEFAULT_BASE_VERSION};
use async_trait::async_trait;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use uuid::Uuid;

#[derive(PartialEq, Debug, Clone)]
struct Data {
    tasks: HashMap<Uuid, TaskMap>,
    base_version: VersionId,
    operations: Vec<(bool, Operation)>,
    working_set: Vec<Option<Uuid>>,
}

#[async_trait]
impl AsyncStorage for InMemoryStorage {
    async fn get_task(&mut self, uuid: Uuid) -> Result<Option<TaskMap>> {
        match self.data.tasks.get(&uuid) {
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
                    self.data
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
        if let ent @ Entry::Vacant(_) = self.data.tasks.entry(uuid) {
            ent.or_insert_with(TaskMap::new);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn set_task(&mut self, uuid: Uuid, task: TaskMap) -> Result<()> {
        self.data.tasks.insert(uuid, task);
        Ok(())
    }

    async fn delete_task(&mut self, uuid: Uuid) -> Result<bool> {
        Ok(self.data.tasks.remove(&uuid).is_some())
    }

    async fn all_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        Ok(self
            .data
            .tasks
            .iter()
            .map(|(u, t)| (*u, t.clone()))
            .collect())
    }

    async fn all_task_uuids(&mut self) -> Result<Vec<Uuid>> {
        Ok(self.data.tasks.keys().copied().collect())
    }

    async fn base_version(&mut self) -> Result<VersionId> {
        Ok(self.data.base_version)
    }

    async fn set_base_version(&mut self, version: VersionId) -> Result<()> {
        self.data.base_version = version;
        Ok(())
    }

    async fn get_task_operations(&mut self, uuid: Uuid) -> Result<Vec<Operation>> {
        Ok(self
            .data
            .operations
            .iter()
            .filter(|(_, op)| op.get_uuid() == Some(uuid))
            .map(|(_, op)| op.clone())
            .collect())
    }

    async fn unsynced_operations(&mut self) -> Result<Vec<Operation>> {
        Ok(self
            .data
            .operations
            .iter()
            .filter(|(synced, _)| !synced)
            .map(|(_, op)| op.clone())
            .collect())
    }

    async fn num_unsynced_operations(&mut self) -> Result<usize> {
        Ok(self
            .data
            .operations
            .iter()
            .filter(|(synced, _)| !synced)
            .count())
    }

    async fn add_operation(&mut self, op: Operation) -> Result<()> {
        self.data.operations.push((false, op));
        Ok(())
    }

    async fn remove_operation(&mut self, op: Operation) -> Result<()> {
        if let Some((synced, last_op)) = self.data.operations.last() {
            if *synced {
                return Err(Error::Database(
                    "Last operation has been synced -- cannot remove".to_string(),
                ));
            }
            if last_op == &op {
                self.data.operations.pop();
                return Ok(());
            }
        }
        Err(Error::Database(
            "Last operation does not match -- cannot remove".to_string(),
        ))
    }

    async fn sync_complete(&mut self) -> Result<()> {
        // Mark all operations as synced, but drop operations which no longer have a
        // corresponding task.
        let new_operations = self
            .data
            .operations
            .iter()
            .filter(|(_, op)| {
                if let Some(uuid) = op.get_uuid() {
                    self.data.tasks.contains_key(&uuid)
                } else {
                    true
                }
            })
            .map(|(_, op)| (true, op.clone()))
            .collect();
        self.data.operations = new_operations;

        Ok(())
    }

    async fn get_working_set(&mut self) -> Result<Vec<Option<Uuid>>> {
        Ok(self.data.working_set.clone())
    }

    async fn add_to_working_set(&mut self, uuid: Uuid) -> Result<usize> {
        let working_set = &mut self.data.working_set;
        working_set.push(Some(uuid));
        Ok(working_set.len())
    }

    async fn set_working_set_item(&mut self, index: usize, uuid: Option<Uuid>) -> Result<()> {
        let working_set = &mut self.data.working_set;
        if index >= working_set.len() {
            return Err(Error::Database(format!(
                "Index {} is not in the working set",
                index
            )));
        }
        working_set[index] = uuid;

        self.normalize_working_set();
        Ok(())
    }

    async fn clear_working_set(&mut self) -> Result<()> {
        self.data.working_set = vec![None];
        Ok(())
    }
}

/// InMemoryStorage is a simple in-memory task storage implementation.  It is not useful for
/// production data, but is useful for testing purposes.
#[derive(PartialEq, Debug, Clone)]
pub(super) struct InMemoryStorage {
    data: Data,
}

impl InMemoryStorage {
    pub(super) fn new() -> InMemoryStorage {
        InMemoryStorage {
            data: Data {
                tasks: HashMap::new(),
                base_version: DEFAULT_BASE_VERSION,
                operations: vec![],
                working_set: vec![None],
            },
        }
    }

    // Remove any `None` items from the end of the working set, keeping index 0.
    fn normalize_working_set(&mut self) {
        while self.data.working_set.len() > 1 && self.data.working_set.last() == Some(&None) {
            self.data.working_set.pop();
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn storage() -> InMemoryStorage {
        InMemoryStorage::new()
    }

    crate::storage::test::async_storage_tests!(storage());
}
