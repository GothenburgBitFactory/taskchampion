#![allow(clippy::new_without_default)]

use crate::errors::{Error, Result};
use crate::operation::Operation;
use crate::storage::{Storage, StorageTxn, TaskMap, VersionId, DEFAULT_BASE_VERSION};
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

impl StorageTxn for Txn<'_> {
    fn get_task(&mut self, uuid: Uuid) -> Result<Option<TaskMap>> {
        match self.data_ref().tasks.get(&uuid) {
            None => Ok(None),
            Some(t) => Ok(Some(t.clone())),
        }
    }

    fn get_pending_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        let res = self
            .get_working_set()?
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

    fn create_task(&mut self, uuid: Uuid) -> Result<bool> {
        if let ent @ Entry::Vacant(_) = self.mut_data_ref().tasks.entry(uuid) {
            ent.or_insert_with(TaskMap::new);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn set_task(&mut self, uuid: Uuid, task: TaskMap) -> Result<()> {
        self.mut_data_ref().tasks.insert(uuid, task);
        Ok(())
    }

    fn delete_task(&mut self, uuid: Uuid) -> Result<bool> {
        Ok(self.mut_data_ref().tasks.remove(&uuid).is_some())
    }

    fn all_tasks<'a>(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        Ok(self
            .data_ref()
            .tasks
            .iter()
            .map(|(u, t)| (*u, t.clone()))
            .collect())
    }

    fn all_task_uuids<'a>(&mut self) -> Result<Vec<Uuid>> {
        Ok(self.data_ref().tasks.keys().copied().collect())
    }

    fn base_version(&mut self) -> Result<VersionId> {
        Ok(self.data_ref().base_version)
    }

    fn set_base_version(&mut self, version: VersionId) -> Result<()> {
        self.mut_data_ref().base_version = version;
        Ok(())
    }

    fn get_task_operations(&mut self, uuid: Uuid) -> Result<Vec<Operation>> {
        Ok(self
            .data_ref()
            .operations
            .iter()
            .filter(|(_, op)| op.get_uuid() == Some(uuid))
            .map(|(_, op)| op.clone())
            .collect())
    }

    fn unsynced_operations(&mut self) -> Result<Vec<Operation>> {
        Ok(self
            .data_ref()
            .operations
            .iter()
            .filter(|(synced, _)| !synced)
            .map(|(_, op)| op.clone())
            .collect())
    }

    fn num_unsynced_operations(&mut self) -> Result<usize> {
        Ok(self
            .data_ref()
            .operations
            .iter()
            .filter(|(synced, _)| !synced)
            .count())
    }

    fn add_operation(&mut self, op: Operation) -> Result<()> {
        self.mut_data_ref().operations.push((false, op));
        Ok(())
    }

    fn remove_operation(&mut self, op: Operation) -> Result<()> {
        if let Some((synced, last_op)) = self.data_ref().operations.last() {
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

    fn sync_complete(&mut self) -> Result<()> {
        let data = self.data_ref();

        // Mark all operations as synced, but drop operations which no longer have a
        // corresponding task.
        let new_operations = data
            .operations
            .iter()
            .filter(|(_, op)| {
                if let Some(uuid) = op.get_uuid() {
                    data.tasks.contains_key(&uuid)
                } else {
                    true
                }
            })
            .map(|(_, op)| (true, op.clone()))
            .collect();
        self.mut_data_ref().operations = new_operations;

        Ok(())
    }

    fn get_working_set(&mut self) -> Result<Vec<Option<Uuid>>> {
        Ok(self.data_ref().working_set.clone())
    }

    fn add_to_working_set(&mut self, uuid: Uuid) -> Result<usize> {
        let working_set = &mut self.mut_data_ref().working_set;
        working_set.push(Some(uuid));
        Ok(working_set.len())
    }

    fn set_working_set_item(&mut self, index: usize, uuid: Option<Uuid>) -> Result<()> {
        let working_set = &mut self.mut_data_ref().working_set;
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

    fn clear_working_set(&mut self) -> Result<()> {
        self.mut_data_ref().working_set = vec![None];
        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
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
                tasks: HashMap::new(),
                base_version: DEFAULT_BASE_VERSION,
                operations: vec![],
                working_set: vec![None],
            },
        }
    }
}

impl Storage for InMemoryStorage {
    fn txn<F, R>(&mut self, f: F) -> Result<R>
    where
        F: for<'a> FnOnce(&'a mut (dyn StorageTxn + 'a)) -> Result<R>,
    {
        f(&mut Txn {
            storage: self,
            new_data: None,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn storage() -> InMemoryStorage {
        InMemoryStorage::new()
    }

    crate::storage::test::storage_tests!(storage());
}
