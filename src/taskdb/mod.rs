use std::collections::HashSet;

use crate::errors::Result;
use crate::operation::Operation;
use crate::server::Server;
use crate::storage::{Storage, TaskMap};
use crate::Operations;
use uuid::Uuid;

mod apply;
mod snapshot;
mod sync;
pub(crate) mod undo;
mod working_set;

/// A TaskDb is the backend for a replica.  It manages the storage, operations, synchronization,
/// and so on, and all the invariants that come with it.  It leaves the meaning of particular task
/// properties to the replica and task implementations.
pub(crate) struct TaskDb<S: Storage> {
    storage: S,
}

impl<S: Storage> TaskDb<S> {
    /// Create a new TaskDb with the given backend storage
    pub(crate) fn new(storage: S) -> TaskDb<S> {
        TaskDb { storage }
    }

    /// Apply `operations` to the database in a single transaction.
    ///
    /// The operations will be appended to the list of local operations, and the set of tasks will
    /// be updated accordingly.
    ///
    /// Any operations for which `add_to_working_set` returns true will cause the relevant
    /// task to be added to the working set.
    pub(crate) async fn commit_operations<F>(
        &mut self,
        operations: Operations,
        add_to_working_set: F,
    ) -> Result<()>
    where
        F: Fn(&Operation) -> bool,
    {
        let mut txn = self.storage.txn().await?;
        apply::apply_operations(txn.as_mut(), &operations).await?;

        // Calculate the task(s) to add to the working set.
        let mut to_add = Vec::new();
        for operation in &operations {
            if add_to_working_set(operation) {
                match operation {
                    Operation::Create { uuid }
                    | Operation::Update { uuid, .. }
                    | Operation::Delete { uuid, .. } => to_add.push(*uuid),
                    _ => {}
                }
            }
        }
        let mut working_set: HashSet<Uuid> = txn
            .get_working_set()
            .await?
            .iter()
            .filter_map(|u| *u)
            .collect();
        for uuid in to_add {
            // Double-check that we are not adding a task to the working-set twice.
            if !working_set.contains(&uuid) {
                txn.add_to_working_set(uuid).await?;
                working_set.insert(uuid);
            }
        }

        for operation in operations {
            txn.add_operation(operation).await?;
        }

        txn.commit().await
    }

    /// Get all tasks.
    pub(crate) async fn all_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        let mut txn = self.storage.txn().await?;
        txn.all_tasks().await
    }

    /// Get the UUIDs of all tasks
    pub(crate) async fn all_task_uuids(&mut self) -> Result<Vec<Uuid>> {
        let mut txn = self.storage.txn().await?;
        txn.all_task_uuids().await
    }

    /// Get the working set
    pub(crate) async fn working_set(&mut self) -> Result<Vec<Option<Uuid>>> {
        let mut txn = self.storage.txn().await?;
        txn.get_working_set().await
    }

    /// Get a single task, by uuid.
    pub(crate) async fn get_task(&mut self, uuid: Uuid) -> Result<Option<TaskMap>> {
        let mut txn = self.storage.txn().await?;
        txn.get_task(uuid).await
    }

    /// Get all pending tasks from the working set
    pub(crate) async fn get_pending_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        let mut txn = self.storage.txn().await?;
        txn.get_pending_tasks().await
    }

    pub(crate) async fn get_task_operations(&mut self, uuid: Uuid) -> Result<Operations> {
        let mut txn = self.storage.txn().await?;
        txn.get_task_operations(uuid).await
    }

    /// Rebuild the working set using a function to identify tasks that should be in the set.  This
    /// renumbers the existing working-set tasks to eliminate gaps, and also adds any tasks that
    /// are not already in the working set but should be.  The rebuild occurs in a single
    /// trasnsaction against the storage backend.
    pub(crate) async fn rebuild_working_set<F>(
        &mut self,
        in_working_set: F,
        renumber: bool,
    ) -> Result<()>
    where
        F: Fn(&TaskMap) -> bool,
    {
        working_set::rebuild(self.storage.txn().await?.as_mut(), in_working_set, renumber).await
    }

    /// Sync to the given server, pulling remote changes and pushing local changes.
    ///
    /// If `avoid_snapshots` is true, the sync operations produces a snapshot only when the server
    /// indicate it is urgent (snapshot urgency "high").  This allows time for other replicas to
    /// create a snapshot before this one does.
    ///
    /// Set this to true on systems more constrained in CPU, memory, or bandwidth than a typical desktop
    /// system
    pub(crate) async fn sync(
        &mut self,
        server: &mut Box<dyn Server>,
        avoid_snapshots: bool,
    ) -> Result<()> {
        let mut txn = self.storage.txn().await?;
        sync::sync(server, txn.as_mut(), avoid_snapshots).await
    }

    /// Return the operations back to and including the last undo point, or since the last sync if
    /// no undo point is found.
    ///
    /// The operations are returned in the order they were applied. Use
    /// [`commit_reversed_operations`] to "undo" them.
    pub(crate) async fn get_undo_operations(&mut self) -> Result<Operations> {
        let mut txn = self.storage.txn().await?;
        undo::get_undo_operations(txn.as_mut()).await
    }

    /// Commit the reverse of the given operations, beginning with the last operation in the given
    /// operations and proceeding to the first.
    ///
    /// This method only supports reversing operations if they precisely match local operations
    /// that have not yet been synchronized, and will return `false` if this is not the case.
    pub(crate) async fn commit_reversed_operations(
        &mut self,
        undo_ops: Operations,
    ) -> Result<bool> {
        let mut txn = self.storage.txn().await?;
        undo::commit_reversed_operations(txn.as_mut(), undo_ops).await
    }

    /// Get the number of un-synchronized operations in storage, excluding undo
    /// operations.
    pub(crate) async fn num_operations(&mut self) -> Result<usize> {
        let mut txn = self.storage.txn().await?;
        Ok(txn
            .unsynced_operations()
            .await?
            .iter()
            .filter(|o| !o.is_undo_point())
            .count())
    }

    /// Get the number of (un-synchronized) undo points in storage.
    pub(crate) async fn num_undo_points(&mut self) -> Result<usize> {
        let mut txn = self.storage.txn().await?;
        Ok(txn
            .unsynced_operations()
            .await?
            .iter()
            .filter(|o| o.is_undo_point())
            .count())
    }

    // functions for supporting tests

    #[cfg(test)]
    pub(crate) async fn sorted_tasks(&mut self) -> Vec<(Uuid, Vec<(String, String)>)> {
        let mut res: Vec<(Uuid, Vec<(String, String)>)> = self
            .all_tasks()
            .await
            .unwrap()
            .iter()
            .map(|(u, t)| {
                let mut t = t
                    .iter()
                    .map(|(p, v)| (p.clone(), v.clone()))
                    .collect::<Vec<(String, String)>>();
                t.sort();
                (*u, t)
            })
            .collect();
        res.sort();
        res
    }

    #[cfg(test)]
    pub(crate) async fn operations(&mut self) -> Vec<Operation> {
        let mut txn = self.storage.txn().await.unwrap();
        txn.unsynced_operations().await.unwrap().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::inmemory::InMemoryStorage;

    use super::*;
    use chrono::Utc;
    use pretty_assertions::assert_eq;
    use uuid::Uuid;

    #[tokio::test]
    async fn commit_operations() -> Result<()> {
        let mut db = TaskDb::new(InMemoryStorage::new());
        let uuid = Uuid::new_v4();
        let now = Utc::now();
        let mut ops = Operations::new();
        ops.push(Operation::Create { uuid });
        ops.push(Operation::Update {
            uuid,
            property: String::from("title"),
            value: Some("my task".into()),
            timestamp: now,
            old_value: Some("old".into()),
        });

        db.commit_operations(ops, |_| false).await?;

        assert_eq!(
            db.sorted_tasks().await,
            vec![(uuid, vec![("title".into(), "my task".into())])]
        );
        assert_eq!(
            db.operations().await,
            vec![
                Operation::Create { uuid },
                Operation::Update {
                    uuid,
                    property: String::from("title"),
                    value: Some("my task".into()),
                    timestamp: now,
                    old_value: Some("old".into()),
                },
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn commit_operations_update_working_set() -> Result<()> {
        let mut db = TaskDb::new(InMemoryStorage::new());
        let mut uuids = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        uuids.sort();
        let [uuid1, uuid2, uuid3] = uuids;

        // uuid1 already exists in the working set.
        {
            let mut txn = db.storage.txn().await?;
            txn.add_to_working_set(uuid1).await?;
            txn.commit().await?;
        }

        let mut ops = Operations::new();
        ops.push(Operation::Create { uuid: uuid1 });
        ops.push(Operation::Create { uuid: uuid2 });
        ops.push(Operation::Create { uuid: uuid3 });
        ops.push(Operation::Create { uuid: uuid2 });
        ops.push(Operation::Create { uuid: uuid3 });

        // return true for updates to uuid1 or uuid2.
        let add_to_working_set = |op: &Operation| match op {
            Operation::Create { uuid } => *uuid == uuid1 || *uuid == uuid2,
            _ => false,
        };
        db.commit_operations(ops, add_to_working_set).await?;

        assert_eq!(
            db.sorted_tasks().await,
            vec![(uuid1, vec![]), (uuid2, vec![]), (uuid3, vec![]),]
        );
        assert_eq!(
            db.operations().await,
            vec![
                Operation::Create { uuid: uuid1 },
                Operation::Create { uuid: uuid2 },
                Operation::Create { uuid: uuid3 },
                Operation::Create { uuid: uuid2 },
                Operation::Create { uuid: uuid3 },
            ]
        );

        // uuid2 was added to the working set, once, and uuid3 was not.
        assert_eq!(
            db.working_set().await?,
            vec![None, Some(uuid1), Some(uuid2)],
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_num_operations() {
        let mut db = TaskDb::new(InMemoryStorage::new());
        let mut ops = Operations::new();
        ops.push(Operation::Create {
            uuid: Uuid::new_v4(),
        });
        ops.push(Operation::UndoPoint);
        ops.push(Operation::Create {
            uuid: Uuid::new_v4(),
        });
        db.commit_operations(ops, |_| false).await.unwrap();
        assert_eq!(db.num_operations().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_num_undo_points() {
        let mut db = TaskDb::new(InMemoryStorage::new());
        let mut ops = Operations::new();
        ops.push(Operation::UndoPoint);
        db.commit_operations(ops, |_| false).await.unwrap();
        assert_eq!(db.num_undo_points().await.unwrap(), 1);

        let mut ops = Operations::new();
        ops.push(Operation::UndoPoint);
        db.commit_operations(ops, |_| false).await.unwrap();
        assert_eq!(db.num_undo_points().await.unwrap(), 2);
    }
}
