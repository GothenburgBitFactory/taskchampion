use std::collections::HashSet;

use crate::errors::Result;
use crate::operation::Operation;
use crate::storage::{StorageTxn, TaskMap};
use crate::{Operations, Server};
use uuid::Uuid;

mod apply;
mod snapshot;
mod sync;
pub(crate) mod undo;
mod working_set;

/// A TaskDb is the backend for a replica.  It manages the operations, synchronization,
/// and so on, and all the invariants that come with it.  It leaves the meaning of particular task
/// properties to the replica and task implementations.
pub(crate) struct TaskDb;

impl TaskDb {
    /// Apply `operations` to the database in a single transaction.
    ///
    /// The operations will be appended to the list of local operations, and the set of tasks will
    /// be updated accordingly.
    ///
    /// Any operations for which `add_to_working_set` returns true will cause the relevant
    /// task to be added to the working set.
    pub(crate) fn commit_operations<F>(
        txn: &mut dyn StorageTxn,
        operations: Operations,
        add_to_working_set: F,
    ) -> Result<()>
    where
        F: Fn(&Operation) -> bool,
    {
        apply::apply_operations(txn, &operations)?;

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
        let mut working_set: HashSet<Uuid> =
            txn.get_working_set()?.iter().filter_map(|u| *u).collect();
        for uuid in to_add {
            // Double-check that we are not adding a task to the working-set twice.
            if !working_set.contains(&uuid) {
                txn.add_to_working_set(uuid)?;
                working_set.insert(uuid);
            }
        }

        for operation in operations {
            txn.add_operation(operation)?;
        }

        txn.commit()
    }

    /// Get all tasks.
    pub(crate) fn all_tasks(txn: &mut dyn StorageTxn) -> Result<Vec<(Uuid, TaskMap)>> {
        txn.all_tasks()
    }

    /// Get the UUIDs of all tasks
    pub(crate) fn all_task_uuids(txn: &mut dyn StorageTxn) -> Result<Vec<Uuid>> {
        txn.all_task_uuids()
    }

    /// Get the working set
    pub(crate) fn working_set(txn: &mut dyn StorageTxn) -> Result<Vec<Option<Uuid>>> {
        txn.get_working_set()
    }

    /// Get a single task, by uuid.
    pub(crate) fn get_task(txn: &mut dyn StorageTxn, uuid: Uuid) -> Result<Option<TaskMap>> {
        txn.get_task(uuid)
    }

    /// Get all pending tasks from the working set
    pub(crate) fn get_pending_tasks(txn: &mut dyn StorageTxn) -> Result<Vec<(Uuid, TaskMap)>> {
        txn.get_pending_tasks()
    }

    pub(crate) fn get_task_operations(txn: &mut dyn StorageTxn, uuid: Uuid) -> Result<Operations> {
        txn.get_task_operations(uuid)
    }

    /// Rebuild the working set using a function to identify tasks that should be in the set.  This
    /// renumbers the existing working-set tasks to eliminate gaps, and also adds any tasks that
    /// are not already in the working set but should be.  The rebuild occurs in a single
    /// trasnsaction against the storage backend.
    pub(crate) fn rebuild_working_set<F>(
        txn: &mut dyn StorageTxn,
        in_working_set: F,
        renumber: bool,
    ) -> Result<()>
    where
        F: Fn(&TaskMap) -> bool,
    {
        working_set::rebuild(txn, in_working_set, renumber)
    }

    /// Sync to the given server, pulling remote changes and pushing local changes.
    ///
    /// If `avoid_snapshots` is true, the sync operations produces a snapshot only when the server
    /// indicate it is urgent (snapshot urgency "high").  This allows time for other replicas to
    /// create a snapshot before this one does.
    ///
    /// Set this to true on systems more constrained in CPU, memory, or bandwidth than a typical desktop
    /// system
    pub(crate) fn sync(
        txn: &mut dyn StorageTxn,
        server: &mut dyn Server,
        avoid_snapshots: bool,
    ) -> Result<()> {
        sync::sync(server, txn, avoid_snapshots)
    }

    /// Return the operations back to and including the last undo point, or since the last sync if
    /// no undo point is found.
    ///
    /// The operations are returned in the order they were applied. Use
    /// [`commit_reversed_operations`] to "undo" them.
    pub(crate) fn get_undo_operations(txn: &mut dyn StorageTxn) -> Result<Operations> {
        undo::get_undo_operations(txn)
    }

    /// Commit the reverse of the given operations, beginning with the last operation in the given
    /// operations and proceeding to the first.
    ///
    /// This method only supports reversing operations if they precisely match local operations
    /// that have not yet been synchronized, and will return `false` if this is not the case.
    pub(crate) fn commit_reversed_operations(
        txn: &mut dyn StorageTxn,
        undo_ops: Operations,
    ) -> Result<bool> {
        undo::commit_reversed_operations(txn, undo_ops)
    }

    /// Get the number of un-synchronized operations in storage, excluding undo
    /// operations.
    pub(crate) fn num_operations(txn: &mut dyn StorageTxn) -> Result<usize> {
        Ok(txn
            .unsynced_operations()?
            .iter()
            .filter(|o| !o.is_undo_point())
            .count())
    }

    /// Get the number of (un-synchronized) undo points in storage.
    pub(crate) fn num_undo_points(txn: &mut dyn StorageTxn) -> Result<usize> {
        Ok(txn
            .unsynced_operations()?
            .iter()
            .filter(|o| o.is_undo_point())
            .count())
    }

    // functions for supporting tests

    #[cfg(test)]
    pub(crate) fn sorted_tasks(txn: &mut dyn StorageTxn) -> Vec<(Uuid, Vec<(String, String)>)> {
        let mut res: Vec<(Uuid, Vec<(String, String)>)> = TaskDb::all_tasks(txn)
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
    pub(crate) fn operations(txn: &mut dyn StorageTxn) -> Vec<Operation> {
        txn.unsynced_operations().unwrap().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{InMemoryStorage, Storage};
    use chrono::Utc;
    use pretty_assertions::assert_eq;
    use uuid::Uuid;

    #[tokio::test]
    async fn commit_operations() -> Result<()> {
        let storage = InMemoryStorage::new();
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

        storage
            .txn(move |txn| {
                TaskDb::commit_operations(txn, ops, |_| false)?;

                assert_eq!(
                    TaskDb::sorted_tasks(txn),
                    vec![(uuid, vec![("title".into(), "my task".into())])]
                );
                assert_eq!(
                    TaskDb::operations(txn),
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
            })
            .await
    }

    #[tokio::test]
    async fn commit_operations_update_working_set() -> Result<()> {
        let storage = InMemoryStorage::new();
        let mut uuids = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        uuids.sort();
        let [uuid1, uuid2, uuid3] = uuids;

        // uuid1 already exists in the working set.

        storage
            .txn(move |txn| {
                txn.add_to_working_set(uuid1)?;
                txn.commit()
            })
            .await?;

        let mut ops = Operations::new();
        ops.push(Operation::Create { uuid: uuid1 });
        ops.push(Operation::Create { uuid: uuid2 });
        ops.push(Operation::Create { uuid: uuid3 });
        ops.push(Operation::Create { uuid: uuid2 });
        ops.push(Operation::Create { uuid: uuid3 });

        // return true for updates to uuid1 or uuid2.
        let add_to_working_set = move |op: &Operation| match op {
            Operation::Create { uuid } => *uuid == uuid1 || *uuid == uuid2,
            _ => false,
        };
        storage
            .txn(move |txn| {
                TaskDb::commit_operations(txn, ops, add_to_working_set)?;

                assert_eq!(
                    TaskDb::sorted_tasks(txn),
                    vec![(uuid1, vec![]), (uuid2, vec![]), (uuid3, vec![]),]
                );
                assert_eq!(
                    TaskDb::operations(txn),
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
                    TaskDb::working_set(txn)?,
                    vec![None, Some(uuid1), Some(uuid2)],
                );
                Ok(())
            })
            .await
    }

    #[tokio::test]
    async fn test_num_operations() -> Result<()> {
        let storage = InMemoryStorage::new();
        let mut ops = Operations::new();
        ops.push(Operation::Create {
            uuid: Uuid::new_v4(),
        });
        ops.push(Operation::UndoPoint);
        ops.push(Operation::Create {
            uuid: Uuid::new_v4(),
        });
        storage
            .txn(move |txn| {
                TaskDb::commit_operations(txn, ops, |_| false).unwrap();
                assert_eq!(TaskDb::num_operations(txn).unwrap(), 2);

                Ok(())
            })
            .await
    }

    #[tokio::test]
    async fn test_num_undo_points() -> Result<()> {
        let storage = InMemoryStorage::new();
        storage
            .txn(|txn| {
                let mut ops = Operations::new();
                ops.push(Operation::UndoPoint);
                TaskDb::commit_operations(txn, ops, |_| false).unwrap();
                assert_eq!(TaskDb::num_undo_points(txn).unwrap(), 1);

                let mut ops = Operations::new();
                ops.push(Operation::UndoPoint);
                TaskDb::commit_operations(txn, ops, |_| false).unwrap();
                assert_eq!(TaskDb::num_undo_points(txn).unwrap(), 2);

                Ok(())
            })
            .await
    }
}
