use crate::errors::{Error, Result};
use crate::operation::Operation;
use crate::server::SyncOp;
use crate::storage::{StorageTxn, TaskMap};
use crate::Operations;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use uuid::Uuid;

/// Apply `operations` to the database in the given single transaction.
///
/// This updates the set of tasks in the database, but does not modify the list of operations.
/// If the operation does not make sense in the current state, it is ignored.
///
/// The transaction is not committed.
pub(super) async fn apply_operations(
    txn: &mut dyn StorageTxn,
    operations: &Operations,
) -> Result<()> {
    // A cache of TaskMaps updated in this sequence of operations, but for which `txn.set_task` has
    // not yet been called.
    let mut tasks: HashMap<Uuid, Option<TaskMap>> = HashMap::new();

    async fn get_cache<'t>(
        uuid: Uuid,
        tasks: &'t mut HashMap<Uuid, Option<TaskMap>>,
        txn: &mut dyn StorageTxn,
    ) -> Result<Option<&'t mut TaskMap>> {
        match tasks.entry(uuid) {
            Entry::Occupied(occupied_entry) => Ok(occupied_entry.into_mut().as_mut()),
            Entry::Vacant(vacant_entry) => {
                let task = txn.get_task(uuid).await?;
                Ok(vacant_entry.insert(task).as_mut())
            }
        }
    }

    // Call `txn.set_task` for this task, if necessary, and remove from the cache.
    async fn flush_cache(
        uuid: Uuid,
        tasks: &mut HashMap<Uuid, Option<TaskMap>>,
        txn: &mut dyn StorageTxn,
    ) -> Result<()> {
        if let Entry::Occupied(occupied_entry) = tasks.entry(uuid) {
            let v = occupied_entry.remove();
            if let Some(taskmap) = v {
                txn.set_task(uuid, taskmap).await?;
            }
        }
        Ok(())
    }

    for operation in operations {
        match operation {
            Operation::Create { uuid } => {
                // The create_task method will do nothing if the task exists. If it was cached
                // as not existing, clear that information. If it had cached updates, then there
                // is no harm flushing those updates now.
                flush_cache(*uuid, &mut tasks, txn).await?;
                txn.create_task(*uuid).await?;
            }
            Operation::Delete { uuid, .. } => {
                // The delete_task method will do nothing if the task does not exist.
                txn.delete_task(*uuid).await?;
                // The task now unconditionally does not exist. If there was a pending
                // `txn.set_task`, it can safely be skipped.
                tasks.insert(*uuid, None);
            }
            Operation::Update {
                uuid,
                property,
                value,
                ..
            } => {
                let task = get_cache(*uuid, &mut tasks, txn).await?;
                // If the task does not exist, do nothing.
                if let Some(task) = task {
                    if let Some(v) = value {
                        task.insert(property.clone(), v.clone());
                    } else {
                        task.remove(property);
                    }
                }
            }
            Operation::UndoPoint => {}
        }
    }

    // Flush any remaining tasks in the cache.
    while let Some((uuid, _)) = tasks.iter().next() {
        flush_cache(*uuid, &mut tasks, txn).await?;
    }

    Ok(())
}

/// Apply a [`SyncOp`] to the TaskDb's set of tasks (without recording it in the list of operations)
pub(super) async fn apply_op(txn: &mut dyn StorageTxn, op: &SyncOp) -> Result<()> {
    match op {
        SyncOp::Create { uuid } => {
            // insert if the task does not already exist
            if !txn.create_task(*uuid).await? {
                return Err(Error::Database(format!("Task {uuid} already exists")));
            }
        }
        SyncOp::Delete { ref uuid } => {
            if !txn.delete_task(*uuid).await? {
                return Err(Error::Database(format!("Task {uuid} does not exist")));
            }
        }
        SyncOp::Update {
            ref uuid,
            ref property,
            ref value,
            timestamp: _,
        } => {
            // update if this task exists, otherwise ignore
            if let Some(mut task) = txn.get_task(*uuid).await? {
                match value {
                    Some(ref val) => task.insert(property.to_string(), val.clone()),
                    None => task.remove(property),
                };
                txn.set_task(*uuid, task).await?;
            } else {
                return Err(Error::Database(format!("Task {uuid} does not exist")));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::vec_init_then_push)]
    use super::*;
    use crate::storage::inmemory::InMemoryStorage;
    use crate::storage::{taskmap_with, Storage, TaskMap};
    use crate::taskdb::TaskDb;
    use chrono::Utc;
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;
    use uuid::Uuid;

    #[tokio::test]
    async fn apply_operations_create() -> Result<()> {
        let mut db = TaskDb::new(InMemoryStorage::new());
        let uuid = Uuid::new_v4();
        let mut ops = Operations::new();
        ops.push(Operation::Create { uuid });

        {
            let mut txn = db.storage.txn().await?;
            apply_operations(txn.as_mut(), &ops).await?;
            txn.commit().await?;
        }

        assert_eq!(db.sorted_tasks().await, vec![(uuid, vec![])]);
        Ok(())
    }

    #[tokio::test]
    async fn apply_operations_create_exists() -> Result<()> {
        let mut db = TaskDb::new(InMemoryStorage::new());
        let uuid = Uuid::new_v4();
        {
            let mut txn = db.storage.txn().await?;
            txn.create_task(uuid).await?;
            txn.set_task(uuid, taskmap_with(vec![("foo".into(), "bar".into())]))
                .await?;
            txn.commit().await?;
        }

        {
            let mut ops = Operations::new();
            ops.push(Operation::Create { uuid });
            let mut txn = db.storage.txn().await?;
            apply_operations(txn.as_mut(), &ops).await?;
            txn.commit().await?;
        }

        assert_eq!(
            db.sorted_tasks().await,
            vec![(uuid, vec![("foo".into(), "bar".into())])]
        );
        Ok(())
    }

    #[tokio::test]
    async fn apply_operations_create_exists_update() -> Result<()> {
        let mut db = TaskDb::new(InMemoryStorage::new());
        let now = Utc::now();
        let uuid = Uuid::new_v4();
        {
            let mut txn = db.storage.txn().await?;
            txn.create_task(uuid).await?;
            txn.set_task(uuid, taskmap_with(vec![("foo".into(), "bar".into())]))
                .await?;
            txn.commit().await?;
        }

        {
            let mut ops = Operations::new();
            ops.push(Operation::Create { uuid });
            ops.push(Operation::Update {
                uuid,
                property: String::from("title"),
                value: Some("my task".into()),
                timestamp: now,
                old_value: None,
            });
            let mut txn = db.storage.txn().await?;
            apply_operations(txn.as_mut(), &ops).await?;
            txn.commit().await?;
        }

        assert_eq!(
            db.sorted_tasks().await,
            vec![(
                uuid,
                vec![
                    ("foo".into(), "bar".into()),
                    ("title".into(), "my task".into())
                ]
            )]
        );
        Ok(())
    }

    #[tokio::test]
    async fn apply_operations_create_update() -> Result<()> {
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
            old_value: None,
        });

        {
            let mut txn = db.storage.txn().await?;
            apply_operations(txn.as_mut(), &ops).await?;
            txn.commit().await?;
        }

        assert_eq!(
            db.sorted_tasks().await,
            vec![(uuid, vec![("title".into(), "my task".into())])]
        );
        Ok(())
    }

    #[tokio::test]
    async fn apply_operations_create_update_delete_prop() -> Result<()> {
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
            old_value: None,
        });
        ops.push(Operation::Update {
            uuid,
            property: String::from("priority"),
            value: Some("H".into()),
            timestamp: now,
            old_value: None,
        });
        ops.push(Operation::Update {
            uuid,
            property: String::from("title"),
            value: None,
            timestamp: now,
            old_value: Some("my task".into()),
        });

        {
            let mut txn = db.storage.txn().await?;
            apply_operations(txn.as_mut(), &ops).await?;
            txn.commit().await?;
        }

        assert_eq!(
            db.sorted_tasks().await,
            vec![(uuid, vec![("priority".into(), "H".into())])]
        );
        Ok(())
    }

    #[tokio::test]
    async fn apply_operations_update_does_not_exist() -> Result<()> {
        let mut db = TaskDb::new(InMemoryStorage::new());
        let uuid = Uuid::new_v4();
        let now = Utc::now();
        let mut ops = Operations::new();
        ops.push(Operation::Update {
            uuid,
            property: String::from("title"),
            value: Some("my task".into()),
            timestamp: now,
            old_value: None,
        });

        {
            let mut txn = db.storage.txn().await?;
            apply_operations(txn.as_mut(), &ops).await?;
            txn.commit().await?;
        }

        assert_eq!(db.sorted_tasks().await, vec![]);
        Ok(())
    }

    #[tokio::test]
    async fn apply_operations_delete_then_update() -> Result<()> {
        let mut db = TaskDb::new(InMemoryStorage::new());
        let uuid = Uuid::new_v4();
        let now = Utc::now();
        let mut ops = Operations::new();
        ops.push(Operation::Create { uuid });
        ops.push(Operation::Update {
            uuid,
            property: String::from("old"),
            value: Some("uhoh".into()),
            timestamp: now,
            old_value: None,
        });
        ops.push(Operation::Delete {
            uuid,
            old_task: taskmap_with(vec![]),
        });
        ops.push(Operation::Update {
            uuid,
            property: String::from("new"),
            value: Some("uhoh".into()),
            timestamp: now,
            old_value: None,
        });

        {
            let mut txn = db.storage.txn().await?;
            apply_operations(txn.as_mut(), &ops).await?;
            txn.commit().await?;
        }

        assert_eq!(db.sorted_tasks().await, vec![]);
        Ok(())
    }

    #[tokio::test]
    async fn apply_operations_several_tasks() -> Result<()> {
        let mut db = TaskDb::new(InMemoryStorage::new());
        let mut uuids = [Uuid::new_v4(), Uuid::new_v4()];
        uuids.sort();
        let now = Utc::now();
        let mut ops = Operations::new();
        ops.push(Operation::Create { uuid: uuids[0] });
        ops.push(Operation::Create { uuid: uuids[1] });
        ops.push(Operation::Update {
            uuid: uuids[0],
            property: String::from("p"),
            value: Some("1".into()),
            timestamp: now,
            old_value: None,
        });
        ops.push(Operation::Update {
            uuid: uuids[1],
            property: String::from("p"),
            value: Some("2".into()),
            timestamp: now,
            old_value: None,
        });

        {
            let mut txn = db.storage.txn().await?;
            apply_operations(txn.as_mut(), &ops).await?;
            txn.commit().await?;
        }

        assert_eq!(
            db.sorted_tasks().await,
            vec![
                (uuids[0], vec![("p".into(), "1".into())]),
                (uuids[1], vec![("p".into(), "2".into())])
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn apply_operations_create_delete() -> Result<()> {
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
            old_value: None,
        });
        ops.push(Operation::Delete {
            uuid,
            old_task: taskmap_with(vec![]),
        });

        {
            let mut txn = db.storage.txn().await?;
            apply_operations(txn.as_mut(), &ops).await?;
            txn.commit().await?;
        }

        assert_eq!(db.sorted_tasks().await, vec![]);
        Ok(())
    }

    #[tokio::test]
    async fn apply_operations_delete_not_present() -> Result<()> {
        let mut db = TaskDb::new(InMemoryStorage::new());
        let uuid = Uuid::new_v4();
        let mut ops = Operations::new();
        ops.push(Operation::Delete {
            uuid,
            old_task: taskmap_with(vec![]),
        });

        {
            let mut txn = db.storage.txn().await?;
            apply_operations(txn.as_mut(), &ops).await?;
            txn.commit().await?;
        }

        assert_eq!(db.sorted_tasks().await, vec![]);
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_create() -> Result<()> {
        let mut db = TaskDb::new(InMemoryStorage::new());
        let uuid = Uuid::new_v4();
        let op = SyncOp::Create { uuid };

        {
            let mut txn = db.storage.txn().await?;
            apply_op(txn.as_mut(), &op).await?;
            txn.commit().await?;
        }

        assert_eq!(db.sorted_tasks().await, vec![(uuid, vec![])]);
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_create_exists() -> Result<()> {
        let mut db = TaskDb::new(InMemoryStorage::new());
        let uuid = Uuid::new_v4();
        {
            let mut txn = db.storage.txn().await?;
            txn.create_task(uuid).await?;
            let mut taskmap = TaskMap::new();
            taskmap.insert("foo".into(), "bar".into());
            txn.set_task(uuid, taskmap).await?;
            txn.commit().await?;
        }

        let op = SyncOp::Create { uuid };
        {
            let mut txn = db.storage.txn().await?;
            assert!(apply_op(txn.as_mut(), &op).await.is_err());
        }

        // create did not delete the old task..
        assert_eq!(
            db.sorted_tasks().await,
            vec![(uuid, vec![("foo".into(), "bar".into())])]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_apply_create_update() -> Result<()> {
        let mut db = TaskDb::new(InMemoryStorage::new());
        let uuid = Uuid::new_v4();
        let now = Utc::now();
        let op1 = SyncOp::Create { uuid };

        {
            let mut txn = db.storage.txn().await?;
            apply_op(txn.as_mut(), &op1).await?;
            txn.commit().await?;
        }

        let op2 = SyncOp::Update {
            uuid,
            property: String::from("title"),
            value: Some("my task".into()),
            timestamp: now,
        };
        {
            let mut txn = db.storage.txn().await?;
            apply_op(txn.as_mut(), &op2).await?;
            txn.commit().await?;
        }

        assert_eq!(
            db.sorted_tasks().await,
            vec![(uuid, vec![("title".into(), "my task".into())])]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_create_update_delete_prop() -> Result<()> {
        let mut db = TaskDb::new(InMemoryStorage::new());
        let uuid = Uuid::new_v4();
        let now = Utc::now();
        let op1 = SyncOp::Create { uuid };
        {
            let mut txn = db.storage.txn().await?;
            apply_op(txn.as_mut(), &op1).await?;
            txn.commit().await?;
        }

        let op2 = SyncOp::Update {
            uuid,
            property: String::from("title"),
            value: Some("my task".into()),
            timestamp: now,
        };
        {
            let mut txn = db.storage.txn().await?;
            apply_op(txn.as_mut(), &op2).await?;
            txn.commit().await?;
        }

        let op3 = SyncOp::Update {
            uuid,
            property: String::from("priority"),
            value: Some("H".into()),
            timestamp: now,
        };
        {
            let mut txn = db.storage.txn().await?;
            apply_op(txn.as_mut(), &op3).await?;
            txn.commit().await?;
        }

        let op4 = SyncOp::Update {
            uuid,
            property: String::from("title"),
            value: None,
            timestamp: now,
        };
        {
            let mut txn = db.storage.txn().await?;
            apply_op(txn.as_mut(), &op4).await?;
            txn.commit().await?;
        }

        let mut exp = HashMap::new();
        let mut task = HashMap::new();
        task.insert(String::from("priority"), String::from("H"));
        exp.insert(uuid, task);
        assert_eq!(
            db.sorted_tasks().await,
            vec![(uuid, vec![("priority".into(), "H".into())])]
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_update_does_not_exist() -> Result<()> {
        let mut db = TaskDb::new(InMemoryStorage::new());
        let uuid = Uuid::new_v4();
        let op = SyncOp::Update {
            uuid,
            property: String::from("title"),
            value: Some("my task".into()),
            timestamp: Utc::now(),
        };
        {
            let mut txn = db.storage.txn().await?;
            assert_eq!(
                apply_op(txn.as_mut(), &op).await.err().unwrap().to_string(),
                format!("Task Database Error: Task {} does not exist", uuid)
            );
            txn.commit().await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_create_delete() -> Result<()> {
        let mut db = TaskDb::new(InMemoryStorage::new());
        let uuid = Uuid::new_v4();
        let now = Utc::now();

        let op1 = SyncOp::Create { uuid };
        {
            let mut txn = db.storage.txn().await?;
            apply_op(txn.as_mut(), &op1).await?;
            txn.commit().await?;
        }

        let op2 = SyncOp::Update {
            uuid,
            property: String::from("priority"),
            value: Some("H".into()),
            timestamp: now,
        };
        {
            let mut txn = db.storage.txn().await?;
            apply_op(txn.as_mut(), &op2).await?;
            txn.commit().await?;
        }

        let op3 = SyncOp::Delete { uuid };
        {
            let mut txn = db.storage.txn().await?;
            apply_op(txn.as_mut(), &op3).await?;
            txn.commit().await?;
        }

        assert_eq!(db.sorted_tasks().await, vec![]);
        let mut old_task = TaskMap::new();
        old_task.insert("priority".into(), "H".into());

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_delete_not_present() -> Result<()> {
        let mut db = TaskDb::new(InMemoryStorage::new());
        let uuid = Uuid::new_v4();
        let op = SyncOp::Delete { uuid };
        {
            let mut txn = db.storage.txn().await?;
            assert!(apply_op(txn.as_mut(), &op).await.is_err());
            txn.commit().await?;
        }

        Ok(())
    }
}
