use crate::errors::{Error, Result};
use crate::operation::Operation;
use crate::server::SyncOp;
use crate::storage::StorageTxn;
use crate::Operations;

/// Apply `operations` to the database in the given single transaction.
///
/// This updates the set of tasks in the database, but does not modify the list of operations.
/// If the operation does not make sense in the current state, it is ignored.
///
/// The transaction is not committed.
pub(super) fn apply_operations(txn: &mut dyn StorageTxn, operations: &Operations) -> Result<()> {
    for operation in operations {
        match operation {
            Operation::Create { uuid } => {
                // The create_task method will do nothing if the task exists.
                txn.create_task(*uuid)?;
            }
            Operation::Delete { uuid, .. } => {
                // The delete_task method will do nothing if the task does not exist.
                txn.delete_task(*uuid)?;
            }
            Operation::Update {
                uuid,
                property,
                value,
                ..
            } => {
                // TODO: could cache this value from op to op
                let task = txn.get_task(*uuid)?;
                // If the task does not exist, do nothing.
                if let Some(mut task) = task {
                    if let Some(v) = value {
                        task.insert(property.clone(), v.clone());
                    } else {
                        task.remove(property);
                    }
                    txn.set_task(*uuid, task)?;
                }
            }
            Operation::UndoPoint => {}
        }
    }
    Ok(())
}

/// Apply the given SyncOp to the replica, updating both the task data and adding a
/// Operation to the list of operations.  Returns the TaskMap of the task after the
/// operation has been applied (or an empty TaskMap for Delete).  It is not an error
/// to create an existing task, nor to delete a nonexistent task.
#[cfg(test)]
pub(super) fn apply_and_record(
    txn: &mut dyn StorageTxn,
    op: SyncOp,
) -> Result<crate::storage::TaskMap> {
    use crate::storage::TaskMap;
    match op {
        SyncOp::Create { uuid } => {
            let created = txn.create_task(uuid)?;
            if created {
                txn.add_operation(Operation::Create { uuid })?;
                txn.commit()?;
                Ok(TaskMap::new())
            } else {
                Ok(txn
                    .get_task(uuid)?
                    .expect("create_task failed but task does not exist"))
            }
        }
        SyncOp::Delete { uuid } => {
            let task = txn.get_task(uuid)?;
            if let Some(task) = task {
                txn.delete_task(uuid)?;
                txn.add_operation(Operation::Delete {
                    uuid,
                    old_task: task,
                })?;
                txn.commit()?;
                Ok(TaskMap::new())
            } else {
                Ok(TaskMap::new())
            }
        }
        SyncOp::Update {
            uuid,
            property,
            value,
            timestamp,
        } => {
            let task = txn.get_task(uuid)?;
            if let Some(mut task) = task {
                let old_value = task.get(&property).cloned();
                if let Some(ref v) = value {
                    task.insert(property.clone(), v.clone());
                } else {
                    task.remove(&property);
                }
                txn.set_task(uuid, task.clone())?;
                txn.add_operation(Operation::Update {
                    uuid,
                    property,
                    old_value,
                    value,
                    timestamp,
                })?;
                txn.commit()?;
                Ok(task)
            } else {
                Err(Error::Database(format!("Task {} does not exist", uuid)))
            }
        }
    }
}

/// Apply a [`SyncOp`] to the TaskDb's set of tasks (without recording it in the list of operations)
pub(super) fn apply_op(txn: &mut dyn StorageTxn, op: &SyncOp) -> Result<()> {
    match op {
        SyncOp::Create { uuid } => {
            // insert if the task does not already exist
            if !txn.create_task(*uuid)? {
                return Err(Error::Database(format!("Task {} already exists", uuid)));
            }
        }
        SyncOp::Delete { ref uuid } => {
            if !txn.delete_task(*uuid)? {
                return Err(Error::Database(format!("Task {} does not exist", uuid)));
            }
        }
        SyncOp::Update {
            ref uuid,
            ref property,
            ref value,
            timestamp: _,
        } => {
            // update if this task exists, otherwise ignore
            if let Some(mut task) = txn.get_task(*uuid)? {
                match value {
                    Some(ref val) => task.insert(property.to_string(), val.clone()),
                    None => task.remove(property),
                };
                txn.set_task(*uuid, task)?;
            } else {
                return Err(Error::Database(format!("Task {} does not exist", uuid)));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{taskmap_with, TaskMap};
    use crate::taskdb::TaskDb;
    use chrono::Utc;
    use pretty_assertions::assert_eq;
    use std::collections::HashMap;
    use uuid::Uuid;

    #[test]
    fn apply_operations_create() -> Result<()> {
        let mut db = TaskDb::new_inmemory();
        let uuid = Uuid::new_v4();
        let mut ops = Operations::new();
        ops.add(Operation::Create { uuid });

        {
            let mut txn = db.storage.txn()?;
            apply_operations(txn.as_mut(), &ops)?;
            txn.commit()?;
        }

        assert_eq!(db.sorted_tasks(), vec![(uuid, vec![]),]);
        Ok(())
    }

    #[test]
    fn apply_operations_create_exists() -> Result<()> {
        let mut db = TaskDb::new_inmemory();
        let uuid = Uuid::new_v4();
        {
            let mut txn = db.storage.txn()?;
            txn.create_task(uuid)?;
            txn.set_task(uuid, taskmap_with(vec![("foo".into(), "bar".into())]))?;
            txn.commit()?;
        }

        {
            let mut ops = Operations::new();
            ops.add(Operation::Create { uuid });
            let mut txn = db.storage.txn()?;
            apply_operations(txn.as_mut(), &ops)?;
            txn.commit()?;
        }

        assert_eq!(
            db.sorted_tasks(),
            vec![(uuid, vec![("foo".into(), "bar".into())])]
        );
        Ok(())
    }

    #[test]
    fn apply_operations_create_update() -> Result<()> {
        let mut db = TaskDb::new_inmemory();
        let uuid = Uuid::new_v4();
        let now = Utc::now();
        let mut ops = Operations::new();
        ops.add(Operation::Create { uuid });
        ops.add(Operation::Update {
            uuid,
            property: String::from("title"),
            value: Some("my task".into()),
            timestamp: now,
            old_value: None,
        });

        {
            let mut txn = db.storage.txn()?;
            apply_operations(txn.as_mut(), &ops)?;
            txn.commit()?;
        }

        assert_eq!(
            db.sorted_tasks(),
            vec![(uuid, vec![("title".into(), "my task".into())])]
        );
        Ok(())
    }

    #[test]
    fn apply_operations_create_update_delete_prop() -> Result<()> {
        let mut db = TaskDb::new_inmemory();
        let uuid = Uuid::new_v4();
        let now = Utc::now();
        let mut ops = Operations::new();
        ops.add(Operation::Create { uuid });
        ops.add(Operation::Update {
            uuid,
            property: String::from("title"),
            value: Some("my task".into()),
            timestamp: now,
            old_value: None,
        });
        ops.add(Operation::Update {
            uuid,
            property: String::from("priority"),
            value: Some("H".into()),
            timestamp: now,
            old_value: None,
        });
        ops.add(Operation::Update {
            uuid,
            property: String::from("title"),
            value: None,
            timestamp: now,
            old_value: Some("my task".into()),
        });

        {
            let mut txn = db.storage.txn()?;
            apply_operations(txn.as_mut(), &ops)?;
            txn.commit()?;
        }

        assert_eq!(
            db.sorted_tasks(),
            vec![(uuid, vec![("priority".into(), "H".into())])]
        );
        Ok(())
    }

    #[test]
    fn apply_operations_update_does_not_exist() -> Result<()> {
        let mut db = TaskDb::new_inmemory();
        let uuid = Uuid::new_v4();
        let now = Utc::now();
        let mut ops = Operations::new();
        ops.add(Operation::Update {
            uuid,
            property: String::from("title"),
            value: Some("my task".into()),
            timestamp: now,
            old_value: None,
        });

        {
            let mut txn = db.storage.txn()?;
            apply_operations(txn.as_mut(), &ops)?;
            txn.commit()?;
        }

        assert_eq!(db.sorted_tasks(), vec![]);
        Ok(())
    }

    #[test]
    fn apply_operations_create_delete() -> Result<()> {
        let mut db = TaskDb::new_inmemory();
        let uuid = Uuid::new_v4();
        let now = Utc::now();
        let mut ops = Operations::new();
        ops.add(Operation::Create { uuid });
        ops.add(Operation::Update {
            uuid,
            property: String::from("title"),
            value: Some("my task".into()),
            timestamp: now,
            old_value: None,
        });
        ops.add(Operation::Delete {
            uuid,
            old_task: taskmap_with(vec![]),
        });

        {
            let mut txn = db.storage.txn()?;
            apply_operations(txn.as_mut(), &ops)?;
            txn.commit()?;
        }

        assert_eq!(db.sorted_tasks(), vec![]);
        Ok(())
    }

    #[test]
    fn apply_operations_delete_not_present() -> Result<()> {
        let mut db = TaskDb::new_inmemory();
        let uuid = Uuid::new_v4();
        let mut ops = Operations::new();
        ops.add(Operation::Delete {
            uuid,
            old_task: taskmap_with(vec![]),
        });

        {
            let mut txn = db.storage.txn()?;
            apply_operations(txn.as_mut(), &ops)?;
            txn.commit()?;
        }

        assert_eq!(db.sorted_tasks(), vec![]);
        Ok(())
    }

    #[test]
    fn test_apply_create() -> Result<()> {
        let mut db = TaskDb::new_inmemory();
        let uuid = Uuid::new_v4();
        let op = SyncOp::Create { uuid };

        {
            let mut txn = db.storage.txn()?;
            apply_op(txn.as_mut(), &op)?;
            txn.commit()?;
        }

        assert_eq!(db.sorted_tasks(), vec![(uuid, vec![]),]);
        Ok(())
    }

    #[test]
    fn test_apply_create_exists() -> Result<()> {
        let mut db = TaskDb::new_inmemory();
        let uuid = Uuid::new_v4();
        {
            let mut txn = db.storage.txn()?;
            txn.create_task(uuid)?;
            let mut taskmap = TaskMap::new();
            taskmap.insert("foo".into(), "bar".into());
            txn.set_task(uuid, taskmap)?;
            txn.commit()?;
        }

        let op = SyncOp::Create { uuid };
        {
            let mut txn = db.storage.txn()?;
            assert!(apply_op(txn.as_mut(), &op).is_err());
        }

        // create did not delete the old task..
        assert_eq!(
            db.sorted_tasks(),
            vec![(uuid, vec![("foo".into(), "bar".into())])]
        );
        Ok(())
    }

    #[test]
    fn test_apply_create_update() -> Result<()> {
        let mut db = TaskDb::new_inmemory();
        let uuid = Uuid::new_v4();
        let now = Utc::now();
        let op1 = SyncOp::Create { uuid };

        {
            let mut txn = db.storage.txn()?;
            apply_op(txn.as_mut(), &op1)?;
            txn.commit()?;
        }

        let op2 = SyncOp::Update {
            uuid,
            property: String::from("title"),
            value: Some("my task".into()),
            timestamp: now,
        };
        {
            let mut txn = db.storage.txn()?;
            apply_op(txn.as_mut(), &op2)?;
            txn.commit()?;
        }

        assert_eq!(
            db.sorted_tasks(),
            vec![(uuid, vec![("title".into(), "my task".into())])]
        );

        Ok(())
    }

    #[test]
    fn test_apply_create_update_delete_prop() -> Result<()> {
        let mut db = TaskDb::new_inmemory();
        let uuid = Uuid::new_v4();
        let now = Utc::now();
        let op1 = SyncOp::Create { uuid };
        {
            let mut txn = db.storage.txn()?;
            apply_op(txn.as_mut(), &op1)?;
            txn.commit()?;
        }

        let op2 = SyncOp::Update {
            uuid,
            property: String::from("title"),
            value: Some("my task".into()),
            timestamp: now,
        };
        {
            let mut txn = db.storage.txn()?;
            apply_op(txn.as_mut(), &op2)?;
            txn.commit()?;
        }

        let op3 = SyncOp::Update {
            uuid,
            property: String::from("priority"),
            value: Some("H".into()),
            timestamp: now,
        };
        {
            let mut txn = db.storage.txn()?;
            apply_op(txn.as_mut(), &op3)?;
            txn.commit()?;
        }

        let op4 = SyncOp::Update {
            uuid,
            property: String::from("title"),
            value: None,
            timestamp: now,
        };
        {
            let mut txn = db.storage.txn()?;
            apply_op(txn.as_mut(), &op4)?;
            txn.commit()?;
        }

        let mut exp = HashMap::new();
        let mut task = HashMap::new();
        task.insert(String::from("priority"), String::from("H"));
        exp.insert(uuid, task);
        assert_eq!(
            db.sorted_tasks(),
            vec![(uuid, vec![("priority".into(), "H".into())])]
        );

        Ok(())
    }

    #[test]
    fn test_apply_update_does_not_exist() -> Result<()> {
        let mut db = TaskDb::new_inmemory();
        let uuid = Uuid::new_v4();
        let op = SyncOp::Update {
            uuid,
            property: String::from("title"),
            value: Some("my task".into()),
            timestamp: Utc::now(),
        };
        {
            let mut txn = db.storage.txn()?;
            assert_eq!(
                apply_and_record(txn.as_mut(), op)
                    .err()
                    .unwrap()
                    .to_string(),
                format!("Task Database Error: Task {} does not exist", uuid)
            );
            txn.commit()?;
        }

        Ok(())
    }

    #[test]
    fn test_apply_create_delete() -> Result<()> {
        let mut db = TaskDb::new_inmemory();
        let uuid = Uuid::new_v4();
        let now = Utc::now();

        let op1 = SyncOp::Create { uuid };
        {
            let mut txn = db.storage.txn()?;
            apply_op(txn.as_mut(), &op1)?;
            txn.commit()?;
        }

        let op2 = SyncOp::Update {
            uuid,
            property: String::from("priority"),
            value: Some("H".into()),
            timestamp: now,
        };
        {
            let mut txn = db.storage.txn()?;
            apply_op(txn.as_mut(), &op2)?;
            txn.commit()?;
        }

        let op3 = SyncOp::Delete { uuid };
        {
            let mut txn = db.storage.txn()?;
            apply_op(txn.as_mut(), &op3)?;
            txn.commit()?;
        }

        assert_eq!(db.sorted_tasks(), vec![]);
        let mut old_task = TaskMap::new();
        old_task.insert("priority".into(), "H".into());

        Ok(())
    }

    #[test]
    fn test_apply_delete_not_present() -> Result<()> {
        let mut db = TaskDb::new_inmemory();
        let uuid = Uuid::new_v4();
        let op = SyncOp::Delete { uuid };
        {
            let mut txn = db.storage.txn()?;
            assert!(apply_op(txn.as_mut(), &op).is_err());
            txn.commit()?;
        }

        Ok(())
    }
}
