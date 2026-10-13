use super::apply;
use crate::errors::Result;
use crate::operation::{Operation, Operations};
use crate::storage::StorageTxn;
use chrono::Utc;
use log::{debug, info};

/// Return the operations back to and including the last undo point, or since the last sync if no
/// undo point is found.
///
/// The operations are returned in the order they were applied. Use [`commit_reversed_operations`]
/// to "undo" them.
pub(crate) async fn get_undo_operations(txn: &mut dyn StorageTxn) -> Result<Operations> {
    let local_ops = txn.unsynced_operations().await?;
    let last_undo_op_idx = local_ops
        .iter()
        .enumerate()
        .rev()
        .find(|(_, op)| op.is_undo_point())
        .map(|(i, _)| i);
    if let Some(last_undo_op_idx) = last_undo_op_idx {
        Ok(local_ops[last_undo_op_idx..].to_vec())
    } else {
        Ok(local_ops)
    }
}

/// Generate the operations that reverse the effect of `op`, given the current state of the task in
/// `txn`.
///
/// Returns `Ok(None)` if the reversal cannot be applied cleanly:
///  - A reversed Create is a Delete, which fails if the task does not exist.
///  - A reversed Delete is a Create, which fails if the task still exists.
///  - A reversed Update restores the previous `old_value`, and fails if the task's current value
///    for the property is not the `value` the original operation set.
async fn reverse_op(txn: &mut dyn StorageTxn, op: &Operation) -> Result<Option<Operations>> {
    Ok(match op {
        Operation::Create { uuid } => txn.get_task(*uuid).await?.map(|old_task| {
            vec![Operation::Delete {
                uuid: *uuid,
                old_task,
            }]
        }),
        Operation::Delete { uuid, old_task } => {
            if txn.get_task(*uuid).await?.is_some() {
                None
            } else {
                let mut ops = vec![Operation::Create { uuid: *uuid }];
                // The original update timestamps are not available, but that does not matter as
                // these operations simply restore the task's properties.
                let timestamp = Utc::now();
                for (property, value) in old_task {
                    ops.push(Operation::Update {
                        uuid: *uuid,
                        property: property.clone(),
                        old_value: None,
                        value: Some(value.clone()),
                        timestamp,
                    });
                }
                Some(ops)
            }
        }
        Operation::Update {
            uuid,
            property,
            old_value,
            value,
            timestamp,
        } => {
            let current = txn
                .get_task(*uuid)
                .await?
                .and_then(|mut task| task.remove(property));
            if &current != value {
                None
            } else {
                Some(vec![Operation::Update {
                    uuid: *uuid,
                    property: property.clone(),
                    old_value: value.clone(),
                    value: old_value.clone(),
                    timestamp: *timestamp,
                }])
            }
        }
        Operation::UndoPoint => Some(vec![]),
    })
}

/// Commit the reverse of the given operations, beginning with the last operation in the given
/// operations and proceeding to the first.
///
/// If the given operations are exactly the most recent un-synchronized local operations, they are
/// removed from the list of operations and their effect on the tasks is reversed. Otherwise, a
/// fresh reversed operation is generated from each given operation and committed, allowing undo of
/// operations that have already been synchronized or that have newer operations layered on top.
///
/// In either case the reversal must apply cleanly, and this method returns `false` without making
/// any change if it does not:
///  - A reversed Create is a Delete, which fails if the task does not exist.
///  - A reversed Delete is a Create, which fails if the task still exists.
///  - A reversed Update fails if the task's current value for the property differs from the value
///    the original operation set.
pub(crate) async fn commit_reversed_operations(
    txn: &mut dyn StorageTxn,
    undo_ops: Operations,
) -> Result<bool> {
    let local_ops = txn.unsynced_operations().await?;
    let mut undo_ops = undo_ops.to_vec();

    if undo_ops.is_empty() {
        return Ok(false);
    }

    // Determine whether the operations to undo are exactly the most recent local operations. If
    // so, they can simply be removed from the operation list; otherwise, the reversed operations
    // are recorded as new operations.
    let undo_is_local_tail = undo_ops.len() <= local_ops.len()
        && local_ops[local_ops.len() - undo_ops.len()..] == undo_ops[..];

    let mut applied = false;
    undo_ops.reverse();
    for op in undo_ops {
        debug!("Reversing operation {op:?}");
        let Some(rev_ops) = reverse_op(txn, &op).await? else {
            info!("Undo failed: reversed operation does not apply cleanly.");
            debug!("operation that could not be reversed: {op:?}");
            return Ok(false);
        };

        apply::apply_operations(txn, &rev_ops).await?;
        if !rev_ops.is_empty() {
            applied = true;
        }

        if undo_is_local_tail {
            txn.remove_operation(op).await?;
        } else {
            for rev_op in rev_ops {
                txn.add_operation(rev_op).await?;
            }
        }
    }

    txn.commit().await?;

    Ok(applied)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::inmemory::InMemoryStorage;
    use crate::storage::{taskmap_with, Storage, TaskMap};
    use crate::taskdb::TaskDb;
    use crate::{Operation, Operations};
    use chrono::Utc;
    use pretty_assertions::assert_eq;
    use uuid::Uuid;

    /// Commit `ops` to a fresh in-memory database and return it.
    async fn setup(ops: Operations) -> Result<TaskDb<InMemoryStorage>> {
        let mut db = TaskDb::new(InMemoryStorage::new());
        db.commit_operations(ops, |_| false).await?;
        Ok(db)
    }

    #[tokio::test]
    #[allow(clippy::vec_init_then_push)]
    async fn test_apply_create() -> Result<()> {
        let mut db = TaskDb::new(InMemoryStorage::new());
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let timestamp = Utc::now();

        let mut ops = Operations::new();
        // apply a few ops, capture the DB state, make an undo point, and then apply a few more
        // ops.
        ops.push(Operation::Create { uuid: uuid1 });
        ops.push(Operation::Update {
            uuid: uuid1,
            property: "prop".into(),
            value: Some("v1".into()),
            old_value: None,
            timestamp,
        });
        ops.push(Operation::Create { uuid: uuid2 });
        ops.push(Operation::Update {
            uuid: uuid2,
            property: "prop".into(),
            value: Some("v2".into()),
            old_value: None,
            timestamp,
        });
        ops.push(Operation::Update {
            uuid: uuid2,
            property: "prop2".into(),
            value: Some("v3".into()),
            old_value: Some("v2".into()),
            timestamp,
        });
        db.commit_operations(ops, |_| false).await?;

        let db_state = db.sorted_tasks().await;

        let mut ops = Operations::new();
        ops.push(Operation::UndoPoint);
        ops.push(Operation::Delete {
            uuid: uuid1,
            old_task: [("prop".to_string(), "v1".to_string())].into(),
        });
        ops.push(Operation::Update {
            uuid: uuid2,
            property: "prop".into(),
            value: None,
            old_value: Some("v2".into()),
            timestamp,
        });
        ops.push(Operation::Update {
            uuid: uuid2,
            property: "prop2".into(),
            value: Some("new-value".into()),
            old_value: Some("v3".into()),
            timestamp,
        });
        db.commit_operations(ops, |_| false).await?;

        assert_eq!(
            db.operations().await.len(),
            9,
            "{:#?}",
            db.operations().await
        );

        let undo_ops = get_undo_operations(db.storage.txn().await?.as_mut()).await?;
        assert_eq!(undo_ops.len(), 4, "{:#?}", undo_ops);
        assert_eq!(&undo_ops[..], &db.operations().await[5..]);

        assert!(commit_reversed_operations(db.storage.txn().await?.as_mut(), undo_ops).await?);

        // Note that we've subtracted the length of undo_ops.
        assert_eq!(
            db.operations().await.len(),
            5,
            "{:#?}",
            db.operations().await
        );
        assert_eq!(
            db.sorted_tasks().await,
            db_state,
            "{:#?}",
            db.sorted_tasks().await
        );

        // Note that the number of undo operations is equal to the number of operations in the
        // database here because there are no UndoPoints.
        let undo_ops = get_undo_operations(db.storage.txn().await?.as_mut()).await?;
        assert_eq!(undo_ops.len(), 5, "{:#?}", undo_ops);

        assert!(commit_reversed_operations(db.storage.txn().await?.as_mut(), undo_ops).await?);

        // empty db
        assert_eq!(
            db.operations().await.len(),
            0,
            "{:#?}",
            db.operations().await
        );
        assert_eq!(
            db.sorted_tasks().await,
            vec![],
            "{:#?}",
            db.sorted_tasks().await
        );

        let undo_ops = get_undo_operations(db.storage.txn().await?.as_mut()).await?;
        assert_eq!(undo_ops.len(), 0, "{:#?}", undo_ops);

        // nothing left to undo, so commit_undo_ops() returns false
        assert!(!commit_reversed_operations(db.storage.txn().await?.as_mut(), undo_ops).await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_reverse_create_present() -> Result<()> {
        // A reversed Create is a Delete that restores the task's current data.
        let uuid = Uuid::new_v4();
        let timestamp = Utc::now();
        let mut db = setup(vec![
            Operation::Create { uuid },
            Operation::Update {
                uuid,
                property: "prop1".into(),
                old_value: None,
                value: Some("v1".into()),
                timestamp,
            },
        ])
        .await?;
        let mut txn = db.storage.txn().await?;
        let rev = reverse_op(txn.as_mut(), &Operation::Create { uuid }).await?;
        assert_eq!(
            rev,
            Some(vec![Operation::Delete {
                uuid,
                old_task: taskmap_with(vec![("prop1".into(), "v1".into())]),
            }])
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_reverse_create_absent() -> Result<()> {
        // A reversed Create fails cleanly when the task does not exist.
        let uuid = Uuid::new_v4();
        let mut db = setup(vec![]).await?;
        let mut txn = db.storage.txn().await?;
        assert_eq!(
            reverse_op(txn.as_mut(), &Operation::Create { uuid }).await?,
            None
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_reverse_delete_absent() -> Result<()> {
        // A reversed Delete recreates the task and restores its data.
        let uuid = Uuid::new_v4();
        let mut db = setup(vec![]).await?;
        let mut txn = db.storage.txn().await?;
        let rev = reverse_op(
            txn.as_mut(),
            &Operation::Delete {
                uuid,
                old_task: taskmap_with(vec![("prop1".into(), "v1".into())]),
            },
        )
        .await?
        .unwrap();
        assert_eq!(rev.len(), 2);
        assert_eq!(rev[0], Operation::Create { uuid });
        assert!(matches!(
            &rev[1],
            Operation::Update { uuid: u, property: p, value: Some(v), .. }
                if u == &uuid && p == "prop1" && v == "v1"
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_reverse_delete_present() -> Result<()> {
        // A reversed Delete fails cleanly when the task still exists.
        let uuid = Uuid::new_v4();
        let mut db = setup(vec![Operation::Create { uuid }]).await?;
        let mut txn = db.storage.txn().await?;
        assert_eq!(
            reverse_op(
                txn.as_mut(),
                &Operation::Delete {
                    uuid,
                    old_task: TaskMap::new(),
                }
            )
            .await?,
            None
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_reverse_update_match() -> Result<()> {
        // A reversed Update restores the previous value when the current value matches the value
        // the original operation set.
        let uuid = Uuid::new_v4();
        let timestamp = Utc::now();
        let mut db = setup(vec![
            Operation::Create { uuid },
            Operation::Update {
                uuid,
                property: "prop".into(),
                old_value: None,
                value: Some("v2".into()),
                timestamp,
            },
        ])
        .await?;
        let mut txn = db.storage.txn().await?;
        let rev = reverse_op(
            txn.as_mut(),
            &Operation::Update {
                uuid,
                property: "prop".into(),
                old_value: Some("v1".into()),
                value: Some("v2".into()),
                timestamp,
            },
        )
        .await?;
        assert_eq!(
            rev,
            Some(vec![Operation::Update {
                uuid,
                property: "prop".into(),
                old_value: Some("v2".into()),
                value: Some("v1".into()),
                timestamp,
            }])
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_reverse_update_mismatch() -> Result<()> {
        // A reversed Update fails cleanly when the current value differs from the value the
        // original operation set.
        let uuid = Uuid::new_v4();
        let timestamp = Utc::now();
        let mut db = setup(vec![
            Operation::Create { uuid },
            Operation::Update {
                uuid,
                property: "prop".into(),
                old_value: None,
                value: Some("other".into()),
                timestamp,
            },
        ])
        .await?;
        let mut txn = db.storage.txn().await?;
        let rev = reverse_op(
            txn.as_mut(),
            &Operation::Update {
                uuid,
                property: "prop".into(),
                old_value: Some("v1".into()),
                value: Some("v2".into()),
                timestamp,
            },
        )
        .await?;
        assert_eq!(rev, None);
        Ok(())
    }

    #[tokio::test]
    async fn test_reverse_undo_point() -> Result<()> {
        let mut db = setup(vec![]).await?;
        let mut txn = db.storage.txn().await?;
        assert_eq!(
            reverse_op(txn.as_mut(), &Operation::UndoPoint).await?,
            Some(vec![])
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_reversed_fails_cleanly() -> Result<()> {
        // When a reversal cannot be applied cleanly, the commit fails and makes no change.
        let uuid = Uuid::new_v4();
        let mut db = setup(vec![Operation::Create { uuid }]).await?;
        let before = db.sorted_tasks().await;

        // Reversing a Delete for a task that still exists fails.
        assert!(
            !commit_reversed_operations(
                db.storage.txn().await?.as_mut(),
                vec![Operation::Delete {
                    uuid,
                    old_task: TaskMap::new(),
                }],
            )
            .await?
        );

        // Reversing a Create for a task that does not exist fails.
        assert!(
            !commit_reversed_operations(
                db.storage.txn().await?.as_mut(),
                vec![Operation::Create {
                    uuid: Uuid::new_v4(),
                }],
            )
            .await?
        );

        assert_eq!(db.sorted_tasks().await, before);
        assert_eq!(db.operations().await.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_commit_reversed_after_sync_adds_operations() -> Result<()> {
        // When the operations to undo are no longer the most recent local operations (for example
        // because they have been synced), their reversal is committed as new operations rather
        // than by removing them.
        let uuid = Uuid::new_v4();
        let timestamp = Utc::now();
        let create = Operation::Create { uuid };
        let update = Operation::Update {
            uuid,
            property: "prop".into(),
            old_value: None,
            value: Some("v1".into()),
            timestamp,
        };
        let mut db = setup(vec![create.clone(), update.clone()]).await?;

        // Mark the operations as synced, so they are no longer the local tail.
        {
            let mut txn = db.storage.txn().await?;
            txn.sync_complete().await?;
            txn.commit().await?;
        }
        assert_eq!(db.operations().await.len(), 0);

        // Undo both operations; the reversal is recorded as new operations.
        assert!(
            commit_reversed_operations(db.storage.txn().await?.as_mut(), vec![create, update])
                .await?
        );

        assert_eq!(db.sorted_tasks().await, vec![]);
        let new_ops = db.operations().await;
        assert_eq!(new_ops.len(), 2, "{new_ops:#?}");
        Ok(())
    }
}
