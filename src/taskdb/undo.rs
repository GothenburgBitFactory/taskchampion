use super::apply;
use crate::errors::Result;
use crate::operation::{Operation, Operations};
use crate::server::SyncOp;
use crate::storage::StorageTxn;
use chrono::Utc;
use log::{debug, info, trace};

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

/// Generate a sequence of SyncOp's to reverse the effects of this Operation.
fn reverse_ops(op: Operation) -> Vec<SyncOp> {
    match op {
        Operation::Create { uuid } => vec![SyncOp::Delete { uuid }],
        Operation::Delete { uuid, mut old_task } => {
            let mut ops = vec![SyncOp::Create { uuid }];
            // We don't have the original update timestamp, but it doesn't
            // matter because this SyncOp will just be applied and discarded.
            let timestamp = Utc::now();
            for (property, value) in old_task.drain() {
                ops.push(SyncOp::Update {
                    uuid,
                    property,
                    value: Some(value),
                    timestamp,
                });
            }
            ops
        }
        Operation::Update {
            uuid,
            property,
            old_value,
            timestamp,
            ..
        } => vec![SyncOp::Update {
            uuid,
            property,
            value: old_value,
            timestamp,
        }],
        Operation::UndoPoint => vec![],
    }
}

/// Commit the reverse of the given operations, beginning with the last operation in the given
/// operations and proceeding to the first.
///
/// This method only supports reversing operations if they precisely match local operations that
/// have not yet been synchronized, and will return `false` if this is not the case.
pub(crate) async fn commit_reversed_operations(
    txn: &mut dyn StorageTxn,
    undo_ops: Operations,
) -> Result<bool> {
    let mut applied = false;
    let local_ops = txn.unsynced_operations().await?;
    let mut undo_ops = undo_ops.to_vec();

    if undo_ops.is_empty() {
        return Ok(false);
    }

    // TODO Support concurrent undo by adding the reverse of undo_ops rather than popping from operations.

    // Verify that undo_ops are the most recent local ops.
    let mut ok = false;
    let local_undo_ops;
    if undo_ops.len() <= local_ops.len() {
        let new_len = local_ops.len() - undo_ops.len();
        local_undo_ops = &local_ops[new_len..];
        if local_undo_ops == undo_ops {
            ok = true;
        }
    }
    if !ok {
        info!("Undo failed: concurrent changes to the database occurred.");
        debug!("local_ops={:#?}\nundo_ops={:#?}", local_ops, undo_ops);
        return Ok(applied);
    }

    undo_ops.reverse();
    for op in undo_ops {
        debug!("Reversing operation {:?}", op);
        let rev_ops = reverse_ops(op.clone());
        for op in rev_ops {
            trace!("Applying reversed operation {:?}", op);
            apply::apply_op(txn, &op).await?;
            applied = true;
        }
        txn.remove_operation(op).await?;
    }

    txn.commit().await?;

    Ok(applied)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::inmemory::InMemoryStorage;
    use crate::storage::Storage;
    use crate::{storage::taskmap_with, taskdb::TaskDb};
    use crate::{Operation, Operations};
    use chrono::Utc;
    use pretty_assertions::assert_eq;
    use uuid::Uuid;

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

        // Try committing the wrong set of ops.
        assert!(!commit_reversed_operations(
            db.storage.txn().await?.as_mut(),
            undo_ops[1..=2].to_vec(),
        )
        .await?);

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

    #[test]
    fn test_reverse_create() {
        let uuid = Uuid::new_v4();
        assert_eq!(
            reverse_ops(Operation::Create { uuid }),
            vec![SyncOp::Delete { uuid }]
        );
    }

    #[test]
    fn test_reverse_delete() {
        let uuid = Uuid::new_v4();
        let reversed = reverse_ops(Operation::Delete {
            uuid,
            old_task: taskmap_with(vec![("prop1".into(), "v1".into())]),
        });
        assert_eq!(reversed.len(), 2);
        assert_eq!(reversed[0], SyncOp::Create { uuid });
        assert!(matches!(
            &reversed[1],
            SyncOp::Update { uuid: u, property: p, value: Some(v), ..}
                if u == &uuid && p == "prop1" && v == "v1"
        ));
    }

    #[test]
    fn test_reverse_update() {
        let uuid = Uuid::new_v4();
        let timestamp = Utc::now();
        assert_eq!(
            reverse_ops(Operation::Update {
                uuid,
                property: "prop".into(),
                old_value: Some("foo".into()),
                value: Some("v".into()),
                timestamp,
            }),
            vec![SyncOp::Update {
                uuid,
                property: "prop".into(),
                value: Some("foo".into()),
                timestamp,
            }]
        );
    }

    #[test]
    fn test_reverse_undo_point() {
        assert_eq!(reverse_ops(Operation::UndoPoint), vec![]);
    }
}
