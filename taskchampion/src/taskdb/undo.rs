use super::apply;
use crate::errors::Result;
use crate::operation::Operation;
use crate::server::SyncOp;
use crate::storage::StorageTxn;
use chrono::Utc;
use log::{debug, info, trace};

/// Local operations until the most recent UndoPoint.
pub fn get_undo_ops(txn: &mut dyn StorageTxn) -> Result<Vec<Operation>> {
    let mut local_ops = txn.operations().unwrap();
    let mut undo_ops: Vec<Operation> = Vec::new();

    while let Some(op) = local_ops.pop() {
        if op == Operation::UndoPoint {
            break;
        }
        undo_ops.push(op);
    }

    Ok(undo_ops)
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

/// Commit operations to storage, returning a boolean indicating success.
pub fn commit_undo_ops(txn: &mut dyn StorageTxn, mut undo_ops: Vec<Operation>) -> Result<bool> {
    let mut applied = false;
    let mut local_ops = txn.operations().unwrap();

    // Add UndoPoint to undo_ops unless this undo will empty the operations database, in which case
    // there is no UndoPoint.
    if local_ops.len() > undo_ops.len() {
        undo_ops.push(Operation::UndoPoint);
    }

    // Drop undo_ops iff they're the latest operations.
    // TODO Support concurrent undo by adding the reverse of undo_ops rather than popping from operations.
    let old_len = local_ops.len();
    let undo_len = undo_ops.len();
    let new_len = old_len - undo_len;
    let local_undo_ops = &local_ops[new_len..old_len];
    undo_ops.reverse();
    if local_undo_ops != undo_ops {
        info!("Undo failed: concurrent changes to the database occurred.");
        debug!(
            "local_undo_ops={:#?}\nundo_ops={:#?}",
            local_undo_ops, undo_ops
        );
        return Ok(applied);
    }
    undo_ops.reverse();
    local_ops.truncate(new_len);

    for op in undo_ops {
        debug!("Reversing operation {:?}", op);
        let rev_ops = reverse_ops(op);
        for op in rev_ops {
            trace!("Applying reversed operation {:?}", op);
            apply::apply_op(txn, &op)?;
            applied = true;
        }
    }

    if undo_len != 0 {
        txn.set_operations(local_ops)?;
        txn.commit()?;
    }

    Ok(applied)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{storage::taskmap_with, taskdb::TaskDb};
    use crate::{Operation, Operations};
    use chrono::Utc;
    use pretty_assertions::assert_eq;
    use uuid::Uuid;

    #[test]
    fn test_apply_create() -> Result<()> {
        let mut db = TaskDb::new_inmemory();
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let timestamp = Utc::now();

        let mut ops = Operations::new();
        // apply a few ops, capture the DB state, make an undo point, and then apply a few more
        // ops.
        ops.add(Operation::Create { uuid: uuid1 });
        ops.add(Operation::Update {
            uuid: uuid1,
            property: "prop".into(),
            value: Some("v1".into()),
            old_value: None,
            timestamp,
        });
        ops.add(Operation::Create { uuid: uuid2 });
        ops.add(Operation::Update {
            uuid: uuid2,
            property: "prop".into(),
            value: Some("v2".into()),
            old_value: None,
            timestamp,
        });
        ops.add(Operation::Update {
            uuid: uuid2,
            property: "prop2".into(),
            value: Some("v3".into()),
            old_value: Some("v2".into()),
            timestamp,
        });
        db.commit_operations(ops, |_| false)?;

        let db_state = db.sorted_tasks();

        let mut ops = Operations::new_with_undo_point();
        ops.add(Operation::Delete {
            uuid: uuid1,
            old_task: [("prop".to_string(), "v1".to_string())].into(),
        });
        ops.add(Operation::Update {
            uuid: uuid2,
            property: "prop".into(),
            value: None,
            old_value: Some("v2".into()),
            timestamp,
        });
        ops.add(Operation::Update {
            uuid: uuid2,
            property: "prop2".into(),
            value: Some("new-value".into()),
            old_value: Some("v3".into()),
            timestamp,
        });
        db.commit_operations(ops, |_| false)?;

        assert_eq!(db.operations().len(), 9, "{:#?}", db.operations());

        let undo_ops = get_undo_ops(db.storage.txn()?.as_mut())?;
        assert_eq!(undo_ops.len(), 3, "{:#?}", undo_ops);

        assert!(commit_undo_ops(db.storage.txn()?.as_mut(), undo_ops)?);

        // Note that we've subtracted the length of undo_ops plus one for the UndoPoint.
        assert_eq!(db.operations().len(), 5, "{:#?}", db.operations());
        assert_eq!(db.sorted_tasks(), db_state, "{:#?}", db.sorted_tasks());

        // Note that the number of undo operations is equal to the number of operations in the
        // database here because there are no UndoPoints.
        let undo_ops = get_undo_ops(db.storage.txn()?.as_mut())?;
        assert_eq!(undo_ops.len(), 5, "{:#?}", undo_ops);

        assert!(commit_undo_ops(db.storage.txn()?.as_mut(), undo_ops)?);

        // empty db
        assert_eq!(db.operations().len(), 0, "{:#?}", db.operations());
        assert_eq!(db.sorted_tasks(), vec![], "{:#?}", db.sorted_tasks());

        let undo_ops = get_undo_ops(db.storage.txn()?.as_mut())?;
        assert_eq!(undo_ops.len(), 0, "{:#?}", undo_ops);

        // nothing left to undo, so commit_undo_ops() returns false
        assert!(!commit_undo_ops(db.storage.txn()?.as_mut(), undo_ops)?);

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
