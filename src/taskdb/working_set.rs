use crate::errors::Result;
use crate::storage::{StorageTxn, TaskMap};
use std::collections::HashSet;

/// Rebuild the working set using a function to identify tasks that should be in the set.  This
/// renumbers the existing working-set tasks to eliminate gaps, and also adds any tasks that
/// are not already in the working set but should be.  The rebuild occurs in a single
/// trasnsaction against the storage backend.
pub(crate) fn rebuild<F>(txn: &mut dyn StorageTxn, in_working_set: F, renumber: bool) -> Result<()>
where
    F: Fn(&TaskMap) -> bool,
{
    let old_ws = txn.get_working_set()?;
    let mut new_ws = vec![None]; // index 0 is always None
    let mut seen = HashSet::new();

    // The goal here is for existing working-set items to be "compressed' down to index 1, so
    // we begin by scanning the current working set and inserting any tasks that should still
    // be in the set into new_ws, implicitly dropping any tasks that are no longer in the
    // working set.
    for elt in &old_ws[1..] {
        if let Some(uuid) = elt {
            if let Some(task) = txn.get_task(*uuid)? {
                if in_working_set(&task) {
                    // The existing working-set item is still in the working set -- no change.
                    new_ws.push(Some(*uuid));
                    seen.insert(*uuid);
                } else {
                    // The item should not be present. If we are not renumbering, then insert a
                    // blank working-set item here
                    if !renumber {
                        new_ws.push(None);
                    }
                }
                continue;
            }
        } else {
            // This item was already None.
            new_ws.push(None);
        }
    }

    // Now go hunting for tasks that should be in this list but are not, adding them at the
    // end of the list, whether renumbering or not
    for (uuid, task) in txn.all_tasks()? {
        if !seen.contains(&uuid) && in_working_set(&task) {
            new_ws.push(Some(uuid));
        }
    }

    // Now use `set_working_set_item` to update any items within the range of the current
    // working set.
    for (i, (old, new)) in old_ws.iter().zip(new_ws.iter()).enumerate() {
        if old != new {
            txn.set_working_set_item(i, *new)?;
        }
    }

    // If there are more new items, add them.
    match new_ws.len().cmp(&old_ws.len()) {
        std::cmp::Ordering::Less => {
            // Overall working set has shrunk, so set remaining items to None.
            for (i, item) in old_ws.iter().enumerate().skip(new_ws.len()) {
                if item.is_some() {
                    txn.set_working_set_item(i, None)?;
                }
            }
        }
        std::cmp::Ordering::Equal => {}
        std::cmp::Ordering::Greater => {
            // Overall working set has grown, so add new items to the end.
            for uuid in &new_ws[old_ws.len()..] {
                txn.add_to_working_set(uuid.expect("new ws items should not be None"))?;
            }
        }
    }

    txn.commit()?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::taskdb::TaskDb;
    use crate::{Operation, Operations};
    use chrono::Utc;
    use uuid::Uuid;

    #[test]
    fn rebuild_working_set_renumber() -> Result<()> {
        rebuild_working_set(true)
    }

    #[test]
    fn rebuild_working_set_no_renumber() -> Result<()> {
        rebuild_working_set(false)
    }

    fn rebuild_working_set(renumber: bool) -> Result<()> {
        let mut db = TaskDb::new_inmemory();
        let mut uuids = vec![];
        uuids.push(Uuid::new_v4());
        println!("uuids[0]: {:?} - pending, not in working set", uuids[0]);
        uuids.push(Uuid::new_v4());
        println!("uuids[1]: {:?} - pending, in working set", uuids[1]);
        uuids.push(Uuid::new_v4());
        println!("uuids[2]: {:?} - not pending, not in working set", uuids[2]);
        uuids.push(Uuid::new_v4());
        println!("uuids[3]: {:?} - not pending, in working set", uuids[3]);
        uuids.push(Uuid::new_v4());
        println!("uuids[4]: {:?} - pending, in working set", uuids[4]);

        // add everything to the TaskDb
        let mut ops = Operations::new();
        for uuid in &uuids {
            ops.push(Operation::Create { uuid: *uuid });
        }
        for i in &[0usize, 1, 4] {
            ops.push(Operation::Update {
                uuid: uuids[*i],
                property: String::from("status"),
                value: Some("pending".into()),
                old_value: None,
                timestamp: Utc::now(),
            });
        }
        db.commit_operations(ops, |_| false)?;

        // set the existing working_set as we want it
        {
            let mut txn = db.storage.txn()?;
            txn.clear_working_set()?;

            for i in &[1usize, 3, 4] {
                txn.add_to_working_set(uuids[*i])?;
            }

            txn.commit()?;
        }

        assert_eq!(
            db.working_set()?,
            vec![None, Some(uuids[1]), Some(uuids[3]), Some(uuids[4])]
        );

        rebuild(
            db.storage.txn()?.as_mut(),
            |t| {
                if let Some(status) = t.get("status") {
                    status == "pending"
                } else {
                    false
                }
            },
            renumber,
        )?;

        let exp = if renumber {
            // uuids[1] and uuids[4] are already in the working set, so are compressed
            // to the top, and then uuids[0] is added.
            vec![None, Some(uuids[1]), Some(uuids[4]), Some(uuids[0])]
        } else {
            // uuids[1] and uuids[4] are already in the working set, at indexes 1 and 3,
            // and then uuids[0] is added.
            vec![None, Some(uuids[1]), None, Some(uuids[4]), Some(uuids[0])]
        };

        assert_eq!(db.working_set()?, exp);

        Ok(())
    }

    #[test]
    fn rebuild_working_set_no_change() -> Result<()> {
        let mut db = TaskDb::new_inmemory();

        let mut uuids = vec![];
        uuids.push(Uuid::new_v4());
        println!("uuids[0]: {:?} - pending, in working set", uuids[0]);
        uuids.push(Uuid::new_v4());
        println!("uuids[1]: {:?} - pending, in working set", uuids[1]);
        uuids.push(Uuid::new_v4());
        println!("uuids[2]: {:?} - pending, not in working set", uuids[2]);

        // add everything to the TaskDb
        let mut ops = Operations::new();
        for uuid in &uuids {
            ops.push(Operation::Create { uuid: *uuid });
            ops.push(Operation::Update {
                uuid: *uuid,
                property: String::from("status"),
                value: Some("pending".into()),
                old_value: None,
                timestamp: Utc::now(),
            });
        }
        db.commit_operations(ops, |_| false)?;

        // set the existing working_set as we want it, containing UUIDs 0 and 1.
        {
            let mut txn = db.storage.txn()?;
            txn.clear_working_set()?;

            for i in &[0, 1] {
                txn.add_to_working_set(uuids[*i])?;
            }

            txn.commit()?;
        }
        rebuild(
            db.storage.txn()?.as_mut(),
            |t| {
                if let Some(status) = t.get("status") {
                    status == "pending"
                } else {
                    false
                }
            },
            true,
        )?;

        assert_eq!(
            db.working_set()?,
            vec![None, Some(uuids[0]), Some(uuids[1]), Some(uuids[2])]
        );
        Ok(())
    }

    #[test]
    fn rebuild_working_set_shrinks() -> Result<()> {
        let mut db = TaskDb::new_inmemory();

        let mut uuids = vec![];
        uuids.push(Uuid::new_v4());
        println!("uuids[0]: {:?} - pending, in working set", uuids[0]);
        uuids.push(Uuid::new_v4());
        println!("uuids[1]: {:?} - not pending, in working set", uuids[1]);
        uuids.push(Uuid::new_v4());
        println!("uuids[2]: {:?} - not pending, in working set", uuids[2]);

        // add everything to the TaskDb
        let mut ops = Operations::new();
        for uuid in &uuids {
            ops.push(Operation::Create { uuid: *uuid });
        }
        ops.push(Operation::Update {
            uuid: uuids[0],
            property: String::from("status"),
            value: Some("pending".into()),
            old_value: None,
            timestamp: Utc::now(),
        });
        db.commit_operations(ops, |_| false)?;

        // set the existing working_set as we want it, containing all three UUIDs.
        {
            let mut txn = db.storage.txn()?;
            txn.clear_working_set()?;

            for uuid in &uuids {
                txn.add_to_working_set(*uuid)?;
            }

            txn.commit()?;
        }
        rebuild(
            db.storage.txn()?.as_mut(),
            |t| {
                if let Some(status) = t.get("status") {
                    status == "pending"
                } else {
                    false
                }
            },
            true,
        )?;

        assert_eq!(db.working_set()?, vec![None, Some(uuids[0])]);
        Ok(())
    }
}
