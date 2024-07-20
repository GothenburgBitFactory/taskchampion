use std::collections::HashSet;

use crate::errors::Result;
use crate::operation::Operation;
use crate::server::{Server, SyncOp};
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
pub(crate) struct TaskDb {
    storage: Box<dyn Storage>,
}

impl TaskDb {
    /// Create a new TaskDb with the given backend storage
    pub(crate) fn new(storage: Box<dyn Storage>) -> TaskDb {
        TaskDb { storage }
    }

    #[cfg(test)]
    pub(crate) fn new_inmemory() -> TaskDb {
        #[cfg(test)]
        use crate::storage::InMemoryStorage;

        TaskDb::new(Box::new(InMemoryStorage::new()))
    }

    /// Apply `operations` to the database in a single transaction.
    ///
    /// The operations will be appended to the list of local operations, and the set of tasks will
    /// be updated accordingly.
    ///
    /// Any operations for which `add_to_working_set` returns true will cause the relevant
    /// task to be added to the working set.
    pub(crate) fn commit_operations<F>(
        &mut self,
        operations: Operations,
        add_to_working_set: F,
    ) -> Result<()>
    where
        F: Fn(&Operation) -> bool,
    {
        let mut txn = self.storage.txn()?;
        apply::apply_operations(txn.as_mut(), &operations)?;

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

    /// Apply an operation to the TaskDb.  This will update the set of tasks and add a Operation to
    /// the set of operations in the TaskDb, and return the TaskMap containing the resulting task's
    /// properties (or an empty TaskMap for deletion).
    ///
    /// Aside from synchronization operations, this is the only way to modify the TaskDb.  In cases
    /// where an operation does not make sense, this function will do nothing and return an error
    /// (but leave the TaskDb in a consistent state).
    pub(crate) fn apply(&mut self, op: SyncOp) -> Result<TaskMap> {
        let mut txn = self.storage.txn()?;
        apply::apply_and_record(txn.as_mut(), op)
    }

    /// Add an UndoPoint operation to the list of replica operations.
    pub(crate) fn add_undo_point(&mut self) -> Result<()> {
        let mut txn = self.storage.txn()?;
        txn.add_operation(Operation::UndoPoint)?;
        txn.commit()
    }

    /// Get all tasks.
    pub(crate) fn all_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        let mut txn = self.storage.txn()?;
        txn.all_tasks()
    }

    /// Get the UUIDs of all tasks
    pub(crate) fn all_task_uuids(&mut self) -> Result<Vec<Uuid>> {
        let mut txn = self.storage.txn()?;
        txn.all_task_uuids()
    }

    /// Get the working set
    pub(crate) fn working_set(&mut self) -> Result<Vec<Option<Uuid>>> {
        let mut txn = self.storage.txn()?;
        txn.get_working_set()
    }

    /// Get a single task, by uuid.
    pub(crate) fn get_task(&mut self, uuid: Uuid) -> Result<Option<TaskMap>> {
        let mut txn = self.storage.txn()?;
        txn.get_task(uuid)
    }

    /// Rebuild the working set using a function to identify tasks that should be in the set.  This
    /// renumbers the existing working-set tasks to eliminate gaps, and also adds any tasks that
    /// are not already in the working set but should be.  The rebuild occurs in a single
    /// trasnsaction against the storage backend.
    pub(crate) fn rebuild_working_set<F>(&mut self, in_working_set: F, renumber: bool) -> Result<()>
    where
        F: Fn(&TaskMap) -> bool,
    {
        working_set::rebuild(self.storage.txn()?.as_mut(), in_working_set, renumber)
    }

    /// Add the given uuid to the working set and return its index; if it is already in the working
    /// set, its index is returned.  This does *not* renumber any existing tasks.
    pub(crate) fn add_to_working_set(&mut self, uuid: Uuid) -> Result<usize> {
        let mut txn = self.storage.txn()?;
        // search for an existing entry for this task..
        for (i, elt) in txn.get_working_set()?.iter().enumerate() {
            if *elt == Some(uuid) {
                // (note that this drops the transaction with no changes made)
                return Ok(i);
            }
        }
        // and if not found, add one
        let i = txn.add_to_working_set(uuid)?;
        txn.commit()?;
        Ok(i)
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
        &mut self,
        server: &mut Box<dyn Server>,
        avoid_snapshots: bool,
    ) -> Result<()> {
        let mut txn = self.storage.txn()?;
        sync::sync(server, txn.as_mut(), avoid_snapshots)
    }

    /// Return undo local operations until the most recent UndoPoint, returning an empty Vec if there are no
    /// local operations to undo.
    pub(crate) fn get_undo_ops(&mut self) -> Result<Vec<Operation>> {
        let mut txn = self.storage.txn()?;
        undo::get_undo_ops(txn.as_mut())
    }

    /// Undo local operations in storage, returning a boolean indicating success.
    pub(crate) fn commit_undo_ops(&mut self, undo_ops: Vec<Operation>) -> Result<bool> {
        let mut txn = self.storage.txn()?;
        undo::commit_undo_ops(txn.as_mut(), undo_ops)
    }

    /// Get the number of un-synchronized operations in storage, excluding undo
    /// operations.
    pub(crate) fn num_operations(&mut self) -> Result<usize> {
        let mut txn = self.storage.txn().unwrap();
        Ok(txn
            .operations()?
            .iter()
            .filter(|o| !o.is_undo_point())
            .count())
    }

    /// Get the number of (un-synchronized) undo points in storage.
    pub(crate) fn num_undo_points(&mut self) -> Result<usize> {
        let mut txn = self.storage.txn().unwrap();
        Ok(txn
            .operations()?
            .iter()
            .filter(|o| o.is_undo_point())
            .count())
    }

    // functions for supporting tests

    #[cfg(test)]
    pub(crate) fn sorted_tasks(&mut self) -> Vec<(Uuid, Vec<(String, String)>)> {
        let mut res: Vec<(Uuid, Vec<(String, String)>)> = self
            .all_tasks()
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
    pub(crate) fn operations(&mut self) -> Vec<Operation> {
        let mut txn = self.storage.txn().unwrap();
        txn.operations().unwrap().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::test::TestServer;
    use crate::storage::InMemoryStorage;
    use chrono::Utc;
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;
    use uuid::Uuid;

    #[test]
    fn commit_operations() -> Result<()> {
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
            old_value: Some("old".into()),
        });

        db.commit_operations(ops, |_| false)?;

        assert_eq!(
            db.sorted_tasks(),
            vec![(uuid, vec![("title".into(), "my task".into())])]
        );
        assert_eq!(
            db.operations(),
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

    #[test]
    fn commit_operations_update_working_set() -> Result<()> {
        let mut db = TaskDb::new_inmemory();
        let mut uuids = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        uuids.sort();
        let [uuid1, uuid2, uuid3] = uuids;

        // uuid1 already exists in the working set.
        db.add_to_working_set(uuid1)?;

        let mut ops = Operations::new();
        ops.add(Operation::Create { uuid: uuid1 });
        ops.add(Operation::Create { uuid: uuid2 });
        ops.add(Operation::Create { uuid: uuid3 });
        ops.add(Operation::Create { uuid: uuid2 });
        ops.add(Operation::Create { uuid: uuid3 });

        // return true for updates to uuid1 or uuid2.
        let add_to_working_set = |op: &Operation| match op {
            Operation::Create { uuid } => *uuid == uuid1 || *uuid == uuid2,
            _ => false,
        };
        db.commit_operations(ops, add_to_working_set)?;

        assert_eq!(
            db.sorted_tasks(),
            vec![(uuid1, vec![]), (uuid2, vec![]), (uuid3, vec![]),]
        );
        assert_eq!(
            db.operations(),
            vec![
                Operation::Create { uuid: uuid1 },
                Operation::Create { uuid: uuid2 },
                Operation::Create { uuid: uuid3 },
                Operation::Create { uuid: uuid2 },
                Operation::Create { uuid: uuid3 },
            ]
        );

        // uuid2 was added to the working set, once, and uuid3 was not.
        assert_eq!(db.working_set()?, vec![None, Some(uuid1), Some(uuid2)],);
        Ok(())
    }

    #[test]
    fn test_apply() {
        // this verifies that the operation is both applied and included in the list of
        // operations; more detailed tests are in the `apply` module.
        let mut db = TaskDb::new_inmemory();
        let uuid = Uuid::new_v4();
        let op = SyncOp::Create { uuid };
        db.apply(op).unwrap();

        assert_eq!(db.sorted_tasks(), vec![(uuid, vec![]),]);
        assert_eq!(db.operations(), vec![Operation::Create { uuid }]);
    }

    #[test]
    fn test_add_undo_point() {
        let mut db = TaskDb::new_inmemory();
        db.add_undo_point().unwrap();
        assert_eq!(db.operations(), vec![Operation::UndoPoint]);
    }

    #[test]
    fn test_num_operations() {
        let mut db = TaskDb::new_inmemory();
        db.apply(SyncOp::Create {
            uuid: Uuid::new_v4(),
        })
        .unwrap();
        db.add_undo_point().unwrap();
        db.apply(SyncOp::Create {
            uuid: Uuid::new_v4(),
        })
        .unwrap();
        assert_eq!(db.num_operations().unwrap(), 2);
    }

    #[test]
    fn test_num_undo_points() {
        let mut db = TaskDb::new_inmemory();
        db.add_undo_point().unwrap();
        assert_eq!(db.num_undo_points().unwrap(), 1);
        db.add_undo_point().unwrap();
        assert_eq!(db.num_undo_points().unwrap(), 2);
    }

    fn newdb() -> TaskDb {
        TaskDb::new(Box::new(InMemoryStorage::new()))
    }

    #[derive(Debug)]
    enum Action {
        Op(SyncOp),
        Sync,
    }

    fn action_sequence_strategy() -> impl Strategy<Value = Vec<(Action, u8)>> {
        // Create, Update, Delete, or Sync on client 1, 2, .., followed by a round of syncs
        "([CUDS][123])*S1S2S3S1S2".prop_map(|seq| {
            let uuid = Uuid::parse_str("83a2f9ef-f455-4195-b92e-a54c161eebfc").unwrap();
            seq.as_bytes()
                .chunks(2)
                .map(|action_on| {
                    let action = match action_on[0] {
                        b'C' => Action::Op(SyncOp::Create { uuid }),
                        b'U' => Action::Op(SyncOp::Update {
                            uuid,
                            property: "title".into(),
                            value: Some("foo".into()),
                            timestamp: Utc::now(),
                        }),
                        b'D' => Action::Op(SyncOp::Delete { uuid }),
                        b'S' => Action::Sync,
                        _ => unreachable!(),
                    };
                    let acton = action_on[1] - b'1';
                    (action, acton)
                })
                .collect::<Vec<(Action, u8)>>()
        })
    }

    proptest! {
        #[test]
        // check that various sequences of operations on mulitple db's do not get the db's into an
        // incompatible state.  The main concern here is that there might be a sequence of create
        // and delete operations that results in a task existing in one TaskDb but not existing in
        // another.  So, the generated sequences focus on a single task UUID.
        fn transform_sequences_of_operations(action_sequence in action_sequence_strategy()) {
        let mut server: Box<dyn Server> = Box::new(TestServer::new());
            let mut dbs = [newdb(), newdb(), newdb()];

            for (action, db) in action_sequence {
                println!("{:?} on db {}", action, db);

                let db = &mut dbs[db as usize];
                match action {
                    Action::Op(op) => {
                        if let Err(e) = db.apply(op) {
                            println!("  {:?} (ignored)", e);
                        }
                    },
                    Action::Sync => db.sync(&mut server, false).unwrap(),
                }
            }

            assert_eq!(dbs[0].sorted_tasks(), dbs[0].sorted_tasks());
            assert_eq!(dbs[1].sorted_tasks(), dbs[2].sorted_tasks());
        }
    }
}
