use crate::depmap::DependencyMap;
use crate::errors::Result;
use crate::operation::{Operation, Operations};
use crate::server::Server;
use crate::storage::{Storage, TaskMap};
use crate::task::{Status, Task};
use crate::taskdb::TaskDb;
use crate::workingset::WorkingSet;
use crate::{TaskData, Error};
use anyhow::Context;
use chrono::{Duration, Utc};
use log::trace;
use std::collections::HashMap;
use std::rc::Rc;
use uuid::Uuid;

/// A replica represents an instance of a user's task data, providing an easy interface
/// for querying and modifying that data.
///
/// ## Tasks
///
/// Tasks are uniquely identified by UUIDs.
/// Most task modifications are performed via the [`Task`](crate::Task) and
/// [`TaskMut`](crate::TaskMut) types.  Use of two types for tasks allows easy
/// read-only manipulation of lots of tasks, with exclusive access required only
/// for modifications.
///
/// ## Working Set
///
/// A replica maintains a "working set" of tasks that are of current concern to the user,
/// specifically pending tasks.  These are indexed with small, easy-to-type integers.  Newly
/// pending tasks are automatically added to the working set, and the working set can be
/// "renumbered" when necessary.
pub struct Replica {
    taskdb: TaskDb,

    /// If true, this replica has already added an undo point.
    added_undo_point: bool,

    /// The dependency map for this replica, if it has been calculated.
    depmap: Option<Rc<DependencyMap>>,
}

impl Replica {
    pub fn new(storage: Box<dyn Storage>) -> Replica {
        Replica {
            taskdb: TaskDb::new(storage),
            added_undo_point: false,
            depmap: None,
        }
    }

    #[cfg(test)]
    pub fn new_inmemory() -> Replica {
        Replica::new(Box::new(crate::storage::InMemoryStorage::new()))
    }

    /// Update an existing task.  If the value is Some, the property is added or updated.  If the
    /// value is None, the property is deleted.  It is not an error to delete a nonexistent
    /// property.
    #[deprecated(since = "0.7.0", note = "please use TaskData instead")]
    pub fn update_task<S1, S2>(
        &mut self,
        uuid: Uuid,
        property: S1,
        value: Option<S2>,
    ) -> Result<TaskMap>
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        let value = value.map(|v| v.into());
        let property = property.into();
        let mut ops = self.make_operations();
        let Some(mut task) = self.get_task_data(uuid)? else {
            return Err(Error::Database(format!("Task {} does not exist", uuid)));
        };
        task.update(property, value, &mut ops);
        self.commit_operations(ops)?;
        Ok(self
            .taskdb
            .get_task(uuid)?
            .expect("task should exist after an update"))
    }

    /// Add the given uuid to the working set, returning its index.
    pub(crate) fn add_to_working_set(&mut self, uuid: Uuid) -> Result<usize> {
        self.taskdb.add_to_working_set(uuid)
    }

    /// Get all tasks represented as a map keyed by UUID
    pub fn all_tasks(&mut self) -> Result<HashMap<Uuid, Task>> {
        let depmap = self.dependency_map(false)?;
        let mut res = HashMap::new();
        for (uuid, tm) in self.taskdb.all_tasks()?.drain(..) {
            res.insert(uuid, Task::new(uuid, tm, depmap.clone()));
        }
        Ok(res)
    }

    /// Get the UUIDs of all tasks
    pub fn all_task_uuids(&mut self) -> Result<Vec<Uuid>> {
        self.taskdb.all_task_uuids()
    }

    /// Get the "working set" for this replica.  This is a snapshot of the current state,
    /// and it is up to the caller to decide how long to store this value.
    pub fn working_set(&mut self) -> Result<WorkingSet> {
        Ok(WorkingSet::new(self.taskdb.working_set()?))
    }

    /// Get the dependency map for all pending tasks.
    ///
    /// A task dependency is recognized when a task in the working set depends on a task with
    /// status equal to Pending.
    ///
    /// The data in this map is cached when it is first requested and may not contain modifications
    /// made locally in this Replica instance.  The result is reference-counted and may
    /// outlive the Replica.
    ///
    /// If `force` is true, then the result is re-calculated from the current state of the replica,
    /// although previously-returned dependency maps are not updated.
    pub fn dependency_map(&mut self, force: bool) -> Result<Rc<DependencyMap>> {
        if force || self.depmap.is_none() {
            // note: we can't use self.get_task here, as that depends on a
            // DependencyMap

            let mut dm = DependencyMap::new();
            // temporary cache tracking whether tasks are considered Pending or not.
            let mut is_pending_cache: HashMap<Uuid, bool> = HashMap::new();
            let ws = self.working_set()?;
            // for each task in the working set
            for i in 1..=ws.largest_index() {
                // get the task UUID
                if let Some(u) = ws.by_index(i) {
                    // get the task
                    if let Some(taskmap) = self.taskdb.get_task(u)? {
                        // search the task's keys
                        for p in taskmap.keys() {
                            // for one matching `dep_..`
                            if let Some(dep_str) = p.strip_prefix("dep_") {
                                // and extract the UUID from the key
                                if let Ok(dep) = Uuid::parse_str(dep_str) {
                                    // the dependency is pending if
                                    let dep_pending = {
                                        // we've determined this before and cached the result
                                        if let Some(dep_pending) = is_pending_cache.get(&dep) {
                                            *dep_pending
                                        } else if let Some(dep_taskmap) =
                                            // or if we get the task
                                            self.taskdb.get_task(dep)?
                                        {
                                            // and its status is "pending"
                                            let dep_pending = matches!(
                                                dep_taskmap
                                                    .get("status")
                                                    .map(|tm| Status::from_taskmap(tm)),
                                                Some(Status::Pending)
                                            );
                                            is_pending_cache.insert(dep, dep_pending);
                                            dep_pending
                                        } else {
                                            false
                                        }
                                    };
                                    if dep_pending {
                                        dm.add_dependency(u, dep);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            self.depmap = Some(Rc::new(dm));
        }

        // at this point self.depmap is guaranteed to be Some(_)
        Ok(self.depmap.as_ref().unwrap().clone())
    }

    /// Get an existing task by its UUID
    pub fn get_task(&mut self, uuid: Uuid) -> Result<Option<Task>> {
        let depmap = self.dependency_map(false)?;
        Ok(self
            .taskdb
            .get_task(uuid)?
            .map(move |tm| Task::new(uuid, tm, depmap)))
    }

    /// get an existing task by its UUID, as a [`TaskData`](crate::TaskData).
    pub fn get_task_data(&mut self, uuid: Uuid) -> Result<Option<TaskData>> {
        Ok(self
            .taskdb
            .get_task(uuid)?
            .map(move |tm| TaskData::new(uuid, tm)))
    }

    /// Create a new task.
    ///
    /// This uses the high-level task interface. To create a task with the low-level
    /// interface, use [`TaskData::create`](crate::TaskData::create).
    pub fn new_task(&mut self, status: Status, description: String) -> Result<Task> {
        let uuid = Uuid::new_v4();
        let mut ops = self.make_operations();
        let now = format!("{}", Utc::now().timestamp());
        let mut task = TaskData::create(uuid, &mut ops);
        task.update("modified", Some(now.clone()), &mut ops);
        task.update("description", Some(description), &mut ops);
        task.update("status", Some(status.to_taskmap().to_string()), &mut ops);
        task.update("entry", Some(now), &mut ops);
        self.commit_operations(ops)?;
        trace!("task {} created", uuid);
        Ok(self
            .get_task(uuid)?
            .expect("Task should exist after creation"))
    }

    /// Create a new, empty task with the given UUID.  This is useful for importing tasks, but
    /// otherwise should be avoided in favor of `new_task`.  If the task already exists, this
    /// does nothing and returns the existing task.
    #[deprecated(since = "0.7.0", note = "please use TaskData instead")]
    pub fn import_task_with_uuid(&mut self, uuid: Uuid) -> Result<Task> {
        let mut ops = self.make_operations();
        TaskData::create(uuid, &mut ops);
        self.commit_operations(ops)?;
        Ok(self
            .get_task(uuid)?
            .expect("Task should exist after creation"))
    }

    /// Delete a task.  The task must exist.  Note that this is different from setting status to
    /// Deleted; this is the final purge of the task.
    ///
    /// Deletion may interact poorly with modifications to the same task on other replicas. For
    /// example, if a task is deleted on replica 1 and its description modified on replica 1, then
    /// after both replicas have fully synced, the resulting task will only have a `description`
    /// property.
    pub fn delete_task(&mut self, uuid: Uuid) -> Result<()> {
        let Some(task) = self.get_task_data(uuid)? else {
            return Err(Error::Database(format!("Task {} does not exist", uuid)));
        };
        let mut ops = self.make_operations();
        task.delete(&mut ops);
        self.commit_operations(ops)?;
        trace!("task {} deleted", uuid);
        Ok(())
    }

    /// Commit a set of operations to the replica.
    ///
    /// All local state on the replica will be updated accordingly, including the working set and
    /// and temporarily cached data.
    pub fn commit_operations(&mut self, operations: Operations) -> Result<()> {
        // Add tasks to the working set when the status property is updated from anything other
        // than pending or recurring to one of those two statuses.
        let pending = Status::Pending.to_taskmap();
        let recurring = Status::Recurring.to_taskmap();
        let is_p_or_r = |val: &Option<String>| {
            if let Some(val) = val {
                val == pending || val == recurring
            } else {
                false
            }
        };
        let add_to_working_set = |op: &Operation| match op {
            Operation::Update {
                property,
                value,
                old_value,
                ..
            } => property == "status" && !is_p_or_r(old_value) && is_p_or_r(value),
            _ => false,
        };
        self.taskdb
            .commit_operations(operations, add_to_working_set)?;

        // The cached dependency map may now be invalid, do not retain it. Any existing Task values
        // will continue to use the old map.
        self.depmap = None;

        Ok(())
    }

    /// Synchronize this replica against the given server.  The working set is rebuilt after
    /// this occurs, but without renumbering, so any newly-pending tasks should appear in
    /// the working set.
    ///
    /// If `avoid_snapshots` is true, the sync operations produces a snapshot only when the server
    /// indicate it is urgent (snapshot urgency "high").  This allows time for other replicas to
    /// create a snapshot before this one does.
    ///
    /// Set this to true on systems more constrained in CPU, memory, or bandwidth than a typical desktop
    /// system
    pub fn sync(&mut self, server: &mut Box<dyn Server>, avoid_snapshots: bool) -> Result<()> {
        self.taskdb
            .sync(server, avoid_snapshots)
            .context("Failed to synchronize with server")?;
        self.rebuild_working_set(false)
            .context("Failed to rebuild working set after sync")?;
        Ok(())
    }

    /// Return undo local operations until the most recent UndoPoint, returning an empty Vec if there are no
    /// local operations to undo.
    pub fn get_undo_ops(&mut self) -> Result<Vec<Operation>> {
        self.taskdb.get_undo_ops()
    }

    /// Undo local operations in storage, returning a boolean indicating success.
    pub fn commit_undo_ops(&mut self, undo_ops: Vec<Operation>) -> Result<bool> {
        self.taskdb.commit_undo_ops(undo_ops)
    }

    /// Rebuild this replica's working set, based on whether tasks are pending or not.  If
    /// `renumber` is true, then existing tasks may be moved to new working-set indices; in any
    /// case, on completion all pending and recurring tasks are in the working set and all tasks
    /// with other statuses are not.
    pub fn rebuild_working_set(&mut self, renumber: bool) -> Result<()> {
        let pending = String::from(Status::Pending.to_taskmap());
        let recurring = String::from(Status::Recurring.to_taskmap());
        self.taskdb.rebuild_working_set(
            |t| {
                if let Some(st) = t.get("status") {
                    st == &pending || st == &recurring
                } else {
                    false
                }
            },
            renumber,
        )?;
        Ok(())
    }

    /// Expire old, deleted tasks.
    ///
    /// Expiration entails removal of tasks from the replica. Any modifications that occur after
    /// the deletion (such as operations synchronized from other replicas) will do nothing.
    ///
    /// Tasks are eligible for expiration when they have status Deleted and have not been modified
    /// for 180 days (about six months). Note that completed tasks are not eligible.
    pub fn expire_tasks(&mut self) -> Result<()> {
        let six_mos_ago = Utc::now() - Duration::days(180);
        self.all_tasks()?
            .iter()
            .filter(|(_, t)| t.get_status() == Status::Deleted)
            .filter(|(_, t)| {
                if let Some(m) = t.get_modified() {
                    m < six_mos_ago
                } else {
                    false
                }
            })
            .try_for_each(|(u, _)| self.delete_task(*u))?;
        Ok(())
    }

    /// Add an UndoPoint, if one has not already been added by this Replica.  This occurs
    /// automatically when a change is made.  The `force` flag allows forcing a new UndoPoint
    /// even if one has already been created by this Replica, and may be useful when a Replica
    /// instance is held for a long time and used to apply more than one user-visible change.
    #[deprecated(since = "0.7.0", note = "commit an Operation::UndoPoint instead")]
    pub fn add_undo_point(&mut self, force: bool) -> Result<()> {
        if force || !self.added_undo_point {
            let ops = Operations::new_with_undo_point();
            self.commit_operations(ops)?;
            self.added_undo_point = true;
        }
        Ok(())
    }

    /// Make a new `Operations`, with an undo operation if one has not already been added by
    /// this `Replica` insance
    fn make_operations(&mut self) -> Operations {
        if self.added_undo_point {
            Operations::new()
        } else {
            self.added_undo_point = true;
            Operations::new_with_undo_point()
        }
    }

    /// Get the number of operations local to this replica and not yet synchronized to the server.
    pub fn num_local_operations(&mut self) -> Result<usize> {
        self.taskdb.num_operations()
    }

    /// Get the number of undo points available (number of times `undo` will succeed).
    pub fn num_undo_points(&mut self) -> Result<usize> {
        self.taskdb.num_undo_points()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::Status;
    use chrono::TimeZone;
    use pretty_assertions::assert_eq;
    use std::collections::HashSet;
    use uuid::Uuid;

    #[test]
    fn new_task() {
        let mut rep = Replica::new_inmemory();

        let t = rep.new_task(Status::Pending, "a task".into()).unwrap();
        assert_eq!(t.get_description(), String::from("a task"));
        assert_eq!(t.get_status(), Status::Pending);
        assert!(t.get_modified().is_some());
    }

    #[test]
    fn modify_task() {
        let mut rep = Replica::new_inmemory();

        let t = rep.new_task(Status::Pending, "a task".into()).unwrap();

        let mut t = t.into_mut(&mut rep);
        t.set_description(String::from("past tense")).unwrap();
        t.set_status(Status::Completed).unwrap();
        // check that values have changed on the TaskMut
        assert_eq!(t.get_description(), "past tense");
        assert_eq!(t.get_status(), Status::Completed);

        // check that values have changed after into_immut
        let t = t.into_immut();
        assert_eq!(t.get_description(), "past tense");
        assert_eq!(t.get_status(), Status::Completed);

        // check that values have changed in storage, too
        let t = rep.get_task(t.get_uuid()).unwrap().unwrap();
        assert_eq!(t.get_description(), "past tense");
        assert_eq!(t.get_status(), Status::Completed);

        // and check for the corresponding operations, cleaning out the timestamps
        // and modified properties as these are based on the current time
        let now = Utc::now();
        let clean_op = |op: Operation| {
            if let Operation::Update {
                uuid,
                property,
                mut old_value,
                mut value,
                ..
            } = op
            {
                // rewrite automatically-created dates to "just-now" for ease
                // of testing
                if property == "modified" || property == "end" || property == "entry" {
                    if value.is_some() {
                        value = Some("just-now".into());
                    }
                    if old_value.is_some() {
                        old_value = Some("just-now".into());
                    }
                }
                Operation::Update {
                    uuid,
                    property,
                    old_value,
                    value,
                    timestamp: now,
                }
            } else {
                op
            }
        };
        assert_eq!(
            rep.taskdb
                .operations()
                .drain(..)
                .map(clean_op)
                .collect::<Vec<_>>(),
            vec![
                Operation::UndoPoint,
                Operation::Create { uuid: t.get_uuid() },
                Operation::Update {
                    uuid: t.get_uuid(),
                    property: "modified".into(),
                    old_value: None,
                    value: Some("just-now".into()),
                    timestamp: now,
                },
                Operation::Update {
                    uuid: t.get_uuid(),
                    property: "description".into(),
                    old_value: None,
                    value: Some("a task".into()),
                    timestamp: now,
                },
                Operation::Update {
                    uuid: t.get_uuid(),
                    property: "status".into(),
                    old_value: None,
                    value: Some("pending".into()),
                    timestamp: now,
                },
                Operation::Update {
                    uuid: t.get_uuid(),
                    property: "entry".into(),
                    old_value: None,
                    value: Some("just-now".into()),
                    timestamp: now,
                },
                Operation::Update {
                    uuid: t.get_uuid(),
                    property: "modified".into(),
                    old_value: Some("just-now".into()),
                    value: Some("just-now".into()),
                    timestamp: now,
                },
                Operation::Update {
                    uuid: t.get_uuid(),
                    property: "description".into(),
                    old_value: Some("a task".into()),
                    value: Some("past tense".into()),
                    timestamp: now,
                },
                Operation::Update {
                    uuid: t.get_uuid(),
                    property: "end".into(),
                    old_value: None,
                    value: Some("just-now".into()),
                    timestamp: now,
                },
                Operation::Update {
                    uuid: t.get_uuid(),
                    property: "status".into(),
                    old_value: Some("pending".into()),
                    value: Some("completed".into()),
                    timestamp: now,
                },
            ]
        );

        // num_local_operations includes all but the undo point
        assert_eq!(rep.num_local_operations().unwrap(), 9);

        // num_undo_points includes only the undo point
        assert_eq!(rep.num_undo_points().unwrap(), 1);
    }

    #[test]
    fn delete_task() {
        let mut rep = Replica::new_inmemory();

        let t = rep.new_task(Status::Pending, "a task".into()).unwrap();
        let uuid = t.get_uuid();

        rep.delete_task(uuid).unwrap();
        assert_eq!(rep.get_task(uuid).unwrap(), None);
    }

    #[test]
    fn commit_operations() -> Result<()> {
        // This mostly tests the working-set callback, as `TaskDB::commit_operations` has
        // tests for the remaining functionality.
        let mut rep = Replica::new_inmemory();

        // Generate the depmap so later assertions can verify it is reset.
        rep.dependency_map(true).unwrap();
        assert!(rep.depmap.is_some());

        let t = rep.new_task(Status::Pending, "a task".into())?;
        let uuid1 = t.get_uuid();

        let mut ops = Operations::new();

        // uuid2 is created and deleted, but this does not affect the
        // working set.
        let uuid2 = Uuid::new_v4();
        ops.add(Operation::Create { uuid: uuid2 });
        ops.add(Operation::Delete {
            uuid: uuid2,
            old_task: TaskMap::new(),
        });

        let update_op = |uuid, property: &str, old_value: Option<&str>, value: Option<&str>| {
            Operation::Update {
                uuid,
                property: property.to_string(),
                value: value.map(|v| v.to_string()),
                timestamp: Utc::now(),
                old_value: old_value.map(|v| v.to_string()),
            }
        };

        // uuid3 has status deleted, so is not added to the working set.
        let uuid3 = Uuid::new_v4();
        ops.add(update_op(uuid3, "status", None, Some("deleted")));

        // uuid4 goes from pending to pending, so is not added to the working set.
        let uuid4 = Uuid::new_v4();
        ops.add(update_op(uuid4, "status", Some("pending"), Some("pending")));

        // uuid5 goes from recurring to recurring, so is not added to the working set.
        let uuid5 = Uuid::new_v4();
        ops.add(update_op(
            uuid5,
            "status",
            Some("recurring"),
            Some("recurring"),
        ));

        // uuid6 goes from recurring to pending, so is not added to the working set.
        let uuid6 = Uuid::new_v4();
        ops.add(update_op(
            uuid6,
            "status",
            Some("recurring"),
            Some("pending"),
        ));

        // uuid7 goes from pending to recurring, so is not added to the working set.
        let uuid7 = Uuid::new_v4();
        ops.add(update_op(
            uuid7,
            "status",
            Some("pending"),
            Some("recurring"),
        ));

        // uuid8 goes from no-status to recurring, so is added to the working set.
        let uuid8 = Uuid::new_v4();
        ops.add(update_op(uuid8, "status", None, Some("recurring")));

        // uuid9 goes from no-status to pending, so is added to the working set.
        let uuid9 = Uuid::new_v4();
        ops.add(update_op(uuid9, "status", None, Some("pending")));

        // uuid10 goes from deleted to pending, so is added to the working set.
        let uuid10 = Uuid::new_v4();
        ops.add(update_op(
            uuid10,
            "status",
            Some("deleted"),
            Some("pending"),
        ));

        // uuid11 goes from pending to deleted, so is not added to the working set.
        let uuid11 = Uuid::new_v4();
        ops.add(update_op(
            uuid11,
            "status",
            Some("pending"),
            Some("deleted"),
        ));

        // uuid12 goes from pending to no-status, so is not added to the working set.
        let uuid12 = Uuid::new_v4();
        ops.add(update_op(uuid12, "status", Some("pending"), None));

        rep.commit_operations(ops)?;

        let ws = rep.working_set()?;
        assert!(ws.by_uuid(uuid1).is_some());
        assert!(ws.by_uuid(uuid2).is_none());
        assert!(ws.by_uuid(uuid3).is_none());
        assert!(ws.by_uuid(uuid4).is_none());
        assert!(ws.by_uuid(uuid5).is_none());
        assert!(ws.by_uuid(uuid6).is_none());
        assert!(ws.by_uuid(uuid7).is_none());
        assert!(ws.by_uuid(uuid8).is_some());
        assert!(ws.by_uuid(uuid9).is_some());
        assert!(ws.by_uuid(uuid10).is_some());
        assert!(ws.by_uuid(uuid11).is_none());
        assert!(ws.by_uuid(uuid12).is_none());

        // Cached dependency map was reset.
        assert!(rep.depmap.is_none());

        Ok(())
    }

    #[test]
    fn get_and_modify() {
        let mut rep = Replica::new_inmemory();

        let t = rep
            .new_task(Status::Pending, "another task".into())
            .unwrap();
        let uuid = t.get_uuid();

        let t = rep.get_task(uuid).unwrap().unwrap();
        assert_eq!(t.get_description(), String::from("another task"));

        let mut t = t.into_mut(&mut rep);
        t.set_status(Status::Deleted).unwrap();
        t.set_description("gone".into()).unwrap();

        let t = rep.get_task(uuid).unwrap().unwrap();
        assert_eq!(t.get_status(), Status::Deleted);
        assert_eq!(t.get_description(), "gone");

        rep.rebuild_working_set(true).unwrap();

        let ws = rep.working_set().unwrap();
        assert!(ws.by_uuid(t.get_uuid()).is_none());
    }

    #[test]
    fn get_task_data() {
        let mut rep = Replica::new_inmemory();

        let t = rep
            .new_task(Status::Pending, "another task".into())
            .unwrap();
        let uuid = t.get_uuid();

        let t = rep.get_task_data(uuid).unwrap().unwrap();
        assert_eq!(t.get_uuid(), uuid);
        assert_eq!(t.get("description"), Some("another task"));

        assert!(rep.get_task_data(Uuid::new_v4()).unwrap().is_none());
    }

    #[test]
    fn rebuild_working_set_includes_recurring() {
        let mut rep = Replica::new_inmemory();

        let t = rep
            .new_task(Status::Completed, "another task".into())
            .unwrap();
        let uuid = t.get_uuid();

        let t = rep.get_task(uuid).unwrap().unwrap();
        {
            let mut t = t.into_mut(&mut rep);
            t.set_status(Status::Recurring).unwrap();
        }

        rep.rebuild_working_set(true).unwrap();

        let ws = rep.working_set().unwrap();
        assert!(ws.by_uuid(uuid).is_some());
    }

    #[test]
    fn new_pending_adds_to_working_set() {
        let mut rep = Replica::new_inmemory();

        let t = rep
            .new_task(Status::Pending, "to-be-pending".into())
            .unwrap();
        let uuid = t.get_uuid();

        let ws = rep.working_set().unwrap();
        assert_eq!(ws.len(), 1); // only one non-none value
        assert!(ws.by_index(0).is_none());
        assert_eq!(ws.by_index(1), Some(uuid));

        let ws = rep.working_set().unwrap();
        assert_eq!(ws.by_uuid(t.get_uuid()), Some(1));
    }

    #[test]
    fn new_recurring_adds_to_working_set() {
        let mut rep = Replica::new_inmemory();

        let t = rep
            .new_task(Status::Recurring, "to-be-recurring".into())
            .unwrap();
        let uuid = t.get_uuid();

        let ws = rep.working_set().unwrap();
        assert_eq!(ws.len(), 1); // only one non-none value
        assert!(ws.by_index(0).is_none());
        assert_eq!(ws.by_index(1), Some(uuid));

        let ws = rep.working_set().unwrap();
        assert_eq!(ws.by_uuid(t.get_uuid()), Some(1));
    }

    #[test]
    fn get_does_not_exist() {
        let mut rep = Replica::new_inmemory();
        let uuid = Uuid::new_v4();
        assert_eq!(rep.get_task(uuid).unwrap(), None);
    }

    #[test]
    fn expire() {
        let mut rep = Replica::new_inmemory();
        let mut t;

        rep.new_task(Status::Pending, "keeper 1".into()).unwrap();
        rep.new_task(Status::Completed, "keeper 2".into()).unwrap();

        t = rep.new_task(Status::Deleted, "keeper 3".into()).unwrap();
        {
            let mut t = t.into_mut(&mut rep);
            // set entry, with modification set as a side-effect
            t.set_entry(Some(Utc::now())).unwrap();
        }

        t = rep.new_task(Status::Deleted, "goner".into()).unwrap();
        {
            let mut t = t.into_mut(&mut rep);
            t.set_modified(Utc.with_ymd_and_hms(1980, 1, 1, 0, 0, 0).unwrap())
                .unwrap();
        }

        rep.expire_tasks().unwrap();

        for (_, t) in rep.all_tasks().unwrap() {
            println!("got task {}", t.get_description());
            assert!(t.get_description().starts_with("keeper"));
        }
    }

    #[test]
    fn dependency_map() {
        let mut rep = Replica::new_inmemory();

        let mut tasks = vec![];
        for _ in 0..4 {
            tasks.push(rep.new_task(Status::Pending, "t".into()).unwrap());
        }

        let uuids: Vec<_> = tasks.iter().map(|t| t.get_uuid()).collect();

        // t[3] depends on t[2], and t[1]
        {
            let mut t = tasks.pop().unwrap().into_mut(&mut rep);
            t.add_dependency(uuids[2]).unwrap();
            t.add_dependency(uuids[1]).unwrap();
        }

        // t[2] depends on t[0]
        {
            let mut t = tasks.pop().unwrap().into_mut(&mut rep);
            t.add_dependency(uuids[0]).unwrap();
        }

        // t[1] depends on t[0]
        {
            let mut t = tasks.pop().unwrap().into_mut(&mut rep);
            t.add_dependency(uuids[0]).unwrap();
        }

        // generate the dependency map, forcing an update based on the newly-added
        // dependencies
        let dm = rep.dependency_map(true).unwrap();

        assert_eq!(
            dm.dependencies(uuids[3]).collect::<HashSet<_>>(),
            HashSet::from([uuids[1], uuids[2]])
        );
        assert_eq!(
            dm.dependencies(uuids[2]).collect::<HashSet<_>>(),
            HashSet::from([uuids[0]])
        );
        assert_eq!(
            dm.dependencies(uuids[1]).collect::<HashSet<_>>(),
            HashSet::from([uuids[0]])
        );
        assert_eq!(
            dm.dependencies(uuids[0]).collect::<HashSet<_>>(),
            HashSet::from([])
        );

        assert_eq!(
            dm.dependents(uuids[3]).collect::<HashSet<_>>(),
            HashSet::from([])
        );
        assert_eq!(
            dm.dependents(uuids[2]).collect::<HashSet<_>>(),
            HashSet::from([uuids[3]])
        );
        assert_eq!(
            dm.dependents(uuids[1]).collect::<HashSet<_>>(),
            HashSet::from([uuids[3]])
        );
        assert_eq!(
            dm.dependents(uuids[0]).collect::<HashSet<_>>(),
            HashSet::from([uuids[1], uuids[2]])
        );

        // mark t[0] as done, removing it from the working set
        rep.get_task(uuids[0])
            .unwrap()
            .unwrap()
            .into_mut(&mut rep)
            .done()
            .unwrap();
        let dm = rep.dependency_map(true).unwrap();

        assert_eq!(
            dm.dependencies(uuids[3]).collect::<HashSet<_>>(),
            HashSet::from([uuids[1], uuids[2]])
        );
        assert_eq!(
            dm.dependencies(uuids[2]).collect::<HashSet<_>>(),
            HashSet::from([])
        );
        assert_eq!(
            dm.dependencies(uuids[1]).collect::<HashSet<_>>(),
            HashSet::from([])
        );
        assert_eq!(
            dm.dependents(uuids[0]).collect::<HashSet<_>>(),
            HashSet::from([])
        );
    }
}
