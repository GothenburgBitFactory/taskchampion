use crate::depmap::DependencyMap;
use crate::errors::Result;
use crate::operation::{Operation, Operations};
use crate::server::Server;
use crate::storage::{Storage, TaskMap};
use crate::task::{Status, Task};
use crate::taskdb::TaskDb;
use crate::workingset::WorkingSet;
use crate::{Error, TaskData};
use anyhow::Context;
use chrono::{DateTime, Duration, Utc};
use log::trace;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

/// A replica represents an instance of a user's task data, providing an easy interface
/// for querying and modifying that data.
///
/// ## Tasks
///
/// Tasks are uniquely identified by UUIDs. Most task modifications are performed via the
/// [`TaskData`](crate::TaskData) or [`Task`](crate::Task) types. The first is a lower-level type
/// that wraps the key-value store representing a task, while the second is a higher-level type
/// that supports methods to update specific properties, maintain dependencies and tags, and so on.
///
/// ## Operations
///
/// Changes to a replica occur by committing [`Operations`]s. All methods that change a replica
/// take an argument of type `&mut Operations`, and the necessary operations are added to that
/// sequence. Those changes may be reflected locally, such as in a [`Task`] or [`TaskData`] value, but
/// are not reflected in the Replica's storage until committed with [`Replica::commit_operations`].
/**
```rust
# #[cfg(feature = "storage-sqlite")]
# {
# use taskchampion::chrono::{TimeZone, Utc};
# use taskchampion::{storage::AccessMode, Operations, Replica, Status, Uuid, SqliteStorage};
# use tempfile::TempDir;
# async fn main() -> anyhow::Result<()> {
# let tmp_dir = TempDir::new()?;
# let mut replica = Replica::new(SqliteStorage::new(
#   tmp_dir.path().to_path_buf(),
#   AccessMode::ReadWrite,
#   true
# )?);
// Create a new task, recording the required operations.
let mut ops = Operations::new();
let uuid = Uuid::new_v4();
let mut t = replica.create_task(uuid, &mut ops).await?;
t.set_description("my first task".into(), &mut ops)?;
t.set_status(Status::Pending, &mut ops)?;
t.set_entry(Some(Utc::now()), &mut ops)?;

// Commit those operations to storage.
replica.commit_operations(ops).await?;
#
# Ok(())
# }
# }
```
**/
/// Undo is supported by producing an [`Operations`] value representing the operations to be
/// undone. These are then committed with [`Replica::commit_reversed_operations`].
///
/// ## Working Set
///
/// A replica maintains a "working set" of tasks that are of current concern to the user,
/// specifically pending tasks.  These are indexed with small, easy-to-type integers.  Newly
/// pending tasks are automatically added to the working set, and the working set can be
/// "renumbered" when necessary.
pub struct Replica<S: Storage> {
    taskdb: TaskDb<S>,

    /// If true, this replica has already added an undo point.
    added_undo_point: bool,

    /// The dependency map for this replica, if it has been calculated.
    depmap: Option<Arc<DependencyMap>>,
}

impl<S: Storage> Replica<S> {
    pub fn new(storage: S) -> Replica<S> {
        Replica {
            taskdb: TaskDb::new(storage),
            added_undo_point: false,
            depmap: None,
        }
    }

    /// Update an existing task.  If the value is Some, the property is added or updated.  If the
    /// value is None, the property is deleted.  It is not an error to delete a nonexistent
    /// property.
    #[deprecated(since = "0.7.0", note = "please use TaskData instead")]
    pub async fn update_task<S1, S2>(
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
        let Some(mut task) = self.get_task_data(uuid).await? else {
            return Err(Error::Database(format!("Task {} does not exist", uuid)));
        };
        task.update(property, value, &mut ops);
        self.commit_operations(ops).await?;
        Ok(self
            .taskdb
            .get_task(uuid)
            .await?
            .expect("task should exist after an update"))
    }

    /// Get all tasks represented as a map keyed by UUID
    pub async fn all_tasks(&mut self) -> Result<HashMap<Uuid, Task>> {
        let depmap = self.dependency_map(false).await?;
        let mut res = HashMap::new();
        for (uuid, tm) in self.taskdb.all_tasks().await?.drain(..) {
            res.insert(uuid, Task::new(TaskData::new(uuid, tm), depmap.clone()));
        }
        Ok(res)
    }

    /// Get all task represented as a map of [`TaskData`] keyed by UUID
    pub async fn all_task_data(&mut self) -> Result<HashMap<Uuid, TaskData>> {
        let mut res = HashMap::new();
        for (uuid, tm) in self.taskdb.all_tasks().await?.drain(..) {
            res.insert(uuid, TaskData::new(uuid, tm));
        }
        Ok(res)
    }

    /// Get the UUIDs of all tasks
    pub async fn all_task_uuids(&mut self) -> Result<Vec<Uuid>> {
        self.taskdb.all_task_uuids().await
    }

    /// Get an array containing all pending tasks
    pub async fn pending_tasks(&mut self) -> Result<Vec<Task>> {
        let depmap = self.dependency_map(false).await?;
        let res = self
            .pending_task_data()
            .await?
            .into_iter()
            .map(|taskdata| Task::new(taskdata, depmap.clone()))
            .collect();

        Ok(res)
    }

    pub async fn pending_task_data(&mut self) -> Result<Vec<TaskData>> {
        let res = self
            .taskdb
            .get_pending_tasks()
            .await?
            .into_iter()
            .map(|(uuid, taskmap)| TaskData::new(uuid, taskmap))
            .collect::<Vec<_>>();

        Ok(res)
    }

    /// Get the "working set" for this replica.  This is a snapshot of the current state,
    /// and it is up to the caller to decide how long to store this value.
    pub async fn working_set(&mut self) -> Result<WorkingSet> {
        Ok(WorkingSet::new(self.taskdb.working_set().await?))
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
    ///
    /// Calculating this value requires a scan of the full working set and may not be performant.
    /// The [`TaskData`] API avoids generating this value.
    pub async fn dependency_map(&mut self, force: bool) -> Result<Arc<DependencyMap>> {
        if force || self.depmap.is_none() {
            // note: we can't use self.get_task here, as that depends on a
            // DependencyMap

            let mut dm = DependencyMap::new();
            // temporary cache tracking whether tasks are considered Pending or not.
            let mut is_pending_cache: HashMap<Uuid, bool> = HashMap::new();
            let ws = self.working_set().await?;
            // for each task in the working set
            for i in 1..=ws.largest_index() {
                // get the task UUID
                if let Some(u) = ws.by_index(i) {
                    // get the task
                    if let Some(taskmap) = self.taskdb.get_task(u).await? {
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
                                            self.taskdb.get_task(dep).await?
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
            self.depmap = Some(Arc::new(dm));
        }

        // at this point self.depmap is guaranteed to be Some(_)
        Ok(self.depmap.as_ref().unwrap().clone())
    }

    /// Get an existing task by its UUID
    pub async fn get_task(&mut self, uuid: Uuid) -> Result<Option<Task>> {
        let depmap = self.dependency_map(false).await?;
        Ok(self
            .taskdb
            .get_task(uuid)
            .await?
            .map(move |tm| Task::new(TaskData::new(uuid, tm), depmap)))
    }

    /// Get an existing task by its UUID, as a [`TaskData`](crate::TaskData).
    pub async fn get_task_data(&mut self, uuid: Uuid) -> Result<Option<TaskData>> {
        Ok(self
            .taskdb
            .get_task(uuid)
            .await?
            .map(move |tm| TaskData::new(uuid, tm)))
    }

    /// Get the operations that led to the given task.
    ///
    /// This set of operations is suitable for providing an overview of the task history, but does
    /// not satisfy any invariants around operations and task state. That is, it is not guaranteed
    /// that the returned operations, if applied in order, would generate the current task state.
    ///
    /// It is also not guaranteed to be the same on every replica. Differences can occur when
    /// conflicting operations were performed on different replicas. The "losing" operations in
    /// those conflicts may not appear on all replicas. In practice, conflicts are rare and the
    /// results of this function will be the same on all replicas for most tasks.
    pub async fn get_task_operations(&mut self, uuid: Uuid) -> Result<Operations> {
        self.taskdb.get_task_operations(uuid).await
    }

    /// Create a new task, setting `modified`, `description`, `status`, and `entry`.
    ///
    /// This uses the high-level task interface. To create a task with the low-level
    /// interface, use [`TaskData::create`](crate::TaskData::create).
    #[deprecated(
        since = "0.7.0",
        note = "please use `create_task` and call `Task` methods `set_status`, `set_description`, and `set_entry`"
    )]
    pub async fn new_task(&mut self, status: Status, description: String) -> Result<Task> {
        let uuid = Uuid::new_v4();
        let mut ops = self.make_operations();
        let now = format!("{}", Utc::now().timestamp());
        let mut task = TaskData::create(uuid, &mut ops);
        task.update("modified", Some(now.clone()), &mut ops);
        task.update("description", Some(description), &mut ops);
        task.update("status", Some(status.to_taskmap().to_string()), &mut ops);
        task.update("entry", Some(now), &mut ops);
        self.commit_operations(ops).await?;
        trace!("task {} created", uuid);
        Ok(self
            .get_task(uuid)
            .await?
            .expect("Task should exist after creation"))
    }

    /// Create a new task.
    ///
    /// Use [Uuid::new_v4] to invent a new task ID, if necessary. If the task already
    /// exists, it is returned.
    pub async fn create_task(&mut self, uuid: Uuid, ops: &mut Operations) -> Result<Task> {
        if let Some(task) = self.get_task(uuid).await? {
            return Ok(task);
        }
        let depmap = self.dependency_map(false).await?;
        Ok(Task::new(TaskData::create(uuid, ops), depmap))
    }

    /// Create a new, empty task with the given UUID.  This is useful for importing tasks, but
    /// otherwise should be avoided in favor of `create_task`.  If the task already exists, this
    /// does nothing and returns the existing task.
    #[deprecated(since = "0.7.0", note = "please use TaskData instead")]
    pub async fn import_task_with_uuid(&mut self, uuid: Uuid) -> Result<Task> {
        let mut ops = self.make_operations();
        TaskData::create(uuid, &mut ops);
        self.commit_operations(ops).await?;
        Ok(self
            .get_task(uuid)
            .await?
            .expect("Task should exist after creation"))
    }

    /// Delete a task.  The task must exist.  Note that this is different from setting status to
    /// Deleted; this is the final purge of the task.
    ///
    /// Deletion may interact poorly with modifications to the same task on other replicas. For
    /// example, if a task is deleted on replica 1 and its description modified on replica 1, then
    /// after both replicas have fully synced, the resulting task will only have a `description`
    /// property.
    #[deprecated(since = "0.7.0", note = "please use TaskData::delete")]
    pub async fn delete_task(&mut self, uuid: Uuid) -> Result<()> {
        let Some(mut task) = self.get_task_data(uuid).await? else {
            return Err(Error::Database(format!("Task {} does not exist", uuid)));
        };
        let mut ops = self.make_operations();
        task.delete(&mut ops);
        self.commit_operations(ops).await?;
        trace!("task {} deleted", uuid);
        Ok(())
    }

    /// Commit a set of operations to the replica.
    ///
    /// All local state on the replica will be updated accordingly, including the working set and
    /// and temporarily cached data.
    pub async fn commit_operations(&mut self, operations: Operations) -> Result<()> {
        if operations.is_empty() {
            return Ok(());
        }

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
            .commit_operations(operations, add_to_working_set)
            .await?;

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
    pub async fn sync(
        &mut self,
        server: &mut Box<dyn Server>,
        avoid_snapshots: bool,
    ) -> Result<()> {
        self.taskdb
            .sync(server, avoid_snapshots)
            .await
            .context("Failed to synchronize with server")?;
        self.rebuild_working_set(false)
            .await
            .context("Failed to rebuild working set after sync")?;
        Ok(())
    }

    /// Return the operations back to and including the last undo point, or since the last sync if
    /// no undo point is found.
    ///
    /// The operations are returned in the order they were applied. Use
    /// [`Replica::commit_reversed_operations`] to "undo" them.
    pub async fn get_undo_operations(&mut self) -> Result<Operations> {
        self.taskdb.get_undo_operations().await
    }

    /// Commit the reverse of the given operations, beginning with the last operation in the given
    /// operations and proceeding to the first.
    ///
    /// This method only supports reversing operations if they precisely match local operations
    /// that have not yet been synchronized, and will return `false` if this is not the case.
    pub async fn commit_reversed_operations(&mut self, operations: Operations) -> Result<bool> {
        if !self.taskdb.commit_reversed_operations(operations).await? {
            return Ok(false);
        }

        // Both the dependency map and the working set are potentially now invalid.
        self.depmap = None;
        self.rebuild_working_set(false)
            .await
            .context("Failed to rebuild working set after committing reversed operations")?;

        Ok(true)
    }

    /// Rebuild this replica's working set, based on whether tasks are pending or not.  If
    /// `renumber` is true, then existing tasks may be moved to new working-set indices; in any
    /// case, on completion all pending and recurring tasks are in the working set and all tasks
    /// with other statuses are not.
    pub async fn rebuild_working_set(&mut self, renumber: bool) -> Result<()> {
        let pending = String::from(Status::Pending.to_taskmap());
        let recurring = String::from(Status::Recurring.to_taskmap());
        self.taskdb
            .rebuild_working_set(
                |t| {
                    if let Some(st) = t.get("status") {
                        st == &pending || st == &recurring
                    } else {
                        false
                    }
                },
                renumber,
            )
            .await?;
        Ok(())
    }

    /// Expire old, deleted tasks.
    ///
    /// Expiration entails removal of tasks from the replica. Any modifications that occur after
    /// the deletion (such as operations synchronized from other replicas) will do nothing.
    ///
    /// Tasks are eligible for expiration when they have status Deleted and have not been modified
    /// for 180 days (about six months). Note that completed tasks are not eligible.
    pub async fn expire_tasks(&mut self) -> Result<()> {
        let six_mos_ago = Utc::now() - Duration::days(180);
        let mut ops = Operations::new();
        let deleted = Status::Deleted.to_taskmap();
        self.all_task_data()
            .await?
            .drain()
            .filter(|(_, t)| t.get("status") == Some(deleted))
            .filter(|(_, t)| {
                t.get("modified").is_some_and(|m| {
                    m.parse().is_ok_and(|time_sec| {
                        DateTime::from_timestamp(time_sec, 0).is_some_and(|dt| dt < six_mos_ago)
                    })
                })
            })
            .for_each(|(_, mut t)| t.delete(&mut ops));
        self.commit_operations(ops).await
    }
    /// Add an UndoPoint, if one has not already been added by this Replica.  This occurs
    /// automatically when a change is made.  The `force` flag allows forcing a new UndoPoint
    /// even if one has already been created by this Replica, and may be useful when a Replica
    /// instance is held for a long time and used to apply more than one user-visible change.
    #[deprecated(
        since = "0.7.0",
        note = "Push an `Operation::UndoPoint` onto your `Operations` instead."
    )]
    pub async fn add_undo_point(&mut self, force: bool) -> Result<()> {
        if force || !self.added_undo_point {
            let ops = vec![Operation::UndoPoint];
            self.commit_operations(ops).await?;
            self.added_undo_point = true;
        }
        Ok(())
    }

    /// Make a new `Operations`, with an undo operation if one has not already been added by
    /// this `Replica` insance
    fn make_operations(&mut self) -> Operations {
        let mut ops = Operations::new();
        if !self.added_undo_point {
            ops.push(Operation::UndoPoint);
            self.added_undo_point = true;
        }
        ops
    }

    /// Get the number of operations local to this replica and not yet synchronized to the server.
    pub async fn num_local_operations(&mut self) -> Result<usize> {
        self.taskdb.num_operations().await
    }

    /// Get the number of undo points available (number of times `undo` will succeed).
    pub async fn num_undo_points(&mut self) -> Result<usize> {
        self.taskdb.num_undo_points().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{storage::inmemory::InMemoryStorage, task::Status};
    use chrono::{DateTime, TimeZone};
    use pretty_assertions::assert_eq;
    use std::collections::HashSet;
    use uuid::Uuid;

    const JUST_NOW: Option<DateTime<Utc>> = DateTime::from_timestamp(1800000000, 0);

    /// Rewrite automatically-created dates to "just-now" or `JUST_NOW` for ease of testing.
    fn clean_op(op: Operation) -> Operation {
        if let Operation::Update {
            uuid,
            property,
            mut old_value,
            mut value,
            ..
        } = op
        {
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
                timestamp: JUST_NOW.unwrap(),
            }
        } else {
            op
        }
    }

    #[tokio::test]
    async fn new_task() {
        let mut rep = Replica::new(InMemoryStorage::new());

        #[allow(deprecated)]
        let t = rep
            .new_task(Status::Pending, "a task".into())
            .await
            .unwrap();
        assert_eq!(t.get_description(), String::from("a task"));
        assert_eq!(t.get_status(), Status::Pending);
        assert!(t.get_modified().is_some());
    }

    #[tokio::test]
    async fn modify_task() {
        let mut rep = Replica::new(InMemoryStorage::new());

        // Further test the deprecated `new_task` method.
        #[allow(deprecated)]
        let mut t = rep
            .new_task(Status::Pending, "a task".into())
            .await
            .unwrap();

        let mut ops = Operations::new();
        t.set_description(String::from("past tense"), &mut ops)
            .unwrap();
        t.set_status(Status::Completed, &mut ops).unwrap();
        // check that values have changed on the Task
        assert_eq!(t.get_description(), "past tense");
        assert_eq!(t.get_status(), Status::Completed);

        // check that values have not changed in storage, yet
        let t = rep.get_task(t.get_uuid()).await.unwrap().unwrap();
        assert_eq!(t.get_description(), "a task");
        assert_eq!(t.get_status(), Status::Pending);

        // check that values have changed in storage after commit
        rep.commit_operations(ops).await.unwrap();
        let t = rep.get_task(t.get_uuid()).await.unwrap().unwrap();
        assert_eq!(t.get_description(), "past tense");
        assert_eq!(t.get_status(), Status::Completed);

        // and check for the corresponding operations, cleaning out the timestamps
        // and modified properties as these are based on the current time
        assert_eq!(
            rep.taskdb
                .operations()
                .await
                .into_iter()
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
                    timestamp: JUST_NOW.unwrap(),
                },
                Operation::Update {
                    uuid: t.get_uuid(),
                    property: "description".into(),
                    old_value: None,
                    value: Some("a task".into()),
                    timestamp: JUST_NOW.unwrap(),
                },
                Operation::Update {
                    uuid: t.get_uuid(),
                    property: "status".into(),
                    old_value: None,
                    value: Some("pending".into()),
                    timestamp: JUST_NOW.unwrap(),
                },
                Operation::Update {
                    uuid: t.get_uuid(),
                    property: "entry".into(),
                    old_value: None,
                    value: Some("just-now".into()),
                    timestamp: JUST_NOW.unwrap(),
                },
                Operation::Update {
                    uuid: t.get_uuid(),
                    property: "modified".into(),
                    old_value: Some("just-now".into()),
                    value: Some("just-now".into()),
                    timestamp: JUST_NOW.unwrap(),
                },
                Operation::Update {
                    uuid: t.get_uuid(),
                    property: "description".into(),
                    old_value: Some("a task".into()),
                    value: Some("past tense".into()),
                    timestamp: JUST_NOW.unwrap(),
                },
                Operation::Update {
                    uuid: t.get_uuid(),
                    property: "end".into(),
                    old_value: None,
                    value: Some("just-now".into()),
                    timestamp: JUST_NOW.unwrap(),
                },
                Operation::Update {
                    uuid: t.get_uuid(),
                    property: "status".into(),
                    old_value: Some("pending".into()),
                    value: Some("completed".into()),
                    timestamp: JUST_NOW.unwrap(),
                },
            ]
        );

        // num_local_operations includes all but the undo point
        assert_eq!(rep.num_local_operations().await.unwrap(), 9);

        // num_undo_points includes only the undo point
        assert_eq!(rep.num_undo_points().await.unwrap(), 1);

        // A second undo point is counted.
        let ops = vec![Operation::UndoPoint];
        rep.commit_operations(ops).await.unwrap();
        assert_eq!(rep.num_undo_points().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn delete_task() {
        let mut rep = Replica::new(InMemoryStorage::new());

        let uuid = Uuid::new_v4();
        let mut ops = Operations::new();
        rep.create_task(uuid, &mut ops).await.unwrap();
        rep.commit_operations(ops).await.unwrap();

        #[allow(deprecated)]
        rep.delete_task(uuid).await.unwrap();
        assert_eq!(rep.get_task(uuid).await.unwrap(), None);
    }

    #[tokio::test]
    async fn all_tasks() {
        let mut rep = Replica::new(InMemoryStorage::new());

        let (uuid1, uuid2) = (Uuid::new_v4(), Uuid::new_v4());
        let mut ops = Operations::new();
        rep.create_task(uuid1, &mut ops).await.unwrap();
        rep.create_task(uuid2, &mut ops).await.unwrap();
        rep.commit_operations(ops).await.unwrap();

        let all_tasks = rep.all_tasks().await.unwrap();
        assert_eq!(all_tasks.len(), 2);
        assert_eq!(all_tasks.get(&uuid1).unwrap().get_uuid(), uuid1);
        assert_eq!(all_tasks.get(&uuid2).unwrap().get_uuid(), uuid2);

        let all_tasks = rep.all_task_data().await.unwrap();
        assert_eq!(all_tasks.len(), 2);
        assert_eq!(all_tasks.get(&uuid1).unwrap().get_uuid(), uuid1);
        assert_eq!(all_tasks.get(&uuid2).unwrap().get_uuid(), uuid2);

        let mut all_uuids = rep.all_task_uuids().await.unwrap();
        all_uuids.sort();
        let mut exp_uuids = vec![uuid1, uuid2];
        exp_uuids.sort();
        assert_eq!(all_uuids.len(), 2);
        assert_eq!(all_uuids, exp_uuids);
    }

    #[tokio::test]
    async fn pending_tasks() {
        let mut rep = Replica::new(InMemoryStorage::new());

        let (uuid1, uuid2, uuid3) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let mut ops = Operations::new();

        let mut t1 = rep.create_task(uuid1, &mut ops).await.unwrap();
        t1.set_status(Status::Pending, &mut ops).unwrap();

        let mut t2 = rep.create_task(uuid2, &mut ops).await.unwrap();
        t2.set_status(Status::Pending, &mut ops).unwrap();

        let mut t3 = rep.create_task(uuid3, &mut ops).await.unwrap();
        t3.set_status(Status::Completed, &mut ops).unwrap();

        rep.commit_operations(ops).await.unwrap();

        let pending_tasks = rep.pending_tasks().await.unwrap();
        assert_eq!(pending_tasks.len(), 2);
        assert_eq!(pending_tasks.first().unwrap().get_uuid(), uuid1);
        assert_eq!(pending_tasks.get(1).unwrap().get_uuid(), uuid2);
    }

    #[tokio::test]
    async fn commit_operations() -> Result<()> {
        // This mostly tests the working-set callback, as `TaskDB::commit_operations` has
        // tests for the remaining functionality.
        let mut rep = Replica::new(InMemoryStorage::new());

        // Generate the depmap so later assertions can verify it is reset.
        rep.dependency_map(true).await.unwrap();
        assert!(rep.depmap.is_some());

        let mut ops = Operations::new();
        let uuid1 = Uuid::new_v4();
        let mut t = rep.create_task(uuid1, &mut ops).await.unwrap();
        t.set_status(Status::Pending, &mut ops).unwrap();

        // uuid2 is created and deleted, but this does not affect the
        // working set.
        let uuid2 = Uuid::new_v4();
        ops.push(Operation::Create { uuid: uuid2 });
        ops.push(Operation::Delete {
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
        ops.push(update_op(uuid3, "status", None, Some("deleted")));

        // uuid4 goes from pending to pending, so is not added to the working set.
        let uuid4 = Uuid::new_v4();
        ops.push(update_op(uuid4, "status", Some("pending"), Some("pending")));

        // uuid5 goes from recurring to recurring, so is not added to the working set.
        let uuid5 = Uuid::new_v4();
        ops.push(update_op(
            uuid5,
            "status",
            Some("recurring"),
            Some("recurring"),
        ));

        // uuid6 goes from recurring to pending, so is not added to the working set.
        let uuid6 = Uuid::new_v4();
        ops.push(update_op(
            uuid6,
            "status",
            Some("recurring"),
            Some("pending"),
        ));

        // uuid7 goes from pending to recurring, so is not added to the working set.
        let uuid7 = Uuid::new_v4();
        ops.push(update_op(
            uuid7,
            "status",
            Some("pending"),
            Some("recurring"),
        ));

        // uuid8 goes from no-status to recurring, so is added to the working set.
        let uuid8 = Uuid::new_v4();
        ops.push(update_op(uuid8, "status", None, Some("recurring")));

        // uuid9 goes from no-status to pending, so is added to the working set.
        let uuid9 = Uuid::new_v4();
        ops.push(update_op(uuid9, "status", None, Some("pending")));

        // uuid10 goes from deleted to pending, so is added to the working set.
        let uuid10 = Uuid::new_v4();
        ops.push(update_op(
            uuid10,
            "status",
            Some("deleted"),
            Some("pending"),
        ));

        // uuid11 goes from pending to deleted, so is not added to the working set.
        let uuid11 = Uuid::new_v4();
        ops.push(update_op(
            uuid11,
            "status",
            Some("pending"),
            Some("deleted"),
        ));

        // uuid12 goes from pending to no-status, so is not added to the working set.
        let uuid12 = Uuid::new_v4();
        ops.push(update_op(uuid12, "status", Some("pending"), None));

        rep.commit_operations(ops).await?;

        let ws = rep.working_set().await?;
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

    #[tokio::test]
    async fn commit_reversed_operations() -> Result<()> {
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let uuid3 = Uuid::new_v4();

        let mut rep = Replica::new(InMemoryStorage::new());

        let mut ops = Operations::new();
        ops.push(Operation::UndoPoint);
        rep.create_task(uuid1, &mut ops).await.unwrap();
        ops.push(Operation::UndoPoint);
        rep.create_task(uuid2, &mut ops).await.unwrap();
        rep.commit_operations(ops).await?;
        assert_eq!(rep.num_undo_points().await.unwrap(), 2);

        // Trying to reverse-commit the wrong operations fails.
        let ops = vec![Operation::Delete {
            uuid: uuid3,
            old_task: TaskMap::new(),
        }];
        assert!(!rep.commit_reversed_operations(ops).await?);

        // Commiting the correct operations succeeds
        let ops = rep.get_undo_operations().await?;
        assert!(rep.commit_reversed_operations(ops).await?);
        assert_eq!(rep.num_undo_points().await.unwrap(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn get_and_modify() {
        let mut rep = Replica::new(InMemoryStorage::new());

        let mut ops = Operations::new();
        let uuid = Uuid::new_v4();
        let mut t = rep.create_task(uuid, &mut ops).await.unwrap();
        t.set_status(Status::Pending, &mut ops).unwrap();
        t.set_description("another task".into(), &mut ops).unwrap();
        rep.commit_operations(ops).await.unwrap();

        let mut t = rep.get_task(uuid).await.unwrap().unwrap();
        assert_eq!(t.get_description(), String::from("another task"));

        let mut ops = Operations::new();
        t.set_status(Status::Deleted, &mut ops).unwrap();
        t.set_description("gone".into(), &mut ops).unwrap();
        rep.commit_operations(ops).await.unwrap();

        let t = rep.get_task(uuid).await.unwrap().unwrap();
        assert_eq!(t.get_status(), Status::Deleted);
        assert_eq!(t.get_description(), "gone");

        rep.rebuild_working_set(true).await.unwrap();

        let ws = rep.working_set().await.unwrap();
        assert!(ws.by_uuid(t.get_uuid()).is_none());
    }

    #[tokio::test]
    async fn get_task_data_and_operations() {
        let mut rep = Replica::new(InMemoryStorage::new());

        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let mut ops = Operations::new();
        let mut t = rep.create_task(uuid1, &mut ops).await.unwrap();
        t.set_description("another task".into(), &mut ops).unwrap();
        let mut t2 = rep.create_task(uuid2, &mut ops).await.unwrap();
        t2.set_description("a distraction!".into(), &mut ops)
            .unwrap();
        rep.commit_operations(ops).await.unwrap();

        let t = rep.get_task_data(uuid1).await.unwrap().unwrap();
        assert_eq!(t.get_uuid(), uuid1);
        assert_eq!(t.get("description"), Some("another task"));
        assert_eq!(
            rep.get_task_operations(uuid1)
                .await
                .unwrap()
                .into_iter()
                .map(clean_op)
                .collect::<Vec<_>>(),
            vec![
                Operation::Create { uuid: uuid1 },
                Operation::Update {
                    uuid: uuid1,
                    property: "modified".into(),
                    old_value: None,
                    value: Some("just-now".into()),
                    timestamp: JUST_NOW.unwrap(),
                },
                Operation::Update {
                    uuid: uuid1,
                    property: "description".into(),
                    old_value: None,
                    value: Some("another task".into()),
                    timestamp: JUST_NOW.unwrap(),
                },
            ]
        );

        assert!(rep.get_task_data(Uuid::new_v4()).await.unwrap().is_none());
        assert_eq!(
            rep.get_task_operations(Uuid::new_v4()).await.unwrap(),
            vec![]
        );
    }

    #[tokio::test]
    async fn rebuild_working_set_includes_recurring() {
        let mut rep = Replica::new(InMemoryStorage::new());

        let uuid = Uuid::new_v4();
        let mut ops = Operations::new();
        let mut t = rep.create_task(uuid, &mut ops).await.unwrap();
        t.set_status(Status::Completed, &mut ops).unwrap();
        rep.commit_operations(ops).await.unwrap();

        let mut t = rep.get_task(uuid).await.unwrap().unwrap();
        let mut ops = Operations::new();
        t.set_status(Status::Recurring, &mut ops).unwrap();
        rep.commit_operations(ops).await.unwrap();

        rep.rebuild_working_set(true).await.unwrap();

        let ws = rep.working_set().await.unwrap();
        assert!(ws.by_uuid(uuid).is_some());
    }

    #[tokio::test]
    async fn new_pending_adds_to_working_set() {
        let mut rep = Replica::new(InMemoryStorage::new());

        let uuid = Uuid::new_v4();
        let mut ops = Operations::new();
        let mut t = rep.create_task(uuid, &mut ops).await.unwrap();
        t.set_status(Status::Pending, &mut ops).unwrap();
        rep.commit_operations(ops).await.unwrap();

        let ws = rep.working_set().await.unwrap();
        assert_eq!(ws.len(), 1); // only one non-none value
        assert!(ws.by_index(0).is_none());
        assert_eq!(ws.by_index(1), Some(uuid));

        let ws = rep.working_set().await.unwrap();
        assert_eq!(ws.by_uuid(t.get_uuid()), Some(1));
    }

    #[tokio::test]
    async fn new_recurring_adds_to_working_set() {
        let mut rep = Replica::new(InMemoryStorage::new());

        let uuid = Uuid::new_v4();
        let mut ops = Operations::new();
        let mut t = rep.create_task(uuid, &mut ops).await.unwrap();
        t.set_status(Status::Recurring, &mut ops).unwrap();
        rep.commit_operations(ops).await.unwrap();

        let ws = rep.working_set().await.unwrap();
        assert_eq!(ws.len(), 1); // only one non-none value
        assert!(ws.by_index(0).is_none());
        assert_eq!(ws.by_index(1), Some(uuid));

        let ws = rep.working_set().await.unwrap();
        assert_eq!(ws.by_uuid(t.get_uuid()), Some(1));
    }

    #[tokio::test]
    async fn get_does_not_exist() {
        let mut rep = Replica::new(InMemoryStorage::new());
        let uuid = Uuid::new_v4();
        assert_eq!(rep.get_task(uuid).await.unwrap(), None);
    }

    #[tokio::test]
    async fn expire() {
        let mut rep = Replica::new(InMemoryStorage::new());
        let mut ops = Operations::new();

        // uuid1 is old but pending, so is not expired.
        let keeper_uuid1 = Uuid::new_v4();
        let mut t = rep.create_task(keeper_uuid1, &mut ops).await.unwrap();
        t.set_description("keeper 1".into(), &mut ops).unwrap();
        t.set_modified(Utc.with_ymd_and_hms(1980, 1, 1, 0, 0, 0).unwrap(), &mut ops)
            .unwrap();
        t.set_status(Status::Pending, &mut ops).unwrap();

        // uuid2 is old but completed, so is not expired.
        let keeper_uuid2 = Uuid::new_v4();
        let mut t = rep.create_task(keeper_uuid2, &mut ops).await.unwrap();
        t.set_description("keeper 2".into(), &mut ops).unwrap();
        t.set_modified(Utc.with_ymd_and_hms(1980, 1, 1, 0, 0, 0).unwrap(), &mut ops)
            .unwrap();
        t.set_status(Status::Completed, &mut ops).unwrap();

        // uuid3 is deleted but recently modified, so is not expired.
        let keeper_uuid3 = Uuid::new_v4();
        let mut t = rep.create_task(keeper_uuid3, &mut ops).await.unwrap();
        t.set_description("keeper 3".into(), &mut ops).unwrap();
        t.set_status(Status::Deleted, &mut ops).unwrap();
        t.set_modified(Utc::now(), &mut ops).unwrap();
        t.set_entry(Some(Utc::now()), &mut ops).unwrap();

        // uuid4 was deleted long ago, so it is expired.
        let goner_uuid4 = Uuid::new_v4();
        let mut t = rep.create_task(goner_uuid4, &mut ops).await.unwrap();
        t.set_description("goner".into(), &mut ops).unwrap();
        t.set_status(Status::Deleted, &mut ops).unwrap();
        t.set_modified(Utc.with_ymd_and_hms(1980, 1, 1, 0, 0, 0).unwrap(), &mut ops)
            .unwrap();
        rep.commit_operations(ops).await.unwrap();

        rep.expire_tasks().await.unwrap();

        assert!(rep.get_task_data(keeper_uuid1).await.unwrap().is_some());
        assert!(rep.get_task_data(keeper_uuid2).await.unwrap().is_some());
        assert!(rep.get_task_data(keeper_uuid3).await.unwrap().is_some());
        assert!(rep.get_task_data(goner_uuid4).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn dependency_map() {
        let mut rep = Replica::new(InMemoryStorage::new());

        let mut tasks = vec![];
        let mut ops = Operations::new();
        for _ in 0..4 {
            let mut t = rep.create_task(Uuid::new_v4(), &mut ops).await.unwrap();
            t.set_status(Status::Pending, &mut ops).unwrap();
            tasks.push(t);
        }
        let uuids: Vec<_> = tasks.iter().map(|t| t.get_uuid()).collect();

        // t[3] depends on t[2], and t[1]
        let mut t = tasks.pop().unwrap();
        t.add_dependency(uuids[2], &mut ops).unwrap();
        t.add_dependency(uuids[1], &mut ops).unwrap();

        // t[2] depends on t[0]
        let mut t = tasks.pop().unwrap();
        t.add_dependency(uuids[0], &mut ops).unwrap();

        // t[1] depends on t[0]
        let mut t = tasks.pop().unwrap();
        t.add_dependency(uuids[0], &mut ops).unwrap();

        rep.commit_operations(ops).await.unwrap();

        // generate the dependency map, forcing an update based on the newly-added dependencies.
        // This need not be forced since the `commit_operations` invalidated the cached value.
        let dm = rep.dependency_map(false).await.unwrap();

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
        let mut ops = Operations::new();
        rep.get_task(uuids[0])
            .await
            .unwrap()
            .unwrap()
            .done(&mut ops)
            .unwrap();
        rep.commit_operations(ops).await.unwrap();
        let dm = rep.dependency_map(false).await.unwrap();

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
