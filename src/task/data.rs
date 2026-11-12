use super::task::Prop;
use super::{utc_timestamp, Status, Timestamp};
use crate::{storage::TaskMap, Operation, Operations};
use chrono::Utc;
use uuid::Uuid;

/// A task.
///
/// This type presents a low-level interface consisting only of a key/value map. Interpretation of
/// fields is up to the user, and modifications both modify the [`TaskData`] and create one or
/// more [`Operation`](crate::Operation) values that can later be committed to the replica.
///
/// This interface is intended for sophisticated applications like Taskwarrior which give meaning
/// to key and values themselves. Use [`Task`](crate::Task) for a higher-level interface with
/// methods to update status, set tags, and so on.
///
/// See the documentation for [`crate::Replica`] for background on the `ops` arguments to methods
/// on this type.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TaskData {
    uuid: Uuid,
    taskmap: TaskMap,
}

impl TaskData {
    /// Constructor for a TaskData representing an existing task.
    pub(crate) fn new(uuid: Uuid, taskmap: TaskMap) -> Self {
        Self { uuid, taskmap }
    }

    /// Create a new, empty task with the given UUID.
    pub fn create(uuid: Uuid, ops: &mut Operations) -> Self {
        ops.push(Operation::Create { uuid });
        Self {
            uuid,
            taskmap: TaskMap::new(),
        }
    }

    /// Get this task's UUID.
    pub fn get_uuid(&self) -> Uuid {
        self.uuid
    }

    /// Get the taskmap (used only for deprecated `Task::get_taskmap`).
    pub(in crate::task) fn get_taskmap(&self) -> &TaskMap {
        &self.taskmap
    }

    /// Get a value on this task.
    pub fn get(&self, property: impl AsRef<str>) -> Option<&str> {
        self.taskmap.get(property.as_ref()).map(|v| v.as_str())
    }

    /// Check if the given property is set.
    pub fn has(&self, property: impl AsRef<str>) -> bool {
        self.taskmap.contains_key(property.as_ref())
    }

    /// Enumerate all properties on this task, in arbitrary order.
    pub fn properties(&self) -> impl Iterator<Item = &String> {
        self.taskmap.keys()
    }

    /// Enumerate all properties and their values on this task, in arbitrary order.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        self.taskmap.iter()
    }

    /// Get the status of this task. This defaults to `Pending` if no status is set.
    pub fn get_status(&self) -> Status {
        self.get(Prop::Status.as_ref())
            .map(Status::from_taskmap)
            .unwrap_or(Status::Pending)
    }

    /// Get the description of this task. This is an empty string if no description is set.
    pub fn get_description(&self) -> &str {
        self.get(Prop::Description.as_ref()).unwrap_or("")
    }

    /// Get the entry time for this task.
    pub fn get_entry(&self) -> Option<Timestamp> {
        self.get_timestamp(Prop::Entry.as_ref())
    }

    /// Get the priority of this task. This is an empty string if no priority is set.
    pub fn get_priority(&self) -> &str {
        self.get(Prop::Priority.as_ref()).unwrap_or("")
    }

    /// Get the wait time.  If this value is set, it will be returned, even
    /// if it is in the past.
    pub fn get_wait(&self) -> Option<Timestamp> {
        self.get_timestamp(Prop::Wait.as_ref())
    }

    /// Get the scheduled time a task can be started.
    pub fn get_scheduled(&self) -> Option<Timestamp> {
        self.get_timestamp("scheduled")
    }

    /// Get the modification time for this task.
    pub fn get_modified(&self) -> Option<Timestamp> {
        self.get_timestamp(Prop::Modified.as_ref())
    }

    /// Get the due time for this task.
    pub fn get_due(&self) -> Option<Timestamp> {
        self.get_timestamp(Prop::Due.as_ref())
    }

    /// Get the given timestamp property.
    ///
    /// This will return `None` if the property is not set, or if it is not a valid
    /// timestamp. Otherwise, a correctly parsed Timestamp is returned.
    pub(in crate::task) fn get_timestamp(&self, property: &str) -> Option<Timestamp> {
        if let Some(ts) = self.get(property) {
            if let Ok(ts) = ts.parse() {
                return Some(utc_timestamp(ts));
            }
            // if the value does not parse as an integer, default to None
        }
        None
    }

    /// Set or remove a value on this task, adding an Update operation to the
    /// set of operations.
    ///
    /// Setting a value to `None` removes that value from the task.
    ///
    /// This method does not have any special handling of the `modified` property.
    pub fn update(
        &mut self,
        property: impl Into<String>,
        value: Option<String>,
        ops: &mut Operations,
    ) {
        let property = property.into();
        let old_value = self.taskmap.get(&property).cloned();
        if let Some(value) = &value {
            self.taskmap.insert(property.clone(), value.clone());
        } else {
            self.taskmap.remove(&property);
        }
        ops.push(Operation::Update {
            uuid: self.uuid,
            property,
            old_value,
            value,
            timestamp: Utc::now(),
        });
    }

    /// Delete this task.
    ///
    /// Note that this is different from setting status to [`Deleted`](crate::Status::Deleted):
    /// the resulting operation removes the task from the database.
    ///
    /// Deletion may interact poorly with modifications to the same task on other replicas. For
    /// example, if a task is deleted on replica 1 and its description modified on replica 2, then
    /// after both replicas have fully synced, the resulting task will only have a `description`
    /// property.
    ///
    /// After this call, the `TaskData` value still exists but has no properties and should be
    /// dropped.
    pub fn delete(&mut self, ops: &mut Operations) {
        ops.push(Operation::Delete {
            uuid: self.uuid,
            old_task: std::mem::take(&mut self.taskmap),
        });
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::DateTime;
    use pretty_assertions::assert_eq;

    const TEST_UUID: Uuid = Uuid::from_u128(1234);

    fn make_ops(ops: &[Operation]) -> Operations {
        let mut res = Operations::new();
        for op in ops {
            res.push(op.clone());
        }
        res
    }

    /// Set all operations' timestamps to the given timestamp, to ease use of
    /// `assert_eq!`.
    fn set_all_timestamps(ops: &mut Operations, set_to: DateTime<Utc>) {
        for op in ops {
            if let Operation::Update { timestamp, .. } = op {
                *timestamp = set_to;
            }
        }
    }
    #[test]
    fn create() {
        let mut ops = Operations::new();
        let t = TaskData::create(TEST_UUID, &mut ops);
        assert_eq!(t.uuid, TEST_UUID);
        assert_eq!(t.get_uuid(), TEST_UUID);
        assert_eq!(t.taskmap, TaskMap::new());
        assert_eq!(ops, make_ops(&[Operation::Create { uuid: TEST_UUID }]));
    }

    #[test]
    fn get_uuid() {
        let t = TaskData::new(TEST_UUID, TaskMap::new());
        assert_eq!(t.get_uuid(), TEST_UUID);
    }

    #[test]
    fn get() {
        let t = TaskData::new(TEST_UUID, [("prop".to_string(), "val".to_string())].into());
        assert_eq!(t.get("prop"), Some("val"));
        assert_eq!(t.get("nosuch"), None)
    }

    #[test]
    fn has() {
        let t = TaskData::new(TEST_UUID, [("prop".to_string(), "val".to_string())].into());
        assert!(t.has("prop"));
        assert!(!t.has("nosuch"));
    }

    #[test]
    fn properties() {
        let t = TaskData::new(
            TEST_UUID,
            [
                ("prop1".to_string(), "val".to_string()),
                ("prop2".to_string(), "val".to_string()),
            ]
            .into(),
        );
        let mut props: Vec<_> = t.properties().collect();
        props.sort();
        assert_eq!(props, vec!["prop1", "prop2"]);
    }

    #[test]
    fn iter() {
        let t = TaskData::new(
            TEST_UUID,
            [
                ("prop1".to_string(), "val1".to_string()),
                ("prop2".to_string(), "val2".to_string()),
            ]
            .into(),
        );
        let mut props: Vec<_> = t.iter().map(|(p, v)| (p.as_str(), v.as_str())).collect();
        props.sort();
        assert_eq!(props, vec![("prop1", "val1"), ("prop2", "val2")]);
    }

    #[test]
    fn getters_defaults() {
        let t = TaskData::new(TEST_UUID, TaskMap::new());
        assert_eq!(t.get_status(), crate::Status::Pending);
        assert_eq!(t.get_description(), "");
        assert_eq!(t.get_priority(), "");
        assert_eq!(t.get_entry(), None);
        assert_eq!(t.get_wait(), None);
        assert_eq!(t.get_scheduled(), None);
        assert_eq!(t.get_due(), None);
        assert_eq!(t.get_modified(), None);
    }

    #[test]
    fn getters_values() {
        let secs = |s: i64| DateTime::<Utc>::from_timestamp(s, 0).unwrap();
        let t = TaskData::new(
            TEST_UUID,
            [
                ("status", "completed"),
                ("description", "a task"),
                ("priority", "H"),
                ("entry", "1631649600"),
                ("wait", "1631649601"),
                ("scheduled", "1631649602"),
                ("due", "1631649603"),
                ("modified", "1631649604"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        );
        assert_eq!(t.get_status(), crate::Status::Completed);
        assert_eq!(t.get_description(), "a task");
        assert_eq!(t.get_priority(), "H");
        assert_eq!(t.get_entry(), Some(secs(1631649600)));
        assert_eq!(t.get_wait(), Some(secs(1631649601)));
        assert_eq!(t.get_scheduled(), Some(secs(1631649602)));
        assert_eq!(t.get_due(), Some(secs(1631649603)));
        assert_eq!(t.get_modified(), Some(secs(1631649604)));
    }

    #[test]
    fn getters_invalid_timestamp() {
        let t = TaskData::new(
            TEST_UUID,
            [
                ("entry", "not-a-number"),
                ("wait", "not-a-number"),
                ("scheduled", "not-a-number"),
                ("due", "not-a-number"),
                ("modified", "not-a-number"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        );
        assert_eq!(t.get_entry(), None);
        assert_eq!(t.get_wait(), None);
        assert_eq!(t.get_scheduled(), None);
        assert_eq!(t.get_due(), None);
        assert_eq!(t.get_modified(), None);
    }

    #[test]
    fn update_new_prop() {
        let mut ops = Operations::new();
        let mut t = TaskData::new(TEST_UUID, TaskMap::new());
        t.update("prop1", Some("val1".into()), &mut ops);
        let now = Utc::now();
        set_all_timestamps(&mut ops, now);
        assert_eq!(
            ops,
            make_ops(&[Operation::Update {
                uuid: TEST_UUID,
                property: "prop1".into(),
                old_value: None,
                value: Some("val1".into()),
                timestamp: now,
            }])
        );
        assert_eq!(t.get("prop1"), Some("val1"));
    }

    #[test]
    fn update_existing_prop() {
        let mut ops = Operations::new();
        let mut t = TaskData::new(TEST_UUID, [("prop1".to_string(), "val".to_string())].into());
        t.update("prop1", Some("new".into()), &mut ops);
        let now = Utc::now();
        set_all_timestamps(&mut ops, now);
        assert_eq!(
            ops,
            make_ops(&[Operation::Update {
                uuid: TEST_UUID,
                property: "prop1".into(),
                old_value: Some("val".into()),
                value: Some("new".into()),
                timestamp: now,
            }])
        );
        assert_eq!(t.get("prop1"), Some("new"));
    }

    #[test]
    fn update_remove_prop() {
        let mut ops = Operations::new();
        let mut t = TaskData::new(TEST_UUID, [("prop1".to_string(), "val".to_string())].into());
        t.update("prop1", None, &mut ops);
        let now = Utc::now();
        set_all_timestamps(&mut ops, now);
        assert_eq!(
            ops,
            make_ops(&[Operation::Update {
                uuid: TEST_UUID,
                property: "prop1".into(),
                old_value: Some("val".into()),
                value: None,
                timestamp: now,
            }])
        );
        assert_eq!(t.get("prop1"), None);
    }

    #[test]
    fn delete() {
        let mut ops = Operations::new();
        let mut t = TaskData::new(TEST_UUID, [("prop1".to_string(), "val".to_string())].into());
        t.delete(&mut ops);
        assert_eq!(
            ops,
            make_ops(&[Operation::Delete {
                uuid: TEST_UUID,
                old_task: [("prop1".to_string(), "val".to_string())].into(),
            }])
        );
    }
}
