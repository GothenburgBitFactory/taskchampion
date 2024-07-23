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
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct TaskData {
    uuid: Uuid,
    // Temporarily pub(crate) to allow access from Task.
    pub(crate) taskmap: TaskMap,
}

impl TaskData {
    /// Constructor for a TaskData representing an existing task.
    pub(crate) fn new(uuid: Uuid, taskmap: TaskMap) -> Self {
        Self { uuid, taskmap }
    }

    /// Create a new, empty task with the given UUID.
    pub fn create(uuid: Uuid, operations: &mut Operations) -> Self {
        operations.add(Operation::Create { uuid });
        Self {
            uuid,
            taskmap: TaskMap::new(),
        }
    }

    /// Get this task's UUID.
    pub fn uuid(&self) -> Uuid {
        self.uuid
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

    /// Set or remove a value on this task, adding an Update operation to the
    /// set of operations.
    ///
    /// Setting a value to `None` removes that value from the task.
    pub fn update(
        &mut self,
        property: impl Into<String>,
        value: Option<String>,
        operations: &mut Operations,
    ) {
        let property = property.into();
        let old_value = self.taskmap.get(&property).cloned();
        if let Some(value) = &value {
            self.taskmap.insert(property.clone(), value.clone());
        } else {
            self.taskmap.remove(&property);
        }
        operations.add(Operation::Update {
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
    pub fn delete(self, operations: &mut Operations) {
        operations.add(Operation::Delete {
            uuid: self.uuid,
            old_task: self.taskmap,
        });
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use pretty_assertions::assert_eq;

    const TEST_UUID: Uuid = Uuid::from_u128(1234);

    fn make_ops(ops: &[Operation]) -> Operations {
        let mut res = Operations::new();
        for op in ops {
            res.add(op.clone());
        }
        res
    }

    #[test]
    fn create() {
        let mut ops = Operations::new();
        let t = TaskData::create(TEST_UUID, &mut ops);
        assert_eq!(t.uuid, TEST_UUID);
        assert_eq!(t.uuid(), TEST_UUID);
        assert_eq!(t.taskmap, TaskMap::new());
        assert_eq!(ops, make_ops(&[Operation::Create { uuid: TEST_UUID }]));
    }

    #[test]
    fn uuid() {
        let t = TaskData::new(TEST_UUID, TaskMap::new());
        assert_eq!(t.uuid(), TEST_UUID);
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
    fn update_new_prop() {
        let mut ops = Operations::new();
        let mut t = TaskData::new(TEST_UUID, TaskMap::new());
        t.update("prop1", Some("val1".into()), &mut ops);
        let now = Utc::now();
        ops.set_all_timestamps(now);
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
        ops.set_all_timestamps(now);
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
        ops.set_all_timestamps(now);
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
        let t = TaskData::new(TEST_UUID, [("prop1".to_string(), "val".to_string())].into());
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
