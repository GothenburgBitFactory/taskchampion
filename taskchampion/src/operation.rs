use crate::storage::TaskMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::cmp::{Ord, Ordering};
use uuid::Uuid;

/// An Operation defines a single change to the task database, as stored locally in the replica.
///
/// Operations are the means by which changes are made to the database, typically batched together
/// into [`Operations`] and committed to the replica.
#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub enum Operation {
    /// Create a new task.
    ///
    /// On undo, the task is deleted.
    Create { uuid: Uuid },

    /// Delete an existing task.
    ///
    /// On undo, the task's data is restored from old_task.
    Delete { uuid: Uuid, old_task: TaskMap },

    /// Update an existing task, setting the given property to the given value.  If the value is
    /// None, then the corresponding property is deleted.
    ///
    /// On undo, the property is set back to its previous value.
    Update {
        uuid: Uuid,
        property: String,
        old_value: Option<String>,
        value: Option<String>,
        timestamp: DateTime<Utc>,
    },

    /// Mark a point in the operations history to which the user might like to undo.  Users
    /// typically want to undo more than one operation at a time (for example, most changes update
    /// both the `modified` property and some other task property -- the user would like to "undo"
    /// both updates at the same time).  Applying an UndoPoint does nothing.
    UndoPoint,
}

impl Operation {
    /// Determine whether this is an undo point.
    pub fn is_undo_point(&self) -> bool {
        self == &Self::UndoPoint
    }

    /// Get the UUID for this function, if it has one.
    pub fn get_uuid(&self) -> Option<Uuid> {
        match self {
            Operation::Create { uuid: u } => Some(*u),
            Operation::Delete { uuid: u, .. } => Some(*u),
            Operation::Update { uuid: u, .. } => Some(*u),
            Operation::UndoPoint => None,
        }
    }
}

impl PartialOrd for Operation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Operation {
    /// Define an order for operations.
    ///
    /// First, orders by Uuid, with all UndoPoints first. Then, by type, with Creates, then Updates,
    /// then Deletes. Updates are ordered by timestamp and, where that is equal, by the remaining
    /// fields. This ordering is intended to be "human-readable", even in confusing situations like
    /// multiple creations of the same task.
    fn cmp(&self, other: &Self) -> Ordering {
        use Operation::*;
        use Ordering::*;
        fn type_idx(op: &Operation) -> u8 {
            match op {
                UndoPoint => 0,
                Create { .. } => 1,
                Update { .. } => 2,
                Delete { .. } => 3,
            }
        }
        Equal
            // First sort by UUID. UndoPoint's have `None` as uuid, and are thus sorted first.
            .then(self.get_uuid().cmp(&other.get_uuid()))
            // Then sort by type.
            .then(type_idx(self).cmp(&type_idx(other)))
            // Then sort within the same type. Only match arms with `self` and `other` the same
            // type are possible, as we have already sorted by type.
            .then_with(|| {
                match (self, other) {
                    (Create { uuid: uuid1 }, Create { uuid: uuid2 }) => uuid1.cmp(uuid2),
                    (
                        Delete {
                            uuid: uuid1,
                            old_task: old_task1,
                        },
                        Delete {
                            uuid: uuid2,
                            old_task: old_task2,
                        },
                    ) => uuid1.cmp(uuid2).then_with(|| {
                        let mut old_task1 = old_task1.iter().collect::<Vec<_>>();
                        old_task1.sort();
                        let mut old_task2 = old_task2.iter().collect::<Vec<_>>();
                        old_task2.sort();
                        old_task1.cmp(&old_task2)
                    }),
                    (
                        Update {
                            uuid: uuid1,
                            property: property1,
                            value: value1,
                            old_value: old_value1,
                            timestamp: timestamp1,
                        },
                        Update {
                            uuid: uuid2,
                            property: property2,
                            value: value2,
                            old_value: old_value2,
                            timestamp: timestamp2,
                        },
                    ) => Equal
                        // Sort Updates principally by timestamp.
                        .then(uuid1.cmp(uuid2))
                        .then(timestamp1.cmp(timestamp2))
                        .then(property1.cmp(property2))
                        .then(value1.cmp(value2))
                        .then(old_value1.cmp(old_value2)),
                    (UndoPoint, UndoPoint) => Equal,
                    _ => unreachable!(),
                }
            })
    }
}

/// Operations are a sequence of [`Operation`] values, which can be committed in a single
/// transaction with [`Replica::commit_operations`](crate::Replica::commit_operations).
pub type Operations = Vec<Operation>;

#[cfg(test)]
mod test {
    use super::*;
    use crate::errors::Result;
    use chrono::Utc;
    use pretty_assertions::assert_eq;

    use Operation::*;

    #[test]
    fn test_json_create() -> Result<()> {
        let uuid = Uuid::new_v4();
        let op = Create { uuid };
        let json = serde_json::to_string(&op)?;
        assert_eq!(json, format!(r#"{{"Create":{{"uuid":"{}"}}}}"#, uuid));
        let deser: Operation = serde_json::from_str(&json)?;
        assert_eq!(deser, op);
        Ok(())
    }

    #[test]
    fn test_json_delete() -> Result<()> {
        let uuid = Uuid::new_v4();
        let old_task = vec![("foo".into(), "bar".into())].drain(..).collect();
        let op = Delete { uuid, old_task };
        let json = serde_json::to_string(&op)?;
        assert_eq!(
            json,
            format!(
                r#"{{"Delete":{{"uuid":"{}","old_task":{{"foo":"bar"}}}}}}"#,
                uuid
            )
        );
        let deser: Operation = serde_json::from_str(&json)?;
        assert_eq!(deser, op);
        Ok(())
    }

    #[test]
    fn test_json_update() -> Result<()> {
        let uuid = Uuid::new_v4();
        let timestamp = Utc::now();

        let op = Update {
            uuid,
            property: "abc".into(),
            old_value: Some("true".into()),
            value: Some("false".into()),
            timestamp,
        };

        let json = serde_json::to_string(&op)?;
        assert_eq!(
            json,
            format!(
                r#"{{"Update":{{"uuid":"{}","property":"abc","old_value":"true","value":"false","timestamp":"{:?}"}}}}"#,
                uuid, timestamp,
            )
        );
        let deser: Operation = serde_json::from_str(&json)?;
        assert_eq!(deser, op);
        Ok(())
    }

    #[test]
    fn test_json_update_none() -> Result<()> {
        let uuid = Uuid::new_v4();
        let timestamp = Utc::now();

        let op = Update {
            uuid,
            property: "abc".into(),
            old_value: None,
            value: None,
            timestamp,
        };

        let json = serde_json::to_string(&op)?;
        assert_eq!(
            json,
            format!(
                r#"{{"Update":{{"uuid":"{}","property":"abc","old_value":null,"value":null,"timestamp":"{:?}"}}}}"#,
                uuid, timestamp,
            )
        );
        let deser: Operation = serde_json::from_str(&json)?;
        assert_eq!(deser, op);
        Ok(())
    }

    #[test]
    fn op_order() {
        let mut uuid1 = Uuid::new_v4();
        let mut uuid2 = Uuid::new_v4();
        if uuid2 < uuid1 {
            (uuid1, uuid2) = (uuid2, uuid1);
        }
        let now1 = Utc::now();
        let now2 = now1 + chrono::Duration::seconds(1);

        let create1 = Operation::Create { uuid: uuid1 };
        let create2 = Operation::Create { uuid: uuid2 };

        let update1 = Operation::Update {
            uuid: uuid1,
            property: "prop1".into(),
            old_value: None,
            value: None,
            timestamp: now1,
        };
        let update2_now1_prop1_val1 = Operation::Update {
            uuid: uuid2,
            property: "prop1".into(),
            old_value: None,
            value: None,
            timestamp: now1,
        };
        let update2_now1_prop1_val2 = Operation::Update {
            uuid: uuid2,
            property: "prop1".into(),
            old_value: None,
            value: Some("v".into()),
            timestamp: now1,
        };
        let update2_now1_prop2_val1 = Operation::Update {
            uuid: uuid2,
            property: "prop2".into(),
            old_value: None,
            value: None,
            timestamp: now1,
        };
        let update2_now1_prop2_val2 = Operation::Update {
            uuid: uuid2,
            property: "prop2".into(),
            old_value: None,
            value: Some("v".into()),
            timestamp: now1,
        };
        let update2_now2_prop1_val1 = Operation::Update {
            uuid: uuid2,
            property: "prop1".into(),
            old_value: None,
            value: None,
            timestamp: now2,
        };
        let update2_now2_prop1_val2 = Operation::Update {
            uuid: uuid2,
            property: "prop1".into(),
            old_value: None,
            value: Some("v".into()),
            timestamp: now2,
        };
        let update2_now2_prop2_val1 = Operation::Update {
            uuid: uuid2,
            property: "prop2".into(),
            old_value: None,
            value: None,
            timestamp: now2,
        };
        let update2_now2_prop2_val2_oldval1 = Operation::Update {
            uuid: uuid2,
            property: "prop2".into(),
            old_value: None,
            value: Some("v".into()),
            timestamp: now2,
        };
        let update2_now2_prop2_val2_oldval2 = Operation::Update {
            uuid: uuid2,
            property: "prop2".into(),
            old_value: Some("v2".into()),
            value: Some("v".into()),
            timestamp: now2,
        };
        let update2_now2_prop2_val2_oldval3 = Operation::Update {
            uuid: uuid2,
            property: "prop2".into(),
            old_value: Some("v3".into()),
            value: Some("v".into()),
            timestamp: now2,
        };

        let delete1 = Operation::Delete {
            uuid: uuid1,
            old_task: TaskMap::from([("a".to_string(), "a".to_string())]),
        };
        let delete1b = Operation::Delete {
            uuid: uuid1,
            old_task: TaskMap::from([("b".to_string(), "b".to_string())]),
        };
        let delete2 = Operation::Delete {
            uuid: uuid2,
            old_task: TaskMap::from([("a".to_string(), "a".to_string())]),
        };

        let undo_point = Operation::UndoPoint;

        // Specify order all of these operations should be in.
        let total_order = vec![
            undo_point,
            create1,
            update1,
            delete1,
            delete1b,
            create2,
            update2_now1_prop1_val1,
            update2_now1_prop1_val2,
            update2_now1_prop2_val1,
            update2_now1_prop2_val2,
            update2_now2_prop1_val1,
            update2_now2_prop1_val2,
            update2_now2_prop2_val1,
            update2_now2_prop2_val2_oldval1,
            update2_now2_prop2_val2_oldval2,
            update2_now2_prop2_val2_oldval3,
            delete2,
        ];

        // Check that each operation compares the same as the comparison of its index. This is more
        // thorough than just sorting a list, which would not perform every pairwise comparison.
        for i in 0..total_order.len() {
            for j in 0..total_order.len() {
                let a = &total_order[i];
                let b = &total_order[j];
                assert_eq!(a.cmp(b), i.cmp(&j), "{a:?} <??> {b:?} ([{i}] <??> [{j}])");
            }
        }
    }
}
