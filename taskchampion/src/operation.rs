use crate::storage::TaskMap;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
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
}

/// Operations contains a sequence of [`Operation`] values, which can be committed in a single
/// transaction with [`Replica::commit`](crate::Replica::commit).
#[derive(PartialEq, Eq, Debug, Default)]
pub struct Operations(Vec<Operation>);

impl Operations {
    /// Create a new, empty set of operations.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new set of operations beginning with an undo point.
    ///
    /// This is a convenience method to begin creating a batch of operations that can be
    /// undone together.
    pub fn new_with_undo_point() -> Self {
        let mut ops = Self::default();
        ops.add(Operation::UndoPoint);
        ops
    }

    /// Add a new operation to the end of this sequence.
    pub fn add(&mut self, op: Operation) {
        self.0.push(op);
    }

    /// For tests, it's useful to set the timestamps of all updates to the same value.
    #[cfg(test)]
    pub fn set_all_timestamps(&mut self, set_to: DateTime<Utc>) {
        for op in &mut self.0 {
            if let Operation::Update { timestamp, .. } = op {
                *timestamp = set_to;
            }
        }
    }
}

impl IntoIterator for Operations {
    type Item = Operation;
    type IntoIter = std::vec::IntoIter<Operation>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a> IntoIterator for &'a Operations {
    type Item = &'a Operation;
    type IntoIter = std::slice::Iter<'a, Operation>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

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
    fn operations_empty() {
        let ops = Operations::new();
        assert_eq!(ops.0, vec![]);
    }

    #[test]
    fn operations_with_undo_point() {
        let ops = Operations::new_with_undo_point();
        assert_eq!(ops.0, vec![UndoPoint]);
    }

    #[test]
    fn operations_add() {
        let uuid = Uuid::new_v4();
        let mut ops = Operations::new();
        ops.add(Create { uuid });
        assert_eq!(ops.0, vec![Create { uuid }]);
    }
}
