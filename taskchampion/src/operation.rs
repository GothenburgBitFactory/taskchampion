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
}
