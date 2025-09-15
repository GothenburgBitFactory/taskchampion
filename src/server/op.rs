use crate::operation::Operation;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A SyncOp defines a single change to the task database, that can be synchronized
/// via a server.
#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub(crate) enum SyncOp {
    /// Create a new task.
    ///
    /// On application, if the task already exists, the operation does nothing.
    Create { uuid: Uuid },

    /// Delete an existing task.
    ///
    /// On application, if the task does not exist, the operation does nothing.
    Delete { uuid: Uuid },

    /// Update an existing task, setting the given property to the given value.  If the value is
    /// None, then the corresponding property is deleted.
    ///
    /// If the given task does not exist, the operation does nothing.
    Update {
        uuid: Uuid,
        property: String,
        value: Option<String>,
        timestamp: DateTime<Utc>,
    },
}

use SyncOp::*;

impl SyncOp {
    // Transform takes two operations A and B that happened concurrently and produces two
    // operations A' and B' such that `apply(apply(S, A), B') = apply(apply(S, B), A')`. This
    // function is used to serialize operations in a process similar to a Git "rebase".
    //
    //        *
    //       / \
    //  op1 /   \ op2
    //     /     \
    //    *       *
    //
    // this function "completes the diamond:
    //
    //    *       *
    //     \     /
    // op2' \   / op1'
    //       \ /
    //        *
    //
    // such that applying op2' after op1 has the same effect as applying op1' after op2.  This
    // allows two different systems which have already applied op1 and op2, respectively, and thus
    // reached different states, to return to the same state by applying op2' and op1',
    // respectively.
    pub(crate) fn transform(
        operation1: SyncOp,
        operation2: SyncOp,
    ) -> (Option<SyncOp>, Option<SyncOp>) {
        match (&operation1, &operation2) {
            // Two creations or deletions of the same uuid reach the same state, so there's no need
            // for any further operations to bring the state together.
            (&Create { uuid: uuid1 }, &Create { uuid: uuid2 }) if uuid1 == uuid2 => (None, None),
            (&Delete { uuid: uuid1 }, &Delete { uuid: uuid2 }) if uuid1 == uuid2 => (None, None),

            // Given a create and a delete of the same task, one of the operations is invalid: the
            // create implies the task does not exist, but the delete implies it exists.  Somewhat
            // arbitrarily, we prefer the Create
            (&Create { uuid: uuid1 }, &Delete { uuid: uuid2 }) if uuid1 == uuid2 => {
                (Some(operation1), None)
            }
            (&Delete { uuid: uuid1 }, &Create { uuid: uuid2 }) if uuid1 == uuid2 => {
                (None, Some(operation2))
            }

            // And again from an Update and a Create, prefer the Update
            (&Update { uuid: uuid1, .. }, &Create { uuid: uuid2 }) if uuid1 == uuid2 => {
                (Some(operation1), None)
            }
            (&Create { uuid: uuid1 }, &Update { uuid: uuid2, .. }) if uuid1 == uuid2 => {
                (None, Some(operation2))
            }

            // Given a delete and an update, prefer the delete
            (&Update { uuid: uuid1, .. }, &Delete { uuid: uuid2 }) if uuid1 == uuid2 => {
                (None, Some(operation2))
            }
            (&Delete { uuid: uuid1 }, &Update { uuid: uuid2, .. }) if uuid1 == uuid2 => {
                (Some(operation1), None)
            }

            // Two updates to the same property of the same task might conflict.
            (
                Update {
                    uuid: uuid1,
                    property: property1,
                    value: value1,
                    timestamp: timestamp1,
                },
                Update {
                    uuid: uuid2,
                    property: property2,
                    value: value2,
                    timestamp: timestamp2,
                },
            ) if uuid1 == uuid2 && property1 == property2 => {
                // if the value is the same, there's no conflict
                if value1 == value2 {
                    (None, None)
                } else if timestamp1 < timestamp2 {
                    // prefer the later modification
                    (None, Some(operation2))
                } else {
                    // prefer the later modification or, if the modifications are the same,
                    // just choose one of them
                    (Some(operation1), None)
                }
            }

            // anything else is not a conflict of any sort, so return the operations unchanged
            (_, _) => (Some(operation1), Some(operation2)),
        }
    }

    /// Convert the public Operation type into a SyncOp. `UndoPoint` operations are converted to
    /// `None`.
    pub(crate) fn from_op(op: Operation) -> Option<Self> {
        match op {
            Operation::Create { uuid } => Some(SyncOp::Create { uuid }),
            Operation::Delete { uuid, .. } => Some(SyncOp::Delete { uuid }),
            Operation::Update {
                uuid,
                property,
                value,
                timestamp,
                ..
            } => Some(SyncOp::Update {
                uuid,
                property,
                value,
                timestamp,
            }),
            Operation::UndoPoint => None,
        }
    }

    /// Convert a SyncOp to an [`Operation`], lossily.
    ///
    /// The `Operation` type keeps old values to support undoing operations, but this information
    /// is not preserved in `SyncOp`. This function makes those values (`old_task` for `Delete` and
    /// `old_value` for `Update`) to empty.
    pub(crate) fn into_op(self) -> Operation {
        match self {
            Create { uuid } => Operation::Create { uuid },
            Delete { uuid } => Operation::Delete {
                uuid,
                old_task: crate::storage::TaskMap::new(),
            },
            Update {
                uuid,
                property,
                value,
                timestamp,
            } => Operation::Update {
                uuid,
                property,
                value,
                timestamp,
                old_value: None,
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::TaskMap;
    use crate::taskdb::TaskDb;
    use crate::Operations;
    use crate::{errors::Result, storage::inmemory::InMemoryStorage};
    use chrono::{Duration, Utc};
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;

    #[test]
    fn test_json_create() -> Result<()> {
        let uuid = Uuid::new_v4();
        let op = Create { uuid };
        let json = serde_json::to_string(&op)?;
        assert_eq!(json, format!(r#"{{"Create":{{"uuid":"{}"}}}}"#, uuid));
        let deser: SyncOp = serde_json::from_str(&json)?;
        assert_eq!(deser, op);
        Ok(())
    }

    #[test]
    fn test_json_delete() -> Result<()> {
        let uuid = Uuid::new_v4();
        let op = Delete { uuid };
        let json = serde_json::to_string(&op)?;
        assert_eq!(json, format!(r#"{{"Delete":{{"uuid":"{}"}}}}"#, uuid));
        let deser: SyncOp = serde_json::from_str(&json)?;
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
            value: Some("false".into()),
            timestamp,
        };

        let json = serde_json::to_string(&op)?;
        assert_eq!(
            json,
            format!(
                r#"{{"Update":{{"uuid":"{}","property":"abc","value":"false","timestamp":"{:?}"}}}}"#,
                uuid, timestamp,
            )
        );
        let deser: SyncOp = serde_json::from_str(&json)?;
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
            value: None,
            timestamp,
        };

        let json = serde_json::to_string(&op)?;
        assert_eq!(
            json,
            format!(
                r#"{{"Update":{{"uuid":"{}","property":"abc","value":null,"timestamp":"{:?}"}}}}"#,
                uuid, timestamp,
            )
        );
        let deser: SyncOp = serde_json::from_str(&json)?;
        assert_eq!(deser, op);
        Ok(())
    }

    async fn test_transform(
        setup: Option<SyncOp>,
        o1: SyncOp,
        o2: SyncOp,
        exp1p: Option<SyncOp>,
        exp2p: Option<SyncOp>,
    ) {
        let (o1p, o2p) = SyncOp::transform(o1.clone(), o2.clone());
        assert_eq!((&o1p, &o2p), (&exp1p, &exp2p));

        // check that the two operation sequences have the same effect, enforcing the invariant of
        // the transform function.
        let mut db1 = TaskDb::new(InMemoryStorage::new());
        let mut ops1 = Operations::new();
        if let Some(o) = setup.clone() {
            ops1.push(o.into_op());
        }
        ops1.push(o1.into_op());
        if let Some(o) = o2p {
            ops1.push(o.into_op());
        }
        db1.commit_operations(ops1, |_| false).await.unwrap();

        let mut db2 = TaskDb::new(InMemoryStorage::new());
        let mut ops2 = Operations::new();
        if let Some(o) = setup {
            ops2.push(o.into_op());
        }
        ops2.push(o2.into_op());
        if let Some(o) = o1p {
            ops2.push(o.into_op());
        }
        db2.commit_operations(ops2, |_| false).await.unwrap();

        assert_eq!(db1.sorted_tasks().await, db2.sorted_tasks().await);
    }

    #[tokio::test]
    async fn test_unrelated_create() {
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();

        test_transform(
            None,
            Create { uuid: uuid1 },
            Create { uuid: uuid2 },
            Some(Create { uuid: uuid1 }),
            Some(Create { uuid: uuid2 }),
        )
        .await;
    }

    #[tokio::test]
    async fn test_related_updates_different_props() {
        let uuid = Uuid::new_v4();
        let timestamp = Utc::now();

        test_transform(
            Some(Create { uuid }),
            Update {
                uuid,
                property: "abc".into(),
                value: Some("true".into()),
                timestamp,
            },
            Update {
                uuid,
                property: "def".into(),
                value: Some("false".into()),
                timestamp,
            },
            Some(Update {
                uuid,
                property: "abc".into(),
                value: Some("true".into()),
                timestamp,
            }),
            Some(Update {
                uuid,
                property: "def".into(),
                value: Some("false".into()),
                timestamp,
            }),
        )
        .await;
    }

    #[tokio::test]
    async fn test_related_updates_same_prop() {
        let uuid = Uuid::new_v4();
        let timestamp1 = Utc::now();
        let timestamp2 = timestamp1 + Duration::seconds(10);

        test_transform(
            Some(Create { uuid }),
            Update {
                uuid,
                property: "abc".into(),
                value: Some("true".into()),
                timestamp: timestamp1,
            },
            Update {
                uuid,
                property: "abc".into(),
                value: Some("false".into()),
                timestamp: timestamp2,
            },
            None,
            Some(Update {
                uuid,
                property: "abc".into(),
                value: Some("false".into()),
                timestamp: timestamp2,
            }),
        )
        .await;
    }

    #[tokio::test]
    async fn test_related_updates_same_prop_same_time() {
        let uuid = Uuid::new_v4();
        let timestamp = Utc::now();

        test_transform(
            Some(Create { uuid }),
            Update {
                uuid,
                property: "abc".into(),
                value: Some("true".into()),
                timestamp,
            },
            Update {
                uuid,
                property: "abc".into(),
                value: Some("false".into()),
                timestamp,
            },
            Some(Update {
                uuid,
                property: "abc".into(),
                value: Some("true".into()),
                timestamp,
            }),
            None,
        )
        .await;
    }

    fn uuid_strategy() -> impl Strategy<Value = Uuid> {
        prop_oneof![
            Just(Uuid::parse_str("83a2f9ef-f455-4195-b92e-a54c161eebfc").unwrap()),
            Just(Uuid::parse_str("56e0be07-c61f-494c-a54c-bdcfdd52d2a7").unwrap()),
            Just(Uuid::parse_str("4b7ed904-f7b0-4293-8a10-ad452422c7b3").unwrap()),
            Just(Uuid::parse_str("9bdd0546-07c8-4e1f-a9bc-9d6299f4773b").unwrap()),
        ]
    }

    fn operation_strategy() -> impl Strategy<Value = SyncOp> {
        prop_oneof![
            uuid_strategy().prop_map(|uuid| Create { uuid }),
            uuid_strategy().prop_map(|uuid| Delete { uuid }),
            (uuid_strategy(), "(title|project|status)").prop_map(|(uuid, property)| {
                Update {
                    uuid,
                    property,
                    value: Some("true".into()),
                    timestamp: Utc::now(),
                }
            }),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig {
          cases: 1024, .. ProptestConfig::default()
        })]
        #[test]
        /// Check that, given two operations, their transform produces the same result, as
        /// required by the invariant.
        fn transform_invariant_holds(o1 in operation_strategy(), o2 in operation_strategy()) {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let (o1p, o2p) = SyncOp::transform(o1.clone(), o2.clone());

                    let mut ops1 = Operations::new();
                    let mut ops2 = Operations::new();
                    let mut db1 = TaskDb::new(InMemoryStorage::new());
                    let mut db2 = TaskDb::new(InMemoryStorage::new());

                    // Ensure that any expected tasks already exist
                    for o in [&o1, &o2] {
                        match o {
                            Update { uuid, .. } | Delete { uuid } => {
                                ops1.push(Operation::Create { uuid: *uuid });
                                ops2.push(Operation::Create { uuid: *uuid });
                            }
                            _ => {},
                        }
                    }

                    ops1.push(o1.into_op());
                    ops2.push(o2.into_op());

                    if let Some(o2p) = o2p {
                        ops1.push(o2p.into_op());
                    }
                    if let Some(o1p) = o1p {
                        ops2.push(o1p.into_op());
                    }

                    db1.commit_operations(ops1, |_| false).await.unwrap();
                    db2.commit_operations(ops2, |_| false).await.unwrap();

                    assert_eq!(db1.sorted_tasks().await, db2.sorted_tasks().await);
                });
        }
    }

    #[test]
    fn test_from_op_create() {
        let uuid = Uuid::new_v4();
        assert_eq!(
            SyncOp::from_op(Operation::Create { uuid }),
            Some(SyncOp::Create { uuid })
        );
    }

    #[test]
    fn test_from_op_delete() {
        let uuid = Uuid::new_v4();
        assert_eq!(
            SyncOp::from_op(Operation::Delete {
                uuid,
                old_task: TaskMap::new()
            }),
            Some(SyncOp::Delete { uuid })
        );
    }

    #[test]
    fn test_from_op_update() {
        let uuid = Uuid::new_v4();
        let timestamp = Utc::now();
        assert_eq!(
            SyncOp::from_op(Operation::Update {
                uuid,
                property: "prop".into(),
                old_value: Some("foo".into()),
                value: Some("v".into()),
                timestamp,
            }),
            Some(SyncOp::Update {
                uuid,
                property: "prop".into(),
                value: Some("v".into()),
                timestamp,
            })
        );
    }

    #[test]
    fn test_from_op_undo_point() {
        assert_eq!(SyncOp::from_op(Operation::UndoPoint), None);
    }
}
