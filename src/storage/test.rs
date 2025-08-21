//! Tests for storage backends. This tests consistency across multiple method calls, to ensure that
//! all implementations are consistent.

use super::{Storage, TaskMap};
use crate::errors::Result;
use crate::storage::{taskmap_with, DEFAULT_BASE_VERSION};
use crate::{Error, Operation};
use anyhow::anyhow;
use chrono::Utc;
use pretty_assertions::assert_eq;
use uuid::Uuid;

/// Define a collection of storage tests that apply to all storage implementations.
macro_rules! storage_tests {
    ($storage:expr) => {
        #[tokio::test]
        async fn get_working_set_empty() -> $crate::errors::Result<()> {
            $crate::storage::test::get_working_set_empty($storage).await
        }

        #[tokio::test]
        async fn add_to_working_set() -> $crate::errors::Result<()> {
            $crate::storage::test::add_to_working_set($storage).await
        }

        #[tokio::test]
        async fn clear_working_set() -> $crate::errors::Result<()> {
            $crate::storage::test::clear_working_set($storage).await
        }

        #[tokio::test]
        async fn drop_transaction() -> $crate::errors::Result<()> {
            $crate::storage::test::drop_transaction($storage).await
        }

        #[tokio::test]
        async fn create() -> $crate::errors::Result<()> {
            $crate::storage::test::create($storage).await
        }

        #[tokio::test]
        async fn create_exists() -> $crate::errors::Result<()> {
            $crate::storage::test::create_exists($storage).await
        }

        #[tokio::test]
        async fn get_missing() -> $crate::errors::Result<()> {
            $crate::storage::test::get_missing($storage).await
        }

        #[tokio::test]
        async fn set_task() -> $crate::errors::Result<()> {
            $crate::storage::test::set_task($storage).await
        }

        #[tokio::test]
        async fn delete_task_missing() -> $crate::errors::Result<()> {
            $crate::storage::test::delete_task_missing($storage).await
        }

        #[tokio::test]
        async fn delete_task_exists() -> $crate::errors::Result<()> {
            $crate::storage::test::delete_task_exists($storage).await
        }

        #[tokio::test]
        async fn all_tasks_empty() -> $crate::errors::Result<()> {
            $crate::storage::test::all_tasks_empty($storage).await
        }

        #[tokio::test]
        async fn all_tasks_and_uuids() -> $crate::errors::Result<()> {
            $crate::storage::test::all_tasks_and_uuids($storage).await
        }

        #[tokio::test]
        async fn base_version_default() -> Result<()> {
            $crate::storage::test::base_version_default($storage).await
        }

        #[tokio::test]
        async fn base_version_setting() -> Result<()> {
            $crate::storage::test::base_version_setting($storage).await
        }

        #[tokio::test]
        async fn unsynced_operations() -> Result<()> {
            $crate::storage::test::unsynced_operations($storage).await
        }

        #[tokio::test]
        async fn remove_operations() -> Result<()> {
            $crate::storage::test::remove_operations($storage).await
        }

        #[tokio::test]
        async fn task_operations() -> Result<()> {
            $crate::storage::test::task_operations($storage).await
        }

        #[tokio::test]
        async fn sync_complete() -> Result<()> {
            $crate::storage::test::sync_complete($storage).await
        }

        #[tokio::test]
        async fn set_working_set_item() -> Result<()> {
            $crate::storage::test::set_working_set_item($storage).await
        }
    };
}
pub(crate) use storage_tests;

pub(super) async fn get_working_set_empty(storage: impl Storage) -> Result<()> {
    storage
        .txn(move |txn| {
            let ws = txn.get_working_set()?;
            assert_eq!(ws, vec![None]);
            Ok(())
        })
        .await
}

pub(super) async fn add_to_working_set(storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    storage
        .txn(move |txn| {
            txn.add_to_working_set(uuid1)?;
            txn.add_to_working_set(uuid2)?;
            txn.commit()
        })
        .await?;

    storage
        .txn(move |txn| {
            let ws = txn.get_working_set()?;
            assert_eq!(ws, vec![None, Some(uuid1), Some(uuid2)]);
            Ok(())
        })
        .await
}

pub(super) async fn clear_working_set(storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    storage
        .txn(move |txn| {
            txn.add_to_working_set(uuid1)?;
            txn.add_to_working_set(uuid2)?;
            txn.commit()
        })
        .await?;

    storage
        .txn(move |txn| {
            txn.clear_working_set()?;
            txn.add_to_working_set(uuid2)?;
            txn.add_to_working_set(uuid1)?;
            txn.commit()
        })
        .await?;

    storage
        .txn(move |txn| {
            let ws = txn.get_working_set()?;
            assert_eq!(ws, vec![None, Some(uuid2), Some(uuid1)]);
            Ok(())
        })
        .await
}

pub(super) async fn drop_transaction(storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    storage
        .txn(move |txn| {
            assert!(txn.create_task(uuid1)?);
            txn.commit()
        })
        .await?;

    let result: Result<()> = storage
        .txn(move |txn| {
            assert!(txn.create_task(uuid2)?);
            Err(Error::Other(anyhow!("Intentional error")))
        })
        .await;

    assert!(result.is_err());

    storage
        .txn(move |txn| {
            let uuids = txn.all_task_uuids()?;
            assert_eq!(uuids, [uuid1]);
            Ok(())
        })
        .await
}

pub(super) async fn create(storage: impl Storage) -> Result<()> {
    let uuid = Uuid::new_v4();

    storage
        .txn(move |txn| {
            assert!(txn.create_task(uuid)?);
            txn.commit()
        })
        .await?;

    storage
        .txn(move |txn| {
            let task = txn.get_task(uuid)?;
            assert_eq!(task, Some(taskmap_with(vec![])));
            Ok(())
        })
        .await
}

pub(super) async fn create_exists(storage: impl Storage) -> Result<()> {
    let uuid = Uuid::new_v4();

    storage
        .txn(move |txn| {
            assert!(txn.create_task(uuid)?);
            txn.commit()
        })
        .await?;

    storage
        .txn(move |txn| {
            assert!(!txn.create_task(uuid)?);
            txn.commit()
        })
        .await
}

pub(super) async fn get_missing(storage: impl Storage) -> Result<()> {
    let uuid = Uuid::new_v4();
    storage
        .txn(move |txn| {
            let task = txn.get_task(uuid)?;
            assert_eq!(task, None);
            Ok(())
        })
        .await
}

pub(super) async fn set_task(storage: impl Storage) -> Result<()> {
    let uuid = Uuid::new_v4();
    storage
        .txn(move |txn| {
            txn.set_task(uuid, taskmap_with(vec![("k".to_string(), "v".to_string())]))?;
            txn.commit()
        })
        .await?;

    storage
        .txn(move |txn| {
            let task = txn.get_task(uuid)?;
            assert_eq!(
                task,
                Some(taskmap_with(vec![("k".to_string(), "v".to_string())]))
            );
            Ok(())
        })
        .await
}

pub(super) async fn delete_task_missing(storage: impl Storage) -> Result<()> {
    let uuid = Uuid::new_v4();
    storage
        .txn(move |txn| {
            assert!(!txn.delete_task(uuid)?);
            Ok(())
        })
        .await
}

pub(super) async fn delete_task_exists(storage: impl Storage) -> Result<()> {
    let uuid = Uuid::new_v4();
    storage
        .txn(move |txn| {
            assert!(txn.create_task(uuid)?);
            txn.commit()
        })
        .await?;

    storage
        .txn(move |txn| {
            assert!(txn.delete_task(uuid)?);
            txn.commit()
        })
        .await
}

pub(super) async fn all_tasks_empty(storage: impl Storage) -> Result<()> {
    storage
        .txn(move |txn| {
            let tasks = txn.all_tasks()?;
            assert_eq!(tasks, vec![]);
            Ok(())
        })
        .await
}

pub(super) async fn all_tasks_and_uuids(storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    storage
        .txn(move |txn| {
            assert!(txn.create_task(uuid1)?);
            txn.set_task(
                uuid1,
                taskmap_with(vec![("num".to_string(), "1".to_string())]),
            )?;
            assert!(txn.create_task(uuid2)?);
            txn.set_task(
                uuid2,
                taskmap_with(vec![("num".to_string(), "2".to_string())]),
            )?;
            txn.commit()
        })
        .await?;

    storage
        .txn(move |txn| {
            let mut tasks = txn.all_tasks()?;

            // order is nondeterministic, so sort by uuid
            tasks.sort_by(|a, b| a.0.cmp(&b.0));

            let mut exp = vec![
                (
                    uuid1,
                    taskmap_with(vec![("num".to_string(), "1".to_string())]),
                ),
                (
                    uuid2,
                    taskmap_with(vec![("num".to_string(), "2".to_string())]),
                ),
            ];
            exp.sort_by(|a, b| a.0.cmp(&b.0));

            assert_eq!(tasks, exp);
            Ok(())
        })
        .await?;

    storage
        .txn(move |txn| {
            let mut uuids = txn.all_task_uuids()?;
            uuids.sort();

            let mut exp = vec![uuid1, uuid2];
            exp.sort();

            assert_eq!(uuids, exp);
            Ok(())
        })
        .await
}

pub(super) async fn base_version_default(storage: impl Storage) -> Result<()> {
    storage
        .txn(move |txn| {
            assert_eq!(txn.base_version()?, DEFAULT_BASE_VERSION);
            Ok(())
        })
        .await
}

pub(super) async fn base_version_setting(storage: impl Storage) -> Result<()> {
    let u = Uuid::new_v4();
    storage
        .txn(move |txn| {
            txn.set_base_version(u)?;
            txn.commit()
        })
        .await?;

    storage
        .txn(move |txn| {
            assert_eq!(txn.base_version()?, u);
            Ok(())
        })
        .await
}

pub(super) async fn unsynced_operations(storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    let uuid3 = Uuid::new_v4();

    // create some operations
    storage
        .txn(move |txn| {
            txn.add_operation(Operation::Create { uuid: uuid1 })?;
            txn.add_operation(Operation::Create { uuid: uuid2 })?;
            txn.commit()
        })
        .await?;

    // read them back
    storage
        .txn(move |txn| {
            let ops = txn.unsynced_operations()?;
            assert_eq!(
                ops,
                vec![
                    Operation::Create { uuid: uuid1 },
                    Operation::Create { uuid: uuid2 },
                ]
            );

            assert_eq!(txn.num_unsynced_operations()?, 2);
            Ok(())
        })
        .await?;

    // Sync them.
    storage
        .txn(move |txn| {
            txn.sync_complete()?;
            txn.commit()
        })
        .await?;

    // create some more operations (to test adding operations after sync)
    storage
        .txn(move |txn| {
            txn.add_operation(Operation::Create { uuid: uuid3 })?;
            txn.add_operation(Operation::Delete {
                uuid: uuid3,
                old_task: TaskMap::new(),
            })?;
            txn.commit()
        })
        .await?;

    // read them back
    storage
        .txn(move |txn| {
            let ops = txn.unsynced_operations()?;
            assert_eq!(
                ops,
                vec![
                    Operation::Create { uuid: uuid3 },
                    Operation::Delete {
                        uuid: uuid3,
                        old_task: TaskMap::new()
                    },
                ]
            );
            assert_eq!(txn.num_unsynced_operations()?, 2);
            Ok(())
        })
        .await?;

    // Remove the right one
    storage
        .txn(move |txn| {
            txn.remove_operation(Operation::Delete {
                uuid: uuid3,
                old_task: TaskMap::new(),
            })?;
            txn.commit()
        })
        .await?;

    // read the remaining op back
    storage
        .txn(move |txn| {
            let ops = txn.unsynced_operations()?;
            assert_eq!(ops, vec![Operation::Create { uuid: uuid3 },]);
            assert_eq!(txn.num_unsynced_operations()?, 1);
            Ok(())
        })
        .await
}

pub(super) async fn remove_operations(storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    let uuid3 = Uuid::new_v4();
    let uuid4 = Uuid::new_v4();

    // Create some tasks and operations.
    storage
        .txn(move |txn| {
            txn.create_task(uuid1)?;
            txn.create_task(uuid2)?;
            txn.create_task(uuid3)?;

            txn.add_operation(Operation::Create { uuid: uuid1 })?;
            txn.add_operation(Operation::Create { uuid: uuid2 })?;
            txn.add_operation(Operation::Create { uuid: uuid3 })?;
            txn.commit()
        })
        .await?;

    // Remove the uuid3 operation.
    storage
        .txn(move |txn| {
            txn.remove_operation(Operation::Create { uuid: uuid3 })?;
            assert_eq!(txn.num_unsynced_operations()?, 2);
            txn.commit()
        })
        .await?;

    // Remove a nonexistent operation
    storage
        .txn(move |txn| {
            assert!(txn
                .remove_operation(Operation::Create { uuid: uuid4 })
                .is_err());
            Ok(())
        })
        .await?;

    // Remove an operation that is not most recent.
    storage
        .txn(move |txn| {
            assert!(txn
                .remove_operation(Operation::Create { uuid: uuid1 })
                .is_err());
            Ok(())
        })
        .await?;

    // Mark operations as synced.
    storage
        .txn(move |txn| {
            txn.sync_complete()?;
            txn.commit()
        })
        .await?;

    // Try to remove the synced operation.
    storage
        .txn(move |txn| {
            assert!(txn
                .remove_operation(Operation::Create { uuid: uuid2 })
                .is_err());
            Ok(())
        })
        .await
}

pub(super) async fn task_operations(storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    let uuid3 = Uuid::new_v4();
    let now = Utc::now();

    // Create some tasks and operations.
    storage
        .txn(move |txn| {
            txn.create_task(uuid1)?;
            txn.create_task(uuid2)?;
            txn.create_task(uuid3)?;

            txn.add_operation(Operation::UndoPoint)?;
            txn.add_operation(Operation::Create { uuid: uuid1 })?;
            txn.add_operation(Operation::Create { uuid: uuid1 })?;
            txn.add_operation(Operation::UndoPoint)?;
            txn.add_operation(Operation::Delete {
                uuid: uuid2,
                old_task: TaskMap::new(),
            })?;
            txn.add_operation(Operation::Update {
                uuid: uuid3,
                property: "p".into(),
                old_value: None,
                value: Some("P".into()),
                timestamp: now,
            })?;
            txn.add_operation(Operation::Delete {
                uuid: uuid3,
                old_task: TaskMap::new(),
            })?;

            txn.commit()
        })
        .await?;

    // remove the last operation to verify it doesn't appear
    storage
        .txn(move |txn| {
            txn.remove_operation(Operation::Delete {
                uuid: uuid3,
                old_task: TaskMap::new(),
            })?;
            txn.commit()
        })
        .await?;

    // read them back
    storage
        .txn(move |txn| {
            let ops = txn.get_task_operations(uuid1)?;
            assert_eq!(
                ops,
                vec![
                    Operation::Create { uuid: uuid1 },
                    Operation::Create { uuid: uuid1 },
                ]
            );
            let ops = txn.get_task_operations(uuid2)?;
            assert_eq!(
                ops,
                vec![Operation::Delete {
                    uuid: uuid2,
                    old_task: TaskMap::new()
                }]
            );
            let ops = txn.get_task_operations(uuid3)?;
            assert_eq!(
                ops,
                vec![Operation::Update {
                    uuid: uuid3,
                    property: "p".into(),
                    old_value: None,
                    value: Some("P".into()),
                    timestamp: now,
                }]
            );
            Ok(())
        })
        .await?;

    // Sync and verify the task operations still exist.
    storage
        .txn(move |txn| {
            txn.sync_complete()?;

            let ops = txn.get_task_operations(uuid1)?;
            assert_eq!(ops.len(), 2);
            let ops = txn.get_task_operations(uuid2)?;
            assert_eq!(ops.len(), 1);
            let ops = txn.get_task_operations(uuid3)?;
            assert_eq!(ops.len(), 1);
            Ok(())
        })
        .await
}

pub(super) async fn sync_complete(storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    // Create some tasks and operations.
    storage
        .txn(move |txn| {
            txn.create_task(uuid1)?;
            txn.create_task(uuid2)?;

            txn.add_operation(Operation::Create { uuid: uuid1 })?;
            txn.add_operation(Operation::Create { uuid: uuid2 })?;

            txn.commit()
        })
        .await?;

    // Sync and verify the task operations still exist.
    storage
        .txn(move |txn| {
            txn.sync_complete()?;

            let ops = txn.get_task_operations(uuid1)?;
            assert_eq!(ops.len(), 1);
            let ops = txn.get_task_operations(uuid2)?;
            assert_eq!(ops.len(), 1);
            Ok(())
        })
        .await?;

    // Delete uuid2.
    storage
        .txn(move |txn| {
            txn.delete_task(uuid2)?;
            txn.add_operation(Operation::Delete {
                uuid: uuid2,
                old_task: TaskMap::new(),
            })?;

            txn.commit()
        })
        .await?;

    // Sync and verify that uuid1's operations still exist, but uuid2's do not.
    storage
        .txn(move |txn| {
            txn.sync_complete()?;

            let ops = txn.get_task_operations(uuid1)?;
            assert_eq!(ops.len(), 1);
            let ops = txn.get_task_operations(uuid2)?;
            assert_eq!(ops.len(), 0);
            Ok(())
        })
        .await
}

pub(super) async fn set_working_set_item(storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    storage
        .txn(move |txn| {
            txn.add_to_working_set(uuid1)?;
            txn.add_to_working_set(uuid2)?;
            txn.commit()
        })
        .await?;

    storage
        .txn(move |txn| {
            let ws = txn.get_working_set()?;
            assert_eq!(ws, vec![None, Some(uuid1), Some(uuid2)]);
            Ok(())
        })
        .await?;

    // Clear one item
    storage
        .txn(move |txn| {
            txn.set_working_set_item(1, None)?;
            txn.commit()
        })
        .await?;

    storage
        .txn(move |txn| {
            let ws = txn.get_working_set()?;
            assert_eq!(ws, vec![None, None, Some(uuid2)]);
            Ok(())
        })
        .await?;

    // Override item
    storage
        .txn(move |txn| {
            txn.set_working_set_item(2, Some(uuid1))?;
            txn.commit()
        })
        .await?;

    storage
        .txn(move |txn| {
            let ws = txn.get_working_set()?;
            assert_eq!(ws, vec![None, None, Some(uuid1)]);
            Ok(())
        })
        .await?;

    // Set the last item to None
    storage
        .txn(move |txn| {
            txn.set_working_set_item(1, Some(uuid1))?;
            txn.set_working_set_item(2, None)?;
            txn.commit()
        })
        .await?;

    storage
        .txn(move |txn| {
            let ws = txn.get_working_set()?;
            // Note no trailing `None`.
            assert_eq!(ws, vec![None, Some(uuid1)]);
            Ok(())
        })
        .await
}
