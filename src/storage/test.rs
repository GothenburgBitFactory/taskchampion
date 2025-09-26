//! Tests for storage backends. This tests consistency across multiple method calls, to ensure that
//! all implementations are consistent.

use super::{Storage, TaskMap};
use crate::errors::Result;
use crate::storage::{taskmap_with, DEFAULT_BASE_VERSION};
use crate::Operation;
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

pub(super) async fn get_working_set_empty(mut storage: impl Storage) -> Result<()> {
    {
        let mut txn = storage.txn().await?;
        let ws = txn.get_working_set().await?;
        assert_eq!(ws, vec![None]);
    }

    Ok(())
}

pub(super) async fn add_to_working_set(mut storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    {
        let mut txn = storage.txn().await?;
        txn.add_to_working_set(uuid1).await?;
        txn.add_to_working_set(uuid2).await?;
        txn.commit().await?;
    }

    {
        let mut txn = storage.txn().await?;
        let ws = txn.get_working_set().await?;
        assert_eq!(ws, vec![None, Some(uuid1), Some(uuid2)]);
    }

    Ok(())
}

pub(super) async fn clear_working_set(mut storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    {
        let mut txn = storage.txn().await?;
        txn.add_to_working_set(uuid1).await?;
        txn.add_to_working_set(uuid2).await?;
        txn.commit().await?;
    }

    {
        let mut txn = storage.txn().await?;
        txn.clear_working_set().await?;
        txn.add_to_working_set(uuid2).await?;
        txn.add_to_working_set(uuid1).await?;
        txn.commit().await?;
    }

    {
        let mut txn = storage.txn().await?;
        let ws = txn.get_working_set().await?;
        assert_eq!(ws, vec![None, Some(uuid2), Some(uuid1)]);
    }

    Ok(())
}

pub(super) async fn drop_transaction(mut storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    {
        let mut txn = storage.txn().await?;
        assert!(txn.create_task(uuid1).await?);
        txn.commit().await?;
    }

    {
        let mut txn = storage.txn().await?;
        assert!(txn.create_task(uuid2).await?);
        std::mem::drop(txn); // Unnecessary explicit drop of transaction
    }

    {
        let mut txn = storage.txn().await?;
        let uuids = txn.all_task_uuids().await?;

        assert_eq!(uuids, [uuid1]);
    }

    Ok(())
}

pub(super) async fn create(mut storage: impl Storage) -> Result<()> {
    let uuid = Uuid::new_v4();
    {
        let mut txn = storage.txn().await?;
        assert!(txn.create_task(uuid).await?);
        txn.commit().await?;
    }
    {
        let mut txn = storage.txn().await?;
        let task = txn.get_task(uuid).await?;
        assert_eq!(task, Some(taskmap_with(vec![])));
    }
    Ok(())
}

pub(super) async fn create_exists(mut storage: impl Storage) -> Result<()> {
    let uuid = Uuid::new_v4();
    {
        let mut txn = storage.txn().await?;
        assert!(txn.create_task(uuid).await?);
        txn.commit().await?;
    }
    {
        let mut txn = storage.txn().await?;
        assert!(!txn.create_task(uuid).await?);
        txn.commit().await?;
    }
    Ok(())
}

pub(super) async fn get_missing(mut storage: impl Storage) -> Result<()> {
    let uuid = Uuid::new_v4();
    {
        let mut txn = storage.txn().await?;
        let task = txn.get_task(uuid).await?;
        assert_eq!(task, None);
    }
    Ok(())
}

pub(super) async fn set_task(mut storage: impl Storage) -> Result<()> {
    let uuid = Uuid::new_v4();
    {
        let mut txn = storage.txn().await?;
        txn.set_task(uuid, taskmap_with(vec![("k".to_string(), "v".to_string())]))
            .await?;
        txn.commit().await?;
    }
    {
        let mut txn = storage.txn().await?;
        let task = txn.get_task(uuid).await?;
        assert_eq!(
            task,
            Some(taskmap_with(vec![("k".to_string(), "v".to_string())]))
        );
    }
    Ok(())
}

pub(super) async fn delete_task_missing(mut storage: impl Storage) -> Result<()> {
    let uuid = Uuid::new_v4();
    {
        let mut txn = storage.txn().await?;
        assert!(!txn.delete_task(uuid).await?);
    }
    Ok(())
}

pub(super) async fn delete_task_exists(mut storage: impl Storage) -> Result<()> {
    let uuid = Uuid::new_v4();
    {
        let mut txn = storage.txn().await?;
        assert!(txn.create_task(uuid).await?);
        txn.commit().await?;
    }
    {
        let mut txn = storage.txn().await?;
        assert!(txn.delete_task(uuid).await?);
    }
    Ok(())
}

pub(super) async fn all_tasks_empty(mut storage: impl Storage) -> Result<()> {
    {
        let mut txn = storage.txn().await?;
        let tasks = txn.all_tasks().await?;
        assert_eq!(tasks, vec![]);
    }
    Ok(())
}

pub(super) async fn all_tasks_and_uuids(mut storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    {
        let mut txn = storage.txn().await?;
        assert!(txn.create_task(uuid1).await?);
        txn.set_task(
            uuid1,
            taskmap_with(vec![("num".to_string(), "1".to_string())]),
        )
        .await?;
        assert!(txn.create_task(uuid2).await?);
        txn.set_task(
            uuid2,
            taskmap_with(vec![("num".to_string(), "2".to_string())]),
        )
        .await?;
        txn.commit().await?;
    }
    {
        let mut txn = storage.txn().await?;
        let mut tasks = txn.all_tasks().await?;

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
    }
    {
        let mut txn = storage.txn().await?;
        let mut uuids = txn.all_task_uuids().await?;
        uuids.sort();

        let mut exp = vec![uuid1, uuid2];
        exp.sort();

        assert_eq!(uuids, exp);
    }
    Ok(())
}

pub(super) async fn base_version_default(mut storage: impl Storage) -> Result<()> {
    {
        let mut txn = storage.txn().await?;
        assert_eq!(txn.base_version().await?, DEFAULT_BASE_VERSION);
    }
    Ok(())
}

pub(super) async fn base_version_setting(mut storage: impl Storage) -> Result<()> {
    let u = Uuid::new_v4();
    {
        let mut txn = storage.txn().await?;
        txn.set_base_version(u).await?;
        txn.commit().await?;
    }
    {
        let mut txn = storage.txn().await?;
        assert_eq!(txn.base_version().await?, u);
    }
    Ok(())
}

pub(super) async fn unsynced_operations(mut storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    let uuid3 = Uuid::new_v4();

    // create some operations
    {
        let mut txn = storage.txn().await?;
        txn.add_operation(Operation::Create { uuid: uuid1 }).await?;
        txn.add_operation(Operation::Create { uuid: uuid2 }).await?;
        txn.commit().await?;
    }

    // read them back
    {
        let mut txn = storage.txn().await?;
        let ops = txn.unsynced_operations().await?;
        assert_eq!(
            ops,
            vec![
                Operation::Create { uuid: uuid1 },
                Operation::Create { uuid: uuid2 },
            ]
        );

        assert_eq!(txn.num_unsynced_operations().await?, 2);
    }

    // Sync them.
    {
        let mut txn = storage.txn().await?;
        txn.sync_complete().await?;
        txn.commit().await?;
    }

    // create some more operations (to test adding operations after sync)
    {
        let mut txn = storage.txn().await?;
        txn.add_operation(Operation::Create { uuid: uuid3 }).await?;
        txn.add_operation(Operation::Delete {
            uuid: uuid3,
            old_task: TaskMap::new(),
        })
        .await?;
        txn.commit().await?;
    }

    // read them back
    {
        let mut txn = storage.txn().await?;
        let ops = txn.unsynced_operations().await?;
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
        assert_eq!(txn.num_unsynced_operations().await?, 2);
    }

    // Remove the right one
    {
        let mut txn = storage.txn().await?;
        txn.remove_operation(Operation::Delete {
            uuid: uuid3,
            old_task: TaskMap::new(),
        })
        .await?;
        txn.commit().await?;
    }

    // read the remaining op back
    {
        let mut txn = storage.txn().await?;
        let ops = txn.unsynced_operations().await?;
        assert_eq!(ops, vec![Operation::Create { uuid: uuid3 },]);
        assert_eq!(txn.num_unsynced_operations().await?, 1);
    }
    Ok(())
}

pub(super) async fn remove_operations(mut storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    let uuid3 = Uuid::new_v4();
    let uuid4 = Uuid::new_v4();

    // Create some tasks and operations.
    {
        let mut txn = storage.txn().await?;

        txn.create_task(uuid1).await?;
        txn.create_task(uuid2).await?;
        txn.create_task(uuid3).await?;

        txn.add_operation(Operation::Create { uuid: uuid1 }).await?;
        txn.add_operation(Operation::Create { uuid: uuid2 }).await?;
        txn.add_operation(Operation::Create { uuid: uuid3 }).await?;
        txn.commit().await?;
    }

    // Remove the uuid3 operation.
    {
        let mut txn = storage.txn().await?;
        txn.remove_operation(Operation::Create { uuid: uuid3 })
            .await?;
        assert_eq!(txn.num_unsynced_operations().await?, 2);
        txn.commit().await?;
    }

    // Remove a nonexistent operation
    {
        let mut txn = storage.txn().await?;
        assert!(txn
            .remove_operation(Operation::Create { uuid: uuid4 })
            .await
            .is_err());
    }

    // Remove an operation that is not most recent.
    {
        let mut txn = storage.txn().await?;
        assert!(txn
            .remove_operation(Operation::Create { uuid: uuid1 })
            .await
            .is_err());
    }

    // Mark operations as synced.
    {
        let mut txn = storage.txn().await?;
        txn.sync_complete().await?;
        txn.commit().await?;
    }

    // Try to remove the synced operation.
    {
        let mut txn = storage.txn().await?;
        assert!(txn
            .remove_operation(Operation::Create { uuid: uuid2 })
            .await
            .is_err());
    }

    Ok(())
}

pub(super) async fn task_operations(mut storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    let uuid3 = Uuid::new_v4();
    let now = Utc::now();

    // Create some tasks and operations.
    {
        let mut txn = storage.txn().await?;

        txn.create_task(uuid1).await?;
        txn.create_task(uuid2).await?;
        txn.create_task(uuid3).await?;

        txn.add_operation(Operation::UndoPoint).await?;
        txn.add_operation(Operation::Create { uuid: uuid1 }).await?;
        txn.add_operation(Operation::Create { uuid: uuid1 }).await?;
        txn.add_operation(Operation::UndoPoint).await?;
        txn.add_operation(Operation::Delete {
            uuid: uuid2,
            old_task: TaskMap::new(),
        })
        .await?;
        txn.add_operation(Operation::Update {
            uuid: uuid3,
            property: "p".into(),
            old_value: None,
            value: Some("P".into()),
            timestamp: now,
        })
        .await?;
        txn.add_operation(Operation::Delete {
            uuid: uuid3,
            old_task: TaskMap::new(),
        })
        .await?;

        txn.commit().await?;
    }

    // remove the last operation to verify it doesn't appear
    {
        let mut txn = storage.txn().await?;
        txn.remove_operation(Operation::Delete {
            uuid: uuid3,
            old_task: TaskMap::new(),
        })
        .await?;
        txn.commit().await?;
    }

    // read them back
    {
        let mut txn = storage.txn().await?;
        let ops = txn.get_task_operations(uuid1).await?;
        assert_eq!(
            ops,
            vec![
                Operation::Create { uuid: uuid1 },
                Operation::Create { uuid: uuid1 },
            ]
        );
        let ops = txn.get_task_operations(uuid2).await?;
        assert_eq!(
            ops,
            vec![Operation::Delete {
                uuid: uuid2,
                old_task: TaskMap::new()
            }]
        );
        let ops = txn.get_task_operations(uuid3).await?;
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
    }

    // Sync and verify the task operations still exist.
    {
        let mut txn = storage.txn().await?;

        txn.sync_complete().await?;

        let ops = txn.get_task_operations(uuid1).await?;
        assert_eq!(ops.len(), 2);
        let ops = txn.get_task_operations(uuid2).await?;
        assert_eq!(ops.len(), 1);
        let ops = txn.get_task_operations(uuid3).await?;
        assert_eq!(ops.len(), 1);
    }

    Ok(())
}

pub(super) async fn sync_complete(mut storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    // Create some tasks and operations.
    {
        let mut txn = storage.txn().await?;

        txn.create_task(uuid1).await?;
        txn.create_task(uuid2).await?;

        txn.add_operation(Operation::Create { uuid: uuid1 }).await?;
        txn.add_operation(Operation::Create { uuid: uuid2 }).await?;

        txn.commit().await?;
    }

    // Sync and verify the task operations still exist.
    {
        let mut txn = storage.txn().await?;

        txn.sync_complete().await?;

        let ops = txn.get_task_operations(uuid1).await?;
        assert_eq!(ops.len(), 1);
        let ops = txn.get_task_operations(uuid2).await?;
        assert_eq!(ops.len(), 1);
    }

    // Delete uuid2.
    {
        let mut txn = storage.txn().await?;

        txn.delete_task(uuid2).await?;
        txn.add_operation(Operation::Delete {
            uuid: uuid2,
            old_task: TaskMap::new(),
        })
        .await?;

        txn.commit().await?;
    }

    // Sync and verify that uuid1's operations still exist, but uuid2's do not.
    {
        let mut txn = storage.txn().await?;

        txn.sync_complete().await?;

        let ops = txn.get_task_operations(uuid1).await?;
        assert_eq!(ops.len(), 1);
        let ops = txn.get_task_operations(uuid2).await?;
        assert_eq!(ops.len(), 0);
    }

    Ok(())
}

pub(super) async fn set_working_set_item(mut storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    {
        let mut txn = storage.txn().await?;
        txn.add_to_working_set(uuid1).await?;
        txn.add_to_working_set(uuid2).await?;
        txn.commit().await?;
    }

    {
        let mut txn = storage.txn().await?;
        let ws = txn.get_working_set().await?;
        assert_eq!(ws, vec![None, Some(uuid1), Some(uuid2)]);
    }

    // Clear one item
    {
        let mut txn = storage.txn().await?;
        txn.set_working_set_item(1, None).await?;
        txn.commit().await?;
    }

    {
        let mut txn = storage.txn().await?;
        let ws = txn.get_working_set().await?;
        assert_eq!(ws, vec![None, None, Some(uuid2)]);
    }

    // Override item
    {
        let mut txn = storage.txn().await?;
        txn.set_working_set_item(2, Some(uuid1)).await?;
        txn.commit().await?;
    }

    {
        let mut txn = storage.txn().await?;
        let ws = txn.get_working_set().await?;
        assert_eq!(ws, vec![None, None, Some(uuid1)]);
    }

    // Set the last item to None
    {
        let mut txn = storage.txn().await?;
        txn.set_working_set_item(1, Some(uuid1)).await?;
        txn.set_working_set_item(2, None).await?;
        txn.commit().await?;
    }

    {
        let mut txn = storage.txn().await?;
        let ws = txn.get_working_set().await?;
        // Note no trailing `None`.
        assert_eq!(ws, vec![None, Some(uuid1)]);
    }

    Ok(())
}
