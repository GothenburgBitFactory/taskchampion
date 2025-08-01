//! Tests for storage backends. This tests consistency across multiple method calls, to ensure that
//! all implementations are consistent.

use super::TaskMap;
use crate::errors::Result;
use crate::storage::async_storage::AsyncStorage;
use crate::storage::{taskmap_with, DEFAULT_BASE_VERSION};
use crate::Operation;
use chrono::Utc;
use pretty_assertions::assert_eq;
use uuid::Uuid;

/// Define a collection of storage tests that apply to all storage implementations.
macro_rules! async_storage_tests {
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
pub(crate) use async_storage_tests;

pub(super) async fn get_working_set_empty(mut storage: impl AsyncStorage) -> Result<()> {
    let ws = storage.get_working_set().await?;
    assert_eq!(ws, vec![None]);
    Ok(())
}

pub(super) async fn add_to_working_set(mut storage: impl AsyncStorage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    storage.add_to_working_set(uuid1).await?;
    storage.add_to_working_set(uuid2).await?;

    let ws = storage.get_working_set().await?;
    assert_eq!(ws, vec![None, Some(uuid1), Some(uuid2)]);

    Ok(())
}

pub(super) async fn clear_working_set(mut storage: impl AsyncStorage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    storage.add_to_working_set(uuid1).await?;
    storage.add_to_working_set(uuid2).await?;

    storage.clear_working_set().await?;
    storage.add_to_working_set(uuid2).await?;
    storage.add_to_working_set(uuid1).await?;

    let ws = storage.get_working_set().await?;
    assert_eq!(ws, vec![None, Some(uuid2), Some(uuid1)]);

    Ok(())
}

pub(super) async fn create(mut storage: impl AsyncStorage) -> Result<()> {
    let uuid = Uuid::new_v4();
    assert!(storage.create_task(uuid).await?);
    let task = storage.get_task(uuid).await?;
    assert_eq!(task, Some(taskmap_with(vec![])));
    Ok(())
}

pub(super) async fn create_exists(mut storage: impl AsyncStorage) -> Result<()> {
    let uuid = Uuid::new_v4();
    assert!(storage.create_task(uuid).await?);
    assert!(!storage.create_task(uuid).await?);
    Ok(())
}

pub(super) async fn get_missing(mut storage: impl AsyncStorage) -> Result<()> {
    let uuid = Uuid::new_v4();
    let task = storage.get_task(uuid).await?;
    assert_eq!(task, None);
    Ok(())
}

pub(super) async fn set_task(mut storage: impl AsyncStorage) -> Result<()> {
    let uuid = Uuid::new_v4();
    storage
        .set_task(uuid, taskmap_with(vec![("k".to_string(), "v".to_string())]))
        .await?;
    let task = storage.get_task(uuid).await?;
    assert_eq!(
        task,
        Some(taskmap_with(vec![("k".to_string(), "v".to_string())]))
    );
    Ok(())
}

pub(super) async fn delete_task_missing(mut storage: impl AsyncStorage) -> Result<()> {
    let uuid = Uuid::new_v4();
    assert!(!storage.delete_task(uuid).await?);
    Ok(())
}

pub(super) async fn delete_task_exists(mut storage: impl AsyncStorage) -> Result<()> {
    let uuid = Uuid::new_v4();
    assert!(storage.create_task(uuid).await?);
    assert!(storage.delete_task(uuid).await?);
    Ok(())
}

pub(super) async fn all_tasks_empty(mut storage: impl AsyncStorage) -> Result<()> {
    let tasks = storage.all_tasks().await?;
    assert_eq!(tasks, vec![]);
    Ok(())
}

pub(super) async fn all_tasks_and_uuids(mut storage: impl AsyncStorage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    assert!(storage.create_task(uuid1).await?);
    storage
        .set_task(
            uuid1,
            taskmap_with(vec![("num".to_string(), "1".to_string())]),
        )
        .await?;
    assert!(storage.create_task(uuid2).await?);
    storage
        .set_task(
            uuid2,
            taskmap_with(vec![("num".to_string(), "2".to_string())]),
        )
        .await?;

    let mut tasks = storage.all_tasks().await?;
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

    let mut uuids = storage.all_task_uuids().await?;
    uuids.sort();
    let mut exp = vec![uuid1, uuid2];
    exp.sort();
    assert_eq!(uuids, exp);

    Ok(())
}

pub(super) async fn base_version_default(mut storage: impl AsyncStorage) -> Result<()> {
    assert_eq!(storage.base_version().await?, DEFAULT_BASE_VERSION);
    Ok(())
}

pub(super) async fn base_version_setting(mut storage: impl AsyncStorage) -> Result<()> {
    let u = Uuid::new_v4();
    storage.set_base_version(u).await?;
    assert_eq!(storage.base_version().await?, u);
    Ok(())
}

pub(super) async fn unsynced_operations(mut storage: impl AsyncStorage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    let uuid3 = Uuid::new_v4();

    // create some operations
    storage
        .add_operation(Operation::Create { uuid: uuid1 })
        .await?;
    storage
        .add_operation(Operation::Create { uuid: uuid2 })
        .await?;

    // read them back
    let ops = storage.unsynced_operations().await?;
    assert_eq!(
        ops,
        vec![
            Operation::Create { uuid: uuid1 },
            Operation::Create { uuid: uuid2 },
        ]
    );
    assert_eq!(storage.num_unsynced_operations().await?, 2);

    // Sync them.
    storage.sync_complete().await?;

    // create some more operations
    storage
        .add_operation(Operation::Create { uuid: uuid3 })
        .await?;
    storage
        .add_operation(Operation::Delete {
            uuid: uuid3,
            old_task: TaskMap::new(),
        })
        .await?;

    // read them back
    let ops = storage.unsynced_operations().await?;
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
    assert_eq!(storage.num_unsynced_operations().await?, 2);

    // Remove the right one
    storage
        .remove_operation(Operation::Delete {
            uuid: uuid3,
            old_task: TaskMap::new(),
        })
        .await?;

    // read the remaining op back
    let ops = storage.unsynced_operations().await?;
    assert_eq!(ops, vec![Operation::Create { uuid: uuid3 },]);
    assert_eq!(storage.num_unsynced_operations().await?, 1);

    Ok(())
}

pub(super) async fn remove_operations(mut storage: impl AsyncStorage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    let uuid3 = Uuid::new_v4();
    let uuid4 = Uuid::new_v4();

    // Create some tasks and operations.
    storage.create_task(uuid1).await?;
    storage.create_task(uuid2).await?;
    storage.create_task(uuid3).await?;
    storage
        .add_operation(Operation::Create { uuid: uuid1 })
        .await?;
    storage
        .add_operation(Operation::Create { uuid: uuid2 })
        .await?;
    storage
        .add_operation(Operation::Create { uuid: uuid3 })
        .await?;

    // Remove the uuid3 operation.
    storage
        .remove_operation(Operation::Create { uuid: uuid3 })
        .await?;
    assert_eq!(storage.num_unsynced_operations().await?, 2);

    // Remove a nonexistent operation
    assert!(storage
        .remove_operation(Operation::Create { uuid: uuid4 })
        .await
        .is_err());

    // Remove an operation that is not most recent.
    assert!(storage
        .remove_operation(Operation::Create { uuid: uuid1 })
        .await
        .is_err());

    // Mark operations as synced.
    storage.sync_complete().await?;

    // Try to remove the synced operation.
    assert!(storage
        .remove_operation(Operation::Create { uuid: uuid2 })
        .await
        .is_err());

    Ok(())
}

pub(super) async fn task_operations(mut storage: impl AsyncStorage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    let uuid3 = Uuid::new_v4();
    let now = Utc::now();

    // Create some tasks and operations.
    storage.create_task(uuid1).await?;
    storage.create_task(uuid2).await?;
    storage.create_task(uuid3).await?;
    storage.add_operation(Operation::UndoPoint).await?;
    storage
        .add_operation(Operation::Create { uuid: uuid1 })
        .await?;
    storage
        .add_operation(Operation::Create { uuid: uuid1 })
        .await?;
    storage.add_operation(Operation::UndoPoint).await?;
    storage
        .add_operation(Operation::Delete {
            uuid: uuid2,
            old_task: TaskMap::new(),
        })
        .await?;
    storage
        .add_operation(Operation::Update {
            uuid: uuid3,
            property: "p".into(),
            old_value: None,
            value: Some("P".into()),
            timestamp: now,
        })
        .await?;
    storage
        .add_operation(Operation::Delete {
            uuid: uuid3,
            old_task: TaskMap::new(),
        })
        .await?;

    // remove the last operation to verify it doesn't appear
    storage
        .remove_operation(Operation::Delete {
            uuid: uuid3,
            old_task: TaskMap::new(),
        })
        .await?;

    // read them back
    let ops1 = storage.get_task_operations(uuid1).await?;
    assert_eq!(
        ops1,
        vec![
            Operation::Create { uuid: uuid1 },
            Operation::Create { uuid: uuid1 },
        ]
    );
    let ops2 = storage.get_task_operations(uuid2).await?;
    assert_eq!(
        ops2,
        vec![Operation::Delete {
            uuid: uuid2,
            old_task: TaskMap::new()
        }]
    );
    let ops3 = storage.get_task_operations(uuid3).await?;
    assert_eq!(
        ops3,
        vec![Operation::Update {
            uuid: uuid3,
            property: "p".into(),
            old_value: None,
            value: Some("P".into()),
            timestamp: now,
        }]
    );

    // Sync and verify the task operations still exist.
    storage.sync_complete().await?;
    let ops1 = storage.get_task_operations(uuid1).await?;
    assert_eq!(ops1.len(), 2);
    let ops2 = storage.get_task_operations(uuid2).await?;
    assert_eq!(ops2.len(), 1);
    let ops3 = storage.get_task_operations(uuid3).await?;
    assert_eq!(ops3.len(), 1);

    Ok(())
}

pub(super) async fn sync_complete(mut storage: impl AsyncStorage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    // Create some tasks and operations.
    storage.create_task(uuid1).await?;
    storage.create_task(uuid2).await?;
    storage
        .add_operation(Operation::Create { uuid: uuid1 })
        .await?;
    storage
        .add_operation(Operation::Create { uuid: uuid2 })
        .await?;

    // Sync and verify the task operations still exist.
    storage.sync_complete().await?;
    let ops1 = storage.get_task_operations(uuid1).await?;
    assert_eq!(ops1.len(), 1);
    let ops2 = storage.get_task_operations(uuid2).await?;
    assert_eq!(ops2.len(), 1);

    // Delete uuid2.
    storage.delete_task(uuid2).await?;
    storage
        .add_operation(Operation::Delete {
            uuid: uuid2,
            old_task: TaskMap::new(),
        })
        .await?;

    // Sync and verify that uuid1's operations still exist, but uuid2's do not.
    storage.sync_complete().await?;
    let ops1 = storage.get_task_operations(uuid1).await?;
    assert_eq!(ops1.len(), 1);
    let ops2 = storage.get_task_operations(uuid2).await?;
    assert_eq!(ops2.len(), 0);

    Ok(())
}

pub(super) async fn set_working_set_item(mut storage: impl AsyncStorage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    storage.add_to_working_set(uuid1).await?;
    storage.add_to_working_set(uuid2).await?;
    let ws = storage.get_working_set().await?;
    assert_eq!(ws, vec![None, Some(uuid1), Some(uuid2)]);

    // Clear one item
    storage.set_working_set_item(1, None).await?;
    let ws = storage.get_working_set().await?;
    assert_eq!(ws, vec![None, None, Some(uuid2)]);

    // Override item
    storage.set_working_set_item(2, Some(uuid1)).await?;
    let ws = storage.get_working_set().await?;
    assert_eq!(ws, vec![None, None, Some(uuid1)]);

    // Set the last item to None, causing the list to shrink
    storage.set_working_set_item(1, Some(uuid1)).await?;
    storage.set_working_set_item(2, None).await?;

    let ws = storage.get_working_set().await?;
    // Note no trailing `None`.
    assert_eq!(ws, vec![None, Some(uuid1)]);

    Ok(())
}
