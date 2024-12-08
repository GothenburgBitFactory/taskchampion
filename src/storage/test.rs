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
        #[test]
        fn get_working_set_empty() -> $crate::errors::Result<()> {
            $crate::storage::test::get_working_set_empty($storage)
        }

        #[test]
        fn add_to_working_set() -> $crate::errors::Result<()> {
            $crate::storage::test::add_to_working_set($storage)
        }

        #[test]
        fn clear_working_set() -> $crate::errors::Result<()> {
            $crate::storage::test::clear_working_set($storage)
        }

        #[test]
        fn drop_transaction() -> $crate::errors::Result<()> {
            $crate::storage::test::drop_transaction($storage)
        }

        #[test]
        fn create() -> $crate::errors::Result<()> {
            $crate::storage::test::create($storage)
        }

        #[test]
        fn create_exists() -> $crate::errors::Result<()> {
            $crate::storage::test::create_exists($storage)
        }

        #[test]
        fn get_missing() -> $crate::errors::Result<()> {
            $crate::storage::test::get_missing($storage)
        }

        #[test]
        fn set_task() -> $crate::errors::Result<()> {
            $crate::storage::test::set_task($storage)
        }

        #[test]
        fn delete_task_missing() -> $crate::errors::Result<()> {
            $crate::storage::test::delete_task_missing($storage)
        }

        #[test]
        fn delete_task_exists() -> $crate::errors::Result<()> {
            $crate::storage::test::delete_task_exists($storage)
        }

        #[test]
        fn all_tasks_empty() -> $crate::errors::Result<()> {
            $crate::storage::test::all_tasks_empty($storage)
        }

        #[test]
        fn all_tasks_and_uuids() -> $crate::errors::Result<()> {
            $crate::storage::test::all_tasks_and_uuids($storage)
        }

        #[test]
        fn base_version_default() -> Result<()> {
            $crate::storage::test::base_version_default($storage)
        }

        #[test]
        fn base_version_setting() -> Result<()> {
            $crate::storage::test::base_version_setting($storage)
        }

        #[test]
        fn unsynced_operations() -> Result<()> {
            $crate::storage::test::unsynced_operations($storage)
        }

        #[test]
        fn remove_operations() -> Result<()> {
            $crate::storage::test::remove_operations($storage)
        }

        #[test]
        fn task_operations() -> Result<()> {
            $crate::storage::test::task_operations($storage)
        }

        #[test]
        fn sync_complete() -> Result<()> {
            $crate::storage::test::sync_complete($storage)
        }

        #[test]
        fn set_working_set_item() -> Result<()> {
            $crate::storage::test::set_working_set_item($storage)
        }
    };
}
pub(crate) use storage_tests;

pub(super) fn get_working_set_empty(mut storage: impl Storage) -> Result<()> {
    {
        let mut txn = storage.txn()?;
        let ws = txn.get_working_set()?;
        assert_eq!(ws, vec![None]);
    }

    Ok(())
}

pub(super) fn add_to_working_set(mut storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    {
        let mut txn = storage.txn()?;
        txn.add_to_working_set(uuid1)?;
        txn.add_to_working_set(uuid2)?;
        txn.commit()?;
    }

    {
        let mut txn = storage.txn()?;
        let ws = txn.get_working_set()?;
        assert_eq!(ws, vec![None, Some(uuid1), Some(uuid2)]);
    }

    Ok(())
}

pub(super) fn clear_working_set(mut storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    {
        let mut txn = storage.txn()?;
        txn.add_to_working_set(uuid1)?;
        txn.add_to_working_set(uuid2)?;
        txn.commit()?;
    }

    {
        let mut txn = storage.txn()?;
        txn.clear_working_set()?;
        txn.add_to_working_set(uuid2)?;
        txn.add_to_working_set(uuid1)?;
        txn.commit()?;
    }

    {
        let mut txn = storage.txn()?;
        let ws = txn.get_working_set()?;
        assert_eq!(ws, vec![None, Some(uuid2), Some(uuid1)]);
    }

    Ok(())
}

pub(super) fn drop_transaction(mut storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    {
        let mut txn = storage.txn()?;
        assert!(txn.create_task(uuid1)?);
        txn.commit()?;
    }

    {
        let mut txn = storage.txn()?;
        assert!(txn.create_task(uuid2)?);
        std::mem::drop(txn); // Unnecessary explicit drop of transaction
    }

    {
        let mut txn = storage.txn()?;
        let uuids = txn.all_task_uuids()?;

        assert_eq!(uuids, [uuid1]);
    }

    Ok(())
}

pub(super) fn create(mut storage: impl Storage) -> Result<()> {
    let uuid = Uuid::new_v4();
    {
        let mut txn = storage.txn()?;
        assert!(txn.create_task(uuid)?);
        txn.commit()?;
    }
    {
        let mut txn = storage.txn()?;
        let task = txn.get_task(uuid)?;
        assert_eq!(task, Some(taskmap_with(vec![])));
    }
    Ok(())
}

pub(super) fn create_exists(mut storage: impl Storage) -> Result<()> {
    let uuid = Uuid::new_v4();
    {
        let mut txn = storage.txn()?;
        assert!(txn.create_task(uuid)?);
        txn.commit()?;
    }
    {
        let mut txn = storage.txn()?;
        assert!(!txn.create_task(uuid)?);
        txn.commit()?;
    }
    Ok(())
}

pub(super) fn get_missing(mut storage: impl Storage) -> Result<()> {
    let uuid = Uuid::new_v4();
    {
        let mut txn = storage.txn()?;
        let task = txn.get_task(uuid)?;
        assert_eq!(task, None);
    }
    Ok(())
}

pub(super) fn set_task(mut storage: impl Storage) -> Result<()> {
    let uuid = Uuid::new_v4();
    {
        let mut txn = storage.txn()?;
        txn.set_task(uuid, taskmap_with(vec![("k".to_string(), "v".to_string())]))?;
        txn.commit()?;
    }
    {
        let mut txn = storage.txn()?;
        let task = txn.get_task(uuid)?;
        assert_eq!(
            task,
            Some(taskmap_with(vec![("k".to_string(), "v".to_string())]))
        );
    }
    Ok(())
}

pub(super) fn delete_task_missing(mut storage: impl Storage) -> Result<()> {
    let uuid = Uuid::new_v4();
    {
        let mut txn = storage.txn()?;
        assert!(!txn.delete_task(uuid)?);
    }
    Ok(())
}

pub(super) fn delete_task_exists(mut storage: impl Storage) -> Result<()> {
    let uuid = Uuid::new_v4();
    {
        let mut txn = storage.txn()?;
        assert!(txn.create_task(uuid)?);
        txn.commit()?;
    }
    {
        let mut txn = storage.txn()?;
        assert!(txn.delete_task(uuid)?);
    }
    Ok(())
}

pub(super) fn all_tasks_empty(mut storage: impl Storage) -> Result<()> {
    {
        let mut txn = storage.txn()?;
        let tasks = txn.all_tasks()?;
        assert_eq!(tasks, vec![]);
    }
    Ok(())
}

pub(super) fn all_tasks_and_uuids(mut storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    {
        let mut txn = storage.txn()?;
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
        txn.commit()?;
    }
    {
        let mut txn = storage.txn()?;
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
    }
    {
        let mut txn = storage.txn()?;
        let mut uuids = txn.all_task_uuids()?;
        uuids.sort();

        let mut exp = vec![uuid1, uuid2];
        exp.sort();

        assert_eq!(uuids, exp);
    }
    Ok(())
}

pub(super) fn base_version_default(mut storage: impl Storage) -> Result<()> {
    {
        let mut txn = storage.txn()?;
        assert_eq!(txn.base_version()?, DEFAULT_BASE_VERSION);
    }
    Ok(())
}

pub(super) fn base_version_setting(mut storage: impl Storage) -> Result<()> {
    let u = Uuid::new_v4();
    {
        let mut txn = storage.txn()?;
        txn.set_base_version(u)?;
        txn.commit()?;
    }
    {
        let mut txn = storage.txn()?;
        assert_eq!(txn.base_version()?, u);
    }
    Ok(())
}

pub(super) fn unsynced_operations(mut storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    let uuid3 = Uuid::new_v4();

    // create some operations
    {
        let mut txn = storage.txn()?;
        txn.add_operation(Operation::Create { uuid: uuid1 })?;
        txn.add_operation(Operation::Create { uuid: uuid2 })?;
        txn.commit()?;
    }

    // read them back
    {
        let mut txn = storage.txn()?;
        let ops = txn.unsynced_operations()?;
        assert_eq!(
            ops,
            vec![
                Operation::Create { uuid: uuid1 },
                Operation::Create { uuid: uuid2 },
            ]
        );

        assert_eq!(txn.num_unsynced_operations()?, 2);
    }

    // Sync them.
    {
        let mut txn = storage.txn()?;
        txn.sync_complete()?;
        txn.commit()?;
    }

    // create some more operations (to test adding operations after sync)
    {
        let mut txn = storage.txn()?;
        txn.add_operation(Operation::Create { uuid: uuid3 })?;
        txn.add_operation(Operation::Delete {
            uuid: uuid3,
            old_task: TaskMap::new(),
        })?;
        txn.commit()?;
    }

    // read them back
    {
        let mut txn = storage.txn()?;
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
    }

    // Remove the right one
    {
        let mut txn = storage.txn()?;
        txn.remove_operation(Operation::Delete {
            uuid: uuid3,
            old_task: TaskMap::new(),
        })?;
        txn.commit()?;
    }

    // read the remaining op back
    {
        let mut txn = storage.txn()?;
        let ops = txn.unsynced_operations()?;
        assert_eq!(ops, vec![Operation::Create { uuid: uuid3 },]);
        assert_eq!(txn.num_unsynced_operations()?, 1);
    }
    Ok(())
}

pub(super) fn remove_operations(mut storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    let uuid3 = Uuid::new_v4();
    let uuid4 = Uuid::new_v4();

    // Create some tasks and operations.
    {
        let mut txn = storage.txn()?;

        txn.create_task(uuid1)?;
        txn.create_task(uuid2)?;
        txn.create_task(uuid3)?;

        txn.add_operation(Operation::Create { uuid: uuid1 })?;
        txn.add_operation(Operation::Create { uuid: uuid2 })?;
        txn.add_operation(Operation::Create { uuid: uuid3 })?;
        txn.commit()?;
    }

    // Remove the uuid3 operation.
    {
        let mut txn = storage.txn()?;
        txn.remove_operation(Operation::Create { uuid: uuid3 })?;
        assert_eq!(txn.num_unsynced_operations()?, 2);
        txn.commit()?;
    }

    // Remove a nonexistent operation
    {
        let mut txn = storage.txn()?;
        assert!(txn
            .remove_operation(Operation::Create { uuid: uuid4 })
            .is_err());
    }

    // Remove an operation that is not most recent.
    {
        let mut txn = storage.txn()?;
        assert!(txn
            .remove_operation(Operation::Create { uuid: uuid1 })
            .is_err());
    }

    // Mark operations as synced.
    {
        let mut txn = storage.txn()?;
        txn.sync_complete()?;
        txn.commit()?;
    }

    // Try to remove the synced operation.
    {
        let mut txn = storage.txn()?;
        assert!(txn
            .remove_operation(Operation::Create { uuid: uuid2 })
            .is_err());
    }

    Ok(())
}

pub(super) fn task_operations(mut storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    let uuid3 = Uuid::new_v4();
    let now = Utc::now();

    // Create some tasks and operations.
    {
        let mut txn = storage.txn()?;

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

        txn.commit()?;
    }

    // remove the last operation to verify it doesn't appear
    {
        let mut txn = storage.txn()?;
        txn.remove_operation(Operation::Delete {
            uuid: uuid3,
            old_task: TaskMap::new(),
        })?;
        txn.commit()?;
    }

    // read them back
    {
        let mut txn = storage.txn()?;
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
    }

    // Sync and verify the task operations still exist.
    {
        let mut txn = storage.txn()?;

        txn.sync_complete()?;

        let ops = txn.get_task_operations(uuid1)?;
        assert_eq!(ops.len(), 2);
        let ops = txn.get_task_operations(uuid2)?;
        assert_eq!(ops.len(), 1);
        let ops = txn.get_task_operations(uuid3)?;
        assert_eq!(ops.len(), 1);
    }

    Ok(())
}

pub(super) fn sync_complete(mut storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    // Create some tasks and operations.
    {
        let mut txn = storage.txn()?;

        txn.create_task(uuid1)?;
        txn.create_task(uuid2)?;

        txn.add_operation(Operation::Create { uuid: uuid1 })?;
        txn.add_operation(Operation::Create { uuid: uuid2 })?;

        txn.commit()?;
    }

    // Sync and verify the task operations still exist.
    {
        let mut txn = storage.txn()?;

        txn.sync_complete()?;

        let ops = txn.get_task_operations(uuid1)?;
        assert_eq!(ops.len(), 1);
        let ops = txn.get_task_operations(uuid2)?;
        assert_eq!(ops.len(), 1);
    }

    // Delete uuid2.
    {
        let mut txn = storage.txn()?;

        txn.delete_task(uuid2)?;
        txn.add_operation(Operation::Delete {
            uuid: uuid2,
            old_task: TaskMap::new(),
        })?;

        txn.commit()?;
    }

    // Sync and verify that uuid1's operations still exist, but uuid2's do not.
    {
        let mut txn = storage.txn()?;

        txn.sync_complete()?;

        let ops = txn.get_task_operations(uuid1)?;
        assert_eq!(ops.len(), 1);
        let ops = txn.get_task_operations(uuid2)?;
        assert_eq!(ops.len(), 0);
    }

    Ok(())
}

pub(super) fn set_working_set_item(mut storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    {
        let mut txn = storage.txn()?;
        txn.add_to_working_set(uuid1)?;
        txn.add_to_working_set(uuid2)?;
        txn.commit()?;
    }

    {
        let mut txn = storage.txn()?;
        let ws = txn.get_working_set()?;
        assert_eq!(ws, vec![None, Some(uuid1), Some(uuid2)]);
    }

    // Clear one item
    {
        let mut txn = storage.txn()?;
        txn.set_working_set_item(1, None)?;
        txn.commit()?;
    }

    {
        let mut txn = storage.txn()?;
        let ws = txn.get_working_set()?;
        assert_eq!(ws, vec![None, None, Some(uuid2)]);
    }

    // Override item
    {
        let mut txn = storage.txn()?;
        txn.set_working_set_item(2, Some(uuid1))?;
        txn.commit()?;
    }

    {
        let mut txn = storage.txn()?;
        let ws = txn.get_working_set()?;
        assert_eq!(ws, vec![None, None, Some(uuid1)]);
    }

    Ok(())
}
