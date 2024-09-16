//! Tests for storage backends. This tests consistency across multiple method calls, to ensure that
//! all implementations are consistent.
//!
//! Each method should be called from a test of the same name.

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
        fn get_pending_tasks() -> $crate::errors::Result<()> {
            $crate::storage::test::get_pending_tasks($storage)
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
        fn operations() -> Result<()> {
            $crate::storage::test::operations($storage)
        }

        #[test]
        fn task_operations() -> Result<()> {
            $crate::storage::test::task_operations($storage)
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

pub(super) fn get_pending_tasks(mut storage: impl Storage) -> Result<()> {
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
        let mut tasks = txn.get_pending_tasks()?;

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
        tasks.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(tasks, exp);
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

pub(super) fn operations(mut storage: impl Storage) -> Result<()> {
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
        let ops = txn.operations()?;
        assert_eq!(
            ops,
            vec![
                Operation::Create { uuid: uuid1 },
                Operation::Create { uuid: uuid2 },
            ]
        );

        assert_eq!(txn.num_operations()?, 2);
    }

    // Clear them.
    {
        let mut txn = storage.txn()?;
        txn.sync_complete()?;
        txn.commit()?;
    }

    // create some more operations (to test adding operations after clearing)
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
        let ops = txn.operations()?;
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
        assert_eq!(txn.num_operations()?, 2);
    }

    // Remove the wrong one
    {
        let mut txn = storage.txn()?;
        assert!(txn
            .remove_operation(Operation::Create { uuid: uuid3 })
            .is_err())
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
        let ops = txn.operations()?;
        assert_eq!(ops, vec![Operation::Create { uuid: uuid3 },]);
        assert_eq!(txn.num_operations()?, 1);
    }

    // Remove the last one
    {
        let mut txn = storage.txn()?;
        txn.remove_operation(Operation::Create { uuid: uuid3 })?;
        assert_eq!(txn.num_operations()?, 0);
        txn.commit()?;
    }

    // Remove a nonexistent operation
    {
        let mut txn = storage.txn()?;
        assert!(txn
            .remove_operation(Operation::Create { uuid: uuid3 })
            .is_err());
    }

    Ok(())
}

pub(super) fn task_operations(mut storage: impl Storage) -> Result<()> {
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();
    let uuid3 = Uuid::new_v4();
    let now = Utc::now();

    // create some operations
    {
        let mut txn = storage.txn()?;
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
