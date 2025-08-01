// src/storage/adapter.rs

use crate::operation::Operation;
use crate::storage::{Storage, StorageTxn, TaskMap, VersionId};
use crate::{errors::Result, storage::async_storage::AsyncStorage};
use tokio::runtime::{Builder, Runtime};
use uuid::Uuid;

/// A synchronous wrapper around an asynchronous storage backend.
///
/// This struct implements the synchronous `Storage` trait and internally
/// drives an `AsyncStorage` implementation on a Tokio runtime. This allows
/// the storage backend to be implemented asynchronously without requiring
/// the entire application to be async immediately.
pub(crate) struct BlockingStorageAdapter {
    /// A handle to the Tokio runtime to execute async tasks.
    rt: Runtime,
    /// The actual async storage implementation, boxed to handle any type
    /// that implements the `AsyncStorage` trait.
    inner: Box<dyn AsyncStorage>,
}

impl BlockingStorageAdapter {
    /// Create a new BlockingStorageAdapter, wrapping the given async storage backend.
    ///
    /// This will build a new single-threaded Tokio runtime to run the backend on.
    pub(crate) fn new(storage: Box<dyn AsyncStorage>) -> Result<Self> {
        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");
        Ok(BlockingStorageAdapter { rt, inner: storage })
    }
}

/// Implementation of the original, synchronous `Storage` trait.
impl Storage for BlockingStorageAdapter {
    /// The `txn` method in this adapter model doesn't start a "real"
    /// database transaction. It simply returns a `BlockingStorageTxn`
    /// wrapper that provides access to the underlying async storage methods.
    fn txn<'a>(&'a mut self) -> Result<Box<dyn StorageTxn + 'a>> {
        Ok(Box::new(BlockingStorageTxn {
            rt: &self.rt,
            storage: &mut self.inner,
        }))
    }
}

/// A struct that implements the synchronous `StorageTxn` trait by
/// blocking on calls to the async `AsyncStorage` trait.
struct BlockingStorageTxn<'a> {
    rt: &'a Runtime,
    storage: &'a mut Box<dyn AsyncStorage>,
}

/// Implementation of the original, synchronous `StorageTxn` trait.
impl<'a> StorageTxn for BlockingStorageTxn<'a> {
    fn get_task(&mut self, uuid: Uuid) -> Result<Option<TaskMap>> {
        self.rt.block_on(self.storage.get_task(uuid))
    }

    fn get_pending_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        self.rt.block_on(self.storage.get_pending_tasks())
    }

    fn create_task(&mut self, uuid: Uuid) -> Result<bool> {
        self.rt.block_on(self.storage.create_task(uuid))
    }

    fn set_task(&mut self, uuid: Uuid, task: TaskMap) -> Result<()> {
        self.rt.block_on(self.storage.set_task(uuid, task))
    }

    fn delete_task(&mut self, uuid: Uuid) -> Result<bool> {
        self.rt.block_on(self.storage.delete_task(uuid))
    }

    fn all_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        self.rt.block_on(self.storage.all_tasks())
    }

    fn all_task_uuids(&mut self) -> Result<Vec<Uuid>> {
        self.rt.block_on(self.storage.all_task_uuids())
    }

    fn base_version(&mut self) -> Result<VersionId> {
        self.rt.block_on(self.storage.base_version())
    }

    fn set_base_version(&mut self, version: VersionId) -> Result<()> {
        self.rt.block_on(self.storage.set_base_version(version))
    }

    fn get_task_operations(&mut self, uuid: Uuid) -> Result<Vec<Operation>> {
        self.rt.block_on(self.storage.get_task_operations(uuid))
    }

    fn unsynced_operations(&mut self) -> Result<Vec<Operation>> {
        self.rt.block_on(self.storage.unsynced_operations())
    }

    fn num_unsynced_operations(&mut self) -> Result<usize> {
        self.rt.block_on(self.storage.num_unsynced_operations())
    }

    fn add_operation(&mut self, op: Operation) -> Result<()> {
        self.rt.block_on(self.storage.add_operation(op))
    }

    fn remove_operation(&mut self, op: Operation) -> Result<()> {
        self.rt.block_on(self.storage.remove_operation(op))
    }

    fn sync_complete(&mut self) -> Result<()> {
        self.rt.block_on(self.storage.sync_complete())
    }

    fn get_working_set(&mut self) -> Result<Vec<Option<Uuid>>> {
        self.rt.block_on(self.storage.get_working_set())
    }

    fn add_to_working_set(&mut self, uuid: Uuid) -> Result<usize> {
        self.rt.block_on(self.storage.add_to_working_set(uuid))
    }

    fn set_working_set_item(&mut self, index: usize, uuid: Option<Uuid>) -> Result<()> {
        self.rt
            .block_on(self.storage.set_working_set_item(index, uuid))
    }

    fn clear_working_set(&mut self) -> Result<()> {
        self.rt.block_on(self.storage.clear_working_set())
    }

    fn is_empty(&mut self) -> Result<bool> {
        self.rt.block_on(self.storage.is_empty())
    }

    /// Since the new `AsyncStorage` trait does not have a concept of
    /// transactions, this commit method is a no-op. The underlying async
    /// methods are effectively committed immediately.
    fn commit(&mut self) -> Result<()> {
        Ok(())
    }
}
