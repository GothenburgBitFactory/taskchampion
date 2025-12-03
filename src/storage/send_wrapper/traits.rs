use crate::errors::Result;
use crate::operation::Operation;
use crate::storage::{TaskMap, VersionId};
use async_trait::async_trait;
use uuid::Uuid;

/// This trait is identical to [`crate::storage::StorageTxn`] except that it is not Send.
#[async_trait(?Send)]
pub(in crate::storage) trait WrappedStorageTxn {
    async fn get_task(&mut self, uuid: Uuid) -> Result<Option<TaskMap>>;
    async fn get_pending_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>>;
    async fn create_task(&mut self, uuid: Uuid) -> Result<bool>;
    async fn set_task(&mut self, uuid: Uuid, task: TaskMap) -> Result<()>;
    async fn delete_task(&mut self, uuid: Uuid) -> Result<bool>;
    async fn all_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>>;
    async fn all_task_uuids(&mut self) -> Result<Vec<Uuid>>;
    async fn base_version(&mut self) -> Result<VersionId>;
    async fn set_base_version(&mut self, version: VersionId) -> Result<()>;
    async fn get_task_operations(&mut self, uuid: Uuid) -> Result<Vec<Operation>>;
    async fn unsynced_operations(&mut self) -> Result<Vec<Operation>>;
    async fn num_unsynced_operations(&mut self) -> Result<usize>;
    async fn add_operation(&mut self, op: Operation) -> Result<()>;
    async fn remove_operation(&mut self, op: Operation) -> Result<()>;
    async fn sync_complete(&mut self) -> Result<()>;
    async fn get_working_set(&mut self) -> Result<Vec<Option<Uuid>>>;
    async fn add_to_working_set(&mut self, uuid: Uuid) -> Result<usize>;
    async fn set_working_set_item(&mut self, index: usize, uuid: Option<Uuid>) -> Result<()>;
    async fn clear_working_set(&mut self) -> Result<()>;
    #[allow(clippy::wrong_self_convention)] // mut is required here for storage access
    async fn is_empty(&mut self) -> Result<bool> {
        let mut empty = true;
        empty = empty && self.all_tasks().await?.is_empty();
        empty = empty && self.get_working_set().await? == vec![None];
        empty = empty && self.base_version().await? == Uuid::nil();
        empty = empty && self.unsynced_operations().await?.is_empty();
        Ok(empty)
    }
    async fn commit(&mut self) -> Result<()>;
}

/// This trait is similar to [`crate::storage::Storage`] except that it is not Send.
#[async_trait(?Send)]
pub(in crate::storage) trait WrappedStorage {
    async fn txn<'a>(&'a mut self) -> Result<Box<dyn WrappedStorageTxn + 'a>>;
}
