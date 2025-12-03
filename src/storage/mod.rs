/*!
This module defines the backend storage used by [`Replica`](crate::Replica).

It defines a [trait](crate::storage::Storage) for storage implementations, and provides a default
on-disk implementation as well as an in-memory implementation for testing.

Typical uses of this crate do not interact directly with this module. However, users who wish to
implement their own storage backends can implement the traits defined here and pass the result to
[`Replica`](crate::Replica).
*/

use crate::errors::Result;
use crate::operation::Operation;
use async_trait::async_trait;
use std::collections::HashMap;
use uuid::Uuid;

#[cfg(test)]
mod test;

mod config;

#[cfg(feature = "storage-sqlite")]
pub mod sqlite;

pub use config::AccessMode;

pub mod inmemory;

#[cfg(feature = "storage-sqlite")]
mod send_wrapper;

#[doc(hidden)]
/// For compatibility with 0.6 and earlier, [`Operation`] is re-exported here.
pub use crate::Operation as ReplicaOp;

/// An in-memory representation of a task as a simple hashmap
pub type TaskMap = HashMap<String, String>;

#[cfg(test)]
pub(crate) fn taskmap_with(mut properties: Vec<(String, String)>) -> TaskMap {
    let mut rv = TaskMap::new();
    for (p, v) in properties.drain(..) {
        rv.insert(p, v);
    }
    rv
}

/// The type of VersionIds
use crate::server::VersionId;

/// The default for base_version, if none exists in the DB.
const DEFAULT_BASE_VERSION: Uuid = crate::server::NIL_VERSION_ID;

/// A Storage transaction, in which storage operations are performed.
///
/// # Concurrency
///
/// Serializable consistency must be maintained.  Concurrent access is unusual
/// and some implementations may simply apply a mutex to limit access to
/// one transaction at a time.
///
/// # Commiting and Aborting
///
/// A transaction is not visible to other readers until it is committed with
/// [`crate::storage::StorageTxn::commit`].  Transactions are aborted if they are dropped.
/// It is safe and performant to drop transactions that did not modify any data without committing.
#[async_trait]
pub trait StorageTxn: Send {
    /// Get an (immutable) task, if it is in the storage
    async fn get_task(&mut self, uuid: Uuid) -> Result<Option<TaskMap>>;

    /// Get a vector of all pending tasks from the working_set
    async fn get_pending_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>>;

    /// Create an (empty) task, only if it does not already exist.  Returns true if
    /// the task was created (did not already exist).
    async fn create_task(&mut self, uuid: Uuid) -> Result<bool>;

    /// Set a task, overwriting any existing task.  If the task does not exist, this implicitly
    /// creates it (use `get_task` to check first, if necessary).
    async fn set_task(&mut self, uuid: Uuid, task: TaskMap) -> Result<()>;

    /// Delete a task, if it exists.  Returns true if the task was deleted (already existed)
    async fn delete_task(&mut self, uuid: Uuid) -> Result<bool>;

    /// Get the uuids and bodies of all tasks in the storage, in undefined order.
    async fn all_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>>;

    /// Get the uuids of all tasks in the storage, in undefined order.
    async fn all_task_uuids(&mut self) -> Result<Vec<Uuid>>;

    /// Get the current base_version for this storage -- the last version synced from the server.
    async fn base_version(&mut self) -> Result<VersionId>;

    /// Set the current base_version for this storage.
    async fn set_base_version(&mut self, version: VersionId) -> Result<()>;

    /// Get the set of operations for the given task.
    async fn get_task_operations(&mut self, uuid: Uuid) -> Result<Vec<Operation>>;

    /// Get the current set of outstanding operations (operations that have not been synced to the
    /// server yet)
    async fn unsynced_operations(&mut self) -> Result<Vec<Operation>>;

    /// Get the current set of outstanding operations (operations that have not been synced to the
    /// server yet)
    async fn num_unsynced_operations(&mut self) -> Result<usize>;

    /// Add an operation to the end of the list of operations in the storage.  Note that this
    //. merely *stores* the operation; it is up to the TaskDb to apply it.
    async fn add_operation(&mut self, op: Operation) -> Result<()>;

    /// Remove an operation from the end of the list of operations in the storage.  The operation
    /// must exactly match the most recent operation, and must not be synced. Note that like
    /// `add_operation` this only affects the list of operations.
    async fn remove_operation(&mut self, op: Operation) -> Result<()>;

    /// A sync has been completed, so all operations should be marked as synced. The storage
    /// may perform additional cleanup at this time.
    async fn sync_complete(&mut self) -> Result<()>;

    /// Get the entire working set, with each task UUID at its appropriate (1-based) index.
    /// Element 0 is always None.
    async fn get_working_set(&mut self) -> Result<Vec<Option<Uuid>>>;

    /// Add a task to the working set and return its (one-based) index.  This index will be one greater
    /// than the highest used index.
    async fn add_to_working_set(&mut self, uuid: Uuid) -> Result<usize>;

    /// Update the working set task at the given index.  This cannot add a new item to the
    /// working set.
    async fn set_working_set_item(&mut self, index: usize, uuid: Option<Uuid>) -> Result<()>;

    /// Clear all tasks from the working set in preparation for a renumbering operation.
    /// Note that this is the only way items are removed from the set.
    async fn clear_working_set(&mut self) -> Result<()>;

    /// Check whether this storage is entirely empty
    #[allow(clippy::wrong_self_convention)] // mut is required here for storage access
    async fn is_empty(&mut self) -> Result<bool> {
        let mut empty = true;
        empty = empty && self.all_tasks().await?.is_empty();
        empty = empty && self.get_working_set().await? == vec![None];
        empty = empty && self.base_version().await? == Uuid::nil();
        empty = empty && self.unsynced_operations().await?.is_empty();
        Ok(empty)
    }

    /// Commit any changes made in the transaction.  It is an error to call this more than
    /// once.
    async fn commit(&mut self) -> Result<()>;
}

/// A trait for objects able to act as task storage.  Most of the interesting behavior is in the
/// [`crate::storage::StorageTxn`] trait.
#[async_trait]
pub trait Storage: Send {
    /// Begin a transaction
    async fn txn<'a>(&'a mut self) -> Result<Box<dyn StorageTxn + Send + 'a>>;
}
