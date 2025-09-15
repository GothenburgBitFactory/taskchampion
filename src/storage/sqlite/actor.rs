use async_trait::async_trait;
use std::path::Path;
use std::sync::mpsc;
use std::thread;
use tokio::sync::oneshot;

use crate::errors::{Error, Result};
use crate::operation::Operation;
use crate::storage::config::AccessMode;
use crate::storage::sqlite::SqliteStorage;
use crate::storage::{Storage, StorageTxn, TaskMap, VersionId};
use uuid::Uuid;

/// An enum for messages sent to the sync thread actor.
pub(crate) enum ActorMessage {
    // Transaction control
    BeginTxn,
    Commit(oneshot::Sender<Result<()>>),
    Rollback(oneshot::Sender<Result<()>>),

    // Transactional operations
    GetTask(Uuid, oneshot::Sender<Result<Option<TaskMap>>>),
    GetPendingTasks(oneshot::Sender<Result<Vec<(Uuid, TaskMap)>>>),
    CreateTask(Uuid, oneshot::Sender<Result<bool>>),
    SetTask(Uuid, TaskMap, oneshot::Sender<Result<()>>),
    DeleteTask(Uuid, oneshot::Sender<Result<bool>>),
    AllTasks(oneshot::Sender<Result<Vec<(Uuid, TaskMap)>>>),
    AllTaskUuids(oneshot::Sender<Result<Vec<Uuid>>>),
    BaseVersion(oneshot::Sender<Result<VersionId>>),
    SetBaseVersion(VersionId, oneshot::Sender<Result<()>>),
    GetTaskOperations(Uuid, oneshot::Sender<Result<Vec<Operation>>>),
    UnsyncedOperations(oneshot::Sender<Result<Vec<Operation>>>),
    NumUnsyncedOperations(oneshot::Sender<Result<usize>>),
    AddOperation(Operation, oneshot::Sender<Result<()>>),
    RemoveOperation(Operation, oneshot::Sender<Result<()>>),
    SyncComplete(oneshot::Sender<Result<()>>),
    GetWorkingSet(oneshot::Sender<Result<Vec<Option<Uuid>>>>),
    AddToWorkingSet(Uuid, oneshot::Sender<Result<usize>>),
    SetWorkingSetItem(usize, Option<Uuid>, oneshot::Sender<Result<()>>),
    ClearWorkingSet(oneshot::Sender<Result<()>>),
}

/// State owned by the dedicated synchronous thread. It handles the low-level,
/// sync db ops.
struct Actor {
    storage: SqliteStorage,
    receiver: mpsc::Receiver<ActorMessage>,
}

impl Actor {
    fn run(&mut self) {
        // The outer loop waits for a BeginTxn message. If the channel is disconnected,
        // the thread will exit gracefully.
        while let Ok(ActorMessage::BeginTxn) = self.receiver.recv() {
            match self.storage.txn() {
                Ok(mut txn) => Self::handle_transaction(&self.receiver, &mut txn),
                Err(e) => {
                    // If we can't start a transaction, something is wrong.
                    // Log the error and exit the thread.
                    log::error!("Could not start SQLite transaction: {e}");
                    break;
                }
            }
        }
        // Exiting the loop means the sender was dropped, so the thread can terminate.
    }

    /// The inner loop for handling messages within an active transaction.
    fn handle_transaction(
        receiver: &mpsc::Receiver<ActorMessage>,
        txn: &mut crate::storage::sqlite::Txn,
    ) {
        while let Ok(msg) = receiver.recv() {
            match msg {
                ActorMessage::Commit(resp) => {
                    let _ = resp.send(txn.commit());
                    return; // Transaction over, return to the outer loop.
                }
                ActorMessage::Rollback(resp) => {
                    // The sync txn is implicitly rolled back when it's dropped.
                    let _ = resp.send(Ok(()));
                    return; // Transaction over, return to the outer loop.
                }
                ActorMessage::GetTask(uuid, resp) => {
                    let _ = resp.send(txn.get_task(uuid));
                }
                ActorMessage::GetPendingTasks(resp) => {
                    let _ = resp.send(txn.get_pending_tasks());
                }
                ActorMessage::CreateTask(uuid, resp) => {
                    let _ = resp.send(txn.create_task(uuid));
                }
                ActorMessage::SetTask(uuid, t, resp) => {
                    let _ = resp.send(txn.set_task(uuid, t));
                }
                ActorMessage::DeleteTask(uuid, resp) => {
                    let _ = resp.send(txn.delete_task(uuid));
                }
                ActorMessage::AllTasks(resp) => {
                    let _ = resp.send(txn.all_tasks());
                }
                ActorMessage::AllTaskUuids(resp) => {
                    let _ = resp.send(txn.all_task_uuids());
                }
                ActorMessage::BaseVersion(resp) => {
                    let _ = resp.send(txn.base_version());
                }
                ActorMessage::SetBaseVersion(v, resp) => {
                    let _ = resp.send(txn.set_base_version(v));
                }
                ActorMessage::GetTaskOperations(u, resp) => {
                    let _ = resp.send(txn.get_task_operations(u));
                }
                ActorMessage::UnsyncedOperations(resp) => {
                    let _ = resp.send(txn.unsynced_operations());
                }
                ActorMessage::NumUnsyncedOperations(resp) => {
                    let _ = resp.send(txn.num_unsynced_operations());
                }
                ActorMessage::AddOperation(o, resp) => {
                    let _ = resp.send(txn.add_operation(o));
                }
                ActorMessage::RemoveOperation(o, resp) => {
                    let _ = resp.send(txn.remove_operation(o));
                }
                ActorMessage::SyncComplete(resp) => {
                    let _ = resp.send(txn.sync_complete());
                }
                ActorMessage::GetWorkingSet(resp) => {
                    let _ = resp.send(txn.get_working_set());
                }
                ActorMessage::AddToWorkingSet(u, resp) => {
                    let _ = resp.send(txn.add_to_working_set(u));
                }
                ActorMessage::SetWorkingSetItem(i, u, resp) => {
                    let _ = resp.send(txn.set_working_set_item(i, u));
                }
                ActorMessage::ClearWorkingSet(resp) => {
                    let _ = resp.send(txn.clear_working_set());
                }
                ActorMessage::BeginTxn => {
                    // This is a logic error. A new transaction was requested
                    // while one was already in progress.
                    log::error!("Received BeginTxn while a transaction was already in progress");
                    // We break here, which will cause the sync txn to be rolled back.
                    return;
                }
            };
        }
    }
}

/// SqliteStorageActor is an async actor wrapper for the sync SqliteStorage.
#[derive(Clone)]
pub struct SqliteStorageActor {
    sender: mpsc::Sender<ActorMessage>,
}

impl SqliteStorageActor {
    pub fn new<P: AsRef<Path>>(
        path: P,
        access_mode: AccessMode,
        create_if_missing: bool,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::channel();
        let path = path.as_ref().to_path_buf();

        // Use a sync_channel to block until the thread has initialized.
        let (init_sender, init_receiver) = mpsc::sync_channel(0);

        thread::spawn(move || {
            match SqliteStorage::new(path, access_mode, create_if_missing) {
                Ok(storage) => {
                    // Send Ok back to the caller of `new` and then start the actor loop.
                    init_sender.send(Ok(())).unwrap();
                    let mut actor = Actor { storage, receiver };
                    actor.run();
                }
                Err(e) => {
                    // Send the initialization error back.
                    init_sender.send(Err(e)).unwrap();
                }
            }
        });

        // Block until the thread sends its initialization result.
        init_receiver.recv().unwrap()?;
        Ok(Self { sender })
    }
}

#[async_trait]
impl Storage for SqliteStorageActor {
    // Sends the BeginTxn message to the underlying SqliteStorage. Now that the txn has
    // begun, this async txn obj can be passed around and operated upon, as it
    // communicates with the underlying sync txn.
    async fn txn<'a>(&'a mut self) -> Result<Box<dyn StorageTxn + Send + 'a>> {
        self.sender
            .send(ActorMessage::BeginTxn)
            .map_err(|e| Error::Other(e.into()))?;
        Ok(Box::new(ActorTxn::new(self.sender.clone())))
    }
}

/// An async proxy for a transaction running on the sync actor thread.
pub(crate) struct ActorTxn {
    sender: mpsc::Sender<ActorMessage>,
    committed: bool,
}

impl ActorTxn {
    fn new(sender: mpsc::Sender<ActorMessage>) -> Self {
        Self {
            sender,
            committed: false,
        }
    }

    async fn call<R, F>(&self, f: F) -> Result<R>
    where
        F: FnOnce(oneshot::Sender<Result<R>>) -> ActorMessage,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(f(tx))
            .map_err(|e| Error::Other(e.into()))?;
        rx.await?
    }
}

impl Drop for ActorTxn {
    fn drop(&mut self) {
        if !self.committed {
            // If the transaction proxy is dropped without being committed,
            // we send a Rollback message. We don't need to wait for the response.
            let (tx, _rx) = oneshot::channel();
            let _ = self.sender.send(ActorMessage::Rollback(tx));
        }
    }
}

#[async_trait]
impl StorageTxn for ActorTxn {
    async fn commit(&mut self) -> Result<()> {
        let res = self.call(ActorMessage::Commit).await;
        if res.is_ok() {
            self.committed = true;
        }
        res
    }

    async fn get_task(&mut self, uuid: Uuid) -> Result<Option<TaskMap>> {
        self.call(|tx| ActorMessage::GetTask(uuid, tx)).await
    }

    async fn create_task(&mut self, uuid: Uuid) -> Result<bool> {
        self.call(|tx| ActorMessage::CreateTask(uuid, tx)).await
    }

    async fn set_task(&mut self, uuid: Uuid, task: TaskMap) -> Result<()> {
        self.call(|tx| ActorMessage::SetTask(uuid, task, tx)).await
    }

    async fn delete_task(&mut self, uuid: Uuid) -> Result<bool> {
        self.call(|tx| ActorMessage::DeleteTask(uuid, tx)).await
    }

    async fn get_pending_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        self.call(ActorMessage::GetPendingTasks).await
    }

    async fn all_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        self.call(ActorMessage::AllTasks).await
    }

    async fn all_task_uuids(&mut self) -> Result<Vec<Uuid>> {
        self.call(ActorMessage::AllTaskUuids).await
    }

    async fn base_version(&mut self) -> Result<VersionId> {
        self.call(ActorMessage::BaseVersion).await
    }

    async fn set_base_version(&mut self, version: VersionId) -> Result<()> {
        self.call(|tx| ActorMessage::SetBaseVersion(version, tx))
            .await
    }

    async fn get_task_operations(&mut self, uuid: Uuid) -> Result<Vec<Operation>> {
        self.call(|tx| ActorMessage::GetTaskOperations(uuid, tx))
            .await
    }

    async fn unsynced_operations(&mut self) -> Result<Vec<Operation>> {
        self.call(ActorMessage::UnsyncedOperations).await
    }

    async fn num_unsynced_operations(&mut self) -> Result<usize> {
        self.call(ActorMessage::NumUnsyncedOperations).await
    }

    async fn add_operation(&mut self, op: Operation) -> Result<()> {
        self.call(|tx| ActorMessage::AddOperation(op, tx)).await
    }

    async fn remove_operation(&mut self, op: Operation) -> Result<()> {
        self.call(|tx| ActorMessage::RemoveOperation(op, tx)).await
    }

    async fn sync_complete(&mut self) -> Result<()> {
        self.call(ActorMessage::SyncComplete).await
    }

    async fn get_working_set(&mut self) -> Result<Vec<Option<Uuid>>> {
        self.call(ActorMessage::GetWorkingSet).await
    }

    async fn add_to_working_set(&mut self, uuid: Uuid) -> Result<usize> {
        self.call(|tx| ActorMessage::AddToWorkingSet(uuid, tx))
            .await
    }

    async fn set_working_set_item(&mut self, index: usize, uuid: Option<Uuid>) -> Result<()> {
        self.call(|tx| ActorMessage::SetWorkingSetItem(index, uuid, tx))
            .await
    }

    async fn clear_working_set(&mut self) -> Result<()> {
        self.call(ActorMessage::ClearWorkingSet).await
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::config::AccessMode;
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;

    fn storage() -> Result<SqliteStorageActor> {
        let tmp_dir = TempDir::new()?;
        SqliteStorageActor::new(tmp_dir.path(), AccessMode::ReadWrite, true)
    }

    #[tokio::test]
    async fn test_implicit_rollback() -> Result<()> {
        let mut storage = storage()?;
        let uuid = Uuid::new_v4();

        // Begin a transaction, create a task, but do not commit.
        // The transaction will go out of scope, triggering Drop.
        {
            let mut txn = storage.txn().await?;
            assert!(txn.create_task(uuid).await?);
            // txn is dropped here, which should trigger a rollback message.
        }

        // Begin a new transaction and verify the task does not exist.
        let mut txn = storage.txn().await?;
        let task = txn.get_task(uuid).await?;
        assert_eq!(task, None, "Task should not exist after implicit rollback");

        Ok(())
    }

    #[tokio::test]
    async fn test_init_failure() -> Result<()> {
        let tmp_dir = TempDir::new()?;
        let file_path = tmp_dir.path().join("a_file");
        std::fs::write(&file_path, "I am a file, not a directory")?;

        // Try to create the storage inside a path that is a file, not a directory.
        // This should cause SqliteStorage::new to fail inside the actor thread.
        let result = SqliteStorageActor::new(&file_path, AccessMode::ReadWrite, true);
        assert!(
            result.is_err(),
            "Initialization should fail for an invalid path"
        );

        // Check for the expected error message propagated from the actor thread.
        if let Err(Error::Database(msg)) = result {
            assert!(
                msg.contains("Cannot create directory"),
                "Error message should indicate a directory creation problem"
            );
        } else {
            panic!("Expected a Database error");
        }

        Ok(())
    }
}
