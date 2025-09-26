use crate::errors::{Error, Result};
use crate::operation::Operation;
use crate::storage::config::AccessMode;
use crate::storage::sqlite::inner::SqliteStorageInner;
use crate::storage::{Storage, StorageTxn, TaskMap, VersionId};
use async_trait::async_trait;
use rusqlite::types::FromSql;
use rusqlite::ToSql;
use std::path::Path;
use std::sync::mpsc;
use std::thread;
use tokio::sync::oneshot;
use uuid::Uuid;

mod inner;
mod schema;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub(crate) enum SqliteError {
    #[error("SQLite transaction already committted")]
    TransactionAlreadyCommitted,
    #[error("Task storage was opened in read-only mode")]
    ReadOnlyStorage,
}

/// Newtype to allow implementing `FromSql` for foreign `uuid::Uuid`
pub(crate) struct StoredUuid(pub(crate) Uuid);

/// Conversion from Uuid stored as a string (rusqlite's uuid feature stores as binary blob)
impl FromSql for StoredUuid {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let u = Uuid::parse_str(value.as_str()?)
            .map_err(|_| rusqlite::types::FromSqlError::InvalidType)?;
        Ok(StoredUuid(u))
    }
}

/// Store Uuid as string in database
impl ToSql for StoredUuid {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        let s = self.0.to_string();
        Ok(s.into())
    }
}

/// An enum for messages sent to the sync thread actor.
pub(crate) enum ActorMessage {
    // Transaction control
    BeginTxn(oneshot::Sender<Result<mpsc::Sender<TxnMessage>>>),
}

pub(crate) enum TxnMessage {
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
    storage: SqliteStorageInner,
    receiver: mpsc::Receiver<ActorMessage>,
}

impl Actor {
    fn run(&mut self) {
        // The outer loop waits for a BeginTxn message. If the channel is disconnected,
        // the thread will exit gracefully.
        while let Ok(ActorMessage::BeginTxn(reply_sender)) = self.receiver.recv() {
            let (txn_sender, txn_receiver) = mpsc::channel::<TxnMessage>();
            match self.storage.txn() {
                Ok(mut txn) => {
                    // Send the new transaction channel sender back
                    if reply_sender.send(Ok(txn_sender)).is_err() {
                        log::warn!("Client disconnected before transaction could be established");
                        continue; // Don't handle the txn if the client is gone.
                    }
                    Self::handle_transaction(&txn_receiver, &mut txn);
                }
                Err(e) => {
                    // Send the database error back to the caller
                    log::error!("Could not start SQLite transaction: {e}");
                    let _ = reply_sender.send(Err(e));
                }
            }
        }
    }

    /// The inner loop for handling messages within an active transaction.
    fn handle_transaction(
        receiver: &mpsc::Receiver<TxnMessage>,
        txn: &mut crate::storage::sqlite::inner::Txn,
    ) {
        while let Ok(msg) = receiver.recv() {
            match msg {
                TxnMessage::Commit(resp) => {
                    let _ = resp.send(txn.commit());
                    return; // Transaction over, return to the outer loop.
                }
                TxnMessage::Rollback(resp) => {
                    // The sync txn is implicitly rolled back when it's dropped.
                    let _ = resp.send(Ok(()));
                    return; // Transaction over, return to the outer loop.
                }
                TxnMessage::GetTask(uuid, resp) => {
                    let _ = resp.send(txn.get_task(uuid));
                }
                TxnMessage::GetPendingTasks(resp) => {
                    let _ = resp.send(txn.get_pending_tasks());
                }
                TxnMessage::CreateTask(uuid, resp) => {
                    let _ = resp.send(txn.create_task(uuid));
                }
                TxnMessage::SetTask(uuid, t, resp) => {
                    let _ = resp.send(txn.set_task(uuid, t));
                }
                TxnMessage::DeleteTask(uuid, resp) => {
                    let _ = resp.send(txn.delete_task(uuid));
                }
                TxnMessage::AllTasks(resp) => {
                    let _ = resp.send(txn.all_tasks());
                }
                TxnMessage::AllTaskUuids(resp) => {
                    let _ = resp.send(txn.all_task_uuids());
                }
                TxnMessage::BaseVersion(resp) => {
                    let _ = resp.send(txn.base_version());
                }
                TxnMessage::SetBaseVersion(v, resp) => {
                    let _ = resp.send(txn.set_base_version(v));
                }
                TxnMessage::GetTaskOperations(u, resp) => {
                    let _ = resp.send(txn.get_task_operations(u));
                }
                TxnMessage::UnsyncedOperations(resp) => {
                    let _ = resp.send(txn.unsynced_operations());
                }
                TxnMessage::NumUnsyncedOperations(resp) => {
                    let _ = resp.send(txn.num_unsynced_operations());
                }
                TxnMessage::AddOperation(o, resp) => {
                    let _ = resp.send(txn.add_operation(o));
                }
                TxnMessage::RemoveOperation(o, resp) => {
                    let _ = resp.send(txn.remove_operation(o));
                }
                TxnMessage::SyncComplete(resp) => {
                    let _ = resp.send(txn.sync_complete());
                }
                TxnMessage::GetWorkingSet(resp) => {
                    let _ = resp.send(txn.get_working_set());
                }
                TxnMessage::AddToWorkingSet(u, resp) => {
                    let _ = resp.send(txn.add_to_working_set(u));
                }
                TxnMessage::SetWorkingSetItem(i, u, resp) => {
                    let _ = resp.send(txn.set_working_set_item(i, u));
                }
                TxnMessage::ClearWorkingSet(resp) => {
                    let _ = resp.send(txn.clear_working_set());
                }
            };
        }
    }
}

/// SqliteStorageActor is an async actor wrapper for the sync SqliteStorage.
#[derive(Clone)]
pub struct SqliteStorage {
    sender: mpsc::Sender<ActorMessage>,
}

impl SqliteStorage {
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
            match SqliteStorageInner::new(path, access_mode, create_if_missing) {
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
impl Storage for SqliteStorage {
    // Sends the BeginTxn message to the underlying SqliteStorage. Now that the txn has
    // begun, this async txn obj can be passed around and operated upon, as it
    // communicates with the underlying sync txn.
    async fn txn<'a>(&'a mut self) -> Result<Box<dyn StorageTxn + Send + 'a>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(ActorMessage::BeginTxn(reply_tx))
            .map_err(|e| Error::Other(e.into()))?;
        let txn_sender = reply_rx.await??;
        Ok(Box::new(ActorTxn::new(txn_sender)))
    }
}

/// An async proxy for a transaction running on the sync actor thread.
pub(super) struct ActorTxn {
    sender: mpsc::Sender<TxnMessage>,
    committed: bool,
}

impl ActorTxn {
    fn new(sender: mpsc::Sender<TxnMessage>) -> Self {
        Self {
            sender,
            committed: false,
        }
    }

    async fn call<R, F>(&self, f: F) -> Result<R>
    where
        F: FnOnce(oneshot::Sender<Result<R>>) -> TxnMessage,
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
            let _ = self.sender.send(TxnMessage::Rollback(tx));
        }
    }
}

#[async_trait]
impl StorageTxn for ActorTxn {
    async fn commit(&mut self) -> Result<()> {
        let res = self.call(TxnMessage::Commit).await;
        if res.is_ok() {
            self.committed = true;
        }
        res
    }

    async fn get_task(&mut self, uuid: Uuid) -> Result<Option<TaskMap>> {
        self.call(|tx| TxnMessage::GetTask(uuid, tx)).await
    }

    async fn create_task(&mut self, uuid: Uuid) -> Result<bool> {
        self.call(|tx| TxnMessage::CreateTask(uuid, tx)).await
    }

    async fn set_task(&mut self, uuid: Uuid, task: TaskMap) -> Result<()> {
        self.call(|tx| TxnMessage::SetTask(uuid, task, tx)).await
    }

    async fn delete_task(&mut self, uuid: Uuid) -> Result<bool> {
        self.call(|tx| TxnMessage::DeleteTask(uuid, tx)).await
    }

    async fn get_pending_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        self.call(TxnMessage::GetPendingTasks).await
    }

    async fn all_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        self.call(TxnMessage::AllTasks).await
    }

    async fn all_task_uuids(&mut self) -> Result<Vec<Uuid>> {
        self.call(TxnMessage::AllTaskUuids).await
    }

    async fn base_version(&mut self) -> Result<VersionId> {
        self.call(TxnMessage::BaseVersion).await
    }

    async fn set_base_version(&mut self, version: VersionId) -> Result<()> {
        self.call(|tx| TxnMessage::SetBaseVersion(version, tx))
            .await
    }

    async fn get_task_operations(&mut self, uuid: Uuid) -> Result<Vec<Operation>> {
        self.call(|tx| TxnMessage::GetTaskOperations(uuid, tx))
            .await
    }

    async fn unsynced_operations(&mut self) -> Result<Vec<Operation>> {
        self.call(TxnMessage::UnsyncedOperations).await
    }

    async fn num_unsynced_operations(&mut self) -> Result<usize> {
        self.call(TxnMessage::NumUnsyncedOperations).await
    }

    async fn add_operation(&mut self, op: Operation) -> Result<()> {
        self.call(|tx| TxnMessage::AddOperation(op, tx)).await
    }

    async fn remove_operation(&mut self, op: Operation) -> Result<()> {
        self.call(|tx| TxnMessage::RemoveOperation(op, tx)).await
    }

    async fn sync_complete(&mut self) -> Result<()> {
        self.call(TxnMessage::SyncComplete).await
    }

    async fn get_working_set(&mut self) -> Result<Vec<Option<Uuid>>> {
        self.call(TxnMessage::GetWorkingSet).await
    }

    async fn add_to_working_set(&mut self, uuid: Uuid) -> Result<usize> {
        self.call(|tx| TxnMessage::AddToWorkingSet(uuid, tx)).await
    }

    async fn set_working_set_item(&mut self, index: usize, uuid: Option<Uuid>) -> Result<()> {
        self.call(|tx| TxnMessage::SetWorkingSetItem(index, uuid, tx))
            .await
    }

    async fn clear_working_set(&mut self) -> Result<()> {
        self.call(TxnMessage::ClearWorkingSet).await
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::config::AccessMode;
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;

    fn storage() -> Result<SqliteStorage> {
        let tmp_dir = TempDir::new()?;
        SqliteStorage::new(tmp_dir.path(), AccessMode::ReadWrite, true)
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
        let result = SqliteStorage::new(&file_path, AccessMode::ReadWrite, true);
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
