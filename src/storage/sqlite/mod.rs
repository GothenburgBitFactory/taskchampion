use crate::errors::Result;
use crate::operation::Operation;
use crate::storage::config::AccessMode;
use crate::storage::sqlite::inner::SqliteStorageInner;
use crate::storage::{actor, TaskMap, VersionId};
use rusqlite::types::FromSql;
use rusqlite::ToSql;
use std::path::Path;
use tokio::sync::mpsc::{
    unbounded_channel as channel, UnboundedReceiver as Receiver, UnboundedSender as Sender,
};
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
pub enum SqliteActorMessage {
    // Transaction control
    BeginTxn(oneshot::Sender<Result<Sender<SqliteTxnMessage>>>),
}

impl actor::ActorMessage for SqliteActorMessage {
    type TxnMessage = SqliteTxnMessage;

    fn begin_txn_message(reply_sender: oneshot::Sender<Result<Sender<Self::TxnMessage>>>) -> Self {
        Self::BeginTxn(reply_sender)
    }
}

pub enum SqliteTxnMessage {
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

impl actor::TxnMessage for SqliteTxnMessage {
    fn commit_message(reply_sender: oneshot::Sender<Result<()>>) -> Self {
        Self::Commit(reply_sender)
    }

    fn rollback_message(reply_sender: oneshot::Sender<Result<()>>) -> Self {
        Self::Rollback(reply_sender)
    }

    fn get_task_message(
        uuid: Uuid,
        reply_sender: oneshot::Sender<Result<Option<TaskMap>>>,
    ) -> Self {
        Self::GetTask(uuid, reply_sender)
    }

    fn create_task_message(uuid: Uuid, reply_sender: oneshot::Sender<Result<bool>>) -> Self {
        Self::CreateTask(uuid, reply_sender)
    }

    fn set_task_message(
        uuid: Uuid,
        task: TaskMap,
        reply_sender: oneshot::Sender<Result<()>>,
    ) -> Self {
        Self::SetTask(uuid, task, reply_sender)
    }

    fn delete_task_message(uuid: Uuid, reply_sender: oneshot::Sender<Result<bool>>) -> Self {
        Self::DeleteTask(uuid, reply_sender)
    }

    fn get_pending_tasks_message(
        reply_sender: oneshot::Sender<Result<Vec<(Uuid, TaskMap)>>>,
    ) -> Self {
        Self::GetPendingTasks(reply_sender)
    }

    fn all_tasks_message(reply_sender: oneshot::Sender<Result<Vec<(Uuid, TaskMap)>>>) -> Self {
        Self::AllTasks(reply_sender)
    }

    fn all_task_uuids_message(reply_sender: oneshot::Sender<Result<Vec<Uuid>>>) -> Self {
        Self::AllTaskUuids(reply_sender)
    }

    fn base_version_message(reply_sender: oneshot::Sender<Result<VersionId>>) -> Self {
        Self::BaseVersion(reply_sender)
    }

    fn set_base_version_message(
        version: VersionId,
        reply_sender: oneshot::Sender<Result<()>>,
    ) -> Self {
        Self::SetBaseVersion(version, reply_sender)
    }

    fn get_task_operations_message(
        uuid: Uuid,
        reply_sender: oneshot::Sender<Result<Vec<Operation>>>,
    ) -> Self {
        Self::GetTaskOperations(uuid, reply_sender)
    }

    fn unsynced_operations_message(reply_sender: oneshot::Sender<Result<Vec<Operation>>>) -> Self {
        Self::UnsyncedOperations(reply_sender)
    }

    fn num_unsynced_operations_message(reply_sender: oneshot::Sender<Result<usize>>) -> Self {
        Self::NumUnsyncedOperations(reply_sender)
    }

    fn add_operation_message(op: Operation, reply_sender: oneshot::Sender<Result<()>>) -> Self {
        Self::AddOperation(op, reply_sender)
    }

    fn remove_operation_message(op: Operation, reply_sender: oneshot::Sender<Result<()>>) -> Self {
        Self::RemoveOperation(op, reply_sender)
    }

    fn sync_complete_message(reply_sender: oneshot::Sender<Result<()>>) -> Self {
        Self::SyncComplete(reply_sender)
    }

    fn get_working_set_message(reply_sender: oneshot::Sender<Result<Vec<Option<Uuid>>>>) -> Self {
        Self::GetWorkingSet(reply_sender)
    }

    fn add_to_working_set_message(
        uuid: Uuid,
        reply_sender: oneshot::Sender<Result<usize>>,
    ) -> Self {
        Self::AddToWorkingSet(uuid, reply_sender)
    }

    fn set_working_set_item_message(
        index: usize,
        uuid: Option<Uuid>,
        reply_sender: oneshot::Sender<Result<()>>,
    ) -> Self {
        Self::SetWorkingSetItem(index, uuid, reply_sender)
    }

    fn clear_working_set_message(reply_sender: oneshot::Sender<Result<()>>) -> Self {
        Self::ClearWorkingSet(reply_sender)
    }
}

/// State owned by the dedicated synchronous thread. It handles the low-level,
/// sync db ops.
struct Actor {
    storage: SqliteStorageInner,
}

impl Actor {
    fn run(mut self, mut receiver: Receiver<SqliteActorMessage>) {
        // The outer loop waits for a BeginTxn message. If the channel is disconnected,
        // the thread will exit gracefully.
        while let Some(SqliteActorMessage::BeginTxn(reply_sender)) = receiver.blocking_recv() {
            let (txn_sender, mut txn_receiver) = channel::<SqliteTxnMessage>();
            match self.storage.txn() {
                Ok(mut txn) => {
                    // Send the new transaction channel sender back
                    if reply_sender.send(Ok(txn_sender)).is_err() {
                        log::warn!("Client disconnected before transaction could be established");
                        continue; // Don't handle the txn if the client is gone.
                    }
                    Self::handle_transaction(&mut txn_receiver, &mut txn);
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
        receiver: &mut Receiver<SqliteTxnMessage>,
        txn: &mut crate::storage::sqlite::inner::Txn,
    ) {
        while let Some(msg) = receiver.blocking_recv() {
            match msg {
                SqliteTxnMessage::Commit(resp) => {
                    let _ = resp.send(txn.commit());
                    return; // Transaction over, return to the outer loop.
                }
                SqliteTxnMessage::Rollback(resp) => {
                    // The sync txn is implicitly rolled back when it's dropped.
                    let _ = resp.send(Ok(()));
                    return; // Transaction over, return to the outer loop.
                }
                SqliteTxnMessage::GetTask(uuid, resp) => {
                    let _ = resp.send(txn.get_task(uuid));
                }
                SqliteTxnMessage::GetPendingTasks(resp) => {
                    let _ = resp.send(txn.get_pending_tasks());
                }
                SqliteTxnMessage::CreateTask(uuid, resp) => {
                    let _ = resp.send(txn.create_task(uuid));
                }
                SqliteTxnMessage::SetTask(uuid, t, resp) => {
                    let _ = resp.send(txn.set_task(uuid, t));
                }
                SqliteTxnMessage::DeleteTask(uuid, resp) => {
                    let _ = resp.send(txn.delete_task(uuid));
                }
                SqliteTxnMessage::AllTasks(resp) => {
                    let _ = resp.send(txn.all_tasks());
                }
                SqliteTxnMessage::AllTaskUuids(resp) => {
                    let _ = resp.send(txn.all_task_uuids());
                }
                SqliteTxnMessage::BaseVersion(resp) => {
                    let _ = resp.send(txn.base_version());
                }
                SqliteTxnMessage::SetBaseVersion(v, resp) => {
                    let _ = resp.send(txn.set_base_version(v));
                }
                SqliteTxnMessage::GetTaskOperations(u, resp) => {
                    let _ = resp.send(txn.get_task_operations(u));
                }
                SqliteTxnMessage::UnsyncedOperations(resp) => {
                    let _ = resp.send(txn.unsynced_operations());
                }
                SqliteTxnMessage::NumUnsyncedOperations(resp) => {
                    let _ = resp.send(txn.num_unsynced_operations());
                }
                SqliteTxnMessage::AddOperation(o, resp) => {
                    let _ = resp.send(txn.add_operation(o));
                }
                SqliteTxnMessage::RemoveOperation(o, resp) => {
                    let _ = resp.send(txn.remove_operation(o));
                }
                SqliteTxnMessage::SyncComplete(resp) => {
                    let _ = resp.send(txn.sync_complete());
                }
                SqliteTxnMessage::GetWorkingSet(resp) => {
                    let _ = resp.send(txn.get_working_set());
                }
                SqliteTxnMessage::AddToWorkingSet(u, resp) => {
                    let _ = resp.send(txn.add_to_working_set(u));
                }
                SqliteTxnMessage::SetWorkingSetItem(i, u, resp) => {
                    let _ = resp.send(txn.set_working_set_item(i, u));
                }
                SqliteTxnMessage::ClearWorkingSet(resp) => {
                    let _ = resp.send(txn.clear_working_set());
                }
            };
        }
    }
}

/// An async actor-based SQLite-backed storage backend.
pub type SqliteStorage = actor::ActorStorage<SqliteActorMessage, SqliteTxnMessage>;

impl SqliteStorage {
    pub fn new<P: AsRef<Path>>(
        path: P,
        access_mode: AccessMode,
        create_if_missing: bool,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Use a sync_channel to block until the thread has initialized.
        let (init_sender, init_receiver) = std::sync::mpsc::sync_channel(0);

        let actor_fn = move |receiver| {
            match SqliteStorageInner::new(path, access_mode, create_if_missing) {
                Ok(storage) => {
                    // Send Ok back to the caller of `new` and then start the actor loop.
                    init_sender.send(Ok(())).unwrap();
                    let actor = Actor { storage };
                    actor.run(receiver);
                }
                Err(e) => {
                    // Send the initialization error back.
                    init_sender.send(Err(e)).unwrap();
                }
            }
        };

        let storage = Self::new_internal(actor_fn);

        // Block until the thread sends its initialization result.
        init_receiver.recv().unwrap()?;
        Ok(storage)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::errors::Error;
    use crate::storage::config::AccessMode;
    use crate::storage::Storage;
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
