//! A generic storage backend based on the actor pattern.
//
use crate::errors::{Error, Result};
use crate::operation::Operation;
use crate::storage::{Storage, StorageTxn, TaskMap, VersionId};
use async_trait::async_trait;
use tokio::sync::{mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender}, oneshot};
use uuid::Uuid;

/// A generic storage backend based on the actor pattern.
///
/// This struct provides an asynchronous interface to a synchronous storage backend by
/// communicating with an "actor" thread.
///
/// Type Parameters:
/// - `A`: The message type for the actor. This type must implement the `ActorMessage` trait.
/// - `T`: The message type for a transaction. This type must implement the `TxnMessage` trait.
pub struct ActorStorage<A, T> {
    sender: UnboundedSender<A>,
    _phantom: std::marker::PhantomData<T>,
}

impl<A: Send + 'static, T> ActorStorage<A, T> {
    pub(super) fn new_internal<F>(actor_fn: F) -> Self
    where
        F: FnOnce(UnboundedReceiver<A>),
        F: Send + 'static,
    {
        let (sender, receiver) = unbounded_channel();
        std::thread::spawn(move || actor_fn(receiver));
        Self {
            sender,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<A, T> Storage for ActorStorage<A, T>
where
    A: ActorMessage<TxnMessage = T> + Send + Sync + 'static,
    T: TxnMessage + Send + Sync + 'static,
{
    async fn txn<'a>(&'a mut self) -> Result<Box<dyn StorageTxn + Send + 'a>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .send(A::begin_txn_message(reply_tx))
            .map_err(|e| Error::Other(e.into()))?;
        let txn_sender = reply_rx.await??;
        Ok(Box::new(ActorTxn::new(txn_sender)))
    }
}

/// The message type for the actor.
pub trait ActorMessage {
    /// The message type for a transaction.
    type TxnMessage;

    /// Create a `BeginTxn` message.
    fn begin_txn_message(
        reply_sender: oneshot::Sender<Result<UnboundedSender<Self::TxnMessage>>>,
    ) -> Self;
}

/// An async proxy for a transaction running on the sync actor thread.
pub(super) struct ActorTxn<T: TxnMessage + Send + Sync + 'static> {
    sender: UnboundedSender<T>,
    committed: bool,
}

impl<T: TxnMessage + Send + Sync + 'static> ActorTxn<T> {
    fn new(sender: UnboundedSender<T>) -> Self {
        Self {
            sender,
            committed: false,
        }
    }

    pub(super) async fn call<R, F>(&self, f: F) -> Result<R>
    where
        F: FnOnce(oneshot::Sender<Result<R>>) -> T,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        self.sender
            .send(f(tx))
            .map_err(|e| Error::Other(e.into()))?;
        rx.await?
    }
}

impl<T: TxnMessage + Send + Sync + 'static> Drop for ActorTxn<T> {
    fn drop(&mut self) {
        if !self.committed {
            // If the transaction proxy is dropped without being committed,
            // we send a Rollback message. We don't need to wait for the response.
            let (tx, _rx) = oneshot::channel();
            let _ = self.sender.send(T::rollback_message(tx));
        }
    }
}

#[async_trait]
impl<T: TxnMessage + Send + Sync + 'static> StorageTxn for ActorTxn<T> {
    async fn get_task(&mut self, uuid: Uuid) -> Result<Option<TaskMap>> {
        self.call(|tx| T::get_task_message(uuid, tx)).await
    }

    async fn create_task(&mut self, uuid: Uuid) -> Result<bool> {
        self.call(|tx| T::create_task_message(uuid, tx)).await
    }

    async fn set_task(&mut self, uuid: Uuid, task: TaskMap) -> Result<()> {
        self.call(|tx| T::set_task_message(uuid, task, tx)).await
    }

    async fn delete_task(&mut self, uuid: Uuid) -> Result<bool> {
        self.call(|tx| T::delete_task_message(uuid, tx)).await
    }

    async fn get_pending_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        self.call(T::get_pending_tasks_message).await
    }

    async fn all_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        self.call(T::all_tasks_message).await
    }

    async fn all_task_uuids(&mut self) -> Result<Vec<Uuid>> {
        self.call(T::all_task_uuids_message).await
    }

    async fn base_version(&mut self) -> Result<VersionId> {
        self.call(T::base_version_message).await
    }

    async fn set_base_version(&mut self, version: VersionId) -> Result<()> {
        self.call(|tx| T::set_base_version_message(version, tx))
            .await
    }

    async fn get_task_operations(&mut self, uuid: Uuid) -> Result<Vec<Operation>> {
        self.call(|tx| T::get_task_operations_message(uuid, tx))
            .await
    }

    async fn unsynced_operations(&mut self) -> Result<Vec<Operation>> {
        self.call(T::unsynced_operations_message).await
    }

    async fn num_unsynced_operations(&mut self) -> Result<usize> {
        self.call(T::num_unsynced_operations_message).await
    }

    async fn add_operation(&mut self, op: Operation) -> Result<()> {
        self.call(|tx| T::add_operation_message(op, tx)).await
    }

    async fn remove_operation(&mut self, op: Operation) -> Result<()> {
        self.call(|tx| T::remove_operation_message(op, tx)).await
    }

    async fn sync_complete(&mut self) -> Result<()> {
        self.call(T::sync_complete_message).await
    }

    async fn get_working_set(&mut self) -> Result<Vec<Option<Uuid>>> {
        self.call(T::get_working_set_message).await
    }

    async fn add_to_working_set(&mut self, uuid: Uuid) -> Result<usize> {
        self.call(|tx| T::add_to_working_set_message(uuid, tx))
            .await
    }

    async fn set_working_set_item(&mut self, index: usize, uuid: Option<Uuid>) -> Result<()> {
        self.call(|tx| T::set_working_set_item_message(index, uuid, tx))
            .await
    }

    async fn clear_working_set(&mut self) -> Result<()> {
        self.call(T::clear_working_set_message).await
    }

    async fn commit(&mut self) -> Result<()> {
        let res = self.call(T::commit_message).await;
        if res.is_ok() {
            self.committed = true;
        }
        res
    }
}

/// The message type for a transaction.
pub trait TxnMessage {
    /// Create a `Commit` message.
    fn commit_message(reply_sender: oneshot::Sender<Result<()>>) -> Self;

    /// Create a `Rollback` message.
    fn rollback_message(reply_sender: oneshot::Sender<Result<()>>) -> Self;

    fn get_task_message(uuid: Uuid, reply_sender: oneshot::Sender<Result<Option<TaskMap>>>)
        -> Self;

    fn create_task_message(uuid: Uuid, reply_sender: oneshot::Sender<Result<bool>>) -> Self;

    fn set_task_message(
        uuid: Uuid,
        task: TaskMap,
        reply_sender: oneshot::Sender<Result<()>>,
    ) -> Self;

    fn delete_task_message(uuid: Uuid, reply_sender: oneshot::Sender<Result<bool>>) -> Self;

    fn get_pending_tasks_message(
        reply_sender: oneshot::Sender<Result<Vec<(Uuid, TaskMap)>>>,
    ) -> Self;

    fn all_tasks_message(reply_sender: oneshot::Sender<Result<Vec<(Uuid, TaskMap)>>>) -> Self;

    fn all_task_uuids_message(reply_sender: oneshot::Sender<Result<Vec<Uuid>>>) -> Self;

    fn base_version_message(reply_sender: oneshot::Sender<Result<VersionId>>) -> Self;

    fn set_base_version_message(
        version: VersionId,
        reply_sender: oneshot::Sender<Result<()>>,
    ) -> Self;

    fn get_task_operations_message(
        uuid: Uuid,
        reply_sender: oneshot::Sender<Result<Vec<Operation>>>,
    ) -> Self;

    fn unsynced_operations_message(reply_sender: oneshot::Sender<Result<Vec<Operation>>>) -> Self;

    fn num_unsynced_operations_message(reply_sender: oneshot::Sender<Result<usize>>) -> Self;

    fn add_operation_message(op: Operation, reply_sender: oneshot::Sender<Result<()>>) -> Self;

    fn remove_operation_message(op: Operation, reply_sender: oneshot::Sender<Result<()>>) -> Self;

    fn sync_complete_message(reply_sender: oneshot::Sender<Result<()>>) -> Self;

    fn get_working_set_message(reply_sender: oneshot::Sender<Result<Vec<Option<Uuid>>>>) -> Self;

    fn add_to_working_set_message(uuid: Uuid, reply_sender: oneshot::Sender<Result<usize>>)
        -> Self;

    fn set_working_set_item_message(
        index: usize,
        uuid: Option<Uuid>,
        reply_sender: oneshot::Sender<Result<()>>,
    ) -> Self;

    fn clear_working_set_message(reply_sender: oneshot::Sender<Result<()>>) -> Self;
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::Storage;

    // A message for the main actor in tests
    enum TestActorMessage {
        BeginTxn,
    }

    // A message for the transaction actor in tests. Unused in this test.
    enum TestTxnMessage {}

    impl ActorMessage for TestActorMessage {
        type TxnMessage = TestTxnMessage;

        fn begin_txn_message(
            _reply_sender: oneshot::Sender<Result<UnboundedSender<Self::TxnMessage>>>,
        ) -> Self {
            Self::BeginTxn
        }
    }

    impl TxnMessage for TestTxnMessage {
        // None of these are used in this test.
        fn commit_message(_: oneshot::Sender<Result<()>>) -> Self {
            unimplemented!()
        }
        fn rollback_message(_: oneshot::Sender<Result<()>>) -> Self {
            unimplemented!()
        }
        fn get_task_message(_: Uuid, _: oneshot::Sender<Result<Option<TaskMap>>>) -> Self {
            unimplemented!()
        }
        fn create_task_message(_: Uuid, _: oneshot::Sender<Result<bool>>) -> Self {
            unimplemented!()
        }
        fn set_task_message(_: Uuid, _: TaskMap, _: oneshot::Sender<Result<()>>) -> Self {
            unimplemented!()
        }
        fn delete_task_message(_: Uuid, _: oneshot::Sender<Result<bool>>) -> Self {
            unimplemented!()
        }
        fn get_pending_tasks_message(_: oneshot::Sender<Result<Vec<(Uuid, TaskMap)>>>) -> Self {
            unimplemented!()
        }
        fn all_tasks_message(_: oneshot::Sender<Result<Vec<(Uuid, TaskMap)>>>) -> Self {
            unimplemented!()
        }
        fn all_task_uuids_message(_: oneshot::Sender<Result<Vec<Uuid>>>) -> Self {
            unimplemented!()
        }
        fn base_version_message(_: oneshot::Sender<Result<VersionId>>) -> Self {
            unimplemented!()
        }
        fn set_base_version_message(_: VersionId, _: oneshot::Sender<Result<()>>) -> Self {
            unimplemented!()
        }
        fn get_task_operations_message(
            _: Uuid,
            _: oneshot::Sender<Result<Vec<Operation>>>,
        ) -> Self {
            unimplemented!()
        }
        fn unsynced_operations_message(_: oneshot::Sender<Result<Vec<Operation>>>) -> Self {
            unimplemented!()
        }
        fn num_unsynced_operations_message(_: oneshot::Sender<Result<usize>>) -> Self {
            unimplemented!()
        }
        fn add_operation_message(_: Operation, _: oneshot::Sender<Result<()>>) -> Self {
            unimplemented!()
        }
        fn remove_operation_message(_: Operation, _: oneshot::Sender<Result<()>>) -> Self {
            unimplemented!()
        }
        fn sync_complete_message(_: oneshot::Sender<Result<()>>) -> Self {
            unimplemented!()
        }
        fn get_working_set_message(_: oneshot::Sender<Result<Vec<Option<Uuid>>>>) -> Self {
            unimplemented!()
        }
        fn add_to_working_set_message(_: Uuid, _: oneshot::Sender<Result<usize>>) -> Self {
            unimplemented!()
        }
        fn set_working_set_item_message(
            _: usize,
            _: Option<Uuid>,
            _: oneshot::Sender<Result<()>>,
        ) -> Self {
            unimplemented!()
        }
        fn clear_working_set_message(_: oneshot::Sender<Result<()>>) -> Self {
            unimplemented!()
        }
    }

    type TestStorage = ActorStorage<TestActorMessage, TestTxnMessage>;

    fn panic_actor_fn(mut receiver: UnboundedReceiver<TestActorMessage>) {
        // This actor just panics as soon as it's asked to do anything.
        if let Some(TestActorMessage::BeginTxn) = receiver.blocking_recv() {
            panic!("actor panicking as requested");
        }
    }

    #[tokio::test]
    async fn test_actor_panic() {
        let mut storage = TestStorage::new_internal(panic_actor_fn);
        let res = storage.txn().await;
        assert!(matches!(res, Err(Error::Other(_))));
    }
}
