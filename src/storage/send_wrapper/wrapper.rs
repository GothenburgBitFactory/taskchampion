use super::actor::{ActorImpl, ActorMessage, TxnMessage};
use super::WrappedStorage;
use crate::errors::Result;
use crate::operation::Operation;
use crate::storage::{Storage, StorageTxn, TaskMap, VersionId};
use async_trait::async_trait;
use std::future::Future;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

/// Wrapper wraps a `!Send` storage implementation (specifically, implementing [`WrappedStorage`]
/// to make it Send ([`Storage`]).
///
/// Async runtimes like Tokio can move a future between threads to support efficient execution.
/// This requires the future to also implement `Send`. For most purposes, this is not an issue, but
/// a few types are `!Send` and any async function handling such types are also `!Send`.
///
/// On WASM, the wrapped storage runs in an async task, but not in a thread.
pub(in crate::storage) struct Wrapper {
    // Both fields in this struct are `Option<..>` to allow them to be dropped individually
    // in `Wrapper::drop`.
    sender: Option<mpsc::UnboundedSender<ActorMessage>>,
    #[cfg(not(target_arch = "wasm32"))]
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Wrapper {
    /// Create a new wrapper.
    ///
    /// The constructor is called in a dedicated single-threaded runtime, and all operations on the
    /// resulting WrappedStorage implementation will also occur in that thread. As such, neither
    /// the constructor nor any of the [`WrappedStorage`] or [`super::traits::WrappedStorageTxn`]
    /// methods are required to implement `Send`.
    pub(in crate::storage) async fn new<S, FN, FUT>(constructor: FN) -> Result<Self>
    where
        S: WrappedStorage,
        FUT: Future<Output = Result<S>>,
        FN: FnOnce() -> FUT + Send + 'static,
    {
        let (sender, receiver) = mpsc::unbounded_channel();
        // Use a oneshot channel to block until the thread has initialized.
        let (init_sender, init_receiver): (oneshot::Sender<Result<_>>, _) = oneshot::channel();

        let in_thread = async move |init_sender: oneshot::Sender<Result<_>>| {
            match constructor().await {
                Ok(storage) => {
                    // Send Ok back to the caller of `new` and then start the actor loop.
                    let _ = init_sender.send(Ok(()));
                    let mut actor = ActorImpl::new(storage, receiver);
                    actor.run().await;
                }
                Err(e) => {
                    // Send the initialization error back.
                    let _ = init_sender.send(Err(e));
                }
            }
        };

        // On WASM, we do not have threads, so spawn the constructor in the current thread.
        #[cfg(target_arch = "wasm32")]
        {
            wasm_bindgen_futures::spawn_local(in_thread(init_sender));
        }

        // Otherwise, spawn a new thread, and within that a local Tokio RT that can handle !Send
        // futures.
        #[cfg(not(target_arch = "wasm32"))]
        let thread = {
            use std::thread;
            use tokio::runtime;
            thread::spawn(move || {
                let rt = match runtime::Builder::new_current_thread().build() {
                    Ok(rt) => rt,
                    Err(e) => {
                        let _ = init_sender.send(Err(e.into()));
                        return;
                    }
                };

                rt.block_on(in_thread(init_sender));
            })
        };

        // Wait until the thread sends its initialization result.
        init_receiver.await??;
        Ok(Self {
            sender: Some(sender),
            #[cfg(not(target_arch = "wasm32"))]
            thread: Some(thread),
        })
    }
}

#[async_trait]
impl Storage for Wrapper {
    // Sends the BeginTxn message to the underlying ActorImpl. Now that the txn has
    // begun, this async txn obj can be passed around and operated upon, as it
    // communicates with the underlying sync txn.
    async fn txn<'a>(&'a mut self) -> Result<Box<dyn StorageTxn + Send + 'a>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.sender
            .as_mut()
            .expect("txn called after drop")
            .send(ActorMessage::BeginTxn(reply_tx))?;
        let txn_sender = reply_rx.await??;
        Ok(Box::new(WrapperTxn::new(txn_sender)))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for Wrapper {
    fn drop(&mut self) {
        // Deleting the sender signals to the actor thread that it should drop the
        // wrapped storage and terminate.
        self.sender = None;
        // Wait for the thread to terminate, indicating that the wrapped storage has
        // been fully dropped.
        let _ = self.thread.take().expect("thread joined twice").join();
    }
}

/// An async proxy for a transaction running on the sync actor thread.
struct WrapperTxn {
    sender: mpsc::UnboundedSender<TxnMessage>,
    committed: bool,
}

impl WrapperTxn {
    fn new(sender: mpsc::UnboundedSender<TxnMessage>) -> Self {
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
        self.sender.send(f(tx))?;
        rx.await?
    }
}

impl Drop for WrapperTxn {
    fn drop(&mut self) {
        if !self.committed {
            // If the transaction proxy is dropped without being committed,
            // we send a Rollback message. There's nothing we can do if this
            // fails, so ignore the result and do not use a channel to wait
            // for a response.
            let _ = self.sender.send(TxnMessage::Rollback);
        }
    }
}

#[async_trait]
impl StorageTxn for WrapperTxn {
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

    async fn is_empty(&mut self) -> Result<bool> {
        self.call(TxnMessage::IsEmpty).await
    }
}
