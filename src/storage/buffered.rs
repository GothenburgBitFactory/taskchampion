use crate::errors::{Error, Result};
use crate::operation::Operation;
use crate::storage::send_wrapper::{WrappedStorage, WrappedStorageTxn};
use crate::storage::{TaskMap, VersionId};
use async_trait::async_trait;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

/// The task writes and added operations made between the transaction's start or a
/// `sync_complete` and the next `sync_complete` or commit.
#[derive(Default)]
struct Epoch {
    tasks: HashMap<Uuid, Option<TaskMap>>,
    ops: Vec<Operation>,
}

impl Epoch {
    fn ops_for(&self, uuid: Uuid) -> impl Iterator<Item = Operation> + '_ {
        self.ops
            .iter()
            .filter(move |op| op.get_uuid() == Some(uuid))
            .cloned()
    }

    async fn write(self, t: &mut dyn WrappedStorageTxn) -> Result<()> {
        for (uuid, task) in self.tasks {
            match task {
                Some(task) => t.set_task(uuid, task).await?,
                None => _ = t.delete_task(uuid).await?,
            }
        }
        for op in self.ops {
            t.add_operation(op).await?;
        }
        Ok(())
    }
}

/// The pending writes of one buffered transaction.
#[derive(Default)]
struct State {
    removed: Vec<Operation>,
    synced: Vec<Epoch>,
    pending: Epoch,
    base_version: Option<VersionId>,
    working_set: Option<Vec<Option<Uuid>>>,
}

impl State {
    fn epochs(&self) -> impl Iterator<Item = &Epoch> {
        self.synced.iter().chain([&self.pending])
    }

    fn changes(&self) -> impl Iterator<Item = (&Uuid, &Option<TaskMap>)> {
        self.epochs().flat_map(|e| &e.tasks)
    }

    async fn get_task(&self, t: &mut dyn WrappedStorageTxn, uuid: Uuid) -> Result<Option<TaskMap>> {
        match self.epochs().filter_map(|e| e.tasks.get(&uuid)).last() {
            Some(task) => Ok(task.clone()),
            None => t.get_task(uuid).await,
        }
    }

    async fn working_set(&self, t: &mut dyn WrappedStorageTxn) -> Result<Vec<Option<Uuid>>> {
        match &self.working_set {
            Some(ws) => Ok(ws.clone()),
            None => t.get_working_set().await,
        }
    }
}

/// A transaction over `inner` that holds its writes in memory and a lock until commit.
pub(super) struct Txn<'a, S, L> {
    inner: &'a mut S,
    state: State,
    /// A value dropped when the transaction commits or is dropped, such as a lock guard.
    lock: Option<L>,
}

impl<'a, S: WrappedStorage, L> Txn<'a, S, L> {
    pub(super) fn new(inner: &'a mut S, lock: L) -> Self {
        Txn {
            inner,
            state: State::default(),
            lock: Some(lock),
        }
    }

    fn parts(&mut self) -> (&mut S, &mut State) {
        (&mut *self.inner, &mut self.state)
    }

    async fn working_set_mut(&mut self) -> Result<&mut Vec<Option<Uuid>>> {
        let (inner, state) = self.parts();
        let ws = match state.working_set.take() {
            Some(ws) => ws,
            None => inner.txn().await?.get_working_set().await?,
        };
        Ok(state.working_set.insert(ws))
    }
}

#[async_trait(?Send)]
impl<S: WrappedStorage, L> WrappedStorageTxn for Txn<'_, S, L> {
    async fn get_task(&mut self, uuid: Uuid) -> Result<Option<TaskMap>> {
        let (inner, state) = self.parts();
        let mut t = inner.txn().await?;
        state.get_task(t.as_mut(), uuid).await
    }

    async fn get_pending_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        let (inner, state) = self.parts();
        let mut t = inner.txn().await?;
        let mut tasks = vec![];
        let ws = state.working_set(t.as_mut()).await?;
        for uuid in ws.into_iter().flatten() {
            if let Some(task) = state.get_task(t.as_mut(), uuid).await? {
                tasks.push((uuid, task));
            }
        }
        Ok(tasks)
    }

    async fn create_task(&mut self, uuid: Uuid) -> Result<bool> {
        if self.get_task(uuid).await?.is_some() {
            return Ok(false);
        }
        self.set_task(uuid, TaskMap::new()).await?;
        Ok(true)
    }

    async fn set_task(&mut self, uuid: Uuid, task: TaskMap) -> Result<()> {
        self.state.pending.tasks.insert(uuid, Some(task));
        Ok(())
    }

    async fn delete_task(&mut self, uuid: Uuid) -> Result<bool> {
        let existed = self.get_task(uuid).await?.is_some();
        self.state.pending.tasks.insert(uuid, None);
        Ok(existed)
    }

    async fn all_tasks(&mut self) -> Result<Vec<(Uuid, TaskMap)>> {
        let (inner, state) = self.parts();
        let mut t = inner.txn().await?;
        let mut tasks: HashMap<_, _> = t.all_tasks().await?.into_iter().collect();
        for (uuid, task) in state.changes() {
            match task {
                Some(task) => tasks.insert(*uuid, task.clone()),
                None => tasks.remove(uuid),
            };
        }
        Ok(tasks.into_iter().collect())
    }

    async fn all_task_uuids(&mut self) -> Result<Vec<Uuid>> {
        let (inner, state) = self.parts();
        let mut t = inner.txn().await?;
        let mut uuids: HashSet<_> = t.all_task_uuids().await?.into_iter().collect();
        for (uuid, task) in state.changes() {
            match task {
                Some(_) => uuids.insert(*uuid),
                None => uuids.remove(uuid),
            };
        }
        Ok(uuids.into_iter().collect())
    }

    async fn base_version(&mut self) -> Result<VersionId> {
        let (inner, state) = self.parts();
        match state.base_version {
            Some(version) => Ok(version),
            None => inner.txn().await?.base_version().await,
        }
    }

    async fn set_base_version(&mut self, version: VersionId) -> Result<()> {
        self.state.base_version = Some(version);
        Ok(())
    }

    async fn get_task_operations(&mut self, uuid: Uuid) -> Result<Vec<Operation>> {
        let (inner, state) = self.parts();
        let mut t = inner.txn().await?;
        let mut ops = t.get_task_operations(uuid).await?;
        let removed = state.removed.iter().filter(|o| o.get_uuid() == Some(uuid));
        ops.truncate(ops.len() - removed.count());
        // A sync drops the operations of every task that is absent at that sync.
        let mut exists = t.get_task(uuid).await?.is_some();
        for epoch in &state.synced {
            ops.extend(epoch.ops_for(uuid));
            if let Some(task) = epoch.tasks.get(&uuid) {
                exists = task.is_some();
            }
            if !exists {
                ops.clear();
            }
        }
        ops.extend(state.pending.ops_for(uuid));
        Ok(ops)
    }

    async fn unsynced_operations(&mut self) -> Result<Vec<Operation>> {
        let (inner, state) = self.parts();
        let mut ops = vec![];
        if state.synced.is_empty() {
            ops = inner.txn().await?.unsynced_operations().await?;
            ops.truncate(ops.len() - state.removed.len());
        }
        ops.extend(state.pending.ops.iter().cloned());
        Ok(ops)
    }

    async fn num_unsynced_operations(&mut self) -> Result<usize> {
        Ok(self.unsynced_operations().await?.len())
    }

    async fn add_operation(&mut self, op: Operation) -> Result<()> {
        self.state.pending.ops.push(op);
        Ok(())
    }

    async fn remove_operation(&mut self, op: Operation) -> Result<()> {
        let (inner, state) = self.parts();
        let last = match state.pending.ops.last() {
            Some(last) => Some(last.clone()),
            None if state.synced.is_empty() => {
                let stored = inner.txn().await?.unsynced_operations().await?;
                stored.into_iter().rev().nth(state.removed.len())
            }
            None => None,
        };
        if last.as_ref() != Some(&op) {
            return Err(Error::Database(
                "Last operation does not match -- cannot remove".to_string(),
            ));
        }
        if state.pending.ops.pop().is_none() {
            state.removed.push(op);
        }
        Ok(())
    }

    async fn sync_complete(&mut self) -> Result<()> {
        let epoch = std::mem::take(&mut self.state.pending);
        self.state.synced.push(epoch);
        Ok(())
    }

    async fn get_working_set(&mut self) -> Result<Vec<Option<Uuid>>> {
        let (inner, state) = self.parts();
        let mut t = inner.txn().await?;
        state.working_set(t.as_mut()).await
    }

    async fn add_to_working_set(&mut self, uuid: Uuid) -> Result<usize> {
        let ws = self.working_set_mut().await?;
        let index = ws.len();
        ws.push(Some(uuid));
        Ok(index)
    }

    async fn set_working_set_item(&mut self, index: usize, uuid: Option<Uuid>) -> Result<()> {
        let ws = self.working_set_mut().await?;
        if index >= ws.len() {
            ws.resize(index + 1, None);
        }
        ws[index] = uuid;
        while ws.len() > 1 && ws.last() == Some(&None) {
            ws.pop();
        }
        Ok(())
    }

    async fn clear_working_set(&mut self) -> Result<()> {
        *self.working_set_mut().await? = vec![None];
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        let state = std::mem::take(&mut self.state);
        let mut t = self.inner.txn().await?;
        for op in state.removed {
            t.remove_operation(op).await?;
        }
        for epoch in state.synced {
            epoch.write(t.as_mut()).await?;
            t.sync_complete().await?;
        }
        state.pending.write(t.as_mut()).await?;
        if let Some(version) = state.base_version {
            t.set_base_version(version).await?;
        }
        if let Some(ws) = state.working_set {
            let mut stored = t.get_working_set().await?;
            // Each gap is added as the nil uuid and then set to None.
            for uuid in ws.iter().skip(stored.len()) {
                let uuid = uuid.unwrap_or_default();
                t.add_to_working_set(uuid).await?;
                stored.push(Some(uuid));
            }
            for (index, old) in stored.into_iter().enumerate().skip(1) {
                let new = ws.get(index).copied().flatten();
                if old != new {
                    t.set_working_set_item(index, new).await?;
                }
            }
        }
        t.commit().await?;
        drop(self.lock.take());
        Ok(())
    }
}

#[cfg(test)]
pub(super) mod test {
    use super::Txn;
    use crate::errors::Result;
    use crate::storage::inmemory::InMemoryStorage;
    use crate::storage::send_wrapper::{WrappedStorage, WrappedStorageTxn, Wrapper};
    use crate::{Operation, Operations, Replica};
    use async_trait::async_trait;
    use uuid::Uuid;

    pub(crate) struct Buffered<S>(pub(crate) S);

    #[async_trait(?Send)]
    impl<S: WrappedStorage> WrappedStorage for Buffered<S> {
        async fn txn<'a>(&'a mut self) -> Result<Box<dyn WrappedStorageTxn + 'a>> {
            Ok(Box::new(Txn::new(&mut self.0, ())))
        }
    }

    async fn storage() -> Result<Wrapper> {
        Wrapper::new(async || Ok(Buffered(InMemoryStorage::new()))).await
    }

    crate::storage::test::storage_tests!(storage().await.unwrap());

    #[tokio::test]
    async fn undo_commits() -> Result<()> {
        let uuid = Uuid::new_v4();
        let mut rep = Replica::new(storage().await?);
        let mut ops = Operations::new();
        ops.push(Operation::UndoPoint);
        rep.create_task(uuid, &mut ops).await?;
        rep.commit_operations(ops).await?;
        let ops = rep.get_undo_operations().await?;
        assert!(rep.commit_reversed_operations(ops).await?);
        assert_eq!(rep.get_task_data(uuid).await?, None);
        Ok(())
    }
}
