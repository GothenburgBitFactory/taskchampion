use crate::{errors::Result, server::cloud::service::ObjectInfo};
use async_trait::async_trait;

#[async_trait]
pub(crate) trait AsyncObjectIterator {
    async fn next(&mut self) -> Option<Result<ObjectInfo>>;
}

#[cfg(test)]
// This struct takes a synchronous iterator `I` and adapts it for async.
pub(crate) struct SyncIteratorWrapper<I>
where
    I: Iterator<Item = Result<ObjectInfo>> + Send,
{
    pub inner: I,
}

#[cfg(test)]
#[async_trait]
impl<I> AsyncObjectIterator for SyncIteratorWrapper<I>
where
    I: Iterator<Item = Result<ObjectInfo>> + Send + Sync,
{
    async fn next(&mut self) -> Option<Result<ObjectInfo>> {
        self.inner.next()
    }
}
