use super::{apply, snapshot};
use crate::errors::Result;
use crate::server::{AddVersionResult, GetVersionResult, Server, SnapshotUrgency, SyncOp};
use crate::storage::StorageTxn;
use crate::Error;
use log::{info, trace, warn};
use serde::{Deserialize, Serialize};
use std::str;

#[derive(Serialize, Deserialize, Debug)]
struct Version {
    operations: Vec<SyncOp>,
}

/// Sync to the given server, pulling remote changes and pushing local changes.
pub(super) async fn sync(
    server: &mut Box<dyn Server>,
    txn: &mut dyn StorageTxn,
    avoid_snapshots: bool,
) -> Result<()> {
    // if this taskdb is entirely empty, then start by getting and applying a snapshot
    if txn.is_empty().await? {
        trace!("storage is empty; attempting to apply a snapshot");
        if let Some((version, snap)) = server.get_snapshot().await? {
            snapshot::apply_snapshot(txn, version, snap.as_ref()).await?;
            trace!("applied snapshot for version {}", version);
        }
    }

    // For historical purposes, we keep transformed server operations in storage as synced
    // operations. These will be added at the end of the sync process, when the outer loop is
    // complete.
    let mut transformed_server_ops = Vec::new();

    // retry synchronizing until the server accepts our version (this allows for races between
    // replicas trying to sync to the same server).  If the server insists on the same base
    // version twice, then we have diverged.
    let mut requested_parent_version_id = None;
    'outer: loop {
        trace!("beginning sync outer loop");
        let mut base_version_id = txn.base_version().await?;

        let mut local_ops = txn.unsynced_operations().await?;
        let sync_ops = local_ops.drain(..).filter_map(SyncOp::from_op);
        let mut sync_ops_peekable = sync_ops.peekable();

        // batch operations into versions of no more than a million bytes to avoid excessively large http requests.
        let sync_ops_batched = std::iter::from_fn(|| {
            let mut batch_size = 0;
            let mut batch = Vec::new();

            while let Some(op) = sync_ops_peekable.next_if(|op| {
                batch_size += serde_json::to_string(&op).unwrap().len();
                // include if the batch is empty or if the batch size limit is not exceeded.
                batch.is_empty() || batch_size <= 1000000
            }) {
                batch.push(op);
            }

            Some(batch)
        });

        for mut sync_ops_batch in sync_ops_batched {
            // first pull changes and "rebase" on top of them
            loop {
                trace!("beginning sync inner loop");
                if let GetVersionResult::Version {
                    version_id,
                    history_segment,
                    ..
                } = server.get_child_version(base_version_id).await?
                {
                    let version_str = str::from_utf8(&history_segment).unwrap();
                    let version: Version = serde_json::from_str(version_str).unwrap();

                    // apply this version and update base_version in storage
                    info!("applying version {:?} from server", version_id);
                    apply_version(
                        txn,
                        &mut sync_ops_batch,
                        &mut transformed_server_ops,
                        version,
                    )
                    .await?;
                    txn.set_base_version(version_id).await?;
                    base_version_id = version_id;
                } else {
                    info!("no child versions of {:?}", base_version_id);
                    // at the moment, no more child versions, so we can try adding our own
                    break;
                }
            }

            if sync_ops_batch.is_empty() {
                info!("no changes to push to server");
                // nothing to sync back to the server..
                break 'outer;
            }

            trace!("sending {} operations to the server", sync_ops_batch.len());

            // now make a version of our local changes and push those
            let new_version = Version {
                operations: sync_ops_batch,
            };
            let history_segment = serde_json::to_string(&new_version).unwrap().into();
            info!("sending new version to server");
            let (res, snapshot_urgency) =
                server.add_version(base_version_id, history_segment).await?;
            match res {
                AddVersionResult::Ok(new_version_id) => {
                    info!("version {:?} received by server", new_version_id);
                    txn.set_base_version(new_version_id).await?;
                    base_version_id = new_version_id;

                    // make a snapshot if the server indicates it is urgent enough
                    let base_urgency = if avoid_snapshots {
                        SnapshotUrgency::High
                    } else {
                        SnapshotUrgency::Low
                    };
                    if snapshot_urgency >= base_urgency {
                        let snapshot = snapshot::make_snapshot(txn).await?;
                        server.add_snapshot(new_version_id, snapshot).await?;
                    }
                }
                AddVersionResult::ExpectedParentVersion(parent_version_id) => {
                    info!(
                        "new version rejected; must be based on {:?}",
                        parent_version_id
                    );
                    if let Some(requested) = requested_parent_version_id {
                        if parent_version_id == requested {
                            return Err(Error::OutOfSync);
                        }
                    }
                    requested_parent_version_id = Some(parent_version_id);
                    break;
                }
            }
        }
    }

    // Add the transformed server ops to the DB. Critically, these are immediately marked as synced
    // (via `txn.sync_complete`) and thus not subject to any of the invariants around operations
    // and task state.
    for op in transformed_server_ops {
        txn.add_operation(op.into_op()).await?;
    }

    txn.sync_complete().await?;
    txn.commit().await?;
    Ok(())
}

async fn apply_version(
    txn: &mut dyn StorageTxn,
    local_ops: &mut Vec<SyncOp>,
    transformed_server_ops: &mut Vec<SyncOp>,
    mut version: Version,
) -> Result<()> {
    // The situation here is that the server has already applied all server operations, and we
    // have already applied all local operations, so states have diverged by several
    // operations.  We need to figure out what operations to apply locally and on the server in
    // order to return to the same state.
    //
    // Operational transforms provide this on an operation-by-operation basis.  To break this
    // down, we treat each server operation individually, in order.  For each such operation,
    // we start in this state:
    //
    //
    //      base state-*
    //                / \-server op
    //               *   *
    //     local    / \ /
    //     ops     *   *
    //            / \ / new
    //           *   * local
    //   local  / \ / ops
    //   state-*   *
    //      new-\ /
    // server op *-new local state
    //
    // This is slightly complicated by the fact that the transform function can return None,
    // indicating no operation is required.  If this happens for a local op, we can just omit
    // it.  If it happens for server op, then we must copy the remaining local ops.
    for server_op in version.operations.drain(..) {
        trace!(
            "rebasing local operations onto server operation {:?}",
            server_op
        );
        let mut new_local_ops = Vec::with_capacity(local_ops.len());
        let mut svr_op = Some(server_op);
        for local_op in local_ops.drain(..) {
            if let Some(o) = svr_op {
                let (new_server_op, new_local_op) = SyncOp::transform(o, local_op.clone());
                trace!("local operation {:?} -> {:?}", local_op, new_local_op);
                svr_op = new_server_op;
                if let Some(o) = new_local_op {
                    new_local_ops.push(o);
                }
            } else {
                trace!(
                    "local operation {:?} unchanged (server operation consumed)",
                    local_op
                );
                new_local_ops.push(local_op);
            }
        }
        if let Some(o) = svr_op {
            if let Err(e) = apply::apply_op(txn, &o).await {
                warn!("Invalid operation when syncing: {} (ignored)", e);
            }
            transformed_server_ops.push(o);
        }
        *local_ops = new_local_ops;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::server::test::TestServer;
    use crate::storage::inmemory::InMemoryStorage;
    use crate::storage::{Storage, TaskMap};
    use crate::taskdb::snapshot::SnapshotTasks;
    use crate::taskdb::TaskDb;
    use crate::{Operation, Operations};
    use chrono::Utc;
    use pretty_assertions::assert_eq;
    use uuid::Uuid;

    fn expect_operations(mut got: Vec<Operation>, mut exp: Vec<Operation>) {
        got.sort();
        exp.sort();
        assert_eq!(got, exp);
    }

    #[tokio::test]
    async fn test_sync() -> Result<()> {
        let mut server: Box<dyn Server> = TestServer::new().server();

        let mut db1 = TaskDb::new(InMemoryStorage::new());
        sync(&mut server, db1.storage.txn().await?.as_mut(), false).await?;

        let mut db2 = TaskDb::new(InMemoryStorage::new());
        sync(&mut server, db2.storage.txn().await?.as_mut(), false).await?;

        // make some changes in parallel to db1 and db2..
        let uuid1 = Uuid::new_v4();
        let mut ops = Operations::new();
        let now1 = Utc::now();
        ops.push(Operation::Create { uuid: uuid1 });
        ops.push(Operation::Update {
            uuid: uuid1,
            property: "title".into(),
            value: Some("my first task".into()),
            old_value: None,
            timestamp: now1,
        });

        let uuid2 = Uuid::new_v4();
        ops.push(Operation::Create { uuid: uuid2 });
        ops.push(Operation::Update {
            uuid: uuid2,
            property: "title".into(),
            value: Some("my second task".into()),
            old_value: None,
            timestamp: now1,
        });
        db1.commit_operations(ops, |_| false).await?;

        // and synchronize those around
        sync(&mut server, db1.storage.txn().await?.as_mut(), false).await?;
        sync(&mut server, db2.storage.txn().await?.as_mut(), false).await?;
        sync(&mut server, db1.storage.txn().await?.as_mut(), false).await?;
        assert_eq!(db1.sorted_tasks().await, db2.sorted_tasks().await);

        // now make updates to the same task on both sides
        let mut ops = Operations::new();
        let now2 = now1 + chrono::Duration::seconds(1);
        ops.push(Operation::Update {
            uuid: uuid2,
            property: "priority".into(),
            value: Some("H".into()),
            old_value: None,
            timestamp: now2,
        });
        db1.commit_operations(ops, |_| false).await?;

        let mut ops = Operations::new();
        let now3 = now2 + chrono::Duration::seconds(1);
        ops.push(Operation::Update {
            uuid: uuid2,
            property: "project".into(),
            value: Some("personal".into()),
            old_value: None,
            timestamp: now3,
        });
        db1.commit_operations(ops, |_| false).await?;

        // and synchronize those around
        sync(&mut server, db1.storage.txn().await?.as_mut(), false).await?;
        sync(&mut server, db2.storage.txn().await?.as_mut(), false).await?;
        sync(&mut server, db1.storage.txn().await?.as_mut(), false).await?;
        assert_eq!(db1.sorted_tasks().await, db2.sorted_tasks().await);

        for (dbnum, db) in [(1, &mut db1), (2, &mut db2)] {
            eprintln!("checking db{dbnum}");
            expect_operations(
                db.get_task_operations(uuid1).await?,
                vec![
                    Operation::Create { uuid: uuid1 },
                    Operation::Update {
                        uuid: uuid1,
                        property: "title".into(),
                        value: Some("my first task".into()),
                        old_value: None,
                        timestamp: now1,
                    },
                ],
            );
            expect_operations(
                db.get_task_operations(uuid2).await?,
                vec![
                    Operation::Create { uuid: uuid2 },
                    Operation::Update {
                        uuid: uuid2,
                        property: "title".into(),
                        value: Some("my second task".into()),
                        old_value: None,
                        timestamp: now1,
                    },
                    Operation::Update {
                        uuid: uuid2,
                        property: "priority".into(),
                        value: Some("H".into()),
                        old_value: None,
                        timestamp: now2,
                    },
                    Operation::Update {
                        uuid: uuid2,
                        property: "project".into(),
                        value: Some("personal".into()),
                        old_value: None,
                        timestamp: now3,
                    },
                ],
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_create_delete() -> Result<()> {
        let mut server: Box<dyn Server> = TestServer::new().server();

        let mut db1 = TaskDb::new(InMemoryStorage::new());
        sync(&mut server, db1.storage.txn().await?.as_mut(), false).await?;

        let mut db2 = TaskDb::new(InMemoryStorage::new());
        sync(&mut server, db2.storage.txn().await?.as_mut(), false).await?;

        // create and update a task..
        let uuid = Uuid::new_v4();
        let mut ops = Operations::new();
        let now1 = Utc::now();
        ops.push(Operation::Create { uuid });
        ops.push(Operation::Update {
            uuid,
            property: "title".into(),
            value: Some("my first task".into()),
            old_value: None,
            timestamp: now1,
        });
        db1.commit_operations(ops, |_| false).await?;

        // and synchronize those around
        sync(&mut server, db1.storage.txn().await?.as_mut(), false).await?;
        sync(&mut server, db2.storage.txn().await?.as_mut(), false).await?;
        sync(&mut server, db1.storage.txn().await?.as_mut(), false).await?;
        assert_eq!(db1.sorted_tasks().await, db2.sorted_tasks().await);

        // delete and re-create the task on db1
        let mut ops = Operations::new();
        let now2 = now1 + chrono::Duration::seconds(1);
        ops.push(Operation::Delete {
            uuid,
            old_task: TaskMap::new(),
        });
        ops.push(Operation::Create { uuid });
        ops.push(Operation::Update {
            uuid,
            property: "title".into(),
            value: Some("my second task".into()),
            old_value: None,
            timestamp: now2,
        });
        db1.commit_operations(ops, |_| false).await?;

        // and on db2, update a property of the task
        let mut ops = Operations::new();
        let now3 = now2 + chrono::Duration::seconds(1);
        ops.push(Operation::Update {
            uuid,
            property: "project".into(),
            value: Some("personal".into()),
            old_value: None,
            timestamp: now3,
        });
        db2.commit_operations(ops, |_| false).await?;

        sync(&mut server, db1.storage.txn().await?.as_mut(), false).await?;
        sync(&mut server, db2.storage.txn().await?.as_mut(), false).await?;
        sync(&mut server, db1.storage.txn().await?.as_mut(), false).await?;
        assert_eq!(db1.sorted_tasks().await, db2.sorted_tasks().await);

        // This is a case where the task operations appear different on the replicas,
        // because the update to "project" on db2 loses to the delete.
        expect_operations(
            db1.get_task_operations(uuid).await?,
            vec![
                Operation::Create { uuid },
                Operation::Create { uuid },
                Operation::Update {
                    uuid,
                    property: "title".into(),
                    value: Some("my first task".into()),
                    old_value: None,
                    timestamp: now1,
                },
                Operation::Update {
                    uuid,
                    property: "title".into(),
                    value: Some("my second task".into()),
                    old_value: None,
                    timestamp: now2,
                },
                Operation::Delete {
                    uuid,
                    old_task: TaskMap::new(),
                },
            ],
        );
        expect_operations(
            db2.get_task_operations(uuid).await?,
            vec![
                Operation::Create { uuid },
                Operation::Create { uuid },
                Operation::Update {
                    uuid,
                    property: "title".into(),
                    value: Some("my first task".into()),
                    old_value: None,
                    timestamp: now1,
                },
                Operation::Update {
                    uuid,
                    property: "title".into(),
                    value: Some("my second task".into()),
                    old_value: None,
                    timestamp: now2,
                },
                // This operation is not visible on db1 because the task is already deleted there
                // when this update is synced in.
                Operation::Update {
                    uuid,
                    property: "project".into(),
                    value: Some("personal".into()),
                    old_value: None,
                    timestamp: now3,
                },
                Operation::Delete {
                    uuid,
                    old_task: TaskMap::new(),
                },
            ],
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_sync_conflicting_updates() -> Result<()> {
        let mut server: Box<dyn Server> = TestServer::new().server();

        let mut db1 = TaskDb::new(InMemoryStorage::new());
        sync(&mut server, db1.storage.txn().await?.as_mut(), false).await?;

        let mut db2 = TaskDb::new(InMemoryStorage::new());
        sync(&mut server, db2.storage.txn().await?.as_mut(), false).await?;

        // create and update a task..
        let uuid = Uuid::new_v4();
        let mut ops = Operations::new();
        let now1 = Utc::now();
        ops.push(Operation::Create { uuid });
        ops.push(Operation::Update {
            uuid,
            property: "title".into(),
            value: Some("my first task".into()),
            old_value: None,
            timestamp: now1,
        });
        db1.commit_operations(ops, |_| false).await?;

        // and synchronize those around
        sync(&mut server, db1.storage.txn().await?.as_mut(), false).await?;
        sync(&mut server, db2.storage.txn().await?.as_mut(), false).await?;
        sync(&mut server, db1.storage.txn().await?.as_mut(), false).await?;
        assert_eq!(db1.sorted_tasks().await, db2.sorted_tasks().await);

        // add different updates on db1 and db2
        let mut ops = Operations::new();
        let now2 = now1 + chrono::Duration::seconds(1);
        ops.push(Operation::Update {
            uuid,
            property: "title".into(),
            value: Some("from db1".into()),
            old_value: None,
            timestamp: now2,
        });
        db1.commit_operations(ops, |_| false).await?;

        // and on db2, update a property of the task
        let mut ops = Operations::new();
        let now3 = now2 + chrono::Duration::seconds(1);
        ops.push(Operation::Update {
            uuid,
            property: "title".into(),
            value: Some("from db2".into()),
            old_value: None,
            timestamp: now3,
        });
        db2.commit_operations(ops, |_| false).await?;

        sync(&mut server, db1.storage.txn().await?.as_mut(), false).await?;
        sync(&mut server, db2.storage.txn().await?.as_mut(), false).await?;
        sync(&mut server, db1.storage.txn().await?.as_mut(), false).await?;
        assert_eq!(db1.sorted_tasks().await, db2.sorted_tasks().await);

        expect_operations(
            db1.get_task_operations(uuid).await?,
            vec![
                Operation::Create { uuid },
                Operation::Update {
                    uuid,
                    property: "title".into(),
                    value: Some("my first task".into()),
                    old_value: None,
                    timestamp: now1,
                },
                // This operation is not visible on db2 because the "from db2" update has a later
                // timestamp and thus wins over this one.
                Operation::Update {
                    uuid,
                    property: "title".into(),
                    value: Some("from db1".into()),
                    old_value: None,
                    timestamp: now2,
                },
                Operation::Update {
                    uuid,
                    property: "title".into(),
                    value: Some("from db2".into()),
                    old_value: None,
                    timestamp: now3,
                },
            ],
        );
        expect_operations(
            db2.get_task_operations(uuid).await?,
            vec![
                Operation::Create { uuid },
                Operation::Update {
                    uuid,
                    property: "title".into(),
                    value: Some("my first task".into()),
                    old_value: None,
                    timestamp: now1,
                },
                Operation::Update {
                    uuid,
                    property: "title".into(),
                    value: Some("from db2".into()),
                    old_value: None,
                    timestamp: now3,
                },
            ],
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_sync_add_snapshot_start_with_snapshot() -> Result<()> {
        let mut test_server = TestServer::new();

        let mut server: Box<dyn Server> = test_server.server();
        let mut db1 = TaskDb::new(InMemoryStorage::new());

        let uuid = Uuid::new_v4();
        let mut ops = Operations::new();
        ops.push(Operation::Create { uuid });
        ops.push(Operation::Update {
            uuid,
            property: "title".into(),
            value: Some("my first task".into()),
            old_value: None,
            timestamp: Utc::now(),
        });
        db1.commit_operations(ops, |_| false).await?;

        test_server.set_snapshot_urgency(SnapshotUrgency::High);
        sync(&mut server, db1.storage.txn().await?.as_mut(), false).await?;

        // assert that a snapshot was added
        let base_version = db1.storage.txn().await?.base_version().await?;
        let (v, s) = test_server
            .snapshot()
            .ok_or_else(|| anyhow::anyhow!("no snapshot"))?;
        assert_eq!(v, base_version);

        let tasks = SnapshotTasks::decode(&s)?.into_inner();
        assert_eq!(tasks[0].0, uuid);

        // update the taskdb and sync again
        let mut ops = Operations::new();
        ops.push(Operation::Update {
            uuid,
            property: "title".into(),
            value: Some("my first task, updated".into()),
            old_value: None,
            timestamp: Utc::now(),
        });
        db1.commit_operations(ops, |_| false).await?;
        sync(&mut server, db1.storage.txn().await?.as_mut(), false).await?;

        // delete the first version, so that db2 *must* initialize from
        // the snapshot
        test_server.delete_version(Uuid::nil());

        // sync to a new DB and check that we got the expected results
        let mut db2 = TaskDb::new(InMemoryStorage::new());
        sync(&mut server, db2.storage.txn().await?.as_mut(), false).await?;

        let task = db2.get_task(uuid).await?.unwrap();
        assert_eq!(task.get("title").unwrap(), "my first task, updated");

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_avoids_snapshot() -> Result<()> {
        let test_server = TestServer::new();

        let mut server: Box<dyn Server> = test_server.server();
        let mut db1 = TaskDb::new(InMemoryStorage::new());

        let uuid = Uuid::new_v4();
        let mut ops = Operations::new();
        ops.push(Operation::Create { uuid });
        db1.commit_operations(ops, |_| false).await?;

        test_server.set_snapshot_urgency(SnapshotUrgency::Low);
        sync(&mut server, db1.storage.txn().await?.as_mut(), true).await?;

        // assert that a snapshot was not added, because we indicated
        // we wanted to avoid snapshots and it was only low urgency
        assert_eq!(test_server.snapshot(), None);

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_batched() -> Result<()> {
        let test_server = TestServer::new();

        let mut server: Box<dyn Server> = test_server.server();

        let mut db = TaskDb::new(InMemoryStorage::new());
        sync(&mut server, db.storage.txn().await?.as_mut(), false).await?;

        // add a task to db
        let uuid1 = Uuid::new_v4();
        let mut ops = Operations::new();
        ops.push(Operation::Create { uuid: uuid1 });
        ops.push(Operation::Update {
            uuid: uuid1,
            property: "title".into(),
            value: Some("my first task".into()),
            old_value: None,
            timestamp: Utc::now(),
        });
        db.commit_operations(ops, |_| false).await?;

        sync(&mut server, db.storage.txn().await?.as_mut(), true).await?;
        assert_eq!(test_server.versions_len(), 1);

        // chars are four bytes, but they're only one when converted to a String
        let data = vec!['a'; 400000];

        // add some large operations to db
        let mut ops = Operations::new();
        for _ in 0..3 {
            ops.push(Operation::Update {
                uuid: uuid1,
                property: "description".into(),
                value: Some(data.iter().collect()),
                old_value: None,
                timestamp: Utc::now(),
            });
        }
        db.commit_operations(ops, |_| false).await?;

        // this sync batches the operations into two versions.
        sync(&mut server, db.storage.txn().await?.as_mut(), true).await?;
        assert_eq!(test_server.versions_len(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_sync_batches_at_least_one_op() -> Result<()> {
        let test_server = TestServer::new();

        let mut server: Box<dyn Server> = test_server.server();

        let mut db = TaskDb::new(InMemoryStorage::new());
        sync(&mut server, db.storage.txn().await?.as_mut(), false).await?;

        // add a task to db
        let uuid1 = Uuid::new_v4();
        let mut ops = Operations::new();
        ops.push(Operation::Create { uuid: uuid1 });
        ops.push(Operation::Update {
            uuid: uuid1,
            property: "title".into(),
            value: Some("my first task".into()),
            old_value: None,
            timestamp: Utc::now(),
        });
        db.commit_operations(ops, |_| false).await?;

        sync(&mut server, db.storage.txn().await?.as_mut(), true).await?;
        assert_eq!(test_server.versions_len(), 1);

        // add an operation greater than the batch limit
        let data = vec!['a'; 1000001];
        let mut ops = Operations::new();
        ops.push(Operation::Update {
            uuid: uuid1,
            property: "description".into(),
            value: Some(data.iter().collect()),
            old_value: None,
            timestamp: Utc::now(),
        });
        db.commit_operations(ops, |_| false).await?;

        sync(&mut server, db.storage.txn().await?.as_mut(), true).await?;
        assert_eq!(test_server.versions_len(), 2);

        Ok(())
    }
}
