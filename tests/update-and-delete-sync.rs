use taskchampion::chrono::{TimeZone, Utc};
use taskchampion::{storage::InMemoryStorage, Operations, Replica, ServerConfig, Status, Uuid};
use tempfile::TempDir;

#[test]
#[cfg(feature = "server-local")]
fn update_and_delete_sync_delete_first() -> anyhow::Result<()> {
    update_and_delete_sync(true)
}

#[test]
#[cfg(feature = "server-local")]
fn update_and_delete_sync_update_first() -> anyhow::Result<()> {
    update_and_delete_sync(false)
}

/// Test what happens when an update is sync'd into a repo after a task is deleted.
/// If delete_first, then the deletion is sync'd to the server first; otherwise
/// the update is sync'd first.  Either way, the task is gone.
#[cfg(feature = "server-local")]
fn update_and_delete_sync(delete_first: bool) -> anyhow::Result<()> {
    // set up two replicas, and demonstrate replication between them
    let mut rep1 = Replica::new(InMemoryStorage::new());
    let mut rep2 = Replica::new(InMemoryStorage::new());

    let tmp_dir = TempDir::new().expect("TempDir failed");
    let mut server = ServerConfig::Local {
        server_dir: tmp_dir.path().to_path_buf(),
    }
    .into_server()?;

    // add a task on rep1, and sync it to rep2
    let mut ops = Operations::new();
    let u = Uuid::new_v4();
    let mut t = rep1.create_task(u, &mut ops)?;
    t.set_description("test task".into(), &mut ops)?;
    t.set_status(Status::Pending, &mut ops)?;
    t.set_entry(Some(Utc::now()), &mut ops)?;
    rep1.commit_operations(ops)?;

    rep1.sync(&mut server, false)?;
    rep2.sync(&mut server, false)?;

    // mark the task as deleted, long in the past, on rep2
    {
        let mut ops = Operations::new();
        let mut t = rep2.get_task(u)?.unwrap();
        t.set_status(Status::Deleted, &mut ops)?;
        t.set_modified(Utc.with_ymd_and_hms(1980, 1, 1, 0, 0, 0).unwrap(), &mut ops)?;
        rep2.commit_operations(ops)?;
    }

    // sync it back to rep1
    rep2.sync(&mut server, false)?;
    rep1.sync(&mut server, false)?;

    // expire the task on rep1 and check that it is gone locally
    rep1.expire_tasks()?;
    assert!(rep1.get_task(u)?.is_none());

    // modify the task on rep2
    {
        let mut ops = Operations::new();
        let mut t = rep2.get_task(u)?.unwrap();
        t.set_description("modified".to_string(), &mut ops)?;
        rep2.commit_operations(ops)?;
    }

    // sync back and forth
    if delete_first {
        rep1.sync(&mut server, false)?;
    }
    rep2.sync(&mut server, false)?;
    rep1.sync(&mut server, false)?;
    if !delete_first {
        rep2.sync(&mut server, false)?;
    }

    // check that the task is gone on both replicas
    assert!(rep1.get_task(u)?.is_none());
    assert!(rep2.get_task(u)?.is_none());

    Ok(())
}
