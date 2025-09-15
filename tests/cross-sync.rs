use chrono::Utc;
use pretty_assertions::assert_eq;
use taskchampion::{Operations, Replica, ServerConfig, Status, StorageConfig, Uuid};
use tempfile::TempDir;

#[test]
#[cfg(feature = "server-local")]
fn cross_sync() -> anyhow::Result<()> {
    // set up two replicas, and demonstrate replication between them
    let mut rep1 = Replica::new(StorageConfig::InMemory.into_storage()?);
    let mut rep2 = Replica::new(StorageConfig::InMemory.into_storage()?);

    let tmp_dir = TempDir::new().expect("TempDir failed");
    let server_config = ServerConfig::Local {
        server_dir: tmp_dir.path().to_path_buf(),
    };
    let mut server = server_config.into_server()?;

    let (uuid1, uuid2) = (Uuid::new_v4(), Uuid::new_v4());
    let mut ops = Operations::new();

    // add some tasks on rep1
    let mut t1 = rep1.create_task(uuid1, &mut ops)?;
    t1.set_description("test 1".into(), &mut ops)?;
    t1.set_status(Status::Pending, &mut ops)?;
    t1.set_entry(Some(Utc::now()), &mut ops)?;
    let mut t2 = rep1.create_task(uuid2, &mut ops)?;
    t2.set_description("test 2".into(), &mut ops)?;
    t2.set_status(Status::Pending, &mut ops)?;
    t2.set_entry(Some(Utc::now()), &mut ops)?;

    // modify t1
    t1.start(&mut ops)?;

    rep1.commit_operations(ops)?;

    rep1.sync(&mut server, false)?;
    rep2.sync(&mut server, false)?;

    // those tasks should exist on rep2 now
    let mut t12 = rep2.get_task(uuid1)?.expect("expected task 1 on rep2");
    let t22 = rep2.get_task(uuid2)?.expect("expected task 2 on rep2");

    assert_eq!(t12.get_description(), "test 1");
    assert_eq!(t12.is_active(), true);
    assert_eq!(t22.get_description(), "test 2");
    assert_eq!(t22.is_active(), false);

    // make non-conflicting changes on the two replicas
    let mut ops = Operations::new();
    t2.set_status(Status::Completed, &mut ops)?;
    rep2.commit_operations(ops)?;

    let mut ops = Operations::new();
    t12.set_status(Status::Completed, &mut ops)?;
    rep2.commit_operations(ops)?;

    // sync those changes back and forth
    rep1.sync(&mut server, false)?; // rep1 -> server
    rep2.sync(&mut server, false)?; // server -> rep2, rep2 -> server
    rep1.sync(&mut server, false)?; // server -> rep1

    let t1 = rep1.get_task(uuid1)?.expect("expected task 1 on rep1");
    assert_eq!(t1.get_status(), Status::Completed);

    let mut t22 = rep2.get_task(uuid2)?.expect("expected task 2 on rep2");
    assert_eq!(t22.get_status(), Status::Completed);

    rep1.rebuild_working_set(true)?;
    rep2.rebuild_working_set(true)?;

    // Make task 2 pending again, and observe that it is in the working set in both replicas after
    // sync.
    let mut ops = Operations::new();
    t22.set_status(Status::Pending, &mut ops)?;
    rep2.commit_operations(ops)?;

    let ws = rep2.working_set()?;
    assert_eq!(ws.by_index(1), Some(uuid2));

    // Pending status is not sync'd to rep1 yet.
    let ws = rep1.working_set()?;
    assert_eq!(ws.by_index(1), None);

    rep2.sync(&mut server, false)?;
    rep1.sync(&mut server, false)?;

    let ws = rep1.working_set()?;
    assert_eq!(ws.by_index(1), Some(uuid2));

    Ok(())
}
