#![cfg(all(feature = "server-local", feature = "iterative-tasks"))]

//! Two replicas completing the same iterative task before syncing must produce
//! the same successor task UUIDv5.

use pretty_assertions::assert_eq;
use std::collections::BTreeSet;
use taskchampion::storage::inmemory::InMemoryStorage;
use taskchampion::{Operations, Replica, ServerConfig, Status, Uuid};
use tempfile::TempDir;

#[tokio::test]
async fn concurrent_iterative_completion_converges() -> anyhow::Result<()> {
    let mut rep1 = Replica::new(InMemoryStorage::new());
    let mut rep2 = Replica::new(InMemoryStorage::new());

    let tmp_dir = TempDir::new().expect("TempDir failed");
    let server_config = ServerConfig::Local {
        server_dir: tmp_dir.path().to_path_buf(),
    };
    let mut server = server_config.into_server().await?;

    // Create an iterative task on rep1 and replicate it to rep2.
    let uuid = Uuid::new_v4();
    let mut ops = Operations::new();
    let mut t = rep1.create_task(uuid, &mut ops).await?;
    t.set_value("iter", Some("daily".into()), &mut ops)?;
    t.set_status(Status::Iterative, &mut ops)?;
    rep1.commit_operations(ops).await?;
    rep1.sync(&mut server, false).await?;
    rep2.sync(&mut server, false).await?;

    // Both replicas complete the same task before syncing again.
    let mut ops = Operations::new();
    let mut t1 = rep1.get_task(uuid).await?.expect("task on rep1");
    t1.set_status(Status::Completed, &mut ops)?;
    rep1.commit_operations(ops).await?;

    let mut ops = Operations::new();
    let mut t2 = rep2.get_task(uuid).await?.expect("task on rep2");
    t2.set_status(Status::Completed, &mut ops)?;
    rep2.commit_operations(ops).await?;

    // Sync back and forth until both have seen each other's changes.
    rep1.sync(&mut server, false).await?;
    rep2.sync(&mut server, false).await?;
    rep1.sync(&mut server, false).await?;

    let all1 = rep1.all_tasks().await?;
    let all2 = rep2.all_tasks().await?;

    // Convergence: identical task sets, exactly the completed log + one successor.
    let keys1: BTreeSet<Uuid> = all1.keys().copied().collect();
    let keys2: BTreeSet<Uuid> = all2.keys().copied().collect();
    assert_eq!(keys1, keys2, "replicas should converge to the same tasks");
    assert_eq!(
        all1.len(),
        2,
        "one completed + one successor, not a duplicate successor"
    );

    // The original is completed.
    assert_eq!(all1[&uuid].get_status(), Status::Completed);

    // The other task is the successor, and points back.
    let successor = all1
        .values()
        .find(|t| t.get_uuid() != uuid)
        .expect("successor should exist");
    assert_eq!(successor.get_status(), Status::Iterative);
    assert_eq!(
        successor
            .get_value("parent")
            .and_then(|p| Uuid::parse_str(p).ok()),
        Some(uuid),
        "successor's parent is the completed log"
    );

    Ok(())
}
