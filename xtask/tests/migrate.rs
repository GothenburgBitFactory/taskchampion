//! Smoke test for the `migrate-recurring` xtask logic.
//!
//! Populates a temporary sqlite store with a mix of task statuses, runs the
//! migration function, then re-opens the store and asserts the expected
//! conversions and non-conversions.

use std::path::Path;

use taskchampion::storage::AccessMode;
use taskchampion::{Operations, Replica, SqliteStorage, Status, Uuid};
use xtask_migrate::{migrate_recurring, MigrateSummary};

#[path = "../src/migrate.rs"]
#[allow(dead_code)] // CLI entry point is only invoked from main.rs
mod xtask_migrate;

async fn populate(path: &Path) -> (Uuid, Uuid, Uuid, Uuid) {
    let storage = SqliteStorage::new(path, AccessMode::ReadWrite, true)
        .await
        .unwrap();
    let mut replica = Replica::new(storage);
    let mut ops = Operations::new();

    // recurring task with a `recur` UDA — should migrate.
    let rec_uuid = Uuid::new_v4();
    let mut rec = replica.create_task(rec_uuid, &mut ops).await.unwrap();
    rec.set_value("description", Some("pay rent".into()), &mut ops)
        .unwrap();
    rec.set_value("recur", Some("monthly".into()), &mut ops)
        .unwrap();
    rec.set_value("mask", Some("--".into()), &mut ops).unwrap();
    rec.set_status(Status::Recurring, &mut ops).unwrap();

    // recurring task without a `recur` UDA — should be counted as skipped.
    let rec_no_recur = Uuid::new_v4();
    let mut bad = replica.create_task(rec_no_recur, &mut ops).await.unwrap();
    bad.set_value("description", Some("broken recurring".into()), &mut ops)
        .unwrap();
    bad.set_status(Status::Recurring, &mut ops).unwrap();

    // pending child task pointing at the recurring task — should be left alone.
    let child_uuid = Uuid::new_v4();
    let mut child = replica.create_task(child_uuid, &mut ops).await.unwrap();
    child
        .set_value("description", Some("pay rent — March".into()), &mut ops)
        .unwrap();
    child
        .set_value("parent", Some(rec_uuid.to_string()), &mut ops)
        .unwrap();
    child.set_status(Status::Pending, &mut ops).unwrap();

    // unrelated pending task — should be left alone.
    let plain_uuid = Uuid::new_v4();
    let mut plain = replica.create_task(plain_uuid, &mut ops).await.unwrap();
    plain
        .set_value("description", Some("standalone".into()), &mut ops)
        .unwrap();
    plain.set_status(Status::Pending, &mut ops).unwrap();

    replica.commit_operations(ops).await.unwrap();
    (rec_uuid, rec_no_recur, child_uuid, plain_uuid)
}

#[tokio::test]
async fn migrate_converts_recurring_only() {
    let tmp = tempfile::tempdir().unwrap();
    let (rec_uuid, rec_no_recur, child_uuid, plain_uuid) = populate(tmp.path()).await;

    let summary = migrate_recurring(tmp.path(), false, taskchampion::IterType::Chained)
        .await
        .unwrap();
    assert_eq!(
        summary,
        MigrateSummary {
            migrated: 1,
            children_left: 1,
            skipped_no_recur: 1,
        }
    );

    // Re-open and verify state.
    let storage = SqliteStorage::new(tmp.path(), AccessMode::ReadWrite, false)
        .await
        .unwrap();
    let mut replica = Replica::new(storage);
    let tasks = replica.all_tasks().await.unwrap();

    let rec = tasks.get(&rec_uuid).expect("recurring task present");
    assert_eq!(rec.get_status(), Status::Iterative);
    assert_eq!(rec.get_value("iter"), Some("monthly"));
    assert_eq!(rec.get_value("iter_type"), Some("chained"));
    assert_eq!(rec.get_value("recur"), None);
    assert_eq!(rec.get_value("mask"), None);
    assert!(
        rec.get_due().is_some(),
        "iterative task should have due set"
    );
    assert!(
        rec.get_value("rrule").is_some(),
        "iterative task should have rrule set"
    );

    let bad = tasks.get(&rec_no_recur).expect("bad recurring present");
    assert_eq!(
        bad.get_status(),
        Status::Recurring,
        "recurring task without `recur` UDA should be left untouched"
    );

    let child = tasks.get(&child_uuid).expect("child present");
    assert_eq!(child.get_status(), Status::Pending);
    assert_eq!(
        child.get_value("parent"),
        Some(rec_uuid.to_string()).as_deref()
    );

    let plain = tasks.get(&plain_uuid).expect("plain present");
    assert_eq!(plain.get_status(), Status::Pending);
}

#[tokio::test]
async fn migrate_dry_run_does_not_commit() {
    let tmp = tempfile::tempdir().unwrap();
    let (rec_uuid, _, _, _) = populate(tmp.path()).await;

    let summary = migrate_recurring(tmp.path(), true, taskchampion::IterType::Chained)
        .await
        .unwrap();
    assert_eq!(summary.migrated, 1);

    let storage = SqliteStorage::new(tmp.path(), AccessMode::ReadWrite, false)
        .await
        .unwrap();
    let mut replica = Replica::new(storage);
    let tasks = replica.all_tasks().await.unwrap();
    let rec = tasks.get(&rec_uuid).unwrap();
    assert_eq!(
        rec.get_status(),
        Status::Recurring,
        "dry-run should not have committed any changes"
    );
    assert_eq!(rec.get_value("recur"), Some("monthly"));
}
