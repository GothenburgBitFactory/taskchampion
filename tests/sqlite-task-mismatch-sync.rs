#![cfg(all(feature = "server-local", feature = "storage-sqlite"))]

use rusqlite::{params, Connection};
use taskchampion::storage::inmemory::InMemoryStorage;
use taskchampion::{
    storage::AccessMode, Operations, Replica, ServerConfig, SqliteStorage, Status, Uuid,
};
use tempfile::TempDir;

#[tokio::test]
async fn sync_continues_when_task_data_differs_from_operation_history() -> anyhow::Result<()> {
    let replica_dir = TempDir::new()?;
    let server_dir = TempDir::new()?;

    let mut server = ServerConfig::Local {
        server_dir: server_dir.path().to_path_buf(),
    }
    .into_server()
    .await?;

    let mut local =
        Replica::new(SqliteStorage::new(replica_dir.path(), AccessMode::ReadWrite, true).await?);
    let mut remote = Replica::new(InMemoryStorage::new());

    let changed_uuid = Uuid::new_v4();
    let unchanged_uuid = Uuid::new_v4();

    let mut ops = Operations::new();

    let mut changed_task = local.create_task(changed_uuid, &mut ops).await?;
    changed_task.set_description("original description".into(), &mut ops)?;
    changed_task.set_status(Status::Pending, &mut ops)?;

    let mut unchanged_task = local.create_task(unchanged_uuid, &mut ops).await?;
    unchanged_task.set_description("unchanged task".into(), &mut ops)?;
    unchanged_task.set_status(Status::Pending, &mut ops)?;

    local.commit_operations(ops).await?;
    local.sync(&mut server, false).await?;
    remote.sync(&mut server, false).await?;

    // Change the task row directly, leaving the operations table untouched.
    drop(local);
    let connection = Connection::open(replica_dir.path().join("taskchampion.sqlite3"))?;
    let data: String = connection.query_row(
        "SELECT data FROM tasks WHERE uuid = ?1",
        [changed_uuid.to_string()],
        |row| row.get(0),
    )?;
    let mut task_data: serde_json::Value = serde_json::from_str(&data)?;
    task_data["description"] = serde_json::Value::String("manual database edit".into());

    let rows_changed = connection.execute(
        "UPDATE tasks SET data = ?1 WHERE uuid = ?2",
        params![task_data.to_string(), changed_uuid.to_string()],
    )?;
    assert_eq!(rows_changed, 1);
    drop(connection);

    let mut local =
        Replica::new(SqliteStorage::new(replica_dir.path(), AccessMode::ReadWrite, false).await?);

    // Both syncs should complete despite the mismatch.
    local.sync(&mut server, false).await?;
    remote.sync(&mut server, false).await?;

    assert_eq!(
        local
            .get_task(changed_uuid)
            .await?
            .unwrap()
            .get_description(),
        "manual database edit"
    );
    assert_eq!(
        remote
            .get_task(changed_uuid)
            .await?
            .unwrap()
            .get_description(),
        "original description"
    );

    assert_eq!(
        local
            .get_task(unchanged_uuid)
            .await?
            .unwrap()
            .get_description(),
        "unchanged task"
    );
    assert_eq!(
        remote
            .get_task(unchanged_uuid)
            .await?
            .unwrap()
            .get_description(),
        "unchanged task"
    );

    Ok(())
}
