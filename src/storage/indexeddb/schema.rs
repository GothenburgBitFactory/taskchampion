use crate::errors::Result;
use idb::{Database, KeyPath};

/// The IndexedDB database version. This can be incremented to trigger calls to
/// `upgrade`.
pub(super) const DB_VERSION: u32 = 1;

pub(super) const TASKS: &str = "tasks";
pub(super) const OPERATIONS: &str = "operations";
pub(super) const OPERATIONS_BY_UUID: &str = "operations_by_uuid";
pub(super) const OPERATIONS_BY_UNSYNCED: &str = "operations_by_unsynced";
pub(super) const SYNC_META: &str = "sync_meta";
pub(super) const WORKING_SET: &str = "working_set";

/* Current Schema:
 *
 *  - Object store `tasks` stores TaskMaps as values, in the form of a JS object. Keys are UUIDs in
 *    the form of a string.
 *
 *  - Object store `operations` stores values {uuid, operation, unsynced}, with `unsynced` omitted
 *    for synced operations. Keys are auto-incrementing integers. The `uuid` and `unsynced`
 *    properties are indexed.
 *
 *  - Object store `sync_meta` stores string values with string keys. At the moment it only stores
 *    the base revision.
 *
 *  - Object store `working_set` stores UUIDs as strings, keyed by the working set ID as an integer.
 */

fn upgrade_0_to_1(db: Database) -> Result<()> {
    // Create the `tasks` table
    let mut params = idb::ObjectStoreParams::new();
    params.key_path(None);
    params.auto_increment(false);
    db.create_object_store(TASKS, params)?;

    // Create the `operations` table
    let mut params = idb::ObjectStoreParams::new();
    params.key_path(None);
    params.auto_increment(true);
    let ops = db.create_object_store(OPERATIONS, params)?;

    ops.create_index(OPERATIONS_BY_UUID, KeyPath::new_single("uuid"), None)?;
    ops.create_index(
        OPERATIONS_BY_UNSYNCED,
        KeyPath::new_single("unsynced"),
        None,
    )?;

    // Create the `sync_meta` table
    let mut params = idb::ObjectStoreParams::new();
    params.key_path(None);
    params.auto_increment(false);
    db.create_object_store(SYNC_META, params)?;

    // Create the `working_set` table
    let mut params = idb::ObjectStoreParams::new();
    params.key_path(None);
    params.auto_increment(false);
    db.create_object_store(WORKING_SET, params)?;

    Ok(())
}

pub(super) fn upgrade(db: Database, old_version: u32) -> Result<()> {
    if old_version == 0 {
        upgrade_0_to_1(db)
    } else {
        unreachable!("old_version should be less than DB_VERSION");
    }
}
