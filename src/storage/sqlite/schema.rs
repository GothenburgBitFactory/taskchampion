use crate::errors::{Error, Result};
use anyhow::Context;
use rusqlite::{params, Connection, OptionalExtension, Transaction};

/// A database schema version.
///
/// The first value is the major version, with different major versions completely incompatible
/// with one another.
///
/// The second is the minor version, with all minor versions in the same major version being
/// compatible with one another. That is, a TaskChampion binary with a latest version of (MAJ, MIN)
/// can safely operate on any DB with major version MAJ, whether its minor version is greater or
/// smaller than MIN, and can upgrade that DB to (MAJ, MIN) if necessary.
///
/// For example, a new index would trigger an increment of the minor version, as older versions of
/// TaskChampion can still safely use the DB with the addition of the index.
#[derive(Copy, Clone, Debug, PartialOrd, Ord, PartialEq, Eq)]
pub(super) struct DbVersion(pub u32, pub u32);

type UpgradeFn = fn(&Transaction) -> Result<()>;

/// DB Versions and functions to upgrade to them.
///
/// Add new vesions here, in order. Prefer minor version bumps for better compatibility, using
/// techniques like virtual columns where possible.
const VERSIONS: &[(DbVersion, UpgradeFn)] = &[
    (DbVersion(0, 1), upgrade_to_0_1),
    (DbVersion(0, 2), upgrade_to_0_2),
];
pub(super) const LATEST_VERSION: DbVersion = VERSIONS[VERSIONS.len() - 1].0;

pub(super) fn upgrade_db(con: &mut Connection) -> Result<()> {
    let mut current_version = get_db_version(con)?;

    if current_version.0 > LATEST_VERSION.0 {
        return Err(Error::Database(
            "Database is too new for this version of TaskChampion".into(),
        ));
    }

    for (version, upgrade) in VERSIONS {
        if current_version < *version {
            let t = con.transaction()?;
            upgrade(&t)?;
            t.commit()?;
            current_version = *version;
        }
    }

    Ok(())
}

/// Update to DbVersion(0, 1).
///
/// This function can upgrade from any schema before (0, 1), including those of TaskChampion
/// 0.8 and 0.9, as all of its operations are performed only if the schema element does not
/// already exist.
fn upgrade_to_0_1(t: &Transaction) -> Result<()> {
    let create_tables = vec![
            "CREATE TABLE IF NOT EXISTS operations (id INTEGER PRIMARY KEY AUTOINCREMENT, data STRING);",
            "CREATE TABLE IF NOT EXISTS sync_meta (key STRING PRIMARY KEY, value STRING);",
            "CREATE TABLE IF NOT EXISTS tasks (uuid STRING PRIMARY KEY, data STRING);",
            "CREATE TABLE IF NOT EXISTS working_set (id INTEGER PRIMARY KEY, uuid STRING);",
        ];
    for q in create_tables {
        t.execute(q, []).context("Creating table")?;
    }

    // -- At this point the DB schema is that of TaskChampion 0.8.0.

    // Check for and add the `operations.uuid` column.
    if !has_column(t, "operations", "uuid")? {
        t.execute(
            r#"ALTER TABLE operations ADD COLUMN uuid GENERATED ALWAYS AS (
                coalesce(json_extract(data, "$.Update.uuid"),
                         json_extract(data, "$.Create.uuid"),
                         json_extract(data, "$.Delete.uuid"))) VIRTUAL"#,
            [],
        )
        .context("Adding operations.uuid")?;

        t.execute("CREATE INDEX operations_by_uuid ON operations (uuid)", [])
            .context("Creating operations_by_uuid")?;
    }

    if !has_column(t, "operations", "synced")? {
        t.execute(
            "ALTER TABLE operations ADD COLUMN synced bool DEFAULT false",
            [],
        )
        .context("Adding operations.synced")?;

        t.execute(
            "CREATE INDEX operations_by_synced ON operations (synced)",
            [],
        )
        .context("Creating operations_by_synced")?;
    }

    // -- At this point the DB schema is that of TaskChampion 0.9.0.

    create_version_table(t)?;

    set_db_version(t, DbVersion(0, 1))?;

    Ok(())
}

/// Update to DbVersion(0, 2).
///
/// This fixes some bad syntax in the schema in DbVersion(0, 1) -- use of double quotes in the
/// JSON paths passed to `json_extract`. This syntax works with SQLite 3.45.1 but not 3.50.4, but
/// is invalid per the grammar in both versions.
fn upgrade_to_0_2(t: &Transaction) -> Result<()> {
    // Fix the `operations.uuid` column. Note that this column is virtual and
    // thus contains no data, so dropping it is not a lossy operation.
    t.execute(r#"DROP INDEX operations_by_uuid"#, [])
        .context("Dropping index operatoins_by_uuid")?;
    t.execute(r#"ALTER TABLE operations DROP COLUMN uuid"#, [])
        .context("Removing incorrect operations.uuid")?;
    t.execute(
        r#"ALTER TABLE operations ADD COLUMN uuid GENERATED ALWAYS AS (
                coalesce(json_extract(data, '$.Update.uuid'),
                         json_extract(data, '$.Create.uuid'),
                         json_extract(data, '$.Delete.uuid'))) VIRTUAL"#,
        [],
    )
    .context("Creating correct operations.uuid")?;
    t.execute("CREATE INDEX operations_by_uuid ON operations (uuid)", [])
        .context("Creating index operations_by_uuid")?;

    set_db_version(t, DbVersion(0, 2))?;

    Ok(())
}

fn create_version_table(t: &Transaction) -> Result<()> {
    // The `singleton` column constrains this table to have no more than one row.
    t.execute(
        r#"CREATE TABLE IF NOT EXISTS version (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 0),
                major INTEGER,
                minor INTEGER)"#,
        [],
    )
    .context("Creating table")?;
    Ok(())
}

/// Get the current DB version, from the `version` table. If the table or row does not exist, that
/// is considered version (0, 0).
///
/// This takes a connection for efficiency: this is called every time a Storage instance is
/// created, so the overhead of BEGIN and COMMIT for a transaction is unnecessary in the happy
/// path.
pub(super) fn get_db_version(con: &mut Connection) -> Result<DbVersion> {
    let version: Option<(u32, u32)> = match con
        .query_row("SELECT major, minor FROM version", [], |r| {
            Ok((r.get("major")?, r.get("minor")?))
        })
        .optional()
    {
        Ok(v) => v,
        Err(err @ rusqlite::Error::SqliteFailure(_, _)) => {
            // This error may have occurred because the "version" table does not exist, in which
            // case the version is (0, 0).
            if has_column(&con.transaction()?, "version", "major")? {
                return Err(err.into());
            }
            None
        }
        Err(err) => return Err(err.into()),
    };
    let (major, minor) = version.unwrap_or((0, 0));
    Ok(DbVersion(major, minor))
}

/// Set the current DB version.
fn set_db_version(t: &Transaction, version: DbVersion) -> Result<()> {
    let DbVersion(major, minor) = version;
    t.execute(
        r#"INSERT INTO version (singleton, major, minor) VALUES (0, ?, ?)
               ON CONFLICT(singleton) do UPDATE SET major=?, minor=?"#,
        params![major, minor, major, minor],
    )?;
    Ok(())
}

fn has_column(t: &Transaction, table: &str, column: &str) -> Result<bool> {
    let res: u32 = t
        .query_row(
            "SELECT COUNT(*) AS c FROM pragma_table_xinfo(?) WHERE name=?",
            [table, column],
            |r| r.get(0),
        )
        .with_context(|| format!("Checking for {}.{}", table, column))?;
    Ok(res > 0)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn get_db_version_no_table() -> Result<()> {
        let mut con = Connection::open_in_memory()?;
        assert_eq!(get_db_version(&mut con)?, DbVersion(0, 0));
        Ok(())
    }

    #[test]
    fn get_db_version_empty() -> Result<()> {
        let mut con = Connection::open_in_memory()?;
        let t = con.transaction()?;
        create_version_table(&t)?;
        t.commit()?;
        assert_eq!(get_db_version(&mut con)?, DbVersion(0, 0));
        Ok(())
    }

    #[test]
    fn get_db_version_set() -> Result<()> {
        let mut con = Connection::open_in_memory()?;
        let t = con.transaction()?;
        create_version_table(&t)?;
        set_db_version(&t, DbVersion(3, 5))?;
        t.commit()?;
        assert_eq!(get_db_version(&mut con)?, DbVersion(3, 5));
        Ok(())
    }

    #[test]
    fn get_db_version_set_twice() -> Result<()> {
        let mut con = Connection::open_in_memory()?;
        let t = con.transaction()?;
        create_version_table(&t)?;
        set_db_version(&t, DbVersion(3, 5))?;
        set_db_version(&t, DbVersion(4, 7))?;
        t.commit()?;
        assert_eq!(get_db_version(&mut con)?, DbVersion(4, 7));
        Ok(())
    }

    #[test]
    fn test_upgrade_to_0_1() -> Result<()> {
        let mut con = Connection::open_in_memory()?;
        {
            let t = con.transaction()?;
            upgrade_to_0_1(&t)?;
            t.commit()?;
        }
        {
            let t = con.transaction()?;
            assert!(has_column(&t, "operations", "id")?);
            assert!(has_column(&t, "operations", "data")?);
            assert!(has_column(&t, "operations", "uuid")?);
            assert!(has_column(&t, "sync_meta", "key")?);
            assert!(has_column(&t, "sync_meta", "value")?);
            assert!(has_column(&t, "tasks", "uuid")?);
            assert!(has_column(&t, "tasks", "data")?);
            assert!(has_column(&t, "working_set", "id")?);
            assert!(has_column(&t, "working_set", "uuid")?);
        }
        assert_eq!(get_db_version(&mut con)?, DbVersion(0, 1));
        Ok(())
    }

    #[test]
    fn test_upgrade_to_0_2() -> Result<()> {
        let mut con = Connection::open_in_memory()?;
        {
            let t = con.transaction()?;
            upgrade_to_0_1(&t)?;
            upgrade_to_0_2(&t)?;
            t.commit()?;
        }
        {
            let t = con.transaction()?;
            assert!(has_column(&t, "operations", "id")?);
            assert!(has_column(&t, "operations", "data")?);
            assert!(has_column(&t, "operations", "uuid")?);
            assert!(has_column(&t, "sync_meta", "key")?);
            assert!(has_column(&t, "sync_meta", "value")?);
            assert!(has_column(&t, "tasks", "uuid")?);
            assert!(has_column(&t, "tasks", "data")?);
            assert!(has_column(&t, "working_set", "id")?);
            assert!(has_column(&t, "working_set", "uuid")?);
        }
        assert_eq!(get_db_version(&mut con)?, DbVersion(0, 2));
        Ok(())
    }
}
