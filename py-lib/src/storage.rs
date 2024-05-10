use pyo3::prelude::*;
use taskchampion::storage::{
    InMemoryStorage as TCInMemoryStorage, SqliteStorage as TCSqliteStorage,
};
// TODO: actually make the storage usable and extensible, rn it just exists /shrug
pub trait Storage {}
#[pyclass]
pub struct InMemoryStorage(TCInMemoryStorage);

#[pymethods]
impl InMemoryStorage {
    #[new]
    pub fn new() -> InMemoryStorage {
        InMemoryStorage(TCInMemoryStorage::new())
    }
}

impl Storage for InMemoryStorage {}

#[pyclass]
pub struct SqliteStorage(TCSqliteStorage);

#[pymethods]
impl SqliteStorage {
    #[new]
    pub fn new(path: String, create_if_missing: bool) -> anyhow::Result<Self> {
        // TODO: kinda ugly, prettify;
        Ok(SqliteStorage(TCSqliteStorage::new(
            path,
            create_if_missing,
        )?))
    }
}

impl Storage for SqliteStorage {}
