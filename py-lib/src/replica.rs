use std::collections::HashMap;

use crate::status::Status;
use crate::Task;
use pyo3::{exceptions::PyOSError, prelude::*};
use taskchampion::storage::SqliteStorage;
use taskchampion::Replica as TCReplica;

#[pyclass]
/// A replica represents an instance of a user's task data, providing an easy interface
/// for querying and modifying that data.
pub struct Replica(TCReplica);

unsafe impl Send for Replica {}

#[pymethods]
impl Replica {
    #[new]
    pub fn new(path: String, exists: bool) -> PyResult<Replica> {
        let storage = SqliteStorage::new(path, exists);
        // TODO convert this and other match Result into ? for less boilerplate.
        match storage {
            Ok(v) => Ok(Replica(TCReplica::new(Box::new(v)))),
            Err(e) => Err(PyOSError::new_err(e.to_string())),
        }
    }
    pub fn new_task(&mut self, status: Status, description: String) {
        let _ = self.0.new_task(status.into(), description);
    }

    pub fn all_task_uuids(&mut self) -> PyResult<Vec<String>> {
        match self.0.all_task_uuids() {
            Ok(r) => Ok(r.iter().map(|uuid| uuid.to_string()).collect()),
            Err(e) => Err(PyOSError::new_err(e.to_string())),
        }
    }
    pub fn all_tasks(&mut self) -> PyResult<HashMap<String, Task>> {
        match self.0.all_tasks() {
            Ok(v) => Ok(v
                .into_iter()
                .map(|(key, value)| (key.to_string(), Task(value)))
                .collect()),
            Err(e) => Err(PyOSError::new_err(e.to_string())),
        }
    }
}
