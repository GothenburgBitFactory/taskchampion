use std::collections::HashMap;

use crate::status::Status;
use crate::{Task, WorkingSet};
use pyo3::{exceptions::PyOSError, prelude::*};
use taskchampion::storage::SqliteStorage;
use taskchampion::{Replica as TCReplica, Uuid};

#[pyclass]
/// A replica represents an instance of a user's task data, providing an easy interface
/// for querying and modifying that data.
pub struct Replica(TCReplica);

unsafe impl Send for Replica {}
#[pymethods]
impl Replica {
    #[new]
    /// Instantiates the Replica
    ///
    /// Args:
    ///     path (str): path to the directory with the database
    ///     create_if_missing (bool): create the database if it does not exist
    /// Raises:
    ///     OsError: if database does not exist, and create_if_missing is false
    pub fn new(path: String, exists: bool) -> PyResult<Replica> {
        let storage = SqliteStorage::new(path, exists);

        // TODO convert this and other match Result into ? for less boilerplate.
        match storage {
            Ok(v) => Ok(Replica(TCReplica::new(Box::new(v)))),
            Err(e) => Err(PyOSError::new_err(e.to_string())),
        }
    }
    /// Create a new task
    /// The task must not already exist.
    pub fn new_task(&mut self, status: Status, description: String) {
        let _ = self.0.new_task(status.into(), description);
    }

    /// Get a list of all uuids for tasks in the replica.
    pub fn all_task_uuids(&mut self) -> PyResult<Vec<String>> {
        match self.0.all_task_uuids() {
            Ok(r) => Ok(r.iter().map(|uuid| uuid.to_string()).collect()),
            Err(e) => Err(PyOSError::new_err(e.to_string())),
        }
    }

    /// Get a list of all tasks in the replica.
    pub fn all_tasks(&mut self) -> PyResult<HashMap<String, Task>> {
        match self.0.all_tasks() {
            Ok(v) => Ok(v
                .into_iter()
                .map(|(key, value)| (key.to_string(), Task(value)))
                .collect()),
            Err(e) => Err(PyOSError::new_err(e.to_string())),
        }
    }

    pub fn update_task(
        &mut self,
        uuid: String,
        property: String,
        value: Option<String>,
    ) -> PyResult<HashMap<String, String>> {
        let uuid = Uuid::parse_str(&uuid).unwrap();
        match self.0.update_task(uuid, property, value) {
            Ok(res) => Ok(res),
            Err(e) => Err(PyOSError::new_err(e.to_string())),
        }
    }

    pub fn working_set(&mut self) -> PyResult<WorkingSet> {
        match self.0.working_set() {
            Ok(ws) => Ok(WorkingSet(ws)),
            Err(err) => Err(PyOSError::new_err(err.to_string())),
        }
    }

    // pub fn dependency_map(&self, force: bool) {
    //     self.0.dependency_map(force)
    // }

    pub fn get_task(&mut self, uuid: String) -> PyResult<Option<Task>> {
        // TODO: it should be possible to wrap this into a HOF that does two maps automatically
        // thus reducing boilerplate
        self.0
            .get_task(Uuid::parse_str(&uuid).unwrap())
            .map(|opt| opt.map(|t| Task(t)))
            .map_err(|e| PyOSError::new_err(e.to_string()))
    }

    pub fn import_task_with_uuid(&mut self, uuid: String) -> PyResult<Task> {
        self.0
            .import_task_with_uuid(Uuid::parse_str(&uuid).unwrap())
            .map(|task| Task(task))
            .map_err(|err| PyOSError::new_err(err.to_string()))
    }
    pub fn sync(&self, _avoid_snapshots: bool) {
        todo!()
    }

    pub fn rebuild_working_set(&mut self, renumber: bool) -> PyResult<()> {
        self.0
            .rebuild_working_set(renumber)
            .map_err(|err| PyOSError::new_err(err.to_string()))
    }
    pub fn add_undo_point(&mut self, force: bool) -> PyResult<()> {
        self.0
            .add_undo_point(force)
            .map_err(|err| PyOSError::new_err(err.to_string()))
    }
    pub fn num_local_operations(&mut self) -> PyResult<usize> {
        self.0
            .num_local_operations()
            .map_err(|err| PyOSError::new_err(err.to_string()))
    }

    pub fn num_undo_points(&mut self) -> PyResult<usize> {
        self.0
            .num_local_operations()
            .map_err(|err| PyOSError::new_err(err.to_string()))
    }
}
