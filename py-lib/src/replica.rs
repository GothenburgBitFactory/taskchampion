use pyo3::{exceptions::PyOSError, prelude::*};
use std::convert::From;
use taskchampion::{storage::SqliteStorage, Replica, Status};
#[pyclass]
pub struct TCReplica(Replica);

unsafe impl Send for TCReplica {}

#[pymethods]
impl TCReplica {
    #[new]
    pub fn new(path: String, exists: bool) -> PyResult<TCReplica> {
        let storage = SqliteStorage::new(path, exists);
        match storage {
            Ok(v) => Ok(TCReplica(Replica::new(Box::new(v)))),
            Err(e) => Err(PyOSError::new_err(e.to_string())),
        }
    }
    pub fn new_task(&mut self, status: TCStatus, description: String) {
        let _ = self.0.new_task(status.into(), description);
    }

    pub fn all_task_uuids(&mut self) -> PyResult<Vec<String>> {
        match self.0.all_task_uuids() {
            Ok(r) => Ok(r.iter().map(|uuid| uuid.to_string()).collect()),
            Err(e) => Err(PyOSError::new_err(e.to_string())),
        }
    }
}

#[pyclass]
#[derive(Clone, Copy)]
pub enum TCStatus {
    Pending,
    Completed,
    Deleted,
    Recurring,
    Unknown,
}

impl From<TCStatus> for Status {
    fn from(status: TCStatus) -> Self {
        match status {
            TCStatus::Pending => Status::Pending,
            TCStatus::Completed => Status::Completed,
            TCStatus::Deleted => Status::Deleted,
            TCStatus::Recurring => Status::Recurring,
            _ => Status::Unknown(format!("unknown TCStatus {}", status as u32)),
        }
    }
}

impl From<Status> for TCStatus {
    fn from(status: Status) -> Self {
        match status {
            Status::Pending => TCStatus::Pending,
            Status::Completed => TCStatus::Completed,
            Status::Deleted => TCStatus::Deleted,
            Status::Recurring => TCStatus::Recurring,
            Status::Unknown(_) => TCStatus::Unknown,
        }
    }
}
