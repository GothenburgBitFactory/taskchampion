use crate::{status::Status, Annotation};
use pyo3::prelude::*;
use taskchampion::{Tag as TCTag, Task as TCTask};
// TODO: actually create a front-facing user class, instead of this data blob
#[pyclass]
pub struct Task(pub(crate) TCTask);

unsafe impl Send for Task {}

#[pymethods]
impl Task {
    /// Get a tasks UUID
    ///
    /// Returns:
    ///     str: UUID of a task
    // TODO: possibly determine if it's possible to turn this from/into python's UUID instead
    pub fn get_uuid(&self) -> String {
        self.0.get_uuid().to_string()
    }
    /// Get a task's status
    /// Returns:
    ///     Status: Status subtype
    pub fn get_status(&self) -> Status {
        self.0.get_status().into()
    }

    pub fn get_taskmap(&self) -> PyResult<()> {
        unimplemented!()
    }
    /// Get the entry timestamp for a task
    ///
    /// Returns:
    ///     str: RFC3339 timestamp
    ///     None: No timestamp
    // Attempt to convert this into a python datetime later on
    pub fn get_entry(&self) -> Option<String> {
        self.0.get_entry().map(|timestamp| timestamp.to_rfc3339())
    }

    /// Get the task's priority
    ///
    /// Returns:
    ///     str: Task's priority
    pub fn get_priority(&self) -> String {
        self.0.get_priority().to_string()
    }

    /// Get the wait timestamp of the task
    ///
    /// Returns:
    ///     str: RFC3339 timestamp
    ///     None: No timesamp
    pub fn get_wait(&self) -> Option<String> {
        self.0.get_wait().map(|timestamp| timestamp.to_rfc3339())
    }
    /// Check if the task is waiting
    ///
    /// Returns:
    ///     bool: if the task is waiting
    pub fn is_waiting(&self) -> bool {
        self.0.is_waiting()
    }

    /// Check if the task is active
    ///
    /// Returns:
    ///     bool: if the task is active
    pub fn is_active(&self) -> bool {
        self.0.is_active()
    }
    /// Check if the task is blocked
    ///
    /// Returns:
    ///     bool: if the task is blocked
    pub fn is_blocked(&self) -> bool {
        self.0.is_blocked()
    }
    /// Check if the task is blocking
    ///
    /// Returns:
    ///     bool: if the task is blocking
    pub fn is_blocking(&self) -> bool {
        self.0.is_blocking()
    }
    /// Check if the task has a tag
    ///
    /// Returns:
    ///     bool: if the task has a given tag
    pub fn has_tag(&self, tag: &str) -> bool {
        if let Ok(tag) = TCTag::try_from(tag) {
            self.0.has_tag(&tag)
        } else {
            false
        }
    }

    /// Get task tags
    ///
    /// Returns:
    ///     list[str]: list of tags
    pub fn get_tags(&self) -> Vec<String> {
        self.0
            .get_tags()
            .into_iter()
            .map(|v| v.to_string())
            .collect()
    }
    /// Get task annotations
    ///
    /// Returns:
    ///     list[Annotation]: list of task annotations
    pub fn get_annotations(&self) -> Vec<Annotation> {
        self.0
            .get_annotations()
            .into_iter()
            .map(|annotation| Annotation(annotation))
            .collect()
    }

    /// Get a task UDA
    ///
    /// Arguments:
    ///     namespace (str): argument namespace
    ///     key (str): argument key
    ///
    /// Returns:
    ///     str: UDA value
    ///     None: Not found
    pub fn get_uda(&self, namespace: &str, key: &str) -> Option<&str> {
        self.0.get_uda(namespace, key)
    }

    // TODO: this signature is ugly and confising, possibly replace this with a struct in the
    // actual code
    /// get all the task's UDAs
    ///
    /// Returns:
    ///     Uh oh, ew?
    pub fn get_udas(&self) -> Vec<((&str, &str), &str)> {
        self.0.get_udas().collect()
    }
    /// Get the task modified time
    ///
    /// Returns:
    ///     str: RFC3339 modified time
    ///     None: Not applicable
    pub fn get_modified(&self) -> Option<String> {
        self.0
            .get_modified()
            .map(|timestamp| timestamp.to_rfc3339())
    }

    /// Get the task's due date
    ///
    /// Returns:
    ///     str: RFC3339 due date
    ///     None: No such value
    pub fn get_due(&self) -> Option<String> {
        self.0.get_due().map(|timestamp| timestamp.to_rfc3339())
    }
    /// Get a list of tasks dependencies
    ///
    /// Returns:
    ///     list[str]: List of UUIDs of the task depends on
    pub fn get_dependencies(&self) -> Vec<String> {
        self.0
            .get_dependencies()
            .into_iter()
            .map(|uuid| uuid.to_string())
            .collect()
    }
    /// Get the task's property value
    ///
    /// Returns:
    ///     str: property value
    ///     None: no such value
    pub fn get_value(&self, property: String) -> Option<&str> {
        self.0.get_value(property)
    }
}
