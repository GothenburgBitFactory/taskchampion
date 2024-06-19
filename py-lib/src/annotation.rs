use chrono::DateTime;
use pyo3::prelude::*;
use taskchampion::Annotation as TCAnnotation;
#[pyclass]
/// An annotation for the task
pub struct Annotation(pub(crate) TCAnnotation);

#[pymethods]
impl Annotation {
    #[new]
    pub fn new() -> Self {
        Annotation(TCAnnotation {
            entry: DateTime::default(),
            description: String::new(),
        })
    }
    #[getter]
    pub fn entry(&self) -> String {
        self.0.entry.to_rfc3339()
    }

    #[setter]
    pub fn set_entry(&mut self, time: String) {
        self.0.entry = DateTime::parse_from_rfc3339(&time).unwrap().into()
    }

    #[getter]
    pub fn description(&self) -> String {
        self.0.description.clone()
    }

    #[setter]
    pub fn set_description(&mut self, description: String) {
        self.0.description = description
    }
}
