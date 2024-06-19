use pyo3::prelude::*;
use taskchampion::{DependencyMap as TCDependencyMap, Uuid};

#[pyclass]
pub struct DependencyMap(pub(crate) TCDependencyMap);

#[pymethods]
impl DependencyMap {
    // TODO: possibly optimize this later, if possible
    pub fn dependencies(&self, dep_of: String) -> Vec<String> {
        let uuid = Uuid::parse_str(&dep_of).unwrap();
        self.0.dependencies(uuid).map(|uuid| uuid.into()).collect()
    }

    pub fn dependents(&self, dep_on: String) -> Vec<String> {
        let uuid = Uuid::parse_str(&dep_on).unwrap();

        self.0.dependencies(uuid).map(|uuid| uuid.into()).collect()
    }
}
