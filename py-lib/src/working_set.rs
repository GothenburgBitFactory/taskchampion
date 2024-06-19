use pyo3::prelude::*;
use taskchampion::Uuid;
use taskchampion::WorkingSet as TCWorkingSet;
// TODO: convert working set into python's iterable type
#[pyclass]
pub struct WorkingSet(pub(crate) TCWorkingSet);

#[pymethods]
impl WorkingSet {
    pub fn __len__(&self) -> usize {
        self.0.len()
    }

    pub fn largest_index(&self) -> usize {
        self.0.largest_index()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn by_index(&self, index: usize) -> Option<String> {
        self.0.by_index(index).map(|uuid| uuid.into())
    }

    pub fn by_uuid(&self, uuid: String) -> Option<usize> {
        // TODO I don't like the conversion, should use try-expect or something else as an input
        self.0.by_uuid(Uuid::parse_str(&uuid).unwrap())
    }

    fn __iter__(_slf: PyRef<'_, Self>) -> PyResult<Py<WorkingSet>> {
        todo!("Figure way to propertly implement iterator for python")
        // Usability-wise we want it to hold the reference to the iterator, so that
        // with each iteration the state persists.
    }
}
