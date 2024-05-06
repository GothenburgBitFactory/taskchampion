use pyo3::prelude::*;
use taskchampion::Task as TCTask;

#[pyclass]
pub struct Task(pub(crate) TCTask);

unsafe impl Send for Task {}

#[pymethods]
impl Task {}
