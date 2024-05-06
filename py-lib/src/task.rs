use pyo3::prelude::*;
use taskchampion::Task as TCTask;

// TODO: actually create a front-facing user class, instead of this data blob
#[pyclass]
pub struct Task(pub(crate) TCTask);

unsafe impl Send for Task {}

#[pymethods]
impl Task {}
