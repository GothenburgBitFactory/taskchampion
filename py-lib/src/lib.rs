use pyo3::prelude::*;
pub mod replica;
use replica::*;
pub mod status;
use status::*;
pub mod task;
use task::*;

#[pymodule]
fn taskchampion(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Status>()?;
    m.add_class::<Replica>()?;

    Ok(())
}
