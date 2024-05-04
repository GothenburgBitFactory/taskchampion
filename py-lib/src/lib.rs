pub mod replica;
use pyo3::prelude::*;
use replica::{TCReplica, TCStatus};
/// Formats the sum of two numbers as string.
#[pymodule]
fn py_lib(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<TCStatus>()?;
    m.add_class::<TCReplica>()?;
    Ok(())
}
