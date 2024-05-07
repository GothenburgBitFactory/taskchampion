use pyo3::prelude::*;
pub mod replica;
use replica::*;
pub mod status;
use status::*;
pub mod task;
use task::*;
pub mod annotation;
use annotation::*;
pub mod working_set;
use working_set::*;

#[pymodule]
fn taskchampion(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Status>()?;
    m.add_class::<Replica>()?;
    m.add_class::<Task>()?;
    m.add_class::<Annotation>()?;
    m.add_class::<WorkingSet>()?;

    Ok(())
}
