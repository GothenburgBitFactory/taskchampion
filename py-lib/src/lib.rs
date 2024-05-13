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
pub mod tag;
use tag::*;
pub mod storage;
use storage::*;
pub mod dependency_map;
use dependency_map::*;

#[pymodule]
fn taskchampion(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Status>()?;
    m.add_class::<Replica>()?;
    m.add_class::<Task>()?;
    m.add_class::<Annotation>()?;
    m.add_class::<WorkingSet>()?;
    m.add_class::<Tag>()?;
    m.add_class::<InMemoryStorage>()?;
    m.add_class::<SqliteStorage>()?;
    m.add_class::<DependencyMap>()?;

    Ok(())
}
