use pyo3::prelude::*;
use taskchampion::Tag as TCTag;

/// TODO: following the api there currently is no way to construct the task by hand, not sure if this is
/// correct
#[pyclass]
pub struct Tag(pub(crate) TCTag);

#[pymethods]
impl Tag {
    #[new]
    pub fn new(tag: String) -> anyhow::Result<Self> {
        Ok(Tag(tag.parse()?))
    }
    pub fn is_synthetic(&self) -> bool {
        self.0.is_synthetic()
    }

    pub fn is_user(&self) -> bool {
        self.0.is_user()
    }
}
