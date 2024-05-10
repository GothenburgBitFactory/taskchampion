# Notes while developing the project

- renamed the package to taskchampion, instead of py_lib, so the python imports work nicely
- Implementing 2nd python class that is mutable and that has it's own mutable methods should be easier than trying to use single object that can have both states.
- Scratch that, sanely wrapping TaskMut with a lifetime param is not possible w/ pyo3, python just cannot handle lifetimes
