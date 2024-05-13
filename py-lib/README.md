# Python Taskchampion Bindings

This submodule contains bindings to the Taskchampion

# TODO

- There is no good way to describe functions that accept interface (e.g. `Replica::new` accepts any of the storage implementations, but Python bindings lack such mechanisms), currently, `Replica::new` just constructs the SqliteStorage from the params passed into the constructor.
- Currently Task class is just a reflection of the rust's `Task` struct, but constructing the standalone `TaskMut` is impossible, as Pyo3 bindings do not allow lifetimes (python has no alternatives to them). Would be nice to expand the `Task` class to include the methods from `TaskMut` and convert into the mutable state and back when they are called.
- It is possible to convert `WorkingSet` into a python iterator (you can iterate over it via `for item in <blah>:` or `next(<blah>)`), but that needs a way to store the current state.

