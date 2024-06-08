# Usage

## Conceptual Overview

The following provide a brief overview of the core concepts in TaskChampion.
Subsequent chapters, and API documentation, provide more detail.

### Replica

A TaskChampion replica is a local copy of a user’s task data.
As the name suggests, several replicas of the same data can exist (such as on a user’s laptop and on their phone) and can synchronize with one another.

A replica contains a collection of tasks, indexed by UUID.
It also stores a working set, and ancillary information to support synchronization.

### Task

A task is the unit of work that TaskChampion tracks.
A task is represented as a map of strings to strings.
The meaning of those strings are given in the [task model](./tasks.md).

### Working Set

A working set contains, roughly, the tasks that are currently pending.
It assigns a short, integer identifier to each such task, which is easier for users to remember and type.
The working set can be "rebuilt" as the task list changes, updating the identifiers for some tasks.

### Storage

Storage defines where and how tasks are stored.

### Server

A server supports synchronizing tasks among several replicas.
This may refer to an instance of `taskchampion-sync-server` or a number of other options.

## APIs

### Rust

TaskChampion is implemented in Rust, and implementation represents its primary public API.
It is documented at [docs.rs/taskchampion](https://docs.rs/taskchampion/latest/taskchampion/).

### C

The C API contains a rough mapping of Rust types to opaque C structures, and Rust methods to C functions.

The `taskchampion-lib` crate generates libraries suitable for use from C (or any C-compatible language).
It is a "normal" Cargo crate that happens to export a number of `extern "C"` symbols, and also contains a [`taskchampion.h`](https://github.com/GothenburgBitFactory/taskchampion/blob/main/lib/taskchampion.h) defining those symbols.
The primary documentation for the C API is in the header file.

*WARNING: the C API is not yet stable!*
Please consult with the TaskChampion developers before relying on this API.
