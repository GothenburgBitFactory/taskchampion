#![deny(clippy::all)]
/*!

This crate implements the core of TaskChampion, the [replica](crate::Replica).

Users of this crate can manipulate a task database using this API, including synchronizing that task database with others via a synchronization server.

Example uses of this crate:
 * user interfaces for task management, such as mobile apps, web apps, or command-line interfaces
 * integrations for task management, such as synchronization with ticket-tracking systems or
   request forms.

# Replica

A TaskChampion replica is a local copy of a user's task data.  As the name suggests, several
replicas of the same data can exist (such as on a user's laptop and on their phone) and can
synchronize with one another.

Replicas are accessed using the [`Replica`] type.

# Task Storage

Replicas access the task database via a [storage object](crate::storage::Storage).
Create a storage object with [`StorageConfig`].

The [`storage`] module supports pluggable storage for a replica's data.
An implementation is provided, but users of this crate can provide their own implementation as well.

# Server

Replica synchronization takes place against a server.
Create a server with [`ServerConfig`].

The [`server`] module defines the interface a server must meet.
Users can define their own server impelementations.

# Feature Flags

Support for some optional functionality is controlled by feature flags.

 * `server-gcp` - sync to Google Cloud Platform
 * `server-sync` - sync to the taskchampion-sync-server
 * `sync` - enables all of the sync features above
 * `bundled` - activates bundling system libraries like sqlite

 By default, `sync` and `bundled` are enabled.

# See Also

See the [TaskChampion Book](http://gothenburgbitfactory.github.io/taskchampion)
for more information about the design and usage of the tool.

# Minimum Supported Rust Version (MSRV)

This crate supports Rust version 1.70.0 and higher.

 */
mod depmap;
mod errors;
mod operation;
mod replica;
pub mod server;
pub mod storage;
mod task;
mod taskdb;
mod utils;
mod workingset;

pub use depmap::DependencyMap;
pub use errors::Error;
pub use operation::{Operation, Operations};
pub use replica::Replica;
pub use server::{Server, ServerConfig};
pub use storage::StorageConfig;
pub use task::{utc_timestamp, Annotation, Status, Tag, Task, TaskData};
pub use workingset::WorkingSet;

/// Re-exported type from the `uuid` crate, for ease of compatibility for consumers of this crate.
pub use uuid::Uuid;

/// Re-exported chrono module.
pub use chrono;
