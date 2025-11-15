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

The [`storage`] module supports pluggable storage for a replica's data.
An implementation is provided, but users of this crate can provide their own implementation as well.

# Server

Replica synchronization takes place against a server.
Create a server with [`ServerConfig`].

The [`server`] module defines the interface a server must meet.
Several server implementations are included, and users can define their own implementations.

# Example

```rust
# #[cfg(all(feature = "storage-sqlite", feature = "server-local"))]
# {
# use taskchampion::{storage::AccessMode, ServerConfig, Replica, SqliteStorage};
# use tempfile::TempDir;
# async fn main() -> anyhow::Result<()> {
# let taskdb_dir = TempDir::new()?;
# let taskdb_dir = taskdb_dir.path().to_path_buf();
# let server_dir = TempDir::new()?;
# let server_dir = server_dir.path().to_path_buf();
// Create a new Replica, storing data on disk.
let storage = SqliteStorage::new(
  taskdb_dir,
  AccessMode::ReadWrite,
  true,
)?;
let mut replica = Replica::new(storage);

// Set up a local, on-disk server.
let server_config = ServerConfig::Local { server_dir };
let mut server = server_config.into_server().await?;

// Sync to that server.
replica.sync(&mut server, true).await?;
#
# Ok(())
# }
# }
```

# Feature Flags

Support for some optional functionality is controlled by feature flags.

 * `server-aws` - sync to Amazon Web Services
 * `server-gcp` - sync to Google Cloud Platform
 * `server-sync` - sync to the taskchampion-sync-server
 * `server-local` - sync to a local file
 * `sync` - enables all of the sync features above
 * `storage-sqlite` - store task data locally in SQLite
 * `storage` - enables all of the storage features above
 * `bundled` - activates bundling system libraries like sqlite
 * `tls-webpki-roots` - use TLS roots bundled with the library, instead of reading them from
   system configuration.
 * `tls-native-roots` - use native (system) TLS roots, instead of those bundled with rustls.
   If both `tls-webpki-roots` and `tls-native-roots` are given, `tls-native-roots` takes
   precedence. At least one of the `tls-*-roots` features must be enabled to support any
   of the HTTPS-based `server-*` features.

By default, `sync`, `storage`, `bundled`, and `tls-webpki-roots` are enabled.

# See Also

See the [TaskChampion Book](http://gothenburgbitfactory.github.io/taskchampion)
for more information about the design and usage of the tool.

# Minimum Supported Rust Version (MSRV)

This crate supports Rust version 1.82.0 and higher.
