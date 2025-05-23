/*!
This module defines the client interface to TaskChampion sync servers.

It defines a [trait](crate::server::Server) for servers, and implements both local and remote
servers.

Typical uses of this crate do not interact directly with this module; [`ServerConfig`] is
sufficient. However, users who wish to implement their own server interfaces can implement the
traits defined here and pass the result to [`Replica`](crate::Replica).
*/

#[cfg(test)]
pub(crate) mod test;

mod config;
mod op;
mod types;

#[cfg(feature = "encryption")]
mod encryption;

#[cfg(feature = "server-local")]
mod local;

#[cfg(feature = "server-sync")]
mod sync;

#[cfg(feature = "cloud")]
mod cloud;

pub use config::*;
pub use types::*;

pub(crate) use op::SyncOp;
