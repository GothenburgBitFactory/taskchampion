/**

This module defines the client interface to TaskChampion sync servers.
It defines a [trait](crate::server::Server) for servers, and implements both local (on-disk) and remote (via etesync) servers.

Typical uses of this crate do not interact directly with this module; [`ServerConfig`](crate::ServerConfig) is sufficient.
However, users who wish to implement their own server interfaces can implement the traits defined here and pass the result to [`Replica`](crate::Replica).

*/

#[cfg(test)]
pub(crate) mod test;

mod config;
mod ete;
mod local;
mod types;

pub use config::ServerConfig;
pub use ete::EteServer;
pub use local::LocalServer;
pub use types::*;
