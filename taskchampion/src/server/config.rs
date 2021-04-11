use super::types::Server;
use super::LocalServer;
use std::path::PathBuf;

// TODO: LocalServerConfig

// TODO: doc
pub struct EteServerConfig {
    /// base URL of the etesync server
    pub server_url: String,
    /// Etesync username
    pub username: String,
    /// Etesync key
    pub key: Vec<u8>,
}

/// The configuration for a replica's access to a sync server.
pub enum ServerConfig {
    /// A local task database, for situations with a single replica.
    Local {
        /// Path containing the server's DB
        server_dir: PathBuf,
    },

    /// A remote etesync server
    EteServer(EteServerConfig),
}

impl ServerConfig {
    /// Get a server based on this configuration
    pub fn into_server(self) -> anyhow::Result<Box<dyn Server>> {
        Ok(match self {
            ServerConfig::Local { server_dir } => Box::new(LocalServer::new(server_dir)?),
            _ => todo!(),
            //ServerConfig::EteServer(config) => Box::new(EteServer::new(config)),
        })
    }
}
