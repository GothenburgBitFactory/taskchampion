use super::types::Server;
use super::LocalServer;
use std::path::PathBuf;

/// The configuration for a replica's access to a sync server.
pub enum ServerConfig {
    /// A local task database, for situations with a single replica.
    Local {
        /// Path containing the server's DB
        server_dir: PathBuf,
    },
}

impl ServerConfig {
    /// Get a server based on this configuration
    pub fn into_server(self) -> anyhow::Result<Box<dyn Server>> {
        Ok(match self {
            ServerConfig::Local { server_dir } => Box::new(LocalServer::new(server_dir)?),
        })
    }
}
