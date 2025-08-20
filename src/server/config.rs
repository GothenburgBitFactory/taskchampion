use super::types::Server;
use crate::errors::Result;
#[cfg(feature = "server-aws")]
pub use crate::server::cloud::aws::AwsCredentials;
#[cfg(feature = "server-aws")]
use crate::server::cloud::aws::AwsService;
#[cfg(feature = "server-gcp")]
use crate::server::cloud::gcp::GcpService;
#[cfg(feature = "cloud")]
use crate::server::cloud::CloudServer;
#[cfg(feature = "server-local")]
use crate::server::local::LocalServer;
#[cfg(feature = "server-sync")]
use crate::server::sync::SyncServer;
use std::path::PathBuf;
#[cfg(feature = "server-sync")]
use uuid::Uuid;

/// The configuration for a replica's access to a sync server.
///
/// This enum is non-exhaustive, as users should only be constructing required
/// variants, not matching on it.
#[non_exhaustive]
pub enum ServerConfig {
    /// A local task database, for situations with a single replica.
    #[cfg(feature = "server-local")]
    Local {
        /// Path containing the server's DB
        server_dir: PathBuf,
    },
    /// A remote taskchampion-sync-server instance
    #[cfg(feature = "server-sync")]
    Remote {
        /// The base URL of the Sync server
        url: String,

        /// Client ID to identify and authenticate this replica to the server
        client_id: Uuid,

        /// Private encryption secret used to encrypt all data sent to the server.  This can
        /// be any suitably un-guessable string of bytes.
        encryption_secret: Vec<u8>,
    },
    /// A Google Cloud Platform storage bucket.
    #[cfg(feature = "server-gcp")]
    Gcp {
        /// Bucket in which to store the task data. This bucket must not be used for any other
        /// purpose.
        ///
        /// No special bucket configuration is reqiured.
        bucket: String,
        /// Path to a GCP credential file, in JSON format.
        ///
        /// If `None`, then [Application Default
        /// Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
        /// are used. Typically these are associated with the user's Google Cloud account.
        ///
        /// If `Some(path)`, then the path must be to a service account key. The service account
        /// must have a role with the following permissions:
        ///
        /// - storage.buckets.create
        /// - storage.buckets.get
        /// - storage.buckets.update
        /// - storage.objects.create
        /// - storage.objects.get
        /// - storage.objects.list
        /// - storage.objects.update
        /// - storage.objects.delete
        ///
        /// See the following GCP resources for more information:
        /// - <https://cloud.google.com/docs/authentication#service-accounts>
        /// - <https://cloud.google.com/iam/docs/keys-create-delete#creating>
        credential_path: Option<String>,
        /// Private encryption secret used to encrypt all data sent to the server.  This can
        /// be any suitably un-guessable string of bytes.
        encryption_secret: Vec<u8>,
    },
    /// An Amazon Web Services storage bucket.
    #[cfg(feature = "server-aws")]
    Aws {
        /// Region in which the bucket is located.
        region: Option<String>,
        /// Bucket in which to store the task data.
        ///
        /// This bucket must not be used for any other purpose. No special bucket configuration is
        /// required.
        bucket: String,
        /// Credential configuration for access to the bucket.
        credentials: AwsCredentials,
        /// Private encryption secret used to encrypt all data sent to the server.  This can
        /// be any suitably un-guessable string of bytes.
        encryption_secret: Vec<u8>,
        // endpoint_url is an optional URL to specify the hostname of an s3-compatible service.
        // When endpoint_url is used, region is ignored by the underlying S3 client.
        endpoint_url: Option<String>,
        // force_path_style is used to force the S3 client to use path-style URLs instead of
        // virtual-hosted-style (subdomain) URLs for the bucket.
        force_path_style: bool,
    },
}

impl ServerConfig {
    /// Get a server based on this configuration
    pub fn into_server(self) -> Result<Box<dyn Server>> {
        Ok(match self {
            #[cfg(feature = "server-local")]
            ServerConfig::Local { server_dir } => Box::new(LocalServer::new(server_dir)?),
            #[cfg(feature = "server-sync")]
            ServerConfig::Remote {
                url,
                client_id,
                encryption_secret,
            } => Box::new(SyncServer::new(url, client_id, encryption_secret)?),
            #[cfg(feature = "server-gcp")]
            ServerConfig::Gcp {
                bucket,
                credential_path,
                encryption_secret,
            } => Box::new(CloudServer::new(
                GcpService::new(bucket, credential_path)?,
                encryption_secret,
            )?),
            #[cfg(feature = "server-aws")]
            ServerConfig::Aws {
                region,
                bucket,
                credentials,
                encryption_secret,
                endpoint_url,
                force_path_style,
            } => Box::new(CloudServer::new(
                AwsService::new(region, bucket, credentials, endpoint_url, force_path_style)?,
                encryption_secret,
            )?),
        })
    }
}
