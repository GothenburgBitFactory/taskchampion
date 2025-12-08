use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
/// Errors returned from taskchampion operations
pub enum Error {
    /// A server-related error
    #[error("Server Error: {0}")]
    Server(String),
    /// A task-database-related error
    #[error("Task Database Error: {0}")]
    Database(String),
    /// An error specifically indicating that the local replica cannot
    /// be synchronized with the sever, due to being out of date or some
    /// other irrecoverable error.
    #[error("Local replica is out of sync with the server")]
    OutOfSync,
    /// A usage error
    #[error("Usage Error: {0}")]
    Usage(String),
    /// A general error.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Convert private and third party errors into Error::Other.
macro_rules! other_error {
    ( $error:ty ) => {
        impl From<$error> for Error {
            fn from(err: $error) -> Self {
                Self::Other(err.into())
            }
        }
    };
}
other_error!(io::Error);
other_error!(serde_json::Error);
other_error!(tokio::sync::oneshot::error::RecvError);

#[cfg(feature = "storage-sqlite")]
other_error!(rusqlite::Error);
#[cfg(feature = "storage-sqlite")]
other_error!(crate::storage::sqlite::SqliteError);
#[cfg(feature = "server-gcp")]
other_error!(google_cloud_storage::http::Error);
#[cfg(feature = "server-gcp")]
other_error!(google_cloud_storage::client::google_cloud_auth::error::Error);
#[cfg(feature = "server-aws")]
other_error!(aws_sdk_s3::Error);
#[cfg(feature = "server-aws")]
other_error!(aws_sdk_s3::primitives::ByteStreamError);

impl<T: Sync + Send + 'static> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    fn from(err: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::Other(err.into())
    }
}

/// Convert reqwest errors more carefully
#[cfg(feature = "http")]
impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        if let Some(status_code) = err.status() {
            let msg = format!(
                "{} responded with {} {}",
                err.url().map(|u| u.as_str()).unwrap_or("unknown"),
                status_code.as_u16(),
                status_code.canonical_reason().unwrap_or("unknown"),
            );
            return Self::Server(msg);
        }
        Self::Server(err.to_string())
    }
}

pub(crate) type Result<T> = std::result::Result<T, Error>;
