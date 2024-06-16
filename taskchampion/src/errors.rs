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
other_error!(rusqlite::Error);
other_error!(crate::storage::sqlite::SqliteError);
#[cfg(feature = "server-gcp")]
other_error!(google_cloud_storage::http::Error);
#[cfg(feature = "server-gcp")]
other_error!(google_cloud_storage::client::google_cloud_auth::error::Error);

/// Convert ureq errors more carefully
#[cfg(feature = "server-sync")]
impl From<ureq::Error> for Error {
    fn from(ureq_err: ureq::Error) -> Self {
        match ureq_err {
            ureq::Error::Status(status, response) => {
                let msg = format!("{} responded with {} {}", response.get_url(), status, response.status_text());
                Self::Server(msg.into())
            }
            ureq::Error::Transport(_) => Self::Server(ureq_err.to_string()),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn ureq_error_status() {
        let err = ureq::Error::Status(418, ureq::Response::new(418, "I Am a Teapot", "uhoh").unwrap());
        assert_eq!(Error::from(err).to_string(), "Server Error: https://example.com/ responded with 418 I Am a Teapot");
    }
}
