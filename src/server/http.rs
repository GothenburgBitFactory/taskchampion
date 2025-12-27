//! Common support for HTTP client
//!
//! This contains some utilities to make using `reqwest` easier, including getting
//! the correct TLS certificate store.

use crate::errors::Result;

#[cfg(all(
    not(target_arch = "wasm32"),
    not(any(feature = "tls-native-roots", feature = "tls-webpki-roots"))
))]
compile_error!(
    "Either feature \"tls-native-roots\" or \"tls-webpki-roots\" must be enabled for HTTP client support."
);

static USER_AGENT: &str = concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION"),);

/// Create a new [`reqwest::Client`] with configuration appropriate to this library.
#[cfg(not(target_arch = "wasm32"))]
pub(super) fn client() -> Result<reqwest::Client> {
    use std::time::Duration;
    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .user_agent(USER_AGENT)
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(60));

    // Select native or webpki certs depending on features
    let client = client.tls_built_in_root_certs(false);
    #[cfg(feature = "tls-native-roots")]
    let client = client.tls_built_in_native_certs(true);
    #[cfg(all(feature = "tls-webpki-roots", not(feature = "tls-native-roots")))]
    let client = client.tls_built_in_webpki_certs(true);

    Ok(client.build()?)
}

/// Create a new [`reqwest::Client`] with configuration appropriate to this library.
///
/// On WASM, this uses the Fetch API.
#[cfg(target_arch = "wasm32")]
pub(super) fn client() -> Result<reqwest::Client> {
    let client = reqwest::Client::builder().user_agent(USER_AGENT);

    // Timeouts and TLS cannot be configured via the Fetch API.

    Ok(client.build()?)
}
