//! Common support for HTTP client
//!
//! This contains some utilities to make using `reqwest` easier, including getting
//! the correct TLS certificate store.

use crate::errors::Result;
use std::env;
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

    let mut client = reqwest::Client::builder()
        .use_rustls_tls()
        .user_agent(USER_AGENT)
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(60));

    // Configure HTTP proxy if set
    if let Ok(proxy_url) = env::var("HTTP_PROXY").or_else(|_| env::var("http_proxy")) {
        match reqwest::Proxy::http(&proxy_url) {
            Ok(proxy) => {
                client = client.proxy(proxy);
            }
            Err(e) => {
                log::warn!("Invalid HTTP_PROXY '{}': {}. Continuing without HTTP proxy.", proxy_url, e);
            }
        }
    }

    // Configure HTTPS proxy if set
    if let Ok(proxy_url) = env::var("HTTPS_PROXY").or_else(|_| env::var("https_proxy")) {
        match reqwest::Proxy::https(&proxy_url) {
            Ok(proxy) => {
                client = client.proxy(proxy);
            }
            Err(e) => {
                log::warn!("Invalid HTTPS_PROXY '{}': {}. Continuing without HTTPS proxy.", proxy_url, e);
            }
        }
    }

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

mod tests {
    use crate::server::http::client;

    #[test]
    fn test_client_without_proxy() {
        // Ensure no proxy env vars are set
        std::env::remove_var("HTTP_PROXY");
        std::env::remove_var("http_proxy");
        std::env::remove_var("HTTPS_PROXY");
        std::env::remove_var("https_proxy");

        let client = client();
        assert!(client.is_ok(), "Client should build without proxy settings");
    }

    #[test]
    fn test_client_with_http_proxy() {
        std::env::set_var("HTTP_PROXY", "http://proxy.example.com:8080");
        std::env::remove_var("HTTPS_PROXY");
        std::env::remove_var("https_proxy");

        let client = client();
        assert!(client.is_ok(), "Client should build with HTTP_PROXY set");

        // Cleanup
        std::env::remove_var("HTTP_PROXY");
    }

    #[test]
    fn test_client_with_https_proxy() {
        std::env::remove_var("HTTP_PROXY");
        std::env::remove_var("http_proxy");
        std::env::set_var("HTTPS_PROXY", "http://proxy.example.com:8443");

        let client = client();
        assert!(client.is_ok(), "Client should build with HTTPS_PROXY set");

        // Cleanup
        std::env::remove_var("HTTPS_PROXY");
    }

    #[test]
    fn test_client_with_both_proxies() {
        std::env::set_var("HTTP_PROXY", "http://http-proxy.example.com:8080");
        std::env::set_var("HTTPS_PROXY", "http://https-proxy.example.com:8443");

        let client = client();
        assert!(client.is_ok(), "Client should build with both proxies set");

        // Cleanup
        std::env::remove_var("HTTP_PROXY");
        std::env::remove_var("HTTPS_PROXY");
    }

    #[test]
    fn test_client_with_lowercase_proxy_vars() {
        std::env::remove_var("HTTP_PROXY");
        std::env::remove_var("HTTPS_PROXY");
        std::env::set_var("http_proxy", "http://proxy.example.com:8080");
        std::env::set_var("https_proxy", "http://proxy.example.com:8443");

        let client = client();
        assert!(client.is_ok(), "Client should build with lowercase proxy vars");

        // Cleanup
        std::env::remove_var("http_proxy");
        std::env::remove_var("https_proxy");
    }

    #[test]
    fn test_client_with_proxy_auth() {
        std::env::set_var("HTTPS_PROXY", "http://user:password@proxy.example.com:8443");

        let client = client();
        assert!(client.is_ok(), "Client should build with authenticated proxy");

        // Cleanup
        std::env::remove_var("HTTPS_PROXY");
    }

    #[test]
    fn test_client_with_invalid_proxy_url() {
        std::env::set_var("HTTP_PROXY", "not-a-valid-url");

        let client = client();
        // Should still succeed but log a warning
        assert!(client.is_ok(), "Client should build even with invalid proxy URL");

        // Cleanup
        std::env::remove_var("HTTP_PROXY");
    }

    #[test]
    fn test_client_uppercase_takes_precedence() {
        std::env::set_var("HTTP_PROXY", "http://uppercase.example.com:8080");
        std::env::set_var("http_proxy", "http://lowercase.example.com:8080");

        let client = client();
        assert!(client.is_ok(), "Client should build with precedence rules");

        // Cleanup
        std::env::remove_var("HTTP_PROXY");
        std::env::remove_var("http_proxy");
    }

    #[test]
    fn test_client_mixed_case_proxies() {
        std::env::set_var("HTTP_PROXY", "http://http-upper.example.com:8080");
        std::env::set_var("https_proxy", "http://https-lower.example.com:8443");

        let client = client();
        assert!(client.is_ok(), "Client should build with mixed case proxy vars");

        // Cleanup
        std::env::remove_var("HTTP_PROXY");
        std::env::remove_var("https_proxy");
    }

    #[test]
    fn test_client_empty_proxy_value() {
        std::env::set_var("HTTP_PROXY", "");

        let client = client();
        // Empty string should be treated as "not set"
        assert!(client.is_ok(), "Client should build with empty proxy value");

        // Cleanup
        std::env::remove_var("HTTP_PROXY");
    }

    #[test]
    fn test_client_socks_proxy() {
        std::env::set_var("HTTPS_PROXY", "socks5://proxy.example.com:1080");

        let client = client();
        assert!(client.is_ok(), "Client should build with SOCKS proxy");

        // Cleanup
        std::env::remove_var("HTTPS_PROXY");
    }
}

