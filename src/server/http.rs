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

    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .user_agent(USER_AGENT)
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(60));

    // configure client proxy
    #[cfg(not(target_arch = "wasm32"))]
    let client = configure_proxy(client);
    // Select native or webpki certs depending on features
    let client = client.tls_built_in_root_certs(false);
    #[cfg(feature = "tls-native-roots")]
    let client = client.tls_built_in_native_certs(true);
    #[cfg(all(feature = "tls-webpki-roots", not(feature = "tls-native-roots")))]
    let client = client.tls_built_in_webpki_certs(true);

    Ok(client.build()?)
}
#[cfg(not(target_arch = "wasm32"))]
fn configure_proxy(mut client: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
    // Configure HTTP proxy if set
    if let Ok(proxy_url) = env::var("HTTP_PROXY").or_else(|_| env::var("http_proxy")) {
        match reqwest::Proxy::http(&proxy_url) {
            Ok(proxy) => {
                client = client.proxy(proxy);
            }
            Err(e) => {
                log::warn!("Invalid HTTP_PROXY '{proxy_url}': {e}. Continuing without HTTP proxy.");
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
                log::warn!(
                    "Invalid HTTPS_PROXY '{proxy_url}': {e}. Continuing without HTTPS proxy."
                );
            }
        }
    }
    client
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

#[cfg(test)]
mod tests {
    use super::client;

    #[test]
    fn test_client_proxy_configurations() {
        // Helper function to test a scenario and cleanup
        let test_scenario = |setup: fn(), description: &str| {
            setup();
            let client = client();
            assert!(client.is_ok(), "{}", description);
            // Cleanup all possible env vars
            std::env::remove_var("HTTP_PROXY");
            std::env::remove_var("http_proxy");
            std::env::remove_var("HTTPS_PROXY");
            std::env::remove_var("https_proxy");
        };

        // Test 1: No proxy
        test_scenario(
            || {
                std::env::remove_var("HTTP_PROXY");
                std::env::remove_var("http_proxy");
                std::env::remove_var("HTTPS_PROXY");
                std::env::remove_var("https_proxy");
            },
            "Client should build without proxy settings",
        );

        // Test 2: HTTP proxy
        test_scenario(
            || {
                std::env::set_var("HTTP_PROXY", "http://proxy.example.com:8080");
            },
            "Client should build with HTTP_PROXY set",
        );

        // Test 3: HTTPS proxy
        test_scenario(
            || {
                std::env::set_var("HTTPS_PROXY", "http://proxy.example.com:8443");
            },
            "Client should build with HTTPS_PROXY set",
        );

        // Test 4: Both proxies
        test_scenario(
            || {
                std::env::set_var("HTTP_PROXY", "http://http-proxy.example.com:8080");
                std::env::set_var("HTTPS_PROXY", "http://https-proxy.example.com:8443");
            },
            "Client should build with both proxies set",
        );

        // Test 5: Lowercase proxy vars
        test_scenario(
            || {
                std::env::set_var("http_proxy", "http://proxy.example.com:8080");
                std::env::set_var("https_proxy", "http://proxy.example.com:8443");
            },
            "Client should build with lowercase proxy vars",
        );

        // Test 6: Proxy with authentication
        test_scenario(
            || {
                std::env::set_var("HTTPS_PROXY", "http://user:password@proxy.example.com:8443");
            },
            "Client should build with authenticated proxy",
        );

        // Test 7: Invalid proxy URL
        test_scenario(
            || {
                std::env::set_var("HTTP_PROXY", "not-a-valid-url");
            },
            "Client should build even with invalid proxy URL",
        );

        // Test 8: Uppercase takes precedence
        test_scenario(
            || {
                std::env::set_var("HTTP_PROXY", "http://uppercase.example.com:8080");
                std::env::set_var("http_proxy", "http://lowercase.example.com:8080");
            },
            "Client should build with precedence rules",
        );

        // Test 9: Mixed case proxies
        test_scenario(
            || {
                std::env::set_var("HTTP_PROXY", "http://http-upper.example.com:8080");
                std::env::set_var("https_proxy", "http://https-lower.example.com:8443");
            },
            "Client should build with mixed case proxy vars",
        );

        // Test 10: Empty proxy value
        test_scenario(
            || {
                std::env::set_var("HTTP_PROXY", "");
            },
            "Client should build with empty proxy value",
        );

        // Test 11: SOCKS proxy
        test_scenario(
            || {
                std::env::set_var("HTTPS_PROXY", "socks5://proxy.example.com:1080");
            },
            "Client should build with SOCKS proxy",
        );
    }
}
