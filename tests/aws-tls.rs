#![cfg(all(feature = "server-aws", target_os = "linux"))]
use taskchampion::{server::AwsCredentials, ServerConfig};

mod tls_utils;

#[tokio::test]
/// Check that the AWS server implementation correctly uses, or does not use,
/// tls-native-roots.
async fn aws_tls() -> anyhow::Result<()> {
    tls_utils::reset_seen_ssl_file();

    // This will fail getting the salt due to bad credentials, but after making a TLS connection,
    // which is what this test requires.
    let _ = ServerConfig::Aws {
        region: Some("us-east-2".into()),
        bucket: "gotheneburgbitfactory".into(),
        credentials: AwsCredentials::AccessKey {
            access_key_id: "not".into(),
            secret_access_key: "valid".into(),
        },
        endpoint_url: None,
        force_path_style: false,
        encryption_secret: b"abc".into(),
    }
    .into_server()
    .await;

    tls_utils::assert_expected_seen_ssl_file();

    Ok(())
}
