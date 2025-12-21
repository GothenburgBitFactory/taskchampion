#![cfg(all(feature = "server-sync", target_os = "linux"))]
use taskchampion::ServerConfig;
use uuid::Uuid;

mod tls_utils;

#[tokio::test]
/// Check that the sync server implementation correctly uses, or does not use,
/// tls-native-roots.
async fn sync_server_tls() -> anyhow::Result<()> {
    tls_utils::reset_seen_ssl_file();

    let mut server = ServerConfig::Remote {
        url: "https://gothenburgbitfactory.org/".to_string(),
        client_id: Uuid::new_v4(),
        encryption_secret: b"abc".into(),
    }
    .into_server()
    .await?;
    // This will return a 404, which is not a `Result::Err`.
    server.get_child_version(Uuid::new_v4()).await?;

    tls_utils::assert_expected_seen_ssl_file();

    Ok(())
}
