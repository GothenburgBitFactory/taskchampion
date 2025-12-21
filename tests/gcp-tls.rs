#![cfg(all(feature = "server-gcp", target_os = "linux"))]
use std::{
    fs::{metadata, set_permissions, File},
    io::Write,
    os::unix::fs::PermissionsExt,
};

use taskchampion::ServerConfig;
use tempfile::TempDir;

mod tls_utils;

#[tokio::test]
/// Check that the GCP server implementation correctly uses, or does not use,
/// tls-native-roots.
async fn gcp_tls() -> anyhow::Result<()> {
    // In order to attempt a connection, GCP requires a file containing what appears to be valid
    // credentials.
    let tmp_dir = TempDir::new()?;
    let creds_path = tmp_dir.path().join("creds.json");
    File::create_new(&creds_path)
        .expect("Could not open temp creds file")
        .write_all(
            br#"{
  "client_id": "765432109876-abcdefghijklmnopqrstuvwxyz.apps.googleusercontent.com",
  "client_secret": "aBcDeFgHiJkLmNoPqRsTuVwXyZ",
  "refresh_token": "1/abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz",
  "type": "authorized_user"
}"#,
        )
        .expect("Could not write temp creds file");

    // File::create_new creates files with permissions 0o000, which the GCP client
    // will not be able to read.
    let metadata = metadata(&creds_path)?;
    let mut permissions = metadata.permissions();
    permissions.set_mode(0o777);
    set_permissions(&creds_path, permissions)?;

    tls_utils::reset_seen_ssl_file();

    // This will fail getting the salt due to bad credentials, but after making a TLS connection,
    // which is what this test requires.
    let _ = ServerConfig::Gcp {
        bucket: "no-bucket".into(),
        credential_path: Some(creds_path.to_str().unwrap().into()),
        encryption_secret: b"abc".into(),
    }
    .into_server()
    .await;

    tls_utils::assert_expected_seen_ssl_file();

    Ok(())
}
