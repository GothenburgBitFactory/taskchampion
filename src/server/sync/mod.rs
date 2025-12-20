use crate::errors::{Error, Result};
use crate::server::{
    http, AddVersionResult, GetVersionResult, HistorySegment, Server, Snapshot, SnapshotUrgency,
    VersionId,
};
use async_trait::async_trait;
use reqwest::StatusCode;
use url::Url;
use uuid::Uuid;

use super::encryption::{Cryptor, Sealed, Secret, Unsealed};

pub(crate) struct SyncServer {
    base_url: Url,
    client_id: Uuid,
    cryptor: Cryptor,
    client: reqwest::Client,
}

/// The content-type for history segments (opaque blobs of bytes)
const HISTORY_SEGMENT_CONTENT_TYPE: &str = "application/vnd.taskchampion.history-segment";

/// The content-type for snapshots (opaque blobs of bytes)
const SNAPSHOT_CONTENT_TYPE: &str = "application/vnd.taskchampion.snapshot";

/// A SyncServer communicates with a sync server over HTTP.
impl SyncServer {
    /// Construct a new SyncServer.
    ///
    /// The `url` parameter represents the base URL of the sync server, encompassing the protocol, hostname, and optional path
    /// components where the server is hosted. When constructing URLs for server endpoints, the respective path components
    /// will be appended to this base URL.
    ///
    /// Pass a client_id to identify this client to the server.  Multiple replicas synchronizing the same task history
    /// should use the same client_id.
    pub(crate) fn new(
        url: String,
        client_id: Uuid,
        encryption_secret: Vec<u8>,
    ) -> Result<SyncServer> {
        let mut url = Url::parse(&url)
            .map_err(|_| Error::Server(format!("Could not parse {url} as a URL")))?;
        // Ensure the path has a trailing slash, so that `Url::join` correctly appends
        // additional path segments to it.
        let path = url.path();
        if !path.ends_with('/') {
            url.set_path(&format!("{path}/"));
        }
        Ok(SyncServer {
            base_url: url,
            client_id,
            cryptor: Cryptor::new(client_id, &Secret(encryption_secret.to_vec()))?,
            client: http::client()?,
        })
    }

    /// Construct a full endpoint URL by joining the base url with additional
    /// path components.
    fn construct_endpoint_url(&self, path_components: &str) -> Result<Url> {
        self.base_url.join(path_components).map_err(|_| {
            Error::Server(format!(
                "Could not build url from base {} and path component(s) {}",
                self.base_url, path_components
            ))
        })
    }
}

/// Read a UUID-bearing header or fail trying
fn get_uuid_header(resp: &reqwest::Response, name: &str) -> Result<Uuid> {
    let value = resp
        .headers()
        .get(name)
        .ok_or_else(|| anyhow::anyhow!("Response does not have {} header", name))?
        .to_str()
        .map_err(|_| anyhow::anyhow!("Response has invalid {} header", name))?;
    let value = Uuid::parse_str(value)
        .map_err(|e| anyhow::anyhow!("{} header is not a valid UUID: {}", name, e))?;
    Ok(value)
}

/// Read the X-Snapshot-Request header and return a SnapshotUrgency
fn get_snapshot_urgency(resp: &reqwest::Response) -> SnapshotUrgency {
    match resp.headers().get("X-Snapshot-Request") {
        None => SnapshotUrgency::None,
        Some(hdr) => match hdr.to_str() {
            Ok("urgency=low") => SnapshotUrgency::Low,
            Ok("urgency=high") => SnapshotUrgency::High,
            _ => SnapshotUrgency::None,
        },
    }
}

/// Get the content-type header.
fn get_content_type(resp: &reqwest::Response) -> Option<&str> {
    match resp.headers().get("Content-Type") {
        None => None,
        Some(hdr) => hdr.to_str().ok(),
    }
}

async fn sealed_from_resp(
    resp: reqwest::Response,
    version_id: Uuid,
    content_type: &str,
) -> Result<Sealed> {
    if get_content_type(&resp) == Some(content_type) {
        let payload = resp.bytes().await?;
        Ok(Sealed {
            version_id,
            payload: payload.to_vec(),
        })
    } else {
        Err(Error::Server(String::from(
            "Response did not have expected content-type",
        )))
    }
}

#[async_trait(?Send)]
impl Server for SyncServer {
    async fn add_version(
        &mut self,
        parent_version_id: VersionId,
        history_segment: HistorySegment,
    ) -> Result<(AddVersionResult, SnapshotUrgency)> {
        let url = self.construct_endpoint_url(
            format!("v1/client/add-version/{parent_version_id}").as_str(),
        )?;
        let unsealed = Unsealed {
            version_id: parent_version_id,
            payload: history_segment,
        };
        let sealed = self.cryptor.seal(unsealed)?;
        let resp = self
            .client
            .post(url)
            .header("Content-Type", HISTORY_SEGMENT_CONTENT_TYPE)
            .header("X-Client-Id", &self.client_id.to_string())
            .body(sealed.payload)
            .send()
            .await?;

        if resp.status() == StatusCode::CONFLICT {
            let parent_version_id = get_uuid_header(&resp, "X-Parent-Version-Id")?;
            return Ok((
                AddVersionResult::ExpectedParentVersion(parent_version_id),
                SnapshotUrgency::None,
            ));
        }

        match resp.error_for_status() {
            Ok(resp) => {
                let version_id = get_uuid_header(&resp, "X-Version-Id")?;
                Ok((
                    AddVersionResult::Ok(version_id),
                    get_snapshot_urgency(&resp),
                ))
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn get_child_version(
        &mut self,
        parent_version_id: VersionId,
    ) -> Result<GetVersionResult> {
        let url = self.construct_endpoint_url(
            format!("v1/client/get-child-version/{parent_version_id}").as_str(),
        )?;

        match self
            .client
            .get(url)
            .header("X-Client-Id", &self.client_id.to_string())
            .send()
            .await?
            .error_for_status()
        {
            Ok(resp) => {
                let parent_version_id = get_uuid_header(&resp, "X-Parent-Version-Id")?;
                let version_id = get_uuid_header(&resp, "X-Version-Id")?;
                let sealed =
                    sealed_from_resp(resp, parent_version_id, HISTORY_SEGMENT_CONTENT_TYPE).await?;
                let history_segment = self.cryptor.unseal(sealed)?.payload;
                Ok(GetVersionResult::Version {
                    version_id,
                    parent_version_id,
                    history_segment,
                })
            }
            Err(err) if err.status() == Some(StatusCode::NOT_FOUND) => {
                Ok(GetVersionResult::NoSuchVersion)
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn add_snapshot(&mut self, version_id: VersionId, snapshot: Snapshot) -> Result<()> {
        let url =
            self.construct_endpoint_url(format!("v1/client/add-snapshot/{version_id}").as_str())?;
        let unsealed = Unsealed {
            version_id,
            payload: snapshot,
        };
        let sealed = self.cryptor.seal(unsealed)?;
        Ok(self
            .client
            .post(url)
            .header("Content-Type", SNAPSHOT_CONTENT_TYPE)
            .header("X-Client-Id", &self.client_id.to_string())
            .body(sealed.payload)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map(|_| ())?)
    }

    async fn get_snapshot(&mut self) -> Result<Option<(VersionId, Snapshot)>> {
        let url = self.construct_endpoint_url("v1/client/snapshot")?;
        match self
            .client
            .get(url)
            .header("X-Client-Id", &self.client_id.to_string())
            .send()
            .await?
            .error_for_status()
        {
            Ok(resp) => {
                let version_id = get_uuid_header(&resp, "X-Version-Id")?;
                let sealed = sealed_from_resp(resp, version_id, SNAPSHOT_CONTENT_TYPE).await?;
                let snapshot = self.cryptor.unseal(sealed)?.payload;
                Ok(Some((version_id, snapshot)))
            }
            Err(err) if err.status() == Some(StatusCode::NOT_FOUND) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
}

// httptest is not available on WASM32, so do not build these tests
// on that platform (they wouldn't run anyway!).
#[cfg(all(not(target_arch = "wasm32"), test))]
mod test {
    use super::*;
    use crate::Server as ServerTrait;
    use httptest::{matchers::*, responders::*, Expectation, Server};
    use uuid::uuid;

    #[test]
    fn sync_server_url_construction() -> anyhow::Result<()> {
        let client_id = Uuid::new_v4();
        let encryption_secret = vec![];
        let bare_domain = SyncServer::new(
            "https://example.com".into(),
            client_id,
            encryption_secret.clone(),
        )?;
        let no_slash_path = SyncServer::new(
            "https://example.com/foo/bar".into(),
            client_id,
            encryption_secret.clone(),
        )?;
        let slash_path = SyncServer::new(
            "https://example.com/foo/bar/".into(),
            client_id,
            encryption_secret,
        )?;

        assert_eq!(
            bare_domain.construct_endpoint_url("v1/a/b")?,
            Url::parse("https://example.com/v1/a/b")?
        );
        assert_eq!(
            no_slash_path.construct_endpoint_url("v1/a/b")?,
            Url::parse("https://example.com/foo/bar/v1/a/b")?
        );
        assert_eq!(
            slash_path.construct_endpoint_url("v1/a/b")?,
            Url::parse("https://example.com/foo/bar/v1/a/b")?
        );
        Ok(())
    }

    const ENCRYPTION_SECRET: &[u8] = b"abc";
    const CLIENT_ID: Uuid = uuid!("ea82d570-3d7e-494a-a581-babe65dc7b3b");

    fn encrypt(version_id: Uuid, payload: impl Into<Vec<u8>>) -> anyhow::Result<Sealed> {
        let cryptor = Cryptor::new(CLIENT_ID, &Secret(ENCRYPTION_SECRET.to_vec()))?;
        let unsealed = Unsealed {
            version_id,
            payload: payload.into(),
        };
        Ok(cryptor.seal(unsealed)?)
    }

    #[tokio::test]
    async fn add_version() -> anyhow::Result<()> {
        let version_id = uuid!("e2ceb7df-706d-4a9d-ac26-09824e239092");
        let parent_version_id = uuid!("785fd86c-c11f-48b6-9557-27ec78bb568c");
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of!(
                request::method_path(
                    "POST",
                    format!("/v1/client/add-version/{parent_version_id}")
                ),
                request::headers(contains(("x-client-id", CLIENT_ID.to_string()))),
            ))
            .respond_with(
                status_code(200)
                    .append_header("x-version-id", version_id.to_string())
                    .append_header("x-parent-version-id", parent_version_id.to_string())
                    .append_header("content-type", HISTORY_SEGMENT_CONTENT_TYPE),
            ),
        );
        let mut svr = SyncServer::new(server.url("/").to_string(), CLIENT_ID, b"abc".to_vec())?;
        let res = svr
            .add_version(parent_version_id, b"abc".to_vec())
            .await
            .unwrap();
        assert_eq!(res.0, AddVersionResult::Ok(version_id));
        Ok(())
    }

    #[tokio::test]
    async fn add_version_conflict() -> anyhow::Result<()> {
        let parent_version_id = uuid!("785fd86c-c11f-48b6-9557-27ec78bb568c");
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of!(
                request::method_path(
                    "POST",
                    format!("/v1/client/add-version/{parent_version_id}")
                ),
                request::headers(contains(("x-client-id", CLIENT_ID.to_string()))),
            ))
            .respond_with(
                status_code(409)
                    .append_header("x-parent-version-id", parent_version_id.to_string()),
            ),
        );
        let mut svr = SyncServer::new(server.url("/").to_string(), CLIENT_ID, b"abc".to_vec())?;
        let res = svr
            .add_version(parent_version_id, b"abc".to_vec())
            .await
            .unwrap();
        assert_eq!(
            res.0,
            AddVersionResult::ExpectedParentVersion(parent_version_id)
        );
        Ok(())
    }

    #[tokio::test]
    async fn add_version_error() -> anyhow::Result<()> {
        let parent_version_id = uuid!("785fd86c-c11f-48b6-9557-27ec78bb568c");
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of!(
                request::method_path(
                    "POST",
                    format!("/v1/client/add-version/{parent_version_id}")
                ),
                request::headers(contains(("x-client-id", CLIENT_ID.to_string()))),
            ))
            .respond_with(status_code(404)),
        );
        let mut svr = SyncServer::new(server.url("/").to_string(), CLIENT_ID, b"abc".to_vec())?;
        assert!(svr
            .add_version(parent_version_id, b"abc".to_vec())
            .await
            .is_err());
        Ok(())
    }

    #[tokio::test]
    async fn get_child_version() -> anyhow::Result<()> {
        let version_id = uuid!("e2ceb7df-706d-4a9d-ac26-09824e239092");
        let parent_version_id = uuid!("785fd86c-c11f-48b6-9557-27ec78bb568c");
        let server = Server::run();
        let sealed = encrypt(parent_version_id, b"abc")?;
        server.expect(
            Expectation::matching(all_of!(
                request::method_path(
                    "GET",
                    format!("/v1/client/get-child-version/{parent_version_id}")
                ),
                request::headers(contains(("x-client-id", CLIENT_ID.to_string()))),
            ))
            .respond_with(
                status_code(200)
                    .body(sealed.payload)
                    .append_header("x-version-id", version_id.to_string())
                    .append_header("x-parent-version-id", parent_version_id.to_string())
                    .append_header("content-type", HISTORY_SEGMENT_CONTENT_TYPE),
            ),
        );
        let mut svr = SyncServer::new(server.url("/").to_string(), CLIENT_ID, b"abc".to_vec())?;
        let res = svr.get_child_version(parent_version_id).await.unwrap();
        assert_eq!(
            res,
            GetVersionResult::Version {
                version_id,
                parent_version_id,
                history_segment: b"abc".to_vec()
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn get_child_version_not_found() -> anyhow::Result<()> {
        let version_id = uuid!("e2ceb7df-706d-4a9d-ac26-09824e239092");
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of!(
                request::method_path("GET", format!("/v1/client/get-child-version/{version_id}")),
                request::headers(contains(("x-client-id", CLIENT_ID.to_string()))),
            ))
            .respond_with(status_code(404)),
        );
        let mut svr = SyncServer::new(server.url("/").to_string(), CLIENT_ID, b"abc".to_vec())?;
        assert_eq!(
            svr.get_child_version(version_id).await?,
            GetVersionResult::NoSuchVersion
        );
        Ok(())
    }

    #[tokio::test]
    async fn get_child_version_error() -> anyhow::Result<()> {
        let version_id = uuid!("e2ceb7df-706d-4a9d-ac26-09824e239092");
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of!(
                request::method_path("GET", format!("/v1/client/get-child-version/{version_id}")),
                request::headers(contains(("x-client-id", CLIENT_ID.to_string()))),
            ))
            .respond_with(status_code(502)),
        );
        let mut svr = SyncServer::new(server.url("/").to_string(), CLIENT_ID, b"abc".to_vec())?;
        assert!(svr.get_child_version(version_id).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn add_snapshot() -> anyhow::Result<()> {
        let version_id = uuid!("e2ceb7df-706d-4a9d-ac26-09824e239092");
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of!(
                request::method_path("POST", format!("/v1/client/add-snapshot/{version_id}")),
                request::headers(contains(("x-client-id", CLIENT_ID.to_string()))),
                request::headers(contains(("content-type", SNAPSHOT_CONTENT_TYPE))),
                // The encrypted body is different every time, so not tested
            ))
            .respond_with(status_code(200)),
        );
        let mut svr = SyncServer::new(server.url("/").to_string(), CLIENT_ID, b"abc".to_vec())?;
        svr.add_snapshot(version_id, b"abc".to_vec()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn add_snapshot_error() -> anyhow::Result<()> {
        let version_id = uuid!("e2ceb7df-706d-4a9d-ac26-09824e239092");
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of!(
                request::method_path("POST", format!("/v1/client/add-snapshot/{version_id}")),
                request::headers(contains(("x-client-id", CLIENT_ID.to_string()))),
                request::headers(contains(("content-type", SNAPSHOT_CONTENT_TYPE))),
                // The encrypted body is different every time, so not tested
            ))
            .respond_with(status_code(500)),
        );
        let mut svr = SyncServer::new(server.url("/").to_string(), CLIENT_ID, b"abc".to_vec())?;
        assert!(svr.add_snapshot(version_id, b"abc".to_vec()).await.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn get_snapshot_found() -> anyhow::Result<()> {
        let version_id = Uuid::new_v4();
        let sealed = encrypt(version_id, b"abc")?;
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of!(
                request::method_path("GET", "/v1/client/snapshot"),
                request::headers(contains(("x-client-id", CLIENT_ID.to_string()))),
            ))
            .respond_with(
                status_code(200)
                    .body(sealed.payload)
                    .append_header("x-version-id", version_id.to_string())
                    .append_header("content-type", SNAPSHOT_CONTENT_TYPE),
            ),
        );
        let mut svr = SyncServer::new(server.url("/").to_string(), CLIENT_ID, b"abc".to_vec())?;
        assert_eq!(
            svr.get_snapshot().await?,
            Some((version_id, b"abc".to_vec()))
        );
        Ok(())
    }

    #[tokio::test]
    async fn get_snapshot_not_found() -> anyhow::Result<()> {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of!(
                request::method_path("GET", "/v1/client/snapshot"),
                request::headers(contains(("x-client-id", CLIENT_ID.to_string()))),
            ))
            .respond_with(status_code(404)),
        );
        let mut svr = SyncServer::new(server.url("/").to_string(), CLIENT_ID, b"abc".to_vec())?;
        assert_eq!(svr.get_snapshot().await?, None);
        Ok(())
    }

    #[tokio::test]
    async fn get_snapshot_error() -> anyhow::Result<()> {
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of!(
                request::method_path("GET", "/v1/client/snapshot"),
                request::headers(contains(("x-client-id", CLIENT_ID.to_string()))),
            ))
            .respond_with(status_code(403)),
        );
        let mut svr = SyncServer::new(server.url("/").to_string(), CLIENT_ID, b"abc".to_vec())?;
        assert!(svr.get_snapshot().await.is_err());
        Ok(())
    }
}
