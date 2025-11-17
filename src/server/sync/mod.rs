use crate::errors::{Error, Result};
use crate::server::{
    AddVersionResult, GetVersionResult, HistorySegment, Server, Snapshot, SnapshotUrgency,
    VersionId,
};
use async_trait::async_trait;
use std::time::Duration;
use url::Url;
use uuid::Uuid;

use super::encryption::{Cryptor, Sealed, Secret, Unsealed};

#[cfg(not(any(feature = "tls-native-roots", feature = "tls-webpki-roots")))]
compile_error!(
    "Either feature \"tls-native-roots\" or \"tls-webpki-roots\" must be enabled for TLS support."
);

pub(crate) struct SyncServer {
    base_url: Url,
    client_id: Uuid,
    cryptor: Cryptor,
    agent: ureq::Agent,
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
            .map_err(|_| Error::Server(format!("Could not parse {} as a URL", url)))?;
        // Ensure the path has a trailing slash, so that `Url::join` correctly appends
        // additional path segments to it.
        let path = url.path();
        if !path.ends_with('/') {
            url.set_path(&format!("{}/", path));
        }
        Ok(SyncServer {
            base_url: url,
            client_id,
            cryptor: Cryptor::new(client_id, &Secret(encryption_secret.to_vec()))?,
            agent: ureq::AgentBuilder::new()
                .timeout_connect(Duration::from_secs(10))
                .timeout_read(Duration::from_secs(60))
                .build(),
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
fn get_uuid_header(resp: &ureq::Response, name: &str) -> Result<Uuid> {
    let value = resp
        .header(name)
        .ok_or_else(|| anyhow::anyhow!("Response does not have {} header", name))?;
    let value = Uuid::parse_str(value)
        .map_err(|e| anyhow::anyhow!("{} header is not a valid UUID: {}", name, e))?;
    Ok(value)
}

/// Read the X-Snapshot-Request header and return a SnapshotUrgency
fn get_snapshot_urgency(resp: &ureq::Response) -> SnapshotUrgency {
    match resp.header("X-Snapshot-Request") {
        None => SnapshotUrgency::None,
        Some(hdr) => match hdr {
            "urgency=low" => SnapshotUrgency::Low,
            "urgency=high" => SnapshotUrgency::High,
            _ => SnapshotUrgency::None,
        },
    }
}

fn sealed_from_resp(resp: ureq::Response, version_id: Uuid, content_type: &str) -> Result<Sealed> {
    use std::io::Read;
    if resp.header("Content-Type") == Some(content_type) {
        let mut reader = resp.into_reader();
        let mut payload = vec![];
        reader.read_to_end(&mut payload)?;
        Ok(Sealed {
            version_id,
            payload,
        })
    } else {
        Err(Error::Server(String::from(
            "Response did not have expected content-type",
        )))
    }
}

#[async_trait]
impl Server for SyncServer {
    async fn add_version(
        &mut self,
        parent_version_id: VersionId,
        history_segment: HistorySegment,
    ) -> Result<(AddVersionResult, SnapshotUrgency)> {
        let url = self.construct_endpoint_url(
            format!("v1/client/add-version/{}", parent_version_id).as_str(),
        )?;
        let unsealed = Unsealed {
            version_id: parent_version_id,
            payload: history_segment,
        };
        let sealed = self.cryptor.seal(unsealed)?;
        match self
            .agent
            .post(url.as_str())
            .set("Content-Type", HISTORY_SEGMENT_CONTENT_TYPE)
            .set("X-Client-Id", &self.client_id.to_string())
            .send_bytes(sealed.as_ref())
        {
            Ok(resp) => {
                let version_id = get_uuid_header(&resp, "X-Version-Id")?;
                Ok((
                    AddVersionResult::Ok(version_id),
                    get_snapshot_urgency(&resp),
                ))
            }
            Err(ureq::Error::Status(409, resp)) => {
                let parent_version_id = get_uuid_header(&resp, "X-Parent-Version-Id")?;
                Ok((
                    AddVersionResult::ExpectedParentVersion(parent_version_id),
                    SnapshotUrgency::None,
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
            format!("v1/client/get-child-version/{}", parent_version_id).as_str(),
        )?;

        match self
            .agent
            .get(url.as_str())
            .set("X-Client-Id", &self.client_id.to_string())
            .call()
        {
            Ok(resp) => {
                let parent_version_id = get_uuid_header(&resp, "X-Parent-Version-Id")?;
                let version_id = get_uuid_header(&resp, "X-Version-Id")?;
                let sealed =
                    sealed_from_resp(resp, parent_version_id, HISTORY_SEGMENT_CONTENT_TYPE)?;
                let history_segment = self.cryptor.unseal(sealed)?.payload;
                Ok(GetVersionResult::Version {
                    version_id,
                    parent_version_id,
                    history_segment,
                })
            }
            Err(ureq::Error::Status(404, _)) => Ok(GetVersionResult::NoSuchVersion),
            Err(err) => Err(err.into()),
        }
    }

    async fn add_snapshot(&mut self, version_id: VersionId, snapshot: Snapshot) -> Result<()> {
        let url =
            self.construct_endpoint_url(format!("v1/client/add-snapshot/{}", version_id).as_str())?;
        let unsealed = Unsealed {
            version_id,
            payload: snapshot,
        };
        let sealed = self.cryptor.seal(unsealed)?;
        Ok(self
            .agent
            .post(url.as_str())
            .set("Content-Type", SNAPSHOT_CONTENT_TYPE)
            .set("X-Client-Id", &self.client_id.to_string())
            .send_bytes(sealed.as_ref())
            .map(|_| ())?)
    }

    async fn get_snapshot(&mut self) -> Result<Option<(VersionId, Snapshot)>> {
        let url = self.construct_endpoint_url("v1/client/snapshot")?;
        match self
            .agent
            .get(url.as_str())
            .set("X-Client-Id", &self.client_id.to_string())
            .call()
        {
            Ok(resp) => {
                let version_id = get_uuid_header(&resp, "X-Version-Id")?;
                let sealed = sealed_from_resp(resp, version_id, SNAPSHOT_CONTENT_TYPE)?;
                let snapshot = self.cryptor.unseal(sealed)?.payload;
                Ok(Some((version_id, snapshot)))
            }
            Err(ureq::Error::Status(404, _)) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
}
