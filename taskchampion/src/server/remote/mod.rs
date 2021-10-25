use crate::server::{
    AddVersionResult, GetVersionResult, HistorySegment, Server, Snapshot, SnapshotUrgency,
    VersionId,
};
use std::time::Duration;
use uuid::Uuid;

mod crypto;
use crypto::{Ciphertext, Cleartext, Secret};

pub struct RemoteServer {
    origin: String,
    client_key: Uuid,
    encryption_secret: Secret,
    agent: ureq::Agent,
}

/// The content-type for history segments (opaque blobs of bytes)
const HISTORY_SEGMENT_CONTENT_TYPE: &str = "application/vnd.taskchampion.history-segment";

/// The content-type for snapshots (opaque blobs of bytes)
const SNAPSHOT_CONTENT_TYPE: &str = "application/vnd.taskchampion.snapshot";

/// A RemoeServer communicates with a remote server over HTTP (such as with
/// taskchampion-sync-server).
impl RemoteServer {
    /// Construct a new RemoteServer.  The `origin` is the sync server's protocol and hostname
    /// without a trailing slash, such as `https://tcsync.example.com`.  Pass a client_key to
    /// identify this client to the server.  Multiple replicas synchronizing the same task history
    /// should use the same client_key.
    pub fn new(origin: String, client_key: Uuid, encryption_secret: Vec<u8>) -> RemoteServer {
        RemoteServer {
            origin,
            client_key,
            encryption_secret: encryption_secret.into(),
            agent: ureq::AgentBuilder::new()
                .timeout_connect(Duration::from_secs(10))
                .timeout_read(Duration::from_secs(60))
                .build(),
        }
    }
}

/// Read a UUID-bearing header or fail trying
fn get_uuid_header(resp: &ureq::Response, name: &str) -> eyre::Result<Uuid> {
    let value = resp
        .header(name)
        .ok_or_else(|| eyre::eyre!("Response does not have {} header", name))?;
    let value = Uuid::parse_str(value)
        .map_err(|e| eyre::eyre!("{} header is not a valid UUID: {}", name, e))?;
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

impl Server for RemoteServer {
    fn add_version(
        &mut self,
        parent_version_id: VersionId,
        history_segment: HistorySegment,
    ) -> eyre::Result<(AddVersionResult, SnapshotUrgency)> {
        let url = format!(
            "{}/v1/client/add-version/{}",
            self.origin, parent_version_id
        );
        let cleartext = Cleartext {
            version_id: parent_version_id,
            payload: history_segment,
        };
        let ciphertext = cleartext.seal(&self.encryption_secret)?;
        match self
            .agent
            .post(&url)
            .set("Content-Type", HISTORY_SEGMENT_CONTENT_TYPE)
            .set("X-Client-Key", &self.client_key.to_string())
            .send_bytes(ciphertext.as_ref())
        {
            Ok(resp) => {
                let version_id = get_uuid_header(&resp, "X-Version-Id")?;
                Ok((
                    AddVersionResult::Ok(version_id),
                    get_snapshot_urgency(&resp),
                ))
            }
            Err(ureq::Error::Status(status, resp)) if status == 409 => {
                let parent_version_id = get_uuid_header(&resp, "X-Parent-Version-Id")?;
                Ok((
                    AddVersionResult::ExpectedParentVersion(parent_version_id),
                    SnapshotUrgency::None,
                ))
            }
            Err(err) => Err(err.into()),
        }
    }

    fn get_child_version(
        &mut self,
        parent_version_id: VersionId,
    ) -> eyre::Result<GetVersionResult> {
        let url = format!(
            "{}/v1/client/get-child-version/{}",
            self.origin, parent_version_id
        );
        match self
            .agent
            .get(&url)
            .set("X-Client-Key", &self.client_key.to_string())
            .call()
        {
            Ok(resp) => {
                let parent_version_id = get_uuid_header(&resp, "X-Parent-Version-Id")?;
                let version_id = get_uuid_header(&resp, "X-Version-Id")?;
                let ciphertext = Ciphertext::from_resp(resp, HISTORY_SEGMENT_CONTENT_TYPE)?;
                let history_segment = ciphertext
                    .open(&self.encryption_secret, parent_version_id)?
                    .payload;
                Ok(GetVersionResult::Version {
                    version_id,
                    parent_version_id,
                    history_segment,
                })
            }
            Err(ureq::Error::Status(status, _)) if status == 404 => {
                Ok(GetVersionResult::NoSuchVersion)
            }
            Err(err) => Err(err.into()),
        }
    }

    fn add_snapshot(&mut self, version_id: VersionId, snapshot: Snapshot) -> eyre::Result<()> {
        let url = format!("{}/v1/client/add-snapshot/{}", self.origin, version_id);
        let cleartext = Cleartext {
            version_id,
            payload: snapshot,
        };
        let ciphertext = cleartext.seal(&self.encryption_secret)?;
        Ok(self
            .agent
            .post(&url)
            .set("Content-Type", SNAPSHOT_CONTENT_TYPE)
            .set("X-Client-Key", &self.client_key.to_string())
            .send_bytes(ciphertext.as_ref())
            .map(|_| ())?)
    }

    fn get_snapshot(&mut self) -> eyre::Result<Option<(VersionId, Snapshot)>> {
        let url = format!("{}/v1/client/snapshot", self.origin);
        match self
            .agent
            .get(&url)
            .set("X-Client-Key", &self.client_key.to_string())
            .call()
        {
            Ok(resp) => {
                let version_id = get_uuid_header(&resp, "X-Version-Id")?;
                let ciphertext = Ciphertext::from_resp(resp, SNAPSHOT_CONTENT_TYPE)?;
                let snapshot = ciphertext
                    .open(&self.encryption_secret, version_id)?
                    .payload;
                Ok(Some((version_id, snapshot)))
            }
            Err(ureq::Error::Status(status, _)) if status == 404 => Ok(None),
            Err(err) => Err(err.into()),
        }
    }
}
