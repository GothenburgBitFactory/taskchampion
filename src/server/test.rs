use crate::errors::Result;
use crate::server::{
    AddVersionResult, GetVersionResult, HistorySegment, Server, Snapshot, SnapshotUrgency,
    VersionId, NIL_VERSION_ID,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use uuid::Uuid;

struct Version {
    version_id: VersionId,
    parent_version_id: VersionId,
    history_segment: HistorySegment,
}

/// TestServer implements the Server trait with a test implementation.
pub(crate) struct TestServer {
    latest_version_id: VersionId,
    // NOTE: indexed by parent_version_id!
    versions: HashMap<VersionId, Version>,
    snapshot_urgency: SnapshotUrgency,
    snapshot: Option<(VersionId, Snapshot)>,
}

impl TestServer {
    /// A test server has no notion of clients, signatures, encryption, etc.
    pub(crate) fn new() -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            latest_version_id: NIL_VERSION_ID,
            versions: HashMap::new(),
            snapshot_urgency: SnapshotUrgency::None,
            snapshot: None,
        }))
    }
    // feel free to add any test utility functions here

    // Get a boxed Server implementation referring to this TestServer
    // pub(crate) fn server(&self) -> Arc<Mutex<dyn Server + Send>> {
    //     Arc::new(Mutex::new(self.clone()))
    // }

    pub(crate) fn set_snapshot_urgency(&mut self, urgency: SnapshotUrgency) {
        self.snapshot_urgency = urgency;
    }

    /// Get the latest snapshot added to this server
    pub(crate) fn snapshot(&self) -> Option<(VersionId, Snapshot)> {
        self.snapshot.as_ref().cloned()
    }

    /// Delete a version from storage
    pub(crate) fn delete_version(&mut self, parent_version_id: VersionId) {
        self.versions.remove(&parent_version_id);
    }

    pub(crate) fn versions_len(&self) -> usize {
        self.versions.len()
    }
}

impl Server for TestServer {
    /// Add a new version.  If the given version number is incorrect, this responds with the
    /// appropriate version and expects the caller to try again.
    fn add_version(
        &mut self,
        parent_version_id: VersionId,
        history_segment: HistorySegment,
    ) -> Result<(AddVersionResult, SnapshotUrgency)> {
        // no client lookup
        // no signature validation

        // check the parent_version_id for linearity
        if self.latest_version_id != NIL_VERSION_ID && parent_version_id != self.latest_version_id {
            return Ok((
                AddVersionResult::ExpectedParentVersion(self.latest_version_id),
                SnapshotUrgency::None,
            ));
        }

        // invent a new ID for this version
        let version_id = Uuid::new_v4();

        self.versions.insert(
            parent_version_id,
            Version {
                version_id,
                parent_version_id,
                history_segment,
            },
        );
        self.latest_version_id = version_id;

        // reply with the configured urgency and reset it to None
        let urgency = self.snapshot_urgency;
        self.snapshot_urgency = SnapshotUrgency::None;
        Ok((AddVersionResult::Ok(version_id), urgency))
    }

    /// Get a vector of all versions after `since_version`
    fn get_child_version(&mut self, parent_version_id: VersionId) -> Result<GetVersionResult> {
        if let Some(version) = self.versions.get(&parent_version_id) {
            Ok(GetVersionResult::Version {
                version_id: version.version_id,
                parent_version_id: version.parent_version_id,
                history_segment: version.history_segment.clone(),
            })
        } else {
            Ok(GetVersionResult::NoSuchVersion)
        }
    }

    fn add_snapshot(&mut self, version_id: VersionId, snapshot: Snapshot) -> Result<()> {
        // test implementation -- does not perform any validation
        self.snapshot = Some((version_id, snapshot));
        Ok(())
    }

    fn get_snapshot(&mut self) -> Result<Option<(VersionId, Snapshot)>> {
        Ok(self.snapshot.clone())
    }
}
