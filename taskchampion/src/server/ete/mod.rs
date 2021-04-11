#![allow(unused_variables)]
#![allow(dead_code)]
#![allow(unused_imports)]
// TODO ^^

use crate::server::config::EteServerConfig;
use crate::server::{AddVersionResult, GetVersionResult, HistorySegment, Server};
use anyhow::{bail, Context, Result};
use etebase::{error::Error as EteError, Account, Client, Collection, ItemMetadata, User};
use serde::{Deserialize, Serialize};

type VersionId = String;

const COLLECTION_TYPE: &str = "taskchampion.task-history";
const COLLECTION_NAME: &str = "task-history";

pub struct EteServer {
    account: Account,
}

/* Ete Storage
 *
 * See https://docs.etebase.com for the Etebase documentation.
 *
 * This module stores all task data in a single collection named `task-history`, of type
 * `taskchampion.task-history`.  An account with multiple matching collections is corrupted and
 * will cause an error.  Within the account, the metadata contains ..  TODO
 */

/// A RemoeServer communicates with a remote server over HTTP (such as with
/// taskchampion-sync-server).
impl EteServer {
    // TODO: support signup and login operations that result in a saved session

    /// Construct a new EteServer from config
    pub fn new(config: EteServerConfig) -> Result<EteServer> {
        let client = Client::new("taskchampion", &config.server_url)?;
        let account = Account::login_key(client, &config.username, &config.key)?;
        Ok(EteServer { account })
    }

    /// Get the collection in this account containing the task history, creating it if necessary.
    fn get_collection(&self) -> Result<Collection> {
        let cm = self.account.collection_manager()?;

        // look for un-deleted collections of COLLECTION_TYPE; there should be zero or one
        let collections = cm.list(COLLECTION_TYPE, None)?;

        let mut collection = None;
        for coll in collections.data() {
            if coll.is_deleted() {
                continue;
            }
            let meta = coll.meta()?;
            if meta.name() != Some(COLLECTION_NAME) {
                continue;
            }
            if collection.is_some() {
                bail!("Account contains multiple {} collections", COLLECTION_NAME);
            }
            collection = Some(coll);
        }

        if let Some(collection) = collection {
            return Ok(collection.clone());
        }

        let collection = cm.create(
            COLLECTION_TYPE,
            ItemMetadata::new()
                .set_name(Some(COLLECTION_NAME))
                .set_description(Some("TaskChampion Task History")),
            b"{}", // empty JSON content
        )?;
        cm.upload(&collection, None)?;
        Ok(collection)
    }

    fn add_version(
        &mut self,
        parent_version_id: VersionId,
        history_segment: HistorySegment,
    ) -> anyhow::Result<AddVersionResult> {
        let mut coll = self.get_collection()?;
        // TODO: transactions https://docs.etebase.com/guides/using_collections#transactions
        let mut content = CollectionContent::from_collection(&coll)?;
        let latest_version = content.latest_version.unwrap_or_else(|| "".to_string());

        if latest_version != parent_version_id {
            // parent versions differ -- refuse the request
            return Ok(AddVersionResult::ExpectedParentVersion(
                /* TODO */ uuid::Uuid::new_v4(),
            ));
        }

        let cm = self.account.collection_manager()?;
        let im = cm.item_manager(&coll)?;
        let item = im.create(&ItemMetadata::new(), &history_segment)?;

        let new_version_id = item.uid().to_owned();

        content.latest_version = Some(new_version_id.clone());
        let content = serde_json::to_vec(&content).context("encoding collection content")?;

        im.batch(vec![&item].into_iter(), None)
            .context("writing new history item")?;
        coll.set_content(&content)
            .context("setting collection content")?;
        cm.upload(&coll, None).context("uploading collection")?;

        Ok(AddVersionResult::Ok(/* TODO */ uuid::Uuid::new_v4()))
    }

    fn get_child_version(
        &mut self,
        parent_version_id: VersionId,
    ) -> anyhow::Result<GetVersionResult> {
        todo!()
    }
}

/// Content of the Collection's 'content' property, in JSON
#[derive(Serialize, Deserialize)]
struct CollectionContent {
    latest_version: Option<VersionId>,
}

impl CollectionContent {
    fn from_collection(coll: &Collection) -> Result<Self> {
        let content = coll.content()?;
        Ok(serde_json::from_slice(&content)?)
    }
}

/*
/// Read a UUID-bearing header or fail trying
fn get_uuid_header(resp: &ureq::Response, name: &str) -> anyhow::Result<Uuid> {
    let value = resp
        .header(name)
        .ok_or_else(|| anyhow::anyhow!("Response does not have {} header", name))?;
    let value = Uuid::parse_str(value)
        .map_err(|e| anyhow::anyhow!("{} header is not a valid UUID: {}", name, e))?;
    Ok(value)
}

impl Server for EteServer {
    fn add_version(
        &mut self,
        parent_version_id: VersionId,
        history_segment: HistorySegment,
    ) -> anyhow::Result<AddVersionResult> {
        let url = format!("{}/client/add-version/{}", self.origin, parent_version_id);
        let history_cleartext = HistoryCleartext {
            parent_version_id,
            history_segment,
        };
        let history_ciphertext = history_cleartext.seal(&self.encryption_secret)?;
        match self
            .agent
            .post(&url)
            .set(
                "Content-Type",
                "application/vnd.taskchampion.history-segment",
            )
            .set("X-Client-Key", &self.client_key.to_string())
            .send_bytes(history_ciphertext.as_ref())
        {
            Ok(resp) => {
                let version_id = get_uuid_header(&resp, "X-Version-Id")?;
                Ok(AddVersionResult::Ok(version_id))
            }
            Err(ureq::Error::Status(status, resp)) if status == 409 => {
                let parent_version_id = get_uuid_header(&resp, "X-Parent-Version-Id")?;
                Ok(AddVersionResult::ExpectedParentVersion(parent_version_id))
            }
            Err(err) => Err(err.into()),
        }
    }

    fn get_child_version(
        &mut self,
        parent_version_id: VersionId,
    ) -> anyhow::Result<GetVersionResult> {
        let url = format!(
            "{}/client/get-child-version/{}",
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
                let history_ciphertext: HistoryCiphertext = resp.try_into()?;
                let history_segment = history_ciphertext
                    .open(&self.encryption_secret, parent_version_id)?
                    .history_segment;
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
}
*/

#[cfg(test)]
mod test {
    use super::*;
    use env_logger;
    use lazy_static::lazy_static;
    use std::env;
    use std::sync::{Mutex, MutexGuard};
    use tindercrypt::rand::fill_buf;

    lazy_static! {
        static ref ETE_MUTEX: Mutex<()> = Mutex::new(());
    }

    /// Set up to run tests:
    /// - Initialize logging
    /// - Return None if ETEBASE_TEST_HOST is not set (skippig the test); otherwise
    /// - Create a brand-new user on the test server,
    /// - Generate a corresponding EteServerConfig
    /// - Lock a mutex to prevent concurrent calls to the server (which causes
    ///   500 errors, because it does not lock SQLite)
    fn setup<'a>() -> Option<(EteServerConfig, MutexGuard<'a, ()>)> {
        let server_url = format!(
            "http://{}",
            match env::var("ETEBASE_TEST_HOST") {
                Err(_) => return None,
                Ok(v) => v,
            },
        );

        let guard = ETE_MUTEX.lock().unwrap();

        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Debug) // TODO
            .is_test(true)
            .try_init();

        let mut key = [0u8; 32];
        fill_buf(&mut key);

        let mut buf = [0u8; 16];
        fill_buf(&mut buf);
        let username = buf
            .iter()
            .map(|i| format!("{:x}", i))
            .collect::<Vec<_>>()
            .join("")
            .to_owned();

        let user = etebase::User::new(&username, "test@example.com");

        let client = etebase::Client::new("tests", &server_url)
            .context("could not create client")
            .unwrap();
        let acct = etebase::Account::signup_key(client, &user, &key)
            .context("could not sign up user")
            .unwrap();
        Some((
            EteServerConfig {
                server_url,
                username,
                key: key.to_vec(),
            },
            guard,
        ))
    }

    #[test]
    fn test_get_collection_fresh() -> Result<()> {
        if let Some((config, _server_guard)) = setup() {
            let server = EteServer::new(config)?;
            let coll = server.get_collection()?;
            assert_eq!(&coll.collection_type()?, COLLECTION_TYPE);
            assert_eq!(coll.meta()?.name(), Some(COLLECTION_NAME));
        }
        Ok(())
    }

    #[test]
    fn test_get_collection_existing() -> Result<()> {
        if let Some((config, _server_guard)) = setup() {
            let server = EteServer::new(config)?;

            // make an existing collection, with a description differing from
            // the normal collection so we can be sure this is the one returned.
            let cm = server.account.collection_manager()?;
            let coll = cm.create(
                COLLECTION_TYPE,
                ItemMetadata::new()
                    .set_name(Some(COLLECTION_NAME))
                    .set_description(Some("test collection")),
                &[], // Empty content
            )?;
            cm.upload(&coll, None)?;

            let coll = server.get_collection()?;
            assert_eq!(&coll.collection_type()?, COLLECTION_TYPE);
            assert_eq!(coll.meta()?.name(), Some(COLLECTION_NAME));
            assert_eq!(coll.meta()?.description(), Some("test collection"));
        }
        Ok(())
    }

    #[test]
    fn test_add_version_to_empty() -> Result<()> {
        if let Some((config, _server_guard)) = setup() {
            let mut server = EteServer::new(config)?;
            let coll = server.add_version("parent".to_string(), b"history".to_vec())?;
        }
        Ok(())
    }
}
