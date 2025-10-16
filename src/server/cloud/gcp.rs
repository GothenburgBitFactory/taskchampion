use super::service::{validate_object_name, ObjectInfo, Service};
use crate::errors::Result;
use crate::server::cloud::iter::AsyncObjectIterator;
use async_trait::async_trait;
use google_cloud_storage::client::google_cloud_auth::credentials::CredentialsFile;
use google_cloud_storage::client::{Client, ClientConfig};
use google_cloud_storage::http::error::ErrorResponse;
use google_cloud_storage::http::Error as GcsError;
use google_cloud_storage::http::{self, objects};

/// A [`Service`] implementation based on the Google Cloud Storage service.
pub(in crate::server) struct GcpService {
    client: Client,
    bucket: String,
}

/// Determine whether the given result contains an HTTP error with the given code.
fn is_http_error<T>(query: u16, res: &std::result::Result<T, http::Error>) -> bool {
    match res {
        // Errors from RPC's.
        Err(GcsError::Response(ErrorResponse { code, .. })) => *code == query,
        // Errors from reqwest (downloads, uploads).
        Err(GcsError::HttpClient(e)) => e.status().map(|s| s.as_u16()) == Some(query),
        _ => false,
    }
}

impl GcpService {
    pub(in crate::server) async fn new(
        bucket: String,
        credential_path: Option<String>,
    ) -> Result<Self> {
        let config: ClientConfig = if let Some(credentials) = credential_path {
            let credentials = CredentialsFile::new_from_file(credentials).await?;
            ClientConfig::default()
                .with_credentials(credentials)
                .await?
        } else {
            ClientConfig::default().with_auth().await?
        };

        Ok(Self {
            client: Client::new(config),
            bucket,
        })
    }
}
#[async_trait]
impl Service for GcpService {
    async fn put(&mut self, name: &str, value: &[u8]) -> Result<()> {
        validate_object_name(name);
        let upload_type =
            objects::upload::UploadType::Simple(objects::upload::Media::new(name.to_string()));
        self.client
            .upload_object(
                &objects::upload::UploadObjectRequest {
                    bucket: self.bucket.clone(),
                    ..Default::default()
                },
                value.to_vec(),
                &upload_type,
            )
            .await?;
        Ok(())
    }

    async fn get(&mut self, name: &str) -> Result<Option<Vec<u8>>> {
        validate_object_name(name);
        let download_res = self
            .client
            .download_object(
                &objects::get::GetObjectRequest {
                    bucket: self.bucket.clone(),
                    object: name.to_string(),
                    ..Default::default()
                },
                &objects::download::Range::default(),
            )
            .await;
        if is_http_error(404, &download_res) {
            Ok(None)
        } else {
            Ok(Some(download_res?))
        }
    }

    async fn del(&mut self, name: &str) -> Result<()> {
        validate_object_name(name);
        let del_res = self
            .client
            .delete_object(&objects::delete::DeleteObjectRequest {
                bucket: self.bucket.clone(),
                object: name.to_string(),
                ..Default::default()
            })
            .await;
        if !is_http_error(404, &del_res) {
            del_res?;
        }
        Ok(())
    }

    async fn list<'a>(&'a mut self, prefix: &'a str) -> Box<dyn AsyncObjectIterator + Send + 'a> {
        validate_object_name(prefix);
        Box::new(ObjectIterator {
            service: self,
            prefix: prefix.to_string(),
            last_response: None,
            next_index: 0,
        })
    }

    async fn compare_and_swap(
        &mut self,
        name: &str,
        existing_value: Option<Vec<u8>>,
        new_value: Vec<u8>,
    ) -> Result<bool> {
        validate_object_name(name);
        let get_res = self
            .client
            .get_object(&objects::get::GetObjectRequest {
                bucket: self.bucket.clone(),
                object: name.to_string(),
                ..Default::default()
            })
            .await;
        // Determine the object's generation. See https://cloud.google.com/storage/docs/metadata#generation-number
        let generation = if is_http_error(404, &get_res) {
            // If a value was expected, that expectation has not been met.
            if existing_value.is_some() {
                return Ok(false);
            }
            // Generation 0 indicates that the object does not yet exist.
            0
        } else {
            get_res?.generation
        };

        // If the file existed, then verify its contents.
        if generation > 0 {
            let data = self
                .client
                .download_object(
                    &objects::get::GetObjectRequest {
                        bucket: self.bucket.clone(),
                        object: name.to_string(),
                        // Fetch the same generation.
                        generation: Some(generation),
                        ..Default::default()
                    },
                    &objects::download::Range::default(),
                )
                .await?;
            if Some(data) != existing_value {
                return Ok(false);
            }
        }

        // When testing, an object named "$pfx-racing-delete" is deleted between get_object and
        // put_object.
        #[cfg(test)]
        if name.ends_with("-racing-delete") {
            println!("deleting object {name}");
            let del_res = self
                .client
                .delete_object(&objects::delete::DeleteObjectRequest {
                    bucket: self.bucket.clone(),
                    object: name.to_string(),
                    ..Default::default()
                })
                .await;
            if !is_http_error(404, &del_res) {
                del_res?;
            }
        }

        // When testing, if the object is named "$pfx-racing-put" then the value "CHANGED" is
        // written to it between get_object and put_object.
        #[cfg(test)]
        if name.ends_with("-racing-put") {
            println!("changing object {name}");
            let upload_type =
                objects::upload::UploadType::Simple(objects::upload::Media::new(name.to_string()));
            self.client
                .upload_object(
                    &objects::upload::UploadObjectRequest {
                        bucket: self.bucket.clone(),
                        ..Default::default()
                    },
                    b"CHANGED".to_vec(),
                    &upload_type,
                )
                .await?;
        }

        // Finally, put the new value with a condition that the generation hasn't changed.
        let upload_type =
            objects::upload::UploadType::Simple(objects::upload::Media::new(name.to_string()));
        let upload_res = self
            .client
            .upload_object(
                &objects::upload::UploadObjectRequest {
                    bucket: self.bucket.clone(),
                    if_generation_match: Some(generation),
                    ..Default::default()
                },
                new_value.to_vec(),
                &upload_type,
            )
            .await;
        if is_http_error(412, &upload_res) {
            // A 412 indicates the precondition was not satisfied: the given generation
            // is no longer the latest.
            Ok(false)
        } else {
            upload_res?;
            Ok(true)
        }
    }
}

/// An Iterator returning names of objects from `list_objects`.
///
/// This handles response pagination by fetching one page at a time.
struct ObjectIterator<'a> {
    service: &'a mut GcpService,
    prefix: String,
    last_response: Option<objects::list::ListObjectsResponse>,
    next_index: usize,
}

impl ObjectIterator<'_> {
    async fn fetch_batch(&mut self) -> Result<()> {
        let mut page_token = None;
        if let Some(ref resp) = self.last_response {
            page_token.clone_from(&resp.next_page_token);
        }
        self.last_response = Some(
            self.service
                .client
                .list_objects(&objects::list::ListObjectsRequest {
                bucket: self.service.bucket.clone(),
                prefix: Some(self.prefix.clone()),
                page_token,
                #[cfg(test)] // For testing, use a small page size.
                max_results: Some(6),
                ..Default::default()
            })
                .await?,
        );
        self.next_index = 0;
        Ok(())
    }
}

#[async_trait]
impl AsyncObjectIterator for ObjectIterator<'_> {
    async fn next(&mut self) -> Option<Result<ObjectInfo>> {
        // If the iterator is just starting, fetch the first response.
        if self.last_response.is_none() {
            if let Err(e) = self.fetch_batch().await {
                return Some(Err(e));
            }
        }
        if let Some(ref result) = self.last_response {
            if let Some(ref items) = result.items {
                if self.next_index < items.len() {
                    // Return a result from the existing response.
                    let obj = &items[self.next_index];
                    self.next_index += 1;
                    // It's unclear when `time_created` would be None, so default to 0 in that case
                    // or when the timestamp is not a valid u64 (before 1970).
                    let creation = obj.time_created.map(|t| t.unix_timestamp()).unwrap_or(0);
                    let creation: u64 = creation.try_into().unwrap_or(0);
                    return Some(Ok(ObjectInfo {
                        name: obj.name.clone(),
                        creation,
                    }));
                } else if result.next_page_token.is_some() {
                    // Fetch the next page and try again.
                    if let Err(e) = self.fetch_batch().await {
                        return Some(Err(e));
                    }
                    return self.next().await;
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Make a service if `GCP_TEST_BUCKET` is set, as well as a function to put a unique prefix on
    /// an object name, so that tests do not interfere with one another.
    ///
    /// Set up this bucket with a lifecyle policy to delete objects with age > 1 day. While passing
    /// tests should correctly clean up after themselves, failing tests may leave objects in the
    /// bucket.
    ///
    /// When the environment variable is not set, this returns false and the test does not run.
    /// Note that the Rust test runner will still show "ok" for the test, as there is no way to
    /// indicate anything else.
    async fn make_service() -> Option<GcpService> {
        let Ok(bucket) = std::env::var("GCP_TEST_BUCKET") else {
            return None;
        };

        let Ok(credential_path) = std::env::var("GCP_TEST_CREDENTIAL_PATH") else {
            return None;
        };

        Some(
            GcpService::new(bucket, Some(credential_path))
                .await
                .unwrap(),
        )
    }

    crate::server::cloud::test::service_tests!(make_service().await);
}
