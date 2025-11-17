use super::service::{validate_object_name, ObjectInfo, Service};
use crate::{errors::Result, server::cloud::iter::AsyncObjectIterator};
use async_trait::async_trait;
use aws_config::{
    meta::region::RegionProviderChain, profile::ProfileFileCredentialsProvider, BehaviorVersion,
    Region,
};
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    self as s3,
    error::ProvideErrorMetadata,
    operation::{get_object::GetObjectOutput, list_objects_v2::ListObjectsV2Output},
};
use aws_smithy_runtime::client::http::hyper_014::HyperClientBuilder;

#[cfg(not(any(feature = "tls-native-roots", feature = "tls-webpki-roots")))]
compile_error!(
    "Either feature \"tls-native-roots\" or \"tls-webpki-roots\" must be enabled for TLS support."
);

/// A [`Service`] implementation based on AWS S3.
pub(in crate::server) struct AwsService {
    client: s3::Client,
    bucket: String,
}

/// Credential configuration for access to the AWS service.
///
/// These credentials must have a least the following policy, with BUCKETNAME replaced by
/// the bucket name:
///
/// ```json
/// {
///     "Version": "2012-10-17",
///     "Statement": [
///         {
///             "Sid": "TaskChampion",
///             "Effect": "Allow",
///             "Action": [
///                 "s3:PutObject",
///                 "s3:GetObject",
///                 "s3:ListBucket",
///                 "s3:DeleteObject"
///             ],
///             "Resource": [
///                 "arn:aws:s3:::BUCKETNAME",
///                 "arn:aws:s3:::BUCKETNAME/*"
///             ]
///         }
///     ]
/// }
/// ```
#[non_exhaustive]
pub enum AwsCredentials {
    /// A pair of access key ID and secret access key.
    AccessKey {
        access_key_id: String,
        secret_access_key: String,
    },
    /// A named profile from the profile files in the user's home directory.
    Profile { profile_name: String },
    /// Use the [default credential
    /// sources](https://docs.rs/aws-config/latest/aws_config/default_provider/credentials/struct.DefaultCredentialsChain.html),
    /// such as enviroment variables, the default profile, or the task/instance IAM role.
    Default,
}

impl AwsService {
    pub(in crate::server) async fn new(
        region: String,
        bucket: String,
        creds: AwsCredentials,
    ) -> Result<Self> {
        let mut config_provider = aws_config::defaults(BehaviorVersion::v2024_03_28());
        match creds {
            AwsCredentials::AccessKey {
                access_key_id,
                secret_access_key,
            } => {
                config_provider = config_provider.credentials_provider(Credentials::from_keys(
                    access_key_id,
                    secret_access_key,
                    None,
                ));
            }
            AwsCredentials::Profile { profile_name } => {
                config_provider = config_provider.credentials_provider(
                    ProfileFileCredentialsProvider::builder()
                        .profile_name(profile_name)
                        .build(),
                );
            }
            AwsCredentials::Default => {
                // Just use the default.
            }
        }

        let builder = hyper_rustls::HttpsConnectorBuilder::new();

        // Only one of with_native_roots and with_webpki_roots is supported, so prefer
        // native roots.
        #[cfg(feature = "tls-native-roots")]
        let builder = builder.with_native_roots();
        #[cfg(all(feature = "tls-webpki-roots", not(feature = "tls-native-roots")))]
        let builder = builder.with_webpki_roots();

        let tls_connector = builder.https_only().enable_http2().build();

        let hyper_client = HyperClientBuilder::new().build(tls_connector);
        config_provider = config_provider.http_client(hyper_client);

        let config = config_provider
            .region(RegionProviderChain::first_try(Region::new(region)))
            .load()
            .await;

        let client = s3::client::Client::new(&config);
        Ok(Self { client, bucket })
    }
}

/// Convert an error that can be converted to `s3::Error` (but not [`crate::Error`]) into
/// `s3::Error`. One such error is SdkError, which has type parameters that are difficult to
/// constrain in order to write `From<SdkError<..>> for crate::Error`.
fn aws_err<E: Into<s3::Error>>(err: E) -> s3::Error {
    err.into()
}

/// Convert a `NoSuchKey` error into `Ok(None)`, and `Ok(..)` into `Ok(Some(..))`.
#[allow(clippy::result_large_err)] // s3::Error is large, it's not our fault!
fn if_key_exists<T>(
    res: std::result::Result<T, s3::Error>,
) -> std::result::Result<Option<T>, s3::Error> {
    res
        // convert Result<T, E> to Result<Option<T>, E>
        .map(Some)
        // handle NoSuchKey
        .or_else(|err| match err {
            s3::Error::NoSuchKey(_) => Ok(None),
            err => Err(err),
        })
}

/// Get the body of a `get_object` result.
async fn get_body(get_res: GetObjectOutput) -> Result<Vec<u8>> {
    Ok(get_res.body.collect().await?.to_vec())
}

#[async_trait]
impl Service for AwsService {
    async fn put(&mut self, name: &str, value: &[u8]) -> Result<()> {
        validate_object_name(name);
        self.client
            .put_object()
            .bucket(self.bucket.clone())
            .key(name)
            .body(value.to_vec().into())
            .send()
            .await
            .map_err(aws_err)?;
        Ok(())
    }

    async fn get(&mut self, name: &str) -> Result<Option<Vec<u8>>> {
        validate_object_name(name);
        let Some(get_res) = if_key_exists(
            self.client
                .get_object()
                .bucket(self.bucket.clone())
                .key(name)
                .send()
                .await
                .map_err(aws_err),
        )?
        else {
            return Ok(None);
        };
        Ok(Some(get_body(get_res).await?))
    }

    async fn del(&mut self, name: &str) -> Result<()> {
        validate_object_name(name);
        self.client
            .delete_object()
            .bucket(self.bucket.clone())
            .key(name)
            .send()
            .await
            .map_err(aws_err)?;
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
        let get_res = if_key_exists(
            self.client
                .get_object()
                .bucket(self.bucket.clone())
                .key(name)
                .send()
                .await
                .map_err(aws_err),
        )?;

        // Check the expectation and gather the e_tag for the existing value.
        let e_tag;
        if let Some(get_res) = get_res {
            // If a value was not expected but one exists, that expectation has not been met.
            let Some(existing_value) = existing_value else {
                return Ok(false);
            };
            e_tag = get_res.e_tag.clone();
            let body = get_body(get_res).await?;
            if body != existing_value {
                return Ok(false);
            }
        } else {
            // If a value was expected but none exists, that expectation has not been met.
            if existing_value.is_some() {
                return Ok(false);
            }
            e_tag = None;
        };

        // When testing, an object named "$pfx-racing-delete" is deleted between get_object and
        // put_object.
        #[cfg(test)]
        if name.ends_with("-racing-delete") {
            println!("deleting object {name}");
            self.client
                .delete_object()
                .bucket(self.bucket.clone())
                .key(name)
                .send()
                .await
                .map_err(aws_err)?;
        }

        // When testing, if the object is named "$pfx-racing-put" then the value "CHANGED" is
        // written to it between get_object and put_object.
        #[cfg(test)]
        if name.ends_with("-racing-put") {
            println!("changing object {name}");
            self.client
                .put_object()
                .bucket(self.bucket.clone())
                .key(name)
                .body(b"CHANGED".to_vec().into())
                .send()
                .await
                .map_err(aws_err)?;
        }

        // Try to put the object, using an appropriate conditional.
        let mut put_builder = self.client.put_object();
        if let Some(e_tag) = e_tag {
            put_builder = put_builder.if_match(e_tag);
        } else {
            put_builder = put_builder.if_none_match("*");
        }
        match put_builder
            .bucket(self.bucket.clone())
            .key(name)
            .body(new_value.to_vec().into())
            .send()
            .await
            .map_err(aws_err)
        {
            Ok(_) => Ok(true),
            // If the key disappears, S3 returns 404.
            Err(err) if err.code() == Some("NoSuchKey") => Ok(false),
            // PreconditionFailed occurs if the file changed unexpectedly
            Err(err) if err.code() == Some("PreconditionFailed") => Ok(false),
            // Docs describe this as a "conflicting operation" with no further details.
            Err(err) if err.code() == Some("ConditionalRequestConflict") => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

/// An Iterator returning names of objects from `list_objects_v2`.
///
/// This handles response pagination by fetching one page at a time.
struct ObjectIterator<'a> {
    service: &'a mut AwsService,
    prefix: String,
    last_response: Option<ListObjectsV2Output>,
    next_index: usize,
}

impl ObjectIterator<'_> {
    async fn fetch_batch(&mut self) -> Result<()> {
        let mut continuation_token = None;
        if let Some(ref resp) = self.last_response {
            continuation_token.clone_from(&resp.next_continuation_token);
        }

        // Use the default max_keys in production, but a smaller value in testing so
        // we can test the pagination.
        #[cfg(test)]
        let max_keys = Some(8);
        #[cfg(not(test))]
        let max_keys = None;

        self.last_response = None;
        self.last_response = Some(
            self.service
                .client
                .list_objects_v2()
                .bucket(self.service.bucket.clone())
                .prefix(self.prefix.clone())
                .set_max_keys(max_keys)
                .set_continuation_token(continuation_token)
                .send()
                .await
                .map_err(aws_err)?,
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
            if let Some(ref items) = result.contents {
                if self.next_index < items.len() {
                    // Return a result from the existing response.
                    let obj = &items[self.next_index];
                    self.next_index += 1;
                    // Use `last_modified` as a proxy for creation time, since most objects
                    // are not updated after they are created.
                    let creation = obj.last_modified.map(|t| t.secs()).unwrap_or(0);
                    let creation: u64 = creation.try_into().unwrap_or(0);
                    let name = obj.key.as_ref().expect("object has no key").clone();
                    return Some(Ok(ObjectInfo {
                        name: name.clone(),
                        creation,
                    }));
                } else if result.next_continuation_token.is_some() {
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

    /// Make a service.
    ///
    /// The service is only created if the following environment variables are set:
    ///  * `AWS_TEST_REGION` - region containing the test bucket
    ///  * `AWS_TEST_BUCKET` - test bucket
    ///  * `AWS_TEST_ACCESS_KEY_ID` / `AWS_TEST_SECRET_ACCESS_KEY` - credentials for access to the
    ///    bucket.
    ///
    /// Set up the bucket with a lifecyle policy to delete objects with age > 1 day. While passing
    /// tests should correctly clean up after themselves, failing tests may leave objects in the
    /// bucket.
    ///
    /// When the environment variables are not set, this returns false and the test does not run.
    /// Note that the Rust test runner will still show "ok" for the test, as there is no way to
    /// indicate anything else.
    async fn make_service() -> Option<AwsService> {
        let fail_if_not_set = std::env::var("AWS_FAIL_IF_NOT_SET").is_ok();
        let Ok(region) = std::env::var("AWS_TEST_REGION") else {
            if fail_if_not_set {
                panic!("AWS_TEST_REGION not set");
            }
            return None;
        };

        let Ok(bucket) = std::env::var("AWS_TEST_BUCKET") else {
            if fail_if_not_set {
                panic!("AWS_TEST_BUCKET not set");
            }
            return None;
        };

        let Ok(access_key_id) = std::env::var("AWS_TEST_ACCESS_KEY_ID") else {
            if fail_if_not_set {
                panic!("AWS_TEST_ACCESS_KEY_ID not set");
            }
            return None;
        };

        let Ok(secret_access_key) = std::env::var("AWS_TEST_SECRET_ACCESS_KEY") else {
            if fail_if_not_set {
                panic!("AWS_TEST_SECRET_ACCESS_KEY not set");
            }
            return None;
        };

        Some(
            AwsService::new(
                region,
                bucket,
                AwsCredentials::AccessKey {
                    access_key_id,
                    secret_access_key,
                },
            )
            .await
            .unwrap(),
        )
    }

    crate::server::cloud::test::service_tests!(make_service().await);
}
