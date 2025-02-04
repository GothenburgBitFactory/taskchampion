use crate::errors::Result;

/// Information about an object as returned from `Service::list`
pub(in crate::server) struct ObjectInfo {
    /// Name of the object.
    pub(in crate::server) name: String,
    /// Creation time of the object, in seconds since the UNIX epoch.
    pub(in crate::server) creation: u64,
}

/// An abstraction of a cloud-storage service.
///
/// The underlying cloud storage is assumed to be a map from object names to object values,
/// similar to a HashMap, with the addition of a compare-and-swap operation. Object names
/// are always simple strings from the character set `[a-zA-Z0-9-]`, no more than 100 characters
/// in length.
pub(in crate::server) trait Service {
    /// Put an object into cloud storage. If the object exists, it is overwritten.
    fn put(&mut self, name: &str, value: &[u8]) -> Result<()>;

    /// Get an object from cloud storage, or None if the object does not exist.
    fn get(&mut self, name: &str) -> Result<Option<Vec<u8>>>;

    /// Delete an object. Does nothing if the object does not exist.
    fn del(&mut self, name: &str) -> Result<()>;

    /// Enumerate objects with the given prefix.
    fn list<'a>(&'a mut self, prefix: &'a str)
        -> Box<dyn Iterator<Item = Result<ObjectInfo>> + 'a>;

    /// Compare the existing object's value with `existing_value`, and replace with `new_value`
    /// only if the values match. Returns true if the replacement occurred.
    fn compare_and_swap(
        &mut self,
        name: &str,
        existing_value: Option<Vec<u8>>,
        new_value: Vec<u8>,
    ) -> Result<bool>;
}

/// Enforce the limits on `name` described for [`Service`].
///
/// Names are generated by `server.rs` in a way that is not affected by user input, so this only
/// asserts in debug builds.
pub(in crate::server) fn validate_object_name(name: &str) {
    debug_assert!(name.is_ascii());
    debug_assert!(name.len() <= 100);
    debug_assert!(name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'));
}
