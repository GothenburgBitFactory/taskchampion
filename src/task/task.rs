use super::tag::{SyntheticTag, TagInner};
use super::{utc_timestamp, Annotation, Status, Tag, Timestamp};
use crate::depmap::DependencyMap;
use crate::errors::{Error, Result};
use crate::storage::TaskMap;
use crate::{Operations, TaskData};
use chrono::prelude::*;
use log::trace;
use std::convert::AsRef;
use std::convert::TryInto;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

/// A task, with a high-level interface.
///
/// Building on [`crate::TaskData`], this type implements the task model, with ergonomic APIs to
/// manipulate tasks without deep familiarity with the [task
/// model](https://gothenburgbitfactory.org/taskchampion/tasks.html#keys).
///
/// Note that Task objects represent a snapshot of the task at a moment in time, and are not
/// protected by the atomicity of the backend storage.  Concurrent modifications are safe,
/// but a Task that is cached for more than a few seconds may cause the user to see stale
/// data.  Fetch, use, and drop Tasks quickly.
///
/// See the documentation for [`crate::Replica`] for background on the `ops` arguments to methods
/// on this type.
#[derive(Debug, Clone)]
pub struct Task {
    // The underlying task data.
    data: TaskData,

    // The dependency map for this replica, for rapidly computing synthetic tags.
    depmap: Arc<DependencyMap>,

    // True if an operation has alredy been emitted to update the `modified` property.
    updated_modified: bool,
}

impl PartialEq for Task {
    fn eq(&self, other: &Task) -> bool {
        // compare only the task data; depmap is just present for reference
        self.data == other.data
    }
}

/// An enum containing all of the key names defined in the data model, with the exception
/// of the properties containing data (`tag_..`, etc.)
#[derive(strum_macros::AsRefStr, strum_macros::EnumString)]
#[strum(serialize_all = "kebab-case")]
enum Prop {
    Description,
    Due,
    Modified,
    Start,
    Status,
    Priority,
    Wait,
    End,
    Entry,
}

#[allow(clippy::ptr_arg)]
fn uda_string_to_tuple(key: &str) -> (&str, &str) {
    let mut iter = key.splitn(2, '.');
    let first = iter.next().unwrap();
    let second = iter.next();
    if let Some(second) = second {
        (first, second)
    } else {
        ("", first)
    }
}

fn uda_tuple_to_string(namespace: impl AsRef<str>, key: impl AsRef<str>) -> String {
    let namespace = namespace.as_ref();
    let key = key.as_ref();
    if namespace.is_empty() {
        key.into()
    } else {
        format!("{}.{}", namespace, key)
    }
}

impl Task {
    pub(crate) fn new(data: TaskData, depmap: Arc<DependencyMap>) -> Task {
        Task {
            data,
            depmap,
            updated_modified: false,
        }
    }

    /// Convert this Task into a TaskData.
    pub fn into_task_data(self) -> TaskData {
        self.data
    }

    /// Get this task's UUID.
    pub fn get_uuid(&self) -> Uuid {
        self.data.get_uuid()
    }

    #[deprecated(since = "0.7.0", note = "please use TaskData::properties")]
    pub fn get_taskmap(&self) -> &TaskMap {
        self.data.get_taskmap()
    }

    pub fn get_status(&self) -> Status {
        self.data
            .get(Prop::Status.as_ref())
            .map(Status::from_taskmap)
            .unwrap_or(Status::Pending)
    }

    pub fn get_description(&self) -> &str {
        self.data.get(Prop::Description.as_ref()).unwrap_or("")
    }

    pub fn get_entry(&self) -> Option<Timestamp> {
        self.get_timestamp(Prop::Entry.as_ref())
    }

    pub fn get_priority(&self) -> &str {
        self.data.get(Prop::Priority.as_ref()).unwrap_or("")
    }

    /// Get the wait time.  If this value is set, it will be returned, even
    /// if it is in the past.
    pub fn get_wait(&self) -> Option<Timestamp> {
        self.get_timestamp(Prop::Wait.as_ref())
    }

    /// Determine whether this task is waiting now.
    pub fn is_waiting(&self) -> bool {
        if let Some(ts) = self.get_wait() {
            return ts > Utc::now();
        }
        false
    }

    /// Determine whether this task is active -- that is, that it has been started
    /// and not stopped.
    pub fn is_active(&self) -> bool {
        self.data.has(Prop::Start.as_ref())
    }

    /// Determine whether this task is blocked -- that is, has at least one unresolved dependency.
    pub fn is_blocked(&self) -> bool {
        self.depmap.dependencies(self.get_uuid()).next().is_some()
    }

    /// Determine whether this task is blocking -- that is, has at least one unresolved dependent.
    pub fn is_blocking(&self) -> bool {
        self.depmap.dependents(self.get_uuid()).next().is_some()
    }

    /// Determine whether a given synthetic tag is present on this task.  All other
    /// synthetic tag calculations are based on this one.
    fn has_synthetic_tag(&self, synth: &SyntheticTag) -> bool {
        match synth {
            SyntheticTag::Waiting => self.is_waiting(),
            SyntheticTag::Active => self.is_active(),
            SyntheticTag::Pending => self.get_status() == Status::Pending,
            SyntheticTag::Completed => self.get_status() == Status::Completed,
            SyntheticTag::Deleted => self.get_status() == Status::Deleted,
            SyntheticTag::Blocked => self.is_blocked(),
            SyntheticTag::Unblocked => !self.is_blocked(),
            SyntheticTag::Blocking => self.is_blocking(),
        }
    }

    /// Check if this task has the given tag
    pub fn has_tag(&self, tag: &Tag) -> bool {
        match tag.inner() {
            TagInner::User(s) => self.data.has(format!("tag_{}", s)),
            TagInner::Synthetic(st) => self.has_synthetic_tag(st),
        }
    }

    /// Iterate over the task's tags
    pub fn get_tags(&self) -> impl Iterator<Item = Tag> + '_ {
        use strum::IntoEnumIterator;

        self.data
            .properties()
            .filter_map(|k| {
                if let Some(tag) = k.strip_prefix("tag_") {
                    if let Ok(tag) = tag.try_into() {
                        return Some(tag);
                    }
                    // note that invalid "tag_*" are ignored
                }
                None
            })
            .chain(
                SyntheticTag::iter()
                    .filter(move |st| self.has_synthetic_tag(st))
                    .map(|st| Tag::from_inner(TagInner::Synthetic(st))),
            )
    }

    /// Iterate over the task's annotations, in arbitrary order.
    pub fn get_annotations(&self) -> impl Iterator<Item = Annotation> + '_ {
        self.data.iter().filter_map(|(k, v)| {
            if let Some(ts) = k.strip_prefix("annotation_") {
                if let Ok(ts) = ts.parse::<i64>() {
                    return Some(Annotation {
                        entry: utc_timestamp(ts),
                        description: v.to_owned(),
                    });
                }
                // note that invalid "annotation_*" are ignored
            }
            None
        })
    }

    /// Get the named user defined attributes (UDA).  This will return None
    /// for any key defined in the Task data model, regardless of whether
    /// it is set or not.
    #[deprecated(note = "namespaced UDAs will not be supported in the future")]
    pub fn get_uda(&self, namespace: &str, key: &str) -> Option<&str> {
        #[allow(deprecated)]
        self.get_legacy_uda(uda_tuple_to_string(namespace, key).as_ref())
    }

    /// Get the user defined attributes (UDAs) of this task, in arbitrary order.  Each key is split
    /// on the first `.` character.  Legacy keys that do not contain `.` are represented as `("",
    /// key)`.
    #[deprecated(note = "namespaced UDAs will not be supported in the future")]
    pub fn get_udas(&self) -> impl Iterator<Item = ((&str, &str), &str)> + '_ {
        self.data
            .iter()
            .filter(|(k, _)| !Task::is_known_key(k))
            .map(|(k, v)| (uda_string_to_tuple(k), v.as_ref()))
    }

    /// Get the named user defined attribute (UDA) in a legacy format.  This will return None for
    /// any key defined in the Task data model, regardless of whether it is set or not.
    #[deprecated(note = "please use Task::get_user_defined_attribute")]
    pub fn get_legacy_uda(&self, key: &str) -> Option<&str> {
        self.get_user_defined_attribute(key)
    }

    /// Get the named user defined attribute (UDA). This will return None for any key
    /// defined in the Task data model, regardless of whether it is set or not.
    pub fn get_user_defined_attribute(&self, key: &str) -> Option<&str> {
        if Task::is_known_key(key) {
            return None;
        }
        self.data.get(key)
    }

    /// Like `get_udas`, but returning each UDA key as a single string.
    #[deprecated(note = "please use Task::get_user_defined_attributes")]
    pub fn get_legacy_udas(&self) -> impl Iterator<Item = (&str, &str)> + '_ {
        self.get_user_defined_attributes()
    }

    /// Return each UDA key as a single string.
    pub fn get_user_defined_attributes(&self) -> impl Iterator<Item = (&str, &str)> + '_ {
        self.data
            .iter()
            .filter(|(p, _)| !Task::is_known_key(p))
            .map(|(p, v)| (p.as_ref(), v.as_ref()))
    }

    /// Get the modification time for this task.
    pub fn get_modified(&self) -> Option<Timestamp> {
        self.get_timestamp(Prop::Modified.as_ref())
    }

    /// Get the due time for this task.
    pub fn get_due(&self) -> Option<Timestamp> {
        self.get_timestamp(Prop::Due.as_ref())
    }

    /// Get the UUIDs of tasks on which this task depends.
    ///
    /// This includes all dependencies, regardless of their status.  In fact, it may include
    /// dependencies that do not exist.
    pub fn get_dependencies(&self) -> impl Iterator<Item = Uuid> + '_ {
        self.data.properties().filter_map(|p| {
            if let Some(dep_str) = p.strip_prefix("dep_") {
                if let Ok(u) = Uuid::parse_str(dep_str) {
                    return Some(u);
                }
                // (un-parseable dep_.. properties are ignored)
            }
            None
        })
    }

    /// Get task's property value by name.
    pub fn get_value<S: Into<String>>(&self, property: S) -> Option<&str> {
        let property = property.into();
        self.data.get(property)
    }

    /// Set the task's status.
    ///
    /// This also updates the task's "end" property appropriately.
    pub fn set_status(&mut self, status: Status, ops: &mut Operations) -> Result<()> {
        match status {
            Status::Pending | Status::Recurring => {
                // clear "end" when a task becomes "pending" or "recurring"
                if self.data.has(Prop::End.as_ref()) {
                    self.set_timestamp(Prop::End.as_ref(), None, ops)?;
                }
            }
            Status::Completed | Status::Deleted => {
                // set "end" when a task is deleted or completed
                if !self.data.has(Prop::End.as_ref()) {
                    self.set_timestamp(Prop::End.as_ref(), Some(Utc::now()), ops)?;
                }
            }
            _ => {}
        }
        self.set_value(
            Prop::Status.as_ref(),
            Some(String::from(status.to_taskmap())),
            ops,
        )
    }

    pub fn set_description(&mut self, description: String, ops: &mut Operations) -> Result<()> {
        self.set_value(Prop::Description.as_ref(), Some(description), ops)
    }

    pub fn set_priority(&mut self, priority: String, ops: &mut Operations) -> Result<()> {
        self.set_value(Prop::Priority.as_ref(), Some(priority), ops)
    }

    pub fn set_entry(&mut self, entry: Option<Timestamp>, ops: &mut Operations) -> Result<()> {
        self.set_timestamp(Prop::Entry.as_ref(), entry, ops)
    }

    pub fn set_wait(&mut self, wait: Option<Timestamp>, ops: &mut Operations) -> Result<()> {
        self.set_timestamp(Prop::Wait.as_ref(), wait, ops)
    }

    pub fn set_modified(&mut self, modified: Timestamp, ops: &mut Operations) -> Result<()> {
        self.set_timestamp(Prop::Modified.as_ref(), Some(modified), ops)
    }

    /// Set a tasks's property by name.
    ///
    /// This will automatically update the `modified` timestamp if it has not already been
    /// modified, but will recognize modifications of the `modified` property and not make further
    /// updates to it. Use [`TaskData::update`] to modify the task without this behavior.
    pub fn set_value<S: Into<String>>(
        &mut self,
        property: S,
        value: Option<String>,
        ops: &mut Operations,
    ) -> Result<()> {
        let property = property.into();

        // update the modified timestamp unless we are setting it explicitly
        if &property != "modified" && !self.updated_modified {
            let now = format!("{}", Utc::now().timestamp());
            trace!("task {}: set property modified={:?}", self.get_uuid(), now);
            self.data.update(Prop::Modified.as_ref(), Some(now), ops);
            self.updated_modified = true;
        }
        self.updated_modified = true;

        if let Some(ref v) = value {
            trace!(
                "task {}: set property {}={:?}",
                self.get_uuid(),
                property,
                v
            );
        } else {
            trace!("task {}: remove property {}", self.get_uuid(), property);
        }

        self.data.update(property, value, ops);
        Ok(())
    }

    /// Start the task by setting "start" to the current timestamp, if the task is not already
    /// active.
    pub fn start(&mut self, ops: &mut Operations) -> Result<()> {
        if self.is_active() {
            return Ok(());
        }
        self.set_timestamp(Prop::Start.as_ref(), Some(Utc::now()), ops)
    }

    /// Stop the task by removing the `start` key
    pub fn stop(&mut self, ops: &mut Operations) -> Result<()> {
        self.set_timestamp(Prop::Start.as_ref(), None, ops)
    }

    /// Mark this task as complete
    pub fn done(&mut self, ops: &mut Operations) -> Result<()> {
        self.set_status(Status::Completed, ops)
    }

    /// Mark this task as deleted.
    ///
    /// Note that this does not delete the task.  It merely marks the task as
    /// deleted.
    #[deprecated(
        since = "0.7.0",
        note = "please call `Task::set_status` with `Status::Deleted`"
    )]
    pub fn delete(&mut self, ops: &mut Operations) -> Result<()> {
        self.set_status(Status::Deleted, ops)
    }

    /// Add a tag to this task.  Does nothing if the tag is already present.
    pub fn add_tag(&mut self, tag: &Tag, ops: &mut Operations) -> Result<()> {
        if tag.is_synthetic() {
            return Err(Error::Usage(String::from(
                "Synthetic tags cannot be modified",
            )));
        }
        self.set_value(format!("tag_{}", tag), Some("".to_owned()), ops)
    }

    /// Remove a tag from this task.  Does nothing if the tag is not present.
    pub fn remove_tag(&mut self, tag: &Tag, ops: &mut Operations) -> Result<()> {
        if tag.is_synthetic() {
            return Err(Error::Usage(String::from(
                "Synthetic tags cannot be modified",
            )));
        }
        self.set_value(format!("tag_{}", tag), None, ops)
    }

    /// Add a new annotation.  Note that annotations with the same entry time
    /// will overwrite one another.
    pub fn add_annotation(&mut self, ann: Annotation, ops: &mut Operations) -> Result<()> {
        self.set_value(
            format!("annotation_{}", ann.entry.timestamp()),
            Some(ann.description),
            ops,
        )
    }

    /// Remove an annotation, based on its entry time.
    pub fn remove_annotation(&mut self, entry: Timestamp, ops: &mut Operations) -> Result<()> {
        self.set_value(format!("annotation_{}", entry.timestamp()), None, ops)
    }

    pub fn set_due(&mut self, due: Option<Timestamp>, ops: &mut Operations) -> Result<()> {
        self.set_timestamp(Prop::Due.as_ref(), due, ops)
    }

    /// Set a user-defined attribute (UDA).  This will fail if the key is defined by the data
    /// model.
    #[deprecated(note = "namespaced UDAs will not be supported in the future")]
    pub fn set_uda(
        &mut self,
        namespace: impl AsRef<str>,
        key: impl AsRef<str>,
        value: impl Into<String>,
        ops: &mut Operations,
    ) -> Result<()> {
        let key = uda_tuple_to_string(namespace, key);
        #[allow(deprecated)]
        self.set_legacy_uda(key, value, ops)
    }

    /// Remove a user-defined attribute (UDA).  This will fail if the key is defined by the data
    /// model.
    #[deprecated(note = "namespaced UDAs will not be supported in the future")]
    pub fn remove_uda(
        &mut self,
        namespace: impl AsRef<str>,
        key: impl AsRef<str>,
        ops: &mut Operations,
    ) -> Result<()> {
        let key = uda_tuple_to_string(namespace, key);
        #[allow(deprecated)]
        self.remove_legacy_uda(key, ops)
    }

    /// Set a user-defined attribute (UDA), where the key is a legacy key.
    #[deprecated(note = "please use Task::set_user_defined_attribute")]
    pub fn set_legacy_uda(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
        ops: &mut Operations,
    ) -> Result<()> {
        self.set_user_defined_attribute(key, value, ops)
    }

    /// Set a user-defined attribute (UDA).
    pub fn set_user_defined_attribute(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
        ops: &mut Operations,
    ) -> Result<()> {
        let key = key.into();
        if Task::is_known_key(&key) {
            return Err(Error::Usage(format!(
                "Property name {} as special meaning in a task and cannot be used as a UDA",
                key
            )));
        }
        self.set_value(key, Some(value.into()), ops)
    }

    /// Remove a user-defined attribute (UDA), where the key is a legacy key.
    #[deprecated(note = "please use Task::remove_user_defined_attribute")]
    pub fn remove_legacy_uda(
        &mut self,
        key: impl Into<String>,
        ops: &mut Operations,
    ) -> Result<()> {
        self.remove_user_defined_attribute(key, ops)
    }

    /// Remove a user-defined attribute (UDA).
    pub fn remove_user_defined_attribute(
        &mut self,
        key: impl Into<String>,
        ops: &mut Operations,
    ) -> Result<()> {
        let key = key.into();
        if Task::is_known_key(&key) {
            return Err(Error::Usage(format!(
                "Property name {} as special meaning in a task and cannot be used as a UDA",
                key
            )));
        }
        self.set_value(key, None, ops)
    }

    /// Add a dependency.
    pub fn add_dependency(&mut self, dep: Uuid, ops: &mut Operations) -> Result<()> {
        let key = format!("dep_{}", dep);
        self.set_value(key, Some("".to_string()), ops)
    }

    /// Remove a dependency.
    pub fn remove_dependency(&mut self, dep: Uuid, ops: &mut Operations) -> Result<()> {
        let key = format!("dep_{}", dep);
        self.set_value(key, None, ops)
    }

    /// Get the given timestamp property.
    ///
    /// This will return `None` if the property is not set, or if it is not a valid
    /// timestamp. Otherwise, a correctly parsed Timestamp is returned.
    pub fn get_timestamp(&self, property: &str) -> Option<Timestamp> {
        if let Some(ts) = self.data.get(property) {
            if let Ok(ts) = ts.parse() {
                return Some(utc_timestamp(ts));
            }
            // if the value does not parse as an integer, default to None
        }
        None
    }

    /// Set the given timestamp property, mapping the value correctly.
    pub fn set_timestamp(
        &mut self,
        property: &str,
        value: Option<Timestamp>,
        ops: &mut Operations,
    ) -> Result<()> {
        self.set_value(property, value.map(|v| v.timestamp().to_string()), ops)
    }

    // -- utility functions

    fn is_known_key(key: &str) -> bool {
        Prop::from_str(key).is_ok()
            || key.starts_with("tag_")
            || key.starts_with("annotation_")
            || key.starts_with("dep_")
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod test {
    use super::*;
    use crate::{storage::inmemory::InMemoryStorage, Replica};
    use pretty_assertions::assert_eq;
    use std::collections::HashSet;

    fn dm() -> Arc<DependencyMap> {
        Arc::new(DependencyMap::new())
    }

    // Test task mutation by modifying a task and checking the assertions both on the
    // modified task and on a re-loaded task after the operations are committed. Then,
    // apply the same operations again and check that the result is the same.
    async fn with_mut_task<MODIFY: Fn(&mut Task, &mut Operations), ASSERT: Fn(&Task)>(
        modify: MODIFY,
        assert: ASSERT,
    ) {
        let mut replica = Replica::new(InMemoryStorage::new());
        let mut ops = Operations::new();
        let uuid = Uuid::new_v4();
        let mut task = replica.create_task(uuid, &mut ops).await.unwrap();

        // Modify the task
        modify(&mut task, &mut ops);

        // Check assertions about the task before committing it.
        assert(&task);
        println!("commiting operations from first call to modify function");
        replica.commit_operations(ops).await.unwrap();

        // Check assertions on task loaded from storage
        let mut task = replica.get_task(uuid).await.unwrap().unwrap();
        assert(&task);

        // Apply the operations again, checking that they do not fail.
        let mut ops = Operations::new();
        modify(&mut task, &mut ops);

        // Changes should still be as expected before commit.
        assert(&task);
        println!("commiting operations from second call to modify function");
        replica.commit_operations(ops).await.unwrap();

        // Changes should still be as expected when loaded from storage.
        let task = replica.get_task(uuid).await.unwrap().unwrap();
        assert(&task);
    }

    /// Create a user tag, without checking its validity
    fn utag(name: &'static str) -> Tag {
        Tag::from_inner(TagInner::User(name.into()))
    }

    /// Create a synthetic tag
    fn stag(synth: SyntheticTag) -> Tag {
        Tag::from_inner(TagInner::Synthetic(synth))
    }

    #[test]
    fn test_is_active_never_started() {
        let task = Task::new(TaskData::new(Uuid::new_v4(), TaskMap::new()), dm());
        assert!(!task.is_active());
    }

    #[test]
    fn test_is_active_active() {
        let task = Task::new(
            TaskData::new(
                Uuid::new_v4(),
                vec![(String::from("start"), String::from("1234"))]
                    .drain(..)
                    .collect(),
            ),
            dm(),
        );

        assert!(task.is_active());
    }

    #[test]
    fn test_is_active_inactive() {
        let task = Task::new(TaskData::new(Uuid::new_v4(), Default::default()), dm());
        assert!(!task.is_active());
    }

    #[test]
    fn test_entry_not_set() {
        let task = Task::new(TaskData::new(Uuid::new_v4(), TaskMap::new()), dm());
        assert_eq!(task.get_entry(), None);
    }

    #[test]
    fn test_entry_set() {
        let ts = Utc.with_ymd_and_hms(1980, 1, 1, 0, 0, 0).unwrap();
        let task = Task::new(
            TaskData::new(
                Uuid::new_v4(),
                vec![(String::from("entry"), format!("{}", ts.timestamp()))]
                    .drain(..)
                    .collect(),
            ),
            dm(),
        );
        assert_eq!(task.get_entry(), Some(ts));
    }

    #[test]
    fn test_wait_not_set() {
        let task = Task::new(TaskData::new(Uuid::new_v4(), TaskMap::new()), dm());

        assert!(!task.is_waiting());
        assert_eq!(task.get_wait(), None);
    }

    #[test]
    fn test_wait_in_past() {
        let ts = Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap();
        let task = Task::new(
            TaskData::new(
                Uuid::new_v4(),
                vec![(String::from("wait"), format!("{}", ts.timestamp()))]
                    .drain(..)
                    .collect(),
            ),
            dm(),
        );

        assert!(!task.is_waiting());
        assert_eq!(task.get_wait(), Some(ts));
    }

    #[test]
    fn test_wait_in_future() {
        let ts = Utc.with_ymd_and_hms(3000, 1, 1, 0, 0, 0).unwrap();
        let task = Task::new(
            TaskData::new(
                Uuid::new_v4(),
                vec![(String::from("wait"), format!("{}", ts.timestamp()))]
                    .drain(..)
                    .collect(),
            ),
            dm(),
        );

        assert!(task.is_waiting());
        assert_eq!(task.get_wait(), Some(ts));
    }

    #[test]
    fn test_has_tag() {
        let task = Task::new(
            TaskData::new(
                Uuid::new_v4(),
                vec![
                    (String::from("tag_abc"), String::from("")),
                    (String::from("start"), String::from("1234")),
                ]
                .drain(..)
                .collect(),
            ),
            dm(),
        );

        assert!(task.has_tag(&utag("abc")));
        assert!(!task.has_tag(&utag("def")));
        assert!(task.has_tag(&stag(SyntheticTag::Active)));
        assert!(task.has_tag(&stag(SyntheticTag::Pending)));
        assert!(!task.has_tag(&stag(SyntheticTag::Waiting)));
    }

    #[test]
    fn test_get_tags() {
        let task = Task::new(
            TaskData::new(
                Uuid::new_v4(),
                vec![
                    (String::from("tag_abc"), String::from("")),
                    (String::from("tag_def"), String::from("")),
                    // set `wait` so the synthetic tag WAITING is present
                    (String::from("wait"), String::from("33158909732")),
                ]
                .drain(..)
                .collect(),
            ),
            dm(),
        );

        let tags: HashSet<_> = task.get_tags().collect();
        let exp = HashSet::from([
            utag("abc"),
            utag("def"),
            stag(SyntheticTag::Pending),
            stag(SyntheticTag::Waiting),
            stag(SyntheticTag::Unblocked),
        ]);
        assert_eq!(tags, exp);
    }

    #[test]
    fn test_get_tags_invalid_tags() {
        let task = Task::new(
            TaskData::new(
                Uuid::new_v4(),
                vec![
                    (String::from("tag_ok"), String::from("")),
                    (String::from("tag_"), String::from("")),
                    (String::from("tag_123"), String::from("")),
                    (String::from("tag_a!!"), String::from("")),
                ]
                .drain(..)
                .collect(),
            ),
            dm(),
        );

        // only "ok" is OK
        let tags: HashSet<_> = task.get_tags().collect();
        assert_eq!(
            tags,
            HashSet::from([
                utag("ok"),
                stag(SyntheticTag::Pending),
                stag(SyntheticTag::Unblocked)
            ])
        );
    }

    #[test]
    fn test_get_due() {
        let test_time = Utc.with_ymd_and_hms(2033, 1, 1, 0, 0, 0).unwrap();
        let task = Task::new(
            TaskData::new(
                Uuid::new_v4(),
                vec![(String::from("due"), format!("{}", test_time.timestamp()))]
                    .drain(..)
                    .collect(),
            ),
            dm(),
        );
        assert_eq!(task.get_due(), Some(test_time))
    }

    #[test]
    fn test_get_invalid_due() {
        let task = Task::new(
            TaskData::new(
                Uuid::new_v4(),
                vec![(String::from("due"), String::from("invalid"))]
                    .drain(..)
                    .collect(),
            ),
            dm(),
        );
        assert_eq!(task.get_due(), None);
    }

    #[tokio::test]
    async fn test_due_new_task() {
        with_mut_task(|_task, _ops| {}, |task| assert_eq!(task.get_due(), None)).await;
    }

    #[tokio::test]
    async fn test_add_due() {
        let test_time = Utc.with_ymd_and_hms(2033, 1, 1, 0, 0, 0).unwrap();
        with_mut_task(
            |task, ops| {
                task.set_due(Some(test_time), ops).unwrap();
            },
            |task| assert_eq!(task.get_due(), Some(test_time)),
        )
        .await;
    }

    #[tokio::test]
    async fn test_remove_due() {
        with_mut_task(
            |task, ops| {
                task.data.update("due", Some("some-time".into()), ops);
                assert!(task.data.has("due"));
                task.set_due(None, ops).unwrap();
            },
            |task| {
                assert!(!task.data.has("due"));
            },
        )
        .await;
    }

    #[test]
    fn test_get_priority_default() {
        let task = Task::new(TaskData::new(Uuid::new_v4(), TaskMap::new()), dm());
        assert_eq!(task.get_priority(), "");
    }

    #[test]
    fn test_get_annotations() {
        let task = Task::new(
            TaskData::new(
                Uuid::new_v4(),
                vec![
                    (
                        String::from("annotation_1635301873"),
                        String::from("left message"),
                    ),
                    (
                        String::from("annotation_1635301883"),
                        String::from("left another message"),
                    ),
                    (String::from("annotation_"), String::from("invalid")),
                    (String::from("annotation_abcde"), String::from("invalid")),
                ]
                .drain(..)
                .collect(),
            ),
            dm(),
        );

        let mut anns: Vec<_> = task.get_annotations().collect();
        anns.sort();
        assert_eq!(
            anns,
            vec![
                Annotation {
                    entry: Utc.timestamp_opt(1635301873, 0).unwrap(),
                    description: "left message".into()
                },
                Annotation {
                    entry: Utc.timestamp_opt(1635301883, 0).unwrap(),
                    description: "left another message".into()
                }
            ]
        );
    }

    #[tokio::test]
    async fn test_add_annotation() {
        with_mut_task(
            |task, ops| {
                task.add_annotation(
                    Annotation {
                        entry: Utc.timestamp_opt(1635301900, 0).unwrap(),
                        description: "right message".into(),
                    },
                    ops,
                )
                .unwrap();
            },
            |task| {
                let k = "annotation_1635301900";
                assert_eq!(task.data.get(k).unwrap(), "right message".to_owned());
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_add_annotation_overwrite() {
        with_mut_task(
            |task, ops| {
                task.add_annotation(
                    Annotation {
                        entry: Utc.timestamp_opt(1635301900, 0).unwrap(),
                        description: "right message".into(),
                    },
                    ops,
                )
                .unwrap();
                task.add_annotation(
                    Annotation {
                        entry: Utc.timestamp_opt(1635301900, 0).unwrap(),
                        description: "right message 2".into(),
                    },
                    ops,
                )
                .unwrap();
            },
            |task| {
                let k = "annotation_1635301900";
                assert_eq!(task.data.get(k).unwrap(), "right message 2".to_owned());
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_remove_annotation() {
        with_mut_task(
            |task, ops| {
                task.data
                    .update("annotation_1635301883", Some("left message".into()), ops);
                task.set_value(
                    "annotation_1635301883",
                    Some("left another message".into()),
                    ops,
                )
                .unwrap();

                task.remove_annotation(Utc.timestamp_opt(1635301883, 0).unwrap(), ops)
                    .unwrap();
            },
            |task| {
                let mut anns: Vec<_> = task.get_annotations().collect();
                anns.sort();
                assert_eq!(anns, vec![]);
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_set_get_priority() {
        with_mut_task(
            |task, ops| {
                task.set_priority("H".into(), ops).unwrap();
            },
            |task| {
                assert_eq!(task.get_priority(), "H");
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_get_priority_new_task() {
        with_mut_task(
            |_task, _ops| {},
            |task| {
                assert_eq!(task.get_priority(), "");
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_set_status_pending() {
        with_mut_task(
            |task, ops| {
                task.data.update("status", Some("completed".into()), ops);
                task.data.update("end", Some("right now".into()), ops);
                task.done(ops).unwrap();
                task.set_status(Status::Pending, ops).unwrap();
            },
            |task| {
                assert_eq!(task.get_status(), Status::Pending);
                assert!(!task.data.has("end"));
                assert!(task.has_tag(&stag(SyntheticTag::Pending)));
                assert!(!task.has_tag(&stag(SyntheticTag::Completed)));
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_set_status_recurring() {
        with_mut_task(
            |task, ops| {
                task.data.update("status", Some("completed".into()), ops);
                task.data.update("end", Some("right now".into()), ops);
                task.set_status(Status::Recurring, ops).unwrap();
            },
            |task| {
                assert_eq!(task.get_status(), Status::Recurring);
                assert!(!task.data.has("end"));
                assert!(!task.has_tag(&stag(SyntheticTag::Pending))); // recurring is not +PENDING
                assert!(!task.has_tag(&stag(SyntheticTag::Completed)));
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_set_status_completed() {
        with_mut_task(
            |task, ops| {
                task.set_status(Status::Completed, ops).unwrap();
            },
            |task| {
                assert_eq!(task.get_status(), Status::Completed);
                assert!(task.data.has("end"));
                assert!(!task.has_tag(&stag(SyntheticTag::Pending)));
                assert!(task.has_tag(&stag(SyntheticTag::Completed)));
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_set_status_deleted() {
        with_mut_task(
            |task, ops| {
                task.set_status(Status::Deleted, ops).unwrap();
            },
            |task| {
                assert_eq!(task.get_status(), Status::Deleted);
                assert!(task.data.has("end"));
                assert!(!task.has_tag(&stag(SyntheticTag::Pending)));
                assert!(!task.has_tag(&stag(SyntheticTag::Completed)));
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_set_get_value() {
        let property = "property-name";
        with_mut_task(
            |task, ops| {
                task.set_value(property, Some("value".into()), ops).unwrap();
            },
            |task| {
                assert_eq!(task.get_value(property), Some("value"));
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_set_get_value_none() {
        let property = "property-name";
        with_mut_task(
            |task, ops| {
                task.data.update(property, Some("value".into()), ops);
                task.set_value(property, None, ops).unwrap();
            },
            |task| {
                assert_eq!(task.get_value(property), None);
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_start() {
        with_mut_task(
            |task, ops| {
                task.start(ops).unwrap();
            },
            |task| {
                assert!(task.data.has("start"));
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_stop() {
        with_mut_task(
            |task, ops| {
                task.data.update("start", Some("right now".into()), ops);
                task.stop(ops).unwrap();
            },
            |task| {
                assert!(!task.data.has("start"));
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_done() {
        with_mut_task(
            |task, ops| {
                task.done(ops).unwrap();
            },
            |task| {
                assert_eq!(task.get_status(), Status::Completed);
                assert!(task.data.has("end"));
                assert!(task.has_tag(&stag(SyntheticTag::Completed)));
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_delete() {
        with_mut_task(
            |task, ops| {
                #[allow(deprecated)]
                task.delete(ops).unwrap();
            },
            |task| {
                assert_eq!(task.get_status(), Status::Deleted);
                assert!(task.data.has("end"));
                assert!(!task.has_tag(&stag(SyntheticTag::Completed)));
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_add_tags() {
        with_mut_task(
            |task, ops| {
                task.add_tag(&utag("abc"), ops).unwrap();
            },
            |task| {
                assert!(task.data.has("tag_abc"));
                assert!(task.has_tag(&utag("abc")));
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_remove_tags() {
        with_mut_task(
            |task, ops| {
                task.data.update("tag_abc", Some("x".into()), ops);
                task.remove_tag(&utag("abc"), ops).unwrap();
            },
            |task| {
                assert!(!task.data.has("tag_abc"));
            },
        )
        .await;
    }

    #[test]
    fn test_get_udas() {
        let task = Task::new(
            TaskData::new(
                Uuid::new_v4(),
                vec![
                    ("description".into(), "not a uda".into()),
                    ("modified".into(), "not a uda".into()),
                    ("start".into(), "not a uda".into()),
                    ("status".into(), "not a uda".into()),
                    ("wait".into(), "not a uda".into()),
                    ("start".into(), "not a uda".into()),
                    ("tag_abc".into(), "not a uda".into()),
                    ("dep_1234".into(), "not a uda".into()),
                    ("annotation_1234".into(), "not a uda".into()),
                    ("githubid".into(), "123".into()),
                    ("jira.url".into(), "h://x".into()),
                ]
                .drain(..)
                .collect(),
            ),
            dm(),
        );

        let mut udas: Vec<_> = task.get_udas().collect();
        udas.sort_unstable();
        assert_eq!(
            udas,
            vec![(("", "githubid"), "123"), (("jira", "url"), "h://x")]
        );
    }

    #[test]
    fn test_get_uda() {
        let task = Task::new(
            TaskData::new(
                Uuid::new_v4(),
                vec![
                    ("description".into(), "not a uda".into()),
                    ("githubid".into(), "123".into()),
                    ("jira.url".into(), "h://x".into()),
                ]
                .drain(..)
                .collect(),
            ),
            dm(),
        );

        assert_eq!(task.get_uda("", "description"), None); // invalid UDA
        assert_eq!(task.get_uda("", "githubid"), Some("123"));
        assert_eq!(task.get_uda("jira", "url"), Some("h://x"));
        assert_eq!(task.get_uda("bugzilla", "url"), None);
    }

    #[test]
    fn test_get_legacy_uda() {
        let task = Task::new(
            TaskData::new(
                Uuid::new_v4(),
                vec![
                    ("description".into(), "not a uda".into()),
                    ("dep_1234".into(), "not a uda".into()),
                    ("githubid".into(), "123".into()),
                    ("jira.url".into(), "h://x".into()),
                ]
                .drain(..)
                .collect(),
            ),
            dm(),
        );

        assert_eq!(task.get_legacy_uda("description"), None); // invalid UDA
        assert_eq!(task.get_legacy_uda("dep_1234"), None); // invalid UDA
        assert_eq!(task.get_legacy_uda("githubid"), Some("123"));
        assert_eq!(task.get_legacy_uda("jira.url"), Some("h://x"));
        assert_eq!(task.get_legacy_uda("bugzilla.url"), None);
    }

    #[test]
    fn test_get_user_defined_attribute() {
        let task = Task::new(
            TaskData::new(
                Uuid::new_v4(),
                vec![
                    ("description".into(), "not a uda".into()),
                    ("dep_1234".into(), "not a uda".into()),
                    ("githubid".into(), "123".into()),
                    ("jira.url".into(), "h://x".into()),
                ]
                .drain(..)
                .collect(),
            ),
            dm(),
        );

        assert_eq!(task.get_user_defined_attribute("description"), None); // invalid UDA
        assert_eq!(task.get_user_defined_attribute("dep_1234"), None); // invalid UDA
        assert_eq!(task.get_user_defined_attribute("githubid"), Some("123"));
        assert_eq!(task.get_user_defined_attribute("jira.url"), Some("h://x"));
        assert_eq!(task.get_user_defined_attribute("bugzilla.url"), None);
    }

    #[tokio::test]
    async fn test_set_uda() {
        with_mut_task(
            |task, ops| {
                task.set_uda("jira", "url", "h://y", ops).unwrap();
                task.set_uda("", "jiraid", "TW-1234", ops).unwrap();
            },
            |task| {
                let mut udas: Vec<_> = task.get_udas().collect();
                udas.sort_unstable();
                assert_eq!(
                    udas,
                    vec![(("", "jiraid"), "TW-1234"), (("jira", "url"), "h://y")]
                );
            },
        )
        .await
    }

    #[tokio::test]
    async fn test_set_legacy_uda() {
        with_mut_task(
            |task, ops| {
                task.set_legacy_uda("jira.url", "h://y", ops).unwrap();
                task.set_legacy_uda("jiraid", "TW-1234", ops).unwrap();
            },
            |task| {
                let mut udas: Vec<_> = task.get_udas().collect();
                udas.sort_unstable();
                assert_eq!(
                    udas,
                    vec![(("", "jiraid"), "TW-1234"), (("jira", "url"), "h://y")]
                );
            },
        )
        .await
    }

    #[tokio::test]
    async fn test_set_user_defined_attribute() {
        with_mut_task(
            |task, ops| {
                task.set_user_defined_attribute("jira.url", "h://y", ops)
                    .unwrap();
                task.set_user_defined_attribute("jiraid", "TW-1234", ops)
                    .unwrap();
            },
            |task| {
                let mut udas: Vec<_> = task.get_udas().collect();
                udas.sort_unstable();
                assert_eq!(
                    udas,
                    vec![(("", "jiraid"), "TW-1234"), (("jira", "url"), "h://y")]
                );
            },
        )
        .await
    }

    #[tokio::test]
    async fn test_set_uda_invalid() {
        with_mut_task(
            |task, ops| {
                assert!(task.set_uda("", "modified", "123", ops).is_err());
                assert!(task.set_uda("", "tag_abc", "123", ops).is_err());
                assert!(task.set_legacy_uda("modified", "123", ops).is_err());
                assert!(task.set_legacy_uda("tag_abc", "123", ops).is_err());
                assert!(task
                    .set_user_defined_attribute("modified", "123", ops)
                    .is_err());
                assert!(task
                    .set_user_defined_attribute("tag_abc", "123", ops)
                    .is_err());
            },
            |_task| {},
        )
        .await
    }

    #[tokio::test]
    async fn test_remove_uda() {
        with_mut_task(
            |task, ops| {
                task.data.update("github.id", Some("123".into()), ops);
                task.remove_uda("github", "id", ops).unwrap();
            },
            |task| {
                let udas: Vec<_> = task.get_udas().collect();
                assert_eq!(udas, vec![]);
            },
        )
        .await
    }

    #[tokio::test]
    async fn test_remove_legacy_uda() {
        with_mut_task(
            |task, ops| {
                task.data.update("githubid", Some("123".into()), ops);
                task.remove_legacy_uda("githubid", ops).unwrap();
            },
            |task| {
                let udas: Vec<_> = task.get_udas().collect();
                assert_eq!(udas, vec![]);
            },
        )
        .await
    }

    #[tokio::test]
    async fn test_remove_user_defined_attribute() {
        with_mut_task(
            |task, ops| {
                task.data.update("githubid", Some("123".into()), ops);
                task.remove_user_defined_attribute("githubid", ops).unwrap();
            },
            |task| {
                let udas: Vec<_> = task.get_user_defined_attributes().collect();
                assert_eq!(udas, vec![]);
            },
        )
        .await
    }

    #[tokio::test]
    async fn test_remove_uda_invalid() {
        with_mut_task(
            |task, ops| {
                assert!(task.remove_uda("", "modified", ops).is_err());
                assert!(task.remove_uda("", "tag_abc", ops).is_err());
                assert!(task.remove_legacy_uda("modified", ops).is_err());
                assert!(task.remove_legacy_uda("tag_abc", ops).is_err());
                assert!(task.remove_user_defined_attribute("modified", ops).is_err());
                assert!(task.remove_user_defined_attribute("tag_abc", ops).is_err());
            },
            |_task| {},
        )
        .await
    }

    #[tokio::test]
    async fn test_dependencies_one() {
        let dep1 = Uuid::new_v4();
        with_mut_task(
            |task, ops| {
                task.add_dependency(dep1, ops).unwrap();
            },
            |task| {
                let deps = task.get_dependencies().collect::<Vec<_>>();
                assert!(deps.contains(&dep1));
            },
        )
        .await
    }

    #[tokio::test]
    async fn test_dependencies_two() {
        let dep1 = Uuid::new_v4();
        let dep2 = Uuid::new_v4();
        with_mut_task(
            |task, ops| {
                task.add_dependency(dep1, ops).unwrap();
                task.add_dependency(dep2, ops).unwrap();
            },
            |task| {
                let deps = task.get_dependencies().collect::<Vec<_>>();
                assert!(deps.contains(&dep1));
                assert!(deps.contains(&dep2));
            },
        )
        .await
    }

    #[tokio::test]
    async fn test_dependencies_removed() {
        let dep1 = Uuid::new_v4();
        let dep2 = Uuid::new_v4();
        with_mut_task(
            |task, ops| {
                task.add_dependency(dep1, ops).unwrap();
                task.add_dependency(dep2, ops).unwrap();
                task.remove_dependency(dep2, ops).unwrap();
            },
            |task| {
                let deps = task.get_dependencies().collect::<Vec<_>>();
                assert!(deps.contains(&dep1));
                assert!(!deps.contains(&dep2));
            },
        )
        .await
    }

    #[tokio::test]
    async fn dependencies_tags() {
        let mut rep = Replica::new(InMemoryStorage::new());
        let mut ops = Operations::new();
        let (uuid1, uuid2) = (Uuid::new_v4(), Uuid::new_v4());

        let mut t1 = rep.create_task(uuid1, &mut ops).await.unwrap();
        t1.set_status(Status::Pending, &mut ops).unwrap();
        t1.add_dependency(uuid2, &mut ops).unwrap();

        let mut t2 = rep.create_task(uuid2, &mut ops).await.unwrap();
        t2.set_status(Status::Pending, &mut ops).unwrap();

        rep.commit_operations(ops).await.unwrap();

        // force-refresh depmap
        rep.dependency_map(true).await.unwrap();

        let t1 = rep.get_task(uuid1).await.unwrap().unwrap();
        let t2 = rep.get_task(uuid2).await.unwrap().unwrap();
        assert!(t1.has_tag(&stag(SyntheticTag::Blocked)));
        assert!(!t1.has_tag(&stag(SyntheticTag::Unblocked)));
        assert!(!t1.has_tag(&stag(SyntheticTag::Blocking)));
        assert!(!t2.has_tag(&stag(SyntheticTag::Blocked)));
        assert!(t2.has_tag(&stag(SyntheticTag::Unblocked)));
        assert!(t2.has_tag(&stag(SyntheticTag::Blocking)));
    }

    #[tokio::test]
    async fn set_value_modified() {
        with_mut_task(
            |task, ops| {
                // set the modified property to something in the past..
                task.set_value("modified", Some("1671820000".into()), ops)
                    .unwrap();
                // update another property
                task.set_description("fun times".into(), ops).unwrap();
            },
            |task| {
                // verify the modified property was not updated
                assert_eq!(task.get_value("modified").unwrap(), "1671820000")
            },
        )
        .await
    }
}
