use super::tag::{SyntheticTag, TagInner};
use super::{utc_timestamp, Annotation, Status, Tag, Timestamp};
use crate::depmap::DependencyMap;
use crate::errors::{Error, Result};
use crate::storage::TaskMap;
use crate::task::utc_now;
#[cfg(feature = "iterative-tasks")]
use crate::task::{iter, local_tz, IterType};
use crate::{Operations, TaskData};
use chrono::prelude::*;
use log::trace;
#[cfg(feature = "iterative-tasks")]
use rrule::RRuleSet;
use std::convert::AsRef;
use std::convert::TryInto;
use std::str::FromStr;
use std::sync::Arc;
use uuid::Uuid;

/// Fixed namespace for deriving an iterative task's successor UUID via UUIDv5.
///
/// This is derived (per RFC 4122) as
/// `v5(NAMESPACE_URL, "taskchampion-iterative-task")`.
/// `Uuid::new_v5` is not `const`, so the precomputed value is inlined here. The
/// test verifies it still matches.
#[cfg(feature = "iterative-tasks")]
const ITERATIVE_NAMESPACE: Uuid = Uuid::from_u128(0x6ab813ab_3ff5_56f4_820e_31ada24844af);

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
        format!("{namespace}.{key}")
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

    /// Get the scheduled time a task can be started.
    pub fn get_scheduled(&self) -> Option<Timestamp> {
        self.get_timestamp("scheduled")
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
            SyntheticTag::Pending => match self.get_status() {
                Status::Pending => true,
                #[cfg(feature = "iterative-tasks")]
                Status::Iterative => true,
                _ => false,
            },
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
            TagInner::User(s) => self.data.has(format!("tag_{s}")),
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
                        trace!("success with tag {tag}");
                        return Some(tag);
                    }
                    // note that invalid "tag_*" are ignored
                    trace!("skipped tag {tag}");
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
            #[cfg(feature = "iterative-tasks")]
            Status::Completed if self.get_status() == Status::Iterative => {
                return self.set_iterative_completed(ops);
            }
            Status::Completed | Status::Deleted => {
                // set "end" when a task is deleted or completed
                if !self.data.has(Prop::End.as_ref()) {
                    self.set_timestamp(Prop::End.as_ref(), Some(utc_now()), ops)?;
                }
            }
            #[cfg(feature = "iterative-tasks")]
            Status::Iterative => {
                self.set_iterative_status(ops)?;
            }
            Status::Unknown(_) => {}
        }
        self.set_value(
            Prop::Status.as_ref(),
            Some(String::from(status.to_taskmap())),
            ops,
        )
    }

    /// The task's highest-priority date
    #[cfg(feature = "iterative-tasks")]
    fn schedule_anchor(&self) -> Option<DateTime<Utc>> {
        self.get_due()
            .or_else(|| self.get_scheduled())
            .or_else(|| self.get_wait())
    }

    #[cfg(feature = "iterative-tasks")]
    fn set_iterative_status(&mut self, ops: &mut Operations) -> Result<()> {
        if let Some(iter) = self.data.get("iter") {
            // There is no default iteration type or first date. Choosing them is
            // left to the client.
            match self.data.get("iter_type") {
                Some(t) => {
                    IterType::from_str(t).map_err(|e| {
                        Error::Usage(format!(
                            "iter_type {t:?} is not fixed, fixed+ or chained ({e})."
                        ))
                    })?;
                }
                None => {
                    return Err(Error::Usage(
                        "Iterative tasks require an 'iter_type' of fixed, fixed+ or chained."
                            .into(),
                    ))
                }
            }
            // The highest-priority date the caller set is the first occurrence.
            let Some(anchor_date) = self.schedule_anchor() else {
                return Err(Error::Usage(
                    "Iterative tasks require a 'due', 'scheduled' or 'wait' date.".into(),
                ));
            };
            let dt_start = anchor_date.with_timezone(&local_tz());
            // Check that the `iter` is parseable
            iter::bake(iter, dt_start)?;
            // Set the initial series count if not set. 1-based, since it is a count.
            if self.data.get("iter_count").is_none() {
                self.set_value("iter_count", Some("1".to_string()), ops)?;
            }
            // Stamp `entry` if absent.
            if self.get_entry().is_none() {
                self.set_entry(Some(utc_timestamp(utc_now().timestamp())), ops)?;
            }
            Ok(())
        } else {
            Err(Error::Usage(
                "Iterative tasks require an 'iter' value.".into(),
            ))
        }
    }

    #[cfg(feature = "iterative-tasks")]
    fn set_iterative_completed(&mut self, ops: &mut Operations) -> Result<()> {
        let now = utc_timestamp(utc_now().timestamp());
        let uuid = self.get_uuid();

        // Compute the next occurrence's date from the current schedule before
        // changing anything.
        let iter_type = match self.data.get("iter_type") {
            Some(t) => IterType::from_str(t)
                .map_err(|e| Error::Iterative(format!("Couldn't parse iter type {}", e)))?,
            None => {
                return Err(Error::Iterative(format!(
                    "Task {uuid}: has no iter_type. Set it to fixed, fixed+ or chained with `task edit`."
                )))
            }
        };
        let iter_str = self.data.get("iter").ok_or_else(|| {
            Error::Iterative(format!("Task {uuid}: has iterative status but no iter."))
        })?;
        let unvalidated = iter::str2rrule(iter_str).map_err(|e| {
            Error::Iterative(format!(
                "Task {uuid}: iter {iter_str:?} could not be parsed ({e}).."
            ))
        })?;
        let schedule = iter::without_count(&unvalidated)?;
        // A rule whose UNTIL has passed is an exhausted series rather than an
        // error, so it yields no set instead of failing.
        let anchored_set = |anchor: DateTime<rrule::Tz>| -> Result<Option<RRuleSet>> {
            match schedule.clone().validate(anchor) {
                Ok(rule) => Ok(Some(RRuleSet::new(anchor).rrule(rule))),
                Err(rrule::RRuleError::ValidationError(
                    rrule::ValidationError::UntilBeforeStart { .. },
                )) => Ok(None),
                Err(e) => Err(Error::Iterative(format!(
                    "Task {uuid}: stored iter is not valid for this task's dates ({e})."
                ))),
            }
        };
        // The first occurrence strictly after `cutoff`. This filters the
        // iterator directly because rrule's `after()` is inclusive despite its
        // docs, and `limit()` keeps rrule's iteration guard armed.
        let first_after = |set: RRuleSet, cutoff: DateTime<rrule::Tz>| {
            let set = set.limit();
            (&set).into_iter().find(|d| *d > cutoff).map(|d| d.to_utc())
        };
        // Anchor the schedule off the highest-priority present date.
        let anchor_old = self
            .schedule_anchor()
            .map(|t| t.with_timezone(&local_tz()))
            .ok_or_else(|| {
                Error::Iterative(format!(
                    "Task {uuid}: has no due, scheduled or wait date to schedule from. Add one with `task edit`."
                ))
            })?;
        let now_local = utc_now().with_timezone(&local_tz());
        let next_anchor = match iter_type {
            IterType::Fixed => {
                // First occurrence strictly after the anchor date.
                anchored_set(anchor_old)?.and_then(|set| first_after(set, anchor_old))
            }
            IterType::FixedPlus => {
                // Strictly after both now and the completed occurrence, so it
                // skips missed occurrences but still advances when completed early.
                let cutoff = std::cmp::max(now_local, anchor_old);
                anchored_set(anchor_old)?.and_then(|set| first_after(set, cutoff))
            }
            IterType::Chained => {
                // Chained schedules the next occurrence after the completion
                // time. Anchoring at "now" makes now itself the first
                // occurrence, so it has to be skipped.
                let freq = unvalidated.get_freq();
                let period = |dt: &DateTime<rrule::Tz>| -> Option<(i32, u32)> {
                    match freq {
                        rrule::Frequency::Daily => Some((dt.year(), dt.ordinal())),
                        rrule::Frequency::Weekly => {
                            let w = dt.iso_week();
                            Some((w.year(), w.week()))
                        }
                        rrule::Frequency::Monthly => Some((dt.year(), dt.month())),
                        rrule::Frequency::Yearly => Some((dt.year(), 0)),
                        _ => None,
                    }
                };
                let now_period = if iter::selects_within_period(&unvalidated) {
                    None
                } else {
                    period(&now_local)
                };
                anchored_set(now_local)?.and_then(|set| {
                    let set = set.limit();
                    (&set)
                        .into_iter()
                        .find(|d| match now_period {
                            Some(np) => period(d) != Some(np),
                            None => *d > now_local,
                        })
                        .map(|d| d.to_utc())
                })
            }
        };

        // Spawn the next instance only if the schedule yields a future occurrence
        // and the series has not reached its rrule COUNT length.
        let cap = unvalidated.get_count();
        let pos = match self.data.get("iter_count") {
            Some(s) => s.parse::<u32>().map_err(|e| {
                Error::Iterative(format!(
                    "Task {uuid}: iter_count {s:?} is not a number ({e}). Correct it with `task edit`."
                ))
            })?,
            None => 1,
        };
        if cap.is_none_or(|c| pos.saturating_add(1) <= c) {
            if let Some(next_anchor) = next_anchor {
                self.spawn_successor(next_anchor, anchor_old, pos, now, ops)?;
            }
        }
        // Finally, complete the task.
        self.set_value(
            Prop::Status.as_ref(),
            Some(String::from(Status::Completed.to_taskmap())),
            ops,
        )?;
        self.set_timestamp(Prop::End.as_ref(), Some(now), ops)?;
        self.set_value("iter", None, ops)?;
        self.set_value("iter_type", None, ops)?;
        Ok(())
    }

    /// Build the successor for a completed iterative occurrence.
    ///
    /// The successor is a copy of `self` under a deterministic UUID, its schedule
    /// re-anchored to `next_anchor` and its dates advanced by the same wall-clock
    /// delta. `iter_count` is the completed instance's count, the successor's
    /// `iter_count` is `iter_count + 1`.
    #[cfg(feature = "iterative-tasks")]
    fn spawn_successor(
        &self,
        next_anchor: Timestamp,
        anchor_old: DateTime<rrule::Tz>,
        iter_count: u32,
        now: Timestamp,
        ops: &mut Operations,
    ) -> Result<()> {
        let self_uuid = self.get_uuid();
        let successor_uuid = Uuid::new_v5(&ITERATIVE_NAMESPACE, self_uuid.as_bytes());
        let mut successor = Task::new(
            TaskData::create(successor_uuid, ops),
            Arc::new(DependencyMap::new()),
        );

        // Copy the source properties, except those handled explicitly below.
        for (prop, value) in self.data.iter() {
            let set_below = matches!(
                prop.as_str(),
                "status"
                    | "modified"
                    | "end"
                    | "start"
                    | "due"
                    | "scheduled"
                    | "wait"
                    | "entry"
                    | "iter_count"
                    | "until"
            ) || prop.starts_with("dep_")
                || prop.starts_with("annotation_");
            if !set_below {
                successor.data.update(prop, Some(value.to_owned()), ops);
            }
        }

        successor.set_value(
            Prop::Status.as_ref(),
            Some(String::from(Status::Iterative.to_taskmap())),
            ops,
        )?;

        successor.set_value(
            "iter_count",
            Some(iter_count.saturating_add(1).to_string()),
            ops,
        )?;

        // Advance each present date by the same delta as the anchor, with local
        // DST awareness.
        let naive_delta =
            next_anchor.with_timezone(&local_tz()).naive_local() - anchor_old.naive_local();
        let raw_delta = next_anchor - anchor_old.with_timezone(&Utc);
        let shift = |date: Timestamp| -> Timestamp {
            let naive = date.with_timezone(&local_tz()).naive_local() + naive_delta;
            match local_tz().from_local_datetime(&naive) {
                chrono::LocalResult::Single(t) => t.with_timezone(&Utc),
                chrono::LocalResult::Ambiguous(earliest, _) => earliest.with_timezone(&Utc),
                // Nonexistent local time due to spring-forward, leap seconds etc:
                // fall back to raw UTC delta for this date.
                chrono::LocalResult::None => date + raw_delta,
            }
        };
        if let Some(due) = self.get_due() {
            successor.set_due(Some(shift(due)), ops)?;
        }
        if let Some(scheduled) = self.get_scheduled() {
            successor.set_scheduled(Some(shift(scheduled)), ops)?;
        }
        if let Some(wait) = self.get_wait() {
            successor.set_wait(Some(shift(wait)), ops)?;
        }
        if let Some(until) = self.get_timestamp("until") {
            successor.set_timestamp("until", Some(shift(until)), ops)?;
        }

        successor.set_entry(Some(now), ops)?;
        Ok(())
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

    pub fn set_scheduled(
        &mut self,
        scheduled: Option<Timestamp>,
        ops: &mut Operations,
    ) -> Result<()> {
        self.set_timestamp("scheduled", scheduled, ops)
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
            let now = format!("{}", utc_now().timestamp());
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
        self.set_value(format!("tag_{tag}"), Some("".to_owned()), ops)
    }

    /// Remove a tag from this task.  Does nothing if the tag is not present.
    pub fn remove_tag(&mut self, tag: &Tag, ops: &mut Operations) -> Result<()> {
        if tag.is_synthetic() {
            return Err(Error::Usage(String::from(
                "Synthetic tags cannot be modified",
            )));
        }
        self.set_value(format!("tag_{tag}"), None, ops)
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
                "Property name {key} as special meaning in a task and cannot be used as a UDA"
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
                "Property name {key} as special meaning in a task and cannot be used as a UDA"
            )));
        }
        self.set_value(key, None, ops)
    }

    /// Add a dependency.
    pub fn add_dependency(&mut self, dep: Uuid, ops: &mut Operations) -> Result<()> {
        let key = format!("dep_{dep}");
        self.set_value(key, Some("".to_string()), ops)
    }

    /// Remove a dependency.
    pub fn remove_dependency(&mut self, dep: Uuid, ops: &mut Operations) -> Result<()> {
        let key = format!("dep_{dep}");
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
            || Task::is_iterative_key(key)
    }

    /// Keys the iterative-tasks system uses.
    fn is_iterative_key(key: &str) -> bool {
        key == "iter_count"
    }
}

#[cfg(test)]
#[allow(deprecated)]
mod test {
    use super::*;
    #[cfg(feature = "iterative-tasks")]
    use crate::task::time::mock_tz;
    use crate::{storage::inmemory::InMemoryStorage, task::time::mock_time, Replica};
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
        let taskdata = TaskData::new(
            Uuid::new_v4(),
            vec![
                (String::from("tag_ok"), String::from("")),
                (String::from("tag_"), String::from("")),
                (String::from("tag_123"), String::from("")),
                (String::from("tag_!!a"), String::from("")),
                (String::from("tag_a!!"), String::from("")),
                (String::from("tag_\u{1f980}a"), String::from("")),
                (String::from("tag_\u{1f980}"), String::from("")),
            ]
            .drain(..)
            .collect(),
        );
        trace!("{taskdata:?}");
        let task = Task::new(taskdata, dm());

        // only "ok" is OK
        let tags: HashSet<_> = task.get_tags().collect();
        assert_eq!(
            tags,
            HashSet::from([
                stag(SyntheticTag::Pending),
                utag("a!!"),
                utag("\u{1f980}a"),
                utag("\u{1f980}"),
                stag(SyntheticTag::Unblocked),
                utag("ok"),
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

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_set_status_iterative_basic() {
        let due = Utc.with_ymd_and_hms(2026, 1, 10, 12, 0, 0).unwrap();
        with_mut_task(
            |task, ops| {
                task.data.update("iter", Some("daily".into()), ops);
                task.data.update("iter_type", Some("fixed".into()), ops);
                task.set_due(Some(due), ops).unwrap();
                task.set_status(Status::Iterative, ops).unwrap();
            },
            |task| {
                assert_eq!(task.get_status(), Status::Iterative);
                assert_eq!(task.get_due(), Some(due));
                assert_eq!(task.data.get("iter_type"), Some("fixed"));
                assert_eq!(task.data.get("iter_count"), Some("1"));
            },
        )
        .await;
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_set_status_iterative_requires_iter_type() {
        // There is no default iteration type, so the transition refuses to guess.
        let mut replica = Replica::new(InMemoryStorage::new());
        let mut ops = Operations::new();
        let mut task = replica.create_task(Uuid::new_v4(), &mut ops).await.unwrap();
        task.data.update("iter", Some("daily".into()), &mut ops);
        task.set_due(Some(Utc::now()), &mut ops).unwrap();
        let result = task.set_status(Status::Iterative, &mut ops);
        assert!(matches!(result, Err(Error::Usage(_))));
        assert_eq!(task.data.get("iter_type"), None);
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_set_status_iterative_rejects_invalid_iter_type() {
        let mut replica = Replica::new(InMemoryStorage::new());
        let mut ops = Operations::new();
        let mut task = replica.create_task(Uuid::new_v4(), &mut ops).await.unwrap();
        task.data.update("iter", Some("daily".into()), &mut ops);
        task.data
            .update("iter_type", Some("sideways".into()), &mut ops);
        task.set_due(Some(Utc::now()), &mut ops).unwrap();
        let result = task.set_status(Status::Iterative, &mut ops);
        assert!(matches!(result, Err(Error::Usage(_))));
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_set_status_iterative_uses_due_when_set() {
        let preset_due = Utc.with_ymd_and_hms(2026, 1, 10, 12, 0, 0).unwrap();
        let mut replica = Replica::new(InMemoryStorage::new());
        let mut ops = Operations::new();
        let uuid = Uuid::new_v4();
        let mut task = replica.create_task(uuid, &mut ops).await.unwrap();
        task.set_due(Some(preset_due), &mut ops).unwrap();
        task.data.update("iter", Some("weekly".into()), &mut ops);
        task.data
            .update("iter_type", Some("fixed".into()), &mut ops);
        task.set_status(Status::Iterative, &mut ops).unwrap();
        assert_eq!(task.get_due(), Some(preset_due));
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_set_status_iterative_requires_anchor_date() {
        // There is no default first date, so a task with none of due, scheduled
        // or wait cannot become iterative.
        let mut replica = Replica::new(InMemoryStorage::new());
        let mut ops = Operations::new();
        let mut task = replica.create_task(Uuid::new_v4(), &mut ops).await.unwrap();
        task.data.update("iter", Some("weekdays".into()), &mut ops);
        task.data
            .update("iter_type", Some("fixed".into()), &mut ops);
        let result = task.set_status(Status::Iterative, &mut ops);
        assert!(matches!(result, Err(Error::Usage(_))));
        assert_eq!(task.get_due(), None);
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_set_status_iterative_no_iter() {
        let mut replica = Replica::new(InMemoryStorage::new());
        let mut ops = Operations::new();
        let uuid = Uuid::new_v4();
        let mut task = replica.create_task(uuid, &mut ops).await.unwrap();
        task.data
            .update("iter_type", Some("fixed".into()), &mut ops);
        task.set_due(Some(Utc::now()), &mut ops).unwrap();
        let result = task.set_status(Status::Iterative, &mut ops);
        assert!(matches!(result, Err(Error::Usage(_))));
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_set_status_iterative_invalid_iter() {
        let mut replica = Replica::new(InMemoryStorage::new());
        let mut ops = Operations::new();
        let uuid = Uuid::new_v4();
        let mut task = replica.create_task(uuid, &mut ops).await.unwrap();
        task.data.update("iter", Some("3blarg".into()), &mut ops);
        task.data
            .update("iter_type", Some("fixed".into()), &mut ops);
        task.set_due(Some(Utc::now()), &mut ops).unwrap();
        let result = task.set_status(Status::Iterative, &mut ops);
        assert!(matches!(result, Err(Error::Usage(_))));
    }

    #[cfg(feature = "iterative-tasks")]
    async fn setup_iterative_task(
        iter: &str,
        iter_type: &str,
    ) -> (Replica<InMemoryStorage>, Task, Operations, Uuid) {
        let mut replica = Replica::new(InMemoryStorage::new());
        let mut ops = Operations::new();
        let uuid = Uuid::new_v4();
        let mut task = replica.create_task(uuid, &mut ops).await.unwrap();
        task.data.update("iter", Some(iter.into()), &mut ops);
        task.data
            .update("iter_type", Some(iter_type.into()), &mut ops);
        task.set_due(Some(utc_timestamp(utc_now().timestamp())), &mut ops)
            .unwrap();
        task.set_status(Status::Iterative, &mut ops).unwrap();
        (replica, task, ops, uuid)
    }

    /// UUID of the successor a task spawns when it is completed.
    #[cfg(feature = "iterative-tasks")]
    fn successor_of(uuid: Uuid) -> Uuid {
        Uuid::new_v5(&ITERATIVE_NAMESPACE, uuid.as_bytes())
    }

    #[cfg(feature = "iterative-tasks")]
    #[test]
    fn iterative_namespace_is_derived() {
        // The inlined constant must match its documented RFC 4122 derivation.
        assert_eq!(
            ITERATIVE_NAMESPACE,
            Uuid::new_v5(&Uuid::NAMESPACE_URL, b"taskchampion-iterative-task")
        );
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_complete_without_dates_errors() {
        // A task that has lost all of due, scheduled and wait has nothing to
        // schedule from, so completion reports that rather than inventing a date.
        mock_time::set(time_start());
        let (_replica, mut task, mut ops, uuid) = setup_iterative_task("daily", "fixed").await;
        task.set_due(None, &mut ops).unwrap();
        let result = task.set_status(Status::Completed, &mut ops);
        mock_time::reset();
        assert!(matches!(result, Err(Error::Iterative(_))));
        assert_eq!(task.get_status(), Status::Iterative);
        assert!(ops.iter().all(
            |op| !matches!(op, crate::Operation::Create { uuid: u } if *u == successor_of(uuid))
        ));
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_complete_without_iter_type_errors() {
        mock_time::set(time_start());
        let (_replica, mut task, mut ops, _uuid) = setup_iterative_task("daily", "fixed").await;
        task.set_value("iter_type", None, &mut ops).unwrap();
        let result = task.set_status(Status::Completed, &mut ops);
        mock_time::reset();
        assert!(matches!(result, Err(Error::Iterative(_))));
        assert_eq!(task.get_status(), Status::Iterative);
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_complete_iterative_status_is_completed() {
        // Completing an iterative task makes the handle itself Completed (it is now
        // the log); the live instance is the separate successor.
        let (_, mut task, mut ops, _) = setup_iterative_task("daily", "fixed").await;
        task.set_status(Status::Completed, &mut ops).unwrap();
        assert_eq!(task.get_status(), Status::Completed);
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_complete_iterative_spawns_successor() {
        mock_time::set(time_start());
        let (mut replica, mut task, mut ops, uuid) = setup_iterative_task("daily", "fixed").await;
        task.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();
        mock_time::reset();

        let all = replica.all_tasks().await.unwrap();
        assert_eq!(
            all.len(),
            2,
            "should have the completed log + its successor"
        );

        // `self` is now the completed log: status Completed, end set, no schedule.
        let log = all.get(&uuid).unwrap();
        assert_eq!(log.get_status(), Status::Completed);
        assert!(log.data.has("end"));
        assert_eq!(log.get_value("iter"), None);
        assert_eq!(log.get_value("iter_type"), None);

        // The successor is the new live instance: schedule copied, entry is now,
        // status Iterative.
        let succ = all
            .get(&successor_of(uuid))
            .expect("successor should exist");
        assert_eq!(succ.get_status(), Status::Iterative);
        assert!(succ.get_value("iter").is_some());
        assert!(succ.get_value("iter_type").is_some());
        assert_eq!(
            succ.get_value("entry"),
            Some(time_start().timestamp().to_string().as_str())
        );
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_complete_iterative_unblocks_dependents() {
        // An iterative task with a dependent on it.
        let (mut replica, _, ops, iter_uuid) = setup_iterative_task("daily", "fixed").await;
        replica.commit_operations(ops).await.unwrap();

        let mut ops = Operations::new();
        let dep_uuid = Uuid::new_v4();
        let mut dep_task = replica.create_task(dep_uuid, &mut ops).await.unwrap();
        dep_task.set_status(Status::Pending, &mut ops).unwrap();
        dep_task.add_dependency(iter_uuid, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();

        // Complete the iterative task.
        let mut ops = Operations::new();
        let mut iter_task = replica.get_task(iter_uuid).await.unwrap().unwrap();
        iter_task.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();

        // No dependency rewriting happened: the dependent still points at the same
        // uuid, which is now Completed, so it is unblocked.
        let dep_task = replica.get_task(dep_uuid).await.unwrap().unwrap();
        let deps: Vec<Uuid> = dep_task.get_dependencies().collect();
        assert_eq!(deps, vec![iter_uuid], "dependent edge is unchanged");
        assert!(
            !deps.contains(&successor_of(iter_uuid)),
            "dependent is not rerouted to the successor"
        );
        let completed = replica.get_task(iter_uuid).await.unwrap().unwrap();
        assert_eq!(completed.get_status(), Status::Completed);
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_undo_iterative_completion_leaves_dependent_untouched() {
        // Completing an iterative task never mutates a dependent, so undoing the
        // completion restores prior state exactly (the dependent's modified and dep
        // edge are preserved).
        let (mut replica, _, ops, iter_uuid) = setup_iterative_task("daily", "fixed").await;
        replica.commit_operations(ops).await.unwrap();

        let mut ops = Operations::new();
        let dep_uuid = Uuid::new_v4();
        let mut dep_task = replica.create_task(dep_uuid, &mut ops).await.unwrap();
        dep_task.set_status(Status::Pending, &mut ops).unwrap();
        dep_task.add_dependency(iter_uuid, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();

        let before = replica.get_task(dep_uuid).await.unwrap().unwrap();
        let modified_before = before.get_value("modified").map(str::to_owned);
        let deps_before: Vec<Uuid> = before.get_dependencies().collect();

        // Complete behind an undo point, then undo.
        let mut ops = Operations::new();
        ops.push(crate::Operation::UndoPoint);
        let mut iter_task = replica.get_task(iter_uuid).await.unwrap().unwrap();
        iter_task.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();

        let undo_ops = replica.get_undo_operations().await.unwrap();
        assert!(replica.commit_reversed_operations(undo_ops).await.unwrap());

        let after = replica.get_task(dep_uuid).await.unwrap().unwrap();
        assert_eq!(
            after.get_value("modified").map(str::to_owned),
            modified_before,
            "dependent's modified should be preserved"
        );
        assert_eq!(
            after.get_dependencies().collect::<Vec<_>>(),
            deps_before,
            "dependent's dependency edge should be preserved"
        );
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_complete_iterative_no_rrule_error() {
        let mut replica = Replica::new(InMemoryStorage::new());
        let mut ops = Operations::new();
        let uuid = Uuid::new_v4();
        let mut task = replica.create_task(uuid, &mut ops).await.unwrap();
        // Manually set status=iterative without going through set_status(Iterative),
        // so no "rrule" property is stored.
        task.data
            .update("status", Some("iterative".into()), &mut ops);
        let result = task.set_status(Status::Completed, &mut ops);
        assert!(matches!(result, Err(Error::Iterative(_))));
    }

    // time_start = 2026-01-01 00:00:00 UTC.  Weekly occurrences: Jan 8, Jan 15, Jan 22, Jan 29 …
    // time_twenty_four_days_later = 2026-01-25 00:00:00 UTC (task is ~2.5 weeks overdue when completed).
    #[cfg(feature = "iterative-tasks")]
    fn time_start() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
    }
    #[cfg(feature = "iterative-tasks")]
    fn time_twenty_four_days_later() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 25, 0, 0, 0).unwrap()
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_fixed_advances_from_schedule() {
        mock_time::set(time_start());
        let (mut replica, mut task, mut ops, uuid) = setup_iterative_task("weekly", "fixed").await;
        mock_time::set(time_twenty_four_days_later());
        task.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();
        mock_time::reset();
        // Fixed: orig_due is Jan 1; first weekly occurrence strictly after Jan 1 = Jan 8.
        let succ = replica.get_task(successor_of(uuid)).await.unwrap().unwrap();
        assert_eq!(
            succ.get_due(),
            Some(time_start() + chrono::Duration::weeks(1))
        );
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_fixed_plus_advances_from_now() {
        mock_time::set(time_start());
        let (mut replica, mut task, mut ops, uuid) = setup_iterative_task("weekly", "fixed+").await;
        mock_time::set(time_twenty_four_days_later());
        task.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();
        mock_time::reset();
        // FixedPlus: first weekly occurrence strictly after now (Jan 25) = Jan 29
        let succ = replica.get_task(successor_of(uuid)).await.unwrap().unwrap();
        assert_eq!(
            succ.get_due(),
            Some(time_start() + chrono::Duration::weeks(4))
        );
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_fixed_plus_advances_when_completed_early() {
        // The first due date is at Jan 1. Complete it early. FixedPlus must still
        // advance past the occurrence just completed rather than returning it
        // again.
        mock_time::set(time_start());
        let (mut replica, mut task, mut ops, uuid) = setup_iterative_task("weekly", "fixed+").await;
        mock_time::set(time_start() - chrono::Duration::days(1));
        task.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();
        mock_time::reset();
        // Next weekly occurrence strictly after the completed one (Jan 1) = Jan 8.
        let succ = replica.get_task(successor_of(uuid)).await.unwrap().unwrap();
        assert_eq!(
            succ.get_due(),
            Some(time_start() + chrono::Duration::weeks(1))
        );
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_chained_advances_period_from_now() {
        mock_time::set(time_start());
        let (mut replica, mut task, mut ops, uuid) =
            setup_iterative_task("weekly", "chained").await;
        mock_time::set(time_twenty_four_days_later());
        task.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();
        mock_time::reset();
        // Chained: rule anchored to now (Jan 25), first occurrence after now = Feb 1
        let succ = replica.get_task(successor_of(uuid)).await.unwrap().unwrap();
        assert_eq!(
            succ.get_due(),
            Some(time_twenty_four_days_later() + chrono::Duration::weeks(1))
        );
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_chained_byday_advances_within_the_period() {
        mock_tz::set(rrule::Tz::UTC);
        // 2026-01-05 is a Monday.
        let monday = Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap();
        mock_time::set(monday);
        let (mut replica, mut task, mut ops, uuid) =
            setup_iterative_task("FREQ=WEEKLY;BYDAY=MO,WE,FR", "chained").await;
        // The first due is the occurrence on or after now, i.e. that Monday.
        assert_eq!(task.get_due(), Some(monday));
        task.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();
        mock_time::reset();
        mock_tz::reset();

        let succ = replica.get_task(successor_of(uuid)).await.unwrap().unwrap();
        assert_eq!(
            succ.get_due(),
            Some(Utc.with_ymd_and_hms(2026, 1, 7, 0, 0, 0).unwrap()),
            "chained Mon/Wed/Fri completed on Monday should advance to Wednesday"
        );
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_chained_weekdays_advances_a_day_not_a_week() {
        mock_tz::set(rrule::Tz::UTC);
        for (completed_on, expected_next) in [
            // Monday -> Tuesday.
            ((2026, 1, 5), (2026, 1, 6)),
            // Friday -> the following Monday.
            ((2026, 1, 9), (2026, 1, 12)),
        ] {
            let now = Utc
                .with_ymd_and_hms(completed_on.0, completed_on.1, completed_on.2, 0, 0, 0)
                .unwrap();
            mock_time::set(now);
            let (mut replica, mut task, mut ops, uuid) =
                setup_iterative_task("weekdays", "chained").await;
            assert_eq!(task.get_due(), Some(now));
            task.set_status(Status::Completed, &mut ops).unwrap();
            replica.commit_operations(ops).await.unwrap();

            let succ = replica.get_task(successor_of(uuid)).await.unwrap().unwrap();
            assert_eq!(
                succ.get_due(),
                Some(
                    Utc.with_ymd_and_hms(
                        expected_next.0,
                        expected_next.1,
                        expected_next.2,
                        0,
                        0,
                        0
                    )
                    .unwrap()
                ),
                "chained weekdays completed on {completed_on:?}"
            );
        }
        mock_time::reset();
        mock_tz::reset();
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_chained_then_fixed_anchors_to_current_due() {
        // A task completed as Chained advances its successor's due to a new
        // weekday. Switching that successor to Fixed must then anchor off its
        // current due, not a stale original anchor.
        mock_time::set(time_start());
        let (mut replica, mut task, mut ops, uuid) =
            setup_iterative_task("weekly", "chained").await;
        // Complete once as Chained: successor due = Jan 25 + 1 week = Feb 1.
        mock_time::set(time_twenty_four_days_later());
        task.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();
        let succ_uuid = successor_of(uuid);
        let chained_due = replica
            .get_task(succ_uuid)
            .await
            .unwrap()
            .unwrap()
            .get_due()
            .unwrap();
        assert_eq!(
            chained_due,
            time_twenty_four_days_later() + chrono::Duration::weeks(1)
        );
        // Switch the successor to Fixed and complete it. Fixed anchors off its
        // current due (Feb 1), so its successor's due is one week on (Feb 8).
        let mut ops = Operations::new();
        let mut succ = replica.get_task(succ_uuid).await.unwrap().unwrap();
        succ.set_value("iter_type", Some("fixed".into()), &mut ops)
            .unwrap();
        succ.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();
        mock_time::reset();
        let succ2 = replica
            .get_task(successor_of(succ_uuid))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            succ2.get_due(),
            Some(chained_due + chrono::Duration::weeks(1))
        );
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_iterative_is_pending_synthetic_tag() {
        let (_replica, task, _ops, _uuid) = setup_iterative_task("weekly", "fixed").await;
        assert_eq!(task.get_status(), Status::Iterative);
        assert!(task.has_tag(&stag(SyntheticTag::Pending)));
        assert!(!task.has_tag(&stag(SyntheticTag::Completed)));
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_exhausted_rrule_completes_without_successor() {
        // A finite RRULE (COUNT/UNTIL) must complete the task as its final
        // occurrence.
        mock_time::set(time_start());
        let (mut replica, mut task, mut ops, uuid) =
            setup_iterative_task("FREQ=DAILY;COUNT=1", "fixed").await;
        // Re-assert the mocked time after the setup await (the thread-local may
        // not survive it), so completion anchors at the mocked time.
        mock_time::set(time_start());
        // Previously this returned Err and left the task stuck as Iterative.
        task.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();
        mock_time::reset();
        // The original task is Completed, not stuck as Iterative.
        let done = replica.get_task(uuid).await.unwrap().unwrap();
        assert_eq!(done.get_status(), Status::Completed);
        // No successor was spawned for the exhausted schedule.
        assert!(replica
            .get_task(successor_of(uuid))
            .await
            .unwrap()
            .is_none());
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_chained_daily_keeps_local_wall_clock_across_dst() {
        use chrono::Timelike;
        // 2026-03-07 13:00 UTC = 08:00 EST, the day before US spring-forward
        let completed_at = Utc.with_ymd_and_hms(2026, 3, 7, 13, 0, 0).unwrap();
        mock_tz::set(rrule::Tz::America__New_York);
        mock_time::set(completed_at);
        let (mut replica, mut task, mut ops, uuid) = setup_iterative_task("daily", "fixed").await;
        // Re-assert the mocked zone and time after the setup.
        mock_tz::set(rrule::Tz::America__New_York);
        mock_time::set(completed_at);
        task.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();
        mock_time::reset();
        let succ = replica.get_task(successor_of(uuid)).await.unwrap().unwrap();
        let due = succ.get_due().unwrap();
        mock_tz::reset();
        // Next daily occurrence is 08:00 local the following day, after
        // after spring-forward.
        let due_local = due.with_timezone(&rrule::Tz::America__New_York);
        assert_eq!(
            due_local.hour(),
            8,
            "daily task keeps 08:00 local, got {due_local}"
        );
        assert_eq!(
            due_local.date_naive(),
            chrono::NaiveDate::from_ymd_opt(2026, 3, 8).unwrap()
        );
        assert_eq!(due, Utc.with_ymd_and_hms(2026, 3, 8, 12, 0, 0).unwrap());
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_scheduled_wait_keep_wall_clock_across_dst() {
        use chrono::Timelike;
        let ny = rrule::Tz::America__New_York;
        // Anchor `due` = Fri 2026-03-06 12:00 EST, the week before US spring-forward.
        let due = Utc.with_ymd_and_hms(2026, 3, 6, 17, 0, 0).unwrap();
        mock_tz::set(ny);
        mock_time::set(due);
        let (mut replica, mut task, mut ops, uuid) = setup_iterative_task("weekly", "fixed").await;
        mock_tz::set(ny);
        mock_time::set(due);
        // `scheduled` 3 days before due: its span (03-03 -> 03-10) crosses the
        // 03-08 boundary. `wait` 10 days before due: its span (02-24 -> 03-03)
        // does NOT cross it, which is where a raw UTC delta drifts the wall clock.
        task.set_due(Some(due), &mut ops).unwrap();
        task.set_scheduled(Some(due - chrono::Duration::days(3)), &mut ops)
            .unwrap();
        task.set_wait(Some(due - chrono::Duration::days(10)), &mut ops)
            .unwrap();
        task.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();
        mock_time::reset();

        let succ = replica.get_task(successor_of(uuid)).await.unwrap().unwrap();
        let new_due = succ.get_due().unwrap().with_timezone(&ny);
        let new_scheduled = succ.get_scheduled().unwrap().with_timezone(&ny);
        let new_wait = succ.get_wait().unwrap().with_timezone(&ny);
        mock_tz::reset();

        // Fixed weekly: next due is 2026-03-13 12:00 EDT. Every date keeps its
        // 12:00 local time, including `wait` whose span does not cross the DST
        // boundary the anchor crosses.
        assert_eq!(new_due.hour(), 12, "due keeps 12:00 local, got {new_due}");
        assert_eq!(
            new_scheduled.hour(),
            12,
            "scheduled keeps 12:00 local, got {new_scheduled}"
        );
        assert_eq!(
            new_wait.hour(),
            12,
            "wait keeps 12:00 local, got {new_wait}"
        );
        // Calendar-day spacing is preserved (due-3d, due-10d).
        assert_eq!(
            new_due.date_naive(),
            chrono::NaiveDate::from_ymd_opt(2026, 3, 13).unwrap()
        );
        assert_eq!(
            new_scheduled.date_naive(),
            chrono::NaiveDate::from_ymd_opt(2026, 3, 10).unwrap()
        );
        assert_eq!(
            new_wait.date_naive(),
            chrono::NaiveDate::from_ymd_opt(2026, 3, 3).unwrap()
        );
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_scheduled_and_wait_advance_fixed() {
        // `scheduled` and `wait` advance by the same delta as the anchoring
        // `due`, so their spacing relative to `due` is preserved.
        mock_time::set(time_start());
        let (mut replica, mut task, mut ops, uuid) = setup_iterative_task("weekly", "fixed").await;
        mock_time::set(time_start());
        task.set_scheduled(Some(time_start() - chrono::Duration::days(2)), &mut ops)
            .unwrap();
        task.set_wait(Some(time_start() - chrono::Duration::days(5)), &mut ops)
            .unwrap();
        mock_time::set(time_twenty_four_days_later());
        task.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();
        mock_time::reset();

        let succ = replica.get_task(successor_of(uuid)).await.unwrap().unwrap();
        let due = succ.get_due().unwrap();
        // Fixed anchors off the original due (Jan 1); next weekly is Jan 8.
        assert_eq!(due, time_start() + chrono::Duration::weeks(1));
        assert_eq!(
            succ.get_scheduled(),
            Some(due - chrono::Duration::days(2)),
            "scheduled keeps its 2-day lead on due"
        );
        assert_eq!(
            succ.get_wait(),
            Some(due - chrono::Duration::days(5)),
            "wait keeps its 5-day lead on due"
        );
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_scheduled_and_wait_advance_fixed_plus() {
        mock_time::set(time_start());
        let (mut replica, mut task, mut ops, uuid) = setup_iterative_task("weekly", "fixed+").await;
        mock_time::set(time_start());
        task.set_scheduled(Some(time_start() - chrono::Duration::days(2)), &mut ops)
            .unwrap();
        task.set_wait(Some(time_start() - chrono::Duration::days(5)), &mut ops)
            .unwrap();
        mock_time::set(time_twenty_four_days_later());
        task.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();
        mock_time::reset();

        let succ = replica.get_task(successor_of(uuid)).await.unwrap().unwrap();
        let due = succ.get_due().unwrap();
        // FixedPlus skips missed occurrences: first weekly after now (Jan 25) is Jan 29.
        assert_eq!(due, time_start() + chrono::Duration::weeks(4));
        assert_eq!(succ.get_scheduled(), Some(due - chrono::Duration::days(2)));
        assert_eq!(succ.get_wait(), Some(due - chrono::Duration::days(5)));
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_scheduled_and_wait_advance_chained() {
        mock_time::set(time_start());
        let (mut replica, mut task, mut ops, uuid) =
            setup_iterative_task("weekly", "chained").await;
        mock_time::set(time_start());
        task.set_scheduled(Some(time_start() - chrono::Duration::days(2)), &mut ops)
            .unwrap();
        task.set_wait(Some(time_start() - chrono::Duration::days(5)), &mut ops)
            .unwrap();
        mock_time::set(time_twenty_four_days_later());
        task.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();
        mock_time::reset();

        let succ = replica.get_task(successor_of(uuid)).await.unwrap().unwrap();
        let due = succ.get_due().unwrap();
        // Chained anchors to now (Jan 25); next weekly is Feb 1.
        assert_eq!(
            due,
            time_twenty_four_days_later() + chrono::Duration::weeks(1)
        );
        assert_eq!(succ.get_scheduled(), Some(due - chrono::Duration::days(2)));
        assert_eq!(succ.get_wait(), Some(due - chrono::Duration::days(5)));
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_count_limits_series_length() {
        mock_time::set(time_start());
        let (mut replica, mut task, mut ops, uuid) =
            setup_iterative_task("FREQ=DAILY;COUNT=2", "fixed").await;
        mock_time::set(time_start());
        task.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();

        let succ_uuid = successor_of(uuid);
        let succ = replica.get_task(succ_uuid).await.unwrap().unwrap();
        assert_eq!(
            succ.get_value("iter"),
            Some("FREQ=DAILY;COUNT=2"),
            "successor carries the schedule unchanged"
        );
        assert_eq!(
            succ.get_value("iter_count"),
            Some("2"),
            "successor is the second instance in the series"
        );

        // Complete the second (final) instance; the count is now exhausted.
        mock_time::set(time_start());
        let mut ops = Operations::new();
        let mut succ = replica.get_task(succ_uuid).await.unwrap().unwrap();
        succ.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();
        mock_time::reset();

        assert_eq!(
            replica
                .get_task(succ_uuid)
                .await
                .unwrap()
                .unwrap()
                .get_status(),
            Status::Completed
        );
        assert!(
            replica
                .get_task(successor_of(succ_uuid))
                .await
                .unwrap()
                .is_none(),
            "no third instance is spawned once COUNT is exhausted"
        );
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_blank_iter_rejected() {
        // A blank `iter` has no schedule, so setting Iterative status is an error.
        let mut replica = Replica::new(InMemoryStorage::new());
        let mut ops = Operations::new();
        let uuid = Uuid::new_v4();
        let mut task = replica.create_task(uuid, &mut ops).await.unwrap();
        task.data.update("iter", Some("".into()), &mut ops);
        task.data
            .update("iter_type", Some("fixed".into()), &mut ops);
        task.set_due(Some(Utc::now()), &mut ops).unwrap();
        let result = task.set_status(Status::Iterative, &mut ops);
        assert!(matches!(result, Err(Error::Usage(_))));
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_status_roundtrip_preserves_series_position() {
        // A live successor's series position is stored in `iter_count`. Toggling
        // its status Iterative -> Pending -> Iterative re-bakes the rule from
        // `iter` but leaves `iter_count` untouched, so the series stays capped.
        mock_time::set(time_start());
        let (mut replica, mut task, mut ops, uuid) =
            setup_iterative_task("FREQ=DAILY;COUNT=2", "fixed").await;
        mock_time::set(time_start());
        task.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();
        mock_time::reset();

        let succ_uuid = successor_of(uuid);
        let mut succ = replica.get_task(succ_uuid).await.unwrap().unwrap();
        assert_eq!(succ.get_value("iter_count"), Some("2"));

        // Round-trip the successor's status through Pending and back.
        let mut ops = Operations::new();
        succ.set_status(Status::Pending, &mut ops).unwrap();
        succ.set_status(Status::Iterative, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();

        let succ = replica.get_task(succ_uuid).await.unwrap().unwrap();
        assert_eq!(
            succ.get_value("iter_count"),
            Some("2"),
            "status round-trip must not change the series position"
        );
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_iter_count_increments_across_the_series() {
        // Each instance records its 1-based position in `iter_count`, the
        // completed record keeps its own position, and successors keep counting up.
        mock_time::set(time_start());
        let (mut replica, mut task, mut ops, uuid) = setup_iterative_task("daily", "fixed").await;
        mock_time::set(time_start());
        task.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();

        // The completed root keeps its own position (1).
        let root = replica.get_task(uuid).await.unwrap().unwrap();
        assert_eq!(
            root.get_value("iter_count"),
            Some("1"),
            "the completed record keeps its position"
        );

        // The second instance is position 2.
        let succ2_uuid = successor_of(uuid);
        let mut succ2 = replica.get_task(succ2_uuid).await.unwrap().unwrap();
        assert_eq!(succ2.get_value("iter_count"), Some("2"));

        // Completing it spawns a third instance at position 3.
        mock_time::set(time_start());
        let mut ops = Operations::new();
        succ2.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();
        mock_time::reset();

        let succ3 = replica
            .get_task(successor_of(succ2_uuid))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(succ3.get_value("iter_count"), Some("3"));
    }

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_anchors_off_scheduled_when_no_due() {
        // With no `due`, the highest-priority present date (`scheduled`) anchors
        // the schedule and advances; the successor still has no `due`.
        mock_time::set(time_start());
        let (mut replica, mut task, mut ops, uuid) = setup_iterative_task("weekly", "fixed").await;
        mock_time::set(time_start());
        task.set_due(None, &mut ops).unwrap();
        task.set_scheduled(Some(time_start()), &mut ops).unwrap();
        mock_time::set(time_twenty_four_days_later());
        task.set_status(Status::Completed, &mut ops).unwrap();
        replica.commit_operations(ops).await.unwrap();
        mock_time::reset();

        let succ = replica.get_task(successor_of(uuid)).await.unwrap().unwrap();
        assert_eq!(succ.get_due(), None);
        assert_eq!(
            succ.get_scheduled(),
            Some(time_start() + chrono::Duration::weeks(1)),
            "scheduled anchors the schedule and advances one week"
        );
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

    #[cfg(feature = "iterative-tasks")]
    #[tokio::test]
    async fn test_iterative_keys_not_udas() {
        with_mut_task(
            |task, ops| {
                assert!(task
                    .set_user_defined_attribute("iter_count", "1", ops)
                    .is_err());
                task.set_user_defined_attribute("iter", "weekly", ops)
                    .unwrap();
                task.set_user_defined_attribute("iter_type", "fixed", ops)
                    .unwrap();
                task.set_value("iter_count", Some("1".into()), ops).unwrap();
            },
            |task| {
                let keys: Vec<&str> = task.get_user_defined_attributes().map(|(k, _)| k).collect();
                assert!(keys.contains(&"iter"));
                assert!(keys.contains(&"iter_type"));
                assert!(!keys.contains(&"iter_count"));
            },
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
