# Task Model

Tasks are stored internally as a key/value map with string keys and values.
Display layers should apply appropriate defaults where necessary.

## Validity

_Any_ key/value map is a valid task, including an empty task.
Consumers of task data must make a best effort to interpret any map, even if it contains apparently contradictory information.
For example, a task with status "completed" but no "end" key present should be interpreted as completed at an unknown time.

## Atomicity

Replicas only synchronize with one another occasionally, so it is impossible to know the "current" state of a task with certainty.
This makes some kinds of modifications challenging.
For example, suppose task tags were updated by reading a list of tags from a property of the key/value map, adding a tag, and writing the result back.
Suppose two such modifications are made in different replicas, one setting `tags` to "oldtag,newtag1" and one setting `tags` to "oldtag,newtag2".
Reconciling these two changes on a sync operation would result in one change winning, losing one of the new tags.

The key names given below avoid this issue, allowing user updates such as adding a tag or deleting a dependency to be represented in a single modification.

## Value Representations

Integers are stored in decimal notation.

Timestamps are stored as UNIX epoch timestamps, in the form of an integer.

## Keys

The following keys, and key formats, are defined:

* `status` - one of `pending` for a pending task (the default), `completed`, `deleted`, or `recurring`
* `description` - the one-line summary of the task
* `modified` - the time of the last modification of this task
* `start` - the most recent time at which this task was started (a task with no `start` key is not active)
* `end` - if present, the time at which this task was completed or deleted (note that this key may not agree with `status`: it may be present for a pending task, or absent for a deleted or completed task)
* `tag_<tag>` - indicates this task has tag `<tag>` (value is ignored)
* `wait` - indicates the time before which this task should be hidden, as it is not actionable
* `entry` - the time at which the task was created
* `annotation_<timestamp>` - value is an annotation created at the given time; for example, `annotation_1693329505`.
* `dep_<uuid>` - indicates this task depends on another task identified by `<uuid>`; the value is ignored; for example, `dep_8c4fed9c-c0d2-40c2-936d-36fc44e084a0`

Note that while TaskChampion recognizes "R" as a status, it does not implement recurrence directly.

### UDAs

Any unrecognized keys are treated as "user-defined attributes" (UDAs).
These attributes can be used to store additional data associated with a task.
For example, applications that synchronize tasks with other systems such as calendars or team planning services might store unique identifiers for those systems as UDAs.
The application defining a UDA defines the format of the value.

UDAs _should_ have a namespaced structure of the form `<namespace>.<key>`, where `<namespace>` identifies the application defining the UDA.
For example, a service named "DevSync" synchronizing tasks from GitHub might use UDAs like `devsync.github.issue-id`.
Note that many existing UDAs for Taskwarrior integrations do not follow this pattern; these are referred to as legacy UDAs.
