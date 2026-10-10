# TaskChampion Iterative Tasks

Iterative tasks are tasks that repeat on a schedule. Completing one closes it and creates the next instance in the series.

## Iteration Handling in TaskChampion

All task iteration is handled in TaskChampion. Front ends only need to accept the `Iterative` task status and call the provided `Task::set_status()/done()` functions instead of directly manipulating raw `TaskData`. Front ends such as TaskWarrior that do not support iteration see these tasks as an unrecognized status and generally ignore them.

## Iteration Types

There are three types of iteration supported by iterative tasks.

### Fixed

When a `fixed` interval task is completed, the next due date is set to the next interval from when it was originally due. As an example, if rent is due on the 20th of the month, the next due date will be on the 20th of the next month, regardless of if I paid it on the 15th, the 20th or the 25th.

### Fixed+

When a `fixed+` interval task is completed, the next due date is set to the next interval from when it was originally due _on or after right now_. For example, if I have to take out the garbage bins every week but miss two because I'm on vacation, I shouldn't take them out multiple days in a row to make up for it.

### Chained

When a chained interval task is completed, the next due date is set to the next interval from when it was completed. As an example, if I normally get a haircut every six weeks and I got one today, my next one should be six weeks from today, even if I got this one three weeks late, or two days early.

The iteration type is stored in the `iter_type` property, which can be one of `fixed`, `fixed+`, and `chained`. Each also accepts a short form: `fx`, `f+` or `fp`, and `ch`.

TaskChampion has no default `iter_type`, so choosing one is left to the front end.

## Dates and Counts

Four optional date properties interact with iterative tasks.

- `due` - The hard date that a task must be completed by. e.g. rent is due on the 15th.
- `scheduled` - The date after which a task can be done. e.g. Christmas decorations can't be put up until after Thanksgiving
- `wait` - Hides a task until after the `wait` so as to not clutter up the task list.
- `until` - When the task should be closed because its deadline was missed by too much. Closing a task that is past its `until` is the responsibility of the front end, not TaskChampion. To end a whole series on a date, use the rule's `UNTIL` option instead.

Front ends generally assume, but do not strictly enforce, that `due` >= `scheduled` >= `wait`, i.e. a task should be `due` after it is `scheduled`, and shouldn't be either of those while `wait`ing.

When an iterative task is closed, the `due`, `scheduled`, `wait` and `until` are all advanced by the same amount of time if they exist. The advancement period is calculated for the highest priority date (the "anchor"), and then the others are moved forward by the same time delta. This preserves the spacing between them.

Additionally, each iterative task records its position in the series in an `iter_count` property. This is used to end iterative tasks that have a specific number of iterations requested. 

## RRULE Based Iteration Definition

There are a lot of ways to define iteration periods, which can be extremely complex. "Every four years on the second Tuesday in November", "The first weekday that isn't a Monday, on or after the 15th of April", and "Weekly on Monday, Tuesday, Thursday and Friday, except for the third Friday of the month" are all real-world examples.

RFC 5545 Section 3.8.5.3 defines RRules for exactly this. RRules are capable of representing many, though of course not all, iteration periods and there is a well tested RRule library for Rust.

Combining RRules with the iteration types takes a bit of thought. The rule is derived in an unbaked, anchor-independent form (no `DTSTART`), so it carries no anchor of its own. The anchor is the task's highest-priority date (`due` > `scheduled` > `wait`). On completion the rule is re-anchored to compute the next occurrence for that date: fixed anchors off the current anchor date, fixed+ also anchors off the anchor date but advances to the next occurrence on or after now, and chained anchors off the completion time ("now"). The remaining dates then shift by the same delta, as described above.

Counting up in the `iter_count` property, rather than decrementing the `COUNT` of the rrule, keeps `iter` as it was originally written. Since the rule is not stored, the only place left to decrement is `iter` itself, and of the four accepted forms only a raw RRULE could carry a decremented count. A shorthand, an ISO-8601 duration and a natural-language phrase have nowhere to put one, so decrementing would mean replacing the user's own text with a generated RRULE on the first completion. Counting up also records position rather than remainder, so an instance knows it is the third of five.

Note: RRules represent time to the nearest whole second, so all iterative task dates and times are also tracked to the nearest whole second.

### RRule Generation

For all the advantages of RRules, they are a unique syntax that (almost) no one wants to write regularly. The `iter` value is therefore accepted in four flavors:

1. Raw RRules, for people who wish to use them directly.
2. TaskWarrior-style duration shorthand, such as `daily`, `3wk`, `weekdays`, `fortnight`, `2year`, etc.
3. ISO-8601 durations, such as `P2W`, `P3D` or `PT12H`, as long as they don't mix months or years with smaller units, such as `P1Y3D`, or a duration combining weeks with any other component, such as `P2W3D`, which ISO-8601 also doesn't allow.
4. Free-form natural language: handled by the [`text2rrule`](https://crates.io/crates/text2rrule) crate. Phrases such as `every Monday`, `every other Tuesday`, or `every two weeks on friday` are parsed into an RRULE string and then into an `RRule` value. `text2rrule` supports multiple locales.

The rrule generator tries to parse the `iter` value as raw RRULE first, then the TaskWarrior shorthand parser, then ISO-8601, then `text2rrule`.

## Iterative Task Flow

At any moment a series has one live Iterative task, handled like any other task. Each time it is completed it is replaced by a successor with a new deterministically derived UUID, so the live UUID advances over the life of the series. The completed instances remain as records. Iterative tasks need special handling in only two places.

### Creation / Status Change

To make a task iterative with `Task::new` or `Task::set_status`, it must have a non-empty `iter`, an `iter_type`, and at least one of `due`, `scheduled` or `wait`. If any of these is missing or invalid, the call returns an error and the status is not changed. When the status is set to Iterative, the `iter` value is checked that it can be parsed into a useable rrule, but the parsed rule is not stored. Instead it is derived again from `iter` when the task is completed, so an edit to `iter` always takes effect, however it was made. An `iter_count` property is set to 1 and each successor task increments it.

The highest-priority of the `due`, `scheduled`, or `wait` dates is used as the first occurrence.

At this point, an iterative task acts the same as any other task.

### Done

The second time that an Iterative task has special handling is when it has its status set to “Completed” using Task::set_status or Task::done.

The iterative task closes itself and spawns its successor, a new Iterative task for the next occurrence. The successor's UUID is derived deterministically as a UUIDv5 from the completed task's UUID (`v5(ITERATIVE_NAMESPACE, completed_uuid)`), so two replicas completing the same task before syncing derive the same successor UUID and converge rather than producing duplicates.

The property changes, where `self` is the task being completed and `successor` is the new instance:

| Property          | `self`         | `successor`        |
| ----------------- | -------------- | ------------------ |
| status            | Completed      | Iterative          |
| end               | now            | none               |
| start             | unchanged      | none               |
| entry             | unchanged      | now                |
| iter_count        | unchanged      | `self`'s + 1       |
| iter, iter_type   | none (cleared) | copied from `self` |
| `dep_<UUID>`      | unchanged      | none               |
| `annotation_<ts>` | unchanged      | none               |
| `tag_<tag>`       | unchanged      | copied from `self` |
| modified          | now            | now                |
| all others        | unchanged      | copied from `self` |

Because the task the dependents point at is the one that becomes Completed, dependent tasks are unblocked automatically with no dep rewriting. This means completion only ever changes the task itself and the successor it creates.

The successor's `due`, `scheduled`, `wait` and `until` dates are computed from the rule derived from `iter` and re-anchored based on the iteration type and the highest-priority date. Once its next value is computed, the remainder shift by the same amount of time so their spacing is preserved:

- fixed anchors off the current anchor date
- fixed+ anchors off the anchor date but advancing to the next occurrence on or after now
- chained anchors off the completion time.

The successor's `iter_count` is one greater than the completed instance's. If the rule carries a `COUNT`, the series ends once an instance's `iter_count` reaches the `COUNT`. The rule is copied unchanged.

Note: because completing a task makes its own status `Completed`, the handle used to complete it now refers to the completed occurrence.

Because there is only ever one live task in a series, it is not possible to end up with multiple pending tasks that are all copies of each other. Since the successor UUIDs are deterministic, two replicas completing the same task before syncing converge to a single completed task and a single successor task.

### Ending a Series

A series ends on its own when the iteration rule runs out: either an instance's `iter_count` reaches the rule's `COUNT`, or the rule's `UNTIL` has passed. In both cases the last instance completes normally and no successor is created.

There are two ways to end a series early:

- Delete the current instance. Deletion does not create a successor, so the series stops there.
- Clear `iter`, which makes the task an ordinary one that can then be completed or deleted like any other.

### Interpreting Incomplete Iterative Tasks

As with any task, an `iterative` task must be have a reasonable interpretation even when it does not fully meet the requirements for an iterative task, for example if `iter` is manually cleared without changing the status. A task with status `iterative` that is missing `iter`, `iter_type`, or all of `due`, `scheduled` and `wait`, or whose `iter` or `iter_type` cannot be parsed is interpreted as a pending task that does not iterate. It stays in the working set and carries the `PENDING` synthetic tag like any other live iterative task.

Attempting to complete a broken iterative task returns an error describing what is missing and leaves the task unchanged, so the front end can prompt the user to repair it or deal with it some other way.

## Issues and Limitations

### Legacy Applications

Applications not built on TaskChampion 3.0 or later are completely left out, as all iterative functionality is implemented in TaskChampion.

All applications that haven’t been updated to recognize the Iterative status may hide or mishandle Iterative tasks. The biggest issue is likely to be marking an Iterative task done and then losing it as an iterative task. If that happens, it will disappear from that client's perspective and the iteration never advances. However, TaskWarrior ignores tasks with unknown statuses so this is unlikely to happen.

### Scheduling Timezone and Cross-Replica Convergence

Schedules are computed in the completing replica's local timezone.

If two replicas in different timezones complete the same occurrence before syncing, each computes the next occurrence in its own timezone. Because the successor UUID is deterministic, both replicas still produce the same successor task rather than duplicates.

The successor's `due`, `scheduled`, and `wait` are separate operations, so they are not resolved as a group: each field is resolved independently, in a last-writer-wins system by that operation's own timestamp. In principle this means a successor could take `due` from one replica and `scheduled` from another. In practice, this could only happen if the task was simultaneously completed in different timezones in a way that interleaved the completion times.

Either way the result converges to a single valid successor that self-corrects on the next completion, not duplicate tasks or corruption.
