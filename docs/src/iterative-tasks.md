# TaskChampion Iterative Tasks

Iterative tasks are a feature for handling repeating tasks inside of TaskChampion that can replace or live alongside TaskWarrior's recurring system.

The primary goals are:

- 1. Provide a more intuitive repeating task model
- 2. Handle synchronizing repeating tasks across replicas cleanly
- 3. Support any TaskChampion front end
- 4. Fix many of TaskWarrior's recurring tasks [issues](https://github.com/GothenburgBitFactory/taskwarrior/issues?q=is%3Aissue%20state%3Aopen%20label%3Atopic%3Arecurrence)

### Nomenclature

In order to help keep things clear and for a few technical reasons, the new system is referred to as `Iterative` tasks, while the current system is referred to as `Recurring` tasks.

## Iterative Tasks Main Ideas

The major components of the iterative task system are:

- 1. Iteration handling in TaskChampion
- 2. New task status
- 3. Iteration Types
- 4. RRULE based iteration scheduling
- 5. Iterative task flow

### Iteration Handling in TaskChampion

All task iteration is handled in TaskChampion. Front ends like TaskWarrior only need to be updated to accept the new `Iterative` task status and call the provided `Task::set_status()/done()` functions instead of directly manipulating raw `TaskData`.

### New Task Status

A new task status, Iterative, is used for iterative tasks. This prevents current recurrence code from interfering with the iteration system and vice versa.

### Iteration Interval Types

There are three types of iteration supported by iterative tasks.

#### Fixed

When a `fixed` interval task is completed, the next due date is set to the next interval from when it was originally due. As an example, if rent is due on the 20th of the month, the next due date will be on the 20th of the next month, regardless of if I paid it on the 15th, the 20th or the 25th. These are called "fixed" tasks.

#### Fixed+

When a `fixed+` interval task is completed, the next due date is set to the next interval from when it was originally due _on or after right now_. For example, if I have to take out the garbage bins every week but miss two because I'm on vacation, I shouldn't take them out multiple days in a row to make up for it.

#### Chained

When a chained interval task is completed, the next due date is set to the next interval from when it was completed. As an example, if I normally get a haircut every six weeks and I got one today, my next one should be six weeks from today, even if I got this one three weeks late, or two days early.

In order to handle these iteration styles, an `iter_type` UDA is added, which can be one of `fixed`, `fixed+`, and `chained`. Each also accepts a short form: `fx`, `f+` or `fp`, and `ch`.

`iter_type` defaults to `fixed`, which is what TaskWarrior's older `recur:` already does, so a task moved from `recur:monthly` to `iter:monthly` keeps the same schedule.

### Dates and Counts

TaskWarrior defines four optional date fields that interact with iterative tasks.

- `due` - The hard date that a task must be completed by. e.g. rent is due on the 15th.
- `scheduled` - The date after which a task can be done. e.g. Christmas decorations can't be put up until after Thanksgiving
- `wait` - Hides a task until after the `wait` so as to not clutter up the task list.
- `until` - Unfortunately the meaning for this field is overloaded for recurrence and normal tasks. For a recurring task, this is the end of the recurrence period. For a normal task this is when a task will be closed because the deadline was missed by too much. Iterative tasks take the normal-task interpretation. Responsibility for closing a task that is past its `until` belongs to the front ends, not TaskChampion. A series end date is instead handled by the rrule's `UNTIL` option.

TaskWarrior assumes but does not strictly enforce that `due` >= `scheduled` >= `wait`, i.e. a task should be `due` after it is `scheduled`, and shouldn't be either of those while `wait`ing.

When an iterative task is closed, the `due`, `scheduled`, `wait` and `until` are all advanced by the same amount of time if they exist. The advancement period is calculated for the highest priority date, and then the others are moved forward by the same time delta. This preserves the spacing between them.

Finally, RRules support a `COUNT` that limits the number of times a task iterates. Each iterative task records its position in the series in an `iter_count` UDA. When that count reaches the rule's `COUNT` no successor is created, ending the series after the intended number of iterations.

Counting up in a separate attribute, rather than decrementing the `COUNT` in the rule itself, keeps the stored rule identical to the one that `iter` describes. Decrementing would mean re-parsing and rewriting the rule on every iteration, and would leave the rule and `iter` disagreeing, which in turn breaks the date anchoring that `fixed` and `fixed+` depend on.

### RRULE Based Iteration Definition

There are a lot of ways to define iteration periods, which can be extremely complex. "Every four years on the second Tuesday in November", "The first weekday that isn't a Monday, on or after the 15th of April", and "Weekly on Monday, Tuesday, Thursday and Friday, except for the third Friday of the month" are all things that I have to deal with.

Implementing those kinds of rules is difficult, but luckily RFC 5545 Section 3.8.5.3 exists for just this kind of thing. RRules are capable of representing many, though of course not all, iteration periods and there is a well tested RRule library for Rust.

Combining RRules with the iteration types takes a bit of thought. The rule is stored in an unbaked, anchor-independent form (no `DTSTART`). The anchor is the task's highest-priority date (`due` > `scheduled` > `wait`). On completion the rule is re-anchored to compute the next occurrence for that date: fixed anchors off the current anchor date, fixed+ also anchors off the anchor date but advances to the next occurrence on or after now, and chained anchors off the completion time ("now"). The remaining dates then shift by the same delta, as described above.

#### RRule Generation

For all advantages of RRules, they are a unique syntax that (almost) no one wants to write regularly. The `iter` value is therefore accepted in four flavors:

1. Raw RRules, for people who wish to use them directly.
2. TaskWarrior-style shorthand: The same style as the recurring tasks: `daily`, `3wk`, `weekdays`, `fortnight`, `2year`, etc. These are mapped mechanically to RRules by a built-in parser.
3. ISO-8601 durations, such as `P2W`, `P3D` or `PT12H`, as long as they don't mix months or years with smaller units, such as `P1Y3D`, or a duration combining weeks with any other component, such as `P2W3D`, which ISO-8601 also doesn't allow.
4. Free-form natural language: handled by the [`text2rrule`](https://crates.io/crates/text2rrule) crate. Phrases such as `every Monday`, `every other Tuesday`, or `every two weeks on friday` are parsed into an RRULE string and then into an `RRule` value. `text2rrule` supports multiple locales.

Parsing tries a raw RRULE first, then the shorthand parser, then ISO-8601, then `text2rrule`, so existing `recur` values keep working unchanged.

### Iterative Task Flow

At any moment a series has one live Iterative task object, handled like any other task. Each time it is completed it is replaced by a successor with a new UUID, so the live UUID advances over the life of the series. The completed instances remain as records. A `series` attribute (see below) ties all instances together. Iterative tasks need special handling in only two places.

Because the UUID of the current iteration changes on every completion, it can't be used to refer to the entire series. The `series` attribute, which is the UUID of the first instance, can be as it is the same for every instance. It also makes "every instance of this series" a single filter, where following the `iter_prior` chain would be one lookup per instance.

#### Creation / Status Change

When an Iterative task is created, it must have Iterative as its status and a non-empty `iter` entry, similar to a recurring task. When the status is set to Iterative (via `Task::new` or `Task::set_status`), the `iter` value is parsed into an RRule and stored, in unbaked form, as an `rrule` UDA. The `series` attribute is also set, and is the task's own UUID if not already present. Every later instance in the series carries this same value. An `iter_count` attribute is set to 1 and each successor increments it.

The first date is chosen as follows:

- If the caller has already set `due`, `scheduled`, or `wait` on the task before the transition, the highest priority value is used as the first anchor date and is preserved exactly.
- If none of those are set, the first `due` date is the first RRule occurrence on or after now. For example, with `iter = weekdays` on a Saturday, the first due date will be the following Monday. With `iter = weekly` on any day, the first due date will be that same day.

If `iter_type` is not set, it defaults to `fixed`, which is the behavior TaskWarrior's older `recur:` already has.

At this point, an iterative task acts the same as any other task.

#### Done

The second time that an Iterative task has special handling is when it has its status set to “Completed” using Task::set_status or Task::done.

The iterative task closes itself and spawns its successor, a new Iterative task for the next occurrence. The successor's UUID is derived deterministically as a UUIDv5 from the completed task's UUID (`v5(ITERATIVE_NAMESPACE, completed_uuid)`), so two replicas completing the same task before syncing derive the same successor UUID and converge rather than producing duplicates. The successor's `iter_prior` points at the task it succeeded, forming a chain, and it carries the same `series` value as every other instance.

The attribute changes, where `self` is the task being completed and `successor` is the new instance:

| Attribute         | `self`         | `successor`        |
| ----------------- | -------------- | ------------------ |
| status            | Completed      | Iterative          |
| end               | now            | none               |
| start             | unchanged      | none               |
| entry             | unchanged      | now                |
| iter_prior        | unchanged      | `self`'s UUID      |
| series            | unchanged      | copied from `self` |
| iter_count        | unchanged      | `self`'s + 1       |
| iter, iter_type   | none (cleared) | copied from `self` |
| `dep_<UUID>`      | unchanged      | none               |
| `annotation_<ts>` | unchanged      | none               |
| `tag_<tag>`       | unchanged      | copied from `self` |
| modified          | now            | now                |
| all others        | unchanged      | copied from `self` |

Because the task the dependents point at is the one that becomes Completed, dependent tasks are unblocked automatically with no dep rewriting. This means completion only ever changes the task itself and the successor it creates.

The successor's `due`, `scheduled` and `wait` dates are computed from the stored rule, re-anchored per iteration type. The anchor is the highest-priority date. Once its next value is computed, the remainder shift by the same delta so their spacing is preserved:

- fixed anchors off the current anchor date
- fixed+ anchors off the anchor date but advancing to the next occurrence on or after now
- chained anchors off the completion time.

If the rule carries a `COUNT`, the successor's `iter_count` is one greater than the completed instance's, and the series ends once an instance's `iter_count` reaches the `COUNT`. The rule is copied unchanged.

Note: because completing a task makes its own status `Completed`, the handle used to complete it now refers to the completed occurrence.

This is basically an inversion of the recurring system, where a hidden “parent” task spawns new pending tasks. The advantage is that there is only ever one live task in a series, so it is not possible to end up with multiple pending tasks that are all copies of each other. With the deterministic successor UUID, two replicas completing the same task before syncing converge to a single completed task and a single successor rather than diverging. Iterative tasks also require less special handling in the codebase, as there is no hidden parent template to maintain.

#### Ending a Series

A series ends on its own when the iteration rule runs out: either an instance's `iter_count` reaches the rule's `COUNT`, or the rule's `UNTIL` has passed. In both cases the last instance completes normally and no successor is created.

There are two ways to end a series early:

- Delete the current instance. Deletion does not create a successor, so the series stops there.
- Clear `iter`, which makes the task an ordinary one that can then be completed or deleted like any other.

## Issues and Limitations

### Legacy Applications

Pre-3.0 based applications are completely left out as all iterative functionality is implemented in TaskChampion.

All applications that haven’t been updated to recognize the Iterative status may hide or mishandle Iterative tasks. The biggest issue is likely to be marking an Iterative task done and then losing it as an iterative task. If that happens, it will disappear from that client's perspective and the iteration never advances. However, TaskWarrior ignores tasks with unknown statuses so this is unlikely to happen.

### Scheduling Timezone and Cross-Replica Convergence

Schedules are computed in the completing replica's local timezone, the same as TaskWarrior's recurrence.

If two replicas in different timezones complete the same occurrence before syncing, each computes the next occurrence in its own timezone. Because the successor UUID is deterministic, both replicas still produce the same successor task rather than duplicates.

The successor's `due`, `scheduled`, and `wait` are separate operations, so they are not resolved as a group: each field is resolved independently, last-writer-wins by that operation's own timestamp. In principle this means a successor could take `due` from one replica and `scheduled` from another. In practice, this could only happen if the task was simultaneously completed in different timezones in a way that interleaved the completion times.

Either way the result converges to a single valid successor that self-corrects on the next completion, not duplicate tasks or corruption.
