# TaskChampion Iterative Tasks

There are several issues with how the TaskWarrior/TaskChampion ecosystem handles recurring tasks. See the TaskWarrior [issue list](https://github.com/GothenburgBitFactory/taskwarrior/issues?q=is%3Aissue%20state%3Aopen%20label%3Atopic%3Arecurrence) for examples.

This proposal is for a significant redesign of the recurrence system, with a different set of trade offs. Therefore, it is currently being proposed in a way that it could either replace or live alongside the current recurring task system.

### Nomenclature

In order to help keep things clear and for a few technical reasons, the proposed system will be referred to as iterative tasks, while the current system will be referred to as recurring tasks.

## Iterative Tasks Main Ideas

The major components of the iterative task system are:

- 1. Iteration handling in TaskChampion
- 2. New task status
- 3. Iteration Types
- 4. RRULE based iteration scheduling
- 5. Iterative task flow

### Iteration Handling in TaskChampion

Currently, task recurrence is handled by the various frontends: primarily TaskWarrior, but also the mobile apps, web apps, and other platforms. This means that each frontend has to implement their own recurrence handler, each with its own quirks and syncing issues.

Task iteration will be handled in TaskChampion, as discussed in [issue 595](https://github.com/GothenburgBitFactory/taskchampion/issues/595). This will primarily be done in the Task struct with additions to the constructor and set_status functions. This does mean that frontends that primarily rely on the underlying TaskData struct will miss out on task iteration.

The frontends will see iterative tasks as normal tasks, with due, start, wait, end, and other properties as expected. The primary difference will be the new Iterative status and a few UDAs.

### New Task Status

A new task status will be created, Iterative, for iterative tasks. This will prevent current recurrence code from interfering with the iteration system and vice versa.

Note: This could be changed, see [Upgrading](#Upgrading) for more discussion.

### Iteration Types

There are several styles of iteration used in todo applications generally, but two stand out as most common:

#### Fixed Interval

When a fixed interval task is completed, the next due date is set to the next interval from when it was originally due. As an example, if rent is due on the 20th of the month, the next due date will be on the 20th of the next month, regardless of if I paid it on the 15th, the 20th or the 25th.

This does invite the question of how to handle tasks that are overdue by more than their iteration period. If I missed January and February's payments, should completing the task once move the next due date to February 20th or March 20th? Lets call just moving to the next interval "fixed" and the next interval in the future "fixed+"

#### Chained

When a chained interval task is completed, the next due date is set to the next interval from when it was completed. As an example, if I normally get a haircut every six weeks and I got one today, my next one should be six weeks from today, even if I got this one three weeks late, or two days early.

In order to handle these iteration styles, an "iter_type" UDA will be added, which can be one of "fixed", "fixed+", and "chained".

### RRULE Based Iteration Definition

There are a lot of ways to define iteration periods, which can be extremely complex. "Every four years on the second Tuesday in November", "The first weekday that isn't a Monday, on or after the 15th of April", and "Weekly on Monday, Tuesday, Thursday and Friday, except for the third Friday of the month" are all things that I have to deal with.

Implementing those kinds of rules is difficult, but luckily RFC 5545 Section 3.8.5.3 exists for just this kind of thing. RRules are capable of representing many, though of course not all, iteration periods and there is a well tested RRule library for Rust.

Combining RRules with the iteration types takes a bit of thought. The rule is stored in an unbaked, anchor-independent form (no `DTSTART`); the anchor lives in the task's `due`. On completion the rule is re-anchored to compute the next due: fixed and fixed+ anchor off the current due, chained anchors off the completion time ("now").

#### RRule Generation

For all advantages of RRules, they are a unique syntax that no one wants to write regularly. The `iter` value is therefore accepted in two flavors:

1. TaskWarrior-style shorthand: The same style as the recurring tasks: `daily`, `3wk`, `weekdays`, `fortnight`, `2year`, etc. These are mapped mechanically to RRules by a built-in parser.
2. Free-form natural language: handled by the [`text2rrule`](https://crates.io/crates/text2rrule) crate. Phrases such as `every Monday`, `every other Tuesday`, or `every two weeks on friday` are parsed into an RRULE string and then into an `RRule` value. `text2rrule` supports multiple locales.

Parsing tries the shorthand parser first and falls back to `text2rrule` on failure, so existing `recur` values keep working unchanged.

### Iterative Task Flow

At any moment a series has one live Iterative task object, handled like any other task. Each time it is completed it is replaced by a successor with a new UUID, so the live UUID advances over the life of the series. The completed instances remain as records. A `series` attribute (see below) ties all instances together. Iterative tasks need special handling in only two places.

#### Creation / Status Change

When an Iterative task is created, it must have Iterative as its status and an `iter` entry, similar to a recurring task. When the status is set to Iterative (via `Task::new` or `Task::set_status`), the `iter` value is parsed into an RRule and stored, in unbaked (anchor-independent) form, as an `rrule` UDA. A `series` attribute is also stamped, set to the task's own UUID if not already present; every later instance in the series carries this same value.

The first due date is chosen as follows:

- If the caller has already set `due` on the task before the transition, that value is used as the first due date and is preserved exactly.
- If `due` is not set, the first due date is the first RRule occurrence on or after now. For example, with `iter = weekdays` on a Saturday, the first due date will be the following Monday; with `iter = weekly` on any day, the first due date will be that same day.

If `iter_type` is not set, it defaults to `chained`. At this point, an iterative task acts the same as any other task.

#### Done

The second time that an Iterative task has special handling is when it has its status set to “Completed” using Task::set_status or Task::done.

The iterative task closes itself spawns its successor, a new Iterative task for the next occurrence. The successor's UUID is derived deterministically as a UUIDv5 from the completed task's UUID (`v5(ITERATIVE_NAMESPACE, completed_uuid)`), so two replicas completing the same task before syncing derive the same successor UUID and converge rather than producing duplicates. The successor's `parent` points at the task it succeeded, forming a chain, and it carries the same `series` value as every other instance.

The attribute changes, where `self` is the task being completed and `successor` is the new instance:

| Attribute              | `self`         | `successor`        |
| ---------------------- | -------------- | ------------------ |
| status                 | Completed      | Iterative          |
| end                    | now            | none               |
| start                  | unchanged      | none               |
| entry                  | unchanged      | now                |
| parent                 | unchanged      | `self`'s UUID      |
| series                 | unchanged      | copied from `self` |
| iter, rrule, iter_type | none (cleared) | copied from `self` |
| dep\_<UUID>            | unchanged      | none               |
| modified               | now            | now                |
| all others             | unchanged      | copied from `self` |

Because the task the dependents point at is the one that becomes Completed, dependent tasks are unblocked automatically with no dep rewriting. This means completion only ever changes the task itself and the successor it creates.

The next due date is computed from the stored (unbaked) rule, re-anchored per iteration type: fixed and fixed+ anchor off the current due, chained off the completion time. The successor is created with this due.

Note: because completing a task makes its own status `Completed`, the handle used to complete it now refers to the competed occurrence.

Yes, this is basically an inversion of the current recurring system, where a hidden “parent” task spawns new pending tasks. The advantage is that there is only ever one live task in a series, so it is not possible to end up with multiple pending tasks that are all copies of each other. With the deterministic successor UUID, two replicas completing the same task before syncing converge to a single completed log and a single successor rather than diverging. Iterative tasks also require less special handling, as there is no hidden parent template to maintain.

### Integration With TaskWarrior

Integration with TaskChampion primarily involves adding the new Iterative status, handling hooks in task creation and status change, and the RRule generator.

Integration with TaskWarrior will require adding the new Iterative status so that Iterative tasks are visible, and ‘iter’ as a built in UDA.

## Potential Issues and Limitations

There are a few potential issues with this approach

### Legacy Applications

First, pre-3.0 based applications are completely left out as all iterative functionality is implemented in TaskChampion.

Second, all applications that haven’t been updated to recognize the Iterative status may hide or mishandle Iterative tasks. The biggest issue is likely to be marking an Iterative task done and then losing it as an iterative task. If that happens, it will disappear from that client's perspective and the iteration never advances. There are a few possible mitigations, such as a secondary UDA flag that legacy clients wouldn't set, but this might just need to be documented as a possible issue during a transition phase. This is the largest risk in the proposal.

### Reconstructing History

The rule is stored unbaked and re-anchored on each completion rather than back-calculable as a single fixed schedule, especially for chained tasks. Searching for things like “give me the last five dates this was done” is done by walking the completed instances of the series.

### Multiple Upcoming Tasks

The recurring task system supports having multiple open upcoming tasks based on the recurring template task. Iterative tasks don’t support that, but calculating the next arbitrary number of due dates is trivial, which should cover most use cases.

### Scheduling Timezone and Cross-Replica Convergence

Schedules are computed in the completing replica's local timezone, the same as Taskwarrior's recurrence.

If two replicas in different timezones complete the same occurrence before syncing, each computes the next occurrence in its own timezone. When syncing the time/date based field values are resolved last-writer-wins by sync order. This will cause a one-off discrepancy that self-corrects on the next completion, not duplicate tasks or corruption.

## Alternatives

### Upgrading

One possible alternative to maintaining two separate systems for periodic tasks would be to upgrade existing recurring tasks to the new system.

The first approach would be to have TaskChampion just start treating recurring tasks as iterative tasks. This would need to be a major breaking version, as using task recurrence with legacy tools would cause many problems.

The second would be to have TaskChampion migrate existing recurring tasks to iterative tasks. Migration of existing recurring tasks is deferred to TaskWarrior rather than handled in TaskChampion.

### Just Moving Recurrence Handling

It would be possible to move the current recurrence system to TaskChampion, either as is of with the changes suggested in the [abandoned RFC](https://github.com/GothenburgBitFactory/taskwarrior/blob/develop/doc/devel/rfcs/recurrence.md). This would be simpler in several ways, but would still have most of the same backwards compatibility issues and not fix many of the known problems with recurring tasks.
