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

Combining RRules with the iteration types will take a bit of though. Fixed and fixed+ is easy enough, but in order to do chained it will be necessary to modify the DTSTART rule upon task completion.

#### RRule Generation

For all advantages of RRules, they are a unique syntax that no one wants to write regularly. There are plain English (or other languages?) to RRule parsers for several programming languages, though not Rust. Looking at the current supported recurrence frequencies, they all map to RRules in a simple mechanical way. We could start with a simple parser that covers the current set, then expand it in the future.

This would happen in the following phases:

1. Map current recur values to Rrules. This is a straightforward mechanical mapping.
2. Simple descriptions: every Monday, Every six weeks, alternate Thursdays, etc
3. Compound descriptions: Every Monday, Wednesday, and Sunday. The last Friday of every month, etc
4. (maybe) Fuzzy descriptions and calculated holidays

### Iterative Task Flow

An iterative task consists of a single task object, like any other task. In general, an Iterative task will be handled the same as any other task, except at two points during its lifetime. To put too fine a point on it, Iterative tasks do not need any special handling like recurring tasks do, outside of two places that TaskChampion deals with.

#### Creation / Status Change

When an Iterative take is created, it must have Iterative as its status and an ‘iter’ entry, similar to a recurring task. When Task::new is invoked, the 'iter', 'start', 'end', and 'until' will be parsed into an RRule and set as a 'rrule' UDA. The first due date will be calculated, if not already set, along with any other properties needed. At this point, an iterative task acts the same as any other task

If a task is changed from some other status to iterative using the Task::change_status function, the same logic as creation is followed.

#### Done

The second time that an Iterative task has special handling is when it has its status set to “Completed” using Task::set_status or Task::done.

First, a copy of the task will be created, with a new UUID and a “Completed” status. It will also have a link back to the original iterative task via the 'parent' UUID attribute. This is similar to the TaskWarrior ’log’ command.

Second, the next due date and wait are calculated from the RRule. For fixed or fixed+ styles, this is a single RRule library call. For chained, it requires updating the RRULEs DTSTART before calling.

Yes, this is basically an inversion of the current recurring system, where a hidden “parent” task spawns new pending tasks. The advantage is that since the spawned tasks are completed, it’s not possible to end up with multiple pending tasks that are all copies of each other. In case of a sync issue, there may be multiple copies of completed tasks, but that is far less important. Iterative tasks also require less special handling, as there is no need to hide a parent task most of the time and then still have a way to find it when the user wants to edit or delete it.

### Integration With TaskWarrior

Integration with TaskChampion primarily involves adding the new Iterative status, handling hooks in task creation and status change, and the RRule generator.

Integration with TaskWarrior will require adding the new Iterative status so that Iterative tasks are visible, and ‘iter’ as a built in UDA.

## Potential Issues and Limitations

There are a few potential issues with this approach

### Legacy Applications

First, pre-3.0 based applications are completely left out as all iterative functionality is implemented in TaskChampion.

Second, all applications that haven’t been updated to recognize the Iterative status may hide or mishandle Iterative tasks. The biggest issue is likely to be marking an Iterative task done and then losing it as an iterative task. If that happens, it will disappear from that client's perspective and the iteration never advances. There are a few possible mitigations, such as a secondary UDA flag that legacy clients wouldn't set, but this might just need to be documented as a possible issue during a transition phase. This is the largest risk in the proposal.

### RRule Modification

Modifying the RRule on task completion for chained tasks does make the RRule history inconsistent. Searching for things like “give me the last five dates this was done” will need to be done by searching the spawned completed tasks rather than just back calculating the RRule. This is almost certainly a minor issue for almost all use cases.

### Multiple Upcoming Tasks

The recurring task system supports having multiple open upcoming tasks based on the recurring template task. Iterative tasks don’t support that, but calculating the next arbitrary number of due dates is trivial, which should cover most use cases.

## Alternatives

### Upgrading

One possible alternative to maintaining two separate systems for periodic tasks would be to upgrade existing recurring tasks to the new system.

The first approach would be to have TaskChampion just start treating recurring tasks as iterative tasks. This would need to be a major breaking version, as using task recurrence with legacy tools would cause many problems.

The second would be to have TaskChampion migrate existing recurring tasks to iterative tasks. This could be done, but should only be an option if specificity requested.

### Just Moving Recurrence Handling

It would be possible to move the current recurrence system to TaskChampion, either as is of with the changes suggested in the [abandoned RFC](https://github.com/GothenburgBitFactory/taskwarrior/blob/develop/doc/devel/rfcs/recurrence.md). This would be simpler in several ways, but would still have most of the same backwards compatibility issues and not fix many of the known problems with recurring tasks.
