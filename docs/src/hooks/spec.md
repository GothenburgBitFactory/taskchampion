## Hooks

The API defined here is the [Hooks] API.

# Hooks

A hook system is a way to run other programs/scripts at certain points in Taskchampion's execution, so that processing can be influenced.
These callout points correspond to locations in the code where some critical processing is occurring.
These are called *events*.

Hook scripts are associated with events.
When that event occurs, all associated hooks scripts are run.
Data is exchanged between Taskchampion and the hook scripts.

The hook scripts may be written in any suitable programming language.
A typical hook script will read data, perform some function, then write data and diagnostics and exit with a status code.

There is no configuration required for the hook scripts—their presence, accessibility and execute permission is all that is required.
Beware, that the replica will only execute hooks, if compiled with hook support and when a
hook base path has been set.

## Events

Hooks are triggered by events.

| Event       |                                                                                                                                                                                                                                           |
|-------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `on-launch` | This event is triggered at instantiation of the replica, or at call time of `with_hook_base_path`. This event provides an opportunity to prevent further processing.                                                                      |
| `on-exit`   | This event is triggered at drop time of the replica. This event has no opportunity to modify tasks.                                                                                                                                       |
| `on-add`    | This event is triggered when a new task is created, just before the task is committed. This event provides an opportunity to approve, deny, or modify the task being created.                                                             |
| `on-modify` | This event is triggered when an existing task is modified, just before the task is committed. This event provides an opportunity to approve, deny, or further change the modification of the task.                                        |
| `on-commit` | This event is triggered when operations are committed. Instead of `on-add` or `on-modify` the task has not be (crudely) constructed from operations.
                Use this if you need fine-grained control over the committed operations.  This event provides an opportunity to approve, deny, or further change the operations.                                                                          |

Task completion and deletion are considered modifications, indicated by the change to the 'status' attribute.


## File structure
Hooks should be contained in one directory called “hook base path”, this is where the
search for hooks starts.

The next directories should contain the event name *and* the event version (separated by a
single `_`) (that is: `<event>_<version>`).
These directories can than be filled with hook programs/scripts. Each hook can optionally
have a `*.metadata.json` file associated with it, containing further metadata.
This file has the same name of the hook, but the extension (everything
after the last dot) replaced with `metadata.json`.

The following listing is an example structure, where only the `check_for_correct_project`
hook has a metadata file associated with it. Both hook event use the `v3` version.

```text
./tests/hook_structure
├── on-add_v3
│   ├── check_for_correct_project.metadata.json
│   └── check_for_correct_project.sh
└── on-modify_v3
    └── re_sort_annotations
```

When there are multiple scripts associated with the same event, the order of execution is the collating sequence of the script name.
A hook script can be disabled by either removing execute permission, renaming it, or moving it to another location.

The hook base path is client defined and should be taken from their respective
documentation (e.g., Taskwarrior uses `rc.hooks.location`).

## Input

Input to hook scripts consists of JSON-formatted objects, one per line, in [JSON format](https://www.json.org/json-en.html).

This contained keys are dependent on the event (e.g., `on-commit` provides JSON encoded
operations, whilst `on-add` supplies a task encoded as JSON).

A JSON encoded task might look like:

```
{"description":"Buy some milk","entry":"20141118T050231Z","status":"pending","uuid":"a360fc44-315c-4366-b70c-ea7e7520b749"}
```

Before the event-specific input the hook script will be supplied with an object of metadata.

The following fields are present:

| Field name                     | Meaning                                                                                         | Example value                                       |
|--------------------------------|-------------------------------------------------------------------------------------------------|-----------------------------------------------------|
| `db_location`                  | Path to the database location with which this replica has been initialized.                     | `/home/user/.local/share/task/taskchampion.sqlite3` |
| `taskchampion_version`         | The version of the taskchampion library calling the hook. Same as the one on crates.io.         |  `3.4`                                              |
<!-- | `args:task ...`                | The script has read-only access to the command line that Taskwarrior received.                  |              | -->
<!-- | `command:add\|done\|sync\|...` | The command Taskwarrior recognized in its command line arguments.                               |              | -->
<!-- | `rc:/path/to/.taskrc`          | Absolute path of rc file used by Taskwarrior after all overrides have been applied.             |              | -->


## Output

Output from a hook script is a combination of a JSON and text.
Whitespace within the line is permitted, but the JSON or text output *must* occupy only one line of output.
As it is read back line-wise.

Feedback text is optional for a successful hook script, but required for a failing script.
If provided it will be logged by Taskchampion (error level on failure, or info on
success).

Here is sample output from a hypothetical spell-checking hook script:

```
{"description":"Buy some milk","entry":"20141118T050231Z","status":"pending","uuid":"a360fc44-315c-4366-b70c-ea7e7520b749"}
Spell checking found no errors.
```

The output is a single line JSON object which represents an optionally modified task, and a single line of feedback text to be displayed.

Whilst the feedback output is not limited, the amount of expected JSON objects is
dependent on the event.

## Exit Status

A hook script may exit with a zero status which signals that all is well.
All objects emitted will be processed, and any feedback will be emitted at info log level.

A hook script may exit with a non-zero status which signals a problem.
This terminates processing.
All JSON objects emitted will be ignored.
Feedback is required in this case, and will be emitted at error log level.

## Interfaces

The hook scripts have different responsibilities, depending on the event.
Some are provided with JSON input, some are expected to indicate that processing should continue or stop.
All may provide feedback messages.

| Event       |Version| Input                             | Output                     | Status Zero 0                       | Status Non-Zero                      |
|-------------|-------|-----------------------------------|----------------------------|-------------------------------------|--------------------------------------|
| `on-launch` |   v3  | None                              | No JSON                    | Optional feedback becomes info logs | Required feedback becomes error logs |
|             |       |                                   | Feedback                   |  Processing continues               | Processing terminates                |
| `on-exit`   |   v3  | JSON for all added/modified tasks | No JSON                    | Optional feedback becomes info logs | Required feedback becomes error logs |
|             |       |                                   | Feedback                   |                                     |                                      |
| `on-add`    |   v3  | JSON for added task               | JSON for added task        |                                     | JSON ignored                         |
|             |       |                                   | Feedback                   | Optional feedback becomes info logs | Required feedback becomes error logs |
|             |       |                                   |                            | Processing continues                | Processing terminates                |
| `on-modify` |   v3  | JSON for original task            | JSON for modified tasks    |                                     | JSON ignored                         |
|             |       | JSON for modified task            | Feedback                   | Optional feedback becomes info logs | Required feedback becomes error logs |
|             |       |                                   |                            | Processing continues                | Processing terminates                |
| `on-commit` |   v1  | one JSON object for each operation| JSON objects for operations|                                     | JSON ignored                         |
|             |       |                                   | Feedback                   | Optional feedback becomes info logs | Required feedback becomes error logs |
|             |       |                                   |                            | Processing continues                | Processing terminates                |

This means that `on-launch` hooks should expect 0 lines of input, `on-exit` 0 to many, `on-add` 1, `on-modify` 2, and `on-commit` 0 to many.

## Safety

With any hook system there are potential issues which involve race conditions, infinite loops and runaway cascading effects.
Various measures are taken to mitigate these problems:

- Malformed JSON is rejected (i.e., not JSON at all or does not decode as expected structure)
- The avoidance of race-conditions in presence of syncs is avoided by
**not** running hooks at sync time by default. They can however op-in
into being run on tasks received by sync, through the metadata system.

Please note: installing hook scripts can be as dangerous as installing any software, and if you are unsure of what the script does, don't install it.

## Examples

Here are some example use cases for hooks.
This list is intended as inspiration, and may not represent actual scripts that exist.

- Policy enforcement: referencing issues, description length, project values
- Shadow files
- User defined special tags
- Automatic priority escalation
- Automated data backup
- Making certain tasks read-only
- Disabling features like recurrence or priorities
- Spell-checking
- Preventing use of profanity
- Performing a sync on launch

Some programs (like Taskwarrior) might come with example hook scripts pre-provided.

## Backward Compatibility

The change is unfortunately not backwards compatible to the previous Taskwarrior v1 and v2 hook
APIs.

Care has been taken to avoid extensive breaking changes, but some decisions changed
nonetheless:

### The composed JSON of tasks no longer encodes the UDAs according to their TW configuration supplied type
The type encoding was always a bad idea, as a change of the configured type, did not change the already present UDAs.
Furthermore, Taskchampion is meant to be independent of the specifics of a UDA and as such
should not need to know its type. UDAs are therefore passed unmodified to JSON.

### `on-add`'s and `on-modify`'s input is re-constructed according to heuristics
Taskchampion does not know about the “adding/modifying a task” concepts; it works solely
on the basis of operations. Thus, the input for the `on-add` or
`on-modify` hooks must be re-constructed.

Currently, that happens according to the following algorithm:

1. All operations are sorted into separate stacks according to their associated UUID.
1. Every stack's operations are sorted (`Created`, then `Update`)
1. The operations are folded into a single task object (i.e., updates are applied).
1. If the stack started with a `Create` operation, the task is categorized as “added”
   otherwise it is categorized as “modified”.

The returned tasks are turned back into operations with the following algorithm.

1. If a task with the same UUID is already present in the db, it is diffed and all changed
   properties generate `Update` operations.
1. If the task is not present, a `Create`, followed by the needed `Update` operations, is
   generated.

Beware, that the algorithms can change (e.g., to improve the accuracy)
without a version bump of the associated events. If you need _exact_ knowledge of the
underlying task, you need to use a `on-commit` hook.

<!-- TODO(@bpeetz): Are there more backwards incompatible things to document here? <2025-06-28> -->
