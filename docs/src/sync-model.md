# Synchronization Model

Synchronization occurs between disconnected replicas, mediated by a server.
The replicas never communicate directly with one another.
The server does not have access to the task data; it sees only opaque blobs of data with a small amount of metadata.

## Operational Transforms

Synchronization is based on [operational transformation](https://en.wikipedia.org/wiki/Operational_transformation).
This section will assume some familiarity with the concept.

## State and Operations

At a given time, the set of tasks in a replica's storage is the essential "state" of that replica.
All modifications to that state occur via operations, as defined below.
We can draw a network, or graph, with the nodes representing states and the edges representing operations.
For example:

```text
  o -- State: {abc-d123: 'get groceries', priority L}
  |
  | -- Operation: set abc-d123 priority to H
  |
  o -- State: {abc-d123: 'get groceries', priority H}
```

For those familiar with distributed version control systems, a state is analogous to a revision, while an operation is analogous to a commit.

Fundamentally, synchronization involves all replicas agreeing on a single, linear sequence of operations and the state that those operations create.
Since the replicas are not connected, each may have additional operations that have been applied locally, but which have not yet been agreed on.
The synchronization process uses operational transformation to "linearize" those operations.

This process is analogous (vaguely) to rebasing a sequence of Git commits.
Critically, though, operations cannot merge; in effect, the operations must be rebased.
Furthermore, once an operation has been sent to the server it cannot be changed; in effect, the server does not permit "force push".

### Sync Operations

The operations are:

 * `Create(uuid)`
 * `Delete(uuid)`
 * `Update(uuid, property, value, timestamp)`

The Create form creates a new task.
Creating a task that already exists has no effect.

Similarly, the Delete form deletes an existing task.
Deleting a task that does not exist has no effect.

The Update form updates the given property of the given task, where the property and values are strings.
The `property` is the property being updated, and the `value` gives its new value (or None to delete a property).
The timestamp on updates serves as additional metadata and is used to resolve conflicts.
Updating a task that does not exist has no effect.

### Versions

Occasionally, database states are given a name (that takes the form of a UUID).
The system as a whole (all replicas) constructs a branch-free sequence of versions and the operations that separate each version from the next.
The version with the nil UUID is implicitly the empty database.

The server stores the operations to change a state from a "parent" version to a "child" version, and provides that information as needed to replicas.
Replicas use this information to update their local task databases, and to generate new versions to send to the server.

In order to transmit new changes to the server, replicas generate a new version.
The changes are represented as a sequence of operations with the state resulting from the final operation corresponding to the version.
In order to keep the versions in a single sequence, the server will only accept a proposed version from a replica if its parent version matches the latest version on the server.

In the non-conflict case (such as with a single replica), then, a replica's synchronization process involves gathering up the operations it has accumulated since its last synchronization; bundling those operations into a version; and sending that version to the server.

### Replica Invariant

A replica stores the current state of all tasks, a sequence of as-yet un-synchronized operations, and the last version at which synchronization occurred (the "base version").
The replica's un-synchronized operations are already reflected in its local `tasks`, so the following invariant holds:

> Applying the replica's sequence of operations to the set of tasks at the base version gives a set of tasks identical
> to the replica's current state.

### Transformation

When the latest version on the server contains operations that are not present in the replica, then the states have diverged.
For example, here the replica has local operations `a`-`b`, but the server has a new version with operations `w`-`z`:

```text
  o  -- version N
 w|\a
  o o
 x|  \b
  o   o
 y|    \c
  o     o -- replica's local state
 z|
  o -- version N+1
```

(diagram notation: `o` designates a state, lower-case letters designate operations, and versions are presented as if they were numbered sequentially)

In this situation, the replica must "rebase" the local operations onto the latest version from the server and try again.
This process is performed using operational transformation (OT).
The result of this transformation is a sequence of operations based on the latest version, and a sequence of operations the replica can apply to its local task database to reach the same state
Continuing the example above, the resulting operations are shown with `'`:

```text
  o  -- version N
 w|\a
  o o
 x|  \b
  o   o
 y|    \c
  o     o -- replica's intermediate local state
 z|     |w'
  o-N+1 o
 a'\    |x'
    o   o
   b'\  |y'
      o o
     c'\|z'
        o  -- version N+2
```

The replica applies w' through z' locally, and sends a' through c' to the server as the operations to generate version N+2.
Either path through this graph, a-b-c-w'-x'-y'-z' or a'-b'-c'-w-x-y-z, must generate *precisely* the same final state at version N+2.
Careful selection of the operations and the transformation function ensures this.

See the comments in the source code for the details of how this transformation process is implemented.

## Synchronization Process

To perform a synchronization, the replica first requests the child version of its stored base version from the server (GetChildVersion).
If such a version exists, it applies the transformation described above, resulting in an updated state and an updated list of local operations.
The replica repeats this process until the server indicates no additional child versions exist.
If there are no un-synchronized local operations, the process is complete.

Otherwise, the replica creates a new version containing its local operations, giving its base version as the parent version, and transmits that to the server (AddVersion).
In most cases, this will succeed, but if another replica has created a new version in the interim, then the new version will conflict with that other replica's new version and the server will respond with the new expected parent version.
In this case, the entire process repeats.
If the server indicates a conflict twice with the same expected base version, that is an indication that the replica has diverged (something serious has gone wrong).

## Servers

A replica depends on periodic synchronization for performant operation.
Without synchronization, its list of pending operations would grow indefinitely, and tasks could never be expired.
So all replicas, even "singleton" replicas which do not replicate task data with any other replica, should synchronize periodically.

TaskChampion provides a `LocalServer` for this purpose.
It implements the `get_child_version` and `add_version` operations as described, storing data on-disk locally.
