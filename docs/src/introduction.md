# TaskChampion

TaskChampion implements the task storage and synchronization behind Taskwarrior.
It includes an implementation with Rust and C APIs, allowing any application to maintain and manipulate its own replica.
It also includes a specification for tasks and how they are synchronized, inviting alternative implementations of replicas or task servers.

## Relationship with Taskwarrior

TaskChampion was originally developed as a project "inspired by" Taskwarrior, and later incorporated into Taskwarrior in its 3.0 release.
Taskwarrior depends on TaskChampion, but does not have any kind of privileged access to its implementation details.
Any other application can also use TaskChampion to implement similar functionality, and even interoperate with Taskwarrior either in the same replica or via sync.
