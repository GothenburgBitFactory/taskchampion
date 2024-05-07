from taskchampion import Replica, Status
import pytest

from pathlib import Path

# TODO: instantiate the in-memory replica, this will do for now


@pytest.fixture
def empty_replica(tmp_path: Path) -> Replica:
    return Replica(str(tmp_path), True)


@pytest.fixture
def replica_with_tasks(empty_replica: Replica):
    empty_replica.new_task(Status.Pending, "Task 1")
    empty_replica.new_task(Status.Pending, "Task 2")

    return empty_replica


def test_constructor(tmp_path: Path):
    r = Replica(str(tmp_path), True)

    assert r is not None


def test_constructor_throws_error_with_missing_database(tmp_path: Path):
    with pytest.raises(OSError):
        Replica(str(tmp_path), False)


def test_new_task(empty_replica: Replica):
    empty_replica.new_task(Status.Completed, "This is a desription")

    tasks = empty_replica.all_task_uuids()

    assert len(tasks) == 1


def test_all_task_uuids(empty_replica: Replica):
    empty_replica.new_task(Status.Completed, "Task 1")
    empty_replica.new_task(Status.Completed, "Task 2")
    empty_replica.new_task(Status.Completed, "Task 3")

    tasks = empty_replica.all_task_uuids()
    assert len(tasks) == 3


def test_all_tasks(empty_replica: Replica):
    empty_replica.new_task(Status.Completed, "Task 1")
    empty_replica.new_task(Status.Completed, "Task 2")
    empty_replica.new_task(Status.Completed, "Task 3")

    tasks = empty_replica.all_tasks()

    assert len(tasks) == 3
    keys = tasks.keys()

    for key in keys:
        assert tasks[key] != 0


def test_working_set(replica_with_tasks: Replica):
    ws = replica_with_tasks.working_set()
    pass
