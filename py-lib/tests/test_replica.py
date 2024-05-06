from taskchampion import Replica, Status
import pytest

from pathlib import Path

# TODO: instantiate the in-memory replica, this will do for now


@pytest.fixture
def new_replica(tmp_path: Path) -> Replica:
    return Replica(str(tmp_path), True)


def test_constructor(tmp_path: Path):
    r = Replica(str(tmp_path), True)

    assert r is not None


def test_constructor_throws_error_with_missing_database(tmp_path: Path):
    with pytest.raises(OSError):
        Replica(str(tmp_path), False)


def test_new_task(new_replica: Replica):
    new_replica.new_task(Status.Completed, "This is a desription")

    tasks = new_replica.all_task_uuids()

    assert len(tasks) == 1


def test_all_task_uuids(new_replica: Replica):
    new_replica.new_task(Status.Completed, "Task 1")
    new_replica.new_task(Status.Completed, "Task 2")
    new_replica.new_task(Status.Completed, "Task 3")

    tasks = new_replica.all_task_uuids()
    assert len(tasks) == 3


def test_all_tasks(new_replica: Replica):
    new_replica.new_task(Status.Completed, "Task 1")
    new_replica.new_task(Status.Completed, "Task 2")
    new_replica.new_task(Status.Completed, "Task 3")

    tasks = new_replica.all_tasks()

    assert len(tasks) == 3
    keys = tasks.keys()

    for key in keys:
        assert tasks[key] != 0
