from taskchampion import Task, Replica, Status, Tag
import pytest
from uuid import UUID


@pytest.fixture
def new_task(tmp_path):
    r = Replica(str(tmp_path), True)
    task = r.new_task(Status.Pending, "Task 1")
    return task


@pytest.fixture
def waiting_task(tmp_path):
    r = Replica(str(tmp_path), True)
    task = r.new_task(Status.Pending, "Task 1")
    uuid = task.get_uuid()
    r.update_task(uuid, "priority", "10")
    # Fragile test, but I cannot mock Rust's Chrono, so this will do.
    # This is the largest possible unix timestamp, so the tests should work
    # until 2038 o7
    r.update_task(uuid, "wait", "2147483647")
    r.update_task(uuid, "tag", "sample_tag")
    # Need to refresh the tag, the one that's in memory is stale
    task = r.get_task(uuid)
    return task


@pytest.fixture
def started_task(tmp_path):
    r = Replica(str(tmp_path), True)
    task = r.new_task(Status.Pending, "Task 1")
    uuid = task.get_uuid()
    r.update_task(uuid, "start", "1147483647")
    # Need to refresh the tag, the one that's in memory is stale
    task = r.get_task(uuid)
    return task


@pytest.fixture
def blocked_task(tmp_path):
    r = Replica(str(tmp_path), True)
    task = r.new_task(Status.Pending, "Task 1")
    uuid = task.get_uuid()
    r.update_task(uuid, "start", "1147483647")
    # Fragile test, but I cannot mock Rust's Chrono, so this will do.
    # Need to refresh the tag, the one that's in memory is stale
    task = r.get_task(uuid)
    return task


@pytest.fixture
def due_task(tmp_path):
    r = Replica(str(tmp_path), True)
    task = r.new_task(Status.Pending, "Task 1")
    uuid = task.get_uuid()
    r.update_task(uuid, "due", "1147483647")
    # Need to refresh the tag, the one that's in memory is stale
    task = r.get_task(uuid)
    return task


def test_get_uuid(new_task: Task):
    uuid = new_task.get_uuid()
    assert uuid is not None
    UUID(uuid)  # This tests that the UUID is valid, it raises exception if not


@pytest.mark.skip("This could be a bug")
def test_get_status(new_task: Task):
    status = new_task.get_status()

    assert status is Status.Pending


def test_get_taskmap(new_task: Task):
    taskmap = new_task.get_taskmap()

    for key in taskmap.keys():
        assert key in ["modified", "description", "entry", "status"]


def test_get_priority(waiting_task: Task):
    priority = waiting_task.get_priority()
    assert priority == "10"


def test_get_wait(waiting_task: Task):
    wait = waiting_task.get_wait()
    assert wait == "2038-01-19T03:14:07+00:00"


def test_is_waiting(waiting_task: Task):
    assert waiting_task.is_waiting()


def test_is_active(started_task: Task):
    assert started_task.is_active()


@pytest.mark.skip()
def test_is_blocked(started_task: Task):
    assert started_task.is_blocked()


@pytest.mark.skip()
def test_is_blocking(started_task: Task):
    assert started_task.is_blocking()


@pytest.mark.skip("Enable this when able to add tags to the tasks")
def test_has_tag(waiting_task: Task):
    assert waiting_task.has_tag(Tag("sample_tag"))


@pytest.mark.skip("Enable this when able to add tags to the tasks")
def test_get_tags(waiting_task: Task):
    assert waiting_task.get_tags()


def test_get_modified(waiting_task: Task):
    assert waiting_task.get_modified() is not None


def test_get_due(due_task: Task):
    assert due_task.get_due() == "2006-05-13T01:27:27+00:00"
