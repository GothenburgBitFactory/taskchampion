from taskchampion import Task, Replica
import pytest


@pytest.fixture
def new_task(tmp_path):
    r = Replica(str(tmp_path), True)
    r.get
    return Task()
    pass
