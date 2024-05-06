from taskchampion import Task
import pytest


@pytest.fixture
def new_task():
    return Task()
    pass
