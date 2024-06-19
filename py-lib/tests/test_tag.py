import pytest
from taskchampion import Tag


@pytest.fixture
def user_tag():
    return Tag("user_tag")


@pytest.fixture
def synthetic_tag():
    return Tag("UNBLOCKED")


def test_user_tag(user_tag: Tag):
    assert user_tag.is_user()
    assert not user_tag.is_synthetic()


def test_synthetic_tag(synthetic_tag: Tag):
    assert synthetic_tag.is_synthetic()
    assert not synthetic_tag.is_user()
