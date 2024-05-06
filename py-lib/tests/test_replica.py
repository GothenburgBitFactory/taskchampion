from taskchampion import Replica
import pytest

from pathlib import Path


def test_constructor(tmp_path: Path):
    r = Replica(str(tmp_path), True)

    assert r is not None


def test_constructor_throws_error_with_missing_database(tmp_path: Path):
    with pytest.raises(OSError):
        Replica(str(tmp_path), False)
