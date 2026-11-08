from pathlib import Path
import shutil

import duckdb
import pytest


@pytest.fixture
def fixture_dir():
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def database(tmp_path):
    return tmp_path / "cloudflow.duckdb"


@pytest.fixture
def query(database):
    def execute(sql, parameters=None):
        with duckdb.connect(str(database), read_only=True) as connection:
            connection.execute("SET TimeZone = 'UTC'")
            return connection.execute(sql, parameters or []).fetchall()
    return execute


@pytest.fixture
def copy_batch(tmp_path, fixture_dir):
    def copy(name="clean"):
        destination = tmp_path / f"{name}_copy"
        shutil.copytree(fixture_dir / name, destination)
        return destination
    return copy
