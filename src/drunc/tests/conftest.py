import os
from pathlib import Path

import pytest


@pytest.fixture
def load_test_config():
    DUNEDAQ_DB_PATH = os.getenv("DUNEDAQ_DB_PATH")
    if DUNEDAQ_DB_PATH is None:
        DUNEDAQ_DB_PATH = ""
    cwd = Path(os.path.abspath(__file__))
    test_configs = cwd.parent / ".." / ".." / ".." / "config" / "tests"
    test_configs = test_configs.resolve()
    DUNEDAQ_DB_PATH += f":{str(test_configs)}"
    os.environ["DUNEDAQ_DB_PATH"] = DUNEDAQ_DB_PATH
