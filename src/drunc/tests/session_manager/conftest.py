from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_config_environment(monkeypatch):
    monkeypatch.setenv("DUNEDAQ_DB_PATH", "valid_path/")
    mock_files = [Path(f"mock_file_{i}.data.xml") for i in range(1, 4)]

    with patch("pathlib.Path.rglob", return_value=mock_files):
        mock_configuration = MagicMock()
        mock_configuration.get_dals.return_value = [
            MagicMock(id="session_1"),
            MagicMock(id="session_2"),
        ]

        yield mock_configuration
