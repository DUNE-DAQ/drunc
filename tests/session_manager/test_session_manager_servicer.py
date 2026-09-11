from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from druncschema.description_pb2 import Description
from druncschema.request_response_pb2 import ResponseFlag
from druncschema.session_manager_pb2 import (
    ActiveSession,
    AllActiveSessions,
    AllConfigKeys,
    ConfigKey,
    LoadSessionRequest,
    LoadSessionResponse,
)

from drunc.exceptions import DruncSetupException


def test_describe(
    session_manager, mock_request, mock_context, command_description_list, mock_logger
):
    response = session_manager.describe(mock_request, mock_context)
    mock_logger.debug.assert_any_call("Initialised session manager")

    assert isinstance(response, Description)
    assert response.name == "dummy_name"
    assert response.commands == command_description_list
    assert response.flag == ResponseFlag.EXECUTED_SUCCESSFULLY


def test_list_all_sessions(session_manager, mock_request, mock_context, mock_logger):
    mock_config = ConfigKey(
        file="dummy_config_file", session_id="dummy_config_session_id"
    )

    mock_session = ActiveSession(
        name="dummy_session", user="dummy_user", config_key=mock_config
    )

    session_manager._active_sessions = {mock_config.session_id: mock_session}
    response = session_manager.list_all_sessions(mock_request, mock_context)
    mock_logger.debug.assert_any_call(f"{response.name} running list_all_sessions")

    assert isinstance(response, AllActiveSessions)
    assert response.name == "dummy_name"
    assert response.active_sessions == [mock_session]
    assert response.flag == ResponseFlag.EXECUTED_SUCCESSFULLY


def test_list_all_configs_no_db_path(
    session_manager, mock_request, mock_context, mock_logger, monkeypatch
):
    """
    Test when the DUNEDAQ_DB_PATH environment variable is not set.
    """
    monkeypatch.delenv("DUNEDAQ_DB_PATH", raising=False)

    with pytest.raises(DruncSetupException) as excinfo:
        session_manager.list_all_configs(mock_request, mock_context)
    assert "DUNEDAQ_DB_PATH" in str(excinfo.value)

    # Verify that the logger logged the error
    mock_logger.error.assert_any_call("DUNEDAQ_DB_PATH not set")


def test_list_all_configs_no_files_found(
    session_manager, mock_request, mock_context, mock_logger, monkeypatch
):
    """
    Test when no configuration files are found in the database path.
    """
    monkeypatch.setenv("DUNEDAQ_DB_PATH", "valid_path/")

    with patch("pathlib.Path.rglob", return_value=[]):
        with pytest.raises(DruncSetupException) as excinfo:
            session_manager.list_all_configs(mock_request, mock_context)
        assert "Config files" in str(excinfo.value)

        mock_logger.error.assert_any_call("No configuration files found")


def test_list_all_configs_files_not_parsed(
    session_manager, mock_request, mock_context, mock_logger, monkeypatch
):
    """
    Test when configuration files are found but not parsed succesfully.
    """
    monkeypatch.setenv("DUNEDAQ_DB_PATH", "valid_path/")
    mock_files = [Path(f"mock_file_{i}.data.xml") for i in range(1, 4)]

    with patch("pathlib.Path.rglob", return_value=mock_files):
        with patch(
            "drunc.session_manager.session_manager.Configuration",
            side_effect=Exception("Config failed"),
        ):
            with pytest.raises(DruncSetupException) as excinfo:
                session_manager.list_all_configs(mock_request, mock_context)

            # Check that the exception contains the expected error message
            assert "Config files" in str(excinfo.value)


def test_list_all_configs_files_parsed(
    session_manager, mock_request, mock_context, mock_logger, monkeypatch
):
    """
    Test when configuration files are found and parsed successfully.
    """
    monkeypatch.setenv("DUNEDAQ_DB_PATH", "valid_path/")
    mock_files = [Path(f"mock_file_{i}.data.xml") for i in range(1, 4)]

    with patch("pathlib.Path.rglob", return_value=mock_files):
        mockConfiguration = MagicMock()
        mockConfiguration.get_dals.return_value = [
            MagicMock(id="session_1"),
            MagicMock(id="session_2"),
        ]

        expected_config_keys = [
            ConfigKey(file=f"mock_file_{i}.data.xml", session_id=f"session_{j}")
            for i in range(1, 4)
            for j in range(1, 3)
        ]

        with patch(
            "drunc.session_manager.session_manager.Configuration",
            return_value=mockConfiguration,
        ):
            response = session_manager.list_all_configs(mock_request, mock_context)

            assert isinstance(response, AllConfigKeys)
            assert response.name == "dummy_name"
            assert response.flag == ResponseFlag.EXECUTED_SUCCESSFULLY
            assert len(response.config_keys) == 6
            assert response.config_keys == expected_config_keys


def test_list_all_configs_dals_missing(
    session_manager, mock_request, mock_context, mock_logger, monkeypatch
):
    """
    Test when configuration files are found, but DALs are missing or invalid.
    """
    monkeypatch.setenv("DUNEDAQ_DB_PATH", "valid_path/")

    mock_files = [Path(f"mock_file_{i}.data.xml") for i in range(1, 4)]

    with patch("pathlib.Path.rglob", return_value=mock_files):
        mock_config = MagicMock()
        mock_config.get_dals.side_effect = Exception("DALs missing or invalid")

        with patch(
            "drunc.session_manager.session_manager.Configuration",
            return_value=mock_config,
        ):
            with pytest.raises(DruncSetupException) as excinfo:
                session_manager.list_all_configs(mock_request, mock_context)

            assert "Session DALs" in str(excinfo.value)

            mock_logger.error.assert_any_call(
                "Failed to get DALs from mock_file_1.data.xml: DALs missing or invalid"
            )


def test_load_session(session_manager, mock_context, mock_logger):
    """
    Test loading a session with a given configuration key.
    """
    session_file = "dummy_config_file"
    session_id = "dummy_config_session_id"
    session_name = "session_name"
    session_user = "session_user"

    mock_config = ConfigKey(file=session_file, session_id=session_id)
    mock_request = LoadSessionRequest(config_key=mock_config)

    response = session_manager.load_session(mock_request, mock_context)
    session = session_manager._active_sessions[session_id]
    mock_logger.debug.assert_any_call(f"{session_manager.name} running load_session")

    assert session.name == session_name
    assert session.user == session_user
    assert session.config_key == mock_config

    assert isinstance(response, LoadSessionResponse)
    assert response.session.name == session_name
    assert response.session.user == session_user
    assert response.session.config_key == mock_config


def test_load_session_duplicate_id(session_manager, mock_context):
    """
    Test loading a session with a duplicate session ID.
    """
    session_file = "dummy_config_file"
    session_id = "dummy_config_session_id"

    mock_config = ConfigKey(file=session_file, session_id=session_id)
    mock_request = LoadSessionRequest(config_key=mock_config)

    session_manager.load_session(mock_request, mock_context)
    with pytest.raises(DruncSetupException) as excinfo:
        session_manager.load_session(mock_request, mock_context)

    assert "Unable to load session" in str(excinfo.value)
    assert len(session_manager._active_sessions) == 1
    assert session_manager._active_sessions[session_id].config_key == mock_config
