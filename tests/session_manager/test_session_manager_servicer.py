from pathlib import Path
from unittest.mock import MagicMock, patch

from druncschema.description_pb2 import Description
from druncschema.request_response_pb2 import ResponseFlag
from druncschema.session_manager_pb2 import (ActiveSession, AllActiveSessions,
                                             AllConfigKeys, ConfigKey)


def test_describe(
    session_manager, mock_request, mock_context, command_description_list, mock_logger
):
    response = session_manager.describe(mock_request, mock_context)
    mock_logger.debug.assert_any_call("Initialized SessionManager")

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

    response = session_manager.list_all_configs(mock_request, mock_context)

    mock_logger.error.assert_any_call("DUNEDAQ_DB_PATH not set")
    assert isinstance(response, AllConfigKeys)
    assert response.name == "dummy_name"
    assert response.flag == ResponseFlag.FAILED
    assert response.config_keys == []


def test_list_all_configs_no_files_found(
    session_manager, mock_request, mock_context, mock_logger, monkeypatch
):
    """
    Test when no configuration files are found in the database path.
    """
    monkeypatch.setenv("DUNEDAQ_DB_PATH", "valid_path/")

    with patch("pathlib.Path.rglob", return_value=[]):
        response = session_manager.list_all_configs(mock_request, mock_context)

        assert isinstance(response, AllConfigKeys)
        assert response.name == "dummy_name"
        assert response.flag == ResponseFlag.EXECUTED_SUCCESSFULLY
        assert response.config_keys == []


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
            response = session_manager.list_all_configs(mock_request, mock_context)

            assert (
                mock_logger.error.call_count == 3
            )  # should error 3 times - once per each file

            assert isinstance(response, AllConfigKeys)
            assert response.name == "dummy_name"
            assert response.flag == ResponseFlag.EXECUTED_SUCCESSFULLY
            assert response.config_keys == []


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
