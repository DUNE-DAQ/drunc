from unittest.mock import MagicMock, patch

import grpc
import pytest

from druncschema.description_pb2 import Description
from druncschema.request_response_pb2 import (
    ResponseFlag,
)
from druncschema.session_manager_pb2 import (
    ActiveSession,
    AllActiveSessions,
    AllConfigKeys,
    ConfigKey,
)
from druncschema.token_pb2 import Token

from drunc.session_manager.session_manager_driver import SessionManagerDriver

dummy_config = ConfigKey(
    file="dummy_config_file",
    session_id="dummy_config_session_id",
)

dummy_active_session = ActiveSession(
    name="dummy_session", user="dummy_user", config_key=dummy_config
)

dummy_config_keys = MagicMock()


class FakeRpcError(grpc.RpcError):
    def __init__(self, message="Dummy error"):
        super().__init__()
        self._message = message

    def code(self):
        return grpc.StatusCode.INTERNAL

    def details(self):
        return self._message


def mock_raise_exception(ex):
    raise ex


@pytest.fixture
def mock_stub():
    stub = MagicMock()
    stub.describe.return_value = Description(
        name="dummy_session_manager",
        type="session_manager",
        commands=[],
        children=[],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        token=None,
    )
    stub.list_all_sessions.return_value = AllActiveSessions(
        name="dummy_session_manager",
        token=None,
        active_sessions=[dummy_active_session],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )
    stub.list_all_configs.return_value = AllConfigKeys(
        name="dummy_session_manager",
        config_keys=dummy_config_keys,
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )
    return stub


@pytest.fixture
def token():
    return Token(user_name="test-token", token="13")


@patch("drunc.session_manager.session_manager_driver.SessionManagerStub")
def test_describe_success(mock_session_stub, mock_stub, token):
    mock_session_stub.return_value = mock_stub

    driver = SessionManagerDriver(address="mock_address", token=token)

    response = driver.describe()

    mock_stub.describe.assert_called_once()
    assert isinstance(response, Description)
    assert response.name == "dummy_session_manager"
    assert response.commands == []
    assert response.children == []
    assert response.flag == ResponseFlag.EXECUTED_SUCCESSFULLY


@patch("drunc.session_manager.session_manager_driver.SessionManagerStub")
def test_list_all_sessions(mock_session_stub, mock_stub, token):
    mock_session_stub.return_value = mock_stub

    driver = SessionManagerDriver(address="mock_address", token=token)

    response = driver.list_all_sessions()

    assert isinstance(response, AllActiveSessions)
    assert response.name == "dummy_session_manager"
    assert response.active_sessions == [dummy_active_session]
    assert response.flag == ResponseFlag.EXECUTED_SUCCESSFULLY


@patch("drunc.session_manager.session_manager_driver.SessionManagerStub")
def test_list_all_configs(mock_session_stub, mock_stub, token):
    mock_session_stub.return_value = mock_stub

    driver = SessionManagerDriver(address="mock_address", token=token)

    response = driver.list_all_configs()

    assert isinstance(response, AllConfigKeys)
    assert response.name == "dummy_session_manager"
    assert response.config_keys == []
    assert response.flag == ResponseFlag.EXECUTED_SUCCESSFULLY


@patch("drunc.session_manager.session_manager_driver.SessionManagerStub")
@patch("drunc.session_manager.session_manager_driver.handle_grpc_error")
def test_describe_grpc_error(
    mock_handle_grpc_error, mock_session_stub, mock_stub, token
):
    mock_session_stub.return_value = mock_stub
    driver = SessionManagerDriver(address="mock_address", token=token)

    mock_stub.describe.side_effect = FakeRpcError()
    mock_handle_grpc_error.side_effect = lambda e: mock_raise_exception(e)

    with pytest.raises(grpc.RpcError) as exc_info:
        driver.describe()

    assert str(exc_info.value.details()) == "Dummy error"
    mock_handle_grpc_error.assert_called_once()


@patch("drunc.session_manager.session_manager_driver.SessionManagerStub")
@patch("drunc.session_manager.session_manager_driver.handle_grpc_error")
def test_list_all_sessions_grpc_error(
    mock_handle_grpc_error, mock_session_stub, mock_stub, token
):
    mock_session_stub.return_value = mock_stub

    driver = SessionManagerDriver(address="mock_address", token=token)

    mock_stub.list_all_sessions.side_effect = FakeRpcError()
    mock_handle_grpc_error.side_effect = lambda e: mock_raise_exception(e)

    with pytest.raises(grpc.RpcError) as grpc_error:
        driver.list_all_sessions()

    assert str(grpc_error.value.details()) == "Dummy error"
    mock_handle_grpc_error.assert_called_once()


@patch("drunc.session_manager.session_manager_driver.SessionManagerStub")
@patch("drunc.session_manager.session_manager_driver.handle_grpc_error")
def test_list_all_configs_grpc_error(
    mock_handle_grpc_error, mock_session_stub, mock_stub, token
):
    mock_session_stub.return_value = mock_stub

    driver = SessionManagerDriver(address="mock_address", token=token)

    mock_stub.list_all_configs.side_effect = FakeRpcError()
    mock_handle_grpc_error.side_effect = lambda e: mock_raise_exception(e)

    with pytest.raises(grpc.RpcError) as grpc_error:
        driver.list_all_configs()

    assert str(grpc_error.value.details()) == "Dummy error"
    mock_handle_grpc_error.assert_called_once()
