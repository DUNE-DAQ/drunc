from unittest.mock import MagicMock, patch

import pytest
from druncschema.process_manager_pb2 import ProcessQuery, ProcessUUID
from druncschema.token_pb2 import Token

from drunc.process_manager.process_manager import (
    ProcessInstanceList,
    ResponseFlag,
)
from drunc.tests.process_manager.process_manager_mock_impl import ConcreteProcessManager


@pytest.fixture(scope="module")
def grpc_add_to_server():
    from druncschema.process_manager_pb2_grpc import (
        add_ProcessManagerServicer_to_server,
    )

    return add_ProcessManagerServicer_to_server


@pytest.fixture(scope="module")
def mock_logger():
    """Create a mock logger that captures get_logger calls without actual logging."""
    with patch("drunc.process_manager.process_manager.get_logger") as mock_get_logger:
        # Create a mock logger instance that get_logger will return
        mock_logger_instance = MagicMock()
        mock_get_logger.return_value = mock_logger_instance
        mock_get_logger.logger_instance = mock_logger_instance
        yield mock_get_logger


@pytest.fixture(scope="module")
def grpc_servicer(mock_logger):
    """Create ConcreteProcessManager instance with mocked logger."""
    servicer = ConcreteProcessManager()
    servicer._mock_logger = mock_logger
    return servicer


@pytest.fixture(scope="module")
def grpc_stub_cls(grpc_channel):
    from druncschema.process_manager_pb2_grpc import ProcessManagerStub

    return ProcessManagerStub


def test_kill(grpc_stub):
    # set up a dummy request
    token = Token()
    uuids = [ProcessUUID(uuid="uuid1"), ProcessUUID(uuid="uuid2")]
    names = ["name1", "name2"]
    user = "test_user"
    session = "test_session"
    query = ProcessQuery(
        token=token, uuids=uuids, names=names, user=user, session=session
    )
    request = query

    # make the request and get the response
    response = grpc_stub.kill(request)

    # default not implemented response is expected
    expected_response = ProcessInstanceList(
        name="concrete_process_manager",
        token=None,
        values=[],
        flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
    )
    assert expected_response.name == response.name
    assert expected_response.token == response.token
    assert expected_response.values == response.values
    assert expected_response.flag == response.flag
