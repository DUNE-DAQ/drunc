from unittest.mock import MagicMock, patch

import grpc
import grpc_testing
import pytest
from druncschema.process_manager_pb2 import (
    DESCRIPTOR,
    ProcessInstance,
    ProcessQuery,
    ProcessUUID,
)
from druncschema.token_pb2 import Token

from drunc.process_manager.process_manager import (
    ProcessInstanceList,
    ResponseFlag,
)
from drunc.tests.process_manager.process_manager_mock_impls import (
    ConcreteProcessManager,
)


@pytest.fixture(scope="module")
def mock_logger():
    """
    Create a mock logger that intercepts get_logger calls during testing.

    This fixture patches the logger creation to prevent actual logging operations
    during tests while still allowing the code under test to interact with a
    logger interface.

    Yields:
        MagicMock: Patched get_logger function with accessible logger instance
    """
    with patch("drunc.process_manager.process_manager.get_logger") as mock_get_logger:
        # Create a mock logger instance that behaves like a real logger
        mock_logger_instance = MagicMock()
        mock_get_logger.return_value = mock_logger_instance
        mock_get_logger.logger_instance = mock_logger_instance
        yield mock_get_logger


@pytest.fixture(scope="function")
def grpc_servicer(mock_logger):
    """
    Create and configure a ConcreteProcessManager instance for testing.

    This fixture instantiates the process manager servicer with a mocked logger
    The servicer implements the ProcessManager gRPC service interface.

    Args:
        mock_logger: Mock logger fixture to prevent actual logging operations

    Returns:
        ConcreteProcessManager: Configured servicer instance ready for testing
    """
    servicer = ConcreteProcessManager()
    servicer._mock_logger = mock_logger
    return servicer


@pytest.fixture(scope="function")
def grpc_test_server_no_impl(grpc_servicer):
    """
    Create a gRPC testing server.

    Args:
        grpc_servicer: The ProcessManager servicer instance to register

    Returns:
        grpc_testing.Server: Configured testing server instance
    """

    servicers = {DESCRIPTOR.services_by_name["ProcessManager"]: grpc_servicer}

    test_server = grpc_testing.server_from_dictionary(
        servicers, grpc_testing.strict_real_time()
    )
    # Define the expected response (same for all endpoints)
    expected_response = ProcessInstanceList(
        name="process_manager_no_impl",
        token=None,
        values=[],
        flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
    )

    return (test_server, expected_response)


def _test_kill(grpc_test_server, expected_response):
    """
    Test the kill RPC method, it will return a not implemented response
    as only the derived classes have an implementation.
    Args:
        grpc_test_server: gRPC testing server for invoking RPC methods
    """
    # Create authentication token for the request
    token = Token()

    # Create process identifiers for targeting specific processes
    uuids = [ProcessUUID(uuid="uuid1"), ProcessUUID(uuid="uuid2")]
    names = ["name1", "name2"]
    user = "test_user"
    session = "test_session"

    # Construct the process query with all required identification fields
    request = ProcessQuery(
        token=token, uuids=uuids, names=names, user=user, session=session
    )

    # invoke the method
    kill_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(
            DESCRIPTOR.services_by_name["ProcessManager"].methods_by_name["kill"]
        ),
        invocation_metadata={},
        request=request,
        timeout=1,
    )

    # blocks until response is ready
    response, metadata, code, details = kill_method.termination()

    # Verify the RPC completed successfully
    assert code == grpc.StatusCode.OK

    # Verify all response fields match expected values
    assert expected_response.name == response.name
    assert expected_response.token == response.token
    assert expected_response.values == response.values
    assert expected_response.flag == response.flag


@pytest.fixture(scope="function")
def grpc_test_server_with_mock_kill_impl(grpc_servicer):
    """
    Create a gRPC testing server with pre-configured kill_impl mocking.

    Args:
        grpc_servicer: The ProcessManager servicer instance to register

    Returns:
        grpc_testing.Server: Configured testing server instance
    """

    mock_process_uuid = ProcessUUID(uuid="mocked-uuid-123")

    mock_process_instance = ProcessInstance(
        uuid=mock_process_uuid,
        status_code=ProcessInstance.StatusCode.RUNNING,
        return_code=0,
    )

    mock_token = Token()

    mock_kill_impl_response = ProcessInstanceList(
        name="kill_with_impl",
        token=mock_token,
        values=[mock_process_instance],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    grpc_servicer._kill_impl = MagicMock(return_value=mock_kill_impl_response)

    servicers = {DESCRIPTOR.services_by_name["ProcessManager"]: grpc_servicer}

    test_server = grpc_testing.server_from_dictionary(
        servicers, grpc_testing.strict_real_time()
    )

    # define the response we expect from our request
    expected_response = ProcessInstanceList(
        name="kill_with_impl",
        token=mock_token,
        values=[mock_process_instance],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    return (test_server, expected_response)


def test_kill_with_impl(grpc_test_server_with_mock_kill_impl):
    """
    Test the kill RPC method, mocking in a concrete implementation
    which would be from a derived class.
    Args:
        grpc_test_server: gRPC testing server for invoking RPC methods
    """
    test_server, expected_response = grpc_test_server_with_mock_kill_impl
    _test_kill(test_server, expected_response)


def test_kill_with_no_impl(grpc_test_server_no_impl):
    """
    Test the kill RPC method, without any implementation.
    Args:
        grpc_test_server: gRPC testing server for invoking RPC methods
    """
    test_server, expected_response = grpc_test_server_no_impl
    _test_kill(test_server, expected_response)
