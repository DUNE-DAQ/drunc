from unittest.mock import MagicMock, patch

import google.protobuf.any_pb2
import grpc
import grpc_testing
import pytest
from druncschema.description_pb2 import Description
from druncschema.process_manager_pb2 import (
    DESCRIPTOR,
    BootRequest,
    ProcessDescription,
    ProcessInstance,
    ProcessMetadata,
    ProcessQuery,
    ProcessRestriction,
    ProcessUUID,
)
from druncschema.request_response_pb2 import Request
from druncschema.token_pb2 import Token

from drunc.process_manager.process_manager import (
    ProcessInstanceList,
    ResponseFlag,
)
from drunc.tests.process_manager.process_manager_mock_impls import (
    ConcreteProcessManager,
    ProcessManager,
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

    servicer = ConcreteProcessManager(session="mock_session")
    servicer._mock_logger = mock_logger
    return servicer


@pytest.fixture(scope="function")
def grpc_test_server_no_impl(grpc_servicer):
    """
    Create a gRPC testing server with no methods implemented.

    Args:
        grpc_servicer: The ProcessManager servicer instance to register

    Returns:
        grpc_testing.Server: Configured testing server instance
    """

    servicers = {DESCRIPTOR.services_by_name["ProcessManager"]: grpc_servicer}

    test_server = grpc_testing.server_from_dictionary(
        servicers, grpc_testing.strict_real_time()
    )

    return (test_server, grpc_servicer)


def _test_kill(grpc_test_server, expected_response):
    """
    Test that invoking the kill method gives the expected response
    Args:
        grpc_test_server: gRPC testing server for invoking RPC methods
        expected_response: the response expected
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
    test_server, _ = grpc_test_server_no_impl

    expected_response = ProcessInstanceList(
        name="process_manager_no_impl",
        token=None,
        values=[],
        flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
    )

    _test_kill(test_server, expected_response)


def _test_describe(grpc_test_server, expected_response: Description):
    """
    Test that invoking the describe method gives the expected response
    Args:
        grpc_test_server: gRPC testing server for invoking RPC methods
        expected_response: the response expected
    """

    token = Token()
    data_any = google.protobuf.any_pb2.Any()
    data_any.value = b"mock_data"  # Convert string to bytes

    request = Request(token=token, data=data_any)

    # invoke the method
    method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(
            DESCRIPTOR.services_by_name["ProcessManager"].methods_by_name["describe"]
        ),
        invocation_metadata={},
        request=request,
        timeout=1,
    )

    # blocks until response is ready
    response, metadata, code, details = method.termination()

    # Verify the RPC completed successfully
    assert code == grpc.StatusCode.OK

    # Verify all response fields match expected values
    assert expected_response.type == response.type
    assert expected_response.name == response.name
    assert expected_response.info == response.info
    assert expected_response.session == response.session
    assert expected_response.commands == response.commands
    assert expected_response.children == response.children
    assert expected_response.flag == response.flag
    assert expected_response.token == response.token


def test_describe_with_impl(grpc_test_server_no_impl):
    """
    Test the describe endpoint, which is concrete in process manager
    Args:
        grpc_test_server: gRPC testing server for invoking RPC methods
    """
    process_manager: ProcessManager
    (test_server, process_manager) = grpc_test_server_no_impl

    process_manager.get_log_path = MagicMock(return_value="mock_log_path")

    expected_response = Description(
        type="process_manager",
        name="process_manager_no_impl",
        info=process_manager.get_log_path(),
        session="mock_session",
        commands=process_manager.commands,
        children=[],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        token=None,
    )

    _test_describe(test_server, expected_response)


def _test_boot(grpc_test_server, expected_response: ProcessInstanceList):
    """
    Test that invoking the boot method gives the expected response when the
    request is consistent.

    Args:
        grpc_test_server: gRPC testing server for invoking RPC methods
        expected_response: ProcessInstanceList containing the expected response data
    """
    token = expected_response.token

    if len(expected_response.values) == 0:
        mock_process_restriction = None
        mock_process_description = None
    else:
        assert len(expected_response.values) == 1, (
            "response should only give one value by design"
        )
        mock_process_restriction = expected_response.values[0].process_restriction
        mock_process_description = expected_response.values[0].process_description

    # Construct the request from the process in the expected response
    boot_request = BootRequest(
        token=token,
        process_description=mock_process_description,
        process_restriction=mock_process_restriction,
    )

    # Invoke the boot method via gRPC testing framework
    boot_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(
            DESCRIPTOR.services_by_name["ProcessManager"].methods_by_name["boot"]
        ),
        invocation_metadata={},
        request=boot_request,
        timeout=1,
    )

    response: ProcessInstanceList
    # Block until response is ready and extract all response components
    response, metadata, code, details = boot_method.termination()

    # Verify the RPC completed successfully without errors
    assert code == grpc.StatusCode.OK

    # Verify all response fields match expected values
    assert expected_response.name == response.name
    assert expected_response.token == response.token
    assert expected_response.values == response.values
    assert expected_response.flag == response.flag
    if len(expected_response.values) == 0:
        assert len(response.values) == 0
    else:
        assert len(response.values) == 1
        assert (
            expected_response.values[0].process_description
            == response.values[0].process_description
        )
        assert (
            expected_response.values[0].process_restriction
            == response.values[0].process_restriction
        )


@pytest.fixture(scope="function")
def grpc_test_server_with_mock_boot_impl(grpc_servicer):
    """
    Create a gRPC testing server with pre-configured boot_impl mocking.

    This fixture sets up a complete testing environment for the boot endpoint
    by mocking the abstract _boot_impl method with realistic test data.
    The mock returns a successful process boot scenario.

    Args:
        grpc_servicer: The ProcessManager servicer instance to register

    Returns:
        tuple: (test_server, expected_response) for use in boot tests
    """
    # Create mock process UUID for the booted process
    mock_process_uuid = ProcessUUID(uuid="booted-process-uuid-456")

    # Create mock process metadata matching what would be created during boot
    mock_process_metadata = ProcessMetadata(
        uuid=mock_process_uuid,
        user="test_user",
        session="test_session",
        name="booted_process",
        hostname="target_host",
        tree_id="1.0",
    )

    # Configure mock process description for the booted process
    mock_process_description = ProcessDescription(
        metadata=mock_process_metadata,
        executable_and_arguments=[
            ProcessDescription.ExecAndArgs(
                exec="booted_executable", args="--production"
            )
        ],
        process_execution_directory="/opt/production",
        process_logs_path="/var/log/production",
    )

    # Set up mock process restrictions
    mock_process_restriction = ProcessRestriction(
        allowed_hosts=["production_host"], allowed_host_types=["production_type"]
    )

    # Create mock process instance representing the successfully booted process
    mock_process_instance = ProcessInstance(
        uuid=mock_process_uuid,
        process_description=mock_process_description,
        process_restriction=mock_process_restriction,
        status_code=ProcessInstance.StatusCode.RUNNING,
        return_code=0,
    )

    # Create mock authentication token for the response
    mock_token = Token()

    # Configure the expected successful boot response
    mock_boot_impl_response = ProcessInstanceList(
        name="boot_with_impl",
        token=mock_token,
        values=[mock_process_instance],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    # Mock the abstract _boot_impl method to return our test response
    grpc_servicer._boot_impl = MagicMock(return_value=mock_boot_impl_response)

    # Register the servicer with the gRPC testing framework
    servicers = {DESCRIPTOR.services_by_name["ProcessManager"]: grpc_servicer}
    test_server = grpc_testing.server_from_dictionary(
        servicers, grpc_testing.strict_real_time()
    )

    # Define the response we expect from our test request
    expected_response = ProcessInstanceList(
        name="boot_with_impl",
        token=mock_token,
        values=[mock_process_instance],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    return (test_server, expected_response)


def test_boot_with_impl(grpc_test_server_with_mock_boot_impl):
    """
    Test the boot RPC method with a concrete implementation.

    This test verifies that the boot endpoint correctly invokes the
    abstract _boot_impl method and returns the expected response when
    a concrete implementation is provided (e.g., from a derived class).

    The test simulates a successful process boot scenario where:
    - A valid boot request is submitted
    - The implementation successfully starts the process
    - A ProcessInstanceList is returned with RUNNING status

    Args:
        grpc_test_server_with_mock_boot_impl: Fixture providing configured test server
    """
    test_server, expected_response = grpc_test_server_with_mock_boot_impl
    _test_boot(test_server, expected_response)


def test_boot_with_no_impl(grpc_test_server_no_impl):
    """
    Test the boot RPC method without any concrete implementation.

    This test verifies that the boot endpoint correctly handles the case
    where no concrete implementation is provided for the abstract _boot_impl
    method. The expected behavior is to return a NOT_EXECUTED_NOT_IMPLEMENTED
    response flag.

    This scenario occurs when:
    - The base ProcessManager class is used directly
    - A derived class hasn't implemented the _boot_impl method
    - The system gracefully degrades rather than crashing

    Args:
        grpc_test_server_no_impl: Fixture providing test server without implementation
    """
    test_server, _ = grpc_test_server_no_impl

    # Define the expected response when no implementation is available
    expected_response = ProcessInstanceList(
        name="process_manager_no_impl",
        token=None,
        values=[],
        flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
    )

    _test_boot(test_server, expected_response)
