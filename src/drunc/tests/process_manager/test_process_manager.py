"""
This suite tests the grpc endpoints of the abstract process manager class.
Checking that the serialisation/deserialisation of requests and responses
works as expected.

"""

from unittest.mock import MagicMock, patch

import google.protobuf.any_pb2
import grpc
import grpc_testing
import pytest
from druncschema.description_pb2 import Description
from druncschema.process_manager_pb2 import (
    DESCRIPTOR,
    BootRequest,
    LogLines,
    LogRequest,
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


def _test_terminate(grpc_test_server, expected_response: ProcessInstanceList):
    """
    Test that invoking the terminate method gives the expected response.

    Args:
        grpc_test_server: gRPC testing server for invoking RPC methods
        expected_response: ProcessInstanceList containing the expected response data
    """
    token = expected_response.token

    # Create the data payload for the terminate request
    data_any = google.protobuf.any_pb2.Any()
    data_any.value = b"terminate_session_data"

    # Construct the terminate request
    request = Request(token=token, data=data_any)

    # Invoke the terminate method via gRPC testing framework
    terminate_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(
            DESCRIPTOR.services_by_name["ProcessManager"].methods_by_name["terminate"]
        ),
        invocation_metadata={},
        request=request,
        timeout=1,
    )

    response: ProcessInstanceList
    # Block until response is ready and extract all response components
    response, metadata, code, details = terminate_method.termination()

    # Verify the RPC completed successfully without errors
    assert code == grpc.StatusCode.OK

    # Verify all response fields match expected values
    assert expected_response.name == response.name
    assert expected_response.token == response.token
    assert expected_response.flag == response.flag

    # Verify process details match expected response
    assert len(expected_response.values) == len(response.values)
    for expected_process, actual_process in zip(
        expected_response.values, response.values
    ):
        assert expected_process.uuid == actual_process.uuid
        assert expected_process.status_code == actual_process.status_code
        assert expected_process.return_code == actual_process.return_code
        assert (
            expected_process.process_description == actual_process.process_description
        )
        assert (
            expected_process.process_restriction == actual_process.process_restriction
        )


@pytest.fixture(scope="function")
def grpc_test_server_with_mock_terminate_impl(grpc_servicer):
    """
    Create a gRPC testing server with pre-configured terminate_impl mocking.

    Args:
        grpc_servicer: The ProcessManager servicer instance to register

    Returns:
        tuple: (test_server, expected_response) for use in terminate tests
    """
    # Create mock process UUID for terminated process
    mock_process_uuid = ProcessUUID(uuid="terminated-process-uuid-123")

    # Create mock process metadata
    mock_process_metadata = ProcessMetadata(
        uuid=mock_process_uuid,
        user="session_user",
        session="terminated_session",
        name="terminated_process",
        hostname="target_host",
        tree_id="1.0",
    )

    # Configure mock process description
    mock_process_description = ProcessDescription(
        metadata=mock_process_metadata,
        executable_and_arguments=[
            ProcessDescription.ExecAndArgs(exec="terminated_service", args="--daemon")
        ],
        process_execution_directory="/opt/service",
        process_logs_path="/var/log/service",
    )

    # Set up mock process restrictions
    mock_process_restriction = ProcessRestriction(
        allowed_hosts=["target_host"], allowed_host_types=["service_host"]
    )

    # Create mock process instance representing terminated process
    mock_process_instance = ProcessInstance(
        uuid=mock_process_uuid,
        process_description=mock_process_description,
        process_restriction=mock_process_restriction,
        status_code=ProcessInstance.StatusCode.DEAD,
        return_code=0,
    )

    # Create mock authentication token
    mock_token = Token()

    # Configure the terminate_impl response that will be returned by the mock
    mock_terminate_impl_response = ProcessInstanceList(
        name="terminate_with_impl",
        token=mock_token,
        values=[mock_process_instance],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    # Mock the abstract _terminate_impl method to return our test response
    grpc_servicer._terminate_impl = MagicMock(return_value=mock_terminate_impl_response)

    # Register the servicer with the gRPC testing framework
    servicers = {DESCRIPTOR.services_by_name["ProcessManager"]: grpc_servicer}
    test_server = grpc_testing.server_from_dictionary(
        servicers, grpc_testing.strict_real_time()
    )

    return (test_server, mock_terminate_impl_response)


def test_terminate_with_impl(grpc_test_server_with_mock_terminate_impl):
    """
    Test the terminate RPC method with a concrete implementation.

    Args:
        grpc_test_server_with_mock_terminate_impl: Fixture providing configured test server
    """
    test_server, expected_response = grpc_test_server_with_mock_terminate_impl
    _test_terminate(test_server, expected_response)


def test_terminate_with_no_impl(grpc_test_server_no_impl):
    """
    Test the terminate RPC method without any concrete implementation.

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

    _test_terminate(test_server, expected_response)


def _test_restart(grpc_test_server, expected_response: ProcessInstanceList):
    """
    Test that invoking the restart method gives the expected response.

    Args:
        grpc_test_server: gRPC testing server for invoking RPC methods
        expected_response: ProcessInstanceList containing the expected response data
    """
    token = expected_response.token

    # Extract process identifiers from expected response
    uuids = [process.uuid for process in expected_response.values]
    names = (
        [
            process.process_description.metadata.name
            for process in expected_response.values
        ]
        if expected_response.values
        else []
    )
    user = (
        expected_response.values[0].process_description.metadata.user
        if expected_response.values
        else "test_user"
    )
    session = (
        expected_response.values[0].process_description.metadata.session
        if expected_response.values
        else "test_session"
    )

    # Construct the process query for restart request
    request = ProcessQuery(
        token=token, uuids=uuids, names=names, user=user, session=session
    )

    # Invoke the restart method via gRPC testing framework
    restart_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(
            DESCRIPTOR.services_by_name["ProcessManager"].methods_by_name["restart"]
        ),
        invocation_metadata={},
        request=request,
        timeout=1,
    )

    response: ProcessInstanceList
    # Block until response is ready and extract all response components
    response, metadata, code, details = restart_method.termination()

    # Verify the RPC completed successfully without errors
    assert code == grpc.StatusCode.OK

    # Verify all response fields match expected values
    assert expected_response.name == response.name
    assert expected_response.token == response.token
    assert expected_response.flag == response.flag

    # Verify process details match expected response
    assert len(expected_response.values) == len(response.values)
    for expected_process, actual_process in zip(
        expected_response.values, response.values
    ):
        assert expected_process.uuid == actual_process.uuid
        assert expected_process.status_code == actual_process.status_code
        assert expected_process.return_code == actual_process.return_code
        assert (
            expected_process.process_description == actual_process.process_description
        )
        assert (
            expected_process.process_restriction == actual_process.process_restriction
        )


@pytest.fixture(scope="function")
def grpc_test_server_with_mock_restart_impl(grpc_servicer):
    """
    Create a gRPC testing server with pre-configured restart_impl mocking.

    Args:
        grpc_servicer: The ProcessManager servicer instance to register

    Returns:
        tuple: (test_server, expected_response) for use in restart tests
    """
    # Create mock process UUID for restarted process
    mock_process_uuid = ProcessUUID(uuid="restarted-process-uuid-789")

    # Create mock process metadata
    mock_process_metadata = ProcessMetadata(
        uuid=mock_process_uuid,
        user="restart_user",
        session="restart_session",
        name="restarted_process",
        hostname="restart_host",
        tree_id="1.0",
    )

    # Configure mock process description
    mock_process_description = ProcessDescription(
        metadata=mock_process_metadata,
        executable_and_arguments=[
            ProcessDescription.ExecAndArgs(exec="restarted_service", args="--restart")
        ],
        process_execution_directory="/opt/restart",
        process_logs_path="/var/log/restart",
    )

    # Set up mock process restrictions
    mock_process_restriction = ProcessRestriction(
        allowed_hosts=["restart_host"], allowed_host_types=["restart_type"]
    )

    # Create mock process instance representing restarted process
    mock_process_instance = ProcessInstance(
        uuid=mock_process_uuid,
        process_description=mock_process_description,
        process_restriction=mock_process_restriction,
        status_code=ProcessInstance.StatusCode.RUNNING,
        return_code=0,
    )

    # Create mock authentication token
    mock_token = Token()

    # Configure the restart_impl response that will be returned by the mock
    mock_restart_impl_response = ProcessInstanceList(
        name="restart_with_impl",
        token=mock_token,
        values=[mock_process_instance],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    # Mock the abstract _restart_impl method to return our test response
    grpc_servicer._restart_impl = MagicMock(return_value=mock_restart_impl_response)

    # Register the servicer with the gRPC testing framework
    servicers = {DESCRIPTOR.services_by_name["ProcessManager"]: grpc_servicer}
    test_server = grpc_testing.server_from_dictionary(
        servicers, grpc_testing.strict_real_time()
    )

    return (test_server, mock_restart_impl_response)


def test_restart_with_impl(grpc_test_server_with_mock_restart_impl):
    """
    Test the restart RPC method with a concrete implementation.

    Args:
        grpc_test_server_with_mock_restart_impl: Fixture providing configured test server
    """
    test_server, expected_response = grpc_test_server_with_mock_restart_impl
    _test_restart(test_server, expected_response)


def test_restart_with_no_impl(grpc_test_server_no_impl):
    """
    Test the restart RPC method without any concrete implementation.

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

    _test_restart(test_server, expected_response)


def _test_ps(grpc_test_server, expected_response: ProcessInstanceList):
    """
    Test that invoking the ps method gives the expected response.

    Args:
        grpc_test_server: gRPC testing server for invoking RPC methods
        expected_response: ProcessInstanceList containing the expected response data
    """
    token = expected_response.token

    # Extract process identifiers from expected response
    uuids = [process.uuid for process in expected_response.values]
    names = (
        [
            process.process_description.metadata.name
            for process in expected_response.values
        ]
        if expected_response.values
        else []
    )
    user = (
        expected_response.values[0].process_description.metadata.user
        if expected_response.values
        else "test_user"
    )
    session = (
        expected_response.values[0].process_description.metadata.session
        if expected_response.values
        else "test_session"
    )

    # Construct the process query for ps request
    request = ProcessQuery(
        token=token, uuids=uuids, names=names, user=user, session=session
    )

    # Invoke the ps method via gRPC testing framework
    ps_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(
            DESCRIPTOR.services_by_name["ProcessManager"].methods_by_name["ps"]
        ),
        invocation_metadata={},
        request=request,
        timeout=1,
    )

    response: ProcessInstanceList
    # Block until response is ready and extract all response components
    response, metadata, code, details = ps_method.termination()

    # Verify the RPC completed successfully without errors
    assert code == grpc.StatusCode.OK

    # Verify all response fields match expected values
    assert expected_response.name == response.name
    assert expected_response.token == response.token
    assert expected_response.flag == response.flag

    # Verify process details match expected response
    assert len(expected_response.values) == len(response.values)
    for expected_process, actual_process in zip(
        expected_response.values, response.values
    ):
        assert expected_process.uuid == actual_process.uuid
        assert expected_process.status_code == actual_process.status_code
        assert expected_process.return_code == actual_process.return_code
        assert (
            expected_process.process_description == actual_process.process_description
        )
        assert (
            expected_process.process_restriction == actual_process.process_restriction
        )


@pytest.fixture(scope="function")
def grpc_test_server_with_mock_ps_impl(grpc_servicer):
    """
    Create a gRPC testing server with pre-configured ps_impl mocking.

    Args:
        grpc_servicer: The ProcessManager servicer instance to register

    Returns:
        tuple: (test_server, expected_response) for use in ps tests
    """
    # Create mock process UUID for process status query
    mock_process_uuid = ProcessUUID(uuid="ps-process-uuid-999")

    # Create mock process metadata
    mock_process_metadata = ProcessMetadata(
        uuid=mock_process_uuid,
        user="ps_user",
        session="ps_session",
        name="ps_process",
        hostname="ps_host",
        tree_id="1.0",
    )

    # Configure mock process description
    mock_process_description = ProcessDescription(
        metadata=mock_process_metadata,
        executable_and_arguments=[
            ProcessDescription.ExecAndArgs(exec="ps_service", args="--status")
        ],
        process_execution_directory="/opt/ps",
        process_logs_path="/var/log/ps",
    )

    # Set up mock process restrictions
    mock_process_restriction = ProcessRestriction(
        allowed_hosts=["ps_host"], allowed_host_types=["ps_type"]
    )

    # Create mock process instance representing process status
    mock_process_instance = ProcessInstance(
        uuid=mock_process_uuid,
        process_description=mock_process_description,
        process_restriction=mock_process_restriction,
        status_code=ProcessInstance.StatusCode.RUNNING,
        return_code=0,
    )

    # Create mock authentication token
    mock_token = Token()

    # Configure the ps_impl response that will be returned by the mock
    mock_ps_impl_response = ProcessInstanceList(
        name="ps_with_impl",
        token=mock_token,
        values=[mock_process_instance],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    # Mock the abstract _ps_impl method to return our test response
    grpc_servicer._ps_impl = MagicMock(return_value=mock_ps_impl_response)

    # Register the servicer with the gRPC testing framework
    servicers = {DESCRIPTOR.services_by_name["ProcessManager"]: grpc_servicer}
    test_server = grpc_testing.server_from_dictionary(
        servicers, grpc_testing.strict_real_time()
    )

    return (test_server, mock_ps_impl_response)


def test_ps_with_impl(grpc_test_server_with_mock_ps_impl):
    """
    Test the ps RPC method with a concrete implementation.

    Args:
        grpc_test_server_with_mock_ps_impl: Fixture providing configured test server
    """
    test_server, expected_response = grpc_test_server_with_mock_ps_impl
    _test_ps(test_server, expected_response)


def test_ps_with_no_impl(grpc_test_server_no_impl):
    """
    Test the ps RPC method without any concrete implementation.

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

    _test_ps(test_server, expected_response)


def _test_logs(grpc_test_server, expected_response: LogLines):
    """
    Test that invoking the logs method gives the expected response.

    Args:
        grpc_test_server: gRPC testing server for invoking RPC methods
        expected_response: LogLines containing the expected response data
    """
    token = expected_response.token

    # Extract process UUID from expected response
    uuid = (
        expected_response.uuid
        if expected_response.uuid
        else ProcessUUID(uuid="test_uuid")
    )

    # Construct the process query for logs request
    query = ProcessQuery(
        token=token, uuids=[uuid], names=[], user="test_user", session="test_session"
    )

    # Construct the log request
    request = LogRequest(token=token, query=query, how_far=100)

    # Invoke the logs method via gRPC testing framework
    logs_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(
            DESCRIPTOR.services_by_name["ProcessManager"].methods_by_name["logs"]
        ),
        invocation_metadata={},
        request=request,
        timeout=1,
    )

    response: LogLines
    # Block until response is ready and extract all response components
    response, metadata, code, details = logs_method.termination()

    # Verify the RPC completed successfully without errors
    assert code == grpc.StatusCode.OK

    print("Expected response:", expected_response)
    print("Actual response:", response)

    # Verify all response fields match expected values
    assert expected_response.name == response.name
    assert expected_response.token == response.token
    assert expected_response.uuid == response.uuid
    assert expected_response.lines == response.lines
    assert expected_response.flag == response.flag


@pytest.fixture(scope="function")
def grpc_test_server_with_mock_logs_impl(grpc_servicer):
    """
    Create a gRPC testing server with pre-configured logs_impl mocking.

    Args:
        grpc_servicer: The ProcessManager servicer instance to register

    Returns:
        tuple: (test_server, expected_response) for use in logs tests
    """

    mock_process_uuid = ProcessUUID(uuid="logs-process-uuid-888")
    mock_token = Token()
    mock_log_lines = [
        "2024-01-01 10:00:00 INFO Starting service",
        "2024-01-01 10:00:01 INFO Service initialized",
        "2024-01-01 10:00:02 DEBUG Processing request",
    ]

    # Configure the logs_impl response that will be returned by the mock
    mock_logs_impl_response = LogLines(
        name="logs_with_impl",
        token=mock_token,
        uuid=mock_process_uuid,
        lines=mock_log_lines,
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )
    grpc_servicer._logs_impl = MagicMock(return_value=mock_logs_impl_response)

    # Register the servicer with the gRPC testing framework
    servicers = {DESCRIPTOR.services_by_name["ProcessManager"]: grpc_servicer}
    test_server = grpc_testing.server_from_dictionary(
        servicers, grpc_testing.strict_real_time()
    )

    return (test_server, mock_logs_impl_response)


def test_logs_with_impl(grpc_test_server_with_mock_logs_impl):
    """
    Test the logs RPC method with a concrete implementation.

    Args:
        grpc_test_server_with_mock_logs_impl: Fixture providing configured test server
    """
    test_server, expected_response = grpc_test_server_with_mock_logs_impl
    _test_logs(test_server, expected_response)


def test_logs_with_no_impl(grpc_test_server_no_impl):
    """
    Test the logs RPC method without any concrete implementation.

    Args:
        grpc_test_server_no_impl: Fixture providing test server without implementation
    """
    test_server, _ = grpc_test_server_no_impl

    # Define the expected response when no implementation is available
    expected_response = LogLines(
        name="process_manager_no_impl",
        token=None,
        uuid=None,
        lines=[],
        flag=ResponseFlag.NOT_EXECUTED_NOT_IMPLEMENTED,
    )

    _test_logs(test_server, expected_response)
