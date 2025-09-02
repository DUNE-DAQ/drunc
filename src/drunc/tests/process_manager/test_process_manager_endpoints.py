"""
This suite tests the grpc endpoints of the abstract process manager class.
"""

from unittest.mock import MagicMock, patch

import grpc
import grpc_testing
import pytest
from druncschema.process_manager_pb2 import (
    DESCRIPTOR,
)

from drunc.tests.process_manager.process_manager_mock_impls import (
    ConcreteProcessManager,
)


@pytest.fixture(scope="function")
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
def grpc_test_server_factory(grpc_servicer):
    """
    Create a function for generating gRPC test servers with specific endpoint mocks.

    Args:
        grpc_servicer: The ProcessManager servicer instance to register

    Returns:
        function: Factory function that accepts (endpoint_name, expected_response) parameters
    """

    def create_server(endpoint_name, expected_response):
        """
        Create a gRPC test server with a specific endpoint mocked.

        Args:
            endpoint_name (str): Name of the endpoint method to mock (e.g., 'kill', 'boot')
            expected_response: The response object to return from the mocked method

        Returns:
            tuple: (test_server, expected_response) for use in endpoint tests
        """
        # Mock the abstract implementation method for the specified endpoint
        mock_method = MagicMock(return_value=expected_response)
        setattr(grpc_servicer, f"_{endpoint_name}_impl", mock_method)

        # Register the servicer with the gRPC testing framework
        servicers = {DESCRIPTOR.services_by_name["ProcessManager"]: grpc_servicer}
        test_server = grpc_testing.server_from_dictionary(
            servicers, grpc_testing.strict_real_time()
        )

        return (test_server, expected_response)

    return create_server


def test_kill_endpoint(grpc_test_server_factory):
    """
    Test that invoking the kill method gives the expected response.

    Validates that the kill endpoint correctly processes ProcessQuery requests
    and returns the expected ProcessInstanceList response format.
    """
    from drunc.tests.process_manager.dummy_requests import PROCESS_QUERY_REQUEST
    from drunc.tests.process_manager.dummy_responses import KILL_RESPONSE

    grpc_test_server, expected_response = grpc_test_server_factory(
        "kill", KILL_RESPONSE
    )

    # Invoke the kill method via gRPC testing framework
    kill_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(
            DESCRIPTOR.services_by_name["ProcessManager"].methods_by_name["kill"]
        ),
        invocation_metadata={},
        request=PROCESS_QUERY_REQUEST,
        timeout=1,
    )

    # Block until response is ready and extract all response components
    response, metadata, code, details = kill_method.termination()

    # Verify the RPC completed successfully
    assert code == grpc.StatusCode.OK

    # Verify all response fields match expected values
    assert expected_response == response


def test_boot_endpoint(grpc_test_server_factory):
    """
    Test that invoking the boot method gives the expected response.

    Validates that the boot endpoint correctly processes BootRequest messages
    and returns the expected ProcessInstanceList response format.
    """
    from drunc.tests.process_manager.dummy_requests import BOOT_REQUEST
    from drunc.tests.process_manager.dummy_responses import BOOT_RESPONSE

    grpc_test_server, expected_response = grpc_test_server_factory(
        "boot", BOOT_RESPONSE
    )

    # Invoke the boot method via gRPC testing framework
    boot_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(
            DESCRIPTOR.services_by_name["ProcessManager"].methods_by_name["boot"]
        ),
        invocation_metadata={},
        request=BOOT_REQUEST,
        timeout=1,
    )

    # Block until response is ready and extract all response components
    response, metadata, code, details = boot_method.termination()

    # Verify the RPC completed successfully without errors
    assert code == grpc.StatusCode.OK

    # Verify all response fields match expected values
    assert expected_response == response


def test_terminate_endpoint(grpc_test_server_factory):
    """
    Test that invoking the terminate method gives the expected response.

    Validates that the terminate endpoint correctly processes generic requests
    and returns the expected ProcessInstanceList response format.
    """
    from drunc.tests.process_manager.dummy_requests import GENERIC_REQUEST
    from drunc.tests.process_manager.dummy_responses import TERMINATE_RESPONSE

    grpc_test_server, expected_response = grpc_test_server_factory(
        "terminate", TERMINATE_RESPONSE
    )

    # Invoke the terminate method via gRPC testing framework
    terminate_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(
            DESCRIPTOR.services_by_name["ProcessManager"].methods_by_name["terminate"]
        ),
        invocation_metadata={},
        request=GENERIC_REQUEST,
        timeout=1,
    )

    # Block until response is ready and extract all response components
    response, metadata, code, details = terminate_method.termination()

    # Verify the RPC completed successfully without errors
    assert code == grpc.StatusCode.OK

    # Verify all response fields match expected values
    assert expected_response == response


def test_restart_endpoint(grpc_test_server_factory):
    """
    Test that invoking the restart method gives the expected response.

    Validates that the restart endpoint correctly processes ProcessQuery requests
    and returns the expected ProcessInstanceList response format.
    """
    from drunc.tests.process_manager.dummy_requests import PROCESS_QUERY_REQUEST
    from drunc.tests.process_manager.dummy_responses import RESTART_RESPONSE

    grpc_test_server, expected_response = grpc_test_server_factory(
        "restart", RESTART_RESPONSE
    )

    # Invoke the restart method via gRPC testing framework
    restart_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(
            DESCRIPTOR.services_by_name["ProcessManager"].methods_by_name["restart"]
        ),
        invocation_metadata={},
        request=PROCESS_QUERY_REQUEST,
        timeout=1,
    )

    # Block until response is ready and extract all response components
    response, metadata, code, details = restart_method.termination()

    # Verify the RPC completed successfully without errors
    assert code == grpc.StatusCode.OK

    # Verify all response fields match expected values
    assert expected_response == response


def test_ps_endpoint(grpc_test_server_factory):
    """
    Test that invoking the ps method gives the expected response.

    Validates that the ps endpoint correctly processes ProcessQuery requests
    and returns the expected ProcessInstanceList response format.
    """
    from drunc.tests.process_manager.dummy_requests import PROCESS_QUERY_REQUEST
    from drunc.tests.process_manager.dummy_responses import PS_RESPONSE

    grpc_test_server, expected_response = grpc_test_server_factory("ps", PS_RESPONSE)

    # Invoke the ps method via gRPC testing framework
    ps_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(
            DESCRIPTOR.services_by_name["ProcessManager"].methods_by_name["ps"]
        ),
        invocation_metadata={},
        request=PROCESS_QUERY_REQUEST,
        timeout=1,
    )

    # Block until response is ready and extract all response components
    response, metadata, code, details = ps_method.termination()

    # Verify the RPC completed successfully without errors
    assert code == grpc.StatusCode.OK

    # Verify all response fields match expected values
    assert expected_response == response


def test_logs_endpoint(grpc_test_server_factory):
    """
    Test that invoking the logs method gives the expected response.

    Validates that the logs endpoint correctly processes LogRequest messages
    """
    from drunc.tests.process_manager.dummy_requests import LOG_REQUEST
    from drunc.tests.process_manager.dummy_responses import LOGS_RESPONSE

    grpc_test_server, expected_response = grpc_test_server_factory(
        "logs", LOGS_RESPONSE
    )

    # Invoke the logs method via gRPC testing framework
    logs_method = grpc_test_server.invoke_unary_unary(
        method_descriptor=(
            DESCRIPTOR.services_by_name["ProcessManager"].methods_by_name["logs"]
        ),
        invocation_metadata={},
        request=LOG_REQUEST,
        timeout=1,
    )

    # Block until response is ready and extract all response components
    response, metadata, code, details = logs_method.termination()

    # Verify the RPC completed successfully without errors
    assert code == grpc.StatusCode.OK

    # Verify all response fields match expected values
    assert expected_response == response
