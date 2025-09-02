"""
Test suite for ProcessManagerDriver gRPC method invocations.

This module tests that the ProcessManagerDriver correctly invokes the underlying
gRPC stub methods and properly handles gRPC exceptions.
"""

from unittest.mock import MagicMock, patch

import grpc
import pytest
from druncschema.token_pb2 import Token

from drunc.process_manager.process_manager_driver import ProcessManagerDriver


@pytest.fixture(scope="function")
def mock_driver():
    """
    Create a ProcessManagerDriver instance with a mocked gRPC stub.

    This fixture creates a driver instance where the underlying gRPC channel
    and stub are mocked.

    Returns:
        ProcessManagerDriver: Driver instance with mocked dependencies
    """
    with (
        patch("drunc.process_manager.process_manager_driver.grpc.insecure_channel"),
        patch(
            "drunc.process_manager.process_manager_driver.ProcessManagerStub"
        ) as mock_stub_class,
    ):
        # Create mock stub instance that will be returned by ProcessManagerStub()
        mock_stub = MagicMock()
        mock_stub_class.return_value = mock_stub

        # Initialize driver with mocked dependencies
        driver = ProcessManagerDriver(address="localhost:50051", token=Token())

        # Attach mock stub for easy access in tests
        driver._mock_stub = mock_stub

        return driver


def test_terminate_success(mock_driver):
    """
    Test that terminate method correctly calls stub.terminate and returns response.

    Verifies that the terminate method creates the correct request, calls the
    underlying gRPC stub, and returns the expected response.
    """
    from drunc.tests.process_manager.dummy_responses import TERMINATE_RESPONSE

    # Configure mock stub to return expected response
    mock_driver._mock_stub.terminate.return_value = TERMINATE_RESPONSE

    # Call the method under test
    response = mock_driver.terminate(timeout=30)

    # Verify stub method was called exactly once
    mock_driver._mock_stub.terminate.assert_called_once()

    # Extract the actual call arguments
    call_args = mock_driver._mock_stub.terminate.call_args
    request = call_args[0][0]
    timeout = call_args[1]["timeout"]

    # Verify request structure and timeout parameter
    assert hasattr(request, "token")
    assert timeout == 30
    assert response == TERMINATE_RESPONSE


def test_terminate_grpc_error(mock_driver):
    """
    Test that terminate method properly handles gRPC exceptions.

    Verifies that when the gRPC stub raises an exception, the driver
    calls the error handling utility function which then re-raises.
    """
    # Configure mock stub to raise gRPC error
    grpc_error = grpc.RpcError("Connection failed")
    mock_driver._mock_stub.terminate.side_effect = grpc_error

    with patch(
        "drunc.process_manager.process_manager_driver.handle_grpc_error"
    ) as mock_handler:
        # Configure mock handler to re-raise as the real function does
        mock_handler.side_effect = grpc_error

        # Expect the exception to be raised after error handling
        with pytest.raises(grpc.RpcError):
            mock_driver.terminate()

        # Verify error handler was called with the exception
        mock_handler.assert_called_once_with(grpc_error)


def test_kill_success(mock_driver):
    """
    Test that kill method correctly calls stub.kill and returns response.
    """
    from drunc.tests.process_manager.dummy_requests import PROCESS_QUERY_REQUEST
    from drunc.tests.process_manager.dummy_responses import KILL_RESPONSE

    mock_driver._mock_stub.kill.return_value = KILL_RESPONSE

    response = mock_driver.kill(PROCESS_QUERY_REQUEST, timeout=45)

    mock_driver._mock_stub.kill.assert_called_once()
    call_args = mock_driver._mock_stub.kill.call_args
    request = call_args[0][0]
    timeout = call_args[1]["timeout"]

    assert request == PROCESS_QUERY_REQUEST
    assert timeout == 45
    assert response == KILL_RESPONSE


def test_kill_grpc_error(mock_driver):
    """
    Test that kill method properly handles gRPC exceptions.
    """
    from drunc.tests.process_manager.dummy_requests import PROCESS_QUERY_REQUEST

    grpc_error = grpc.RpcError("Service unavailable")
    mock_driver._mock_stub.kill.side_effect = grpc_error

    with patch(
        "drunc.process_manager.process_manager_driver.handle_grpc_error"
    ) as mock_handler:
        mock_handler.side_effect = grpc_error

        with pytest.raises(grpc.RpcError):
            mock_driver.kill(PROCESS_QUERY_REQUEST)

        mock_handler.assert_called_once_with(grpc_error)


def test_logs_success(mock_driver):
    """
    Test that logs method correctly calls stub.logs and returns response.
    """
    from drunc.tests.process_manager.dummy_requests import LOG_REQUEST
    from drunc.tests.process_manager.dummy_responses import LOGS_RESPONSE

    mock_driver._mock_stub.logs.return_value = LOGS_RESPONSE

    response = mock_driver.logs(LOG_REQUEST, timeout=20)

    mock_driver._mock_stub.logs.assert_called_once()
    call_args = mock_driver._mock_stub.logs.call_args
    request = call_args[0][0]
    timeout = call_args[1]["timeout"]

    assert request == LOG_REQUEST
    assert timeout == 20
    assert response == LOGS_RESPONSE


def test_logs_grpc_error(mock_driver):
    """
    Test that logs method properly handles gRPC exceptions.
    """
    from drunc.tests.process_manager.dummy_requests import LOG_REQUEST

    grpc_error = grpc.RpcError("Authentication failed")
    mock_driver._mock_stub.logs.side_effect = grpc_error

    with patch(
        "drunc.process_manager.process_manager_driver.handle_grpc_error"
    ) as mock_handler:
        mock_handler.side_effect = grpc_error

        with pytest.raises(grpc.RpcError):
            mock_driver.logs(LOG_REQUEST)

        mock_handler.assert_called_once_with(grpc_error)


def test_ps_success(mock_driver):
    """
    Test that ps method correctly calls stub.ps and returns response.
    """
    from drunc.tests.process_manager.dummy_requests import PROCESS_QUERY_REQUEST
    from drunc.tests.process_manager.dummy_responses import PS_RESPONSE

    mock_driver._mock_stub.ps.return_value = PS_RESPONSE

    response = mock_driver.ps(PROCESS_QUERY_REQUEST, timeout=15)

    mock_driver._mock_stub.ps.assert_called_once()
    call_args = mock_driver._mock_stub.ps.call_args
    request = call_args[0][0]
    timeout = call_args[1]["timeout"]

    assert request == PROCESS_QUERY_REQUEST
    assert timeout == 15
    assert response == PS_RESPONSE


def test_ps_grpc_error(mock_driver):
    """
    Test that ps method properly handles gRPC exceptions.
    """
    from drunc.tests.process_manager.dummy_requests import PROCESS_QUERY_REQUEST

    grpc_error = grpc.RpcError("Request timeout")
    mock_driver._mock_stub.ps.side_effect = grpc_error

    with patch(
        "drunc.process_manager.process_manager_driver.handle_grpc_error"
    ) as mock_handler:
        mock_handler.side_effect = grpc_error

        with pytest.raises(grpc.RpcError):
            mock_driver.ps(PROCESS_QUERY_REQUEST)

        mock_handler.assert_called_once_with(grpc_error)


def test_flush_success(mock_driver):
    """
    Test that flush method correctly calls stub.flush and returns response.
    """
    from drunc.tests.process_manager.dummy_requests import PROCESS_QUERY_REQUEST
    from drunc.tests.process_manager.dummy_responses import FLUSH_RESPONSE

    mock_driver._mock_stub.flush.return_value = FLUSH_RESPONSE

    response = mock_driver.flush(PROCESS_QUERY_REQUEST, timeout=25)

    mock_driver._mock_stub.flush.assert_called_once()
    call_args = mock_driver._mock_stub.flush.call_args
    request = call_args[0][0]
    timeout = call_args[1]["timeout"]

    assert request == PROCESS_QUERY_REQUEST
    assert timeout == 25
    assert response == FLUSH_RESPONSE


def test_flush_grpc_error(mock_driver):
    """
    Test that flush method properly handles gRPC exceptions.
    """
    from drunc.tests.process_manager.dummy_requests import PROCESS_QUERY_REQUEST

    grpc_error = grpc.RpcError("Server error")
    mock_driver._mock_stub.flush.side_effect = grpc_error

    with patch(
        "drunc.process_manager.process_manager_driver.handle_grpc_error"
    ) as mock_handler:
        mock_handler.side_effect = grpc_error

        with pytest.raises(grpc.RpcError):
            mock_driver.flush(PROCESS_QUERY_REQUEST)

        mock_handler.assert_called_once_with(grpc_error)


def test_restart_success(mock_driver):
    """
    Test that restart method correctly calls stub.restart and returns response.
    """
    from drunc.tests.process_manager.dummy_requests import PROCESS_QUERY_REQUEST
    from drunc.tests.process_manager.dummy_responses import RESTART_RESPONSE

    mock_driver._mock_stub.restart.return_value = RESTART_RESPONSE

    response = mock_driver.restart(PROCESS_QUERY_REQUEST, timeout=40)

    mock_driver._mock_stub.restart.assert_called_once()
    call_args = mock_driver._mock_stub.restart.call_args
    request = call_args[0][0]
    timeout = call_args[1]["timeout"]

    assert request == PROCESS_QUERY_REQUEST
    assert timeout == 40
    assert response == RESTART_RESPONSE


def test_restart_grpc_error(mock_driver):
    """
    Test that restart method properly handles gRPC exceptions.
    """
    from drunc.tests.process_manager.dummy_requests import PROCESS_QUERY_REQUEST

    grpc_error = grpc.RpcError("Network unreachable")
    mock_driver._mock_stub.restart.side_effect = grpc_error

    with patch(
        "drunc.process_manager.process_manager_driver.handle_grpc_error"
    ) as mock_handler:
        mock_handler.side_effect = grpc_error

        with pytest.raises(grpc.RpcError):
            mock_driver.restart(PROCESS_QUERY_REQUEST)

        mock_handler.assert_called_once_with(grpc_error)


def test_describe_success(mock_driver):
    """
    Test that describe method correctly calls stub.describe and returns response.
    """
    from drunc.tests.process_manager.dummy_responses import DESCRIBE_RESPONSE

    mock_driver._mock_stub.describe.return_value = DESCRIBE_RESPONSE

    response = mock_driver.describe(timeout=10)

    mock_driver._mock_stub.describe.assert_called_once()
    call_args = mock_driver._mock_stub.describe.call_args
    request = call_args[0][0]
    timeout = call_args[1]["timeout"]

    # Describe method creates a generic Request with just a token
    assert hasattr(request, "token")
    assert timeout == 10
    assert response == DESCRIBE_RESPONSE


def test_describe_grpc_error(mock_driver):
    """
    Test that describe method properly handles gRPC exceptions.
    """
    grpc_error = grpc.RpcError("Service not found")
    mock_driver._mock_stub.describe.side_effect = grpc_error

    with patch(
        "drunc.process_manager.process_manager_driver.handle_grpc_error"
    ) as mock_handler:
        mock_handler.side_effect = grpc_error

        with pytest.raises(grpc.RpcError):
            mock_driver.describe()

        mock_handler.assert_called_once_with(grpc_error)
