"""
This module tests that the SessionManagerDriver correctly invokes the underlying
gRPC stub methods and properly handles gRPC exceptions.
"""

from unittest.mock import patch, MagicMock

import grpc
import pytest
from drunc.utils.grpc_utils import extract_grpc_rich_error, handle_grpc_error, GrpcErrorDetails


@pytest.mark.parametrize(
    "method_name, expected_response",
    [
        ("describe", "describe_response"),
        ("list_all_sessions", "all_active_sessions_response"),
        ("list_all_configs", "all_config_keys_response"),
    ],
    indirect=[
        "expected_response"
    ],  # Tells pytest to treat 'expected_response' as fixture names
)
def test_grpc_success(mock_driver, method_name, expected_response):
    """
    Test that the methods correctly call the stub and return response.
    """
    # Configure mock stub to return expected response
    getattr(mock_driver._mock_stub, method_name).return_value = expected_response

    # Call the method under test
    response = getattr(mock_driver, method_name)()

    getattr(mock_driver._mock_stub, method_name).assert_called_once()

    call_args = getattr(mock_driver._mock_stub, method_name).call_args
    request = call_args[0][0]

    assert hasattr(request, "token")
    assert response == expected_response


@pytest.mark.parametrize(
    "method_name",
    [
        "describe",
        "list_all_sessions",
        "list_all_configs",
    ],
)
def test_grpc_raises_error(mock_driver, method_name, generic_request):
    """
    Test that the methods handle grpc exceptions.
    """
    # Set the side effect for the correct stub method
    grpc_error = grpc.RpcError("Connection failed")
    getattr(mock_driver._mock_stub, method_name).side_effect = grpc_error

    with patch(
        "drunc.session_manager.session_manager_driver.handle_grpc_error") as mock_handler, \
            patch('drunc.session_manager.session_manager_driver.extract_grpc_rich_error') as mock_extract:
        mock_handler.side_effect = grpc_error

        with pytest.raises(grpc.RpcError):
            # Dynamically call the method on the driver
            getattr(mock_driver, method_name)(generic_request)

        mock_handler.assert_called_once()


@pytest.mark.parametrize("method_name", [
    "describe",
    "list_all_sessions",
    "list_all_configs",
])
def test_grpc_error_handling(mock_driver, method_name):
    """
    Test that gRPC errors are handled and logged.
    """
    grpc_error = grpc.RpcError("Simulated gRPC failure")
    getattr(mock_driver.stub, method_name).side_effect = grpc_error

    error_details = GrpcErrorDetails(
        code="INVALID_ARGUMENT",
        message="Invalid request parameters",
        details=["field_violations: field=token, description=Invalid token format"]
    )

    with patch("drunc.session_manager.session_manager_driver.extract_grpc_rich_error", return_value=error_details) as mock_extract, \
         patch("drunc.session_manager.session_manager_driver.handle_grpc_error") as mock_handler, \
         patch("grpc_status.rpc_status.from_call", return_value=MagicMock()):
        
        mock_handler.side_effect = grpc_error

        with pytest.raises(grpc.RpcError):
            getattr(mock_driver, method_name)()

        mock_extract.assert_called_once_with(grpc_error)
        mock_driver.log.debug.assert_called_once_with(error_details)
        mock_handler.assert_called_once_with(grpc_error)

        logged = mock_driver.log.debug.call_args[0][0]
        assert "[INVALID_ARGUMENT]" in str(logged)
        assert "Invalid token format" in str(logged)



def test_grpc_error_fallback(mock_driver):
    grpc_error = grpc.RpcError("Basic gRPC error")
    mock_driver.stub.describe.side_effect = grpc_error

    with patch("grpc_status.rpc_status.from_call", return_value=None), \
         patch("drunc.session_manager.session_manager_driver.extract_grpc_rich_error") as mock_extract, \
         patch("drunc.session_manager.session_manager_driver.handle_grpc_error", side_effect=grpc_error):

        mock_extract.return_value = GrpcErrorDetails(
            code="UNKNOWN",
            message="Basic gRPC error",
            details=[]
        )

        with pytest.raises(grpc.RpcError):
            mock_driver.describe()

        mock_extract.assert_called_once()
        mock_driver.log.debug.assert_called_once()
