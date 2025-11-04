"""
This module tests that the SessionManagerDriver correctly invokes the underlying
gRPC stub methods and properly handles gRPC exceptions.
"""

from unittest.mock import patch

import grpc
import pytest


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
def test_grpc_error(mock_driver, method_name, generic_request):
    """
    Test that the methods handle grpc exceptions.
    """
    # Set the side effect for the correct stub method
    grpc_error = grpc.RpcError("Connection failed")
    getattr(mock_driver._mock_stub, method_name).side_effect = grpc_error

    with patch(
        "drunc.session_manager.session_manager_driver.handle_grpc_error"
    ) as mock_handler:
        mock_handler.side_effect = grpc_error

        with pytest.raises(grpc.RpcError):
            # Dynamically call the method on the driver
            getattr(mock_driver, method_name)(generic_request)

        mock_handler.assert_called_once()
