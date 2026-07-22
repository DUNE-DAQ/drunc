"""
This module tests that the SessionManagerDriver correctly invokes the underlying
gRPC stub methods and properly handles gRPC exceptions.

They use a real stub and interceptor and a mocked channel.
"""

from unittest.mock import patch

import grpc
import pytest

from drunc.utils.grpc_utils import GrpcErrorDetails


@pytest.mark.parametrize(
    "method_name", ["describe", "list_all_sessions", "list_all_configs"]
)
def test_grpc_error_handling(mock_driver, method_name):
    """
    Test that gRPC errors are handled and logged.
    """

    grpc_error = grpc.RpcError("Simulated gRPC failure")
    mock_driver._fake_channel.error = grpc_error

    error_details = GrpcErrorDetails(
        code="INVALID_ARGUMENT",
        message="Invalid request parameters",
        details=["field_violations: field=token, description=Invalid token format"],
    )

    with (
        patch("drunc.utils.grpc_utils.extract_grpc_rich_error") as mock_extract,
        patch("drunc.utils.grpc_utils.handle_grpc_error") as mock_handler,
    ):
        mock_extract.return_value = error_details
        mock_handler.side_effect = grpc_error

        # Execute driver method
        with pytest.raises(grpc.RpcError):
            getattr(mock_driver, method_name)()

        # Assert the interceptor successfully caught the error
        mock_extract.assert_called_once_with(grpc_error)
        mock_handler.assert_called_once_with(grpc_error)

        assert mock_driver.log.error.call_count == 2
        logged = mock_driver.log.error.call_args_list[1][0][0]
        assert logged == error_details


@pytest.mark.parametrize(
    "method_name, expected_response",
    [
        ("describe", "describe_response"),
        ("list_all_sessions", "all_active_sessions_response"),
        ("list_all_configs", "all_config_keys_response"),
    ],
    indirect=["expected_response"],
)
def test_grpc_success(mock_driver, method_name, expected_response):
    mock_driver._fake_channel.response = expected_response

    response = getattr(mock_driver, method_name)()

    assert response == expected_response


@pytest.mark.parametrize(
    "method_name", ["describe", "list_all_sessions", "list_all_configs"]
)
def test_grpc_error_fallback(mock_driver, method_name):
    """
    Test that the client correctly handles a gRPC error when no rich error details are available.
    """
    grpc_error = grpc.RpcError("Basic gRPC error")
    mock_driver._fake_channel.error = grpc_error

    error_details = GrpcErrorDetails(
        code="UNKNOWN", message="Basic gRPC error", details=[]
    )

    with (
        patch("drunc.utils.grpc_utils.extract_grpc_rich_error") as mock_extract,
        patch("drunc.utils.grpc_utils.handle_grpc_error") as mock_handler,
    ):
        mock_extract.return_value = error_details
        mock_handler.side_effect = grpc_error

        # Execute driver method
        with pytest.raises(grpc.RpcError):
            getattr(mock_driver, method_name)()

        # Assert fallback logic
        mock_extract.assert_called_once()
        assert mock_driver.log.error.call_count == 2
        logged = mock_driver.log.error.call_args_list[1][0][0]
        assert logged == error_details
