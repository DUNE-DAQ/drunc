"""
This suite verifies that protobuf message serialisation and deserialisation function
correctly across all Session Manager gRPC endpoints using a real server/client setup.
The focus is strictly on validating message compatibility across the network boundary,
without regard to endpoint-specific business logic.
"""

from concurrent import futures
from unittest.mock import MagicMock, patch

import grpc
import pytest
from druncschema.session_manager_pb2_grpc import (
    SessionManagerStub,
    add_SessionManagerServicer_to_server,
)
from grpc._channel import _InactiveRpcError

from drunc.session_manager.session_manager import SessionManager


class SessionManagerSerialisationTestSuite:
    """Test suite for gRPC serialisation/deserialisation verification."""

    def __init__(self):
        self.server_port = "50051"
        self.server_address = f"localhost:{self.server_port}"
        self.server = None
        self.channel = None
        self.stub = None
        self.servicer = None

    def setup_server_and_client(self, method_name, mock_response):
        """Initialise a real gRPC server and client for serialisation testing.

        Args:
            method_name: Name of the method to mock (e.g., 'describe')
            mock_response: The response object to return from the mocked method
        """
        # Mock the logger to prevent logging interference during tests
        with patch("drunc.session_manager.session_manager.get_logger") as mock_logger:
            mock_logger_instance = MagicMock()
            mock_logger.return_value = mock_logger_instance
            self.servicer = SessionManager(name="dummy_session", configuration=[])

        # Mock only the specific method being tested before adding servicer to server
        setattr(self.servicer, method_name, MagicMock(return_value=mock_response))

        # Configure and start the gRPC server
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        add_SessionManagerServicer_to_server(self.servicer, self.server)
        listen_addr = f"[::]:{self.server_port}"
        self.server.add_insecure_port(listen_addr)
        self.server.start()

        # Create client channel and stub
        self.channel = grpc.insecure_channel(self.server_address)
        self.stub = SessionManagerStub(self.channel)

    def teardown_server_and_client(self):
        """Clean up gRPC server and client resources."""
        if self.channel:
            self.channel.close()
        if self.server:
            self.server.stop(grace=0)
        self.stub = None
        self.servicer = None


@pytest.fixture(scope="function")
def serialisation_test_suite():
    """
    Pytest fixture for SessionManagerserialisationTestSuite.
    """
    suite = SessionManagerSerialisationTestSuite()
    yield suite
    suite.teardown_server_and_client()


@pytest.mark.parametrize(
    "method_name, expected_response",
    [
        ("describe", "describe_response"),
        ("list_all_sessions", "all_active_sessions_response"),
        ("list_all_configs", "all_config_keys_response"),
    ],
    indirect=["expected_response"],
)
def test_serialisation(
    serialisation_test_suite,
    method_name,
    expected_response,
    mock_config_environment,
    generic_request,
):
    """Test roundtrip endpoint serialisation/deserialisation."""

    if method_name == "list_all_configs":
        with patch(
            "drunc.session_manager.session_manager.Configuration",
            return_value=mock_config_environment,
        ):
            serialisation_test_suite.setup_server_and_client(
                method_name, expected_response
            )
    else:
        serialisation_test_suite.setup_server_and_client(method_name, expected_response)

    response = getattr(serialisation_test_suite.stub, method_name)(generic_request)

    assert response == expected_response


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
def test_serialisation_round_trip_wrong_request(
    serialisation_test_suite,
    method_name,
    expected_response,
    mock_config_environment,
):
    """
    Test that wrong request types trigger gRPC errors with a valid response.

    This validates that tests will fail if the request type is changed for an
    endpoint, ensuring the serialisation validation is working correctly.
    """

    if method_name == "list_all_configs":
        with patch(
            "drunc.session_manager.session_manager.Configuration",
            return_value=mock_config_environment,
        ):
            serialisation_test_suite.setup_server_and_client(
                method_name, expected_response
            )
    else:
        serialisation_test_suite.setup_server_and_client(
            method_name, "invalid_request_type"
        )

    # Expect gRPC error when when sending invalid request type
    with pytest.raises(_InactiveRpcError) as exc_info:
        getattr(serialisation_test_suite.stub, method_name)(expected_response)

        # Check specific error code and message
        error = exc_info.value
        assert error.code() == grpc.StatusCode.INTERNAL
        assert "exception serializing request!" in error.details().lower()


@pytest.mark.parametrize(
    "method_name, request_type, expected_response",
    [
        ("describe", "generic_request", MagicMock(return_value="not a protobuf")),
        (
            "list_all_sessions",
            "generic_request",
            MagicMock(return_value="not a protobuf"),
        ),
        ("list_all_configs", "invalid_request_type", "all_config_keys_response"),
    ],
    indirect=[
        "request_type",
        "expected_response",
    ],  # Tells pytest to treat 'request types' and 'expected_response' as fixture names
)
def test_serialisation_round_trip_wrong_response(
    serialisation_test_suite,
    method_name,
    request_type,
    expected_response,
    mock_config_environment,
):
    """
    Test that wrong response types trigger gRPC errors with a valid request.

    This validates that tests will fail if the response type is changed for an
    endpoint, ensuring the deserialisation validation is working correctly.
    """

    if method_name == "list_all_configs":
        with patch(
            "drunc.session_manager.session_manager.Configuration",
            return_value=mock_config_environment,
        ):
            serialisation_test_suite.setup_server_and_client(
                method_name, expected_response
            )
    else:
        serialisation_test_suite.setup_server_and_client(method_name, expected_response)

    # Expect gRPC error when server returns invalid response type (MagicMock)
    with pytest.raises(_InactiveRpcError) as exc_info:
        getattr(serialisation_test_suite.stub, method_name)(request_type)
        # Check specific error code and message
        error = exc_info.value
        assert error.code() == grpc.StatusCode.INTERNAL
        assert "failed to serialize response!" in error.details().lower()
