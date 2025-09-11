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
from drunc.tests.session_manager.dummy_requests import GENERIC_REQUEST
from drunc.tests.session_manager.dummy_responses import (
    DUMMY_ALLACTIVESESSIONS_RESPONSE,
    DUMMY_ALLCONFIGKEYS_RESPONSE,
    DUMMY_DESCRIBE_RESPONSE,
)


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
        ("describe", DUMMY_DESCRIBE_RESPONSE),
        ("list_all_sessions", DUMMY_ALLACTIVESESSIONS_RESPONSE),
        ("list_all_configs", DUMMY_ALLCONFIGKEYS_RESPONSE),
    ],
)
def test_serialisation(
    serialisation_test_suite,
    method_name,
    expected_response,
    mock_config_environment,
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

    response = getattr(serialisation_test_suite.stub, method_name)(GENERIC_REQUEST)

    assert response == expected_response


@pytest.mark.parametrize(
    "method_name, request_type, expected_response",
    [
        ("describe", "invalid_request_type", DUMMY_DESCRIBE_RESPONSE),
        ("list_all_sessions", "invalid_request_type", DUMMY_ALLACTIVESESSIONS_RESPONSE),
        ("list_all_configs", "invalid_request_type", DUMMY_ALLCONFIGKEYS_RESPONSE),
    ],
)
def test_serialisation_round_trip_wrong_request(
    serialisation_test_suite,
    method_name,
    request_type,
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
        serialisation_test_suite.setup_server_and_client(method_name, request_type)

    # Expect gRPC error when when sending invalid request type
    with pytest.raises(_InactiveRpcError):
        getattr(serialisation_test_suite.stub, method_name)(expected_response)


@pytest.mark.parametrize(
    "method_name, request_type, expected_response",
    [
        ("describe", GENERIC_REQUEST, MagicMock(return_value="not a protobuf")),
        (
            "list_all_sessions",
            GENERIC_REQUEST,
            MagicMock(return_value="not a protobuf"),
        ),
        ("list_all_configs", "invalid_request_type", DUMMY_ALLCONFIGKEYS_RESPONSE),
    ],
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
    with pytest.raises(_InactiveRpcError):
        getattr(serialisation_test_suite.stub, method_name)(request_type)
