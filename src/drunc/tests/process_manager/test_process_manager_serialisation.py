"""
Integration test suite for Process Manager gRPC serialisation/deserialisation.

This suite tests that protobuf message serialisation and deserialisation works correctly
for all process manager endpoints using a real server/client setup. The focus is purely
on ensuring message types can be correctly serialised/deserialised across the network
boundary without caring about the actual endpoint logic
"""

from concurrent import futures
from unittest.mock import MagicMock, patch

import grpc
import pytest
from druncschema.process_manager_pb2_grpc import (
    ProcessManagerStub,
    add_ProcessManagerServicer_to_server,
)

from drunc.tests.process_manager.process_manager_mock_impls import (
    ConcreteProcessManager,
)


class ProcessManagerserialisationTestSuite:
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
            method_name: Name of the method to mock (e.g., 'boot', 'kill')
            mock_response: The response object to return from the mocked method
        """
        # Mock the logger to prevent logging interference during tests
        with patch("drunc.process_manager.process_manager.get_logger") as mock_logger:
            mock_logger_instance = MagicMock()
            mock_logger.return_value = mock_logger_instance
            self.servicer = ConcreteProcessManager(session="test_session")

        # Mock only the specific method being tested before adding servicer to server
        setattr(self.servicer, method_name, MagicMock(return_value=mock_response))

        # Configure and start the gRPC server
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        add_ProcessManagerServicer_to_server(self.servicer, self.server)
        listen_addr = f"[::]:{self.server_port}"
        self.server.add_insecure_port(listen_addr)
        self.server.start()

        # Create client channel and stub
        self.channel = grpc.insecure_channel(self.server_address)
        self.stub = ProcessManagerStub(self.channel)

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
    Pytest fixture for ProcessManagerserialisationTestSuite.
    """
    suite = ProcessManagerserialisationTestSuite()
    yield suite
    suite.teardown_server_and_client()


def test_boot_serialisation(serialisation_test_suite):
    """Test boot endpoint serialisation/deserialisation."""
    from drunc.tests.process_manager.dummy_requests import BOOT_REQUEST
    from drunc.tests.process_manager.dummy_responses import BOOT_RESPONSE

    serialisation_test_suite.setup_server_and_client("boot", BOOT_RESPONSE)

    response = serialisation_test_suite.stub.boot(BOOT_REQUEST)
    assert response == BOOT_RESPONSE


def test_kill_serialisation(serialisation_test_suite):
    """Test kill endpoint serialisation/deserialisation."""
    from drunc.tests.process_manager.dummy_requests import PROCESS_QUERY_REQUEST
    from drunc.tests.process_manager.dummy_responses import KILL_RESPONSE

    serialisation_test_suite.setup_server_and_client("kill", KILL_RESPONSE)

    response = serialisation_test_suite.stub.kill(PROCESS_QUERY_REQUEST)
    assert response == KILL_RESPONSE


def test_restart_serialisation(serialisation_test_suite):
    """Test restart endpoint serialisation/deserialisation."""
    from drunc.tests.process_manager.dummy_requests import PROCESS_QUERY_REQUEST
    from drunc.tests.process_manager.dummy_responses import RESTART_RESPONSE

    serialisation_test_suite.setup_server_and_client("restart", RESTART_RESPONSE)

    response = serialisation_test_suite.stub.restart(PROCESS_QUERY_REQUEST)
    assert response == RESTART_RESPONSE


def test_ps_serialisation(serialisation_test_suite):
    """Test ps endpoint serialisation/deserialisation."""
    from drunc.tests.process_manager.dummy_requests import PROCESS_QUERY_REQUEST
    from drunc.tests.process_manager.dummy_responses import PS_RESPONSE

    serialisation_test_suite.setup_server_and_client("ps", PS_RESPONSE)

    response = serialisation_test_suite.stub.ps(PROCESS_QUERY_REQUEST)
    assert response == PS_RESPONSE


def test_terminate_serialisation(serialisation_test_suite):
    """Test terminate endpoint serialisation/deserialisation."""
    from drunc.tests.process_manager.dummy_requests import GENERIC_REQUEST
    from drunc.tests.process_manager.dummy_responses import TERMINATE_RESPONSE

    serialisation_test_suite.setup_server_and_client("terminate", TERMINATE_RESPONSE)

    response = serialisation_test_suite.stub.terminate(GENERIC_REQUEST)
    assert response == TERMINATE_RESPONSE


def test_logs_serialisation(serialisation_test_suite):
    """Test logs endpoint serialisation/deserialisation."""
    from drunc.tests.process_manager.dummy_requests import LOG_REQUEST
    from drunc.tests.process_manager.dummy_responses import LOGS_RESPONSE

    serialisation_test_suite.setup_server_and_client("logs", LOGS_RESPONSE)

    response = serialisation_test_suite.stub.logs(LOG_REQUEST)
    assert response == LOGS_RESPONSE


def test_describe_serialisation(serialisation_test_suite):
    """Test describe endpoint serialisation/deserialisation."""
    from drunc.tests.process_manager.dummy_requests import GENERIC_REQUEST
    from drunc.tests.process_manager.dummy_responses import DESCRIBE_RESPONSE

    serialisation_test_suite.setup_server_and_client("describe", DESCRIBE_RESPONSE)

    response = serialisation_test_suite.stub.describe(GENERIC_REQUEST)
    assert response == DESCRIBE_RESPONSE


def test_flush_serialisation(serialisation_test_suite):
    """Test flush endpoint serialisation/deserialisation."""
    from drunc.tests.process_manager.dummy_requests import PROCESS_QUERY_REQUEST
    from drunc.tests.process_manager.dummy_responses import FLUSH_RESPONSE

    serialisation_test_suite.setup_server_and_client("flush", FLUSH_RESPONSE)

    response = serialisation_test_suite.stub.flush(PROCESS_QUERY_REQUEST)
    assert response == FLUSH_RESPONSE


def test_wrong_request_type_raises_serialisation_error(serialisation_test_suite):
    """
    Test that wrong request types trigger gRPC errors with a valid response.

    This validates that tests will fail if the request type is changed for an
    endpoint, ensuring the serialisation validation is working correctly.
    """
    from grpc._channel import _InactiveRpcError

    from drunc.tests.process_manager.dummy_responses import BOOT_RESPONSE

    # Using boot method as representative test - not necessary to test all endpoints this way
    serialisation_test_suite.setup_server_and_client("boot", BOOT_RESPONSE)

    # Expect gRPC error when sending invalid request type
    with pytest.raises(_InactiveRpcError) as exc_info:
        serialisation_test_suite.stub.boot("invalid_request_type")

    # Check specific error code and message
    error = exc_info.value
    assert error.code() == grpc.StatusCode.INTERNAL
    assert "exception serializing request!" in error.details().lower()


def test_wrong_response_type_raises_deserialisation_error(serialisation_test_suite):
    """
    Test that wrong response types trigger gRPC errors with a valid request.

    This validates that tests will fail if the response type is changed for an
    endpoint, ensuring the deserialisation validation is working correctly.
    """
    from grpc._channel import _InactiveRpcError

    from drunc.tests.process_manager.dummy_requests import BOOT_REQUEST

    # Using boot method as representative test - not necessary to test all endpoints this way
    # MagicMock will return an invalid response type, triggering deserialisation error
    serialisation_test_suite.setup_server_and_client("boot", MagicMock())

    # Expect gRPC error when server returns invalid response type
    with pytest.raises(_InactiveRpcError) as exc_info:
        serialisation_test_suite.stub.boot(BOOT_REQUEST)

    # Check specific error code and message
    error = exc_info.value
    assert error.code() == grpc.StatusCode.INTERNAL
    assert "failed to serialize response!" in error.details().lower()
