"""
Test suite for Process Manager gRPC serialisation/deserialisation.

This suite tests that protobuf message serialisation and deserialisation works correctly
for all process manager endpoints using a real server/client setup. The focus is purely
on ensuring message types can be correctly serialised/deserialised across the network
boundary without caring about the actual endpoint logic.

Note that the process manager servicer is mocked for these tests - it's completely independent
of the process manager implementation.

If any of the tests fail it's likely that druncschema is out of sync with the dummy requests/responses
set up in conftest.py. Any updates there will also be needed in the process manager itself.
"""

from concurrent import futures
from unittest.mock import MagicMock

import grpc
import pytest
from druncschema.process_manager_pb2_grpc import (
    ProcessManagerStub,
    add_ProcessManagerServicer_to_server,
)


class ProcessManagerSerialisationTestSuite:
    """Test suite for gRPC serialisation/deserialisation verification."""

    def __init__(self):
        self.server_port = "50051"
        self.server_address = f"localhost:{self.server_port}"
        self.server = None
        self.channel = None
        self.stub = None
        self.servicer = None

    def setup_server_and_client(self, method_name, mock_response):
        """
        Initialise a real gRPC server and client for serialisation testing.

        Args:
            method_name: Name of the method to mock (e.g., 'boot', 'kill')
            mock_response: The response object to return from the mocked method
        """
        # Create a mock servicer for testing serialisation only
        self.servicer = MagicMock()

        # Configure the specific method to return the mock response
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
    Pytest fixture for ProcessManagerSerialisationTestSuite.

    Yields:
        ProcessManagerSerialisationTestSuite: Test suite instance with server/client lifecycle management
    """
    suite = ProcessManagerSerialisationTestSuite()
    yield suite
    suite.teardown_server_and_client()


def test_boot_serialisation(serialisation_test_suite, boot_request, boot_response):
    """Test boot endpoint serialisation/deserialisation."""
    serialisation_test_suite.setup_server_and_client("boot", boot_response)

    response = serialisation_test_suite.stub.boot(boot_request)
    assert response == boot_response


def test_kill_serialisation(
    serialisation_test_suite, process_query_request, kill_response
):
    """Test kill endpoint serialisation/deserialisation."""
    serialisation_test_suite.setup_server_and_client("kill", kill_response)

    response = serialisation_test_suite.stub.kill(process_query_request)
    assert response == kill_response


def test_restart_serialisation(
    serialisation_test_suite, process_query_request, restart_response
):
    """Test restart endpoint serialisation/deserialisation."""
    serialisation_test_suite.setup_server_and_client("restart", restart_response)

    response = serialisation_test_suite.stub.restart(process_query_request)
    assert response == restart_response


def test_ps_serialisation(serialisation_test_suite, process_query_request, ps_response):
    """Test ps endpoint serialisation/deserialisation."""
    serialisation_test_suite.setup_server_and_client("ps", ps_response)

    response = serialisation_test_suite.stub.ps(process_query_request)
    assert response == ps_response


def test_terminate_serialisation(
    serialisation_test_suite, generic_request, terminate_response
):
    """Test terminate endpoint serialisation/deserialisation."""
    serialisation_test_suite.setup_server_and_client("terminate", terminate_response)

    response = serialisation_test_suite.stub.terminate(generic_request)
    assert response == terminate_response


def test_logs_serialisation(serialisation_test_suite, log_request, logs_response):
    """Test logs endpoint serialisation/deserialisation."""
    serialisation_test_suite.setup_server_and_client("logs", logs_response)

    response = serialisation_test_suite.stub.logs(log_request)
    assert response == logs_response


def test_describe_serialisation(
    serialisation_test_suite, generic_request, describe_response
):
    """Test describe endpoint serialisation/deserialisation."""
    serialisation_test_suite.setup_server_and_client("describe", describe_response)

    response = serialisation_test_suite.stub.describe(generic_request)
    assert response == describe_response


def test_flush_serialisation(
    serialisation_test_suite, process_query_request, flush_response
):
    """Test flush endpoint serialisation/deserialisation."""
    serialisation_test_suite.setup_server_and_client("flush", flush_response)

    response = serialisation_test_suite.stub.flush(process_query_request)
    assert response == flush_response


def test_wrong_request_type_raises_serialisation_error(
    serialisation_test_suite, boot_response
):
    """
    Test that wrong request types trigger gRPC errors with a valid response.

    This validates that tests will fail if the request type is changed for an
    endpoint, ensuring the serialisation validation is working correctly.
    """
    from grpc._channel import _InactiveRpcError

    # Using boot method as representative test - not necessary to test all endpoints this way
    serialisation_test_suite.setup_server_and_client("boot", boot_response)

    # Expect gRPC error when sending invalid request type
    with pytest.raises(_InactiveRpcError) as exc_info:
        serialisation_test_suite.stub.boot("invalid_request_type")

    # Check specific error code and message
    error = exc_info.value
    assert error.code() == grpc.StatusCode.INTERNAL
    assert "exception serializing request!" in error.details().lower()


def test_wrong_response_type_raises_deserialisation_error(
    serialisation_test_suite, boot_request
):
    """
    Test that wrong response types trigger gRPC errors with a valid request.

    This validates that tests will fail if the response type is changed for an
    endpoint, ensuring the deserialisation validation is working correctly.
    """
    from grpc._channel import _InactiveRpcError

    # Using boot method as representative test - not necessary to test all endpoints this way
    # MagicMock will return an invalid response type, triggering deserialisation error
    serialisation_test_suite.setup_server_and_client("boot", MagicMock())

    # Expect gRPC error when server returns invalid response type
    with pytest.raises(_InactiveRpcError) as exc_info:
        serialisation_test_suite.stub.boot(boot_request)

    # Check specific error code and message
    error = exc_info.value
    assert error.code() == grpc.StatusCode.INTERNAL
    assert "failed to serialize response!" in error.details().lower()
