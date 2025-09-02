"""
Integration test suite for Process Manager gRPC serialisation/deserialisation.

This suite tests that protobuf message serialisation and deserialisation works correctly
for all process manager endpoints using a real server/client setup. The focus is purely
on ensuring message types can be correctly serialized/deserialized across the network
boundary without caring about the actual endpoint logic
"""

from concurrent import futures
from unittest.mock import MagicMock, patch

import google.protobuf.any_pb2
import grpc
import pytest
from druncschema.description_pb2 import Description
from druncschema.process_manager_pb2 import (
    BootRequest,
    LogLines,
    LogRequest,
    ProcessDescription,
    ProcessInstance,
    ProcessInstanceList,
    ProcessMetadata,
    ProcessQuery,
    ProcessRestriction,
    ProcessUUID,
)
from druncschema.process_manager_pb2_grpc import (
    ProcessManagerStub,
    add_ProcessManagerServicer_to_server,
)
from druncschema.request_response_pb2 import Request, ResponseFlag
from druncschema.token_pb2 import Token

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
        """Initialize a real gRPC server and client for serialisation testing.

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
    mock_response = ProcessInstanceList(
        name="boot_endpoint",
        token=Token(),
        values=[
            ProcessInstance(
                uuid=ProcessUUID(uuid="test-boot-uuid"),
                status_code=ProcessInstance.StatusCode.RUNNING,
                return_code=0,
            )
        ],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    serialisation_test_suite.setup_server_and_client("boot", mock_response)

    request = BootRequest(
        token=Token(),
        process_description=ProcessDescription(
            metadata=ProcessMetadata(name="test_process")
        ),
        process_restriction=ProcessRestriction(),
    )

    response = serialisation_test_suite.stub.boot(request)
    assert response == mock_response


def test_kill_serialisation(serialisation_test_suite):
    """Test kill endpoint serialisation/deserialisation."""
    mock_response = ProcessInstanceList(
        name="kill_endpoint",
        token=Token(),
        values=[],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    serialisation_test_suite.setup_server_and_client("kill", mock_response)

    request = ProcessQuery(
        token=Token(),
        uuids=[ProcessUUID(uuid="test-uuid")],
        names=["test_process"],
        user="test_user",
        session="test_session",
    )

    response = serialisation_test_suite.stub.kill(request)
    assert response == mock_response


def test_restart_serialisation(serialisation_test_suite):
    """Test restart endpoint serialisation/deserialisation."""
    mock_response = ProcessInstanceList(
        name="restart_endpoint",
        token=Token(),
        values=[],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    serialisation_test_suite.setup_server_and_client("restart", mock_response)

    request = ProcessQuery(
        token=Token(),
        uuids=[ProcessUUID(uuid="test-uuid")],
        names=["test_process"],
        user="test_user",
        session="test_session",
    )

    response = serialisation_test_suite.stub.restart(request)
    assert response == mock_response


def test_ps_serialisation(serialisation_test_suite):
    """Test ps endpoint serialisation/deserialisation."""
    mock_response = ProcessInstanceList(
        name="ps_endpoint",
        token=Token(),
        values=[],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    serialisation_test_suite.setup_server_and_client("ps", mock_response)

    request = ProcessQuery(
        token=Token(),
        uuids=[ProcessUUID(uuid="test-uuid")],
        names=["test_process"],
        user="test_user",
        session="test_session",
    )

    response = serialisation_test_suite.stub.ps(request)
    assert response == mock_response


def test_terminate_serialisation(serialisation_test_suite):
    """Test terminate endpoint serialisation/deserialisation."""
    mock_response = ProcessInstanceList(
        name="terminate_endpoint",
        token=Token(),
        values=[],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    serialisation_test_suite.setup_server_and_client("terminate", mock_response)

    data_any = google.protobuf.any_pb2.Any()
    data_any.value = b"test_data"
    request = Request(token=Token(), data=data_any)

    response = serialisation_test_suite.stub.terminate(request)
    assert response == mock_response


def test_logs_serialisation(serialisation_test_suite):
    """Test logs endpoint serialisation/deserialisation."""
    mock_response = LogLines(
        name="logs_endpoint",
        token=Token(),
        uuid=ProcessUUID(uuid="test-uuid"),
        lines=["test log line 1", "test log line 2"],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    serialisation_test_suite.setup_server_and_client("logs", mock_response)

    request = LogRequest(
        token=Token(),
        query=ProcessQuery(
            token=Token(),
            uuids=[ProcessUUID(uuid="test-uuid")],
            names=["test_process"],
            user="test_user",
            session="test_session",
        ),
        how_far=100,
    )

    response = serialisation_test_suite.stub.logs(request)
    assert response == mock_response


def test_describe_serialisation(serialisation_test_suite):
    """Test describe endpoint serialisation/deserialisation."""
    mock_response = Description(
        type="process_manager",
        name="test_process_manager",
        info="/var/log/test",
        session="test_session",
        commands=[],
        children=[],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        token=Token(),
    )

    serialisation_test_suite.setup_server_and_client("describe", mock_response)

    data_any = google.protobuf.any_pb2.Any()
    data_any.value = b"test_data"
    request = Request(token=Token(), data=data_any)

    response = serialisation_test_suite.stub.describe(request)
    assert response == mock_response


def test_flush_serialisation(serialisation_test_suite):
    """Test flush endpoint serialisation/deserialisation."""
    mock_response = ProcessInstanceList(
        name="flush_endpoint",
        token=Token(),
        values=[],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )

    serialisation_test_suite.setup_server_and_client("flush", mock_response)

    request = ProcessQuery(
        token=Token(),
        uuids=[ProcessUUID(uuid="test-uuid")],
        names=["test_process"],
        user="test_user",
        session="test_session",
    )

    response = serialisation_test_suite.stub.flush(request)
    assert response == mock_response


def test_wrong_request_type_raises_serialisation_error(serialisation_test_suite):
    """
    Test that wrong request types trigger gRPC errors with a valid response.

    This validates that tests will fail if the request type is changed for an
    endpoint, ensuring the serialisation validation is working correctly.
    """
    from grpc._channel import _InactiveRpcError

    # Using boot method as representative test - not necessary to test all endpoints this way
    mock_response = ProcessInstanceList(
        name="boot_endpoint",
        token=Token(),
        values=[
            ProcessInstance(
                uuid=ProcessUUID(uuid="test-boot-uuid"),
                status_code=ProcessInstance.StatusCode.RUNNING,
                return_code=0,
            )
        ],
        flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
    )
    serialisation_test_suite.setup_server_and_client("boot", mock_response)

    # Expect gRPC error when sending invalid request type
    with pytest.raises(_InactiveRpcError):
        serialisation_test_suite.stub.boot("invalid_request_type")


def test_wrong_response_type_raises_deserialisation_error(serialisation_test_suite):
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
    with pytest.raises(_InactiveRpcError):
        request = BootRequest(
            token=Token(),
            process_description=ProcessDescription(
                metadata=ProcessMetadata(name="test_process")
            ),
            process_restriction=ProcessRestriction(),
        )
        serialisation_test_suite.stub.boot(request)
