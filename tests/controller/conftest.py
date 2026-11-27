from concurrent import futures
from unittest.mock import MagicMock

import grpc
import pytest
from druncschema.controller_pb2 import StatusRequest, StatusResponse
from druncschema.controller_pb2_grpc import (
    ControllerServicer,
    ControllerStub,
    add_ControllerServicer_to_server,
)
from druncschema.token_pb2 import Token

from drunc.exceptions import DruncSetupException
from drunc.utils.grpc_utils import (
    RichErrorClientInterceptor,
    RichErrorServerInterceptor,
)


class DummyControllerServicer(ControllerServicer):
    """
    Dummy ControllerServicer used to trigger `DruncException`s without
    building a full `Controller`. This could be replaced by any other
    Exception raised in the `Controller` methods.
    """

    def status(self, request: StatusRequest, context) -> StatusResponse:
        raise DruncSetupException(
            message="Controller is not ready",
            details="Controller has not finished initialising",
        )


class ControllerRichErrorTestSuite:
    """Test suite for rich error message propagation for the Controller."""

    def __init__(self):
        self.server_port = "50054"
        self.server_address = f"localhost:{self.server_port}"
        self.server = None
        self.channel = None
        self.stub = None
        self.servicer = None
        self.mock_client_logger = None

    def setup_server_and_client(self):
        """Initialise a real gRPC server and client for testing rich error handling."""
        self.servicer = DummyControllerServicer()

        self.server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=10),
            interceptors=[RichErrorServerInterceptor()],
        )
        add_ControllerServicer_to_server(self.servicer, self.server)
        listen_addr = f"[::]:{self.server_port}"
        self.server.add_insecure_port(listen_addr)
        self.server.start()

        # Create a mock logger for the client interceptor
        self.mock_client_logger = MagicMock()
        client_interceptor = RichErrorClientInterceptor(logger=self.mock_client_logger)

        raw_channel = grpc.insecure_channel(self.server_address)
        self.channel = grpc.intercept_channel(raw_channel, client_interceptor)
        self.stub = ControllerStub(self.channel)

    def teardown_server_and_client(self):
        """Clean up gRPC server and client resources."""
        if self.channel:
            self.channel.close()
        if self.server:
            self.server.stop(grace=0)
        self.stub = None
        self.servicer = None


@pytest.fixture(scope="function")
def controller_rich_error_test_suite():
    """
    Pytest fixture for ControllerRichErrorTestSuite.
    """
    suite = ControllerRichErrorTestSuite()
    yield suite
    suite.teardown_server_and_client()


@pytest.fixture(scope="function")
def status_request():
    return StatusRequest(
        token=Token(),
        target="",
        execute_along_path=False,
        execute_on_all_subsequent_children_in_path=True,
    )


@pytest.fixture
def rich_error_stub():
    """Yield a factory building a stub against a server using the rich error interceptor."""
    servers, channels = [], []

    def make(servicer):
        server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=2),
            interceptors=[RichErrorServerInterceptor()],
        )
        add_ControllerServicer_to_server(servicer, server)
        port = server.add_insecure_port("localhost:0")
        server.start()
        channel = grpc.insecure_channel(f"localhost:{port}")
        servers.append(server)
        channels.append(channel)
        return ControllerStub(channel)

    yield make
    for channel in channels:
        channel.close()
    for server in servers:
        server.stop(grace=0)
