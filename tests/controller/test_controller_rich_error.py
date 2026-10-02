"""Test Controller rich error handling with a real gRPC server with RichErrorServerInterceptor
and a real client stub with RichErrorClientInterceptor.

"""

from concurrent import futures
from unittest.mock import MagicMock, patch

import grpc
import pytest
from druncschema.controller_pb2 import StatusRequest, StatusResponse
from druncschema.controller_pb2_grpc import (
    ControllerServicer,
    ControllerStub,
    add_ControllerServicer_to_server,
)
from druncschema.token_pb2 import Token
from google.rpc import error_details_pb2, status_pb2

from drunc.exceptions import DruncSetupException
from drunc.utils.grpc_utils import (
    RichErrorClientInterceptor,
    RichErrorServerInterceptor,
    extract_grpc_rich_error,
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


def test_drunc_exception_rich_error(controller_rich_error_test_suite, status_request):
    """
    Test that a `DruncSetupException` raised by the server is returned to the
    client as a rich gRPC error, and that the client interceptor catches it and
    logs it.
    """
    controller_rich_error_test_suite.setup_server_and_client()
    stub = controller_rich_error_test_suite.stub

    # Patch but allow the real extract_grpc_rich_error to be called as this only happens
    # when the ClientInterceptor catches the RpcError
    with patch(
        "drunc.utils.grpc_utils.extract_grpc_rich_error",
        wraps=extract_grpc_rich_error,
    ) as mock_extract_grpc_rich_error:
        with pytest.raises(grpc.RpcError) as excinfo:
            stub.status(status_request)

    err = excinfo.value

    assert err.code() == grpc.StatusCode.FAILED_PRECONDITION
    assert "Controller is not ready" in err.details()

    # Unpack rich error metadata
    status = status_pb2.Status()
    for key, value in err.trailing_metadata():
        if key == "grpc-status-details-bin":
            status.ParseFromString(value)

            base_error = None
            precond = None

            for detail in status.details:
                if detail.Is(error_details_pb2.ErrorInfo.DESCRIPTOR):
                    base_error = error_details_pb2.ErrorInfo()
                    detail.Unpack(base_error)
                elif detail.Is(error_details_pb2.PreconditionFailure.DESCRIPTOR):
                    precond = error_details_pb2.PreconditionFailure()
                    detail.Unpack(precond)

            assert base_error is not None
            assert base_error.reason == "DruncSetupException"
            assert base_error.domain == "drunc"

            assert precond is not None
            assert len(precond.violations) > 0
            violation = precond.violations[0]
            assert violation.type == "MISSING OR INVALID"
            assert "Controller has not finished initialising" in violation.description

    mock_extract_grpc_rich_error.assert_called_once()
    controller_rich_error_test_suite.mock_client_logger.error.assert_called_once()
