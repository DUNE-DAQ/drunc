"""Test Controller rich error handling with a real gRPC server with RichErrorServerInterceptor
and a real client stub with RichErrorClientInterceptor.

"""

from unittest.mock import patch

import grpc
import pytest
from druncschema.controller_pb2 import StatusResponse
from druncschema.controller_pb2_grpc import ControllerServicer
from google.rpc import error_details_pb2, status_pb2

from drunc.utils.grpc_utils import (
    extract_grpc_rich_error,
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


class _ConfigurableServicer(ControllerServicer):
    """Servicer whose `status` either raises the given exception or succeeds."""

    def __init__(self, exc: Exception | None = None):
        self.exc = exc

    def status(self, request, context):
        if self.exc is not None:
            raise self.exc
        return StatusResponse()


def _error_info(err: grpc.RpcError):
    status = status_pb2.Status()
    for key, value in err.trailing_metadata():
        if key == "grpc-status-details-bin":
            status.ParseFromString(value)
    for detail in status.details:
        if detail.Is(error_details_pb2.ErrorInfo.DESCRIPTOR):
            info = error_details_pb2.ErrorInfo()
            detail.Unpack(info)
            return info
    return None


def test_successful_call(rich_error_stub, status_request):
    """A call that raises nothing returns its response normally."""
    stub = rich_error_stub(_ConfigurableServicer())
    assert isinstance(stub.status(status_request), StatusResponse)


def test_unexpected_exception_becomes_internal_error(rich_error_stub, status_request):
    """A non-Drunc exception is reported as INTERNAL with an UNEXPECTED_SERVER_ERROR reason."""
    stub = rich_error_stub(_ConfigurableServicer(ValueError("boom")))

    with pytest.raises(grpc.RpcError) as excinfo:
        stub.status(status_request)

    err = excinfo.value
    assert err.code() == grpc.StatusCode.INTERNAL
    assert "boom" in err.details()
    info = _error_info(err)
    assert info is not None
    assert info.reason == "UNEXPECTED_SERVER_ERROR"
    assert info.domain == "drunc"
