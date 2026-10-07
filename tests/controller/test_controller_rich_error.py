"""Test Controller rich error handling with a real gRPC server with RichErrorServerInterceptor
and a real client stub with RichErrorClientInterceptor.

"""

from concurrent import futures
from types import MethodType
from unittest.mock import MagicMock, patch

import grpc
import pytest
from druncschema.controller_pb2 import StatusRequest, StatusResponse
from druncschema.controller_pb2_grpc import (
    ControllerServicer,
    ControllerStub,
    add_ControllerServicer_to_server,
)
from druncschema.request_response_pb2 import ResponseFlag
from druncschema.token_pb2 import Token
from google.rpc import error_details_pb2, status_pb2

from drunc.controller.controller import Controller
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

#################################################################################
# Test Controller propagate_concurently
#################################################################################

class FakeChild:
    def __init__(
        self,
        name: str,
        stub: ControllerStub,
        included: bool = True,
    ):
        self.name = name
        self.included = included
        self._stub = stub

    def status(self, target: str = "") -> StatusResponse:
        return self._stub.status(
            StatusRequest(
                token=Token(),
                target=target,
                execute_along_path=False,
                execute_on_all_subsequent_children_in_path=True,
            )
        )

@pytest.fixture
def controller_degrader():
    controller = MagicMock(spec=Controller)
    controller.log = MagicMock()

    controller._degrade_response = MethodType(
        Controller._degrade_response,
        controller,
    )
    controller._degrade_status_response = MethodType(
        Controller._degrade_status_response,
        controller,
    )

    return controller

class ParentPropagatingServicer(ControllerServicer):
    """Dummy parent servicer that propagates to a single failing child via
    Controller.propagate_concurrently, exercising the safe_call wrapper."""

    def __init__(self, child: FakeChild):
        self.child = child

    def status(self, request: StatusRequest, context) -> StatusResponse:
        Controller.propagate_concurrently(
            lambda child, target: child.status(target),
            [(self.child, "")],
        )
        return StatusResponse(name="parent", token=request.token)  # not reached


def test_propagate_concurrently_terminal_error(status_request):
    """
    Test than when a child raises a DruncSetupException, the parent catches the grpc.RpcError
    and raises a ChildCommandFailure with the child's rich error details.
    """
    # Child server which raises DruncSetupException.
    child_server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=5),
        interceptors=[RichErrorServerInterceptor()],
    )
    add_ControllerServicer_to_server(DummyControllerServicer(), child_server)
    child_port = child_server.add_insecure_port("[::]:0")
    child_server.start()

    child_raw_channel = grpc.insecure_channel(f"localhost:{child_port}")
    child_channel = grpc.intercept_channel(
        child_raw_channel, RichErrorClientInterceptor(logger=MagicMock())
    )
    child_stub = ControllerStub(child_channel)
    fake_child = FakeChild(name="child1", stub=child_stub)

    # Parent server which propagates to the child and catches the RpcError.
    parent_server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=5),
        interceptors=[RichErrorServerInterceptor()],
    )
    add_ControllerServicer_to_server(
        ParentPropagatingServicer(child=fake_child), parent_server
    )
    parent_port = parent_server.add_insecure_port("[::]:0")
    parent_server.start()

    parent_channel = grpc.insecure_channel(f"localhost:{parent_port}")
    parent_stub = ControllerStub(parent_channel)

    try:
        with pytest.raises(grpc.RpcError) as excinfo:
            parent_stub.status(status_request)

        err = excinfo.value
        assert err.code() == grpc.StatusCode.INTERNAL
        assert "child1" in err.details()

        status = status_pb2.Status()
        for key, value in err.trailing_metadata():
            if key == "grpc-status-details-bin":
                status.ParseFromString(value)

        error_infos = []
        precond = None
        for detail in status.details:
            if detail.Is(error_details_pb2.ErrorInfo.DESCRIPTOR):
                info = error_details_pb2.ErrorInfo()
                detail.Unpack(info)
                error_infos.append(info)
            elif detail.Is(error_details_pb2.PreconditionFailure.DESCRIPTOR):
                precond = error_details_pb2.PreconditionFailure()
                detail.Unpack(precond)

        # Two ErrorInfo -one for the parent's wrapper, and the
        # child's original error, each with their own `reason`.
        # assert len(error_infos) == 2
        
        parent_error = next(
            info
            for info in error_infos
            if info.reason == "CHILD_COMMAND_EXECUTION_FAILED"
        )
        child_error = next(
            info
            for info in error_infos
            if info.reason == "DruncSetupException"
        )

        assert parent_error.domain == "drunc"
        assert parent_error.metadata["message"] == "Child 'child1' failed"

        assert child_error.domain == "drunc"
        assert "Controller is not ready" in child_error.metadata["message"]
    finally:
        parent_channel.close()
        child_channel.close()
        parent_server.stop(grace=0)
        child_server.stop(grace=0)


def test_propagate_concurrently_degrades_child_error(controller_degrader):
    child_server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=5),
        interceptors=[RichErrorServerInterceptor()],
    )
    add_ControllerServicer_to_server(DummyControllerServicer(), child_server)
    child_port = child_server.add_insecure_port("[::]:0")
    child_server.start()

    child_raw_channel = grpc.insecure_channel(f"localhost:{child_port}")
    child_channel = grpc.intercept_channel(
        child_raw_channel,
        RichErrorClientInterceptor(logger=MagicMock()),
    )
    child = FakeChild(
        name="child1",
        stub=ControllerStub(child_channel),
    )

    try:
        responses = Controller.propagate_concurrently(
            lambda child, target: child.status(target),
            [(child, "")],
            abort_on_child_error=False,
            degrade_response=controller_degrader._degrade_status_response,
        )

        assert len(responses) == 1

        response = responses[0]
        assert response.name == "child1"
        assert response.flag == ResponseFlag.DRUNC_EXCEPTION_THROWN
        assert response.status.state == "error"
        assert response.status.sub_state == "error"
        assert response.status.in_error
        assert response.status.included

        assert response.error.message == "Child 'child1' failed"
        assert len(response.error.details) == 2

        original_error = error_details_pb2.ErrorInfo()
        assert response.error.details[0].Unpack(original_error)
        assert original_error.reason == "DruncSetupException"
        assert original_error.metadata["message"] == "Controller is not ready"

        precondition = error_details_pb2.PreconditionFailure()
        assert response.error.details[1].Unpack(precondition)
        assert len(precondition.violations) == 1
        assert (
            precondition.violations[0].description
            == "Controller has not finished initialising"
        )

        controller_degrader.log.error.assert_called_once()

    finally:
        child_channel.close()
        child_server.stop(grace=0)