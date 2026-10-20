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
from druncschema.request_response_pb2 import ResponseFlag
from druncschema.token_pb2 import Token
from google.rpc import error_details_pb2, status_pb2

from drunc.controller.controller import Controller
from drunc.exceptions import ChildCommandTerminalException, DruncSetupException
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

    def setup_server_and_client(self, servicer=None):
        """Initialise a real gRPC server and client for testing rich error handling."""
        self.servicer = servicer or DummyControllerServicer()

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



###### Test propagate_concurrenlty



class FakeRichDetails:
    details: list = []


class FakeRpcError(grpc.RpcError):
    """Minimal stand-in so safe_call's `getattr(error, 'rich_error_details', None)` succeeds."""
    def __init__(self):
        self.rich_error_details = FakeRichDetails()


def make_child(name: str) -> MagicMock:
    child = MagicMock()
    child.name = name
    return child


class FakeSelf:
    def __init__(self):
        self.log = MagicMock()

    _degrade_response = Controller._degrade_response


def make_bare_controller() -> Controller:
    """Construct a real Controller instance without running __init__,
    for unit-testing methods that only need `self.log`.

    Also sets the attributes terminate() touches, since __del__ calls
    terminate() when this bare instance is garbage-collected.
    """
    controller = Controller.__new__(Controller)
    controller.log = MagicMock()
    controller.name = "bare-test-controller"
    controller.connectivity_service = None
    controller.opmon_publisher = None
    controller.stop_event = None
    return controller


class PropagatingStatusServicer(ControllerServicer):
    """Servicer whose status() uses propagate_concurrently with a degrade_response,
    so a child RPC failure degrades gracefully instead of aborting the whole call."""

    def __init__(self):
        super().__init__()
        self.controller = Controller.__new__(Controller)
        self.controller.log = MagicMock()
        self.controller.name = "bare-test-controller"
        self.controller.connectivity_service = None
        self.controller.opmon_publisher = None
        self.controller.stop_event = None

    def status(self, request: StatusRequest, context) -> StatusResponse:
        good = make_child("good")
        good.status.return_value = StatusResponse(
            token=None, name="good", flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )

        bad = make_child("bad")
        bad.status.side_effect = FakeRpcError()

        child_responses = Controller.propagate_concurrently(
            lambda child, target: child.status(target),
            [(good, "good"), (bad, "bad")],
            degrade_response=self.controller._degrade_status_response,
        )

        response = StatusResponse(
            token=None, name="parent", flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
        )
        response.children.extend(child_responses)
        return response


def test_degrade_status_response_marks_child_in_error():
    bad = make_child("bad")
    bad.included = True
    failure = ChildCommandTerminalException(child_name="bad", child_details=[])

    controller = make_bare_controller()
    response = controller._degrade_status_response(bad, failure)

    assert response.flag == ResponseFlag.DRUNC_EXCEPTION_THROWN
    assert response.status.in_error is True
    assert response.status.state == "error"


def test_propagate_concurrently_degrades_over_real_rpc(
    controller_rich_error_test_suite, status_request
):
    """A child RPC failure inside propagate_concurrently must not abort the
    parent's own RPC when a degrade_response is supplied."""
    controller_rich_error_test_suite.setup_server_and_client(
        servicer=PropagatingStatusServicer()
    )
    stub = controller_rich_error_test_suite.stub

    response = stub.status(status_request)  # should NOT raise grpc.RpcError

    children_by_name = {child.name: child for child in response.children}
    assert children_by_name["good"].flag == ResponseFlag.EXECUTED_SUCCESSFULLY
    assert children_by_name["bad"].flag == ResponseFlag.DRUNC_EXCEPTION_THROWN
    assert children_by_name["bad"].status.in_error is True


def make_status_response(name: str, flag=ResponseFlag.EXECUTED_SUCCESSFULLY) -> StatusResponse:
    return StatusResponse(token=None, name=name, flag=flag)


def test_propagate_concurrently_nested_grandchild_degrades():
    """Parent -> A -> (B, C); C fails non-terminally and is degraded at A's
    level, so A itself still reports success up to Parent."""
    controller = make_bare_controller()

    # Leaf children of A.
    child_b = make_child("B")
    child_b.status.return_value = make_status_response("B")

    child_c = make_child("C")
    child_c.status.side_effect = FakeRpcError()

    def a_status(target):
        # A's own status propagates to its children (B, C), degrading C's failure.
        grandchild_responses = Controller.propagate_concurrently(
            lambda child, t: child.status(t),
            [(child_b, "B"), (child_c, "C")],
            degrade_response=controller._degrade_status_response,
        )
        response = make_status_response("A")
        response.children.extend(grandchild_responses)
        return response

    child_a = make_child("A")
    child_a.status.side_effect = a_status

    # Parent -> A.
    parent_responses = Controller.propagate_concurrently(
        lambda child, target: child.status(target),
        [(child_a, "A")],
        degrade_response=controller._degrade_status_response,
    )

    assert len(parent_responses) == 1
    a_response = parent_responses[0]

    # A itself did not fail — C's failure was absorbed at A's own level.
    assert a_response.name == "A"
    assert a_response.flag == ResponseFlag.EXECUTED_SUCCESSFULLY

    grandchildren_by_name = {c.name: c for c in a_response.children}
    assert grandchildren_by_name["B"].flag == ResponseFlag.EXECUTED_SUCCESSFULLY
    assert grandchildren_by_name["C"].flag == ResponseFlag.DRUNC_EXCEPTION_THROWN
    assert grandchildren_by_name["C"].status.in_error is True


def test_degrade_response_sets_error_message_and_details():
    bad = make_child("bad")
    detail = error_details_pb2.ErrorInfo(reason="SOME_REASON", domain="drunc")
    failure = ChildCommandTerminalException(child_name="bad", child_details=[detail])

    controller = make_bare_controller()
    response = controller._degrade_response(StatusResponse, bad, failure)

    assert response.error.message == str(failure)
    assert "bad" in response.error.message  # ChildCommandExceptionCommon's message includes child_name
    assert len(response.error.details) == 1



def test_degrade_response_details_roundtrip_through_any():
    bad = make_child("bad")
    original_detail = error_details_pb2.ErrorInfo(reason="SOME_REASON", domain="drunc")
    failure = ChildCommandTerminalException(child_name="bad", child_details=[original_detail])

    controller = make_bare_controller()
    response = controller._degrade_response(StatusResponse, bad, failure)

    packed = response.error.details[0]
    assert packed.Is(error_details_pb2.ErrorInfo.DESCRIPTOR)

    unpacked = error_details_pb2.ErrorInfo()
    packed.Unpack(unpacked)
    assert unpacked.reason == "SOME_REASON"
    assert unpacked.domain == "drunc"


def test_degrade_response_with_no_details_leaves_details_empty():
    bad = make_child("bad")
    failure = ChildCommandTerminalException(child_name="bad", child_details=[])

    controller = make_bare_controller()
    response = controller._degrade_response(StatusResponse, bad, failure)

    assert list(response.error.details) == []