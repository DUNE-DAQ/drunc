from types import SimpleNamespace
from unittest.mock import MagicMock

import grpc
import pytest
from google.rpc import error_details_pb2

import drunc.controller.controller as controller_module
from drunc.controller.controller import Controller
from drunc.exceptions import DruncCommandException, ChildCommandException
from druncschema.controller_pb2 import ExecuteFSMCommandResponse, FSMResponseFlag
from druncschema.request_response_pb2 import ResponseFlag
from drunc.utils.grpc_utils import GrpcErrorDetails, ServerTimeout


class FakeRpcError(grpc.RpcError):
    pass

@pytest.fixture
def controller():
    instance = Controller.__new__(Controller)
    instance.name = "test-controller"
    instance.log = MagicMock()
    instance.connectivity_service = None
    instance.children_nodes = []
    instance.opmon_publisher = None
    instance.stop_event = None
    return instance


def test_propagation_degrades_rpc_error_and_keeps_sibling(
    controller, monkeypatch
):
    child_detail = error_details_pb2.ErrorInfo(
        reason="REMOTE_CHILD_FAILURE",
        domain="test",
    )

    def fake_extract_grpc_rich_error(_error):
        return GrpcErrorDetails(
            code="INTERNAL",
            message="child failed",
            details=[child_detail],
        )

    monkeypatch.setattr(
        controller_module,
        "extract_grpc_rich_error",
        fake_extract_grpc_rich_error,
    )

    children = [
        (SimpleNamespace(name="C_with_failure"), ""),
        (SimpleNamespace(name="C_no_failure"), ""),
    ]

    # Simulate calling child processes, where one fails and one succeeds
    def call_child(child, _target):
        if child.name == "C_with_failure":
            raise FakeRpcError("child failed")
        return ExecuteFSMCommandResponse(
            name=child.name,
            command_name="start",
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            fsm_flag=FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY,
        )

    responses = Controller.propagate_concurrently(
        call_child,
        children,
        degrade_response=lambda child, failure: controller._degrade_fsm_response(
            child, failure, "start"
        ),
    )
    # Should return 2 responses, one for each child
    assert len(responses) == 2
    assert [response.name for response in responses] == ["C_with_failure", "C_no_failure"]

    # The first response is that of thechild that failed
    assert responses[0].flag == ResponseFlag.DRUNC_EXCEPTION_THROWN
    assert responses[0].fsm_flag == FSMResponseFlag.FSM_FAILED

    # The second response is that of the child that succeeded
    assert responses[1].flag == ResponseFlag.EXECUTED_SUCCESSFULLY

    # Check that the attached ResponseError contains the error details
    # from failing child
    unpacked_details = []
    for packed in responses[0].error.details:
        detail = error_details_pb2.ErrorInfo()
        if packed.Unpack(detail):
            unpacked_details.append(detail)

    assert any(
        detail.reason == "REMOTE_CHILD_FAILURE"
        for detail in unpacked_details
    )


def test_propagation_degrades_local_drunc_exception(controller):
    """Test that propagation degrades local DruncCommandException correctly."""
    child = SimpleNamespace(name="local-child")

    def call_child(_child, _target):
        raise DruncCommandException("local failure")

    responses = Controller.propagate_concurrently(
        call_child,
        [(child, "")],
        degrade_response=lambda child, failure: controller._degrade_fsm_response(
            child, failure, "start"
        ),
    )

    assert responses[0].flag == ResponseFlag.DRUNC_EXCEPTION_THROWN
    assert responses[0].fsm_flag == FSMResponseFlag.FSM_FAILED
    assert "local failure" in responses[0].error.message


def test_propagation_does_not_degrade_timeout():
    """Test that propagation does not degrade ServerTimeout exceptions."""
    def call_child(_child, _target):
        raise ServerTimeout("timed out")

    with pytest.raises(ServerTimeout):
        Controller.propagate_concurrently(
            call_child,
            [(SimpleNamespace(name="slow-child"), "")],
            degrade_response=lambda *_: pytest.fail("timeout was degraded"),
        )


def test_propagation_does_not_degrade_unexpected_local_exception(controller):
    def call_child(_child, _target):
        raise RuntimeError("Unexpected local failure")

    with pytest.raises(RuntimeError, match="Unexpected local failure"):
        Controller.propagate_concurrently(
            call_child,
            [(SimpleNamespace(name="child"), "")],
            degrade_response=lambda child, failure: controller._degrade_fsm_response(
                child, failure, "start"
            ),
        )


def test_propagation_without_degrade_callback_raises_child_exception(
    monkeypatch,
):
    """Test that propagation without a degrade callback raises ChildCommandException."""
    child = SimpleNamespace(name="C_with_failure")
    rpc_error = FakeRpcError("remote failure")
    original_detail = error_details_pb2.ErrorInfo(
        reason="REMOTE_FAILURE",
        domain="test",
    )

    monkeypatch.setattr(
        controller_module,
        "extract_grpc_rich_error",
        lambda _error: GrpcErrorDetails(
            code="INTERNAL",
            message="remote failure",
            details=[original_detail],
        ),
    )

    def call_child(_child, _target):
        raise rpc_error

    with pytest.raises(ChildCommandException) as exc_info:
        Controller.propagate_concurrently(
            call_child,
            [(child, "")], # do not provide a degrade_response callback
        )

    failure = exc_info.value
    assert failure.child_name == "C_with_failure"
    assert failure.child_code == "INTERNAL"
    assert original_detail in failure.rich_details


def test_nested_rpc_error_preserves_grandchild_details(controller, monkeypatch):
    grandchild_detail = error_details_pb2.ErrorInfo(
        reason="CHILDREN_FAILURE",
        domain="test",
    )
    # Make the grandchild return an error with rich details
    monkeypatch.setattr(
        controller_module,
        "extract_grpc_rich_error",
        lambda _error: GrpcErrorDetails(
            code="INTERNAL",
            message="grandchild failed",
            details=[grandchild_detail],
        ),
    )

    # Helper function to create a successful response for a given name
    def success(name):
        return ExecuteFSMCommandResponse(
            name=name,
            command_name="start",
            flag=ResponseFlag.EXECUTED_SUCCESSFULLY,
            fsm_flag=FSMResponseFlag.FSM_EXECUTED_SUCCESSFULLY,
        )

    # Simulate a middle child that calls thier own child (a grandchild) and propagates its error
    def call_middle(_child, _target):
        grandchildren = Controller.propagate_concurrently(
            lambda _grandchild, _target: (_ for _ in ()).throw(
                FakeRpcError("grandchild failed")
            ),
            [(SimpleNamespace(name="grandchild"), "")],
            degrade_response=lambda grandchild, failure: controller._degrade_fsm_response(
                grandchild, failure, "start"
            ),
        )

        middle_child = success("middle")
        middle_child.children.extend(grandchildren)
        return middle_child

    root_children = Controller.propagate_concurrently(
        call_middle,
        [(SimpleNamespace(name="middle"), "")],
        degrade_response=lambda child, failure: controller._degrade_fsm_response(
            child, failure, "start"
        ),
    )

    middle_child = root_children[0]
    grandchild = middle_child.children[0]

    assert middle_child.name == "middle"
    assert grandchild.name == "grandchild"
    assert grandchild.fsm_flag == FSMResponseFlag.FSM_FAILED
    assert grandchild.flag == ResponseFlag.DRUNC_EXCEPTION_THROWN

    unpacked = []
    for packed in grandchild.error.details:
        detail = error_details_pb2.ErrorInfo()
        if packed.Unpack(detail):
            unpacked.append(detail)

    assert any(detail.reason == "CHILDREN_FAILURE" for detail in unpacked)