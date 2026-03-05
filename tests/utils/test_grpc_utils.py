from unittest.mock import patch

import grpc
import pytest
from google.protobuf.any_pb2 import Any
from google.rpc import error_details_pb2, status_pb2

from drunc.utils.grpc_utils import GrpcErrorDetails, extract_grpc_rich_error


def make_grpc_error_with_details(code, message, detail_messages):
    """
    Create a mocked grpc.RpcError and a corresponding rich Status object
    containing packed error detail messages.

    Args:
        code (grpc.StatusCode): The gRPC status code
        message (str): The error message
        detail_messages (List[Message]): List of protobuf error detail messages

    Returns:
        Tuple[grpc.RpcError, Status]: A fake gRPC error and its associated rich status.
    """
    status = status_pb2.Status(code=code.value[0], message=message)
    for detail_msg in detail_messages:
        any_detail = Any()
        any_detail.Pack(detail_msg)
        status.details.append(any_detail)

    class FakeRpcError(grpc.RpcError):
        def code(self):
            return code

    return FakeRpcError(), status


def test_grpc_error_details_validation_success():
    """
    Test that GrpcErrorDetails validates on creation.
    """
    valid = GrpcErrorDetails(code="OK", message="Success", details=["All good"])
    assert str(valid) == "[OK] Success\nAll good"


def test_grpc_error_details_validation_fail():
    """
    Test that GrpcErrorDetails validates on creation.
    """

    class BrokenGrpcErrorDetails:
        def __str__(self):
            raise AttributeError("BrokenGrpcErrorDetails")

    with pytest.raises(TypeError) as excinfo:
        GrpcErrorDetails(
            code="FAIL", message="Fail", details=[BrokenGrpcErrorDetails()]
        )

    assert "Invalid data in GrpcErrorDetails" in str(excinfo.value)


def test_bad_request_detail():
    """
    Test extraction of BadRequest error details.
    """
    detail = error_details_pb2.BadRequest(
        field_violations=[
            error_details_pb2.BadRequest.FieldViolation(
                field="email", description="Invalid format"
            )
        ]
    )
    grpc_error, status = make_grpc_error_with_details(
        grpc.StatusCode.INVALID_ARGUMENT, "Bad request", [detail]
    )

    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=status):
        result = extract_grpc_rich_error(grpc_error)

    assert result.code == "INVALID_ARGUMENT"
    field_violation = result.details[0].field_violations[0]
    assert "Invalid format" in field_violation.description


def test_quota_failure_detail():
    """
    Test extraction of QuotaFailure error details.
    """
    detail = error_details_pb2.QuotaFailure(
        violations=[
            error_details_pb2.QuotaFailure.Violation(
                subject="user", description="Quota exceeded"
            )
        ]
    )
    grpc_error, status = make_grpc_error_with_details(
        grpc.StatusCode.RESOURCE_EXHAUSTED, "Quota issue", [detail]
    )

    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=status):
        result = extract_grpc_rich_error(grpc_error)

    assert result.code == "RESOURCE_EXHAUSTED"
    violation = result.details[0].violations[0]
    assert violation.subject == "user"
    assert violation.description == "Quota exceeded"


def test_error_info_detail():
    """
    Test extraction of ErrorInfo error details.
    """
    detail = error_details_pb2.ErrorInfo(reason="NOT_FOUND", domain="example.com")
    grpc_error, status = make_grpc_error_with_details(
        grpc.StatusCode.NOT_FOUND, "Missing resource", [detail]
    )

    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=status):
        result = extract_grpc_rich_error(grpc_error)

    assert result.code == "NOT_FOUND"
    assert result.details[0].reason == "NOT_FOUND"
    assert result.details[0].domain == "example.com"
    assert result.details[0].metadata == {}


def test_precondition_failure_detail():
    """
    Test extraction of PreconditionFailure error details.
    """
    detail = error_details_pb2.PreconditionFailure(
        violations=[
            error_details_pb2.PreconditionFailure.Violation(
                type="LOCKED", subject="resource", description="Locked"
            )
        ]
    )
    grpc_error, status = make_grpc_error_with_details(
        grpc.StatusCode.FAILED_PRECONDITION, "Precondition failed", [detail]
    )

    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=status):
        result = extract_grpc_rich_error(grpc_error)

    assert result.code == "FAILED_PRECONDITION"
    assert result.details[0].violations[0].type == "LOCKED"
    assert result.details[0].violations[0].subject == "resource"
    assert result.details[0].violations[0].description == "Locked"


def test_help_detail():
    """
    Test extraction of Help error details.
    """
    detail = error_details_pb2.Help(
        links=[error_details_pb2.Help.Link(description="See docs", url="link_docs")]
    )
    grpc_error, status = make_grpc_error_with_details(
        grpc.StatusCode.INVALID_ARGUMENT, "Need help", [detail]
    )

    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=status):
        result = extract_grpc_rich_error(grpc_error)

    assert result.code == "INVALID_ARGUMENT"
    assert len(result.details) == 1
    assert len(result.details[0].links) == 1
    assert result.details[0].links[0].description == "See docs"
    assert result.details[0].links[0].url == "link_docs"


def test_debug_info_detail():
    """
    Test extraction of DebugInfo error details.
    """
    detail = error_details_pb2.DebugInfo(
        stack_entries=["func1()", "func2()"], detail="trace"
    )
    grpc_error, status = make_grpc_error_with_details(
        grpc.StatusCode.INTERNAL, "Debug info", [detail]
    )

    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=status):
        result = extract_grpc_rich_error(grpc_error)

    assert result.code == "INTERNAL"
    assert result.details[0].stack_entries == ["func1()", "func2()"]
    assert result.details[0].detail == "trace"


def test_localised_message_detail():
    """
    Test extraction of LocalizedMessage error details.
    """
    detail = error_details_pb2.LocalizedMessage(
        locale="en-US", message="Something went wrong"
    )
    grpc_error, status = make_grpc_error_with_details(
        grpc.StatusCode.INTERNAL, "Localized error", [detail]
    )

    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=status):
        result = extract_grpc_rich_error(grpc_error)

    assert result.code == "INTERNAL"
    assert result.details[0].locale == "en-US"
    assert result.details[0].message == "Something went wrong"


def test_resource_info_detail():
    """
    Test extraction of ResourceInfo error details.
    """
    detail = error_details_pb2.ResourceInfo(
        resource_type="db",
        resource_name="users",
        owner="admin",
        description="Access denied",
    )
    grpc_error, status = make_grpc_error_with_details(
        grpc.StatusCode.PERMISSION_DENIED, "Resource issue", [detail]
    )

    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=status):
        result = extract_grpc_rich_error(grpc_error)

    assert result.code == "PERMISSION_DENIED"
    assert result.details[0].resource_type == "db"
    assert result.details[0].resource_name == "users"
    assert result.details[0].owner == "admin"
    assert result.details[0].description == "Access denied"


def test_request_info_detail():
    """
    Test extraction of RequestInfo error details.
    """
    detail = error_details_pb2.RequestInfo(
        request_id="test_request", serving_data="metadata"
    )
    grpc_error, status = make_grpc_error_with_details(
        grpc.StatusCode.INTERNAL, "Request info", [detail]
    )

    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=status):
        result = extract_grpc_rich_error(grpc_error)

    assert result.code == "INTERNAL"
    assert result.details[0].request_id == "test_request"
    assert result.details[0].serving_data == "metadata"


def test_unknown_detail_type():
    """
    Test unknown detail type.
    """
    any_detail = Any()
    any_detail.type_url = "unknown_test_url"
    status = status_pb2.Status(
        code=grpc.StatusCode.UNKNOWN.value[0],
        message="Unknown error",
        details=[any_detail],
    )

    class FakeRpcError(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.UNKNOWN

    grpc_error = FakeRpcError()

    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=status):
        result = extract_grpc_rich_error(grpc_error)

    assert result.code == "UNKNOWN"
    assert any("Unknown detail type" in d for d in result.details)


def test_rpc_status_not_implemented():
    """
    Test that extract_grpc_rich_error handles NotImplementedError from rpc_status.from_call.
    Return the correct code and an empty details list.
    """

    class FakeRpcError(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.INTERNAL

    grpc_error = FakeRpcError()

    with patch(
        "drunc.utils.grpc_utils.rpc_status.from_call", side_effect=NotImplementedError
    ):
        result = extract_grpc_rich_error(grpc_error)

    assert result.code == "INTERNAL"
    assert result.details == []


def test_rpc_status_none():
    """
    Test that extract_grpc_rich_error handles a None return from rpc_status.from_call.
    Return the correct code and an empty details list.
    """

    class FakeRpcError(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.INTERNAL

    grpc_error = FakeRpcError()

    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=None):
        result = extract_grpc_rich_error(grpc_error)

    assert result.code == "INTERNAL"
    assert result.details == []


def test_empty_detail_message():
    """
    Test that extract_grpc_rich_error handles a detail message with no populated fields.
    Should fallback to using str(detail) and include it in the details list.
    """
    detail = error_details_pb2.RetryInfo()  # No fields set
    grpc_error, status = make_grpc_error_with_details(
        grpc.StatusCode.UNAVAILABLE, "Retry later", [detail]
    )

    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=status):
        result = extract_grpc_rich_error(grpc_error)

    assert result.code == "UNAVAILABLE"
    assert len(result.details) == 1  # Should fallback to str(detail)


def test_multiple_detail_types():
    """
    Test that extract_grpc_rich_error correctly extracts multiple known error detail types.
    Should include formatted output from both ErrorInfo and Help messages.
    """
    detail1 = error_details_pb2.ErrorInfo(reason="NOT_FOUND", domain="example.com")
    detail2 = error_details_pb2.Help(
        links=[error_details_pb2.Help.Link(description="See docs", url="docs_link")]
    )
    grpc_error, status = make_grpc_error_with_details(
        grpc.StatusCode.INVALID_ARGUMENT, "Multiple issues", [detail1, detail2]
    )

    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=status):
        result = extract_grpc_rich_error(grpc_error)

    assert result.code == "INVALID_ARGUMENT"
    assert len(result.details) == 2
    assert result.details[0].reason == "NOT_FOUND"
    assert result.details[0].domain == "example.com"
    assert result.details[1].links[0].description == "See docs"
    assert result.details[1].links[0].url == "docs_link"
