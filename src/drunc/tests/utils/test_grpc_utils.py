from unittest.mock import Mock, patch

import grpc
import pytest
from google.protobuf.any_pb2 import Any
from google.rpc import status_pb2, error_details_pb2, code_pb2

from drunc.utils.grpc_utils import extract_grpc_rich_error, GrpcErrorDetails


@pytest.fixture
def error_info_detail():
    """Create an ErrorInfo error detail"""
    return error_details_pb2.ErrorInfo(
        reason="RESOURCE_EXHAUSTED",
        domain="example.com",
        metadata={
            "service": "api-gateway",
            "quota_limit": "1000"
        }
    )


@pytest.fixture
def error_info_minimal():
    """Create an ErrorInfo with minimal fields"""
    return error_details_pb2.ErrorInfo(
        reason="INVALID_ARGUMENT"
    )


@pytest.fixture
def error_info_with_empty_metadata():
    """Create an ErrorInfo with empty metadata"""
    return error_details_pb2.ErrorInfo(
        reason="NOT_FOUND",
        domain="api.example.com",
        metadata={}
    )


@pytest.fixture
def bad_request_detail():
    """Create a BadRequest error detail with field violations"""
    bad_request = error_details_pb2.BadRequest()
    violation = bad_request.field_violations.add()
    violation.field = "email"
    violation.description = "Invalid email format"
    return bad_request


@pytest.fixture
def rich_status_with_error_info(error_info_detail):
    """Create a rich Status with ErrorInfo detail"""
    status = status_pb2.Status(
        code=code_pb2.RESOURCE_EXHAUSTED,
        message="Quota exceeded",
    )
    detail = Any()
    detail.Pack(error_info_detail)
    status.details.append(detail)
    return status


@pytest.fixture
def rich_status_with_minimal_error_info(error_info_minimal):
    """Create a rich Status with minimal ErrorInfo"""
    status = status_pb2.Status(
        code=code_pb2.INVALID_ARGUMENT,
        message="Invalid input",
    )
    detail = Any()
    detail.Pack(error_info_minimal)
    status.details.append(detail)
    return status


@pytest.fixture
def rich_status_with_empty_metadata(error_info_with_empty_metadata):
    """Create a rich Status with ErrorInfo with empty metadata"""
    status = status_pb2.Status(
        code=code_pb2.NOT_FOUND,
        message="Resource not found",
    )
    detail = Any()
    detail.Pack(error_info_with_empty_metadata)
    status.details.append(detail)
    return status


def test_extract_grpc_rich_error_with_error_info_full(rich_status_with_error_info):
    """
    Test extract_grpc_rich_error with ErrorInfo containing reason, domain, and metadata.
    """
    grpc_error = Mock(spec=grpc.RpcError)
    grpc_error.code = Mock()
    grpc_error.details = Mock()
    grpc_error.code.return_value = grpc.StatusCode.RESOURCE_EXHAUSTED
    grpc_error.details.return_value = "Quota exceeded"
    
    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=rich_status_with_error_info):
        error_details = extract_grpc_rich_error(grpc_error)
    
    assert error_details.code == "RESOURCE_EXHAUSTED"
    assert error_details.message == "Quota exceeded"
    assert len(error_details.details) > 0
    # Check that ErrorInfo fields are present
    assert any("reason" in detail and "RESOURCE_EXHAUSTED" in detail for detail in error_details.details)
    assert any("domain" in detail and "example.com" in detail for detail in error_details.details)
    assert any("metadata" in detail for detail in error_details.details)


def test_extract_grpc_rich_error_with_error_info_minimal(rich_status_with_minimal_error_info):
    """
    Test extract_grpc_rich_error with ErrorInfo containing only required fields.
    """
    grpc_error = Mock(spec=grpc.RpcError)
    grpc_error.code = Mock()
    grpc_error.details = Mock()
    grpc_error.code.return_value = grpc.StatusCode.INVALID_ARGUMENT
    grpc_error.details.return_value = "Invalid input"
    
    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=rich_status_with_minimal_error_info):
        error_details = extract_grpc_rich_error(grpc_error)
    
    assert error_details.code == "INVALID_ARGUMENT"
    assert error_details.message == "Invalid input"
    assert len(error_details.details) > 0
    assert any("reason" in detail and "INVALID_ARGUMENT" in detail for detail in error_details.details)


def test_extract_grpc_rich_error_with_error_info_empty_metadata(rich_status_with_empty_metadata):
    """
    Test extract_grpc_rich_error with ErrorInfo with empty metadata map.
    """
    grpc_error = Mock(spec=grpc.RpcError)
    grpc_error.code = Mock()
    grpc_error.details = Mock()
    grpc_error.code.return_value = grpc.StatusCode.NOT_FOUND
    grpc_error.details.return_value = "Resource not found"
    
    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=rich_status_with_empty_metadata):
        error_details = extract_grpc_rich_error(grpc_error)
    
    assert error_details.code == "NOT_FOUND"
    assert error_details.message == "Resource not found"
    assert any("reason" in detail and "NOT_FOUND" in detail for detail in error_details.details)
    assert any("domain" in detail and "api.example.com" in detail for detail in error_details.details)


def test_extract_grpc_rich_error_with_error_info_and_bad_request(error_info_detail, bad_request_detail):
    """
    Test extract_grpc_rich_error with both ErrorInfo and BadRequest detail types.
    """
    # Create status with both ErrorInfo and BadRequest
    status = status_pb2.Status(
        code=code_pb2.FAILED_PRECONDITION,
        message="Precondition failed",
    )
    
    detail1 = Any()
    detail1.Pack(error_info_detail)
    status.details.append(detail1)
    
    detail2 = Any()
    detail2.Pack(bad_request_detail)
    status.details.append(detail2)
    
    grpc_error = Mock(spec=grpc.RpcError)
    grpc_error.code = Mock()
    grpc_error.details = Mock()
    grpc_error.code.return_value = grpc.StatusCode.FAILED_PRECONDITION
    grpc_error.details.return_value = "Precondition failed"
    
    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=status):
        error_details = extract_grpc_rich_error(grpc_error)
    
    assert error_details.code == "FAILED_PRECONDITION"
    assert len(error_details.details) >= 2
    # Check for ErrorInfo fields
    assert any("reason" in detail for detail in error_details.details)
    # Check for BadRequest fields
    assert any("email" in detail for detail in error_details.details)


def test_extract_grpc_rich_error_with_error_info_authentication():
    """
    Test extract_grpc_rich_error with ErrorInfo for authentication error.
    """
    error_info = error_details_pb2.ErrorInfo(
        reason="CREDENTIALS_INVALID",
        domain="auth.myapp.com",
        metadata={
            "auth_method": "api_key",
            "error_code": "401"
        }
    )
    
    detail = Any()
    detail.Pack(error_info)
    
    status = status_pb2.Status(
        code=code_pb2.UNAUTHENTICATED,
        message="Invalid API key",
        details=[detail]
    )
    
    grpc_error = Mock(spec=grpc.RpcError)
    grpc_error.code = Mock()
    grpc_error.details = Mock()
    grpc_error.code.return_value = grpc.StatusCode.UNAUTHENTICATED
    grpc_error.details.return_value = "Invalid API key"
    
    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=status):
        error_details = extract_grpc_rich_error(grpc_error)
    
    assert error_details.code == "UNAUTHENTICATED"
    assert error_details.message == "Invalid API key"
    assert any("CREDENTIALS_INVALID" in detail for detail in error_details.details)
    assert any("auth.myapp.com" in detail for detail in error_details.details)


def test_extract_grpc_rich_error_with_error_info_quota_exceeded():
    """
    Test extract_grpc_rich_error with ErrorInfo for quota/resource exhaustion.
    """
    error_info = error_details_pb2.ErrorInfo(
        reason="QUOTA_EXCEEDED",
        domain="billing.myapp.com",
        metadata={
            "quota_type": "requests_per_minute",
            "current_usage": "1000",
            "limit": "1000"
        }
    )
    
    detail = Any()
    detail.Pack(error_info)
    
    status = status_pb2.Status(
        code=code_pb2.RESOURCE_EXHAUSTED,
        message="API quota exceeded",
        details=[detail]
    )
    
    grpc_error = Mock(spec=grpc.RpcError)
    grpc_error.code = Mock()
    grpc_error.details = Mock()
    grpc_error.code.return_value = grpc.StatusCode.RESOURCE_EXHAUSTED
    grpc_error.details.return_value = "API quota exceeded"
    
    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=status):
        error_details = extract_grpc_rich_error(grpc_error)
    
    assert error_details.code == "RESOURCE_EXHAUSTED"
    details_str = "\n".join(error_details.details)
    assert "QUOTA_EXCEEDED" in details_str
    assert "requests_per_minute" in details_str


def test_extract_grpc_rich_error_with_error_info_permission_denied():
    """
    Test extract_grpc_rich_error with ErrorInfo for permission denied.
    """
    error_info = error_details_pb2.ErrorInfo(
        reason="PERMISSION_DENIED",
        domain="auth.example.com",
        metadata={
            "resource": "users/123",
            "permission": "write"
        }
    )
    
    detail = Any()
    detail.Pack(error_info)
    
    status = status_pb2.Status(
        code=code_pb2.PERMISSION_DENIED,
        message="Permission denied",
        details=[detail]
    )
    
    grpc_error = Mock(spec=grpc.RpcError)
    grpc_error.code = Mock()
    grpc_error.details = Mock()
    grpc_error.code.return_value = grpc.StatusCode.PERMISSION_DENIED
    grpc_error.details.return_value = "Permission denied"
    
    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=status):
        error_details = extract_grpc_rich_error(grpc_error)
    
    assert error_details.code == "PERMISSION_DENIED"
    assert any("PERMISSION_DENIED" in detail for detail in error_details.details)
    assert any("auth.example.com" in detail for detail in error_details.details)


def test_extract_grpc_rich_error_with_error_info_rate_limit():
    """
    Test extract_grpc_rich_error with ErrorInfo for rate limiting.
    """
    error_info = error_details_pb2.ErrorInfo(
        reason="RATE_LIMIT_EXCEEDED",
        domain="api.example.com",
        metadata={
            "limit": "100",
            "window": "60s",
            "retry_after": "30"
        }
    )
    
    detail = Any()
    detail.Pack(error_info)
    
    status = status_pb2.Status(
        code=code_pb2.RESOURCE_EXHAUSTED,
        message="Rate limit exceeded",
        details=[detail]
    )
    
    grpc_error = Mock(spec=grpc.RpcError)
    grpc_error.code = Mock()
    grpc_error.details = Mock()
    grpc_error.code.return_value = grpc.StatusCode.RESOURCE_EXHAUSTED
    grpc_error.details.return_value = "Rate limit exceeded"
    
    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=status):
        error_details = extract_grpc_rich_error(grpc_error)
    
    assert error_details.code == "RESOURCE_EXHAUSTED"
    details_str = " ".join(error_details.details)
    assert "metadata" in details_str.lower()
    assert any("RATE_LIMIT_EXCEEDED" in detail for detail in error_details.details)


def test_grpc_error_details_str_representation():
    """
    Test the string representation of GrpcErrorDetails.
    """
    error_details = GrpcErrorDetails(
        code="INVALID_ARGUMENT",
        message="Invalid request",
        details=["field: email", "description: Invalid format"]
    )
    
    str_repr = str(error_details)
    
    assert "[INVALID_ARGUMENT]" in str_repr
    assert "Invalid request" in str_repr
    assert "• field: email" in str_repr
    assert "• description: Invalid format" in str_repr


def test_extract_grpc_rich_error_unknown_detail_type():
    """
    Test extract_grpc_rich_error with unknown/unparseable error detail type.
    """
    # Create an Any with an unknown type
    unknown_any = Any()
    unknown_any.type_url = "type.googleapis.com/unknown.DetailType"
    unknown_any.value = b"some binary data"
    
    status = status_pb2.Status(
        code=code_pb2.UNKNOWN,
        message="Unknown error",
        details=[unknown_any]
    )
    
    grpc_error = Mock(spec=grpc.RpcError)
    grpc_error.code = Mock()
    grpc_error.details = Mock()
    grpc_error.code.return_value = grpc.StatusCode.UNKNOWN
    grpc_error.details.return_value = "Unknown error"
    
    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=status):
        error_details = extract_grpc_rich_error(grpc_error)
    
    assert error_details.code == "UNKNOWN"
    # Should have a detail about the unknown type
    assert any("Unknown detail type" in d for d in error_details.details)
    assert any("unknown.DetailType" in d for d in error_details.details)


def test_extract_grpc_rich_error_with_error_info_full(rich_status_with_error_info):
    """
    Test extract_grpc_rich_error with ErrorInfo containing reason, domain, and metadata.
    """
    grpc_error = Mock(spec=grpc.RpcError)
    grpc_error.code = Mock()
    grpc_error.details = Mock()
    grpc_error.code.return_value = grpc.StatusCode.RESOURCE_EXHAUSTED
    grpc_error.details.return_value = "Quota exceeded"
    
    with patch("drunc.utils.grpc_utils.rpc_status.from_call", return_value=rich_status_with_error_info):
        error_details = extract_grpc_rich_error(grpc_error)
    
    assert error_details.code == "RESOURCE_EXHAUSTED"
    assert error_details.message == "Quota exceeded"
    assert len(error_details.details) > 0
    # Check that ErrorInfo fields are present
    assert any("reason" in detail and "RESOURCE_EXHAUSTED" in detail for detail in error_details.details)
    assert any("domain" in detail and "example.com" in detail for detail in error_details.details)
    assert any("metadata" in detail for detail in error_details.details)

