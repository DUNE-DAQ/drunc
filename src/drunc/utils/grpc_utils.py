from typing import NoReturn

import grpc
from druncschema.generic_pb2 import PlainText
from druncschema.request_response_pb2 import Response, ResponseFlag
from druncschema.token_pb2 import Token
from google.protobuf import any_pb2
from google.rpc import code_pb2
from dataclasses import dataclass
from typing import List
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import Message
from google.rpc import error_details_pb2, status_pb2
from grpc_status import rpc_status
import grpc


from drunc.exceptions import DruncCommandException, DruncException


class UnpackingError(DruncCommandException):
    def __init__(self, data, format):
        self.data = data
        self.format = format

        super().__init__(
            f"Cannot unpack '{data}' to '{format.DESCRIPTOR.name}'",
            code_pb2.INVALID_ARGUMENT,
        )


def unpack_error_response(name: str, text: str, token: Token) -> Response:
    """Create a response for unpacking errors.

    Args:
        name: The name of the command or service.
        text: The error message to include in the response.
        token: The token associated with the request.

    Returns:
        response: the response object containing the error message.
    """
    return Response(
        name=name,
        token=token,
        data=pack_to_any(PlainText(text=text)),
        flag=ResponseFlag.NOT_EXECUTED_BAD_REQUEST_FORMAT,
        children=[],
    )


def pack_to_any(data):
    any = any_pb2.Any()
    any.Pack(data)
    return any


def unpack_any(data, format):
    if not data.Is(format.DESCRIPTOR):
        raise UnpackingError(data, format)
    req = format()
    data.Unpack(req)
    return req


class ServerUnreachable(DruncException):
    def __init__(self, message):
        self.message = message
        super(ServerUnreachable, self).__init__(message)


class ServerTimeout(DruncException):
    def __init__(self, message):
        self.message = message
        super(ServerTimeout, self).__init__(message)


def server_is_reachable(grpc_error):
    if hasattr(grpc_error, "_state"):
        if grpc_error._state.code == grpc.StatusCode.UNAVAILABLE:
            return False

    elif hasattr(grpc_error, "_code"):
        if grpc_error._code == grpc.StatusCode.UNAVAILABLE:
            return False

    return True


def rethrow_if_unreachable_server(grpc_error):
    if not server_is_reachable(grpc_error):
        if hasattr(grpc_error, "_state"):
            raise ServerUnreachable(grpc_error._state.details) from grpc_error
        elif hasattr(grpc_error, "_details"):
            raise ServerUnreachable(grpc_error._details) from grpc_error


def rethrow_if_timeout(grpc_error):
    if hasattr(grpc_error, "_state"):
        if grpc_error._state.code == grpc.StatusCode.DEADLINE_EXCEEDED:
            raise ServerTimeout(grpc_error._state.details) from grpc_error


def handle_grpc_error(error: grpc.RpcError) -> NoReturn:
    """Handle gRPC errors by rethrowing them with appropriate context.

    Args:
        error: The gRPC error to handle.
    """
    rethrow_if_unreachable_server(error)
    rethrow_if_timeout(error)
    raise error


def interrupt_if_unreachable_server(grpc_error):
    if not server_is_reachable(grpc_error):
        if hasattr(grpc_error, "_state"):
            return grpc_error._state.details
        elif hasattr(grpc_error, "_details"):
            return grpc_error._details


def copy_token(token: Token) -> Token:
    """Create a copy of the original token.

    Args:
        token: The original token to copy.

    Returns:
        A copy of the original token.
    """
    token_copy = Token()
    token_copy.CopyFrom(token)
    return token_copy



@dataclass
class GrpcErrorDetails:
    code: str
    message: str
    details: List[str]
    
    def __str__(self):
        lines = [f"[{self.code}] {self.message}"]
        for detail in self.details:
            lines.append(f"  • {detail}")
        return "\n".join(lines)


def format_error_details(detail: Message) -> list[str]:
    """
    Format protobuf message fields into human-readable strings.
    """

    
    results = []
    
    for field in detail.DESCRIPTOR.fields:
        value = getattr(detail, field.name)
        
        # Skip empty values (but keep 0 and False)
        if not value and value != 0 and value is not False:
            continue
        
        # Handle nested messages
        if field.type == FieldDescriptor.TYPE_MESSAGE:
            if field.label == FieldDescriptor.LABEL_REPEATED:
                # Handle repeated nested messages
                for item in value:
                    parts = _extract_message_parts(item)
                    if parts:
                        results.append(f"{field.name}: {', '.join(parts)}")
            else:
                # Handle single nested message
                parts = _extract_message_parts(value)
                if parts:
                    results.append(f"{field.name}: {', '.join(parts)}")
        else:
            # Handle simple fields
            results.append(f"{field.name}: {value}")
    
    return results if results else [str(detail)]


def _extract_message_parts(message: Message) -> list[str]:
    """Extract non-empty field=value pairs from a message."""
    parts = []
    for field in message.DESCRIPTOR.fields:
        value = getattr(message, field.name)
        if value not in (None, "", [], {}) and value != 0 and value is not False:
            parts.append(f"{field.name}={value}")
    return parts


# All known Google error detail types
_ERROR_DETAIL_TYPES = [
    error_details_pb2.BadRequest,
    error_details_pb2.QuotaFailure,
    error_details_pb2.RetryInfo,
    error_details_pb2.PreconditionFailure,
    error_details_pb2.ErrorInfo,
    error_details_pb2.Help,
    error_details_pb2.DebugInfo,
    error_details_pb2.LocalizedMessage,
    error_details_pb2.ResourceInfo,
    error_details_pb2.RequestInfo,
]


def extract_grpc_rich_error(grpc_error: grpc.RpcError) -> GrpcErrorDetails:
    """
    Extract rich error details from a gRPC error using Google's error model.
    
    Args:
        grpc_error: The gRPC error to parse
        
    Returns:
        GrpcErrorDetails with structured error information
    """
    code = grpc_error.code().name if grpc_error.code() else "UNKNOWN"
    try:
        status = rpc_status.from_call(grpc_error)
    except NotImplementedError:
        return GrpcErrorDetails(code=code, message="No message", details=[])
    
    # Fallback to simple error if no rich status
    if status is None:
        return GrpcErrorDetails(code=code, message="No message", details=[])
    
    # Extract all error details
    error_info = None
    error_details = []
    for any_detail in status.details:
        detail_extracted = False
        for detail_type in _ERROR_DETAIL_TYPES:
            if any_detail.Is(detail_type.DESCRIPTOR):
                msg = detail_type()
                any_detail.Unpack(msg)
                error_details.extend(format_error_details(msg))
                detail_extracted = True
                break
        
        # If we couldn't parse the detail, add its type name
        if not detail_extracted:
            error_details.append(f"Unknown detail type: {any_detail.type_url}")
    
    return GrpcErrorDetails(
        code=code,
        message=status.message or "No message",
        details=error_details
    )