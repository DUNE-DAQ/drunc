from dataclasses import dataclass
from typing import List, NoReturn, Optional

import grpc
from druncschema.generic_pb2 import PlainText
from druncschema.request_response_pb2 import Response, ResponseFlag
from druncschema.token_pb2 import Token
from google.protobuf import any_pb2
from google.protobuf.descriptor import FieldDescriptor
from google.protobuf.message import Message
from google.rpc import code_pb2, error_details_pb2
from grpc_status import rpc_status

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


def server_is_reachable(grpc_error: grpc.RpcError) -> bool:
    """
    Check if server is reachable.

    Args:
        grpc_error (grpc.RpcError): The gRPC error

    Returns:
        bool: True if the server is reachable, False if the error indicates it is unavailable
    """
    if hasattr(grpc_error, "_state"):
        if grpc_error._state.code == grpc.StatusCode.UNAVAILABLE:
            return False

    elif hasattr(grpc_error, "_code"):
        if grpc_error._code == grpc.StatusCode.UNAVAILABLE:
            return False

    return True


def rethrow_if_unreachable_server(grpc_error: grpc.RpcError) -> NoReturn:
    """
    Raise a ServerUnreachable exception if the gRPC error indicates the server is unreachable.

    Args:
        grpc_error (grpc.RpcError): The gRPC error

    Raises:
        ServerUnreachable: If the error indicates the server is unavailable
    """
    if not server_is_reachable(grpc_error):
        if hasattr(grpc_error, "_state"):
            raise ServerUnreachable(grpc_error._state.details) from grpc_error
        elif hasattr(grpc_error, "_details"):
            raise ServerUnreachable(grpc_error._details) from grpc_error


def rethrow_if_timeout(grpc_error: grpc.RpcError) -> NoReturn:
    """
    Raise a ServerTimeout if timeout.

    Args:
        grpc_error (grpc.RpcError): The gRPC error

    Raises:
        ServerTimeout: If the error code is DEADLINE_EXCEEDED
    """
    if hasattr(grpc_error, "_state"):
        if grpc_error._state.code == grpc.StatusCode.DEADLINE_EXCEEDED:
            raise ServerTimeout(grpc_error._state.details) from grpc_error


def handle_grpc_error(error: grpc.RpcError) -> NoReturn:
    """
    Handle gRPC errors by rethrowing them with appropriate context.

    Args:
        error: The gRPC error to handle.
    Raises:
        A custom exception if the error matches a known category, or the original
        gRPC error if no classification applies.
    """
    rethrow_if_unreachable_server(error)
    rethrow_if_timeout(error)
    raise error


def interrupt_if_unreachable_server(grpc_error: grpc.RpcError) -> Optional[str]:
    """
    Interrupt if server is not reachable and return the error details.

    Args:
        grpc_error (grpc.RpcError): The gRPC error

    Returns:
        str | None: The internal error details if the server is unreachable and details are available;
                    otherwise, returns None.
    """
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
    """
    A structured representation of a gRPC error, including its status code,
    message, and any extracted rich error details.

    Attributes:
        code (str): The gRPC status code name (e.g., "NOT_FOUND")
        message (str): The error message from the gRPC status
        details (List[str]): A list of formatted error detail strings
    """

    code: str
    message: str
    details: List[str]

    def __str__(self):
        """
        Return a human-readable string representation of the error.
        """
        lines = [f"[{self.code}] {self.message}"]
        for detail in self.details:
            lines.append(f"{detail}")
        return "\n".join(lines)


def format_error_details(detail: Message) -> list[str]:
    """
    Format protobuf message fields into human-readable strings.

    Args:
        detail (Message): A protobuf message representing a gRPC error detail

    Returns:
        list[str]: A list of formatted strings describing the message's fields and values.
                    Format: "field_name: value" for simple messages
                    or "field_name: field1=value1, field2=value2" for nested messages
    """

    results = []

    for field in detail.DESCRIPTOR.fields:
        value = getattr(detail, field.name)

        # Skip empty values
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
    """
    Extract field=value pairs from a message.

    Args:
        message (Message): A protobuf message instance to extract fields from

    Returns:
        list[str]: A list of strings representing non-empty field=value pairs
    """
    parts = []
    for field in message.DESCRIPTOR.fields:
        value = getattr(message, field.name)
        if value not in (None, "", [], {}) and value != 0 and value is not False:
            parts.append(f"{field.name}={value}")
    return parts


# All known Google error detail types.
# More info here https://github.com/googleapis/googleapis/blob/master/google/rpc/error_details.proto
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

        if not detail_extracted:
            error_details.append(f"Unknown detail type: {any_detail.type_url}")

    return GrpcErrorDetails(
        code=code, message=status.message or "No message", details=error_details
    )
